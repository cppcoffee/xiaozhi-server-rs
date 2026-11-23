use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::AsrCfg;

mod doubao;
pub use doubao::DoubaoAsr;

const MSG_FULL_CLIENT_REQUEST: u8 = 0x01;
const MSG_AUDIO_ONLY_REQUEST: u8 = 0x02;
const SERIAL_JSON: u8 = 0x01;
const COMPRESS_GZIP: u8 = 0x01;
const FLAG_SEQUENCE: u8 = 0x01;
const FLAG_LAST: u8 = 0x02;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const ASR_PACKET_MS: u32 = 120;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);

/// Steps to wait for the final result after flush (100ms/step). Doubao normally answers in <1s;
/// 10s avoids dropping a whole turn just because the result was a bit slow
const FLUSH_WAIT_STEPS: usize = 100;

#[derive(Debug)]
pub enum AsrCmd {
    Start(CancellationToken),
    Pcm(Vec<i16>),
    Flush,
}

pub struct AsrOutcome {
    pub turn: CancellationToken,
    pub text: String,
}

fn gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

fn gunzip(data: &[u8]) -> Result<Vec<u8>> {
    let dec = flate2::read::GzDecoder::new(data);
    let mut out = Vec::new();
    dec.take(MAX_RESPONSE_BYTES as u64 + 1)
        .read_to_end(&mut out)?;
    ensure!(
        out.len() <= MAX_RESPONSE_BYTES,
        "Decompressed ASR response exceeds the size limit"
    );
    Ok(out)
}

fn frame(msg_type: u8, last: bool, sequence: Option<i32>, payload: &[u8]) -> Result<Vec<u8>> {
    let body = gzip(payload)?;
    let mut buf = Vec::with_capacity(12 + body.len());
    let mut flags = if last { FLAG_LAST } else { 0 };
    if sequence.is_some() {
        flags |= FLAG_SEQUENCE;
    }
    let serialization = if msg_type == MSG_AUDIO_ONLY_REQUEST && sequence.is_some() {
        0 // Raw PCM in API Key mode.
    } else {
        SERIAL_JSON
    };
    buf.extend_from_slice(&[
        0x11,
        (msg_type << 4) | flags,
        (serialization << 4) | COMPRESS_GZIP,
        0,
    ]);
    if let Some(sequence) = sequence {
        buf.extend_from_slice(&(if last { -sequence } else { sequence }).to_be_bytes());
    }
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Parse a server response frame
fn parse_frame(res: &[u8], legacy: bool) -> Result<(Value, bool)> {
    ensure!(
        res.len() >= 4 && res[0] >> 4 == 1,
        "Invalid ASR response header"
    );
    let header_size = usize::from(res[0] & 0x0f) * 4;
    ensure!(
        header_size >= 4 && res.len() >= header_size,
        "Incomplete ASR response header"
    );
    let msg_type = res[1] >> 4;
    let flags = res[1] & 0x0f;
    let compress = res[2] & 0x0f;
    ensure!(
        matches!(msg_type, 0x09 | 0x0f),
        "Unknown ASR response type: {msg_type}"
    );
    let mut data = &res[header_size..];
    let error_code = if msg_type == 0x0f {
        Some(read_u32(&mut data)?)
    } else {
        if flags & FLAG_SEQUENCE != 0 || (legacy && data.starts_with(&[0; 4]) && data.len() > 4) {
            read_u32(&mut data)?;
        }
        None
    };
    let len = read_u32(&mut data)? as usize;
    ensure!(len == data.len(), "Invalid ASR response payload length");
    let decompressed;
    if compress == COMPRESS_GZIP {
        decompressed = gunzip(data)?;
        data = &decompressed;
    } else {
        ensure!(
            compress == 0,
            "Unknown ASR response compression format: {compress}"
        );
    }
    if let Some(code) = error_code {
        let message: String = String::from_utf8_lossy(data).chars().take(240).collect();
        bail!("ASR returned error code={code}: {message}");
    }
    let value = serde_json::from_slice(data).context("Invalid ASR response JSON")?;
    Ok((value, flags & FLAG_LAST != 0))
}

fn read_u32(data: &mut &[u8]) -> Result<u32> {
    let (bytes, rest) = data
        .split_first_chunk()
        .context("Incomplete ASR response field")?;
    *data = rest;
    Ok(u32::from_be_bytes(*bytes))
}

fn request_json(cfg: &AsrCfg, sample_rate: u32) -> Value {
    if cfg.speech_api_key.is_some() {
        return json!({
            "user": { "uid": "xiaozhi-rust" },
            "audio": { "format": "pcm", "codec": "raw", "rate": sample_rate, "bits": 16, "channel": 1 },
            "request": {
                "model_name": "bigmodel", "result_type": "full",
                "enable_itn": true, "enable_punc": true, "show_utterances": true
            }
        });
    }
    json!({
        "app": { "appid": cfg.appid, "token": cfg.access_token },
        "user": { "uid": "streaming_asr_service" },
        "request": {
            "reqid": Uuid::new_v4().to_string(),
            "workflow": "audio_in,resample,partition,vad,fe,decode,itn,nlu_punctuate",
            "show_utterances": true,
            "result_type": "single",
            "sequence": 1,
            "end_window_size": 200,
            "corpus": {"boosting_table_name": "", "correct_table_name": ""}
        },
        "audio": {
            "format": "pcm",
            "codec": "pcm",
            "rate": sample_rate,
            "bits": 16,
            "channel": 1,
            "sample_rate": sample_rate
        }
    })
}

/// Extract text from the definite utterances of a response frame
fn extract_text(v: &Value) -> Option<String> {
    if let Some(code) = v.get("code").and_then(|c| c.as_i64()) {
        // 1013: no valid speech, which is normal
        if code == 1013 {
            return None;
        }
    }
    let result = v.get("result")?;
    let mut out = String::new();
    if let Some(utts) = result.get("utterances").and_then(|u| u.as_array()) {
        for u in utts {
            if u.get("definite").and_then(|d| d.as_bool()).unwrap_or(false) {
                if let Some(t) = u.get("text").and_then(|t| t.as_str()) {
                    out.push_str(t);
                }
            }
        }
    }
    if out.trim().is_empty() {
        None
    } else {
        Some(out.trim().to_string())
    }
}

/// Drain every frame already received on the ws without blocking, returning any final text
async fn drain_pending(asr: &mut DoubaoAsr, wait_ms: u64) -> Result<Option<String>> {
    let modern = asr.cfg.speech_api_key.is_some();
    let mut text = None;
    let mut first = true;
    loop {
        let timeout = if first {
            std::time::Duration::from_millis(wait_ms)
        } else {
            std::time::Duration::from_millis(0)
        };
        first = false;
        let Some(ws) = asr.ws.as_mut() else {
            return Ok(text);
        };
        match tokio::time::timeout(timeout, ws.next()).await {
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                let (value, last) = parse_frame(&bytes, !modern)?;
                if let Some(code) = value["code"].as_i64() {
                    ensure!(
                        matches!(code, 0 | 1000 | 1013 | 20_000_000),
                        "ASR recognition failed with code={code}"
                    );
                }
                if asr.preconnected {
                    asr.preconnected = false;
                    asr.replay_pcm.clear();
                }
                if modern {
                    if let Some(confirmed) = extract_text(&value) {
                        asr.confirmed_text = Some(confirmed);
                    }
                    // Optimized streaming may emit interim text before the user stops speaking.
                    if last {
                        let final_text = value["result"]["text"]
                            .as_str()
                            .map(str::trim)
                            .filter(|text| !text.is_empty())
                            .map(str::to_owned)
                            .or_else(|| asr.confirmed_text.take())
                            .unwrap_or_default();
                        info!("ASR final transcript: {final_text}");
                        return Ok(Some(final_text));
                    }
                } else if let Some(recognized) = extract_text(&value) {
                    info!("ASR transcript: {recognized}");
                    text = Some(recognized);
                }
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                asr.ws = None;
                return Ok(text);
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(error))) => return Err(error.into()),
            Err(_) => return Ok(text),
        }
    }
}

/// Run one cancellable ASR stream per accepted listen start.
pub async fn run_asr(
    cfg: AsrCfg,
    sample_rate: u32,
    mut rx: mpsc::Receiver<AsrCmd>,
    out: mpsc::Sender<AsrOutcome>,
    cancel: CancellationToken,
) {
    let mut asr = DoubaoAsr::new(cfg, sample_rate);
    let mut active: Option<CancellationToken> = None;
    loop {
        let idle_watch = !asr.has_pcm && asr.ws.is_some();
        let cmd = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = async {
                match &active {
                    Some(turn) => turn.cancelled().await,
                    None => std::future::pending().await,
                }
            } => {
                asr.reset();
                active = None;
                continue;
            }
            _ = asr.wait_idle_close(), if idle_watch => {
                debug!("Preconnected ASR socket closed while idle; will reconnect on speech");
                asr.reset();
                continue;
            }
            cmd = rx.recv() => match cmd { Some(cmd) => cmd, None => break },
        };
        if let AsrCmd::Start(turn) = cmd {
            asr.reset();
            active = Some(turn.clone());
            // Pre-connect ASR WS during pre-speech silence so the ~270ms TLS+init handshake
            // overlaps with the user starting to speak instead of blocking the first audio frames.
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = turn.cancelled() => {
                    asr.reset();
                    active = None;
                }
                res = asr.connect() => {
                    match res {
                        Ok(()) => asr.preconnected = true,
                        Err(e) => {
                            debug!("ASR preconnect failed. Retrying on the first audio packet: {e:#}");
                            asr.reset();
                        }
                    }
                }
            }
            continue;
        }
        let Some(turn) = active.clone() else { continue };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = turn.cancelled() => {
                asr.reset();
                active = None;
                continue;
            }
            result = read_result(&mut asr, cmd) => result,
        };
        if let Some(text) = result {
            // A final result retires this input. Queued PCM and duplicate stops have no effect.
            asr.reset();
            active = None;
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = turn.cancelled() => {},
                result = out.send(AsrOutcome { turn: turn.clone(), text }) => {
                    if result.is_err() { break; }
                }
            }
        }
    }
}

async fn replay_preconnected(asr: &mut DoubaoAsr, replay: &[i16]) -> Result<()> {
    debug!(
        samples = replay.len(),
        "Preconnected ASR expired; reconnecting and replaying initial audio"
    );
    asr.reset();
    asr.connect().await?;
    asr.send_pcm(replay).await
}

async fn send_pcm_and_drain(asr: &mut DoubaoAsr, pcm: &[i16]) -> Result<Option<String>> {
    let attempt = match asr.send_pcm(pcm).await {
        Ok(()) => drain_pending(asr, 0).await,
        Err(e) => Err(e),
    };
    let expired = matches!(&attempt, Err(_)) || (matches!(&attempt, Ok(None)) && asr.ws.is_none());
    if expired && asr.preconnected && !asr.replay_pcm.is_empty() {
        let replay = std::mem::take(&mut asr.replay_pcm);
        replay_preconnected(asr, &replay).await?;
        return drain_pending(asr, 0).await;
    }
    attempt
}

async fn flush_and_wait(asr: &mut DoubaoAsr) -> Result<Option<String>> {
    let started = tokio::time::Instant::now();
    asr.flush().await?;
    for _ in 0..FLUSH_WAIT_STEPS {
        if let Some(text) = drain_pending(asr, 100).await? {
            debug!(
                elapsed_ms = started.elapsed().as_millis(),
                "ASR flush to final"
            );
            return Ok(Some(text));
        }
        if asr.ws.is_none() {
            break;
        }
    }
    // Complete a silent or failed input so the device can leave listening.
    Ok(Some(String::new()))
}

async fn read_result(asr: &mut DoubaoAsr, cmd: AsrCmd) -> Option<String> {
    let result = match cmd {
        AsrCmd::Pcm(pcm) => send_pcm_and_drain(asr, &pcm).await,
        AsrCmd::Flush if !asr.has_pcm => Ok(Some(String::new())),
        AsrCmd::Flush => {
            let attempt = flush_and_wait(asr).await;
            let expired = matches!(&attempt, Err(_))
                || (matches!(&attempt, Ok(Some(t)) if t.is_empty()) && asr.ws.is_none());
            if expired && asr.preconnected && !asr.replay_pcm.is_empty() {
                let replay = std::mem::take(&mut asr.replay_pcm);
                match replay_preconnected(asr, &replay).await {
                    Ok(()) => flush_and_wait(asr).await,
                    Err(e) => Err(e),
                }
            } else {
                attempt
            }
        }
        AsrCmd::Start(_) => unreachable!("start is handled by the ASR worker"),
    };
    result.unwrap_or_else(|error| {
        warn!("ASR stream failed: {error:#}");
        asr.reset();
        Some(String::new())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    #[tokio::test]
    async fn empty_turn_finishes_without_an_asr_audio_packet() {
        let mut packet_counts = Vec::new();
        for modern in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                assert!(matches!(ws.next().await, Some(Ok(Message::Binary(_)))));
                ws.send(Message::Binary(
                    frame(9, false, Some(1), br#"{"code":20000000}"#)
                        .unwrap()
                        .into(),
                ))
                .await
                .unwrap();
                let packets = usize::from(matches!(ws.next().await, Some(Ok(Message::Binary(_)))));
                if packets > 0 {
                    let _ = ws
                        .send(Message::Binary(
                            frame(9, true, Some(2), br#"{"code":20000000,"result":{}}"#)
                                .unwrap()
                                .into(),
                        ))
                        .await;
                }
                packets
            });

            let mut cfg = crate::config::Config::parse(include_str!("../../config.example.toml"))
                .unwrap()
                .asr;
            cfg.appid = "test-app".into();
            cfg.access_token = "test-token".into();
            cfg.speech_api_key = modern.then(|| "test-voice-key".into());
            cfg.ws_url = format!("ws://{address}");
            let (tx, rx) = mpsc::channel(8);
            let (out, mut results) = mpsc::channel(1);
            let cancel = CancellationToken::new();
            let worker = tokio::spawn(run_asr(cfg, 16_000, rx, out, cancel.clone()));
            tx.send(AsrCmd::Start(CancellationToken::new()))
                .await
                .unwrap();
            tx.send(AsrCmd::Flush).await.unwrap();
            assert!(tokio::time::timeout(Duration::from_secs(2), results.recv())
                .await
                .unwrap()
                .unwrap()
                .text
                .is_empty());
            let packets = server.await.unwrap();
            eprintln!("ASR modern={modern} empty audio packets={packets}");
            cancel.cancel();
            worker.await.unwrap();
            packet_counts.push(packets);
        }
        assert_eq!(packet_counts, [0, 0]);
    }

    #[tokio::test]
    async fn project_key_sends_pcm_and_waits_for_full_final_transcript() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                socket,
                |request: &Request, response: Response| {
                    assert_eq!(request.headers()["X-Api-Key"], "test-voice-key");
                    assert_eq!(
                        request.headers()["X-Api-Resource-Id"],
                        "volc.seedasr.sauc.duration"
                    );
                    assert_ne!(request.headers()["X-Api-Request-Id"], "");
                    assert!(!request.headers().contains_key("X-Api-Access-Key"));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let init = ws.next().await.unwrap().unwrap().into_data();
            assert_eq!(&init[..8], &[0x11, 0x11, 0x11, 0, 0, 0, 0, 1]);
            let request: Value = serde_json::from_slice(&gunzip(&init[12..]).unwrap()).unwrap();
            assert_eq!(request["request"]["model_name"], "bigmodel");
            assert_eq!(request["request"]["result_type"], "full");
            assert_eq!(request["audio"]["rate"], 16000);
            assert!(request.get("app").is_none());
            ws.send(Message::Binary(
                frame(9, false, Some(1), br#"{"code":20000000}"#)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();

            let audio1 = ws.next().await.unwrap().unwrap().into_data();
            assert_eq!(&audio1[..8], &[0x11, 0x21, 0x01, 0, 0, 0, 0, 2]);
            assert_eq!(gunzip(&audio1[12..]).unwrap().len(), 2 * 960 * 2);
            let partial =
                json!({"result":{"text":"现在","utterances":[{"text":"现在","definite":true}]}});
            ws.send(Message::Binary(
                frame(9, false, Some(2), &serde_json::to_vec(&partial).unwrap())
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();

            let audio2 = ws.next().await.unwrap().unwrap().into_data();
            assert_eq!(&audio2[..8], &[0x11, 0x21, 0x01, 0, 0, 0, 0, 3]);
            assert_eq!(gunzip(&audio2[12..]).unwrap().len(), 2 * 960 * 2);

            let terminal = ws.next().await.unwrap().unwrap().into_data();
            assert_eq!(&terminal[..4], &[0x11, 0x23, 0x01, 0]);
            assert_eq!(&terminal[4..8], &(-4i32).to_be_bytes());
            assert_eq!(gunzip(&terminal[12..]).unwrap().len(), 960 * 2);
            let result = json!({"code":20000000,"result":{"text":"现在几点了？"}});
            ws.send(Message::Binary(
                frame(9, true, Some(4), &serde_json::to_vec(&result).unwrap())
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
        });

        let mut cfg = crate::config::Config::parse(include_str!("../../config.example.toml"))
            .unwrap()
            .asr;
        cfg.speech_api_key = Some("test-voice-key".into());
        cfg.ws_url = format!("ws://{address}");
        let (tx, rx) = mpsc::channel(8);
        let (out, mut results) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(run_asr(cfg, 16000, rx, out, cancel.clone()));
        tx.send(AsrCmd::Start(CancellationToken::new()))
            .await
            .unwrap();
        for _ in 0..5 {
            tx.send(AsrCmd::Pcm(vec![500; 960])).await.unwrap();
        }
        tx.send(AsrCmd::Flush).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), results.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.text, "现在几点了？");
        cancel.cancel();
        worker.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn preconnected_idle_timeout_reconnects_and_transcribes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // 1st connection: accept init, then send Doubao idle timeout error 45000081.
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws1 = tokio_tungstenite::accept_async(socket).await.unwrap();
            assert!(matches!(ws1.next().await, Some(Ok(Message::Binary(_)))));
            ws1.send(Message::Binary(
                frame(9, false, Some(1), br#"{"code":20000000}"#)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
            let mut err_frame = vec![0x11, 0xf0, 0x10, 0x00];
            err_frame.extend_from_slice(&45_000_081u32.to_be_bytes());
            let body = br#"{"error":"Timed out waiting for the next audio packet."}"#;
            err_frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
            err_frame.extend_from_slice(body);
            ws1.send(Message::Binary(err_frame.into())).await.unwrap();
            let _ = ws1.close(None).await;

            // 2nd connection: receive replayed/fresh audio and return final text.
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws2 = tokio_tungstenite::accept_async(socket).await.unwrap();
            assert!(matches!(ws2.next().await, Some(Ok(Message::Binary(_)))));
            ws2.send(Message::Binary(
                frame(9, false, Some(1), br#"{"code":20000000}"#)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
            while let Some(Ok(Message::Binary(data))) = ws2.next().await {
                if data.len() >= 2 && (data[1] & FLAG_LAST) != 0 {
                    let result = json!({"code":20000000,"result":{"text":"现在几点了？"}});
                    ws2.send(Message::Binary(
                        frame(9, true, Some(2), &serde_json::to_vec(&result).unwrap())
                            .unwrap()
                            .into(),
                    ))
                    .await
                    .unwrap();
                    break;
                }
            }
        });

        let mut cfg = crate::config::Config::parse(include_str!("../../config.example.toml"))
            .unwrap()
            .asr;
        cfg.speech_api_key = Some("test-voice-key".into());
        cfg.ws_url = format!("ws://{address}");
        let (tx, rx) = mpsc::channel(8);
        let (out, mut results) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(run_asr(cfg, 16_000, rx, out, cancel.clone()));
        tx.send(AsrCmd::Start(CancellationToken::new()))
            .await
            .unwrap();
        // Give the idle watcher time to observe 45000081 before speech arrives.
        tokio::time::sleep(Duration::from_millis(50)).await;
        for _ in 0..3 {
            tx.send(AsrCmd::Pcm(vec![800; 960])).await.unwrap();
        }
        tx.send(AsrCmd::Flush).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), results.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.text, "现在几点了？");
        cancel.cancel();
        worker.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn preconnected_expiry_mid_initial_audio_replays_buffered_pcm() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // 1st connection: accept init, then reply with 45000081 once audio hits the wire.
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws1 = tokio_tungstenite::accept_async(socket).await.unwrap();
            assert!(matches!(ws1.next().await, Some(Ok(Message::Binary(_)))));
            ws1.send(Message::Binary(
                frame(9, false, Some(1), br#"{"code":20000000}"#)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
            // Wait for the first audio packet, simulate WAN RTT, then return 45000081.
            assert!(matches!(ws1.next().await, Some(Ok(Message::Binary(_)))));
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mut err_frame = vec![0x11, 0xf0, 0x10, 0x00];
            err_frame.extend_from_slice(&45_000_081u32.to_be_bytes());
            let body = br#"{"error":"Timed out waiting for the next audio packet."}"#;
            err_frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
            err_frame.extend_from_slice(body);
            ws1.send(Message::Binary(err_frame.into())).await.unwrap();
            let _ = ws1.close(None).await;

            // 2nd connection: verify all 3 frames (3 * 1920 = 5760 bytes) are replayed cleanly.
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws2 = tokio_tungstenite::accept_async(socket).await.unwrap();
            assert!(matches!(ws2.next().await, Some(Ok(Message::Binary(_)))));
            ws2.send(Message::Binary(
                frame(9, false, Some(1), br#"{"code":20000000}"#)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
            let mut total_pcm = 0usize;
            while let Some(Ok(Message::Binary(data))) = ws2.next().await {
                let pcm = gunzip(&data[12..]).unwrap();
                total_pcm += pcm.len();
                if data.len() >= 2 && (data[1] & FLAG_LAST) != 0 {
                    assert_eq!(total_pcm, 3 * 960 * 2);
                    let result = json!({"code":20000000,"result":{"text":"重放成功"}});
                    ws2.send(Message::Binary(
                        frame(9, true, Some(3), &serde_json::to_vec(&result).unwrap())
                            .unwrap()
                            .into(),
                    ))
                    .await
                    .unwrap();
                    break;
                }
            }
        });

        let mut cfg = crate::config::Config::parse(include_str!("../../config.example.toml"))
            .unwrap()
            .asr;
        cfg.speech_api_key = Some("test-voice-key".into());
        cfg.ws_url = format!("ws://{address}");
        let (tx, rx) = mpsc::channel(8);
        let (out, mut results) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(run_asr(cfg, 16_000, rx, out, cancel.clone()));
        tx.send(AsrCmd::Start(CancellationToken::new()))
            .await
            .unwrap();
        // Send 3 frames (3rd frame hits wire and triggers 45000081 from mock server).
        for _ in 0..3 {
            tx.send(AsrCmd::Pcm(vec![800; 960])).await.unwrap();
        }
        // Let the error arrive on ws1 before Flush, or let 4th frame / Flush observe it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(AsrCmd::Flush).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), results.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.text, "重放成功");
        cancel.cancel();
        worker.await.unwrap();
        server.await.unwrap();
    }
}

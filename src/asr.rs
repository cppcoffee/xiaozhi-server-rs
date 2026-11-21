use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use tokio_util::sync::CancellationToken;

use crate::config::{credential_missing, AsrCfg};

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

const MSG_FULL_CLIENT_REQUEST: u8 = 0x01;
const MSG_AUDIO_ONLY_REQUEST: u8 = 0x02;
const SERIAL_JSON: u8 = 0x01;
const COMPRESS_GZIP: u8 = 0x01;
const FLAG_SEQUENCE: u8 = 0x01;
const FLAG_LAST: u8 = 0x02;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const ASR_PACKET_MS: u32 = 120;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);

/// Flush 后等最终结果的步数（100ms/步）。豆包正常 <1s，给到 10s，
/// 避免出结果慢一点就把整轮静默丢掉
const FLUSH_WAIT_STEPS: usize = 100;

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

/// 解析服务端返回帧
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

/// 从返回帧里挑出 definite 的语句文本
fn extract_text(v: &Value) -> Option<String> {
    if let Some(code) = v.get("code").and_then(|c| c.as_i64()) {
        // 1013：无有效语音，属正常
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

/// ASR 长连接管理：懒连接、流式送音频、吐最终文本
type AsrSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct DoubaoAsr {
    cfg: AsrCfg,
    sample_rate: u32,
    ws: Option<AsrSocket>,
    next_sequence: Option<i32>,
    pending_pcm: Vec<u8>,
    has_pcm: bool,
    confirmed_text: Option<String>,
}

impl DoubaoAsr {
    pub fn new(cfg: AsrCfg, sample_rate: u32) -> Self {
        Self {
            cfg,
            sample_rate,
            ws: None,
            next_sequence: None,
            pending_pcm: Vec::new(),
            has_pcm: false,
            confirmed_text: None,
        }
    }

    async fn connect(&mut self) -> Result<()> {
        self.reset();
        self.ws = Some(
            tokio::time::timeout(CONNECT_TIMEOUT, self.open())
                .await
                .context("ASR connection or initialization timed out")??,
        );
        self.next_sequence = self.cfg.speech_api_key.as_ref().map(|_| 2);
        info!(
            "ASR connection established ({} / {})",
            self.cfg.ws_url, self.cfg.resource_id
        );
        Ok(())
    }

    async fn open(&self) -> Result<AsrSocket> {
        let mut req = self.cfg.ws_url.clone().into_client_request()?;
        let h = req.headers_mut();
        let request_id = Uuid::new_v4().to_string();
        let modern = self.cfg.speech_api_key.is_some();
        if let Some(key) = &self.cfg.speech_api_key {
            ensure!(
                !credential_missing(key),
                "Set [speech].api_key to your Doubao voice project API key"
            );
            let mut key = HeaderValue::from_str(key).context("Invalid [speech].api_key format")?;
            key.set_sensitive(true);
            h.insert("X-Api-Key", key);
            h.insert("X-Api-Request-Id", request_id.parse()?);
        } else {
            ensure!(
                !credential_missing(&self.cfg.appid) && !credential_missing(&self.cfg.access_token),
                "Set [asr].appid and [asr].access_token or configure [speech].api_key"
            );
            h.insert("X-Api-App-Key", self.cfg.appid.parse()?);
            let mut token = HeaderValue::from_str(&self.cfg.access_token)
                .context("Invalid [asr].access_token format")?;
            token.set_sensitive(true);
            h.insert("X-Api-Access-Key", token);
        }
        h.insert("X-Api-Resource-Id", self.cfg.resource_id.parse()?);
        h.insert("X-Api-Connect-Id", request_id.parse()?);
        let config = WebSocketConfig::default().max_message_size(Some(MAX_RESPONSE_BYTES));
        let (mut ws, response) =
            tokio_tungstenite::connect_async_with_config(req, Some(config), true).await?;
        let log_id = response
            .headers()
            .get("X-Tt-Logid")
            .and_then(|value| value.to_str().ok())
            .unwrap_or(&request_id);
        let payload = serde_json::to_vec(&request_json(&self.cfg, self.sample_rate))?;
        let f = frame(
            MSG_FULL_CLIENT_REQUEST,
            false,
            modern.then_some(1),
            &payload,
        )?;
        ws.send(Message::Binary(f.into())).await?;
        while let Some(msg) = ws.next().await {
            match msg? {
                Message::Binary(bytes) => {
                    let (value, last) = parse_frame(&bytes, !modern)
                        .with_context(|| format!("ASR initialization failed (LogID {log_id})"))?;
                    ensure!(
                        !last,
                        "ASR received a terminal frame during initialization (LogID {log_id})"
                    );
                    if let Some(code) = value["code"].as_i64() {
                        ensure!(
                            matches!(code, 0 | 1000 | 20_000_000),
                            "ASR initialization failed with code={code} (LogID {log_id})"
                        );
                    }
                    debug!("ASR initialization succeeded (LogID {log_id})");
                    return Ok(ws);
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => break,
            }
        }
        bail!("ASR initialization returned no valid response (LogID {log_id})")
    }

    async fn send_pcm(&mut self, pcm: &[i16]) -> Result<()> {
        if pcm.is_empty() {
            return Ok(());
        }
        if self.ws.is_none() {
            self.connect().await?;
        }
        let bytes = pcm.iter().flat_map(|s| s.to_le_bytes());
        if self.cfg.speech_api_key.is_some() {
            self.pending_pcm.extend(bytes);
            let packet_size = (self.sample_rate * ASR_PACKET_MS / 1000 * 2) as usize;
            // Retain the final audio for the terminal packet, including exact packet multiples.
            while self.pending_pcm.len() > packet_size {
                let packet: Vec<u8> = self.pending_pcm.drain(..packet_size).collect();
                self.send_audio(&packet, false).await?;
            }
        } else {
            self.send_audio(&bytes.collect::<Vec<_>>(), false).await?;
        }
        self.has_pcm = true;
        Ok(())
    }

    async fn send_audio(&mut self, data: &[u8], last: bool) -> Result<()> {
        if let Some(ws) = self.ws.as_mut() {
            let f = frame(MSG_AUDIO_ONLY_REQUEST, last, self.next_sequence, data)?;
            ws.send(Message::Binary(f.into())).await?;
            if let Some(sequence) = self.next_sequence.as_mut() {
                *sequence += 1;
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut self.pending_pcm);
        self.send_audio(&pending, true).await
    }

    fn reset(&mut self) {
        self.ws = None;
        self.pending_pcm.clear();
        self.has_pcm = false;
        self.confirmed_text = None;
        self.next_sequence = None;
    }

    async fn close(&mut self) {
        if let Some(mut ws) = self.ws.take() {
            let _ = ws.close(None).await;
        }
    }
}

/// 非阻塞地取出 ws 上已到达的所有帧，返回可能出现的最终文本
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
                    if let Err(e) = res {
                        debug!("ASR preconnect failed. Retrying on the first audio packet: {e:#}");
                        asr.reset();
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

async fn read_result(asr: &mut DoubaoAsr, cmd: AsrCmd) -> Option<String> {
    let result = match cmd {
        AsrCmd::Pcm(pcm) => {
            if let Err(e) = asr.send_pcm(&pcm).await {
                error!("Failed to send audio to ASR: {e:#}");
                asr.reset();
                return asr.cfg.speech_api_key.as_ref().map(|_| String::new());
            }
            drain_pending(asr, 0).await
        }
        AsrCmd::Flush if !asr.has_pcm => Ok(Some(String::new())),
        AsrCmd::Flush => {
            async {
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
            .await
        }
        AsrCmd::Start(_) => unreachable!("start is handled by the ASR worker"),
    };
    match result {
        Ok(text) => text,
        Err(error) => {
            warn!("ASR stream failed: {error:#}");
            asr.reset();
            Some(String::new())
        }
    }
}

/// `--check` 用：完成握手和模型初始化，验证鉴权及资源 ID。
pub async fn probe(cfg: &crate::config::AsrCfg, sample_rate: u32) -> Result<String> {
    let mut asr = DoubaoAsr::new(cfg.clone(), sample_rate);
    asr.connect().await?;
    let _ = tokio::time::timeout(Duration::from_secs(1), asr.close()).await;
    Ok(format!(
        "Model initialized (resource_id={})",
        cfg.resource_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
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

            let mut cfg = crate::config::Config::parse(include_str!("../config.example.toml"))
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

        let mut cfg = crate::config::Config::parse(include_str!("../config.example.toml"))
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
}

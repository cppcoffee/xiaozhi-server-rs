use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use base64::Engine;
use serde::Deserialize;
use tokio::sync::mpsc;

mod doubao;
pub use doubao::DoubaoTts;

const MAX_V3_BODY: usize = 8 * 1024 * 1024;
const SYNTH_TIMEOUT: Duration = Duration::from_secs(15);

fn shared_http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .pool_idle_timeout(Duration::from_secs(90))
                .tcp_nodelay(true)
                .build()
                .expect("build TTS HTTP client")
        })
        .clone()
}

#[derive(Deserialize)]
struct TtsChunk {
    code: i64,
    message: Option<String>,
    data: Option<String>,
}

fn decode_v3_chunk(line: &[u8]) -> Result<(Option<Vec<u8>>, bool)> {
    if line.iter().all(u8::is_ascii_whitespace) {
        return Ok((None, false));
    }
    let chunk: TtsChunk = serde_json::from_slice(line).context("Invalid TTS chunk JSON")?;
    let complete = chunk.code == 20_000_000;
    if chunk.code != 0 && !complete {
        let message: String = chunk
            .message
            .unwrap_or_default()
            .chars()
            .take(240)
            .collect();
        bail!("TTS returned error code={}: {message}", chunk.code);
    }
    let audio = match chunk.data {
        Some(data) if !data.is_empty() => {
            Some(base64::engine::general_purpose::STANDARD.decode(data)?)
        }
        _ => None,
    };
    Ok((audio, complete))
}

async fn stream_v3_audio(mut response: reqwest::Response, tx: mpsc::Sender<Vec<u8>>) -> Result<()> {
    let mut line = Vec::new();
    let mut received = 0usize;
    let mut total_pcm = 0usize;
    let mut complete = false;
    'response: while let Some(bytes) = response.chunk().await? {
        received += bytes.len();
        ensure!(
            received <= MAX_V3_BODY,
            "TTS response exceeds the size limit"
        );
        for part in bytes.split_inclusive(|byte| *byte == b'\n') {
            line.extend_from_slice(part);
            if part.ends_with(b"\n") {
                let (audio, done) = decode_v3_chunk(&line)?;
                line.clear();
                if let Some(pcm) = audio {
                    total_pcm += pcm.len();
                    if tx.send(pcm).await.is_err() {
                        return Ok(());
                    }
                }
                if done {
                    complete = true;
                    // Drain the trailing HTTP chunked EOF briefly so hyper pools the TLS connection
                    // instead of tearing it down between sentences.
                    let _ = tokio::time::timeout(Duration::from_millis(20), async {
                        while let Ok(Some(_)) = response.chunk().await {}
                    })
                    .await;
                    break 'response;
                }
            }
        }
    }
    if !complete {
        let (audio, done) = decode_v3_chunk(&line)?;
        if let Some(pcm) = audio {
            total_pcm += pcm.len();
            let _ = tx.send(pcm).await;
        }
        complete = done;
    }
    ensure!(complete, "TTS response is missing its terminal marker");
    ensure!(
        total_pcm > 0 && total_pcm % 2 == 0,
        "TTS returned empty or incomplete PCM16 audio"
    );
    Ok(())
}

/// Parse the TTS response: raw pcm is used as-is; wav skips the RIFF header
pub fn strip_wav_header(data: &[u8]) -> Vec<u8> {
    if data.len() > 44 && &data[0..4] == b"RIFF" && &data[8..12] == b"WAVE" {
        // find the data chunk
        let mut i = 12;
        while i + 8 <= data.len() {
            let id = &data[i..i + 4];
            let size =
                u32::from_le_bytes([data[i + 4], data[i + 5], data[i + 6], data[i + 7]]) as usize;
            if id == b"data" {
                let start = i + 8;
                let end = (start + size).min(data.len());
                return data[start..end].to_vec();
            }
            i += 8 + size;
        }
        data[44..].to_vec()
    } else {
        data.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use uuid::Uuid;

    #[tokio::test]
    async fn v3_requires_terminal_status_and_joins_fragmented_pcm() {
        use axum::{body::Body, http::HeaderMap, response::Response, routing::post, Json, Router};

        async fn synthesize(headers: HeaderMap, Json(body): Json<Value>) -> Response {
            assert_eq!(headers["X-Api-Key"], "test-voice-key");
            assert_eq!(headers["X-Api-Resource-Id"], "seed-tts-2.0");
            assert!(Uuid::parse_str(headers["X-Api-Request-Id"].to_str().unwrap()).is_ok());
            assert!(!headers.contains_key("Authorization"));
            assert_eq!(
                body["req_params"]["speaker"],
                "zh_female_xiaohe_uranus_bigtts"
            );
            assert_eq!(
                body["req_params"]["audio_params"],
                json!({"format":"pcm","sample_rate":24000})
            );
            let tail = match body["req_params"]["text"].as_str().unwrap() {
                "成功" => "{\"code\":0,\"data\":\"AwQ=\"}\n{\"code\":20000000,\"data\":null}",
                "服务器拒绝" => "{\"code\":45000010,\"message\":\"鉴权失败\"}\n",
                "缺少结束标记" => "",
                _ => unreachable!(),
            };
            let body = format!("\r\n{{\"code\":0,\"data\":\"AQI=\"}}\r\n{tail}");
            let fragments: Vec<_> = body
                .as_bytes()
                .chunks(3)
                .map(|part| Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(part)))
                .collect();
            Response::builder()
                .header("X-Tt-Logid", "test-log")
                .body(Body::from_stream(futures_util::stream::iter(fragments)))
                .unwrap()
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = crate::config::Config::parse(include_str!("../../config.example.toml"))
            .unwrap()
            .tts;
        cfg.speech_api_key = Some("test-voice-key".into());
        cfg.api_url = format!("http://{}/tts", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/tts", post(synthesize)))
                .await
                .unwrap();
        });
        let tts = DoubaoTts::new(cfg);
        assert_eq!(tts.synth("成功").await.unwrap(), [1, 2, 3, 4]);
        let error = tts.synth("服务器拒绝").await.unwrap_err();
        assert!(format!("{error:#}").contains("code=45000010"));
        assert!(format!("{error:#}").contains("test-log"));
        let error = tts.synth("缺少结束标记").await.unwrap_err();
        assert!(format!("{error:#}").contains("missing its terminal marker"));
        server.abort();
    }

    #[test]
    fn wav_header_strip() {
        let mut wav = b"RIFF\0\0\0\0WAVE".to_vec();
        wav.extend_from_slice(b"fmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&[0u8; 16]);
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&4u32.to_le_bytes());
        wav.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(strip_wav_header(&wav), vec![1u8, 2, 3, 4]);
        assert_eq!(strip_wav_header(&[1, 2, 3, 4]), vec![1u8, 2, 3, 4]);
    }
}

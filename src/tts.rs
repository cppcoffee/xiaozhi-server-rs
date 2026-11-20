use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::Engine;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::config::{credential_missing, TtsCfg};

const MAX_V3_BODY: usize = 8 * 1024 * 1024;
const SYNTH_TIMEOUT: Duration = Duration::from_secs(15);

/// 豆包 HTTP 语音合成。V3 分块响应拼为整句 PCM，供会话层发送。
pub struct DoubaoTts {
    cfg: TtsCfg,
    client: reqwest::Client,
}

impl DoubaoTts {
    pub fn new(cfg: TtsCfg) -> Self {
        Self {
            cfg,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("build TTS HTTP client"),
        }
    }

    /// 返回裸 PCM16（24k 单声道）字节；若服务端返回 wav 会自动剥头
    pub async fn synth(&self, text: &str) -> Result<Vec<u8>> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(key) = &self.cfg.speech_api_key {
            return self.synth_v3(text, key).await;
        }
        self.synth_v1(text).await
    }

    async fn synth_v3(&self, text: &str, key: &str) -> Result<Vec<u8>> {
        ensure!(
            !credential_missing(key),
            "请在 [speech] 填写豆包语音项目 API Key"
        );
        let mut key = HeaderValue::from_str(key).context("[speech].api_key 格式不合法")?;
        key.set_sensitive(true);
        let request_id = Uuid::new_v4().to_string();
        let response = self
            .client
            .post(&self.cfg.api_url)
            .header("X-Api-Key", key)
            .header("X-Api-Resource-Id", &self.cfg.resource_id)
            .header("X-Api-Request-Id", &request_id)
            .json(&json!({
                "req_params": {
                    "text": text,
                    "speaker": self.cfg.voice,
                    "audio_params": { "format": "pcm", "sample_rate": 24000 }
                }
            }))
            .timeout(SYNTH_TIMEOUT)
            .send()
            .await?;
        let log_id = response
            .headers()
            .get("X-Tt-Logid")
            .and_then(|value| value.to_str().ok())
            .unwrap_or(&request_id)
            .to_owned();
        let status = response.status();
        ensure!(
            status.is_success(),
            "TTS V3 HTTP {status}（LogID {log_id}）"
        );
        read_v3_audio(response)
            .await
            .with_context(|| format!("TTS V3（LogID {log_id}）"))
    }

    async fn synth_v1(&self, text: &str) -> Result<Vec<u8>> {
        ensure!(
            !credential_missing(&self.cfg.appid) && !credential_missing(&self.cfg.access_token),
            "请填写 [tts] 的旧版 APP ID 和 Access Token，或配置 [speech].api_key"
        );
        let body = json!({
            "app": {
                "appid": self.cfg.appid,
                "token": self.cfg.access_token,
                "cluster": self.cfg.cluster
            },
            "user": { "uid": "xiaozhi-rust" },
            "audio": { "voice_type": self.cfg.voice, "encoding": "pcm" },
            "request": {
                "reqid": Uuid::new_v4().to_string(),
                "text": text,
                "text_type": "plain",
                "operation": "query",
                "with_frontend": 1,
                "frontend_type": "unitTson"
            }
        });

        let mut auth = HeaderValue::from_str(&format!("Bearer;{}", self.cfg.access_token))
            .context("[tts].access_token 格式不合法")?;
        auth.set_sensitive(true);
        let resp = self
            .client
            .post(&self.cfg.api_url)
            .header("Authorization", auth)
            .json(&body)
            .timeout(SYNTH_TIMEOUT)
            .send()
            .await?;

        let status = resp.status();
        let v: Value = resp.json().await?;
        if let Some(data) = v.get("data").and_then(|d| d.as_str()) {
            let raw = base64::engine::general_purpose::STANDARD.decode(data)?;
            Ok(strip_wav_header(&raw))
        } else {
            Err(anyhow!("TTS 返回异常 {}: {}", status, v))
        }
    }
}

#[derive(Deserialize)]
struct TtsChunk {
    code: i64,
    message: Option<String>,
    data: Option<String>,
}

fn append_v3_chunk(line: &[u8], audio: &mut Vec<u8>) -> Result<bool> {
    if line.iter().all(u8::is_ascii_whitespace) {
        return Ok(false);
    }
    let chunk: TtsChunk = serde_json::from_slice(line).context("TTS 分块 JSON 无效")?;
    let complete = chunk.code == 20_000_000;
    if chunk.code != 0 && !complete {
        let message: String = chunk
            .message
            .unwrap_or_default()
            .chars()
            .take(240)
            .collect();
        bail!("TTS 返回错误 code={}: {message}", chunk.code);
    }
    if let Some(data) = chunk.data {
        audio.extend(base64::engine::general_purpose::STANDARD.decode(data)?);
    }
    Ok(complete)
}

async fn read_v3_audio(mut response: reqwest::Response) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    let mut audio = Vec::new();
    let mut received = 0;
    let mut complete = false;
    'response: while let Some(bytes) = response.chunk().await? {
        received += bytes.len();
        ensure!(received <= MAX_V3_BODY, "TTS 响应超出大小限制");
        for part in bytes.split_inclusive(|byte| *byte == b'\n') {
            line.extend_from_slice(part);
            if part.ends_with(b"\n") {
                if append_v3_chunk(&line, &mut audio)? {
                    complete = true;
                    break 'response;
                }
                line.clear();
            }
        }
    }
    if !complete {
        complete = append_v3_chunk(&line, &mut audio)?;
    }
    ensure!(complete, "TTS 响应缺少结束标记");
    ensure!(
        !audio.is_empty() && audio.len() % 2 == 0,
        "TTS 返回空或不完整的 PCM16"
    );
    Ok(audio)
}

/// 解析 TTS 返回：pcm 裸流直接用；wav 则跳过 RIFF 头
pub fn strip_wav_header(data: &[u8]) -> Vec<u8> {
    if data.len() > 44 && &data[0..4] == b"RIFF" && &data[8..12] == b"WAVE" {
        // 找 data chunk
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
        let mut cfg = crate::config::Config::parse(include_str!("../config.example.toml"))
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
        assert!(format!("{error:#}").contains("缺少结束标记"));
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

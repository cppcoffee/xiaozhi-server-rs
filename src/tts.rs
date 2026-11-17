use anyhow::{anyhow, Result};
use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::config::TtsCfg;

/// 豆包语音合成 HTTP 版（volcengine v1 /api/v1/tts），整句返回音频
pub struct DoubaoTts {
    cfg: TtsCfg,
    client: reqwest::Client,
}

impl DoubaoTts {
    pub fn new(cfg: TtsCfg) -> Self {
        Self {
            cfg,
            client: reqwest::Client::new(),
        }
    }

    /// 返回裸 PCM16（24k 单声道）字节；若服务端返回 wav 会自动剥头
    pub async fn synth(&self, text: &str) -> Result<Vec<u8>> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(Vec::new());
        }
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

        let resp = self
            .client
            .post(&self.cfg.api_url)
            .header("Authorization", "Bearer;")
            .json(&body)
            .timeout(std::time::Duration::from_secs(15))
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

/// 解析 TTS 返回：pcm 裸流直接用；wav 则跳过 RIFF 头
pub fn strip_wav_header(data: &[u8]) -> Vec<u8> {
    if data.len() > 44 && &data[0..4] == b"RIFF" && &data[8..12] == b"WAVE" {
        // 找 data chunk
        let mut i = 12;
        while i + 8 <= data.len() {
            let id = &data[i..i + 4];
            let size = u32::from_le_bytes([data[i + 4], data[i + 5], data[i + 6], data[i + 7]])
                as usize;
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

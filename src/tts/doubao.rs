use anyhow::{anyhow, ensure, Context, Result};
use base64::Engine;
use reqwest::header::HeaderValue;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::config::{credential_missing, TtsCfg};

use super::{shared_http_client, stream_v3_audio, strip_wav_header, SYNTH_TIMEOUT};

/// Doubao HTTP speech synthesis. V3 emits PCM chunk by chunk, lowering the first-sound latency.
#[derive(Clone)]
pub struct DoubaoTts {
    cfg: TtsCfg,
    client: reqwest::Client,
}

impl DoubaoTts {
    pub fn new(cfg: TtsCfg) -> Self {
        Self {
            cfg,
            client: shared_http_client(),
        }
    }

    /// V3 can output at the sample rate negotiated with the device (16k saves 33% download and encode); V1 defaults to 24k.
    pub fn preferred_sample_rate(&self, negotiated: u32) -> u32 {
        if self.cfg.speech_api_key.is_some() {
            match negotiated {
                8000 | 16000 | 24000 => negotiated,
                _ => 24_000,
            }
        } else {
            24_000
        }
    }

    /// Return one whole sentence as raw PCM16 (24k mono) bytes; reused by tests and simple callers.
    #[allow(dead_code)]
    pub async fn synth(&self, text: &str) -> Result<Vec<u8>> {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(32);
        let producer = self.synth_stream(text, 24_000, tx);
        let consumer = async {
            let mut audio = Vec::new();
            while let Some(chunk) = rx.recv().await {
                audio.extend_from_slice(&chunk);
            }
            Ok::<_, anyhow::Error>(audio)
        };
        let ((), audio) = tokio::try_join!(producer, consumer)?;
        Ok(audio)
    }

    /// Streaming synthesis: each PCM chunk is written to `tx` as it arrives, without waiting for the whole sentence.
    pub async fn synth_stream(
        &self,
        text: &str,
        sample_rate: u32,
        tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(());
        }
        if let Some(key) = &self.cfg.speech_api_key {
            return self.synth_v3_stream(text, key, sample_rate, tx).await;
        }
        let pcm = self.synth_v1(text).await?;
        if !pcm.is_empty() {
            let _ = tx.send(pcm).await;
        }
        Ok(())
    }

    async fn synth_v3_stream(
        &self,
        text: &str,
        key: &str,
        sample_rate: u32,
        tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        ensure!(
            !credential_missing(key),
            "Set [speech].api_key to your Doubao voice project API key"
        );
        let mut key = HeaderValue::from_str(key).context("Invalid [speech].api_key format")?;
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
                    "audio_params": { "format": "pcm", "sample_rate": sample_rate }
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
        ensure!(status.is_success(), "TTS V3 HTTP {status} (LogID {log_id})");
        stream_v3_audio(response, tx)
            .await
            .with_context(|| format!("TTS V3 (LogID {log_id})"))
    }

    async fn synth_v1(&self, text: &str) -> Result<Vec<u8>> {
        ensure!(
            !credential_missing(&self.cfg.appid) && !credential_missing(&self.cfg.access_token),
            "Set [tts].appid and [tts].access_token or configure [speech].api_key"
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
            .context("Invalid [tts].access_token format")?;
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
            Err(anyhow!("Unexpected TTS response (HTTP {}): {}", status, v))
        }
    }
}

use anyhow::{bail, ensure, Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::{credential_missing, AsrCfg};

use super::{
    frame, parse_frame, request_json, ASR_PACKET_MS, CONNECT_TIMEOUT, MAX_RESPONSE_BYTES,
    MSG_AUDIO_ONLY_REQUEST, MSG_FULL_CLIENT_REQUEST,
};

pub(super) type AsrSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// ASR long connection: lazy connect, stream audio, yield final text
pub struct DoubaoAsr {
    pub(super) cfg: AsrCfg,
    sample_rate: u32,
    pub(super) ws: Option<AsrSocket>,
    next_sequence: Option<i32>,
    pending_pcm: Vec<u8>,
    pub(super) has_pcm: bool,
    pub(super) confirmed_text: Option<String>,
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

    pub(super) async fn connect(&mut self) -> Result<()> {
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

    pub(super) async fn send_pcm(&mut self, pcm: &[i16]) -> Result<()> {
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

    pub(super) async fn flush(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut self.pending_pcm);
        self.send_audio(&pending, true).await
    }

    pub(super) fn reset(&mut self) {
        self.ws = None;
        self.pending_pcm.clear();
        self.has_pcm = false;
        self.confirmed_text = None;
        self.next_sequence = None;
    }
}

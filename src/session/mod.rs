mod audio;
mod connection;
mod input;
mod turn;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::config::Config;
use crate::mcp::DeviceMcp;

pub use connection::handle_connection;
#[allow(unused_imports)]
pub(crate) use turn::{
    run_turn, run_turn_with_rate, FAILURE_RESERVE, FIRST_AUDIO_BUDGET, TOTAL_TURN_BUDGET,
};

pub(crate) struct AudioFrame {
    pub data: Vec<u8>,
    pub cancel: CancellationToken,
    pub sent: oneshot::Sender<()>,
}

pub struct Shared {
    pub cfg: Arc<Config>,
    pub mcp: Arc<DeviceMcp>,
    pub closed: CancellationToken,
    pub ws_tx: mpsc::Sender<Message>,
    pub audio_tx: mpsc::Sender<AudioFrame>,
    pub session_id: String,
    pub device_id: String,
    pub history: Mutex<Vec<Value>>,
    pub turn_cancel: Mutex<Option<CancellationToken>>,
}

impl Shared {
    pub async fn send_json(&self, v: Value) {
        let text = v.to_string();
        debug!("-> {text}");
        if self.closed.is_cancelled() {
            return;
        }
        if self.ws_tx.try_send(Message::Text(text.into())).is_err() {
            warn!("WS control queue unavailable, closing connection");
            self.closed.cancel();
        }
    }

    pub async fn send_audio(&self, data: Vec<u8>, cancel: &CancellationToken) -> Result<()> {
        let (sent, receipt) = oneshot::channel();
        let frame = AudioFrame {
            data,
            cancel: cancel.clone(),
            sent,
        };
        tokio::select! {
            biased;
            _ = self.closed.cancelled() => bail!("device disconnected"),
            _ = cancel.cancelled() => bail!("turn cancelled"),
            result = async {
                self.audio_tx.send(frame).await.context("audio writer closed")?;
                receipt.await.context("audio frame was not written")
            } => result,
        }
    }

    pub async fn send_tts_state(&self, state: &str, text: Option<&str>) {
        let v = crate::protocol::tts_state(state, &self.session_id, text);
        self.send_json(v).await;
    }

    /// Retire input and reply together before admitting the next result.
    pub(super) async fn abort_turn(&self, input: Option<CancellationToken>) {
        let mut current = self.turn_cancel.lock().await;
        if let Some(input) = input {
            input.cancel();
        }
        if let Some(turn) = current.take() {
            turn.cancel();
            self.send_tts_state("stop", None).await;
        }
    }
}

/// websocket upgrade: one session per connection
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<crate::config::ConfigStore>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let device_id = headers
        .get("device-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    let cfg = state.current();
    ws.max_message_size(crate::mcp::MAX_MESSAGE)
        .on_upgrade(move |sock| async move {
            handle_connection(sock, cfg, device_id).await;
        })
}

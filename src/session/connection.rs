use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::asr::{run_asr, AsrCmd, AsrOutcome};
use crate::config::Config;
use crate::mcp::DeviceMcp;
use crate::tts::DoubaoTts;

use super::input::InputState;
use super::turn::run_turn_with_rate;
use super::{AudioFrame, Shared};

/// Close the connection after this long without any message (an idle device only sends on wake)
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Full lifecycle of one websocket connection
pub async fn handle_connection(sock: WebSocket, cfg: Arc<Config>, device_id: String) {
    let session_id = Uuid::new_v4().to_string();
    info!("Device connected: device_id={device_id} session_id={session_id}");

    let (sink, mut stream) = sock.split();
    let (ws_tx, ws_rx) = mpsc::channel::<Message>(512);
    let (audio_tx, audio_rx) = mpsc::channel::<AudioFrame>(8);

    let closed = CancellationToken::new();
    let _connection_guard = closed.clone().drop_guard();
    spawn_writer(sink, ws_rx, audio_rx, closed.clone());

    let (asr_tx, asr_rx) = mpsc::channel::<AsrCmd>(64);
    let (asr_out_tx, asr_out_rx) = mpsc::channel::<AsrOutcome>(8);
    let asr_cancel = CancellationToken::new();
    let _asr_guard = asr_cancel.clone().drop_guard();
    let manual_tx = asr_out_tx.clone();
    {
        let cfg_asr = cfg.asr.clone();
        let cancel = asr_cancel.clone();
        tokio::spawn(async move {
            run_asr(
                cfg_asr,
                crate::config::INPUT_SAMPLE_RATE,
                asr_rx,
                asr_out_tx,
                cancel,
            )
            .await;
        });
    }

    let mcp = DeviceMcp::new(session_id.clone(), ws_tx.clone(), closed.clone());
    let negotiated_sample_rate = Arc::new(AtomicU32::new(24_000));
    let shared = Arc::new(Shared {
        mcp,
        closed: closed.clone(),
        cfg: cfg.clone(),
        ws_tx,
        audio_tx,
        session_id,
        device_id: device_id.clone(),
        history: Mutex::new(Vec::new()),
        turn_cancel: Mutex::new(None),
    });

    let brain = spawn_brain(shared.clone(), asr_out_rx, negotiated_sample_rate.clone());

    let mut input_state = match InputState::new(&cfg) {
        Ok(state) => state,
        Err(e) => {
            error!("Failed to initialize the Opus decoder: {e}");
            return;
        }
    };

    loop {
        let msg = tokio::select! {
            _ = closed.cancelled() => break,
            msg = stream.next() => msg,
            _ = tokio::time::sleep(IDLE_TIMEOUT) => {
                if shared.turn_cancel.lock().await.is_some() {
                    continue; // a turn is running, so this is not idle
                }
                info!("Closing WebSocket after {} seconds of inactivity", IDLE_TIMEOUT.as_secs());
                // Give the writer a chance to send Close before cancelling it.
                if shared.ws_tx.try_send(Message::Close(None)).is_ok() {
                    let _ = tokio::time::timeout(Duration::from_secs(1), closed.cancelled()).await;
                }
                break;
            }
        };
        let msg = match msg {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                warn!("WebSocket read failed: {e}");
                break;
            }
            None => break,
        };
        match msg {
            Message::Text(t) => {
                input_state
                    .handle_text(&t, &shared, &negotiated_sample_rate, &asr_tx, &manual_tx)
                    .await;
            }
            Message::Binary(b) if input_state.listening => {
                input_state.handle_audio(&b, &shared, &asr_tx);
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    info!("Device disconnected: {device_id}");
    closed.cancel();
    asr_cancel.cancel();
    shared.abort_turn(input_state.input.take()).await;
    brain.abort();
    let _ = brain.await;
}

/// All outbound messages go through one queue, so tasks never race for the socket.
fn spawn_writer(
    mut sink: SplitSink<WebSocket, Message>,
    mut ws_rx: mpsc::Receiver<Message>,
    mut audio_rx: mpsc::Receiver<AudioFrame>,
    closed: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            // Controls retain priority. Speech awaits each write receipt, so stop
            // cannot overtake its audio and stale queued frames can be discarded.
            let (msg, receipt) = tokio::select! {
                biased;
                _ = closed.cancelled() => break,
                msg = ws_rx.recv() => match msg { Some(msg) => (msg, None), None => break },
                frame = audio_rx.recv() => match frame {
                    Some(frame) if !frame.cancel.is_cancelled() => (Message::Binary(frame.data.into()), Some(frame.sent)),
                    Some(_) => continue,
                    None => break,
                },
            };
            let closing = matches!(&msg, Message::Close(_));
            // Let an already-started small WS frame finish instead of tearing down
            // the entire WebSocket on barge-in/abort; cancelled queued frames are dropped above.
            let Ok(sent) = tokio::select!(
                biased;
                _ = closed.cancelled() => break,
                result = tokio::time::timeout(Duration::from_secs(5), sink.send(msg)) => result,
            ) else {
                warn!("WS write timeout");
                break;
            };
            if sent.is_err() {
                break;
            }
            if let Some(receipt) = receipt {
                let _ = receipt.send(());
            }
            if closing {
                break;
            }
        }
        closed.cancel();
    });
}

/// ASR and manual text share one serialized worker, never the WS reader.
fn spawn_brain(
    shared: Arc<Shared>,
    mut asr_out_rx: mpsc::Receiver<AsrOutcome>,
    negotiated_sr: Arc<AtomicU32>,
) -> JoinHandle<()> {
    let tts = DoubaoTts::new(shared.cfg.tts.clone());
    tokio::spawn(async move {
        loop {
            let outcome = tokio::select! {
                _ = shared.closed.cancelled() => break,
                result = asr_out_rx.recv() => match result { Some(result) => result, None => break },
            };
            let AsrOutcome { turn, text } = outcome;
            if text.trim().is_empty() || crate::text::is_exit(&text) {
                let _guard = shared.turn_cancel.lock().await;
                if turn.is_cancelled() {
                    continue;
                }
                turn.cancel();
                if text.trim().is_empty() {
                    shared.send_tts_state("start", None).await;
                    shared.send_tts_state("stop", None).await;
                    continue;
                }
                info!("[{}] User ended the session: {text}", shared.device_id);
                shared
                    .send_json(crate::protocol::stt_text(&text, &shared.session_id))
                    .await;
                shared
                    .send_json(json!({"type": "goodbye", "session_id": shared.session_id}))
                    .await;
                if shared.ws_tx.try_send(Message::Close(None)).is_err() {
                    shared.closed.cancel();
                }
                break;
            }
            let output_sr = tts.preferred_sample_rate(negotiated_sr.load(Ordering::Relaxed));
            if let Err(e) = run_turn_with_rate(&shared, &tts, text, turn, output_sr).await {
                error!("Failed to process the turn: {e}");
            }
        }
    })
}

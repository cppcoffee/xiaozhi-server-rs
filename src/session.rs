use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::asr::{run_asr, AsrCmd, AsrOutcome};
use crate::audio::{OpusIn, OpusOut};
use crate::config::Config;
use crate::llm::chat_stream_sentences;
use crate::mcp::DeviceMcp;
use crate::text::clean_for_tts;
use crate::tts::DoubaoTts;
use crate::vad::{EnergyVad, VadEvent};

/// How many recent conversation rounds to keep
const HISTORY_ROUNDS: usize = 6;

/// Close the connection after this long without any message (an idle device only sends on wake)
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Playback start pre-buffer frames: send the first 2 frames (120ms) back-to-back to build the ESP32 jitter buffer
const PRE_BUFFER_FRAMES: usize = 2;
const OUTPUT_FRAME_MS: u32 = 60;
const OUTPUT_FRAME_INTERVAL: Duration = Duration::from_millis(OUTPUT_FRAME_MS as u64);

/// Oversized old tool results are omitted from later rounds to avoid pointless prefill time
const COMPACT_TOOL_HISTORY_BYTES: usize = 512;

/// Server-side silence fallback: if the device misses its local endpoint and never sends listen stop,
/// this much consecutive silence after speech ends the turn and triggers recognition instead of waiting out the device's 15s timeout.
const SERVER_SILENCE_FALLBACK_MS: u32 = 1200;

// One absolute clock per accepted text turn; progress never renews these limits.
pub(crate) const FIRST_AUDIO_BUDGET: Duration = Duration::from_secs(75);
pub(crate) const FAILURE_RESERVE: Duration = Duration::from_secs(10);
pub(crate) const TOTAL_TURN_BUDGET: Duration = Duration::from_secs(180);
const TIMEOUT_REPLY: &str = "回复超时，请稍后再试。";

struct TurnBudget {
    started: Instant,
    first_deadline: Instant,
    total_deadline: Instant,
    first_audio: AtomicBool,
}
impl TurnBudget {
    fn new() -> Self {
        let started = Instant::now();
        Self {
            started,
            first_deadline: started + FIRST_AUDIO_BUDGET,
            total_deadline: started + TOTAL_TURN_BUDGET,
            first_audio: AtomicBool::new(false),
        }
    }

    async fn normal_expired(&self) {
        tokio::time::sleep_until(self.first_deadline - FAILURE_RESERVE).await;
        if self.first_audio.load(Ordering::SeqCst) {
            tokio::time::sleep_until(self.total_deadline).await;
        }
    }

    fn recovery_deadline(&self) -> Instant {
        if self.first_audio.load(Ordering::SeqCst) {
            self.total_deadline
        } else {
            self.first_deadline
        }
    }
}

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
    async fn abort_turn(&self, input: Option<CancellationToken>) {
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

/// Full lifecycle of one websocket connection
pub async fn handle_connection(sock: WebSocket, cfg: Arc<Config>, device_id: String) {
    let session_id = Uuid::new_v4().to_string();
    info!("Device connected: device_id={device_id} session_id={session_id}");

    let (sink, mut stream) = sock.split();
    let (ws_tx, ws_rx) = mpsc::channel::<Message>(512);
    let (audio_tx, mut audio_rx) = mpsc::channel::<AudioFrame>(8);

    let closed = CancellationToken::new();
    let _connection_guard = closed.clone().drop_guard();
    let writer_closed = closed.clone();

    // Writer task: all outbound messages go through one queue, so tasks never race for the socket
    tokio::spawn(async move {
        let mut sink = sink;
        let mut ws_rx = ws_rx;
        loop {
            // Controls retain priority. Speech awaits each write receipt, so stop
            // cannot overtake its audio and stale queued frames can be discarded.
            let (msg, receipt) = tokio::select! {
                biased;
                _ = writer_closed.cancelled() => break,
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
            let sent = tokio::select! {
                biased;
                _ = writer_closed.cancelled() => break,
                result = tokio::time::timeout(Duration::from_secs(5), sink.send(msg)) => {
                    match result {
                        Ok(result) => result,
                        Err(_) => { warn!("WS write timeout"); break; }
                    }
                },
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
        writer_closed.cancel();
    });

    // ASR channel
    let (asr_tx, asr_rx) = mpsc::channel::<AsrCmd>(64);
    let (asr_out_tx, mut asr_out_rx) = mpsc::channel::<AsrOutcome>(8);
    let asr_cancel = CancellationToken::new();
    let _asr_guard = asr_cancel.clone().drop_guard();
    let manual_tx = asr_out_tx.clone();
    {
        let cfg_asr = cfg.asr.clone();
        let cancel = asr_cancel.clone();
        let sr = crate::config::INPUT_SAMPLE_RATE;
        tokio::spawn(async move {
            run_asr(cfg_asr, sr, asr_rx, asr_out_tx, cancel).await;
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
        session_id: session_id.clone(),
        device_id: device_id.clone(),
        history: Mutex::new(Vec::new()),
        turn_cancel: Mutex::new(None),
    });

    // ASR and manual text share one serialized worker, never the WS reader.
    let brain = {
        let shared = shared.clone();
        let tts = DoubaoTts::new(cfg.tts.clone());
        let negotiated_sr = negotiated_sample_rate.clone();
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
    };

    // Reader task
    let mut decoder = match OpusIn::new(crate::config::INPUT_SAMPLE_RATE) {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to initialize the Opus decoder: {e}");
            return;
        }
    };
    let mut vad = EnergyVad::new(&cfg.vad, crate::config::INPUT_FRAME_MS);
    const PRE_ROLL_FRAMES: usize = 3;
    let mut pre_roll = VecDeque::with_capacity(PRE_ROLL_FRAMES);
    let mut manual_mode = false;
    let mut listening = false;
    let mut speech_started = false;
    let mut post_speech_silent_frames: u32 = 0;
    let mut input = None;
    let mut received_frames = 0;
    let mut peak_rms = 0_f64;

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
                let v: Value = match serde_json::from_str(&t) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                debug!("<- {t}");
                match v["type"].as_str().unwrap_or("") {
                    "hello" => {
                        if let Some(sr) = v["audio_params"]["sample_rate"].as_u64() {
                            negotiated_sample_rate.store(sr as u32, Ordering::Relaxed);
                        }
                        let welcome = cfg.welcome(&session_id, &v);
                        shared.send_json(welcome).await;
                        if v["features"]["mcp"] == true {
                            shared.mcp.start();
                        }
                    }
                    "listen" => {
                        let state = v["state"].as_str().unwrap_or("");
                        if let Some(mode) = v["mode"].as_str() {
                            manual_mode = mode == "manual";
                        }
                        match state {
                            "start" => {
                                shared.abort_turn(input.take()).await;
                                let turn = CancellationToken::new();
                                if asr_tx.try_send(AsrCmd::Start(turn.clone())).is_err() {
                                    closed.cancel();
                                }
                                input = Some(turn);
                                vad = EnergyVad::new(&cfg.vad, crate::config::INPUT_FRAME_MS);
                                pre_roll.clear();
                                received_frames = 0;
                                peak_rms = 0.0;
                                speech_started = false;
                                post_speech_silent_frames = 0;
                                listening = true;
                            }
                            "stop" if listening => {
                                debug!(
                                    target: "xiaozhi_input",
                                    device_id,
                                    received_frames,
                                    peak_rms,
                                    threshold = cfg.vad.rms_threshold,
                                    "listen input"
                                );
                                listening = false;
                                speech_started = false;
                                post_speech_silent_frames = 0;
                                if asr_tx.try_send(AsrCmd::Flush).is_err() {
                                    closed.cancel();
                                }
                            }
                            "detect" if manual_mode => {
                                if let Some(text) =
                                    v["text"].as_str().filter(|s| !s.trim().is_empty())
                                {
                                    shared.abort_turn(input.take()).await;
                                    listening = false;
                                    speech_started = false;
                                    post_speech_silent_frames = 0;
                                    let turn = CancellationToken::new();
                                    input = Some(turn.clone());
                                    if manual_tx
                                        .try_send(AsrOutcome {
                                            turn,
                                            text: text.to_owned(),
                                        })
                                        .is_err()
                                    {
                                        warn!("Manual input queue is full. Closing the connection.");
                                        closed.cancel();
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    "abort" => {
                        info!("Received abort. Cancelling the current turn.");
                        shared.abort_turn(input.take()).await;
                        listening = false;
                        speech_started = false;
                        post_speech_silent_frames = 0;
                    }
                    "goodbye" => {
                        debug!("Device sent goodbye");
                        listening = false;
                        speech_started = false;
                        post_speech_silent_frames = 0;
                    }
                    "ping" => {
                        shared.send_json(crate::protocol::pong()).await;
                    }
                    "mcp" => shared.mcp.receive(&v),
                    "iot" => {
                        debug!("Ignoring unsupported message type: {}", v["type"]);
                    }
                    other => debug!("Unknown message type: {other}"),
                }
            }
            Message::Binary(b) if listening => {
                let pcm = match decoder.decode(&b) {
                    Ok(p) => p.to_vec(),
                    Err(e) => {
                        debug!("Failed to decode Opus packet: {e}");
                        continue;
                    }
                };
                received_frames += 1;
                let (event, level) = vad.push_with_rms(&pcm);
                peak_rms = peak_rms.max(level);
                let fallback_frames = (SERVER_SILENCE_FALLBACK_MS / crate::config::INPUT_FRAME_MS)
                    .max(cfg.vad.end_frames.saturating_mul(2));
                match event {
                    VadEvent::StartSpeech => {
                        post_speech_silent_frames = 0;
                        if !speech_started {
                            info!("Speech detected");
                            speech_started = true;
                            if pre_roll
                                .drain(..)
                                .chain(std::iter::once(pcm))
                                .any(|frame| asr_tx.try_send(AsrCmd::Pcm(frame)).is_err())
                            {
                                closed.cancel();
                            }
                        } else if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() {
                            closed.cancel();
                        }
                    }
                    VadEvent::None if speech_started || vad.is_speaking() => {
                        if level >= cfg.vad.rms_threshold {
                            post_speech_silent_frames = 0;
                        } else {
                            post_speech_silent_frames += 1;
                        }
                        if !manual_mode && post_speech_silent_frames >= fallback_frames {
                            info!(
                                "Ending input after {} ms of post-speech silence",
                                post_speech_silent_frames * crate::config::INPUT_FRAME_MS
                            );
                            listening = false;
                            speech_started = false;
                            post_speech_silent_frames = 0;
                            if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err()
                                || asr_tx.try_send(AsrCmd::Flush).is_err()
                            {
                                closed.cancel();
                            }
                            continue;
                        }
                        if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() {
                            closed.cancel();
                        }
                    }
                    VadEvent::None => {
                        if pre_roll.len() == PRE_ROLL_FRAMES {
                            pre_roll.pop_front();
                        }
                        pre_roll.push_back(pcm);
                    }
                    VadEvent::EndSpeech(capped) => {
                        // On a short pause prefer the device's local listen stop while still forwarding the tail to ASR;
                        // past the server silence fallback (1.2s default) or the 20s recording cap, the server ends the turn itself.
                        if !capped {
                            post_speech_silent_frames += 1;
                            if !manual_mode && post_speech_silent_frames >= fallback_frames {
                                info!(
                                    "Ending input after {} ms of post-speech silence",
                                    post_speech_silent_frames * crate::config::INPUT_FRAME_MS
                                );
                                listening = false;
                                speech_started = false;
                                post_speech_silent_frames = 0;
                                if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err()
                                    || asr_tx.try_send(AsrCmd::Flush).is_err()
                                {
                                    closed.cancel();
                                }
                                continue;
                            }
                            debug!("Pause detected. Forwarding audio while waiting for the device to stop listening.");
                            if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() {
                                closed.cancel();
                            }
                            continue;
                        }
                        info!("Ending input at the recording duration limit");
                        listening = false;
                        speech_started = false;
                        post_speech_silent_frames = 0;
                        if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err()
                            || asr_tx.try_send(AsrCmd::Flush).is_err()
                        {
                            closed.cancel();
                        }
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    info!("Device disconnected: {device_id}");
    closed.cancel();
    asr_cancel.cancel();
    shared.abort_turn(input.take()).await;
    brain.abort();
    let _ = brain.await;
}

enum PacerItem {
    SentenceStart(String),
    Audio(Vec<u8>),
}

#[allow(dead_code)]
pub(crate) async fn run_turn(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: CancellationToken,
) -> Result<()> {
    let output_sr = tts.preferred_sample_rate(24_000);
    run_turn_with_rate(shared, tts, user_text, token, output_sr).await
}

pub(crate) async fn run_turn_with_rate(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: CancellationToken,
    output_sample_rate: u32,
) -> Result<()> {
    let user_text = user_text.trim().to_string();
    if user_text.is_empty() {
        return Ok(());
    }
    {
        let mut cur = shared.turn_cancel.lock().await;
        if token.is_cancelled() || shared.closed.is_cancelled() {
            return Ok(());
        }
        if let Some(old) = cur.take() {
            old.cancel();
        }
        *cur = Some(token.clone());
    }
    let budget = TurnBudget::new();
    let normal_cancel = token.child_token();
    let mut timed_out = false;
    let result = tokio::select! {
        biased;
        _ = token.cancelled() => Ok(None),
        _ = shared.closed.cancelled() => Ok(None),
        _ = budget.normal_expired() => {
            timed_out = true;
            Err(anyhow::anyhow!("turn deadline exceeded"))
        },
        result = run_turn_inner(shared, tts, user_text, &normal_cancel, &budget, output_sample_rate) => result.map(Some),
    };
    // Dropping the inner future aborts HTTP and rmcp's request guard cancels RPCs.
    // Keep recovery on the parent token so abort/disconnect still interrupts it.
    normal_cancel.cancel();
    if !token.is_cancelled() && !shared.closed.is_cancelled() {
        if let Err(e) = &result {
            error!("Conversation failed: {e:#}");
            let text = if timed_out {
                TIMEOUT_REPLY
            } else {
                "暂时无法获取可靠回复，请稍后再试。"
            };
            shared.send_tts_state("sentence_start", Some(text)).await;
            let deadline = budget.recovery_deadline();
            if Instant::now() < deadline {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {},
                    _ = shared.closed.cancelled() => {},
                    _ = tokio::time::timeout_at(
                        deadline,
                        speak_single_sentence(shared, tts, text, &token, &budget, output_sample_rate),
                    ) => {},
                }
            }
        }
    }
    let mut current = shared.turn_cancel.lock().await;
    if !token.is_cancelled() && !shared.closed.is_cancelled() {
        if let Ok(Some(transcript)) = &result {
            let mut hist = shared.history.lock().await;
            hist.extend(transcript.iter().cloned());
            trim_history(&mut hist);
        }
        // A device may send the next listen start as soon as it sees this stop.
        *current = None;
        shared.send_tts_state("stop", None).await;
    }
    token.cancel();
    result.map(|_| ())
}

fn trim_history(hist: &mut Vec<Value>) {
    let starts: Vec<usize> = hist
        .iter()
        .enumerate()
        .filter(|(_, m)| m["role"] == "user")
        .map(|(i, _)| i)
        .collect();
    if starts.len() > HISTORY_ROUNDS {
        hist.drain(..starts[starts.len() - HISTORY_ROUNDS]);
    }
}

fn compact_history_message(msg: &Value) -> Value {
    if msg["role"].as_str() == Some("tool") {
        if let Some(content) = msg["content"].as_str() {
            if content.len() > COMPACT_TOOL_HISTORY_BYTES {
                let mut compact = msg.clone();
                compact["content"] = json!(
                    "{\"note\":\"prior turn tool output omitted; call tool again for current readings\"}"
                );
                return compact;
            }
        }
    }
    msg.clone()
}

async fn run_turn_inner(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: &CancellationToken,
    budget: &TurnBudget,
    output_sample_rate: u32,
) -> Result<Vec<Value>> {
    info!("[{}] User: {user_text}", shared.device_id);
    shared
        .send_json(crate::protocol::stt_text(&user_text, &shared.session_id))
        .await;
    shared.send_tts_state("start", None).await;

    let (sentence_tx, mut sentence_rx) = mpsc::channel::<String>(16);
    // 64 frames ≈ 3.84s prefetch buffer: sentence N+1 is synthesized while sentence N plays, removing gaps
    let (pacer_tx, pacer_rx) = mpsc::channel::<PacerItem>(64);

    let question = user_text.as_str();
    let reply_fut = async move {
        if let Some(answer) = shared.mcp.direct_answer(question, token).await {
            let mut buffer = answer.clone();
            for sentence in crate::text::split_sentences(&mut buffer, true) {
                sentence_tx
                    .send(sentence)
                    .await
                    .context("TTS queue closed")?;
            }
            return Ok(vec![json!({"role":"assistant", "content":answer})]);
        }

        // Large old tool results in history keep only a hint, reducing model prefill latency.
        let mut messages = Vec::new();
        if !shared.cfg.llm.prompt.trim().is_empty() {
            messages.push(json!({"role": "system", "content": shared.cfg.llm.prompt}));
        }
        {
            let hist = shared.history.lock().await;
            messages.extend(hist.iter().map(compact_history_message));
        }
        messages.push(json!({"role": "user", "content": question}));
        let model_started = Instant::now();
        let (_, transcript) = chat_stream_sentences(
            &shared.cfg.llm,
            messages,
            shared.mcp.clone(),
            token.clone(),
            Some(sentence_tx),
        )
        .await?;
        debug!(
            elapsed_ms = model_started.elapsed().as_millis(),
            "LLM and tools complete"
        );
        Ok::<_, anyhow::Error>(transcript)
    };

    let tts_fut = async move {
        let mut encoder = OpusOut::new(output_sample_rate, OUTPUT_FRAME_MS)?;
        while let Some(raw) = sentence_rx.recv().await {
            if token.is_cancelled() {
                bail!("turn cancelled");
            }
            let text = clean_for_tts(&raw);
            if text.is_empty() {
                continue;
            }
            synthesize_sentence_to_pacer(tts, &mut encoder, &text, output_sample_rate, &pacer_tx, true)
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    };

    let pacer_fut = run_audio_pacer(shared, pacer_rx, token, budget);

    let (transcript, (), ()) = tokio::try_join!(reply_fut, tts_fut, pacer_fut)?;
    let mut history = vec![json!({"role":"user", "content":user_text})];
    history.extend(transcript);
    Ok(history)
}

async fn synthesize_sentence_to_pacer(
    tts: &DoubaoTts,
    encoder: &mut OpusOut,
    text: &str,
    sample_rate: u32,
    pacer_tx: &mpsc::Sender<PacerItem>,
    emit_sentence_start: bool,
) -> Result<()> {
    let synth_started = Instant::now();
    let (pcm_tx, mut pcm_rx) = mpsc::channel::<Vec<u8>>(16);
    let synth = tts.synth_stream(text, sample_rate, pcm_tx);
    let encode = async {
        let mut emitted_start = !emit_sentence_start;
        let mut produced_frames = 0usize;
        while let Some(pcm_chunk) = pcm_rx.recv().await {
            for frame in encoder.push_pcm_bytes(&pcm_chunk) {
                if !emitted_start {
                    emitted_start = true;
                    pacer_tx
                        .send(PacerItem::SentenceStart(text.to_owned()))
                        .await
                        .map_err(|_| anyhow!("audio pacer closed"))?;
                }
                produced_frames += 1;
                pacer_tx
                    .send(PacerItem::Audio(frame))
                    .await
                    .map_err(|_| anyhow!("audio pacer closed"))?;
            }
        }
        if let Some(tail) = encoder.finish_stream() {
            if !emitted_start {
                pacer_tx
                    .send(PacerItem::SentenceStart(text.to_owned()))
                    .await
                    .map_err(|_| anyhow!("audio pacer closed"))?;
            }
            produced_frames += 1;
            pacer_tx
                .send(PacerItem::Audio(tail))
                .await
                .map_err(|_| anyhow!("audio pacer closed"))?;
        }
        Ok::<_, anyhow::Error>(produced_frames)
    };
    let ((), produced_frames) = tokio::try_join!(synth, encode)?;
    debug!(
        elapsed_ms = synth_started.elapsed().as_millis(),
        frames = produced_frames,
        "TTS sentence synthesized"
    );
    ensure!(produced_frames > 0, "TTS produced no Opus frames");
    Ok(())
}

/// Pacer on an independent monotonic clock: the first 2 frames go out back-to-back, then a 60ms steady rate, and after an idle queue it re-anchors to avoid a burst.
async fn run_audio_pacer(
    shared: &Arc<Shared>,
    mut pacer_rx: mpsc::Receiver<PacerItem>,
    token: &CancellationToken,
    budget: &TurnBudget,
) -> Result<()> {
    let mut sent_frames: usize = 0;
    let mut next_deadline: Option<Instant> = None;
    while let Some(item) = pacer_rx.recv().await {
        if token.is_cancelled() {
            bail!("turn cancelled");
        }
        match item {
            PacerItem::SentenceStart(text) => {
                shared.send_tts_state("sentence_start", Some(&text)).await;
            }
            PacerItem::Audio(frame) => {
                if sent_frames >= PRE_BUFFER_FRAMES {
                    let now = Instant::now();
                    let deadline = match next_deadline {
                        Some(d) if now <= d + OUTPUT_FRAME_INTERVAL => d,
                        _ => now,
                    };
                    if deadline > now {
                        tokio::time::sleep_until(deadline).await;
                    }
                    next_deadline = Some(deadline + OUTPUT_FRAME_INTERVAL);
                }
                shared.send_audio(frame, token).await?;
                sent_frames += 1;
                if sent_frames == PRE_BUFFER_FRAMES {
                    next_deadline = Some(Instant::now() + OUTPUT_FRAME_INTERVAL);
                }
                if !budget.first_audio.swap(true, Ordering::SeqCst) {
                    debug!(
                        elapsed_ms = budget.started.elapsed().as_millis(),
                        "text turn to first audio"
                    );
                }
            }
        }
    }
    if sent_frames > 0 {
        if token.is_cancelled() {
            bail!("turn cancelled");
        }
        // One frame tail margin before tts:stop so the device drains buffered frames cleanly.
        tokio::time::sleep(OUTPUT_FRAME_INTERVAL).await;
    }
    Ok(())
}

/// Single-sentence fallback playback for failures/timeouts (reuses the same streaming encode and pacer path)
async fn speak_single_sentence(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    text: &str,
    token: &CancellationToken,
    budget: &TurnBudget,
    output_sample_rate: u32,
) -> Result<()> {
    let (pacer_tx, pacer_rx) = mpsc::channel::<PacerItem>(32);
    let synth_fut = async move {
        let mut encoder = OpusOut::new(output_sample_rate, OUTPUT_FRAME_MS)?;
        synthesize_sentence_to_pacer(tts, &mut encoder, text, output_sample_rate, &pacer_tx, false)
            .await
    };
    let pacer_fut = run_audio_pacer(shared, pacer_rx, token, budget);
    let ((), ()) = tokio::try_join!(synth_fut, pacer_fut)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_trims_whole_turns_not_individual_tool_messages() {
        let mut history = Vec::new();
        for i in 0..8 {
            history.extend([
                json!({"role":"user", "content":i.to_string()}),
                json!({"role":"assistant", "content":null, "tool_calls":[{"id":i.to_string()}]}),
                json!({"role":"tool", "tool_call_id":i.to_string(), "content":"{}"}),
                json!({"role":"assistant", "content":"answer"}),
            ]);
        }
        trim_history(&mut history);
        assert_eq!(history.len(), HISTORY_ROUNDS * 4);
        assert_eq!(history[0]["role"], "user");
        assert_eq!(history[0]["content"], "2");
        assert_eq!(
            history[1]["tool_calls"][0]["id"],
            history[2]["tool_call_id"]
        );
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn first_audio_disarms_only_the_initial_limit_not_the_total_limit() {
        let budget = Arc::new(TurnBudget::new());
        budget.first_audio.store(true, Ordering::SeqCst);
        let timer = {
            let budget = budget.clone();
            tokio::spawn(async move { budget.normal_expired().await })
        };
        tokio::time::advance(FIRST_AUDIO_BUDGET).await;
        tokio::task::yield_now().await;
        assert!(!timer.is_finished());
        tokio::time::advance(TOTAL_TURN_BUDGET - FIRST_AUDIO_BUDGET).await;
        timer.await.unwrap();
        assert_eq!(Instant::now(), budget.total_deadline);
    }
}

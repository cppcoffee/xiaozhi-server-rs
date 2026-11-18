use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
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
use crate::audio::{bytes_to_i16, OpusIn, OpusOut};
use crate::config::Config;
use crate::llm::chat_stream;
use crate::mcp::DeviceMcp;
use crate::text::{clean_for_tts, split_sentences};
use crate::tts::DoubaoTts;
use crate::vad::{EnergyVad, VadEvent};

/// 保留最近多少轮对话上下文
const HISTORY_ROUNDS: usize = 6;

/// 多久没有任何消息就断开这条连接（设备待机时只有唤醒才发消息）
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

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

/// websocket 升级：每个连接一个会话
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

/// 一条 websocket 连接的完整生命周期
pub async fn handle_connection(sock: WebSocket, cfg: Arc<Config>, device_id: String) {
    let session_id = Uuid::new_v4().to_string();
    info!("设备连接: device_id={device_id} session_id={session_id}");

    let (sink, mut stream) = sock.split();
    let (ws_tx, ws_rx) = mpsc::channel::<Message>(512);
    let (audio_tx, mut audio_rx) = mpsc::channel::<AudioFrame>(2);

    let closed = CancellationToken::new();
    let _connection_guard = closed.clone().drop_guard();
    let writer_closed = closed.clone();

    // 写任务：所有出站消息统一排队，避免多任务竞争 socket
    tokio::spawn(async move {
        let mut sink = sink;
        let mut ws_rx = ws_rx;
        loop {
            // Controls retain priority. Speech awaits each write receipt, so stop
            // cannot overtake its audio and stale queued frames can be discarded.
            let (msg, receipt, audio_cancel) = tokio::select! {
                biased;
                _ = writer_closed.cancelled() => break,
                msg = ws_rx.recv() => match msg { Some(msg) => (msg, None, None), None => break },
                frame = audio_rx.recv() => match frame {
                    Some(frame) if !frame.cancel.is_cancelled() => (Message::Binary(frame.data.into()), Some(frame.sent), Some(frame.cancel)),
                    Some(_) => continue,
                    None => break,
                },
            };
            let sent = tokio::select! {
                biased;
                _ = writer_closed.cancelled() => break,
                _ = async {
                    if let Some(cancel) = &audio_cancel { cancel.cancelled().await; }
                    else { std::future::pending::<()>().await; }
                } => {
                    // SinkExt::send may have partially written a frame. Do not
                    // reuse that socket or finish sending cancelled audio later.
                    warn!("in-flight audio write cancelled, closing WS");
                    break;
                },
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
        }
        writer_closed.cancel();
    });

    // ASR 通道
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
                    info!("[{}] 用户结束会话: {text}", shared.device_id);
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
                if let Err(e) = run_turn(&shared, &tts, text, turn).await {
                    error!("回合处理失败: {e}");
                }
            }
        })
    };

    // 读任务
    let mut decoder = match OpusIn::new(crate::config::INPUT_SAMPLE_RATE) {
        Ok(d) => d,
        Err(e) => {
            error!("Opus 解码器初始化失败: {e}");
            return;
        }
    };
    let mut vad = EnergyVad::new(&cfg.vad, crate::config::INPUT_FRAME_MS);
    const PRE_ROLL_FRAMES: usize = 3;
    let mut pre_roll = VecDeque::with_capacity(PRE_ROLL_FRAMES);
    let mut manual_mode = false;
    let mut listening = false;
    let mut input = None;

    loop {
        let msg = tokio::select! {
            _ = closed.cancelled() => break,
            msg = stream.next() => msg,
            _ = tokio::time::sleep(IDLE_TIMEOUT) => {
                if shared.turn_cancel.lock().await.is_some() {
                    continue; // 正在回话，不算空闲
                }
                info!("空闲 {}s，关闭连接回收资源", IDLE_TIMEOUT.as_secs());
                break;
            }
        };
        let msg = match msg {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                warn!("websocket 读错误: {e}");
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
                                listening = true;
                            }
                            "stop" if listening => {
                                listening = false;
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
                                    let turn = CancellationToken::new();
                                    input = Some(turn.clone());
                                    if manual_tx
                                        .try_send(AsrOutcome {
                                            turn,
                                            text: text.to_owned(),
                                        })
                                        .is_err()
                                    {
                                        warn!("手动文本队列已满，关闭连接");
                                        closed.cancel();
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    "abort" => {
                        info!("收到 abort，打断当前回合");
                        shared.abort_turn(input.take()).await;
                        listening = false;
                    }
                    "ping" => {
                        shared.send_json(crate::protocol::pong()).await;
                    }
                    "mcp" => shared.mcp.receive(&v),
                    "iot" => {
                        debug!("忽略不支持的消息类型: {}", v["type"]);
                    }
                    other => debug!("未知消息类型: {other}"),
                }
            }
            Message::Binary(b) if listening => {
                let pcm = match decoder.decode(&b) {
                    Ok(p) => p.to_vec(),
                    Err(e) => {
                        debug!("Opus 解码失败: {e}");
                        continue;
                    }
                };
                match vad.push(&pcm) {
                    VadEvent::StartSpeech => {
                        info!("检测到说话开始");
                        if pre_roll
                            .drain(..)
                            .chain(std::iter::once(pcm))
                            .any(|frame| asr_tx.try_send(AsrCmd::Pcm(frame)).is_err())
                        {
                            closed.cancel();
                        }
                    }
                    VadEvent::None if vad.is_speaking() => {
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
                    VadEvent::EndSpeech => {
                        info!("检测到说话结束");
                        listening = false;
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

    info!("设备断开: {device_id}");
    closed.cancel();
    asr_cancel.cancel();
    shared.abort_turn(input.take()).await;
    brain.abort();
    let _ = brain.await;
}

pub(crate) async fn run_turn(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: CancellationToken,
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
        result = run_turn_inner(shared, tts, user_text, &normal_cancel, &budget) => result.map(Some),
    };
    // Dropping the inner future aborts HTTP and rmcp's request guard cancels RPCs.
    // Keep recovery on the parent token so abort/disconnect still interrupts it.
    normal_cancel.cancel();
    if !token.is_cancelled() && !shared.closed.is_cancelled() {
        if let Err(e) = &result {
            error!("对话失败（未生成实时读数）: {e:#}");
            let text = if timed_out {
                TIMEOUT_REPLY
            } else {
                "暂时无法获取可靠回复，请稍后再试。"
            };
            shared.send_tts_state("sentence_start", Some(text)).await;
            let deadline = budget.recovery_deadline();
            if Instant::now() < deadline {
                if let Ok(mut encoder) = OpusOut::new(24_000, 60) {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {},
                        _ = shared.closed.cancelled() => {},
                        _ = tokio::time::timeout_at(deadline, speak_audio(shared, tts, &mut encoder, text, &token, &budget)) => {},
                    }
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

async fn run_turn_inner(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: &CancellationToken,
    budget: &TurnBudget,
) -> Result<Vec<Value>> {
    info!("[{}] 用户: {user_text}", shared.device_id);
    shared
        .send_json(crate::protocol::stt_text(&user_text, &shared.session_id))
        .await;
    shared.send_tts_state("start", None).await;

    // 组织上下文
    let mut messages: Vec<Value> = Vec::new();
    if !shared.cfg.llm.prompt.trim().is_empty() {
        messages.push(json!({"role": "system", "content": shared.cfg.llm.prompt}));
    }
    {
        let hist = shared.history.lock().await;
        messages.extend(hist.iter().cloned());
    }
    messages.push(json!({"role": "user", "content": user_text}));

    let model_started = Instant::now();
    let (mut buf, transcript) = chat_stream(
        &shared.cfg.llm,
        messages,
        shared.mcp.clone(),
        token.clone(),
    )
    .await?;
    debug!(elapsed_ms = model_started.elapsed().as_millis(), "LLM and tools complete");
    let mut encoder = OpusOut::new(24_000, 60)?;
    for sentence in split_sentences(&mut buf, true) {
        speak(shared, tts, &mut encoder, &sentence, token, budget).await?;
    }
    let mut history = vec![json!({"role":"user", "content":user_text})];
    history.extend(transcript);
    Ok(history)
}

/// Text/status notifications never satisfy the first-audio deadline.
async fn speak(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    encoder: &mut OpusOut,
    sentence: &str,
    token: &CancellationToken,
    budget: &TurnBudget,
) -> Result<()> {
    let text = clean_for_tts(sentence);
    if text.is_empty() {
        return Ok(());
    }
    shared.send_tts_state("sentence_start", Some(&text)).await;
    speak_audio(shared, tts, encoder, &text, token, budget).await
}

async fn speak_audio(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    encoder: &mut OpusOut,
    text: &str,
    token: &CancellationToken,
    budget: &TurnBudget,
) -> Result<()> {
    let synth_started = Instant::now();
    let pcm_bytes = tts.synth(text).await?;
    debug!(elapsed_ms = synth_started.elapsed().as_millis(), "TTS sentence synthesized");
    ensure!(!pcm_bytes.is_empty(), "TTS returned no audio");
    let pcm = bytes_to_i16(&pcm_bytes);
    let frames = encoder.encode_padded(&pcm);
    ensure!(!frames.is_empty(), "TTS produced no Opus frames");
    let mut next = Instant::now();
    for frame in frames {
        if token.is_cancelled() {
            bail!("turn cancelled");
        }
        shared.send_audio(frame, token).await?;
        if !budget.first_audio.swap(true, Ordering::SeqCst) {
            debug!(
                elapsed_ms = budget.started.elapsed().as_millis(),
                "text turn to first audio"
            );
        }
        next += Duration::from_millis(60);
        tokio::time::sleep_until(next).await;
    }
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

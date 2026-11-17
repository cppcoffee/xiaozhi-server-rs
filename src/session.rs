use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::asr::{run_asr, AsrCmd};
use crate::audio::{bytes_to_i16, OpusIn, OpusOut};
use crate::config::Config;
use crate::llm::chat_stream;
use crate::text::{clean_for_tts, split_sentences};
use crate::tts::DoubaoTts;
use crate::vad::{EnergyVad, VadEvent};

/// 保留最近多少轮对话上下文
const HISTORY_ROUNDS: usize = 6;

/// 多久没有任何消息就断开这条连接（设备待机时只有唤醒才发消息）
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

pub struct Shared {
    pub cfg: Arc<Config>,
    pub ws_tx: mpsc::Sender<Message>,
    pub session_id: String,
    pub device_id: String,
    pub history: Mutex<Vec<Value>>,
    pub turn_cancel: Mutex<Option<CancellationToken>>,
    pub speaking: AtomicBool,
}

impl Shared {
    pub async fn send_json(&self, v: Value) {
        let text = v.to_string();
        debug!("-> {text}");
        let _ = self.ws_tx.send(Message::Text(text.into())).await;
    }

    pub async fn send_audio(&self, frame: Vec<u8>) {
        let _ = self.ws_tx.send(Message::Binary(frame.into())).await;
    }

    pub async fn send_tts_state(&self, state: &str, text: Option<&str>) {
        let v = crate::protocol::tts_state(state, &self.session_id, text);
        self.send_json(v).await;
    }

    /// 打断当前回合；真的打断了才补发 tts stop，让设备退出播放态
    pub async fn abort_turn(&self) -> bool {
        let token = self.turn_cancel.lock().await.take();
        self.speaking.store(false, Ordering::SeqCst);
        let live = matches!(&token, Some(t) if !t.is_cancelled());
        if let Some(t) = token {
            t.cancel();
        }
        if live {
            self.send_tts_state("stop", None).await;
        }
        live
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
    ws.on_upgrade(move |sock| async move {
        handle_connection(sock, cfg, device_id).await;
    })
}

/// 一条 websocket 连接的完整生命周期
pub async fn handle_connection(sock: WebSocket, cfg: Arc<Config>, device_id: String) {
    let session_id = Uuid::new_v4().to_string();
    info!("设备连接: device_id={device_id} session_id={session_id}");

    let (sink, mut stream) = sock.split();
    let (ws_tx, ws_rx) = mpsc::channel::<Message>(512);

    // 写任务：所有出站消息统一排队，避免多任务竞争 socket
    tokio::spawn(async move {
        let mut sink = sink;
        let mut ws_rx = ws_rx;
        while let Some(msg) = ws_rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    // ASR 通道
    let (asr_tx, asr_rx) = mpsc::channel::<AsrCmd>(64);
    let (asr_out_tx, mut asr_out_rx) = mpsc::channel::<String>(8);
    let asr_cancel = CancellationToken::new();
    {
        let cfg_asr = cfg.asr.clone();
        let cancel = asr_cancel.clone();
        let sr = crate::config::INPUT_SAMPLE_RATE;
        tokio::spawn(async move {
            run_asr(cfg_asr, sr, asr_rx, asr_out_tx, cancel).await;
        });
    }

    let shared = Arc::new(Shared {
        cfg: cfg.clone(),
        ws_tx,
        session_id: session_id.clone(),
        device_id: device_id.clone(),
        history: Mutex::new(Vec::new()),
        turn_cancel: Mutex::new(None),
        speaking: AtomicBool::new(false),
    });

    // 大脑任务：识别出文本 → 生成回复 → 合成语音
    {
        let shared = shared.clone();
        let tts = DoubaoTts::new(cfg.tts.clone());
        tokio::spawn(async move {
            while let Some(text) = asr_out_rx.recv().await {
                if text.trim().is_empty() {
                    // 空回合：回一对 start/stop，否则设备会一直等它的 30 秒超时
                    warn!("本回合没有识别到语音，通知设备结束");
                    shared.send_tts_state("start", None).await;
                    shared.send_tts_state("stop", None).await;
                    continue;
                }
                if let Err(e) = run_turn(&shared, &tts, text).await {
                    error!("回合处理失败: {e}");
                }
            }
        });
    }

    // 读任务
    let mut decoder = match OpusIn::new(crate::config::INPUT_SAMPLE_RATE) {
        Ok(d) => d,
        Err(e) => {
            error!("Opus 解码器初始化失败: {e}");
            return;
        }
    };
    let mut vad = EnergyVad::new(&cfg.vad, crate::config::INPUT_FRAME_MS);
    let mut manual_mode = false;
    let mut flushing = false;

    loop {
        let msg = tokio::select! {
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
                    }
                    "listen" => {
                        let state = v["state"].as_str().unwrap_or("");
                        if let Some(mode) = v["mode"].as_str() {
                            manual_mode = mode == "manual";
                        }
                        match state {
                            "start" => {
                                // 设备开始拾音：打断正在播放的回复，重开 ASR 流
                                shared.abort_turn().await;
                                let _ = asr_tx.send(AsrCmd::Reset).await;
                                flushing = false;
                            }
                            "stop" => {
                                // 设备侧报结束：VAD 开关只影响谁先收尾，最终结果必须出
                                let _ = asr_tx.send(AsrCmd::Flush).await;
                                flushing = true;
                            }
                            "detect" => {
                                if let Some(text) = v["text"].as_str() {
                                    if !text.trim().is_empty() && manual_mode {
                                        // 设备直接给出文本（部分固件手按说话）
                                        let _ = asr_out_tx_for_manual(&shared, text).await;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    "abort" => {
                        info!("收到 abort，打断当前回合");
                        shared.abort_turn().await;
                        let _ = asr_tx.send(AsrCmd::Reset).await;
                        flushing = false;
                    }
                    "ping" => {
                        shared.send_json(crate::protocol::pong()).await;
                    }
                    "iot" | "mcp" => {
                        debug!("忽略不支持的消息类型: {}", v["type"]);
                    }
                    other => debug!("未知消息类型: {other}"),
                }
            }
            Message::Binary(b) => {
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
                        flushing = false;
                        let _ = asr_tx.send(AsrCmd::Pcm(pcm)).await;
                    }
                    VadEvent::None => {
                        if vad.is_speaking() && !flushing {
                            let _ = asr_tx.send(AsrCmd::Pcm(pcm)).await;
                        }
                    }
                    VadEvent::EndSpeech => {
                        info!("检测到说话结束");
                        if !flushing {
                            let _ = asr_tx.send(AsrCmd::Pcm(pcm)).await;
                            let _ = asr_tx.send(AsrCmd::Flush).await;
                            flushing = true;
                        }
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    info!("设备断开: {device_id}");
    asr_cancel.cancel();
    shared.abort_turn().await;
}

/// 手动模式下设备直传文本
async fn asr_out_tx_for_manual(shared: &Arc<Shared>, text: &str) {
    // 直接走一次完整回合
    let tts = DoubaoTts::new(shared.cfg.tts.clone());
    if let Err(e) = run_turn(shared, &tts, text.to_string()).await {
        error!("手动回合失败: {e}");
    }
}

async fn run_turn(shared: &Arc<Shared>, tts: &DoubaoTts, user_text: String) -> Result<()> {
    let user_text = user_text.trim().to_string();
    if user_text.is_empty() {
        return Ok(());
    }
    let token = CancellationToken::new();
    {
        let mut cur = shared.turn_cancel.lock().await;
        if let Some(old) = cur.take() {
            old.cancel();
        }
        *cur = Some(token.clone());
    }
    shared.speaking.store(true, Ordering::SeqCst);

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

    let (tx, mut rx) = mpsc::channel::<String>(64);
    let llm_cfg = shared.cfg.llm.clone();
    let llm_task = tokio::spawn(async move { chat_stream(&llm_cfg, messages, tx).await });

    let mut encoder = OpusOut::new(24_000, 60)?;
    let mut buf = String::new();
    let mut full_reply = String::new();

    while let Some(delta) = rx.recv().await {
        if token.is_cancelled() {
            break;
        }
        buf.push_str(&delta);
        full_reply.push_str(&delta);
        for sentence in split_sentences(&mut buf, false) {
            if token.is_cancelled() {
                break;
            }
            speak(shared, tts, &mut encoder, &sentence, &token).await?;
        }
    }
    if !token.is_cancelled() {
        for sentence in split_sentences(&mut buf, true) {
            speak(shared, tts, &mut encoder, &sentence, &token).await?;
        }
        shared.send_tts_state("stop", None).await;
    }
    let _ = llm_task.await;
    shared.speaking.store(false, Ordering::SeqCst);
    // 回合结束立刻摘掉 token，否则下一次 abort 会把已结束的回合误判成“正在播放”
    *shared.turn_cancel.lock().await = None;

    // 记录历史（丢弃被打断的半句）
    let mut hist = shared.history.lock().await;
    hist.push(json!({"role": "user", "content": user_text}));
    if !full_reply.trim().is_empty() {
        hist.push(json!({"role": "assistant", "content": full_reply}));
    }
    let keep = HISTORY_ROUNDS * 2;
    if hist.len() > keep {
        let drop_n = hist.len() - keep;
        hist.drain(..drop_n);
    }
    Ok(())
}

/// 合成一句话并按 60ms 一帧定速推给设备
async fn speak(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    encoder: &mut OpusOut,
    sentence: &str,
    token: &CancellationToken,
) -> Result<()> {
    let text = clean_for_tts(sentence);
    if text.is_empty() {
        return Ok(());
    }
    shared.send_tts_state("sentence_start", Some(&text)).await;

    let pcm_bytes = match tts.synth(&text).await {
        Ok(b) => b,
        Err(e) => {
            error!("TTS 合成失败: {e}");
            return Ok(());
        }
    };
    if pcm_bytes.is_empty() {
        return Ok(());
    }
    let pcm = bytes_to_i16(&pcm_bytes);
    let frames = encoder.encode_padded(&pcm);

    let mut next = Instant::now();
    for frame in frames {
        if token.is_cancelled() {
            return Ok(());
        }
        shared.send_audio(frame).await;
        next += Duration::from_millis(60);
        tokio::time::sleep_until(next.into()).await;
    }
    Ok(())
}

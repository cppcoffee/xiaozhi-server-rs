use std::io::Write;

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::AsrCfg;

/// 供 UI/会话层下发的指令
#[derive(Debug)]
pub enum AsrCmd {
    /// 一帧 PCM16（16k 单声道）
    Pcm(Vec<i16>),
    /// 说话结束，通知服务端出最终结果
    Flush,
    /// 丢弃当前会话（打断时用）
    Reset,
}

const MSG_FULL_CLIENT_REQUEST: u8 = 0x01;
const MSG_AUDIO_ONLY_REQUEST: u8 = 0x02;
const SERIAL_JSON: u8 = 0x01;
const COMPRESS_GZIP: u8 = 0x01;

/// Flush 后等最终结果的步数（100ms/步）。豆包正常 <1s，给到 10s，
/// 避免出结果慢一点就把整轮静默丢掉
const FLUSH_WAIT_STEPS: usize = 100;

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(data).ok();
    enc.finish().unwrap_or_default()
}

fn gunzip(data: &[u8]) -> Option<Vec<u8>> {
    let mut dec = flate2::read::GzDecoder::new(data);
    let mut out = Vec::new();
    std::io::Read::read_to_end(&mut dec, &mut out).ok()?;
    Some(out)
}

/// 豆包 v3 二进制帧头：4 字节
fn header(msg_type: u8, flags: u8, compress: u8) -> [u8; 4] {
    [
        (0x01 << 4) | 0x01,                // version=1, header_size=1
        (msg_type << 4) | (flags & 0x0f),  // msg type + flags
        (SERIAL_JSON << 4) | compress,     // serialization + compression
        0x00,
    ]
}

fn frame(msg_type: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let body = gzip(payload);
    let mut buf = Vec::with_capacity(8 + body.len());
    buf.extend_from_slice(&header(msg_type, flags, COMPRESS_GZIP));
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// 解析服务端返回帧
fn parse_frame(res: &[u8]) -> Result<Value> {
    if res.len() < 4 {
        return Err(anyhow!("响应长度不足: {}", res.len()));
    }
    let msg_type = res[1] >> 4;
    let compress = res[2] & 0x0f;

    if msg_type == 0x0f {
        // 错误帧：code(4) + length(4) + json
        let code = if res.len() >= 8 {
            u32::from_be_bytes([res[4], res[5], res[6], res[7]])
        } else {
            0
        };
        let body = if res.len() > 12 { &res[12..] } else { &res[4..] };
        let body = if compress == COMPRESS_GZIP {
            gunzip(body).unwrap_or_else(|| body.to_vec())
        } else {
            body.to_vec()
        };
        let v: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
        return Err(anyhow!("ASR 错误 code={} body={}", code, v));
    }

    let body = if res.len() >= 12 {
        let len = u32::from_be_bytes([res[8], res[9], res[10], res[11]]) as usize;
        if len > 0 && 12 + len <= res.len() {
            &res[12..12 + len]
        } else {
            &res[8..]
        }
    } else {
        &res[4..]
    };
    let raw = if body.first() == Some(&0x1f) {
        gunzip(body).unwrap_or_else(|| body.to_vec())
    } else {
        body.to_vec()
    };
    let v: Value = serde_json::from_slice(&raw)?;
    Ok(v)
}

fn request_json(cfg: &AsrCfg, sample_rate: u32) -> Value {
    json!({
        "app": { "appid": cfg.appid, "token": cfg.access_token },
        "user": { "uid": "streaming_asr_service" },
        "request": {
            "reqid": Uuid::new_v4().to_string(),
            "workflow": "audio_in,resample,partition,vad,fe,decode,itn,nlu_punctuate",
            "show_utterances": true,
            "result_type": "single",
            "sequence": 1,
            "end_window_size": 200,
            "corpus": {"boosting_table_name": "", "correct_table_name": ""}
        },
        "audio": {
            "format": "pcm",
            "codec": "pcm",
            "rate": sample_rate,
            "bits": 16,
            "channel": 1,
            "sample_rate": sample_rate
        }
    })
}

/// 从返回帧里挑出 definite 的语句文本
fn extract_text(v: &Value) -> Option<String> {
    if let Some(code) = v.get("code").and_then(|c| c.as_i64()) {
        // 1013：无有效语音，属正常
        if code == 20000000 || code == 1013 {
            return None;
        }
    }
    let result = v.get("result")?;
    let mut out = String::new();
    if let Some(utts) = result.get("utterances").and_then(|u| u.as_array()) {
        for u in utts {
            if u.get("definite").and_then(|d| d.as_bool()).unwrap_or(false) {
                if let Some(t) = u.get("text").and_then(|t| t.as_str()) {
                    out.push_str(t);
                }
            }
        }
    }
    if out.trim().is_empty() {
        None
    } else {
        Some(out.trim().to_string())
    }
}

/// ASR 长连接管理：懒连接、流式送音频、吐最终文本
pub struct DoubaoAsr {
    cfg: AsrCfg,
    sample_rate: u32,
    ws: Option<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
}

impl DoubaoAsr {
    pub fn new(cfg: AsrCfg, sample_rate: u32) -> Self {
        Self {
            cfg,
            sample_rate,
            ws: None,
        }
    }

    async fn connect(&mut self) -> Result<()> {
        let mut req = self.cfg.ws_url.clone().into_client_request()?;
        let h = req.headers_mut();
        h.insert("X-Api-App-Key", self.cfg.appid.parse()?);
        h.insert("X-Api-Access-Key", self.cfg.access_token.parse()?);
        h.insert("X-Api-Resource-Id", self.cfg.resource_id.parse()?);
        h.insert("X-Api-Connect-Id", Uuid::new_v4().to_string().parse()?);

        let (ws, _) = tokio_tungstenite::connect_async(req).await?;
        self.ws = Some(ws);

        // 初始化请求
        let payload = serde_json::to_vec(&request_json(&self.cfg, self.sample_rate))?;
        let f = frame(MSG_FULL_CLIENT_REQUEST, 0x00, &payload);
        self.ws.as_mut().unwrap().send(Message::Binary(f.into())).await?;

        // 等初始化响应
        if let Some(msg) = self.ws.as_mut().unwrap().next().await {
            match msg? {
                Message::Binary(b) => {
                    let v = parse_frame(&b)?;
                    if let Some(code) = v.get("code").and_then(|c| c.as_i64()) {
                        if code != 0 && code != 1000 && code != 20000000 {
                            return Err(anyhow!("ASR 初始化失败: {}", v));
                        }
                    }
                    debug!("ASR 初始化响应: {}", v);
                }
                other => warn!("ASR 初始化收到非二进制帧: {:?}", other),
            }
        }
        info!("ASR 连接已建立 ({} / {})", self.cfg.ws_url, self.cfg.resource_id);
        Ok(())
    }

    async fn send_pcm(&mut self, pcm: &[i16]) -> Result<()> {
        if self.ws.is_none() {
            self.connect().await?;
        }
        let bytes: Vec<u8> = pcm.iter().flat_map(|s| s.to_le_bytes()).collect();
        let f = frame(MSG_AUDIO_ONLY_REQUEST, 0x00, &bytes);
        if let Some(ws) = self.ws.as_mut() {
            ws.send(Message::Binary(f.into())).await?;
        }
        Ok(())
    }

    /// 发结束帧（gzip 空负载），通知服务端出最终结果
    async fn flush(&mut self) -> Result<()> {
        if let Some(ws) = self.ws.as_mut() {
            let f = frame(MSG_AUDIO_ONLY_REQUEST, 0x02, b"");
            ws.send(Message::Binary(f.into())).await?;
        }
        Ok(())
    }

    async fn close(&mut self) {
        if let Some(mut ws) = self.ws.take() {
            let _ = ws.close(None).await;
        }
    }
}

/// 非阻塞地取出 ws 上已到达的所有帧，返回可能出现的最终文本
async fn drain_pending(asr: &mut DoubaoAsr, wait_ms: u64) -> Option<String> {
    let mut text = None;
    let mut first = true;
    loop {
        let timeout = if first {
            std::time::Duration::from_millis(wait_ms)
        } else {
            std::time::Duration::from_millis(0)
        };
        first = false;
        let Some(ws) = asr.ws.as_mut() else { return text };
        match tokio::time::timeout(timeout, ws.next()).await {
            Ok(Some(Ok(Message::Binary(b)))) => match parse_frame(&b) {
                Ok(v) => {
                    if let Some(t) = extract_text(&v) {
                        info!("ASR 识别: {t}");
                        text = Some(t);
                    } else if v.get("result").is_some() {
                        debug!("ASR 中间结果: {v}");
                    }
                }
                Err(e) => warn!("ASR 解析失败: {e}"),
            },
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                asr.ws = None;
                return text;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => {
                warn!("ASR 连接错误: {e}");
                asr.ws = None;
                return text;
            }
            Err(_) => return text,
        }
    }
}

/// 运行 ASR：接收音频帧 → 吐最终识别文本
pub async fn run_asr(
    cfg: AsrCfg,
    sample_rate: u32,
    mut rx: mpsc::Receiver<AsrCmd>,
    out: mpsc::Sender<String>,
    cancel: tokio_util::sync::CancellationToken,
) {
    let mut asr = DoubaoAsr::new(cfg, sample_rate);
    let mut waiting_final = false;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                asr.close().await;
                return;
            }
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { asr.close().await; return; };
                match cmd {
                    AsrCmd::Pcm(pcm) => {
                        if waiting_final { continue; }
                        if let Err(e) = asr.send_pcm(&pcm).await {
                            error!("发送 ASR 音频失败: {e}");
                            asr.close().await;
                            continue;
                        }
                        if let Some(text) = drain_pending(&mut asr, 0).await {
                            waiting_final = true;
                            if out.send(text).await.is_err() { return; }
                        }
                    }
                    AsrCmd::Flush => {
                        if waiting_final {
                            // 本轮已出结果，为下一句重新建流
                            asr.close().await;
                            waiting_final = false;
                            continue;
                        }
                        if let Err(e) = asr.flush().await {
                            warn!("发送 ASR 结束帧失败: {e}");
                            asr.close().await;
                        }
                        // 等服务端出最终结果
                        let mut got = None;
                        for _ in 0..FLUSH_WAIT_STEPS {
                            if let Some(t) = drain_pending(&mut asr, 100).await {
                                got = Some(t);
                                break;
                            }
                            if asr.ws.is_none() { break; }
                        }
                        waiting_final = true;
                        match got {
                            Some(text) => {
                                if out.send(text).await.is_err() { return; }
                            }
                            None => {
                                // 静音/纯噪声：关流，并给上层一个空回合标记，否则设备一直等
                                asr.close().await;
                                if out.send(String::new()).await.is_err() { return; }
                            }
                        }
                    }
                    AsrCmd::Reset => {
                        asr.close().await;
                        waiting_final = false;
                    }
                }
            }
        }
    }
}

/// `--check` 用：只做握手 + 初始化，验证 appid/token/resource_id 是否匹配
pub async fn probe(cfg: &crate::config::AsrCfg, sample_rate: u32) -> Result<String> {
    if cfg.appid.contains("你的") || cfg.access_token.contains("你的") {
        return Err(anyhow!("asr.appid / asr.access_token 还是占位符（请检查 config.toml 的 [asr]）"));
    }
    let mut asr = DoubaoAsr::new(cfg.clone(), sample_rate);
    asr.connect().await?;
    // 发一个结束帧，确认能被服务端接受
    asr.flush().await?;
    let text = drain_pending(&mut asr, 1500).await;
    asr.close().await;
    Ok(match text {
        Some(t) => format!("连接成功 ({} )，静音探测返回: {}", cfg.resource_id, t),
        None => format!("连接成功 ({} )，静音探测无文本（正常）", cfg.resource_id),
    })
}

/// 小智 websocket 协议里用到的消息格式（与 Python 版逐字段对齐）
use serde_json::{json, Value};

/// 本机时区相对 UTC 的秒数，跟随操作系统设置
pub fn utc_offset_secs() -> i64 {
    chrono::Local::now().offset().local_minus_utc() as i64
}

pub fn now_str() -> String {
    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn tts_state(state: &str, session_id: &str, text: Option<&str>) -> Value {
    let mut v = json!({"type": "tts", "state": state, "session_id": session_id});
    if let Some(t) = text {
        v["text"] = json!(t);
    }
    v
}

pub fn stt_text(text: &str, session_id: &str) -> Value {
    json!({"type": "stt", "text": text, "session_id": session_id})
}

pub fn pong() -> Value {
    json!({"type": "pong", "timestamp": now_str()})
}

/// Message formats used by the xiaozhi websocket protocol (field-for-field aligned with the Python version)
use serde_json::{json, Value};

/// Seconds the local timezone is offset from UTC, following the OS setting
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

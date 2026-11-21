/// OTA: tells the device which websocket to connect to and syncs its clock
use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use serde_json::{json, Value};
use tracing::info;

use crate::config::{local_ip, Config, ConfigStore};
use crate::protocol::utc_offset_secs;

/// OTA response: websocket address built from the Host the device used, timezone from the local setting
pub fn response(cfg: &Config, device_version: &str, host: Option<&str>) -> Value {
    json!({
        "server_time": {
            "timestamp": chrono::Utc::now().timestamp_millis(),
            "timezone_offset": utc_offset_secs() / 60
        },
        "firmware": {
            "version": device_version,
            "url": ""
        },
        "websocket": {
            "url": ws_url(cfg, host),
            "token": ""
        }
    })
}

/// Reply with the same hostname the device used to reach OTA
fn ws_url(cfg: &Config, host: Option<&str>) -> String {
    let host = host
        .and_then(bare_host)
        .map(str::to_string)
        .unwrap_or_else(local_ip);
    format!("ws://{host}:{}/xiaozhi/v1/", cfg.server.ws_port)
}

/// Strip the port from the Host header; keep brackets for IPv6
fn bare_host(host: &str) -> Option<&str> {
    let host = host.trim();
    if host.is_empty() {
        return None;
    }
    if let Some(end) = host.find(']') {
        return Some(&host[..=end]);
    }
    match host.rfind(':') {
        Some(i) if !host[..i].contains(':') => Some(&host[..i]),
        _ => Some(host),
    }
}

/// OTA endpoint: the device gets its websocket address and clock sync here
pub async fn handler(
    State(state): State<Arc<ConfigStore>>,
    headers: HeaderMap,
    body: String,
) -> Json<Value> {
    let cfg = state.current();
    let device_id = headers
        .get("device-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let client_id = headers
        .get("client-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let body_json: Value = serde_json::from_str(&body).unwrap_or(json!({}));

    let version = ["device-version", "firmware-version", "application-version"]
        .iter()
        .find_map(|h| {
            headers
                .get(*h)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        })
        .or_else(|| {
            body_json
                .get("application")
                .and_then(|a| a.get("version"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "0.0.0".to_string());

    let host = headers.get("host").and_then(|v| v.to_str().ok());
    info!("OTA request: device_id={device_id} client_id={client_id} version={version}");
    Json(response(&cfg, &version, host))
}

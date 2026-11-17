/// OTA：设备请求 OTA 时告诉它连哪个 websocket，并给设备对时
use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use serde_json::{json, Value};
use tracing::info;

use crate::config::{local_ip, Config, ConfigStore};
use crate::protocol::utc_offset_secs;

/// OTA 响应：websocket 地址按设备请求用的 Host 组装，时区取本机设置
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

/// 设备从哪个地址访问 OTA，就用同一个主机名下发 websocket 地址
fn ws_url(cfg: &Config, host: Option<&str>) -> String {
    let host = match host.and_then(bare_host) {
        Some(h) => h.to_string(),
        None => local_ip(),
    };
    format!("ws://{host}:{}/xiaozhi/v1/", cfg.server.ws_port)
}

/// Host 头去掉端口；IPv6 保留方括号
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ota_uses_configured_websocket_port() {
        let mut cfg = Config::parse(include_str!("../config.toml")).unwrap();
        cfg.server.ws_port = 9000;
        cfg.server.http_port = 9003;
        let info = response(&cfg, "1.0.0", Some("192.168.1.100:9003"));
        assert_eq!(
            info["websocket"]["url"],
            "ws://192.168.1.100:9000/xiaozhi/v1/"
        );
    }

    #[test]
    fn host_without_port() {
        assert_eq!(bare_host("192.168.1.100:8003"), Some("192.168.1.100"));
        assert_eq!(bare_host("192.168.1.100"), Some("192.168.1.100"));
        assert_eq!(bare_host(" [::1]:8003 "), Some("[::1]"));
        assert_eq!(bare_host(""), None);
    }
}

/// OTA 接口：设备从这里拿 websocket 地址和对时
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
    info!("OTA 请求 device_id={device_id} client_id={client_id} version={version}");
    Json(response(&cfg, &version, host))
}

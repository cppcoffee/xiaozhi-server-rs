mod asr;
mod audio;
mod check;
mod config;
mod llm;
mod logging;
mod mcp;
mod ota;
mod protocol;
mod session;
mod text;
mod tts;
mod vad;
mod webcfg;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::routing::{get, post};
use axum::Router;
use tracing::{info, warn};

use config::{Config, ConfigStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 参数：可选 config 路径 + --check（只校验凭证，不启动服务）
    let mut path = "config.toml".to_string();
    let mut do_check = false;
    for a in std::env::args().skip(1) {
        if a == "--check" {
            do_check = true;
        } else {
            path = a;
        }
    }
    let cfg = Arc::new(Config::load(&path)?);
    logging::init(&cfg.log);

    if do_check {
        let ok = check::run(&cfg).await;
        std::process::exit(if ok { 0 } else { 1 });
    }
    info!(
        "小智 Rust Server 启动：ws={} http={} llm={} asr={} tts={}",
        cfg.server.ws_port,
        cfg.server.http_port,
        cfg.llm.model,
        cfg.asr.resource_id,
        cfg.tts.voice
    );
    if cfg.llm.api_key.contains("你的") {
        warn!("config.toml 里 llm.api_key 还是占位符，请填入火山方舟 API Key");
    }

    let state = Arc::new(ConfigStore::new(path, (*cfg).clone()));

    let app = Router::new()
        .route("/xiaozhi/ota/", post(ota::handler))
        .route("/xiaozhi/ota/check", post(ota::handler))
        .route("/xiaozhi/v1/", get(session::ws_handler))
        .merge(webcfg::routes())
        .route("/", get(|| async { "xiaozhi rust server ok" }))
        .with_state(state.clone());

    let ws_addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.ws_port));
    let http_addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.http_port));

    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await.with_context(|| format!("无法监听 HTTP(OTA) {http_addr}"))?;
    let ws_listener = tokio::net::TcpListener::bind(ws_addr)
        .await.with_context(|| format!("无法监听 WebSocket {ws_addr}"))?;

    info!("HTTP(OTA) 监听 {http_addr}");
    info!("WebSocket 监听 {ws_addr}  路径 /xiaozhi/v1/");
    let http = axum::serve(http_listener, app.clone().into_make_service_with_connect_info::<SocketAddr>());
    let ws = axum::serve(ws_listener, app.into_make_service_with_connect_info::<SocketAddr>());
    tokio::try_join!(async { http.await }, async { ws.await })?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/support/device_tools.rs"]
mod device_tools;

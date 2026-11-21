mod asr;
mod audio;
pub mod check;
pub mod config;
mod llm;
pub mod logging;
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
use axum::serve::ListenerExt;
use axum::Router;
use tracing::{info, warn};

pub use config::{Config, ConfigStore};

pub fn build_router(state: Arc<ConfigStore>) -> Router {
    Router::new()
        .route("/xiaozhi/ota/", post(ota::handler))
        .route("/xiaozhi/ota/check", post(ota::handler))
        .route("/xiaozhi/v1/", get(session::ws_handler))
        .merge(webcfg::routes())
        .route("/", get(|| async { "xiaozhi rust server ok" }))
        .with_state(state)
}

pub async fn serve(cfg: Arc<Config>, config_path: impl Into<String>) -> anyhow::Result<()> {
    info!(
        "Starting Xiaozhi Rust server: ws={} http={} llm={} asr={} tts={}",
        cfg.server.ws_port, cfg.server.http_port, cfg.llm.model, cfg.asr.resource_id, cfg.tts.voice
    );
    if cfg.llm.api_key.contains("你的") {
        warn!("llm.api_key is a placeholder. Set it to your Volcengine Ark API key.");
    }

    let state = Arc::new(ConfigStore::new(config_path.into(), (*cfg).clone()));
    let app = build_router(state);

    let ws_addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.ws_port));
    let http_addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.http_port));

    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("Failed to bind the OTA HTTP listener to {http_addr}"))?
        .tap_io(|stream| {
            let _ = stream.set_nodelay(true);
        });
    let ws_listener = tokio::net::TcpListener::bind(ws_addr)
        .await
        .with_context(|| format!("Failed to bind the WebSocket listener to {ws_addr}"))?
        .tap_io(|stream| {
            let _ = stream.set_nodelay(true);
        });

    info!("OTA HTTP listening on {http_addr}");
    info!("WebSocket listening on {ws_addr} at /xiaozhi/v1/");
    let http = axum::serve(
        http_listener,
        app.clone()
            .into_make_service_with_connect_info::<SocketAddr>(),
    );
    let ws = axum::serve(
        ws_listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    );
    tokio::try_join!(async { http.await }, async { ws.await })?;
    Ok(())
}

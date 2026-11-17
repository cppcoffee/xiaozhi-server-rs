mod asr;
mod audio;
mod check;
mod config;
mod llm;
mod logging;
mod ota;
mod protocol;
mod session;
mod text;
mod tts;
mod vad;
mod webcfg;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tracing::{info, warn};

use config::{Config, ConfigStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 参数：可选 config 路径 + --check（只校验凭证，不启动服务）
    let mut path = "config.yaml".to_string();
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
        cfg.server.port,
        cfg.server.http_port,
        cfg.llm.model,
        cfg.asr.resource_id,
        cfg.tts.voice
    );
    if cfg.llm.api_key.contains("你的") {
        warn!("config.yaml 里 llm.api_key 还是占位符，请填入火山方舟 API Key");
    }

    let state = Arc::new(ConfigStore::new(path, (*cfg).clone()));

    let app = Router::new()
        .route("/xiaozhi/ota/", post(ota::handler))
        .route("/xiaozhi/ota/check", post(ota::handler))
        .route("/xiaozhi/v1/", get(session::ws_handler))
        .route("/config", get(webcfg::page).post(webcfg::save))
        .route("/", get(|| async { "xiaozhi rust server ok" }))
        .with_state(state.clone());

    let ws_addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.port));
    let http_addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.http_port));

    let ws_app = app.clone();
    let http_app = app;
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
        info!("HTTP(OTA) 监听 {http_addr}");
        axum::serve(listener, http_app).await.unwrap();
    });

    let listener = tokio::net::TcpListener::bind(ws_addr).await?;
    info!("WebSocket 监听 {ws_addr}  路径 /xiaozhi/v1/");
    axum::serve(listener, ws_app).await?;
    Ok(())
}

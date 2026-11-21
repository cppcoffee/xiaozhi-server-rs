/// 浏览器配置页：GET 显示 config.toml 原文，POST 校验后写回（注释原样保留）
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, Form, Request, State};
use axum::http::{header, uri::Authority, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::{routing::get, Router};
use tracing::{info, warn};

use crate::config::{Config, ConfigStore};

const PAGE: &str = r#"<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>xiaozhi-server-rs 配置</title>
<style>
body{margin:16px;font:14px/1.6 system-ui,sans-serif}
textarea{width:100%;box-sizing:border-box;min-height:70vh;font:13px/1.5 ui-monospace,Menlo,monospace;white-space:pre}
.err{color:#b00;white-space:pre-wrap}.ok{color:#070}
</style></head><body>
<h1>xiaozhi-server-rs 配置</h1>
<p>保存后对重启后的新连接生效（监听端口也只在重启后改变）；正在通话的设备重连后生效。请从服务所在电脑打开 http://127.0.0.1:8000/config。</p>
{{MESSAGE}}
<form method="post" action="/config">
<textarea name="config" spellcheck="false" wrap="off">{{TOML}}</textarea>
<button type="submit">保存</button>
</form></body></html>
"#;

pub fn routes() -> Router<Arc<ConfigStore>> {
    Router::new()
        .route("/config", get(page).post(save))
        .route_layer(middleware::from_fn(local_only))
}

async fn local_only(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<Authority>().ok());
    // Validate the Host as well so an arbitrary browser origin cannot redirect
    // its DNS to a loopback address and read the local configuration.
    if !peer.ip().is_loopback() || !host.as_ref().is_some_and(local_host) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn local_host(authority: &Authority) -> bool {
    let host = authority.host();
    let ip = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost") || ip.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

async fn page(State(state): State<Arc<ConfigStore>>) -> Response {
    let text = std::fs::read_to_string(&state.path).unwrap_or_default();
    render(StatusCode::OK, &text, "")
}

async fn save(
    State(state): State<Arc<ConfigStore>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    if !same_origin(&headers) {
        warn!("Rejected cross-origin POST /config");
        return render(StatusCode::FORBIDDEN, "", "跨站请求被拒绝");
    }
    let text = form.get("config").cloned().unwrap_or_default();
    let next = match Config::parse(&text) {
        Ok(c) => c,
        Err(e) => {
            return render(
                StatusCode::BAD_REQUEST,
                &text,
                &format!(
                    r#"<p class="err">TOML 解析失败，未保存：{}</p>"#,
                    escape(&e.to_string())
                ),
            )
        }
    };
    if let Err(e) = state.replace(&text, next) {
        return render(
            StatusCode::INTERNAL_SERVER_ERROR,
            &text,
            &format!(r#"<p class="err">写入失败：{}</p>"#, escape(&e.to_string())),
        );
    }
    info!("Configuration saved through the web UI: {}", state.path);
    render(StatusCode::OK, &text, r#"<p class="ok">已保存。</p>"#)
}

fn render(status: StatusCode, text: &str, message: &str) -> Response {
    let page = PAGE
        .replace("{{MESSAGE}}", message)
        .replace("{{TOML}}", &escape(text));
    (status, Html(page)).into_response()
}

/// 挡掉别的网站借浏览器发起的跨站提交
fn same_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return true; // curl 等工具不带 Origin
    };
    let Some(host) = headers.get("host").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    origin.split("://").nth(1).unwrap_or(origin) == host
}

fn escape(text: &str) -> String {
    html_escape::encode_text(text).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        state: Arc<ConfigStore>,
    }

    impl Fixture {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("xiaozhi-config-{}.toml", uuid::Uuid::new_v4()));
            let text = include_str!("../config.example.toml");
            std::fs::write(&path, text).unwrap();
            Self {
                state: Arc::new(ConfigStore::new(
                    path.to_str().unwrap().to_string(),
                    Config::parse(text).unwrap(),
                )),
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.state.path);
        }
    }

    #[tokio::test]
    async fn save_toml_preserves_comments_and_applies_on_restart() {
        let fixture = Fixture::new();
        let old = fixture.state.current();
        let text = format!(
            "# 网页保存保留注释\n{}",
            include_str!("../config.example.toml").replace("ws_port = 26003", "ws_port = 9000")
        );
        let response = save(
            State(fixture.state.clone()),
            HeaderMap::new(),
            Form(HashMap::from([("config".into(), text.clone())])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(std::fs::read_to_string(&fixture.state.path).unwrap(), text);
        assert_eq!(
            Config::load(&fixture.state.path).unwrap().server.ws_port,
            9000
        );
        // Listeners cannot move on reload: OTA keeps the bound port until restart.
        assert_eq!(fixture.state.current().server.ws_port, 26003);
        assert_eq!(fixture.state.current().server.http_port, old.server.http_port);
    }

    #[tokio::test]
    async fn invalid_toml_does_not_replace_file_or_active_config() {
        let fixture = Fixture::new();
        let old = fixture.state.current();
        let response = save(
            State(fixture.state.clone()),
            HeaderMap::new(),
            Form(HashMap::from([("config".into(), "[server".into())])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .contains("TOML 解析失败"));
        assert_eq!(
            std::fs::read_to_string(&fixture.state.path).unwrap(),
            include_str!("../config.example.toml")
        );
        assert!(Arc::ptr_eq(&old, &fixture.state.current()));
    }
    #[test]
    fn only_loopback_authorities_are_allowed() {
        for name in [
            "127.0.0.1",
            "[::1]",
            "localhost",
            "LOCALHOST:8000",
        ] {
            assert!(local_host(&name.parse().unwrap()), "{name}");
        }
        for name in [
            "localhost.example",
            "192.168.1.2",
            "0.0.0.0",
            "[::]",
        ] {
            assert!(!local_host(&name.parse().unwrap()), "{name}");
        }
    }

    #[tokio::test]
    async fn management_rejects_remote_peers_and_untrusted_hosts_before_reading_or_writing() {
        let client = reqwest::Client::new();
        for (peer, host, expected) in [
            (
                "192.168.1.25:41000",
                "127.0.0.1",
                StatusCode::FORBIDDEN,
            ),
            (
                "127.0.0.1:41000",
                "other.example",
                StatusCode::FORBIDDEN,
            ),
            ("127.0.0.1:41000", "127.0.0.1", StatusCode::OK),
        ] {
            let fixture = Fixture::new();
            let initial = std::fs::read_to_string(&fixture.state.path).unwrap();
            let saved = format!("# management-test-change\n{initial}");
            let app = routes()
                .with_state(fixture.state.clone())
                .layer(axum::Extension(ConnectInfo(
                    peer.parse::<SocketAddr>().unwrap(),
                )));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/config", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

            let reply = client
                .get(&url)
                .header(header::HOST, host)
                .header("X-Forwarded-For", "127.0.0.1")
                .send()
                .await
                .unwrap();
            assert_eq!(reply.status(), expected);
            if expected == StatusCode::OK {
                assert_eq!(reply.headers()[header::CACHE_CONTROL], "no-store");
                assert!(reply.text().await.unwrap().contains("[server]"));
            } else {
                assert!(reply.text().await.unwrap().is_empty());
            }
            let reply = client
                .post(&url)
                .header(header::HOST, host)
                .header(header::ORIGIN, format!("http://{host}"))
                .form(&[("config", &saved)])
                .send()
                .await
                .unwrap();
            assert_eq!(reply.status(), expected);
            let actual = std::fs::read_to_string(&fixture.state.path).unwrap();
            assert_eq!(
                &actual,
                if expected == StatusCode::OK {
                    &saved
                } else {
                    &initial
                }
            );
            server.abort();
        }
    }
}

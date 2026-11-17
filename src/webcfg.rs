/// 浏览器配置页：GET 显示 config.toml 原文，POST 校验后写回（注释原样保留）
use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Form, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
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
<p>保存后立即对新的连接生效；正在通话的设备重连后生效。此页无密码，仅限局域网。</p>
{{MESSAGE}}
<form method="post" action="/config">
<textarea name="config" spellcheck="false" wrap="off">{{TOML}}</textarea>
<button type="submit">保存</button>
</form></body></html>
"#;

pub async fn page(State(state): State<Arc<ConfigStore>>) -> Response {
    let text = std::fs::read_to_string(&state.path).unwrap_or_default();
    render(StatusCode::OK, &text, "")
}

pub async fn save(
    State(state): State<Arc<ConfigStore>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    if !same_origin(&headers) {
        warn!("拒绝跨站 POST /config");
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
    info!("配置已通过网页更新：{}", state.path);
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
            let text = include_str!("../config.toml");
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
    async fn save_toml_preserves_comments_and_updates_new_connections() {
        let fixture = Fixture::new();
        let old = fixture.state.current();
        let text = format!(
            "# 网页保存保留注释\n{}",
            include_str!("../config.toml").replace("ws_port = 8000", "ws_port = 9000")
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
        assert_eq!(fixture.state.current().server.ws_port, 9000);
        assert_eq!(old.server.ws_port, 8000);
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
            include_str!("../config.toml")
        );
        assert!(Arc::ptr_eq(&old, &fixture.state.current()));
    }
}

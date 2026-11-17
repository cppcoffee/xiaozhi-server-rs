use anyhow::Result;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::debug;

use crate::config::LlmCfg;

/// 调用火山方舟 Ark（OpenAI 兼容）流式接口，按 delta 顺序吐出文本片段
pub async fn chat_stream(
    cfg: &LlmCfg,
    messages: Vec<Value>,
    tx: mpsc::Sender<String>,
) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/chat/completions", cfg.base_url.trim_end_matches('/'));
    let body = json!({
        "model": cfg.model,
        "messages": messages,
        "stream": true,
        "temperature": 0.7,
        "max_tokens": 500,
    });

    let resp = client
        .post(&url)
        .bearer_auth(&cfg.api_key)
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let code = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Ark LLM 请求失败 {}: {}", code, text);
    }

    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
        // 按行切分 SSE
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buf.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim();
            if line.is_empty() || !line.starts_with("data:") {
                continue;
            }
            let data = line[5..].trim();
            if data == "[DONE]" {
                return Ok(());
            }
            match serde_json::from_str::<Value>(data) {
                Ok(v) => {
                    if let Some(delta) = v["choices"].get(0) {
                        if let Some(text) = delta["delta"]["content"].as_str() {
                            if !text.is_empty() && tx.send(text.to_string()).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(e) => debug!("SSE 解析跳过: {e} — {data}"),
            }
        }
    }
    Ok(())
}

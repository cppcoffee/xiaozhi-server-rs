use anyhow::Result;
use serde_json::json;

use crate::config::Config;
use crate::tts::DoubaoTts;

/// `--check`：只验证三组云端凭证是否可用，不启动服务
pub async fn run(cfg: &Config) -> bool {
    println!("=== 小智 Rust Server 凭证自检 ===\n");
    let mut ok = true;

    // 1) Ark LLM
    match check_llm(cfg).await {
        Ok(msg) => println!("[ OK ] LLM(Ark)      {msg}"),
        Err(e) => {
            ok = false;
            println!("[FAIL] LLM(Ark)      {e}");
        }
    }

    // 2) TTS
    match check_tts(cfg).await {
        Ok(msg) => println!("[ OK ] TTS(豆包)     {msg}"),
        Err(e) => {
            ok = false;
            println!("[FAIL] TTS(豆包)     {e}");
        }
    }

    // 3) ASR
    match crate::asr::probe(&cfg.asr, crate::config::INPUT_SAMPLE_RATE).await {
        Ok(msg) => println!("[ OK ] ASR(豆包)     {msg}"),
        Err(e) => {
            ok = false;
            println!("[FAIL] ASR(豆包)     {e}");
        }
    }

    println!();
    if ok {
        println!("全部通过，可以启动服务：{} config.toml", env!("CARGO_PKG_NAME"));
    } else {
        println!("有失败项，按上面提示修正 config.toml 后重试");
    }
    ok
}

async fn check_llm(cfg: &Config) -> Result<String> {
    if cfg.llm.api_key.contains("你的") {
        anyhow::bail!("llm.api_key 还是占位符");
    }
    let url = format!("{}/chat/completions", cfg.llm.base_url.trim_end_matches('/'));
    let resp = reqwest::Client::new()
        .post(&url)
        .bearer_auth(&cfg.llm.api_key)
        .json(&json!({
            "model": cfg.llm.model,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 1,
            "stream": false
        }))
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(json!({}));
    if status.is_success() {
        Ok(format!("model={} 调用成功", cfg.llm.model))
    } else if status.as_u16() == 404 {
        anyhow::bail!(
            "model={} 不存在。若使用推理接入点，把 llm.model 改成 ep-xxxxxxxx；原始响应: {}",
            cfg.llm.model,
            body
        )
    } else {
        anyhow::bail!("HTTP {} — {}", status, body)
    }
}

async fn check_tts(cfg: &Config) -> Result<String> {
    if cfg.tts.appid.contains("你的") || cfg.tts.access_token.contains("你的") {
        anyhow::bail!("tts.appid / tts.access_token 还是占位符");
    }
    let tts = DoubaoTts::new(cfg.tts.clone());
    let pcm = tts.synth("你好，我是小智").await?;
    if pcm.is_empty() {
        anyhow::bail!("返回音频为空，检查音色是否已购买");
    }
    // 24k 单声道 16bit
    let ms = pcm.len() as f64 / (24000.0 * 2.0) * 1000.0;
    Ok(format!(
        "voice={} 合成 {} 字节（约 {:.0}ms 音频）",
        cfg.tts.voice,
        pcm.len(),
        ms
    ))
}

use anyhow::Result;
use serde_json::json;

use crate::config::Config;
use crate::logging::ascii;
use crate::tts::DoubaoTts;

/// `--check`: verify the three cloud endpoints without starting the device server.
pub async fn run(cfg: &Config) -> bool {
    println!("=== Xiaozhi Rust server credential check ===\n");
    let llm_ok = report("LLM (Ark)", check_llm(cfg).await);
    let tts_ok = report("TTS (Doubao)", check_tts(cfg).await);
    let asr_ok = report(
        "ASR (Doubao)",
        crate::asr::probe(&cfg.asr, crate::config::INPUT_SAMPLE_RATE).await,
    );
    let ok = llm_ok && tts_ok && asr_ok;
    if ok {
        println!("\nAll checks passed.");
    } else {
        println!("\nOne or more checks failed. Update the configuration and try again.");
    }
    ok
}

fn report(service: &str, result: Result<String>) -> bool {
    match result {
        Ok(message) => {
            println!("[ OK ] {service:<12} {}", ascii(&message));
            true
        }
        Err(error) => {
            println!("[FAIL] {service:<12} {}", ascii(&format!("{error:#}")));
            false
        }
    }
}

async fn check_llm(cfg: &Config) -> Result<String> {
    if cfg.llm.api_key.contains("你的") {
        anyhow::bail!("llm.api_key is a placeholder");
    }
    let url = format!(
        "{}/chat/completions",
        cfg.llm.base_url.trim_end_matches('/')
    );
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
        Ok(format!("Model request succeeded (model={})", cfg.llm.model))
    } else if status.as_u16() == 404 {
        anyhow::bail!(
            "Model {} was not found. For an inference endpoint, set llm.model to ep-xxxxxxxx. Response: {}",
            cfg.llm.model,
            body
        )
    } else {
        anyhow::bail!("HTTP {}: {}", status, body)
    }
}

async fn check_tts(cfg: &Config) -> Result<String> {
    let tts = DoubaoTts::new(cfg.tts.clone());
    let pcm = tts.synth("你好，我是小智").await?;
    if pcm.is_empty() {
        anyhow::bail!(
            "No audio returned. Check that the project has access to the configured voice."
        );
    }
    // 24k mono 16bit
    let ms = pcm.len() as f64 / (24000.0 * 2.0) * 1000.0;
    Ok(format!(
        "Synthesized {} bytes (about {:.0} ms of audio) with voice={}",
        pcm.len(),
        ms,
        cfg.tts.voice
    ))
}

use std::sync::{Arc, RwLock};

use serde::Deserialize;

/// 与设备固件约定的上行音频：16 kHz 单声道、60 ms 一帧 Opus（见 weather-esp32/main/voice.c）
pub const INPUT_SAMPLE_RATE: u32 = 16_000;
pub const INPUT_FRAME_MS: u32 = 60;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerCfg,
    #[serde(default)]
    pub log: LogCfg,
    #[serde(default)]
    pub vad: VadCfg,
    pub llm: LlmCfg,
    pub asr: AsrCfg,
    pub tts: TtsCfg,
}

fn d_ws_port() -> u16 {
    8000
}
fn d_http_port() -> u16 {
    8003
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerCfg {
    #[serde(default = "d_ws_port", alias = "port")]
    pub ws_port: u16,
    #[serde(default = "d_http_port")]
    pub http_port: u16,
}

impl Default for ServerCfg {
    fn default() -> Self {
        Self {
            ws_port: d_ws_port(),
            http_port: d_http_port(),
        }
    }
}

fn d_level() -> String {
    "info".into()
}
fn d_log_dir() -> String {
    "logs".into()
}
fn d_keep() -> usize {
    2
}
fn d_max_mb() -> u64 {
    50
}

/// 运行日志：等级、目录、保留文件数、单文件上限
#[derive(Debug, Clone, Deserialize)]
pub struct LogCfg {
    #[serde(default = "d_level")]
    pub level: String,
    #[serde(default = "d_log_dir")]
    pub dir: String,
    #[serde(default = "d_keep")]
    pub keep: usize,
    #[serde(default = "d_max_mb")]
    pub max_mb: u64,
}

impl Default for LogCfg {
    fn default() -> Self {
        Self {
            level: d_level(),
            dir: d_log_dir(),
            keep: d_keep(),
            max_mb: d_max_mb(),
        }
    }
}

fn d_rms() -> f64 {
    500.0
}
fn d_start_frames() -> u32 {
    2
}
fn d_end_frames() -> u32 {
    6
}

/// 纯能量门限 VAD：环境噪声大就调高 rms_threshold，说话识别不到就调低
#[derive(Debug, Clone, Deserialize)]
pub struct VadCfg {
    #[serde(default = "d_rms")]
    pub rms_threshold: f64,
    #[serde(default = "d_start_frames")]
    pub start_frames: u32,
    #[serde(default = "d_end_frames")]
    pub end_frames: u32,
}

impl Default for VadCfg {
    fn default() -> Self {
        Self {
            rms_threshold: d_rms(),
            start_frames: d_start_frames(),
            end_frames: d_end_frames(),
        }
    }
}

fn d_llm_url() -> String {
    "https://ark.cn-beijing.volces.com/api/v3".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmCfg {
    #[serde(default = "d_llm_url")]
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    #[serde(default)]
    pub prompt: String,
}

fn d_asr_url() -> String {
    "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async".into()
}

fn d_asr_resource() -> String {
    "volc.seedasr.sauc.duration".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsrCfg {
    /// 留着可指向本地假 ASR 做联调；默认豆包
    #[serde(default = "d_asr_url")]
    pub ws_url: String,
    pub appid: String,
    pub access_token: String,
    /// 并发版换成 volc.seedasr.sauc.concurrent
    #[serde(default = "d_asr_resource")]
    pub resource_id: String,
}

fn d_tts_url() -> String {
    "https://openspeech.bytedance.com/api/v1/tts".into()
}

fn d_cluster() -> String {
    "volcano_tts".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct TtsCfg {
    /// 留着可指向本地假 TTS 做联调；默认豆包
    #[serde(default = "d_tts_url")]
    pub api_url: String,
    pub appid: String,
    pub access_token: String,
    /// 必须是已购买的音色
    pub voice: String,
    #[serde(default = "d_cluster")]
    pub cluster: String,
}

impl Config {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(text)?)
    }

    pub fn load(path: &str) -> anyhow::Result<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
    }

    /// welcome：协议常量 + 设备 hello 里声明的音频参数 + session_id
    pub fn welcome(&self, session_id: &str, hello: &serde_json::Value) -> serde_json::Value {
        let mut w = serde_json::json!({
            "type": "hello",
            "version": 1,
            "transport": "websocket",
            "audio_params": {"format": "opus", "sample_rate": 24000, "channels": 1, "frame_duration": 60}
        });
        if let Some(p) = hello.get("audio_params") {
            w["audio_params"] = p.clone();
        }
        w["session_id"] = serde_json::Value::String(session_id.to_string());
        w
    }
}

/// 探测本机局域网 IP：只在 OTA 请求缺 Host 头时兜底
pub fn local_ip() -> String {
    local_ip_address::local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}

/// 当前配置 + 文件路径；网页保存后热替换，新连接生效
pub struct ConfigStore {
    pub path: String,
    cfg: RwLock<Arc<Config>>,
}

impl ConfigStore {
    pub fn new(path: String, cfg: Config) -> Self {
        Self {
            path,
            cfg: RwLock::new(Arc::new(cfg)),
        }
    }

    pub fn current(&self) -> Arc<Config> {
        self.cfg.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// 先写临时文件再改名，避免把正在用的 config.toml 写坏
    pub fn replace(&self, text: &str, next: Config) -> anyhow::Result<()> {
        let tmp = format!("{}.tmp", self.path);
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, &self.path)?;
        *self.cfg.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(next);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
[llm]
api_key = "test-key"
model = "test-model"
[asr]
appid = "test-asr"
access_token = "test-asr-token"
[tts]
appid = "test-tts"
access_token = "test-tts-token"
voice = "test-voice"
"#;

    #[test]
    fn example_toml_parses() {
        let cfg = Config::parse(include_str!("../config.toml")).unwrap();
        assert_eq!(cfg.server.ws_port, 8000);
        assert_eq!(cfg.server.http_port, 8003);
        assert_eq!(cfg.vad.rms_threshold, 500.0);
        assert!(cfg.llm.prompt.contains("你是一个名叫小智的智能助手"));
        assert!(cfg.llm.prompt.ends_with('\n'));
    }

    #[test]
    fn minimal_toml_uses_defaults() {
        let cfg = Config::parse(MINIMAL).unwrap();
        assert_eq!(cfg.server.ws_port, 8000);
        assert_eq!(cfg.server.http_port, 8003);
        assert_eq!(cfg.log.level, "info");
        assert_eq!(cfg.log.keep, 2);
        assert_eq!(cfg.vad.rms_threshold, 500.0);
        assert_eq!(cfg.vad.start_frames, 2);
        assert_eq!(cfg.vad.end_frames, 6);
        assert_eq!(cfg.llm.base_url, d_llm_url());
        assert!(cfg.llm.prompt.is_empty());
        assert_eq!(cfg.asr.ws_url, d_asr_url());
        assert_eq!(cfg.asr.resource_id, d_asr_resource());
        assert_eq!(cfg.tts.api_url, d_tts_url());
        assert_eq!(cfg.tts.cluster, "volcano_tts");
    }

    #[test]
    fn explicit_server_ports_are_independent() {
        let cfg = Config::parse(&format!(
            "[server]\nws_port = 9000\nhttp_port = 9003\n{MINIMAL}"
        ))
        .unwrap();
        assert_eq!(cfg.server.ws_port, 9000);
        assert_eq!(cfg.server.http_port, 9003);
    }

    #[test]
    fn legacy_port_name_is_still_accepted() {
        let cfg = Config::parse(&format!("[server]\nport = 9000\n{MINIMAL}")).unwrap();
        assert_eq!(cfg.server.ws_port, 9000);
        assert!(
            Config::parse(&format!("[server]\nport = 9000\nws_port = 9001\n{MINIMAL}")).is_err()
        );
    }

    #[test]
    fn invalid_configs_are_rejected() {
        assert!(Config::parse("[llm").is_err());
        assert!(Config::parse("[server]\nport = 8000").is_err());
        assert!(Config::parse("llm:\n  api_key: test").is_err());
        assert!(Config::parse(&format!("[server]\nport = \"invalid\"\n{MINIMAL}")).is_err());
    }
}

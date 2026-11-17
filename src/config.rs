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

fn d_port() -> u16 {
    8000
}
fn d_http_port() -> u16 {
    8003
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerCfg {
    #[serde(default = "d_port")]
    pub port: u16,
    #[serde(default = "d_http_port")]
    pub http_port: u16,
}

impl Default for ServerCfg {
    fn default() -> Self {
        Self {
            port: d_port(),
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
        Ok(serde_yaml::from_str(text)?)
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

    /// 先写临时文件再改名，避免把正在用的 config.yaml 写坏
    pub fn replace(&self, text: &str, next: Config) -> anyhow::Result<()> {
        let tmp = format!("{}.tmp", self.path);
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, &self.path)?;
        *self.cfg.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(next);
        Ok(())
    }
}

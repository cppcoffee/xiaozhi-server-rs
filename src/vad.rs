use crate::config::VadCfg;

#[derive(Debug, PartialEq, Eq)]
pub enum VadEvent {
    None,
    StartSpeech,
    EndSpeech,
}

/// 纯能量门限 VAD：替代本地 Silero 模型，全云端部署时用不到任何本地推理
pub struct EnergyVad {
    threshold: f64,
    start_frames: u32,
    end_frames: u32,
    max_frames: u32,
    voiced: u32,
    silent: u32,
    speaking: bool,
    frames: u32,
}

impl EnergyVad {
    pub fn new(cfg: &VadCfg, frame_ms: u32) -> Self {
        // 最长一句（设备只上传 15s，这里兜住“一直有噪声”的情况）
        let max_frames = MAX_SPEECH_SECS * 1000 / frame_ms.max(1);
        Self {
            threshold: cfg.rms_threshold,
            start_frames: cfg.start_frames.max(1),
            end_frames: cfg.end_frames.max(1),
            max_frames,
            voiced: 0,
            silent: 0,
            speaking: false,
            frames: 0,
        }
    }

    pub fn is_speaking(&self) -> bool {
        self.speaking
    }

    pub fn push(&mut self, pcm: &[i16]) -> VadEvent {
        let rms = rms(pcm);
        let voiced = rms >= self.threshold;

        if !self.speaking {
            if voiced {
                self.voiced += 1;
                if self.voiced >= self.start_frames {
                    self.speaking = true;
                    self.silent = 0;
                    self.frames = 0;
                    return VadEvent::StartSpeech;
                }
            } else {
                self.voiced = 0;
            }
            return VadEvent::None;
        }

        // 说话中
        self.frames += 1;
        if voiced {
            self.silent = 0;
        } else {
            self.silent += 1;
        }
        if self.silent >= self.end_frames || self.frames >= self.max_frames {
            self.speaking = false;
            self.voiced = 0;
            self.silent = 0;
            return VadEvent::EndSpeech;
        }
        VadEvent::None
    }
}

const MAX_SPEECH_SECS: u32 = 20;

pub fn rms(pcm: &[i16]) -> f64 {
    if pcm.is_empty() {
        return 0.0;
    }
    let sum: f64 = pcm.iter().map(|&s| (s as f64) * (s as f64)).sum();
    (sum / pcm.len() as f64).sqrt()
}

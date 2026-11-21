use crate::config::VadCfg;

#[derive(Debug, PartialEq, Eq)]
pub enum VadEvent {
    None,
    StartSpeech,
    /// 说话结束；`true` 表示碰到录音上限而不是自然静音。
    EndSpeech(bool),
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

    #[allow(dead_code)]
    pub fn push(&mut self, pcm: &[i16]) -> VadEvent {
        self.push_with_rms(pcm).0
    }

    pub fn push_with_rms(&mut self, pcm: &[i16]) -> (VadEvent, f64) {
        let level = rms(pcm);
        let voiced = level >= self.threshold;

        if !self.speaking {
            if voiced {
                self.voiced += 1;
                if self.voiced >= self.start_frames {
                    self.speaking = true;
                    self.silent = 0;
                    self.frames = 0;
                    return (VadEvent::StartSpeech, level);
                }
            } else {
                self.voiced = 0;
            }
            return (VadEvent::None, level);
        }

        // 说话中
        self.frames += 1;
        if voiced {
            self.silent = 0;
        } else {
            self.silent += 1;
        }
        if self.silent >= self.end_frames || self.frames >= self.max_frames {
            let capped = self.frames >= self.max_frames;
            self.speaking = false;
            self.voiced = 0;
            self.silent = 0;
            return (VadEvent::EndSpeech(capped), level);
        }
        (VadEvent::None, level)
    }
}

const MAX_SPEECH_SECS: u32 = 20;

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> VadCfg {
        VadCfg {
            rms_threshold: 500.0,
            start_frames: 1,
            end_frames: 1,
        }
    }

    #[test]
    fn silence_and_the_cap_end_differently() {
        let mut vad = EnergyVad::new(&cfg(), 60);
        assert_eq!(vad.push(&[2000; 960]), VadEvent::StartSpeech);
        assert_eq!(vad.push(&[0; 960]), VadEvent::EndSpeech(false));

        let mut vad = EnergyVad::new(&cfg(), 60);
        assert_eq!(vad.push(&[2000; 960]), VadEvent::StartSpeech);
        let mut last = VadEvent::None;
        for _ in 0..MAX_SPEECH_SECS * 1000 / 60 {
            last = vad.push(&[2000; 960]);
            if last != VadEvent::None {
                break;
            }
        }
        assert_eq!(last, VadEvent::EndSpeech(true));
    }
}

pub fn rms(pcm: &[i16]) -> f64 {
    if pcm.is_empty() {
        return 0.0;
    }
    let sum: f64 = pcm.iter().map(|&s| (s as f64) * (s as f64)).sum();
    (sum / pcm.len() as f64).sqrt()
}

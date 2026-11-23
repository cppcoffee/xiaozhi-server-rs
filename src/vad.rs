use crate::config::VadCfg;

const MAX_SPEECH_SECS: u32 = 20;

#[derive(Debug, PartialEq, Eq)]
pub enum VadEvent {
    None,
    StartSpeech,
    /// Speech ended; `true` means the recording cap was hit instead of natural silence.
    EndSpeech(bool),
}

/// Adaptive energy-threshold VAD with noise-floor tracking and start/continue hysteresis
pub struct EnergyVad {
    threshold: f64,
    noise_floor: f64,
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
        // Longest utterance (the device uploads at most 15s; this covers constant noise)
        let max_frames = MAX_SPEECH_SECS * 1000 / frame_ms.max(1);
        Self {
            threshold: cfg.rms_threshold,
            noise_floor: 0.0,
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

    fn start_threshold(&self) -> f64 {
        self.threshold.max(self.noise_floor * 2.2)
    }

    fn continue_threshold(&self) -> f64 {
        (self.start_threshold() * 0.65).max(self.noise_floor * 1.6)
    }

    pub fn is_voiced(&self, level: f64) -> bool {
        let th = if self.speaking {
            self.continue_threshold()
        } else {
            self.start_threshold()
        };
        level >= th
    }

    #[allow(dead_code)]
    pub fn push(&mut self, pcm: &[i16]) -> VadEvent {
        self.push_with_rms(pcm).0
    }

    pub fn push_with_rms(&mut self, pcm: &[i16]) -> (VadEvent, f64) {
        let level = rms(pcm);
        let voiced = self.is_voiced(level);

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
                // Track quiet room floor so steady ambient noise (fan/AC) does not stall endpointing.
                if self.noise_floor == 0.0 {
                    self.noise_floor = level;
                } else {
                    self.noise_floor = self.noise_floor * 0.92 + level * 0.08;
                }
            }
            return (VadEvent::None, level);
        }

        // Speaking
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

    #[test]
    fn adapts_to_ambient_noise_floor_and_keeps_soft_tails() {
        let mut vad = EnergyVad::new(&cfg(), 60);
        for _ in 0..10 {
            assert_eq!(vad.push(&[350; 960]), VadEvent::None);
        }
        // Mild spike above static 500 but below 2.2x 350 floor does not false-trigger.
        assert_eq!(vad.push(&[550; 960]), VadEvent::None);
        assert_eq!(vad.push(&[2000; 960]), VadEvent::StartSpeech);
        // Softer trailing syllable (600 > continue_th ~560) stays voiced.
        assert_eq!(vad.push(&[600; 960]), VadEvent::None);
        // Dropping back to room floor ends speech cleanly.
        assert_eq!(vad.push(&[350; 960]), VadEvent::EndSpeech(false));
    }
}

pub fn rms(pcm: &[i16]) -> f64 {
    if pcm.is_empty() {
        return 0.0;
    }
    let sum: f64 = pcm.iter().map(|&s| (s as f64) * (s as f64)).sum();
    (sum / pcm.len() as f64).sqrt()
}

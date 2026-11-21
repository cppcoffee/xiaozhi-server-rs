use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::asr::{AsrCmd, AsrOutcome};
use crate::audio::OpusIn;
use crate::config::Config;
use crate::vad::{EnergyVad, VadEvent};

use super::Shared;

/// Server-side silence fallback: if the device misses its local endpoint and never sends listen stop,
/// this much consecutive silence after speech ends the turn and triggers recognition instead of waiting out the device's 15s timeout.
const SERVER_SILENCE_FALLBACK_MS: u32 = 1200;

const PRE_ROLL_FRAMES: usize = 3;

pub(super) struct InputState {
    decoder: OpusIn,
    vad: EnergyVad,
    pre_roll: VecDeque<Vec<i16>>,
    manual_mode: bool,
    pub listening: bool,
    speech_started: bool,
    post_speech_silent_frames: u32,
    pub input: Option<CancellationToken>,
    received_frames: usize,
    peak_rms: f64,
}

impl InputState {
    pub fn new(cfg: &Config) -> anyhow::Result<Self> {
        Ok(Self {
            decoder: OpusIn::new(crate::config::INPUT_SAMPLE_RATE)?,
            vad: EnergyVad::new(&cfg.vad, crate::config::INPUT_FRAME_MS),
            pre_roll: VecDeque::with_capacity(PRE_ROLL_FRAMES),
            manual_mode: false,
            listening: false,
            speech_started: false,
            post_speech_silent_frames: 0,
            input: None,
            received_frames: 0,
            peak_rms: 0.0,
        })
    }

    fn stop_listening(&mut self) {
        self.listening = false;
        self.speech_started = false;
        self.post_speech_silent_frames = 0;
    }

    fn finish_audio(
        &mut self,
        pcm: Vec<i16>,
        asr_tx: &mpsc::Sender<AsrCmd>,
        closed: &CancellationToken,
    ) {
        self.stop_listening();
        if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() || asr_tx.try_send(AsrCmd::Flush).is_err() {
            closed.cancel();
        }
    }

    pub async fn handle_text(
        &mut self,
        raw: &str,
        shared: &Shared,
        negotiated_sample_rate: &AtomicU32,
        asr_tx: &mpsc::Sender<AsrCmd>,
        manual_tx: &mpsc::Sender<AsrOutcome>,
    ) {
        let Ok(v) = serde_json::from_str::<Value>(raw) else {
            return;
        };
        debug!("<- {raw}");
        match v["type"].as_str().unwrap_or("") {
            "hello" => {
                if let Some(sr) = v["audio_params"]["sample_rate"].as_u64() {
                    negotiated_sample_rate.store(sr as u32, Ordering::Relaxed);
                }
                let welcome = shared.cfg.welcome(&shared.session_id, &v);
                shared.send_json(welcome).await;
                if v["features"]["mcp"] == true {
                    shared.mcp.start();
                }
            }
            "listen" => {
                self.handle_listen(&v, shared, asr_tx, manual_tx).await;
            }
            "abort" => {
                info!("Received abort. Cancelling the current turn.");
                shared.abort_turn(self.input.take()).await;
                self.stop_listening();
            }
            "goodbye" => {
                debug!("Device sent goodbye");
                self.stop_listening();
            }
            "ping" => {
                shared.send_json(crate::protocol::pong()).await;
            }
            "mcp" => shared.mcp.receive(&v),
            "iot" => debug!("Ignoring unsupported message type: {}", v["type"]),
            other => debug!("Unknown message type: {other}"),
        }
    }

    async fn handle_listen(
        &mut self,
        v: &Value,
        shared: &Shared,
        asr_tx: &mpsc::Sender<AsrCmd>,
        manual_tx: &mpsc::Sender<AsrOutcome>,
    ) {
        let state = v["state"].as_str().unwrap_or("");
        if let Some(mode) = v["mode"].as_str() {
            self.manual_mode = mode == "manual";
        }
        match state {
            "start" => {
                shared.abort_turn(self.input.take()).await;
                let turn = CancellationToken::new();
                if asr_tx.try_send(AsrCmd::Start(turn.clone())).is_err() {
                    shared.closed.cancel();
                }
                self.input = Some(turn);
                self.vad = EnergyVad::new(&shared.cfg.vad, crate::config::INPUT_FRAME_MS);
                self.pre_roll.clear();
                self.received_frames = 0;
                self.peak_rms = 0.0;
                self.speech_started = false;
                self.post_speech_silent_frames = 0;
                self.listening = true;
            }
            "stop" if self.listening => {
                debug!(
                    target: "xiaozhi_input",
                    device_id = shared.device_id,
                    received_frames = self.received_frames,
                    peak_rms = self.peak_rms,
                    threshold = shared.cfg.vad.rms_threshold,
                    "listen input"
                );
                self.stop_listening();
                if asr_tx.try_send(AsrCmd::Flush).is_err() {
                    shared.closed.cancel();
                }
            }
            "detect" if self.manual_mode => {
                if let Some(text) = v["text"].as_str().filter(|s| !s.trim().is_empty()) {
                    shared.abort_turn(self.input.take()).await;
                    self.stop_listening();
                    let turn = CancellationToken::new();
                    self.input = Some(turn.clone());
                    if manual_tx
                        .try_send(AsrOutcome {
                            turn,
                            text: text.to_owned(),
                        })
                        .is_err()
                    {
                        warn!("Manual input queue is full. Closing the connection.");
                        shared.closed.cancel();
                    }
                }
            }
            _ => {}
        }
    }

    pub fn handle_audio(&mut self, bytes: &[u8], shared: &Shared, asr_tx: &mpsc::Sender<AsrCmd>) {
        let pcm = match self.decoder.decode(bytes) {
            Ok(p) => p.to_vec(),
            Err(e) => {
                debug!("Failed to decode Opus packet: {e}");
                return;
            }
        };
        self.received_frames += 1;
        let (event, level) = self.vad.push_with_rms(&pcm);
        self.peak_rms = self.peak_rms.max(level);
        let fallback_frames = (SERVER_SILENCE_FALLBACK_MS / crate::config::INPUT_FRAME_MS)
            .max(shared.cfg.vad.end_frames.saturating_mul(2));

        match event {
            VadEvent::StartSpeech => {
                self.post_speech_silent_frames = 0;
                if !self.speech_started {
                    info!("Speech detected");
                    self.speech_started = true;
                    if self
                        .pre_roll
                        .drain(..)
                        .chain(std::iter::once(pcm))
                        .any(|frame| asr_tx.try_send(AsrCmd::Pcm(frame)).is_err())
                    {
                        shared.closed.cancel();
                    }
                } else if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() {
                    shared.closed.cancel();
                }
            }
            VadEvent::None if self.speech_started || self.vad.is_speaking() => {
                if level >= shared.cfg.vad.rms_threshold {
                    self.post_speech_silent_frames = 0;
                } else {
                    self.post_speech_silent_frames += 1;
                }
                if !self.manual_mode && self.post_speech_silent_frames >= fallback_frames {
                    info!(
                        "Ending input after {} ms of post-speech silence",
                        self.post_speech_silent_frames * crate::config::INPUT_FRAME_MS
                    );
                    self.finish_audio(pcm, asr_tx, &shared.closed);
                    return;
                }
                if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() {
                    shared.closed.cancel();
                }
            }
            VadEvent::None => {
                if self.pre_roll.len() == PRE_ROLL_FRAMES {
                    self.pre_roll.pop_front();
                }
                self.pre_roll.push_back(pcm);
            }
            VadEvent::EndSpeech(capped) => {
                // On a short pause prefer the device's local listen stop while still forwarding the tail to ASR;
                // past the server silence fallback (1.2s default) or the 20s recording cap, the server ends the turn itself.
                if !capped {
                    self.post_speech_silent_frames += 1;
                    if !self.manual_mode && self.post_speech_silent_frames >= fallback_frames {
                        info!(
                            "Ending input after {} ms of post-speech silence",
                            self.post_speech_silent_frames * crate::config::INPUT_FRAME_MS
                        );
                        self.finish_audio(pcm, asr_tx, &shared.closed);
                        return;
                    }
                    debug!("Pause detected. Forwarding audio while waiting for the device to stop listening.");
                    if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() {
                        shared.closed.cancel();
                    }
                    return;
                }
                info!("Ending input at the recording duration limit");
                self.finish_audio(pcm, asr_tx, &shared.closed);
            }
        }
    }
}

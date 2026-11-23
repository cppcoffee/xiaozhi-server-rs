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

/// Server-side silence fallback: ends a turn after natural end-of-speech pause (also drives followup & realtime mode).
const SERVER_SILENCE_FALLBACK_MS: u32 = 780;

const PRE_ROLL_FRAMES: usize = 3;

pub(super) struct InputState {
    decoder: OpusIn,
    vad: EnergyVad,
    pre_roll: VecDeque<Vec<i16>>,
    manual_mode: bool,
    realtime_mode: bool,
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
            realtime_mode: false,
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
        shared: &Shared,
        asr_tx: &mpsc::Sender<AsrCmd>,
    ) {
        if self.realtime_mode {
            // Keep uplink open across speaking/next-turn in realtime mode (AEC barge-in & multi-turn).
            self.speech_started = false;
            self.post_speech_silent_frames = 0;
            self.pre_roll.clear();
            self.vad = EnergyVad::new(&shared.cfg.vad, crate::config::INPUT_FRAME_MS);
            self.input = None;
        } else {
            self.stop_listening();
        }
        if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() || asr_tx.try_send(AsrCmd::Flush).is_err() {
            shared.closed.cancel();
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
            self.realtime_mode = mode == "realtime";
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

    pub async fn handle_audio(
        &mut self,
        bytes: &[u8],
        shared: &Shared,
        asr_tx: &mpsc::Sender<AsrCmd>,
    ) {
        let pcm = match self.decoder.decode(bytes) {
            Ok(p) => p.to_vec(),
            Err(e) => {
                debug!("Failed to decode Opus packet: {e}");
                return;
            }
        };
        self.handle_pcm(pcm, shared, asr_tx).await;
    }

    async fn handle_pcm(&mut self, pcm: Vec<i16>, shared: &Shared, asr_tx: &mpsc::Sender<AsrCmd>) {
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
                    if self.realtime_mode && self.input.is_none() {
                        shared.abort_turn(None).await;
                        let turn = CancellationToken::new();
                        if asr_tx.try_send(AsrCmd::Start(turn.clone())).is_err() {
                            shared.closed.cancel();
                            return;
                        }
                        self.input = Some(turn);
                    }
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
                if self.vad.is_voiced(level) {
                    self.post_speech_silent_frames = 0;
                } else {
                    self.post_speech_silent_frames += 1;
                }
                if !self.manual_mode && self.post_speech_silent_frames >= fallback_frames {
                    info!(
                        "Ending input after {} ms of post-speech silence",
                        self.post_speech_silent_frames * crate::config::INPUT_FRAME_MS
                    );
                    self.finish_audio(pcm, shared, asr_tx);
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
                // past the server silence fallback (780ms default) or the 20s recording cap, the server ends the turn itself.
                if !capped {
                    self.post_speech_silent_frames += 1;
                    if !self.manual_mode && self.post_speech_silent_frames >= fallback_frames {
                        info!(
                            "Ending input after {} ms of post-speech silence",
                            self.post_speech_silent_frames * crate::config::INPUT_FRAME_MS
                        );
                        self.finish_audio(pcm, shared, asr_tx);
                        return;
                    }
                    debug!("Pause detected. Forwarding audio while waiting for the device to stop listening.");
                    if asr_tx.try_send(AsrCmd::Pcm(pcm)).is_err() {
                        shared.closed.cancel();
                    }
                    return;
                }
                info!("Ending input at the recording duration limit");
                self.finish_audio(pcm, shared, asr_tx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn test_shared(cfg: Arc<Config>) -> (Shared, mpsc::Receiver<axum::extract::ws::Message>) {
        let (ws_tx, ws_rx) = mpsc::channel(16);
        let (audio_tx, _audio_rx) = mpsc::channel(4);
        let closed = CancellationToken::new();
        let mcp = crate::mcp::DeviceMcp::new("sid".into(), ws_tx.clone(), closed.clone());
        (
            Shared {
                cfg,
                mcp,
                closed,
                ws_tx,
                audio_tx,
                session_id: "sid".into(),
                device_id: "dev".into(),
                history: Mutex::new(Vec::new()),
                turn_cancel: Mutex::new(None),
            },
            ws_rx,
        )
    }

    #[tokio::test]
    async fn realtime_mode_stays_listening_and_aborts_active_turn_on_barge_in() {
        let cfg = Arc::new(Config::parse(include_str!("../../config.example.toml")).unwrap());
        let (shared, mut ws_rx) = test_shared(cfg.clone());
        let (asr_tx, mut asr_rx) = mpsc::channel(64);
        let (manual_tx, _manual_rx) = mpsc::channel(4);
        let mut input = InputState::new(&cfg).unwrap();
        let sr = AtomicU32::new(16_000);

        input
            .handle_text(
                r#"{"type":"listen","state":"start","mode":"realtime"}"#,
                &shared,
                &sr,
                &asr_tx,
                &manual_tx,
            )
            .await;
        assert!(matches!(asr_rx.try_recv(), Ok(AsrCmd::Start(_))));

        // Turn 1 speech + silence fallback ends input while keeping listening=true.
        for _ in 0..3 {
            input.handle_pcm(vec![2000; 960], &shared, &asr_tx).await;
        }
        for _ in 0..14 {
            input.handle_pcm(vec![0; 960], &shared, &asr_tx).await;
        }
        let mut saw_flush = false;
        while let Ok(cmd) = asr_rx.try_recv() {
            if matches!(cmd, AsrCmd::Flush) {
                saw_flush = true;
            }
        }
        assert!(saw_flush);
        assert!(input.listening);

        // Simulate active TTS playback, then user barges in without sending listen:start.
        let speaking_turn = CancellationToken::new();
        *shared.turn_cancel.lock().await = Some(speaking_turn.clone());
        for _ in 0..2 {
            input.handle_pcm(vec![2000; 960], &shared, &asr_tx).await;
        }
        assert!(speaking_turn.is_cancelled());
        assert!(matches!(asr_rx.try_recv(), Ok(AsrCmd::Start(_))));
        assert!(ws_rx.try_recv().is_ok()); // tts:stop sent on abort
    }
}

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Result};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::audio::OpusOut;
use crate::tts::DoubaoTts;

use super::turn::TurnBudget;
use super::Shared;

/// Playback start pre-buffer frames: send the first 3 frames (180ms) back-to-back to cushion ESP32 Wi-Fi jitter
const PRE_BUFFER_FRAMES: usize = 3;
pub(super) const OUTPUT_FRAME_MS: u32 = 60;
const OUTPUT_FRAME_INTERVAL: Duration = Duration::from_millis(OUTPUT_FRAME_MS as u64);

pub(super) enum PacerItem {
    SentenceStart(String),
    Audio(Vec<u8>),
}

pub(super) async fn synthesize_sentence_to_pacer(
    tts: &DoubaoTts,
    encoder: &mut OpusOut,
    text: &str,
    sample_rate: u32,
    pacer_tx: &mpsc::Sender<PacerItem>,
    emit_sentence_start: bool,
) -> Result<()> {
    let synth_started = Instant::now();
    let (pcm_tx, mut pcm_rx) = mpsc::channel::<Vec<u8>>(16);
    let synth = tts.synth_stream(text, sample_rate, pcm_tx);
    let encode = async {
        let mut emitted_start = !emit_sentence_start;
        let mut produced_frames = 0usize;
        while let Some(pcm_chunk) = pcm_rx.recv().await {
            for frame in encoder.push_pcm_bytes(&pcm_chunk) {
                if !emitted_start {
                    emitted_start = true;
                    pacer_tx
                        .send(PacerItem::SentenceStart(text.to_owned()))
                        .await
                        .map_err(|_| anyhow!("audio pacer closed"))?;
                }
                produced_frames += 1;
                pacer_tx
                    .send(PacerItem::Audio(frame))
                    .await
                    .map_err(|_| anyhow!("audio pacer closed"))?;
            }
        }
        if let Some(tail) = encoder.finish_stream() {
            if !emitted_start {
                pacer_tx
                    .send(PacerItem::SentenceStart(text.to_owned()))
                    .await
                    .map_err(|_| anyhow!("audio pacer closed"))?;
            }
            produced_frames += 1;
            pacer_tx
                .send(PacerItem::Audio(tail))
                .await
                .map_err(|_| anyhow!("audio pacer closed"))?;
        }
        Ok::<_, anyhow::Error>(produced_frames)
    };
    let ((), produced_frames) = tokio::try_join!(synth, encode)?;
    debug!(
        elapsed_ms = synth_started.elapsed().as_millis(),
        frames = produced_frames,
        "TTS sentence synthesized"
    );
    ensure!(produced_frames > 0, "TTS produced no Opus frames");
    Ok(())
}

/// Pacer on an independent monotonic clock: the first 2 frames go out back-to-back, then a 60ms steady rate, and after an idle queue it re-anchors to avoid a burst.
pub(super) async fn run_audio_pacer(
    shared: &Arc<Shared>,
    mut pacer_rx: mpsc::Receiver<PacerItem>,
    token: &CancellationToken,
    budget: &TurnBudget,
) -> Result<()> {
    let mut sent_frames: usize = 0;
    let mut next_deadline: Option<Instant> = None;
    while let Some(item) = pacer_rx.recv().await {
        if token.is_cancelled() {
            bail!("turn cancelled");
        }
        match item {
            PacerItem::SentenceStart(text) => {
                shared.send_tts_state("sentence_start", Some(&text)).await;
            }
            PacerItem::Audio(frame) => {
                if sent_frames >= PRE_BUFFER_FRAMES {
                    let now = Instant::now();
                    let deadline = next_deadline
                        .filter(|d| now <= *d + OUTPUT_FRAME_INTERVAL)
                        .unwrap_or(now);
                    if deadline > now {
                        tokio::time::sleep_until(deadline).await;
                    }
                    next_deadline = Some(deadline + OUTPUT_FRAME_INTERVAL);
                }
                shared.send_audio(frame, token).await?;
                sent_frames += 1;
                if sent_frames == PRE_BUFFER_FRAMES {
                    next_deadline = Some(Instant::now() + OUTPUT_FRAME_INTERVAL);
                }
                if !budget.first_audio.swap(true, Ordering::SeqCst) {
                    debug!(
                        elapsed_ms = budget.started.elapsed().as_millis(),
                        "text turn to first audio"
                    );
                }
            }
        }
    }
    if sent_frames > 0 {
        if token.is_cancelled() {
            bail!("turn cancelled");
        }
        // One frame tail margin before tts:stop so the device drains buffered frames cleanly.
        tokio::time::sleep(OUTPUT_FRAME_INTERVAL).await;
    }
    Ok(())
}

/// Single-sentence fallback playback for failures/timeouts (reuses the same streaming encode and pacer path)
pub(super) async fn speak_single_sentence(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    text: &str,
    token: &CancellationToken,
    budget: &TurnBudget,
    output_sample_rate: u32,
) -> Result<()> {
    let (pacer_tx, pacer_rx) = mpsc::channel::<PacerItem>(32);
    let synth_fut = async move {
        let mut encoder = OpusOut::new(output_sample_rate, OUTPUT_FRAME_MS)?;
        synthesize_sentence_to_pacer(
            tts,
            &mut encoder,
            text,
            output_sample_rate,
            &pacer_tx,
            false,
        )
        .await
    };
    let pacer_fut = run_audio_pacer(shared, pacer_rx, token, budget);
    let ((), ()) = tokio::try_join!(synth_fut, pacer_fut)?;
    Ok(())
}

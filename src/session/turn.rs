use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::audio::OpusOut;
use crate::llm::chat_stream_sentences;
use crate::text::clean_for_tts;
use crate::tts::DoubaoTts;

use super::audio::{
    run_audio_pacer, speak_single_sentence, synthesize_sentence_to_pacer, PacerItem,
    OUTPUT_FRAME_MS,
};
use super::Shared;

/// How many recent conversation rounds to keep
const HISTORY_ROUNDS: usize = 6;

/// Oversized old tool results are omitted from later rounds to avoid pointless prefill time
const COMPACT_TOOL_HISTORY_BYTES: usize = 512;

// One absolute clock per accepted text turn; progress never renews these limits.
pub(crate) const FIRST_AUDIO_BUDGET: Duration = Duration::from_secs(75);
pub(crate) const FAILURE_RESERVE: Duration = Duration::from_secs(10);
pub(crate) const TOTAL_TURN_BUDGET: Duration = Duration::from_secs(180);
const TIMEOUT_REPLY: &str = "回复超时，请稍后再试。";

pub(super) struct TurnBudget {
    pub started: Instant,
    first_deadline: Instant,
    total_deadline: Instant,
    pub first_audio: AtomicBool,
}

impl TurnBudget {
    fn new() -> Self {
        let started = Instant::now();
        Self {
            started,
            first_deadline: started + FIRST_AUDIO_BUDGET,
            total_deadline: started + TOTAL_TURN_BUDGET,
            first_audio: AtomicBool::new(false),
        }
    }

    async fn normal_expired(&self) {
        tokio::time::sleep_until(self.first_deadline - FAILURE_RESERVE).await;
        if self.first_audio.load(Ordering::SeqCst) {
            tokio::time::sleep_until(self.total_deadline).await;
        }
    }

    fn recovery_deadline(&self) -> Instant {
        if self.first_audio.load(Ordering::SeqCst) {
            self.total_deadline
        } else {
            self.first_deadline
        }
    }
}

#[allow(dead_code)]
pub(crate) async fn run_turn(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: CancellationToken,
) -> Result<()> {
    let output_sr = tts.preferred_sample_rate(24_000);
    run_turn_with_rate(shared, tts, user_text, token, output_sr).await
}

pub(crate) async fn run_turn_with_rate(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: CancellationToken,
    output_sample_rate: u32,
) -> Result<()> {
    let user_text = user_text.trim().to_string();
    if user_text.is_empty() {
        return Ok(());
    }
    {
        let mut cur = shared.turn_cancel.lock().await;
        if token.is_cancelled() || shared.closed.is_cancelled() {
            return Ok(());
        }
        if let Some(old) = cur.take() {
            old.cancel();
        }
        *cur = Some(token.clone());
    }
    let budget = TurnBudget::new();
    let normal_cancel = token.child_token();
    let mut timed_out = false;
    let result = tokio::select! {
        biased;
        _ = token.cancelled() => Ok(None),
        _ = shared.closed.cancelled() => Ok(None),
        _ = budget.normal_expired() => {
            timed_out = true;
            Err(anyhow::anyhow!("turn deadline exceeded"))
        },
        result = run_turn_inner(shared, tts, user_text, &normal_cancel, &budget, output_sample_rate) => result.map(Some),
    };
    // Dropping the inner future aborts HTTP and rmcp's request guard cancels RPCs.
    // Keep recovery on the parent token so abort/disconnect still interrupts it.
    normal_cancel.cancel();
    if !token.is_cancelled() && !shared.closed.is_cancelled() {
        if let Err(e) = &result {
            error!("Conversation failed: {e:#}");
            let text = if timed_out {
                TIMEOUT_REPLY
            } else {
                "暂时无法获取可靠回复，请稍后再试。"
            };
            shared.send_tts_state("sentence_start", Some(text)).await;
            let deadline = budget.recovery_deadline();
            if Instant::now() < deadline {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {},
                    _ = shared.closed.cancelled() => {},
                    _ = tokio::time::timeout_at(
                        deadline,
                        speak_single_sentence(shared, tts, text, &token, &budget, output_sample_rate),
                    ) => {},
                }
            }
        }
    }
    let mut current = shared.turn_cancel.lock().await;
    if !token.is_cancelled() && !shared.closed.is_cancelled() {
        if let Ok(Some(transcript)) = &result {
            let mut hist = shared.history.lock().await;
            hist.extend(transcript.iter().cloned());
            trim_history(&mut hist);
        }
        // A device may send the next listen start as soon as it sees this stop.
        *current = None;
        shared.send_tts_state("stop", None).await;
    }
    token.cancel();
    result.map(|_| ())
}

fn trim_history(hist: &mut Vec<Value>) {
    let starts: Vec<usize> = hist
        .iter()
        .enumerate()
        .filter(|(_, m)| m["role"] == "user")
        .map(|(i, _)| i)
        .collect();
    if starts.len() > HISTORY_ROUNDS {
        hist.drain(..starts[starts.len() - HISTORY_ROUNDS]);
    }
}

fn compact_history_message(msg: &Value) -> Value {
    if msg["role"].as_str() == Some("tool") {
        if let Some(content) = msg["content"].as_str() {
            if content.len() > COMPACT_TOOL_HISTORY_BYTES {
                let mut compact = msg.clone();
                compact["content"] = json!(
                    "{\"note\":\"prior turn tool output omitted; call tool again for current readings\"}"
                );
                return compact;
            }
        }
    }
    msg.clone()
}

async fn run_turn_inner(
    shared: &Arc<Shared>,
    tts: &DoubaoTts,
    user_text: String,
    token: &CancellationToken,
    budget: &TurnBudget,
    output_sample_rate: u32,
) -> Result<Vec<Value>> {
    info!("[{}] User: {user_text}", shared.device_id);
    shared
        .send_json(crate::protocol::stt_text(&user_text, &shared.session_id))
        .await;
    shared.send_tts_state("start", None).await;

    let (sentence_tx, mut sentence_rx) = mpsc::channel::<String>(16);
    // 64 frames ≈ 3.84s prefetch buffer: sentence N+1 is synthesized while sentence N plays, removing gaps
    let (pacer_tx, pacer_rx) = mpsc::channel::<PacerItem>(64);

    let question = user_text.as_str();
    let reply_fut = async move {
        if let Some(answer) = shared.mcp.direct_answer(question, token).await {
            let mut buffer = answer.clone();
            for sentence in crate::text::split_sentences(&mut buffer, true) {
                sentence_tx
                    .send(sentence)
                    .await
                    .context("TTS queue closed")?;
            }
            return Ok(vec![json!({"role":"assistant", "content":answer})]);
        }

        // Large old tool results in history keep only a hint, reducing model prefill latency.
        let mut messages = Vec::new();
        if !shared.cfg.llm.prompt.trim().is_empty() {
            messages.push(json!({"role": "system", "content": shared.cfg.llm.prompt}));
        }
        {
            let hist = shared.history.lock().await;
            messages.extend(hist.iter().map(compact_history_message));
        }
        messages.push(json!({"role": "user", "content": question}));
        let model_started = Instant::now();
        let (_, transcript) = chat_stream_sentences(
            &shared.cfg.llm,
            messages,
            shared.mcp.clone(),
            token.clone(),
            Some(sentence_tx),
        )
        .await?;
        debug!(
            elapsed_ms = model_started.elapsed().as_millis(),
            "LLM and tools complete"
        );
        Ok::<_, anyhow::Error>(transcript)
    };

    let tts_fut = async move {
        let mut encoder = OpusOut::new(output_sample_rate, OUTPUT_FRAME_MS)?;
        while let Some(raw) = sentence_rx.recv().await {
            if token.is_cancelled() {
                bail!("turn cancelled");
            }
            let text = clean_for_tts(&raw);
            if text.is_empty() {
                continue;
            }
            synthesize_sentence_to_pacer(
                tts,
                &mut encoder,
                &text,
                output_sample_rate,
                &pacer_tx,
                true,
            )
            .await?;
        }
        Ok::<_, anyhow::Error>(())
    };

    let pacer_fut = run_audio_pacer(shared, pacer_rx, token, budget);

    let (transcript, (), ()) = tokio::try_join!(reply_fut, tts_fut, pacer_fut)?;
    let mut history = vec![json!({"role":"user", "content":user_text})];
    history.extend(transcript);
    Ok(history)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_trims_whole_turns_not_individual_tool_messages() {
        let mut history = Vec::new();
        for i in 0..8 {
            history.extend([
                json!({"role":"user", "content":i.to_string()}),
                json!({"role":"assistant", "content":null, "tool_calls":[{"id":i.to_string()}]}),
                json!({"role":"tool", "tool_call_id":i.to_string(), "content":"{}"}),
                json!({"role":"assistant", "content":"answer"}),
            ]);
        }
        trim_history(&mut history);
        assert_eq!(history.len(), HISTORY_ROUNDS * 4);
        assert_eq!(history[0]["role"], "user");
        assert_eq!(history[0]["content"], "2");
        assert_eq!(
            history[1]["tool_calls"][0]["id"],
            history[2]["tool_call_id"]
        );
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn first_audio_disarms_only_the_initial_limit_not_the_total_limit() {
        let budget = Arc::new(TurnBudget::new());
        budget.first_audio.store(true, Ordering::SeqCst);
        let timer = {
            let budget = budget.clone();
            tokio::spawn(async move { budget.normal_expired().await })
        };
        tokio::time::advance(FIRST_AUDIO_BUDGET).await;
        tokio::task::yield_now().await;
        assert!(!timer.is_finished());
        tokio::time::advance(TOTAL_TURN_BUDGET - FIRST_AUDIO_BUDGET).await;
        timer.await.unwrap();
        assert_eq!(Instant::now(), budget.total_deadline);
    }
}

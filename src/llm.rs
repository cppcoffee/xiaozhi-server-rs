use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::config::LlmCfg;
use crate::mcp::{DeviceMcp, ToolTurn};

pub const MAX_TOOL_ROUNDS: usize = 4;
const MAX_CALLS: usize = 3;
const MAX_TEXT: usize = 64 * 1024;

#[derive(Default)]
struct ToolCall {
    id: String,
    name: String,
    arguments: String,
}
impl ToolCall {
    fn message(&self) -> Value {
        json!({"id":self.id, "type":"function", "function":{"name":self.name,"arguments":self.arguments}})
    }
}

#[derive(Default)]
struct Completion {
    text: String,
    calls: BTreeMap<usize, ToolCall>,
    finish: Option<String>,
}
impl Completion {
    fn push(&mut self, value: Value) -> Result<()> {
        ensure!(value.get("error").is_none(), "LLM stream returned an error");
        let Some(choice) = value["choices"].as_array().and_then(|v| v.first()) else {
            return Ok(());
        };
        ensure!(
            choice["index"].as_u64().unwrap_or(0) == 0,
            "unexpected LLM choice index"
        );
        let delta = &choice["delta"];
        if let Some(text) = delta["content"].as_str() {
            ensure!(self.finish.is_none(), "content after finish_reason");
            self.text.push_str(text);
            ensure!(self.text.len() <= MAX_TEXT, "LLM text too long");
        }
        ensure!(
            delta.get("function_call").is_none_or(Value::is_null),
            "legacy function_call is not supported"
        );
        if let Some(calls) = delta["tool_calls"].as_array() {
            ensure!(self.finish.is_none(), "tool call after finish_reason");
            for delta in calls {
                let index = delta["index"].as_u64().context("missing tool call index")?;
                ensure!(index < MAX_CALLS as u64, "too many tool calls");
                if let Some(kind) = delta["type"].as_str() {
                    ensure!(kind == "function", "unsupported tool call type");
                }
                let call = self.calls.entry(index as usize).or_default();
                if let Some(id) = delta["id"].as_str() {
                    call.id.push_str(id);
                }
                if let Some(name) = delta["function"]["name"].as_str() {
                    call.name.push_str(name);
                }
                if let Some(args) = delta["function"]["arguments"].as_str() {
                    call.arguments.push_str(args);
                }
                ensure!(
                    call.id.len() <= 128 && call.name.len() <= 128 && call.arguments.len() <= 8192,
                    "tool call too large"
                );
            }
        }
        if let Some(reason) = choice["finish_reason"].as_str() {
            ensure!(self.finish.is_none(), "duplicate finish_reason");
            self.finish = Some(reason.to_owned());
        }
        Ok(())
    }

    fn validate(&self, ids: &mut HashSet<String>) -> Result<()> {
        match self.finish.as_deref() {
            Some("stop") => ensure!(
                self.calls.is_empty(),
                "tool calls without tool_calls finish_reason"
            ),
            Some("tool_calls") => ensure!(!self.calls.is_empty(), "empty tool_calls completion"),
            _ => bail!("LLM stream incomplete, truncated or filtered"),
        }
        for (index, call) in self.calls.values().enumerate() {
            ensure!(
                self.calls.contains_key(&index),
                "non-contiguous tool call indexes"
            );
            ensure!(
                !call.id.is_empty() && !call.name.is_empty() && ids.insert(call.id.clone()),
                "invalid/duplicate tool call ID"
            );
        }
        Ok(())
    }
}

async fn completion(
    client: &reqwest::Client,
    cfg: &LlmCfg,
    messages: &[Value],
    tools: &[Value],
    final_round: bool,
    ids: &mut HashSet<String>,
) -> Result<Completion> {
    let mut body = json!({"model":cfg.model, "messages":messages, "stream":true, "temperature":0.7, "max_tokens":1000});
    if !tools.is_empty() {
        body["tools"] = json!(tools);
        body["tool_choice"] = json!(if final_round { "none" } else { "auto" });
        body["parallel_tool_calls"] = json!(false);
    }
    let resp = client
        .post(format!(
            "{}/chat/completions",
            cfg.base_url.trim_end_matches('/')
        ))
        .bearer_auth(&cfg.api_key)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    // Bound the body before it reaches the SSE parser (including unterminated events).
    let mut bytes = 0usize;
    let stream = resp.bytes_stream().map(move |chunk| -> Result<_> {
        let chunk = chunk?;
        bytes += chunk.len();
        ensure!(bytes <= 1024 * 1024, "LLM stream body too large");
        Ok(chunk)
    });
    let mut events = Box::pin(stream.eventsource());
    let mut result = Completion::default();
    while let Some(event) = events.next().await {
        let event = event.context("invalid LLM SSE event")?;
        if event.data.trim() == "[DONE]" {
            break;
        }
        result.push(serde_json::from_str(&event.data).context("invalid LLM stream JSON")?)?;
    }
    result.validate(ids)?;
    Ok(result)
}

/// OpenAI-compatible streaming tool loop. Only completed, tool-free text is spoken:
/// a provider may emit speculative prose before its tool_calls delta arrives.
/// Returns final speech text and the complete assistant/tool history together.
pub async fn chat_stream(
    cfg: &LlmCfg,
    mut messages: Vec<Value>,
    device: Arc<DeviceMcp>,
    cancel: CancellationToken,
) -> Result<(String, Vec<Value>)> {
    let mut turn = ToolTurn::new(&mut messages);
    let initial_len = messages.len();
    let run = async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        let tools = device.llm_tools(&cancel).await;
        let mut ids = HashSet::new();
        for round in 0..=MAX_TOOL_ROUNDS {
            let response = completion(
                &client, cfg, &messages, &tools, round == MAX_TOOL_ROUNDS, &mut ids,
            ).await?;
            if response.calls.is_empty() {
                ensure!(!response.text.trim().is_empty(), "LLM returned no answer");
                let text = turn.answer(response.text);
                messages.push(json!({"role":"assistant", "content":text}));
                return Ok((text, messages.split_off(initial_len)));
            }
            ensure!(round < MAX_TOOL_ROUNDS, "LLM tool round limit exceeded");
            messages.push(json!({"role":"assistant", "content":null,
                "tool_calls":response.calls.values().map(ToolCall::message).collect::<Vec<_>>() }));
            for call in response.calls.values() {
                let content = turn.call(&device, &call.name, &call.arguments, &cancel).await;
                messages.push(json!({"role":"tool", "tool_call_id":call.id, "content":content}));
            }
        }
        unreachable!()
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => bail!("turn cancelled"),
        result = run => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_parallel_calls_and_unicode() {
        let mut c = Completion::default();
        c.push(json!({"choices":[{"delta":{"content":"先别说。", "tool_calls":[
            {"index":1,"id":"b","type":"function","function":{"name":"get_sensor_","arguments":"{"}},
            {"index":0,"id":"a","type":"function","function":{"name":"get_current_time","arguments":"{}"}}
        ]}}]})).unwrap();
        c.push(json!({"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"name":"reading","arguments":"}"}}]},"finish_reason":"tool_calls"}]})).unwrap();
        c.validate(&mut HashSet::new()).unwrap();
        assert_eq!(c.calls[&1].name, "get_sensor_reading");
        assert_eq!(c.calls[&1].arguments, "{}");
    }

    #[test]
    fn null_legacy_field_is_allowed_with_text_and_tool_calls() {
        let mut text = Completion::default();
        text.push(json!({"choices":[{"delta":{"content":"你好", "function_call":null},"finish_reason":"stop"}]})).unwrap();
        text.validate(&mut HashSet::new()).unwrap();
        assert_eq!(text.text, "你好");
        let mut tool = Completion::default();
        tool.push(json!({"choices":[{"delta":{"function_call":null,"tool_calls":[
            {"index":0,"id":"t1","type":"function","function":{"name":"get_current_time","arguments":"{}"}}
        ]},"finish_reason":"tool_calls"}]})).unwrap();
        tool.validate(&mut HashSet::new()).unwrap();
        assert_eq!(tool.calls[&0].name, "get_current_time");
        for legacy in [json!({}), json!("bad"), json!(false)] {
            assert!(Completion::default()
                .push(json!({"choices":[{"delta":{"function_call":legacy}}]}))
                .is_err());
        }
    }

    #[test]
    fn rejects_truncation_duplicate_ids_limits_and_legacy_calls() {
        let mut c = Completion::default();
        assert!(c.validate(&mut HashSet::new()).is_err());
        assert!(c
            .push(json!({"choices":[{"delta":{"function_call":{}}}]}))
            .is_err());
        assert!(c
            .push(json!({"choices":[{"delta":{"tool_calls":[{"index":3}]}}]}))
            .is_err());
        let mut c = Completion::default();
        c.push(json!({"choices":[{"delta":{"tool_calls":[
            {"index":0,"id":"a","function":{"name":"a"}}, {"index":1,"id":"a","function":{"name":"b"}}
        ]},"finish_reason":"tool_calls"}]})).unwrap();
        assert!(c.validate(&mut HashSet::new()).is_err());
    }
}

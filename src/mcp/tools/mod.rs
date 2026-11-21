use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{ensure, Result};
use rmcp::model::JsonObject;
use serde_json::{json, Map, Value};

pub mod sensor;
pub mod time;
pub mod volume;
pub mod weather;

pub(super) const READING_FAILED: &str = "设备数据读取失败，暂时无法提供可靠的实时信息。";

#[derive(Clone, Copy)]
pub(super) struct ReadTool {
    pub name: &'static str,
    pub alias: &'static str,
    pub description: &'static str,
}

const READ_TOOLS: [ReadTool; 3] = [time::SPEC, weather::SPEC, sensor::SPEC];

pub(super) fn is_reading(alias: &str) -> bool {
    READ_TOOLS.iter().any(|t| t.alias == alias)
}

/// Device tools exposed to the model. Read-only data tools use fixed server metadata,
/// the rest pass through the device declaration.
#[derive(Clone)]
pub(super) struct ToolSpec {
    pub alias: String,
    pub name: String,
    pub description: String,
    pub schema: Value,
    pub reading: bool,
}

pub fn empty_schema() -> Value {
    json!({"type":"object", "properties":{}, "additionalProperties":false})
}

/// OpenAI function names may not contain dots: self.audio_speaker.set_volume -> self_audio_speaker_set_volume
fn alias_for(name: &str) -> Option<String> {
    let alias: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    (alias.len() <= 64 && alias.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_'))
        .then_some(alias)
}

pub(super) fn build_tools(raw: Vec<(String, Option<String>, Arc<JsonObject>)>) -> Vec<ToolSpec> {
    let empty = empty_schema();
    let mut tools = Vec::new();
    // Read tools must match the empty schema exactly or they are not exposed; wording and schema are fixed by the server.
    for (name, _, schema) in &raw {
        let Some(read) = READ_TOOLS.iter().find(|t| t.name == name) else {
            continue;
        };
        if schema.as_ref() != empty.as_object().unwrap() {
            continue;
        }
        tools.push(ToolSpec {
            alias: read.alias.to_string(),
            name: name.clone(),
            description: read.description.to_string(),
            schema: empty_schema(),
            reading: true,
        });
    }
    // Other standard tools (volume/brightness/camera/board actions) pass through the device declaration; their results are not validated.
    for (name, description, schema) in raw {
        // A read tool with a non-matching schema is dropped entirely, never exposed as an action tool that bypasses reading validation.
        if READ_TOOLS.iter().any(|t| t.name == name)
            || schema.get("type").and_then(Value::as_str) != Some("object")
        {
            continue;
        }
        let Some(alias) = alias_for(&name) else {
            continue;
        };
        if is_reading(&alias) || tools.iter().any(|t| t.alias == alias) {
            continue;
        }
        tools.push(ToolSpec {
            alias,
            name,
            description: description.unwrap_or_default(),
            schema: Value::Object((*schema).clone()),
            reading: false,
        });
    }
    tools
}

pub(super) enum DirectReading {
    Time,
    Sensor(sensor::SensorQuestion),
}

impl DirectReading {
    pub fn from_question(text: &str) -> Option<Self> {
        let question = text
            .trim()
            .trim_end_matches(['。', '？', '?', '！', '!'])
            .trim_end();
        if time::match_question(question) {
            Some(Self::Time)
        } else {
            sensor::match_question(question).map(Self::Sensor)
        }
    }

    pub fn tool(&self) -> &'static str {
        match self {
            Self::Time => time::ALIAS,
            Self::Sensor(_) => sensor::ALIAS,
        }
    }

    pub fn render(&self, data: &Value) -> Option<String> {
        match self {
            Self::Time => time::render(data),
            Self::Sensor(kind) => sensor::render(*kind, data),
        }
    }
}

pub(super) fn record_reading_data(
    alias: &str,
    data: &mut Value,
    unavailable: &mut BTreeSet<&'static str>,
    cache_warning: &mut Option<&'static str>,
) {
    match alias {
        time::ALIAS => time::check_availability(data, unavailable),
        sensor::ALIAS => sensor::check_availability(data, unavailable),
        weather::ALIAS => weather::process_cache(data, unavailable, cache_warning),
        _ => {}
    }
}

pub(super) fn validate_call_args(
    tool_name: &str,
    reading: bool,
    args: &Map<String, Value>,
) -> Result<()> {
    if reading {
        ensure!(args.is_empty(), "tool takes no arguments");
    } else if tool_name == volume::NAME {
        volume::validate_args(args)?;
    }
    Ok(())
}

pub(super) fn process_action_result(
    tool_name: &str,
    text: &str,
    args: &Map<String, Value>,
) -> Result<Value> {
    // Older devices may return plain text or a JSON boolean for actions.
    let data = serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()));
    if tool_name == volume::NAME {
        volume::validate_result(&data, args)?;
    }
    Ok(data)
}

// Validate only the agreed availability/metadata contract, not forecast business
// rules already owned by the ESP32. Never manufacture defaults for missing data.
pub(super) fn validate_reading(name: &str, data: &Value) -> Result<()> {
    match name {
        time::NAME => time::validate(data),
        sensor::NAME => sensor::validate(data),
        weather::NAME => weather::validate(data),
        _ => Ok(()),
    }
}

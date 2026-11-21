use std::collections::BTreeSet;

use anyhow::{Context, Result};
use chrono::Timelike;
use serde_json::Value;

use super::ReadTool;

pub const NAME: &str = "self.get_current_time";
pub const ALIAS: &str = "get_current_time";
pub const UNAVAILABLE: &str = "设备时间尚未同步，无法确认当前时间。";

pub const SPEC: ReadTool = ReadTool {
    name: NAME,
    alias: ALIAS,
    description: "读取设备本地当前时间（无参数）。synchronized=false 时只能报告未对时，不可猜测。",
};

pub fn match_question(question: &str) -> bool {
    matches!(question, "现在几点" | "现在几点了" | "几点了")
}

pub fn render(data: &Value) -> Option<String> {
    if data["synchronized"] != true {
        return Some(UNAVAILABLE.into());
    }
    let time = chrono::DateTime::parse_from_rfc3339(data["local_time"].as_str()?).ok()?;
    Some(format!("现在是{}点{:02}分。", time.hour(), time.minute()))
}

pub fn check_availability(data: &Value, unavailable: &mut BTreeSet<&'static str>) {
    if data["synchronized"] != true {
        unavailable.insert(UNAVAILABLE);
    }
}

pub fn validate(data: &Value) -> Result<()> {
    let synced = data["synchronized"]
        .as_bool()
        .context("missing synchronized flag")?;
    if synced {
        chrono::DateTime::parse_from_rfc3339(
            data["local_time"]
                .as_str()
                .context("missing ISO local_time")?,
        )
        .context("invalid ISO local_time or UTC offset")?;
    }
    Ok(())
}

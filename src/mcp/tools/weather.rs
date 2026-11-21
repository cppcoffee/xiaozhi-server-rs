use std::collections::BTreeSet;

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, FixedOffset, SecondsFormat};
use serde_json::{json, Value};

use super::ReadTool;

pub const NAME: &str = "self.get_weather_forecast";
pub const ALIAS: &str = "get_weather_forecast";

pub const SPEC: ReadTool = ReadTool {
    name: NAME,
    alias: ALIAS,
    description: "读取天气缓存（5天与21小时，不发新请求）。fetched_at_local/fetched_at 为抓取时间（fetched_at_known=false 时未知但仍可用）；date/hour 为当地预报时点，逐时按抓取顺序不滚动；用 age/stale/retrying 说明时效；WMO 天气码；precip_probability 为预报降水概率（0..100%），非实测降水。",
};

fn forecast_fetch_local_time(data: &Value) -> Option<String> {
    let timestamp = DateTime::from_timestamp(data["fetched_at"].as_i64()?, 0)?;
    let offset = i32::try_from(data["utc_offset_seconds"].as_i64()?).ok()?;
    // RFC3339 offsets have minute precision.
    if offset % 60 != 0 {
        return None;
    }
    Some(
        timestamp
            .with_timezone(&FixedOffset::east_opt(offset)?)
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    )
}

pub fn process_cache(
    data: &mut Value,
    unavailable: &mut BTreeSet<&'static str>,
    cache_warning: &mut Option<&'static str>,
) {
    if data["available"] == false {
        unavailable.insert("设备暂无可用的天气缓存。");
        return;
    }
    if let Some(local) = forecast_fetch_local_time(data) {
        data["fetched_at_local"] = json!(local);
    }
    if data["fetched_at_known"] == false {
        *cache_warning = Some("天气来自旧缓存，抓取时间未知，以下仅为缓存预报。");
    } else if data["stale"] == true || data["retrying"] == true {
        cache_warning.get_or_insert("天气来自旧缓存，可能已过期或正在重试更新。");
    }
}

pub fn validate(data: &Value) -> Result<()> {
    if data["available"] == false {
        return Ok(());
    }
    ensure!(
        data["days"].as_array().is_some_and(|a| a.len() == 5),
        "expected 5 cached days"
    );
    ensure!(
        data["hours"].as_array().is_some_and(|a| a.len() == 21),
        "expected 21 cached hours"
    );
    ensure!(
        data["city"].as_str().is_some_and(|s| !s.trim().is_empty()),
        "missing cached city"
    );
    ensure!(
        data["utc_offset_seconds"]
            .as_i64()
            .is_some_and(|n| (-86400..=86400).contains(&n)),
        "invalid UTC offset"
    );
    ensure!(
        data["age"]
            .as_f64()
            .is_some_and(|n| n.is_finite() && n >= 0.0),
        "invalid cache age"
    );
    ensure!(
        data["stale"].is_boolean() && data["retrying"].is_boolean(),
        "missing cache state"
    );
    let fetched = data.get("fetched_at").context("missing fetched_at")?;
    match data.get("fetched_at_known") {
        Some(Value::Bool(false)) => {
            ensure!(fetched.is_null(), "unknown fetched_at must be null");
        }
        None | Some(Value::Bool(true)) => {
            // Keep older devices with a real timestamp compatible. Unknown
            // wall-clock time must be explicit, never the Unix-zero sentinel.
            ensure!(
                fetched.as_u64().is_some_and(|n| n > 0)
                    || fetched
                        .as_str()
                        .is_some_and(|s| DateTime::parse_from_rfc3339(s).is_ok()),
                "invalid known fetched_at"
            );
        }
        _ => bail!("fetched_at_known must be boolean"),
    }
    Ok(())
}

use std::collections::BTreeSet;

use anyhow::{ensure, Context, Result};
use chrono::DateTime;
use serde_json::Value;

use super::ReadTool;

pub const NAME: &str = "self.get_sensor_reading";
pub const ALIAS: &str = "get_sensor_reading";
pub const UNAVAILABLE: &str = "设备没有可用的本地温湿度读数。";

pub const SPEC: ReadTool = ReadTool {
    name: NAME,
    alias: ALIAS,
    description: "读取最新温湿度与气压（temperature_c、humidity_percent、pressure_hpa，无气压为 null），含 sampled_at（采样时间，null 则未知）和 sample_age_seconds（单调秒数）。available=false 时无可用读数。",
};

#[derive(Clone, Copy)]
pub enum SensorQuestion {
    Temperature,
    Humidity,
}

pub fn match_question(question: &str) -> Option<SensorQuestion> {
    match question {
        "室内多少度" | "室内温度多少" | "室内温度是多少" => {
            Some(SensorQuestion::Temperature)
        }
        "室内湿度多少" | "室内湿度是多少" => Some(SensorQuestion::Humidity),
        _ => None,
    }
}

pub fn render(kind: SensorQuestion, data: &Value) -> Option<String> {
    if data["available"] != true {
        return Some(UNAVAILABLE.into());
    }
    match kind {
        SensorQuestion::Temperature => Some(format!(
            "室内温度是{:.1}摄氏度。",
            data["temperature_c"].as_f64()?
        )),
        SensorQuestion::Humidity => Some(format!(
            "室内湿度是百分之{:.1}。",
            data["humidity_percent"].as_f64()?
        )),
    }
}

pub fn check_availability(data: &Value, unavailable: &mut BTreeSet<&'static str>) {
    if data["available"] != true {
        unavailable.insert(UNAVAILABLE);
    }
}

pub fn validate(data: &Value) -> Result<()> {
    let available = data["available"]
        .as_bool()
        .context("missing sensor available flag")?;
    if available {
        ensure!(
            data["temperature_c"].as_f64().is_some_and(f64::is_finite),
            "invalid local temperature_c"
        );
        ensure!(
            data["humidity_percent"]
                .as_f64()
                .is_some_and(|h| h.is_finite() && (0.0..=100.0).contains(&h)),
            "invalid local humidity_percent"
        );
        if let Some(pressure) = data.get("pressure_hpa").filter(|v| !v.is_null()) {
            ensure!(
                pressure
                    .as_f64()
                    .is_some_and(|p| p.is_finite() && (300.0..=1100.0).contains(&p)),
                "invalid local pressure_hpa"
            );
        }
        if let Some(at) = data.get("sampled_at").filter(|v| !v.is_null()) {
            DateTime::parse_from_rfc3339(at.as_str().context("invalid sampled_at type")?)
                .context("invalid ISO sampled_at or UTC offset")?;
        }
        if let Some(age) = data.get("sample_age_seconds") {
            ensure!(
                age.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0),
                "invalid sensor sample age"
            );
        }
    }
    Ok(())
}

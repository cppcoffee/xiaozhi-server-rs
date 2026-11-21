use anyhow::{ensure, Result};
use serde_json::{Map, Value};

pub const NAME: &str = "self.audio_speaker.set_volume";

pub fn validate_args(args: &Map<String, Value>) -> Result<()> {
    ensure!(
        args.len() == 1
            && args
                .get("volume")
                .and_then(Value::as_u64)
                .is_some_and(|volume| volume <= 100),
        "volume must be an integer from 0 to 100"
    );
    Ok(())
}

pub fn validate_result(data: &Value, args: &Map<String, Value>) -> Result<()> {
    ensure!(data != false, "device could not set speaker volume");
    if let Some(saved) = data.get("saved") {
        ensure!(
            saved == true && data["volume"] == args["volume"],
            "device did not confirm the requested volume was saved"
        );
    }
    Ok(())
}

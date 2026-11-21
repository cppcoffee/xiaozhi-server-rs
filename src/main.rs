use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use xiaozhi_server_rs::{logging, serve, Config};

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to the configuration file
    #[arg(short = 'c', value_name = "FILE", default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    run().await.unwrap_or_else(|error| {
        eprintln!("Error: {}", logging::ascii(&format!("{error:#}")));
        ExitCode::FAILURE
    })
}

async fn run() -> anyhow::Result<ExitCode> {
    let args = Args::parse();
    let cfg = Arc::new(Config::load(&args.config)?);
    let _log_guard = logging::init(&cfg.log);

    serve(cfg, args.config).await?;
    Ok(ExitCode::SUCCESS)
}

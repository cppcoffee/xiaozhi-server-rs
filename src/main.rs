use std::process::ExitCode;
use std::sync::Arc;

use xiaozhi_server_rs::{check, logging, serve, Config};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("Error: {}", logging::ascii(&format!("{error:#}")));
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<ExitCode> {
    // Args: optional config path + --check (only verify credentials, do not start the server)
    let mut path = "config.toml".to_string();
    let mut do_check = false;
    for a in std::env::args().skip(1) {
        if a == "--check" {
            do_check = true;
        } else {
            path = a;
        }
    }
    let cfg = Arc::new(Config::load(&path)?);
    logging::init(&cfg.log);

    if do_check {
        return Ok(if check::run(&cfg).await {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        });
    }

    serve(cfg, path).await?;
    Ok(ExitCode::SUCCESS)
}

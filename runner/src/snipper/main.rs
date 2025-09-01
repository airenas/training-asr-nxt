use std::{
    env,
    io::{self, BufRead, Write},
};

use clap::Parser;
use runner::{utils::snipper::{concat_segments, Segment}, APP_NAME};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug, Clone)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="WAV files snipper", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Input base dir
    #[arg(long, env, default_value = "")]
    input_base: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(
            fmt::Layer::default()
                .with_writer(std::io::stderr) // <-- send logs to stderr
                .compact(),
        )
        .init();
    let args = Args::parse();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = main_int(args) {
            tracing::error!("{}", e);
            return Err(e);
        }
        Ok(())
    })
    .await?
}

fn main_int(args: Args) -> anyhow::Result<()> {
    tracing::info!(name = APP_NAME, "Starting snipper");
    tracing::info!(version = env!("CARGO_APP_VERSION"));
    tracing::info!(input_base = args.input_base);
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

    // let cancel_rx = setup_signal_handlers();

    let mut segments: Vec<Segment> = Vec::new();

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let mut seg: Segment = serde_json::from_str(&line)?;
        seg.file = format!("{}/{}", args.input_base, seg.file);
        segments.push(seg);
    }
    tracing::info!(len = segments.len(), "segments to process");

    let wav_bytes = concat_segments(&segments)?;
    io::stdout().write_all(&wav_bytes)?;

    tracing::info!("finished");
    Ok(())
}

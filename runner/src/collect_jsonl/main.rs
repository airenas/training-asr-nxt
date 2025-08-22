use std::{
    env,
    fs::File,
    io::{BufRead, BufWriter},
    time::Duration,
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use runner::{
    data::errors::RunnerError, db::get_pool, utils::system::setup_signal_handlers, APP_NAME,
};
use serde_json::Value;
use std::io::Write;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug, Clone)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="Data zipper", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Output file
    #[arg(long, env, default_value = "")]
    output: String,
    /// File name/key to collect
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    name: String,
    /// Database URL
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    db_url: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::Layer::default().compact())
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
    tracing::info!(name = APP_NAME, "Starting db filler");
    tracing::info!(version = env!("CARGO_APP_VERSION"));
    tracing::info!(output = args.output);
    tracing::info!(name = args.name);
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

    let cancel_rx = setup_signal_handlers();

    let pool = get_pool(&args.db_url, 1)?;

    tracing::info!("collecting files");
    let files = runner::db::collect_files(pool.clone())?;
    tracing::info!(len = files.len(), "files collected");

    let progress = ProgressBar::new(files.len() as u64);

    progress.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ETA: {eta} {msg}",
        )
        .unwrap()
        .progress_chars("##-"),
    );

    let mut no_data = 0;

    let mut conn = pool.get_timeout(Duration::from_secs(10))?;

    let file = File::create(args.output)?;
    let mut writer = BufWriter::new(file);

    for f in files.iter() {
        if cancel_rx.try_recv().is_ok() {
            progress.set_message("Cancelled");
            tracing::warn!("Runner cancelled");
            return Ok(());
        }
        let data = runner::db::load(&mut conn, &f.id, &args.name);
        match data {
            Ok(data) => {
                for line in data.data.lines() {
                    match line {
                        Ok(line) => {
                            if line.trim().is_empty() {
                                continue; // skip empty lines
                            }
                            let updated_line = process(&line, &f.id)?;
                            writeln!(writer, "{}", updated_line)?;
                        }
                        Err(err) => {
                            return Err(Into::<anyhow::Error>::into(err)
                                .context(format!("Failed to read line for file {}", f.path)));
                        }
                    }
                }
            }
            Err(err) => {
                if let Some(RunnerError::RecordNotFound { id: _, type_: _ }) =
                    err.downcast_ref::<RunnerError>()
                {
                    tracing::warn!(file = f.path, "No file");
                    no_data += 1;
                } else {
                    return Err(err);
                }
            }
        }
        progress.inc(1);
    }

    if no_data > 0 {
        progress.set_message("Runner failed");
        tracing::warn!(no_data, "Runner completed with skips");
    } else {
        progress.finish_with_message("Runner completed");
    }

    tracing::info!("Runner finished");
    Ok(())
}

fn process(line: &str, id: &str) -> anyhow::Result<String> {
    let mut v: Value = serde_json::from_str(line)?;

    if let Value::Object(obj) = &mut v {
        obj.insert("f".to_string(), Value::String(id.to_string()));
    }
    Ok(serde_json::to_string(&v)?)
}

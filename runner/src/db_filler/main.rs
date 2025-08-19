use std::{
    env,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::Context;
use clap::Parser;
use crossbeam_channel::{bounded, select, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use runner::APP_NAME;
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
use symphonia::{
    core::{io::MediaSourceStream, probe::Hint},
    default::get_probe,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

#[derive(Parser, Debug, Clone)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="Data zipper", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Workers
    #[arg(long, env, default_value = "12")]
    workers: u16,
    /// Input dir
    #[arg(long, env, default_value = "./input")]
    input: String,
    /// Audio base dir
    #[arg(long, env, default_value = "")]
    audio_base: String,
    /// Extensions
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    extensions: Vec<String>,
    /// Database URL
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    db_url: String,
    /// Cache file
    #[arg(long, env, default_value = ".runner.file.cache")]
    cache_file: String,
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
    tracing::info!(input = args.input);
    tracing::info!(extensions = args.extensions.join(","));
    tracing::info!(audio_base = args.audio_base);
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

    let (cancel_tx, cancel_rx) = bounded::<()>(2);

    thread::spawn(move || {
        let mut signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
        for signal in &mut signals {
            match signal {
                SIGINT => {
                    tracing::info!("Exit event SIGINT");
                }
                SIGTERM => {
                    tracing::info!("Exit event SIGTERM");
                }
                _ => {}
            }
        }
        tracing::debug!("sending exit event");
        if let Err(e) = cancel_tx.send(()) {
            tracing::error!("Failed to send cancel signal: {}", e);
        }
    });

    let manager = PostgresConnectionManager::new(args.db_url.as_str().parse()?, NoTls);
    let pool = Arc::new(
        Pool::builder()
            .max_size(args.workers as u32)
            .build(manager)?,
    );

    tracing::info!("collecting files");
    let files = runner::files::collect_files(&args.input, &args.extensions, &args.cache_file)?;
    tracing::info!(len = files.len(), "files collected");

    let progress = Arc::new(Mutex::new(ProgressBar::new(files.len() as u64)));

    let input_path = PathBuf::from(&args.input)
        .canonicalize()?
        .as_os_str()
        .to_string_lossy()
        .to_string();

    progress.lock().unwrap().set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ETA: {eta} {msg}",
        )
        .unwrap()
        .progress_chars("##-"),
    );

    let (tx, rx): (Sender<PathBuf>, Receiver<PathBuf>) = bounded(0);

    // producer
    thread::spawn({
        let cancel_rx = cancel_rx.clone();
        move || {
            for f in files {
                select! {
                        send(tx, f) -> res => {
                    if res.is_err() {
                        break;
                    }
                }
                        recv(cancel_rx) -> _ => {
                        break;
                    }
                    }
            }
            tracing::info!("Sender exit");
        }
    });

    let mut handles: Vec<thread::JoinHandle<Result<u16, anyhow::Error>>> = Vec::new();
    let failed_count = Arc::new(Mutex::new(0));

    for i in 0..args.workers {
        tracing::info!(i, "Starting worker");
        let rx = rx.clone();
        let progress = progress.clone();
        let worker_index = i;
        let cancel_rx = cancel_rx.clone();
        let pool = pool.clone();
        let input_path = input_path.clone();
        let audio_base = args.audio_base.clone();
        let failed_count = failed_count.clone();

        handles.push(thread::spawn(move || {
            for file in rx.iter() {
                if cancel_rx.try_recv().is_ok() {
                    break;
                }

                let res: Result<(), anyhow::Error> = (|| {
                    let file_name = get_name(&file, &input_path);
                    tracing::debug!(worker_index, file_name, "Processing file");
                    let audio_file_name = make_audio_name(&file_name, &audio_base);

                    let id = format!("{:x}", md5::compute(&file_name));
                    let duration_in_sec = get_duration(&audio_file_name).context(format!(
                        "Failed to get audio duration, {}", audio_file_name.display()
                    ))?;
                    tracing::debug!(file_name, duration_in_sec);

                    let mut conn = pool.get_timeout(Duration::from_secs(10))?;
                    conn.execute(
                        "INSERT INTO files (id, path, duration_in_sec) VALUES ($1, $2, $3)
                     ON CONFLICT (id) DO NOTHING",
                        &[&id, &file_name.to_string(), &duration_in_sec],
                    )?;
                    Ok(())
                })();

                if let Err(e) = res {
                    tracing::error!(worker_index, file = ?file, "Failed to process file: {}", e);
                    let mut failed = failed_count.lock().unwrap();
                    *failed += 1;
                }
                let pb = progress.lock().unwrap();
                pb.inc(1);
            }
            Ok(worker_index)
        }));
    }

    let mut was_err = false;

    for h in handles {
        let join_res = h.join();
        match join_res {
            Ok(res) => match res {
                Ok(index) => tracing::info!("Worker {} finished", index),
                Err(e) => {
                    was_err = true;
                    tracing::error!("Worker thread error: {:?}", e);
                }
            },
            Err(e) => {
                was_err = true;
                tracing::error!("Failed to join thread: {:?}", e);
            }
        }
    }

    tracing::info!("closing connections");
    drop(pool);

    let failed = *failed_count.lock().unwrap();

    if was_err || failed > 0 {
        progress.lock().unwrap().set_message("Runner failed");
        tracing::warn!(failed, "Runner completed with failures");
    } else {
        progress
            .lock()
            .unwrap()
            .finish_with_message("Runner completed");
    }

    tracing::info!("Runner finished");
    Ok(())
}

fn make_audio_name(file: &str, audio_base: &str) -> PathBuf {
    let audio_file_name = format!("{}/audio.16.wav", file);
    PathBuf::from(audio_base).join(audio_file_name)
}

fn get_name(file: &PathBuf, prefix: &str) -> String {
    let file_name = file.to_string_lossy().to_string();
    let mut res = file_name.strip_prefix(prefix).unwrap().to_string();
    if res.starts_with("/") {
        res = res.strip_prefix("/").unwrap().to_string();
    }
    res
}

fn get_duration(path: &Path) -> anyhow::Result<f64> {
    let file = std::fs::File::open(path)?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());
    let mut hint = Hint::new();
    if let Some(ext) = path.extension() {
        hint.with_extension(&ext.to_string_lossy().into_owned());
    }

    let probed = get_probe().format(&hint, mss, &Default::default(), &Default::default())?;
    let format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.sample_rate.is_some())
        .unwrap();
    let codec_params = &track.codec_params;

    if let Some(n_frames) = codec_params.n_frames {
        let sample_rate = codec_params.sample_rate.unwrap();
        return Ok(n_frames as f64 / sample_rate as f64);
    }
    Ok(0.0)
}

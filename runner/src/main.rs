use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use ansi_term::Color;
use clap::Parser;
use console::Term;
use humantime::format_duration;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use runner::{files, APP_NAME};
use tokio::signal::unix::{signal, SignalKind};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="Data ETl (just files) pipelines runner", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Workers
    #[arg(long, env, default_value = "12")]
    workers: u16,
    /// Input dir
    #[arg(long, env, default_value = "./input")]
    input: String,
    /// Output dir
    #[arg(long, env, default_value = "./output")]
    output: String,
    /// Output file
    #[arg(long, env, default_value = "audio.segments")]
    output_file: String,
    /// Command
    #[arg(long, env, default_value = "")]
    cmd: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::Layer::default().compact())
        .init();
    let args = Args::parse();
    if let Err(e) = main_int(args).await {
        tracing::error!("{}", e);
        return Err(e);
    }
    Ok(())
}

async fn main_int(args: Args) -> anyhow::Result<()> {
    tracing::info!(name = APP_NAME, "Starting runner");
    tracing::info!(version = env!("CARGO_APP_VERSION"));
    tracing::info!(workers = args.workers);
    tracing::info!(input = args.input);
    tracing::info!(output = args.output);
    tracing::info!(output_file = args.output_file);
    tracing::info!(cmd = args.cmd);
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers as usize)
        .build_global()?;

    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancel_flag_clone = cancel_flag.clone();

    tokio::spawn(async move {
        let mut int_stream = signal(SignalKind::interrupt()).unwrap();
        let mut term_stream = signal(SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = int_stream.recv() => tracing::info!("Exit event int"),
            _ = term_stream.recv() => tracing::info!("Exit event term"),
        }
        tracing::debug!("sending exit event");
        cancel_flag_clone.store(true, Ordering::SeqCst);
    });

    tracing::info!("collecting files");
    let files = runner::files::collect_files(&args.input)?;
    tracing::info!(len = files.len(), "files collected");

    let progress = Arc::new(Mutex::new(ProgressBar::new(files.len() as u64)));
    let pb = progress.clone();

    pb.lock().unwrap().set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}",
        )
        .unwrap()
        .progress_chars("##-"),
    );

    let failed_count = Arc::new(Mutex::new(0));
    let skipped_count = Arc::new(Mutex::new(0));
    let success_count = Arc::new(Mutex::new(0));

    // Process files in parallel using rayon
    files.par_iter().for_each(|file| {
        // files.iter().for_each(|file| {

        if cancel_flag.load(Ordering::SeqCst) {
            return;
        }

        let params = runner::Params {
            input_dir: &args.input,
            output_dir: &args.output,
            file_name: file.to_str().unwrap(),
            cmd: &args.cmd,
            result_file_name: &args.output_file,
        };

        tracing::debug!(file = file.display().to_string());
        let res = files::run(&params);
        match res {
            Ok(runner::ProcessStatus::Success) => {
                let mut success = success_count.lock().unwrap();
                *success += 1;
            }
            Ok(runner::ProcessStatus::Skipped) => {
                let mut skipped = skipped_count.lock().unwrap();
                *skipped += 1;
            }
            Err(err) => {
                let mut failed = failed_count.lock().unwrap();
                tracing::error!(
                    file = file.display().to_string(),
                    err = %err,
                    "Error processing file"
                );
                *failed += 1;
            }
        }
        let pb = progress.lock().unwrap();
        let success = *success_count.lock().unwrap();
        let skipped = *skipped_count.lock().unwrap();
        let failed = *failed_count.lock().unwrap();
        let all = pb.length().unwrap_or_default();
        pb.set_message(get_info_str(true, success + skipped, skipped, failed));
        pb.inc(1);

        if !Term::stderr().is_term() {
            let prc = if all > 0 {
                (pb.position() as f32 / all as f32) * 100.0
            } else {
                0.0
            };
            tracing::info!(
                msg = get_info_str(false, success + skipped, skipped, failed),
                eta = format_duration(pb.eta()).to_string(),
                all,
                "%" = format!("{:.2}", prc),
            );
        }
    });

    {
        let pb = progress.lock().unwrap();
        let success = *success_count.lock().unwrap();
        let skipped = *skipped_count.lock().unwrap();
        let failed = *failed_count.lock().unwrap();
        let msg = format!(
            "{} - {}",
            Color::Green.bold().paint("Finished"),
            get_info_str(true, success + skipped, skipped, failed)
        );
        pb.finish_with_message(msg);
        if !Term::stderr().is_term() {
            tracing::info!(
                msg = get_info_str(false, success + skipped, skipped, failed),
                eta = format_duration(pb.eta()).to_string(),
                all = pb.length(),
            );
        }
    }

    tracing::info!("Runner finished");
    Ok(())
}

fn get_info_str(use_color: bool, ok: i32, skipped: i32, failed: i32) -> String {
    format!(
        "ok: {}, skipped: {}, failed: {}",
        paint(use_color, Color::Green, ok),
        paint(use_color, Color::Yellow, skipped),
        paint(use_color, Color::Red, failed)
    )
}

fn paint(use_color: bool, colour: Color, v: i32) -> String {
    if use_color && v > 0 {
        return colour.bold().paint(format!("{}", v)).to_string();
    }
    format!("{}", v)
}

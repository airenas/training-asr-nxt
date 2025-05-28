use std::{
    env,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use ansi_term::Color;
use bytesize::ByteSize;
use clap::Parser;
use console::Term;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use runner::{files, APP_NAME};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
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
    //Extensions for input files. After comman other required files in the ssame dir wanted
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    extensions: Vec<String>,
    /// Minimum Memory on the system to be available start new worker
    #[arg(long, env, default_value = "10G",value_parser = parse_bytesize)]
    minimum_memory: ByteSize,
    /// Output in the same dir as input - do not create new dir
    #[arg(long, env, default_value = "false")]
    same_dir: bool,
    /// Slow start - start workers one by one
    #[arg(long, env, default_value = "false")]
    slow_start: bool,
    /// Cache file
    #[arg(long, env, default_value = ".runner.file.cache")]
    cache_file: String,
}

fn parse_bytesize(s: &str) -> Result<ByteSize, String> {
    s.parse::<ByteSize>().map_err(|e| e.to_string())
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
    tracing::info!(extensions = args.extensions.join(","));
    tracing::info!(output = args.output);
    tracing::info!(output_file = args.output_file);
    tracing::info!(minimum_memory = args.minimum_memory.to_string());
    tracing::info!(same_dir = args.same_dir);
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
    let files = runner::files::collect_files(&args.input, &args.extensions, &args.cache_file)?;
    tracing::info!(len = files.len(), "files collected");

    let progress = Arc::new(Mutex::new(ProgressBar::new(files.len() as u64)));
    let pb = progress.clone();

    pb.lock().unwrap().set_style(
        ProgressStyle::with_template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    let failed_count = Arc::new(Mutex::new(0));
    let skipped_count = Arc::new(Mutex::new(0));
    let eta_calculator = Arc::new(Mutex::new(runner::utils::duration::ETACalculator::new(
        files.len(),
        200,
    )?));
    let memory_threshold_mb = (args.minimum_memory.as_u64()) / (1024 * 1024);

    let active_workers = Arc::new(AtomicUsize::new(0));
    let last_worker_time = Arc::new(Mutex::new(Instant::now()));

    // Process files in parallel using rayon
    files.par_iter().for_each(|file| {
        // files.iter().for_each(|file| {
        let active_workers = active_workers.clone();

        let mut sys = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
        );

        let mut mem_waiting = args.slow_start;
        let mut sleep = false;

        loop {
            if cancel_flag.load(Ordering::SeqCst) {
                return;
            }
            if sleep {
                thread::sleep(Duration::from_secs(10));
            }
            sys.refresh_memory();
            let available_memory_mb = sys.available_memory() / (1024 * 1024);
            if available_memory_mb < memory_threshold_mb {
                tracing::trace!(
                    "Low memory: {}MB available. Wanted {}MB. Workers({}/{})",
                    available_memory_mb,
                    memory_threshold_mb,
                    active_workers.load(Ordering::SeqCst),
                    args.workers,
                );
                mem_waiting = true;
                sleep = true;
            } else {
                if mem_waiting {
                    let mut last_time = last_worker_time.lock().unwrap();
                    let elapsed = last_time.elapsed();
                    if elapsed < Duration::from_secs(10) {
                        tracing::trace!("Worker waiting");
                        sleep = true;
                        continue;
                    }
                    *last_time = Instant::now();
                }
                break;
            }
        }

        {
            let mut last_time = last_worker_time.lock().unwrap();
            *last_time = Instant::now();
        }

        if cancel_flag.load(Ordering::SeqCst) {
            return;
        }

        let params = runner::Params {
            input_dir: &args.input,
            output_dir: &args.output,
            file_name: file.to_str().unwrap(),
            cmd: &args.cmd,
            result_file_name: &args.output_file,
            same_dir: args.same_dir,
        };

        tracing::debug!(file = file.display().to_string());
        active_workers.fetch_add(1, Ordering::SeqCst);
        {
            let pb = progress.lock().unwrap();
            let skipped = *skipped_count.lock().unwrap();
            let failed = *failed_count.lock().unwrap();
            let wrk_str = format!("{}/{}", active_workers.load(Ordering::SeqCst), args.workers);
            let eta_calculator = eta_calculator.lock().unwrap();
            pb.set_message(
                get_info_str(true, eta_calculator.completed() as i32, skipped, failed)
                    + format!(", ({}), wrk: {}", eta_calculator.eta_str(), wrk_str).as_str(),
            );
        }
        let res = files::run(&params);
        active_workers.fetch_sub(1, Ordering::SeqCst);

        match res {
            Ok(runner::ProcessStatus::Success) => {
                let mut eta = eta_calculator.lock().unwrap();
                eta.add_completed_with_duration();
            }
            Ok(runner::ProcessStatus::Skipped) => {
                let mut skipped = skipped_count.lock().unwrap();
                *skipped += 1;
                let mut eta = eta_calculator.lock().unwrap();
                eta.add_completed();
            }
            Err(err) => {
                let mut failed = failed_count.lock().unwrap();
                *failed += 1;
                let mut eta = eta_calculator.lock().unwrap();
                eta.add_completed_with_duration();
                tracing::error!(
                    file = file.display().to_string(),
                    err = %err,
                    "Error processing file"
                );
            }
        }
        let pb = progress.lock().unwrap();
        let skipped = *skipped_count.lock().unwrap();
        let failed = *failed_count.lock().unwrap();
        let all = pb.length().unwrap_or_default();
        let eta_calculator = eta_calculator.lock().unwrap();
        let eta = eta_calculator.eta_str();
        let wrk_str = format!("{}/{}", active_workers.load(Ordering::SeqCst), args.workers);
        pb.set_message(
            get_info_str(true, eta_calculator.completed() as i32, skipped, failed)
                + format!(", ({}), wrk: {}", eta, wrk_str).as_str(),
        );
        pb.inc(1);

        if !Term::stderr().is_term() {
            let prc = if all > 0 {
                (pb.position() as f32 / all as f32) * 100.0
            } else {
                0.0
            };
            tracing::info!(
                msg = get_info_str(false, eta_calculator.completed() as i32, skipped, failed),
                wrk = wrk_str,
                eta = eta.as_ref(),
                all,
                "%" = format!("{:.2}", prc),
            );
        }
    });

    {
        let pb = progress.lock().unwrap();
        let skipped = *skipped_count.lock().unwrap();
        let failed = *failed_count.lock().unwrap();
        let eta_calculator = eta_calculator.lock().unwrap();
        let msg = format!(
            "{} - {}",
            Color::Green.bold().paint("Finished"),
            get_info_str(true, eta_calculator.completed() as i32, skipped, failed)
        );
        pb.finish_with_message(msg);
        if !Term::stderr().is_term() {
            tracing::info!(
                msg = get_info_str(false, eta_calculator.completed() as i32, skipped, failed),
                all = pb.length(),
                "Finished",
            );
        }
    }

    tracing::info!("Runner finished");
    Ok(())
}

fn get_info_str(use_color: bool, ok: i32, skipped: i32, failed: i32) -> String {
    format!(
        "done: {}, skipped: {}, failed: {}",
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

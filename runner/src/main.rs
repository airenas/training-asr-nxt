use std::{
    env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use ansi_term::Color;
use bytesize::ByteSize;
use clap::Parser;
use console::Term;
use crossbeam_channel::{bounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use runner::{
    data::{errors::RunnerError, structs::FileMeta},
    db::get_pool,
    files,
    utils::system::{join_threads, setup_send_files, setup_signal_handlers},
    APP_NAME,
};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug, Clone)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="Data ETl (just files) pipelines runner", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Workers
    #[arg(long, env, default_value = "12")]
    workers: u16,
    /// Input base dir
    #[arg(long, env, default_value = "./input")]
    input_base: String,
    /// Output file
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    output_files: Vec<String>,
    /// Command
    #[arg(long, env, default_value = "")]
    cmd: String,
    //Input files
    #[arg(long, env, value_delimiter = ',', default_values_t = Vec::<String>::new())]
    input_files: Vec<String>,
    /// Minimum Memory on the system to be available start new worker
    #[arg(long, env, default_value = "10G",value_parser = parse_bytesize)]
    minimum_memory: ByteSize,
    /// Slow start - start workers one by one
    #[arg(long, env, default_value = "false")]
    slow_start: bool,
    /// Database URL
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    db_url: String,
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
    tracing::info!(name = APP_NAME, "Starting runner");
    tracing::info!(version = env!("CARGO_APP_VERSION"));
    tracing::info!(workers = args.workers);
    tracing::info!(input_base = args.input_base);
    tracing::info!(input_files = args.input_files.join(","));
    tracing::info!(output_files = args.output_files.join(","));
    tracing::info!(minimum_memory = args.minimum_memory.to_string());
    tracing::info!(cmd = args.cmd);
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

    let cancel_rx = setup_signal_handlers();

    let pool = get_pool(&args.db_url, args.workers as u32)?;

    tracing::info!("collecting files");
    let files: Vec<runner::data::structs::FileMeta> = runner::db::collect_files(pool.clone())?;
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

    let (tx, rx): (Sender<FileMeta>, Receiver<FileMeta>) = bounded(0);

    setup_send_files(files, tx, cancel_rx.clone())?;

    let mut handles: Vec<thread::JoinHandle<Result<u16, anyhow::Error>>> = Vec::new();

    for i in 0..args.workers {
        tracing::info!(i, "Starting worker");
        let rx = rx.clone();
        let progress = progress.clone();
        let failed_count = failed_count.clone();
        let skipped_count = skipped_count.clone();
        let eta_calculator = eta_calculator.clone();
        let active_workers = active_workers.clone();
        let last_worker_time = last_worker_time.clone();
        let args = args.clone();
        let worker_index = i;
        let cancel_rx = cancel_rx.clone();
        let input_files = args.input_files.clone();
        let output_files = args.output_files.clone();
        let pool = pool.clone();

        handles.push(thread::spawn(move || {
            for file in rx.iter() {
                let active_workers = active_workers.clone();

                let mut sys = System::new_with_specifics(
                    RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
                );

                let mut mem_waiting = args.slow_start;
                let mut sleep = false;

                loop {
                    if cancel_rx.try_recv().is_ok() {
                        return Ok(worker_index);
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

                if cancel_rx.try_recv().is_ok() {
                    return Ok(worker_index);
                }

                let params = runner::Params {
                    worker_index,
                    cmd: &args.cmd,
                    input_base_dir: &args.input_base,
                    input_files: &input_files,
                    output_files: &output_files,
                    file_meta: &file,
                    pool: pool.clone(),
                };

                tracing::debug!(file = file.path);
                active_workers.fetch_add(1, Ordering::SeqCst);
                {
                    let pb = progress.lock().unwrap();
                    let skipped = *skipped_count.lock().unwrap();
                    let failed = *failed_count.lock().unwrap();
                    let wrk_str =
                        format!("{}/{}", active_workers.load(Ordering::SeqCst), args.workers);
                    let eta_calculator = eta_calculator.lock().unwrap();
                    pb.set_message(
                        get_info_str(
                            true,
                            eta_calculator.completed() as i32,
                            skipped,
                            failed,
                            eta_calculator.remaining() as i32,
                            eta_calculator.speed_per_day(),
                        ) + format!(", ({}), wrk: {}", eta_calculator.eta_str(), wrk_str).as_str(),
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
                        if let Some(RunnerError::RecordNotFound { id: _, type_: _ }) =
                            err.downcast_ref::<RunnerError>()
                        {
                            tracing::warn!(file = file.path, "No initial file");
                        } else {
                            tracing::error!(
                                file = file.path,
                                err = %err,
                                "Error processing file"
                            );
                        }
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
                    get_info_str(
                        true,
                        eta_calculator.completed() as i32,
                        skipped,
                        failed,
                        eta_calculator.remaining() as i32,
                        eta_calculator.speed_per_day(),
                    ) + format!(", ({eta}), wrk: {wrk_str}").as_str(),
                );
                pb.inc(1);

                if !Term::stderr().is_term() {
                    let prc = if all > 0 {
                        (pb.position() as f32 / all as f32) * 100.0
                    } else {
                        0.0
                    };
                    tracing::info!(
                        msg = get_info_str(
                            false,
                            eta_calculator.completed() as i32,
                            skipped,
                            failed,
                            eta_calculator.remaining() as i32,
                            eta_calculator.speed_per_day(),
                        ),
                        wrk = wrk_str,
                        eta = eta.as_ref(),
                        all,
                        "%" = format!("{:.2}", prc),
                    );
                }
            }
            Ok(worker_index)
        }));
    }

    join_threads(handles)?;

    {
        let pb = progress.lock().unwrap();
        let skipped = *skipped_count.lock().unwrap();
        let failed = *failed_count.lock().unwrap();
        let eta_calculator = eta_calculator.lock().unwrap();
        let msg = format!(
            "{} - {}",
            Color::Green.bold().paint("Finished"),
            get_info_str(
                true,
                eta_calculator.completed() as i32,
                skipped,
                failed,
                eta_calculator.remaining() as i32,
                eta_calculator.speed_per_day(),
            )
        );
        pb.finish_with_message(msg);
        if !Term::stderr().is_term() {
            tracing::info!(
                msg = get_info_str(
                    false,
                    eta_calculator.completed() as i32,
                    skipped,
                    failed,
                    eta_calculator.remaining() as i32,
                    eta_calculator.speed_per_day(),
                ),
                all = pb.length(),
                "Finished",
            );
        }
    }

    tracing::info!("Runner finished");
    Ok(())
}

fn get_info_str(
    use_color: bool,
    ok: i32,
    skipped: i32,
    failed: i32,
    rem: i32,
    speed: f32,
) -> String {
    format!(
        "done: {}, skipped: {}, failed: {}, rem: {}, speed/d: {:.1}",
        paint(use_color, Color::Green, ok),
        paint(use_color, Color::Yellow, skipped),
        paint(use_color, Color::Red, failed),
        paint(use_color, Color::Green, rem),
        speed,
    )
}

fn paint(use_color: bool, colour: Color, v: i32) -> String {
    if use_color && v > 0 {
        return colour.bold().paint(format!("{v}")).to_string();
    }
    format!("{v}")
}

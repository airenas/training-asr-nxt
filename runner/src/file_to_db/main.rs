use std::{
    env,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use clap::Parser;
use crossbeam_channel::{bounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use runner::{
    data::structs::{File, FileMeta},
    utils::system::{join_threads, setup_send_files, setup_signal_handlers},
    APP_NAME,
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
    /// Input base dir
    #[arg(long, env, default_value = "")]
    input_base: String,
    /// Names
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    names: Vec<String>,
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
    tracing::info!(input_base = args.input_base);
    tracing::info!(names = args.names.join(","));
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

    let cancel_rx = setup_signal_handlers();

    let manager: PostgresConnectionManager<NoTls> =
        PostgresConnectionManager::new(args.db_url.as_str().parse()?, NoTls);
    let pool: Arc<Pool<PostgresConnectionManager<NoTls>>> = Arc::new(
        Pool::builder()
            .max_size(args.workers as u32)
            .build(manager)?,
    );

    tracing::info!("collecting files");
    let files = runner::db::collect_files(pool.clone())?;
    tracing::info!(len = files.len(), "files collected");

    let progress = Arc::new(Mutex::new(ProgressBar::new(files.len() as u64)));

    progress.lock().unwrap().set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ETA: {eta} {msg}",
        )
        .unwrap()
        .progress_chars("##-"),
    );

    let (tx, rx): (Sender<FileMeta>, Receiver<FileMeta>) = bounded(0);

    setup_send_files(files, tx, cancel_rx.clone())?;

    let mut handles: Vec<thread::JoinHandle<Result<u16, anyhow::Error>>> = Vec::new();
    let failed_count = Arc::new(Mutex::new(0));

    for i in 0..args.workers {
        tracing::info!(i, "Starting worker");
        let rx = rx.clone();
        let progress = progress.clone();
        let worker_index = i;
        let cancel_rx = cancel_rx.clone();
        let pool = pool.clone();
        let input_base = args.input_base.clone();
        let failed_count = failed_count.clone();
        let names = args.names.clone();

        handles.push(thread::spawn(move || {
            for file in rx.iter() {
                if cancel_rx.try_recv().is_ok() {
                    break;
                }

                for name in names.iter() {
                    let res: Result<(), anyhow::Error> = (|| {
                        let file_name = get_name(&file.path, &input_base, name);
                        tracing::debug!(wrk = worker_index, file = file_name, "Processing file");
                        let data = load(&file_name)?;

                        let file_data = File {
                            id: file.id.clone(),
                            type_: name.clone(),
                            data,
                        };

                        let mut conn = pool.get_timeout(Duration::from_secs(10))?;
                        runner::db::save(&mut conn, file_data)?;

                        Ok(())
                    })();
                    if let Err(e) = res {
                        tracing::error!(
                            worker_index,
                            file = file.path,
                            "Failed to process file: {}",
                            e
                        );
                        let mut failed = failed_count.lock().unwrap();
                        *failed += 1;
                    }
                }

                let pb = progress.lock().unwrap();
                pb.inc(1);
            }
            Ok(worker_index)
        }));
    }

    join_threads(handles)?;

    tracing::info!("closing connections");
    drop(pool);

    let failed = *failed_count.lock().unwrap();

    if failed > 0 {
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

fn load(file_name: &str) -> anyhow::Result<Vec<u8>> {
    let path = Path::new(file_name);
    if !path.exists() {
        return Err(anyhow::anyhow!("File not found: {file_name}"));
    }
    Ok(std::fs::read(path)?)
}

fn get_name(path: &str, prefix: &str, name: &str) -> String {
    let res = format!("{prefix}/{path}/{name}");
    res
}

use std::{
    env,
    fs::File,
    io::{BufWriter, Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use runner::APP_NAME;
use tokio::signal::unix::{signal, SignalKind};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use zip::{write::FileOptions, CompressionMethod, ZipWriter};

#[derive(Parser, Debug)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="Data zipper", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Input dir
    #[arg(long, env, default_value = "./input")]
    input: String,
    /// Output file
    #[arg(long, env, default_value = "audio.segments")]
    output_file: String,
    //File names to collects. Comma separated
    #[arg(long, env, value_delimiter = ',', default_value = "")]
    names: Vec<String>,
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
    tracing::info!(name = APP_NAME, "Starting zipper");
    tracing::info!(version = env!("CARGO_APP_VERSION"));
    tracing::info!(input = args.input);
    tracing::info!(names = args.names.join(","));
    tracing::info!(output_file = args.output_file);
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

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
    let files = runner::files::collect_all_files(&args.input, &args.names)?;
    tracing::info!(len = files.len(), "files collected");

    let input_path = PathBuf::from(&args.input)
        .canonicalize()?
        .as_os_str()
        .to_string_lossy().to_string();

    let progress = ProgressBar::new(files.len() as u64);

    progress.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ETA: {eta} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    let zip_file = File::create(&args.output_file)?;
    let writer = BufWriter::new(zip_file);
    let mut zip = ZipWriter::new(writer);

    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(CompressionMethod::Deflated);

    for f_name in files.iter() {
        let mut f = File::open(&f_name)?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;

        // Make the path relative to args.input
        let relative_path = f_name
            .strip_prefix(&input_path)
            .map_err(|_| {
                anyhow::anyhow!(
                    "File path is not under input directory: {}",
                    f_name.display()
                )
            })?
            .to_string_lossy();

        zip.start_file(relative_path, options)?;
        zip.write_all(&buffer)?;
        progress.inc(1);
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Zipping cancelled by user"));
        }
    }

    zip.finish()?;

    progress.finish_with_message("Zipping completed");

    tracing::info!("Zipper finished");
    Ok(())
}

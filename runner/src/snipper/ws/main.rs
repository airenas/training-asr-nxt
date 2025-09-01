use std::{env, net::SocketAddr};

use axum::{
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use clap::Parser;
use runner::{
    utils::{
        snipper::{concat_segments, Segment},
        system::setup_signal_handlers,
    },
    APP_NAME,
};
use tokio::task;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug, Clone)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="WAV files snipper", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Input base dir
    #[arg(long, env, default_value = "")]
    input_base: String,
    /// Port
    #[arg(long, env, default_value = "8000")]
    port: u16,
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

#[derive(Clone)]
struct AppState {
    input_base: String,
}

async fn main_int(args: Args) -> anyhow::Result<()> {
    tracing::info!(name = APP_NAME, "Starting snipper");
    tracing::info!(version = env!("CARGO_APP_VERSION"));
    tracing::info!(input_base = args.input_base);
    tracing::info!(port = args.port);
    let cwd = env::current_dir()?;
    tracing::info!(cwd = cwd.display().to_string());

    let cancel_rx = setup_signal_handlers();

    let state = AppState {
        input_base: args.input_base.clone(),
    };

    let app = Router::new()
        .route("/concat", post(concat_handler))
        .with_state(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = task::spawn_blocking(move || cancel_rx.recv()).await;
            tracing::info!("Shutdown signal received");
        })
        .await?;

    tracing::info!("finished");
    Ok(())
}

async fn concat_handler(
    State(state): State<AppState>,
    Json(mut segments): Json<Vec<Segment>>,
) -> Response {
    for seg in &mut segments {
        seg.file = format!("{}/{}", state.input_base, seg.file);
    }

    match concat_segments(&segments) {
        Ok(bytes) => (
            [
                ("Content-Type", "audio/wav"),
                ("Content-Disposition", "attachment; filename=\"out.wav\""),
            ],
            bytes,
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Error in concat_segments: {}", e);
            (axum::http::StatusCode::BAD_REQUEST, format!("error: {e}")).into_response()
        },
    }
}

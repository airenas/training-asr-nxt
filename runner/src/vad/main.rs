// Copyright (c) 2026 Xiaomi Corporation
//
// This file demonstrates how to use silero VAD with sherpa-onnx's
// Rust API to remove non-speech segments and save speech-only audio.
//
// See ../README.md for how to run it

use std::{
    fs::File,
    io::{self, BufRead, Write},
};

use clap::Parser;
use runner::APP_NAME;
use serde::{Deserialize, Serialize};
use sherpa_onnx::{SileroVadModelConfig, VadModelConfig, VoiceActivityDetector, Wave};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(version = env!("CARGO_APP_VERSION"), name = APP_NAME, about="VAD worker", 
    long_about = None, author="Airenas V.<airenass@gmail.com>")]
struct Args {
    /// Input model - silero vad ONNX model
    /// You can download it from
    /// curl -SL -O https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/silero_vad.onnx
    #[arg(long, env, default_value = "./silero_vad.onnx")]
    input_model: String,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::Layer::default().compact())
        .init();
    let args = Args::parse();
    if let Err(e) = main_int(args) {
        tracing::error!("{}", e);
        return Err(e);
    }
    Ok(())
}

#[derive(Deserialize, Debug)]
struct Task {
    input_wav: Option<String>,
    output: Option<String>,
}

#[derive(Serialize, Debug)]
struct Segment {
    from: f32,
    to: f32,
}

fn main_int(args: Args) -> anyhow::Result<()> {
    tracing::info!(name = APP_NAME, "Starting runner");
    tracing::info!(version = env!("CARGO_APP_VERSION"));
    tracing::info!(input_model = args.input_model);

    let mut silero_config = SileroVadModelConfig::default();
    silero_config.model = Some(args.input_model);

    silero_config.threshold = 0.5;
    silero_config.min_silence_duration = 0.5;
    silero_config.min_speech_duration = 0.1;
    silero_config.max_speech_duration = 20.0;

    let sample_rate = 16000;
    let vad_config = VadModelConfig {
        silero_vad: silero_config,
        ten_vad: Default::default(),
        sample_rate,
        num_threads: 1,
        provider: Some("cpu".to_string()),
        debug: false,
    };

    let vad = VoiceActivityDetector::create(&vad_config, 30.0)
        .expect("Failed to create VoiceActivityDetector");

    let stdin = io::stdin();
    let reader = stdin.lock();

    tracing::info!("loaded model, waiting for tasks");

    let mut task_count = 0;

    for line in reader.lines() {
        match line {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                task_count += 1;

                let result: Result<(), String> = (|| {
                    let task: Task =
                        serde_json::from_str(&line).map_err(|e| format!("Invalid JSON: {}", e))?;

                    let input_wav = task.input_wav.ok_or("missing input_wav")?;
                    let output = task.output.ok_or("missing output")?;

                    tracing::info!("Input wav file: {}", input_wav);
                    tracing::info!("Output file: {}", output);
                    tracing::info!(task = task_count, "task");

                    let res =
                        process_model(&vad, &input_wav).map_err(|e| format!("process: {}", e))?;

                    let mut file =
                        File::create(&output).map_err(|e| format!("File create: {}", e))?;

                    let line = serde_json::to_string(&res)
                        .map_err(|e| format!("JSON serialize: {}", e))?;
                    file.write(line.as_bytes())
                        .map_err(|e| format!("write: {}", e))?;
                    tracing::info!("written all segments");
                    Ok(())
                })();

                match result {
                    Ok(_) => {
                        println!("wrk-res: ok");
                    }
                    Err(e) => {
                        tracing::error!(e, "faied");
                        let e = e.replace('\n', " ").replace('\r', " ");
                        print!("wrk-res: error: {}", e);
                    }
                }
                io::stdout().flush().ok();
            }
            Err(e) => {
                tracing::error!("Failed to read line: {}", e);
            }
        }
    }
    Ok(())
}

fn process_model(vad: &VoiceActivityDetector, input_wav: &str) -> anyhow::Result<Vec<Segment>> {
    let wave = Wave::read(input_wav)
        .ok_or_else(|| anyhow::anyhow!("Failed to read WAV file: {}", input_wav))?;
    let sample_rate = wave.sample_rate();
    if sample_rate != 16000 {
        return Err(anyhow::anyhow!(
            "Expected sample rate of 16000, but got {}",
            sample_rate
        ));
    }
    let mut speech_segments = Vec::new();
    const WINDOW_SIZE: usize = 512;

    vad.reset();

    for chunk in wave.samples().chunks(WINDOW_SIZE) {
        vad.accept_waveform(chunk);

        while let Some(seg) = vad.front() {
            speech_segments.push(Segment {
                from: seg.start() as f32 / sample_rate as f32,
                to: (seg.start() + seg.n()) as f32 / sample_rate as f32,
            });
            vad.pop();
        }
    }

    vad.flush();
    while let Some(seg) = vad.front() {
        speech_segments.push(Segment {
            from: seg.start() as f32 / sample_rate as f32,
            to: (seg.start() + seg.n()) as f32 / sample_rate as f32,
        });
        vad.pop();
    }

    Ok(speech_segments)
}

use std::io::Cursor;

use anyhow::Context;
use hound::{WavReader, WavWriter};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Segment {
    pub file: String,
    pub from: f64,
    pub to: f64,
}

pub fn concat_segments(segments: &[Segment]) -> anyhow::Result<Vec<u8>> {
    if segments.is_empty() {
        return Err(anyhow::anyhow!("No segments provided"));
    }

    let first_reader = WavReader::open(&segments[0].file)
        .context(format!("open first segment: file {}", segments[0].file))?;
    let spec = first_reader.spec();

    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    let mut writer = WavWriter::new(&mut cursor, spec)?;

    let silence_samples = spec.sample_rate as u64 * spec.channels as u64;

    for (i, seg) in segments.iter().enumerate() {
        tracing::info!(segment= seg.file, from = seg.from, to = seg.to, "add");
        let mut reader = WavReader::open(&seg.file)?;
        let sr = reader.spec().sample_rate as f64;
        let channels = reader.spec().channels as u64;
        let start_sample = (seg.from * sr).floor() as u64 * channels;
        let end_sample = (seg.to * sr).ceil() as u64 * channels;

        for (idx, s) in reader.samples::<i16>().enumerate() {
            let s = s?;
            let idx = idx as u64;
            if idx >= start_sample && idx < end_sample {
                writer.write_sample(s)?;
            }
            if idx >= end_sample {
                break;
            }
        }

        if i < segments.len() - 1 {
            for _ in 0..silence_samples {
                writer.write_sample(0i16)?;
            }
        }
    }

    writer.finalize()?;
    Ok(buffer)
}

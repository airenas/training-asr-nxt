use std::{collections::BTreeMap, io::Cursor};

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

    let mut segments_by_file: BTreeMap<&str, Vec<&Segment>> = BTreeMap::new();
    for seg in segments {
        segments_by_file.entry(&seg.file).or_default().push(seg);
    }

    let first_reader = WavReader::open(&segments[0].file)
        .context(format!("open first segment: file {}", segments[0].file))?;
    let spec = first_reader.spec();

    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    let mut writer = WavWriter::new(&mut cursor, spec)?;

    let silence_samples = spec.sample_rate as u64 * spec.channels as u64;

    for (file, segs) in segments_by_file {
        tracing::info!(segment = file, "start");
        let mut reader = WavReader::open(file)?;
        let sr = reader.spec().sample_rate as f64;
        let channels = reader.spec().channels as u64;

        let mut seg_idx = 0;
        let mut current_seg = segs[seg_idx];
        let mut start_sample = (current_seg.from * sr).floor() as u64 * channels;
        let mut end_sample = (current_seg.to * sr).ceil() as u64 * channels;

        tracing::info!(from = current_seg.from, to = current_seg.to, "add");
        
        for (idx, s) in reader.samples::<i16>().enumerate() {
            let s = s?;
            let idx = idx as u64;
            if idx >= start_sample && idx < end_sample {
                writer.write_sample(s)?;
            }
            if idx >= end_sample {
                for _ in 0..silence_samples {
                    writer.write_sample(0i16)?;
                }
                seg_idx += 1;
                if seg_idx >= segs.len() {
                    break;
                }
                current_seg = segs[seg_idx];
                start_sample = (current_seg.from * sr).floor() as u64 * channels;
                end_sample = (current_seg.to * sr).ceil() as u64 * channels;
                tracing::info!(from = current_seg.from, to = current_seg.to, "add");
            }
        }
    }

    writer.finalize()?;
    Ok(buffer)
}

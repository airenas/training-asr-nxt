use std::{fs, path::PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Cache {
    files: Vec<PathBuf>,
}

pub fn load_cache(
    cache_file: &str,
) -> anyhow::Result<Vec<PathBuf>> {
    if let Ok(cache_content) = fs::read_to_string(cache_file) {
        if let Ok(cache) = serde_json::from_str::<Cache>(&cache_content) {
            tracing::info!(cache_file, "Loaded file list from cache");
            return Ok(cache.files);
        }
    }
    tracing::info!("Cache file not found or invalid");
    Ok(vec![])
}

pub fn save_cache(
    cache_file: &str,
    files : &[PathBuf],
) -> anyhow::Result<()> {
    let copy = files.to_vec();
    let cache = Cache { files: copy };
    let cache_content = serde_json::to_string(&cache)?;
    fs::write(cache_file, cache_content)?;
    tracing::info!("File list cached to: {}", cache_file);

    Ok(())
}
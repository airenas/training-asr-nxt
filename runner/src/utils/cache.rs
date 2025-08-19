use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
};

pub fn load_cache(cache_file: &str) -> anyhow::Result<Vec<PathBuf>> {
    if let Ok(cache_content) = fs::read_to_string(cache_file) {
        let files = cache_content.lines().map(PathBuf::from).collect();
        tracing::info!(cache_file, "Loaded file list from cache");
        return Ok(files);
    }
    tracing::info!("Cache file not found or invalid");
    Ok(vec![])
}

pub fn save_cache(cache_file: &str, files: &[PathBuf]) -> anyhow::Result<()> {
    let mut file = File::create(cache_file)?;
    for path in files {
        let _ = file.write(format!("{}\n", path.display()).as_bytes())?;
    }
    tracing::info!("File list cached to: {}", cache_file);

    Ok(())
}

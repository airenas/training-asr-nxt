use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{self};
use path_absolutize::Absolutize;
// use walkdir::WalkDir;
use jwalk::WalkDir;
use tempfile::TempDir;

use crate::{
    data::structs::File,
    db::{self, exists, load},
    Params, ProcessStatus,
};

pub fn collect_all_files(
    in_dir: &str,
    names: &[String],
    cancel_flag: Arc<AtomicBool>,
) -> anyhow::Result<Vec<PathBuf>> {
    let l_names = names
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let mut raw_files = Vec::new();
    for entry in WalkDir::new(in_dir).parallelism(jwalk::Parallelism::Serial) {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                tracing::error!("walk error: {}", err);
                continue;
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        if !has_name(&entry.path(), &l_names) {
            continue;
        }
        raw_files.push(entry.path().to_path_buf());
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Zipping cancelled by user"));
        }
    }

    tracing::info!(len = raw_files.len(), "total matching files");
    let mut files = Vec::with_capacity(raw_files.len());
    for entry in raw_files {
        match entry.absolutize() {
            Ok(path) => files.push(path.to_path_buf()),
            Err(err) => tracing::error!("skip {}: {}", entry.display(), err),
        }
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Zipping cancelled by user"));
        }
    }

    for n in &l_names {
        tracing::info!(
            suffix = n,
            count = files.iter().filter(|f| check_suffix(f, n)).count(),
            "with suffix"
        );
    }
    files.sort();
    tracing::debug!("Sorted");

    Ok(files)
}

fn check_suffix(e: &Path, suffix: &str) -> bool {
    if suffix.is_empty() {
        return true;
    }
    let file_name = e
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    file_name.ends_with(suffix)
}

fn has_name(e: &Path, suffix: &[String]) -> bool {
    let file = e.file_name().and_then(|s| s.to_str()).unwrap_or("");
    suffix.iter().any(|s| s == file)
}

pub fn run(params: &mut Params) -> anyhow::Result<ProcessStatus> {
    tracing::trace!(file = params.file_meta.path);

    if params.output_files.is_empty() {
        return Err(anyhow::anyhow!(
            "No output files specified, skipping command execution"
        ));
    }

    let mut conn = params.pool.get_timeout(Duration::from_secs(10))?;
    if let Some(file) = params.output_files.first() {
        if exists(&mut conn, &params.file_meta.id, file)? {
            tracing::trace!(file = file, "File already exists");
            return Ok(ProcessStatus::Skipped);
        }
    }

    let dir = TempDir::new()?;

    for file in params.input_files.iter() {
        let file_name = dir.path().join(file);
        let data = load(&mut conn, &params.file_meta.id, file)?;
        std::fs::write(file_name, data.data)?;
    }

    drop(conn);

    let mut input_file = PathBuf::new();
    if !params.input_files.is_empty() {
        input_file = dir.path().join(params.input_files[0].as_str());
    }
    let output_file = dir.path().join(params.output_files[0].as_str());

    let res = (params.run_f)(
        params.cmd,
        params.input_base_dir,
        &input_file,
        &output_file,
        &params.file_meta.path,
    )?;

    tracing::trace!(file = params.file_meta.path, out = res, "Command finished");

    let mut conn = params.pool.get_timeout(Duration::from_secs(10))?;

    for (idx, file) in params.output_files.iter().enumerate() {
        let file_name = dir.path().join(file);
        let data = std::fs::read(file_name);
        match data {
            Ok(data) => {
                let file_data = File {
                    id: params.file_meta.id.clone(),
                    type_: file.to_string(),
                    data,
                };
                db::save(&mut conn, file_data)?;
            }
            Err(e) => {
                if idx == 0 {
                    return Err(anyhow::anyhow!("Failed to read input file: {}", e));
                }
                tracing::warn!(file = file, "Failed to read output file: {}", e);
            }
        }
    }

    Ok(ProcessStatus::Success)
}

pub fn run_cmd(
    cmd: &str,
    input_base_dir: &str,
    input_file: &Path,
    output_file: &Path,
    file_name: &str,
) -> anyhow::Result<String> {
    if !(cmd.contains("{input}") || cmd.contains("{input_wav}")) || !cmd.contains("{output}") {
        return Err(anyhow::anyhow!("Command does not contain placeholders"));
    }

    let audio_wav = make_audio_name(input_base_dir, file_name);

    let parts = cmd.split_whitespace();
    let parts: Vec<String> = parts
        .map(|part| {
            part.replace("{input}", input_file.to_string_lossy().as_ref())
                .replace("{input_wav}", audio_wav.to_string_lossy().as_ref())
                .replace("{output}", output_file.to_string_lossy().as_ref())
        })
        .collect();

    let program = parts
        .first()
        .ok_or_else(|| anyhow::anyhow!("Empty command"))?;
    let args = &parts[1..];

    tracing::trace!(program = program, args = ?args, "Running command");

    let output = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;

    if !output.stdout.is_empty() {
        tracing::trace!(
            "Command stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "Command failed with status: {}, {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(output_file.to_string_lossy().to_string())
}

pub fn save(data: Vec<u8>, file_name: String) -> anyhow::Result<()> {
    let path = Path::new(&file_name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, data)?;
    Ok(())
}

pub fn make_audio_name(audio_base: &str, path: &str) -> PathBuf {
    PathBuf::from(audio_base).join(path).join("audio.16.wav")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_suffix() {
        // Define test cases
        let test_cases = vec![
            // Test case: No extensions provided, should return true
            ("", "file.txt", true),
            // Test case: Matching extension
            ("txt", "file.txt", true),
            // Test case: Non-matching extension
            ("jpg", "file.txt", false),
            // Test case: Multiple extensions, one matches
            ("file.txt", "file.txt", true),
            // Test case: File with no extension
        ];

        for (extensions, file_name, expected) in test_cases {
            let path = std::path::Path::new(file_name);
            let result = check_suffix(path, extensions);
            assert_eq!(
                result, expected,
                "Failed for extensions: {extensions:?}, file_name: {file_name}"
            );
        }
    }
}

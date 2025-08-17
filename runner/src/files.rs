use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio}, sync::{atomic::{AtomicBool, Ordering}, Arc},
};

use anyhow::{self};
use path_absolutize::Absolutize;
// use walkdir::WalkDir;
use jwalk::{WalkDir};

use crate::{
    utils::cache::{load_cache, save_cache},
    Params, ProcessStatus,
};

pub fn collect_files(
    in_dir: &str,
    extensions: &[String],
    cache_file: &str,
) -> anyhow::Result<Vec<PathBuf>> {
    tracing::trace!(in_dir = in_dir, extensions = ?extensions, cache_file = cache_file);
    let res = load_cache(cache_file)?;
    if !res.is_empty() {
        tracing::trace!("Loaded file list from cache");
        return Ok(res);
    }

    let l_ext = extensions
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect::<Vec<String>>();
    let (suffix, extensions) = if !l_ext.is_empty() {
        (l_ext[0].clone(), l_ext[1..].to_vec())
    } else {
        (String::new(), Vec::new())
    };

    let mut files: Vec<PathBuf> = WalkDir::new(in_dir).parallelism(jwalk::Parallelism::Serial)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| check_suffix(&e.path(), &suffix))
        .filter(|e| check_extensions(&e.path(), &extensions))
        .filter_map(|e| {
            e.path()
                .canonicalize()
                .map_err(|err| {
                    tracing::error!("skip {}: {}", e.path().display(), err);
                })
                .ok()
        })
        .collect();
    tracing::debug!(len = files.len(), "Found files in directory");
    files.sort();
    tracing::debug!("Sorted");

    save_cache(cache_file, &files)?;
    Ok(files)
}

pub fn collect_all_files(in_dir: &str, names: &[String], cancel_flag: Arc<AtomicBool>) -> anyhow::Result<Vec<PathBuf>> {
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

fn check_extensions(e: &Path, extensions: &[String]) -> bool {
    if extensions.is_empty() {
        return true;
    }
    let dir = match e.parent() {
        Some(d) => d,
        None => return false,
    };
    let entries = std::fs::read_dir(dir);
    let files = match entries {
        Ok(entries) => entries,
        Err(e) => {
            tracing::error!("Failed to read directory: {}: {}", dir.to_str().unwrap(), e);
            return false;
        }
    };

    for f in files.flatten() {
        if let Some(name) = f.file_name().to_str().map(|s| s.to_lowercase()) {
            if f.file_type().map(|ft| ft.is_file()).unwrap_or(false)
                && extensions.iter().any(|ext| name.ends_with(ext))
            {
                return true;
            }
        }
    }
    false
}

pub fn run(params: &Params) -> anyhow::Result<ProcessStatus> {
    let output_file = get_cache_file_name(
        params.file_name,
        params.input_dir,
        params.output_dir,
        params.result_file_name,
        params.same_dir,
    )?;
    tracing::trace!(file = params.file_name, out = output_file.to_str());
    if output_file.exists() {
        tracing::trace!(file = output_file.to_str(), "File already exists");
        return Ok(ProcessStatus::Skipped);
    }
    // create directory if it does not exist
    if let Some(parent) = output_file.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
            tracing::trace!(file = parent.to_str(), "Directory created");
        }
    }
    let res_tmp = run_cmd(params.cmd, params.input_dir, &output_file, params.file_name)?;

    tracing::trace!(file = params.file_name, out = res_tmp, "Command finished");
    let res = std::fs::rename(res_tmp, &output_file);
    if let Err(e) = res {
        tracing::error!(
            file = params.file_name,
            out = output_file.to_str(),
            "Error renaming file"
        );
        return Err(anyhow::anyhow!("Error renaming file: {}", e));
    }
    tracing::trace!(
        file = params.file_name,
        out = output_file.to_str(),
        "File renamed"
    );
    Ok(ProcessStatus::Success)
}

fn run_cmd(
    cmd: &str,
    _input_dir: &str,
    output_file: &Path,
    file_name: &str,
) -> anyhow::Result<String> {
    if !cmd.contains("{input}") && !cmd.contains("{output}") {
        return Err(anyhow::anyhow!("Command does not contain placeholders"));
    }

    let output_tmp_file = format!("{}.{}", output_file.to_str().unwrap(), "tmp");

    let parts = cmd.split_whitespace();
    let parts: Vec<String> = parts
        .map(|part| {
            part.replace("{input}", file_name)
                .replace("{output}", output_tmp_file.as_str())
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

    Ok(output_tmp_file)
}

fn get_cache_file_name(
    file_path: &str,
    input_dir: &str,
    output_dir: &str,
    name: &str,
    same_dir: bool,
) -> anyhow::Result<PathBuf> {
    let abs_input_dir = Path::new(input_dir)
        .absolutize()
        .map_err(|e| anyhow::anyhow!("Failed to canonicalize input_dir: {}", e))?;
    let abs_file_path = Path::new(file_path)
        .absolutize()
        .map_err(|e| anyhow::anyhow!("Failed to canonicalize file_path: {}", e))?;
    let abs_output_dir = Path::new(output_dir)
        .absolutize()
        .map_err(|e| anyhow::anyhow!("Failed to canonicalize output_dir: {}", e))?;

    let relative_path = abs_file_path
        .as_ref()
        .strip_prefix(abs_input_dir.as_ref())
        .map_err(|e| anyhow::anyhow!("Failed to calculate relative path: {}", e))?;

    let mut output_file_dir = abs_output_dir.join(relative_path);
    if same_dir {
        output_file_dir = output_file_dir.parent().unwrap().to_path_buf();
    }
    let cache_file_path = output_file_dir.join(name);
    Ok(cache_file_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cache_file_name() {
        tracing_subscriber::fmt().with_test_writer().init();
        // Define test cases
        let test_cases = vec![
            // Test case: simple
            (
                "/input/file.m4a",
                "/input",
                "/output",
                "result.txt",
                false,
                "/output/file.m4a/result.txt",
            ),
            (
                "/input/file.m4a",
                "/input",
                "/output",
                "result.txt",
                true,
                "/output/result.txt",
            ),
            (
                "/input/olia/file.m4a",
                "/input",
                "/output",
                "result.txt",
                false,
                "/output/olia/file.m4a/result.txt",
            ),
            (
                "/input/olia/file.m4a",
                "/input",
                "/output",
                "result.txt",
                true,
                "/output/olia/result.txt",
            ),
        ];

        for (file_path, input_dir, output_dir, name, same_dir, expected) in test_cases {
            let result =
                get_cache_file_name(file_path, input_dir, output_dir, name, same_dir).unwrap();
            assert_eq!(result.to_str().unwrap(), expected);
        }
    }

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
            let result = check_suffix(path, &extensions);
            assert_eq!(
                result, expected,
                "Failed for extensions: {:?}, file_name: {}",
                extensions, file_name
            );
        }
    }
}

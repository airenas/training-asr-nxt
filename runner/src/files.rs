use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{self, Ok};
use path_absolutize::Absolutize;
use walkdir::WalkDir;

use crate::{Params, ProcessStatus};

pub fn collect_files(in_dir: &str) -> anyhow::Result<Vec<PathBuf>> {
    let files: Vec<PathBuf> = WalkDir::new(in_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.path().canonicalize().ok().unwrap())
        .collect();
    Ok(files)
}

pub fn run(params: &Params) -> anyhow::Result<ProcessStatus> {
    let output_file = get_cache_file_name(
        params.file_name,
        params.input_dir,
        params.output_dir,
        params.result_file_name,
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

    let output_file_dir = abs_output_dir.join(relative_path);
    let cache_file_path = output_file_dir.join(name);
    Ok(cache_file_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cache_file_name() {
        tracing_subscriber::fmt()
            .with_test_writer() // Ensures logs are captured during tests
            .init();

        let input_dir = "../input";
        let output_dir = "../output";
        let file_path = "../input/1/file.txt";
        let name = "cache.json";

        // Call the function
        let result = get_cache_file_name(file_path, input_dir, output_dir, name);

        assert!(result.is_ok());
        let cache_file_path = result.unwrap();

        let cache_file_path = cache_file_path.to_str().unwrap();

        let current_dir = std::env::current_dir().unwrap();

        let expected_output_file_dir = format!(
            "{}/output/1/file.txt/cache.json",
            current_dir.parent().unwrap().display()
        );

        assert_eq!(cache_file_path, expected_output_file_dir);
    }
}

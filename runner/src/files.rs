use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{self, Ok};
use path_absolutize::Absolutize;
use walkdir::WalkDir;

use crate::{Params, ProcessStatus};

pub fn collect_files(in_dir: &str, extensions: &[String]) -> anyhow::Result<Vec<PathBuf>> {
    let l_ext = extensions
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect::<Vec<String>>();
    
    let files: Vec<PathBuf> = WalkDir::new(in_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            check_extension(e.path(), &l_ext)
        })  
        .map(|e| e.path().canonicalize().ok().unwrap())
        .collect();
    Ok(files)
}

fn check_extension(e: &Path, extensions: &[String]) -> bool {
    if extensions.is_empty() {
        return true;
    }
    let ext = e.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
    extensions.iter().any(|ext2| &ext == ext2)
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
    same_dir: bool
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
        tracing_subscriber::fmt()
            .with_test_writer() 
            .init();
        // Define test cases
        let test_cases = vec![
            // Test case: simple
            ("/input/file.m4a", "/input", "/output", "result.txt", false, "/output/file.m4a/result.txt"),
            ("/input/file.m4a", "/input", "/output", "result.txt", true, "/output/result.txt"),
            ("/input/olia/file.m4a", "/input", "/output", "result.txt", false, "/output/olia/file.m4a/result.txt"),
            ("/input/olia/file.m4a", "/input", "/output", "result.txt", true, "/output/olia/result.txt"),
        ];

        for (file_path, input_dir, output_dir, name, same_dir, expected) in test_cases {
            let result = get_cache_file_name(file_path, input_dir, output_dir, name, same_dir).unwrap();
            assert_eq!(result.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_check_extension() {
        // Define test cases
        let test_cases = vec![
            // Test case: No extensions provided, should return true
            (vec![], "file.txt", true),
            // Test case: Matching extension
            (vec!["txt".to_string()], "file.txt", true),
            // Test case: Non-matching extension
            (vec!["jpg".to_string()], "file.txt", false),
            // Test case: Multiple extensions, one matches
            (vec!["jpg".to_string(), "txt".to_string()], "file.txt", true),
            // Test case: File with no extension
            (vec!["txt".to_string()], "file", false),
            // Test case: Empty file name
            (vec!["txt".to_string()], "", false),
        ];

        for (extensions, file_name, expected) in test_cases {
            let path = std::path::Path::new(file_name);
            let result = check_extension(path, &extensions);
            assert_eq!(
                result, expected,
                "Failed for extensions: {:?}, file_name: {}",
                extensions, file_name
            );
        }
    }
}

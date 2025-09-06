use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

use crate::files::make_audio_name;
use anyhow::Context;
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Task {
    input_wav: String,
    input: String,
    output: String,
}

pub struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    done: i32,
}

impl Worker {
    pub fn new(cmd: &str) -> anyhow::Result<Self> {
        let parts = cmd
            .split_whitespace()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let program = parts
            .first()
            .ok_or_else(|| anyhow::anyhow!("Empty command"))?;
        let args = &parts[1..];

        tracing::trace!(program = program, args = ?args, "Running command");
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("Spawn Python worker")?;

        let stdin = child.stdin.take().context("Open stdin")?;
        let stdout = BufReader::new(child.stdout.take().context("Open stdout")?);

        Ok(Worker {
            child,
            stdin,
            stdout,
            done: 0,
        })
    }

    pub fn run_task(
        &mut self,
        _: &str,
        input_base_dir: &str,
        input_file: &Path,
        output_file: &Path,
        file_name: &str,
    ) -> anyhow::Result<String> {
        self.done += 1;
        let audio_wav = make_audio_name(input_base_dir, file_name);
        let task = Task {
            input_wav: audio_wav.to_string_lossy().to_string(),
            input: input_file.to_string_lossy().to_string(),
            output: output_file.to_string_lossy().to_string(),
        };

        let json = serde_json::to_string(&task)?;
        writeln!(self.stdin, "{}", json)?;
        self.stdin.flush()?;

        let mut response = String::new();
        self.stdout.read_line(&mut response)?;
        let res = response.trim().to_string();
        if res != "ok" {
            return Err(anyhow::anyhow!("Worker failed: {}", res));
        }
        Ok(output_file.to_string_lossy().to_string())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let id = self.child.id();
        tracing::debug!(pid = id, "Stopping worker");

        let pid = Pid::from_raw(id as i32);
        let _ = kill(pid, Signal::SIGTERM);
        let _ = self.child.wait();
    }
}

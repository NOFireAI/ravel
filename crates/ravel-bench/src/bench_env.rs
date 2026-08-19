//! Environment provenance header for the codec bake-off bins
//! (`src/bin/codec_bakeoff.rs`, `src/bin/ts_bakeoff.rs`). A throughput number
//! is meaningless without the machine that produced it, so both bins print
//! this block first and the captured report in `bench/reports/` keeps it
//! (ADR-0075 decision 3's "a number without provenance is not evidence",
//! applied to a local measurement rather than a published S3 one).
//!
//! Best-effort: every field falls back to `"unknown"` rather than failing, so
//! a bake-off still runs on a host missing `/proc` or `uname`. Subprocess and
//! `/proc` reads live here, off the library's deterministic path, exactly as
//! `bench_report`'s provenance helpers do.
#![allow(clippy::expect_used)]

use std::process::Command;

/// Trimmed stdout of `program args`, or `None` on any failure.
fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// The commit these numbers describe. `GITHUB_SHA` (set by CI) wins so a report
/// from a detached HEAD still names the branch tip; otherwise ask git.
pub fn git_commit() -> String {
    if let Ok(sha) = std::env::var("GITHUB_SHA")
        && !sha.trim().is_empty()
    {
        return sha.trim().to_string();
    }
    command_stdout("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string())
}

fn rustc_version() -> String {
    command_stdout("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string())
}

/// First `model name` line of `/proc/cpuinfo`, the human CPU name.
fn cpu_model() -> String {
    let Ok(text) = std::fs::read_to_string("/proc/cpuinfo") else {
        return "unknown".to_string();
    };
    for line in text.lines() {
        if let Some((key, value)) = line.split_once(':')
            && key.trim() == "model name"
        {
            return value.trim().to_string();
        }
    }
    "unknown".to_string()
}

fn core_count() -> String {
    std::thread::available_parallelism()
        .map(|n| n.get().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// `MemTotal` from `/proc/meminfo`, rendered in GiB.
fn total_memory() -> String {
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return "unknown".to_string();
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            if let Some(kb_str) = rest.split_whitespace().next()
                && let Ok(kb) = kb_str.parse::<f64>()
            {
                return format!("{:.1} GiB", kb / (1024.0 * 1024.0));
            }
            return "unknown".to_string();
        }
    }
    "unknown".to_string()
}

fn os_string() -> String {
    command_stdout("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string())
}

/// The multi-line environment block printed at the top of each bake-off
/// report. `title` names which bake-off produced the numbers that follow.
pub fn env_header(title: &str) -> String {
    let mut out = String::new();
    out.push_str("========================================================================\n");
    out.push_str(&format!("{title}\n"));
    out.push_str("========================================================================\n");
    out.push_str(&format!("cpu:     {}\n", cpu_model()));
    out.push_str(&format!("cores:   {}\n", core_count()));
    out.push_str(&format!("memory:  {}\n", total_memory()));
    out.push_str(&format!("os:      {}\n", os_string()));
    out.push_str(&format!("rustc:   {}\n", rustc_version()));
    out.push_str(&format!("commit:  {}\n", git_commit()));
    out.push_str("========================================================================\n");
    out
}

//! `ravel-cli` as a subprocess: the seeded load, the on-demand fold,
//! compaction and sweep, and the decoders the audit reads objects through.
//! Every call carries the store and tenancy flags the servers run with and
//! is bounded by coreutils `timeout`.

use std::process::Command;

pub struct Output {
    /// The exit code; `None` when the process could not run or was killed.
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    pub fn ok(&self) -> bool {
        self.code == Some(0)
    }

    /// One line for the logs and assertion details.
    pub fn summary(&self) -> String {
        let tail = |s: &str| {
            let s = s.trim();
            let start = s.len().saturating_sub(400);
            let mut i = start;
            while !s.is_char_boundary(i) {
                i += 1;
            }
            s[i..].to_string()
        };
        format!(
            "exit {:?}; stderr: {}",
            self.code,
            tail(&self.stderr).replace('\n', " | ")
        )
    }
}

pub struct Cli {
    pub bin: String,
}

impl Cli {
    pub fn from_env() -> Cli {
        Cli {
            bin: std::env::var("RAVEL_CLI").unwrap_or_else(|_| "ravel-cli".to_string()),
        }
    }

    /// Run `ravel-cli --store s3 --tenant-hash-unkeyed <args>` for at most
    /// `timeout_s` seconds.
    pub fn run(&self, args: &[&str], timeout_s: u32) -> Output {
        self.run_env(args, timeout_s, &[])
    }

    /// [`Cli::run`] with `env` set on the process, such as a libfaketime
    /// timestamp file of its own.
    pub fn run_env(&self, args: &[&str], timeout_s: u32, env: &[(&str, &str)]) -> Output {
        let out = Command::new("timeout")
            .arg(timeout_s.to_string())
            .arg(&self.bin)
            .args(["--store", "s3", "--tenant-hash-unkeyed"])
            .args(args)
            .envs(env.iter().copied())
            .output();
        match out {
            Ok(o) => Output {
                code: o.status.code(),
                stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
            },
            Err(e) => Output {
                code: None,
                stdout: String::new(),
                stderr: format!("running {}: {e}", self.bin),
            },
        }
    }
}

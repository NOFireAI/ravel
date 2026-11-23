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

/// Where `ravel-cli-ant` writes its SDK output in the local loop;
/// scripts/local-run.sh collects it as `sdk-cli.jsonl`. In Antithesis the
/// SDK's voidstar handler takes precedence over the variable.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the reshard profile is the first caller")
)]
pub const ANT_SDK_OUTPUT: &str = "/tmp/sdk-cli.jsonl";

/// `ravel-cli-ant`: ravel-cli built with its `antithesis` feature
/// (`ravel/Dockerfile`, stage `cli-ant`), for `provision reshard`, whose
/// generation-append assertions only ravel-cli reaches. Every run writes the
/// SDK output to [`ANT_SDK_OUTPUT`], never to the driver's own file: the
/// CLI's catalog declares every area's assertions, and local-run.sh reads
/// the driver's file as the driver's own.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the reshard profile is the first caller")
)]
pub struct AntCli {
    cli: Cli,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the reshard profile is the first caller")
)]
impl AntCli {
    pub fn new() -> AntCli {
        AntCli {
            cli: Cli {
                bin: "ravel-cli-ant".to_string(),
            },
        }
    }

    /// [`Cli::run`] on `ravel-cli-ant`, its SDK output in [`ANT_SDK_OUTPUT`].
    pub fn run(&self, args: &[&str], timeout_s: u32) -> Output {
        self.cli.run_env(
            args,
            timeout_s,
            &[("ANTITHESIS_SDK_LOCAL_OUTPUT", ANT_SDK_OUTPUT)],
        )
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn ant_cli_is_the_binary_the_driver_image_installs() {
        // driver/Dockerfile copies it to /usr/local/bin/ravel-cli-ant.
        assert_eq!(AntCli::new().cli.bin, "ravel-cli-ant");
    }

    #[test]
    fn ant_cli_writes_its_own_sdk_file_and_the_stock_cli_does_not() {
        let dir = std::env::temp_dir().join(format!("cli-ant-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        // A stand-in binary that prints the SDK output variable it was given.
        let bin = dir.join("print-sdk-output");
        std::fs::write(
            &bin,
            "#!/bin/sh\nprintf '%s' \"${ANTITHESIS_SDK_LOCAL_OUTPUT-unset}\"\n",
        )
        .expect("write the stand-in");
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755))
            .expect("make the stand-in executable");
        let bin = bin.to_string_lossy().into_owned();

        let ant = AntCli {
            cli: Cli { bin: bin.clone() },
        }
        .run(&["provision", "reshard"], 10);
        assert!(ant.ok(), "{}", ant.summary());
        assert_eq!(ant.stdout, "/tmp/sdk-cli.jsonl");

        let stock = Cli { bin }.run(&["provision", "reshard"], 10);
        assert!(stock.ok(), "{}", stock.summary());
        let inherited =
            std::env::var("ANTITHESIS_SDK_LOCAL_OUTPUT").unwrap_or_else(|_| "unset".to_string());
        assert_eq!(stock.stdout, inherited);
        assert_ne!(stock.stdout, ANT_SDK_OUTPUT);

        std::fs::remove_dir_all(&dir).expect("remove the temp dir");
    }
}

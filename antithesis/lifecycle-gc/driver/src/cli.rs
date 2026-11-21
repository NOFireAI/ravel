//! `ravel-cli` as a bounded subprocess: erasure submits and legal holds.
//! Every call carries the store and tenancy flags the servers run with.

use std::process::Command;

pub struct Output {
    /// `None` when the process could not run or was killed by `timeout`.
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    pub fn ok(&self) -> bool {
        self.code == Some(0)
    }

    pub fn summary(&self) -> String {
        let s = self.stderr.trim();
        let mut start = s.len().saturating_sub(300);
        while !s.is_char_boundary(start) {
            start += 1;
        }
        format!(
            "exit {:?}; stderr: {}",
            self.code,
            s[start..].replace('\n', " | ")
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

    pub fn run(&self, args: &[&str], timeout_s: u32) -> Output {
        match Command::new("timeout")
            .arg(timeout_s.to_string())
            .arg(&self.bin)
            .args(["--store", "s3", "--tenant-hash-unkeyed"])
            .args(args)
            .output()
        {
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

/// The `.dreq` key `erase submit` printed (`  key: t/<hex>/<sig>/del/<id>.dreq`).
pub fn submitted_key(stdout: &str) -> Option<String> {
    stdout
        .lines()
        .filter_map(|l| l.trim().strip_prefix("key: "))
        .map(str::trim)
        .find(|k| k.starts_with("t/") && k.ends_with(".dreq"))
        .map(str::to_string)
}

/// `t/<hex>/` from any key under a tenant.
pub fn tenant_prefix(key: &str) -> Option<String> {
    let rest = key.strip_prefix("t/")?;
    let (hex, _) = rest.split_once('/')?;
    (!hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit())).then(|| format!("t/{hex}/"))
}

/// The scopes `hold set` printed (`hold set: <scope>; <guarantee>`), one
/// per line.
pub fn hold_scopes(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .filter_map(|l| l.trim().strip_prefix("hold set: "))
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_string())
        .collect()
}

/// A request id, as a UUID string, from 128 drawn bits.
pub fn uuid_from(hi: u64, lo: u64) -> String {
    // Version 4 and RFC 4122 variant bits, so every parser accepts it.
    let hi = (hi & !0xF000) | 0x4000;
    let lo = (lo & !(0b11 << 62)) | (0b10 << 62);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        hi >> 32,
        (hi >> 16) & 0xFFFF,
        hi & 0xFFFF,
        lo >> 48,
        lo & 0xFFFF_FFFF_FFFF
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_output_names_the_key_and_tenant() {
        let out = "submitted erasure request 0f\n  key: t/abcd01/l/del/0f.dreq\n";
        let key = submitted_key(out);
        assert_eq!(key.as_deref(), Some("t/abcd01/l/del/0f.dreq"));
        assert_eq!(
            tenant_prefix("t/abcd01/l/del/0f.dreq").as_deref(),
            Some("t/abcd01/")
        );
        assert_eq!(tenant_prefix("sys/gc"), None);
        let again = "erasure request 0f already present (idempotent retry); left untouched\n  key: t/abcd01/l/del/0f.dreq";
        assert!(submitted_key(again).is_some());
    }

    #[test]
    fn hold_output_lists_scopes() {
        let out =
            "hold set: t/ab/l/l0/0001/\nhold set: t/ab/l/c/0001/\nhold set: t/ab/l/l1/0001/\n";
        assert_eq!(hold_scopes(out).len(), 3);
        // Since the delete-batch re-read amendment each line states its
        // guarantee after the scope.
        let out = "hold set: t/ab/l/c/0001/; honored by every delete batch that re-reads holds after now\n";
        assert_eq!(hold_scopes(out), vec!["t/ab/l/c/0001/".to_string()]);
    }

    #[test]
    fn uuids_are_well_formed() {
        let u = uuid_from(u64::MAX, 0);
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "4");
        assert!(matches!(&u[19..20], "8" | "9" | "a" | "b"));
    }
}

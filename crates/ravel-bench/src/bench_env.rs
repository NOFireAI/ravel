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

/// The human CPU model resolved from host files, or the string `"unknown"`
/// when no known key form is present. Best-effort and infallible on every
/// platform: x86 Linux names the CPU in `/proc/cpuinfo`'s `model name`, ARM
/// Linux exposes `Model` (Raspberry Pi) or the devicetree `model` node or only
/// a `CPU implementer`/`CPU part` pair, and macOS names it through
/// `sysctl -n machdep.cpu.brand_string`. A host whose CPU cannot be named in
/// any of these records `"unknown"`
/// rather than aborting the stamp, mirroring the other best-effort fields here
/// (an explicit unknown is honest; a stamper that refuses to run makes the
/// whole artifact unavailable). Both provenance sites -- this header and
/// `metricsbench_report` -- call this one resolver so they cannot diverge.
pub fn cpu_model() -> String {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let devicetree = std::fs::read_to_string("/sys/firmware/devicetree/base/model").ok();
    let devicetree = devicetree
        .as_deref()
        .map(trim_devicetree)
        .filter(|s| !s.is_empty());
    let sysctl = if cfg!(target_os = "macos") {
        command_stdout("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        None
    };
    resolve_cpu_model(&cpuinfo, devicetree, sysctl.as_deref())
        .unwrap_or_else(|| "unknown".to_string())
}

/// The resolution decision over fixed inputs, split from the file reads so
/// every key form is testable without host files. Tries, in order: x86's
/// `model name`, the `Model` key (Raspberry Pi and some ARM), the devicetree
/// `model` node (`/sys/firmware/devicetree/base/model`), then a composed
/// identifier from the `CPU implementer`/`CPU part` pair (bare ARM, no name).
/// `None` when none of these is present, which the caller renders `"unknown"`.
fn resolve_cpu_model(
    cpuinfo: &str,
    devicetree_model: Option<&str>,
    sysctl_brand: Option<&str>,
) -> Option<String> {
    // macOS: the sysctl brand string is the only source; /proc does not exist
    // there, so the Linux keys below are vacuously absent.
    if let Some(brand) = sysctl_brand.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(brand.to_string());
    }
    if let Some(model) = cpuinfo_value(cpuinfo, "model name") {
        return Some(model);
    }
    if let Some(model) = cpuinfo_value(cpuinfo, "Model") {
        return Some(model);
    }
    if let Some(model) = devicetree_model.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(model.to_string());
    }
    if let (Some(implementer), Some(part)) = (
        cpuinfo_value(cpuinfo, "CPU implementer"),
        cpuinfo_value(cpuinfo, "CPU part"),
    ) {
        return Some(format!("ARM implementer {implementer} part {part}"));
    }
    None
}

/// The first non-empty value for `key` in `/proc/cpuinfo` content, matching the
/// key exactly after trimming. `None` when the key is absent or its value is
/// blank.
fn cpuinfo_value(cpuinfo: &str, key: &str) -> Option<String> {
    for line in cpuinfo.lines() {
        if let Some((k, value)) = line.split_once(':')
            && k.trim() == key
        {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// The devicetree `model` node is a NUL-terminated string; trim the trailing
/// NUL(s) and surrounding whitespace.
fn trim_devicetree(raw: &str) -> &str {
    raw.trim_matches(|c: char| c == '\0' || c.is_whitespace())
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{cpu_model, resolve_cpu_model};

    /// x86 Linux: the `model name` line wins and parses to its trimmed value.
    #[test]
    fn model_name_resolves_to_its_value() {
        assert_eq!(
            resolve_cpu_model("processor : 0\nmodel name : AMD EPYC 7R13\n", None, None),
            Some("AMD EPYC 7R13".to_string()),
        );
    }

    /// ARM Linux (Raspberry Pi): no `model name`, but a `Model` line names the
    /// board. This is the abort issue #976 fixed; a resolver reverted to
    /// requiring `model name` returns `None` here and fails this test.
    #[test]
    fn the_model_key_resolves_when_model_name_is_absent() {
        assert_eq!(
            resolve_cpu_model(
                "processor : 0\nModel : Raspberry Pi 4 Model B Rev 1.4\n",
                None,
                None,
            ),
            Some("Raspberry Pi 4 Model B Rev 1.4".to_string()),
        );
    }

    /// The devicetree `model` node resolves when neither `model name` nor
    /// `Model` is present in cpuinfo.
    #[test]
    fn the_devicetree_model_resolves_when_cpuinfo_names_no_model() {
        assert_eq!(
            resolve_cpu_model(
                "processor : 0\nCPU implementer : 0x41\n",
                Some("Raspberry Pi Compute Module 4"),
                None,
            ),
            Some("Raspberry Pi Compute Module 4".to_string()),
        );
    }

    /// Bare ARM: only an implementer/part pair, composed into an identifier.
    /// Another form the `model name`-only abort could not name.
    #[test]
    fn implementer_and_part_compose_when_no_name_is_present() {
        assert_eq!(
            resolve_cpu_model(
                "processor : 0\nCPU implementer : 0x41\nCPU part : 0xd0c\n",
                None,
                None,
            ),
            Some("ARM implementer 0x41 part 0xd0c".to_string()),
        );
    }

    /// `model name` outranks `Model` and the implementer/part pair when more
    /// than one form is present.
    #[test]
    fn model_name_outranks_the_other_forms() {
        assert_eq!(
            resolve_cpu_model(
                "model name : Intel Xeon\nModel : some board\nCPU implementer : 0x41\nCPU part : 0xd0c\n",
                Some("a devicetree model"),
                None,
            ),
            Some("Intel Xeon".to_string()),
        );
    }

    /// The no-key case (empty `/proc/cpuinfo`, no devicetree) resolves to
    /// nothing, so the caller records `"unknown"` -- WITHOUT an error. This is
    /// the honest-unknown decision: the stamp still runs.
    /// macOS: the sysctl brand string wins over every Linux key form, and the
    /// fixed input pins the exact value. Demonstrated failing by passing the
    /// brand as None.
    #[test]
    fn sysctl_brand_string_names_the_macos_cpu() {
        assert_eq!(
            resolve_cpu_model("", None, Some("Apple M3 Pro\n")).as_deref(),
            Some("Apple M3 Pro")
        );
        assert_eq!(resolve_cpu_model("", None, Some("   ")), None);
    }

    #[test]
    fn no_recognised_key_resolves_to_none() {
        assert_eq!(
            resolve_cpu_model("processor : 0\nvendor_id : X\n", None, None),
            None
        );
    }

    /// A blank `model name` value is not a model; the resolver falls through it
    /// to the next form rather than returning an empty string.
    #[test]
    fn a_blank_model_name_falls_through() {
        assert_eq!(
            resolve_cpu_model("model name :   \nModel : Pi\n", None, None),
            Some("Pi".to_string()),
        );
    }

    /// The public resolver never aborts: on any host it returns a non-empty
    /// string, `"unknown"` at worst, so the provenance stamp always completes.
    #[test]
    fn cpu_model_is_infallible_and_non_empty() {
        assert!(!cpu_model().is_empty());
    }
}

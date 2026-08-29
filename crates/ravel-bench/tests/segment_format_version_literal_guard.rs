//! Guard against the fixture-metadata drift that issue #862 exposed: a bench
//! fixture that writes current-version RLOG/RSEG bytes but declares a stale
//! hardcoded `segment_format_version` in its commit record. The mismatch is
//! silent -- the object decodes fine -- but it makes the catalog present a v4
//! object as pre-v4, which flips version-gated routing (the whole-segment
//! ranged read) and turned a real `logs_scan_scaling_smoke` failure into a
//! false pass.
//!
//! The mechanical rule: `segment_format_version` in this crate's `src/` is never
//! a bare integer literal. It must be derived from the writer that produced the
//! bytes, so it cannot contradict them:
//!
//! - RLOG (logs) fixtures: `u32::from(ravel_logseg::footer::VERSION)`.
//! - RSEG (metrics) fixtures: `u32::from(ravel_segment::VERSION_V7)`.
//! - RSPAN (spans) fixtures: `u32::from(ravel_rspan::footer::VERSION)`.
//!
//! A fixture that genuinely wants to simulate an OLDER object (a non-current
//! version, or bytes that are not a real segment at all) may keep the literal,
//! but only with a `format-version-literal-ok` marker comment in the eight lines
//! above it, so the choice is deliberate and auditable rather than a stale
//! copy-paste.
//!
//! This does not need the `sql-latency` feature: it reads source text, so it
//! runs on the default `cargo test -p ravel-bench`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::{Path, PathBuf};

/// The commit-record field whose value the catalog reads to decide segment
/// format version.
const FIELD: &str = "segment_format_version:";

/// Marker that whitelists a deliberate literal (an old-object simulation or a
/// non-segment payload). Keep it on the field line or within the eight lines
/// above it, alongside a sentence saying why.
const ALLOW_MARKER: &str = "format-version-literal-ok";

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read_dir src") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn segment_format_version_is_never_a_bare_literal() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src, &mut files);
    assert!(
        !files.is_empty(),
        "found no .rs files under {}; the guard would pass vacuously",
        src.display()
    );

    let mut violations = Vec::new();
    for path in &files {
        let text = fs::read_to_string(path).expect("read source file");
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let Some(pos) = line.find(FIELD) else {
                continue;
            };
            // A literal is a decimal digit as the first non-space token after
            // the field name. `u32::from(...)` starts with `u`, so it is exempt.
            let rest = line[pos + FIELD.len()..].trim_start();
            if !rest.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                continue;
            }
            let window_start = i.saturating_sub(8);
            let allowed = lines[window_start..=i]
                .iter()
                .any(|l| l.contains(ALLOW_MARKER));
            if !allowed {
                let shown = path.strip_prefix(&src).unwrap_or(path);
                violations.push(format!(
                    "src/{}:{}: {}",
                    shown.display(),
                    i + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "`{FIELD}` is set to a hardcoded integer literal. Derive it from the \
         writer that produced the bytes so it cannot contradict them \
         (`u32::from(ravel_logseg::footer::VERSION)` for RLOG, \
         `u32::from(ravel_segment::VERSION_V7)` for RSEG, \
         `u32::from(ravel_rspan::footer::VERSION)` for RSPAN). A fixture that \
         deliberately simulates an older or non-segment object may keep the \
         literal by adding a `{ALLOW_MARKER}` marker comment above it.\n{}",
        violations.join("\n"),
    );
}

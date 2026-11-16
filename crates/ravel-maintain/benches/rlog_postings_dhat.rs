//! High-water heap peak of the RLOG writer's POSTINGS accumulation (issue
//! #3263), per arm and per row-order family, one process per arm.
//!
//! `rlog_postings_alloc` can only bracket an arm's peak: it samples live bytes
//! between operations and bounds the transient inside one. dhat's
//! `HeapStats::max_bytes` is the live-byte maximum its global allocator keeps
//! on every allocation and free, so it is the peak itself. dhat ignores
//! blocks allocated before its profiler starts, so the fixture, built first,
//! is excluded; every figure is requested sizes, as with stats_alloc.
//!
//! The parent process runs no arm. It re-executes this binary once per
//! (family, arm), so each maximum covers one arm alone, and reads one result
//! line from each child.
//!
//! Usage: `cargo bench -p ravel-maintain --bench rlog_postings_dhat`,
//! optionally followed by `-- <geometry>` (default `large`). Exits non-zero
//! when a child fails or prints no result line, an arm's checksum disagrees
//! with arm A in the same family, an arm leaves bytes live after it returns,
//! or, on `large`, arm A's peak falls outside its pre-registered band for the
//! family.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::ops::RangeInclusive;
use std::process::{Command, ExitCode};

#[path = "common/postings.rs"]
mod postings;

use postings::{FAMILIES, Family, arm_label, fixture_for, geometry, run_arm};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const CHILD_FLAG: &str = "--dhat-child";

/// Arm A's pre-registered `max_bytes` band on `large`, per family
/// (docs/internal/bbtree-ordered-maps-3254.md, Candidate 2): random keeps the
/// 0.3 to 3 MB band of the #3254 pre-registration; sorted is the #3263 one.
fn a_large_band(family: Family) -> RangeInclusive<usize> {
    match family {
        Family::Random => 300_000..=3_000_000,
        Family::Sorted => 150_000..=1_000_000,
    }
}

struct ChildResult {
    max_bytes: usize,
    max_blocks: usize,
    total_blocks: u64,
    curr_bytes: usize,
    checksum: u64,
}

/// Runs one arm under a fresh profiler and prints its `HeapStats`.
fn child(family: Family, arm: char, geometry_name: &str) {
    let rows = fixture_for(family, geometry(geometry_name));
    let file = std::env::temp_dir().join(format!("rlog-postings-dhat-{}.json", std::process::id()));
    let profiler = dhat::Profiler::builder().testing().file_name(&file).build();
    let checksum = run_arm(arm, &rows, &mut || {});
    let stats = dhat::HeapStats::get();
    drop(profiler);
    let _ = std::fs::remove_file(&file);
    println!(
        "result max_bytes={} max_blocks={} total_blocks={} curr_bytes={} checksum={checksum:#x}",
        stats.max_bytes, stats.max_blocks, stats.total_blocks, stats.curr_bytes
    );
}

fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))
}

fn parse_result(stdout: &str) -> Option<ChildResult> {
    let mut lines = stdout.lines().filter(|l| l.starts_with("result "));
    let line = lines.next()?;
    if lines.next().is_some() {
        return None;
    }
    let checksum = field(line, "checksum")?.strip_prefix("0x")?;
    Some(ChildResult {
        max_bytes: field(line, "max_bytes")?.parse().ok()?,
        max_blocks: field(line, "max_blocks")?.parse().ok()?,
        total_blocks: field(line, "total_blocks")?.parse().ok()?,
        curr_bytes: field(line, "curr_bytes")?.parse().ok()?,
        checksum: u64::from_str_radix(checksum, 16).ok()?,
    })
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if let Some(i) = args.iter().position(|a| a == CHILD_FLAG) {
        let family = Family::parse(&args[i + 1]).expect("child family");
        let arm = args[i + 2].chars().next().expect("child arm");
        child(family, arm, &args[i + 3]);
        return ExitCode::SUCCESS;
    }

    let name = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .cloned()
        .unwrap_or_else(|| "large".to_string());
    // Under `cargo test --all-targets` the binary runs without `--bench`; a
    // small geometry keeps that smoke run fast while still checking the arms.
    let name = if args.iter().any(|a| a == "--bench") {
        name
    } else {
        "small".to_string()
    };
    let g = geometry(&name);
    let exe = std::env::current_exe().expect("current_exe");
    println!(
        "rlog_postings_dhat geometry={} blocks={} streams={} seed={:#x}",
        g.name,
        g.blocks,
        g.streams,
        postings::SEED
    );

    let mut failed = false;
    for family in FAMILIES {
        let mut a: Option<ChildResult> = None;
        for arm in ['A', 'C'] {
            let label = arm_label(arm).expect("fixed arm letters");
            let out = Command::new(&exe)
                .args([CHILD_FLAG, family.name(), &arm.to_string(), g.name])
                .output()
                .expect("spawn child");
            let stdout = String::from_utf8_lossy(&out.stdout);
            let parsed = if out.status.success() {
                parse_result(&stdout)
            } else {
                None
            };
            let Some(r) = parsed else {
                eprintln!(
                    "FAIL family={} arm={label}: child exited {} without exactly one result line\n{}",
                    family.name(),
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                );
                failed = true;
                continue;
            };
            let vs_a = a
                .as_ref()
                .map(|a| {
                    format!(
                        " vs_A={:+.1}%",
                        (r.max_bytes as f64 / a.max_bytes as f64 - 1.0) * 100.0
                    )
                })
                .unwrap_or_default();
            println!(
                "family={} arm={label} max_bytes={} max_blocks={} total_blocks={} \
                 curr_bytes={} checksum={:#x}{vs_a}",
                family.name(),
                r.max_bytes,
                r.max_blocks,
                r.total_blocks,
                r.curr_bytes,
                r.checksum
            );
            if r.curr_bytes != 0 {
                eprintln!(
                    "FAIL family={} arm={label}: {} bytes still live after the arm returned",
                    family.name(),
                    r.curr_bytes
                );
                failed = true;
            }
            match &a {
                None => a = Some(r),
                Some(a) if a.checksum != r.checksum => {
                    eprintln!(
                        "FAIL family={} arm={label}: checksum differs from arm A",
                        family.name()
                    );
                    failed = true;
                }
                Some(_) => {}
            }
        }
        // The bands were registered for `large` only; there A's peak must be
        // present exactly once and inside its family's band.
        if g.name == "large" {
            let band = a_large_band(family);
            match &a {
                Some(a) => {
                    let verdict = if band.contains(&a.max_bytes) {
                        "PASS"
                    } else {
                        failed = true;
                        "MISS"
                    };
                    println!(
                        "band arm=A family={} geometry=large max_bytes={} band={}..={} {verdict}",
                        family.name(),
                        a.max_bytes,
                        band.start(),
                        band.end()
                    );
                }
                None => {
                    println!(
                        "band arm=A family={} geometry=large max_bytes absent MISS",
                        family.name()
                    );
                    failed = true;
                }
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

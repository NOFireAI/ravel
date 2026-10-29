//! The agents cost benchmarks of ADR-3331 decision 15 (issue #3361).
//!
//! Runs every scenario of `ravel_agents_sim::bench` and checks each figure
//! against the band table in `bench/agents/prereg.md`. Exits non-zero when a
//! figure is outside its band, printed more than once, or expected and
//! missing. `docs/agents-benchmarks.md` describes the scenarios and the
//! report format.
//!
//! ```sh
//! cargo bench -p ravel-agents-sim --bench agents_cost            # small
//! cargo bench -p ravel-agents-sim --bench agents_cost -- full
//! AGENTS_BENCH_SIZE=full cargo bench -p ravel-agents-sim --bench agents_cost
//! ```

use std::alloc::System;
use std::process::ExitCode;

use ravel_agents_sim::bench::{self, SEED, Size, store::heap};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Live heap bytes: everything allocated or grown, less everything freed.
fn live_bytes() -> u64 {
    let s = INSTRUMENTED_SYSTEM.stats();
    let grown = s
        .bytes_allocated
        .saturating_add(s.bytes_reallocated.max(0) as usize);
    let shrunk = s
        .bytes_deallocated
        .saturating_add(s.bytes_reallocated.min(0).unsigned_abs());
    u64::try_from(grown.saturating_sub(shrunk)).unwrap_or(u64::MAX)
}

fn revision() -> String {
    if let Ok(rev) = std::env::var("AGENTS_BENCH_REVISION") {
        return rev;
    }
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn main() -> ExitCode {
    let arg = std::env::args().skip(1).find(|a| !a.starts_with("--"));
    let wanted = arg
        .or_else(|| std::env::var("AGENTS_BENCH_SIZE").ok())
        .unwrap_or_else(|| "small".to_string());
    let Some(size) = Size::parse(&wanted) else {
        eprintln!("unknown size {wanted:?}: small or full");
        return ExitCode::from(64);
    };
    heap::install(live_bytes);
    println!(
        "agents-bench-stamp revision={} store=memory+fault seed={SEED} clock=tokio-paused size={} heap=stats_alloc",
        revision(),
        size.name()
    );
    let outcome = match bench::run(size) {
        Ok(outcome) => outcome,
        Err(e) => {
            eprintln!("agents-bench: run failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    print!("{}", outcome.report);
    println!(
        "agents-bench-summary figures={} failures={}",
        outcome.figures.len(),
        outcome.checked.failures.len()
    );
    if outcome.checked.failures.is_empty() {
        return ExitCode::SUCCESS;
    }
    for failure in &outcome.checked.failures {
        eprintln!("agents-bench FAIL {failure}");
    }
    ExitCode::FAILURE
}

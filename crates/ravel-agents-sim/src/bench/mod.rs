//! The agents cost benchmarks of ADR-3331 decision 15 (issue #3361).
//!
//! Each scenario runs the production store and coordinator over
//! [`store::BenchStore`] on a paused tokio runtime, at fixed operation
//! counts and a fixed seed, and reports per-operation figures: requests and
//! object-body bytes per store phase, CAS conflicts, latency percentiles on
//! the injected clock, and peak heap when the bench binary installs its
//! allocator probe. [`run`] checks every figure against the band table in
//! `benchmarks/agents/prereg.md`. `docs/agents-benchmarks.md` describes the
//! scenarios, the report format and the byte kinds.

pub mod figures;
pub mod scenarios;
pub mod store;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use figures::{Bands, Checked, Figure, Prices, Scope};

/// The seed of every scenario: agent keys, worker ids and slow-store
/// jitter.
pub const SEED: u64 = 3361;

/// The scenarios, in run order.
pub const SCENARIOS: [&str; 8] = [
    "hot",
    "many",
    "dormant",
    "index-p64",
    "index-p2",
    "storm",
    "slow",
    "baseline",
];

/// The discovery scenario family runs on top of [`SCENARIOS`].
pub const DISCOVERY: &str = "discovery";

/// The operation counts a run uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Size {
    Small,
    Full,
}

/// Operation counts of one [`Size`]. See prereg.md, "Scenario sizes".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub hot_rounds: usize,
    pub many_agents: usize,
    pub dormant_agents: usize,
    pub active_agents: usize,
    pub index_messages: usize,
    pub storm_agents: usize,
    pub discovery_agents: Vec<usize>,
    pub baseline_iterations: usize,
}

impl Size {
    pub fn name(self) -> &'static str {
        match self {
            Size::Small => "small",
            Size::Full => "full",
        }
    }

    pub fn parse(text: &str) -> Option<Size> {
        match text {
            "small" => Some(Size::Small),
            "full" => Some(Size::Full),
            _ => None,
        }
    }

    pub fn config(self) -> Config {
        match self {
            Size::Small => Config {
                hot_rounds: 2,
                many_agents: 6,
                dormant_agents: 32,
                active_agents: 3,
                index_messages: 12,
                storm_agents: 8,
                discovery_agents: vec![100],
                baseline_iterations: 8,
            },
            Size::Full => Config {
                hot_rounds: 16,
                many_agents: 128,
                dormant_agents: 2000,
                active_agents: 16,
                index_messages: 256,
                storm_agents: 24,
                discovery_agents: vec![100, 1000, 10_000],
                baseline_iterations: 256,
            },
        }
    }
}

fn repo_file(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative)
}

/// `benchmarks/agents/prereg.md`.
pub fn prereg_path() -> PathBuf {
    repo_file("benchmarks/agents/prereg.md")
}

/// `benchmarks/agents/prices.toml`.
pub fn prices_path() -> PathBuf {
    repo_file("benchmarks/agents/prices.toml")
}

pub fn load_bands() -> Result<Bands, String> {
    let path = prereg_path();
    let text =
        std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
    Bands::parse(&text)
}

pub fn load_prices() -> Result<Prices, String> {
    let path = prices_path();
    let text =
        std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
    Prices::parse(&text)
}

/// One checked run.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub size: Size,
    pub heap: bool,
    pub figures: Vec<Figure>,
    pub checked: Checked,
    /// The figure lines, then the derived dollar lines.
    pub report: String,
}

/// Run every scenario at `size` and check the figures against prereg.md.
pub fn run(size: Size) -> Result<Outcome, String> {
    let bands = load_bands()?;
    let prices = load_prices()?;
    let heap = store::heap::installed();
    let figures = scenarios::run_all(size)?;
    let checked = figures::check(&figures, &bands, size, heap, Scope::All);
    let report = figures::write_report(&figures) + &figures::dollar_lines(&figures, &prices);
    Ok(Outcome {
        size,
        heap,
        figures,
        checked,
        report,
    })
}

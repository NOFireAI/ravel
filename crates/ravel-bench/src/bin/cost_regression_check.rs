//! Request-cost regression gate (ADR-0996 workstream F, task 996-7).
//!
//! Compares two machine-readable cost reports -- a baseline and a candidate --
//! and exits NONZERO when any workstream-F figure regresses past its per-figure
//! band, or when the two cannot be compared on the same basis (a legacy report
//! missing its request surface, or two reports priced under different effective
//! cost profiles).
//!
//! ```text
//! cost_regression_check <baseline-report> <candidate-report> [--bands bands.toml]
//! ```
//!
//! Prints a table of figure / baseline / candidate / band / verdict for EVERY
//! compared figure, pass or fail, so the outcome is a regression report a human
//! reads without the JSON open. Exit codes: `0` no regression, `1` a figure
//! regressed, `2` the reports could not be compared (a typed refusal).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ravel_bench::cost_regression::{Bands, CostReport, compare};

#[derive(Parser, Debug)]
#[command(about = "Fail on an object-store request-cost regression between two bench reports")]
struct Args {
    /// The baseline report (JSON) the candidate is compared against.
    baseline: PathBuf,
    /// The candidate report (JSON) under test.
    candidate: PathBuf,
    /// Per-figure band overrides (TOML). Any class the document omits keeps its
    /// compiled default; the defaults ship in `cost_regression_bands.toml`.
    #[arg(long)]
    bands: Option<PathBuf>,
}

/// Refusal exit code: the reports could not be compared on the same basis.
const EXIT_REFUSED: u8 = 2;
/// Regression exit code: the reports compared, but a figure moved too far.
const EXIT_REGRESSED: u8 = 1;

fn run() -> Result<Result<(), ()>, String> {
    let args = Args::parse();

    let baseline_json = std::fs::read_to_string(&args.baseline)
        .map_err(|e| format!("read baseline {}: {e}", args.baseline.display()))?;
    let candidate_json = std::fs::read_to_string(&args.candidate)
        .map_err(|e| format!("read candidate {}: {e}", args.candidate.display()))?;

    let bands = match &args.bands {
        Some(path) => {
            let toml = std::fs::read_to_string(path)
                .map_err(|e| format!("read bands {}: {e}", path.display()))?;
            Bands::from_toml_str(&toml).map_err(|e| e.to_string())?
        }
        None => Bands::defaults(),
    };

    let baseline =
        CostReport::from_json_str(&baseline_json).map_err(|e| format!("baseline: {e}"))?;
    let candidate =
        CostReport::from_json_str(&candidate_json).map_err(|e| format!("candidate: {e}"))?;

    match compare(&baseline, &candidate, &bands) {
        Ok(comparison) => {
            print!("{}", comparison.render_table());
            if comparison.regressed() {
                eprintln!("REGRESSION: at least one figure moved past its band");
                Ok(Err(()))
            } else {
                eprintln!("OK: every compared figure is within band");
                Ok(Ok(()))
            }
        }
        // A refusal is not a regression verdict: the reports could not be
        // compared on the same basis. Surface it distinctly.
        Err(refusal) => Err(refusal.to_string()),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(())) => ExitCode::from(EXIT_REGRESSED),
        Err(message) => {
            eprintln!("cost_regression_check: {message}");
            ExitCode::from(EXIT_REFUSED)
        }
    }
}

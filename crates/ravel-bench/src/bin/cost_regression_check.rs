//! Request-cost regression gate (ADR-0996 workstream F, task 996-7).
//!
//! Compares two machine-readable cost reports -- a baseline and a candidate --
//! and exits NONZERO when any workstream-F figure regresses past its per-figure
//! band, or when the two cannot be compared on the same basis (a legacy report
//! missing its request surface, or two reports priced under different effective
//! cost profiles).
//!
//! ```text
//! cost_regression_check <baseline-report> <candidate-report>
//!     [--bands bands.toml] [--format cost-report|bench-report]
//! ```
//!
//! `--format` names what the two input documents ARE. `cost-report` (the
//! default) is this tool's own explicit figure list; `bench-report` is the
//! document `bench_report --out` writes, projected into that figure list by
//! [`ravel_bench::cost_report_source`]. Both inputs are read as the same format:
//! comparing a hand-written figure list against a projected one invites exactly
//! the wrong-basis error the profile guard exists to prevent. The chosen format
//! is printed above the table, along with any figure class the source cannot
//! supply, so an unenforced band is visible rather than inferred from a short
//! table.
//!
//! Prints a table of figure / baseline / candidate / band / verdict for EVERY
//! compared figure, pass or fail, so the outcome is a regression report a human
//! reads without the JSON open. Exit codes: `0` no regression, `1` a figure
//! regressed, `2` the reports could not be compared (a typed refusal).

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use ravel_bench::cost_regression::{Bands, CostReport, FigureClass, compare};
use ravel_bench::cost_report_source::{gap_note, project_bench_report_json};

/// What the two input documents are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum SourceFormat {
    /// This tool's own report: an explicit list of figures and their classes.
    CostReport,
    /// The shipping `BenchReport` (`bench_report --out`), projected into the
    /// figure list.
    BenchReport,
}

impl SourceFormat {
    fn as_str(self) -> &'static str {
        match self {
            SourceFormat::CostReport => "cost-report",
            SourceFormat::BenchReport => "bench-report",
        }
    }
}

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
    /// What both input documents are. A document read as the wrong format fails
    /// to parse as a typed refusal, never a partial or silent misreading.
    #[arg(long, value_enum, default_value_t = SourceFormat::CostReport)]
    format: SourceFormat,
}

/// Refusal exit code: the reports could not be compared on the same basis.
const EXIT_REFUSED: u8 = 2;
/// Regression exit code: the reports compared, but a figure moved too far.
const EXIT_REGRESSED: u8 = 1;

/// Read one input document under `format`, returning the report and the figure
/// classes the source could not supply.
fn load(
    path: &Path,
    which: &str,
    format: SourceFormat,
) -> Result<(CostReport, Vec<FigureClass>), String> {
    let json = std::fs::read_to_string(path)
        .map_err(|e| format!("read {which} {}: {e}", path.display()))?;
    match format {
        SourceFormat::CostReport => CostReport::from_json_str(&json)
            .map(|report| (report, Vec::new()))
            .map_err(|e| format!("{which}: {e}")),
        SourceFormat::BenchReport => project_bench_report_json(&json)
            .map(|projection| (projection.report, projection.gaps))
            .map_err(|e| format!("{which}: {e}")),
    }
}

fn run() -> Result<Result<(), ()>, String> {
    let args = Args::parse();

    let bands = match &args.bands {
        Some(path) => {
            let toml = std::fs::read_to_string(path)
                .map_err(|e| format!("read bands {}: {e}", path.display()))?;
            Bands::from_toml_str(&toml).map_err(|e| e.to_string())?
        }
        None => Bands::defaults(),
    };

    let (baseline, base_gaps) = load(&args.baseline, "baseline", args.format)?;
    let (candidate, cand_gaps) = load(&args.candidate, "candidate", args.format)?;
    // The two documents are read as one format, so their gaps are the same set;
    // asserting it here keeps a future per-input format from silently comparing
    // a full figure list against a partial one.
    if base_gaps != cand_gaps {
        return Err(format!(
            "the two reports supply different figure classes ({base_gaps:?} vs {cand_gaps:?}); \
             they cannot be compared on the same basis"
        ));
    }

    match compare(&baseline, &candidate, &bands) {
        Ok(comparison) => {
            println!("source format: {}", args.format.as_str());
            if let Some(note) = gap_note(&base_gaps) {
                println!("{note}");
            }
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

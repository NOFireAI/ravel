//! MetricsBench workload generator and artifact gate (ADR-0927, issue #933).
//!
//! This is the entry point that makes both MetricsBench gates reachable from a
//! real caller rather than only from unit tests: it loads
//! `benchmarks/metrics/workload.json` through
//! [`ravel_bench::metrics_workload::load_workload`] and
//! `benchmarks/metrics/metrics.corpus.json` through
//! [`ravel_bench::promql_corpus::load_corpus`], both of which gate what they
//! return, and refuses to run when either artifact is corrupt.
//!
//! It then generates the profile's sample stream deterministically and prints
//! one JSON document with the workload summary, the corpus summary, and the
//! generation report. Every figure it prints is checked against a stated band
//! and the process exits non-zero outside it, so "exit 0" means the work
//! happened rather than that the tool ran.
//!
//! `--profile` has no default: the profile selects which data the run touches,
//! and every alternative is plausible on any target, so a silent default is
//! refused (CLAUDE.md, measurement discipline).
//!
//! `--require-comparable` is how a caller refuses to publish a figure it must
//! not: the `ci` profile carries its non-comparability in the artifact
//! (ADR-0927 decision 11), and a truncated run of any profile is not that
//! profile.

use std::io::Write;
use std::path::PathBuf;

use clap::Parser;
use ravel_bench::metrics_gen::Generator;
use ravel_bench::metrics_workload::{WorkloadFile, load_workload};
use ravel_bench::promql_corpus::{CorpusEntry, class_counts, load_corpus};

/// Where the checked-in artifacts live, relative to this crate's manifest dir.
const DEFAULT_WORKLOAD: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/metrics/workload.json"
);
/// The checked-in query corpus.
const DEFAULT_CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/metrics/metrics.corpus.json"
);

#[derive(Parser, Debug)]
#[command(
    about = "Gate the MetricsBench artifacts and generate a profile's workload deterministically"
)]
struct Args {
    /// The workload manifest to load and gate.
    #[arg(long, default_value = DEFAULT_WORKLOAD)]
    workload: PathBuf,
    /// The PromQL query corpus to load and gate.
    #[arg(long, default_value = DEFAULT_CORPUS)]
    corpus: PathBuf,
    /// Which profile to generate. No default: the choice selects which data the
    /// run touches.
    #[arg(long)]
    profile: String,
    /// Scrapes to generate. Defaults to the profile's full
    /// `samples_per_series`.
    #[arg(long)]
    steps: Option<u64>,
    /// Timestamp of the first scrape, in milliseconds. Time is a parameter so a
    /// run is reproducible.
    #[arg(long, default_value_t = 0)]
    base_ts_ms: i64,
    /// Write the generated stream here. Omitted, the stream is still encoded,
    /// counted and hashed, but discarded.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Exit non-zero unless this run's figures may be published: the profile is
    /// comparable and the run covers the whole profile.
    #[arg(long, default_value_t = false)]
    require_comparable: bool,
}

/// A band violation. Separate from the artifact and generator errors so a
/// reader can tell "the artifacts are wrong" from "the run measured something
/// outside what was pre-registered".
#[derive(Debug, thiserror::Error)]
enum BandError {
    #[error(
        "cost class `{class}` has {found} corpus entries, expected at least 1: every ADR-0927 \
         cost class must be represented, or the corpus cannot attribute a cost to a shape"
    )]
    EmptyCostClass { class: &'static str, found: usize },
    #[error(
        "corpus entry `{entry_id}` carries no cost class; ADR-0927 decision 5 classes every entry"
    )]
    UnclassedEntry { entry_id: String },
    #[error(
        "no corpus entry runs under profile `{profile}`, so the run would time nothing; a \
         profile with no queries is a hole in the corpus, not an empty result"
    )]
    NoEntriesForProfile { profile: String },
    #[error(
        "corpus entry `{entry_id}` selects metric `{metric}`, which the workload manifest does \
         not emit; the corpus and the generator would measure different data"
    )]
    UnknownMetric { entry_id: String, metric: String },
    #[error("profile `{profile}` is not comparable ({reason}), so its figures cannot be published")]
    NotComparable { profile: String, reason: String },
    #[error(
        "this run generated {steps} of profile `{profile}`'s {expected} steps, so it is not that \
         profile and its figures cannot be published"
    )]
    TruncatedRun {
        profile: String,
        steps: u64,
        expected: u64,
    },
}

/// Every `metricsbench_`-prefixed identifier a query selects. A deliberately
/// small scan rather than a PromQL parse: the corpus only ever names metrics
/// with that prefix, and a name that is not a declared family is the drift this
/// catches.
fn selected_metrics(promql: &str, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes: Vec<char> = promql.chars().collect();
    let mut i = 0usize;
    while i < bytes.len() {
        let start = i;
        if bytes[i].is_ascii_alphabetic() || bytes[i] == '_' {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == '_') {
                i += 1;
            }
            let word: String = bytes[start..i].iter().collect();
            if word.starts_with(prefix) && !out.contains(&word) {
                out.push(word);
            }
        } else {
            i += 1;
        }
    }
    out
}

/// The checks whose bands are stated here rather than left to a reader.
fn check_bands(
    workload: &WorkloadFile,
    entries: &[CorpusEntry],
    profile: &str,
) -> Result<(), BandError> {
    for (class, found) in class_counts(entries) {
        if found == 0 {
            return Err(BandError::EmptyCostClass {
                class: class.slug(),
                found,
            });
        }
    }
    for entry in entries {
        if entry.class.is_none() {
            return Err(BandError::UnclassedEntry {
                entry_id: entry.id.clone(),
            });
        }
    }
    if !entries.iter().any(|e| e.runs_under(profile)) {
        return Err(BandError::NoEntriesForProfile {
            profile: profile.to_string(),
        });
    }
    let emitted = workload.emitted_metric_names();
    let absent = &workload.generator.absent_metric_name;
    // Every family name shares this prefix, and so does the absent sentinel, so
    // it is the one string the scan needs.
    let prefix = "metricsbench_";
    for entry in entries {
        for metric in selected_metrics(&entry.promql, prefix) {
            if &metric != absent && !emitted.contains(&metric) {
                return Err(BandError::UnknownMetric {
                    entry_id: entry.id.clone(),
                    metric,
                });
            }
        }
    }
    Ok(())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let workload = load_workload(&args.workload)?;
    let entries = load_corpus(&args.corpus)?;
    check_bands(&workload, &entries, &args.profile)?;

    let mut generator = Generator::new(&workload, &args.profile, args.base_ts_ms)?;
    let expected_steps = generator.profile().samples_per_series;
    let steps = args.steps.unwrap_or(expected_steps);
    let comparability = generator.profile().comparability.clone();

    let mut sink: Box<dyn Write> = match &args.out {
        Some(path) => Box::new(std::io::BufWriter::new(std::fs::File::create(path)?)),
        None => Box::new(std::io::sink()),
    };
    let report = generator.generate_into(steps, &mut sink)?;
    // Flush at the call site and propagate: a BufWriter dropped without a flush
    // swallows the error and can lose the tail of the artifact while the process
    // still exits zero.
    sink.flush()?;
    drop(sink);

    let per_class: Vec<serde_json::Value> = class_counts(&entries)
        .into_iter()
        .map(|(class, n)| {
            serde_json::json!({
                "class": class.slug(),
                "entries": n,
                "entries_in_profile": entries
                    .iter()
                    .filter(|e| e.class == Some(class) && e.runs_under(&args.profile))
                    .count(),
            })
        })
        .collect();
    let document = serde_json::json!({
        "workload": {
            "path": args.workload,
            "version": workload.version,
            "seed": workload.seed,
            "families": workload.families.len(),
            "label_dimensions": workload.label_dimensions.len(),
            "profiles": workload.profiles.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
        },
        "corpus": {
            "path": args.corpus,
            "entries": entries.len(),
            "entries_in_profile": entries.iter().filter(|e| e.runs_under(&args.profile)).count(),
            "cost_classes": per_class,
        },
        "generation": report,
        "run": {
            "steps_requested": steps,
            "steps_in_full_profile": expected_steps,
            "covers_whole_profile": steps == expected_steps,
            "base_ts_ms": args.base_ts_ms,
            "out": args.out,
        },
    });
    println!("{}", serde_json::to_string_pretty(&document)?);

    if args.require_comparable {
        if let Some(reason) = comparability.reason() {
            return Err(Box::new(BandError::NotComparable {
                profile: args.profile.clone(),
                reason: reason.to_string(),
            }));
        }
        if steps != expected_steps {
            return Err(Box::new(BandError::TruncatedRun {
                profile: args.profile.clone(),
                steps,
                expected: expected_steps,
            }));
        }
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("metricsbench_gen: {err}");
        let mut source = err.source();
        while let Some(cause) = source {
            eprintln!("  caused by: {cause}");
            source = cause.source();
        }
        std::process::exit(1);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn selected_metrics_finds_every_prefixed_identifier_once() {
        let found = selected_metrics(
            "histogram_quantile(0.99, sum by (le) (rate(metricsbench_dur_seconds_bucket[5m]))) \
             + metricsbench_gauge + metricsbench_gauge",
            "metricsbench_",
        );
        assert_eq!(
            found,
            vec![
                "metricsbench_dur_seconds_bucket".to_string(),
                "metricsbench_gauge".to_string()
            ]
        );
        // A label value that merely contains the prefix inside quotes is still
        // an identifier to this scan, which is the conservative direction: it
        // would be reported as an unknown metric rather than silently accepted.
        assert_eq!(
            selected_metrics("rate(other_total[5m])", "metricsbench_"),
            Vec::<String>::new()
        );
    }
}

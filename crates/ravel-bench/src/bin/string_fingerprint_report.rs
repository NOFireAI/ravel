//! Report-only string fingerprint measurement (issue #3710, ADR-3692 work
//! package 4, Plan row T12). See [`ravel_bench::string_fingerprint`] and
//! [`ravel_bench::string_fingerprint_report`] for the measurement itself, and
//! docs/internal/bench-reports.md for the writeup.
//!
//! ```text
//! string_fingerprint_report --subject NAME --corpus-lines CORPUS.log[.gz] --out report.json
//!     [--spec PATH] [--gram-len N] [--budget-share X] [--fallback-min-bits N]
//!     [--zstd-level N] [--block-records N] [--object-records N] [--max-lines N] [--seed N]
//! string_fingerprint_report --subject NAME --corpus-parquet hits.parquet --corpus-column URL \
//!     --out report.json   # parquet-baseline feature only
//! string_fingerprint_report --describe --corpus-lines CORPUS.log [--max-lines N]
//! string_fingerprint_report --decide report1.json report2.json ... [--spec PATH]
//! ```
//!
//! Every run parameter comes from the pre-registration file. A parameter
//! flag is a check, not a setting: a value other than the pre-registered one
//! is refused before the corpus is read, and so is a `--corpus-column` other
//! than the subject's registered column.
//!
//! Exit codes are [`EXIT_CODES`].

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use ravel_bench::string_fingerprint::{Checked, complete, overall, saved_decision, spec};
use ravel_bench::string_fingerprint_report::{
    Config, Corpus, Overrides, describe, file_digest, git_head, run,
};

/// The report was written with no structural problem and every figure in
/// its band; with `--decide`, an overall GO.
const EXIT_OK: u8 = 0;
/// An error, or a report with a structural problem; with `--decide`, a
/// report that could not be read or a set of reports that is not a decision.
const EXIT_ERROR: u8 = 1;
/// The report was written with no structural problem and at least one MISS.
const EXIT_MISS: u8 = 2;
/// With `--decide`, a complete set of reports whose overall decision is NO-GO.
const EXIT_NO_GO: u8 = 3;

const EXIT_CODES: &str = "Exit codes:
  0  measure: the report was written, no structural problem, every figure in its band
     --decide: overall GO
  1  an error, or a report with a structural problem (a missing, duplicate,
     unexpected or not-pre-registered figure; the report is still written);
     --decide: a report could not be read, or the reports are not a decision
     (a subject missing or reported twice, a report with a problem, or a
     subject that is not pre-registered)
  2  measure: the report was written with no structural problem and at least
     one figure outside its band (MISS)
  3  --decide: every pre-registered subject reported once without a problem,
     and the overall decision is NO-GO";

#[derive(Parser, Debug)]
#[command(
    about = "Measure a learned per-block string summary against the production bloom and contains baselines",
    after_help = EXIT_CODES
)]
struct Args {
    /// The pre-registration file.
    #[arg(
        long,
        default_value = "crates/ravel-bench/fixtures/string_fingerprint/subjects.toml"
    )]
    spec: PathBuf,
    /// The subject name from the pre-registration file.
    #[arg(long)]
    subject: Option<String>,
    /// One value per line of this (possibly gzip-compressed) text file.
    #[arg(long, conflicts_with = "decide")]
    corpus_lines: Option<PathBuf>,
    /// One value per row of this Parquet file's named `Utf8` or `Binary` column.
    #[cfg(feature = "parquet-baseline")]
    #[arg(long, requires = "corpus_column", conflicts_with_all = ["corpus_lines", "decide"])]
    corpus_parquet: Option<PathBuf>,
    /// The Parquet column `--corpus-parquet` reads. Must equal the subject's
    /// pre-registered `column`; any other value is refused before the corpus
    /// is read.
    #[cfg(feature = "parquet-baseline")]
    #[arg(long, requires = "corpus_parquet")]
    corpus_column: Option<String>,
    #[arg(long)]
    gram_len: Option<usize>,
    #[arg(long)]
    budget_share: Option<f64>,
    #[arg(long)]
    fallback_min_bits: Option<usize>,
    #[arg(long)]
    zstd_level: Option<i32>,
    #[arg(long)]
    block_records: Option<usize>,
    #[arg(long)]
    object_records: Option<usize>,
    /// With `--describe`, the most values to read (default: all).
    #[arg(long)]
    max_lines: Option<u64>,
    #[arg(long)]
    seed: Option<u64>,
    /// Where to write the JSON report.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Print the corpus facts a pre-registration needs and exit.
    #[arg(long, conflicts_with_all = ["decide", "subject", "out"])]
    describe: bool,
    /// Print the overall decision over these written reports and exit.
    #[arg(long, num_args = 1..)]
    decide: Vec<PathBuf>,
}

fn read_spec(path: &Path) -> Result<spec::FingerprintSpec, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    spec::parse(&text)
}

fn report_code(checked: &Checked) -> u8 {
    if !checked.problems.is_empty() {
        EXIT_ERROR
    } else if !checked.misses.is_empty() {
        EXIT_MISS
    } else {
        EXIT_OK
    }
}

/// The `--decide` output and its exit code: a stamp line naming the git
/// commit and every report read with its sha256, each subject's decision
/// line and its miss and problem counts, then the overall line.
fn decide_overall(spec_path: &Path, reports: &[PathBuf]) -> Result<(Vec<String>, u8), String> {
    let spec = read_spec(spec_path)?;
    let mut read = Vec::new();
    let mut saved = Vec::new();
    for path in reports {
        let json =
            std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let (sha256, _) = file_digest(path)?;
        let name = path
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        read.push(format!("{name}:{sha256}"));
        saved.push(saved_decision(&json).map_err(|e| format!("{}: {e}", path.display()))?);
    }
    let mut lines = vec![format!(
        "string_fingerprint decide stamp git={} reports={}",
        git_head(),
        read.join(",")
    )];
    for s in &saved {
        lines.push(s.decision.line.clone());
        lines.push(format!(
            "string_fingerprint decide subject={} misses={} problems={}",
            s.decision.subject, s.misses, s.problems
        ));
    }
    let names: Vec<String> = spec.subject.iter().map(|s| s.name.clone()).collect();
    let d = overall(&names, &saved);
    lines.push(d.line);
    let code = if !complete(&names, &saved) {
        EXIT_ERROR
    } else if d.go {
        EXIT_OK
    } else {
        EXIT_NO_GO
    };
    Ok((lines, code))
}

fn corpus(args: &Args) -> Result<Corpus, String> {
    #[cfg(feature = "parquet-baseline")]
    let parquet = args.corpus_parquet.clone().zip(args.corpus_column.clone());
    #[cfg(not(feature = "parquet-baseline"))]
    let parquet: Option<(PathBuf, String)> = None;
    match (args.corpus_lines.clone(), parquet) {
        (Some(path), None) => Ok(Corpus::Lines(path)),
        #[cfg(feature = "parquet-baseline")]
        (None, Some((path, column))) => Ok(Corpus::ParquetColumn { path, column }),
        (None, None) => Err("one of --corpus-lines or --corpus-parquet is required".into()),
        _ => Err("--corpus-lines and --corpus-parquet are mutually exclusive".into()),
    }
}

fn measure(args: &Args) -> Result<u8, String> {
    let (Some(subject), Some(out)) = (args.subject.clone(), args.out.clone()) else {
        return Err("--subject and --out are required".into());
    };
    let cfg = Config {
        spec_path: args.spec.clone(),
        subject,
        corpus: corpus(args)?,
        overrides: Overrides {
            gram_len: args.gram_len,
            budget_share: args.budget_share,
            fallback_min_bits: args.fallback_min_bits,
            zstd_level: args.zstd_level,
            block_records: args.block_records,
            object_records: args.object_records,
            max_lines: args.max_lines,
            seed: args.seed,
        },
    };
    let r = run(&cfg)?;
    let json = serde_json::to_string_pretty(&r.report).map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(&out, json + "\n").map_err(|e| format!("write {}: {e}", out.display()))?;
    for line in r.report.lines() {
        println!("{line}");
    }
    println!(
        "string_fingerprint objects={} object_bytes={}",
        r.objects, r.object_bytes
    );
    for p in &r.report.checked.problems {
        eprintln!("string_fingerprint_report: problem: {p}");
    }
    for m in &r.report.checked.misses {
        println!("string_fingerprint miss {m}");
    }
    Ok(report_code(&r.report.checked))
}

fn exec(args: &Args) -> Result<u8, String> {
    if !args.decide.is_empty() {
        let (lines, code) = decide_overall(&args.spec, &args.decide)?;
        for line in lines {
            println!("{line}");
        }
        Ok(code)
    } else if args.describe {
        let line = describe(&corpus(args)?, args.max_lines.unwrap_or(u64::MAX))?;
        println!("{line}");
        Ok(EXIT_OK)
    } else {
        measure(args)
    }
}

fn main() -> ExitCode {
    match exec(&Args::parse()) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            ExitCode::from(EXIT_ERROR)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/string_fingerprint")
            .join(name)
    }

    fn args(extra: &[&str]) -> Args {
        let mut argv = vec!["string_fingerprint_report"];
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv).expect("args")
    }

    /// test_subjects.toml with `from` replaced by `to`, written into `dir`.
    fn edited_spec(dir: &Path, from: &str, to: &str) -> PathBuf {
        let text = std::fs::read_to_string(fixture("test_subjects.toml")).expect("spec");
        assert!(text.contains(from), "{from} not in the test spec");
        let path = dir.join("subjects.toml");
        std::fs::write(&path, text.replace(from, to)).expect("write spec");
        path
    }

    fn measure_code(spec: &Path, dir: &Path) -> Result<u8, String> {
        let corpus = fixture("corpus.txt");
        let out = dir.join("report.json");
        let code = exec(&args(&[
            "--spec",
            spec.to_str().expect("utf8"),
            "--subject",
            "test_subject",
            "--corpus-lines",
            corpus.to_str().expect("utf8"),
            "--out",
            out.to_str().expect("utf8"),
        ]));
        if code.is_ok() {
            assert!(out.exists(), "the report must be written");
        }
        code
    }

    #[test]
    fn measure_exits_0_for_a_clean_in_band_report() {
        let dir = tempfile::tempdir().expect("tempdir");
        let code = measure_code(&fixture("test_subjects.toml"), dir.path());
        assert_eq!(code, Ok(EXIT_OK));
    }

    #[test]
    fn measure_exits_2_for_a_band_miss_without_a_problem() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spec = edited_spec(
            dir.path(),
            r#""lines" = { predict = 18, band = [18, 18] }"#,
            r#""lines" = { predict = 17, band = [17, 17] }"#,
        );
        assert_eq!(measure_code(&spec, dir.path()), Ok(EXIT_MISS));
    }

    #[test]
    fn measure_exits_1_for_a_problem_or_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A problem and a miss together: the problem decides.
        let spec = edited_spec(
            dir.path(),
            r#""saturation/blocks" = { predict = 1, band = [1, 1] }"#,
            r#""saturation/blocks" = { absent = "test" }"#,
        );
        let text = std::fs::read_to_string(&spec).expect("spec");
        std::fs::write(
            &spec,
            text.replace(
                r#""lines" = { predict = 18, band = [18, 18] }"#,
                r#""lines" = { predict = 17, band = [17, 17] }"#,
            ),
        )
        .expect("write spec");
        assert_eq!(measure_code(&spec, dir.path()), Ok(EXIT_ERROR));

        let missing = exec(&args(&[
            "--spec",
            fixture("test_subjects.toml").to_str().expect("utf8"),
            "--subject",
            "test_subject",
            "--corpus-lines",
            fixture("no-such-corpus.txt").to_str().expect("utf8"),
            "--out",
            dir.path().join("r.json").to_str().expect("utf8"),
        ]));
        assert!(missing.is_err(), "{missing:?}");
    }

    #[test]
    fn report_code_ranks_problems_over_misses() {
        let checked = |problems: usize, misses: usize| Checked {
            problems: vec!["p".into(); problems],
            misses: vec!["m".into(); misses],
        };
        assert_eq!(report_code(&checked(0, 0)), EXIT_OK);
        assert_eq!(report_code(&checked(0, 1)), EXIT_MISS);
        assert_eq!(report_code(&checked(1, 0)), EXIT_ERROR);
        assert_eq!(report_code(&checked(1, 1)), EXIT_ERROR);
    }

    fn saved_report(dir: &Path, name: &str, go: bool, problems: usize, misses: usize) -> PathBuf {
        let json = serde_json::json!({
            "decision": {
                "subject": "test_subject",
                "go": go,
                "saturation": "untested",
                "line": format!("string_fingerprint decision test_subject: {}", if go { "GO" } else { "NO-GO" }),
            },
            "checked": {
                "problems": vec!["p"; problems],
                "misses": vec!["m"; misses],
            },
        });
        let path = dir.join(name);
        std::fs::write(&path, json.to_string()).expect("write report");
        path
    }

    fn decide_test_spec(reports: &[PathBuf]) -> Result<(Vec<String>, u8), String> {
        decide_overall(&fixture("test_subjects.toml"), reports)
    }

    #[test]
    fn decide_exits_0_for_go_and_3_for_no_go_and_prints_misses() {
        let dir = tempfile::tempdir().expect("tempdir");
        let go = saved_report(dir.path(), "go.json", true, 0, 2);
        let (lines, code) = decide_test_spec(std::slice::from_ref(&go)).expect("decide");
        assert_eq!(code, EXIT_OK);
        let (sha, _) = file_digest(&go).expect("digest");
        assert!(lines[0].starts_with("string_fingerprint decide stamp git="));
        assert!(
            lines[0].ends_with(&format!(" reports=go.json:{sha}")),
            "{}",
            lines[0]
        );
        assert!(lines.contains(
            &"string_fingerprint decide subject=test_subject misses=2 problems=0".to_string()
        ));
        assert!(lines.last().expect("line").contains("overall: GO"));

        let no_go = saved_report(dir.path(), "no_go.json", false, 0, 0);
        let (lines, code) = decide_test_spec(&[no_go]).expect("decide");
        assert_eq!(code, EXIT_NO_GO);
        assert!(lines.last().expect("line").contains("overall: NO-GO"));
    }

    #[test]
    fn decide_exits_1_for_a_set_that_is_not_a_decision() {
        let dir = tempfile::tempdir().expect("tempdir");
        let problem = saved_report(dir.path(), "problem.json", true, 1, 0);
        assert_eq!(decide_test_spec(&[problem]).expect("decide").1, EXIT_ERROR);
        let a = saved_report(dir.path(), "a.json", false, 0, 0);
        let b = saved_report(dir.path(), "b.json", false, 0, 0);
        assert_eq!(decide_test_spec(&[a, b]).expect("decide").1, EXIT_ERROR);
        let unreadable = decide_test_spec(&[dir.path().join("none.json")]);
        assert!(unreadable.is_err(), "{unreadable:?}");
    }

    #[test]
    fn decide_over_the_committed_run_is_a_no_go_decision() {
        let run = fixture("run_2026-11-05");
        let reports: Vec<PathBuf> = [
            "apache_body",
            "openssh_body",
            "bgl_body",
            "hdfs_body",
            "clickbench_url",
            "clickbench_title",
            "clickbench_searchphrase",
            "clickbench_referer",
        ]
        .iter()
        .map(|s| run.join(format!("{s}.json")))
        .collect();
        let (lines, code) = decide_overall(&fixture("subjects.toml"), &reports).expect("decide");
        assert_eq!(code, EXIT_NO_GO);
        let committed = std::fs::read_to_string(run.join("decide.txt")).expect("decide.txt");
        let committed: Vec<&str> = committed.lines().collect();
        // Everything after the stamp line is reproduced from the run files.
        assert_eq!(
            committed.get(1..lines.len()),
            Some(&lines[1..].iter().map(String::as_str).collect::<Vec<_>>()[..])
        );
        assert_eq!(committed.get(lines.len()), Some(&"EXIT=3"));
        let stamp = committed[0];
        for r in &reports {
            let (sha, _) = file_digest(r).expect("digest");
            let name = r.file_name().expect("name").to_string_lossy();
            assert!(
                stamp.contains(&format!("{name}:{sha}")),
                "{name} not in {stamp}"
            );
        }
    }
}

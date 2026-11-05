//! Report-only measurement of a learned, conservative per-block string
//! summary for equality and `LIKE '%literal%'` predicates on a declared Str
//! column, against the production bloom filter and the production
//! `containsCellsTested` path (issue #3710, ADR-3692 Plan row T12). See
//! [`ravel_bench::string_fingerprint`] and docs/internal/bench-reports.md.
//!
//! ```text
//! string_fingerprint_report --needles PATH --corpus NAME --column NAME \
//!     --rows PATH --out PATH
//!     [--max-gram N] [--df-threshold X] [--budget-share X] [--sample-size N]
//!     [--long-value-bytes N] [--block-records N] [--zstd-level N]
//! string_fingerprint_report --needles PATH --decide REPORT...
//! ```
//!
//! `--rows` is a plain file, one column value per line (`\n`-separated, no
//! embedded `\n` within a value), in the corpus's row order. ClickBench's
//! Str columns (`URL`, `Title`, `SearchPhrase`, `Referer`) must be exported
//! to this form from `hits.parquet` before running; this tool does not read
//! Parquet. Rows are cut into blocks of `--block-records` rows each (the
//! corpus's pre-registered `block_records` when omitted), the last block
//! short if the row count does not divide evenly.
//!
//! Every override flag (`--max-gram`, `--df-threshold`, `--budget-share`,
//! `--sample-size`, `--long-value-bytes`, `--block-records`) must equal the
//! corpus/column's pre-registered value; a mismatch is refused before `--rows`
//! is read, never silently overridden. `--zstd-level` has no per-column
//! pre-registration -- it must equal the production writer's default
//! (`ravel_logseg::RlogConfig::default().zstd_level`), refused the same way,
//! since the comparison is to "the writer's zstd level" and a different level
//! would no longer measure that.
//!
//! `compressed_bytes` is the sum of `zstd::bulk::compress` over each block's
//! newline-joined row bytes at that same zstd level: a documented proxy for
//! the column's real RLOG page bytes. It omits page framing, dictionary
//! encoding, and per-page headers, so it is a lower bound on the real page
//! size, not a byte-identical production object; `fingerprint/index_bytes_share`
//! is therefore an upper bound on the true share. See
//! `string_fingerprint::BLOOM_RULE` and `string_fingerprint::CONTAINS_RULE`
//! for the other two baselines' own documented methods.
//!
//! The written report nests the [`ravel_bench::string_fingerprint::ColumnReport`]
//! under a `string_fingerprint` key (what
//! [`ravel_bench::string_fingerprint::saved_decision`] reads back) alongside
//! run provenance: the rows file's sha256 and byte count, the git commit, the
//! run date, and a host block (CPU, cores, memory, OS, rustc, commit).
//!
//! Exit codes: `0` the report was written and holds no figure problems, `1`
//! the run failed outright (a bad pre-registration, a mismatched parameter, a
//! conservativeness violation) or the report has a figure problem (the report
//! is still written in that second case so the problem can be read). With
//! `--decide`, `0` is an overall GO and `1` anything else.

use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use ravel_bench::bench_env;
use ravel_bench::logbody::grams::BlockText;
use ravel_bench::string_fingerprint::spec::{self, ColumnSpec, CorpusSpec, FingerprintSpec};
use ravel_bench::string_fingerprint::{
    ColumnInput, SavedDecision, measure, overall, saved_decision,
};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(about = "Measure a learned per-block string summary against bloom and contains")]
struct Args {
    /// The pre-registration file (fixtures/string_fingerprint/needles.toml).
    #[arg(long)]
    needles: PathBuf,
    /// The pre-registered corpus to measure.
    #[arg(long, required_unless_present = "decide")]
    corpus: Option<String>,
    /// The pre-registered column to measure.
    #[arg(long, required_unless_present = "decide")]
    column: Option<String>,
    /// The column's values, one per line, in corpus row order.
    #[arg(long, required_unless_present = "decide")]
    rows: Option<PathBuf>,
    /// Where to write the JSON report.
    #[arg(long, required_unless_present = "decide")]
    out: Option<PathBuf>,
    /// Must equal the column's pre-registered max_gram.
    #[arg(long)]
    max_gram: Option<usize>,
    /// Must equal the column's pre-registered df_threshold.
    #[arg(long)]
    df_threshold: Option<f64>,
    /// Must equal the column's pre-registered budget_share.
    #[arg(long)]
    budget_share: Option<f64>,
    /// Must equal the column's pre-registered sample_size.
    #[arg(long)]
    sample_size: Option<usize>,
    /// Must equal the column's pre-registered long_value_bytes.
    #[arg(long)]
    long_value_bytes: Option<u64>,
    /// Must equal the corpus's pre-registered block_records.
    #[arg(long)]
    block_records: Option<usize>,
    /// Must equal the production writer's zstd level.
    #[arg(long)]
    zstd_level: Option<i32>,
    /// Print the overall decision over these written reports and exit.
    #[arg(long, num_args = 1..)]
    decide: Vec<PathBuf>,
}

/// `YYYY-MM-DD` (UTC) for days since 1970-01-01 (Howard Hinnant's
/// `civil_from_days`).
fn civil_date(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

fn today() -> String {
    let days = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.as_secs() / 86_400) as i64)
        .unwrap_or(0);
    civil_date(days)
}

/// sha256 hex digest and byte count of the file at `path`.
fn file_digest(path: &PathBuf) -> Result<(String, u64), String> {
    let mut file =
        std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buf = vec![0u8; 1 << 16];
    let mut total = 0u64;
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        ctx.update(&buf[..n]);
        total += n as u64;
    }
    let hex = ctx
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok((hex, total))
}

/// Splits `bytes` on `\n` into rows, dropping one trailing empty row left by
/// a final newline and a trailing `\r` from each row (CRLF tolerance).
fn split_rows(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut rows: Vec<Vec<u8>> = bytes
        .split(|&b| b == b'\n')
        .map(|r| r.strip_suffix(b"\r").unwrap_or(r).to_vec())
        .collect();
    if rows.last().is_some_and(Vec::is_empty) {
        rows.pop();
    }
    rows
}

fn corpus<'a>(spec: &'a FingerprintSpec, name: &str) -> Result<&'a CorpusSpec, String> {
    spec.corpus(name)
        .ok_or_else(|| format!("string_fingerprint_report: {name} names no pre-registered corpus"))
}

fn column<'a>(corpus: &'a CorpusSpec, name: &str) -> Result<&'a ColumnSpec, String> {
    corpus.column(name).ok_or_else(|| {
        format!(
            "string_fingerprint_report: corpus {} names no column {name}",
            corpus.name
        )
    })
}

/// Refuses any override flag that disagrees with the pre-registration,
/// before `--rows` is read. Float comparisons use bit patterns: a parameter
/// is pinned, not merely close.
fn check_overrides(
    args: &Args,
    corpus: &CorpusSpec,
    column: &ColumnSpec,
    zstd_level: i32,
) -> Result<(), String> {
    let at = |m: String| {
        format!(
            "string_fingerprint_report: corpus {} column {}: {m}",
            corpus.name, column.name
        )
    };
    if let Some(v) = args.block_records
        && v != corpus.block_records
    {
        return Err(at(format!(
            "pre-registered at block_records {}, the run uses {v}",
            corpus.block_records
        )));
    }
    if let Some(v) = args.max_gram
        && v != column.max_gram
    {
        return Err(at(format!(
            "pre-registered at max_gram {}, the run uses {v}",
            column.max_gram
        )));
    }
    if let Some(v) = args.df_threshold
        && v.to_bits() != column.df_threshold.to_bits()
    {
        return Err(at(format!(
            "pre-registered at df_threshold {}, the run uses {v}",
            column.df_threshold
        )));
    }
    if let Some(v) = args.budget_share
        && v.to_bits() != column.budget_share.to_bits()
    {
        return Err(at(format!(
            "pre-registered at budget_share {}, the run uses {v}",
            column.budget_share
        )));
    }
    if let Some(v) = args.sample_size
        && v != column.sample_size
    {
        return Err(at(format!(
            "pre-registered at sample_size {}, the run uses {v}",
            column.sample_size
        )));
    }
    if let Some(v) = args.long_value_bytes
        && v != column.long_value_bytes
    {
        return Err(at(format!(
            "pre-registered at long_value_bytes {}, the run uses {v}",
            column.long_value_bytes
        )));
    }
    if let Some(v) = args.zstd_level
        && v != zstd_level
    {
        return Err(at(format!(
            "the production writer's zstd level is {zstd_level}, the run uses {v}"
        )));
    }
    Ok(())
}

#[derive(Serialize)]
struct Written<'a> {
    corpus: &'a str,
    column: &'a str,
    rows_path: String,
    rows_sha256: String,
    rows_bytes: u64,
    block_records: usize,
    zstd_level: i32,
    git_sha: String,
    date: String,
    host: String,
    string_fingerprint: &'a ravel_bench::string_fingerprint::ColumnReport,
}

fn decide_overall(spec_path: &PathBuf, reports: &[PathBuf]) -> ExitCode {
    let text = match std::fs::read_to_string(spec_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "string_fingerprint_report: read {}: {e}",
                spec_path.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let spec = match spec::parse(&text) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut saved: Vec<SavedDecision> = Vec::new();
    for path in reports {
        let read = std::fs::read_to_string(path)
            .map_err(|e| format!("read {}: {e}", path.display()))
            .and_then(|j| saved_decision(&j).map_err(|e| format!("{}: {e}", path.display())));
        match read {
            Ok(s) => {
                println!("{}", s.decision.line);
                saved.push(s);
            }
            Err(e) => {
                eprintln!("string_fingerprint_report: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let names = spec.columns();
    let d = overall(&names, &saved);
    println!("{}", d.line);
    if d.go {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    if !args.decide.is_empty() {
        return decide_overall(&args.needles, &args.decide);
    }
    let (Some(corpus_name), Some(column_name), Some(rows_path), Some(out)) =
        (&args.corpus, &args.column, &args.rows, &args.out)
    else {
        eprintln!("string_fingerprint_report: --corpus, --column, --rows and --out are required");
        return ExitCode::FAILURE;
    };

    let text = match std::fs::read_to_string(&args.needles) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "string_fingerprint_report: read {}: {e}",
                args.needles.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let spec = match spec::parse(&text) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            return ExitCode::FAILURE;
        }
    };
    let corpus_spec = match corpus(&spec, corpus_name) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let column_spec = match column(corpus_spec, column_name) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let zstd_level = ravel_logseg::RlogConfig::default().zstd_level;
    if let Err(e) = check_overrides(&args, corpus_spec, column_spec, zstd_level) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    let block_records = args.block_records.unwrap_or(corpus_spec.block_records);

    let (rows_sha256, rows_bytes) = match file_digest(rows_path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            return ExitCode::FAILURE;
        }
    };
    let raw = match std::fs::read(rows_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "string_fingerprint_report: read {}: {e}",
                rows_path.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let rows = split_rows(&raw);
    if rows.is_empty() {
        eprintln!(
            "string_fingerprint_report: {} has no rows",
            rows_path.display()
        );
        return ExitCode::FAILURE;
    }

    let mut blocks: Vec<BlockText> = Vec::new();
    for chunk in rows.chunks(block_records) {
        match BlockText::from_lines(chunk) {
            Some(b) => blocks.push(b),
            None => {
                eprintln!(
                    "string_fingerprint_report: {} holds a value with an embedded newline, \
                     which this row format cannot represent",
                    rows_path.display()
                );
                return ExitCode::FAILURE;
            }
        }
    }

    let compressed_bytes: u64 = match blocks
        .iter()
        .map(|b| zstd::bulk::compress(b.bytes(), zstd_level).map(|c| c.len() as u64))
        .sum::<Result<u64, _>>()
    {
        Ok(b) => b,
        Err(e) => {
            eprintln!("string_fingerprint_report: zstd: {e}");
            return ExitCode::FAILURE;
        }
    };

    let input = ColumnInput {
        blocks: &blocks,
        compressed_bytes,
        zstd_level,
    };
    let report = match measure(corpus_name, column_spec, &input) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            return ExitCode::FAILURE;
        }
    };

    for line in report.lines() {
        println!("{line}");
    }
    let problems = report.problems.clone();
    for p in &problems {
        eprintln!("string_fingerprint_report: {p}");
    }

    let written = Written {
        corpus: corpus_name,
        column: column_name,
        rows_path: rows_path.display().to_string(),
        rows_sha256,
        rows_bytes,
        block_records,
        zstd_level,
        git_sha: bench_env::git_commit(),
        date: today(),
        host: bench_env::env_header("string_fingerprint_report"),
        string_fingerprint: &report,
    };
    let json = match serde_json::to_string_pretty(&written) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("string_fingerprint_report: serialize: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = std::fs::write(out, json + "\n") {
        eprintln!("string_fingerprint_report: write {}: {e}", out.display());
        return ExitCode::FAILURE;
    }

    if problems.is_empty() {
        eprintln!(
            "string_fingerprint_report: {} blocks, report at {}",
            blocks.len(),
            out.display()
        );
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::{civil_date, split_rows};

    #[test]
    fn civil_dates() {
        assert_eq!(civil_date(0), "1970-01-01");
        assert_eq!(civil_date(19_723), "2024-01-01");
        assert_eq!(civil_date(20_740), "2026-10-14");
    }

    #[test]
    fn split_rows_drops_one_trailing_blank_and_strips_cr() {
        assert_eq!(
            split_rows(b"a\nb\nc\n"),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            split_rows(b"a\r\nb\r\n"),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(split_rows(b"a\nb"), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(split_rows(b""), Vec::<Vec<u8>>::new());
    }
}

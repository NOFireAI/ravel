//! Report-only structural decode measurement (issue #3711, ADR-3692 work
//! package 4, Plan row T13, arm 1). See [`ravel_bench::structural_fetch`] for
//! the measurement, [`ravel_bench::t13`] for the shared report machinery, and
//! docs/internal/bench-reports.md for the writeup.
//!
//! ```text
//! structural_fetch_report --subject NAME --corpus-lines CORPUS.log --out report.json
//!     [--spec PATH] [--param KEY=VALUE ...]
//! structural_fetch_report --describe --corpus-lines CORPUS.log
//! structural_fetch_report --decide report1.json report2.json ... [--spec PATH]
//! ```
//!
//! Every run parameter comes from the pre-registration file; `--param` is a
//! check, refused before the corpus is read when it differs. Exit codes are
//! [`t13::EXIT_CODES`].

use std::process::ExitCode;

use clap::Parser;
use ravel_bench::t13::{self, Arm};

fn main() -> ExitCode {
    let args = t13::Args::parse();
    match t13::exec(
        Arm::StructuralFetch,
        &args,
        ravel_bench::structural_fetch::run,
    ) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("structural_fetch_report: {e}");
            ExitCode::from(t13::EXIT_ERROR)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::fmt::Write as _;
    use std::path::{Path, PathBuf};

    use ravel_bench::string_fingerprint_report::file_digest;
    use ravel_bench::t13::Report;

    use super::*;

    /// A corpus that compresses about 1.4x, so an object above the
    /// production whole-object bound needs few lines.
    fn write_corpus(dir: &Path, lines: usize) -> PathBuf {
        const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let mut st = 11u64;
        let mut text = String::new();
        for i in 0..lines {
            let _ = write!(text, "{i} ");
            for _ in 0..200 {
                let r = t13::splitmix64(&mut st);
                text.push(ALNUM[(r % ALNUM.len() as u64) as usize] as char);
            }
            text.push('\n');
        }
        let path = dir.join("corpus.txt");
        std::fs::write(&path, text).expect("write corpus");
        path
    }

    fn write_spec(dir: &Path, sha: &str) -> PathBuf {
        let mut text = format!(
            "schema = 1\n[[subject]]\nname = \"gen\"\narm = \"structural_fetch\"\n\
             corpus = \"corpus.txt\"\nsource = \"generated\"\ncorpus_sha256 = \"{sha}\"\n\
             [subject.params]\n"
        );
        for (k, v) in [
            ("max_lines", 200_000),
            ("object_min_bytes", 10_000_000),
            ("object_max_bytes", 12_000_000),
            ("block_records", 2048),
            ("group_blocks", 32),
            ("zstd_level", 3),
            ("trace_run_rows", 4),
            ("matched_rows", 10),
            ("latency_ms", 1),
            ("max_concurrent_gets", 16),
            ("mini_block_bytes", 8192),
            ("mini_block_meta_bytes", 4),
            ("coalesce_gap", 65_536),
            ("runs", 1),
            ("seed", 5),
        ] {
            let _ = writeln!(text, "{k} = {v}");
        }
        text.push_str("[subject.figures]\n");
        for f in ravel_bench::structural_fetch::FIGURES {
            if *f == "target_blocks" {
                // Ten rows spread over ten different blocks.
                text.push_str("\"target_blocks\" = { predict = 10, band = [10, 10] }\n");
            } else {
                let _ = writeln!(
                    text,
                    "\"{f}\" = {{ unpredicted = \"generated test corpus\" }}"
                );
            }
        }
        let path = dir.join("spec.toml");
        std::fs::write(&path, text).expect("write spec");
        path
    }

    fn args(extra: &[&str]) -> t13::Args {
        let mut argv = vec!["structural_fetch_report"];
        argv.extend_from_slice(extra);
        t13::Args::try_parse_from(argv).expect("args")
    }

    #[test]
    fn measures_a_generated_corpus_and_refuses_drift_before_reading() {
        let dir = tempfile::tempdir().expect("tempdir");
        let corpus = write_corpus(dir.path(), 90_000);
        let (sha, _) = file_digest(&corpus).expect("digest");
        let spec = write_spec(dir.path(), &sha);
        let out = dir.path().join("r.json");
        let s = |p: &Path| p.to_str().expect("utf8").to_string();
        let base = [
            "--spec".to_string(),
            s(&spec),
            "--subject".into(),
            "gen".into(),
            "--out".into(),
            s(&out),
        ];

        // A drifted parameter is refused before the corpus is opened: the
        // corpus named here does not exist.
        let mut drift: Vec<String> = base.to_vec();
        drift.extend(["--corpus-lines".into(), s(&dir.path().join("none.txt"))]);
        drift.extend(["--param".into(), "runs=2".into()]);
        let e = t13::exec(
            Arm::StructuralFetch,
            &args(&drift.iter().map(String::as_str).collect::<Vec<_>>()),
            ravel_bench::structural_fetch::run,
        )
        .expect_err("drift");
        assert!(e.contains("differs from the pre-registered 1"), "{e}");
        assert!(!out.exists());

        let mut ok: Vec<String> = base.to_vec();
        ok.extend([
            "--corpus-lines".into(),
            s(&corpus),
            "--param".into(),
            "runs=1".into(),
        ]);
        let code = t13::exec(
            Arm::StructuralFetch,
            &args(&ok.iter().map(String::as_str).collect::<Vec<_>>()),
            ravel_bench::structural_fetch::run,
        )
        .expect("measure");
        assert_eq!(code, t13::EXIT_OK);
        let report: Report =
            serde_json::from_str(&std::fs::read_to_string(&out).expect("read")).expect("json");
        let get = |k: &str| {
            let v: Vec<f64> = report
                .figures
                .iter()
                .filter(|f| f.key == k)
                .map(|f| f.value)
                .collect();
            assert_eq!(v.len(), 1, "{k}");
            v[0]
        };
        assert_eq!(
            report.figures.len(),
            ravel_bench::structural_fetch::FIGURES.len()
        );
        assert!((10_000_000.0..=12_000_000.0).contains(&get("object_bytes")));
        assert_eq!(get("target_blocks"), 10.0);
        // The structural read's GETs are the shared plan's plus one per
        // coalesced range, and its bytes include the plan's.
        assert_eq!(
            get("structural/gets"),
            get("today/plan_gets") + get("structural/data_ranges")
        );
        assert!(get("structural/bytes") > get("today/plan_bytes"));
        assert!(get("structural/data_ranges") <= get("structural/raw_ranges"));
        assert_eq!(
            get("ratio/bytes"),
            get("structural/bytes") / get("today/bytes")
        );
        assert!(
            report
                .decision
                .line
                .starts_with("structural_fetch decision gen: ")
        );

        // A corpus other than the registered one is refused.
        std::fs::write(&corpus, "changed\n").expect("rewrite");
        let e = t13::exec(
            Arm::StructuralFetch,
            &args(&ok.iter().map(String::as_str).collect::<Vec<_>>()),
            ravel_bench::structural_fetch::run,
        )
        .expect_err("sha");
        assert!(e.contains("is not the pre-registered"), "{e}");
    }
}

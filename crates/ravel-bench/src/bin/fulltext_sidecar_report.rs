//! Report-only full-text sidecar measurement (issue #3711, ADR-3692 work
//! package 4, Plan row T13, arm 2). See [`ravel_bench::fulltext_sidecar`] for
//! the measurement, [`ravel_bench::t13`] for the shared report machinery, and
//! docs/internal/bench-reports.md for the writeup.
//!
//! ```text
//! fulltext_sidecar_report --subject NAME --corpus-lines CORPUS.log --out report.json
//!     [--spec PATH] [--param KEY=VALUE ...]
//! fulltext_sidecar_report --describe --corpus-lines CORPUS.log
//! fulltext_sidecar_report --decide report1.json report2.json ... [--spec PATH]
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
        Arm::FulltextSidecar,
        &args,
        ravel_bench::fulltext_sidecar::run,
    ) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("fulltext_sidecar_report: {e}");
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

    /// 6000 lines: 7 event words, 13 node words, two words on every line, and
    /// one token per line no other line holds.
    fn write_corpus(dir: &Path) -> PathBuf {
        let mut text = String::new();
        for i in 0..6000 {
            let _ = writeln!(text, "evt{} node{} status ok id{i}q", i % 7, i % 13);
        }
        let path = dir.join("corpus.txt");
        std::fs::write(&path, text).expect("write corpus");
        path
    }

    fn write_spec(dir: &Path, sha: &str) -> PathBuf {
        let mut text = format!(
            "schema = 1\n[[subject]]\nname = \"gen\"\narm = \"fulltext_sidecar\"\n\
             corpus = \"corpus.txt\"\nsource = \"generated\"\ncorpus_sha256 = \"{sha}\"\n\
             [subject.params]\n"
        );
        for (k, v) in [
            ("max_lines", 7000.0),
            ("object_rows", 2000.0),
            ("block_records", 256.0),
            ("group_blocks", 4.0),
            ("zstd_level", 3.0),
            ("sidecar_stride", 64.0),
            ("sidecar_suffix_bytes", 4096.0),
            ("rare_max_rows", 3.0),
            ("rare_sample", 6.0),
            ("scale_raw_bytes", 1e6),
            ("seed", 9.0),
        ] {
            let _ = writeln!(text, "{k} = {v:?}");
        }
        text.push_str("[subject.figures]\n");
        let exact = [
            ("lines", 6000.0),
            ("objects", 3.0),
            ("blocks", 24.0),
            // 7 + 13 + 2 + 2000 distinct tokens per object.
            ("sidecar/terms", 3.0 * 2022.0),
            ("postings/terms", 3.0 * 2022.0),
            // Five tokens on every line.
            ("sidecar/entries", 30_000.0),
            ("postings/capped_objects", 0.0),
            ("postings/verdict_mismatches", 0.0),
            ("rare/candidates", 6000.0),
            ("rare/tokens", 6.0),
            ("rare/rows", 6.0),
            ("delete/erased_only_terms", 1.0),
            ("delete/residual_terms", 0.0),
        ];
        for f in ravel_bench::fulltext_sidecar::FIGURES {
            match exact.iter().find(|(k, _)| k == f) {
                Some((_, v)) => {
                    let _ = writeln!(
                        text,
                        "\"{f}\" = {{ predict = {v:?}, band = [{v:?}, {v:?}] }}"
                    );
                }
                None => {
                    let _ = writeln!(
                        text,
                        "\"{f}\" = {{ unpredicted = \"generated test corpus\" }}"
                    );
                }
            }
        }
        let path = dir.join("spec.toml");
        std::fs::write(&path, text).expect("write spec");
        path
    }

    fn args(extra: &[&str]) -> t13::Args {
        let mut argv = vec!["fulltext_sidecar_report"];
        argv.extend_from_slice(extra);
        t13::Args::try_parse_from(argv).expect("args")
    }

    #[test]
    fn measures_a_generated_corpus_with_exact_counts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let corpus = write_corpus(dir.path());
        let (sha, _) = file_digest(&corpus).expect("digest");
        let spec = write_spec(dir.path(), &sha);
        let out = dir.path().join("r.json");
        let s = |p: &Path| p.to_str().expect("utf8").to_string();
        let argv = [
            "--spec".to_string(),
            s(&spec),
            "--subject".into(),
            "gen".into(),
            "--corpus-lines".into(),
            s(&corpus),
            "--out".into(),
            s(&out),
        ];
        let code = t13::exec(
            Arm::FulltextSidecar,
            &args(&argv.iter().map(String::as_str).collect::<Vec<_>>()),
            ravel_bench::fulltext_sidecar::run,
        )
        .expect("measure");
        let report: Report =
            serde_json::from_str(&std::fs::read_to_string(&out).expect("read")).expect("json");
        assert_eq!(code, t13::EXIT_OK, "{:?}", report.checked);
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
            ravel_bench::fulltext_sidecar::FIGURES.len()
        );
        // 6000 lines of known length over 1e6 bytes at scale.
        let raw: f64 = (0..6000)
            .map(|i| format!("evt{} node{} status ok id{i}q\n", i % 7, i % 13).len() as f64)
            .sum();
        assert_eq!(get("raw_bytes"), raw);
        assert_eq!(get("rare/scale_objects"), 1e6 / (raw / 3.0));
        assert_eq!(
            get("rare/bytes_ratio_100gb"),
            get("rare/sidecar_bytes_100gb") / get("rare/bloom_bytes_100gb")
        );
        assert_eq!(
            get("sidecar/over_postings"),
            get("sidecar/bytes") / get("postings/bytes")
        );
        // Each rare token is on one row, so the bloom keeps its one true
        // block plus whatever its hashes let through elsewhere.
        assert_eq!(
            get("rare/bloom_blocks_kept"),
            6.0 + get("rare/false_positive_blocks")
        );
        assert!(
            report
                .decision
                .line
                .starts_with("fulltext_sidecar decision gen: ")
        );

        let e = t13::exec(
            Arm::StructuralFetch,
            &args(&argv.iter().map(String::as_str).collect::<Vec<_>>()),
            ravel_bench::structural_fetch::run,
        )
        .expect_err("wrong arm");
        assert!(e.contains("is a fulltext_sidecar subject"), "{e}");
    }
}

//! Report-only full-text sidecar measurement (issue #3711, ADR-3692 work
//! package 4, Plan row T13 arm 2). See [`ravel_bench::fulltext_sidecar`] and
//! [`ravel_bench::fulltext_sidecar_report`] for the measurement itself, and
//! docs/internal/bench-reports.md for the writeup.
//!
//! ```text
//! fulltext_sidecar_report --spec PATH --subject NAME --corpus-lines CORPUS.log --out report.json
//!     [--param name=value ...]
//! fulltext_sidecar_report --spec PATH --decide report1.json report2.json ...
//! ```
//!
//! Every run parameter comes from the pre-registration file. A `--param`
//! is a check, not a setting: a value other than the pre-registered one is
//! refused before the corpus is read.
//!
//! Exit codes are [`ravel_bench::t13::EXIT_CODES`].

use std::process::ExitCode;

use ravel_bench::fulltext_sidecar_report::FulltextSidecar;

fn main() -> ExitCode {
    ravel_bench::t13::main::<FulltextSidecar>()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::path::{Path, PathBuf};

    use clap::Parser;
    use ravel_bench::t13::{Cli, EXIT_ERROR, EXIT_MISS, EXIT_NO_GO, EXIT_OK, exec};

    use super::*;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join(name)
    }

    fn run(extra: &[&str]) -> Result<u8, String> {
        let mut argv = vec!["fulltext_sidecar_report"];
        argv.extend_from_slice(extra);
        exec::<FulltextSidecar>(&Cli::try_parse_from(argv).expect("args"))
    }

    /// The test spec with `from` replaced by `to`, written into `dir`.
    fn edited_spec(dir: &Path, from: &str, to: &str) -> PathBuf {
        let text =
            std::fs::read_to_string(fixture("t13/test_fulltext_sidecar.toml")).expect("spec");
        assert!(text.contains(from), "{from} not in the test spec");
        let path = dir.join("spec.toml");
        std::fs::write(&path, text.replace(from, to)).expect("write spec");
        path
    }

    fn measure(spec: &Path, dir: &Path, params: &[&str]) -> Result<u8, String> {
        let corpus = fixture("logbody/corpus.log");
        let out = dir.join("report.json");
        let mut argv = vec![
            "--spec",
            spec.to_str().expect("utf8"),
            "--subject",
            "smoke",
            "--corpus-lines",
            corpus.to_str().expect("utf8"),
            "--out",
            out.to_str().expect("utf8"),
        ];
        for p in params {
            argv.extend_from_slice(&["--param", p]);
        }
        let code = run(&argv);
        if code.is_ok() {
            assert!(out.exists(), "the report must be written");
        }
        code
    }

    const LINES: &str = r#""object/lines" = { predict = 400, band = [400, 400] }"#;

    #[test]
    fn measure_exits_0_for_a_clean_in_band_report() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spec = fixture("t13/test_fulltext_sidecar.toml");
        assert_eq!(measure(&spec, dir.path(), &["seed=7"]), Ok(EXIT_OK));
    }

    #[test]
    fn measure_exits_2_for_a_band_miss_without_a_problem() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spec = edited_spec(
            dir.path(),
            LINES,
            r#""object/lines" = { predict = 399, band = [399, 399] }"#,
        );
        assert_eq!(measure(&spec, dir.path(), &[]), Ok(EXIT_MISS));
    }

    #[test]
    fn measure_exits_1_for_a_problem_or_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A figure the run emits but the pre-registration calls absent is a
        // problem; the miss beside it does not decide the code.
        let spec = edited_spec(dir.path(), LINES, r#""object/lines" = { absent = "test" }"#);
        assert_eq!(measure(&spec, dir.path(), &[]), Ok(EXIT_ERROR));

        let spec = fixture("t13/test_fulltext_sidecar.toml");
        let missing = run(&[
            "--spec",
            spec.to_str().expect("utf8"),
            "--subject",
            "smoke",
            "--corpus-lines",
            fixture("t13/no-such-corpus.log").to_str().expect("utf8"),
            "--out",
            dir.path().join("r.json").to_str().expect("utf8"),
        ]);
        assert!(missing.is_err(), "{missing:?}");
    }

    #[test]
    fn a_drifted_parameter_is_refused_before_the_corpus_is_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spec = fixture("t13/test_fulltext_sidecar.toml");
        let out = dir.path().join("report.json");
        // The corpus does not exist: a refusal that read it first would say so.
        for p in ["seed=8", "no_such_param=1"] {
            let err = run(&[
                "--spec",
                spec.to_str().expect("utf8"),
                "--subject",
                "smoke",
                "--corpus-lines",
                fixture("t13/no-such-corpus.log").to_str().expect("utf8"),
                "--out",
                out.to_str().expect("utf8"),
                "--param",
                p,
            ])
            .expect_err("refused");
            assert!(err.contains("smoke"), "{err}");
            assert!(!err.contains("no-such-corpus"), "{err}");
        }
        assert!(!out.exists());
    }

    fn saved_report(dir: &Path, name: &str, subject: &str, go: bool, problems: usize) -> PathBuf {
        let json = serde_json::json!({
            "decision": {
                "subject": subject,
                "go": go,
                "variants": Vec::<String>::new(),
                "line": format!("fulltext_sidecar decision {subject}: {}", if go { "GO" } else { "NO-GO" }),
            },
            "checked": { "problems": vec!["p"; problems], "misses": Vec::<String>::new() },
        });
        let path = dir.join(name);
        std::fs::write(&path, json.to_string()).expect("write report");
        path
    }

    #[test]
    fn decide_exits_0_for_go_3_for_no_go_and_1_when_not_a_decision() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spec = fixture("t13/test_fulltext_sidecar.toml");
        let decide = |reports: &[PathBuf]| {
            let mut argv = vec!["--spec", spec.to_str().expect("utf8"), "--decide"];
            argv.extend(reports.iter().map(|p| p.to_str().expect("utf8")));
            run(&argv)
        };
        let go = saved_report(dir.path(), "go.json", "smoke", true, 0);
        assert_eq!(decide(std::slice::from_ref(&go)), Ok(EXIT_OK));
        let no_go = saved_report(dir.path(), "no_go.json", "smoke", false, 0);
        assert_eq!(decide(&[no_go]), Ok(EXIT_NO_GO));
        let problem = saved_report(dir.path(), "p.json", "smoke", true, 1);
        assert_eq!(decide(&[problem]), Ok(EXIT_ERROR));
        assert_eq!(decide(&[go.clone(), go]), Ok(EXIT_ERROR));
        let stranger = saved_report(dir.path(), "x.json", "other", true, 0);
        assert_eq!(decide(&[stranger]), Ok(EXIT_ERROR));
    }
}

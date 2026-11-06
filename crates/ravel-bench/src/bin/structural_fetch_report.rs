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
            ("coalesce_gap", 1_887_437),
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

    /// Every byte and GET figure of `run()` over a fixed generated object,
    /// exactly. Fails if the structural bytes drop the mini-block index
    /// (`+ splan.meta_bytes`), if today's side at a sweep gap is built
    /// without that gap, or if the L0 structural ranges escape the run cap.
    #[test]
    fn pins_the_exact_figures_of_a_generated_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let corpus = write_corpus(dir.path(), 90_000);
        let (sha, _) = file_digest(&corpus).expect("digest");
        let spec = t13::read_spec(&write_spec(dir.path(), &sha)).expect("spec");
        let subject = spec.subject("gen").expect("subject");
        let m = ravel_bench::structural_fetch::run(subject, &corpus).expect("run");
        let get = |k: &str| {
            let v: Vec<f64> = m
                .figures
                .iter()
                .filter(|f| f.key == k)
                .map(|f| f.value)
                .collect();
            assert_eq!(v.len(), 1, "{k}");
            v[0]
        };
        let exact = [
            ("rows", 65_536.0),
            ("blocks", 32.0),
            ("object_bytes", 10_584_418.0),
            ("target_blocks", 10.0),
            ("today/uncomp_bytes", 4_384_239.0),
            ("structural/layout_bytes", 10_457_903.0),
            ("structural/raw_ranges", 71.0),
            ("structural/meta_bytes", 6_016.0),
            ("structural/uncomp_bytes", 183_529.0),
            // Production gap on L0: today reads the whole object after the
            // plan, the structural ranges coalesce to one.
            ("today/plan_gets", 1.0),
            ("today/plan_bytes", 262_144.0),
            ("today/gets", 2.0),
            ("today/bytes", 262_144.0 + 10_584_418.0),
            ("today/data_over_object", 1.0),
            ("structural/data_ranges", 1.0),
            ("structural/gets", 2.0),
            ("structural/bytes", 10_726_043.0),
            // 64 KiB on L1: thirteen ranged GETs on today's side, twelve
            // mini-block ranges on the structural side, whose bytes are the
            // plan, the index and the data: 262,144 + 6,016 + 405,458.
            ("gap_64k/today/plan_gets", 1.0),
            ("gap_64k/today/plan_bytes", 262_144.0),
            ("gap_64k/today/gets", 14.0),
            ("gap_64k/today/bytes", 3_742_501.0),
            ("gap_64k/structural/data_ranges", 12.0),
            ("gap_64k/structural/gets", 13.0),
            ("gap_64k/structural/bytes", 262_144.0 + 6_016.0 + 405_458.0),
            ("gap_64k/ratio/bytes", 673_618.0 / 3_742_501.0),
            ("gap_64k/ratio/gets", 13.0 / 14.0),
            ("gap_256k/today/gets", 14.0),
            ("gap_256k/today/bytes", 3_742_501.0),
            ("gap_256k/structural/bytes", 673_618.0),
            // 1 MiB bridges targets three blocks apart: whole object again.
            ("gap_1m/today/gets", 2.0),
            ("gap_1m/today/bytes", 10_846_562.0),
            ("gap_1m/structural/data_ranges", 1.0),
            ("gap_1m/structural/bytes", 10_726_043.0),
            // 64 KiB on L0: the four-run cap spans the targets on both
            // sides, so today reads the whole object.
            ("l0_64k/today/gets", 2.0),
            ("l0_64k/today/bytes", 10_846_562.0),
            ("l0_64k/structural/data_ranges", 4.0),
            ("l0_64k/structural/gets", 5.0),
            ("l0_64k/structural/bytes", 7_706_827.0),
        ];
        for (k, want) in exact {
            assert_eq!(get(k), want, "{k}");
        }
        // Modelled decode CPU is today's at the same gap scaled by the
        // decompressed bytes each side decodes.
        for p in ["", "gap_64k/", "gap_256k/", "gap_1m/", "l0_64k/"] {
            let today = get(&format!("{p}today/decode_cpu_ms"));
            let s = get(&format!("{p}structural/decode_cpu_ms"));
            let want = today * 183_529.0 / 4_384_239.0;
            assert!((s - want).abs() <= want * 1e-12, "{p}: {s} vs {want}");
        }
        let d = t13::decide(Arm::StructuralFetch, "gen", &m.figures);
        assert!(d.go, "{}", d.line);
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
        // serde_json's default float parse can land one ulp off the written
        // value, so a ratio read back from the report is compared relatively.
        let ratio = get("structural/bytes") / get("today/bytes");
        assert!(
            (get("ratio/bytes") - ratio).abs() <= ratio.abs() * 1e-12,
            "{} against {ratio}",
            get("ratio/bytes")
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

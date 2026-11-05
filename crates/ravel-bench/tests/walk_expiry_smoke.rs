//! `mcp_walk_expiry --profile smoke` end to end: both servers start over a
//! `MemoryStore`, the load runs, every cell of the grid walks through the
//! probe and the cross-check walks through `POST /mcp`, and `result.json`
//! carries every pre-registered figure exactly once. The smoke bands are wide,
//! so this proves the bin runs and reports, not any expiry rate.
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::process::Command;

use ravel_bench::walk_expiry::{BANDS_TOML, Rates, expected_figures, load_bands};

#[test]
fn the_smoke_profile_runs_end_to_end_and_emits_every_figure() {
    let out = tempfile::tempdir().expect("temp dir");
    let output = Command::new(env!("CARGO_BIN_EXE_mcp_walk_expiry"))
        .args(["--profile", "smoke", "--store", "memory", "--out-dir"])
        .arg(out.path())
        .output()
        .expect("the bin runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "exit {:?}\nstdout:\n{stdout}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("RESULT: PASS"), "{stdout}");

    let result: serde_json::Value = serde_json::from_slice(
        &std::fs::read(out.path().join("result.json")).expect("result.json written"),
    )
    .expect("result.json parses");
    assert_eq!(result["result"], "PASS");
    assert!(out.path().join("summary.txt").exists());

    let smoke = load_bands(BANDS_TOML).expect("bands load").smoke;
    let rates: Rates = Rates {
        commit_interval_s: result["stamps"]["rates"]["commit_interval_s"].as_f64(),
        commit_lag_s: result["stamps"]["rates"]["commit_lag_s"].as_f64(),
    };
    let wanted = expected_figures(&smoke, rates);
    let rows = result["figures"].as_array().expect("figures array");
    let by_name: BTreeMap<&str, usize> = rows
        .iter()
        .map(|row| {
            (
                row["name"].as_str().expect("named"),
                row["values"].as_array().expect("values").len(),
            )
        })
        .collect();
    assert_eq!(by_name.len(), wanted.len());
    for figure in &wanted {
        assert_eq!(
            by_name.get(figure.name.as_str()),
            Some(&1),
            "{} emitted exactly once",
            figure.name
        );
    }

    // Every cell, the two cross-check cells included, ran all its walks and
    // read at least one page per walk.
    let cells = result["cells"].as_array().expect("cells array");
    assert_eq!(
        cells.len(),
        smoke.probe_cells().len() + smoke.mcp_cells().len()
    );
    assert_eq!(
        (smoke.grid.walks_per_cell, smoke.grid.mcp_crosscheck_walks),
        (2, 2)
    );
    for cell in cells {
        assert_eq!(cell["tally"]["walks"], 2, "{}", cell["label"]);
        assert_eq!(cell["tally"]["short"], 0, "{}", cell["label"]);
        assert_eq!(cell["tally"]["failed"], 0, "{}", cell["label"]);
    }
    let pages = result["stamps"]["pages"].as_u64().expect("pages");
    assert!(pages >= 2 * cells.len() as u64, "{pages} pages");
}

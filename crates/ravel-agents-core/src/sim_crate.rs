//! `ravel-agents-sim` is dev-only (ADR-3331 decision 1, ADR-0068): it may be
//! a dev-dependency, never a normal or build dependency of a workspace
//! crate. Cargo does not enforce that, so this test reads `cargo metadata`.

#![allow(clippy::expect_used)]

use serde_json::Value;

const SIM: &str = "ravel-agents-sim";

/// Workspace packages with a normal or build dependency on the sim crate,
/// and the number of packages scanned.
fn shipping_dependents(metadata: &Value) -> (Vec<String>, usize) {
    let packages = metadata["packages"].as_array().cloned().unwrap_or_default();
    let mut offenders = Vec::new();
    for package in &packages {
        let name = package["name"].as_str().unwrap_or_default();
        let deps = package["dependencies"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let ships_sim = deps
            .iter()
            .any(|dep| dep["name"] == SIM && dep["kind"].as_str() != Some("dev"));
        if ships_sim && name != SIM {
            offenders.push(name.to_string());
        }
    }
    (offenders, packages.len())
}

#[test]
fn no_workspace_crate_depends_on_the_sim_crate() {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.toml");
    let output = std::process::Command::new(cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
            "--manifest-path",
            manifest,
        ])
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).expect("metadata JSON");
    let (offenders, scanned) = shipping_dependents(&metadata);
    let names: Vec<&str> = metadata["packages"]
        .as_array()
        .map(|p| p.iter().filter_map(|p| p["name"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        names.contains(&SIM) && names.contains(&"ravel-server"),
        "metadata did not list the workspace: {names:?}"
    );
    assert!(scanned > 20, "only {scanned} packages scanned");
    assert_eq!(offenders, Vec::<String>::new());
}

#[test]
fn the_scan_reports_a_normal_or_build_dependency_and_allows_a_dev_one() {
    let metadata = serde_json::json!({
        "packages": [
            {"name": "a", "dependencies": [{"name": SIM, "kind": null}]},
            {"name": "b", "dependencies": [{"name": SIM, "kind": "build"}]},
            {"name": "c", "dependencies": [{"name": SIM, "kind": "dev"}]},
            {"name": "d", "dependencies": [{"name": "ravel-sim", "kind": null}]},
            {"name": SIM, "dependencies": []},
        ]
    });
    assert_eq!(
        shipping_dependents(&metadata),
        (vec!["a".to_string(), "b".to_string()], 5)
    );
}

//! One integration test binary for most of the files under `tests/`, so the
//! crate links one binary for them instead of one per file (issue #2523).
//!
//! Each member file stays at its own path and is compiled here as a module, so
//! a test reports as `<file>::<test>`, and `file!()` (which proptest uses to
//! find a `<file>.proptest-regressions` seed file) still names the member file.
//! The flight members carry `#![cfg(feature = "flight-sql")]` and compile to
//! nothing without that feature.
//!
//! Files that cannot share a process with these stay separate targets in
//! Cargo.toml: those run by name (`--test`), those that install a global
//! allocator, and those that assert on process-wide state another file's tests
//! would disturb.

mod util;

#[path = "admission_parity.rs"]
mod admission_parity;
#[path = "aggregate_emit_reservation.rs"]
mod aggregate_emit_reservation;
#[path = "alerts_provider.rs"]
mod alerts_provider;
#[path = "attrs_map_vs_declared.rs"]
mod attrs_map_vs_declared;
#[path = "audit_provider.rs"]
mod audit_provider;
#[path = "bounded_topk_aggregate.rs"]
mod bounded_topk_aggregate;
#[path = "ceiling_breach.rs"]
mod ceiling_breach;
#[path = "deadline.rs"]
mod deadline;
#[path = "distinct_on.rs"]
mod distinct_on;
#[path = "erasure_dedup_positions.rs"]
mod erasure_dedup_positions;
#[path = "expr_planner_surface.rs"]
mod expr_planner_surface;
#[path = "extract_field.rs"]
mod extract_field;
#[path = "flight.rs"]
mod flight;
#[path = "flight_differential.rs"]
mod flight_differential;
#[path = "flight_distributed.rs"]
mod flight_distributed;
#[path = "flight_erasure.rs"]
mod flight_erasure;
#[path = "flight_tenancy.rs"]
mod flight_tenancy;
#[path = "io_shape_accounting.rs"]
mod io_shape_accounting;
#[path = "labels_dict_compaction.rs"]
mod labels_dict_compaction;
#[path = "live_accounting.rs"]
mod live_accounting;
#[path = "logs_columnar.rs"]
mod logs_columnar;
#[path = "logs_count_from_stats.rs"]
mod logs_count_from_stats;
#[path = "logs_declared_columns.rs"]
mod logs_declared_columns;
#[path = "logs_declared_minmax_from_stamps.rs"]
mod logs_declared_minmax_from_stamps;
#[path = "logs_differential.rs"]
mod logs_differential;
#[path = "logs_fast_path_projection_routing.rs"]
mod logs_fast_path_projection_routing;
#[path = "logs_metadata_agg.rs"]
mod logs_metadata_agg;
#[path = "logs_plan_concurrency.rs"]
mod logs_plan_concurrency;
#[path = "logs_provider.rs"]
mod logs_provider;
#[path = "logs_request_cost_knob_routing.rs"]
mod logs_request_cost_knob_routing;
#[path = "logs_scan_fetch_pushdown.rs"]
mod logs_scan_fetch_pushdown;
#[path = "logs_selective_scan_amplification.rs"]
mod logs_selective_scan_amplification;
#[path = "logs_stats_load.rs"]
mod logs_stats_load;
#[path = "logs_striped_directory_accounting.rs"]
mod logs_striped_directory_accounting;
#[path = "logs_topk_late_materialization.rs"]
mod logs_topk_late_materialization;
#[path = "logs_trace_id_hex_literal.rs"]
mod logs_trace_id_hex_literal;
#[path = "logs_uncached_assignment.rs"]
mod logs_uncached_assignment;
#[path = "logs_whole_object_phase_wire_bytes.rs"]
mod logs_whole_object_phase_wire_bytes;
#[path = "logs_whole_segment_fast_path.rs"]
mod logs_whole_segment_fast_path;
#[path = "memory_accounting.rs"]
mod memory_accounting;
#[path = "memory_ceiling_and_budget_errors.rs"]
mod memory_ceiling_and_budget_errors;
#[path = "memory_group_values_compensation.rs"]
mod memory_group_values_compensation;
#[path = "memory_pool_attribution.rs"]
mod memory_pool_attribution;
#[path = "merged_run_dedup_parity.rs"]
mod merged_run_dedup_parity;
#[path = "page_walk.rs"]
mod page_walk;
#[path = "parallel_final_aggregation.rs"]
mod parallel_final_aggregation;
#[path = "parallel_final_default.rs"]
mod parallel_final_default;
#[path = "parquet_ddl.rs"]
mod parquet_ddl;
#[path = "parquet_tables.rs"]
mod parquet_tables;
#[path = "pipeline.rs"]
mod pipeline;
#[path = "pushdown_memory.rs"]
mod pushdown_memory;
#[path = "query_accounting.rs"]
mod query_accounting;
#[path = "scan_budgets.rs"]
mod scan_budgets;
#[path = "scan_soa_adoption.rs"]
mod scan_soa_adoption;
#[path = "security.rs"]
mod security;
#[path = "segment_timing.rs"]
mod segment_timing;
#[path = "skip_partial_aggregation.rs"]
mod skip_partial_aggregation;
#[path = "sliding_frame_total_order.rs"]
mod sliding_frame_total_order;
#[path = "snapshot_retry.rs"]
mod snapshot_retry;
#[path = "spans_columnar.rs"]
mod spans_columnar;
#[path = "spans_differential.rs"]
mod spans_differential;
#[path = "spans_provider.rs"]
mod spans_provider;
#[path = "spill.rs"]
mod spill;
#[path = "sql_postings_prefix_pruning.rs"]
mod sql_postings_prefix_pruning;
#[path = "sql_postings_pruning.rs"]
mod sql_postings_pruning;
#[path = "statement_complexity.rs"]
mod statement_complexity;
#[path = "stddev_var_family_rejected.rs"]
mod stddev_var_family_rejected;
#[path = "validate_grouped_minmax_and_stddev.rs"]
mod validate_grouped_minmax_and_stddev;

/// With `autotests = false`, a file added under `tests/` is compiled by
/// nothing until it is declared, and a test that is never compiled never
/// fails. Every `.rs` file beside this one must be exactly one of: a module
/// above, or a `[[test]]` target in Cargo.toml.
#[test]
#[allow(clippy::expect_used)]
fn every_test_file_is_a_member_or_a_declared_target() {
    let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let tests_dir = crate_dir.join("tests");
    let root = std::fs::read_to_string(tests_dir.join("it.rs")).expect("read tests/it.rs");
    let manifest = std::fs::read_to_string(crate_dir.join("Cargo.toml")).expect("read Cargo.toml");

    let mut files = Vec::new();
    for entry in std::fs::read_dir(&tests_dir).expect("list tests/") {
        let path = entry.expect("read a tests/ entry").path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            let name = path.file_name().expect("a file name").to_string_lossy();
            files.push(name.into_owned());
        }
    }
    // A scan that finds almost nothing would pass every file it did not see.
    assert!(
        files.len() >= 70,
        "found only {} test files under tests/; the scan itself is broken",
        files.len()
    );

    let mut problems = Vec::new();
    for file in files.iter().filter(|file| file.as_str() != "it.rs") {
        let member = root.contains(&format!("#[path = \"{file}\"]"));
        let target = manifest.contains(&format!("path = \"tests/{file}\""));
        match (member, target) {
            (true, false) | (false, true) => {}
            (false, false) => problems.push(format!(
                "{file}: neither a module of tests/it.rs nor a [[test]] target, so it never runs"
            )),
            (true, true) => problems.push(format!(
                "{file}: both a module of tests/it.rs and a [[test]] target, so it runs twice"
            )),
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

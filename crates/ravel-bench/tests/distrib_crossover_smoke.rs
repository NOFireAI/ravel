//! Smoke test for the local-vs-distributed crossover bench core (ADR-0071,
//! issue #869): runs `distrib_crossover::run` over a tiny `MemoryStore` corpus
//! and asserts the two paths agree on what the query computes and on the
//! host-independent cost counters, exactly the invariants ADR-0071 guarantees.
//! This is the CI/smoke-runnable target for the bench; it exercises the same
//! path the `distrib_crossover_bench` bin runs (mirrors `s3_e2e_smoke.rs` /
//! `query_latency_smoke.rs`).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_bench::distrib_crossover::{CrossoverConfig, run};
use ravel_object_store::memory::MemoryStore;

#[tokio::test]
async fn distributed_crossover_matches_local_over_memory_store() {
    let store = Arc::new(MemoryStore::new());
    let config = CrossoverConfig::smoke(store, "memory".to_string());
    let report = run(&config).await;

    assert!(report.accepted_points > 0, "corpus must ingest something");
    assert!(
        report.corpus_segments > 0,
        "corpus must resolve to at least one segment"
    );
    assert!(
        report.corpus_shards >= 2,
        "corpus must span multiple shards so the fan-out cuts multiple slices, got {}",
        report.corpus_shards
    );

    // The local baseline panel, then one distributed panel per worker count.
    assert_eq!(
        report.panels.len(),
        1 + config.worker_counts.len(),
        "one local panel plus one distributed panel per worker count"
    );
    let local = report
        .panels
        .iter()
        .find(|p| p.path == "local")
        .expect("a local panel");
    assert!(
        local.matched_series > 0,
        "the query must match series (else the crossover measures nothing)"
    );

    for panel in report.panels.iter().filter(|p| p.path == "distributed") {
        // Distribution changes where bytes are fetched, never what the query
        // computes: the matched-series count is identical to local.
        assert_eq!(
            panel.matched_series, local.matched_series,
            "distributed worker_count={} matched {} series, local matched {}",
            panel.worker_count, panel.matched_series, local.matched_series
        );
        // ADR-0071 performance invariant: a scalar query's store-request count
        // and bytes-moved are identical local vs distributed (the distributed
        // path folds every worker's real accounting, not zeros).
        assert_eq!(
            panel.s3_requests, local.s3_requests,
            "distributed s3_requests must equal local for a scalar query"
        );
        assert_eq!(
            panel.s3_bytes, local.s3_bytes,
            "distributed bytes-moved must equal local for a scalar query"
        );
        assert!(
            panel.s3_bytes > 0 && panel.s3_requests > 0,
            "the distributed panel must report real folded accounting, not zeros"
        );
        assert_eq!(
            panel.segments_fetched, local.segments_fetched,
            "both paths fetch the same pinned snapshot"
        );
    }
}

/// Every metric column the panel reports -- including the ADR-0074 additions
/// `wall_ms_p99` and `coordinator_cpu_ms` -- is present and finite on the local
/// panel and on every worker-count panel. This drives the real `run()` path the
/// bin uses (not a helper in isolation) over a tiny in-process `MemoryStore`, so
/// it runs in generic CI with no MinIO, and it is the gate that a new column
/// stays wired through `panel_report` into the serialized report.
#[tokio::test]
async fn crossover_reports_all_metric_columns() {
    let store = Arc::new(MemoryStore::new());
    let config = CrossoverConfig::smoke(store, "memory".to_string());
    let report = run(&config).await;

    // One local panel plus one per worker count; each must carry finite metrics.
    assert_eq!(
        report.panels.len(),
        1 + config.worker_counts.len(),
        "one local panel plus one distributed panel per worker count"
    );
    assert!(
        report.panels.iter().any(|p| p.path == "local"),
        "a local baseline panel must be present"
    );
    for wc in &config.worker_counts {
        assert!(
            report
                .panels
                .iter()
                .any(|p| p.path == "distributed" && p.worker_count == *wc),
            "a distributed panel must be present for worker_count={wc}"
        );
    }

    for panel in &report.panels {
        let label = format!("panel path={} workers={}", panel.path, panel.worker_count);
        // Wall-time percentiles, including the new p99, are finite and ordered.
        for (name, v) in [
            ("wall_ms_p50", panel.wall_ms_p50),
            ("wall_ms_p95", panel.wall_ms_p95),
            ("wall_ms_p99", panel.wall_ms_p99),
            ("wall_ms_max", panel.wall_ms_max),
            ("coordinator_cpu_ms", panel.coordinator_cpu_ms),
        ] {
            assert!(v.is_finite(), "{label}: {name} must be finite, got {v}");
            assert!(v >= 0.0, "{label}: {name} must be non-negative, got {v}");
        }
        assert!(
            panel.wall_ms_p50 <= panel.wall_ms_p95 + f64::EPSILON
                && panel.wall_ms_p95 <= panel.wall_ms_p99 + f64::EPSILON
                && panel.wall_ms_p99 <= panel.wall_ms_max + f64::EPSILON,
            "{label}: percentiles must be monotonic non-decreasing \
             (p50={} p95={} p99={} max={})",
            panel.wall_ms_p50,
            panel.wall_ms_p95,
            panel.wall_ms_p99,
            panel.wall_ms_max,
        );
        // The cost counters are populated on every panel.
        assert!(
            panel.matched_series > 0,
            "{label}: matched_series must be present (the query must match series)"
        );
        assert!(
            panel.segments_fetched > 0,
            "{label}: segments_fetched must be present"
        );
        assert!(
            panel.s3_requests > 0 && panel.s3_get_requests > 0 && panel.s3_bytes > 0,
            "{label}: store-request and bytes-moved columns must be present, got \
             reqs={} gets={} bytes={}",
            panel.s3_requests,
            panel.s3_get_requests,
            panel.s3_bytes,
        );
    }
}

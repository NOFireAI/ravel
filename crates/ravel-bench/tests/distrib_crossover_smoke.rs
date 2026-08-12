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

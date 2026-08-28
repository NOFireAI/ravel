//! Smoke tests for the `sql_latency_bench` harness (ADR-0100 decision 4),
//! behind the `sql-latency` feature. They drive the `--generate` lane end to
//! end against a `MemoryStore` and pin the three properties the report
//! contract rests on:
//!
//! - the generated lane reports per-statement min/median/max, a cold number,
//!   non-zero scan block counters, and the dataset object count;
//! - an entry whose `required_declarations` are not satisfied is *skipped*
//!   (never run) with the missing key named, and is absent from the measured
//!   set;
//! - `--runs 1` still reports a cold number, with min == median == max.
//!
//! The `--tenant` lane cannot be exercised here: it needs a tenant already
//! loaded into durable storage by `ravel-cli load --parquet`, which CI does not
//! provide. Its code path is deliberately thin over `measure_corpus`/
//! `dataset_info`, both of which the generated lane exercises fully.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_query::DEFAULT_FETCH_CONCURRENCY;

use ravel_bench::sql_corpus::{
    CorpusEntry, Modification, RequiredDeclaration, RequiredDeclaredType, checked_default_corpus,
};
use ravel_bench::sql_latency::{GenerateConfig, measure_corpus, run_generated};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_sql::DEFAULT_MAX_QUERY_BYTES;
use ravel_types::{TenantId, TimeRange};

/// The frozen query clock the generated lane uses (`4h` in ns); the generated
/// data lands in ingest-hour bucket 0 near the epoch, so a resolve over
/// `[0, NOW_NS]` stays bounded.
const NOW_NS: i64 = 4 * 3_600_000_000_000;

fn small_generate_config(store: Arc<dyn ObjectStoreBackend>, runs: usize) -> GenerateConfig {
    GenerateConfig {
        store,
        store_backend: "memory".to_string(),
        region: "n/a".to_string(),
        endpoint: "n/a".to_string(),
        entries: checked_default_corpus().expect("checked-in corpus gates"),
        runs,
        // Small enough for CI, but split across several objects so the object
        // count and the block counters are meaningfully non-trivial.
        records: 60,
        records_per_object: 20,
        extra_attrs: 4,
        max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
        cache_bytes: 0,
        deadline: Duration::from_secs(30),
        continue_on_error: false,
        fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
        progress_jsonl: None,
        tenant_max_bytes: ravel_bench::sql_latency::DEFAULT_TENANT_MAX_BYTES,
        parallel_final_aggregation: false,
        max_segments: ravel_query::DEFAULT_MAX_SEGMENTS,
        explain_dir: None,
        warm_catalog: false,
    }
}

#[tokio::test]
async fn generated_lane_reports_per_query_min_median_max_and_scan_diagnostics() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = run_generated(&small_generate_config(Arc::clone(&store), 3))
        .await
        .expect("generated lane runs");

    // Dataset-level: the object count is a first-class figure and must be
    // populated. 60 records at 20 per object is 3 objects.
    assert!(
        report.dataset.object_count > 0,
        "dataset object count must be reported and non-zero"
    );
    assert_eq!(report.dataset.object_count, 3, "60 records / 20 per object");
    // Pinned to the exact count, not `> 0`. A harness whose whole job is to
    // report magnitudes has to be checked against magnitudes: `> 0` holds just
    // as well when a figure is a fraction of the truth, which is how an
    // accounting bug survives a green suite. Every record generated must be
    // counted once.
    assert_eq!(
        report.dataset.rows, 60,
        "every generated record is counted exactly once"
    );
    // Bytes cannot be pinned exactly (encoding and compression move with the
    // format), so bound them PER OBJECT rather than in absolute terms. An
    // absolute floor is the trap: this fixture stores about 1300 bytes per
    // object, so any floor low enough to be safe for one object is also passed
    // by a figure that counts only one object out of three. Verified by
    // mutation -- summing `.take(1)` of the segments passed a flat 1 KiB floor
    // and fails this. The per-object band is what makes an under-count visible.
    let per_object = report.dataset.stored_bytes / report.dataset.object_count as u64;
    assert!(
        (512..1024 * 1024).contains(&per_object),
        "stored bytes {} over {} objects is {} per object, outside the \
         plausible band: the figure is being under- or over-counted",
        report.dataset.stored_bytes,
        report.dataset.object_count,
        per_object
    );
    assert_eq!(report.dataset.layout, "pre-compaction");
    assert_eq!(report.provenance.runs, 3);

    // Every checked-in entry is satisfied by the installed declaration union,
    // so nothing is skipped and every entry is measured.
    assert!(
        report.skipped.is_empty(),
        "generated lane installs the union of required declarations: {:?}",
        report.skipped
    );
    let corpus_len = checked_default_corpus().expect("corpus").len();
    assert_eq!(
        report.entries.len(),
        corpus_len,
        "every corpus entry is measured"
    );

    // Per-entry: min <= median <= max, a positive cold number, and the two
    // typed-duration entries carry their declared dependency (so they ran, not
    // skipped).
    for e in &report.entries {
        assert!(
            e.min_ms <= e.median_ms && e.median_ms <= e.max_ms,
            "entry `{}` violates min<=median<=max: {} {} {}",
            e.id,
            e.min_ms,
            e.median_ms,
            e.max_ms
        );
        assert!(e.cold_ms > 0.0, "entry `{}` cold time must be > 0", e.id);
    }
    for id in ["typed_duration_threshold_count", "typed_duration_sum"] {
        assert!(
            report.entries.iter().any(|e| e.id == id),
            "typed entry `{id}` must be measured, not skipped"
        );
    }

    // Scan diagnostics: at least one statement scans blocks over the durable
    // objects, proving the block counters are wired rather than always zero.
    // Every in-process entry carries them; only the Flight lane reports `None`.
    for e in &report.entries {
        assert!(
            e.scan.is_some(),
            "the in-process lane reports scan diagnostics for `{}`",
            e.id
        );
    }
    let scans: Vec<&ravel_bench::sql_latency::ScanDiagnostics> = report
        .entries
        .iter()
        .filter_map(|e| e.scan.as_ref())
        .collect();
    assert!(
        scans.iter().any(|s| s.blocks_total > 0),
        "at least one entry must report a non-zero blocks_total"
    );
    assert!(
        scans
            .iter()
            .any(|s| s.segments == report.dataset.object_count),
        "a full-window statement sees every dataset segment"
    );

    // A scanning statement must charge object-store reads proportional to the
    // dataset, not merely non-zero ones. `> 0` cannot distinguish a counter
    // wired to one object from a counter wired to all three, and this harness
    // exists to attribute per-object cost -- a systematically low figure here
    // would point other epics at the wrong bottleneck. Each of the three
    // objects needs at least one GET, and its bytes must reach the same
    // plausible floor the dataset does.
    let scanning = report
        .entries
        .iter()
        .filter_map(|e| e.scan.as_ref().map(|s| (e, s)))
        .filter(|(_, s)| s.blocks_total > 0)
        .max_by_key(|(_, s)| s.object_store_get_requests)
        .expect("at least one entry scans blocks");
    assert!(
        scanning.1.object_store_get_requests >= report.dataset.object_count as u64,
        "entry `{}` charged {} GETs for a {}-object dataset: fewer GETs than \
         objects means the accounting is under-counting reads",
        scanning.0.id,
        scanning.1.object_store_get_requests,
        report.dataset.object_count
    );
    assert!(
        scanning.1.object_store_bytes >= 1_024,
        "entry `{}` charged only {} object-store bytes across {} objects",
        scanning.0.id,
        scanning.1.object_store_bytes,
        report.dataset.object_count
    );

    // The report serializes to JSON (the machine-readable half of the
    // contract).
    serde_json::to_string(&report).expect("report serializes");
}

#[tokio::test]
async fn an_entry_with_an_unsatisfied_required_declaration_is_skipped_with_the_key_named() {
    // Publish a real dataset so the statement below is genuinely executable.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = run_generated(&small_generate_config(Arc::clone(&store), 1))
        .await
        .expect("generated lane runs");
    let tenant = TenantId::new(report.provenance.dataset_id.clone());

    // The statement reads only fixed columns, so it runs fine against the base
    // schema -- but it *requires* the `duration_ms` declared column. Measure it
    // with an EMPTY declared set, the way the tenant lane would against a tenant
    // that never declared duration_ms.
    let entry = CorpusEntry {
        id: "needs_duration".to_string(),
        sql: "SELECT count(*) FROM logs".to_string(),
        constructs: vec!["count".to_string()],
        expected_rows: Some(1),
        upstream_id: None,
        modified: Modification::Verbatim,
        required_declarations: vec![RequiredDeclaration::new(
            "duration_ms",
            RequiredDeclaredType::I64,
        )],
    };
    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let (measured, skipped, failed) = measure_corpus(
        &store,
        tenant.hash(),
        std::slice::from_ref(&entry),
        &[], // no declared columns: duration_ms is unsatisfied
        1,
        window,
        NOW_NS,
        0,
        Duration::from_secs(30),
        false,
        None,
        ravel_bench::sql_latency::ExecutorSettings::default(),
        false,
        None,
    )
    .await
    .expect("measure_corpus runs");
    assert!(failed.is_empty(), "a skip is not a failure");

    // The entry is skipped, with the missing key named, and is absent from the
    // measurement set.
    //
    // Prove-the-test: this assertion is what fires when the skip is defeated.
    // Deleting the `continue;` at the end of the `if let Some(..) =
    // first_unsatisfied(..)` block in `measure_corpus` (src/sql_latency.rs) lets
    // the entry run -- `SELECT count(*) FROM logs` executes against the base
    // schema and produces a latency number -- so `measured` becomes length 1
    // and `skipped` empty, failing both assertions below.
    assert!(
        measured.is_empty(),
        "an unsatisfied entry must not be measured: {measured:?}"
    );
    assert_eq!(skipped.len(), 1, "the entry must be skipped");
    assert_eq!(skipped[0].id, "needs_duration");
    assert_eq!(
        skipped[0].missing_key, "duration_ms",
        "the skip reason must name the missing declared column"
    );
    assert!(
        skipped[0].reason.contains("duration_ms"),
        "the human reason names the key: {}",
        skipped[0].reason
    );
}

#[tokio::test]
async fn runs_1_reports_a_cold_number_and_equal_min_median_max() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = run_generated(&small_generate_config(Arc::clone(&store), 1))
        .await
        .expect("generated lane runs");

    assert_eq!(report.provenance.runs, 1);
    assert!(!report.entries.is_empty());
    for e in &report.entries {
        assert!(
            e.cold_ms > 0.0,
            "entry `{}` still reports a cold number",
            e.id
        );
        assert_eq!(
            e.min_ms, e.cold_ms,
            "with one run the min is the cold run for `{}`",
            e.id
        );
        assert_eq!(
            e.min_ms, e.median_ms,
            "one run: min == median for `{}`",
            e.id
        );
        assert_eq!(
            e.median_ms, e.max_ms,
            "one run: median == max for `{}`",
            e.id
        );
    }
}

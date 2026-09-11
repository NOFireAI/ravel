//! `ravel-cli catalog fold` report completeness (#1598): the human-readable
//! report must print every `FoldReport` field, `--json` must serialize the
//! whole struct, and `put_requests` must count every PUT the fold issues.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

/// Same margin as `tests/catalog.rs`: clears the default 1h20m seal margin
/// regardless of which minute of the hour the test runs in.
const SEALED_AGE_NS: i64 = 3 * 60 * 60 * 1_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

fn now_ns() -> i64 {
    ravel_cli::now_ns().expect("system clock readable")
}

async fn publish_segment(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    seq: u64,
    created_unix_ns: i64,
) {
    let tenant_hash = TenantId::new(tenant).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let payload = format!("seg-{shard}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Unlike `publish_segment` above (whose payload is the literal bytes
/// `seg-{shard}-{seq}`), this writes a real, decodable RSEG v1 segment with
/// one named series. The fold's postings build needs a segment it can
/// actually decode before it issues a postings PUT at all, which the
/// deliverable-2 fault-injection test below needs in order to have a
/// postings PUT to fault.
async fn publish_real_segment(store: &MemoryStore, tenant: &str, shard: u32, created_unix_ns: i64) {
    let tenant_hash = TenantId::new(tenant).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let writer_id = Uuid::new_v4();
    let series = vec![SeriesInput {
        series_id: SeriesId([1u8; 16]),
        labels: LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "up".to_string(),
        }])
        .expect("valid labels"),
        samples: vec![Sample {
            ts_ns: created_unix_ns,
            value: 1.0,
        }],
    }];
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write RSEG");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Deliverable 1 (#1598): the pre-change human-readable report printed 10 of
/// `FoldReport`'s 23 fields and silently omitted the other 13, including
/// `column_stats_dictionaries_dropped`. Every field must now appear.
///
/// The count is 10, not 13: the pre-change report emitted 13 LINES, but
/// `store`, `signal` and `seal_margin` are not `FoldReport` fields. Measured
/// against the ADR-1413 T2 fold's own output, which printed exactly those 13
/// lines.
///
/// Prove-the-test: delete any one of the `push_str` lines this test checks
/// for in `render_fold_report` (`services/ravel-cli/src/catalog.rs`) and the
/// matching assertion below fails.
#[tokio::test]
async fn fold_human_report_prints_every_fold_report_field() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-report-fields";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;

    let (report, printed) = catalog::fold(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        false,
    )
    .await
    .expect("fold succeeds");

    // Fields the pre-change report already printed.
    for field in [
        "no_op:",
        "rebuilt:",
        "previous_watermark_hour:",
        "watermark_hour:",
        "seal_margin:",
        "buckets_folded:",
        "entry_count:",
        "part_bytes:",
        "list_requests:",
        "get_requests:",
        "put_requests:",
    ] {
        assert!(
            printed.contains(field),
            "already-printed field {field} missing from report:\n{printed}"
        );
    }
    // Fields the pre-change report silently omitted.
    for field in [
        "parts_total:",
        "parts_reused:",
        "postings_built:",
        "postings_bytes:",
        "column_stats_built:",
        "column_stats_bytes:",
        "column_stats_part_built:",
        "column_stats_part_bytes:",
        "column_stats_part_objects_built:",
        "column_stats_dictionaries_dropped:",
        "layout_drift_count:",
        "frontier_hours_reconciled:",
        "frontier_hours_deferred:",
    ] {
        assert!(
            printed.contains(field),
            "previously-omitted field {field} missing from report:\n{printed}"
        );
    }

    // The two lists above pin TODAY's field set by hand, which is the same
    // mechanism that let the human report drift in the first place: add a
    // `FoldReport` field tomorrow and `--json` carries it for free via
    // `Serialize` while the text renderer silently omits it, with every
    // assertion above still green. Derive the expected key set from the
    // struct instead, so the claim in `render_fold_report`'s doc comment
    // ("both forms print every field") holds for fields nobody has written
    // yet. The hand-written lists stay for their documentation value.
    let value = serde_json::to_value(&report).expect("FoldReport serializes");
    let keys = value
        .as_object()
        .expect("FoldReport is a struct, so it serializes to an object");
    assert!(
        keys.len() >= 23,
        "expected the derived key set to cover the whole struct, got {} keys; \
         if FoldReport shrank, update this floor deliberately",
        keys.len()
    );
    // Compare against the set of keys the report actually emits, not with
    // `printed.contains("{key}:")`. A substring test is satisfied by a LONGER
    // field's line, so it is vacuous for any key that is a suffix of another:
    // `previous_watermark_hour:` contains `watermark_hour:`, and
    // `column_stats_part_bytes:` contains `part_bytes:`. Deleting either of
    // those two lines from the renderer left a substring check green.
    let emitted: std::collections::HashSet<&str> = printed
        .lines()
        .filter_map(|line| line.trim_start().split_once(':').map(|(key, _)| key))
        .collect();
    for key in keys.keys() {
        assert!(
            emitted.contains(key.as_str()),
            "FoldReport field {key} is serialized by --json but missing from \
             the human report:\n{printed}"
        );
    }
}

/// Deliverable 1's `--json` flag: the whole `FoldReport` round-trips through
/// `serde_json`, including a field the human report used to omit.
#[tokio::test]
async fn fold_json_report_serializes_the_whole_fold_report() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-report-json";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;

    let (report, printed) = catalog::fold(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        true,
    )
    .await
    .expect("fold succeeds");

    // The WHOLE of stdout must parse, with no header line to skip: `--json`
    // exists so `... --json | jq .put_requests` works, and a consumer that
    // has to drop a first line by convention is the defect this asserts
    // against. Parsing `printed` directly rather than a suffix of it is the
    // assertion.
    let value: serde_json::Value = serde_json::from_str(&printed)
        .unwrap_or_else(|err| panic!("--json stdout must be one JSON document: {err}\n{printed}"));
    let body = printed.as_str();
    // #1024 requires the store selection to stay visible. It moved from a
    // header line into the document, so it is still there and now machine
    // readable.
    assert_eq!(
        value.get("store"),
        Some(&serde_json::json!("memory")),
        "the store selection must survive as a field, not a header line:\n{body}"
    );
    assert_eq!(
        value.get("column_stats_dictionaries_dropped"),
        Some(&serde_json::json!(report.column_stats_dictionaries_dropped)),
        "JSON must carry a field the human report used to omit, got:\n{body}"
    );
    assert_eq!(
        value.get("put_requests"),
        Some(&serde_json::json!(report.put_requests))
    );
}

/// Deliverable 2 (#1598): `put_requests` must count every PUT the fold
/// issues. `publish_segment`'s payload is not a real encoded segment (it is
/// the literal bytes `seg-0-1`), so `fetch_segment_names` fails to decode it
/// and `build_postings` returns `None` (`crates/ravel-catalog/src/fold.rs`
/// lines 2596-2610): no postings object is built, and this fixture also
/// declares no typed columns, so no column-statistics object is built either
/// (ADR-0850/ADR-1413 gate on `typed_attr_columns` being non-empty). That
/// leaves exactly two PUTs: the fresh tail part (`.csnap`) and the HEAD CAS
/// write.
///
/// This pins the success-path count only: on this fixture every PUT the
/// fold issues also succeeds, so it guards against double-counting (e.g. a
/// stray increment inside *and* outside a `match` arm) rather than against
/// the issued-but-failed case. It cannot distinguish "count before the
/// match" from "count inside `Ok(_) | Err(AlreadyExists)`" the fix changed,
/// because every PUT here lands in `Ok`. That distinction is
/// `fold_put_requests_counts_an_issued_postings_put_that_failed` below,
/// which injects a real (non-`AlreadyExists`) PUT failure.
///
/// Prove-the-test: delete either `counters.put_requests += 1;` this fixture
/// reaches (the part PUT at `fold.rs:1757`, or the HEAD PUT at
/// `fold.rs:2361`) and `put_requests` reads `1` instead of `2` below.
#[tokio::test]
async fn fold_put_requests_counts_exactly_the_objects_this_fold_writes() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-put-requests";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;

    let (report, printed) = catalog::fold(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        false,
    )
    .await
    .expect("fold succeeds");

    assert!(
        !report.postings_built,
        "this fixture's payload cannot decode as a real segment, so no postings object is built: {report:?}"
    );
    assert!(
        !report.column_stats_built
            && !report.column_stats_part_built
            && report.column_stats_part_objects_built == 0,
        "fixture declares no typed columns, so no column-stats object is built: {report:?}"
    );
    assert_eq!(
        report.parts_total - report.parts_reused,
        1,
        "exactly one fresh tail part is written"
    );
    assert_eq!(
        report.put_requests, 2,
        "put_requests must count exactly the part PUT and the HEAD PUT this fold issues:\n{printed}"
    );
}

/// Deliverable 2 (#1598): a PUT that was actually issued and came back a
/// real (non-`AlreadyExists`) error must still count, because the postings
/// PUT's own failure path degrades rather than failing the fold: it warns,
/// omits the postings ref, and lets the fold return a normal report
/// (`crates/ravel-catalog/src/fold.rs`, the postings `match put_result`
/// block around line 1869). `publish_real_segment` writes a real, decodable
/// RSEG segment (unlike `publish_segment` above) specifically so
/// `build_postings` has something to encode and a postings PUT is actually
/// issued for `FaultStore` to fail.
///
/// Prove-the-test: in `fold.rs`, move `counters.put_requests += 1;`
/// (currently at line 1868, issued unconditionally before the postings
/// PUT's `match put_result`) down into the `Ok(_) | Err(StoreError::AlreadyExists)`
/// arm below it. This test's fault fires a real `Transient` error, not
/// `AlreadyExists`, so the moved increment is skipped and `put_requests`
/// reads `2` instead of `3`.
#[tokio::test]
async fn fold_put_requests_counts_an_issued_postings_put_that_failed() {
    let store = MemoryStore::new();
    let tenant = "cli-fold-put-degrade";
    let created = now_ns() - SEALED_AGE_NS;
    publish_real_segment(&store, tenant, 0, created).await;

    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Transient("simulated store outage".to_string()),
        )
        .with_key_contains(".npost"),
    );
    let fault_store = Arc::new(FaultStore::new(store, plan));

    let (report, printed) = catalog::fold(
        fault_store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        false,
    )
    .await
    .expect("an issued-but-failed postings PUT must degrade the fold, not fail it");

    assert_eq!(
        fault_store.fault_count(Op::Put, FaultKind::Transient),
        1,
        "the postings PUT fault must actually fire exactly once"
    );
    assert!(
        !report.postings_built,
        "the faulted postings PUT must degrade: no postings ref is published: {report:?}"
    );
    assert_eq!(
        report.put_requests, 3,
        "put_requests must count the part PUT, the failed-but-issued postings PUT, and the HEAD PUT:\n{printed}"
    );
}

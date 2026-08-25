//! Tripwire for the invariant `DatasetInfo::object_count`'s naming rests on
//! (issue #481), behind the `sql-latency` feature.
//!
//! `dataset_info` (src/sql_latency.rs) reports `object_count` as
//! `snapshot.segments.len()`, and documents it as "the number of stored data
//! objects a query opens" -- the per-object-cost denominator ADR-0100 and
//! docs/guides/clickbench.md teach an operator to read. That reading is only
//! correct while each resolved snapshot segment maps to exactly one distinct
//! stored data object. Nothing asserted it: the 1:1 correspondence was
//! established by reading the catalog during #428's review. If a later change
//! makes one segment reference several objects, or several segments share one
//! object, `object_count` silently starts meaning something else.
//!
//! These tests derive the object-key set INDEPENDENTLY of `segments.len()`:
//! from each segment's own `SegmentRef::data_object_key` (the catalog's view of
//! which stored object that segment opens, reconstructed from identity fields --
//! `verify_object_key` for L0, `reconstruct_l1_part_key` for L1). Counting the
//! DISTINCT keys is a different computation from counting the segment vector's
//! length, so an assertion that the two agree is not circular:
//! `assert_eq!(snapshot.segments.len(), report.object_count)` would be, because
//! it counts the same collection twice and would pass unchanged under exactly
//! the future break this test exists to catch.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ravel_query::DEFAULT_FETCH_CONCURRENCY;

use prost::Message;
use ravel_bench::sql_corpus::checked_default_corpus;
use ravel_bench::sql_latency::{GenerateConfig, run_generated};
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_maintain::{Bucket, CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::commit::v1::CompactionRecord;
use ravel_segment::{
    IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7, WrittenSegment,
};
use ravel_sql::DEFAULT_MAX_QUERY_BYTES;
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

/// The frozen query clock the generated lane resolves against (`4h` in ns,
/// matching `sql_latency`'s own `NOW_NS`). The generated data lands in
/// ingest-hour bucket 0 near the epoch, so a resolve over `[0, NOW_NS]` stays
/// bounded to a handful of buckets.
const NOW_NS: i64 = 4 * 3_600_000_000_000;

/// The union of every `data_object_key` across the snapshot's segments. This is
/// the independent derivation: it reads the catalog's per-segment key, never the
/// length of the segment vector. Returned as an owned set so the caller can
/// compare its cardinality and its membership.
fn distinct_data_object_keys(snapshot: &ravel_catalog::Snapshot) -> HashSet<String> {
    snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect()
}

/// L0: the `--generate` lane publishes 60 records at 20 per object -> three
/// distinct L0 data objects. The reported `object_count` must equal the number
/// of DISTINCT data-object keys the resolved snapshot references, derived here
/// straight from `SegmentRef::data_object_key` rather than from
/// `snapshot.segments.len()` (which is what `dataset_info` itself sums).
///
/// Prove-the-test: making `dataset_info`'s `object_count` count anything other
/// than one object per segment breaks this. Verified by mutation -- changing
/// `object_count: snapshot.segments.len()` to `snapshot.segments.len() * 2`
/// (src/sql_latency.rs) makes the report say 6 while this test derives 3
/// distinct keys, so the final `assert_eq!` fires with
/// `left: 3, right: 6`.
#[tokio::test]
async fn l0_object_count_equals_distinct_data_object_keys() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let cfg = GenerateConfig {
        store: Arc::clone(&store),
        store_backend: "memory".to_string(),
        region: "n/a".to_string(),
        endpoint: "n/a".to_string(),
        entries: checked_default_corpus().expect("checked-in corpus gates"),
        runs: 1,
        records: 60,
        records_per_object: 20,
        extra_attrs: 4,
        max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
        cache_bytes: 0,
        deadline: Duration::from_secs(30),
        continue_on_error: false,
        fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
    };
    let report = run_generated(&cfg).await.expect("generated lane runs");

    // Resolve the same dataset independently of the report, using the tenant
    // the run minted (mirrors how the skip smoke test recovers the tenant).
    let tenant = TenantId::new(report.provenance.dataset_id.clone());
    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let catalog = Arc::new(
        Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog config"),
    );
    let snapshot = catalog
        .resolve(&tenant.hash(), Signal::Logs, window, &[], NOW_NS)
        .await
        .expect("resolve logs snapshot");

    // The generated lane is never compacted, so every segment must be an L0 ref
    // reached by its own commit record. If this ever ceases to hold the L1 test
    // below is the one that matters, and the assumption is worth pinning either
    // way.
    assert!(
        snapshot
            .segments
            .iter()
            .all(|s| s.level == SegmentLevel::L0),
        "the --generate lane publishes L0 segments only, got {:?}",
        snapshot
            .segments
            .iter()
            .map(|s| &s.level)
            .collect::<Vec<_>>()
    );

    let keys = distinct_data_object_keys(&snapshot);
    // Exact magnitude, independently known: 60 records / 20 per object = 3
    // objects, so exactly 3 distinct data-object keys. Not `> 0`: an under-count
    // (counting one object out of three) clears a non-empty check and is exactly
    // the failure class object_count is here to make visible.
    assert_eq!(
        keys.len(),
        3,
        "60 records at 20 per object must be reachable as 3 distinct data-object keys, got {keys:?}"
    );
    // The invariant: the report's object_count equals the count of DISTINCT
    // objects, derived without touching segments.len(). This is what the field's
    // documented meaning ("stored data objects a query opens") rests on.
    assert_eq!(
        keys.len(),
        report.dataset.object_count,
        "object_count must equal the number of distinct data-object keys the \
         snapshot references, not merely the segment count"
    );
}

/// Publish one real single-series RSEG (metrics) segment and its commit record
/// into `(shard, hour)`. Mirrors `compaction_bench`'s `publish_segment`;
/// test-only publish helpers are not importable across crates.
#[allow(clippy::too_many_arguments)]
async fn publish_metric_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    tenant_hash: TenantHash,
    shard: u32,
    hour: u32,
    metric: &str,
    ts_ns: i64,
    created_unix_ns: i64,
    value: f64,
) {
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant, metric, &label_set).expect("series id");
    let series = vec![SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value }],
    }];
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: ts_ns,
        max_ingest_ts_ns: ts_ns,
    };
    let written: WrittenSegment =
        SegmentWriter::write(series, identity, bounds).expect("write segment");
    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 0,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns,
        ingest_hour_bucket: hour,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// The authoritative L1 part count for a compacted bucket, read from its
/// compaction record: a third, independent source of "how many stored objects
/// this dataset comprises" that owes nothing to either the snapshot's segment
/// count or its key set.
async fn compaction_part_count(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> usize {
    let listing = ravel_maintain::read::list_bucket(store, bucket)
        .await
        .expect("list bucket");
    let key = listing
        .compaction_record_keys
        .first()
        .expect("a compaction record exists");
    let got = store
        .get(key, ravel_object_store::GetRange::Full)
        .await
        .expect("get compaction record");
    let record = CompactionRecord::decode(got.data.as_ref()).expect("decode compaction record");
    record.parts.len()
}

/// L1: the compacted shape. `dataset_info`'s `object_count` rests on the same
/// 1:1 segment-to-object correspondence for an L1 snapshot, where each segment
/// is one RSEG part named by `reconstruct_l1_part_key` -- the shape the ticket
/// flags as most likely to gain a part-to-object fan-out later.
///
/// The `sql_latency` harness cannot itself publish a compacted dataset: the
/// `--generate` lane is always pre-compaction and single-stream (one L1 part),
/// and `dataset_info` resolves `Signal::Logs` only. To exercise the L1
/// `SegmentRef` key path with SEVERAL parts (so the distinct-key count is a
/// non-trivial magnitude, not a single-element set that is equal to the segment
/// count for free), this builds a real metrics bucket of distinct single-series
/// segments and compacts it with `max_l1_part_bytes = 1`, which splits every
/// series onto its own part. The invariant asserted is the one `object_count`
/// rests on, on a genuinely compacted snapshot; it is checked against the
/// compaction record's own part count rather than routed through `DatasetInfo`,
/// which has no metrics lane.
#[tokio::test]
async fn l1_part_count_equals_distinct_data_object_keys() {
    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
    const HOUR: u32 = 1_000;
    // Five distinct single-series inputs; with a 1-byte part cap the compactor
    // splits on every series boundary, so the bucket compacts to five L1 parts.
    const SERIES: u32 = 5;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("objcount-l1-invariant");
    let tenant_hash = tenant.hash();
    let shard = 0u32;
    let hour_end_ns = (i64::from(HOUR) + 1) * NS_PER_HOUR;
    let ts_ns = hour_end_ns - NS_PER_SEC;

    for i in 0..SERIES {
        let metric = format!("inv_metric_{i}");
        publish_metric_segment(
            store.as_ref(),
            &tenant,
            tenant_hash,
            shard,
            HOUR,
            &metric,
            ts_ns,
            ts_ns,
            i as f64,
        )
        .await;
    }

    // Seal the bucket (clock well past hour end + seal margin) and compact with a
    // 1-byte part cap to force a part per series.
    let now = hour_end_ns + 3 * NS_PER_HOUR;
    let clock = FixedClock::new(now);
    let config = CompactorConfig {
        max_l1_part_bytes: 1,
        ..CompactorConfig::default()
    };
    let bucket = Bucket::new(tenant_hash, Signal::Metrics, shard, HOUR);
    let outcome = compact_bucket(store.as_ref(), &clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "expected a compaction, got {outcome:?}"
    );

    // Independent source #1: the compaction record's part count.
    let parts = compaction_part_count(store.as_ref(), &bucket).await;
    // Fixture adequacy: a single-part L1 bucket would make the distinct-key set
    // equal the segment count for free (both are size 1), so this scenario would
    // not distinguish the invariant from its circular restatement. Require the
    // split actually produced several parts. This guards the fixture; the
    // invariant is the exact equality below.
    assert_eq!(
        parts, SERIES as usize,
        "a 1-byte part cap over {SERIES} single-series inputs must split into \
         {SERIES} L1 parts, got {parts}"
    );

    // Resolve the compacted bucket independently.
    let window = TimeRange {
        start_ns: i64::from(HOUR) * NS_PER_HOUR,
        end_ns: hour_end_ns,
    };
    let catalog = Arc::new(
        Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog config"),
    );
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Metrics, window, &[], now)
        .await
        .expect("resolve metrics snapshot");

    // The resolved snapshot must be entirely L1 parts (compaction superseded the
    // L0 inputs), else this is not exercising the L1 key path at all.
    assert!(
        !snapshot.segments.is_empty()
            && snapshot
                .segments
                .iter()
                .all(|s| matches!(s.level, SegmentLevel::L1 { .. })),
        "expected a non-empty all-L1 snapshot, got {:?}",
        snapshot
            .segments
            .iter()
            .map(|s| &s.level)
            .collect::<Vec<_>>()
    );

    // Independent source #2: distinct L1 part keys, derived from
    // reconstruct_l1_part_key via SegmentRef::data_object_key -- not from
    // segments.len().
    let key_set = distinct_data_object_keys(&snapshot);

    // The invariant: one distinct stored object per resolved segment. Pinned as
    // exact equality across three independently computed quantities (compaction
    // record parts, resolved segment count, distinct key set), so a part-to-object
    // fan-out or a shared-key collision that left segments.len() unchanged still
    // trips this. A `>= 1` check would not.
    assert_eq!(
        key_set.len(),
        parts,
        "distinct L1 data-object keys ({}) must equal the compaction record's \
         part count ({parts})",
        key_set.len()
    );
    assert_eq!(
        key_set.len(),
        snapshot.segments.len(),
        "each resolved L1 segment must name exactly one distinct data object; \
         {} segments resolved to {} distinct keys",
        snapshot.segments.len(),
        key_set.len()
    );
}

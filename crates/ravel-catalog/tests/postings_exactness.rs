//! Exactness property for phase P5a (docs/metric-index-plan.md 3.3): the
//! name-postings index a fold builds and attaches to HEAD must exactly
//! match a brute-force scan of every covered segment's own catalog, across
//! both RSEG v1 and v2 segment formats. This is an external test (it has no
//! access to `ravel-catalog`'s crate-private helpers), so both the HEAD/part
//! key reconstruction and the segment-catalog decode below duplicate the
//! same public-API-only patterns already established in
//! `snapshot_resolve.rs` (HEAD key) and `fold.rs`'s `fetch_entry_names`
//! (segment section slicing) -- the latter duplication is itself the
//! brute-force reference: it decodes each covered segment independently of
//! `Catalog::fold`'s internal postings build, so a real divergence between
//! the two would show up as a test failure rather than as tautological
//! agreement.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, PartLimits, PostingsLimits, decode_head, decode_part,
    decode_postings,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::SnapshotEntry;
use ravel_segment::{
    ExpectedIdentity, IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput,
    VERSION_V6,
};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

/// Reconstructs the HEAD object key the same way `fold::head_object_key`
/// does, from public `Signal`/`TenantHash` methods only (mirrors
/// `snapshot_resolve.rs`'s `head_key`).
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_input(tenant: &TenantId, metric: &str) -> SeriesInput {
    let labels = labels_for(metric);
    let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
    SeriesInput {
        series_id,
        labels,
        samples: vec![Sample {
            ts_ns: 0,
            value: 1.0,
        }],
    }
}

/// Writes one real RSEG v6 segment covering `metrics` and publishes its
/// commit record. ADR-0027 left v6 the only version; the trailing `_use_v2`
/// flag is retained (ignored) so the call sites need not change.
#[allow(clippy::too_many_arguments)]
async fn publish_real_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    tenant_id: &TenantId,
    shard: u32,
    writer_id: Uuid,
    writer_epoch: u64,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    metrics: &[&str],
    _use_v2: bool,
) {
    let inputs: Vec<SeriesInput> = metrics.iter().map(|m| series_input(tenant_id, m)).collect();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write v6 segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V6),
        created_unix_ns: 0,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Brute-force scan of one entry's own segment catalog: fetches the segment
/// directly and decodes its distinct `__name__` values, independently of
/// `Catalog::fold`'s postings-building path.
async fn brute_force_names(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    entry: &SnapshotEntry,
) -> HashSet<String> {
    let content_hash: [u8; 32] = entry.content_hash.as_slice().try_into().expect("32 bytes");
    let writer_id_bytes: [u8; 16] = entry.writer_id.as_slice().try_into().expect("16 bytes");
    let writer_id = Uuid::from_bytes(writer_id_bytes);
    let data_key = keys::data_key(
        &tenant_hash,
        Signal::Metrics,
        entry.shard,
        writer_id,
        entry.writer_epoch,
        entry.writer_seq,
        &content_hash,
    )
    .expect("data key");
    let got = store
        .get(&data_key, GetRange::Full)
        .await
        .expect("get segment");

    let limits = ReaderLimits::default();
    let location = ravel_segment::open_from_full(&got.data, limits).expect("open segment");
    let expected = ExpectedIdentity {
        tenant_hash: tenant_hash.0,
        shard: entry.shard,
        writer_id: writer_id.to_string(),
        writer_epoch: entry.writer_epoch,
        writer_seq: entry.writer_seq,
    };
    ravel_segment::check_identity(&location.footer, &expected).expect("identity checks out");

    assert_eq!(
        location.version, VERSION_V6,
        "ADR-0027: v6 is the only version"
    );
    // The chunked v5-shaped catalog (unchanged by the v6 EXEMPLARS addition)
    // spans sections; decode it over the whole object (already fetched) and
    // fold to the per-series `SeriesEntry` view.
    let series: Vec<ravel_segment::SeriesEntry> =
        ravel_segment::decode_catalog_v5(&location.footer, &got.data, limits)
            .expect("decode catalog")
            .into_iter()
            .map(|e| e.entry)
            .collect();

    series
        .iter()
        .map(|s| {
            s.labels
                .get(METRIC_NAME_LABEL)
                .expect("series has __name__")
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn postings_match_brute_force_scan_across_v1_and_v2_segments() {
    let store = Arc::new(MemoryStore::new());
    let tenant_id = TenantId::new("acme");
    let tenant_hash = tenant_id.hash();
    let catalog = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        },
    )
    .expect("catalog");

    let hour = 10;
    let now = now_at_seal(hour);

    // Segment A: RSEG v1, metrics cpu + mem.
    publish_real_segment(
        store.as_ref(),
        tenant_hash,
        &tenant_id,
        0,
        Uuid::from_u128(1),
        1,
        1,
        hour,
        &["cpu", "mem"],
        false,
    )
    .await;
    // Segment B: RSEG v2, metrics mem + disk.
    publish_real_segment(
        store.as_ref(),
        tenant_hash,
        &tenant_id,
        0,
        Uuid::from_u128(2),
        1,
        1,
        hour,
        &["mem", "disk"],
        true,
    )
    .await;

    let report = catalog
        .fold(&tenant_hash, Signal::Metrics, Uuid::new_v4(), now, &[])
        .await
        .expect("fold");
    assert!(!report.no_op);
    assert!(
        report.postings_built,
        "postings must build successfully against real RSEG v1/v2 segments"
    );

    let head_bytes = store
        .get(&head_key(&tenant_hash, Signal::Metrics), GetRange::Full)
        .await
        .expect("get head");
    let head = decode_head(&head_bytes.data).expect("decode head");
    let postings_ref = head.postings.clone().expect("postings ref present");

    let part_bytes = store
        .get(&head.parts[0].key, GetRange::Full)
        .await
        .expect("get part");
    let part = decode_part(&part_bytes.data, &PartLimits::default()).expect("decode part");
    assert_eq!(part.entries.len(), 2);

    let mut expected: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (ordinal, entry) in part.entries.iter().enumerate() {
        let names = brute_force_names(store.as_ref(), tenant_hash, entry).await;
        for name in names {
            expected.entry(name).or_default().push(ordinal as u64);
        }
    }

    let expected_part_blake3: Vec<[u8; 32]> = head
        .parts
        .iter()
        .map(|p| p.blake3.as_slice().try_into().expect("32 bytes"))
        .collect();
    let postings_bytes = store
        .get(&postings_ref.key, GetRange::Full)
        .await
        .expect("get postings");
    let decoded = decode_postings(
        &postings_bytes.data,
        &PostingsLimits::default(),
        &expected_part_blake3,
    )
    .expect("decode postings");

    let actual: BTreeMap<String, Vec<u64>> = decoded
        .names
        .into_iter()
        .map(|np| (np.name, np.ordinals))
        .collect();

    assert_eq!(actual, expected);
    // Confirms both v1 and v2 segments genuinely contributed distinct
    // per-entry catalogs, not a degenerate single-format fixture.
    assert_eq!(expected.get("cpu").map(Vec::len), Some(1));
    assert_eq!(expected.get("disk").map(Vec::len), Some(1));
    assert_eq!(expected.get("mem").map(Vec::len), Some(2));
}

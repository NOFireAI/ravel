//! #278 item 3: the postings object is fetched only when an equality
//! `__name__` filter can actually use it.
//!
//! `snapshot_resolve` used to load the postings object unconditionally,
//! before the filter was even consulted, so a query with no equality
//! `__name__` matcher paid a GET (and a decode) for pruning data it could
//! never apply. The resolve now gates that GET on the filter's presence. This
//! external test builds a real fold (so a postings object genuinely exists on
//! HEAD), then counts the postings GETs a plain `resolve` versus a
//! `resolve_pruned(Some(name))` issues over the same snapshot.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V6};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn tenant() -> TenantHash {
    TenantHash([0x3c; 16])
}

fn config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        ..Default::default()
    }
}

fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// One real RSEG v5 segment covering `metrics`, event-stamped inside `hour`
/// so a query over that hour's event range actually selects it, then its
/// commit record published.
async fn publish_real_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    tenant_id: &TenantId,
    shard: u32,
    seq: u64,
    hour: u32,
    metrics: &[&str],
) {
    let event_ts = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    let inputs: Vec<SeriesInput> = metrics
        .iter()
        .map(|m| {
            let labels = labels_for(m);
            let series_id = SeriesId::compute(tenant_id, m, &labels).expect("series id");
            SeriesInput {
                series_id,
                labels,
                samples: vec![Sample {
                    ts_ns: event_ts,
                    value: 1.0,
                }],
            }
        })
        .collect();
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: event_ts,
        max_ingest_ts_ns: event_ts,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write v6 segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: event_ts,
        max_ingest_ts_ns: event_ts,
        segment_format_version: u32::from(VERSION_V6),
        created_unix_ns: event_ts,
        ingest_hour_bucket: hour,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

#[derive(Clone, Default)]
struct RequestLog {
    gets: Arc<Mutex<Vec<String>>>,
}

impl RequestLog {
    fn get_count_for(&self, needle: &str) -> usize {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.contains(needle))
            .count()
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: RequestLog,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<Self>, RequestLog) {
        let log = RequestLog::default();
        (
            Arc::new(CountingStore {
                inner,
                log: log.clone(),
            }),
            log,
        )
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.log.gets.lock().unwrap().push(key.to_string());
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

#[tokio::test]
async fn postings_get_is_skipped_without_a_name_filter_and_issued_with_one() {
    let inner = Arc::new(MemoryStore::new());
    let tenant_hash = tenant();
    let tenant_id = TenantId::new("t".to_string());
    let base_hour = 5_000u32;
    let now_ns = now_at_seal(base_hour);

    // One sealed hour carrying two metrics, so the fold builds a real
    // postings index attached to HEAD.
    publish_real_segment(
        inner.as_ref(),
        tenant_hash,
        &tenant_id,
        0,
        1,
        base_hour,
        &["metric_a", "metric_b"],
    )
    .await;

    let fold_catalog = Catalog::new(inner.clone(), config(1)).expect("catalog");
    let report = fold_catalog
        .fold(&tenant_hash, Signal::Metrics, Uuid::new_v4(), now_ns, &[])
        .await
        .expect("fold");
    assert!(
        report.postings_built,
        "fold must attach a postings object for this test to be meaningful"
    );

    // Range starts at 0 so the event-stamped segment overlaps; the watermark
    // (base_hour) still serves the whole window from parts, so the snapshot
    // path (not listing) runs and the postings GET is on the table.
    let range = ravel_types::TimeRange {
        start_ns: 0,
        end_ns: now_ns,
    };

    // No equality `__name__` filter: the postings object must never be read.
    let (counting, log) = CountingStore::new(inner.clone());
    let catalog = Catalog::new(counting, config(1)).expect("catalog");
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve");
    assert!(!snapshot.segments.is_empty(), "the segment must be served");
    assert!(
        log.get_count_for("/snap/") >= 1,
        "the snapshot parts must be served (otherwise the postings gating is vacuous)"
    );
    assert_eq!(
        log.get_count_for(".npost"),
        0,
        "no usable name filter: the postings GET must be skipped (#278 item 3)"
    );

    // An equality `__name__` filter: the postings object is fetched and used.
    let (counting2, log2) = CountingStore::new(inner.clone());
    let catalog2 = Catalog::new(counting2, config(1)).expect("catalog");
    catalog2
        .resolve_pruned(
            &tenant_hash,
            Signal::Metrics,
            range,
            &[],
            now_ns,
            Some("metric_a"),
        )
        .await
        .expect("resolve_pruned");
    assert!(
        log2.get_count_for(".npost") >= 1,
        "an equality __name__ filter must fetch the postings object"
    );
}

//! ADR-1306 follow-up task 1: the cold object-store cost of one unsealed
//! flush, and of everything outside the unsealed tail, split by phase.
//!
//! The fixture writes a sealed hour of `K` segments, then `N` unsealed flushes
//! per shard in the hour after it (outside a last-5-minutes window) and one
//! more unsealed flush per shard inside that window. Every query runs against
//! a fresh `Catalog` and `QueryEngine`, so no commit record, snapshot part or
//! segment byte is served from a cache. The per-flush figure is read off the
//! difference between two values of `N`, and the per-sealed-segment figure off
//! the difference between two values of `K`, so neither is assumed.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_promql::Value;
use ravel_query::{
    EngineConfig, QueryEngine, QueryPhase, QueryStats, REQUEST_BUDGET_FIXED_OVERHEAD,
    REQUESTS_PER_UNSEALED_FLUSH,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_types::accounting::AccountedOp;
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

const SHARDS: u32 = 2;
const METRIC: &str = "m";
const SEALED_HOUR: u32 = 9_001;
/// Ingest hour of the `N` flushes per shard that fall outside the narrow
/// query's event range but inside its listing window.
const TAIL_HOUR: u32 = SEALED_HOUR + 1;
/// Ingest hour of the flushes inside the narrow query's event range.
const RECENT_HOUR: u32 = SEALED_HOUR + 2;
/// Unsealed flushes per shard inside the last 5 minutes; constant across `N`.
const RECENT_PER_SHARD: u64 = 1;

/// The first instant `SEALED_HOUR` is sealed, 20 minutes into `RECENT_HOUR`.
/// `TAIL_HOUR` and `RECENT_HOUR` are both unsealed at this instant.
const NOW_NS: i64 = (SEALED_HOUR as i64 + 1) * NS_PER_HOUR + MARGIN_NS;

/// The last 5 minutes before `NOW_NS`.
const NARROW: &str = "m[5m]";
/// From `SEALED_HOUR`'s start to `NOW_NS`: the sealed region and the whole
/// unsealed tail.
const WIDE: &str = "m[140m]";

async fn publish_flush(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    shard: u32,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    ts_ns: i64,
) {
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, METRIC, &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample {
            ts_ns,
            value: writer_seq as f64,
        }],
    };
    let written = SegmentWriter::write(vec![input], identity, bounds).expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 0,
        ingest_hour_bucket,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

fn catalog_config() -> CatalogConfig {
    CatalogConfig {
        shard_count: SHARDS,
        ..CatalogConfig::default()
    }
}

/// `sealed` segments in `SEALED_HOUR`, folded, then `tail_per_shard` flushes
/// per shard in `TAIL_HOUR` and `RECENT_PER_SHARD` per shard in the last two
/// minutes before `NOW_NS`, none of them folded. Every sample has a distinct
/// timestamp, so the sample count a query returns is its segment count.
async fn fixture(sealed: u64, tail_per_shard: u64) -> (Arc<MemoryStore>, TenantHash) {
    let store = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let mut seq = 0u64;
    for i in 0..sealed {
        seq += 1;
        let shard = (i % u64::from(SHARDS)) as u32;
        let ts = i64::from(SEALED_HOUR) * NS_PER_HOUR + 10 * NS_PER_MIN + i as i64 * NS_PER_SEC;
        publish_flush(store.as_ref(), &tid, th, shard, seq, SEALED_HOUR, ts).await;
    }
    let folder = Catalog::new(store.clone(), catalog_config()).expect("catalog");
    folder
        .fold(&th, Signal::Metrics, Uuid::new_v4(), NOW_NS, &[], None)
        .await
        .expect("fold seals SEALED_HOUR");
    for shard in 0..SHARDS {
        let offset = i64::from(shard) * NS_PER_SEC;
        for j in 0..tail_per_shard {
            seq += 1;
            let ts = i64::from(TAIL_HOUR) * NS_PER_HOUR + 10 * NS_PER_MIN + j as i64 * NS_PER_MIN;
            publish_flush(store.as_ref(), &tid, th, shard, seq, TAIL_HOUR, ts + offset).await;
        }
        for j in 0..RECENT_PER_SHARD {
            seq += 1;
            let ts = NOW_NS - 2 * NS_PER_MIN + j as i64 * 10 * NS_PER_SEC;
            publish_flush(
                store.as_ref(),
                &tid,
                th,
                shard,
                seq,
                RECENT_HOUR,
                ts + offset,
            )
            .await;
        }
    }
    (store, th)
}

/// Requests a query issued, by the kind of object they touched.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ByObject {
    /// GETs of the snapshot HEAD object.
    head_gets: u64,
    /// GETs of snapshot part objects.
    part_gets: u64,
    /// GETs of postings objects.
    postings_gets: u64,
    /// GETs of L0 commit records.
    record_gets: u64,
    /// GETs of segment data objects.
    data_gets: u64,
    /// LIST pages over a shard's commit-record prefix.
    commit_lists: u64,
    /// LIST pages over the tombstone prefix.
    tombstone_lists: u64,
    /// Any request matching none of the above, or a HEAD request.
    other: u64,
}

/// Wraps a `MemoryStore` and tallies each request by the object kind its key
/// names, so the per-flush part and the rest are counted, not inferred.
struct ClassifyingStore {
    inner: Arc<MemoryStore>,
    tally: Mutex<ByObject>,
}

impl ClassifyingStore {
    fn get_of(&self, key: &str) {
        let mut t = self.tally.lock().unwrap();
        if key.ends_with("/catalog/m/HEAD") {
            t.head_gets += 1;
        } else if key.contains("/catalog/m/snap/") {
            t.part_gets += 1;
        } else if key.contains("/catalog/m/idx/") {
            t.postings_gets += 1;
        } else if key.contains("/m/c/") && key.ends_with(".cmt") {
            t.record_gets += 1;
        } else if key.contains("/m/l0/") && key.ends_with(".rseg") {
            t.data_gets += 1;
        } else {
            t.other += 1;
        }
    }

    fn list_of(&self, prefix: &str) {
        let mut t = self.tally.lock().unwrap();
        if prefix.contains("/m/c/") {
            t.commit_lists += 1;
        } else if prefix.ends_with("/m/del/") {
            t.tombstone_lists += 1;
        } else {
            t.other += 1;
        }
    }
}

#[async_trait]
impl ObjectStoreBackend for ClassifyingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.get_of(key);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.tally.lock().unwrap().other += 1;
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.list_of(prefix);
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.list_of(prefix);
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.list_of(prefix);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// One cold query's cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cost {
    /// `[resolve, plan, probe, scan]`, each `[GET, LIST, HEAD]`, from
    /// `QueryStats::phase_accounting`.
    phases: [[u64; 3]; 4],
    objects: ByObject,
    samples: usize,
}

impl Cost {
    fn total(&self) -> u64 {
        self.phases.iter().flatten().sum()
    }
}

/// Runs `query` at `NOW_NS` on a fresh catalog and engine: every record cache
/// and byte cache starts empty.
async fn cold(store: &Arc<MemoryStore>, th: TenantHash, query: &str) -> Cost {
    let classifying = Arc::new(ClassifyingStore {
        inner: store.clone(),
        tally: Mutex::new(ByObject::default()),
    });
    let backend: Arc<dyn ObjectStoreBackend> = classifying.clone();
    let cat = Catalog::new(backend.clone(), catalog_config()).expect("catalog");
    let engine = QueryEngine::new(Arc::new(cat), backend, EngineConfig::default());
    let (value, stats): (Value, QueryStats) = engine
        .instant_with_stats(
            th,
            query,
            NOW_NS / 1_000_000,
            &[],
            NOW_NS,
            Duration::from_secs(30),
        )
        .await
        .expect("query");
    let samples = match value {
        Value::Matrix(m) => m.iter().map(|(_, s)| s.len()).sum(),
        other => panic!("expected a range vector, got {other:?}"),
    };
    let phases = QueryPhase::ALL.map(|p| {
        let s = stats.phase_accounting.phase(p);
        [
            s.s3_requests(AccountedOp::Get),
            s.s3_requests(AccountedOp::List),
            s.s3_requests(AccountedOp::Head),
        ]
    });
    let objects = *classifying.tally.lock().unwrap();
    Cost {
        phases,
        objects,
        samples,
    }
}

/// The object-kind tally both queries share outside the data GETs: one GET
/// each of the snapshot HEAD, its one part and its one postings object, one
/// commit-record GET per unsealed flush listed, one LIST per shard and one
/// tombstone LIST.
fn objects(record_gets: u64, data_gets: u64) -> ByObject {
    ByObject {
        head_gets: 1,
        part_gets: 1,
        postings_gets: 1,
        record_gets,
        data_gets,
        commit_lists: u64::from(SHARDS),
        tombstone_lists: 1,
        other: 0,
    }
}

#[tokio::test]
async fn cold_recent_query_requests_per_unsealed_flush_by_phase() {
    const K_LOW: u64 = 2;
    const K_HIGH: u64 = 4;
    const N_LOW: u64 = 3;
    const N_HIGH: u64 = 7;
    let shards = u64::from(SHARDS);

    let (store, th) = fixture(K_LOW, N_LOW).await;
    let narrow_low = cold(&store, th, NARROW).await;
    let wide_low = cold(&store, th, WIDE).await;
    let (store, th) = fixture(K_LOW, N_HIGH).await;
    let narrow_high = cold(&store, th, NARROW).await;
    let wide_high = cold(&store, th, WIDE).await;
    let (store, th) = fixture(K_HIGH, N_LOW).await;
    let narrow_more_sealed = cold(&store, th, NARROW).await;
    let wide_more_sealed = cold(&store, th, WIDE).await;

    // Every unsealed flush, in or out of the narrow range, is listed and its
    // commit record GET in resolve: 3 + 2 x (N + 1) GETs, and 2 shard LISTs
    // plus one tombstone LIST. Each segment in range is one whole-object GET,
    // charged to plan; probe and scan issue nothing for a segment under the
    // whole-object threshold.
    let expected = [
        (
            "narrow, K=2, N=3",
            narrow_low,
            [[11, 3, 0], [2, 0, 0], [0, 0, 0], [0, 0, 0]],
            objects(8, 2),
            2,
        ),
        (
            "wide, K=2, N=3",
            wide_low,
            [[11, 3, 0], [10, 0, 0], [0, 0, 0], [0, 0, 0]],
            objects(8, 10),
            10,
        ),
        (
            "narrow, K=2, N=7",
            narrow_high,
            [[19, 3, 0], [2, 0, 0], [0, 0, 0], [0, 0, 0]],
            objects(16, 2),
            2,
        ),
        (
            "wide, K=2, N=7",
            wide_high,
            [[19, 3, 0], [18, 0, 0], [0, 0, 0], [0, 0, 0]],
            objects(16, 18),
            18,
        ),
        (
            "narrow, K=4, N=3",
            narrow_more_sealed,
            [[11, 3, 0], [2, 0, 0], [0, 0, 0], [0, 0, 0]],
            objects(8, 2),
            2,
        ),
        (
            "wide, K=4, N=3",
            wide_more_sealed,
            [[11, 3, 0], [12, 0, 0], [0, 0, 0], [0, 0, 0]],
            objects(8, 12),
            12,
        ),
    ];
    for (label, cost, phases, by_object, samples) in expected {
        assert_eq!(cost.phases, phases, "{label}: requests by phase");
        assert_eq!(cost.objects, by_object, "{label}: requests by object kind");
        assert_eq!(cost.samples, samples, "{label}: samples returned");
        assert_eq!(
            cost.total(),
            cost.objects.head_gets
                + cost.objects.part_gets
                + cost.objects.postings_gets
                + cost.objects.record_gets
                + cost.objects.data_gets
                + cost.objects.commit_lists
                + cost.objects.tombstone_lists,
            "{label}: the phase split and the object tally count the same requests"
        );
    }

    // Per unsealed flush per shard, from the N difference alone.
    let added_flushes = shards * (N_HIGH - N_LOW);
    let narrow_delta = narrow_high.total() - narrow_low.total();
    let wide_delta = wide_high.total() - wide_low.total();
    assert_eq!(narrow_delta % added_flushes, 0);
    assert_eq!(wide_delta % added_flushes, 0);
    let narrow_per_flush = narrow_delta / added_flushes;
    let wide_per_flush = wide_delta / added_flushes;
    assert_eq!(
        narrow_per_flush, 1,
        "a flush outside the narrow range costs its commit-record GET only"
    );
    assert_eq!(
        wide_per_flush, 2,
        "a flush inside the range costs its commit-record GET and one data GET"
    );
    assert_eq!(
        REQUESTS_PER_UNSEALED_FLUSH, wide_per_flush,
        "the constant is the worst case, a flush inside the query's range"
    );

    // Per sealed segment, from the K difference alone: nothing for a narrow
    // query that prunes the sealed hour, one data GET for a wide one.
    let added_sealed = K_HIGH - K_LOW;
    assert_eq!(narrow_more_sealed.total(), narrow_low.total());
    let sealed_delta = wide_more_sealed.total() - wide_low.total();
    assert_eq!(sealed_delta % added_sealed, 0);
    let per_sealed_segment = sealed_delta / added_sealed;
    assert_eq!(per_sealed_segment, 1);

    // The rest: what the wide query costs once every unsealed flush and every
    // sealed segment is taken out. 3 GETs (HEAD, part, postings) and 3 LISTs.
    let unsealed = shards * (N_LOW + RECENT_PER_SHARD);
    let rest = wide_low.total() - wide_per_flush * unsealed - per_sealed_segment * K_LOW;
    assert_eq!(rest, 6);
    // The narrow query's rest is the same 6: its out-of-range flushes cost
    // `narrow_per_flush` and its in-range ones `wide_per_flush`.
    let narrow_rest = narrow_low.total()
        - narrow_per_flush * shards * N_LOW
        - wide_per_flush * shards * RECENT_PER_SHARD;
    assert_eq!(narrow_rest, rest);

    // Scaled to the configured sealed-segment cap, the rest must fit the fixed
    // overhead the budget reserves outside the tail.
    let max_segments = EngineConfig::default().max_segments as u64;
    let overhead_at_max_segments = rest + per_sealed_segment * max_segments;
    assert_eq!(max_segments, 1_024);
    assert_eq!(overhead_at_max_segments, 1_030);
    assert!(
        overhead_at_max_segments <= REQUEST_BUDGET_FIXED_OVERHEAD,
        "{overhead_at_max_segments} requests outside the tail at max_segments exceed \
         the {REQUEST_BUDGET_FIXED_OVERHEAD} fixed overhead"
    );
}

//! B2 integration tests (issue #21): filter/pushdown under the pruning
//! soundness invariant and the tenant-delegating memory bridge.
//!
//! Adversarial no-row-loss cases are checked against the same independent
//! greatest-wins oracle B1 uses: an implementation of the `is_greater` total
//! order fed the same `SegmentFetcher::fetch` bytes but sharing no scan/dedup
//! code with the path under test (review F5/F8). GET-count reduction is proven
//! with a counting store wrapper that records every object key fetched.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{EngineConfig, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{
    RavelTableProvider, SqlConfig, TenantMemoryAccountant, label_match_udf, label_udf,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

// ---------------------------------------------------------------------------
// Counting object store: records every GET key so pruning can be proven.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct GetLog(Arc<Mutex<Vec<(String, usize)>>>);

impl GetLog {
    fn record(&self, key: &str, nbytes: usize) {
        self.0.lock().unwrap().push((key.to_string(), nbytes));
    }
    /// Number of GETs whose key contains `needle`.
    fn count_for(&self, needle: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.contains(needle))
            .count()
    }
    /// Total bytes returned by GETs whose key contains `needle`.
    fn bytes_for(&self, needle: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.contains(needle))
            .map(|(_, n)| *n)
            .sum()
    }
    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: GetLog,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<Self>, GetLog) {
        let log = GetLog::default();
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
        let out = self.inner.get(key, range).await?;
        self.log.record(key, out.data.len());
        Ok(out)
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
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits (issue #298).
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

// ---------------------------------------------------------------------------
// Segment fixtures (general LabelSets so high-cardinality data is expressible).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct SeriesSpec {
    labels: LabelSet,
    samples: Vec<(i64, f64)>,
}

#[derive(Clone, Debug)]
struct SegSpec {
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    series: Vec<SeriesSpec>,
}

fn metric_labels(metric: &str, extra: &[(&str, &str)]) -> LabelSet {
    let mut v = vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }];
    for (k, val) in extra {
        v.push(Label {
            name: (*k).to_string(),
            value: (*val).to_string(),
        });
    }
    LabelSet::new(v).expect("labels")
}

fn series_id_of(labels: &LabelSet) -> [u8; 16] {
    let tenant = TenantId::new("t".to_string());
    let metric = labels.get("__name__").expect("__name__");
    SeriesId::compute(&tenant, metric, labels)
        .expect("series id")
        .0
}

fn metric_series(metric: &str, samples: &[(i64, f64)]) -> SeriesSpec {
    SeriesSpec {
        labels: metric_labels(metric, &[]),
        samples: samples.to_vec(),
    }
}

async fn write_segment(
    store: &dyn ObjectStoreBackend,
    key: &str,
    writer_id: Uuid,
    spec: &SegSpec,
) -> SegmentRef {
    let inputs: Vec<SeriesInput> = spec
        .series
        .iter()
        .filter(|s| !s.samples.is_empty())
        .map(|s| SeriesInput {
            series_id: SeriesId(series_id_of(&s.labels)),
            labels: s.labels.clone(),
            samples: s
                .samples
                .iter()
                .map(|(ts_ns, value)| Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        })
        .collect();

    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: spec.writer_epoch,
        writer_seq: spec.writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    store
        .put(key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment");

    SegmentRef {
        data_object_key: key.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 0,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard: 0,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch: spec.writer_epoch,
        writer_seq: spec.writer_seq,
        created_unix_ns: spec.created_unix_ns,
        level: SegmentLevel::L0,
    }
}

async fn build_snapshot(store: &dyn ObjectStoreBackend, specs: &[SegSpec]) -> Snapshot {
    let mut segments = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        let key = format!("t/metrics/seg-{i}.rseg");
        let writer_id = Uuid::from_u128(1000 + i as u128);
        segments.push(write_segment(store, &key, writer_id, spec).await);
    }
    segments.sort_by_key(|s| (s.created_unix_ns, s.writer_epoch, s.writer_seq, s.shard));
    Snapshot {
        segments,
        segments_pruned: 0,
    }
}

/// Per-series ts -> winning value bits.
type Reduced = HashMap<[u8; 16], BTreeMap<i64, u64>>;

/// Independent greatest-wins oracle over the same fetched bytes, sharing no
/// scan/dedup code with the pipeline under test (review F5).
async fn oracle(store: Arc<dyn ObjectStoreBackend>, snapshot: &Snapshot) -> Reduced {
    type Cand = ((i64, u64, u64, u32), u64);
    let fetcher = SegmentFetcher::new(store);
    let mut best: HashMap<[u8; 16], HashMap<i64, Cand>> = HashMap::new();
    for seg in &snapshot.segments {
        let fetched = fetcher.fetch(TENANT, seg, &[]).await.expect("oracle fetch");
        for fs in fetched {
            for (idx, sample) in fs.samples.iter().enumerate() {
                let cand = (
                    (
                        fs.created_unix_ns,
                        fs.writer_epoch,
                        fs.writer_seq,
                        idx as u32,
                    ),
                    sample.value.to_bits(),
                );
                let slot = best.entry(fs.series_id.0).or_default().entry(sample.ts_ns);
                slot.and_modify(|e| {
                    if cand > *e {
                        *e = cand;
                    }
                })
                .or_insert(cand);
            }
        }
    }
    best.into_iter()
        .map(|(sid, m)| (sid, m.into_iter().map(|(ts, (_p, b))| (ts, b)).collect()))
        .collect()
}

/// Reduce result batches whose first three columns are ts, value, series_id.
fn reduce_ts_value_series(batches: &[RecordBatch]) -> Reduced {
    let mut out: Reduced = HashMap::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value col");
        let sid = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("series_id col");
        for i in 0..batch.num_rows() {
            let series = <[u8; 16]>::try_from(sid.value(i)).expect("16 bytes");
            let prior = out
                .entry(series)
                .or_default()
                .insert(ts.value(i), value.value(i).to_bits());
            assert!(prior.is_none(), "duplicate (series, ts) in result");
        }
    }
    out
}

fn ts_lit(v: i64) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::TimestampNanosecond(Some(v), None))
}

fn register(ctx: &SessionContext, snapshot: Snapshot, store: Arc<dyn ObjectStoreBackend>) {
    let fetcher = SegmentFetcher::new(store);
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    ctx.register_udf(label_udf());
    ctx.register_udf(label_match_udf());
    ctx.register_table("samples", Arc::new(provider))
        .expect("register table");
}

// ---------------------------------------------------------------------------
// GET-count reduction: ts segment-skip and label matcher pruning.
// ---------------------------------------------------------------------------

fn three_disjoint_segments() -> Vec<SegSpec> {
    vec![
        SegSpec {
            created_unix_ns: 1,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![metric_series("m", &[(0, 1.0), (10, 2.0)])],
        },
        SegSpec {
            created_unix_ns: 2,
            writer_epoch: 1,
            writer_seq: 2,
            series: vec![metric_series("m", &[(100, 3.0), (110, 4.0)])],
        },
        SegSpec {
            created_unix_ns: 3,
            writer_epoch: 1,
            writer_seq: 3,
            series: vec![metric_series("m", &[(200, 5.0), (210, 6.0)])],
        },
    ]
}

#[tokio::test]
async fn ts_bounds_skip_out_of_range_segments() {
    let (store, log) = CountingStore::new(Arc::new(MemoryStore::new()));
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let snapshot = build_snapshot(backend.as_ref(), &three_disjoint_segments()).await;

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    register(&ctx, snapshot, Arc::clone(&backend));

    // Query only the middle segment's ts window [100, 111).
    let df = ctx
        .table("samples")
        .await
        .expect("table")
        .filter(col("ts").gt_eq(ts_lit(100)).and(col("ts").lt(ts_lit(111))))
        .expect("filter")
        .select(vec![col("ts"), col("value"), col("series_id")])
        .expect("select");
    let batches = df.collect().await.expect("collect");
    let got = reduce_ts_value_series(&batches);

    // Only seg-1 was fetched: seg-0 and seg-2 are entirely outside the window.
    assert_eq!(log.count_for("seg-1"), 1, "middle segment must be fetched");
    assert_eq!(log.count_for("seg-0"), 0, "segment below range pruned");
    assert_eq!(log.count_for("seg-2"), 0, "segment above range pruned");

    // And the rows returned are exactly the middle segment's samples.
    let total: usize = got.values().map(|m| m.len()).sum();
    assert_eq!(total, 2);
    let m = *got.keys().next().unwrap();
    assert_eq!(got[&m].get(&100), Some(&3.0f64.to_bits()));
    assert_eq!(got[&m].get(&110), Some(&4.0f64.to_bits()));
}

#[tokio::test]
async fn label_equality_prunes_series_pages() {
    // Two series in one segment with large, poorly-compressible value pages so
    // page bytes dominate footer/catalog overhead. A label-equality filter
    // fetches only the matched series' pages, so it reads strictly fewer bytes
    // than an unfiltered scan of the same segment. (GET *count* is not the
    // right metric: adjacent series pages coalesce into one ranged GET, so
    // pruning a series shrinks the range read, not the number of GETs.)
    fn incompressible(n: usize) -> Vec<(i64, f64)> {
        // Distinct normal doubles in [1, 2): fixed exponent, varying mantissa.
        (0..n)
            .map(|i| {
                let bits = 0x3FF0_0000_0000_0000u64
                    | (((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)) >> 12);
                (i as i64, f64::from_bits(bits))
            })
            .collect()
    }
    fn spec() -> SegSpec {
        SegSpec {
            created_unix_ns: 1,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![
                SeriesSpec {
                    labels: metric_labels("cpu", &[("host", "a")]),
                    samples: incompressible(300),
                },
                SeriesSpec {
                    labels: metric_labels("cpu", &[("host", "b")]),
                    samples: incompressible(300),
                },
            ],
        }
    }

    // Filtered scan: only host=a.
    let (store, log) = CountingStore::new(Arc::new(MemoryStore::new()));
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let snapshot = build_snapshot(backend.as_ref(), &[spec()]).await;
    let want = oracle(Arc::clone(&backend), &snapshot).await;

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    // Small suffix so page sections need their own ranged GETs; the
    // whole-object path is disabled (#278 item 5) so this small segment does
    // not get read whole, which would fetch identical bytes with and without
    // the label filter and defeat the byte-reduction assertion below.
    let fetcher = SegmentFetcher::new(Arc::clone(&backend))
        .with_whole_object_threshold(0)
        .with_suffix_len(256);
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    ctx.register_udf(label_udf());
    ctx.register_table("samples", Arc::new(provider)).unwrap();

    // The oracle above fetched through the same counting store; only the
    // filtered scan's GETs should count.
    log.clear();

    let filtered = ctx
        .table("samples")
        .await
        .unwrap()
        .filter(
            label_udf()
                .call(vec![col("labels"), lit("host")])
                .eq(lit("a")),
        )
        .unwrap()
        .select(vec![col("ts"), col("value"), col("series_id")])
        .unwrap()
        .collect()
        .await
        .expect("collect filtered");
    let got = reduce_ts_value_series(&filtered);
    let pruned_bytes = log.bytes_for("seg-0");

    // Exactly one series survives, bit-exact against the oracle.
    assert_eq!(got.len(), 1, "only host=a series returned");
    let a_id = series_id_of(&metric_labels("cpu", &[("host", "a")]));
    assert_eq!(got[&a_id], want[&a_id]);

    // Unfiltered scan of the same segment, same fetcher config.
    let (store2, log2) = CountingStore::new(Arc::new(MemoryStore::new()));
    let backend2: Arc<dyn ObjectStoreBackend> = store2;
    let snapshot2 = build_snapshot(backend2.as_ref(), &[spec()]).await;
    let fetcher2 = SegmentFetcher::new(Arc::clone(&backend2))
        .with_whole_object_threshold(0)
        .with_suffix_len(256);
    let provider2 = RavelTableProvider::new(
        snapshot2,
        TENANT,
        fetcher2,
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    let plan = provider2.plan(1).expect("plan");
    let _ = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect unfiltered");
    let full_bytes = log2.bytes_for("seg-0");

    assert!(
        pruned_bytes < full_bytes,
        "label pruning must reduce bytes fetched: pruned={pruned_bytes} full={full_bytes}"
    );
}

// ---------------------------------------------------------------------------
// Adversarial predicates: no row loss (widen-only pruning).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mixed_or_predicate_loses_no_rows() {
    // Two overlapping segments, one series, values = ts. A mixed
    // `(ts in [3,6)) OR value > 8` predicate must not let ts extraction prune
    // away the value>8 rows that sit outside [3,6).
    let specs = vec![
        SegSpec {
            created_unix_ns: 1,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![metric_series(
                "m",
                &[(0, 0.0), (2, 2.0), (4, 4.0), (8, 8.5)],
            )],
        },
        SegSpec {
            created_unix_ns: 2,
            writer_epoch: 1,
            writer_seq: 2,
            series: vec![metric_series("m", &[(4, 4.0), (5, 5.0), (9, 9.0)])],
        },
    ];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;
    let want_all = oracle(Arc::clone(&store), &snapshot).await;

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    register(&ctx, snapshot, Arc::clone(&store));

    let predicate = col("ts")
        .gt_eq(ts_lit(3))
        .and(col("ts").lt(ts_lit(6)))
        .or(col("value").gt(lit(8.0)));
    let batches = ctx
        .table("samples")
        .await
        .unwrap()
        .filter(predicate)
        .unwrap()
        .select(vec![col("ts"), col("value"), col("series_id")])
        .unwrap()
        .collect()
        .await
        .expect("collect");
    let got = reduce_ts_value_series(&batches);

    // Expected = oracle rows satisfying the predicate, computed independently.
    let mut want: Reduced = HashMap::new();
    for (sid, m) in &want_all {
        for (&ts, &bits) in m {
            let v = f64::from_bits(bits);
            if (3..6).contains(&ts) || v > 8.0 {
                want.entry(*sid).or_default().insert(ts, bits);
            }
        }
    }
    assert_eq!(got, want, "mixed OR predicate must lose no rows");
    // Sanity: the value>8 rows at ts=8 and ts=9 (outside [3,6)) survived.
    let m = *want.keys().next().unwrap();
    assert!(got[&m].contains_key(&8) && got[&m].contains_key(&9));
}

#[tokio::test]
async fn between_predicate_matches_oracle() {
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![metric_series(
            "m",
            &[(0, 0.0), (3, 3.0), (5, 5.0), (7, 7.0), (10, 10.0)],
        )],
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;
    let want_all = oracle(Arc::clone(&store), &snapshot).await;

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    register(&ctx, snapshot, Arc::clone(&store));

    // BETWEEN 3 AND 7 -> inclusive [3, 7].
    let df = ctx.table("samples").await.unwrap();
    let batches = df
        .filter(col("ts").between(ts_lit(3), ts_lit(7)))
        .unwrap()
        .select(vec![col("ts"), col("value"), col("series_id")])
        .unwrap()
        .collect()
        .await
        .expect("collect");
    let got = reduce_ts_value_series(&batches);

    let mut want: Reduced = HashMap::new();
    for (sid, m) in &want_all {
        for (&ts, &bits) in m {
            if (3..=7).contains(&ts) {
                want.entry(*sid).or_default().insert(ts, bits);
            }
        }
    }
    assert_eq!(got, want, "BETWEEN must match the oracle over [3,7]");
}

// ---------------------------------------------------------------------------
// Label negation / regex pushdown: no row loss (widen-only series prune).
//
// The label-matcher path prunes whole series (and their page GETs) inside the
// fetcher against SERIES_TABLE, before DataFusion sees a row. A series wrongly
// excluded there yields rows the Inexact residual can never recover, so it is
// the prune form most able to silently under-fetch. Both cases assert
// bit-exact equality against the independent no-prune `oracle`, and each
// carries an absent-label series (and the regex case a partial-match series)
// to pin the PromQL superset semantics the prune rests on (finding sql1-F01).
// ---------------------------------------------------------------------------

fn labelled_series(metric: &str, extra: &[(&str, &str)], samples: &[(i64, f64)]) -> SeriesSpec {
    SeriesSpec {
        labels: metric_labels(metric, extra),
        samples: samples.to_vec(),
    }
}

#[tokio::test]
async fn label_negation_loses_no_rows() {
    // Three series in one segment: host=a, host=b, and one with no host label
    // at all. `label(labels,'host') != 'a'` must return exactly the host=b
    // series. host=a is excluded by value; the absent-host series is excluded
    // because `label(...)` is NULL there and `NULL != 'a'` is not true. The
    // negation matcher pushed into the fetcher keeps a superset -- it retains
    // the absent-host series (absent-as-empty, `"" != "a"`) -- and the residual
    // removes the surplus, so host=b must survive intact.
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![
            labelled_series("cpu", &[("host", "a")], &[(0, 1.0), (10, 2.0)]),
            labelled_series("cpu", &[("host", "b")], &[(0, 3.0), (10, 4.0)]),
            labelled_series("cpu", &[("region", "us")], &[(0, 5.0), (10, 6.0)]),
        ],
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;
    let want_all = oracle(Arc::clone(&store), &snapshot).await;

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    register(&ctx, snapshot, Arc::clone(&store));

    let batches = ctx
        .table("samples")
        .await
        .unwrap()
        .filter(
            label_udf()
                .call(vec![col("labels"), lit("host")])
                .not_eq(lit("a")),
        )
        .unwrap()
        .select(vec![col("ts"), col("value"), col("series_id")])
        .unwrap()
        .collect()
        .await
        .expect("collect");
    let got = reduce_ts_value_series(&batches);

    // Expected = only the host=b series, straight from the no-prune oracle.
    let b_id = series_id_of(&metric_labels("cpu", &[("host", "b")]));
    let mut want: Reduced = HashMap::new();
    want.insert(b_id, want_all[&b_id].clone());

    assert_eq!(
        got, want,
        "label negation must return exactly the host=b series, losing none of it"
    );
}

#[tokio::test]
async fn label_regex_loses_no_rows() {
    // Five series in one segment. `label_match(labels,'host','web.*')` anchors
    // as `^(?:web.*)$` and reads an absent label as "". Exactly the anchored
    // matches survive:
    //   - host=web1, host=web   -> match
    //   - host=aweb             -> partial: contains "web" but is not an
    //                              anchored match, so it must be excluded
    //   - host=db1              -> no match
    //   - region=us (no host)   -> "" does not match web.*, excluded
    // The regex matcher pushed into the fetcher selects the same anchored
    // superset the residual re-applies; a sub-anchoring or dropped-series bug
    // in the prune would show up as a row-count or value diff against the
    // no-prune oracle.
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![
            labelled_series("cpu", &[("host", "web1")], &[(0, 1.0), (10, 2.0)]),
            labelled_series("cpu", &[("host", "web")], &[(0, 3.0), (10, 4.0)]),
            labelled_series("cpu", &[("host", "aweb")], &[(0, 5.0), (10, 6.0)]),
            labelled_series("cpu", &[("host", "db1")], &[(0, 7.0), (10, 8.0)]),
            labelled_series("cpu", &[("region", "us")], &[(0, 9.0), (10, 10.0)]),
        ],
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;
    let want_all = oracle(Arc::clone(&store), &snapshot).await;

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    register(&ctx, snapshot, Arc::clone(&store));

    let batches = ctx
        .table("samples")
        .await
        .unwrap()
        .filter(label_match_udf().call(vec![col("labels"), lit("host"), lit("web.*")]))
        .unwrap()
        .select(vec![col("ts"), col("value"), col("series_id")])
        .unwrap()
        .collect()
        .await
        .expect("collect");
    let got = reduce_ts_value_series(&batches);

    // Expected = only the two anchored-match series, from the no-prune oracle.
    let mut want: Reduced = HashMap::new();
    for host in ["web1", "web"] {
        let id = series_id_of(&metric_labels("cpu", &[("host", host)]));
        want.insert(id, want_all[&id].clone());
    }

    assert_eq!(
        got, want,
        "label_match must return exactly the anchored web.* series, losing none of them"
    );
}

// ---------------------------------------------------------------------------
// Physical-plan shape: pushdown must not strip the sort-preserving merge.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pushdown_path_preserves_sort_preserving_merge() {
    let specs = three_disjoint_segments();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    register(&ctx, snapshot, Arc::clone(&store));

    let df = ctx
        .table("samples")
        .await
        .unwrap()
        .filter(col("ts").gt_eq(ts_lit(0)))
        .unwrap();
    let physical = df.create_physical_plan().await.expect("physical plan");
    let plan_str = displayable(physical.as_ref()).indent(true).to_string();
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "pushdown must not strip the merge; got:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("CoalescePartitionsExec"),
        "merge must never become an unordered coalesce; got:\n{plan_str}"
    );
}

// ---------------------------------------------------------------------------
// Memory: byte budget, cancellation, and query-vs-tenant layering.
// ---------------------------------------------------------------------------

fn task_ctx_with_pool(config: &SqlConfig, tenant: Arc<TenantMemoryAccountant>) -> Arc<TaskContext> {
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    let rt = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .expect("runtime env");
    Arc::new(TaskContext::default().with_runtime(rt))
}

#[tokio::test]
async fn byte_budget_exceeded_returns_typed_error() {
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![metric_series(
            "m",
            &(0..500).map(|i| (i, i as f64)).collect::<Vec<_>>(),
        )],
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;

    // Tiny per-query byte budget: the first emitted batch overruns it.
    let config = SqlConfig {
        engine: EngineConfig::default(),
        max_query_bytes: 8,
    };
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let task_ctx = task_ctx_with_pool(&config, Arc::clone(&tenant));

    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, config, QueryAccounting::new());
    let plan = provider.plan(1).expect("plan");
    let err = collect(plan, task_ctx).await.expect_err("budget must trip");
    let msg = format!("{err}");
    assert!(
        msg.contains("query memory pool exhausted"),
        "expected query-pool ResourcesExhausted, got: {msg}"
    );
}

#[tokio::test]
async fn dropped_mid_scan_stream_releases_tenant_bytes() {
    // More than one BATCH_ROWS (8192) worth of samples: this test drops the
    // stream after its first batch while more data remains unread, so it is
    // genuinely mid-scan rather than a complete-then-drop of a single batch
    // that happened to be the whole result (Opus checkpoint finding N1).
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![metric_series(
            "m",
            &(0..20_000).map(|i| (i, i as f64)).collect::<Vec<_>>(),
        )],
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;

    let config = SqlConfig::default();
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let task_ctx = task_ctx_with_pool(&config, Arc::clone(&tenant));

    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, config, QueryAccounting::new());
    let plan = provider.plan(1).expect("plan");

    let mut stream = plan.execute(0, task_ctx).expect("execute");
    let first = stream.next().await.expect("one batch").expect("ok batch");
    assert!(first.num_rows() > 0);
    assert!(
        tenant.reserved() > 0,
        "scan should have reserved tenant bytes mid-stream"
    );

    // Drop the stream mid-scan (cancellation): the reservation's drop-time
    // shrink is forwarded to the tenant accountant (review F13).
    drop(stream);
    assert_eq!(
        tenant.reserved(),
        0,
        "dropping a mid-scan stream must return tenant bytes to zero"
    );
}

#[tokio::test]
async fn high_cardinality_trips_query_pool_before_tenant() {
    // Many distinct series, each with a large label value, so a single scan
    // batch materializes a large label dictionary. The per-query pool is small
    // and the tenant budget is huge, so the query pool must trip first.
    let pad = "x".repeat(256);
    let series: Vec<SeriesSpec> = (0..400)
        .map(|i| SeriesSpec {
            labels: metric_labels(&format!("metric_{i}"), &[("pad", pad.as_str())]),
            samples: vec![(i as i64, i as f64)],
        })
        .collect();
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series,
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;

    let config = SqlConfig {
        engine: EngineConfig::default(),
        max_query_bytes: 16 * 1024,
    };
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let task_ctx = task_ctx_with_pool(&config, Arc::clone(&tenant));

    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, config, QueryAccounting::new());
    let plan = provider.plan(1).expect("plan");
    let err = collect(plan, task_ctx)
        .await
        .expect_err("query pool must trip");
    let msg = format!("{err}");
    assert!(
        msg.contains("query memory pool exhausted"),
        "the per-query pool must trip first, not the tenant budget; got: {msg}"
    );
    // Nothing leaked to the tenant: it never approached its limit and all
    // reservations released on the failed stream's drop.
    assert_eq!(
        tenant.reserved(),
        0,
        "tenant budget must be untouched at zero"
    );
}

/// Opus checkpoint finding on B2: every existing memory test sets the tenant
/// ceiling to `1 << 30`, so the tenant-exhausted branch in
/// `TenantDelegatingPool::try_grow` never actually executes, and
/// `high_cardinality_trips_query_pool_before_tenant` proves nothing about
/// check *order* -- it would pass identically even if tenant were checked
/// first, since the tenant limit there is unreachable. This test makes the
/// tenant ceiling the one that actually trips, and asserts the failed
/// tenant grow rolled the query-side counter back to exactly what it was
/// before the call.
#[tokio::test]
async fn tenant_budget_trips_and_rolls_back_the_query_reservation() {
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![metric_series(
            "m",
            &(0..500).map(|i| (i, i as f64)).collect::<Vec<_>>(),
        )],
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;

    // Query ceiling deliberately huge; tenant ceiling tiny, so the tenant
    // budget is the one that must trip.
    let config = SqlConfig {
        engine: EngineConfig::default(),
        max_query_bytes: 1 << 30,
    };
    let tenant = TenantMemoryAccountant::new(8);
    let (pool, _breach) = config.query_pool(Arc::clone(&tenant), QueryAccounting::new());
    let reserved_before = pool.reserved();
    let rt = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .expect("runtime env");
    let task_ctx = Arc::new(TaskContext::default().with_runtime(rt));

    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, config, QueryAccounting::new());
    let plan = provider.plan(1).expect("plan");
    let err = collect(plan, task_ctx)
        .await
        .expect_err("tenant budget must trip");
    let msg = format!("{err}");
    assert!(
        msg.contains("tenant memory budget exhausted"),
        "expected the tenant-budget error, got: {msg}"
    );
    assert_eq!(
        pool.reserved(),
        reserved_before,
        "a failed tenant grow must roll the query-side reservation back exactly, not partially"
    );
    assert_eq!(
        tenant.reserved(),
        0,
        "the tenant accountant must reserve nothing on a failed grow"
    );
}

/// Companion to the test above: with both ceilings equal and both reachable,
/// the query message must still win, proving `try_grow` checks the query
/// budget before the tenant budget rather than the two happening to differ
/// in size in every other test.
#[tokio::test]
async fn query_budget_reported_first_when_both_ceilings_are_equally_reachable() {
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![metric_series(
            "m",
            &(0..500).map(|i| (i, i as f64)).collect::<Vec<_>>(),
        )],
    }];
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &specs).await;

    let config = SqlConfig {
        engine: EngineConfig::default(),
        max_query_bytes: 8,
    };
    let tenant = TenantMemoryAccountant::new(8);
    let task_ctx = task_ctx_with_pool(&config, Arc::clone(&tenant));

    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, config, QueryAccounting::new());
    let plan = provider.plan(1).expect("plan");
    let err = collect(plan, task_ctx).await.expect_err("budget must trip");
    let msg = format!("{err}");
    assert!(
        msg.contains("query memory pool exhausted"),
        "with equal, equally-reachable ceilings the query check must run \
         (and fail) first; got: {msg}"
    );
}

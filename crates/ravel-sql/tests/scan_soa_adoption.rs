//! The metrics scan's columnar batch building (ADR-0099 decision 6) emits
//! exactly what the row path emitted.
//!
//! `RsegScanExec` no longer explodes each fetched series' SoA into one row
//! struct per sample: the merge yields `(run, offset)` cursors and the batch
//! gathers columns from them, adopting a run's timestamp/value buffers as
//! Arrow buffers when a batch sits contiguously inside one run. That is a
//! rewrite of the code that produces every column of every samples batch, so
//! the test is a differential one: an oracle that reimplements the row path
//! (explode into rows, sort each segment's rows by the full 6-tuple, merge the
//! sorted runs) over the same fetched bytes, compared column by column against
//! what the scan emits, provenance included.
//!
//! Two fixtures, because the two batch-building paths are chosen by data
//! shape and the emitted output is identical by construction, so neither path
//! is observable in the output:
//!
//! - overlapping runs per series, so merged batches straddle runs and the
//!   gather path runs;
//! - one run per series with a series longer than the 8192-row batch, so
//!   whole batches sit inside one run and the adoption path runs, and with a
//!   second series so a batch spans more than one series.
//!
//! Which path ran is read back from the scan's own `adopted_batches` /
//! `gathered_batches` metrics, so "both paths were exercised" is asserted,
//! not assumed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, DictionaryArray, FixedSizeBinaryArray, Float64Array, Int64Array, MapArray, StringArray,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::RavelTableProvider;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// `RsegScanExec`'s batch size; the fixtures straddle it deliberately.
const BATCH_ROWS: usize = 8192;

#[derive(Clone, Debug)]
struct SeriesSpec {
    metric: String,
    samples: Vec<(i64, f64)>,
}

#[derive(Clone, Debug)]
struct SegSpec {
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    series: Vec<SeriesSpec>,
}

/// One expected row of the internal (provenance-carrying) scan schema.
#[derive(Clone, Debug, PartialEq)]
struct Row {
    series_id: [u8; 16],
    ts: i64,
    /// Held as bits: a scan must preserve NaN payloads and -0.0 exactly, and
    /// `==` on f64 does not.
    value_bits: u64,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    in_page_index: u32,
    metric: String,
}

impl Row {
    fn key(&self) -> ([u8; 16], i64, i64, u64, u64, u32) {
        (
            self.series_id,
            self.ts,
            self.created_unix_ns,
            self.writer_epoch,
            self.writer_seq,
            self.in_page_index,
        )
    }
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_id_for(metric: &str) -> [u8; 16] {
    let tenant = TenantId::new("t".to_string());
    SeriesId::compute(&tenant, metric, &labels_for(metric))
        .expect("series id")
        .0
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
            series_id: SeriesId(series_id_for(&s.metric)),
            labels: labels_for(&s.metric),
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
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
        declared_column_stats: Default::default(),
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
        pending_erasure: Vec::new(),
    }
}

/// The row path, reimplemented: explode every fetched series' SoA into one row
/// per sample stamped with the run's provenance, sort each segment's rows by
/// the full 6-tuple, then merge the sorted runs in segment order. A stable
/// sort of the runs concatenated in segment order is that merge: equal keys
/// keep the lower segment's row first, exactly as the heap's `(key, run
/// index)` ordering does.
async fn oracle(store: Arc<dyn ObjectStoreBackend>, snapshot: &Snapshot) -> Vec<Row> {
    let fetcher = SegmentFetcher::new(store);
    let mut all: Vec<Row> = Vec::new();
    for seg in &snapshot.segments {
        let (series, _stats) = fetcher
            .fetch_soa(TENANT, seg, &[])
            .await
            .expect("oracle fetch");
        let mut run: Vec<Row> = Vec::new();
        for fs in series {
            let metric = fs
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
                .unwrap_or_default();
            for (i, (&ts, &value)) in fs.timestamps.iter().zip(fs.values.iter()).enumerate() {
                run.push(Row {
                    series_id: fs.series_id.0,
                    ts,
                    value_bits: value.to_bits(),
                    created_unix_ns: fs.created_unix_ns,
                    writer_epoch: fs.writer_epoch,
                    writer_seq: fs.writer_seq,
                    in_page_index: u32::try_from(i).unwrap_or(u32::MAX),
                    metric: metric.clone(),
                });
            }
        }
        run.sort_by_key(Row::key);
        all.extend(run);
    }
    all.sort_by_key(Row::key);
    all
}

/// The `__name__` label of each dictionary entry of a batch's labels column,
/// plus the per-row key into them.
fn labels_column(batch: &RecordBatch) -> (Vec<String>, Vec<usize>) {
    let dict = batch
        .column(3)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("labels is Dictionary(Int32, Map)");
    let maps = dict
        .values()
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("labels dictionary values are a Map");
    let mut names = Vec::with_capacity(maps.len());
    for entry in 0..maps.len() {
        let offsets = maps.value_offsets();
        let (start, end) = (offsets[entry] as usize, offsets[entry + 1] as usize);
        let keys = maps
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map keys are Utf8");
        let values = maps
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map values are Utf8");
        let mut name = String::new();
        for i in start..end {
            if keys.value(i) == "__name__" {
                name = values.value(i).to_string();
            }
        }
        names.push(name);
    }
    let keys = (0..dict.len())
        .map(|i| usize::try_from(dict.keys().value(i)).expect("non-negative dictionary key"))
        .collect();
    (names, keys)
}

/// Decode one emitted batch into rows of the internal schema.
fn batch_rows(batch: &RecordBatch) -> Vec<Row> {
    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ts");
    let value = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("value");
    let series_id = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("series_id");
    let created = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("created_unix_ns");
    let epoch = batch
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("writer_epoch");
    let seq = batch
        .column(6)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("writer_seq");
    let in_page = batch
        .column(7)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("in_page_index");
    let (names, keys) = labels_column(batch);

    (0..batch.num_rows())
        .map(|i| {
            let mut sid = [0u8; 16];
            sid.copy_from_slice(series_id.value(i));
            Row {
                series_id: sid,
                ts: ts.value(i),
                value_bits: value.value(i).to_bits(),
                created_unix_ns: created.value(i),
                writer_epoch: epoch.value(i),
                writer_seq: seq.value(i),
                in_page_index: in_page.value(i),
                metric: names[keys[i]].clone(),
            }
        })
        .collect()
}

/// The scan's own `adopted_batches`/`gathered_batches` counters, summed over
/// partitions.
fn path_counts(scan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
    let metrics = scan.metrics().expect("the scan publishes metrics");
    let count = |name: &str| {
        metrics
            .iter()
            .filter(|m| m.value().name() == name)
            .map(|m| m.value().as_usize())
            .sum::<usize>()
    };
    (count("adopted_batches"), count("gathered_batches"))
}

/// Run one fixture's snapshot through the real scan node (the pre-dedup
/// fragment's `RsegScanExec`, executed directly so the batches asserted on are
/// the ones `build_batch` produced), and return its batches plus the
/// adopted/gathered batch counts.
async fn scan_batches(
    store: Arc<dyn ObjectStoreBackend>,
    snapshot: Snapshot,
) -> (Vec<RecordBatch>, usize, usize) {
    let segments = snapshot.segments.clone();
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        SegmentFetcher::new(store),
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    // One partition, so this stream is the whole globally ordered stream.
    let fragment = provider.worker_fragment(1, &segments).expect("fragment");
    let children = fragment.children();
    let scan = Arc::clone(children.first().expect("scan under the merge"));
    let mut stream = scan
        .execute(0, Arc::new(TaskContext::default()))
        .expect("execute scan");
    let mut batches = Vec::new();
    while let Some(next) = stream.next().await {
        batches.push(next.expect("batch"));
    }
    let (adopted, gathered) = path_counts(&scan);
    (batches, adopted, gathered)
}

/// Three series, each written by three segments over interleaved timestamp
/// ranges: every series' samples come from three runs whose key ranges
/// overlap, so merged batches straddle runs.
fn overlapping_runs() -> Vec<SegSpec> {
    (0..3)
        .map(|s| SegSpec {
            created_unix_ns: 10 + s,
            writer_epoch: 1,
            writer_seq: u64::try_from(s).expect("small") + 1,
            series: (0..3)
                .map(|m| SeriesSpec {
                    metric: format!("m{m}"),
                    // Segment s carries every third timestamp, so the three
                    // segments' runs interleave sample by sample.
                    samples: (0..4000)
                        .map(|i| (i * 3 + s, (i * 3 + s) as f64 + 0.5 * s as f64))
                        .collect(),
                })
                .collect(),
        })
        .collect()
}

/// Two series in one segment each, the first shorter than a batch and the
/// second much longer: the first batch spans both series (gathered), and the
/// batches after it sit contiguously inside the long series' single run
/// (adopted). The long series also crosses the 8192-row batch boundary
/// several times.
fn single_run_per_series() -> Vec<SegSpec> {
    vec![
        SegSpec {
            created_unix_ns: 5,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![SeriesSpec {
                metric: "a_short".into(),
                samples: (0..5_000).map(|i| (i, i as f64)).collect(),
            }],
        },
        SegSpec {
            created_unix_ns: 6,
            writer_epoch: 1,
            writer_seq: 2,
            series: vec![SeriesSpec {
                metric: "b_long".into(),
                samples: (0..30_000).map(|i| (i, i as f64 * 1.5)).collect(),
            }],
        },
    ]
}

/// Every emitted row, in emission order, equals the row path's output, and
/// each batch's labels dictionary holds one entry per distinct series in that
/// batch.
fn assert_matches_row_path(batches: &[RecordBatch], want: &[Row], fixture: &str) {
    let got: Vec<Row> = batches.iter().flat_map(batch_rows).collect();
    assert_eq!(
        got.len(),
        want.len(),
        "{fixture}: emitted row count must match the row path"
    );
    for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        assert_eq!(g, w, "{fixture}: row {i} differs from the row path");
    }

    // The emitted stream is strictly ascending in the declared 6-tuple: the
    // ordering RsegScanExec declares, SortPreservingMergeExec relies on, and
    // RsegDedupExec requires of its input.
    for pair in got.windows(2) {
        assert!(
            pair[0].key() < pair[1].key(),
            "{fixture}: emitted stream must be strictly ordered by the declared 6-tuple"
        );
    }

    for (b, batch) in batches.iter().enumerate() {
        let (names, keys) = labels_column(batch);
        let rows = batch_rows(batch);
        let mut distinct: Vec<[u8; 16]> = rows.iter().map(|r| r.series_id).collect();
        distinct.dedup();
        assert_eq!(
            names.len(),
            distinct.len(),
            "{fixture}: batch {b} must hold one dictionary entry per distinct series"
        );
        let unique: std::collections::HashSet<[u8; 16]> = distinct.iter().copied().collect();
        assert_eq!(
            unique.len(),
            distinct.len(),
            "{fixture}: batch {b}'s series must be contiguous, or one entry per series is wrong"
        );
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                names[keys[i]], row.metric,
                "{fixture}: batch {b} row {i} resolves to the wrong label set"
            );
        }
    }
}

#[tokio::test]
async fn adopted_buffers_match_row_path_over_overlapping_runs() {
    // Fixture 1: overlapping runs per series -> the gather path.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), &overlapping_runs()).await;
    let want = oracle(Arc::clone(&store), &snapshot).await;
    let (batches, adopted, gathered) = scan_batches(Arc::clone(&store), snapshot.clone()).await;

    assert_eq!(
        want.len(),
        3 * 3 * 4000,
        "the overlapping fixture must produce every written sample"
    );
    assert_matches_row_path(&batches, &want, "overlapping runs");
    assert!(
        gathered > 0,
        "overlapping runs must exercise the gather path (adopted={adopted}, gathered={gathered})"
    );
    // A series' samples come from three interleaved runs here, so no batch can
    // sit contiguously inside one run except a trailing remnant; the point of
    // this fixture is that the gather path carried it.
    let overlapping_gathered = gathered;

    // Fixture 2: one run per series, one of them longer than a batch -> the
    // adoption path, plus a first batch that spans two series.
    let store2: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot2 = build_snapshot(store2.as_ref(), &single_run_per_series()).await;
    let want2 = oracle(Arc::clone(&store2), &snapshot2).await;
    let (batches2, adopted2, gathered2) = scan_batches(Arc::clone(&store2), snapshot2).await;

    assert_eq!(
        want2.len(),
        35_000,
        "the single-run fixture must produce every written sample"
    );
    assert_matches_row_path(&batches2, &want2, "single run per series");
    assert!(
        adopted2 > 0,
        "a batch inside one run must adopt its buffers (adopted={adopted2}, gathered={gathered2})"
    );

    // Both paths were genuinely exercised, across the two fixtures.
    assert!(
        overlapping_gathered > 0 && adopted2 > 0,
        "both batch-building paths must run: gathered={overlapping_gathered}, adopted={adopted2}"
    );

    // The fixtures' shapes are what make each path reachable, so pin them:
    // a series crossing the batch boundary, and a batch spanning two series.
    let long_series = series_id_for("b_long");
    let long_rows = want2.iter().filter(|r| r.series_id == long_series).count();
    assert!(
        long_rows > BATCH_ROWS,
        "the long series must cross the batch boundary (rows={long_rows})"
    );
    let multi_series_batches = batches2
        .iter()
        .filter(|b| {
            let mut ids: Vec<[u8; 16]> = batch_rows(b).iter().map(|r| r.series_id).collect();
            ids.dedup();
            ids.len() > 1
        })
        .count();
    assert_eq!(
        multi_series_batches, 1,
        "exactly the boundary batch spans two series"
    );

    // Per-series row counts, as an independent check that nothing was dropped
    // or duplicated by either path.
    let mut per_series: HashMap<[u8; 16], usize> = HashMap::new();
    for row in batches2.iter().flat_map(batch_rows) {
        *per_series.entry(row.series_id).or_default() += 1;
    }
    assert_eq!(per_series.get(&series_id_for("a_short")), Some(&5_000));
    assert_eq!(per_series.get(&long_series), Some(&30_000));
}

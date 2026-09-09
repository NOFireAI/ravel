//! Regression tests for issue #1519: the post-dedup `labels` dictionary must
//! carry one entry per distinct series it references, not one per row.
//!
//! The dedup operator keeps each winner row as a one-row `DictionaryArray`
//! slice, which retains the whole source dictionary, and concatenates ~1024 of
//! them per flush. Before the compaction fix, the flushed dictionary held
//! `rows x distinct-series-per-source-batch` entries, so a `SELECT labels`
//! result ballooned ~430x and a stock 4 MiB Flight client could not read it
//! past a dozen series.
//!
//! These tests drive the real scan -> merge -> dedup pipeline (the same
//! `RavelTableProvider::plan` the SQL endpoint runs), following the harness
//! shape of tests/pipeline.rs, and assert on the flushed batches directly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, DictionaryArray, FixedSizeBinaryArray, MapArray, StringArray,
};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::RavelTableProvider;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([9u8; 16]);

/// One series: its full label set and its `(ts, value)` samples in ts order.
#[derive(Clone)]
struct Series {
    labels: LabelSet,
    samples: Vec<(i64, f64)>,
}

/// A canonical `(name, value)` view of one label set in stored (name-sorted)
/// order, the exact shape the dictionary Map entry encodes.
fn canonical(set: &LabelSet) -> Vec<(String, String)> {
    set.iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

/// Write one real RSEG segment: series `n` carries a stable id derived from
/// `m{n}` plus its label set, so the same logical series keeps one id across
/// segments and distinct series and distinct label sets coincide. `samples`
/// gives, per series index, that segment's subset of `(ts, value)`.
async fn write_segment(
    store: &dyn ObjectStoreBackend,
    key: &str,
    seq: u64,
    series: &[Series],
    samples: &[Vec<(i64, f64)>],
) -> SegmentRef {
    let tenant = TenantId::new("t".to_string());
    let inputs: Vec<SeriesInput> = series
        .iter()
        .enumerate()
        .filter(|(i, _)| !samples[*i].is_empty())
        .map(|(i, s)| {
            let metric = format!("m{i}");
            SeriesInput {
                series_id: SeriesId::compute(&tenant, &metric, &s.labels).expect("series id"),
                labels: s.labels.clone(),
                samples: samples[i]
                    .iter()
                    .map(|(ts_ns, value)| Sample {
                        ts_ns: *ts_ns,
                        value: *value,
                    })
                    .collect(),
            }
        })
        .collect();

    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: Uuid::from_u128(u128::from(seq)).to_string(),
        writer_epoch: 1,
        writer_seq: seq,
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
        writer_id: Uuid::from_u128(u128::from(seq)),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: seq as i64,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
        declared_column_stats: Default::default(),
    }
}

/// Build the snapshot and run the provider's scan -> merge -> dedup pipeline,
/// returning the flushed public-schema batches.
async fn run(series: &[Series]) -> Vec<RecordBatch> {
    let store = Arc::new(MemoryStore::new());
    let per_series: Vec<Vec<(i64, f64)>> = series.iter().map(|s| s.samples.clone()).collect();
    let segment = write_segment(
        store.as_ref(),
        "t/metrics/seg-0.rseg",
        1,
        series,
        &per_series,
    )
    .await;
    let snapshot = Snapshot {
        segments: vec![segment],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let fetcher = SegmentFetcher::new(store as Arc<dyn ObjectStoreBackend>);
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    let plan = provider.plan(1).expect("build plan");
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

/// Decode row `i` of a `Dictionary(Int32, Map(Utf8, Utf8))` labels column into
/// its `(name, value)` pairs in stored order.
fn decode_row(labels: &dyn Array, i: usize) -> Vec<(String, String)> {
    let dict = labels
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("dictionary labels");
    let maps = dict
        .values()
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("map values");
    let keys = maps
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map keys utf8");
    let values = maps
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map values utf8");
    let entry = dict.keys().value(i) as usize;
    let offsets = maps.value_offsets();
    let start = offsets[entry] as usize;
    let end = offsets[entry + 1] as usize;
    (start..end)
        .map(|j| (keys.value(j).to_string(), values.value(j).to_string()))
        .collect()
}

/// The dictionary values length of a batch's labels column: the number of Map
/// entries retained, i.e. the dictionary's entry count.
fn dict_entry_count(batch: &RecordBatch) -> usize {
    let labels = batch.column(3);
    let dict = labels
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("dictionary labels");
    dict.values().len()
}

/// Distinct decoded label sets among a batch's rows.
fn distinct_label_sets(batch: &RecordBatch) -> usize {
    let labels = batch.column(3);
    let mut seen: HashSet<Vec<(String, String)>> = HashSet::new();
    for i in 0..batch.num_rows() {
        seen.insert(decode_row(labels.as_ref(), i));
    }
    seen.len()
}

/// A corpus of `count` series, each with a distinct multi-label set and one
/// label unique to it, plus a shared and a per-series-absent label so the
/// shapes vary. `samples_each` samples per series, strictly ts-ascending.
fn varied_corpus(count: usize, samples_each: usize) -> Vec<Series> {
    (0..count)
        .map(|s| {
            // Every series carries `__name__` and `job`; `host` is unique to
            // the series; `region` is present only on even series, so odd
            // series lack a label the even ones have.
            let mut pairs = vec![
                ("__name__".to_string(), "http_requests".to_string()),
                ("job".to_string(), "api".to_string()),
                ("host".to_string(), format!("host-{s}")),
            ];
            if s % 2 == 0 {
                pairs.push(("region".to_string(), format!("r{}", s % 3)));
            }
            let labels = LabelSet::new(
                pairs
                    .into_iter()
                    .map(|(name, value)| Label { name, value })
                    .collect(),
            )
            .expect("valid labels");
            let samples = (0..samples_each)
                .map(|t| (t as i64 + 1, (s * 1000 + t) as f64))
                .collect();
            Series { labels, samples }
        })
        .collect()
}

/// The decisive test: a `SELECT labels`-shaped result spanning several flushed
/// batches must hold a dictionary bounded by the distinct series it
/// references, not by its row count. The bound is exact: one entry per
/// distinct label set present in the batch.
#[tokio::test]
async fn flushed_dictionary_is_bounded_by_distinct_series_not_rows() {
    // 20 series x 100 samples = 2000 winner rows, which crosses the 1024-row
    // flush threshold, so the result is emitted as more than one batch.
    let series = varied_corpus(20, 100);
    let batches = run(&series).await;

    assert!(
        batches.len() >= 2,
        "expected the result to span multiple flushed batches, got {}",
        batches.len()
    );

    let mut saw_many_rows_few_entries = false;
    for batch in &batches {
        let entries = dict_entry_count(batch);
        let distinct = distinct_label_sets(batch);
        // Exact bound: the compacted dictionary holds precisely the distinct
        // label sets its rows reference. Before the fix this was
        // `distinct-per-source-batch x rows` instead.
        assert_eq!(
            entries,
            distinct,
            "dictionary must hold exactly the distinct series it references \
             (rows={})",
            batch.num_rows()
        );
        // 20 series total bounds every batch's dictionary regardless of rows.
        assert!(
            entries <= 20,
            "dictionary entries {entries} exceeded the 20 distinct series"
        );
        if batch.num_rows() >= 512 && entries <= 20 {
            saw_many_rows_few_entries = true;
        }
    }
    assert!(
        saw_many_rows_few_entries,
        "expected at least one large batch whose dictionary stayed tiny"
    );
}

/// Correctness: every output row's decoded label set must equal the label set
/// its series was written with, including name order within the map, a label
/// unique to one series, and a label absent from half the series. Compaction
/// must not corrupt or reorder any entry.
#[tokio::test]
async fn compaction_preserves_every_rows_label_set() {
    let series = varied_corpus(24, 60);
    let batches = run(&series).await;

    // Ground truth: series id -> its canonical label pairs. Independent of the
    // pipeline; derived straight from the corpus.
    let tenant = TenantId::new("t".to_string());
    let mut expected: std::collections::HashMap<[u8; 16], Vec<(String, String)>> =
        std::collections::HashMap::new();
    for (i, s) in series.iter().enumerate() {
        let metric = format!("m{i}");
        let id = SeriesId::compute(&tenant, &metric, &s.labels).expect("series id");
        expected.insert(id.0, canonical(&s.labels));
    }

    let mut checked = 0usize;
    for batch in &batches {
        let sid = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("series_id col");
        let labels = batch.column(3);
        for i in 0..batch.num_rows() {
            let series_id = <[u8; 16]>::try_from(sid.value(i)).expect("16 bytes");
            let want = expected.get(&series_id).expect("known series");
            let got = decode_row(labels.as_ref(), i);
            assert_eq!(&got, want, "row {i} decoded labels differ from source");
            checked += 1;
        }
    }
    // 24 series x 60 samples, all distinct (series, ts), none deduped away.
    assert_eq!(checked, 24 * 60, "every written sample must appear once");
}

/// Client-boundary size: at the 25-series / 60,000-row shape the diagnosis
/// measured a ~5.87 MB single Arrow IPC body, the post-fix largest single
/// batch body must fit inside the 4 MiB a stock Flight client allows.
#[tokio::test]
async fn largest_ipc_body_fits_a_stock_flight_client() {
    const FOUR_MIB: usize = 4 * 1024 * 1024;

    // 25 series x 2400 samples = 60,000 rows, the shape the diagnosis measured
    // a ~5.87 MB single Arrow IPC body at before the fix.
    let series = varied_corpus(25, 2400);
    let batches = run(&series).await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        60_000,
        "corpus must produce exactly 60,000 winner rows"
    );

    let schema = batches[0].schema();
    let mut largest = 0usize;
    for batch in &batches {
        // One IPC body per record batch, the unit a Flight client reads as a
        // single message.
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref()).expect("ipc writer");
            writer.write(batch).expect("ipc write");
            writer.finish().expect("ipc finish");
        }
        largest = largest.max(buf.len());
    }

    assert!(
        largest < FOUR_MIB,
        "largest single-batch IPC body {largest} bytes must fit the 4 MiB \
         Flight default"
    );
}

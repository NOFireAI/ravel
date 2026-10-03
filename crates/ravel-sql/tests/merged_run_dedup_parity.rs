//! SQL and PromQL pick the same value at a duplicate `(series, ts)` on a
//! run-merged run (issue #2424).
//!
//! A run that merged several writes' samples (ADR-0092 decision 1) stores each
//! sample's own ADR-0010 §5 dedup key in the per-sample provenance column,
//! because merging destroys what the run-wide `(created_unix_ns, writer_epoch,
//! writer_seq)` plus on-disk position used to say. The PromQL merge reads that
//! column (`FetchedSeriesSoa::per_sample_priorities`). The SQL scan must stamp
//! the same keys on its provenance columns, or `RsegDedupExec` resolves a
//! duplicate by a key no sample actually has.
//!
//! Every case publishes real RSEG objects through the catalog and runs the
//! same series through both engines. The PromQL answer comes from
//! `QueryEngine::instant` over a range selector, i.e. the fetcher plus the
//! engine's own merge, not from a reimplementation of it.
//!
//! The fixed case holds (creation times are offsets into the ingest hour):
//!
//! - a run-merged object whose single run holds samples of two writes
//!   (created 100 and 300) with the per-sample column, and whose run-wide
//!   triple is the lexicographic minimum of its samples' keys, as the
//!   compactor's `merged_run_prefix` writes it;
//! - a plain object (created 200) contesting the first timestamp.
//!
//! At `T1` the merged run is ordered as the compactor orders it (ascending
//! key within a timestamp), so position agrees with the column inside the
//! run, and only the cross-run contest separates the two rules: the column
//! says created 300 beats the plain object's 200, the run-wide minimum says
//! 100 loses to it. At `T2` the later write's sample sits first in the run, so
//! position alone picks the earlier write. `T3` is uncontested.
//!
//! The property case draws arbitrary merged runs (any key order within a
//! timestamp, key ties, `-0.0` against `0.0`) and contenders, and asserts the
//! two engines agree on every one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Float64Array, TimestampNanosecondArray};
use proptest::prelude::*;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_promql::Value;
use ravel_query::{EngineConfig, LogSegmentFetcher, QueryEngine, SegmentFetcher};
use ravel_segment::{
    CompactionMetaV4, IngestBounds, RunInputV7, SampleProvenance, SegmentIdentity, SegmentWriter,
    SeriesInput, SeriesInputV7, SeriesValues, VERSION_V7, WrittenSegment, encode_run_v4,
};
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId, TimeRange,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const HOUR: u32 = 9_301;
const METRIC: &str = "m";

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

/// The `k`th sample timestamp: whole minutes into the hour, so the PromQL
/// range selector returns them unchanged.
fn ts(k: i64) -> i64 {
    hour_start() + (k + 1) * 60 * NS_PER_SEC
}

/// Read time: inside the same (unsealed) hour, after every sample.
fn now_ns() -> i64 {
    hour_start() + 30 * 60 * NS_PER_SEC
}

fn labels() -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels")
}

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

fn series_id() -> SeriesId {
    SeriesId::compute(&tenant(), METRIC, &labels()).expect("series id")
}

fn prov(created_offset_ns: i64, writer_seq: u64, in_page_index: u32) -> SampleProvenance {
    SampleProvenance {
        created_unix_ns: hour_start() + created_offset_ns,
        writer_epoch: 1,
        writer_seq,
        in_page_index,
    }
}

/// One object's commit-record identity. For an L0-tagged object the fetcher
/// takes the run-wide triple from here, so `created_offset_ns`/`writer_seq` give
/// the run-wide key a merged run carries.
#[derive(Clone, Copy, Debug)]
struct Writer {
    id: u128,
    created_offset_ns: i64,
    writer_seq: u64,
}

impl Writer {
    /// A commit record's `created_unix_ns` must fall in its ingest hour.
    fn created_unix_ns(&self) -> i64 {
        hour_start() + self.created_offset_ns
    }

    fn identity(&self) -> SegmentIdentity {
        SegmentIdentity {
            tenant_hash: tenant().hash().0,
            shard: 0,
            writer_id: Uuid::from_u128(self.id).to_string(),
            writer_epoch: 1,
            writer_seq: self.writer_seq,
        }
    }
}

fn bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    }
}

/// Publish `written` under a commit record whose identity matches the footer
/// the writer stamped.
async fn publish_written(store: &dyn ObjectStoreBackend, writer: Writer, written: WrittenSegment) {
    let new_record = NewCommitRecord {
        tenant_hash: tenant().hash(),
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::from_u128(writer.id),
        writer_epoch: 1,
        writer_seq: writer.writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: writer.created_unix_ns(),
        ingest_hour_bucket: HOUR,
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

/// Publish one object holding a single run-merged run: `samples` in on-disk
/// order (ascending ts), each with its own dedup key in the per-sample
/// provenance column.
async fn publish_merged(
    store: &dyn ObjectStoreBackend,
    writer: Writer,
    samples: &[(i64, f64, SampleProvenance)],
) {
    let id = series_id();
    let values = SeriesValues::Scalar(
        samples
            .iter()
            .map(|&(ts_ns, value, _)| Sample { ts_ns, value })
            .collect(),
    );
    let run = encode_run_v4(&id, writer.created_unix_ns(), 1, writer.writer_seq, &values)
        .expect("frame run");
    let series = vec![SeriesInputV7 {
        series_id: id,
        labels: labels(),
        runs: vec![RunInputV7 {
            run,
            provenance: Some(samples.iter().map(|&(_, _, p)| p).collect()),
        }],
    }];
    let meta = CompactionMetaV4 {
        ingest_hour_bucket: HOUR,
        input_set_hash: [0u8; 32],
        part_index: 0,
        level: 0,
    };
    let written = SegmentWriter::write_v7_with_provenance(
        series,
        writer.identity(),
        bounds(),
        meta,
        Vec::new(),
    )
    .expect("write run-merged object");
    publish_written(store, writer, written).await;
}

/// Publish one ordinary object (run-wide provenance) holding `samples`.
async fn publish_plain(store: &dyn ObjectStoreBackend, writer: Writer, samples: &[(i64, f64)]) {
    let input = SeriesInput {
        series_id: series_id(),
        labels: labels(),
        samples: samples
            .iter()
            .map(|&(ts_ns, value)| Sample { ts_ns, value })
            .collect(),
    };
    let written =
        SegmentWriter::write(vec![input], writer.identity(), bounds()).expect("write plain");
    publish_written(store, writer, written).await;
}

/// `(ts_ns, value_bits)` of the single series in a PromQL range vector.
fn promql_samples(value: &Value) -> Vec<(i64, u64)> {
    match value {
        Value::Matrix(m) => {
            assert_eq!(m.len(), 1, "one series: {m:?}");
            m[0].1
                .iter()
                .map(|s| (s.ts_ns, s.value.to_bits()))
                .collect()
        }
        other => panic!("expected a range vector, got {other:?}"),
    }
}

/// The series' deduplicated `(ts_ns, value_bits)` as PromQL and as SQL serve
/// them, over every object published to `store`, plus the segment count the
/// SQL snapshot saw.
async fn both_engines(
    store: Arc<dyn ObjectStoreBackend>,
) -> (Vec<(i64, u64)>, Vec<(i64, u64)>, usize) {
    let th = tenant().hash();
    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let engine_config = EngineConfig::default();

    let engine = QueryEngine::new(Arc::clone(&cat), store.clone(), engine_config);
    let (value, _coverage) = engine
        .instant(
            th,
            &format!("{METRIC}[1h]"),
            now_ns() / 1_000_000,
            &[],
            now_ns(),
            Duration::from_secs(5),
        )
        .await
        .expect("PromQL range selector");
    let promql = promql_samples(&value);

    let executor = SqlExecutor::new(
        Arc::clone(&cat),
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        SqlConfig {
            engine: engine_config,
            ..SqlConfig::default()
        },
        1 << 30,
    );
    let outcome = executor
        .execute(
            th,
            &SqlRequest {
                sql: format!(
                    "SELECT ts, value FROM samples \
                     WHERE label(labels, '__name__') = '{METRIC}' ORDER BY ts"
                ),
                window: TimeRange {
                    start_ns: hour_start(),
                    end_ns: hour_start() + NS_PER_HOUR,
                },
                min_tokens: Vec::new(),
                now_ns: now_ns(),
                deadline: Duration::from_secs(30),
                row_window: false,
                max_rows: None,
                budgets: None,
            },
        )
        .await
        .expect("SQL samples query");

    let mut sql: Vec<(i64, u64)> = Vec::new();
    for batch in outcome.output.batches() {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts is Timestamp(ns)");
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value is Float64");
        for i in 0..batch.num_rows() {
            sql.push((ts.value(i), value.value(i).to_bits()));
        }
    }
    (promql, sql, outcome.stats.segments)
}

#[tokio::test]
async fn sql_dedup_matches_promql_on_a_run_merged_run() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // Write A is (created 100, seq 11), write B is (created 300, seq 13).
    //   ts(0): A=1.0 then B=3.0 (ascending key, the compactor's order)
    //   ts(1): B=5.0 then A=4.0 (the later write first)
    //   ts(2): A=6.0
    // Run-wide triple (100, 1, 11): the minimum over the samples' keys.
    publish_merged(
        store.as_ref(),
        Writer {
            id: 0x2424_0001,
            created_offset_ns: 100,
            writer_seq: 11,
        },
        &[
            (ts(0), 1.0, prov(100, 11, 0)),
            (ts(0), 3.0, prov(300, 13, 0)),
            (ts(1), 5.0, prov(300, 13, 1)),
            (ts(1), 4.0, prov(100, 11, 1)),
            (ts(2), 6.0, prov(100, 11, 2)),
        ],
    )
    .await;
    // Write C (created 200) contests ts(0) only.
    publish_plain(
        store.as_ref(),
        Writer {
            id: 0x2424_0002,
            created_offset_ns: 200,
            writer_seq: 2,
        },
        &[(ts(0), 2.0)],
    )
    .await;

    let (promql, sql, segments) = both_engines(store).await;
    assert_eq!(segments, 2, "both objects in the SQL snapshot");
    // Pin the winners the per-sample column dictates, so the parity assertion
    // below is not merely two equal wrong answers.
    assert_eq!(
        promql,
        vec![
            (ts(0), 3.0f64.to_bits()),
            (ts(1), 5.0f64.to_bits()),
            (ts(2), 6.0f64.to_bits()),
        ],
        "PromQL resolves by the per-sample keys"
    );
    assert_eq!(
        sql, promql,
        "SQL must resolve each duplicate (series, ts) to the value PromQL serves"
    );
}

/// A sampled value: finite, with `-0.0` against `0.0` so a key tie falls to
/// the value-bits tie-break and that tie-break is compared too.
fn value() -> impl Strategy<Value = f64> {
    prop::sample::select(vec![1.0, 2.0, -1.5, 0.0, -0.0, 7.25])
}

/// A merged run: up to 8 samples over 4 timestamps, each drawn from one of
/// three writes, in any key order within a timestamp (stable sort by ts
/// only), with small in-page indices so whole-key ties occur.
fn merged_samples() -> impl Strategy<Value = Vec<(i64, f64, SampleProvenance)>> {
    prop::collection::vec(
        (
            0i64..4,
            value(),
            prop::sample::select(vec![(100i64, 11u64), (200, 12), (300, 13)]),
            0u32..3,
        ),
        1..9,
    )
    .prop_map(|raw| {
        let mut out: Vec<(i64, f64, SampleProvenance)> = raw
            .into_iter()
            .map(|(k, v, (created, seq), ipi)| (ts(k), v, prov(created, seq, ipi)))
            .collect();
        out.sort_by_key(|s| s.0);
        out
    })
}

/// A contending plain object's samples: at most one per timestamp.
fn plain_samples() -> impl Strategy<Value = Vec<(i64, f64)>> {
    prop::collection::vec((0i64..4, value()), 0..4).prop_map(|raw| {
        let mut by_ts: BTreeMap<i64, f64> = BTreeMap::new();
        for (k, v) in raw {
            by_ts.entry(ts(k)).or_insert(v);
        }
        by_ts.into_iter().collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Any merged run plus any contender: SQL serves exactly what PromQL
    /// serves, by bits, at every timestamp.
    #[test]
    fn sql_dedup_matches_promql_on_random_merged_runs(
        merged in merged_samples(),
        merged_prefix in prop::sample::select(vec![(50i64, 10u64), (100, 11), (250, 12), (400, 14)]),
        plain in plain_samples(),
        plain_created in prop::sample::select(vec![100i64, 200, 300]),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (promql, sql) = rt.block_on(async {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            publish_merged(
                store.as_ref(),
                Writer {
                    id: 0x2424_0001,
                    created_offset_ns: merged_prefix.0,
                    writer_seq: merged_prefix.1,
                },
                &merged,
            )
            .await;
            if !plain.is_empty() {
                publish_plain(
                    store.as_ref(),
                    Writer {
                        id: 0x2424_0002,
                        created_offset_ns: plain_created,
                        writer_seq: 2,
                    },
                    &plain,
                )
                .await;
            }
            let (promql, sql, _) = both_engines(store).await;
            (promql, sql)
        });
        prop_assert_eq!(sql, promql);
    }
}

//! B1 integration tests for the scan -> sort-preserving-merge -> dedup
//! pipeline (issue #20).
//!
//! The layer-1 scan oracle (review F5) compares the post-dedup pipeline
//! output against an independent greatest-wins reference. That reference
//! reimplements the exact total order of `is_greater`/`merge_segments`
//! (docs/catalog-and-mvcc.md) over the same `SegmentFetcher` bytes:
//! `merge_segments` itself is private to ravel-query, and making it public
//! is outside this ticket's crate scope, so the oracle is a separate
//! implementation of the same normative order fed the same decoded samples
//! (`SegmentFetcher::fetch`) as the path under test decodes with
//! `fetch_soa`. Independent dedup code on each side is exactly what F5
//! requires.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use proptest::prelude::*;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::fault::{FaultKind, FaultPlan, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::RavelTableProvider;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// A logical series inside a segment: metric name plus (ts, value) samples
/// in write order (the writer stable-sorts by ts; equal-ts insertion order
/// is preserved and becomes the in-page-index tiebreak).
#[derive(Clone, Debug)]
struct SeriesSpec {
    metric: String,
    samples: Vec<(i64, f64)>,
}

/// A segment's provenance plus its series. Distinct provenance across
/// segments exercises every field of the dedup total order.
#[derive(Clone, Debug)]
struct SegSpec {
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    series: Vec<SeriesSpec>,
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

/// Write one real RSEG segment for `spec` and put it on `store`, returning a
/// matching `SegmentRef`. Skips series that end up empty (the writer drops
/// them); a segment with no non-empty series is still written.
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
    }
}

/// Build a snapshot from segment specs, writing each onto a fresh
/// `MemoryStore` (optionally paginated). Returns the store and the snapshot.
async fn build_snapshot(
    specs: &[SegSpec],
    page_size: Option<usize>,
) -> (Arc<MemoryStore>, Snapshot) {
    let store = Arc::new(match page_size {
        Some(n) => MemoryStore::with_page_size(n),
        None => MemoryStore::new(),
    });
    let mut segments = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        let key = format!("t/metrics/seg-{i}.rseg");
        let writer_id = Uuid::from_u128(1000 + i as u128);
        segments.push(write_segment(store.as_ref(), &key, writer_id, spec).await);
    }
    // Snapshot iteration order is the deterministic dedup segment order;
    // reproduce it here (created_unix_ns, writer_epoch, writer_seq, shard).
    segments.sort_by_key(|s| (s.created_unix_ns, s.writer_epoch, s.writer_seq, s.shard));
    (
        store,
        Snapshot {
            segments,
            segments_pruned: 0,
        },
    )
}

/// Per-series ts -> winning value bits, the shape both the oracle and the
/// pipeline output reduce to for comparison.
type Reduced = HashMap<[u8; 16], BTreeMap<i64, u64>>;

/// Independent greatest-wins oracle over the same fetched bytes. Ranks by
/// `(created_unix_ns, writer_epoch, writer_seq, in_page_index)` then
/// `value.to_bits()`, greatest wins -- byte-for-byte `is_greater`.
async fn oracle(store: Arc<dyn ObjectStoreBackend>, snapshot: &Snapshot) -> Reduced {
    // (priority tuple, value bits): greater wins, exactly `is_greater`.
    type Cand = ((i64, u64, u64, u32), u64);
    let fetcher = SegmentFetcher::new(store);
    // series -> ts -> best candidate so far
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
                slot.and_modify(|existing| {
                    if cand > *existing {
                        *existing = cand;
                    }
                })
                .or_insert(cand);
            }
        }
    }
    best.into_iter()
        .map(|(sid, ts_map)| {
            (
                sid,
                ts_map
                    .into_iter()
                    .map(|(ts, (_p, bits))| (ts, bits))
                    .collect(),
            )
        })
        .collect()
}

/// Execute the provider's post-dedup pipeline and reduce the output batches
/// to per-series ts -> value bits. Also asserts the public schema, global
/// per-series ts-ascending order, and no duplicate (series, ts).
async fn run_pipeline(
    store: Arc<dyn ObjectStoreBackend>,
    snapshot: Snapshot,
    target_partitions: usize,
    config: EngineConfig,
) -> Reduced {
    let fetcher = SegmentFetcher::new(store);
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, config);
    let plan = provider.plan(target_partitions).expect("build plan");
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    reduce_batches(&batches)
}

fn reduce_batches(batches: &[RecordBatch]) -> Reduced {
    let mut out: Reduced = HashMap::new();
    // Track per-series last ts to assert ascending, contiguous ordering.
    let mut last_ts: HashMap<[u8; 16], i64> = HashMap::new();
    for batch in batches {
        assert_eq!(batch.schema(), ravel_sql::public_schema(), "public schema");
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
            let t = ts.value(i);
            let bits = value.value(i).to_bits();
            if let Some(prev) = last_ts.get(&series) {
                assert!(t > *prev, "output must be strictly ts-ascending per series");
            }
            last_ts.insert(series, t);
            let prior = out.entry(series).or_default().insert(t, bits);
            assert!(prior.is_none(), "duplicate (series, ts) in dedup output");
        }
    }
    out
}

fn assert_reduced_eq(got: &Reduced, want: &Reduced) {
    assert_eq!(
        got.len(),
        want.len(),
        "series count: got {} want {}",
        got.len(),
        want.len()
    );
    let got_total: usize = got.values().map(|m| m.len()).sum();
    let want_total: usize = want.values().map(|m| m.len()).sum();
    assert_eq!(got_total, want_total, "total sample count");
    for (sid, want_map) in want {
        let got_map = got
            .get(sid)
            .unwrap_or_else(|| panic!("missing series {sid:?}"));
        assert_eq!(got_map, want_map, "series {sid:?} sample bits differ");
    }
}

// ---------------------------------------------------------------------------
// Golden fixtures
// ---------------------------------------------------------------------------

/// Drive one spec set through both oracle and pipeline and assert equality,
/// at several partition counts to exercise the merge.
async fn assert_pipeline_matches_oracle(specs: Vec<SegSpec>, page_size: Option<usize>) {
    let (store, snapshot) = build_snapshot(&specs, page_size).await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let want = oracle(Arc::clone(&backend), &snapshot).await;
    for target in [1usize, 2, 4, 8] {
        let got = run_pipeline(
            Arc::clone(&backend),
            snapshot.clone(),
            target,
            EngineConfig::default(),
        )
        .await;
        assert_reduced_eq(&got, &want);
    }
}

#[tokio::test]
async fn golden_multi_segment_overlap_each_field_decides() {
    // Same series, same ts across segments; each provenance field in turn is
    // the sole difference that decides the winner.
    let specs = vec![
        SegSpec {
            created_unix_ns: 10,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![SeriesSpec {
                metric: "m".into(),
                samples: vec![(100, 1.0), (200, 2.0)],
            }],
        },
        // Higher created_unix_ns wins ts=100.
        SegSpec {
            created_unix_ns: 20,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![SeriesSpec {
                metric: "m".into(),
                samples: vec![(100, 111.0)],
            }],
        },
        // Same created, higher writer_epoch wins ts=200.
        SegSpec {
            created_unix_ns: 10,
            writer_epoch: 2,
            writer_seq: 1,
            series: vec![SeriesSpec {
                metric: "m".into(),
                samples: vec![(200, 222.0)],
            }],
        },
    ];
    assert_pipeline_matches_oracle(specs, None).await;
}

/// Regression for the Opus checkpoint finding on B1 (issue #20): every other
/// test in this file calls `provider.plan(n)` and hands the hand-built plan
/// straight to `collect()`, which never runs the DataFusion optimizer. Only
/// the `SessionContext`/`DataFrame` path does, and without
/// `RsegDedupExec::required_input_distribution`/`required_input_ordering`
/// the optimizer struck `SortPreservingMergeExec` outright (not even
/// replacing it with `CoalescePartitionsExec`), so only one scan partition
/// was ever read: silent, unflagged data loss on the crate's documented
/// table-provider entry point. This asserts both the physical plan shape
/// and the full row count, with more than one partition actually in play.
#[tokio::test]
async fn optimizer_path_preserves_sort_preserving_merge_and_full_rows() {
    let specs = vec![
        SegSpec {
            created_unix_ns: 10,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![SeriesSpec {
                metric: "m".into(),
                samples: vec![(100, 1.0), (200, 2.0)],
            }],
        },
        SegSpec {
            created_unix_ns: 20,
            writer_epoch: 1,
            writer_seq: 1,
            series: vec![SeriesSpec {
                metric: "m".into(),
                samples: vec![(100, 111.0)],
            }],
        },
        SegSpec {
            created_unix_ns: 10,
            writer_epoch: 2,
            writer_seq: 1,
            series: vec![SeriesSpec {
                metric: "m".into(),
                samples: vec![(200, 222.0)],
            }],
        },
    ];
    let (store, snapshot) = build_snapshot(&specs, None).await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let want = oracle(Arc::clone(&backend), &snapshot).await;

    // target_partitions=4 against 3 segments: RsegScanExec.new's
    // `n = target_partitions.min(segments.len())` gives 3 real partitions,
    // so a stripped merge would read one of three segments, not all.
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let fetcher = SegmentFetcher::new(Arc::clone(&backend));
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, EngineConfig::default());
    ctx.register_table("samples", Arc::new(provider))
        .expect("register table");

    let df = ctx.table("samples").await.expect("table");
    let physical = df.create_physical_plan().await.expect("physical plan");
    let plan_str = displayable(physical.as_ref()).indent(true).to_string();
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "optimizer must not strip the required merge; got:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("CoalescePartitionsExec"),
        "merge must never be replaced by an unordered coalesce; got:\n{plan_str}"
    );

    let batches = collect(physical, Arc::new(TaskContext::default()))
        .await
        .expect("collect via optimized plan");
    let got = reduce_batches(&batches);
    assert_reduced_eq(&got, &want);
}

#[tokio::test]
async fn golden_duplicate_ts_within_segment() {
    // Duplicate timestamps inside one segment: the later insertion (greater
    // in-page index) wins.
    let specs = vec![SegSpec {
        created_unix_ns: 5,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![SeriesSpec {
            metric: "dup".into(),
            samples: vec![(100, 1.0), (100, 2.0), (100, 3.0), (200, 9.0)],
        }],
    }];
    assert_pipeline_matches_oracle(specs, None).await;
}

#[tokio::test]
async fn golden_value_bit_tiebreak_identical_provenance() {
    // Two segments with byte-identical provenance and one sample at the same
    // ts and in-page index: only value.to_bits() can break the tie. Greater
    // bits win.
    let a = SegSpec {
        created_unix_ns: 7,
        writer_epoch: 3,
        writer_seq: 4,
        series: vec![SeriesSpec {
            metric: "tie".into(),
            samples: vec![(100, 1.0)],
        }],
    };
    let mut b = a.clone();
    b.series[0].samples = vec![(100, 2.0)]; // 2.0 has greater bits than 1.0
    assert_pipeline_matches_oracle(vec![a, b], None).await;
}

#[tokio::test]
async fn golden_nan_and_negative_zero() {
    let nan1 = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan2 = f64::from_bits(0x7ff8_0000_0000_00ff);
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![
            SeriesSpec {
                metric: "nan".into(),
                samples: vec![(1, nan1), (2, nan2), (3, f64::NAN)],
            },
            SeriesSpec {
                metric: "zero".into(),
                samples: vec![
                    (1, -0.0),
                    (2, 0.0),
                    (3, f64::INFINITY),
                    (4, f64::NEG_INFINITY),
                ],
            },
        ],
    }];
    assert_pipeline_matches_oracle(specs, None).await;
}

#[tokio::test]
async fn pagination_with_page_size_two() {
    // Many segments so listing would paginate; MemoryStore::with_page_size(2)
    // exercises the paginated path. (The fetcher reads objects directly, but
    // this keeps the store's listing surface under a small page size.)
    let mut specs = Vec::new();
    for i in 0..5u64 {
        specs.push(SegSpec {
            created_unix_ns: i as i64,
            writer_epoch: 1,
            writer_seq: i + 1,
            series: vec![
                SeriesSpec {
                    metric: "a".into(),
                    samples: vec![(100 + i as i64, i as f64), (100, 42.0 + i as f64)],
                },
                SeriesSpec {
                    metric: "b".into(),
                    samples: vec![(200, i as f64)],
                },
            ],
        });
    }
    assert_pipeline_matches_oracle(specs, Some(2)).await;
}

// ---------------------------------------------------------------------------
// Budget enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn max_samples_budget_trips_on_yielded_rows() {
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![SeriesSpec {
            metric: "m".into(),
            samples: (0..50).map(|i| (i, i as f64)).collect(),
        }],
    }];
    let (store, snapshot) = build_snapshot(&specs, None).await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let config = EngineConfig {
        max_samples: 10,
        ..EngineConfig::default()
    };
    let fetcher = SegmentFetcher::new(backend);
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, config);
    let plan = provider.plan(1).expect("plan");
    let err = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect_err("budget must trip");
    let msg = format!("{err}");
    assert!(msg.contains("too many samples"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// FaultStore: mid-scan GET failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mid_scan_get_failure_is_typed_error_and_fault_fires() {
    use ravel_object_store::fault::FaultStore;

    let spec = SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![SeriesSpec {
            metric: "m".into(),
            samples: (0..64).map(|i| (i, i as f64)).collect(),
        }],
    };

    // Fail the 2nd GET (Nth is 1-indexed): the footer suffix GET succeeds,
    // then a page/section GET fails mid-scan. A tiny suffix length forces
    // more than one GET per segment.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::Permanent("injected".into()))
            .with_occurrence(Occurrence::Nth(2)),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));

    let writer_id = Uuid::from_u128(1);
    let seg = write_segment(store.as_ref(), "t/metrics/seg.rseg", writer_id, &spec).await;
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
    };

    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let fetcher = SegmentFetcher::new(backend).with_suffix_len(64);
    let provider = RavelTableProvider::new(snapshot, TENANT, fetcher, EngineConfig::default());
    let plan = provider.plan(1).expect("plan");
    let err = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect_err("mid-scan GET failure");
    let msg = format!("{err}");
    assert!(
        msg.contains("segment fetch failed"),
        "typed error, got: {msg}"
    );

    // The fault must actually have fired -- prove it via the counter, not
    // just by seeing an error come back.
    assert_eq!(
        store.fault_count(Op::Get, FaultKind::Permanent),
        1,
        "the injected GET fault must have fired exactly once"
    );
}

// ---------------------------------------------------------------------------
// Layer-1 scan oracle over proptest-generated datasets
// ---------------------------------------------------------------------------

/// A pool of interesting f64 values: NaN payloads, signed zero, infinities,
/// a denormal, and ordinary values.
fn interesting_value() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.0f64),
        Just(-0.0f64),
        Just(1.0f64),
        Just(-1.0f64),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(f64::from_bits(0x7ff8_0000_0000_0001)),
        Just(f64::from_bits(0x7ff8_0000_0000_00aa)),
        Just(f64::from_bits(0x0000_0000_0000_0001)), // denormal
        (0i64..5).prop_map(|n| n as f64),
    ]
}

fn arb_series() -> impl Strategy<Value = SeriesSpec> {
    let metric = prop_oneof![Just("a"), Just("b"), Just("c")].prop_map(|s| s.to_string());
    // Small ts range forces duplicate timestamps and cross-segment overlap.
    let samples = prop::collection::vec((0i64..6, interesting_value()), 1..8);
    (metric, samples).prop_map(|(metric, samples)| SeriesSpec { metric, samples })
}

fn arb_segment() -> impl Strategy<Value = SegSpec> {
    let created = 0i64..4;
    let epoch = 1u64..3;
    let seq = 1u64..3;
    let series = prop::collection::vec(arb_series(), 1..4);
    (created, epoch, seq, series).prop_map(|(created_unix_ns, writer_epoch, writer_seq, series)| {
        SegSpec {
            created_unix_ns,
            writer_epoch,
            writer_seq,
            series,
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn scan_oracle_matches_merge_semantics(specs in prop::collection::vec(arb_segment(), 1..5)) {
        // Duplicate series_ids across the same segment are rejected by the
        // writer; collapse duplicate metrics within a segment so each spec
        // is writable. Cross-segment duplicates are the point and are kept.
        let specs: Vec<SegSpec> = specs
            .into_iter()
            .map(|mut seg| {
                let mut seen = std::collections::HashSet::new();
                seg.series.retain(|s| seen.insert(s.metric.clone()));
                seg
            })
            .collect();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            assert_pipeline_matches_oracle(specs, None).await;
        });
    }
}

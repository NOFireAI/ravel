//! Differential proptest proving the spans columnar fast path and the row
//! fallback path produce identical Arrow batches over every projection subset,
//! including erasure-active scenarios that force the row path.
//!
//! See docs/adrs/0110-columnar-spans-scan.md decisions 3 and 7 for the
//! eligibility rule and the fallback contract this test pins down.
//!
//! # Two comparisons, both cross-path
//!
//! The fast path and the row path emit identical output by construction, so the
//! only way to prove they agree is to run the *same* input through both and
//! compare byte for byte. Each generated case does that twice:
//!
//! - **Path agreement** (no erasure). The same corpus and projection run once
//!   with no pending erasure (columnar when the projection excludes `attrs`,
//!   ADR-0110 decision 3 clause a) and once forced onto the row path by a
//!   no-match erasure predicate (which drains the row path yet removes nothing).
//!   The two `Vec<RecordBatch>` must be equal, boundaries included, and the path
//!   metrics must confirm which path each run took.
//! - **Erasure fallback agreement**. A pending erasure predicate that *does*
//!   match some spans forces the row path (decision 3 clause b), which excludes
//!   the erased spans against the merged attribute map the fast path never
//!   builds. Its output is compared against an independent reference: the same
//!   projection over a corpus with the erased spans removed at generation time,
//!   run with no erasure (columnar when eligible). This proves the row path's
//!   erasure filtering agrees with the columnar path on the surviving rows, and
//!   exercises the fallback in the differential rather than only in T3's unit
//!   test.
//!
//! # The generator must carry attributes and events
//!
//! Every generated span carries a `user_id` attribute (the erasure target axis)
//! and most carry `service.name`, an extra dynamic attribute, and one or more
//! events. That is deliberate and load-bearing: an attribute-free, event-free
//! corpus makes both paths decode exactly the same pages, so `pages_skipped` is
//! zero and the whole columnar-vs-row comparison is vacuous (the fast path skips
//! nothing, so it is not exercising the code that differs from the row path). Do
//! not "simplify" the generator to drop attributes or events.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use proptest::prelude::*;
use proptest::test_runner::TestRunner;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::erasure::snapshot_pending_erasure_predicates;
use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanQuery, SpanRecord, StatusCode};
use ravel_sql::{SPAN_COL_ATTRS, SpanSegmentFetcher, SpansScanExec, spans_schema};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([1u8; 16]);
/// The public `spans` table has eleven columns (ADR-0041); a projection is any
/// non-empty ordered subset of these indices.
const SPAN_COL_COUNT: usize = 11;
/// The erasure target attribute. A span is erasable iff its merged attributes
/// carry exactly this `key = value`, which `is_erased_span` matches against
/// (ravel-query's erasure module), so the erased set is known at generation.
const ERASE_KEY: &str = "user_id";
const ERASE_VALUE: &str = "erase-me";
/// Proptest case count, matched to this crate's differential-suite convention
/// (`tests/logs_differential.rs` fixes 48). Each case writes several small RSPAN
/// objects and runs four scans (two comparisons, two paths each), so this keeps
/// the suite's cost in line with the existing differential gates while covering
/// a wide corpus / projection / erasure space.
const CASES: u32 = 64;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 2 records so a small object still has several blocks and
/// the interleave across objects has something real to merge (matches
/// `spans_columnar.rs`).
fn small_blocks() -> RspanConfig {
    RspanConfig {
        block_target_records: 2,
        ..RspanConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Generated corpus
// ---------------------------------------------------------------------------

/// One generated span. `service`/`http_method` are optional so `service_name`
/// (a nullable v3 column) and a dynamic attribute page are present in some rows
/// and absent in others; `with_message` toggles the nullable `status_message`;
/// `events` promotes an `_events_raw` blob into the nested event columns the
/// fast path skips. `erasable` decides whether the row carries the erasure
/// target attribute, so the survivor reference can be built at generation time.
#[derive(Clone, Debug)]
struct SpanSpec {
    trace: u8,
    span_id: u8,
    start: i64,
    service: Option<String>,
    http_method: Option<String>,
    events: usize,
    with_message: bool,
    erasable: bool,
}

impl SpanSpec {
    /// Whether this span carries any attribute beyond the always-present
    /// `user_id`, i.e. a page the attrs-excluding fast path can skip.
    fn has_extra_attr(&self) -> bool {
        self.service.is_some() || self.http_method.is_some() || self.events > 0
    }
}

fn arb_span() -> impl Strategy<Value = SpanSpec> {
    (
        0u8..5,     // trace
        0u8..4,     // span_id
        0i64..1000, // start
        prop::option::of(prop::sample::select(vec![
            "checkout",
            "payments",
            "inventory",
        ])),
        prop::option::of(prop::sample::select(vec!["GET", "POST"])),
        0usize..3,     // events
        any::<bool>(), // with_message
        any::<bool>(), // erasable
    )
        .prop_map(
            |(trace, span_id, start, service, http_method, events, with_message, erasable)| {
                SpanSpec {
                    trace,
                    span_id,
                    start,
                    service: service.map(str::to_string),
                    http_method: http_method.map(str::to_string),
                    events,
                    with_message,
                    erasable,
                }
            },
        )
}

/// One object holds 1..=4 spans; a corpus holds 1..=3 objects. Objects stay
/// non-empty so every object writes at least one block.
fn arb_objects() -> impl Strategy<Value = Vec<Vec<SpanSpec>>> {
    prop::collection::vec(prop::collection::vec(arb_span(), 1..=4), 1..=3)
}

/// A projection is a non-empty ordered subset of the eleven column indices. The
/// permutation-then-prefix shape exercises both which columns are kept and the
/// order they appear in, so column-order and null-placement wiring is covered.
fn arb_projection() -> impl Strategy<Value = Vec<usize>> {
    (
        Just((0..SPAN_COL_COUNT).collect::<Vec<usize>>()).prop_shuffle(),
        1..=SPAN_COL_COUNT,
    )
        .prop_map(|(perm, k)| perm.into_iter().take(k).collect())
}

#[derive(Clone, Debug)]
struct Scenario {
    objects: Vec<Vec<SpanSpec>>,
    projection: Vec<usize>,
}

fn arb_scenario() -> impl Strategy<Value = Scenario> {
    (arb_objects(), arb_projection()).prop_map(|(objects, projection)| Scenario {
        objects,
        projection,
    })
}

// ---------------------------------------------------------------------------
// Materializing the corpus into RSPAN objects
// ---------------------------------------------------------------------------

/// Length-delimited, hex-encoded `_events_raw` value: each event's verbatim
/// payload prefixed with its uvarint length, concatenated and hex-encoded. This
/// is the exact grammar `ravel_rspan::record::parse_events` splits on, so a
/// non-empty blob sequence is always promoted into the nested event columns.
fn events_raw_value(count: usize) -> String {
    let mut raw = Vec::new();
    for i in 0..count {
        // A small, non-empty payload per event; the bytes are kept verbatim as
        // the event's `attrs_blob`, so any non-empty sequence parses.
        let payload = [0x08u8, 0x02, i as u8 + 1];
        put_uvarint(&mut raw, payload.len() as u64);
        raw.extend_from_slice(&payload);
    }
    let mut hex = String::with_capacity(raw.len() * 2);
    for byte in raw {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

fn put_uvarint(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

fn build_span(spec: &SpanSpec) -> SpanRecord {
    let mut attrs: Vec<(String, String)> = Vec::new();
    if let Some(svc) = &spec.service {
        attrs.push(("service.name".to_string(), svc.clone()));
    }
    if let Some(method) = &spec.http_method {
        attrs.push(("http.method".to_string(), method.clone()));
    }
    // The erasure axis: an erasable span carries the target value, otherwise a
    // non-matching one. Present on every span so the merged map is never empty.
    attrs.push((
        ERASE_KEY.to_string(),
        if spec.erasable {
            ERASE_VALUE.to_string()
        } else {
            "keep".to_string()
        },
    ));
    if spec.events > 0 {
        attrs.push(("_events_raw".to_string(), events_raw_value(spec.events)));
    }
    SpanRecord {
        trace_id: [spec.trace; 16],
        span_id: [spec.span_id; 8],
        parent_span_id: if spec.span_id == 0 {
            None
        } else {
            Some([spec.span_id - 1; 8])
        },
        name: format!("span-{}-{}", spec.trace, spec.span_id),
        start_ts_ns: spec.start,
        end_ts_ns: spec.start + 100,
        status_code: StatusCode::Ok,
        status_message: if spec.with_message {
            Some(format!("msg-{}", spec.span_id))
        } else {
            None
        },
        attrs,
    }
}

async fn write_object(store: &MemoryStore, key: &str, records: &[SpanRecord]) -> SegmentRef {
    let mut w = RspanWriter::new(small_blocks(), identity());
    for r in records {
        w.push(r.clone());
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records
        .iter()
        .map(|r| r.start_ts_ns)
        .min()
        .expect("nonempty");
    let max = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// Materialize a corpus of objects onto `store`, one RSPAN object per inner
/// `Vec`, returning the L0 segment refs.
async fn materialize(store: &MemoryStore, objects: &[Vec<SpanSpec>]) -> Vec<SegmentRef> {
    let mut segments = Vec::new();
    for (i, obj) in objects.iter().enumerate() {
        let records: Vec<SpanRecord> = obj.iter().map(build_span).collect();
        let key = format!("spans/obj-{i}.rspan");
        segments.push(write_object(store, &key, &records).await);
    }
    segments
}

// ---------------------------------------------------------------------------
// The scan under test (the shared `spans_columnar.rs` harness shape)
// ---------------------------------------------------------------------------

struct ScanRun {
    batches: Vec<RecordBatch>,
    columnar_batches: usize,
    rowpath_batches: usize,
    pages_decoded: usize,
    pages_skipped: usize,
}

/// Execute a `SpansScanExec` directly over `segments` with the given projection
/// and pending-erasure requests, returning its batches and the path metrics. A
/// single partition keeps the emitted order deterministic so the two paths'
/// batches compare byte for byte.
async fn run_scan(
    store: Arc<dyn ObjectStoreBackend>,
    segments: Vec<SegmentRef>,
    projection: Option<Vec<usize>>,
    erasure_reqs: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> ScanRun {
    let erasure = snapshot_pending_erasure_predicates(&Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: erasure_reqs,
    });
    let scan = SpansScanExec::new(
        TENANT,
        SpanSegmentFetcher::new(store),
        &segments,
        1,
        SpanQuery::ts_range(i64::MIN, i64::MAX),
        None,
        None,
        None,
        None,
        Arc::new(erasure),
        projection,
        QueryAccounting::new(),
    )
    .expect("build scan");

    let mut stream = scan
        .execute(0, Arc::new(TaskContext::default()))
        .expect("execute");
    let mut batches = Vec::new();
    while let Some(next) = stream.next().await {
        batches.push(next.expect("batch"));
    }
    drop(stream);

    let metrics = scan.metrics().expect("metrics");
    let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    ScanRun {
        columnar_batches: count("columnar_batches"),
        rowpath_batches: count("rowpath_batches"),
        pages_decoded: count("pages_decoded"),
        pages_skipped: count("pages_skipped"),
        batches,
    }
}

/// A pending erasure request matching `key = value` on the merged attribute map.
fn erasure_req(key: &str, value: &str) -> ravel_proto::commit::v1::ErasureRequest {
    ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: key.to_string(),
            value: value.to_string(),
        }],
        ..Default::default()
    }
}

/// The scan's output as a single batch under the projected schema, so two runs
/// over corpora with different block layouts (the erasure case) compare on rows
/// alone, independent of how either side chunked its output. An empty run yields
/// a zero-row batch with the same schema, so empty compares equal to empty.
fn concat_run(run: &ScanRun, schema: &SchemaRef) -> RecordBatch {
    concat_batches(schema, &run.batches).expect("concat batches")
}

fn total_rows(run: &ScanRun) -> usize {
    run.batches.iter().map(|b| b.num_rows()).sum()
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

/// The acceptance test (ADR-0110 decision 7): over random RSPAN corpora and
/// random projection subsets, the columnar fast path and the row path produce
/// identical batches, and a pending erasure predicate forces the row path whose
/// output still agrees with an independent survivor reference.
///
/// Run through a manual `TestRunner` rather than the `proptest!` macro so the
/// per-case attribute/event/erasure shares can be tallied and reported (run with
/// `--nocapture` to see them); the runner still shrinks a failing case.
#[test]
fn both_paths_agree_over_every_projection_subset() {
    let total = AtomicU64::new(0);
    let cases_with_extra_attr = AtomicU64::new(0);
    let cases_with_events = AtomicU64::new(0);
    let cases_with_erased_span = AtomicU64::new(0);

    let mut runner = TestRunner::new(ProptestConfig::with_cases(CASES));
    let result = runner.run(&arb_scenario(), |scn| {
        total.fetch_add(1, Ordering::Relaxed);
        let corpus_has_extra_attr = scn.objects.iter().flatten().any(SpanSpec::has_extra_attr);
        let corpus_has_events = scn.objects.iter().flatten().any(|s| s.events > 0);
        let corpus_has_erased = scn.objects.iter().flatten().any(|s| s.erasable);
        if corpus_has_extra_attr {
            cases_with_extra_attr.fetch_add(1, Ordering::Relaxed);
        }
        if corpus_has_events {
            cases_with_events.fetch_add(1, Ordering::Relaxed);
        }
        if corpus_has_erased {
            cases_with_erased_span.fetch_add(1, Ordering::Relaxed);
        }

        let projection = scn.projection.clone();
        let excludes_attrs = !projection.contains(&SPAN_COL_ATTRS);
        let proj_schema: SchemaRef = Arc::new(
            spans_schema()
                .project(&projection)
                .expect("project spans schema"),
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            // --- Comparison 1: columnar vs row agreement over the projection ---
            let store = MemoryStore::new();
            let segments = materialize(&store, &scn.objects).await;
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

            // No erasure: columnar when the projection excludes `attrs`.
            let columnar =
                run_scan(Arc::clone(&store), segments.clone(), Some(projection.clone()), Vec::new())
                    .await;
            // Same projection forced onto the row path by a no-match erasure,
            // which removes nothing so the output is identical.
            let rowpath = run_scan(
                Arc::clone(&store),
                segments.clone(),
                Some(projection.clone()),
                vec![erasure_req("__no_match_key__", "__no_match_value__")],
            )
            .await;

            prop_assert_eq!(
                &columnar.batches,
                &rowpath.batches,
                "fast path and row path disagree for projection {:?}",
                projection
            );

            let rows = total_rows(&columnar);
            if excludes_attrs {
                if rows > 0 {
                    prop_assert!(
                        columnar.columnar_batches > 0,
                        "an attrs-excluding projection over a non-empty corpus must take the fast path (projection {:?})",
                        projection
                    );
                    prop_assert_eq!(
                        columnar.rowpath_batches,
                        0,
                        "the fast path must not fall back for projection {:?}",
                        projection
                    );
                    // Attributes and/or events are present in the corpus, so the
                    // attrs-excluding decode skips their pages: proof the
                    // comparison is not vacuous when the corpus carries them.
                    if corpus_has_extra_attr {
                        prop_assert!(
                            columnar.pages_skipped > 0,
                            "excluding attrs over a corpus with attribute/event pages must skip pages ({} decoded, {} skipped, projection {:?})",
                            columnar.pages_decoded,
                            columnar.pages_skipped,
                            projection
                        );
                    }
                }
                prop_assert_eq!(
                    rowpath.columnar_batches,
                    0,
                    "a no-match erasure must force the row path for projection {:?}",
                    projection
                );
                if rows > 0 {
                    prop_assert!(
                        rowpath.rowpath_batches > 0,
                        "the row path must have run under the no-match erasure (projection {:?})",
                        projection
                    );
                }
            } else {
                // Projecting `attrs` is ineligible, so both runs take the row
                // path; the equality above still guards row-path determinism.
                if rows > 0 {
                    prop_assert!(
                        columnar.rowpath_batches > 0 && columnar.columnar_batches == 0,
                        "an attrs-projecting query must take the row path (projection {:?})",
                        projection
                    );
                }
            }

            // --- Comparison 2: erasure fallback agrees with the survivor reference ---
            let erased = run_scan(
                Arc::clone(&store),
                segments.clone(),
                Some(projection.clone()),
                vec![erasure_req(ERASE_KEY, ERASE_VALUE)],
            )
            .await;

            // Independent reference: the same projection over a corpus with the
            // erasable spans removed at generation, run with no erasure (columnar
            // when eligible). Objects that lose all their spans are dropped.
            let survivor_objects: Vec<Vec<SpanSpec>> = scn
                .objects
                .iter()
                .map(|obj| obj.iter().filter(|s| !s.erasable).cloned().collect::<Vec<_>>())
                .filter(|obj: &Vec<SpanSpec>| !obj.is_empty())
                .collect();
            let survivor_store = MemoryStore::new();
            let survivor_segments = materialize(&survivor_store, &survivor_objects).await;
            let survivor_store: Arc<dyn ObjectStoreBackend> = Arc::new(survivor_store);
            let reference = run_scan(
                survivor_store,
                survivor_segments,
                Some(projection.clone()),
                Vec::new(),
            )
            .await;

            prop_assert_eq!(
                concat_run(&erased, &proj_schema),
                concat_run(&reference, &proj_schema),
                "erasure fallback disagrees with the survivor reference for projection {:?}",
                projection
            );
            prop_assert_eq!(
                erased.columnar_batches,
                0,
                "a matching pending erasure must force the row path for projection {:?}",
                projection
            );
            if total_rows(&erased) > 0 {
                prop_assert!(
                    erased.rowpath_batches > 0,
                    "the row path must have run under a matching erasure (projection {:?})",
                    projection
                );
            }

            Ok(())
        })?;
        Ok(())
    });

    let total = total.load(Ordering::Relaxed).max(1);
    eprintln!(
        "spans_differential: {} cases; extra-attr {:.0}%, events {:.0}%, erased-span {:.0}%",
        total,
        100.0 * cases_with_extra_attr.load(Ordering::Relaxed) as f64 / total as f64,
        100.0 * cases_with_events.load(Ordering::Relaxed) as f64 / total as f64,
        100.0 * cases_with_erased_span.load(Ordering::Relaxed) as f64 / total as f64,
    );

    if let Err(err) = result {
        panic!("differential failed: {err}");
    }
}

//! ADR-4523 R5, step 1 (issue #4530): what the metrics run fold charges to
//! the memory budget against what the process really allocates, on a real
//! PromQL range query.
//!
//! Shape D of the pre-registration: `rate(m[5m])` over 2,000 series of one
//! hour at 15 s, written as L0 flushes and compacted into L1 in a
//! `MemoryStore`, as `tests/l1_read_memory.rs` builds its tenants, and run
//! through `QueryEngine::range_hist_with_budgets`. That call reaches
//! `range_inner`, `prefetch` and `prefetch_metric_plans`, which builds the
//! query's `ScalarFold`; `fetch_all_samples_and_histograms` absorbs each
//! object's runs into it.
//!
//! This binary installs jemalloc as its global allocator. The fold's absorb
//! observer (`run_fold_support::set_absorb_observer`) reads jemalloc's
//! `stats.allocated` after every absorb; the figure kept is the one read at
//! the fold's peak charge, as a delta over what the process held before the
//! engine call.
//!
//! D's figure line therefore reads `process_delta=`, not `live=`: it counts
//! every byte the process allocated across the engine call (decoded objects,
//! the catalog, the evaluator), where shapes A to C's `live=` in
//! `sample_fold.rs` counts only the fold's own containers. D's ratio is not
//! comparable with A to C's.

#![cfg(not(target_env = "msvc"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel};
use ravel_commit::keys::commit_key_for_record;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, PublishOutcome, compact_bucket,
};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_promql::RangeValue;
use ravel_query::run_fold_support;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntryV4, SeriesInput,
    VERSION_V7, decode_catalog_v5, open_from_full,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange};
use tikv_jemalloc_ctl::{Access, AsName, epoch, stats};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const HOUR: u32 = 500_000;
const TENANT: TenantHash = TenantHash([0x52; 16]);
const SERIES: usize = 2_000;
const INTERVAL_NS: i64 = 15 * NS_PER_SEC;
/// One hour at 15 s.
const SAMPLES_PER_SERIES: i64 = NS_PER_HOUR / INTERVAL_NS;
/// L0 flushes compacted into the part, each a contiguous quarter of the hour
/// for every series.
const FLUSHES: i64 = 4;
const QUERY: &str = "rate(m[5m])";
const MIB: u64 = 1_048_576;
/// The query's step.
const STEP_MS: i64 = 60_000;
/// Points per series of `rate(m[5m])` at a 60 s step over the hour: 61
/// steps, start and end inclusive, less the first, whose window holds only
/// the sample at the hour's start and so yields no rate.
const POINTS_PER_SERIES: usize = 60;

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn span_end() -> i64 {
    hour_start() + NS_PER_HOUR
}

fn now_ns() -> i64 {
    span_end() + NS_PER_HOUR
}

fn allocated() -> u64 {
    epoch::advance().expect("jemalloc epoch");
    stats::allocated::read().expect("jemalloc stats.allocated") as u64
}

fn series_labels(i: usize) -> (SeriesId, LabelSet) {
    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: "m".to_string(),
        },
        Label {
            name: "instance".to_string(),
            value: format!("instance-{i}"),
        },
    ])
    .expect("labels");
    let id = SeriesId::compute(&TenantId::new("t"), "m", &labels).expect("series id");
    (id, labels)
}

fn l0_writer_id(seq: u64) -> Uuid {
    Uuid::from_u128(u128::from(seq) + 1)
}

async fn put_l0(store: &MemoryStore, seq: u64, series: Vec<SeriesInput>) {
    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: l0_writer_id(seq).to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write L0");
    let rec = record::build(NewCommitRecord {
        tenant_hash: TENANT,
        signal: Signal::Metrics,
        shard: 0,
        writer_id: l0_writer_id(seq),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: hour_start() + i64::try_from(seq).expect("seq"),
        ingest_hour_bucket: HOUR,
    })
    .expect("commit record");
    store
        .put(
            &rec.object_key,
            written.bytes,
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put data object");
    let commit_key = commit_key_for_record(&rec).expect("commit key");
    store
        .put(
            &commit_key,
            record::encode(&rec),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put commit record");
}

/// Compacts the hour's L0 flushes into L1 and returns every catalog entry of
/// the L1 parts that resolve.
async fn compact_l1(store: &Arc<MemoryStore>) -> Vec<SeriesEntryV4> {
    let outcome = compact_bucket(
        store.as_ref(),
        &FixedClock::new(hour_start() + 3 * NS_PER_HOUR),
        &CompactorConfig::default(),
        &Bucket::new(TENANT, Signal::Metrics, 0, HOUR),
    )
    .await
    .expect("compact_bucket");
    match outcome {
        CompactionOutcome::Compacted { publish, .. } => {
            assert_eq!(publish, PublishOutcome::Published);
        }
        other => panic!("expected the hour to compact, got {other:?}"),
    }
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = Catalog::new(backend, CatalogConfig::default()).expect("catalog");
    let snapshot = catalog
        .resolve(
            &TENANT,
            Signal::Metrics,
            TimeRange {
                start_ns: hour_start(),
                end_ns: span_end(),
            },
            &[],
            now_ns(),
        )
        .await
        .expect("resolve");
    assert!(
        snapshot
            .segments
            .iter()
            .all(|s| matches!(s.level, SegmentLevel::L1 { .. })),
        "only L1 parts resolve"
    );
    let mut entries = Vec::new();
    for segment in &snapshot.segments {
        let object = store
            .get(&segment.data_object_key, GetRange::Full)
            .await
            .expect("get L1 part")
            .data;
        let footer = open_from_full(&object, ReaderLimits::default())
            .expect("footer")
            .footer;
        entries
            .extend(decode_catalog_v5(&footer, &object, ReaderLimits::default()).expect("catalog"));
    }
    entries
}

/// Shape D's tenant: `SERIES` counters sampled every 15 s for one hour.
async fn shape_d_tenant() -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::new());
    let per_flush = SAMPLES_PER_SERIES / FLUSHES;
    for flush in 0..FLUSHES {
        let inputs: Vec<SeriesInput> = (0..SERIES)
            .map(|i| {
                let (series_id, labels) = series_labels(i);
                let samples = (flush * per_flush..(flush + 1) * per_flush)
                    .map(|k| Sample {
                        ts_ns: hour_start() + k * INTERVAL_NS,
                        value: (i % 7 + 1) as f64 * k as f64,
                    })
                    .collect();
                SeriesInput {
                    series_id,
                    labels,
                    samples,
                }
            })
            .collect();
        put_l0(&store, u64::try_from(flush).expect("flush") + 1, inputs).await;
    }
    let entries = compact_l1(&store).await;
    let samples: u64 = entries
        .iter()
        .map(|e| u64::from(e.entry.sample_count))
        .sum();
    println!(
        "R5 fixture shape=D series={} samples={samples}",
        entries.len()
    );
    assert_eq!(entries.len(), SERIES, "shape D's series count");
    assert_eq!(
        samples,
        SERIES as u64 * SAMPLES_PER_SERIES as u64,
        "shape D's sample count"
    );
    store
}

/// Whether the observer records: only while the measured engine call runs.
static ARMED: AtomicBool = AtomicBool::new(false);
/// `stats.allocated` before the measured engine call.
static BASELINE: AtomicU64 = AtomicU64::new(0);
/// `(peak charged, stats.allocated read at that charge)`.
static PEAK: Mutex<Option<(u64, u64)>> = Mutex::new(None);

fn observe(charged: u64) {
    if charged == 0 || !ARMED.load(Ordering::Acquire) {
        return;
    }
    let now = allocated();
    let mut peak = PEAK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if peak.is_none_or(|(c, _)| charged >= c) {
        *peak = Some((charged, now));
    }
}

/// The R5 allocator self-check: jemalloc is the global allocator when a
/// 1 MiB allocation moves `stats.allocated` by 1 MiB.
fn jemalloc_check(lines: &mut Vec<String>) {
    let before = allocated();
    let held: Vec<u8> = Vec::with_capacity(MIB as usize);
    let after = allocated();
    std::hint::black_box(&held);
    drop(held);
    let delta = after.saturating_sub(before);
    let line = format!("R5 jemalloc_check delta={delta}");
    println!("{line}");
    lines.push(line);
    assert!(
        (MIB..2 * MIB).contains(&delta),
        "a {MIB}-byte Vec moved stats.allocated by {delta} B: jemalloc is not measuring"
    );
}

/// `run_fold_support::table_bytes` against jemalloc for one table of each of
/// the fold's key and value types: jemalloc's figure is at least the
/// formula's and within jemalloc's size-class rounding of it
/// ([`within_size_class`]).
fn table_formula_check() {
    let page = jemalloc_page();
    for (name, build) in [
        (
            "by_series",
            run_fold_support::by_series_table as fn(usize) -> run_fold_support::HeldTable,
        ),
        ("prefix_ids", run_fold_support::prefix_ids_table),
    ] {
        let before = allocated();
        let table = build(SERIES);
        let measured = allocated().saturating_sub(before);
        let formula = table.formula_bytes();
        let error = i128::from(formula) - i128::from(measured);
        println!(
            "R5 table_check table={name} entries={SERIES} capacity={} formula={formula} \
             jemalloc={measured} error_bytes={error} error_pct={:.4}",
            table.capacity(),
            error as f64 * 100.0 / measured.max(1) as f64
        );
        assert!(
            within_size_class(formula, measured, page),
            "table {name}: formula {formula} B against jemalloc {measured} B, \
             want formula <= jemalloc <= formula + formula / 4 + {page}"
        );
        drop(table);
    }
}

/// jemalloc's page size, which the size-class bound adds as its last step.
fn jemalloc_page() -> u64 {
    let page: usize = b"arenas.page\0"
        .name()
        .read()
        .expect("jemalloc arenas.page");
    page as u64
}

/// Whether `measured`, jemalloc's figure for one allocation the formula sizes
/// at `formula` bytes, is what jemalloc's size classes allow. jemalloc spaces
/// its size classes four per doubling, so it rounds a large request up by at
/// most a quarter of the power of two below it, which is at most a quarter of
/// the request, plus one page for the extent. Which class a request lands in
/// varies by build and host, so a one-page bound holds on some hosts and not
/// others.
fn within_size_class(formula: u64, measured: u64, page: u64) -> bool {
    formula <= measured && measured <= formula + formula / 4 + page
}

/// The size-class bound accepts both hosts' measured tables, which the
/// one-page bound did not, and still refuses a figure above the bound or
/// below the formula.
#[test]
fn r5_table_bound_is_jemalloc_size_class_spacing() {
    let one_page = |formula: u64, measured: u64| formula <= measured && measured < formula + 4_096;
    // x86_64 Linux: rounded to the next page.
    assert!(within_size_class(299_024, 303_104, 4_096));
    assert!(one_page(299_024, 303_104));
    // The reviewer's host: rounded to the 327,680 B size class.
    assert!(within_size_class(299_016, 327_680, 4_096));
    assert!(
        !one_page(299_016, 327_680),
        "the one-page bound refuses the reviewer's figure"
    );
    assert!(!within_size_class(299_024, 400_000, 4_096));
    assert!(!within_size_class(299_024, 290_000, 4_096));
}

/// The digest of shape D's answer captured from main's code (c9ef663) before
/// the R5 step 2 change (issue #4530): the answer must not change.
const SHAPE_D_DIGEST: u64 = 0x5979_357e_63c2_980e;

/// FNV-1a over the matrix's `Debug` text with its series sorted by labels.
/// `Debug` prints each float in its shortest round-trip form, so two answers
/// with the same digest hold the same values.
fn matrix_digest(matrix: &ravel_promql::HistogramAwareMatrix) -> u64 {
    let mut sorted: Vec<_> = matrix.iter().collect();
    sorted.sort_by(|a, b| a.0.iter().cmp(b.0.iter()));
    format!("{sorted:?}")
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

#[test]
fn r5_shape_d_fold_charge_against_jemalloc() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            assert!(
                run_fold_support::set_absorb_observer(observe),
                "the observer is installed once"
            );
            let mut lines = Vec::new();
            jemalloc_check(&mut lines);
            table_formula_check();

            let store = shape_d_tenant().await;
            let backend: Arc<dyn ObjectStoreBackend> = store.clone();
            let catalog =
                Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog");
            let config = EngineConfig {
                max_series: 1_000_000,
                max_samples: 500_000_000,
                ..EngineConfig::default()
            };
            let budget = Arc::new(MemoryBudget::unlimited());
            let engine = QueryEngine::new(Arc::new(catalog), backend, config)
                .with_memory_budget(Arc::clone(&budget));

            BASELINE.store(allocated(), Ordering::Release);
            ARMED.store(true, Ordering::Release);
            let result = engine
                .range_hist_with_budgets(
                    TENANT,
                    QUERY,
                    hour_start() / 1_000_000,
                    span_end() / 1_000_000,
                    STEP_MS,
                    &[],
                    now_ns(),
                    Duration::from_secs(600),
                    None,
                )
                .await;
            ARMED.store(false, Ordering::Release);
            let (value, _annotations, _stats) = result.expect("shape D answers");
            let RangeValue::Matrix(matrix) = value else {
                panic!("shape D answers a matrix, got {value:?}");
            };
            assert_eq!(matrix.len(), SERIES, "shape D's answered series");
            for (labels, points) in &matrix {
                assert_eq!(
                    points.len(),
                    POINTS_PER_SERIES,
                    "points for {labels:?} over 1 h at a {STEP_MS} ms step"
                );
            }
            let digest = matrix_digest(&matrix);
            println!("R5 digest shape=D {digest:#018x}");
            assert_eq!(digest, SHAPE_D_DIGEST, "shape D's answer changed");

            let baseline = BASELINE.load(Ordering::Acquire);
            let peak = *PEAK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((charged, at_peak)) = peak {
                let line =
                    run_fold_support::r5_line("D", charged, at_peak.saturating_sub(baseline));
                println!("{line}");
                lines.push(line);
            }
            let violations = run_fold_support::presence_violations(&lines, &["D"]);
            assert!(violations.is_empty(), "R5 presence rule: {violations:?}");
            let checks = lines
                .iter()
                .filter(|l| l.starts_with("R5 jemalloc_check "))
                .count();
            assert_eq!(checks, 1, "exactly one allocator self-check line");
        });
}

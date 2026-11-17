//! A metrics series listing (`/api/v1/series`, `/api/v1/labels`,
//! `/api/v1/label/{name}/values`, all through `QueryEngine::resolve_series`)
//! decodes each object's series ids and labels without decoding or charging
//! the per-sample provenance extension a run-merged L1 part carries.
//!
//! The parts are built with the real compactor in a `MemoryStore`. The
//! reference each listing is compared with is the decode that still
//! materializes provenance, `SegmentFetcher::fetch_series`, run per object
//! under an unlimited budget, folded one copy per series id and filtered with
//! the erasure pass the listing applies.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, Snapshot};
use ravel_commit::keys::commit_key_for_record;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, PublishOutcome, compact_bucket,
};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_promql::LabelMatcher;
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::erasure::{ErasurePredicate, retain_series_entries};
use ravel_query::{EngineConfig, FetchError, QueryEngine, QueryError, SegmentFetcher};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7,
    decode_catalog_v5, open_from_full,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const HOUR: u32 = 500_000;
const TENANT: TenantHash = TenantHash([0x43; 16]);
const SPAN_HOURS: i64 = 12;
/// L0 flushes, each a contiguous quarter of the span for every series, so
/// the compactor merges each series' runs and writes the provenance
/// extension.
const FLUSHES: i64 = 4;
const METRIC: &str = "requests_total";

/// The `user_id` value the erasure predicates name: every third series.
const ERASED_USER: &str = "u1";

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn span_end() -> i64 {
    hour_start() + SPAN_HOURS * NS_PER_HOUR
}

fn now_ns() -> i64 {
    span_end() + NS_PER_HOUR
}

fn window() -> TimeRange {
    TimeRange {
        start_ns: hour_start(),
        end_ns: span_end(),
    }
}

#[derive(Debug, Clone, Copy)]
struct Shape {
    series: usize,
    interval_ns: i64,
}

impl Shape {
    fn samples(self) -> u64 {
        self.series as u64 * (SPAN_HOURS * NS_PER_HOUR / self.interval_ns) as u64
    }
}

/// Below 4096 series: the L1 part carries a whole SERIES_META section.
const DENSE: Shape = Shape {
    series: 200,
    interval_ns: 30 * NS_PER_SEC,
};

/// At or above 4096 series: a chunked SERIES_META_CHUNKS catalog.
const CHUNKED: Shape = Shape {
    series: 4_100,
    interval_ns: 900 * NS_PER_SEC,
};

fn series_labels(i: usize) -> (SeriesId, LabelSet) {
    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: METRIC.to_string(),
        },
        Label {
            name: "instance".to_string(),
            value: format!("instance-{i}"),
        },
        Label {
            name: "user_id".to_string(),
            value: format!("u{}", i % 3),
        },
    ])
    .expect("labels");
    let id = SeriesId::compute(&TenantId::new("t"), METRIC, &labels).expect("series id");
    (id, labels)
}

async fn put_l0(store: &MemoryStore, seq: u64, series: Vec<SeriesInput>) {
    let writer_id = Uuid::from_u128(u128::from(seq) + 1);
    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: writer_id.to_string(),
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
        writer_id,
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

/// `shape`'s series written as `FLUSHES` L0 flushes into one ingest hour,
/// compacted into L1 when `compact` holds.
async fn tenant(shape: Shape, compact: bool) -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::new());
    let per_flush = SPAN_HOURS * NS_PER_HOUR / shape.interval_ns / FLUSHES;
    for flush in 0..FLUSHES {
        let inputs = (0..shape.series)
            .map(|i| {
                let (series_id, labels) = series_labels(i);
                let samples = (flush * per_flush..(flush + 1) * per_flush)
                    .map(|k| Sample {
                        ts_ns: hour_start() + k * shape.interval_ns,
                        value: (i as f64) + k as f64,
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
    if compact {
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
    }
    store
}

async fn snapshot(store: &Arc<MemoryStore>) -> Snapshot {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    Catalog::new(backend, CatalogConfig::default())
        .expect("catalog")
        .resolve(&TENANT, Signal::Metrics, window(), &[], now_ns())
        .await
        .expect("resolve")
}

/// One resolved object, as its bytes give it.
#[derive(Debug)]
struct Object {
    l1: bool,
    /// Samples whose runs carry per-sample provenance.
    flagged: u64,
    size: u64,
    /// The inflated length of the LABEL_DICT, SERIES_IDS and SERIES_META
    /// sections, what the catalog decode reserves before it runs.
    catalog_sections: u64,
}

async fn objects(store: &Arc<MemoryStore>) -> Vec<Object> {
    let mut out = Vec::new();
    for segment in snapshot(store).await.segments {
        let object = store
            .get(&segment.data_object_key, GetRange::Full)
            .await
            .expect("get object")
            .data;
        let footer = open_from_full(&object, ReaderLimits::default())
            .expect("footer")
            .footer;
        let flagged = decode_catalog_v5(&footer, &object, ReaderLimits::default())
            .expect("catalog")
            .iter()
            .flat_map(|e| &e.per_sample_provenance)
            .flatten()
            .map(|column| column.len() as u64)
            .sum();
        let catalog_sections = footer
            .sections
            .iter()
            .filter(|s| {
                // LABEL_DICT, SERIES_IDS and SERIES_META (docs/segment-format.md).
                [1, 5, 6].contains(&s.kind)
            })
            .map(|s| s.uncompressed_len)
            .sum();
        out.push(Object {
            l1: matches!(segment.level, SegmentLevel::L1 { .. }),
            flagged,
            size: object.len() as u64,
            catalog_sections,
        });
    }
    out
}

fn engine(store: &Arc<MemoryStore>, budget: &Arc<MemoryBudget>) -> QueryEngine {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog");
    let config = EngineConfig {
        max_series: 1_000_000,
        ..EngineConfig::default()
    };
    QueryEngine::new(Arc::new(catalog), backend, config).with_memory_budget(Arc::clone(budget))
}

fn matchers() -> Vec<LabelMatcher> {
    vec![LabelMatcher::equal("__name__", METRIC)]
}

/// The listing as `/api/v1/series` resolves it, sorted by series id.
async fn listing(
    store: &Arc<MemoryStore>,
    budget_bytes: u64,
) -> Result<Vec<(SeriesId, LabelSet)>, QueryError> {
    let budget = Arc::new(MemoryBudget::new(budget_bytes));
    let (mut series, _coverage) = engine(store, &budget)
        .resolve_series(
            TENANT,
            &matchers(),
            window(),
            &[],
            now_ns(),
            Duration::from_secs(600),
        )
        .await?;
    series.sort_by_key(|(id, _)| *id);
    Ok(series)
}

/// The reference listing: every resolved object's series through the decode
/// that materializes provenance, under `budget_bytes`, one copy per id,
/// filtered by `predicates` as the listing filters them.
async fn reference(
    store: &Arc<MemoryStore>,
    budget_bytes: u64,
    predicates: &[ErasurePredicate],
) -> Result<Vec<(SeriesId, LabelSet)>, FetchError> {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let fetcher =
        SegmentFetcher::new(backend).with_memory_budget(Arc::new(MemoryBudget::new(budget_bytes)));
    let mut by_id = BTreeMap::new();
    for segment in snapshot(store).await.segments {
        let mut entries = fetcher.fetch_series(TENANT, &segment, &matchers()).await?;
        retain_series_entries(&mut entries, predicates);
        for entry in entries {
            by_id.insert(entry.series_id, entry.labels);
        }
    }
    Ok(by_id.into_iter().collect())
}

/// The smallest budget `fits` accepts, with the budget one byte below it
/// refused: what the read held reserved at its peak.
async fn minimal_budget<F, Fut>(fits: F) -> u64
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let (mut lo, mut hi) = (0u64, 1u64 << 30);
    assert!(fits(hi).await, "the read fits {hi} B");
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if fits(mid).await {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    assert!(!fits(hi - 1).await, "refused one byte below {hi} B");
    hi
}

async fn put_dreq(store: &MemoryStore, window_start_ns: i64, window_end_ns: i64) {
    let request_id = Uuid::from_u128(u128::from(window_start_ns.unsigned_abs()) + 7);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: TENANT.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 1,
        predicate: vec![ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: ERASED_USER.to_string(),
        }],
        window_start_ns,
        window_end_ns,
        reason: String::new(),
    };
    let key = keys::erasure_request_key(&TENANT, Signal::Metrics, request_id).expect("dreq key");
    store
        .put(
            &key,
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

fn predicate(window_start_ns: i64, window_end_ns: i64) -> ErasurePredicate {
    ErasurePredicate::new(
        vec![("user_id".to_string(), ERASED_USER.to_string())],
        window_start_ns,
        window_end_ns,
    )
}

/// Over L0 objects and over both L1 catalog forms, with no erasure, with a
/// windowed predicate and with a windowless one added to it, the listing is
/// byte-identical to the reference decode that materializes provenance:
/// the same series, the same labels, the same erasure exclusions.
///
/// FLIP: a skip that drops the entries whose runs carried provenance (every
/// series of the L1 parts) leaves `listing` empty against 200 and 4,100
/// reference series.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_listing_matches_the_provenance_decode_over_l0_and_l1_with_erasure() {
    let windowed = (hour_start() + NS_PER_HOUR, hour_start() + 2 * NS_PER_HOUR);
    for (shape, compact, objects_want) in [
        (DENSE, false, vec![(false, 0); 4]),
        (DENSE, true, vec![(true, DENSE.samples())]),
        (CHUNKED, true, vec![(true, CHUNKED.samples())]),
    ] {
        let store = tenant(shape, compact).await;
        let what = format!("{shape:?}, compacted: {compact}");
        let objects: Vec<(bool, u64)> = objects(&store)
            .await
            .iter()
            .map(|o| (o.l1, o.flagged))
            .collect();
        assert_eq!(objects, objects_want, "{what}: the objects");
        // The series `i` with `i % 3 == 1`.
        let erased = (shape.series + 1) / 3;
        let cases: [(&str, Option<(i64, i64)>, Vec<ErasurePredicate>, usize); 3] = [
            ("no erasure", None, vec![], shape.series),
            (
                "windowed",
                Some(windowed),
                vec![predicate(windowed.0, windowed.1)],
                shape.series,
            ),
            (
                "windowless",
                Some((0, 0)),
                vec![predicate(windowed.0, windowed.1), predicate(0, 0)],
                shape.series - erased,
            ),
        ];
        for (case, dreq, predicates, want) in cases {
            if let Some((start, end)) = dreq {
                put_dreq(&store, start, end).await;
            }
            let reference = reference(&store, u64::MAX, &predicates)
                .await
                .expect("reference listing");
            let listed = listing(&store, u64::MAX).await.expect("listing");
            assert_eq!(reference.len(), want, "{what}, {case}: reference series");
            assert_eq!(
                format!("{listed:?}"),
                format!("{reference:?}"),
                "{what}, {case}"
            );
        }
    }
}

/// One L1 part whose per-sample provenance charge alone is more than the
/// budget: the listing answers under that budget, identically to the
/// unlimited reference, while the decode that materializes provenance is
/// refused under it, through the fetcher and through a PromQL read.
///
/// The peak charges are pinned with the minimal budget each read fits. The
/// listing's is what it fetches plus what the catalog decode reserves before
/// it runs: the part, fetched whole because it is under
/// `DEFAULT_WHOLE_OBJECT_THRESHOLD`, and the inflated LABEL_DICT, SERIES_IDS
/// and SERIES_META. Nothing decoded per sample is charged. What still scales
/// with samples is stored bytes: the encoded provenance columns inside the
/// inflated SERIES_META, and the page bytes of the whole-object fetch. Here
/// that is under `LISTING_BYTES_PER_SAMPLE`, against the 96 B per sample
/// (`PROVENANCE_COLUMN_BYTES_PER_SAMPLE` plus `SAMPLE_PROVENANCE_BYTES`) the
/// provenance decode adds on top of the same figure.
///
/// FLIPS: `CatalogUse::Listing` in `list_series_phase_accounted` replaced by
/// `CatalogUse::Samples` raises the listing's peak by the provenance charge
/// (the `listing_peak` pin), and a skip that drops the entries whose runs
/// carried provenance fails the comparison with the reference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listing_fits_a_budget_its_parts_provenance_does_not() {
    /// The listing's peak charge per sample of the part, as an upper bound.
    const LISTING_BYTES_PER_SAMPLE: u64 = 2;
    let samples = DENSE.samples();
    assert_eq!(samples, 288_000);
    let store = tenant(DENSE, true).await;
    let objects = objects(&store).await;
    let [part] = objects.as_slice() else {
        panic!("one object, got {objects:?}");
    };
    assert!(part.l1);
    assert_eq!(part.flagged, samples, "every sample carries provenance");
    assert!(part.size < ravel_query::DEFAULT_WHOLE_OBJECT_THRESHOLD);
    assert_eq!((part.size, part.catalog_sections), (19_474, 304_890));
    let provenance = samples
        * (ravel_segment::PROVENANCE_COLUMN_BYTES_PER_SAMPLE
            + ravel_segment::SAMPLE_PROVENANCE_BYTES);
    assert_eq!(provenance, 27_648_000);

    let listing_peak = minimal_budget(|limit| {
        let store = Arc::clone(&store);
        async move { listing(&store, limit).await.is_ok() }
    })
    .await;
    let reference_peak = minimal_budget(|limit| {
        let store = Arc::clone(&store);
        async move { reference(&store, limit, &[]).await.is_ok() }
    })
    .await;
    eprintln!(
        "{samples} samples: listing peak {listing_peak} B ({:.4} B/sample), provenance \
         decode peak {reference_peak} B ({:.2} B/sample), provenance charge {provenance} B",
        listing_peak as f64 / samples as f64,
        reference_peak as f64 / samples as f64,
    );
    assert_eq!(
        listing_peak,
        part.size + part.catalog_sections,
        "the listing charges the fetched part and its inflated catalog sections"
    );
    assert_eq!(listing_peak, 324_364);
    assert_eq!(
        reference_peak,
        listing_peak + provenance,
        "the provenance decode charges the provenance on top"
    );
    assert!(listing_peak < LISTING_BYTES_PER_SAMPLE * samples);
    assert!(
        listing_peak < provenance,
        "the provenance alone is over budget"
    );

    let budget = listing_peak;
    let unlimited = reference(&store, u64::MAX, &[]).await.expect("reference");
    assert_eq!(unlimited.len(), DENSE.series);
    let listed = listing(&store, budget).await.expect("the listing fits");
    assert_eq!(format!("{listed:?}"), format!("{unlimited:?}"));

    match reference(&store, budget, &[]).await {
        Err(FetchError::FetchMemoryExhausted { limit, .. }) => assert_eq!(limit, budget),
        other => panic!("the provenance decode is refused, got {other:?}"),
    }
    // What `/api/v1/query_range` calls, over the whole span for one series.
    let range = |budget_bytes: u64| {
        let store = Arc::clone(&store);
        async move {
            engine(&store, &Arc::new(MemoryBudget::new(budget_bytes)))
                .range_hist_with_budgets(
                    TENANT,
                    &format!(r#"rate({METRIC}{{instance="instance-3"}}[5m])"#),
                    hour_start() / 1_000_000,
                    span_end() / 1_000_000,
                    60_000,
                    &[],
                    now_ns(),
                    Duration::from_secs(600),
                    None,
                )
                .await
                .map(|(value, _annotations, _stats)| format!("{value:?}"))
        }
    };
    let answered = range(u64::MAX).await.expect("an unlimited read answers");
    assert!(
        answered.contains("instance-3"),
        "the read returns the series: {answered}"
    );
    let sample_read = range(budget).await;
    match sample_read {
        Err(QueryError::Fetch(FetchError::FetchMemoryExhausted { limit, .. })) => {
            assert_eq!(limit, budget, "refused against the budget");
        }
        Err(other) => panic!("expected FetchMemoryExhausted, got {other:?}"),
        Ok(v) => panic!("the PromQL read keeps its provenance charge and is refused: {v:?}"),
    }
}

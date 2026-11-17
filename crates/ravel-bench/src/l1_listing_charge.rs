//! The memory-budget charge of a series listing over one L1 metrics part, as a
//! function of part size (issue #4393), for the `l1_listing_charge` bin.
//!
//! A metrics listing decodes each L1 part's catalog through the charged path
//! (`docs/query-engine.md`, "Per-sample provenance"): up to
//! [`PROVENANCE_COLUMN_BYTES_PER_SAMPLE`] of decoded provenance columns plus a
//! [`SAMPLE_PROVENANCE_BYTES`] `SampleProvenance` for every sample in the part,
//! whatever the matchers select, on top of the catalog sections' decoded
//! length. That bound grows with the samples a part holds, so the part cap
//! (`--max-l1-part-bytes`) sets the budget a listing needs. This module
//! measures the charge instead of bounding it.
//!
//! Per run it:
//!
//! 1. writes one shard bucket of synthetic L0 metrics flushes onto a
//!    `MemoryStore` with `SegmentWriter::write_histograms` and real commit
//!    records ([`build_bucket`]). The shape follows the MetricsBench `history`
//!    profile (`benchmarks/metrics/workload.json`) for one of four shards:
//!    2,500 series scraped every 15 s, split by that manifest's family shares
//!    into counters (30 %), native histograms (10 %, schema 2, eight buckets),
//!    classic histogram series (15 % rounded down to whole histograms of ten
//!    series: eight `le` buckets, `_sum` and `_count`; 370 of 2,500) and
//!    gauges (the rest, 45.2 % at 2,500). Every flush holds a
//!    contiguous slice of event time for every series, as a replay writes
//!    them, under its own writer sequence, so the compactor merges each
//!    series' runs and writes the per-sample provenance extension (the smoke
//!    test pins a peak above 96 bytes per sample, which only a part carrying
//!    provenance for every sample charges). The
//!    generator's anomalies (missing, stale, reset, out-of-order) are not
//!    injected;
//! 2. compacts the bucket with `ravel_maintain::compact_bucket` at each
//!    `max_l1_part_bytes` cap, from a fresh store holding the same L0 objects,
//!    and refuses a cap that closed fewer than [`MIN_FULL_PARTS`] full parts
//!    (every part but the tail);
//! 3. lists the first part with a matcher selecting every series under a fresh
//!    `MemoryBudget`, and binary-searches the smallest budget limit at which
//!    the listing succeeds to a 1 % resolution ([`minimal_budget`]). A refusal
//!    is `FetchError::FetchMemoryExhausted`; any other error ends the run.
//!
//! The listing call is `SegmentFetcher::fetch_series`. The function the
//! metrics discovery endpoints call per object,
//! `SegmentFetcher::list_series_phase_accounted`, is `pub(crate)` in
//! ravel-query; it runs the same `open_segment` and `decode_selected` as
//! `fetch_series`, and between them fetches the three catalog sections
//! `decode_selected` fetches and decodes SERIES_IDS uncharged, so the
//! reservations the two take are the same. [`engine_listing_succeeds`] lists
//! through `QueryEngine::resolve_series`, which reaches that function, and the
//! smoke test (`tests/l1_listing_charge.rs`) checks the two agree on a
//! one-part bucket.
//!
//! Per cap the report carries the part's wire bytes, its catalog sections'
//! decoded length (the figure `SegmentFetcher` reserves before the decode),
//! its samples and series, the minimal succeeding budget (the peak charge,
//! with the largest refused limit beside it), that peak per sample, and the
//! upper bound issue #4393 states, `(64 + 32) * samples + catalog decode
//! length` ([`upper_bound`]). That formula leaves out the wire bytes the
//! listing's own GETs hold while the catalog decodes: the open GET (the whole
//! object up to 512 KiB, the 64 KiB footer tail above it) and the catalog
//! sections' run. The report carries a bound on those ([`held_fetch_bound`])
//! and their sum with the formula ([`listing_bound`]), and how far the peak
//! lies above the formula. A listing under exactly the formula's figure is
//! refused (the smoke test pins it), so the bound band is checked against
//! [`listing_bound`]. [`check_report`] holds the bands: peak per sample in
//! [`PEAK_PER_SAMPLE_BAND`], the peak at or below the listing bound, and the
//! peak non-decreasing as samples per part grow across the caps. A figure
//! outside its band, missing, or emitted twice is a failure.
//!
//! Run it with
//! `cargo run --release -p ravel-bench --bin l1_listing_charge` (the four
//! default caps) or with `-- --caps 1048576,4194304`. One JSON document goes
//! to stdout and a table to stderr; the exit code is non-zero when a band
//! fails.

use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef};
use ravel_commit::keys::commit_key_for_record;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, PublishOutcome, compact_bucket,
};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_promql::LabelMatcher;
use ravel_query::{
    DEFAULT_SUFFIX_LEN, DEFAULT_WHOLE_OBJECT_THRESHOLD, EngineConfig, FetchError, QueryEngine,
    QueryError, SegmentFetcher,
};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds,
    PROVENANCE_COLUMN_BYTES_PER_SAMPLE, ReaderLimits, ResetHint, SAMPLE_PROVENANCE_BYTES,
    SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues, VERSION_V7, open_from_full,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::metrics_gen::{mix, unit_scaled};

/// The default part caps: 1, 4, 16 and 64 MiB.
pub const DEFAULT_CAPS: [u64; 4] = [1 << 20, 4 << 20, 16 << 20, 64 << 20];

/// Series in one shard bucket: the `history` profile's 10,000 active series
/// over four shards.
pub const DEFAULT_SERIES: usize = 2_500;

/// The `history` profile's scrape interval.
pub const DEFAULT_SCRAPE_SECS: i64 = 15;

/// Full parts (every part but the tail) each cap must close.
pub const MIN_FULL_PARTS: usize = 2;

/// The band the peak charge per sample must fall in, in bytes.
pub const PEAK_PER_SAMPLE_BAND: (f64, f64) = (1.0, 200.0);

/// The figures every cap entry of the report carries, each exactly once.
pub const CAP_FIGURES: [&str; 12] = [
    "part_wire_bytes",
    "catalog_decode_bytes",
    "held_fetch_bytes",
    "samples",
    "series",
    "peak_charge_bytes",
    "refused_at_bytes",
    "peak_per_sample_bytes",
    "upper_bound_bytes",
    "upper_bound_per_sample_bytes",
    "peak_minus_upper_bound_bytes",
    "listing_bound_bytes",
];

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
/// The ingest hour every L0 flush commits into; event time runs up to it.
const HOUR: u32 = 500_000;
const TENANT: &str = "l1-listing-charge";
const SEED: u64 = 4_393;

/// Catalog section kinds (docs/segment-format.md), the ones
/// `SegmentFetcher` reserves the decoded length of before a catalog decode.
const SECTION_LABEL_DICT: u32 = 1;
const SECTION_SERIES_IDS: u32 = 5;
const SECTION_SERIES_META: u32 = 6;
const SECTION_SERIES_IDX: u32 = 8;
const SECTION_SERIES_META_CHUNKS: u32 = 9;

const JOBS: [&str; 4] = [
    "metricsbench-api",
    "metricsbench-web",
    "metricsbench-worker",
    "metricsbench-db",
];
const REGIONS: [&str; 3] = ["eu-west-1", "us-east-1", "ap-south-1"];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const STATUSES: [&str; 4] = ["200", "204", "404", "500"];
const VERSIONS: [&str; 2] = ["1.24.0", "1.25.0"];
const CLASSIC_BOUNDS: [&str; 8] = [
    "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "+Inf",
];
/// Each bucket's share of a classic histogram's observations, cumulative.
const CLASSIC_SHARES: [f64; 8] = [0.05, 0.12, 0.3, 0.55, 0.78, 0.92, 0.98, 1.0];
/// Series one classic histogram writes: its buckets, `_sum` and `_count`.
const CLASSIC_SERIES: usize = CLASSIC_BOUNDS.len() + 2;
const NATIVE_SCHEMA: i32 = 2;
const NATIVE_BUCKETS: u32 = 8;

/// Errors of a run.
#[derive(Debug, thiserror::Error)]
pub enum ListingChargeError {
    #[error("{0}")]
    Setup(String),
    #[error("listing failed with a non-budget error: {0}")]
    Listing(String),
}

fn setup(what: &str, e: impl std::fmt::Display) -> ListingChargeError {
    ListingChargeError::Setup(format!("{what}: {e}"))
}

/// One run's shape.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub series: usize,
    pub scrape_secs: i64,
    /// Hours of event time every series carries.
    pub history_hours: i64,
    /// Hours of event time one L0 flush holds for every series.
    pub flush_hours: i64,
    pub caps: Vec<u64>,
}

impl RunConfig {
    fn steps(&self) -> i64 {
        self.history_hours * NS_PER_HOUR / (self.scrape_secs * NS_PER_SEC)
    }

    fn steps_per_flush(&self) -> i64 {
        self.flush_hours * NS_PER_HOUR / (self.scrape_secs * NS_PER_SEC)
    }

    fn validate(&self) -> Result<(), ListingChargeError> {
        let fail = |m: String| Err(ListingChargeError::Setup(m));
        if self.series == 0 || self.scrape_secs <= 0 {
            return fail("--series and --scrape-secs must be positive".to_string());
        }
        if self.flush_hours <= 0 || self.history_hours < 2 * self.flush_hours {
            return fail(format!(
                "--history-hours ({}) must cover at least two flushes of --flush-hours ({}), \
                 or the compactor merges no runs and writes no provenance",
                self.history_hours, self.flush_hours
            ));
        }
        if self.history_hours % self.flush_hours != 0 {
            return fail(format!(
                "--history-hours ({}) must be a multiple of --flush-hours ({})",
                self.history_hours, self.flush_hours
            ));
        }
        if self.caps.is_empty() || self.caps.contains(&0) {
            return fail("--caps must name at least one positive cap".to_string());
        }
        Ok(())
    }
}

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

/// The clock compaction and listing run at: past the sealing horizon.
fn now_ns() -> i64 {
    hour_start() + 3 * NS_PER_HOUR
}

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

/// The tenant hash every object of the bucket is written under.
pub fn tenant_hash() -> TenantHash {
    tenant().hash()
}

/// The event-time window that holds every sample of `config`'s history.
pub fn history_window(config: &RunConfig) -> TimeRange {
    TimeRange {
        start_ns: hour_start() - config.history_hours * NS_PER_HOUR,
        end_ns: hour_start(),
    }
}

/// What series `index` of the bucket writes.
#[derive(Debug, Clone, Copy)]
enum Role {
    Gauge,
    Counter,
    /// Bucket `0..8`, then `_sum` and `_count`, of classic histogram `group`.
    Classic {
        group: usize,
        slot: usize,
    },
    Native,
    BuildInfo,
}

struct SeriesSpec {
    id: SeriesId,
    labels: LabelSet,
    role: Role,
    /// The per-series coordinate value derivations mix in.
    key: u64,
}

fn label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

/// The bucket's series, split by the `history` manifest's family shares:
/// 400 permille CPU gauge, 300 counter, 150 classic histogram, 100 native
/// histogram, 50 build-info gauge. Classic histograms take whole groups of
/// [`CLASSIC_SERIES`]; the remainder goes to the CPU gauge.
fn series_specs(count: usize) -> Result<Vec<SeriesSpec>, ListingChargeError> {
    let counters = count * 300 / 1000;
    let classic_groups = count * 150 / 1000 / CLASSIC_SERIES;
    let natives = count * 100 / 1000;
    let build_info = count * 50 / 1000;
    let gauges =
        count.saturating_sub(counters + classic_groups * CLASSIC_SERIES + natives + build_info);
    let mut roles = Vec::with_capacity(count);
    roles.extend((0..gauges).map(|_| Role::Gauge));
    roles.extend((0..counters).map(|_| Role::Counter));
    for group in 0..classic_groups {
        roles.extend((0..CLASSIC_SERIES).map(|slot| Role::Classic { group, slot }));
    }
    roles.extend((0..natives).map(|_| Role::Native));
    roles.extend((0..build_info).map(|_| Role::BuildInfo));
    let tenant = tenant();
    roles
        .into_iter()
        .enumerate()
        .map(|(i, role)| {
            let instance = format!("metricsbench-instance-{i}");
            let job = JOBS[i % JOBS.len()];
            let (name, mut labels) = match role {
                Role::Gauge => (
                    "metricsbench_gauge_cpu_percent".to_string(),
                    vec![label("region", REGIONS[i % REGIONS.len()])],
                ),
                Role::Counter => (
                    "metricsbench_requests_total".to_string(),
                    vec![
                        label("method", METHODS[i % METHODS.len()]),
                        label("status", STATUSES[(i / METHODS.len()) % STATUSES.len()]),
                    ],
                ),
                Role::Classic { group, slot } => {
                    let base = "metricsbench_request_duration_seconds";
                    let instance = format!("metricsbench-instance-h{group}");
                    match CLASSIC_BOUNDS.get(slot) {
                        Some(le) => (
                            format!("{base}_bucket"),
                            vec![label("instance", &instance), label("le", le)],
                        ),
                        None if slot == CLASSIC_BOUNDS.len() => {
                            (format!("{base}_sum"), vec![label("instance", &instance)])
                        }
                        None => (format!("{base}_count"), vec![label("instance", &instance)]),
                    }
                }
                Role::Native => ("metricsbench_latency_native".to_string(), Vec::new()),
                Role::BuildInfo => (
                    "metricsbench_build_info".to_string(),
                    vec![label("version", VERSIONS[i % VERSIONS.len()])],
                ),
            };
            if !matches!(role, Role::Classic { .. }) {
                labels.push(label("instance", &instance));
            }
            labels.push(label("__name__", &name));
            labels.push(label("job", job));
            labels.sort_by(|a, b| a.name.cmp(&b.name));
            let labels = LabelSet::new(labels).map_err(|e| setup("label set", e))?;
            let id =
                SeriesId::compute(&tenant, &name, &labels).map_err(|e| setup("series id", e))?;
            let key = match role {
                Role::Classic { group, .. } => 1_000_000 + group as u64,
                _ => i as u64,
            };
            Ok(SeriesSpec {
                id,
                labels,
                role,
                key,
            })
        })
        .collect()
}

/// Observations a counter-like series `key` has seen by step `k`: a per-series
/// rate plus a jitter below it, so the value never decreases.
fn observations(key: u64, k: i64) -> u64 {
    let k = k.unsigned_abs();
    let rate = 1 + mix(SEED, &[key, 0xA1]) % 40;
    k * rate + mix(SEED, &[key, k, 0xA2]) % rate
}

fn scalar_value(spec: &SeriesSpec, k: i64) -> f64 {
    match spec.role {
        Role::Gauge => unit_scaled(mix(SEED, &[spec.key, k.unsigned_abs(), 0xB1]), 100, 8),
        Role::Counter => observations(spec.key, k) as f64,
        Role::Classic { slot, .. } => {
            let count = observations(spec.key, k);
            match CLASSIC_SHARES.get(slot) {
                Some(share) => (count as f64 * share).floor(),
                // `_sum`: a mean of 0.0625 s per observation, exactly representable.
                None if slot == CLASSIC_BOUNDS.len() => count as f64 * 0.0625,
                None => count as f64,
            }
        }
        Role::BuildInfo => 1.0,
        Role::Native => 0.0,
    }
}

fn native_value(spec: &SeriesSpec, k: i64) -> HistogramValue {
    let count = observations(spec.key, k);
    let positive: Vec<u64> = (0..NATIVE_BUCKETS)
        .map(|j| count * u64::from(j + 1) / 36)
        .collect();
    HistogramValue {
        scale: NATIVE_SCHEMA,
        zero_threshold: 0.0,
        sum: Some(count as f64 * 0.0625),
        custom_values: None,
        positive_spans: vec![HistogramSpan {
            offset: -4,
            length: NATIVE_BUCKETS,
        }],
        negative_spans: Vec::new(),
        counts: HistogramCounts::Int {
            zero_count: 0,
            count: positive.iter().sum(),
            positive,
            negative: Vec::new(),
        },
        reset_hint: ResetHint::Unknown,
    }
}

/// One L0 flush: its data object and commit record, ready to PUT.
#[derive(Clone)]
pub struct L0Object {
    data_key: String,
    data: Bytes,
    commit_key: String,
    commit: Bytes,
}

/// The L0 flushes of one shard bucket, kept so every cap compacts the same
/// inputs from a fresh store.
pub struct L0Bucket {
    objects: Vec<L0Object>,
    pub l0_bytes: u64,
    pub samples: u64,
    pub series: usize,
}

/// Writes `config`'s history as L0 flushes through `SegmentWriter`.
pub fn build_bucket(config: &RunConfig) -> Result<L0Bucket, ListingChargeError> {
    config.validate()?;
    let specs = series_specs(config.series)?;
    let steps = config.steps();
    let per_flush = config.steps_per_flush();
    let origin = hour_start() - steps * config.scrape_secs * NS_PER_SEC;
    let interval = config.scrape_secs * NS_PER_SEC;
    let tenant_hash = tenant_hash();
    let mut objects = Vec::new();
    let mut l0_bytes = 0u64;
    let mut samples = 0u64;
    for (flush, start) in (0..steps).step_by(per_flush as usize).enumerate() {
        let seq = flush as u64 + 1;
        let ks = start..(start + per_flush).min(steps);
        let inputs: Vec<SeriesInputV3> = specs
            .iter()
            .map(|spec| {
                let values = match spec.role {
                    Role::Native => SeriesValues::Histogram(
                        ks.clone()
                            .map(|k| HistogramSample {
                                ts_ns: origin + k * interval,
                                value: native_value(spec, k),
                            })
                            .collect(),
                    ),
                    _ => SeriesValues::Scalar(
                        ks.clone()
                            .map(|k| Sample {
                                ts_ns: origin + k * interval,
                                value: scalar_value(spec, k),
                            })
                            .collect(),
                    ),
                };
                SeriesInputV3 {
                    series_id: spec.id,
                    labels: spec.labels.clone(),
                    values,
                }
            })
            .collect();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: seq,
        };
        let created = hour_start() + seq as i64;
        let bounds = IngestBounds {
            min_ingest_ts_ns: hour_start(),
            max_ingest_ts_ns: created,
        };
        let written = SegmentWriter::write_histograms(inputs, identity, bounds)
            .map_err(|e| setup("write L0", e))?;
        let rec = record::build(NewCommitRecord {
            tenant_hash,
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
            max_ingest_ts_ns: created,
            segment_format_version: u32::from(VERSION_V7),
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .map_err(|e| setup("commit record", e))?;
        let commit_key = commit_key_for_record(&rec).map_err(|e| setup("commit key", e))?;
        l0_bytes += written.bytes.len() as u64;
        samples += written.summary.sample_count;
        objects.push(L0Object {
            data_key: rec.object_key.clone(),
            data: written.bytes,
            commit_key,
            commit: record::encode(&rec),
        });
    }
    Ok(L0Bucket {
        objects,
        l0_bytes,
        samples,
        series: specs.len(),
    })
}

/// The bucket's L1 parts after one compaction at one cap.
pub struct Compacted {
    pub store: Arc<MemoryStore>,
    /// Every part, in part order.
    pub parts: Vec<SegmentRef>,
}

/// PUTs `bucket` onto a fresh store and compacts it with `max_l1_part_bytes`
/// set to `cap`, then resolves the bucket's parts.
pub async fn compact_at(
    bucket: &L0Bucket,
    config: &RunConfig,
    cap: u64,
) -> Result<Compacted, ListingChargeError> {
    let store = Arc::new(MemoryStore::new());
    for object in &bucket.objects {
        for (key, bytes) in [
            (&object.data_key, &object.data),
            (&object.commit_key, &object.commit),
        ] {
            store
                .put(key, bytes.clone(), PutOptions::create_if_absent())
                .await
                .map_err(|e| setup("put L0", e))?;
        }
    }
    let compactor = CompactorConfig {
        max_l1_part_bytes: cap,
        ..CompactorConfig::default()
    };
    let outcome = compact_bucket(
        store.as_ref(),
        &FixedClock::new(now_ns()),
        &compactor,
        &Bucket::new(tenant_hash(), Signal::Metrics, 0, HOUR),
    )
    .await
    .map_err(|e| setup("compact_bucket", e))?;
    match outcome {
        CompactionOutcome::Compacted {
            publish: PublishOutcome::Published,
            ..
        } => {}
        other => {
            return Err(ListingChargeError::Setup(format!(
                "cap {cap}: expected the bucket to compact and publish, got {other:?}"
            )));
        }
    }
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog =
        Catalog::new(backend, CatalogConfig::default()).map_err(|e| setup("catalog", e))?;
    let snapshot = catalog
        .resolve(
            &tenant_hash(),
            Signal::Metrics,
            history_window(config),
            &[],
            now_ns(),
        )
        .await
        .map_err(|e| setup("resolve", e))?;
    let mut parts = Vec::new();
    for segment in snapshot.segments {
        match segment.level {
            SegmentLevel::L1 { part_index, .. } => parts.push((part_index, segment)),
            _ => {
                return Err(ListingChargeError::Setup(format!(
                    "cap {cap}: {} resolved as an L0 object after compaction",
                    segment.data_object_key
                )));
            }
        }
    }
    parts.sort_by_key(|(index, _)| *index);
    Ok(Compacted {
        store,
        parts: parts.into_iter().map(|(_, s)| s).collect(),
    })
}

/// The matcher that selects every series of the bucket.
pub fn select_all() -> Result<Vec<LabelMatcher>, ListingChargeError> {
    Ok(vec![
        LabelMatcher::regex("__name__", ".+").map_err(|e| setup("matcher", e))?,
    ])
}

/// What one part holds, read from the object itself.
#[derive(Debug, Clone, Copy)]
pub struct PartShape {
    pub wire_bytes: u64,
    /// The decoded length of the catalog sections `SegmentFetcher` reserves
    /// before a catalog decode, each clamped to the reader's section ceiling.
    pub catalog_decode_bytes: u64,
    /// An upper bound on the wire bytes the listing's GETs hold while the
    /// catalog decodes ([`held_fetch_bound`]).
    pub held_fetch_bytes: u64,
    pub samples: u64,
    pub series: u64,
}

/// Reads `part`'s footer and figures. Refuses a part without a whole
/// SERIES_META section: a chunked catalog's decode length is read from its
/// SERIES_IDX directory, which this bench does not decode.
pub async fn part_shape(
    store: &MemoryStore,
    part: &SegmentRef,
) -> Result<PartShape, ListingChargeError> {
    let object = store
        .get(&part.data_object_key, GetRange::Full)
        .await
        .map_err(|e| setup("get part", e))?
        .data;
    let limits = ReaderLimits::default();
    let location = open_from_full(&object, limits).map_err(|e| setup("part footer", e))?;
    let footer = location.footer;
    if !footer
        .sections
        .iter()
        .any(|s| s.kind == SECTION_SERIES_META)
    {
        return Err(ListingChargeError::Setup(format!(
            "{} carries a chunked catalog (no SERIES_META); lower --series below the \
             chunking threshold",
            part.data_object_key
        )));
    }
    let catalog_decode_bytes = footer
        .sections
        .iter()
        .filter(|s| {
            matches!(
                s.kind,
                SECTION_LABEL_DICT
                    | SECTION_SERIES_IDS
                    | SECTION_SERIES_META
                    | SECTION_SERIES_IDX
                    | SECTION_SERIES_META_CHUNKS
            )
        })
        .map(|s| {
            ravel_memory::decoded_charge(s.uncompressed_len, limits.max_section_uncompressed_bytes)
        })
        .fold(0u64, u64::saturating_add);
    let wire_bytes = object.len() as u64;
    let catalog_sections: Vec<(u64, u64)> = footer
        .sections
        .iter()
        .filter(|s| {
            matches!(
                s.kind,
                SECTION_LABEL_DICT | SECTION_SERIES_IDS | SECTION_SERIES_META
            )
        })
        .map(|s| (s.offset, s.offset.saturating_add(s.len)))
        .collect();
    Ok(PartShape {
        wire_bytes,
        catalog_decode_bytes,
        held_fetch_bytes: held_fetch_bound(wire_bytes, location.footer_offset, &catalog_sections),
        samples: part.sample_count,
        series: part.series_count,
    })
}

/// An upper bound on the wire bytes `SegmentFetcher` holds reserved while it
/// decodes a whole-SERIES_META catalog, from the object's layout and the
/// fetcher's default read shape. `open_segment` reads an object of at most
/// [`DEFAULT_WHOLE_OBJECT_THRESHOLD`] whole, and otherwise its last
/// [`DEFAULT_SUFFIX_LEN`] bytes plus the footer when the footer starts before
/// that tail. `decode_selected` then fetches LABEL_DICT, SERIES_IDS and
/// SERIES_META as one coalesced run, which lies inside the span from the first
/// section's start to the last one's end (`catalog_sections`, as
/// `(start, end)`). Both stay held until the decode returns.
pub fn held_fetch_bound(
    object_size: u64,
    footer_offset: u64,
    catalog_sections: &[(u64, u64)],
) -> u64 {
    if object_size <= DEFAULT_WHOLE_OBJECT_THRESHOLD {
        return object_size;
    }
    let tail = DEFAULT_SUFFIX_LEN.min(object_size);
    let chase = object_size
        .saturating_sub(footer_offset)
        .saturating_sub(tail);
    let start = catalog_sections.iter().map(|r| r.0).min().unwrap_or(0);
    let end = catalog_sections.iter().map(|r| r.1).max().unwrap_or(0);
    tail.saturating_add(chase)
        .saturating_add(end.saturating_sub(start))
}

/// The upper bound issue #4393 states for a listing's charge over one part:
/// `(64 + 32) * samples` of provenance plus the catalog decode length.
pub fn upper_bound(shape: &PartShape) -> u64 {
    (PROVENANCE_COLUMN_BYTES_PER_SAMPLE + SAMPLE_PROVENANCE_BYTES)
        .saturating_mul(shape.samples)
        .saturating_add(shape.catalog_decode_bytes)
}

/// [`upper_bound`] plus the wire bytes the listing's GETs hold
/// ([`held_fetch_bound`]), which that formula leaves out.
pub fn listing_bound(shape: &PartShape) -> u64 {
    upper_bound(shape).saturating_add(shape.held_fetch_bytes)
}

/// Lists `part` with every series selected under a fresh budget of `limit`
/// bytes. `Ok(Some(series))` on success, `Ok(None)` when the budget refused,
/// an error for anything else.
pub async fn fetcher_listing(
    store: &Arc<MemoryStore>,
    part: &SegmentRef,
    limit: u64,
) -> Result<Option<usize>, ListingChargeError> {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let fetcher =
        SegmentFetcher::new(backend).with_memory_budget(Arc::new(MemoryBudget::new(limit)));
    match fetcher
        .fetch_series(tenant_hash(), part, &select_all()?)
        .await
    {
        Ok(entries) => Ok(Some(entries.len())),
        Err(FetchError::FetchMemoryExhausted { .. }) => Ok(None),
        Err(e) => Err(ListingChargeError::Listing(e.to_string())),
    }
}

/// Lists every series of the bucket through `QueryEngine::resolve_series`,
/// the method the metrics discovery endpoints call, under a fresh budget of
/// `limit` bytes. Its per-object read is
/// `SegmentFetcher::list_series_phase_accounted`. `Ok(Some(series))` on
/// success, `Ok(None)` when the budget refused.
pub async fn engine_listing_succeeds(
    store: &Arc<MemoryStore>,
    config: &RunConfig,
    limit: u64,
) -> Result<Option<usize>, ListingChargeError> {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = Catalog::new(Arc::clone(&backend), CatalogConfig::default())
        .map_err(|e| setup("catalog", e))?;
    let engine_config = EngineConfig {
        promql_fetch_fanout: Some(1),
        ..EngineConfig::default()
    };
    let engine = QueryEngine::new(Arc::new(catalog), backend, engine_config)
        .with_memory_budget(Arc::new(MemoryBudget::new(limit)));
    match engine
        .resolve_series(
            tenant_hash(),
            &select_all()?,
            history_window(config),
            &[],
            now_ns(),
            std::time::Duration::from_secs(600),
        )
        .await
    {
        Ok((series, _stats)) => Ok(Some(series.len())),
        Err(QueryError::Fetch(FetchError::FetchMemoryExhausted { .. })) => Ok(None),
        Err(e) => Err(ListingChargeError::Listing(e.to_string())),
    }
}

/// A binary search's answer: the smallest limit found to succeed, and the
/// largest found to be refused, within 1 % of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchOutcome {
    pub succeeds_at: u64,
    pub refused_at: u64,
    pub probes: u32,
}

/// The smallest budget limit at which `listing` succeeds, to a 1 %
/// resolution: on return `succeeds_at` succeeded, `refused_at` was refused (or
/// is zero, never tried), and `refused_at` is at least 99 % of `succeeds_at`.
/// `start` is the first limit tried; the search doubles it until it succeeds,
/// then bisects between the largest refused limit and the smallest succeeding
/// one.
pub async fn minimal_budget<F, Fut>(
    start: u64,
    mut listing: F,
) -> Result<SearchOutcome, ListingChargeError>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<bool, ListingChargeError>>,
{
    let mut probes = 0u32;
    let mut lo = 0u64;
    let mut hi = start.max(1);
    loop {
        probes += 1;
        if listing(hi).await? {
            break;
        }
        lo = hi;
        hi = hi.checked_mul(2).ok_or_else(|| {
            ListingChargeError::Setup("no budget limit up to u64::MAX succeeds".to_string())
        })?;
    }
    while hi - lo > hi / 100 {
        let mid = lo + (hi - lo) / 2;
        probes += 1;
        if listing(mid).await? {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    Ok(SearchOutcome {
        succeeds_at: hi,
        refused_at: lo,
        probes,
    })
}

/// One cap's measured figures.
#[derive(Debug, Clone)]
pub struct CapFigures {
    pub cap_bytes: u64,
    pub parts: usize,
    pub full_parts: usize,
    pub part_index: usize,
    pub shape: PartShape,
    pub search: SearchOutcome,
    pub upper_bound_bytes: u64,
    pub listing_bound_bytes: u64,
    /// Whether a listing under a budget of exactly `listing_bound_bytes`
    /// succeeded.
    pub listing_bound_succeeds: bool,
}

impl CapFigures {
    pub fn peak_per_sample(&self) -> f64 {
        self.search.succeeds_at as f64 / self.shape.samples.max(1) as f64
    }

    pub fn upper_bound_per_sample(&self) -> f64 {
        self.upper_bound_bytes as f64 / self.shape.samples.max(1) as f64
    }

    /// How far the peak lies above [`upper_bound`]; negative below it.
    pub fn peak_minus_upper_bound(&self) -> i64 {
        let diff = i128::from(self.search.succeeds_at) - i128::from(self.upper_bound_bytes);
        i64::try_from(diff).unwrap_or(if diff < 0 { i64::MIN } else { i64::MAX })
    }
}

/// Compacts `bucket` at `cap` and measures the listing charge of its first
/// part.
pub async fn measure_cap(
    bucket: &L0Bucket,
    config: &RunConfig,
    cap: u64,
) -> Result<CapFigures, ListingChargeError> {
    let compacted = compact_at(bucket, config, cap).await?;
    let full_parts = compacted.parts.len().saturating_sub(1);
    if full_parts < MIN_FULL_PARTS {
        return Err(ListingChargeError::Setup(format!(
            "cap {cap} closed {full_parts} full parts of {} total, fewer than {MIN_FULL_PARTS}: \
             raise --history-hours",
            compacted.parts.len()
        )));
    }
    let part_index = 0;
    let part = &compacted.parts[part_index];
    let shape = part_shape(&compacted.store, part).await?;
    let upper = upper_bound(&shape);
    let bound = listing_bound(&shape);
    let store = &compacted.store;
    let search = minimal_budget(upper, |limit| {
        listing_succeeds(store, part, shape.series, limit)
    })
    .await?;
    let listing_bound_succeeds = listing_succeeds(store, part, shape.series, bound).await?;
    Ok(CapFigures {
        cap_bytes: cap,
        parts: compacted.parts.len(),
        full_parts,
        part_index,
        shape,
        search,
        upper_bound_bytes: upper,
        listing_bound_bytes: bound,
        listing_bound_succeeds,
    })
}

/// Whether listing `part` under a budget of `limit` succeeds, returning all
/// `series` it holds. A listing that returns any other count is an error.
pub async fn listing_succeeds(
    store: &Arc<MemoryStore>,
    part: &SegmentRef,
    series: u64,
    limit: u64,
) -> Result<bool, ListingChargeError> {
    match fetcher_listing(store, part, limit).await? {
        Some(listed) if listed as u64 == series => Ok(true),
        Some(listed) => Err(ListingChargeError::Listing(format!(
            "listed {listed} series of a part holding {series}"
        ))),
        None => Ok(false),
    }
}

/// A whole run: the bucket and every cap's figures, in cap order.
pub struct RunReport {
    pub config: RunConfig,
    pub l0_objects: usize,
    pub l0_bytes: u64,
    pub l0_samples: u64,
    pub series: usize,
    pub caps: Vec<CapFigures>,
}

/// Builds the bucket once and measures every cap of `config`, ascending.
pub async fn run(config: RunConfig) -> Result<RunReport, ListingChargeError> {
    let mut config = config;
    config.caps.sort_unstable();
    config.caps.dedup();
    let bucket = build_bucket(&config)?;
    let mut caps = Vec::with_capacity(config.caps.len());
    for &cap in &config.caps {
        caps.push(measure_cap(&bucket, &config, cap).await?);
    }
    Ok(RunReport {
        l0_objects: bucket.objects.len(),
        l0_bytes: bucket.l0_bytes,
        l0_samples: bucket.samples,
        series: bucket.series,
        config,
        caps,
    })
}

/// The report as one JSON document.
pub fn report_json(report: &RunReport) -> Value {
    let caps: Vec<Value> = report
        .caps
        .iter()
        .map(|c| {
            json!({
                "cap_bytes": c.cap_bytes,
                "parts": c.parts,
                "full_parts": c.full_parts,
                "part_index": c.part_index,
                "part_wire_bytes": c.shape.wire_bytes,
                "catalog_decode_bytes": c.shape.catalog_decode_bytes,
                "held_fetch_bytes": c.shape.held_fetch_bytes,
                "samples": c.shape.samples,
                "series": c.shape.series,
                "peak_charge_bytes": c.search.succeeds_at,
                "refused_at_bytes": c.search.refused_at,
                "search_probes": c.search.probes,
                "peak_per_sample_bytes": c.peak_per_sample(),
                "upper_bound_bytes": c.upper_bound_bytes,
                "upper_bound_per_sample_bytes": c.upper_bound_per_sample(),
                "peak_minus_upper_bound_bytes": c.peak_minus_upper_bound(),
                "listing_bound_bytes": c.listing_bound_bytes,
                "listing_bound_succeeds": c.listing_bound_succeeds,
            })
        })
        .collect();
    json!({
        "bench": "l1_listing_charge",
        "issue": 4393,
        "listing_call": "SegmentFetcher::fetch_series, matcher __name__=~\".+\"",
        "resolution": "1 % of the peak",
        "upper_bound_formula": "(64 + 32) * samples + catalog_decode_bytes",
        "listing_bound_formula": "upper_bound_bytes + held_fetch_bytes",
        "bucket": {
            "bucket_series": report.series,
            "scrape_secs": report.config.scrape_secs,
            "history_hours": report.config.history_hours,
            "flush_hours": report.config.flush_hours,
            "l0_objects": report.l0_objects,
            "l0_bytes": report.l0_bytes,
            "l0_samples": report.l0_samples,
        },
        "bands": {
            "peak_per_sample_band_bytes": [PEAK_PER_SAMPLE_BAND.0, PEAK_PER_SAMPLE_BAND.1],
            "peak_at_or_below_listing_bound": true,
            "peak_non_decreasing_with_samples": true,
        },
        "caps": caps,
    })
}

/// A human table of the report.
pub fn report_table(report: &RunReport) -> String {
    let mut out = format!(
        "l1_listing_charge: {} series, {} h history in {} L0 flushes ({} samples, {} B)\n\
         {:>10} {:>5} {:>11} {:>11} {:>9} {:>11} {:>7} {:>12} {:>9} {:>12} {:>9} {:>10}\n",
        report.series,
        report.config.history_hours,
        report.l0_objects,
        report.l0_samples,
        report.l0_bytes,
        "cap",
        "parts",
        "wire B",
        "catalog B",
        "fetch B",
        "samples",
        "series",
        "peak B",
        "peak/smp",
        "bound B",
        "bound/smp",
        "peak-bound",
    );
    for c in &report.caps {
        out.push_str(&format!(
            "{:>10} {:>5} {:>11} {:>11} {:>9} {:>11} {:>7} {:>12} {:>9.2} {:>12} {:>9.2} {:>10}\n",
            c.cap_bytes,
            c.parts,
            c.shape.wire_bytes,
            c.shape.catalog_decode_bytes,
            c.shape.held_fetch_bytes,
            c.shape.samples,
            c.shape.series,
            c.search.succeeds_at,
            c.peak_per_sample(),
            c.upper_bound_bytes,
            c.upper_bound_per_sample(),
            c.peak_minus_upper_bound(),
        ));
    }
    out
}

/// Checks the rendered report `text` against the bands for exactly `caps`.
/// Every cap appears once, every figure of [`CAP_FIGURES`] and
/// `listing_bound_succeeds` appears once per cap in the text, and each is
/// inside its band:
///
/// - peak per sample in [`PEAK_PER_SAMPLE_BAND`];
/// - the largest refused limit below the peak and within 1 % of it;
/// - the peak at or below [`listing_bound`]: a listing under exactly that
///   limit succeeded, and no limit the search refused reaches it;
/// - the peak non-decreasing as samples per part grow.
///
/// Returns every violation.
pub fn check_report(text: &str, caps: &[u64]) -> Result<(), Vec<String>> {
    let mut failures = Vec::new();
    let doc: Value = match serde_json::from_str(text) {
        Ok(doc) => doc,
        Err(e) => return Err(vec![format!("report is not JSON: {e}")]),
    };
    let entries = doc
        .get("caps")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    // A key repeated inside one object survives `from_str` as its last
    // value, so the raw text is counted too.
    for figure in CAP_FIGURES
        .iter()
        .chain(["cap_bytes", "listing_bound_succeeds"].iter())
    {
        let seen = text.matches(&format!("\"{figure}\":")).count();
        if seen != caps.len() {
            failures.push(format!(
                "{figure}: emitted {seen} times for {} caps",
                caps.len()
            ));
        }
    }
    let mut rows = Vec::new();
    for &cap in caps {
        let matching: Vec<&Value> = entries
            .iter()
            .filter(|e| e.get("cap_bytes").and_then(Value::as_u64) == Some(cap))
            .collect();
        let [entry] = matching.as_slice() else {
            failures.push(format!(
                "cap {cap}: {} entries, expected exactly one",
                matching.len()
            ));
            continue;
        };
        let figure = |name: &str| entry.get(name).and_then(Value::as_f64);
        let mut missing = false;
        for name in CAP_FIGURES {
            if figure(name).is_none() {
                failures.push(format!("cap {cap}: {name} missing"));
                missing = true;
            }
        }
        if missing {
            continue;
        }
        let per_sample = figure("peak_per_sample_bytes").unwrap_or(f64::NAN);
        let (lo, hi) = PEAK_PER_SAMPLE_BAND;
        if !(lo..=hi).contains(&per_sample) {
            failures.push(format!(
                "cap {cap}: peak per sample {per_sample} B outside [{lo}, {hi}]"
            ));
        }
        if entry.get("listing_bound_succeeds").and_then(Value::as_bool) != Some(true) {
            failures.push(format!(
                "cap {cap}: the listing does not succeed under the listing bound"
            ));
        }
        let peak = entry.get("peak_charge_bytes").and_then(Value::as_u64);
        let refused = entry.get("refused_at_bytes").and_then(Value::as_u64);
        let bound = entry.get("listing_bound_bytes").and_then(Value::as_u64);
        let samples = entry.get("samples").and_then(Value::as_u64);
        match (peak, refused, bound, samples) {
            (Some(peak), Some(refused), Some(bound), Some(samples)) => {
                if refused >= peak || peak - refused > peak / 100 {
                    failures.push(format!(
                        "cap {cap}: refused at {refused} B and succeeded at {peak} B is not a \
                         1 % bracket"
                    ));
                }
                if refused >= bound {
                    failures.push(format!(
                        "cap {cap}: a {refused} B budget was refused, at or above the listing \
                         bound {bound} B"
                    ));
                }
                rows.push((cap, samples, peak));
            }
            _ => failures.push(format!("cap {cap}: a byte figure is not an integer")),
        }
    }
    rows.sort_by_key(|&(_, samples, _)| samples);
    for pair in rows.windows(2) {
        let ((cap_a, samples_a, peak_a), (cap_b, samples_b, peak_b)) = (pair[0], pair[1]);
        if peak_b < peak_a {
            failures.push(format!(
                "peak fell as samples per part grew: cap {cap_a} ({samples_a} samples) peaks at \
                 {peak_a} B, cap {cap_b} ({samples_b} samples) at {peak_b} B"
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

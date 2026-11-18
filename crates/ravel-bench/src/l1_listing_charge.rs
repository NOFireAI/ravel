//! The memory-budget charge of reading one L1 metrics part's catalog, as a
//! function of part size (issues #4393 and #4433), for the `l1_listing_charge`
//! bin. It measures two paths over the same part, each with its own band:
//!
//! - **Listing**: the series listing the metrics discovery endpoints run,
//!   `QueryEngine::resolve_series`, whose per-object read
//!   (`SegmentFetcher::list_series_phase_accounted`) skips the per-sample
//!   provenance extension (`docs/query-engine.md`, "Per-sample provenance").
//!   Its charge is the wire bytes its GETs hold plus the catalog sections'
//!   decoded length ([`listing_charge`]); what grows with samples is stored
//!   bytes only.
//! - **Sample read**: the catalog decode every sample path runs,
//!   `SegmentFetcher::fetch_series`, which decodes and charges up to
//!   [`PROVENANCE_COLUMN_BYTES_PER_SAMPLE`] of provenance columns plus a
//!   [`SAMPLE_PROVENANCE_BYTES`] `SampleProvenance` for every sample in the
//!   part, whatever the matchers select, on top of the listing's charge. That
//!   grows with the samples a part holds, so the part cap
//!   (`--max-l1-part-bytes`) sets the budget a sample read needs.
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
//!    test pins a sample-read peak above 96 bytes per sample, which only a
//!    part carrying provenance for every sample charges). The generator's
//!    anomalies (missing, stale, reset, out-of-order) are not injected;
//! 2. compacts the bucket with `ravel_maintain::compact_bucket` at each
//!    `max_l1_part_bytes` cap, from a fresh store holding the same L0 objects,
//!    and refuses a cap that closed fewer than [`MIN_FULL_PARTS`] full parts
//!    (every part but the tail);
//! 3. measures the first part on both paths with a matcher selecting every
//!    series under a fresh `MemoryBudget`, binary-searching the smallest
//!    budget limit at which each succeeds to a 1 % resolution
//!    ([`minimal_budget`]). A refusal is `FetchError::FetchMemoryExhausted`;
//!    any other error ends the run.
//!
//! `resolve_series` lists every object a resolve returns and reserves each
//! series it keeps for the rest of the call, so over a bucket of many parts
//! its peak would add the series of the parts listed before the largest one.
//! To measure one part, the listing runs over a store holding only that part:
//! [`isolate_part`] writes the bucket again with only the part's series and
//! compacts it into one part, and refuses a part whose content hash differs
//! from the measured one. The sample read calls `fetch_series` on the part
//! directly.
//!
//! Per cap the report carries the part's wire bytes, its catalog sections'
//! decoded length (the figure `SegmentFetcher` reserves before the decode),
//! the wire bytes the GETs hold while the catalog decodes
//! ([`held_fetch_bound`]: the open GET, the whole object up to 512 KiB and the
//! 64 KiB footer tail above it, and the catalog sections' run), and its
//! samples and series. For each path it carries the minimal succeeding budget
//! (the peak, with the largest refused limit beside it) and that peak per
//! sample. The listing's entry adds [`listing_charge`], held fetch bytes plus
//! catalog decode length. The sample read's adds the upper bound issue #4393
//! states, `(64 + 32) * samples + catalog decode length` ([`upper_bound`]),
//! how far the peak lies above it, and [`sample_read_bound`], that formula
//! plus the held fetch bytes it leaves out. A sample read under exactly
//! [`upper_bound`] is refused (the smoke test pins it).
//!
//! Each search starts at its path's bound, so the peak it reports is at or
//! below that bound whenever a read under it succeeds. [`check_report`] holds
//! the bands: listing peak per sample in [`LISTING_PEAK_PER_SAMPLE_BAND`] and
//! sample-read peak per sample in [`SAMPLE_PEAK_PER_SAMPLE_BAND`], each peak
//! at or below its path's bound, and each path's peak non-decreasing as
//! samples per part grow across the caps. A figure outside its band, missing,
//! or emitted twice is a failure.
//!
//! Run it with
//! `cargo run --release -p ravel-bench --bin l1_listing_charge` (the four
//! default caps) or with `-- --caps 1048576,4194304`. One JSON document goes
//! to stdout and a table to stderr; the exit code is non-zero when a band
//! fails.

use std::collections::HashSet;
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

/// The band the listing's peak charge per sample must fall in, in bytes.
///
/// The listing decodes nothing per sample, so its peak is [`listing_charge`]:
/// the held fetch bytes plus the inflated catalog sections, which hold the
/// encoded provenance columns, about 1 B a sample. Measured 2026-11-18 with
/// this bin, each peak equal to [`listing_charge`] to the byte: 2.30 B a
/// sample at the 1 MiB cap and 1.25, 1.06 and 1.03 B at 4, 16 and 64 MiB (the
/// default caps, 2,500 series over 72 h), where the part is above the
/// whole-object threshold and the 64 to 76 KiB of held fetch bytes amortise
/// over more samples as the cap grows; and 11.98 and 8.91 B at the smoke
/// test's 64 and 256 KiB caps (120 series over 8 h), where the part is fetched
/// whole and its page bytes are held too. The floor catches a listing that
/// stops reserving the catalog decode: the held fetch bytes alone are 0.23,
/// 0.04 and 0.01 B a sample at 4, 16 and 64 MiB. The ceiling catches any
/// provenance decode charged again, 32 B a sample for the records alone and
/// 96 B with the columns; 16 B clears the measured 11.98 B.
pub const LISTING_PEAK_PER_SAMPLE_BAND: (f64, f64) = (1.0, 16.0);

/// The band the sample read's peak charge per sample must fall in, in bytes.
///
/// The sample read charges every sample's provenance in full:
/// [`PROVENANCE_COLUMN_BYTES_PER_SAMPLE`] (64 B) of decoded columns plus a
/// [`SAMPLE_PROVENANCE_BYTES`] (32 B) record, 96 B a sample, on top of the
/// catalog decode and the wire bytes the GETs hold. A sample read under exactly
/// [`upper_bound`] is refused, so with the provenance charged the peak is above
/// 96 B a sample, and the floor is that 96 B. Measured 2026-11-17 with this
/// bin: 98.30 B a sample at the
/// 1 MiB cap and 97.25, 97.06 and 97.03 B at 4, 16 and 64 MiB (the default
/// caps, 2,500 series over 72 h), and 107.98 and 104.91 B at the smoke test's
/// 64 and 256 KiB caps (120 series over 8 h), where the catalog decode and the
/// held fetch bytes amortise over fewer samples. Charging only the 32 B record
/// would drop those by 64 B, to 33.0 to 44.0 B, under the floor; charging no
/// provenance would leave the catalog and fetch bytes alone, at most 12 B. The
/// ceiling catches the provenance charged twice, which would add 96 B, to
/// 193.0 to 204.0 B; 130 B clears the measured 107.98 B and stays well below
/// that.
pub const SAMPLE_PEAK_PER_SAMPLE_BAND: (f64, f64) = (96.0, 130.0);

/// The figures every cap entry of the report carries, each exactly once.
pub const CAP_FIGURES: [&str; 16] = [
    "part_wire_bytes",
    "catalog_decode_bytes",
    "held_fetch_bytes",
    "samples",
    "series",
    "listing_peak_bytes",
    "listing_refused_at_bytes",
    "listing_peak_per_sample_bytes",
    "listing_charge_bytes",
    "sample_peak_bytes",
    "sample_refused_at_bytes",
    "sample_peak_per_sample_bytes",
    "sample_upper_bound_bytes",
    "sample_upper_bound_per_sample_bytes",
    "sample_peak_minus_upper_bound_bytes",
    "sample_read_bound_bytes",
];

/// The pass/fail fields every cap entry carries, each exactly once: whether
/// each path succeeded under a budget of exactly its bound.
pub const CAP_FLAGS: [&str; 2] = ["listing_charge_succeeds", "sample_read_bound_succeeds"];

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
    #[error("a listing or sample read failed with a non-budget error: {0}")]
    Read(String),
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
        // A flush shorter than one scrape step holds no step, so `build_bucket`'s
        // `step_by(steps_per_flush())` would panic on a zero step. This also
        // covers `steps()`, which `history_hours >= 2 * flush_hours` keeps at
        // least twice `steps_per_flush()`.
        if self.steps_per_flush() == 0 {
            return fail(format!(
                "--flush-hours ({}) is shorter than one --scrape-secs ({}) step, so a flush \
                 holds no scrape",
                self.flush_hours, self.scrape_secs
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
    build_bucket_of(config, None)
}

/// [`build_bucket`] restricted to the series in `keep`, when given: every kept
/// series writes exactly the samples it writes in the whole bucket, under the
/// same flushes and writer sequences.
pub fn build_bucket_of(
    config: &RunConfig,
    keep: Option<&HashSet<SeriesId>>,
) -> Result<L0Bucket, ListingChargeError> {
    config.validate()?;
    let mut specs = series_specs(config.series)?;
    if let Some(keep) = keep {
        specs.retain(|spec| keep.contains(&spec.id));
    }
    if specs.is_empty() {
        return Err(ListingChargeError::Setup(
            "the bucket keeps no series".to_string(),
        ));
    }
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
    /// The wire bytes either path's GETs hold while the catalog decodes
    /// ([`held_fetch_bound`]).
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

/// The wire bytes `SegmentFetcher` holds reserved while it decodes a
/// whole-SERIES_META catalog, from the object's layout and the fetcher's
/// default read shape. `open_segment` reads an object of at most
/// [`DEFAULT_WHOLE_OBJECT_THRESHOLD`] whole. A larger one it reads as its last
/// [`DEFAULT_SUFFIX_LEN`] bytes, plus a second GET from the footer's start to
/// the object's end when the footer starts before that tail. `decode_selected`
/// then fetches whichever of LABEL_DICT, SERIES_IDS and SERIES_META
/// (`catalog_sections`, as `(start, end)`) those reads do not already cover,
/// as one run from the first such section's start to the last one's end. All
/// of it stays held until the decode returns.
pub fn held_fetch_bound(
    object_size: u64,
    footer_offset: u64,
    catalog_sections: &[(u64, u64)],
) -> u64 {
    if object_size <= DEFAULT_WHOLE_OBJECT_THRESHOLD {
        return object_size;
    }
    let tail_start = object_size.saturating_sub(DEFAULT_SUFFIX_LEN);
    let (held_from, chase) = if footer_offset < tail_start {
        (footer_offset, object_size - footer_offset)
    } else {
        (tail_start, 0)
    };
    let missing = || {
        catalog_sections
            .iter()
            .filter(|&&(start, end)| !(held_from <= start && end <= object_size))
    };
    let run = match (missing().map(|r| r.0).min(), missing().map(|r| r.1).max()) {
        (Some(start), Some(end)) => end.saturating_sub(start),
        _ => 0,
    };
    (object_size - tail_start)
        .saturating_add(chase)
        .saturating_add(run)
}

/// The listing's charge over one part: the wire bytes its GETs hold
/// ([`held_fetch_bound`]) plus the catalog sections' decoded length, which the
/// fetcher reserves before the decode. The decode skips the per-sample
/// provenance extension, so nothing decoded per sample is added.
pub fn listing_charge(shape: &PartShape) -> u64 {
    shape
        .held_fetch_bytes
        .saturating_add(shape.catalog_decode_bytes)
}

/// The upper bound issue #4393 states for a sample read's charge over one
/// part: `(64 + 32) * samples` of provenance plus the catalog decode length.
pub fn upper_bound(shape: &PartShape) -> u64 {
    (PROVENANCE_COLUMN_BYTES_PER_SAMPLE + SAMPLE_PROVENANCE_BYTES)
        .saturating_mul(shape.samples)
        .saturating_add(shape.catalog_decode_bytes)
}

/// [`upper_bound`] plus the wire bytes the sample read's GETs hold
/// ([`held_fetch_bound`]), which that formula leaves out.
pub fn sample_read_bound(shape: &PartShape) -> u64 {
    upper_bound(shape).saturating_add(shape.held_fetch_bytes)
}

/// Reads `part`'s catalog through `SegmentFetcher::fetch_series`, the decode
/// that charges per-sample provenance, with every series selected under a
/// fresh budget of `limit` bytes. `Ok(Some(series))` on success, `Ok(None)`
/// when the budget refused, an error for anything else.
pub async fn fetcher_sample_read(
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
        Err(e) => Err(ListingChargeError::Read(e.to_string())),
    }
}

/// The series ids `part` holds, read with an unlimited budget.
pub async fn part_series(
    store: &Arc<MemoryStore>,
    part: &SegmentRef,
) -> Result<HashSet<SeriesId>, ListingChargeError> {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    SegmentFetcher::new(backend)
        .fetch_series(tenant_hash(), part, &select_all()?)
        .await
        .map(|entries| entries.into_iter().map(|e| e.series_id).collect())
        .map_err(|e| ListingChargeError::Read(e.to_string()))
}

/// A store whose catalog resolves exactly one object, byte for byte the part
/// `part_index` of `compacted`: `config`'s bucket written again with only that
/// part's series, then compacted at `cap`. The listing measures this store,
/// so its peak is that one part's, without the series `resolve_series` keeps
/// from the parts listed before it. Refuses a compaction that closes more
/// than one part, or one whose content hash is not the measured part's.
pub async fn isolate_part(
    config: &RunConfig,
    cap: u64,
    compacted: &Compacted,
    part_index: usize,
) -> Result<Compacted, ListingChargeError> {
    let part = compacted.parts.get(part_index).ok_or_else(|| {
        ListingChargeError::Setup(format!("cap {cap}: no part {part_index} to isolate"))
    })?;
    let keep = part_series(&compacted.store, part).await?;
    let only = build_bucket_of(config, Some(&keep))?;
    let isolated = compact_at(&only, config, cap).await?;
    match isolated.parts.as_slice() {
        [one] if one.content_hash == part.content_hash => Ok(isolated),
        parts => Err(ListingChargeError::Setup(format!(
            "cap {cap}: the part's {} series compacted alone into {} parts, not the \
             measured part {}",
            keep.len(),
            parts.len(),
            part.data_object_key
        ))),
    }
}

/// Lists every series of the bucket through `QueryEngine::resolve_series`,
/// the method the metrics discovery endpoints call, under a fresh budget of
/// `limit` bytes. Its per-object read is
/// `SegmentFetcher::list_series_phase_accounted`, which skips the per-sample
/// provenance extension. `Ok(Some(series))` on success, `Ok(None)` when the
/// budget refused.
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
        Err(e) => Err(ListingChargeError::Read(e.to_string())),
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

/// The smallest budget limit at which `read` succeeds, to a 1 %
/// resolution: on return `succeeds_at` succeeded, `refused_at` was refused (or
/// is zero, never tried), and `refused_at` is at least 99 % of `succeeds_at`.
/// `start` is the first limit tried; the search doubles it until it succeeds,
/// then bisects between the largest refused limit and the smallest succeeding
/// one.
pub async fn minimal_budget<F, Fut>(
    start: u64,
    mut read: F,
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
        if read(hi).await? {
            break;
        }
        lo = hi;
        hi = hi.checked_mul(2).ok_or_else(|| {
            ListingChargeError::Setup("no budget limit up to u64::MAX succeeds".to_string())
        })?;
    }
    // The 1 % step floors to zero once `hi` drops below 100 bytes, which would
    // leave `mid == lo` and loop forever; keep it at least one byte.
    while hi - lo > (hi / 100).max(1) {
        let mid = lo + (hi - lo) / 2;
        probes += 1;
        if read(mid).await? {
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
    /// The listing's search, through `QueryEngine::resolve_series`.
    pub listing_search: SearchOutcome,
    /// [`listing_charge`] of the part.
    pub listing_charge_bytes: u64,
    /// Whether a listing under a budget of exactly `listing_charge_bytes`
    /// succeeded.
    pub listing_charge_succeeds: bool,
    /// The sample read's search, through `SegmentFetcher::fetch_series`.
    pub sample_search: SearchOutcome,
    pub upper_bound_bytes: u64,
    /// [`sample_read_bound`] of the part.
    pub sample_read_bound_bytes: u64,
    /// Whether a sample read under a budget of exactly
    /// `sample_read_bound_bytes` succeeded.
    pub sample_read_bound_succeeds: bool,
}

impl CapFigures {
    fn per_sample(&self, bytes: u64) -> f64 {
        bytes as f64 / self.shape.samples.max(1) as f64
    }

    pub fn listing_peak_per_sample(&self) -> f64 {
        self.per_sample(self.listing_search.succeeds_at)
    }

    pub fn sample_peak_per_sample(&self) -> f64 {
        self.per_sample(self.sample_search.succeeds_at)
    }

    pub fn upper_bound_per_sample(&self) -> f64 {
        self.per_sample(self.upper_bound_bytes)
    }

    /// How far the sample read's peak lies above [`upper_bound`]; negative
    /// below it.
    pub fn sample_peak_minus_upper_bound(&self) -> i64 {
        let diff =
            i128::from(self.sample_search.succeeds_at) - i128::from(self.upper_bound_bytes);
        i64::try_from(diff).unwrap_or(if diff < 0 { i64::MIN } else { i64::MAX })
    }
}

/// Compacts `bucket` at `cap` and measures the listing and sample-read charges
/// of its first part.
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
    let charge = listing_charge(&shape);
    let bound = sample_read_bound(&shape);
    let store = &compacted.store;
    // Starting each search at its path's bound makes that bound the first
    // probe, so when it succeeds the reported peak is at or below it, and
    // `check_report` can compare the two directly.
    let sample_search = minimal_budget(bound, |limit| {
        sample_read_succeeds(store, part, shape.series, limit)
    })
    .await?;
    let sample_read_bound_succeeds =
        sample_read_succeeds(store, part, shape.series, bound).await?;

    let isolated = isolate_part(config, cap, &compacted, part_index).await?;
    let only = &isolated.store;
    let listing_search = minimal_budget(charge, |limit| {
        listing_succeeds(only, config, shape.series, limit)
    })
    .await?;
    let listing_charge_succeeds = listing_succeeds(only, config, shape.series, charge).await?;
    Ok(CapFigures {
        cap_bytes: cap,
        parts: compacted.parts.len(),
        full_parts,
        part_index,
        shape,
        listing_search,
        listing_charge_bytes: charge,
        listing_charge_succeeds,
        sample_search,
        upper_bound_bytes: upper,
        sample_read_bound_bytes: bound,
        sample_read_bound_succeeds,
    })
}

fn all_series(read: Option<usize>, series: u64, path: &str) -> Result<bool, ListingChargeError> {
    match read {
        Some(listed) if listed as u64 == series => Ok(true),
        Some(listed) => Err(ListingChargeError::Read(format!(
            "the {path} returned {listed} series of a part holding {series}"
        ))),
        None => Ok(false),
    }
}

/// Whether a sample read of `part` ([`fetcher_sample_read`]) under a budget of
/// `limit` succeeds, returning all `series` it holds. A read that returns any
/// other count is an error.
pub async fn sample_read_succeeds(
    store: &Arc<MemoryStore>,
    part: &SegmentRef,
    series: u64,
    limit: u64,
) -> Result<bool, ListingChargeError> {
    all_series(
        fetcher_sample_read(store, part, limit).await?,
        series,
        "sample read",
    )
}

/// Whether a listing of `store` through `QueryEngine::resolve_series`
/// ([`engine_listing_succeeds`]) under a budget of `limit` succeeds, returning
/// all `series`. `store` is one [`isolate_part`] returned, so those are the
/// one part's series. A listing that returns any other count is an error.
pub async fn listing_succeeds(
    store: &Arc<MemoryStore>,
    config: &RunConfig,
    series: u64,
    limit: u64,
) -> Result<bool, ListingChargeError> {
    all_series(
        engine_listing_succeeds(store, config, limit).await?,
        series,
        "listing",
    )
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
                "listing_peak_bytes": c.listing_search.succeeds_at,
                "listing_refused_at_bytes": c.listing_search.refused_at,
                "listing_search_probes": c.listing_search.probes,
                "listing_peak_per_sample_bytes": c.listing_peak_per_sample(),
                "listing_charge_bytes": c.listing_charge_bytes,
                "listing_charge_succeeds": c.listing_charge_succeeds,
                "sample_peak_bytes": c.sample_search.succeeds_at,
                "sample_refused_at_bytes": c.sample_search.refused_at,
                "sample_search_probes": c.sample_search.probes,
                "sample_peak_per_sample_bytes": c.sample_peak_per_sample(),
                "sample_upper_bound_bytes": c.upper_bound_bytes,
                "sample_upper_bound_per_sample_bytes": c.upper_bound_per_sample(),
                "sample_peak_minus_upper_bound_bytes": c.sample_peak_minus_upper_bound(),
                "sample_read_bound_bytes": c.sample_read_bound_bytes,
                "sample_read_bound_succeeds": c.sample_read_bound_succeeds,
            })
        })
        .collect();
    json!({
        "bench": "l1_listing_charge",
        "issues": [4393, 4433],
        "listing_call": "QueryEngine::resolve_series over a store holding only the part, \
                         matcher __name__=~\".+\"",
        "sample_read_call": "SegmentFetcher::fetch_series, matcher __name__=~\".+\"",
        "resolution": "1 % of the peak",
        "listing_charge_formula": "held_fetch_bytes + catalog_decode_bytes",
        "sample_upper_bound_formula": "(64 + 32) * samples + catalog_decode_bytes",
        "sample_read_bound_formula": "sample_upper_bound_bytes + held_fetch_bytes",
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
            "listing_peak_per_sample_band_bytes":
                [LISTING_PEAK_PER_SAMPLE_BAND.0, LISTING_PEAK_PER_SAMPLE_BAND.1],
            "sample_peak_per_sample_band_bytes":
                [SAMPLE_PEAK_PER_SAMPLE_BAND.0, SAMPLE_PEAK_PER_SAMPLE_BAND.1],
            "listing_peak_at_or_below_listing_charge": true,
            "sample_peak_at_or_below_sample_read_bound": true,
            "peaks_non_decreasing_with_samples": true,
        },
        "caps": caps,
    })
}

/// A human table of the report.
pub fn report_table(report: &RunReport) -> String {
    let mut out = format!(
        "l1_listing_charge: {} series, {} h history in {} L0 flushes ({} samples, {} B)\n\
         {:>10} {:>5} {:>11} {:>11} {:>9} {:>11} {:>7} {:>12} {:>9} {:>12} {:>9} {:>12} {:>9} \
         {:>10}\n",
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
        "list B",
        "list/smp",
        "sample B",
        "smpl/smp",
        "bound B",
        "bound/smp",
        "smpl-bound",
    );
    for c in &report.caps {
        out.push_str(&format!(
            "{:>10} {:>5} {:>11} {:>11} {:>9} {:>11} {:>7} {:>12} {:>9.2} {:>12} {:>9.2} {:>12} \
             {:>9.2} {:>10}\n",
            c.cap_bytes,
            c.parts,
            c.shape.wire_bytes,
            c.shape.catalog_decode_bytes,
            c.shape.held_fetch_bytes,
            c.shape.samples,
            c.shape.series,
            c.listing_search.succeeds_at,
            c.listing_peak_per_sample(),
            c.sample_search.succeeds_at,
            c.sample_peak_per_sample(),
            c.upper_bound_bytes,
            c.upper_bound_per_sample(),
            c.sample_peak_minus_upper_bound(),
        ));
    }
    out
}

/// The report keys of one measured path and the band its figures hold.
struct PathKeys {
    path: &'static str,
    peak: &'static str,
    refused: &'static str,
    per_sample: &'static str,
    band: (f64, f64),
    bound: &'static str,
    bound_succeeds: &'static str,
}

const PATHS: [PathKeys; 2] = [
    PathKeys {
        path: "listing",
        peak: "listing_peak_bytes",
        refused: "listing_refused_at_bytes",
        per_sample: "listing_peak_per_sample_bytes",
        band: LISTING_PEAK_PER_SAMPLE_BAND,
        bound: "listing_charge_bytes",
        bound_succeeds: "listing_charge_succeeds",
    },
    PathKeys {
        path: "sample-read",
        peak: "sample_peak_bytes",
        refused: "sample_refused_at_bytes",
        per_sample: "sample_peak_per_sample_bytes",
        band: SAMPLE_PEAK_PER_SAMPLE_BAND,
        bound: "sample_read_bound_bytes",
        bound_succeeds: "sample_read_bound_succeeds",
    },
];

/// Checks the rendered report `text` against the bands for exactly `caps`.
/// Every cap appears once, every figure of [`CAP_FIGURES`] and every flag of
/// [`CAP_FLAGS`] appears once per cap in the text, and for each path, the
/// listing and the sample read, each figure is inside its band:
///
/// - peak per sample in [`LISTING_PEAK_PER_SAMPLE_BAND`] for the listing and
///   [`SAMPLE_PEAK_PER_SAMPLE_BAND`] for the sample read;
/// - the largest refused limit below the peak and within 1 % of it;
/// - the peak at or below the path's bound ([`listing_charge`],
///   [`sample_read_bound`]), compared directly, beside a read under exactly
///   that bound succeeding and no refused limit reaching it;
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
        .chain(CAP_FLAGS.iter())
        .chain(["cap_bytes"].iter())
    {
        let seen = text.matches(&format!("\"{figure}\":")).count();
        if seen != caps.len() {
            failures.push(format!(
                "{figure}: emitted {seen} times for {} caps",
                caps.len()
            ));
        }
    }
    let mut rows: [Vec<(u64, u64, u64)>; 2] = [Vec::new(), Vec::new()];
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
        let samples = entry.get("samples").and_then(Value::as_u64);
        for (keys, rows) in PATHS.iter().zip(rows.iter_mut()) {
            let path = keys.path;
            let per_sample = figure(keys.per_sample).unwrap_or(f64::NAN);
            let (lo, hi) = keys.band;
            if !(lo..=hi).contains(&per_sample) {
                failures.push(format!(
                    "cap {cap}: {path} peak per sample {per_sample} B outside [{lo}, {hi}]"
                ));
            }
            if entry.get(keys.bound_succeeds).and_then(Value::as_bool) != Some(true) {
                failures.push(format!(
                    "cap {cap}: the {path} does not succeed under its bound, {}",
                    keys.bound
                ));
            }
            let peak = entry.get(keys.peak).and_then(Value::as_u64);
            let refused = entry.get(keys.refused).and_then(Value::as_u64);
            let bound = entry.get(keys.bound).and_then(Value::as_u64);
            match (peak, refused, bound, samples) {
                (Some(peak), Some(refused), Some(bound), Some(samples)) => {
                    if refused >= peak || peak - refused > peak / 100 {
                        failures.push(format!(
                            "cap {cap}: the {path} refused at {refused} B and succeeded at \
                             {peak} B is not a 1 % bracket"
                        ));
                    }
                    if peak > bound {
                        failures.push(format!(
                            "cap {cap}: the {path} peak {peak} B is above its bound {bound} B"
                        ));
                    }
                    if refused >= bound {
                        failures.push(format!(
                            "cap {cap}: the {path} was refused a {refused} B budget, at or above \
                             its bound {bound} B"
                        ));
                    }
                    rows.push((cap, samples, peak));
                }
                _ => failures.push(format!(
                    "cap {cap}: a {path} byte figure is not an integer"
                )),
            }
        }
    }
    for (keys, rows) in PATHS.iter().zip(rows.iter_mut()) {
        rows.sort_by_key(|&(_, samples, _)| samples);
        for pair in rows.windows(2) {
            let ((cap_a, samples_a, peak_a), (cap_b, samples_b, peak_b)) = (pair[0], pair[1]);
            if peak_b < peak_a {
                failures.push(format!(
                    "{} peak fell as samples per part grew: cap {cap_a} ({samples_a} samples) \
                     peaks at {peak_a} B, cap {cap_b} ({samples_b} samples) at {peak_b} B",
                    keys.path
                ));
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

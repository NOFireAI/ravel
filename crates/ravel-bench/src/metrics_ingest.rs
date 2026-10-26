//! MetricsBench Remote Write 1.0 ingest lane (ADR-0927, issue #937, task M5).
//!
//! One logical stream is replayed into every participating system: the
//! in-process Ravel path (`IngestRouter`, `WriteMode::Strict`, row endpoint
//! `in-process`), and, behind endpoints supplied by config with no hardcoded
//! hosts, `ravel-server`, Prometheus, VictoriaMetrics and Mimir over portable
//! Remote Write 1.0. Every system is fed the same [`LogicalPoint`]s (scalar
//! samples, staleness markers and native histograms) and the same wire body
//! shape (`prometheus.WriteRequest`, snappy-compressed protobuf), so a byte or
//! a latency difference is a property of the engine, not of the protocol.
//!
//! ## Acknowledgement semantics are data, never normalised (ADR-0927 dec. 3)
//!
//! Ravel's Remote Write surface is strict-mode only: a 2xx means the data
//! object AND its commit record are both durably stored, and the response
//! carries `x-ravel-commit-token`
//! (`services/ravel-server/src/remote_write.rs`). Its rows are tagged
//! [`AckSemantics::DurableOnAck`]: the in-process row carries every minted
//! commit token, the portable row the last `x-ravel-commit-token` it received
//! (`last_commit_token`). A Remote Write 2xx from a comparator means the
//! samples were
//! accepted into a buffer whose durability the harness cannot observe from the
//! client, so those rows are tagged [`AckSemantics::Buffered`] and carry no
//! tokens. A durable-on-ack latency is never placed in the same folded column
//! as a buffered one: [`MetricsIngestReport::max_ack_p99_ms`] refuses a
//! mixed-ack pool rather than reporting a number whose meaning is undefined.
//!
//! ## Ack is not visibility (ADR-0927 dec. 3)
//!
//! A strict ack means durable, not queryable: the default flush delay is 2 s,
//! so a read issued right after an ack can miss the write. The lane never
//! sleeps to paper over that. The Ravel replay returns the commit tokens each
//! strict write minted; a read-your-write pass passes them as
//! `min_commit_token` so the query resolves the exact committed segments
//! deterministically (`metrics_ingest::tests::commit_tokens_make_the_write_
//! read_your_write`).
//!
//! ## Exact-count discipline (ADR-0927 bands 4/5)
//!
//! Every line is classified ([`classify_line`]) as a scalar, a native
//! histogram, a staleness marker or a malformed line, and each row's
//! `stream_counts` counts all four. Every well-formed line is sent: a scalar
//! or a staleness marker (the stale NaN, [`STALE_MARKER_BITS`]) as a sample
//! under `offered_samples`, a native histogram under its own class,
//! `offered_histograms`. Each class closes on its own: `offered_samples ==
//! accepted_samples + rejected_samples + dropped_samples` and the same for
//! histograms. [`check_stream_band`] holds the stream to band 4's
//! classification identity: the four counts sum to `generated_lines`, each
//! count equals the generator's own per-type figure, and `malformed` is
//! zero. [`check_row_accounting`] holds each row to the offering identity,
//! read from the row's own fields: `offered_samples + offered_histograms +
//! excluded_native_histogram + excluded_stale_marker == generated_lines`.
//! Both exclusions are zero on every row this code writes
//! (`tests::the_ci_stream_replays_every_payload_type_with_zero_exclusions`);
//! they stay in the identity so an artifact written before the lane sent
//! every type still reads back closed.
//!
//! ## Bounded streaming replay
//!
//! A replay pulls batches from a [`SampleBatches`] source rather than holding
//! the stream. [`GeneratedStream`] generates one scrape step at a time and
//! queues only that step's points, so what a replay holds is bounded
//! by the batch size and one step's width, not by the run's length
//! (`metricsbench_ingest_smoke::streaming_replay_memory_does_not_grow_with_
//! history_length`). Its bytes, write order and digest are the one-shot
//! generator's (`metrics_ingest::tests::streaming_is_deterministic_and_
//! matches_one_shot_generation`).
//!
//! A figure the lane
//! cannot measure on a system is ABSENT (an `Option`), never a zero standing in
//! for unmeasured: a buffered system has no commit tokens (`None`, not an empty
//! vec), an HTTP comparator has no object-store accounting the bench can see
//! (`None`), and a non-Linux host reports no CPU or peak RSS (`None`). This is
//! the same discipline `report.rs` applies to billed attempts.
//!
//! Report-only: like the rest of `ravel-bench`, this lane never changes library
//! behaviour, it only measures it.
//!
//! ## `profile` and `substrate` (ADR-0927 decision 11, issue #1352)
//!
//! [`MetricsIngestReport`] carries two blocks so a reader never mistakes a
//! non-comparable or unbilled run for a publishable result:
//!
//! [`ProfileRecord`] (`profile`), the workload's own pre-registered figures,
//! this run's actual scope, and the comparability verdict:
//! - `name`: the `--profile` name.
//! - `comparable`: whether these figures may be published (decision 11).
//!   Forced `false` when `steps_run != steps_declared` or when the recorded
//!   stream digest differs from one regenerated from the manifest and anchor,
//!   regardless of the profile's own verdict: a run that stopped short, ran
//!   long, or replayed a stream its parameters do not reproduce is not that
//!   profile.
//! - `comparability_reason`: why not, present exactly when `comparable` is
//!   `false`, absent when `true`. The workload's own `Comparability` reason,
//!   never a second hand-written copy of it, unless the run itself is off the
//!   profile (step count or digest), in which case this states that instead.
//! - `active_series`: series alive at any one instant, as declared.
//! - `steps_run`: steps this run actually generated (the `--steps` flag, or
//!   `steps_declared` when unset).
//! - `steps_declared`: the profile's full declared step count
//!   (`samples_per_series`).
//! - `samples_per_series`, `scrape_interval_secs`, `duration_secs`,
//!   `total_samples`, `churn_basis_points_per_hour`: as declared.
//! - `label_cardinalities`: distinct values per FIXED label dimension, name
//!   to count, read from the manifest. The scaling label is not a dimension
//!   (the manifest gate refuses to let it be declared as one) and is never
//!   in this map; its cardinality depends on the run's churn epochs, so it
//!   is reported separately below.
//! - `scaling_label`: the scaling label's name and its declared cardinality:
//!   - `name`: the scaling label ([`crate::metrics_workload::GeneratorConfig::scaling_label`]).
//!   - `cardinality_declared`: distinct scaling-label values the profile's
//!     full declared run (`steps_declared`) would emit, the generator's
//!     exact count.
//! - `families`: one [`FamilyRecord`] per metric family (`name`, `instances`,
//!   `series_per_instance`), as declared.
//! - `run`: a [`RunRecord`], scoped to `steps_run`, never the declared full
//!   profile:
//!   - `steps`: equal to `steps_run`.
//!   - `workload_version`, `workload_digest`, `seed`: the manifest's format
//!     version, BLAKE3 over the manifest file's bytes, and its seed.
//!   - `anchor_ms`: the first generated step's timestamp, in milliseconds,
//!     the required `--anchor-ms` ([`check_anchor`]).
//!   - `stream_digest`: the generator's BLAKE3 over the replayed stream.
//!     `comparable` is also forced `false` when it differs from a digest
//!     regenerated from the manifest and anchor
//!     ([`recompute_stream_digest`]).
//!   - `total_series_created`: distinct series the generator actually
//!     created over these steps, the generator's exact count.
//!   - `logical_input_bytes`: uncompressed input bytes the generator
//!     produced over these steps, the generator's exact count.
//!   - `total_samples_generated`: samples the generator emitted over these
//!     steps, the generator's exact count.
//!   - `scaling_label_cardinality`: distinct scaling-label values this run
//!     actually emitted over `steps_run`, the generator's exact count.
//!
//! [`Substrate`] (`substrate`), the storage backend a run replayed against:
//! - `store_backend`: the `--store` kind (`memory` or `s3`).
//! - `endpoint_host`: the configured S3 endpoint's host, when `--store s3`
//!   and `RAVEL_S3_ENDPOINT` is set; absent for `MemoryStore` regardless of
//!   the env var, and absent when the var is unset. A scheme-less endpoint
//!   (`localhost:9000`) still resolves to its host. Host only, never the
//!   scheme, path, or credentials.
//! - `backend_bills_requests`: true only for real S3 with no endpoint
//!   override (decision 10); false on `MemoryStore` and on any store behind a
//!   configured endpoint, such as the nightly lane's local RustFS.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use crate::metrics_workload::{Profile, WorkloadFile};
use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{
    Clock, IngestConfig, IngestPoint, IngestRouter, IngestValue, PendingAckBudget, SystemClock,
    WriteMode,
};
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, StoreMetrics, list_all};
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_promql_difftest::generator::STALE_MARKER_BITS;
use ravel_remote_write::proto::prometheus::{
    BucketSpan as PbBucketSpan, Histogram as PbHistogram, Label as PbLabel, Sample as PbSample,
    TimeSeries as PbTimeSeries, WriteRequest, histogram as pb_histogram,
};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ResetHint as SegResetHint,
};
use ravel_types::{CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
use serde::{Deserialize, Serialize};

/// Bytes on the wire per logical sample, the write-amplification denominator:
/// `ts_ns: i64` + `value: f64`. The same constant `ingest`/`report` use.
const LOGICAL_BYTES_PER_SAMPLE: u64 = 16;

/// What a 2xx from a system means about durability (ADR-0927 decision 3). Data
/// on every result row: a durable-on-ack latency never sits in a pooled column
/// with a buffered ack without the distinction on the same row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckSemantics {
    /// A 2xx means the data object and its commit record are both durably
    /// stored. Ravel's strict-only Remote Write surface, and the only ack the
    /// harness can prove is durable.
    DurableOnAck,
    /// A 2xx means the samples were accepted into a buffer; durability follows
    /// asynchronously and is not observable from the client. Prometheus and
    /// VictoriaMetrics Remote Write.
    Buffered,
}

impl AckSemantics {
    /// A short human label for a report row.
    pub fn label(self) -> &'static str {
        match self {
            AckSemantics::DurableOnAck => "durable-on-ack",
            AckSemantics::Buffered => "buffered",
        }
    }
}

/// One logical float sample: a scalar line, or a staleness marker carried as
/// the Prometheus stale NaN ([`STALE_MARKER_BITS`]). A classic histogram
/// already decomposes into its `_bucket`/`_sum`/`_count` float series
/// upstream; a native histogram is a [`LogicalHistogram`].
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalSample {
    /// The metric name (the `__name__` label value), e.g. `mb_gauge`.
    pub metric: String,
    /// The other labels, name/value pairs, excluding `__name__`.
    pub labels: Vec<(String, String)>,
    /// Milliseconds since the epoch (Remote Write's own timestamp unit).
    pub ts_ms: i64,
    /// The scalar value.
    pub value: f64,
}

/// One logical native histogram, sent as a Remote Write 1.0 `Histogram`
/// with integer counts, zero threshold 0, zero count 0, no negative buckets,
/// and one positive span `{offset: 0, length: positive_deltas.len()}`
/// ([`pb_histogram_from`]).
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalHistogram {
    /// The metric name (the `__name__` label value).
    pub metric: String,
    /// The other labels, name/value pairs, excluding `__name__`.
    pub labels: Vec<(String, String)>,
    /// Milliseconds since the epoch.
    pub ts_ms: i64,
    /// The bucket-layout schema.
    pub schema: i32,
    /// Total observations.
    pub count: u64,
    /// Sum of observations.
    pub sum: f64,
    /// Positive-bucket counts in the Remote Write delta encoding: each entry
    /// is the difference against the previous bucket of this sample.
    pub positive_deltas: Vec<i64>,
    /// The Remote Write reset hint. The generator's line format
    /// (`metrics_gen::GeneratedSample::encode`) carries no counter-reset
    /// flag, so [`classify_line`] sets `Unknown`, which tells a receiver to
    /// detect a reset from the counts itself.
    pub reset_hint: pb_histogram::ResetHint,
}

/// A point a replay sends: a float sample or a native histogram.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPoint {
    /// A scalar sample or a staleness marker.
    Sample(LogicalSample),
    /// A native histogram.
    Histogram(LogicalHistogram),
}

impl LogicalPoint {
    fn validate(&self) -> Result<(), RejectionReason> {
        match self {
            LogicalPoint::Sample(s) => s.validate(),
            LogicalPoint::Histogram(h) => h.validate(),
        }
    }

    fn ts_ms(&self) -> i64 {
        match self {
            LogicalPoint::Sample(s) => s.ts_ms,
            LogicalPoint::Histogram(h) => h.ts_ms,
        }
    }

    fn metric(&self) -> &str {
        match self {
            LogicalPoint::Sample(s) => &s.metric,
            LogicalPoint::Histogram(h) => &h.metric,
        }
    }
}

/// Why a logical sample was refused before it reached a system. The lane owns
/// admission for the replay, so the reject count is deterministic and exact;
/// ADR-0927 does not pin the RW1.0 client-side validation rules, so these are a
/// decided detail (see the crate report).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    /// The metric name (`__name__`) was empty. Prometheus rejects an unnamed
    /// series, and Ravel's `SeriesId` hashes the name, so an empty name is a
    /// malformed identity.
    EmptyMetricName,
    /// A label carried an empty name. A label with no name has no identity and
    /// cannot participate in series identity.
    EmptyLabelName,
    /// The value was not finite (NaN or +/-inf) on a plain sample, and was not
    /// the staleness marker's exact bit pattern ([`STALE_MARKER_BITS`]), the
    /// one non-finite value the lane sends.
    NonFiniteValue,
    /// A native histogram whose delta-decoded bucket counts go negative or do
    /// not sum to its `count` (its zero bucket is always empty here).
    InvalidHistogram,
}

/// Shared admission rules on a series identity.
fn validate_identity(metric: &str, labels: &[(String, String)]) -> Result<(), RejectionReason> {
    if metric.is_empty() {
        return Err(RejectionReason::EmptyMetricName);
    }
    if labels.iter().any(|(name, _)| name.is_empty()) {
        return Err(RejectionReason::EmptyLabelName);
    }
    Ok(())
}

/// The canonical label set including `__name__`, sorted by name, so a series
/// has exactly one spelling on the wire and in the id.
fn canonical_labels(metric: &str, labels: &[(String, String)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::with_capacity(labels.len() + 1);
    out.push((METRIC_NAME_LABEL.to_string(), metric.to_string()));
    out.extend(labels.iter().cloned());
    out.sort();
    out
}

impl LogicalSample {
    /// Validate a sample against the lane's client-side admission rules,
    /// naming the first reason it fails. `Ok` means every system receives it.
    /// A non-finite value is refused unless its bits are exactly
    /// [`STALE_MARKER_BITS`] (`tests::validate_exempts_exactly_the_stale_marker_bits`).
    pub fn validate(&self) -> Result<(), RejectionReason> {
        validate_identity(&self.metric, &self.labels)?;
        if !self.value.is_finite() && self.value.to_bits() != STALE_MARKER_BITS {
            return Err(RejectionReason::NonFiniteValue);
        }
        Ok(())
    }

    /// Whether this sample is a staleness marker.
    pub fn is_stale_marker(&self) -> bool {
        self.value.to_bits() == STALE_MARKER_BITS
    }

    fn canonical_labels(&self) -> Vec<(String, String)> {
        canonical_labels(&self.metric, &self.labels)
    }
}

impl LogicalHistogram {
    /// Validate a histogram against the lane's client-side admission rules.
    pub fn validate(&self) -> Result<(), RejectionReason> {
        validate_identity(&self.metric, &self.labels)?;
        if !self.positive_deltas.is_empty() && self.positive_span_len().is_none() {
            return Err(RejectionReason::InvalidHistogram);
        }
        self.absolute_buckets()
            .ok_or(RejectionReason::InvalidHistogram)
            .map(|_| ())
    }

    /// The absolute positive-bucket counts the deltas decode to, or `None`
    /// when one goes negative, overflows, or they do not sum to `count`.
    fn absolute_buckets(&self) -> Option<Vec<u64>> {
        let mut out = Vec::with_capacity(self.positive_deltas.len());
        let mut running: i64 = 0;
        let mut total: u64 = 0;
        for delta in &self.positive_deltas {
            running = running.checked_add(*delta)?;
            let bucket = u64::try_from(running).ok()?;
            total = total.checked_add(bucket)?;
            out.push(bucket);
        }
        (total == self.count).then_some(out)
    }

    fn canonical_labels(&self) -> Vec<(String, String)> {
        canonical_labels(&self.metric, &self.labels)
    }

    /// The single positive span, or none for an empty bucket vector: a
    /// zero-length span is not a valid layout.
    fn positive_span_len(&self) -> Option<u32> {
        u32::try_from(self.positive_deltas.len())
            .ok()
            .filter(|len| *len > 0)
    }
}

/// Percentile of a sorted slice, nearest-rank. Empty maps to zero.
fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

/// Latency percentiles in milliseconds. Mirrors the shape `report.rs` reports;
/// carried per system row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LatencyReport {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub count: usize,
}

impl LatencyReport {
    /// Build the percentiles from a set of nanosecond samples.
    pub fn from_nanos(mut samples_ns: Vec<u64>) -> Self {
        samples_ns.sort_unstable();
        LatencyReport {
            p50: percentile(&samples_ns, 0.50) as f64 / 1e6,
            p95: percentile(&samples_ns, 0.95) as f64 / 1e6,
            p99: percentile(&samples_ns, 0.99) as f64 / 1e6,
            max: samples_ns.last().copied().unwrap_or(0) as f64 / 1e6,
            count: samples_ns.len(),
        }
    }
}

/// The one value [`IngestPhase::elapsed_basis`] takes today: `elapsed_secs`
/// is the sum of the timed write segments, never the replay's wall time.
pub const ELAPSED_BASIS_TIMED_SEGMENTS: &str = "timed_segments";

/// The ingest phase of one system's replay. Ingest and query phases are
/// recorded separately (ADR-0927 decision 9); this lane owns ingest, the query
/// lane owns query.
///
/// Every field added after the nightly lane began writing artifacts carries a
/// serde default, so a reader built on this struct still loads an artifact
/// written before it (`tests::a_pre_change_artifact_loads_with_defaults`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IngestPhase {
    /// Every line of the stream this row replayed, counted by kind. All zero
    /// in an artifact written before the stream was classified.
    #[serde(default)]
    pub stream_counts: StreamCounts,
    /// Native-histogram lines this row did not send, named rather than
    /// skipped. Zero for every row this code writes, which sends every
    /// native histogram; non-zero in artifacts written before it did.
    #[serde(default)]
    pub excluded_native_histogram: u64,
    /// Staleness-marker lines this row did not send. Zero for every row this
    /// code writes, which sends every marker as an offered sample.
    #[serde(default)]
    pub excluded_stale_marker: u64,
    /// Float samples offered to the system: the scalar lines plus the
    /// staleness markers. `accepted + rejected + dropped`.
    pub offered_samples: u64,
    /// BLAKE3 over every point this row offered, in offered order: each
    /// sample's canonical `(series, ts_ms, value_bits)`, a staleness marker
    /// by its bit pattern, and each native histogram by every field it is
    /// encoded with ([`OfferedDigest`]). Empty in an artifact written before
    /// it existed.
    #[serde(default)]
    pub offered_digest: String,
    /// Samples the system acknowledged.
    pub accepted_samples: u64,
    /// Samples the lane refused before sending (client-side admission).
    pub rejected_samples: u64,
    /// Native histograms offered to the system, their own class, never
    /// counted in `offered_samples`. `accepted_histograms +
    /// rejected_histograms + dropped_histograms`.
    #[serde(default)]
    pub offered_histograms: u64,
    /// Native histograms in batches the system acknowledged.
    #[serde(default)]
    pub accepted_histograms: u64,
    /// Native histograms the lane refused before sending.
    #[serde(default)]
    pub rejected_histograms: u64,
    /// Native histograms in batches sent and never acknowledged. On a
    /// comparator row, `histograms_rejected_by_status` summed plus
    /// `retry_exhausted_histograms`.
    #[serde(default)]
    pub dropped_histograms: u64,
    /// Native histograms in batches that ended on a non-retryable non-2xx
    /// response, keyed by that HTTP status. Part of `dropped_histograms`.
    #[serde(default)]
    pub histograms_rejected_by_status: BTreeMap<u16, u64>,
    /// Native histograms in batches still failing once `max_retries` retries
    /// were spent. Part of `dropped_histograms`.
    #[serde(default)]
    pub retry_exhausted_histograms: u64,
    /// Samples in a batch that was sent and never acknowledged: neither
    /// accepted nor rejected, so the accounting still closes. On a comparator
    /// row this is exactly `rejected_by_status` summed plus
    /// `retry_exhausted_samples`; on the in-process row it is a strict write
    /// that did not ack, and both of those stay empty.
    pub dropped_samples: u64,
    /// Batches counted under `dropped_samples`.
    pub dropped_batches: u64,
    /// Comparator samples in batches that ended on a non-retryable non-2xx
    /// response, keyed by that HTTP status. Part of `dropped_samples`.
    #[serde(default)]
    pub rejected_by_status: BTreeMap<u16, u64>,
    /// Comparator samples in batches still failing (a retryable status or a
    /// transport error) once `max_retries` retries were spent. Part of
    /// `dropped_samples`.
    #[serde(default)]
    pub retry_exhausted_samples: u64,
    /// Batches offered (one write call each).
    pub batches: u64,
    /// Transport retries: object-store PUT retries below `InstrumentedStore`
    /// for Ravel, HTTP 429/5xx retries for a comparator.
    pub retries: u64,
    /// Time of the ingest phase, summed over the timed segments only: the
    /// stream is generated batch by batch during the replay, and generating it
    /// is never inside a segment. The two lanes time DIFFERENT work, so a
    /// throughput comparison must account for it: the Ravel segments are the
    /// in-process `write_values` calls and the final `flush_all` (the RW1.0
    /// encode that yields `wire_bytes` runs OUTSIDE them, since the in-process
    /// path never serializes an RW1.0 body to ingest), while an HTTP
    /// comparator's segments are its genuine per-batch encode and POST, which
    /// is real client work for that lane.
    pub elapsed_secs: f64,
    /// What `elapsed_secs` measures, so two artifacts can be told apart
    /// before their throughput figures are compared: always
    /// [`ELAPSED_BASIS_TIMED_SEGMENTS`] for a report this code writes.
    /// Reports written before this field existed carry no marker and read
    /// back as the empty string, meaning wall time, unmarked; their
    /// `elapsed_secs` was one wall-clock span over the write loop of a
    /// stream generated up front. For the Ravel row that span started after
    /// admission and the RW1.0 encode; for a comparator row the encode was
    /// inside it, as it is inside that row's timed segments now. In both
    /// lanes the only basis change is the generation now interleaved between
    /// segments, which the sum excludes.
    #[serde(default)]
    pub elapsed_basis: String,
    /// Accepted logical samples per second.
    pub logical_points_per_sec: f64,
    /// Bytes of the snappy-compressed RW1.0 body offered (the wire shape every
    /// system receives), across accepted-or-dropped batches.
    pub wire_bytes: u64,
    /// Wire bytes per second over the ingest wall time.
    pub wire_bytes_per_sec: f64,
    /// Acknowledgement latency percentiles, per batch. Its meaning depends on
    /// the row's [`AckSemantics`]: for a durable-on-ack row this is time to
    /// durability, for a buffered row time to buffer-accept.
    pub ack_latency_ms: LatencyReport,
    /// Peak resident set of the bench process during the run, or `None` where
    /// the harness cannot read it (non-Linux, or a remote comparator whose
    /// process is not this one). Never a zero standing in for unmeasured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    /// CPU seconds the bench process spent during the run, or `None` where
    /// unmeasured (non-Linux, or a remote comparator). It covers the whole
    /// replay, so it includes generating and classifying the streamed input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_secs: Option<f64>,
}

/// Diagnostic-lane object-store accounting (ADR-0927: never folded into a
/// cross-engine score). Present only for the in-process Ravel path, where the
/// bench owns the store; `None` for a comparator behind an HTTP endpoint whose
/// object store the bench cannot see.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StorageAccounting {
    /// Distinct objects the workload wrote.
    pub object_count: u64,
    /// Total stored (on-object, compressed) bytes.
    pub stored_bytes: u64,
    /// Completed PUT calls (one per logical PUT), from `InstrumentedStore`.
    pub put_count: u64,
    /// PUT payload bytes offered to the backend (wire, stored form).
    pub put_bytes: u64,
    /// Stored bytes divided by accepted samples.
    pub stored_bytes_per_sample: f64,
    /// Stored bytes over logical bytes ingested (`accepted * 16`).
    pub write_amplification: f64,
    /// Whether a request against this backend is billed: `false` on
    /// `MemoryStore` (real counts, but free), `true` on S3. The explicit
    /// representation of a non-billable count instead of a misleading zero.
    pub backend_bills_requests: bool,
}

/// One system's ingest result row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemIngestResult {
    /// The system: `ravel`, `prometheus`, `victoriametrics`, `mimir`. Two
    /// rows can both say `ravel`: the in-process diagnostic row and the
    /// portable row over HTTP, told apart by `endpoint`.
    pub system: String,
    /// What a 2xx from this system means (ADR-0927 decision 3).
    pub ack_semantics: AckSemantics,
    /// The `/api/v1/write` URL replayed against, or `in-process` for the
    /// in-process Ravel row (ADR-0927 2026-10-26 amendment A). Never a
    /// hardcoded host: an HTTP row's endpoint came from config.
    pub endpoint: String,
    /// Per-system client behaviour (batching, retries, backpressure), stated so
    /// two rows are not silently divergent (issue #937 deliverable 1).
    pub client_behavior: String,
    /// The ingest phase figures.
    pub ingest: IngestPhase,
    /// Commit tokens the strict writes minted (ADR-0927 decision 3), encoded.
    /// `Some` only for a durable-on-ack system that returns them; `None` for a
    /// buffered system, which is absence, not an empty vec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_tokens: Option<Vec<String>>,
    /// The `x-ravel-commit-token` header value of the last acknowledged
    /// request, verbatim (the server joins one token per shard the write
    /// touched with commas). Set only on the portable Ravel row; the query
    /// runner passes it as `min_commit_token` (ADR-0927 decision 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_commit_token: Option<String>,
    /// Diagnostic object-store accounting; see [`StorageAccounting`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageAccounting>,
}

impl SystemIngestResult {
    /// Build the Ravel row: strict, durable-on-ack, carrying the commit tokens
    /// and the diagnostic storage accounting.
    ///
    /// The `ack_semantics: AckSemantics::DurableOnAck` set here is the load-
    /// bearing tag: flipping it to `Buffered` lets a durable ack latency pool
    /// with a buffered one (see
    /// `tests::strict_ack_is_reported_separately_from_buffered`).
    pub fn ravel_row(
        endpoint: String,
        ingest: IngestPhase,
        commit_tokens: Vec<String>,
        storage: StorageAccounting,
    ) -> Self {
        SystemIngestResult {
            system: "ravel".to_string(),
            ack_semantics: AckSemantics::DurableOnAck,
            endpoint,
            client_behavior: "in-process IngestRouter, WriteMode::Strict: one write_values call \
                 per batch, awaited to durable ack (data object + commit record); object-store \
                 PUT retries handled below InstrumentedStore (max_retries=10); backpressure via \
                 the shard channel's blocking send"
                .to_string(),
            ingest,
            commit_tokens: Some(commit_tokens),
            last_commit_token: None,
            storage: Some(storage),
        }
    }

    /// Build a buffered comparator row (Prometheus/VictoriaMetrics/Mimir over
    /// portable RW1.0). No commit tokens and no object-store accounting: a
    /// buffered 2xx mints no token the client sees, and the comparator's store
    /// is not the bench's.
    pub fn buffered_http_row(
        system: impl Into<String>,
        endpoint: impl Into<String>,
        client_behavior: impl Into<String>,
        ingest: IngestPhase,
    ) -> Self {
        SystemIngestResult {
            system: system.into(),
            ack_semantics: AckSemantics::Buffered,
            endpoint: endpoint.into(),
            client_behavior: client_behavior.into(),
            ingest,
            commit_tokens: None,
            last_commit_token: None,
            storage: None,
        }
    }

    /// Build a durable-on-ack HTTP row, the portable Ravel row over
    /// `ravel-server`'s `/api/v1/write`, carrying the last commit token it
    /// returned. Only the last header value is kept: a long replay sends one
    /// request per batch, so `commit_tokens` stays absent and the row stays
    /// bounded.
    pub fn durable_http_row(
        system: impl Into<String>,
        endpoint: impl Into<String>,
        client_behavior: impl Into<String>,
        ingest: IngestPhase,
        last_commit_token: Option<String>,
    ) -> Self {
        SystemIngestResult {
            system: system.into(),
            ack_semantics: AckSemantics::DurableOnAck,
            endpoint: endpoint.into(),
            client_behavior: client_behavior.into(),
            ingest,
            commit_tokens: None,
            last_commit_token,
            storage: None,
        }
    }
}

/// A refused ack-latency pool: the named rows do not share one acknowledgement
/// meaning, so a single pooled figure across them has no defined meaning.
#[derive(Clone, Debug, PartialEq)]
pub struct AckConflation {
    /// Each system in the requested pool and its ack meaning.
    pub systems: Vec<(String, AckSemantics)>,
}

impl std::fmt::Display for AckConflation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing to pool ack latency across rows with different acknowledgement meanings: "
        )?;
        for (i, (system, kind)) in self.systems.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{system}={}", kind.label())?;
        }
        Ok(())
    }
}

impl std::error::Error for AckConflation {}

/// The read-your-write query phase of a system's replay. Recorded as its own
/// phase, separate from the ingest phase (ADR-0927 decision 9): ingest and
/// query figures never share a row. Token-bound: the ingest phase's commit
/// tokens are passed as `min_commit_token`, so the read resolves the exact
/// committed segments deterministically without sleeping past the flush delay
/// (ADR-0927 decision 3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemQueryResult {
    /// The system queried: `ravel` (the only durable-on-ack, token-minting
    /// path the bench can read deterministically).
    pub system: String,
    /// The instant PromQL expression evaluated.
    pub query: String,
    /// Milliseconds timestamp the instant query evaluated at: the replay's
    /// newest written sample.
    pub eval_ts_ms: i64,
    /// The commit tokens passed as `min_commit_token`, encoded. A token-bound
    /// read; empty only if the ingest minted none.
    pub min_commit_tokens: Vec<String>,
    /// Series the instant query matched.
    pub matched_series: u64,
    /// Wall time of the query phase.
    pub elapsed_secs: f64,
}

/// The ADR-0927 decision-11 profile figures, plus the workload's own
/// comparability verdict. Every figure here is the profile's pre-registered
/// value, the generator's exact count for a run-scoped one (under `run`), or
/// derived from either -- never from the offered sample count, so a reader
/// can compare this row against the workload definition directly instead of
/// trusting the run that produced it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileRecord {
    /// The `--profile` name (`cardinality`, `history`, `churn`, `ci`, ...).
    pub name: String,
    /// Whether this profile's figures may be compared across runs or
    /// systems (ADR-0927 decision 11). `ci` is `false`: it exists for
    /// reachability, not for a performance or cost claim. Also `false`,
    /// regardless of the profile's own verdict, when `steps_run !=
    /// steps_declared` or when `run.stream_digest` differs from the digest
    /// regenerated from the manifest and anchor: such a run's figures are not
    /// that profile's.
    pub comparable: bool,
    /// Why `comparable` is `false`; absent when it is `true`. The workload's
    /// own [`crate::metrics_workload::Comparability::reason`], never a
    /// second, hand-written copy of it, unless the run itself is off the
    /// profile (step count or stream digest), in which case this states
    /// that instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparability_reason: Option<String>,
    /// Active series the profile declares.
    pub active_series: u64,
    /// Steps this run actually generated (the `--steps` flag, or
    /// `steps_declared` when unset).
    pub steps_run: u64,
    /// The profile's full, declared step count (`samples_per_series`).
    pub steps_declared: u64,
    /// Samples per series per scrape, as declared.
    pub samples_per_series: u64,
    /// Scrape interval, as declared.
    pub scrape_interval_secs: u64,
    /// Run duration, as declared.
    pub duration_secs: u64,
    /// Total samples the profile declares over its full duration.
    pub total_samples: u64,
    /// Distinct values per FIXED label dimension, name to count, straight
    /// from the workload manifest (`WorkloadFile::label_cardinalities`). The
    /// scaling label is never in this map -- the manifest gate refuses to
    /// let it be declared as an ordinary dimension -- and is reported
    /// instead under `scaling_label` and `run.scaling_label_cardinality`.
    pub label_cardinalities: BTreeMap<String, u64>,
    /// The scaling label's name and its declared (full-run) cardinality.
    pub scaling_label: ScalingLabelRecord,
    /// Declared series churn, in basis points per hour.
    pub churn_basis_points_per_hour: u64,
    /// Per-family instance counts, as declared.
    pub families: Vec<FamilyRecord>,
    /// Figures scoped to this run's actual `steps_run`, never the declared
    /// full profile: the generator's exact counts over exactly the steps
    /// this run generated.
    pub run: RunRecord,
}

/// Figures scoped to the run's actual step count (`ProfileRecord::steps_run`),
/// as opposed to the profile's declared full-run figures: the generator's
/// exact counts, never derived from the offered sample count.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    /// Steps this run generated. Equal to `ProfileRecord::steps_run`.
    pub steps: u64,
    /// The workload manifest's format version, as the loader read it. `None`
    /// only in an artifact written before it was recorded.
    #[serde(default)]
    pub workload_version: Option<u32>,
    /// BLAKE3 (hex) over the workload manifest file's bytes. `None` only in an
    /// artifact written before it was recorded.
    #[serde(default)]
    pub workload_digest: Option<String>,
    /// The manifest's generator seed. `None` only in an artifact written
    /// before it was recorded.
    #[serde(default)]
    pub seed: Option<u64>,
    /// Timestamp of the first generated step, in milliseconds since the epoch:
    /// the required `--anchor-ms`, which [`check_anchor`] holds on the scrape
    /// grid and in range. `None` only in an artifact written before it was
    /// recorded.
    #[serde(default)]
    pub anchor_ms: Option<i64>,
    /// The generator's BLAKE3 (hex) over the stream this run replayed
    /// (`GenerationReport::digest`). `None` only in an artifact written before
    /// it was recorded.
    #[serde(default)]
    pub stream_digest: Option<String>,
    /// Distinct series the generator actually created over these steps.
    pub total_series_created: u64,
    /// Logical (uncompressed, pre-wire) input bytes the generator produced
    /// over these steps.
    pub logical_input_bytes: u64,
    /// Samples the generator emitted over these steps (after omissions and
    /// stale markers; the generator's exact count).
    pub total_samples_generated: u64,
    /// Distinct scaling-label values the generator actually emitted over
    /// these steps (`Generator::scaling_label_cardinality`), across every
    /// family and every churn epoch these steps span.
    pub scaling_label_cardinality: u64,
}

/// The largest millisecond timestamp whose nanosecond form fits an `i64`
/// (`i64::MAX / 1_000_000`). Every generated timestamp is converted to
/// nanoseconds on ingest, so no step may land past it.
pub const MAX_ANCHOR_RANGE_MS: i64 = 9_223_372_036_854;

/// Why a `--anchor-ms` value cannot anchor a run.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AnchorError {
    /// Before the epoch.
    #[error("--anchor-ms {anchor_ms} is negative; the anchor is milliseconds since the epoch")]
    Negative { anchor_ms: i64 },
    /// The last step's timestamp would pass [`MAX_ANCHOR_RANGE_MS`].
    #[error(
        "--anchor-ms {anchor_ms} puts the last of {steps} steps at {last_ms} ms ({anchor_ms} + \
         ({steps} - 1) * {interval_ms}), past {max} ms, the largest millisecond whose \
         nanosecond form fits an i64"
    )]
    OutOfRange {
        anchor_ms: i64,
        steps: u64,
        interval_ms: u64,
        last_ms: i128,
        max: i64,
    },
    /// Not a multiple of the profile's scrape interval.
    #[error(
        "--anchor-ms {anchor_ms} is off the scrape grid: {anchor_ms} mod {interval_ms} = \
         {remainder}; it must be a multiple of the profile's scrape interval in milliseconds"
    )]
    OffGrid {
        anchor_ms: i64,
        interval_ms: u64,
        remainder: u64,
    },
    /// The profile declares a zero scrape interval, so no grid exists.
    #[error("the profile declares a zero scrape interval, so no anchor is on its grid")]
    ZeroInterval,
}

/// Check a run's anchor: non-negative, the last step
/// (`anchor_ms + (steps - 1) * interval_ms`) at or before
/// [`MAX_ANCHOR_RANGE_MS`], and on the profile's scrape grid.
pub fn check_anchor(
    anchor_ms: i64,
    scrape_interval_secs: u64,
    steps: u64,
) -> Result<(), AnchorError> {
    if anchor_ms < 0 {
        return Err(AnchorError::Negative { anchor_ms });
    }
    let interval_ms = u128::from(scrape_interval_secs) * 1_000;
    if interval_ms == 0 {
        return Err(AnchorError::ZeroInterval);
    }
    // Saturating wide arithmetic, so the bound itself cannot wrap.
    let span = u128::from(steps.saturating_sub(1)).saturating_mul(interval_ms);
    let last_ms = i128::from(anchor_ms).saturating_add(i128::try_from(span).unwrap_or(i128::MAX));
    let interval_ms_u64 = u64::try_from(interval_ms).unwrap_or(u64::MAX);
    if last_ms > i128::from(MAX_ANCHOR_RANGE_MS) {
        return Err(AnchorError::OutOfRange {
            anchor_ms,
            steps,
            interval_ms: interval_ms_u64,
            last_ms,
            max: MAX_ANCHOR_RANGE_MS,
        });
    }
    let remainder = anchor_ms as u128 % interval_ms;
    if remainder != 0 {
        return Err(AnchorError::OffGrid {
            anchor_ms,
            interval_ms: interval_ms_u64,
            remainder: remainder as u64,
        });
    }
    Ok(())
}

/// The run parameters a report records as provenance, beyond what the
/// manifest itself carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunProvenance {
    /// BLAKE3 (hex) over the manifest file's bytes ([`workload_digest`]).
    pub workload_digest: String,
    /// The first step's timestamp, already through [`check_anchor`].
    pub anchor_ms: i64,
}

/// BLAKE3 (hex) over a workload manifest file's bytes.
pub fn workload_digest(manifest_bytes: &[u8]) -> String {
    blake3::hash(manifest_bytes).to_hex().to_string()
}

/// Regenerate the stream `steps` scrapes of `profile` from `anchor_ms` and
/// return its digest, discarding the bytes as they are produced. This is the
/// digest the run's parameters reproduce, independent of the generator object
/// whose stream was replayed.
pub fn recompute_stream_digest(
    workload: &WorkloadFile,
    profile: &Profile,
    anchor_ms: i64,
    steps: u64,
) -> Result<String, LaneError> {
    let report = crate::metrics_gen::Generator::for_profile(workload, profile, anchor_ms)
        .generate_into(steps, &mut std::io::sink())
        .map_err(|e| LaneError::Generate(e.to_string()))?;
    Ok(report.digest)
}

/// The scaling label's identity and its cardinality over a profile's full
/// declared run. Named separately from `label_cardinalities` because the
/// scaling label is not a dimension: the manifest gate refuses to let it be
/// declared as one, and its cardinality depends on churn epochs rather than
/// being a fixed, manifest-only count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalingLabelRecord {
    /// The scaling label's name (`GeneratorConfig::scaling_label`).
    pub name: String,
    /// Distinct scaling-label values the profile's full declared run
    /// (`steps_declared`) would emit, across every family and every churn
    /// epoch it spans (`Generator::scaling_label_cardinality`).
    pub cardinality_declared: u64,
}

/// One metric family's exact instance count under the run's profile, and the
/// series each instance emits (`WorkloadFile::family_instances`,
/// `WorkloadFile::series_per_instance`) -- the generator's own counts, never a
/// formula re-typed against the manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FamilyRecord {
    /// The family name.
    pub name: String,
    /// Instances (histograms, or plain series) the family emits under this
    /// profile.
    pub instances: u64,
    /// Series one instance emits: 1 for a gauge, a counter, or a native
    /// histogram; bounds-plus-3 for a classic histogram.
    pub series_per_instance: u64,
}

/// Builds the ADR-0927 decision-11 profile block for one run.
///
/// `comparable` is forced `false` whenever `steps_run` differs from
/// `profile`'s declared `samples_per_series`, or whenever `gen_report.digest`
/// (recorded as `run.stream_digest`) differs from the digest
/// [`recompute_stream_digest`] regenerates here from the manifest, the
/// anchor and `steps_run`, regardless of the profile's own verdict: a run
/// that stopped short, ran long, or replayed a stream its parameters do not
/// reproduce is not that profile's figures. `comparability_reason` then
/// states each such cause, not the profile's own (possibly `None`) reason.
/// Otherwise `comparable` and `comparability_reason` come straight from
/// `profile.comparability`.
///
/// `steps_run` is the `--steps` the run resolved to and `logical_input_bytes`
/// the encoded stream's length (the generator's own `bytes` for a streamed
/// run); `provenance` carries the manifest digest and the anchor.
pub fn build_profile_record(
    workload: &WorkloadFile,
    profile: &Profile,
    steps_run: u64,
    logical_input_bytes: u64,
    gen_report: &crate::metrics_gen::GenerationReport,
    provenance: RunProvenance,
) -> Result<ProfileRecord, LaneError> {
    let families = workload
        .families
        .iter()
        .map(|family| FamilyRecord {
            name: family.name.clone(),
            instances: workload.family_instances(profile, family),
            series_per_instance: workload.series_per_instance(family.kind),
        })
        .collect();
    let steps_declared = profile.samples_per_series;
    // Any step count other than the profile's own: a short run is not that
    // profile's figures, and an over-long one is not either.
    let mut off_profile: Vec<String> = Vec::new();
    if steps_run != steps_declared {
        off_profile.push(format!(
            "this run generated {steps_run} steps against profile `{}`'s {steps_declared}, so it \
             is not that profile and its figures cannot be published",
            profile.name
        ));
    }
    let recomputed = recompute_stream_digest(workload, profile, provenance.anchor_ms, steps_run)?;
    if recomputed != gen_report.digest {
        off_profile.push(format!(
            "the replayed stream's digest {} differs from {recomputed}, the digest the manifest \
             and anchor {} regenerate, so the run did not replay the stream its parameters name",
            gen_report.digest, provenance.anchor_ms
        ));
    }
    let comparable = profile.is_publishable() && off_profile.is_empty();
    let comparability_reason = if off_profile.is_empty() {
        profile.comparability.reason().map(String::from)
    } else {
        Some(off_profile.join("; "))
    };
    let generator = crate::metrics_gen::Generator::for_profile(workload, profile, 0);
    Ok(ProfileRecord {
        name: profile.name.clone(),
        comparable,
        comparability_reason,
        active_series: profile.active_series,
        steps_run,
        steps_declared,
        samples_per_series: profile.samples_per_series,
        scrape_interval_secs: profile.scrape_interval_secs,
        duration_secs: profile.duration_secs,
        total_samples: profile.total_samples,
        label_cardinalities: workload.label_cardinalities(),
        scaling_label: ScalingLabelRecord {
            name: workload.generator.scaling_label.clone(),
            cardinality_declared: generator.scaling_label_cardinality(steps_declared),
        },
        churn_basis_points_per_hour: profile.churn_basis_points_per_hour,
        families,
        run: RunRecord {
            steps: steps_run,
            workload_version: Some(workload.version),
            workload_digest: Some(provenance.workload_digest),
            seed: Some(workload.seed),
            anchor_ms: Some(provenance.anchor_ms),
            stream_digest: Some(gen_report.digest.clone()),
            total_series_created: gen_report.total_series_created,
            logical_input_bytes,
            total_samples_generated: gen_report.emitted_samples,
            scaling_label_cardinality: generator.scaling_label_cardinality(steps_run),
        },
    })
}

/// The storage backend a run replayed against, and whether it bills for
/// requests (ADR-0927 decision 10): named at the top level so a reader never
/// mistakes a RustFS-backed `ci` run's request counts for a real S3 cost.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Substrate {
    /// The `--store` kind (`memory` or `s3`), as `StoreKind`'s `Display`.
    pub store_backend: String,
    /// The configured S3 endpoint's host, when `RAVEL_S3_ENDPOINT` is set.
    /// Host only: never the scheme, path, or embedded credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_host: Option<String>,
    /// Whether this substrate bills for requests: false on `MemoryStore` and
    /// on any store behind a configured endpoint, true only for real S3 with
    /// no endpoint override (`harness::backend_bills_requests`).
    pub backend_bills_requests: bool,
}

/// The whole ingest-lane report: one row per participating system, plus the
/// separately-recorded query-phase rows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsIngestReport {
    /// The profile this run replayed, and its ADR-0927 decision-11 figures.
    /// Absent only for the internal unit-test rows built directly from
    /// `SystemIngestResult`s with no profile context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<ProfileRecord>,
    /// The storage substrate this run replayed against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrate: Option<Substrate>,
    /// Report rows, in the order the systems were replayed.
    pub systems: Vec<SystemIngestResult>,
    /// Query-phase rows, recorded separately from ingest (ADR-0927 decision 9).
    /// Empty when no post-ingest query ran.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queries: Vec<SystemQueryResult>,
}

impl MetricsIngestReport {
    /// Assemble a report from its ingest rows, with no query phase, profile,
    /// or substrate attached yet.
    pub fn new(systems: Vec<SystemIngestResult>) -> Self {
        MetricsIngestReport {
            profile: None,
            substrate: None,
            systems,
            queries: Vec::new(),
        }
    }

    /// Attach a query-phase row (builder style), keeping ingest and query
    /// figures in separate columns.
    pub fn with_query(mut self, query: SystemQueryResult) -> Self {
        self.queries.push(query);
        self
    }

    /// Attach the ADR-0927 decision-11 profile record (builder style).
    pub fn with_profile(mut self, profile: ProfileRecord) -> Self {
        self.profile = Some(profile);
        self
    }

    /// Attach the storage substrate record (builder style).
    pub fn with_substrate(mut self, substrate: Substrate) -> Self {
        self.substrate = Some(substrate);
        self
    }

    /// The ingest row for `system`, if present.
    pub fn row(&self, system: &str) -> Option<&SystemIngestResult> {
        self.systems.iter().find(|r| r.system == system)
    }

    /// The query-phase row for `system`, if present.
    pub fn query_row(&self, system: &str) -> Option<&SystemQueryResult> {
        self.queries.iter().find(|r| r.system == system)
    }

    /// ADR-0927 decision 3 as a mechanical guard, and the ABSENT-not-zero rule.
    ///
    /// Returns the MAXIMUM of the named rows' per-row p99 ack latencies, in
    /// milliseconds. This is deliberately NOT a pooled percentile over the
    /// union of the rows' raw ack samples: the report carries each row's
    /// summarized [`LatencyReport`], not its raw sample vector, so a true
    /// pooled percentile is not computable from a report, and this figure never
    /// claims to be one. The name says `max` because the computation is a max.
    ///
    /// The pool is admissible ONLY when every named row shares one
    /// acknowledgement meaning. A mix of durable-on-ack and buffered rows is
    /// refused with an [`AckConflation`] naming each system and its ack meaning,
    /// because folding a durable ack latency with a buffered one reports a
    /// number whose meaning is undefined -- the exact conflation this lane
    /// exists to prevent. Unknown system names are skipped.
    ///
    /// An empty pool (no named row exists) returns `Ok(None)`: an explicit
    /// absence, never a `0.0` standing in for an unmeasured pool, so a real
    /// `0.0` ms max and "no rows to fold" are distinguishable (lines 38-44).
    pub fn max_ack_p99_ms(&self, systems: &[&str]) -> Result<Option<f64>, AckConflation> {
        let rows: Vec<&SystemIngestResult> = systems.iter().filter_map(|s| self.row(s)).collect();
        let kinds: Vec<(String, AckSemantics)> = rows
            .iter()
            .map(|r| (r.system.clone(), r.ack_semantics))
            .collect();
        let distinct: BTreeSet<AckSemantics> = kinds.iter().map(|(_, k)| *k).collect();
        if distinct.len() > 1 {
            return Err(AckConflation { systems: kinds });
        }
        // Empty pool is absence, not zero. Flip this line to `Ok(Some(0.0))` to
        // see `tests::max_ack_p99_over_an_empty_pool_is_absent` fail.
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            rows.iter()
                .map(|r| r.ingest.ack_latency_ms.p99)
                .fold(f64::NEG_INFINITY, f64::max),
        ))
    }
}

/// Everything that can go wrong building or replaying the lane.
#[derive(Debug, thiserror::Error)]
pub enum LaneError {
    /// A series identity or label set could not be built (oversized component).
    #[error("building series identity failed: {0}")]
    Identity(#[from] ravel_types::TypeError),
    /// Snappy compression of the RW1.0 body failed.
    #[error("snappy compression of the Remote Write body failed: {0}")]
    Snappy(String),
    /// Setting up the HTTP transport for a comparator replay failed (building
    /// the reqwest client), before any request left the process. Distinct from
    /// [`LaneError::Snappy`]: a transport-setup failure has nothing to do with
    /// compressing the body, and rendering it as a snappy error misnames it.
    #[error("setting up the Remote Write HTTP transport failed: {0}")]
    Transport(String),
    /// The post-ingest read-your-write query phase failed (building the catalog
    /// or query engine, or evaluating the instant query).
    #[error("the read-your-write query phase failed: {0}")]
    Query(String),
    /// Generating the streamed workload failed, or a [`GeneratedStream`] was
    /// closed before it was drained.
    #[error("generating the replayed stream failed: {0}")]
    Generate(String),
}

/// Map a reqwest client-build failure to the transport-setup variant. The
/// call site in [`replay_over_http`] routes through this, so flipping the
/// constructed variant here (to [`LaneError::Snappy`], the pre-fix bug) is the
/// single line that makes `tests::client_build_failure_is_a_transport_error`
/// fail.
fn transport_setup_error(detail: String) -> LaneError {
    LaneError::Transport(detail)
}

/// The Remote Write 1.0 `Histogram` a logical histogram is sent as: integer
/// counts, zero threshold 0, zero count 0, no negative buckets, and one
/// positive span covering every delta (none for an empty bucket vector).
fn pb_histogram_from(h: &LogicalHistogram) -> PbHistogram {
    PbHistogram {
        count: Some(pb_histogram::Count::CountInt(h.count)),
        sum: h.sum,
        schema: h.schema,
        zero_threshold: 0.0,
        zero_count: Some(pb_histogram::ZeroCount::ZeroCountInt(0)),
        negative_spans: Vec::new(),
        negative_deltas: Vec::new(),
        negative_counts: Vec::new(),
        positive_spans: h
            .positive_span_len()
            .map(|length| PbBucketSpan { offset: 0, length })
            .into_iter()
            .collect(),
        positive_deltas: h.positive_deltas.clone(),
        positive_counts: Vec::new(),
        reset_hint: h.reset_hint as i32,
        timestamp: h.ts_ms,
        custom_values: Vec::new(),
    }
}

/// Group logical points into `prometheus.TimeSeries` by canonical identity:
/// float samples (staleness markers included) under `samples`, native
/// histograms under `histograms`.
fn write_request_from_points(points: &[LogicalPoint]) -> WriteRequest {
    let mut by_series: BTreeMap<Vec<(String, String)>, PbTimeSeries> = BTreeMap::new();
    for point in points {
        let labels = match point {
            LogicalPoint::Sample(s) => s.canonical_labels(),
            LogicalPoint::Histogram(h) => h.canonical_labels(),
        };
        let entry = by_series
            .entry(labels.clone())
            .or_insert_with(|| PbTimeSeries {
                labels: labels
                    .iter()
                    .map(|(name, value)| PbLabel {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
                samples: Vec::new(),
                exemplars: Vec::new(),
                histograms: Vec::new(),
            });
        match point {
            LogicalPoint::Sample(s) => entry.samples.push(PbSample {
                value: s.value,
                timestamp: s.ts_ms,
            }),
            LogicalPoint::Histogram(h) => entry.histograms.push(pb_histogram_from(h)),
        }
    }
    WriteRequest {
        timeseries: by_series.into_values().collect(),
        metadata: Vec::new(),
    }
}

/// Group logical samples into `prometheus.TimeSeries` by canonical identity.
#[cfg(test)]
fn write_request_from(samples: &[LogicalSample]) -> WriteRequest {
    let points: Vec<LogicalPoint> = samples.iter().cloned().map(LogicalPoint::Sample).collect();
    write_request_from_points(&points)
}

fn snappy(req: &WriteRequest) -> Result<Vec<u8>, LaneError> {
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .map_err(|e| LaneError::Snappy(e.to_string()))
}

/// Encode a batch of logical samples as a Remote Write 1.0 body: a
/// `prometheus.WriteRequest` protobuf, snappy block-compressed.
pub fn encode_rw1_body(samples: &[LogicalSample]) -> Result<Vec<u8>, LaneError> {
    let points: Vec<LogicalPoint> = samples.iter().cloned().map(LogicalPoint::Sample).collect();
    encode_rw1_points(&points)
}

/// Encode a batch of logical points as a Remote Write 1.0 body. This is the
/// exact wire shape every HTTP row receives, and its length is the batch's
/// wire-byte figure for the in-process Ravel path too.
pub fn encode_rw1_points(points: &[LogicalPoint]) -> Result<Vec<u8>, LaneError> {
    snappy(&write_request_from_points(points))
}

/// Convert one accepted logical sample into an in-process ingest point. The
/// stored label set carries `__name__` so a PromQL selector matches by name;
/// `SeriesId::compute` hashes the name separately and skips the `__name__`
/// label, so the id is unchanged by its presence (ADR-0005).
fn ingest_point(tenant: &TenantId, point: &LogicalPoint) -> Result<IngestPoint, LaneError> {
    let (metric, point_labels) = match point {
        LogicalPoint::Sample(s) => (&s.metric, &s.labels),
        LogicalPoint::Histogram(h) => (&h.metric, &h.labels),
    };
    let mut labels: Vec<Label> = Vec::with_capacity(point_labels.len() + 1);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.clone(),
    });
    for (name, value) in point_labels {
        labels.push(Label {
            name: name.clone(),
            value: value.clone(),
        });
    }
    let label_set = LabelSet::new(labels)?;
    let series_id = SeriesId::compute(tenant, metric, &label_set)?;
    let ts_ns = point.ts_ms().saturating_mul(1_000_000);
    let value = match point {
        LogicalPoint::Sample(s) => IngestValue::Scalar(Sample {
            ts_ns,
            value: s.value,
        }),
        LogicalPoint::Histogram(h) => IngestValue::Histogram(HistogramSample {
            ts_ns,
            value: histogram_value(h)?,
        }),
    };
    Ok(IngestPoint {
        series_id,
        labels: Arc::new(label_set),
        value,
    })
}

/// The stored form of a validated logical histogram: the same layout
/// [`pb_histogram_from`] puts on the wire, with absolute bucket counts.
fn histogram_value(h: &LogicalHistogram) -> Result<HistogramValue, LaneError> {
    let positive = h.absolute_buckets().ok_or_else(|| {
        LaneError::Generate("a histogram reached ingest without passing validate".to_string())
    })?;
    Ok(HistogramValue {
        scale: h.schema,
        zero_threshold: 0.0,
        sum: Some(h.sum),
        custom_values: None,
        positive_spans: h
            .positive_span_len()
            .map(|length| HistogramSpan { offset: 0, length })
            .into_iter()
            .collect(),
        negative_spans: Vec::new(),
        counts: HistogramCounts::Int {
            zero_count: 0,
            count: h.count,
            positive,
            negative: Vec::new(),
        },
        reset_hint: match h.reset_hint {
            pb_histogram::ResetHint::Unknown => SegResetHint::Unknown,
            pb_histogram::ResetHint::Yes => SegResetHint::Yes,
            pb_histogram::ResetHint::No => SegResetHint::No,
            pb_histogram::ResetHint::Gauge => SegResetHint::Gauge,
        },
    })
}

/// Why a line of a generated stream could not be classified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MalformedReason {
    /// The line does not carry three non-empty tab-separated fields
    /// (timestamp, series, payload).
    MissingField,
    /// The timestamp field is not an `i64` of milliseconds.
    BadTimestamp,
    /// A float payload's (or a native histogram's `sum`) bits are not exactly
    /// sixteen hex digits after `0x`.
    BadFloatBits,
    /// The series field is not `metric{k="v",...}`.
    BadSeries,
    /// The payload is none of the three shapes the encoder writes (`f 0x..`,
    /// `stale`, `h schema=.. count=.. sum=0x.. deltas=..`), a native histogram
    /// whose other fields do not parse, or the line is not UTF-8.
    UnknownPayload,
}

/// One line of a generated stream (the canonical `metrics_gen` encoding,
/// `<ts_ms>\t<metric>{<labels>}\t<payload>`), classified. Every line becomes
/// exactly one record, counted under its kind.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamRecord {
    /// A float payload.
    Scalar(LogicalSample),
    /// A native-histogram payload, sent as a Remote Write 1.0 `Histogram`.
    NativeHistogram(LogicalHistogram),
    /// A staleness marker: a sample whose value is the stale NaN
    /// ([`STALE_MARKER_BITS`]).
    StaleMarker(LogicalSample),
    /// A line that does not parse. `line_no` is 1-based over the whole stream.
    Malformed {
        line_no: u64,
        reason: MalformedReason,
    },
}

/// Exactly sixteen hex digits, the encoder's `{:016x}` bit spelling.
fn parse_hex_bits(hex: &str) -> Option<u64> {
    if hex.len() != 16 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Parse a native-histogram payload body (after `h `).
fn parse_native_histogram(body: &str) -> Result<(i32, u64, u64, Vec<i64>), MalformedReason> {
    let mut fields = body.split(' ');
    let (Some(schema), Some(count), Some(sum), Some(deltas), None) = (
        fields.next().and_then(|f| f.strip_prefix("schema=")),
        fields.next().and_then(|f| f.strip_prefix("count=")),
        fields.next().and_then(|f| f.strip_prefix("sum=0x")),
        fields.next().and_then(|f| f.strip_prefix("deltas=")),
        fields.next(),
    ) else {
        return Err(MalformedReason::UnknownPayload);
    };
    let schema = schema
        .parse::<i32>()
        .map_err(|_| MalformedReason::UnknownPayload)?;
    let count = count
        .parse::<u64>()
        .map_err(|_| MalformedReason::UnknownPayload)?;
    let sum_bits = parse_hex_bits(sum).ok_or(MalformedReason::BadFloatBits)?;
    let deltas = if deltas.is_empty() {
        Vec::new()
    } else {
        deltas
            .split(',')
            .map(|delta| {
                delta
                    .parse::<i64>()
                    .map_err(|_| MalformedReason::UnknownPayload)
            })
            .collect::<Result<Vec<i64>, _>>()?
    };
    Ok((schema, count, sum_bits, deltas))
}

/// Classify one line (without its trailing newline) of a generated stream.
/// `line_no` is the line's 1-based position, carried on a
/// [`StreamRecord::Malformed`] so a bad line can be found.
pub fn classify_line(line_no: u64, line: &str) -> StreamRecord {
    let malformed = |reason| StreamRecord::Malformed { line_no, reason };
    let mut parts = line.splitn(3, '\t');
    let (Some(ts), Some(series), Some(payload)) = (parts.next(), parts.next(), parts.next()) else {
        return malformed(MalformedReason::MissingField);
    };
    if ts.is_empty() || series.is_empty() || payload.is_empty() {
        return malformed(MalformedReason::MissingField);
    }
    let Ok(ts_ms) = ts.parse::<i64>() else {
        return malformed(MalformedReason::BadTimestamp);
    };
    let Some((metric, labels)) = parse_series(series) else {
        return malformed(MalformedReason::BadSeries);
    };
    if let Some(hex) = payload.strip_prefix("f 0x") {
        return match parse_hex_bits(hex) {
            Some(bits) => StreamRecord::Scalar(LogicalSample {
                metric,
                labels,
                ts_ms,
                value: f64::from_bits(bits),
            }),
            None => malformed(MalformedReason::BadFloatBits),
        };
    }
    if payload == "stale" {
        return StreamRecord::StaleMarker(LogicalSample {
            metric,
            labels,
            ts_ms,
            value: f64::from_bits(STALE_MARKER_BITS),
        });
    }
    if let Some(body) = payload.strip_prefix("h ") {
        return match parse_native_histogram(body) {
            Ok((schema, count, sum_bits, positive_deltas)) => {
                StreamRecord::NativeHistogram(LogicalHistogram {
                    metric,
                    labels,
                    ts_ms,
                    schema,
                    count,
                    sum: f64::from_bits(sum_bits),
                    positive_deltas,
                    reset_hint: pb_histogram::ResetHint::Unknown,
                })
            }
            Err(reason) => malformed(reason),
        };
    }
    malformed(MalformedReason::UnknownPayload)
}

/// Per-kind line counts of a replayed stream. Every line is counted under
/// exactly one kind, so `scalar + native_histogram + stale_marker + malformed
/// == generated_lines` holds by construction and [`check_stream_band`] checks
/// it anyway.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCounts {
    /// Lines read from the stream.
    pub generated_lines: u64,
    /// Float lines, offered as samples.
    pub scalar: u64,
    /// Native-histogram lines, offered as histograms.
    pub native_histogram: u64,
    /// Staleness-marker lines, offered as samples.
    pub stale_marker: u64,
    /// Lines that did not parse. Band 4 requires zero.
    pub malformed: u64,
}

impl StreamCounts {
    /// Count one classified line.
    pub fn record(&mut self, record: &StreamRecord) {
        self.generated_lines += 1;
        match record {
            StreamRecord::Scalar(_) => self.scalar += 1,
            StreamRecord::NativeHistogram { .. } => self.native_histogram += 1,
            StreamRecord::StaleMarker { .. } => self.stale_marker += 1,
            StreamRecord::Malformed { .. } => self.malformed += 1,
        }
    }

    /// The four per-kind counts summed.
    pub fn classified(&self) -> u64 {
        self.scalar + self.native_histogram + self.stale_marker + self.malformed
    }
}

/// The classification identity of ADR-0927 band 4 failed: the lines the
/// replay classified do not close against themselves, a per-type count
/// differs from the generator's own, the lines read differ from what the
/// generator emitted, or a line was malformed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "the replayed stream does not close (ADR-0927 band 4): {scalar} scalar + {native_histogram} \
     native histogram + {stale_marker} stale marker + {malformed} malformed = {classified} \
     classified, against {generated_lines} lines read and {emitted_samples} samples the \
     generator emitted, of which {generator_float} float, {generator_native_histogram} native \
     histogram and {generator_stale_marker} stale marker; band 4 requires classified == lines \
     read == emitted, each classified type equal to the generator's, and zero malformed"
)]
pub struct StreamBandViolation {
    pub generated_lines: u64,
    pub scalar: u64,
    pub native_histogram: u64,
    pub stale_marker: u64,
    pub malformed: u64,
    pub classified: u64,
    pub emitted_samples: u64,
    /// The generator's `float_samples`.
    pub generator_float: u64,
    /// The generator's `native_histogram_samples`.
    pub generator_native_histogram: u64,
    /// The generator's `stale_markers`.
    pub generator_stale_marker: u64,
}

/// Check a replayed stream's counts against the generator's own report: the
/// four kinds sum to the lines read, the lines read equal the samples the
/// generator emitted, each of scalar, native histogram and stale marker
/// equals the generator's own count of that type, and no line was malformed.
pub fn check_stream_band(
    counts: &StreamCounts,
    report: &crate::metrics_gen::GenerationReport,
) -> Result<(), StreamBandViolation> {
    let classified = counts.classified();
    if classified == counts.generated_lines
        && counts.generated_lines == report.emitted_samples
        && counts.scalar == report.float_samples
        && counts.native_histogram == report.native_histogram_samples
        && counts.stale_marker == report.stale_markers
        && counts.malformed == 0
    {
        return Ok(());
    }
    Err(StreamBandViolation {
        generated_lines: counts.generated_lines,
        scalar: counts.scalar,
        native_histogram: counts.native_histogram,
        stale_marker: counts.stale_marker,
        malformed: counts.malformed,
        classified,
        emitted_samples: report.emitted_samples,
        generator_float: report.float_samples,
        generator_native_histogram: report.native_histogram_samples,
        generator_stale_marker: report.stale_markers,
    })
}

/// A row's accounting does not close (ADR-0927 band 4): the offering
/// identity, a class's offered count against the stream's lines of that
/// class, or a class's split into accepted, rejected and dropped.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "the sample accounting does not close (ADR-0927 band 4): offering identity {offered} offered \
     samples + {offered_histograms} offered histograms + {excluded_native_histogram} \
     excluded_native_histogram + {excluded_stale_marker} excluded_stale_marker = \
     {offering_total} against {generated_lines} generated lines; {offered} offered samples + \
     {excluded_stale_marker} excluded_stale_marker against {scalar} scalar + {stale_marker} \
     stale marker lines; {offered_histograms} offered histograms + {excluded_native_histogram} \
     excluded_native_histogram against {native_histogram} native histogram lines; {offered} \
     offered against {accepted} accepted + {rejected} rejected + {dropped} dropped samples; \
     {offered_histograms} offered against {accepted_histograms} accepted + \
     {rejected_histograms} rejected + {dropped_histograms} dropped histograms. A silent drop \
     fails the run."
)]
pub struct RowAccountingViolation {
    pub offered: u64,
    pub offered_histograms: u64,
    pub excluded_native_histogram: u64,
    pub excluded_stale_marker: u64,
    /// `offered + offered_histograms + excluded_native_histogram +
    /// excluded_stale_marker`.
    pub offering_total: u64,
    pub generated_lines: u64,
    pub scalar: u64,
    pub stale_marker: u64,
    pub native_histogram: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub dropped: u64,
    pub accepted_histograms: u64,
    pub rejected_histograms: u64,
    pub dropped_histograms: u64,
}

/// Check one row against the stream it replayed, from the row's OWN fields
/// (so a row that drops a count fails even when `counts` still holds it):
/// the offering identity `offered_samples + offered_histograms +
/// excluded_native_histogram + excluded_stale_marker == generated_lines`;
/// `offered_samples + excluded_stale_marker == scalar + stale_marker`;
/// `offered_histograms + excluded_native_histogram == native_histogram`; and
/// each class's offered count equal to its accepted + rejected + dropped.
/// `counts` is the stream's own classification ([`GeneratedStream::finish`]).
pub fn check_row_accounting(
    ingest: &IngestPhase,
    counts: &StreamCounts,
) -> Result<(), RowAccountingViolation> {
    let offering_total = ingest
        .offered_samples
        .saturating_add(ingest.offered_histograms)
        .saturating_add(ingest.excluded_native_histogram)
        .saturating_add(ingest.excluded_stale_marker);
    let split = ingest
        .accepted_samples
        .saturating_add(ingest.rejected_samples)
        .saturating_add(ingest.dropped_samples);
    let histogram_split = ingest
        .accepted_histograms
        .saturating_add(ingest.rejected_histograms)
        .saturating_add(ingest.dropped_histograms);
    if offering_total == counts.generated_lines
        && ingest
            .offered_samples
            .saturating_add(ingest.excluded_stale_marker)
            == counts.scalar.saturating_add(counts.stale_marker)
        && ingest
            .offered_histograms
            .saturating_add(ingest.excluded_native_histogram)
            == counts.native_histogram
        && split == ingest.offered_samples
        && histogram_split == ingest.offered_histograms
    {
        return Ok(());
    }
    Err(RowAccountingViolation {
        offered: ingest.offered_samples,
        offered_histograms: ingest.offered_histograms,
        excluded_native_histogram: ingest.excluded_native_histogram,
        excluded_stale_marker: ingest.excluded_stale_marker,
        offering_total,
        generated_lines: counts.generated_lines,
        scalar: counts.scalar,
        stale_marker: counts.stale_marker,
        native_histogram: counts.native_histogram,
        accepted: ingest.accepted_samples,
        rejected: ingest.rejected_samples,
        dropped: ingest.dropped_samples,
        accepted_histograms: ingest.accepted_histograms,
        rejected_histograms: ingest.rejected_histograms,
        dropped_histograms: ingest.dropped_histograms,
    })
}

/// Marks a histogram record in [`OfferedDigest`]. A sample record starts
/// with its label count, which can never be `u64::MAX`, so the two record
/// kinds never share an encoding.
const DIGEST_HISTOGRAM_TAG: u64 = u64::MAX;

/// Hashes every offered point, in offered order, all integers little-endian
/// and every string length-prefixed, so no two distinct point sequences share
/// an encoding. A sample (a staleness marker included) is its series as
/// sorted label pairs (`__name__` included), `ts_ms`, and the value's
/// `f64::to_bits`. A native histogram is [`DIGEST_HISTOGRAM_TAG`], its
/// series, `ts_ms`, and every field [`pb_histogram_from`] encodes: schema,
/// count, sum bits, zero threshold bits, zero count, reset hint, each
/// positive span, and each positive delta.
#[derive(Clone, Debug, Default)]
pub struct OfferedDigest {
    hasher: blake3::Hasher,
}

impl OfferedDigest {
    /// An empty digest.
    pub fn new() -> Self {
        Self::default()
    }

    fn update_series(&mut self, labels: &[(String, String)]) {
        self.hasher.update(&(labels.len() as u64).to_le_bytes());
        for (name, value) in labels {
            for part in [name, value] {
                self.hasher.update(&(part.len() as u64).to_le_bytes());
                self.hasher.update(part.as_bytes());
            }
        }
    }

    /// Add one offered sample.
    pub fn update(&mut self, sample: &LogicalSample) {
        self.update_series(&sample.canonical_labels());
        self.hasher.update(&sample.ts_ms.to_le_bytes());
        self.hasher.update(&sample.value.to_bits().to_le_bytes());
    }

    /// Add one offered native histogram, by the fields of its wire form.
    pub fn update_histogram(&mut self, histogram: &LogicalHistogram) {
        let wire = pb_histogram_from(histogram);
        self.hasher.update(&DIGEST_HISTOGRAM_TAG.to_le_bytes());
        self.update_series(&histogram.canonical_labels());
        self.hasher.update(&wire.timestamp.to_le_bytes());
        self.hasher.update(&wire.schema.to_le_bytes());
        self.hasher.update(&histogram.count.to_le_bytes());
        self.hasher.update(&wire.sum.to_bits().to_le_bytes());
        self.hasher.update(&wire.zero_threshold.to_bits().to_le_bytes());
        let zero_count = match wire.zero_count {
            Some(pb_histogram::ZeroCount::ZeroCountInt(n)) => n,
            _ => 0,
        };
        self.hasher.update(&zero_count.to_le_bytes());
        self.hasher.update(&wire.reset_hint.to_le_bytes());
        self.hasher
            .update(&(wire.positive_spans.len() as u64).to_le_bytes());
        for span in &wire.positive_spans {
            self.hasher.update(&span.offset.to_le_bytes());
            self.hasher.update(&span.length.to_le_bytes());
        }
        self.hasher
            .update(&(wire.positive_deltas.len() as u64).to_le_bytes());
        for delta in &wire.positive_deltas {
            self.hasher.update(&delta.to_le_bytes());
        }
    }

    /// Add one offered point.
    pub fn update_point(&mut self, point: &LogicalPoint) {
        match point {
            LogicalPoint::Sample(s) => self.update(s),
            LogicalPoint::Histogram(h) => self.update_histogram(h),
        }
    }

    /// The hex digest over every point added so far.
    pub fn finish(&self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }
}

/// A source of points a replay pulls in batches, in stream order, so a
/// replay holds one batch at a time rather than the whole history.
pub trait SampleBatches {
    /// The next at most `max` points (`max` is raised to 1), or `None` once
    /// the stream is exhausted. A returned batch is never empty.
    fn next_batch(&mut self, max: usize) -> Result<Option<Vec<LogicalPoint>>, LaneError>;
    /// Per-kind counts of the lines read so far.
    fn stream_counts(&self) -> StreamCounts;
}

/// The stream line kind a point counts under.
fn count_point(counts: &mut StreamCounts, point: &LogicalPoint) {
    counts.generated_lines += 1;
    match point {
        LogicalPoint::Sample(s) if s.is_stale_marker() => counts.stale_marker += 1,
        LogicalPoint::Sample(_) => counts.scalar += 1,
        LogicalPoint::Histogram(_) => counts.native_histogram += 1,
    }
}

/// A [`SampleBatches`] over samples already in memory, each counted as one
/// line: a staleness marker under `stale_marker`, any other under `scalar`.
pub struct SliceSamples<'s> {
    samples: &'s [LogicalSample],
    pos: usize,
    counts: StreamCounts,
}

impl<'s> SliceSamples<'s> {
    /// Serve `samples` in order.
    pub fn new(samples: &'s [LogicalSample]) -> Self {
        SliceSamples {
            samples,
            pos: 0,
            counts: StreamCounts::default(),
        }
    }
}

impl SampleBatches for SliceSamples<'_> {
    fn next_batch(&mut self, max: usize) -> Result<Option<Vec<LogicalPoint>>, LaneError> {
        let rest = &self.samples[self.pos..];
        if rest.is_empty() {
            return Ok(None);
        }
        let n = max.max(1).min(rest.len());
        self.pos += n;
        let batch: Vec<LogicalPoint> = rest[..n].iter().cloned().map(LogicalPoint::Sample).collect();
        for point in &batch {
            count_point(&mut self.counts, point);
        }
        Ok(Some(batch))
    }

    fn stream_counts(&self) -> StreamCounts {
        self.counts
    }
}

/// A [`SampleBatches`] over points already in memory, each counted as one
/// line of its kind.
pub struct SlicePoints<'s> {
    points: &'s [LogicalPoint],
    pos: usize,
    counts: StreamCounts,
}

impl<'s> SlicePoints<'s> {
    /// Serve `points` in order.
    pub fn new(points: &'s [LogicalPoint]) -> Self {
        SlicePoints {
            points,
            pos: 0,
            counts: StreamCounts::default(),
        }
    }
}

impl SampleBatches for SlicePoints<'_> {
    fn next_batch(&mut self, max: usize) -> Result<Option<Vec<LogicalPoint>>, LaneError> {
        let rest = &self.points[self.pos..];
        if rest.is_empty() {
            return Ok(None);
        }
        let n = max.max(1).min(rest.len());
        self.pos += n;
        for point in &rest[..n] {
            count_point(&mut self.counts, point);
        }
        Ok(Some(rest[..n].to_vec()))
    }

    fn stream_counts(&self) -> StreamCounts {
        self.counts
    }
}

/// The MetricsBench generator as a [`SampleBatches`]: it generates one scrape
/// step at a time into a reused buffer, classifies each line, and queues that
/// step's points: scalars, staleness markers and native histograms, every
/// line but a malformed one. What it holds is bounded by one step's lines
/// plus one batch, whatever the number of steps. Reusing the shipping encoder
/// as the single source keeps this lane from growing a second workload
/// generator (an ADR-0927 rejected alternative).
pub struct GeneratedStream<'a> {
    generator: crate::metrics_gen::Generator<'a>,
    steps: u64,
    tally: crate::metrics_gen::GenerationTally,
    step_bytes: Vec<u8>,
    pending: std::collections::VecDeque<LogicalPoint>,
    counts: StreamCounts,
}

impl<'a> GeneratedStream<'a> {
    /// Stream `steps` scrapes from `generator`, which must not have generated
    /// anything yet.
    pub fn new(generator: crate::metrics_gen::Generator<'a>, steps: u64) -> Self {
        GeneratedStream {
            generator,
            steps,
            tally: crate::metrics_gen::GenerationTally::new(),
            step_bytes: Vec::new(),
            pending: std::collections::VecDeque::new(),
            counts: StreamCounts::default(),
        }
    }

    /// Generate and classify the next step. `false` once every step is done.
    fn advance_step(&mut self) -> Result<bool, LaneError> {
        let step = self.tally.steps_generated();
        if step >= self.steps {
            return Ok(false);
        }
        self.step_bytes.clear();
        self.generator
            .generate_steps_into(step, step + 1, &mut self.step_bytes, &mut self.tally)
            .map_err(|e| LaneError::Generate(e.to_string()))?;
        for raw in self.step_bytes.split_inclusive(|b| *b == b'\n') {
            let raw = raw.strip_suffix(b"\n").unwrap_or(raw);
            let line_no = self.counts.generated_lines + 1;
            let record = match std::str::from_utf8(raw) {
                Ok(line) => classify_line(line_no, line),
                Err(_) => StreamRecord::Malformed {
                    line_no,
                    reason: MalformedReason::UnknownPayload,
                },
            };
            self.counts.record(&record);
            match record {
                StreamRecord::Scalar(sample) | StreamRecord::StaleMarker(sample) => {
                    self.pending.push_back(LogicalPoint::Sample(sample));
                }
                StreamRecord::NativeHistogram(histogram) => {
                    self.pending.push_back(LogicalPoint::Histogram(histogram));
                }
                StreamRecord::Malformed { .. } => {}
            }
        }
        Ok(true)
    }

    /// Close a drained stream: the generator's own report over every step,
    /// and the per-kind counts of every line. Refused before the stream is
    /// exhausted, since the report would then describe samples nobody offered.
    pub fn finish(self) -> Result<(crate::metrics_gen::GenerationReport, StreamCounts), LaneError> {
        if self.tally.steps_generated() != self.steps || !self.pending.is_empty() {
            return Err(LaneError::Generate(format!(
                "the stream was closed after {} of {} steps with {} points unread",
                self.tally.steps_generated(),
                self.steps,
                self.pending.len()
            )));
        }
        let report = self
            .generator
            .finish_report(self.tally)
            .map_err(|e| LaneError::Generate(e.to_string()))?;
        Ok((report, self.counts))
    }
}

impl SampleBatches for GeneratedStream<'_> {
    fn next_batch(&mut self, max: usize) -> Result<Option<Vec<LogicalPoint>>, LaneError> {
        let max = max.max(1);
        while self.pending.len() < max {
            if !self.advance_step()? {
                break;
            }
        }
        if self.pending.is_empty() {
            return Ok(None);
        }
        let n = max.min(self.pending.len());
        Ok(Some(self.pending.drain(..n).collect()))
    }

    fn stream_counts(&self) -> StreamCounts {
        self.counts
    }
}

/// Split a rendered series identity `metric{k="v",k2="v2"}` into its metric
/// name and label pairs. MetricsBench label values carry no commas or quotes,
/// so a simple split is exact for this workload.
fn parse_series(series: &str) -> Option<(String, Vec<(String, String)>)> {
    let open = series.find('{')?;
    let metric = series[..open].to_string();
    let inner = series[open + 1..].strip_suffix('}')?;
    let mut labels = Vec::new();
    if !inner.is_empty() {
        for pair in inner.split(',') {
            let (name, value) = pair.split_once('=')?;
            let value = value.strip_prefix('"')?.strip_suffix('"')?;
            labels.push((name.to_string(), value.to_string()));
        }
    }
    Some((metric, labels))
}

/// Inputs for the in-process Ravel replay.
pub struct RavelReplayConfig {
    /// The backing object store (already constructed from `--store`).
    pub store: Arc<dyn ObjectStoreBackend>,
    /// The `StoreMetrics` handle the store's connector records billed attempts
    /// into, when it has one. `None` for `MemoryStore`.
    pub store_metrics: Option<Arc<StoreMetrics>>,
    /// Whether a request against this backend is billed (`false` for memory).
    pub backend_bills_requests: bool,
    /// Ingest shard count.
    pub shards: u32,
    /// Logical samples per write batch.
    pub batch_size: usize,
    /// Per-batch strict-ack deadline.
    pub ack_timeout_secs: u64,
    /// The tenant to write under.
    pub tenant: TenantId,
}

/// The outcome of a Ravel replay: the report row plus the state a
/// read-your-write pass needs (the wrapped store, tenant, minted tokens, and
/// the newest timestamp written).
pub struct RavelReplayOutcome {
    /// The report row.
    pub result: SystemIngestResult,
    /// The instrumented store the data landed in, so a query reads the same
    /// bytes the writes produced.
    pub store: Arc<dyn ObjectStoreBackend>,
    /// The tenant written under.
    pub tenant: TenantId,
    /// The shard count the replay ran under, so a read-your-write query builds
    /// its catalog over the same shards the writes committed into.
    pub shards: u32,
    /// The commit tokens the strict writes minted, decoded, to pass as
    /// `min_commit_token` for a deterministic read-your-write.
    pub tokens: Vec<CommitToken>,
    /// The newest sample timestamp written, in milliseconds: the instant a
    /// read-your-write query evaluates at. `None` when the stream wrote no
    /// sample (empty or all-rejected): an explicit absence, never `i64::MIN`
    /// masquerading as a real timestamp -- a downstream ms-to-ns conversion of
    /// that sentinel overflows, so the absence must not escape as a number.
    pub max_ts_ms: Option<i64>,
    /// The metric name of the first sample written, the selector a
    /// read-your-write query reads back. `None` when nothing was written.
    pub first_metric: Option<String>,
}

/// Pull points from `source` until `pending` holds `batch_size` valid ones or
/// the stream ends, counting each pulled point as offered and each invalid
/// one as rejected, under its own class (sample or histogram), and adding
/// each pulled point to the offered digest. `on_valid` sees each valid point
/// in stream order and returns what is queued for it.
fn fill_valid<S, T, F>(
    source: &mut S,
    batch_size: usize,
    pending: &mut Vec<T>,
    offer: &mut OfferTally,
    mut on_valid: F,
) -> Result<(), LaneError>
where
    S: SampleBatches + ?Sized,
    F: FnMut(LogicalPoint) -> Result<T, LaneError>,
{
    while pending.len() < batch_size {
        let Some(batch) = source.next_batch(batch_size - pending.len())? else {
            break;
        };
        if batch.is_empty() {
            break;
        }
        for point in batch {
            let is_histogram = matches!(point, LogicalPoint::Histogram(_));
            if is_histogram {
                offer.histograms += 1;
            } else {
                offer.samples += 1;
            }
            offer.digest.update_point(&point);
            match point.validate() {
                Ok(()) => pending.push(on_valid(point)?),
                Err(_) if is_histogram => offer.rejected_histograms += 1,
                Err(_) => offer.rejected_samples += 1,
            }
        }
    }
    Ok(())
}

/// What a replay offered, per class, and the digest over it.
#[derive(Default)]
struct OfferTally {
    samples: u64,
    histograms: u64,
    rejected_samples: u64,
    rejected_histograms: u64,
    digest: OfferedDigest,
}

/// `(samples, histograms)` in a batch: the two classes are counted apart.
fn class_counts<'p>(points: impl IntoIterator<Item = &'p LogicalPoint>) -> (u64, u64) {
    points
        .into_iter()
        .fold((0, 0), |(samples, histograms), point| match point {
            LogicalPoint::Sample(_) => (samples + 1, histograms),
            LogicalPoint::Histogram(_) => (samples, histograms + 1),
        })
}

/// Replay `source` into the in-process Ravel path, pulling one batch at a
/// time so memory is bounded by the batch, not the stream's history. Valid
/// points (samples, staleness markers and native histograms, each through
/// `IngestPoint`) are batched in stream order and written with
/// `WriteMode::Strict`, so each batch's ack means durable and mints commit
/// tokens; invalid points are refused with an exact count. Each class's
/// accounting closes: `offered == accepted + rejected + dropped`, and the row
/// carries the source's [`StreamCounts`].
pub async fn replay_into_ravel<S: SampleBatches + ?Sized>(
    config: &RavelReplayConfig,
    source: &mut S,
) -> Result<RavelReplayOutcome, LaneError> {
    let instrumented = Arc::new(match &config.store_metrics {
        Some(handle) => {
            InstrumentedStore::with_metrics(Arc::clone(&config.store), Arc::clone(handle))
        }
        None => InstrumentedStore::new(Arc::clone(&config.store)),
    });
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let router = Arc::new(IngestRouter::new(
        IngestConfig {
            shard_count: config.shards,
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        ravel_types::Signal::Metrics,
        Arc::clone(&clock),
        Arc::new(PendingAckBudget::new(0)),
    ));

    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);
    let batch_size = config.batch_size.max(1);

    let cpu_before = process_cpu_secs();
    let mut timed = Duration::ZERO;

    let mut pending: Vec<(IngestPoint, LogicalPoint)> = Vec::with_capacity(batch_size);
    let mut offer = OfferTally::default();
    let mut max_ts_ms: Option<i64> = None;
    let mut first_metric: Option<String> = None;
    let mut wire_bytes: u64 = 0;
    let mut ack_latencies_ns: Vec<u64> = Vec::new();
    let mut tokens: Vec<CommitToken> = Vec::new();
    let mut accepted_samples: u64 = 0;
    let mut dropped_samples: u64 = 0;
    let mut accepted_histograms: u64 = 0;
    let mut dropped_histograms: u64 = 0;
    let mut dropped_batches: u64 = 0;
    let mut batches: u64 = 0;

    loop {
        // Client-side admission: valid points (carrying their logical point
        // for the wire encoding) and exact rejections.
        fill_valid(source, batch_size, &mut pending, &mut offer, |logical| {
            let point = ingest_point(&config.tenant, &logical)?;
            let ts_ms = logical.ts_ms();
            max_ts_ms = Some(max_ts_ms.map_or(ts_ms, |m| m.max(ts_ms)));
            first_metric.get_or_insert_with(|| logical.metric().to_string());
            Ok((point, logical))
        })?;
        if pending.is_empty() {
            break;
        }
        let (points, logical): (Vec<IngestPoint>, Vec<LogicalPoint>) = pending.drain(..).unzip();
        // The RW1.0 wire size is encoded OUTSIDE the timed segment (issue #937
        // review finding 5): the in-process path never serializes an RW1.0
        // body to ingest, while the HTTP comparators' encode IS client work
        // inside their own segments.
        wire_bytes += encode_rw1_points(&logical)?.len() as u64;
        let (batch_samples, batch_histograms) = class_counts(&logical);
        drop(logical);
        batches += 1;

        let start = std::time::Instant::now();
        let result = router
            .write_values(
                config.tenant.clone(),
                points,
                WriteMode::Strict,
                ack_deadline,
            )
            .await;
        let took = start.elapsed();
        timed += took;
        match result {
            Ok(receipt) => {
                ack_latencies_ns.push(took.as_nanos() as u64);
                accepted_samples += batch_samples;
                accepted_histograms += batch_histograms;
                tokens.extend(receipt.tokens);
            }
            Err(_) => {
                // The strict write did not durably ack: the batch is dropped,
                // its points neither accepted nor rejected, so the accounting
                // still closes. In process this is rare (a dead shard); over
                // HTTP it is the exhausted-retry case.
                dropped_batches += 1;
                dropped_samples += batch_samples;
                dropped_histograms += batch_histograms;
            }
        }
    }

    let flush_start = std::time::Instant::now();
    router.flush_all().await;
    timed += flush_start.elapsed();
    let elapsed_secs = timed.as_secs_f64().max(1e-9);
    let cpu_secs = match (cpu_before, process_cpu_secs()) {
        (Some(before), Some(after)) => Some((after - before).max(0.0)),
        _ => None,
    };

    let snapshot = metrics.snapshot();
    let objects = list_all(store.as_ref(), "").await.unwrap_or_default();
    let stored_bytes: u64 = objects.iter().map(|o| o.size).sum();
    let object_count = objects.len() as u64;
    let logical_bytes = accepted_samples * LOGICAL_BYTES_PER_SAMPLE;
    let write_amplification = if logical_bytes == 0 {
        0.0
    } else {
        stored_bytes as f64 / logical_bytes as f64
    };
    let stored_bytes_per_sample = if accepted_samples == 0 {
        0.0
    } else {
        stored_bytes as f64 / accepted_samples as f64
    };

    let stream_counts = source.stream_counts();
    let ingest = IngestPhase {
        stream_counts,
        excluded_native_histogram: 0,
        excluded_stale_marker: 0,
        offered_samples: offer.samples,
        offered_digest: offer.digest.finish(),
        accepted_samples,
        rejected_samples: offer.rejected_samples,
        offered_histograms: offer.histograms,
        accepted_histograms,
        rejected_histograms: offer.rejected_histograms,
        dropped_histograms,
        // No HTTP status and no retry loop on the in-process path.
        histograms_rejected_by_status: BTreeMap::new(),
        retry_exhausted_histograms: 0,
        dropped_samples,
        dropped_batches,
        rejected_by_status: BTreeMap::new(),
        retry_exhausted_samples: 0,
        batches,
        retries: snapshot.put.attempts.saturating_sub(snapshot.put.calls),
        elapsed_secs,
        elapsed_basis: ELAPSED_BASIS_TIMED_SEGMENTS.to_string(),
        logical_points_per_sec: accepted_samples as f64 / elapsed_secs,
        wire_bytes,
        wire_bytes_per_sec: wire_bytes as f64 / elapsed_secs,
        ack_latency_ms: LatencyReport::from_nanos(ack_latencies_ns),
        peak_rss_bytes: peak_rss_bytes(),
        cpu_secs,
    };
    let storage = StorageAccounting {
        object_count,
        stored_bytes,
        put_count: snapshot.put.calls,
        put_bytes: snapshot.put.bytes,
        stored_bytes_per_sample,
        write_amplification,
        backend_bills_requests: config.backend_bills_requests,
    };

    let encoded_tokens: Vec<String> = tokens.iter().map(CommitToken::encode).collect();
    let result =
        SystemIngestResult::ravel_row("in-process".to_string(), ingest, encoded_tokens, storage);

    Ok(RavelReplayOutcome {
        result,
        store,
        tenant: config.tenant.clone(),
        shards: config.shards,
        tokens,
        max_ts_ms,
        first_metric,
    })
}

/// Run the post-ingest read-your-write query phase after a Ravel replay: build
/// a catalog and query engine over the same store the writes landed in, pass
/// the minted commit tokens as `min_commit_token`, and evaluate `query` as an
/// instant query at the replay's newest sample timestamp. Because the tokens
/// resolve the exact committed segments, the read sees the just-written rows
/// deterministically without sleeping past the flush delay (ADR-0927 decision
/// 3). Recorded as its own phase, separate from ingest (decision 9).
///
/// Errors with [`LaneError::Query`] if the replay wrote no sample (its
/// `max_ts_ms` is absent), so the ms-to-ns sentinel overflow can never reach
/// the query path.
pub async fn query_after_replay(
    outcome: &RavelReplayOutcome,
    query: &str,
) -> Result<SystemQueryResult, LaneError> {
    let eval_ts_ms = outcome.max_ts_ms.ok_or_else(|| {
        LaneError::Query(
            "the replay wrote no sample, so there is no timestamp to read at".to_string(),
        )
    })?;

    let catalog = Arc::new(
        Catalog::new(
            Arc::clone(&outcome.store),
            CatalogConfig {
                shard_count: outcome.shards,
                ..CatalogConfig::default()
            },
        )
        .map_err(|e| LaneError::Query(e.to_string()))?,
    );
    let engine = QueryEngine::new(catalog, Arc::clone(&outcome.store), EngineConfig::default());

    let now_ns = SystemClock.now_ns();
    let wall_start = std::time::Instant::now();
    let (value, _coverage) = engine
        .instant(
            outcome.tenant.hash(),
            query,
            eval_ts_ms,
            &outcome.tokens,
            now_ns,
            Duration::from_secs(30),
        )
        .await
        .map_err(|e| LaneError::Query(e.to_string()))?;
    let elapsed_secs = wall_start.elapsed().as_secs_f64();

    // A non-vector result is a typed error, never matched_series: 0 -- a
    // scalar evaluation and "the token-bound read matched no series" must not
    // be the same figure (the module's ABSENT-not-zero rule). The binary
    // passes a bare metric name so a vector is produced there; this guards
    // every other caller of the public query_after_replay.
    let matched_series = match value {
        Value::Vector(v) => v.len() as u64,
        other => {
            return Err(LaneError::Query(format!(
                "the read-your-write query produced a non-vector result ({});                  matched_series is defined only for vector evaluations",
                other.type_name()
            )));
        }
    };

    Ok(SystemQueryResult {
        system: "ravel".to_string(),
        query: query.to_string(),
        eval_ts_ms,
        min_commit_tokens: outcome.tokens.iter().map(CommitToken::encode).collect(),
        matched_series,
        elapsed_secs,
    })
}

/// Inputs for a replay over portable Remote Write 1.0. The endpoint comes
/// from config: there is no hardcoded host.
pub struct HttpReplayConfig {
    /// The system name for the row: `ravel`, `prometheus`, `victoriametrics`,
    /// `mimir`.
    pub system: String,
    /// The full `/api/v1/write` URL supplied by the caller.
    pub endpoint: String,
    /// What a 2xx from this receiver means. [`AckSemantics::DurableOnAck`]
    /// for `ravel-server`, whose row then records `last_commit_token`.
    pub ack_semantics: AckSemantics,
    /// Sent as `Authorization: Bearer <token>` when set. Never written into
    /// the row: neither `endpoint` nor `client_behavior` carries it
    /// (`tests::the_portable_ravel_row_sends_the_bearer_and_keeps_the_last_commit_token`).
    pub bearer_token: Option<String>,
    /// Logical points (samples and native histograms) per RW1.0 request.
    pub batch_size: usize,
    /// Retries on a retryable response (429 or 5xx) before the batch is
    /// dropped.
    pub max_retries: u32,
    /// Per-request timeout, bounding the COMPLETE operation including the body
    /// read (ADR-0927 decision 6: a client that bounds only the request records
    /// an expiry as a generic transport failure).
    pub timeout_secs: u64,
}

/// The fixed backoff between RW1.0 retries. A constant rather than a computed
/// estimate: `WriteError`/an HTTP response carries no per-error hint here, the
/// same choice `remote_write.rs` makes with its fixed `RETRY_AFTER_SECONDS`.
const HTTP_RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// The response header `ravel-server` puts the strict write's commit tokens
/// in (`services/ravel-server/src/remote_write.rs`).
pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";

/// Replay `source` into a receiver over Remote Write 1.0, pulling one batch
/// at a time as [`replay_into_ravel`] does. Valid points are batched in
/// stream order, encoded as snappy protobuf, and POSTed to the configured
/// endpoint (with `Authorization: Bearer` when `bearer_token` is set); each
/// batch is retried on a 429/5xx or a transport error up to `max_retries`,
/// then dropped under `retry_exhausted_samples` and
/// `retry_exhausted_histograms`, and a batch answered with any other non-2xx
/// is dropped at once under `rejected_by_status` and
/// `histograms_rejected_by_status`, keyed by that status. A comparator row is
/// [`AckSemantics::Buffered`]: a Remote Write 2xx from a comparator means
/// accepted-into-a-buffer, a durability the client cannot observe, so it is
/// never placed in a pooled column with Ravel's durable ack. A
/// [`AckSemantics::DurableOnAck`] config (the portable Ravel row) records the
/// `x-ravel-commit-token` of the last acknowledged request that carried one.
///
/// The receiver's object store is not the bench's, so this row carries no
/// [`StorageAccounting`], and its CPU/RSS are the remote process's, so they are
/// `None`: absent, never a zero.
pub async fn replay_over_http<S: SampleBatches + ?Sized>(
    config: &HttpReplayConfig,
    source: &mut S,
) -> Result<SystemIngestResult, LaneError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs.max(1)))
        .build()
        .map_err(|e| transport_setup_error(format!("building the HTTP client failed: {e}")))?;

    let mut pending: Vec<LogicalPoint> = Vec::new();
    let mut offer = OfferTally::default();
    let mut timed = Duration::ZERO;

    let batch_size = config.batch_size.max(1);
    let mut ack_latencies_ns: Vec<u64> = Vec::new();
    let mut accepted_samples: u64 = 0;
    let mut dropped_samples: u64 = 0;
    let mut accepted_histograms: u64 = 0;
    let mut dropped_histograms: u64 = 0;
    let mut dropped_batches: u64 = 0;
    let mut batches: u64 = 0;
    let mut retries: u64 = 0;
    let mut wire_bytes: u64 = 0;
    let mut rejected_by_status: BTreeMap<u16, u64> = BTreeMap::new();
    let mut histograms_rejected_by_status: BTreeMap<u16, u64> = BTreeMap::new();
    let mut retry_exhausted_samples: u64 = 0;
    let mut retry_exhausted_histograms: u64 = 0;
    let mut last_commit_token: Option<String> = None;

    loop {
        fill_valid(source, batch_size, &mut pending, &mut offer, Ok)?;
        if pending.is_empty() {
            break;
        }
        let chunk = std::mem::take(&mut pending);
        let segment_start = std::time::Instant::now();
        batches += 1;
        let body = encode_rw1_points(&chunk)?;
        wire_bytes += body.len() as u64;
        let (batch_samples, batch_histograms) = class_counts(&chunk);

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let outcome = loop {
            let mut request = client
                .post(&config.endpoint)
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .header("X-Prometheus-Remote-Write-Version", "0.1.0");
            if let Some(token) = &config.bearer_token {
                request = request.bearer_auth(token);
            }
            let sent = request.body(body.clone()).send().await;
            match sent {
                Ok(resp) if resp.status().is_success() => {
                    break Ok(resp
                        .headers()
                        .get(COMMIT_TOKEN_HEADER)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string));
                }
                Ok(resp) if is_retryable(resp.status()) && attempt < config.max_retries => {
                    attempt += 1;
                    retries += 1;
                    tokio::time::sleep(HTTP_RETRY_BACKOFF).await;
                }
                Ok(resp) if is_retryable(resp.status()) => {
                    break Err(BatchFailure::RetriesExhausted);
                }
                Ok(resp) => break Err(BatchFailure::Status(resp.status().as_u16())),
                Err(_) if attempt < config.max_retries => {
                    attempt += 1;
                    retries += 1;
                    tokio::time::sleep(HTTP_RETRY_BACKOFF).await;
                }
                Err(_) => break Err(BatchFailure::RetriesExhausted),
            }
        };
        match outcome {
            Ok(token) => {
                ack_latencies_ns.push(start.elapsed().as_nanos() as u64);
                accepted_samples += batch_samples;
                accepted_histograms += batch_histograms;
                if token.is_some() {
                    last_commit_token = token;
                }
            }
            Err(failure) => {
                dropped_batches += 1;
                dropped_samples += batch_samples;
                dropped_histograms += batch_histograms;
                match failure {
                    BatchFailure::Status(code) => {
                        if batch_samples > 0 {
                            *rejected_by_status.entry(code).or_insert(0) += batch_samples;
                        }
                        if batch_histograms > 0 {
                            *histograms_rejected_by_status.entry(code).or_insert(0) +=
                                batch_histograms;
                        }
                    }
                    BatchFailure::RetriesExhausted => {
                        retry_exhausted_samples += batch_samples;
                        retry_exhausted_histograms += batch_histograms;
                    }
                }
            }
        }
        timed += segment_start.elapsed();
    }

    let elapsed_secs = timed.as_secs_f64().max(1e-9);
    let stream_counts = source.stream_counts();
    let ingest = IngestPhase {
        stream_counts,
        excluded_native_histogram: 0,
        excluded_stale_marker: 0,
        offered_samples: offer.samples,
        offered_digest: offer.digest.finish(),
        accepted_samples,
        rejected_samples: offer.rejected_samples,
        offered_histograms: offer.histograms,
        accepted_histograms,
        rejected_histograms: offer.rejected_histograms,
        dropped_histograms,
        histograms_rejected_by_status,
        retry_exhausted_histograms,
        dropped_samples,
        dropped_batches,
        rejected_by_status,
        retry_exhausted_samples,
        batches,
        retries,
        elapsed_secs,
        elapsed_basis: ELAPSED_BASIS_TIMED_SEGMENTS.to_string(),
        logical_points_per_sec: accepted_samples as f64 / elapsed_secs,
        wire_bytes,
        wire_bytes_per_sec: wire_bytes as f64 / elapsed_secs,
        ack_latency_ms: LatencyReport::from_nanos(ack_latencies_ns),
        // The receiver runs in another process: its CPU and RSS are not the
        // bench's to report. Absent, never a zero.
        peak_rss_bytes: None,
        cpu_secs: None,
    };
    let client_behavior = format!(
        "reqwest RW1.0 sender: snappy protobuf, {batch} logical points (samples and native \
         histograms)/request sent sequentially (backpressure by awaiting each POST), {auth}, \
         retry on 429/5xx up to {retries} times with a fixed {backoff_ms} ms backoff then drop the \
         batch; {timeout} s per-request timeout bounds the complete operation including the body \
         read",
        batch = batch_size,
        auth = if config.bearer_token.is_some() {
            "bearer auth"
        } else {
            "no auth header"
        },
        retries = config.max_retries,
        backoff_ms = HTTP_RETRY_BACKOFF.as_millis(),
        timeout = config.timeout_secs.max(1),
    );
    Ok(match config.ack_semantics {
        AckSemantics::DurableOnAck => SystemIngestResult::durable_http_row(
            config.system.clone(),
            config.endpoint.clone(),
            client_behavior,
            ingest,
            last_commit_token,
        ),
        AckSemantics::Buffered => SystemIngestResult::buffered_http_row(
            config.system.clone(),
            config.endpoint.clone(),
            client_behavior,
            ingest,
        ),
    })
}

/// Why a comparator batch was dropped.
enum BatchFailure {
    /// A non-retryable non-2xx response, with its status.
    Status(u16),
    /// Still failing (a retryable status or a transport error) after
    /// `max_retries` retries.
    RetriesExhausted,
}

/// Whether a response status warrants a Remote Write retry: 429 (too many
/// requests) or any 5xx (server error). A 4xx other than 429 is a permanent
/// rejection of that batch, not a transient failure.
fn is_retryable(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// Peak resident set of the bench process, in bytes, from `/proc/self/status`'s
/// `VmHWM`. `None` off Linux, where the harness has no portable reader.
#[cfg(target_os = "linux")]
fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

/// CPU seconds the bench process has spent, from `/proc/self/stat`'s `utime`
/// and `stime`. USER_HZ is 100 on effectively every Linux target, which this
/// assumes; the figure is a diagnostic for the in-process run, not a gated
/// count. `None` off Linux.
#[cfg(target_os = "linux")]
fn process_cpu_secs() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // The comm field (index 2) is wrapped in parens and may itself contain
    // spaces or parens; split after the LAST ')', so the numeric fields align
    // regardless of the process name.
    let close = stat.rfind(')')?;
    let rest = stat.get(close + 2..)?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // After the comm field, `rest` begins at field 3 (state). utime is field 14
    // (index 11 here), stime is field 15 (index 12).
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / 100.0)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_secs() -> Option<f64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use ravel_object_store::memory::MemoryStore;

    /// A latency report with a chosen p99, so a report fixture is a number a
    /// reader can check by hand.
    fn latency_with_p99(p99: f64) -> LatencyReport {
        LatencyReport {
            p50: p99 / 2.0,
            p95: p99 * 0.9,
            p99,
            max: p99,
            count: 1,
        }
    }

    fn ingest_phase(p99: f64) -> IngestPhase {
        IngestPhase {
            stream_counts: StreamCounts {
                generated_lines: 1,
                scalar: 1,
                ..StreamCounts::default()
            },
            excluded_native_histogram: 0,
            excluded_stale_marker: 0,
            offered_samples: 1,
            offered_digest: String::new(),
            accepted_samples: 1,
            rejected_samples: 0,
            offered_histograms: 0,
            accepted_histograms: 0,
            rejected_histograms: 0,
            dropped_histograms: 0,
            histograms_rejected_by_status: BTreeMap::new(),
            retry_exhausted_histograms: 0,
            dropped_samples: 0,
            rejected_by_status: BTreeMap::new(),
            retry_exhausted_samples: 0,
            dropped_batches: 0,
            batches: 1,
            retries: 0,
            elapsed_secs: 1.0,
            elapsed_basis: ELAPSED_BASIS_TIMED_SEGMENTS.to_string(),
            logical_points_per_sec: 1.0,
            wire_bytes: 10,
            wire_bytes_per_sec: 10.0,
            ack_latency_ms: latency_with_p99(p99),
            peak_rss_bytes: None,
            cpu_secs: None,
        }
    }

    fn storage_fixture() -> StorageAccounting {
        StorageAccounting {
            object_count: 2,
            stored_bytes: 100,
            put_count: 2,
            put_bytes: 100,
            stored_bytes_per_sample: 100.0,
            write_amplification: 6.25,
            backend_bills_requests: false,
        }
    }

    /// ACCEPTANCE TEST (ADR-0927 decision 3, issue #937 deliverable 4): a
    /// strict-ack (durable-on-ack) row and a buffered-ack row cannot be
    /// conflated. A report that collapses them fails.
    ///
    /// Each row carries its acknowledgement meaning as DATA, and Ravel's row
    /// carries the commit tokens while a buffered row carries none (absent, not
    /// an empty vec). The mechanical guard is [`MetricsIngestReport::
    /// pooled_ack_p99_ms`]: pooling a durable-on-ack latency with a buffered one
    /// is refused, so a single ack figure never silently spans two ack meanings.
    ///
    /// TO SEE THIS TEST FAIL by conflating the ack kinds, flip the
    /// `ack_semantics: AckSemantics::DurableOnAck` line in
    /// [`SystemIngestResult::ravel_row`] to `AckSemantics::Buffered`: both rows
    /// then share `Buffered`, `max_ack_p99_ms(&["ravel", "prometheus"])`
    /// returns `Ok` instead of the `AckConflation` error, and the
    /// `expect_err` below panics.
    #[test]
    fn strict_ack_is_reported_separately_from_buffered() {
        let ravel = SystemIngestResult::ravel_row(
            "in-process".to_string(),
            ingest_phase(3.0),
            vec!["tok-shard-0".to_string()],
            storage_fixture(),
        );
        let prometheus = SystemIngestResult::buffered_http_row(
            "prometheus",
            "http://prom.example:9090/api/v1/write",
            "reqwest RW1.0 sender",
            ingest_phase(9.0),
        );
        let report = MetricsIngestReport::new(vec![ravel, prometheus]);

        // Each row states its own ack meaning; it is not inferred from a zero
        // or a missing field.
        assert_eq!(
            report.row("ravel").expect("ravel row").ack_semantics,
            AckSemantics::DurableOnAck,
            "Ravel's strict Remote Write surface is durable-on-ack"
        );
        assert_eq!(
            report.row("prometheus").expect("prom row").ack_semantics,
            AckSemantics::Buffered,
            "a Prometheus Remote Write 2xx is a buffered ack"
        );

        // Ravel records commit tokens; a buffered system has NONE -- absent,
        // never an empty vec standing in for "buffered mints no token".
        assert_eq!(
            report.row("ravel").expect("ravel row").commit_tokens,
            Some(vec!["tok-shard-0".to_string()]),
            "the durable-on-ack row carries its commit tokens"
        );
        assert_eq!(
            report.row("prometheus").expect("prom row").commit_tokens,
            None,
            "a buffered row carries no commit tokens: absent, not empty"
        );

        // THE CONFLATION GUARD: a durable-on-ack row and a buffered row must not
        // pool into one ack-latency figure. The pool is refused, naming both
        // ack meanings, so the two cannot be collapsed silently.
        let err = report
            .max_ack_p99_ms(&["ravel", "prometheus"])
            .expect_err("a durable-on-ack and a buffered row must not pool into one ack figure");
        let kinds: BTreeSet<AckSemantics> = err.systems.iter().map(|(_, k)| *k).collect();
        assert_eq!(
            kinds,
            BTreeSet::from([AckSemantics::DurableOnAck, AckSemantics::Buffered]),
            "the refusal must name both ack meanings, proving they were not conflated"
        );

        // A single-ack-kind pool IS admissible: the guard refuses a MIX, not
        // every pool. Without this, the test above would pass against a report
        // that refused every pool unconditionally, which is not "kept separate".
        assert_eq!(
            report
                .max_ack_p99_ms(&["ravel"])
                .expect("one durable-on-ack row pools fine"),
            Some(3.0)
        );
        assert_eq!(
            report
                .max_ack_p99_ms(&["prometheus"])
                .expect("one buffered row pools fine"),
            Some(9.0)
        );

        // The two ack meanings survive a JSON round trip as data, so a reader of
        // the artifact sees the distinction without the in-memory types.
        let json = serde_json::to_value(&report).expect("serialize");
        let ravel_ack = json["systems"][0]["ack_semantics"].as_str();
        let prom_ack = json["systems"][1]["ack_semantics"].as_str();
        assert_eq!(ravel_ack, Some("durable_on_ack"));
        assert_eq!(prom_ack, Some("buffered"));
        assert_ne!(
            ravel_ack, prom_ack,
            "the ack meanings must not collapse in JSON"
        );
    }

    /// A base timestamp near now (milliseconds), so a strict write's samples sit
    /// inside a plausible ingest window and a read-your-write query's event-time
    /// range covers them.
    fn base_ts_ms() -> i64 {
        (SystemClock.now_ns() / 1_000_000) - 60_000
    }

    /// Build N valid gauge samples across `series` series at 15 s steps.
    fn valid_stream(n: usize, series: usize) -> Vec<LogicalSample> {
        let base = base_ts_ms();
        (0..n)
            .map(|i| LogicalSample {
                metric: "mb_replay_gauge".to_string(),
                labels: vec![(
                    "instance".to_string(),
                    format!("mb-instance-{}", i % series.max(1)),
                )],
                ts_ms: base + (i as i64) * 15_000,
                value: (i as f64) + 0.5,
            })
            .collect()
    }

    fn memory_config(tenant: &str, batch_size: usize) -> RavelReplayConfig {
        RavelReplayConfig {
            store: Arc::new(MemoryStore::new()),
            store_metrics: None,
            backend_bills_requests: false,
            shards: 2,
            batch_size,
            ack_timeout_secs: 20,
            tenant: TenantId::new(tenant),
        }
    }

    /// REPLAY FIXTURE (exact figures): N known valid samples ingest into Ravel
    /// with `accepted == N` exactly and `rejected == 0`. The accounting closes.
    #[tokio::test]
    async fn replay_of_n_valid_samples_accepts_exactly_n_and_rejects_zero() {
        let n = 40usize;
        let stream = valid_stream(n, 8);
        let cfg = memory_config("mb-replay-accept", 8);
        let outcome = replay_into_ravel(&cfg, &mut SliceSamples::new(&stream))
            .await
            .expect("replay");
        let ing = &outcome.result.ingest;

        assert_eq!(ing.offered_samples, n as u64, "exact offered count");
        assert_eq!(
            ing.accepted_samples, n as u64,
            "every valid sample accepted"
        );
        assert_eq!(ing.rejected_samples, 0, "no valid sample is rejected");
        assert_eq!(ing.dropped_samples, 0, "no batch is dropped in process");
        assert_eq!(ing.dropped_batches, 0);
        assert_eq!(
            ing.accepted_samples + ing.rejected_samples + ing.dropped_samples,
            ing.offered_samples,
            "the accounting must close exactly (ADR-0927 band 4)"
        );
        // 40 samples at batch 8 is exactly 5 batches.
        assert_eq!(ing.batches, 5, "exact batch count");

        // The in-process row says so, so it is never read as the portable
        // Ravel row (ADR-0927 2026-10-26 amendment A).
        assert_eq!(outcome.result.system, "ravel");
        assert_eq!(outcome.result.endpoint, "in-process");
        assert_eq!(outcome.result.last_commit_token, None);

        // Durable-on-ack: one commit token per shard flushed, and the row is
        // tagged strict with tokens recorded.
        assert_eq!(outcome.result.ack_semantics, AckSemantics::DurableOnAck);
        let tokens = outcome
            .result
            .commit_tokens
            .as_ref()
            .expect("strict row records commit tokens");
        assert!(!tokens.is_empty(), "a strict ack mints at least one token");

        // Diagnostic storage accounting is present and non-empty for the
        // in-process path, and marked not-billable on MemoryStore.
        let storage = outcome.result.storage.as_ref().expect("storage present");
        assert!(
            storage.object_count > 0,
            "the run wrote at least one object"
        );
        assert!(storage.put_count > 0, "the run issued at least one PUT");
        assert!(storage.stored_bytes > 0);
        assert!(
            !storage.backend_bills_requests,
            "MemoryStore requests are free: the explicit not-billable marker, not a zero"
        );
    }

    /// REJECTION FIXTURE (exact figures): a stream mixing valid and invalid
    /// samples rejects exactly the invalid ones, and every valid sample is
    /// accepted. The rejects are lane-controlled, so the count is exact.
    #[tokio::test]
    async fn a_mixed_stream_rejects_exactly_the_invalid_samples() {
        let base = base_ts_ms();
        let mut stream = valid_stream(10, 4);
        // Three deterministic rejects, one of each reason.
        stream.push(LogicalSample {
            metric: String::new(), // EmptyMetricName
            labels: vec![("instance".to_string(), "x".to_string())],
            ts_ms: base,
            value: 1.0,
        });
        stream.push(LogicalSample {
            metric: "mb_replay_gauge".to_string(),
            labels: vec![(String::new(), "x".to_string())], // EmptyLabelName
            ts_ms: base,
            value: 1.0,
        });
        stream.push(LogicalSample {
            metric: "mb_replay_gauge".to_string(),
            labels: vec![("instance".to_string(), "y".to_string())],
            ts_ms: base,
            value: f64::NAN, // NonFiniteValue
        });

        let cfg = memory_config("mb-replay-reject", 8);
        let outcome = replay_into_ravel(&cfg, &mut SliceSamples::new(&stream))
            .await
            .expect("replay");
        let ing = &outcome.result.ingest;

        assert_eq!(ing.offered_samples, 13, "10 valid + 3 invalid");
        assert_eq!(ing.rejected_samples, 3, "exactly the three invalid samples");
        assert_eq!(ing.accepted_samples, 10, "every valid sample accepted");
        assert_eq!(ing.dropped_samples, 0);
        assert_eq!(
            ing.accepted_samples + ing.rejected_samples + ing.dropped_samples,
            ing.offered_samples,
            "the accounting must close exactly"
        );
    }

    /// The three client-side rejection reasons are pinned individually, so a
    /// change to one rule does not silently move the reject total.
    #[test]
    fn validation_names_each_rejection_reason() {
        assert_eq!(
            LogicalSample {
                metric: String::new(),
                labels: vec![],
                ts_ms: 0,
                value: 1.0,
            }
            .validate(),
            Err(RejectionReason::EmptyMetricName)
        );
        assert_eq!(
            LogicalSample {
                metric: "m".to_string(),
                labels: vec![(String::new(), "v".to_string())],
                ts_ms: 0,
                value: 1.0,
            }
            .validate(),
            Err(RejectionReason::EmptyLabelName)
        );
        assert_eq!(
            LogicalSample {
                metric: "m".to_string(),
                labels: vec![],
                ts_ms: 0,
                value: f64::INFINITY,
            }
            .validate(),
            Err(RejectionReason::NonFiniteValue)
        );
        assert_eq!(
            LogicalSample {
                metric: "m".to_string(),
                labels: vec![("a".to_string(), "b".to_string())],
                ts_ms: 0,
                value: 1.0,
            }
            .validate(),
            Ok(())
        );
    }

    /// The stale marker's exact bit pattern is the one non-finite value the
    /// lane admits. The canonical quiet NaN and both infinities are still
    /// refused.
    ///
    /// TO SEE THIS FAIL: drop the `&& self.value.to_bits() !=
    /// STALE_MARKER_BITS` clause in `LogicalSample::validate` (exempt
    /// nothing; the marker is refused), or replace the condition with
    /// `!self.value.is_finite() && !self.value.is_nan()` (exempt every NaN;
    /// 0x7ff8000000000000 is accepted).
    #[test]
    fn validate_exempts_exactly_the_stale_marker_bits() {
        let with_bits = |bits: u64| LogicalSample {
            metric: "m".to_string(),
            labels: vec![],
            ts_ms: 0,
            value: f64::from_bits(bits),
        };
        assert_eq!(with_bits(STALE_MARKER_BITS).validate(), Ok(()));
        assert!(with_bits(STALE_MARKER_BITS).is_stale_marker());
        for refused in [
            0x7ff8_0000_0000_0000,
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
        ] {
            assert_eq!(
                with_bits(refused).validate(),
                Err(RejectionReason::NonFiniteValue),
                "{refused:#018x}"
            );
        }
    }

    fn histogram(ts_ms: i64, deltas: Vec<i64>) -> LogicalHistogram {
        // The absolute buckets' total; an invalid (negative) bucket vector
        // gets count 0.
        let mut running = 0i64;
        let total: i64 = deltas
            .iter()
            .map(|d| {
                running += d;
                running
            })
            .sum();
        let count = u64::try_from(total).unwrap_or(0);
        LogicalHistogram {
            metric: "mb_native".to_string(),
            labels: vec![("instance".to_string(), "a".to_string())],
            ts_ms,
            schema: 2,
            count,
            sum: 12.5,
            positive_deltas: deltas,
            reset_hint: pb_histogram::ResetHint::Unknown,
        }
    }

    /// A histogram whose deltas go negative, or whose buckets do not sum to
    /// its count, is refused before sending.
    #[test]
    fn histogram_validation_refuses_inconsistent_buckets() {
        assert_eq!(histogram(0, vec![1, 2, -1]).validate(), Ok(()));
        assert_eq!(
            histogram(0, vec![1, -2]).validate(),
            Err(RejectionReason::InvalidHistogram)
        );
        let mut miscounted = histogram(0, vec![1, 2]);
        miscounted.count += 1;
        assert_eq!(
            miscounted.validate(),
            Err(RejectionReason::InvalidHistogram)
        );
        let mut unnamed = histogram(0, vec![1]);
        unnamed.metric.clear();
        assert_eq!(unnamed.validate(), Err(RejectionReason::EmptyMetricName));
    }

    /// The RW1.0 body carries a native histogram in `histograms` with the
    /// shape the lane promises, decoded back with the same prost types: one
    /// span `{0, deltas.len()}`, integer count, zero threshold 0, zero count
    /// 0, the deltas verbatim, the reset hint, and no float samples. A stale
    /// marker in the same body keeps its bit pattern.
    #[test]
    fn a_native_histogram_decodes_back_with_the_promised_wire_shape() {
        let h = histogram(15_000, vec![3, 1, -2, 0]);
        let stale = LogicalSample {
            metric: "mb_gauge".to_string(),
            labels: vec![],
            ts_ms: 15_000,
            value: f64::from_bits(STALE_MARKER_BITS),
        };
        let body = encode_rw1_points(&[
            LogicalPoint::Histogram(h.clone()),
            LogicalPoint::Sample(stale),
        ])
        .expect("encode");
        let raw = snap::raw::Decoder::new()
            .decompress_vec(&body)
            .expect("snappy");
        let req = WriteRequest::decode(raw.as_slice()).expect("prost decode");
        assert_eq!(req.timeseries.len(), 2);
        let hist_ts = req
            .timeseries
            .iter()
            .find(|ts| ts.labels.iter().any(|l| l.value == "mb_native"))
            .expect("histogram series");
        assert!(hist_ts.samples.is_empty(), "never folded into samples");
        assert_eq!(hist_ts.histograms.len(), 1);
        let wire = &hist_ts.histograms[0];
        assert_eq!(wire.count, Some(pb_histogram::Count::CountInt(h.count)));
        assert_eq!(wire.sum.to_bits(), h.sum.to_bits());
        assert_eq!(wire.schema, 2);
        assert_eq!(wire.zero_threshold.to_bits(), 0.0f64.to_bits());
        assert_eq!(
            wire.zero_count,
            Some(pb_histogram::ZeroCount::ZeroCountInt(0))
        );
        assert_eq!(
            wire.positive_spans,
            vec![PbBucketSpan {
                offset: 0,
                length: 4
            }]
        );
        assert_eq!(wire.positive_deltas, vec![3, 1, -2, 0]);
        assert!(wire.positive_counts.is_empty() && wire.negative_spans.is_empty());
        assert!(wire.negative_deltas.is_empty() && wire.custom_values.is_empty());
        assert_eq!(wire.reset_hint, pb_histogram::ResetHint::Unknown as i32);
        assert_eq!(wire.timestamp, 15_000);

        let stale_ts = req
            .timeseries
            .iter()
            .find(|ts| ts.labels.iter().any(|l| l.value == "mb_gauge"))
            .expect("stale series");
        assert_eq!(stale_ts.samples.len(), 1);
        assert_eq!(stale_ts.samples[0].value.to_bits(), STALE_MARKER_BITS);
    }

    /// Native histograms are their own offered class: a replay of samples
    /// and histograms counts each under its own figures, and the scalar
    /// `offered_samples` holds the samples alone. The in-process path
    /// accepts both kinds through `IngestPoint`.
    ///
    /// TO SEE THIS FAIL: in `fill_valid`, count every point under
    /// `offer.samples` (drop the `if is_histogram` branch); `offered_samples`
    /// becomes 7 and the first assertion fails.
    #[tokio::test]
    async fn native_histograms_are_their_own_offered_class() {
        let base = base_ts_ms();
        let mut points: Vec<LogicalPoint> = valid_stream(4, 2)
            .into_iter()
            .map(LogicalPoint::Sample)
            .collect();
        points.push(LogicalPoint::Sample(LogicalSample {
            metric: "mb_replay_gauge".to_string(),
            labels: vec![("instance".to_string(), "mb-instance-0".to_string())],
            ts_ms: base + 90_000,
            value: f64::from_bits(STALE_MARKER_BITS),
        }));
        points.push(LogicalPoint::Histogram(histogram(base, vec![2, 1, 0])));
        points.push(LogicalPoint::Histogram(histogram(base + 15_000, vec![3, 1, 0])));
        let cfg = memory_config("mb-replay-classes", 3);
        let outcome = replay_into_ravel(&cfg, &mut SlicePoints::new(&points))
            .await
            .expect("replay");
        let ing = &outcome.result.ingest;
        assert_eq!(ing.offered_samples, 5, "four scalars and one stale marker");
        assert_eq!(ing.accepted_samples, 5);
        assert_eq!(ing.offered_histograms, 2);
        assert_eq!(ing.accepted_histograms, 2);
        assert_eq!(ing.rejected_histograms + ing.dropped_histograms, 0);
        assert_eq!(
            ing.stream_counts,
            StreamCounts {
                generated_lines: 7,
                scalar: 4,
                native_histogram: 2,
                stale_marker: 1,
                malformed: 0,
            }
        );
        check_row_accounting(ing, &ing.stream_counts).expect("both classes close");
    }

    /// The ci profile replayed in process: every line is offered, the
    /// per-type counts close against the generator, and both exclusions are
    /// zero.
    #[tokio::test]
    async fn the_ci_stream_replays_every_payload_type_with_zero_exclusions() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let interval_ms = 1_000 * workload.profile("ci").expect("ci").scrape_interval_secs as i64;
        let steps = 5;
        let back = base_ts_ms() - steps as i64 * interval_ms;
        let anchor = back - back.rem_euclid(interval_ms);
        let mut stream = GeneratedStream::new(
            Generator::new(&workload, "ci", anchor).expect("generator builds"),
            steps,
        );
        let outcome = replay_into_ravel(&memory_config("mb-replay-ci-types", 4096), &mut stream)
            .await
            .expect("replay");
        let (report, counts) = stream.finish().expect("drained stream closes");
        check_stream_band(&counts, &report).expect("band 4 classification");
        let ing = &outcome.result.ingest;
        check_row_accounting(ing, &counts).expect("band 4 offering identity");
        assert!(counts.stale_marker > 0 && counts.native_histogram > 0);
        assert_eq!(ing.excluded_native_histogram, 0);
        assert_eq!(ing.excluded_stale_marker, 0);
        assert_eq!(ing.offered_samples, counts.scalar + counts.stale_marker);
        assert_eq!(ing.offered_histograms, counts.native_histogram);
        assert_eq!(ing.accepted_samples, ing.offered_samples);
        assert_eq!(ing.accepted_histograms, ing.offered_histograms);
    }

    /// COMMIT-TOKEN READ-YOUR-WRITE (ADR-0927 decision 3): the tokens a strict
    /// write mints are passed as `min_commit_token`, so a query resolves the
    /// exact committed segments and sees the rows deterministically -- WITHOUT
    /// sleeping past the 2 s flush delay. A run that instead slept would be
    /// measuring the flush delay and calling it query latency.
    #[tokio::test]
    async fn commit_tokens_make_the_write_read_your_write() {
        let n = 24usize;
        let stream = valid_stream(n, 6);
        let cfg = memory_config("mb-replay-ryw", 6);
        let outcome = replay_into_ravel(&cfg, &mut SliceSamples::new(&stream))
            .await
            .expect("replay");

        assert!(
            !outcome.tokens.is_empty(),
            "a strict replay must mint commit tokens to read against"
        );
        let newest = outcome
            .max_ts_ms
            .expect("a non-empty replay has a newest timestamp");

        // The query phase runs through the shared `query_after_replay` helper
        // the binary uses: no sleep between the ack and this read, the tokens
        // (not wall time) make the write visible.
        let query = query_after_replay(&outcome, "mb_replay_gauge")
            .await
            .expect("read-your-write query phase");

        assert_eq!(query.system, "ravel");
        assert_eq!(
            query.eval_ts_ms, newest,
            "the query evaluates at the replay's newest sample"
        );
        assert_eq!(
            query.matched_series, 6,
            "read-your-write must see exactly the six series just written, no sleep"
        );
        assert!(
            !query.min_commit_tokens.is_empty(),
            "the query phase is token-bound: it carries the min_commit_token set it read against"
        );
    }

    /// FIX 4 (issue #937 review finding 4): an empty stream (or an all-rejected
    /// one) leaves NO newest timestamp. The public `replay_into_ravel` models
    /// that as `None`, never `i64::MIN`, so a downstream ms-to-ns conversion of
    /// a sentinel cannot overflow, and the query phase refuses with a typed
    /// error rather than reading at a bogus instant.
    ///
    /// TO SEE THIS FAIL against the pre-fix sentinel: start `max_ts_ms` in
    /// `replay_into_ravel` at `Some(i64::MIN)` instead of `None`; the first
    /// assertion below then fails.
    #[tokio::test]
    async fn an_empty_stream_reports_absent_newest_ts_not_a_sentinel() {
        let cfg = memory_config("mb-replay-empty", 8);
        let outcome = replay_into_ravel(&cfg, &mut SliceSamples::new(&[]))
            .await
            .expect("replay");

        assert_eq!(
            outcome.max_ts_ms, None,
            "an empty replay has no newest timestamp: absent, not i64::MIN"
        );
        assert_eq!(outcome.result.ingest.accepted_samples, 0);
        assert!(outcome.tokens.is_empty());

        // The query phase refuses the absence with a typed error instead of
        // reading at a sentinel instant.
        let err = query_after_replay(&outcome, "mb_replay_gauge")
            .await
            .expect_err("querying an empty replay must be a typed error");
        assert!(matches!(err, LaneError::Query(_)));
    }

    /// The RW1.0 body encodes to non-empty snappy-compressed protobuf and
    /// groups samples of one series together: one `TimeSeries`, two samples.
    #[test]
    fn rw1_body_groups_samples_by_series() {
        let s = |ts: i64, v: f64| LogicalSample {
            metric: "mb_gauge".to_string(),
            labels: vec![("instance".to_string(), "a".to_string())],
            ts_ms: ts,
            value: v,
        };
        let req = write_request_from(&[s(1, 1.0), s(2, 2.0)]);
        assert_eq!(req.timeseries.len(), 1, "one series, two samples grouped");
        assert_eq!(req.timeseries[0].samples.len(), 2);
        // __name__ is present and labels are sorted (instance < __name__ is
        // false: '_' (0x5f) > 'i' (0x69)? no, 'i'=0x69 > '_'=0x5f, so __name__
        // sorts first).
        assert_eq!(req.timeseries[0].labels[0].name, "__name__");
        assert_eq!(req.timeseries[0].labels[0].value, "mb_gauge");

        let body = encode_rw1_body(&[s(1, 1.0)]).expect("encode");
        assert!(!body.is_empty(), "the RW1.0 body is non-empty");
    }

    /// FIX 2 (issue #937 review finding 2): `max_ack_p99_ms` is the MAX of the
    /// rows' per-row p99s, not a pooled percentile, and its name says so. With
    /// two same-ack rows whose p99s are 3.0 and 9.0, the figure is exactly the
    /// larger, 9.0. A true pooled percentile over the union of the two rows' raw
    /// ack samples would be a different number (it would sit between the batches
    /// by rank, not at the larger row's p99), which is precisely why the figure
    /// is named `max` and not `pooled`.
    ///
    /// TO SEE THE MAX ASSERTION FAIL: change the fold in `max_ack_p99_ms` from
    /// `f64::max` to `f64::min`; the figure becomes 3.0 and the assert fails.
    #[test]
    fn max_ack_p99_is_the_max_of_per_row_p99s_over_two_rows() {
        let low = SystemIngestResult::buffered_http_row(
            "prometheus",
            "http://prom.example:9090/api/v1/write",
            "reqwest RW1.0 sender",
            ingest_phase(3.0),
        );
        let high = SystemIngestResult::buffered_http_row(
            "victoriametrics",
            "http://vm.example:8428/api/v1/write",
            "reqwest RW1.0 sender",
            ingest_phase(9.0),
        );
        let report = MetricsIngestReport::new(vec![low, high]);

        // Both rows are buffered, so the pool is admissible; the figure is the
        // MAX of {3.0, 9.0}, exactly 9.0.
        assert_eq!(
            report
                .max_ack_p99_ms(&["prometheus", "victoriametrics"])
                .expect("two buffered rows share one ack meaning, so the pool is admissible"),
            Some(9.0),
            "the figure is the max of the per-row p99s, not a pooled percentile"
        );
    }

    /// FIX 2 (issue #937 review finding 2): an empty pool is ABSENT, not a real
    /// 0.0 ms max. A pool naming no existing row returns `Ok(None)`, so "no rows
    /// to fold" is distinguishable from "the max p99 is 0.0 ms".
    ///
    /// TO SEE THIS FAIL: flip the `if rows.is_empty() { return Ok(None); }` line
    /// in `max_ack_p99_ms` to `Ok(Some(0.0))`; the assert on `None` fails.
    #[test]
    fn max_ack_p99_over_an_empty_pool_is_absent() {
        let report = MetricsIngestReport::new(vec![SystemIngestResult::buffered_http_row(
            "prometheus",
            "http://prom.example:9090/api/v1/write",
            "reqwest RW1.0 sender",
            ingest_phase(3.0),
        )]);
        assert_eq!(
            report
                .max_ack_p99_ms(&["nonexistent"])
                .expect("no ack-kind conflict when no row is named"),
            None,
            "an empty pool is absent, never a 0.0 ms max standing in for unmeasured"
        );
    }

    /// FIX 3 (issue #937 review finding 3): an HTTP transport-setup failure maps
    /// to [`LaneError::Transport`], NOT [`LaneError::Snappy`], so its rendered
    /// message names the transport, not snappy compression the setup never
    /// touched. `replay_over_http`'s client-build `map_err` routes through
    /// `transport_setup_error`, the same constructor exercised here.
    ///
    /// TO SEE THIS FAIL: change `transport_setup_error` to build
    /// `LaneError::Snappy(detail)` (the pre-fix bug); the variant match and the
    /// message assertions below fail.
    #[test]
    fn client_build_failure_is_a_transport_error() {
        let err = transport_setup_error("building the HTTP client failed: boom".to_string());
        assert!(
            matches!(err, LaneError::Transport(_)),
            "a client-build failure is a transport-setup error, not a snappy error"
        );
        let rendered = err.to_string();
        assert_eq!(
            rendered,
            "setting up the Remote Write HTTP transport failed: building the HTTP client failed: \
             boom"
        );
        assert!(
            !rendered.contains("snappy"),
            "the transport-setup message must not claim snappy compression failed"
        );
    }

    /// FIX 5 (issue #937 review finding 5): pins the VALUE side of the fix --
    /// `wire_bytes` equals the exact sum of the per-batch snappy body lengths,
    /// recomputed here independently. The WINDOW side (the encode running
    /// before `wall_start`) is not observable from a value assertion: moving
    /// the accumulation back into the timed loop leaves `wire_bytes`
    /// unchanged. That property is enforced by code structure -- the
    /// precompute loop completes before `wall_start` is taken in
    /// `replay_into_ravel` -- and the name of this test claims only what it
    /// asserts.
    #[tokio::test]
    async fn wire_bytes_equal_the_precomputed_rw1_body_sum() {
        let n = 40usize;
        let batch_size = 8usize;
        let stream = valid_stream(n, 8);
        let cfg = memory_config("mb-replay-wire", batch_size);
        let outcome = replay_into_ravel(&cfg, &mut SliceSamples::new(&stream))
            .await
            .expect("replay");

        // Every sample in `valid_stream` is valid, so the replay batches the
        // stream in its own order; recompute the wire size the same way, from
        // the RW1.0 encoder, entirely outside any timed region.
        let expected_wire_bytes: u64 = stream
            .chunks(batch_size)
            .map(|chunk| encode_rw1_body(chunk).expect("encode").len() as u64)
            .sum();

        assert_eq!(
            outcome.result.ingest.wire_bytes, expected_wire_bytes,
            "wire_bytes is unchanged: the exact sum of the per-batch RW1.0 body lengths, \
             precomputed outside the timed window"
        );
        assert!(
            expected_wire_bytes > 0,
            "a non-empty replay encodes a non-empty RW1.0 body"
        );
    }

    /// The lane classifies the shipping `metrics_gen` encoding: a float line
    /// becomes a scalar sample (with the exact bit-pattern value), and the
    /// native-histogram and staleness lines become their own typed records
    /// with their fields, not skipped. Uses the shipping
    /// `GeneratedSample::encode` so the classifier is pinned to the real
    /// format, not a hand-written stand-in.
    #[test]
    fn classify_line_types_every_shape_the_encoder_writes() {
        use crate::metrics_gen::{GeneratedSample, NativeHistogram, SampleValue};

        let mut text = Vec::new();
        let mut line = Vec::new();
        GeneratedSample {
            ts_ms: 15_000,
            metric: "mb_gauge".to_string(),
            labels: vec![
                ("instance".to_string(), "mb-instance-0".to_string()),
                ("job".to_string(), "api".to_string()),
            ],
            value: SampleValue::Float(3.5),
        }
        .encode(&mut line);
        text.extend_from_slice(&line);
        GeneratedSample {
            ts_ms: 15_000,
            metric: "mb_gauge".to_string(),
            labels: vec![("instance".to_string(), "mb-instance-1".to_string())],
            value: SampleValue::StaleMarker,
        }
        .encode(&mut line);
        text.extend_from_slice(&line);
        GeneratedSample {
            ts_ms: 15_000,
            metric: "mb_native".to_string(),
            labels: vec![("job".to_string(), "api".to_string())],
            value: SampleValue::NativeHistogram(NativeHistogram {
                schema: 2,
                count: 4,
                sum: 1.0,
                positive_deltas: vec![1, 1, 1, 1],
            }),
        }
        .encode(&mut line);
        text.extend_from_slice(&line);

        let text = String::from_utf8(text).expect("utf8");
        let records: Vec<StreamRecord> = text
            .lines()
            .enumerate()
            .map(|(i, line)| classify_line(i as u64 + 1, line))
            .collect();
        assert_eq!(records.len(), 3, "one record per line");
        assert_eq!(
            records[0],
            StreamRecord::Scalar(LogicalSample {
                metric: "mb_gauge".to_string(),
                labels: vec![
                    ("instance".to_string(), "mb-instance-0".to_string()),
                    ("job".to_string(), "api".to_string()),
                ],
                ts_ms: 15_000,
                value: 3.5,
            })
        );
        let StreamRecord::StaleMarker(stale) = &records[1] else {
            panic!("a stale line is a stale marker: {:?}", records[1]);
        };
        assert_eq!(stale.metric, "mb_gauge");
        assert_eq!(
            stale.labels,
            vec![("instance".to_string(), "mb-instance-1".to_string())]
        );
        assert_eq!(stale.ts_ms, 15_000);
        assert_eq!(stale.value.to_bits(), STALE_MARKER_BITS);
        assert_eq!(
            records[2],
            StreamRecord::NativeHistogram(LogicalHistogram {
                metric: "mb_native".to_string(),
                labels: vec![("job".to_string(), "api".to_string())],
                ts_ms: 15_000,
                schema: 2,
                count: 4,
                sum: 1.0,
                positive_deltas: vec![1, 1, 1, 1],
                reset_hint: pb_histogram::ResetHint::Unknown,
            })
        );
    }

    /// Required test (i): one crafted malformed line of each shape gives
    /// exactly one `Malformed` with that shape's reason at that line, and the
    /// well-formed lines around them give none.
    #[test]
    fn each_malformed_shape_is_reported_once_with_its_reason_and_line() {
        let good_f = "15000\tmb_gauge{job=\"api\"}\tf 0x400c000000000000";
        let good_s = "15000\tmb_gauge{job=\"api\"}\tstale";
        let good_h =
            "15000\tmb_native{job=\"api\"}\th schema=2 count=4 sum=0x3ff0000000000000 deltas=";
        let stream = [
            good_f,
            "15000\tmb_gauge{job=\"api\"}",
            "15o00\tmb_gauge{job=\"api\"}\tf 0x400c000000000000",
            "15000\tmb_gauge{job=\"api\"}\tf 0x400c00000000000z",
            "15000\tmb_gauge job=api\tf 0x400c000000000000",
            "15000\tmb_gauge{job=\"api\"}\tq 0x400c000000000000",
            good_s,
            good_h,
        ];
        let mut counts = StreamCounts::default();
        let mut malformed = Vec::new();
        for (i, line) in stream.iter().enumerate() {
            let record = classify_line(i as u64 + 1, line);
            counts.record(&record);
            if let StreamRecord::Malformed { line_no, reason } = record {
                malformed.push((line_no, reason));
            }
        }
        assert_eq!(
            malformed,
            vec![
                (2, MalformedReason::MissingField),
                (3, MalformedReason::BadTimestamp),
                (4, MalformedReason::BadFloatBits),
                (5, MalformedReason::BadSeries),
                (6, MalformedReason::UnknownPayload),
            ]
        );
        assert_eq!(
            counts,
            StreamCounts {
                generated_lines: 8,
                scalar: 1,
                native_histogram: 1,
                stale_marker: 1,
                malformed: 5,
            }
        );

        let mut clean = StreamCounts::default();
        for (i, line) in [good_f, good_s, good_h].iter().enumerate() {
            clean.record(&classify_line(i as u64 + 1, line));
        }
        assert_eq!(clean.malformed, 0, "a correct stream has no malformed line");
        assert_eq!(clean.classified(), 3);
    }

    /// Required test (ii): streaming the full `ci` profile through
    /// [`GeneratedStream`] counts every line under its kind, and those counts
    /// are the generator's own per-type figures. A line the classifier skipped
    /// (a reintroduced `continue`) or counted under the wrong kind moves one
    /// of the three equalities.
    #[test]
    fn streaming_the_ci_profile_reproduces_the_generators_per_type_counts() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let steps = workload.profile("ci").expect("ci").samples_per_series;
        let mut stream = GeneratedStream::new(
            Generator::new(&workload, "ci", 1_700_000_000_000).expect("generator builds"),
            steps,
        );
        let (mut offered_samples, mut offered_histograms) = (0u64, 0u64);
        while let Some(batch) = stream.next_batch(500).expect("generates") {
            let (samples, histograms) = class_counts(&batch);
            offered_samples += samples;
            offered_histograms += histograms;
        }
        let (report, counts) = stream.finish().expect("drained stream closes");

        assert!(report.native_histogram_samples > 0 && report.stale_markers > 0);
        assert_eq!(counts.scalar, report.float_samples);
        assert_eq!(counts.native_histogram, report.native_histogram_samples);
        assert_eq!(counts.stale_marker, report.stale_markers);
        assert_eq!(counts.malformed, 0);
        assert_eq!(counts.generated_lines, report.emitted_samples);
        assert_eq!(
            offered_samples,
            counts.scalar + counts.stale_marker,
            "every scalar and stale-marker line is offered once"
        );
        assert_eq!(
            offered_histograms, counts.native_histogram,
            "every native-histogram line is offered once"
        );
        check_stream_band(&counts, &report).expect("the ci stream closes band 4");
    }

    /// Band 4's stream half names the figures of each failing condition: a
    /// malformed line, a sum that does not close, and lines read that differ
    /// from the samples the generator emitted.
    #[test]
    fn stream_band_violations_name_the_figures() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let (_, report) = Generator::new(&workload, "ci", 0)
            .expect("generator builds")
            .generate_bytes(1)
            .expect("generates");
        let closed = StreamCounts {
            generated_lines: report.emitted_samples,
            scalar: report.float_samples,
            native_histogram: report.native_histogram_samples,
            stale_marker: report.stale_markers,
            malformed: 0,
        };
        check_stream_band(&closed, &report).expect("the generator's own figures close");

        let one_malformed = StreamCounts {
            scalar: closed.scalar - 1,
            malformed: 1,
            ..closed
        };
        let err = check_stream_band(&one_malformed, &report).expect_err("malformed is refused");
        assert_eq!(err.malformed, 1);
        assert!(err.to_string().contains("1 malformed"), "{err}");

        let unsummed = StreamCounts {
            generated_lines: closed.generated_lines + 1,
            ..closed
        };
        let err = check_stream_band(&unsummed, &report).expect_err("an open sum is refused");
        assert_eq!(err.classified + 1, err.generated_lines);

        // Only the classification sum breaks: lines read still equal what the
        // generator emitted, and no line is malformed.
        let one_scalar_short = StreamCounts {
            scalar: closed.scalar - 1,
            ..closed
        };
        let err =
            check_stream_band(&one_scalar_short, &report).expect_err("an open sum is refused");
        assert_eq!(err.generated_lines, err.emitted_samples);
        assert_eq!(err.malformed, 0);
        assert!(
            err.to_string().contains(&format!(
                "{} scalar + {} native histogram + {} stale marker + 0 malformed = {} classified, \
                 against {} lines read",
                closed.scalar - 1,
                closed.native_histogram,
                closed.stale_marker,
                closed.generated_lines - 1,
                closed.generated_lines
            )),
            "{err}"
        );

        let short = StreamCounts {
            generated_lines: closed.generated_lines - 1,
            scalar: closed.scalar - 1,
            ..closed
        };
        let err = check_stream_band(&short, &report).expect_err("a short stream is refused");
        assert_eq!(err.emitted_samples, report.emitted_samples);
        assert!(
            err.to_string().contains(&format!(
                "{} samples the generator emitted",
                report.emitted_samples
            )),
            "{err}"
        );
    }

    /// Required test (iv): the same anchor twice streams the same bytes, and
    /// the streamed digest equals the one-shot `generate_into` digest.
    #[test]
    fn streaming_is_deterministic_and_matches_one_shot_generation() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let anchor = 1_700_000_000_000;
        let steps = 12;
        let drain = || {
            let mut stream = GeneratedStream::new(
                Generator::new(&workload, "ci", anchor).expect("generator builds"),
                steps,
            );
            let mut samples = Vec::new();
            while let Some(batch) = stream.next_batch(333).expect("generates") {
                samples.extend(batch);
            }
            let (report, counts) = stream.finish().expect("closes");
            (report, counts, samples)
        };
        let (first, first_counts, first_samples) = drain();
        let (second, second_counts, second_samples) = drain();
        assert_eq!(first.digest, second.digest);
        assert_eq!(first_counts, second_counts);
        // Compared through the offered digest, which is bit-exact: a stale
        // marker is a NaN, so `PartialEq` on the points never holds.
        let points_digest = |points: &[LogicalPoint]| {
            let mut d = OfferedDigest::new();
            points.iter().for_each(|p| d.update_point(p));
            (points.len(), d.finish())
        };
        assert_eq!(
            points_digest(&first_samples),
            points_digest(&second_samples),
            "same anchor, same write order"
        );

        let mut one_shot_bytes = Vec::new();
        let one_shot = Generator::new(&workload, "ci", anchor)
            .expect("generator builds")
            .generate_into(steps, &mut one_shot_bytes)
            .expect("generates");
        assert_eq!(
            first, one_shot,
            "the streamed report is the one-shot report"
        );
        let one_shot_points: Vec<LogicalPoint> = std::str::from_utf8(&one_shot_bytes)
            .expect("utf8")
            .lines()
            .enumerate()
            .filter_map(|(i, line)| match classify_line(i as u64 + 1, line) {
                StreamRecord::Scalar(sample) | StreamRecord::StaleMarker(sample) => {
                    Some(LogicalPoint::Sample(sample))
                }
                StreamRecord::NativeHistogram(h) => Some(LogicalPoint::Histogram(h)),
                StreamRecord::Malformed { .. } => None,
            })
            .collect();
        assert_eq!(
            points_digest(&first_samples),
            points_digest(&one_shot_points),
            "streaming offers the one-shot stream's points in its order"
        );

        let (other, _, _) = {
            let mut stream = GeneratedStream::new(
                Generator::new(&workload, "ci", anchor + 15_000).expect("generator builds"),
                steps,
            );
            while stream.next_batch(333).expect("generates").is_some() {}
            let (r, c) = stream.finish().expect("closes");
            (r, c, ())
        };
        assert_ne!(other.digest, first.digest, "the anchor reaches the bytes");
    }

    /// Points (every line) the first `steps` steps of the ci profile emit.
    fn ci_points(workload: &WorkloadFile, steps: u64) -> u64 {
        crate::metrics_gen::Generator::new(workload, "ci", 0)
            .expect("builds")
            .generate_bytes(steps)
            .expect("generates")
            .1
            .emitted_samples
    }

    /// Closing a stream with every step generated but samples still queued is
    /// refused: the report would count samples nobody offered.
    #[test]
    fn a_stream_with_every_step_generated_and_samples_unread_refuses_to_finish() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let first_step = ci_points(&workload, 1);
        let both_steps = ci_points(&workload, 2);
        let mut stream =
            GeneratedStream::new(Generator::new(&workload, "ci", 0).expect("builds"), 2);
        // One more than the first step holds forces the second step out.
        let pulled = first_step as usize + 1;
        let batch = stream
            .next_batch(pulled)
            .expect("generates")
            .expect("a batch");
        assert_eq!(batch.len(), pulled);
        let unread = both_steps - pulled as u64;
        assert!(unread > 0, "the premise is samples still queued");
        let err = stream.finish().expect_err("unread samples are refused");
        assert!(
            err.to_string()
                .contains(&format!("after 2 of 2 steps with {unread} points unread")),
            "{err}"
        );
    }

    /// Closing a stream with its queue drained but steps still ungenerated is
    /// refused: the report would describe a shorter stream than declared.
    #[test]
    fn a_drained_queue_short_of_its_steps_refuses_to_finish() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let first_step = ci_points(&workload, 1) as usize;
        let mut stream =
            GeneratedStream::new(Generator::new(&workload, "ci", 0).expect("builds"), 2);
        // Exactly the first step's scalars: generated once, then drained.
        let batch = stream
            .next_batch(first_step)
            .expect("generates")
            .expect("a batch");
        assert_eq!(batch.len(), first_step);
        let err = stream.finish().expect_err("a short stream is refused");
        assert!(
            err.to_string()
                .contains("after 1 of 2 steps with 0 points unread"),
            "{err}"
        );
    }

    /// The anchor contract: non-negative, on the profile's scrape grid, and
    /// the last step inside the nanosecond range. Every refusal names its
    /// figures.
    #[test]
    fn check_anchor_refuses_off_grid_negative_and_out_of_range_anchors() {
        let interval_secs = 15;
        check_anchor(1_700_000_010_000, interval_secs, 120).expect("on grid, in range");
        check_anchor(0, interval_secs, 1).expect("the epoch is on every grid");

        // Off grid by one millisecond.
        let err = check_anchor(1_700_000_010_001, interval_secs, 120).expect_err("off grid");
        assert_eq!(
            err,
            AnchorError::OffGrid {
                anchor_ms: 1_700_000_010_001,
                interval_ms: 15_000,
                remainder: 1,
            }
        );
        assert!(
            err.to_string().contains("1700000010001 mod 15000 = 1"),
            "{err}"
        );

        let err = check_anchor(-15_000, interval_secs, 1).expect_err("negative");
        assert_eq!(err, AnchorError::Negative { anchor_ms: -15_000 });

        // i64::MAX - 1: far past the nanosecond range even for one step.
        let err = check_anchor(i64::MAX - 1, interval_secs, 1).expect_err("out of range");
        assert!(
            matches!(err, AnchorError::OutOfRange { anchor_ms, .. } if anchor_ms == i64::MAX - 1),
            "{err:?}"
        );
        assert!(err.to_string().contains("9223372036854 ms"), "{err}");

        // The bound is on the LAST step: an on-grid anchor whose last step
        // lands exactly on the largest on-grid millisecond passes, and one
        // more step refuses.
        let steps = 4u64;
        let last_on_grid = MAX_ANCHOR_RANGE_MS - MAX_ANCHOR_RANGE_MS % 15_000;
        let anchor = last_on_grid - (steps as i64 - 1) * 15_000;
        check_anchor(anchor, interval_secs, steps).expect("the last step lands on the bound");
        let err = check_anchor(anchor, interval_secs, steps + 1).expect_err("one step past");
        assert_eq!(
            err,
            AnchorError::OutOfRange {
                anchor_ms: anchor,
                steps: steps + 1,
                interval_ms: 15_000,
                last_ms: i128::from(last_on_grid) + 15_000,
                max: MAX_ANCHOR_RANGE_MS,
            }
        );

        assert_eq!(check_anchor(0, 0, 1), Err(AnchorError::ZeroInterval));
    }

    /// A row whose excluded counts were zeroed after classification fails the
    /// offering identity from its own fields, even though the stream's
    /// classification still holds them and `offered == scalar` still closes.
    #[test]
    fn row_accounting_reads_the_offering_identity_from_the_rows_own_fields() {
        let counts = StreamCounts {
            generated_lines: 10,
            scalar: 6,
            native_histogram: 3,
            stale_marker: 1,
            malformed: 0,
        };
        // A row that sends every type: 6 scalars + 1 stale marker offered
        // as samples, 3 histograms offered as their own class.
        let mut ingest = ingest_phase(1.0);
        ingest.stream_counts = counts;
        ingest.offered_samples = 7;
        ingest.accepted_samples = 5;
        ingest.rejected_samples = 1;
        ingest.dropped_samples = 1;
        ingest.offered_histograms = 3;
        ingest.accepted_histograms = 2;
        ingest.dropped_histograms = 1;
        check_row_accounting(&ingest, &counts).expect("the row closes");

        // A row carrying named exclusions (the pre-change lane) still closes.
        let mut excluding = ingest.clone();
        excluding.offered_samples = 6;
        excluding.accepted_samples = 4;
        excluding.excluded_stale_marker = 1;
        excluding.offered_histograms = 0;
        excluding.accepted_histograms = 0;
        excluding.dropped_histograms = 0;
        excluding.excluded_native_histogram = 3;
        check_row_accounting(&excluding, &counts).expect("named exclusions close");

        let mut zeroed = excluding.clone();
        zeroed.excluded_native_histogram = 0;
        zeroed.excluded_stale_marker = 0;
        let err = check_row_accounting(&zeroed, &counts).expect_err("zeroed exclusions");
        assert_eq!(err.offering_total, 6);
        let message = err.to_string();
        for figure in [
            "6 offered samples + 0 offered histograms + 0 excluded_native_histogram + 0 \
             excluded_stale_marker = 6 against 10 generated lines",
            "6 offered samples + 0 excluded_stale_marker against 6 scalar + 1 stale marker lines",
            "0 offered histograms + 0 excluded_native_histogram against 3 native histogram lines",
            "6 offered against 4 accepted + 1 rejected + 1 dropped samples",
        ] {
            assert!(message.contains(figure), "missing `{figure}` in: {message}");
        }

        let mut unsplit = ingest.clone();
        unsplit.dropped_samples = 0;
        let err = check_row_accounting(&unsplit, &counts).expect_err("an open split");
        assert!(
            err.to_string()
                .contains("7 offered against 5 accepted + 1 rejected + 0 dropped samples"),
            "{err}"
        );

        let mut unsplit_histograms = ingest.clone();
        unsplit_histograms.dropped_histograms = 0;
        let err = check_row_accounting(&unsplit_histograms, &counts)
            .expect_err("an open histogram split");
        assert!(
            err.to_string()
                .contains("3 offered against 2 accepted + 0 rejected + 0 dropped histograms"),
            "{err}"
        );
    }

    /// A row that folds its histograms into the scalar `offered_samples`
    /// fails, even when its offering total and its split still close.
    #[test]
    fn a_row_that_folds_histograms_into_offered_samples_fails() {
        let counts = StreamCounts {
            generated_lines: 10,
            scalar: 6,
            native_histogram: 3,
            stale_marker: 1,
            malformed: 0,
        };
        let mut folded = ingest_phase(1.0);
        folded.stream_counts = counts;
        folded.offered_samples = 10;
        folded.accepted_samples = 10;
        folded.rejected_samples = 0;
        folded.dropped_samples = 0;
        let err = check_row_accounting(&folded, &counts).expect_err("folded histograms");
        assert_eq!(err.offering_total, 10, "the total alone cannot see a fold");
        assert!(
            err.to_string()
                .contains("10 offered samples + 0 excluded_stale_marker against 6 scalar"),
            "{err}"
        );
    }

    /// A stream whose native-histogram and stale-marker counts are swapped
    /// still sums to the lines read, so only the per-type comparison against
    /// the generator catches it.
    #[test]
    fn a_classifier_that_swaps_two_types_fails_the_per_type_comparison() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let steps = workload.profile("ci").expect("ci").samples_per_series;
        let (_, report) = Generator::new(&workload, "ci", 0)
            .expect("generator builds")
            .generate_bytes(steps)
            .expect("generates");
        assert_ne!(
            report.native_histogram_samples, report.stale_markers,
            "the premise is two unequal counts, so a swap is visible"
        );
        let swapped = StreamCounts {
            generated_lines: report.emitted_samples,
            scalar: report.float_samples,
            native_histogram: report.stale_markers,
            stale_marker: report.native_histogram_samples,
            malformed: 0,
        };
        assert_eq!(swapped.classified(), swapped.generated_lines);
        let err = check_stream_band(&swapped, &report).expect_err("a swap is refused");
        assert!(
            err.to_string().contains(&format!(
                "{} native histogram and {} stale marker",
                report.native_histogram_samples, report.stale_markers
            )),
            "{err}"
        );
    }

    /// `comparable` needs the recorded stream digest to equal the one the
    /// manifest and anchor regenerate. A tampered digest forces it false with
    /// a reason naming both digests, on a profile and step count that would
    /// otherwise publish.
    #[test]
    fn a_stream_digest_the_parameters_do_not_reproduce_is_not_comparable() {
        use crate::metrics_gen::Generator;
        use crate::metrics_workload::Comparability;

        let workload = ci_workload();
        let ci = workload.profile("ci").expect("ci profile declared");
        let steps = 3;
        let profile = Profile {
            comparability: Comparability::Comparable,
            samples_per_series: steps,
            ..ci.clone()
        };
        let anchor_ms = 1_700_000_010_000;
        let (bytes, gen_report) = Generator::for_profile(&workload, &profile, anchor_ms)
            .generate_bytes(steps)
            .expect("generates");
        let record = build_profile_record(
            &workload,
            &profile,
            steps,
            bytes.len() as u64,
            &gen_report,
            provenance(anchor_ms),
        )
        .expect("regenerates");
        assert!(record.comparable, "the untampered run publishes");
        assert_eq!(record.comparability_reason, None);

        let mut tampered = gen_report.clone();
        tampered.digest = "0".repeat(64);
        let record = build_profile_record(
            &workload,
            &profile,
            steps,
            bytes.len() as u64,
            &tampered,
            provenance(anchor_ms),
        )
        .expect("regenerates");
        assert!(!record.comparable, "a tampered digest never publishes");
        let reason = record.comparability_reason.expect("a reason");
        assert!(
            reason.contains(&format!(
                "the replayed stream's digest {} differs from {}, the digest the manifest and \
                 anchor {anchor_ms} regenerate",
                tampered.digest, gen_report.digest
            )),
            "{reason}"
        );
        assert_eq!(
            record.run.stream_digest.as_deref(),
            Some(&*tampered.digest),
            "the record keeps the digest the run replayed"
        );

        // The same digest recorded under a different anchor does not
        // reproduce either.
        let record = build_profile_record(
            &workload,
            &profile,
            steps,
            bytes.len() as u64,
            &gen_report,
            provenance(anchor_ms + 15_000),
        )
        .expect("regenerates");
        assert!(!record.comparable, "the anchor is part of what is checked");
    }

    /// Both off-profile causes are named when both apply.
    #[test]
    fn off_profile_steps_and_a_tampered_digest_are_both_reasons() {
        use crate::metrics_gen::Generator;
        use crate::metrics_workload::Comparability;

        let workload = ci_workload();
        let ci = workload.profile("ci").expect("ci profile declared");
        let profile = Profile {
            comparability: Comparability::Comparable,
            samples_per_series: 10,
            ..ci.clone()
        };
        let (bytes, mut gen_report) = Generator::for_profile(&workload, &profile, 0)
            .generate_bytes(3)
            .expect("generates");
        gen_report.digest = "f".repeat(64);
        let record = build_profile_record(
            &workload,
            &profile,
            3,
            bytes.len() as u64,
            &gen_report,
            provenance(0),
        )
        .expect("regenerates");
        assert!(!record.comparable);
        let reason = record.comparability_reason.expect("a reason");
        assert!(reason.contains("this run generated 3 steps"), "{reason}");
        assert!(
            reason.contains("the replayed stream's digest ffff"),
            "{reason}"
        );
    }

    /// The offered digest covers the offered tuples in order: the same
    /// samples hash equal, and changing one timestamp, one value's bit
    /// pattern, or the order changes it.
    #[test]
    fn offered_digest_covers_series_timestamp_value_bits_and_order() {
        let samples = valid_stream(4, 2);
        let digest = |s: &[LogicalSample]| {
            let mut d = OfferedDigest::new();
            s.iter().for_each(|x| d.update(x));
            d.finish()
        };
        let base = digest(&samples);
        assert_eq!(base, digest(&samples));

        let mut shifted = samples.clone();
        shifted[1].ts_ms += 1;
        assert_ne!(base, digest(&shifted));

        let mut negated = samples.clone();
        negated[0].value = -0.0;
        let mut zero = samples.clone();
        zero[0].value = 0.0;
        assert_ne!(digest(&negated), digest(&zero), "-0.0 and 0.0 differ");

        let mut reordered = samples.clone();
        reordered.swap(0, 1);
        assert_ne!(base, digest(&reordered));
    }

    /// The offered digest covers staleness markers by bit pattern and native
    /// histograms by every encoded field: one stale marker turned into the
    /// canonical NaN, one bucket delta, the sum, the schema, the count or the
    /// reset hint each change it.
    ///
    /// TO SEE THIS FAIL: drop the `positive_deltas` loop from
    /// `OfferedDigest::update_histogram`; the bucket-delta assertion fails.
    #[test]
    fn offered_digest_covers_stale_markers_and_every_histogram_field() {
        let stale = LogicalSample {
            metric: "mb_gauge".to_string(),
            labels: vec![],
            ts_ms: 15_000,
            value: f64::from_bits(STALE_MARKER_BITS),
        };
        let points = vec![
            LogicalPoint::Sample(stale.clone()),
            LogicalPoint::Histogram(histogram(15_000, vec![3, 1, -2, 0])),
        ];
        let digest = |p: &[LogicalPoint]| {
            let mut d = OfferedDigest::new();
            p.iter().for_each(|x| d.update_point(x));
            d.finish()
        };
        let base = digest(&points);
        assert_eq!(base, digest(&points.clone()));

        let mut quiet_nan = points.clone();
        quiet_nan[0] = LogicalPoint::Sample(LogicalSample {
            value: f64::from_bits(0x7ff8_0000_0000_0000),
            ..stale
        });
        assert_ne!(base, digest(&quiet_nan), "a stale marker is its bit pattern");

        let edit = |f: &dyn Fn(&mut LogicalHistogram)| {
            let mut changed = points.clone();
            if let LogicalPoint::Histogram(h) = &mut changed[1] {
                f(h);
            }
            digest(&changed)
        };
        assert_ne!(base, edit(&|h| h.positive_deltas[2] = -1), "one bucket delta");
        assert_ne!(base, edit(&|h| h.sum = 12.75), "sum");
        assert_ne!(base, edit(&|h| h.schema = 3), "schema");
        assert_ne!(base, edit(&|h| h.count += 1), "count");
        assert_ne!(base, edit(&|h| h.ts_ms += 1), "timestamp");
        assert_ne!(
            base,
            edit(&|h| h.reset_hint = pb_histogram::ResetHint::Yes),
            "reset hint"
        );
    }

    /// A comparator batch answered with a non-retryable status lands under
    /// `rejected_by_status` keyed by that status; one still failing after the
    /// retry budget lands under `retry_exhausted_samples`; the two sum to
    /// `dropped_samples`.
    #[tokio::test]
    async fn comparator_drops_are_bucketed_by_status_and_by_retry_exhaustion() {
        use std::sync::atomic::{AtomicU64, Ordering};

        // Request 1 is accepted, request 2 refused with 400, every later
        // request answered 503.
        let requests = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&requests);
        let app = axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(move || {
                let counter = Arc::clone(&counter);
                async move {
                    match counter.fetch_add(1, Ordering::SeqCst) {
                        0 => axum::http::StatusCode::NO_CONTENT,
                        1 => axum::http::StatusCode::BAD_REQUEST,
                        _ => axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let samples = valid_stream(3, 3);
        let config = HttpReplayConfig {
            system: "stub".to_string(),
            endpoint: format!("http://{addr}/api/v1/write"),
            ack_semantics: AckSemantics::Buffered,
            bearer_token: None,
            batch_size: 1,
            max_retries: 2,
            timeout_secs: 5,
        };
        let row = replay_over_http(&config, &mut SliceSamples::new(&samples))
            .await
            .expect("replay");
        server.abort();

        let ingest = &row.ingest;
        assert_eq!(ingest.offered_samples, 3);
        assert_eq!(ingest.accepted_samples, 1);
        assert_eq!(ingest.rejected_samples, 0);
        assert_eq!(ingest.dropped_samples, 2);
        assert_eq!(ingest.dropped_batches, 2);
        assert_eq!(ingest.rejected_by_status, BTreeMap::from([(400u16, 1u64)]));
        assert_eq!(ingest.retry_exhausted_samples, 1);
        assert_eq!(ingest.retries, 2, "the 503 batch spent the whole budget");
        assert_eq!(
            requests.load(Ordering::SeqCst),
            5,
            "one 2xx, one 400, and the 503 batch's first try plus two retries"
        );
        assert_eq!(
            ingest.rejected_by_status.values().sum::<u64>() + ingest.retry_exhausted_samples,
            ingest.dropped_samples
        );
        assert_eq!(row.ack_semantics, AckSemantics::Buffered);
        assert_eq!(row.last_commit_token, None, "a buffered row records no token");
    }

    /// The portable Ravel row against a stub `ravel-server`: every request
    /// carries `Authorization: Bearer <token>`, `last_commit_token` is the
    /// last `x-ravel-commit-token` the stub returned, histograms land in
    /// their own class, and the token text appears nowhere in the serialized
    /// report.
    ///
    /// TO SEE THIS FAIL: drop the `request.bearer_auth(token)` line in
    /// `replay_over_http` (the header assertion fails), or keep the first
    /// token instead of the last (the `tok-3` assertion fails).
    #[tokio::test]
    async fn the_portable_ravel_row_sends_the_bearer_and_keeps_the_last_commit_token() {
        use std::sync::Mutex as StdMutex;

        let seen: Arc<StdMutex<Vec<Option<String>>>> = Arc::new(StdMutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let app = axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(move |headers: axum::http::HeaderMap| {
                let recorder = Arc::clone(&recorder);
                async move {
                    let n = {
                        let mut seen = recorder.lock().expect("lock");
                        seen.push(
                            headers
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string),
                        );
                        seen.len()
                    };
                    (
                        axum::http::StatusCode::NO_CONTENT,
                        [(COMMIT_TOKEN_HEADER, format!("tok-{n}"))],
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let secret = "metricsbench-secret-7f3a";
        let base = base_ts_ms();
        let mut points: Vec<LogicalPoint> = valid_stream(4, 2)
            .into_iter()
            .map(LogicalPoint::Sample)
            .collect();
        points.push(LogicalPoint::Histogram(histogram(base, vec![1, 1])));
        points.push(LogicalPoint::Histogram(histogram(base + 15_000, vec![2, 1])));
        let config = HttpReplayConfig {
            system: "ravel".to_string(),
            endpoint: format!("http://{addr}/api/v1/write"),
            ack_semantics: AckSemantics::DurableOnAck,
            bearer_token: Some(secret.to_string()),
            batch_size: 2,
            max_retries: 0,
            timeout_secs: 5,
        };
        let row = replay_over_http(&config, &mut SlicePoints::new(&points))
            .await
            .expect("replay");
        server.abort();

        let seen = seen.lock().expect("lock").clone();
        assert_eq!(seen.len(), 3, "six points at two per request");
        for auth in &seen {
            assert_eq!(auth.as_deref(), Some(format!("Bearer {secret}").as_str()));
        }
        assert_eq!(row.system, "ravel");
        assert_eq!(row.endpoint, format!("http://{addr}/api/v1/write"));
        assert_eq!(row.ack_semantics, AckSemantics::DurableOnAck);
        assert_eq!(row.last_commit_token.as_deref(), Some("tok-3"));
        assert_eq!(row.ingest.offered_samples, 4);
        assert_eq!(row.ingest.accepted_samples, 4);
        assert_eq!(row.ingest.offered_histograms, 2);
        assert_eq!(row.ingest.accepted_histograms, 2);
        check_row_accounting(&row.ingest, &row.ingest.stream_counts).expect("closes");

        let report = serde_json::to_string(&MetricsIngestReport::new(vec![row]))
            .expect("serialize report");
        assert!(report.contains("\"last_commit_token\":\"tok-3\""), "{report}");
        assert!(!report.contains(secret), "the bearer token leaked: {report}");
    }

    /// An `ingest` block written before the fields added since the nightly
    /// lane began still loads, each absent field reading back as its default:
    /// `elapsed_basis` empty (wall time, unmarked), counts zero, maps empty.
    #[test]
    fn a_pre_change_artifact_loads_with_defaults() {
        let pre_change = r#"{
            "offered_samples": 120000,
            "accepted_samples": 120000,
            "rejected_samples": 0,
            "dropped_samples": 0,
            "dropped_batches": 0,
            "batches": 240,
            "retries": 0,
            "elapsed_secs": 2.5,
            "logical_points_per_sec": 48000.0,
            "wire_bytes": 1048576,
            "wire_bytes_per_sec": 419430.4,
            "ack_latency_ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0, "max": 4.0, "count": 240}
        }"#;
        let ingest: IngestPhase = serde_json::from_str(pre_change).expect("pre-change loads");
        assert_eq!(ingest.offered_samples, 120_000);
        assert_eq!(ingest.elapsed_basis, "", "unmarked means wall time");
        assert_eq!(ingest.stream_counts, StreamCounts::default());
        assert_eq!(ingest.excluded_native_histogram, 0);
        assert_eq!(ingest.excluded_stale_marker, 0);
        assert_eq!(ingest.offered_digest, "");
        assert!(ingest.rejected_by_status.is_empty());
        assert_eq!(ingest.retry_exhausted_samples, 0);
        assert_eq!(ingest.peak_rss_bytes, None);
        assert_eq!(ingest.offered_histograms, 0);
        assert_eq!(ingest.accepted_histograms, 0);
        assert_eq!(ingest.rejected_histograms, 0);
        assert_eq!(ingest.dropped_histograms, 0);
        assert!(ingest.histograms_rejected_by_status.is_empty());
        assert_eq!(ingest.retry_exhausted_histograms, 0);

        let row: SystemIngestResult = serde_json::from_value(serde_json::json!({
            "system": "mimir",
            "ack_semantics": "buffered",
            "endpoint": "http://mimir.example/api/v1/push",
            "client_behavior": "reqwest",
            "ingest": serde_json::from_str::<serde_json::Value>(pre_change).expect("json"),
        }))
        .expect("a pre-change row loads");
        assert_eq!(row.last_commit_token, None);

        let run: RunRecord = serde_json::from_str(
            r#"{"steps": 5, "total_series_created": 1000, "logical_input_bytes": 4096,
                "total_samples_generated": 5000, "scaling_label_cardinality": 1000}"#,
        )
        .expect("a pre-change run block loads");
        assert_eq!(run.anchor_ms, None);
        assert_eq!(run.workload_version, None);
        assert_eq!(run.workload_digest, None);
        assert_eq!(run.seed, None);
        assert_eq!(run.stream_digest, None);
    }

    fn provenance(anchor_ms: i64) -> RunProvenance {
        RunProvenance {
            workload_digest: workload_digest(b"fixture manifest"),
            anchor_ms,
        }
    }

    fn ci_workload() -> WorkloadFile {
        crate::metrics_workload::load_workload(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../benchmarks/metrics/workload.json"
        )))
        .expect("load the checked-in workload manifest")
    }

    /// Covers the nightly full-steps path that the JSON smoke test's
    /// `--steps 5` run cannot reach: with `steps_run == steps_declared`, the
    /// truncation branch never fires, so `comparable == false` can only be
    /// explained by the `ci` profile's own manifest verdict. Before this test
    /// existed, `--steps 5` forced `comparable` false through truncation on
    /// every run, and the profile's-own-reason branch in
    /// `build_profile_record` had zero coverage.
    #[test]
    fn profile_record_is_non_comparable_for_the_profiles_own_reason_when_not_truncated() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let ci = workload.profile("ci").expect("ci profile declared");
        // Small enough to generate fast; steps_run below is set equal to this
        // so the truncation branch never fires.
        let steps = 3;
        let profile = Profile {
            samples_per_series: steps,
            ..ci.clone()
        };

        let (bytes, gen_report) = Generator::new(&workload, "ci", 0)
            .expect("generator builds")
            .generate_bytes(steps)
            .expect("generates steps");
        let logical_input_bytes = bytes.len() as u64;

        let record = build_profile_record(
            &workload,
            &profile,
            steps,
            logical_input_bytes,
            &gen_report,
            provenance(0),
        )
        .expect("the stream regenerates");

        assert_eq!(
            record.steps_run, record.steps_declared,
            "this test's premise is steps_run == steps_declared"
        );
        assert!(
            !record.comparable,
            "the ci profile is non-comparable by its own manifest verdict, not by truncation"
        );
        assert_eq!(
            record.comparability_reason.as_deref(),
            ci.comparability.reason(),
            "the reason must be the manifest's own reason, not a truncation message, since \
             this run was not truncated"
        );

        assert_eq!(
            record.scaling_label.name, workload.generator.scaling_label,
            "scaling_label.name must name the manifest's own scaling label"
        );
        let generator = Generator::for_profile(&workload, &profile, 0);
        assert_eq!(
            record.scaling_label.cardinality_declared,
            generator.scaling_label_cardinality(record.steps_declared),
            "cardinality_declared must be the generator's own figure at steps_declared"
        );
        assert_eq!(
            record.run.scaling_label_cardinality,
            generator.scaling_label_cardinality(steps),
            "run.scaling_label_cardinality must be the generator's own figure at steps_run"
        );
    }

    /// The other direction: a run PAST the profile's declared step count is
    /// not that profile either, and `--steps` takes any value. Without this
    /// the rule would read `steps_run < steps_declared` and an over-long run
    /// of a comparable profile would publish as that profile.
    #[test]
    fn profile_record_is_non_comparable_when_the_run_exceeds_the_declared_steps() {
        use crate::metrics_gen::Generator;
        use crate::metrics_workload::Comparability;

        let workload = ci_workload();
        let ci = workload.profile("ci").expect("ci profile declared");
        let steps_declared = 10;
        let steps_run = 12;
        let profile = Profile {
            comparability: Comparability::Comparable,
            samples_per_series: steps_declared,
            ..ci.clone()
        };
        assert!(
            profile.is_publishable(),
            "this test's premise is a profile that IS comparable"
        );
        assert!(
            steps_run > steps_declared,
            "this test's premise is a run PAST the declared step count"
        );

        let (bytes, gen_report) = Generator::new(&workload, "ci", 0)
            .expect("generator builds")
            .generate_bytes(steps_run)
            .expect("generates steps");
        let record = build_profile_record(
            &workload,
            &profile,
            steps_run,
            bytes.len() as u64,
            &gen_report,
            provenance(0),
        )
        .expect("the stream regenerates");

        assert!(
            !record.comparable,
            "a run past the declared step count is never comparable, even when the profile is"
        );
        let expected_reason = format!(
            "this run generated {steps_run} steps against profile `{}`'s {steps_declared}, so it \
             is not that profile and its figures cannot be published",
            profile.name
        );
        assert_eq!(
            record.comparability_reason,
            Some(expected_reason),
            "the reason names both step counts"
        );
        assert_eq!(
            record.run.steps, steps_run,
            "run.steps is what this run generated, not the declared count"
        );
    }

    /// The mirror case: a profile whose own verdict IS comparable, but the
    /// run truncates it. `comparable` must still be `false`, and the reason
    /// must be the truncation message, not the (absent) profile reason --
    /// the forcing rule in `build_profile_record` applies regardless of the
    /// profile's own verdict.
    #[test]
    fn profile_record_is_non_comparable_from_truncation_even_when_the_profile_is_comparable() {
        use crate::metrics_gen::Generator;
        use crate::metrics_workload::Comparability;

        let workload = ci_workload();
        let ci = workload.profile("ci").expect("ci profile declared");
        let steps_declared = 10;
        let steps_run = 3;
        let profile = Profile {
            comparability: Comparability::Comparable,
            samples_per_series: steps_declared,
            ..ci.clone()
        };
        assert!(
            profile.is_publishable(),
            "this test's premise is a profile that IS comparable"
        );

        let anchor_ms = 1_700_000_010_000;
        let (bytes, gen_report) = Generator::new(&workload, "ci", anchor_ms)
            .expect("generator builds")
            .generate_bytes(steps_run)
            .expect("generates steps");
        let logical_input_bytes = bytes.len() as u64;

        let record = build_profile_record(
            &workload,
            &profile,
            steps_run,
            logical_input_bytes,
            &gen_report,
            provenance(anchor_ms),
        )
        .expect("the stream regenerates");

        assert_eq!(record.run.anchor_ms, Some(anchor_ms));
        assert_eq!(
            record.run.stream_digest.as_deref(),
            Some(&*gen_report.digest)
        );
        assert_eq!(record.run.workload_version, Some(workload.version));
        assert_eq!(record.run.seed, Some(workload.seed));
        assert_eq!(
            record.run.workload_digest,
            Some(workload_digest(b"fixture manifest"))
        );

        assert!(
            steps_run < steps_declared,
            "this test's premise is a truncated run"
        );
        assert!(
            !record.comparable,
            "a truncated run is never comparable, even when the profile itself is"
        );
        let expected_reason = format!(
            "this run generated {steps_run} steps against profile `{}`'s {steps_declared}, so it \
             is not that profile and its figures cannot be published",
            profile.name
        );
        assert_eq!(
            record.comparability_reason,
            Some(expected_reason),
            "the truncation reason must state the run's actual vs declared step counts"
        );

        let generator = Generator::for_profile(&workload, &profile, 0);
        assert_eq!(
            record.scaling_label.name, workload.generator.scaling_label,
            "scaling_label.name must name the manifest's own scaling label"
        );
        assert_eq!(
            record.scaling_label.cardinality_declared,
            generator.scaling_label_cardinality(steps_declared),
            "cardinality_declared must be the generator's own figure at steps_declared"
        );
        assert_eq!(
            record.run.scaling_label_cardinality,
            generator.scaling_label_cardinality(steps_run),
            "run.scaling_label_cardinality must be the generator's own figure at steps_run, \
             not steps_declared"
        );
    }
}

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

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
use ravel_promql_difftest::generator::STALE_MARKER_BITS;
use ravel_query::{EngineConfig, QueryEngine};
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

/// The `system` of the in-process diagnostic Ravel row and its query row. The
/// portable Ravel row over `/api/v1/write` is `ravel`, like every other engine
/// named by its own name; the diagnostic row gets its own name so a lookup by
/// `system` can never return one in place of the other (ADR-0927 amendment A).
pub const IN_PROCESS_SYSTEM: &str = "ravel-in-process";

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

/// The three payload classes a replay sends, each accounted and digested on
/// its own (ADR-0927 band 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadClass {
    /// A finite float sample.
    Scalar,
    /// A float sample carrying [`STALE_MARKER_BITS`].
    StaleMarker,
    /// A native histogram.
    NativeHistogram,
}

impl PayloadClass {
    /// Every class, in report order.
    pub const ALL: [PayloadClass; 3] = [
        PayloadClass::Scalar,
        PayloadClass::StaleMarker,
        PayloadClass::NativeHistogram,
    ];

    /// The class `point` is sent as.
    pub fn of(point: &LogicalPoint) -> Self {
        match point {
            LogicalPoint::Sample(s) if s.is_stale_marker() => PayloadClass::StaleMarker,
            LogicalPoint::Sample(_) => PayloadClass::Scalar,
            LogicalPoint::Histogram(_) => PayloadClass::NativeHistogram,
        }
    }

    /// The snake_case name the report uses.
    pub fn name(self) -> &'static str {
        match self {
            PayloadClass::Scalar => "scalar",
            PayloadClass::StaleMarker => "stale_marker",
            PayloadClass::NativeHistogram => "native_histogram",
        }
    }
}

/// How a row's accepted set is established.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceBasis {
    /// From the receiver's response alone: a 2xx, or a 400 whose body names
    /// the samples it refused.
    Acknowledged,
    /// From the response, then checked by reading the data back over the
    /// receiver's query API: a Remote Write 1.0 response carries no written
    /// count, and ravel-server answers 204 after partial-admission drops. The
    /// read-back counts series and one probe series per class, so it catches
    /// a dropped series or a dropped point in a probe series, not every
    /// dropped point.
    ReadBack,
}

/// The portable receivers this lane replays into over Remote Write 1.0, each
/// with the response semantics [`replay_over_http`] reads its accepted set by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Receiver {
    /// `ravel-server`: a 2xx is a durable ack carrying commit tokens; the
    /// accepted set is confirmed by read-back. A 400 rejects the request.
    Ravel,
    /// Prometheus: a 400 rejects the whole request.
    Prometheus,
    /// VictoriaMetrics: a 2xx accepts every point except the classes
    /// [`UNSUPPORTED_CLASSES`] names for it.
    VictoriaMetrics,
    /// Mimir: a 400 is a per-sample soft rejection; the body names the
    /// rejected sample and the rest of the request is stored.
    Mimir,
}

impl Receiver {
    /// The row's `system` name.
    pub fn system(self) -> &'static str {
        match self {
            Receiver::Ravel => "ravel",
            Receiver::Prometheus => "prometheus",
            Receiver::VictoriaMetrics => "victoriametrics",
            Receiver::Mimir => "mimir",
        }
    }

    /// What a 2xx from this receiver means (ADR-0927 decision 3).
    pub fn ack_semantics(self) -> AckSemantics {
        match self {
            Receiver::Ravel => AckSemantics::DurableOnAck,
            _ => AckSemantics::Buffered,
        }
    }

    /// How this receiver's accepted set is established.
    pub fn acceptance_basis(self) -> AcceptanceBasis {
        match self {
            Receiver::Ravel => AcceptanceBasis::ReadBack,
            _ => AcceptanceBasis::Acknowledged,
        }
    }

    /// Whether a 400 from this receiver names individual rejected samples
    /// (Mimir) rather than rejecting the whole request.
    fn soft_rejects_samples(self) -> bool {
        matches!(self, Receiver::Mimir)
    }

    /// Whether this receiver stores `class`, per [`UNSUPPORTED_CLASSES`].
    pub fn supports(self, class: PayloadClass) -> bool {
        !UNSUPPORTED_CLASSES
            .iter()
            .any(|u| u.receiver == self && u.class == class)
    }

    /// This receiver's entries of [`UNSUPPORTED_CLASSES`], as recorded on
    /// its row.
    pub fn unsupported_classes(self) -> Vec<UnsupportedClassRecord> {
        UNSUPPORTED_CLASSES
            .iter()
            .filter(|u| u.receiver == self)
            .map(|u| UnsupportedClassRecord {
                class: u.class,
                source: u.source.to_string(),
            })
            .collect()
    }
}

/// A payload class a receiver acknowledges but cannot store, with the source
/// that shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsupportedClass {
    /// The receiver.
    pub receiver: Receiver,
    /// The class it does not store.
    pub class: PayloadClass,
    /// The citation.
    pub source: &'static str,
}

/// The one table of classes a receiver acknowledges without storing. Every
/// class is still sent to every receiver; a point of a class listed here is
/// counted under `unsupported_*`, not `accepted_*`, and band 6 skips that
/// class for that row (`tests::the_unsupported_class_table_names_only_victoriametrics_histograms`).
pub const UNSUPPORTED_CLASSES: &[UnsupportedClass] = &[UnsupportedClass {
    receiver: Receiver::VictoriaMetrics,
    class: PayloadClass::NativeHistogram,
    source: "VictoriaMetrics lib/prompb/prompb.go at v1.115.0: TimeSeries.unmarshalProtobuf \
             reads only labels (field 1) and samples (field 2), so a TimeSeries' histograms \
             (field 4) are skipped while the request is answered 2xx",
}];

/// One entry of a row's `unsupported_classes`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedClassRecord {
    /// The class the receiver does not store.
    pub class: PayloadClass,
    /// The citation, from [`UNSUPPORTED_CLASSES`].
    pub source: String,
}

/// One [`MultisetDigest`] (hex) per payload class over that class's points,
/// each point encoded as [`OfferedDigest`] encodes it. The digest is the sum
/// of per-point hashes, so it does not depend on the order the points were
/// offered or acknowledged in, and a point counted twice changes it
/// (`tests::class_digests_ignore_order_and_count_duplicates`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassDigests {
    pub scalar: String,
    pub stale_marker: String,
    pub native_histogram: String,
}

impl ClassDigests {
    /// The digest of `class`.
    pub fn get(&self, class: PayloadClass) -> &str {
        match class {
            PayloadClass::Scalar => &self.scalar,
            PayloadClass::StaleMarker => &self.stale_marker,
            PayloadClass::NativeHistogram => &self.native_histogram,
        }
    }
}

/// Builds a [`ClassDigests`], one [`MultisetDigest`] per class.
#[derive(Clone, Debug, Default)]
struct ClassDigester {
    scalar: MultisetDigest,
    stale_marker: MultisetDigest,
    native_histogram: MultisetDigest,
}

impl ClassDigester {
    fn update(&mut self, point: &LogicalPoint) {
        match PayloadClass::of(point) {
            PayloadClass::Scalar => self.scalar.add_point(point),
            PayloadClass::StaleMarker => self.stale_marker.add_point(point),
            PayloadClass::NativeHistogram => self.native_histogram.add_point(point),
        }
    }

    fn finish(&self) -> ClassDigests {
        ClassDigests {
            scalar: self.scalar.finish(),
            stale_marker: self.stale_marker.finish(),
            native_histogram: self.native_histogram.finish(),
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
        if !self.sum.is_finite() {
            return Err(RejectionReason::InvalidHistogram);
        }
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

/// [`IngestPhase::elapsed_basis`] of the in-process row, and of every HTTP row
/// written before the 2026-10-28 amendment to ADR-0927: `elapsed_secs` is the
/// sum of the timed write segments, never the replay's wall time.
pub const ELAPSED_BASIS_TIMED_SEGMENTS: &str = "timed_segments";

/// [`IngestPhase::elapsed_basis`] of an HTTP row (ADR-0927, 2026-10-28
/// amendment, decision 4): `elapsed_secs` is the wall time from the first
/// request sent to the last response received, with up to one request in
/// flight per shard.
pub const ELAPSED_BASIS_WALL_IN_FLIGHT: &str = "wall_in_flight";

/// A row is sender-bound when generating its stream took at least this
/// fraction of its `elapsed_secs` ([`IngestPhase::sender_bound`]).
pub const SENDER_BOUND_FRACTION: f64 = 0.9;

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
    /// staleness markers. `accepted + rejected + dropped + unsupported`
    /// ([`check_row_accounting`]).
    pub offered_samples: u64,
    /// BLAKE3 over every point this row offered, in offered order: each
    /// sample's canonical `(series, ts_ms, value_bits)`, a staleness marker
    /// by its bit pattern, and each native histogram by every field it is
    /// encoded with ([`OfferedDigest`]). Empty in an artifact written before
    /// it existed.
    #[serde(default)]
    pub offered_digest: String,
    /// Samples the system accepted, by the row's `acceptance_basis`: on an
    /// HTTP row the samples in requests answered 2xx, plus, on a Mimir 400,
    /// every sample of the request its body does not name. A Remote Write 1.0
    /// response carries no written count, so on an `acknowledged` row a point
    /// the receiver drops after a 2xx is still counted here; the portable
    /// Ravel row is `read_back`, and its `readback` checks the series count
    /// and one probe series per class against it, not every point.
    /// Samples of a class the receiver cannot store are under
    /// `unsupported_samples` instead.
    pub accepted_samples: u64,
    /// Samples the lane refused before sending (client-side admission).
    pub rejected_samples: u64,
    /// Native histograms offered to the system, their own class, never
    /// counted in `offered_samples`. `accepted_histograms +
    /// rejected_histograms + dropped_histograms + unsupported_histograms`.
    #[serde(default)]
    pub offered_histograms: u64,
    /// Native histograms the system accepted, by the same rule as
    /// `accepted_samples`.
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
    /// Native histograms in `retry_exhausted_histograms` whose last attempt
    /// ended on a retryable status, keyed by it; a transport error has no
    /// status and is counted only in the total.
    #[serde(default)]
    pub histograms_retry_exhausted_by_status: BTreeMap<u16, u64>,
    /// Samples sent and not accepted by the receiver: neither accepted nor
    /// rejected by the lane, so the accounting still closes. On every row
    /// `rejected_by_status` summed plus `retry_exhausted_samples` equals it
    /// ([`check_row_accounting`]). The in-process row has no status and no
    /// retry loop, so a strict write that did not ack fails that identity,
    /// as the zero drop budget requires.
    pub dropped_samples: u64,
    /// Batches counted under `dropped_samples`.
    pub dropped_batches: u64,
    /// Comparator samples in batches that ended on a non-retryable non-2xx
    /// response, keyed by that HTTP status. Part of `dropped_samples`.
    #[serde(default)]
    pub rejected_by_status: BTreeMap<u16, u64>,
    /// Samples in batches that ended on a retryable status (429 or 5xx) or
    /// a transport error after the retry budget, which may be zero. Part of
    /// `dropped_samples`.
    #[serde(default)]
    pub retry_exhausted_samples: u64,
    /// Samples in `retry_exhausted_samples` whose last attempt ended on a
    /// retryable status, keyed by it; a transport error has no status and is
    /// counted only in the total.
    #[serde(default)]
    pub retry_exhausted_by_status: BTreeMap<u16, u64>,
    /// Samples (scalar and stale marker) the receiver acknowledged in a class
    /// it cannot store ([`UNSUPPORTED_CLASSES`]). Not accepted, and not an
    /// `excluded_*` line: the point was sent.
    #[serde(default)]
    pub unsupported_samples: u64,
    /// Native histograms the receiver acknowledged without storing
    /// ([`UNSUPPORTED_CLASSES`]).
    #[serde(default)]
    pub unsupported_histograms: u64,
    /// Per class, the digest of every point the lane sent: the valid offered
    /// points, the set a receiver that stores the class must hold (band 6).
    /// Empty strings in an artifact written before it existed.
    #[serde(default)]
    pub expected_digests: ClassDigests,
    /// Per class, the digest of the points counted accepted. Band 6 compares
    /// it with `expected_digests` for every class the row does not list in
    /// `unsupported_classes` ([`check_row_band6`]).
    #[serde(default)]
    pub accepted_digests: ClassDigests,
    /// Batches offered (one write call each).
    pub batches: u64,
    /// Transport retries: object-store PUT retries below `InstrumentedStore`
    /// for Ravel, HTTP 429/5xx retries for a comparator.
    pub retries: u64,
    /// Time of the ingest phase, on the basis `elapsed_basis` names. On an
    /// HTTP row ([`ELAPSED_BASIS_WALL_IN_FLIGHT`]) it is the wall time from
    /// the first request sent to the last response received; the stream is
    /// generated concurrently with sending, and that generation time is
    /// `generation_secs`. On the in-process row
    /// ([`ELAPSED_BASIS_TIMED_SEGMENTS`]) it is the sum of the in-process
    /// `write_values` calls and the final `flush_all`, with generation and
    /// the RW1.0 encode that yields `wire_bytes` outside it. The two bases
    /// time different work, so a throughput comparison across them must
    /// account for it.
    pub elapsed_secs: f64,
    /// What `elapsed_secs` measures, so two artifacts can be told apart
    /// before their throughput figures are compared:
    /// [`ELAPSED_BASIS_WALL_IN_FLIGHT`] on an HTTP row this code writes,
    /// [`ELAPSED_BASIS_TIMED_SEGMENTS`] on the in-process row and on an HTTP
    /// row written before the 2026-10-28 amendment to ADR-0927, whose figures
    /// are not compared with wall-time ones. Reports written before this
    /// field existed carry no marker and read back as the empty string,
    /// meaning wall time, unmarked, over a stream generated up front.
    #[serde(default)]
    pub elapsed_basis: String,
    /// Seconds the replay spent pulling points from the stream (generating,
    /// classifying and admitting them, inside `fill_valid`) and, on an HTTP
    /// row, routing them: observing each into the read-back plan, hashing
    /// its series to a shard and buffering it (`Producer::round`,
    /// `tests::a_producer_round_counts_routing_as_generation`). Waiting on a
    /// full shard queue is not counted. Zero in an artifact written before
    /// it existed.
    #[serde(default)]
    pub generation_secs: f64,
    /// On a [`ELAPSED_BASIS_WALL_IN_FLIGHT`] row, true when `generation_secs`
    /// is at least [`SENDER_BOUND_FRACTION`] of `elapsed_secs`: the row's
    /// throughput then says more about the generator than the receiver.
    /// Always false on a [`ELAPSED_BASIS_TIMED_SEGMENTS`] row, whose
    /// `elapsed_secs` excludes generation ([`is_sender_bound`]).
    #[serde(default)]
    pub sender_bound: bool,
    /// Accepted logical points (scalar samples plus native histograms) per
    /// second.
    pub logical_points_per_sec: f64,
    /// Bytes of the snappy-compressed RW1.0 body offered (the wire shape every
    /// system receives), across accepted-or-dropped batches.
    pub wire_bytes: u64,
    /// Wire bytes per second over the ingest wall time.
    pub wire_bytes_per_sec: f64,
    /// Acknowledgement latency percentiles, per batch, from its first send to
    /// its 2xx response, retries included. Its meaning depends on the row's
    /// [`AckSemantics`]: for a durable-on-ack row this is time to durability,
    /// for a buffered row time to buffer-accept. On an HTTP row it includes
    /// queueing inside the receiver behind the other requests in flight.
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
    /// Stored bytes divided by accepted logical points (scalar samples plus
    /// native histograms), the same records `stored_bytes` holds.
    #[serde(alias = "stored_bytes_per_sample")]
    pub stored_bytes_per_point: f64,
    /// Stored bytes over the logical bytes of the accepted points: 16 per
    /// scalar sample (timestamp and value) and, per native histogram, 24
    /// (timestamp, sum and count) plus 8 per bucket ([`logical_bytes_of`]).
    pub write_amplification: f64,
    /// Whether a request against this backend is billed: `false` on
    /// `MemoryStore` (real counts, but free), `true` on S3. The explicit
    /// representation of a non-billable count instead of a misleading zero.
    pub backend_bills_requests: bool,
}

/// One system's ingest result row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemIngestResult {
    /// The system: `ravel` (the portable row over HTTP), `prometheus`,
    /// `victoriametrics`, `mimir`, or [`IN_PROCESS_SYSTEM`] for the in-process
    /// diagnostic row. Each name names one row.
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
    /// The latest encoded commit token per shard, over every
    /// `x-ravel-commit-token` header the replay received (the server joins
    /// one token per shard a request touched with commas). Set only on the
    /// portable Ravel row
    /// (`tests::the_portable_ravel_row_sends_the_bearer_and_keeps_the_latest_token_of_every_shard`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_tokens_by_shard: Option<BTreeMap<u32, String>>,
    /// `commit_tokens_by_shard`'s tokens joined with commas, in shard order:
    /// a read-your-write fence over every shard the replay wrote, not only
    /// the shards the last request touched. Set only on the portable Ravel
    /// row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_commit_token: Option<String>,
    /// How `accepted_*` was established. Set on every HTTP row; `None` on the
    /// in-process row, whose strict write receipt is its accepted set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_basis: Option<AcceptanceBasis>,
    /// The classes this row's receiver acknowledges without storing, each
    /// with its citation ([`UNSUPPORTED_CLASSES`]). Empty for a receiver
    /// that stores every class.
    #[serde(default)]
    pub unsupported_classes: Vec<UnsupportedClassRecord>,
    /// The tenant the row wrote under, as declared by the operator:
    /// `--tenant` on the in-process row, `--ravel-tenant` on the portable
    /// Ravel row, `None` on a comparator, which is sent no tenant. Declared,
    /// not observed: no response carries a tenant and [`CommitToken`] holds
    /// none.
    #[serde(default)]
    pub tenant: Option<String>,
    /// What the receiver's query API returned for the replayed data, against
    /// what the lane sent ([`readback_over_http`]). `None` on the in-process
    /// row, whose read-your-write phase is its query row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readback: Option<Readback>,
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
            system: IN_PROCESS_SYSTEM.to_string(),
            ack_semantics: AckSemantics::DurableOnAck,
            endpoint,
            client_behavior: "in-process IngestRouter, WriteMode::Strict: one write_values call \
                 per batch, awaited to durable ack (data object + commit record); object-store \
                 PUT retries handled below InstrumentedStore (max_retries=10); backpressure via \
                 the shard channel's blocking send"
                .to_string(),
            ingest,
            commit_tokens: Some(commit_tokens),
            commit_tokens_by_shard: None,
            last_commit_token: None,
            acceptance_basis: None,
            unsupported_classes: Vec::new(),
            tenant: None,
            readback: None,
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
            commit_tokens_by_shard: None,
            last_commit_token: None,
            acceptance_basis: Some(AcceptanceBasis::Acknowledged),
            unsupported_classes: Vec::new(),
            tenant: None,
            readback: None,
            storage: None,
        }
    }

    /// Build a durable-on-ack HTTP row, the portable Ravel row over
    /// `ravel-server`'s `/api/v1/write`, carrying the latest commit token of
    /// every shard the replay wrote. One token per shard, not one per
    /// request: a long replay sends one request per batch, so `commit_tokens`
    /// stays absent and the row stays bounded by the shard count.
    pub fn durable_http_row(
        system: impl Into<String>,
        endpoint: impl Into<String>,
        client_behavior: impl Into<String>,
        ingest: IngestPhase,
        commit_tokens_by_shard: BTreeMap<u32, String>,
    ) -> Self {
        let joined = (!commit_tokens_by_shard.is_empty()).then(|| {
            commit_tokens_by_shard
                .values()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        });
        SystemIngestResult {
            system: system.into(),
            ack_semantics: AckSemantics::DurableOnAck,
            endpoint: endpoint.into(),
            client_behavior: client_behavior.into(),
            ingest,
            commit_tokens: None,
            commit_tokens_by_shard: Some(commit_tokens_by_shard),
            last_commit_token: joined,
            acceptance_basis: Some(AcceptanceBasis::ReadBack),
            unsupported_classes: Vec::new(),
            tenant: None,
            readback: None,
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

/// The figures a read-back measures, or expects. A figure for a class the
/// receiver does not store is `None` in `expected` and is not queried.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadbackExpected {
    /// Distinct series over the replay window, every stored class.
    pub series_count: u64,
    /// `count_over_time` summed over the scalar probe series and the stale
    /// marker probe series: their float samples, which excludes the marker
    /// itself (PromQL range functions skip stale samples).
    pub sample_count: u64,
    /// Distinct series of the native-histogram metric names.
    pub histogram_series_count: Option<u64>,
    /// `count_over_time` over the native-histogram probe series.
    pub histogram_sample_count: Option<u64>,
    /// Whether an instant query at the stale marker probe's timestamp
    /// returns nothing. `None` when no marker was sent, or when its series
    /// sent no float sample within the 5 minute lookback before it, so a
    /// missing marker would read back empty as well.
    pub stale_marker_honored: Option<bool>,
}

/// One HTTP row's read-back over its receiver's query API, after the replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readback {
    /// The query API base URL the read-back ran against.
    pub query_url: String,
    /// Distinct series `/api/v1/series` returned over the replay window.
    pub series_count: u64,
    /// `count_over_time` summed over the float probe series.
    pub sample_count: u64,
    /// Distinct series of the native-histogram metric names, `None` when the
    /// receiver does not store native histograms.
    pub histogram_series_count: Option<u64>,
    /// `count_over_time` over the native-histogram probe series, `None` when
    /// unqueried.
    pub histogram_sample_count: Option<u64>,
    /// Whether the stale marker probe read back as stale, `None` when
    /// unqueried.
    pub stale_marker_honored: Option<bool>,
    /// What the lane sent, in the same figures.
    pub expected: ReadbackExpected,
    /// Each figure that differs from `expected`, named.
    pub mismatches: Vec<String>,
    /// `mismatches` is empty.
    pub matches_expected: bool,
    /// Read-backs taken, the reported one included: 1 when the first
    /// matched or no settle window was given. 0 in a report written before
    /// the field existed.
    #[serde(default)]
    pub attempts: u32,
    /// Milliseconds from the start of the first read-back to the end of the
    /// one reported.
    #[serde(default)]
    pub settled_after_ms: u64,
    /// Wall milliseconds of each query of the reported read-back, keyed by
    /// [`READBACK_QUERY_SERIES`] and its siblings. Empty when no window was
    /// queried, and in a report written before the field existed.
    #[serde(default)]
    pub query_ms: BTreeMap<String, u64>,
}

/// The read-your-write query phase of a system's replay. Recorded as its own
/// phase, separate from the ingest phase (ADR-0927 decision 9): ingest and
/// query figures never share a row. Token-bound: the ingest phase's commit
/// tokens are passed as `min_commit_token`, so the read resolves the exact
/// committed segments deterministically without sleeping past the flush delay
/// (ADR-0927 decision 3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemQueryResult {
    /// The system queried: [`IN_PROCESS_SYSTEM`] (the in-process path, whose
    /// commit tokens the bench holds in full and can read deterministically).
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

/// Whether a run built its in-process row ([`IN_PROCESS_SYSTEM`]), recorded
/// at the top of the report so a reader of a run without one can tell a skip
/// from a lost row. [`MetricsIngestReport::new`] derives it from the rows
/// (`tests::the_in_process_row_disclosure_follows_the_rows`); a report
/// written before the field existed always had the row, so it reads as
/// `present`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InProcessRow {
    /// The run replayed into the in-process Ravel path.
    #[default]
    Present,
    /// The run was given `--no-in-process-row`: portable rows only, no store
    /// opened, and no post-ingest query phase.
    Skipped,
}

/// The whole ingest-lane report: one row per participating system, plus the
/// separately-recorded query-phase rows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsIngestReport {
    /// Whether [`IN_PROCESS_SYSTEM`] is among `systems`.
    #[serde(default)]
    pub in_process_row: InProcessRow,
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
    /// or substrate attached yet. `in_process_row` is `present` exactly when
    /// a row is [`IN_PROCESS_SYSTEM`].
    pub fn new(systems: Vec<SystemIngestResult>) -> Self {
        let in_process_row = if systems.iter().any(|r| r.system == IN_PROCESS_SYSTEM) {
            InProcessRow::Present
        } else {
            InProcessRow::Skipped
        };
        MetricsIngestReport {
            in_process_row,
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
    /// A row configured as durable-on-ack acknowledged requests without ever
    /// returning a commit token, so nothing the receiver did backs the
    /// durability claim (an endpoint that is not ravel-server, or a proxy that
    /// strips the header).
    #[error(
        "{system} at {endpoint} acknowledged {acknowledged} points but never returned \
         x-ravel-commit-token, so its durable-on-ack claim is unbacked"
    )]
    UnbackedDurability {
        /// The row's system.
        system: String,
        /// The row's endpoint.
        endpoint: String,
        /// Points in acknowledged requests.
        acknowledged: u64,
    },
    /// An `x-ravel-commit-token` entry did not decode as a commit token, so
    /// the shard it fences is unknown.
    #[error("{system} returned x-ravel-commit-token entry `{value}`, which is not a commit token")]
    InvalidCommitToken {
        /// The row's system.
        system: String,
        /// The entry, verbatim.
        value: String,
    },
    /// Two commit tokens for one shard named different writers, so the
    /// shard's newest token cannot be chosen by `(epoch, seq)`.
    #[error(
        "{system} returned commit tokens from two writers on shard {shard}: \
         {held_writer} and {new_writer}"
    )]
    CommitTokenWriterConflict {
        /// The row's system.
        system: String,
        /// The shard both tokens fence.
        shard: u32,
        /// The writer of the token already held.
        held_writer: String,
        /// The writer of the token that conflicted.
        new_writer: String,
    },
    /// The row's projected ingest wall time exceeded its ceiling
    /// ([`StallCeiling`]), so it was stopped rather than left to run.
    #[error(
        "{system} ingest stalled: projected {projected_secs:.0} s for the stream exceeds the \
         {ceiling_secs} s ceiling after offering {offered_points} of {stream_points} points"
    )]
    IngestStalled {
        /// The row's system.
        system: String,
        /// The projected total wall time.
        projected_secs: f64,
        /// The ceiling it exceeded.
        ceiling_secs: u64,
        /// Points offered when the check ran.
        offered_points: u64,
        /// Points the whole stream offers.
        stream_points: u64,
    },
    /// A read-back query failed or answered a shape the lane cannot read.
    #[error("the read-back over {system}'s query API failed: {detail}")]
    Readback {
        /// The row's system.
        system: String,
        /// What failed.
        detail: String,
    },
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
     offered against {accepted} accepted + {rejected} rejected + {dropped} dropped + \
     {unsupported} unsupported samples; {offered_histograms} offered against \
     {accepted_histograms} accepted + {rejected_histograms} rejected + {dropped_histograms} \
     dropped + {unsupported_histograms} unsupported histograms; {rejected_by_status} \
     rejected_by_status + {retry_exhausted} retry_exhausted against {dropped} dropped samples; \
     {histograms_rejected_by_status} histograms_rejected_by_status + \
     {retry_exhausted_histograms} retry_exhausted against {dropped_histograms} dropped \
     histograms. A silent drop fails the run."
)]
pub struct RowAccountingViolation {
    pub unsupported: u64,
    pub unsupported_histograms: u64,
    /// `rejected_by_status` summed.
    pub rejected_by_status: u64,
    pub retry_exhausted: u64,
    /// `histograms_rejected_by_status` summed.
    pub histograms_rejected_by_status: u64,
    pub retry_exhausted_histograms: u64,
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
/// `offered_histograms + excluded_native_histogram == native_histogram`;
/// each class's offered count equal to its accepted + rejected + dropped +
/// unsupported; and each class's dropped count equal to its by-status
/// rejections summed plus its retry-exhausted count, on every row
/// (`tests::dropped_points_must_be_bucketed_by_status_or_retry_exhaustion`).
/// `counts` is the stream's own classification ([`GeneratedStream::finish`]).
pub fn check_row_accounting(
    ingest: &IngestPhase,
    counts: &StreamCounts,
) -> Result<(), Box<RowAccountingViolation>> {
    let offering_total = ingest
        .offered_samples
        .saturating_add(ingest.offered_histograms)
        .saturating_add(ingest.excluded_native_histogram)
        .saturating_add(ingest.excluded_stale_marker);
    let split = ingest
        .accepted_samples
        .saturating_add(ingest.rejected_samples)
        .saturating_add(ingest.dropped_samples)
        .saturating_add(ingest.unsupported_samples);
    let histogram_split = ingest
        .accepted_histograms
        .saturating_add(ingest.rejected_histograms)
        .saturating_add(ingest.dropped_histograms)
        .saturating_add(ingest.unsupported_histograms);
    let by_status = |map: &BTreeMap<u16, u64>| map.values().fold(0u64, |a, b| a.saturating_add(*b));
    let rejected_by_status = by_status(&ingest.rejected_by_status);
    let histograms_rejected_by_status = by_status(&ingest.histograms_rejected_by_status);
    if offering_total == counts.generated_lines
        && rejected_by_status.saturating_add(ingest.retry_exhausted_samples)
            == ingest.dropped_samples
        && histograms_rejected_by_status.saturating_add(ingest.retry_exhausted_histograms)
            == ingest.dropped_histograms
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
    Err(Box::new(RowAccountingViolation {
        unsupported: ingest.unsupported_samples,
        unsupported_histograms: ingest.unsupported_histograms,
        rejected_by_status,
        retry_exhausted: ingest.retry_exhausted_samples,
        histograms_rejected_by_status,
        retry_exhausted_histograms: ingest.retry_exhausted_histograms,
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
    }))
}

/// A band 6 failure (ADR-0927 amendment B): a row's accepted set is not its
/// expected set for a class it stores, or two rows were not offered the same
/// expected set.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Band6Violation {
    /// The accepted-set digest differs from the expected-set digest.
    #[error(
        "{system}: the accepted {class} set (digest {accepted}) is not the expected set \
         (digest {expected}); band 6 requires every point of a class the receiver stores to be \
         accepted, with a drop budget of zero",
        class = class.name()
    )]
    AcceptedDiffers {
        system: String,
        class: PayloadClass,
        expected: String,
        accepted: String,
    },
    /// Two rows carry different expected-set digests for one class.
    #[error(
        "the expected {class} set differs between {first_system} (digest {first}) and \
         {second_system} (digest {second}); every class is sent to every portable receiver, so \
         each row's expected set is the whole stream",
        class = class.name()
    )]
    ExpectedDiverged {
        class: PayloadClass,
        first_system: String,
        first: String,
        second_system: String,
        second: String,
    },
}

/// Band 6 on one row: for each class the row does not list in
/// `unsupported_classes`, the accepted-set digest equals the expected-set
/// digest. A digest, not a count: a row that accepted as many points as it
/// was sent but not the same ones fails
/// (`tests::band6_compares_digests_not_counts`).
pub fn check_row_band6(row: &SystemIngestResult) -> Result<(), Band6Violation> {
    for class in PayloadClass::ALL {
        if row.unsupported_classes.iter().any(|u| u.class == class) {
            continue;
        }
        let expected = row.ingest.expected_digests.get(class);
        let accepted = row.ingest.accepted_digests.get(class);
        if expected != accepted {
            return Err(Band6Violation::AcceptedDiffers {
                system: row.system.clone(),
                class,
                expected: expected.to_string(),
                accepted: accepted.to_string(),
            });
        }
    }
    Ok(())
}

/// Band 6 across rows: every row in `rows` carries the same expected-set
/// digest for every class in `classes`. The bin passes every portable row
/// with all three classes, and the in-process row with each portable row and
/// the scalar class
/// (`tests::portable_rows_with_different_expected_digests_fail`).
pub fn check_expected_digests_agree(
    rows: &[&SystemIngestResult],
    classes: &[PayloadClass],
) -> Result<(), Band6Violation> {
    let Some((first, rest)) = rows.split_first() else {
        return Ok(());
    };
    for row in rest {
        for class in classes {
            let a = first.ingest.expected_digests.get(*class);
            let b = row.ingest.expected_digests.get(*class);
            if a != b {
                return Err(Band6Violation::ExpectedDiverged {
                    class: *class,
                    first_system: first.system.clone(),
                    first: a.to_string(),
                    second_system: row.system.clone(),
                    second: b.to_string(),
                });
            }
        }
    }
    Ok(())
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
        self.hasher
            .update(&wire.zero_threshold.to_bits().to_le_bytes());
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

/// An order-independent digest of a multiset of points (ADR-0927, 2026-10-28
/// amendment, decision 5): the sum, modulo 2^256, of BLAKE3 over each point's
/// [`OfferedDigest`] encoding, each hash read as a little-endian integer. It
/// renders as the 32 little-endian bytes of the sum in lowercase hex, so the
/// digest of one point is that point's BLAKE3 hex, and of no points is 64
/// zeros. A sum, unlike XOR, does not cancel a point added twice.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MultisetDigest {
    /// The sum as four little-endian 64-bit limbs.
    limbs: [u64; 4],
}

impl MultisetDigest {
    /// An empty digest.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one point.
    pub fn add_point(&mut self, point: &LogicalPoint) {
        let mut one = OfferedDigest::new();
        one.update_point(point);
        let hash = one.hasher.finalize();
        let mut carry = false;
        for (limb, chunk) in self.limbs.iter_mut().zip(hash.as_bytes().chunks_exact(8)) {
            let mut word = [0u8; 8];
            word.copy_from_slice(chunk);
            let (sum, c1) = limb.overflowing_add(u64::from_le_bytes(word));
            let (sum, c2) = sum.overflowing_add(u64::from(carry));
            *limb = sum;
            carry = c1 || c2;
        }
    }

    /// The sum as 64 lowercase hex characters.
    pub fn finish(&self) -> String {
        self.limbs
            .iter()
            .flat_map(|limb| limb.to_le_bytes())
            .map(|b| format!("{b:02x}"))
            .collect()
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
        let batch: Vec<LogicalPoint> = rest[..n]
            .iter()
            .cloned()
            .map(LogicalPoint::Sample)
            .collect();
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
/// each pulled point to the offered digest and each valid one to its class's
/// expected digest. `on_valid` sees each valid point
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
                Ok(()) => {
                    offer.expected.update(&point);
                    pending.push(on_valid(point)?);
                }
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
    /// Per class, every valid point the lane sent.
    expected: ClassDigester,
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

/// Logical bytes of a native histogram before its buckets: timestamp, sum and
/// count, 8 bytes each.
const LOGICAL_BYTES_PER_HISTOGRAM_BASE: u64 = 24;
/// Logical bytes per native-histogram bucket count.
const LOGICAL_BYTES_PER_HISTOGRAM_BUCKET: u64 = 8;

/// The logical bytes of `points`, the denominator of `write_amplification`:
/// [`LOGICAL_BYTES_PER_SAMPLE`] per scalar sample (a staleness marker
/// included), and per native histogram [`LOGICAL_BYTES_PER_HISTOGRAM_BASE`]
/// plus [`LOGICAL_BYTES_PER_HISTOGRAM_BUCKET`] per bucket.
fn logical_bytes_of<'p>(points: impl IntoIterator<Item = &'p LogicalPoint>) -> u64 {
    points.into_iter().fold(0, |bytes, point| {
        bytes
            + match point {
                LogicalPoint::Sample(_) => LOGICAL_BYTES_PER_SAMPLE,
                LogicalPoint::Histogram(h) => {
                    LOGICAL_BYTES_PER_HISTOGRAM_BASE
                        + LOGICAL_BYTES_PER_HISTOGRAM_BUCKET * h.positive_deltas.len() as u64
                }
            }
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
    let mut generation = Duration::ZERO;

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
    let mut accepted_logical_bytes: u64 = 0;
    let mut accepted_digests = ClassDigester::default();
    let mut dropped_batches: u64 = 0;
    let mut batches: u64 = 0;

    loop {
        // Client-side admission: valid points (carrying their logical point
        // for the wire encoding) and exact rejections.
        let generation_start = std::time::Instant::now();
        fill_valid(source, batch_size, &mut pending, &mut offer, |logical| {
            let point = ingest_point(&config.tenant, &logical)?;
            let ts_ms = logical.ts_ms();
            max_ts_ms = Some(max_ts_ms.map_or(ts_ms, |m| m.max(ts_ms)));
            // The read-your-write probe is an instant query over a scalar
            // series; a native-histogram metric would answer a different shape.
            if matches!(logical, LogicalPoint::Sample(_)) {
                first_metric.get_or_insert_with(|| logical.metric().to_string());
            }
            Ok((point, logical))
        })?;
        generation += generation_start.elapsed();
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
        let batch_logical_bytes = logical_bytes_of(&logical);
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
                accepted_logical_bytes += batch_logical_bytes;
                tokens.extend(receipt.tokens);
                for point in &logical {
                    accepted_digests.update(point);
                }
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
    // Both ratios cover every record stored_bytes holds: scalar samples and
    // native histograms alike.
    let accepted_points = accepted_samples + accepted_histograms;
    let write_amplification = if accepted_logical_bytes == 0 {
        0.0
    } else {
        stored_bytes as f64 / accepted_logical_bytes as f64
    };
    let stored_bytes_per_point = if accepted_points == 0 {
        0.0
    } else {
        stored_bytes as f64 / accepted_points as f64
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
        retry_exhausted_by_status: BTreeMap::new(),
        histograms_retry_exhausted_by_status: BTreeMap::new(),
        unsupported_samples: 0,
        unsupported_histograms: 0,
        expected_digests: offer.expected.finish(),
        accepted_digests: accepted_digests.finish(),
        batches,
        retries: snapshot.put.attempts.saturating_sub(snapshot.put.calls),
        elapsed_secs,
        elapsed_basis: ELAPSED_BASIS_TIMED_SEGMENTS.to_string(),
        generation_secs: generation.as_secs_f64(),
        sender_bound: is_sender_bound(
            ELAPSED_BASIS_TIMED_SEGMENTS,
            generation.as_secs_f64(),
            elapsed_secs,
        ),
        logical_points_per_sec: (accepted_samples + accepted_histograms) as f64 / elapsed_secs,
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
        stored_bytes_per_point,
        write_amplification,
        backend_bills_requests: config.backend_bills_requests,
    };

    let encoded_tokens: Vec<String> = tokens.iter().map(CommitToken::encode).collect();
    let mut result =
        SystemIngestResult::ravel_row("in-process".to_string(), ingest, encoded_tokens, storage);
    result.tenant = Some(config.tenant.as_str().to_string());

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
        system: IN_PROCESS_SYSTEM.to_string(),
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
    /// The receiver: its row's `system`, its ack meaning, and how its
    /// responses are read.
    pub receiver: Receiver,
    /// The full `/api/v1/write` URL supplied by the caller.
    pub endpoint: String,
    /// Sent as `Authorization: Bearer <token>` when set. Never written into
    /// the row: neither `endpoint` nor `client_behavior` carries it
    /// (`tests::the_portable_ravel_row_sends_the_bearer_and_keeps_the_latest_token_of_every_shard`).
    pub bearer_token: Option<String>,
    /// The tenant the operator declares the row wrote under (`--ravel-tenant`
    /// for the portable Ravel row), recorded as given: no response carries a
    /// tenant to check it against. `None` for a comparator.
    pub tenant: Option<String>,
    /// Logical points (samples and native histograms) per RW1.0 request.
    pub batch_size: usize,
    /// Retries on a retryable response (429 or 5xx) before the batch is
    /// dropped.
    pub max_retries: u32,
    /// Per-request timeout, bounding the COMPLETE operation including the body
    /// read (ADR-0927 decision 6: a client that bounds only the request records
    /// an expiry as a generic transport failure).
    pub timeout_secs: u64,
    /// Fixed number of series shards, each with one request in flight
    /// (ADR-0927, 2026-10-28 amendment). Never resharded during a replay.
    pub shards: usize,
    /// Stops the row once its projected wall time exceeds a ceiling. `None`
    /// runs the replay to its end however long it takes.
    pub stall_ceiling: Option<StallCeiling>,
}

/// Default [`HttpReplayConfig::shards`]: Prometheus' `max_shards` (ADR-0927,
/// 2026-10-28 amendment, decision 1).
pub const DEFAULT_HTTP_SHARDS: usize = 50;

/// Default [`HttpReplayConfig::batch_size`]: Prometheus'
/// `max_samples_per_send` (ADR-0927, 2026-10-28 amendment, decision 1).
pub const DEFAULT_HTTP_BATCH_SIZE: usize = 2000;

/// Batches a shard's queue holds while its request is in flight. The
/// producer waits on a full queue, so each shard holds at most this many
/// queued batches, one in flight, and one filling.
const SHARD_QUEUE_BATCHES: usize = 1;

/// A wall-time ceiling for one HTTP row (ADR-0927, 2026-10-28 amendment,
/// decision 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StallCeiling {
    /// The most wall time the row may be projected to take.
    pub max_ingest_secs: u64,
    /// The logical points the whole stream offers, the projection's
    /// denominator.
    pub stream_points: u64,
    /// How long the row sends before its first stall check ([`STALL_GRACE`]
    /// in the bin).
    pub grace: Duration,
    /// The shortest gap between two stall checks, and the longest the drain
    /// waits for a response before checking again ([`STALL_RECHECK`] in the
    /// bin).
    pub recheck: Duration,
}

/// How long the bin lets a row send before its first stall check.
pub const STALL_GRACE: Duration = Duration::from_secs(60);

/// The shortest gap the bin leaves between two stall checks.
pub const STALL_RECHECK: Duration = Duration::from_secs(10);

/// The first retry's backoff (Prometheus' `min_backoff`).
const HTTP_MIN_BACKOFF: Duration = Duration::from_millis(30);

/// The longest backoff (Prometheus' `max_backoff`).
const HTTP_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// The backoff before retry `retry` (0-based): [`HTTP_MIN_BACKOFF`] doubled
/// per earlier retry, capped at [`HTTP_MAX_BACKOFF`], as Prometheus'
/// remote-write queue backs off (`tests::retry_delay_doubles_from_30ms_and_caps_at_5s`).
pub fn retry_delay(retry: u32) -> Duration {
    let min_ms = HTTP_MIN_BACKOFF.as_millis() as u64;
    let max_ms = HTTP_MAX_BACKOFF.as_millis() as u64;
    let ms = 1u64
        .checked_shl(retry)
        .and_then(|factor| min_ms.checked_mul(factor))
        .map_or(max_ms, |ms| ms.min(max_ms));
    Duration::from_millis(ms)
}

/// Whether a row was bound by its own sender rather than by the receiver:
/// true only for a [`ELAPSED_BASIS_WALL_IN_FLIGHT`] row whose generation
/// time is at least [`SENDER_BOUND_FRACTION`] of its wall time
/// (`tests::sender_bound_needs_wall_basis_and_ninety_percent_generation`).
pub fn is_sender_bound(basis: &str, generation_secs: f64, elapsed_secs: f64) -> bool {
    basis == ELAPSED_BASIS_WALL_IN_FLIGHT
        && elapsed_secs > 0.0
        && generation_secs >= SENDER_BOUND_FRACTION * elapsed_secs
}

/// The response header `ravel-server` puts the strict write's commit tokens
/// in (`services/ravel-server/src/remote_write.rs`).
pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";

/// What an HTTP replay sent that a read-back can check: every series of the
/// valid points by the classes it carried, one probe series per class, and
/// the replay window. Built from the points sent, not from the responses.
#[derive(Clone, Debug)]
pub struct ReadbackPlan {
    receiver: Receiver,
    series: HashMap<[u8; 32], SeriesTally>,
    /// Every metric name sent, so the cardinality read-back counts only
    /// series this replay could have written.
    metrics: BTreeSet<String>,
    histogram_metrics: BTreeSet<String>,
    probes: BTreeMap<PayloadClass, Probe>,
    /// The stale marker probe's timestamp and its series' newest float
    /// sample before it.
    stale_probe: Option<(i64, Option<i64>)>,
    min_ts_ms: Option<i64>,
    max_ts_ms: Option<i64>,
}

/// One series' points, by class, in a [`ReadbackPlan`].
#[derive(Clone, Copy, Debug, Default)]
struct SeriesTally {
    /// Bit per [`PayloadClass`] the series carried.
    classes: u8,
    scalar: u64,
    histograms: u64,
    last_scalar_ts_ms: Option<i64>,
}

/// A probe series: the first series seen of a class.
#[derive(Clone, Debug)]
struct Probe {
    key: [u8; 32],
    selector: String,
}

fn class_bit(class: PayloadClass) -> u8 {
    match class {
        PayloadClass::Scalar => 1,
        PayloadClass::StaleMarker => 2,
        PayloadClass::NativeHistogram => 4,
    }
}

/// The PromQL lookback delta every receiver here defaults to: an instant
/// query reads a series' newest sample at most this far back.
const LOOKBACK_MS: i64 = 300_000;

impl ReadbackPlan {
    fn new(receiver: Receiver) -> Self {
        ReadbackPlan {
            receiver,
            series: HashMap::new(),
            metrics: BTreeSet::new(),
            histogram_metrics: BTreeSet::new(),
            probes: BTreeMap::new(),
            stale_probe: None,
            min_ts_ms: None,
            max_ts_ms: None,
        }
    }

    /// Record one valid point the lane sent.
    fn observe(&mut self, point: &LogicalPoint) {
        let class = PayloadClass::of(point);
        let (metric, labels) = match point {
            LogicalPoint::Sample(s) => (&s.metric, &s.labels),
            LogicalPoint::Histogram(h) => (&h.metric, &h.labels),
        };
        let canonical = canonical_labels(metric, labels);
        let key = series_key(&canonical);
        let ts_ms = point.ts_ms();
        self.min_ts_ms = Some(self.min_ts_ms.map_or(ts_ms, |m| m.min(ts_ms)));
        self.max_ts_ms = Some(self.max_ts_ms.map_or(ts_ms, |m| m.max(ts_ms)));
        if !self.metrics.contains(metric.as_str()) {
            self.metrics.insert(metric.clone());
        }
        let tally = self.series.entry(key).or_default();
        let prior_scalar = tally.last_scalar_ts_ms;
        tally.classes |= class_bit(class);
        match class {
            PayloadClass::Scalar => {
                tally.scalar += 1;
                tally.last_scalar_ts_ms = Some(ts_ms);
            }
            PayloadClass::StaleMarker => {}
            PayloadClass::NativeHistogram => {
                tally.histograms += 1;
                self.histogram_metrics.insert(metric.clone());
            }
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = self.probes.entry(class) {
            entry.insert(Probe {
                key,
                selector: promql_selector(metric, labels),
            });
            if class == PayloadClass::StaleMarker {
                self.stale_probe = Some((ts_ms, prior_scalar));
            }
        }
    }

    fn supported_mask(&self) -> u8 {
        PayloadClass::ALL
            .into_iter()
            .filter(|c| self.receiver.supports(*c))
            .fold(0, |mask, c| mask | class_bit(c))
    }

    /// The float probe series (scalar and stale marker), deduplicated, with
    /// their float sample counts.
    fn float_probes(&self) -> Vec<(&Probe, u64)> {
        let mut seen: BTreeSet<[u8; 32]> = BTreeSet::new();
        [PayloadClass::Scalar, PayloadClass::StaleMarker]
            .into_iter()
            .filter(|c| self.receiver.supports(*c))
            .filter_map(|c| self.probes.get(&c))
            .filter(|p| seen.insert(p.key))
            .map(|p| (p, self.series.get(&p.key).map_or(0, |t| t.scalar)))
            .collect()
    }

    /// The figures a read-back of a receiver that stored exactly what was
    /// sent returns, for the classes it stores.
    pub fn expected(&self) -> ReadbackExpected {
        let mask = self.supported_mask();
        let hist_bit = class_bit(PayloadClass::NativeHistogram);
        let histograms = self.receiver.supports(PayloadClass::NativeHistogram)
            && self.probes.contains_key(&PayloadClass::NativeHistogram);
        let stale_marker_honored = match self.stale_probe {
            Some((marker_ts, Some(prior)))
                if self.receiver.supports(PayloadClass::StaleMarker)
                    && marker_ts > prior
                    && marker_ts - prior < LOOKBACK_MS =>
            {
                Some(true)
            }
            _ => None,
        };
        ReadbackExpected {
            series_count: self
                .series
                .values()
                .filter(|t| t.classes & mask != 0)
                .count() as u64,
            sample_count: self.float_probes().iter().map(|(_, n)| n).sum(),
            histogram_series_count: histograms.then(|| {
                self.series
                    .values()
                    .filter(|t| t.classes & hist_bit != 0)
                    .count() as u64
            }),
            histogram_sample_count: histograms.then(|| {
                self.probes
                    .get(&PayloadClass::NativeHistogram)
                    .and_then(|p| self.series.get(&p.key))
                    .map_or(0, |t| t.histograms)
            }),
            stale_marker_honored,
        }
    }
}

/// BLAKE3 over a canonical label set, the series key of a [`ReadbackPlan`].
fn series_key(canonical: &[(String, String)]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for (name, value) in canonical {
        for part in [name, value] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
    }
    *hasher.finalize().as_bytes()
}

/// A PromQL string literal body: backslash, double quote and newline
/// escaped.
fn promql_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// The exact-match selector of one series, its name as `__name__` so any
/// metric name parses.
fn promql_selector(metric: &str, labels: &[(String, String)]) -> String {
    let mut out = format!("{{__name__=\"{}\"", promql_escape(metric));
    let mut sorted: Vec<&(String, String)> = labels.iter().collect();
    sorted.sort();
    for (name, value) in sorted {
        out.push_str(&format!(",{name}=\"{}\"", promql_escape(value)));
    }
    out.push('}');
    out
}

/// A regex alternation matching exactly the given names, each with its
/// regex metacharacters escaped.
fn exact_alternation<'n>(names: impl IntoIterator<Item = &'n String>) -> String {
    names
        .into_iter()
        .map(|name| {
            let mut escaped = String::with_capacity(name.len());
            for c in name.chars() {
                if "\\.+*?()|[]{}^$".contains(c) {
                    escaped.push('\\');
                }
                escaped.push(c);
            }
            escaped
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Go's `time.RFC3339Nano` rendering of a millisecond UTC timestamp:
/// trailing zeros of the fraction trimmed, no fraction when it is zero, and
/// `Z` for UTC. The form Mimir prints a rejected sample's timestamp in.
fn go_rfc3339_nano_ms(ts_ms: i64) -> String {
    let days = ts_ms.div_euclid(86_400_000);
    let ms_of_day = ts_ms.rem_euclid(86_400_000);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let secs = ms_of_day / 1_000;
    let millis = ms_of_day % 1_000;
    let mut out = format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        secs / 3_600,
        secs % 3_600 / 60,
        secs % 60
    );
    if millis != 0 {
        let fraction = format!("{millis:03}");
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Go's `strconv.Quote` for the ASCII range: backslash, double quote and
/// control characters escaped.
fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// The text a Mimir 400 names one rejected point by: Mimir's `sampleError`
/// ends `The affected sample has timestamp <RFC3339Nano> and is from series
/// <series>`, the series as `mimirpb.FromLabelAdaptersToString` prints it,
/// the metric name then `{name="value", ...}` over the other labels in wire
/// order (sorted here), or the bare name when there are none. The bare form
/// is a prefix of the labelled one and of any longer metric name, so a body
/// names a point only where its needle is not followed by `{` or a metric
/// name character ([`mimir_body_names`]).
fn mimir_sample_needle(point: &LogicalPoint) -> String {
    let (metric, labels) = match point {
        LogicalPoint::Sample(s) => (&s.metric, &s.labels),
        LogicalPoint::Histogram(h) => (&h.metric, &h.labels),
    };
    let mut sorted: Vec<&(String, String)> = labels.iter().collect();
    sorted.sort();
    let mut series = metric.clone();
    if !sorted.is_empty() {
        let pairs: Vec<String> = sorted
            .iter()
            .map(|(name, value)| format!("{name}={}", go_quote(value)))
            .collect();
        series.push('{');
        series.push_str(&pairs.join(", "));
        series.push('}');
    }
    format!(
        "timestamp {} and is from series {series}",
        go_rfc3339_nano_ms(point.ts_ms())
    )
}

/// Whether a Mimir 400 body names the point `needle` renders: some
/// occurrence of the needle ends the body or is followed by a character that
/// cannot continue the series rendering, so `series m` does not match inside
/// `series m{instance="a"}` or `series mx`
/// (`tests::a_bare_mimir_needle_is_not_named_by_its_labelled_series`,
/// `tests::a_mimir_needle_is_not_named_by_a_longer_metric_name`,
/// `tests::a_mimir_needle_is_named_when_its_sample_is`).
fn mimir_body_names(body: &str, needle: &str) -> bool {
    body.match_indices(needle).any(|(at, _)| {
        body[at + needle.len()..]
            .chars()
            .next()
            .is_none_or(|c| c != '{' && !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
    })
}

/// An HTTP replay's row and the plan its read-back checks.
#[derive(Debug)]
pub struct HttpReplayOutcome {
    /// The report row.
    pub result: SystemIngestResult,
    /// What was sent, for [`readback_over_http`].
    pub plan: ReadbackPlan,
}

/// [`replay_over_http_with_plan`], keeping only the row.
pub async fn replay_over_http<S: SampleBatches + ?Sized>(
    config: &HttpReplayConfig,
    source: &mut S,
) -> Result<SystemIngestResult, LaneError> {
    replay_over_http_with_plan(config, source)
        .await
        .map(|outcome| outcome.result)
}

/// The shard a point's series is sent on: BLAKE3 over the point's canonical
/// series (sorted label pairs, `__name__` included), encoded as
/// [`OfferedDigest`] encodes a series, its first eight bytes read as a
/// little-endian integer, modulo `shards`. A fixed function of the series,
/// so every point of one series goes to one shard on every run.
fn series_shard(point: &LogicalPoint, shards: usize) -> usize {
    let (metric, labels) = match point {
        LogicalPoint::Sample(s) => (&s.metric, &s.labels),
        LogicalPoint::Histogram(h) => (&h.metric, &h.labels),
    };
    let mut digest = OfferedDigest::new();
    digest.update_series(&canonical_labels(metric, labels));
    let hash = digest.hasher.finalize();
    let mut word = [0u8; 8];
    word.copy_from_slice(&hash.as_bytes()[..8]);
    (u64::from_le_bytes(word) % shards.max(1) as u64) as usize
}

/// What one shard sends with: the client and everything a request needs.
struct ShardSender {
    client: reqwest::Client,
    endpoint: String,
    bearer_token: Option<String>,
    receiver: Receiver,
    max_retries: u32,
}

/// One request a shard finished: its points, how it ended, and when.
struct SentBatch {
    chunk: Vec<LogicalPoint>,
    outcome: BatchOutcome,
    retries: u64,
    wire_bytes: u64,
    /// The first attempt's send.
    sent_at: std::time::Instant,
    /// The last attempt's response, or its transport error.
    answered_at: std::time::Instant,
}

impl ShardSender {
    /// Encode and POST one batch, retrying a 429, a 5xx or a transport error
    /// after [`retry_delay`] up to `max_retries` times.
    async fn send(&self, chunk: Vec<LogicalPoint>) -> Result<SentBatch, LaneError> {
        let body = encode_rw1_points(&chunk)?;
        let wire_bytes = body.len() as u64;
        let sent_at = std::time::Instant::now();
        let mut attempt = 0u32;
        let outcome = loop {
            let mut request = self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .header("X-Prometheus-Remote-Write-Version", "0.1.0");
            if let Some(token) = &self.bearer_token {
                request = request.bearer_auth(token);
            }
            let sent = request.body(body.clone()).send().await;
            match sent {
                Ok(resp) if resp.status().is_success() => {
                    break BatchOutcome::Acknowledged(
                        resp.headers()
                            .get(COMMIT_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string),
                    );
                }
                Ok(resp) if is_retryable(resp.status()) && attempt < self.max_retries => {
                    tokio::time::sleep(retry_delay(attempt)).await;
                    attempt += 1;
                }
                Ok(resp) if is_retryable(resp.status()) => {
                    break BatchOutcome::RetriesExhausted(Some(resp.status().as_u16()));
                }
                Ok(resp)
                    if self.receiver.soft_rejects_samples()
                        && resp.status() == reqwest::StatusCode::BAD_REQUEST =>
                {
                    break BatchOutcome::SoftRejected(resp.text().await.unwrap_or_default());
                }
                Ok(resp) => break BatchOutcome::Rejected(resp.status().as_u16()),
                Err(_) if attempt < self.max_retries => {
                    tokio::time::sleep(retry_delay(attempt)).await;
                    attempt += 1;
                }
                Err(_) => break BatchOutcome::RetriesExhausted(None),
            }
        };
        Ok(SentBatch {
            chunk,
            outcome,
            retries: u64::from(attempt),
            wire_bytes,
            sent_at,
            answered_at: std::time::Instant::now(),
        })
    }
}

/// One shard's sender: it takes the shard's batches in the order the producer
/// queued them and sends each only once the previous one has answered, so a
/// series reaches the receiver in offered order. It stops after the first
/// error it reports.
async fn run_shard(
    sender: Arc<ShardSender>,
    mut batches: tokio::sync::mpsc::Receiver<Vec<LogicalPoint>>,
    results: tokio::sync::mpsc::UnboundedSender<Result<SentBatch, LaneError>>,
) {
    while let Some(chunk) = batches.recv().await {
        let sent = sender.send(chunk).await;
        let failed = sent.is_err();
        if results.send(sent).is_err() || failed {
            return;
        }
    }
}

/// The one place an HTTP replay's per-request outcomes are accounted.
struct HttpCollector {
    receiver: Receiver,
    system: String,
    tally: HttpTally,
    ack_latencies_ns: Vec<u64>,
    dropped_batches: u64,
    batches: u64,
    retries: u64,
    wire_bytes: u64,
    tokens_by_shard: BTreeMap<u32, String>,
    first_sent: Option<std::time::Instant>,
    last_answered: Option<std::time::Instant>,
}

impl HttpCollector {
    fn new(receiver: Receiver) -> Self {
        HttpCollector {
            receiver,
            system: receiver.system().to_string(),
            tally: HttpTally::default(),
            ack_latencies_ns: Vec::new(),
            dropped_batches: 0,
            batches: 0,
            retries: 0,
            wire_bytes: 0,
            tokens_by_shard: BTreeMap::new(),
            first_sent: None,
            last_answered: None,
        }
    }

    /// Account one finished request.
    fn record(&mut self, sent: SentBatch) -> Result<(), LaneError> {
        let SentBatch {
            chunk,
            outcome,
            retries,
            wire_bytes,
            sent_at,
            answered_at,
        } = sent;
        self.batches += 1;
        self.retries += retries;
        self.wire_bytes += wire_bytes;
        self.first_sent = Some(self.first_sent.map_or(sent_at, |t| t.min(sent_at)));
        self.last_answered = Some(
            self.last_answered
                .map_or(answered_at, |t| t.max(answered_at)),
        );
        let receiver = self.receiver;
        let tally = &mut self.tally;
        let dropped_before = tally.dropped();
        match outcome {
            BatchOutcome::Acknowledged(token) => {
                self.ack_latencies_ns
                    .push(answered_at.duration_since(sent_at).as_nanos() as u64);
                for point in &chunk {
                    tally.accept(receiver, point);
                }
                if let Some(header) = token {
                    record_commit_tokens(&self.system, &header, &mut self.tokens_by_shard)?;
                }
            }
            BatchOutcome::SoftRejected(body) => {
                let needles: Vec<String> = chunk.iter().map(mimir_sample_needle).collect();
                let named: Vec<bool> = needles.iter().map(|n| mimir_body_names(&body, n)).collect();
                let none_named = !named.iter().any(|n| *n);
                for (point, is_named) in chunk.iter().zip(named) {
                    if none_named || is_named {
                        tally.reject(point, 400);
                    } else {
                        tally.accept(receiver, point);
                    }
                }
            }
            BatchOutcome::Rejected(code) => {
                for point in &chunk {
                    tally.reject(point, code);
                }
            }
            BatchOutcome::RetriesExhausted(code) => {
                for point in &chunk {
                    tally.exhaust(point, code);
                }
            }
        }
        if tally.dropped() > dropped_before {
            self.dropped_batches += 1;
        }
        Ok(())
    }

    /// Wall time from the first request sent to the last response received,
    /// floored at a nanosecond so a rate never divides by zero.
    fn elapsed_secs(&self) -> f64 {
        match (self.first_sent, self.last_answered) {
            (Some(first), Some(last)) => last.duration_since(first).as_secs_f64().max(1e-9),
            _ => 1e-9,
        }
    }
}

/// When a row's stall check next runs: once it has been sending for
/// `ceiling.grace`, then at most every `ceiling.recheck`.
fn stall_check_due(
    ceiling: &StallCeiling,
    sending_for: Duration,
    since_last_check: Option<Duration>,
) -> bool {
    sending_for >= ceiling.grace && since_last_check.is_none_or(|d| d >= ceiling.recheck)
}

/// The stall check both the producer and the drain run: when one is due
/// ([`stall_check_due`]) and the projection over `offered_points` exceeds
/// the ceiling ([`stall_projection_over`]), [`LaneError::IngestStalled`].
/// Nothing is checked before the first request is sent.
fn check_stall(
    system: &str,
    ceiling: Option<&StallCeiling>,
    sending_since: Option<std::time::Instant>,
    last_check: &mut Option<std::time::Instant>,
    offered_points: u64,
) -> Result<(), LaneError> {
    let (Some(ceiling), Some(since)) = (ceiling, sending_since) else {
        return Ok(());
    };
    let now = std::time::Instant::now();
    let sending_for = now.duration_since(since);
    if !stall_check_due(
        ceiling,
        sending_for,
        last_check.map(|t| now.duration_since(t)),
    ) {
        return Ok(());
    }
    *last_check = Some(now);
    match stall_projection_over(ceiling, sending_for.as_secs_f64(), offered_points) {
        Some(projected_secs) => Err(LaneError::IngestStalled {
            system: system.to_string(),
            projected_secs,
            ceiling_secs: ceiling.max_ingest_secs,
            offered_points,
            stream_points: ceiling.stream_points,
        }),
        None => Ok(()),
    }
}

/// A full batch and the shard it goes to.
type ShardBatch = (usize, Vec<LogicalPoint>);

/// The producer's state across rounds: the points pulled but not yet routed,
/// each shard's filling batch, the offer tally, the read-back plan, and the
/// time spent generating and routing.
struct Producer {
    batch_size: usize,
    pending: Vec<LogicalPoint>,
    buffers: Vec<Vec<LogicalPoint>>,
    offer: OfferTally,
    plan: ReadbackPlan,
    generation: Duration,
}

impl Producer {
    fn new(receiver: Receiver, shards: usize, batch_size: usize) -> Self {
        Self {
            batch_size,
            pending: Vec::new(),
            buffers: vec![Vec::new(); shards],
            offer: OfferTally::default(),
            plan: ReadbackPlan::new(receiver),
            generation: Duration::ZERO,
        }
    }

    /// One round: pull up to `batch_size` valid points, observe each into the
    /// plan and route it to shard `shard_of(point)`, and return every shard
    /// batch that filled, in the order it filled; `None` once the stream has
    /// ended. The whole round, pulling and routing, is added to `generation`
    /// as read on `now`; sending the returned batches is not. A round routes
    /// at most `batch_size` points, so it fills at most one batch per shard.
    fn round<S, R, N>(
        &mut self,
        source: &mut S,
        mut shard_of: R,
        now: N,
    ) -> Result<Option<Vec<ShardBatch>>, LaneError>
    where
        S: SampleBatches + ?Sized,
        R: FnMut(&LogicalPoint) -> usize,
        N: Fn() -> Duration,
    {
        let start = now();
        fill_valid(
            source,
            self.batch_size,
            &mut self.pending,
            &mut self.offer,
            Ok,
        )?;
        if self.pending.is_empty() {
            self.generation += now().saturating_sub(start);
            return Ok(None);
        }
        let mut full = Vec::new();
        for point in self.pending.drain(..) {
            self.plan.observe(&point);
            let shard = shard_of(&point);
            self.buffers[shard].push(point);
            if self.buffers[shard].len() >= self.batch_size {
                full.push((shard, std::mem::take(&mut self.buffers[shard])));
            }
        }
        self.generation += now().saturating_sub(start);
        Ok(Some(full))
    }
}

/// The projected total ingest wall time of a row that has sent for
/// `elapsed_secs` and offered `offered_points` of the `stream_points` its
/// stream will offer: `elapsed_secs * stream_points / offered_points`. `None`
/// before any point is offered, when nothing can be projected.
pub fn project_ingest_secs(
    elapsed_secs: f64,
    offered_points: u64,
    stream_points: u64,
) -> Option<f64> {
    (offered_points > 0).then(|| elapsed_secs * stream_points as f64 / offered_points as f64)
}

/// The stall verdict for one check: `Some(projection)` when the projected
/// total wall time exceeds `ceiling.max_ingest_secs`, else `None`.
pub fn stall_projection_over(
    ceiling: &StallCeiling,
    elapsed_secs: f64,
    offered_points: u64,
) -> Option<f64> {
    project_ingest_secs(elapsed_secs, offered_points, ceiling.stream_points)
        .filter(|projected| *projected > ceiling.max_ingest_secs as f64)
}

/// Replay `source` into a receiver over Remote Write 1.0 the way a Prometheus
/// sender does (ADR-0927, 2026-10-28 amendment). One producer pulls the
/// stream batch by batch as [`replay_into_ravel`] does, counts every point
/// into the offer tally and the read-back plan in offered order, and routes
/// each valid point to shard [`series_shard`] of `shards`. A shard sends a
/// request when it holds `batch_size` points or the stream has ended, and
/// keeps one request in flight, so at most `shards` requests are in flight
/// and every series arrives in offered order. Each shard's queue holds
/// [`SHARD_QUEUE_BATCHES`] batch, and the producer waits on a full queue, so
/// memory stays bounded by a few batches per shard. Every request is encoded
/// as snappy protobuf and POSTed to the configured endpoint (with
/// `Authorization: Bearer` when `bearer_token` is set).
///
/// Each batch's points are accounted by the receiver's response:
/// - a 2xx accepts every point, except a point of a class the receiver
///   cannot store ([`UNSUPPORTED_CLASSES`]), which is counted under
///   `unsupported_*`;
/// - a Mimir 400 rejects only the points its body names
///   ([`mimir_sample_needle`]) and accepts the rest; a Mimir 400 naming none
///   of the batch's points rejects them all;
/// - any other non-retryable non-2xx rejects the whole batch under
///   `rejected_by_status`, keyed by that status;
/// - a 429/5xx or a transport error is retried after [`retry_delay`] up to
///   `max_retries`, then the batch is dropped under `retry_exhausted_*`,
///   keyed by its last status in `retry_exhausted_by_status`.
///
/// A comparator row is [`AckSemantics::Buffered`]: a Remote Write 2xx from a
/// comparator means accepted-into-a-buffer, a durability the client cannot
/// observe, so it is never placed in a pooled column with Ravel's durable
/// ack. The portable Ravel row keeps the newest `x-ravel-commit-token` entry
/// per shard ([`record_commit_tokens`]); an entry that does not decode is
/// [`LaneError::InvalidCommitToken`].
///
/// With a [`StallCeiling`], the row stops with [`LaneError::IngestStalled`]
/// once its projected wall time exceeds the ceiling ([`check_stall`]). The
/// check runs while the producer offers the stream and again while the last
/// responses drain, waking at least every `recheck` while none arrives, so a
/// row whose whole stream fits in the shard buffers is still stopped
/// (`tests::the_drain_stops_a_row_whose_whole_stream_is_already_offered`).
///
/// The receiver's object store is not the bench's, so this row carries no
/// [`StorageAccounting`], and its CPU/RSS are the remote process's, so they are
/// `None`: absent, never a zero.
pub async fn replay_over_http_with_plan<S: SampleBatches + ?Sized>(
    config: &HttpReplayConfig,
    source: &mut S,
) -> Result<HttpReplayOutcome, LaneError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs.max(1)))
        .build()
        .map_err(|e| transport_setup_error(format!("building the HTTP client failed: {e}")))?;

    let receiver = config.receiver;
    let system = receiver.system().to_string();
    let shards = config.shards.max(1);
    let batch_size = config.batch_size.max(1);
    let sender = Arc::new(ShardSender {
        client,
        endpoint: config.endpoint.clone(),
        bearer_token: config.bearer_token.clone(),
        receiver,
        max_retries: config.max_retries,
    });
    let (results_tx, mut results_rx) = tokio::sync::mpsc::unbounded_channel();
    // Dropping the set aborts every sender, so an early return stops them.
    let mut senders = tokio::task::JoinSet::new();
    let mut queues = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (tx, rx) = tokio::sync::mpsc::channel(SHARD_QUEUE_BATCHES);
        senders.spawn(run_shard(Arc::clone(&sender), rx, results_tx.clone()));
        queues.push(tx);
    }
    drop(results_tx);

    let mut producer = Producer::new(receiver, shards, batch_size);
    let mut collector = HttpCollector::new(receiver);
    let ceiling = config.stall_ceiling.as_ref();
    let clock_origin = std::time::Instant::now();
    let now = || clock_origin.elapsed();
    let mut sending_since: Option<std::time::Instant> = None;
    let mut last_stall_check: Option<std::time::Instant> = None;

    let produced: Result<(), LaneError> = async {
        while let Some(full) = producer.round(source, |point| series_shard(point, shards), now)? {
            for (shard, chunk) in full {
                sending_since.get_or_insert_with(std::time::Instant::now);
                queue_batch(&system, &queues[shard], shard, chunk).await?;
            }
            while let Ok(sent) = results_rx.try_recv() {
                collector.record(sent?)?;
            }
            let offered_points = producer.offer.samples + producer.offer.histograms;
            check_stall(
                &system,
                ceiling,
                sending_since,
                &mut last_stall_check,
                offered_points,
            )?;
        }
        for (shard, buffer) in producer.buffers.iter_mut().enumerate() {
            if !buffer.is_empty() {
                let chunk = std::mem::take(buffer);
                sending_since.get_or_insert_with(std::time::Instant::now);
                queue_batch(&system, &queues[shard], shard, chunk).await?;
            }
        }
        Ok(())
    }
    .await;
    // Closing the queues lets every sender finish its last batch and exit.
    drop(queues);
    if let Err(err) = produced {
        // A shard that stopped on its own error closed its queue, and that
        // error, already sent, is the cause.
        while let Ok(sent) = results_rx.try_recv() {
            sent?;
        }
        return Err(err);
    }
    // The whole stream is offered; the drain still runs the stall check, on
    // every response and at least every `recheck` while none arrives.
    let offered_points = producer.offer.samples + producer.offer.histograms;
    loop {
        let next = match ceiling {
            Some(ceiling) => {
                let wait = ceiling.recheck.max(Duration::from_millis(1));
                tokio::time::timeout(wait, results_rx.recv()).await.ok()
            }
            None => Some(results_rx.recv().await),
        };
        match next {
            Some(Some(sent)) => collector.record(sent?)?,
            Some(None) => break,
            None => {}
        }
        check_stall(
            &system,
            ceiling,
            sending_since,
            &mut last_stall_check,
            offered_points,
        )?;
    }
    while senders.join_next().await.is_some() {}

    let Producer {
        offer,
        plan,
        generation,
        ..
    } = producer;
    let elapsed_secs = collector.elapsed_secs();
    let generation_secs = generation.as_secs_f64();
    let stream_counts = source.stream_counts();
    let HttpCollector {
        tally,
        ack_latencies_ns,
        dropped_batches,
        batches,
        retries,
        wire_bytes,
        tokens_by_shard,
        ..
    } = collector;
    let HttpTally {
        accepted_samples,
        accepted_histograms,
        unsupported_samples,
        unsupported_histograms,
        dropped_samples,
        dropped_histograms,
        rejected_by_status,
        histograms_rejected_by_status,
        retry_exhausted_samples,
        retry_exhausted_histograms,
        retry_exhausted_by_status,
        histograms_retry_exhausted_by_status,
        accepted,
    } = tally;
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
        histograms_retry_exhausted_by_status,
        dropped_samples,
        dropped_batches,
        rejected_by_status,
        retry_exhausted_samples,
        retry_exhausted_by_status,
        unsupported_samples,
        unsupported_histograms,
        expected_digests: offer.expected.finish(),
        accepted_digests: accepted.finish(),
        batches,
        retries,
        elapsed_secs,
        elapsed_basis: ELAPSED_BASIS_WALL_IN_FLIGHT.to_string(),
        generation_secs,
        sender_bound: is_sender_bound(ELAPSED_BASIS_WALL_IN_FLIGHT, generation_secs, elapsed_secs),
        logical_points_per_sec: (accepted_samples + accepted_histograms) as f64 / elapsed_secs,
        wire_bytes,
        wire_bytes_per_sec: wire_bytes as f64 / elapsed_secs,
        ack_latency_ms: LatencyReport::from_nanos(ack_latencies_ns),
        // The receiver runs in another process: its CPU and RSS are not the
        // bench's to report. Absent, never a zero.
        peak_rss_bytes: None,
        cpu_secs: None,
    };
    let client_behavior = format!(
        "reqwest RW1.0 sender modelled on Prometheus' remote-write queue: snappy protobuf, \
         {shards} shards fixed for the whole replay (no resharding between min_shards and \
         max_shards), each series routed to one shard by a BLAKE3 hash of its sorted labels, \
         one request in flight per shard, a request sent at {batch} logical points (samples and \
         native histograms) or at stream end, {auth}; 429, 5xx and transport errors retried up \
         to {retries} times with backoff from {min_ms} ms doubling to {max_ms} ms, then the batch \
         is dropped (429 is retried although Prometheus' default retry_on_http_429 is false, \
         since a dropped point fails the zero drop budget); {timeout} s per-request timeout \
         bounds the complete operation including the body read; accepted_* counts points by the \
         receiver's acceptance rule, since an RW1.0 response carries no written count",
        batch = batch_size,
        auth = if config.bearer_token.is_some() {
            "bearer auth"
        } else {
            "no auth header"
        },
        retries = config.max_retries,
        min_ms = HTTP_MIN_BACKOFF.as_millis(),
        max_ms = HTTP_MAX_BACKOFF.as_millis(),
        timeout = config.timeout_secs.max(1),
    );
    let acknowledged = accepted_samples + accepted_histograms;
    if receiver.ack_semantics() == AckSemantics::DurableOnAck
        && acknowledged > 0
        && tokens_by_shard.is_empty()
    {
        return Err(LaneError::UnbackedDurability {
            system,
            endpoint: config.endpoint.clone(),
            acknowledged,
        });
    }
    let mut result = match receiver.ack_semantics() {
        AckSemantics::DurableOnAck => SystemIngestResult::durable_http_row(
            system,
            config.endpoint.clone(),
            client_behavior,
            ingest,
            tokens_by_shard,
        ),
        AckSemantics::Buffered => SystemIngestResult::buffered_http_row(
            system,
            config.endpoint.clone(),
            client_behavior,
            ingest,
        ),
    };
    result.acceptance_basis = Some(receiver.acceptance_basis());
    result.unsupported_classes = receiver.unsupported_classes();
    result.tenant = config.tenant.clone();
    Ok(HttpReplayOutcome { result, plan })
}

/// Hand a full (or final) batch to its shard's sender, waiting while the
/// shard's queue is full.
async fn queue_batch(
    system: &str,
    queue: &tokio::sync::mpsc::Sender<Vec<LogicalPoint>>,
    shard: usize,
    chunk: Vec<LogicalPoint>,
) -> Result<(), LaneError> {
    queue.send(chunk).await.map_err(|_| {
        LaneError::Transport(format!("{system}'s sender for shard {shard} stopped early"))
    })
}

/// How a comparator batch ended.
enum BatchOutcome {
    /// A 2xx, with its `x-ravel-commit-token` header if it carried one.
    Acknowledged(Option<String>),
    /// A Mimir 400, with its body naming the rejected points.
    SoftRejected(String),
    /// A non-retryable non-2xx response, with its status.
    Rejected(u16),
    /// Still failing after `max_retries` retries: the last retryable status,
    /// or `None` for a transport error.
    RetriesExhausted(Option<u16>),
}

/// Per-point accounting of an HTTP replay, both classes apart.
#[derive(Default)]
struct HttpTally {
    accepted_samples: u64,
    accepted_histograms: u64,
    unsupported_samples: u64,
    unsupported_histograms: u64,
    dropped_samples: u64,
    dropped_histograms: u64,
    rejected_by_status: BTreeMap<u16, u64>,
    histograms_rejected_by_status: BTreeMap<u16, u64>,
    retry_exhausted_samples: u64,
    retry_exhausted_histograms: u64,
    retry_exhausted_by_status: BTreeMap<u16, u64>,
    histograms_retry_exhausted_by_status: BTreeMap<u16, u64>,
    accepted: ClassDigester,
}

impl HttpTally {
    fn dropped(&self) -> u64 {
        self.dropped_samples + self.dropped_histograms
    }

    /// A point the receiver acknowledged: accepted, or unsupported when the
    /// receiver cannot store its class.
    fn accept(&mut self, receiver: Receiver, point: &LogicalPoint) {
        let histogram = matches!(point, LogicalPoint::Histogram(_));
        if receiver.supports(PayloadClass::of(point)) {
            self.accepted.update(point);
            if histogram {
                self.accepted_histograms += 1;
            } else {
                self.accepted_samples += 1;
            }
        } else if histogram {
            self.unsupported_histograms += 1;
        } else {
            self.unsupported_samples += 1;
        }
    }

    fn reject(&mut self, point: &LogicalPoint, code: u16) {
        if matches!(point, LogicalPoint::Histogram(_)) {
            self.dropped_histograms += 1;
            *self.histograms_rejected_by_status.entry(code).or_insert(0) += 1;
        } else {
            self.dropped_samples += 1;
            *self.rejected_by_status.entry(code).or_insert(0) += 1;
        }
    }

    fn exhaust(&mut self, point: &LogicalPoint, code: Option<u16>) {
        if matches!(point, LogicalPoint::Histogram(_)) {
            self.dropped_histograms += 1;
            self.retry_exhausted_histograms += 1;
            if let Some(code) = code {
                *self
                    .histograms_retry_exhausted_by_status
                    .entry(code)
                    .or_insert(0) += 1;
            }
        } else {
            self.dropped_samples += 1;
            self.retry_exhausted_samples += 1;
            if let Some(code) = code {
                *self.retry_exhausted_by_status.entry(code).or_insert(0) += 1;
            }
        }
    }
}

/// Fold one `x-ravel-commit-token` header (comma-joined encoded tokens, one
/// per shard the request wrote) into `by_shard`, keeping per shard the token
/// with the highest `(epoch, seq)`: with requests in flight on several
/// series shards, responses arrive in any order, and an older token arriving
/// last must not replace a newer one
/// (`tests::commit_tokens_keep_the_newest_epoch_and_seq_per_shard`). A token
/// from a different writer than the one already held for its shard is
/// [`LaneError::CommitTokenWriterConflict`]
/// (`tests::a_second_writer_on_one_shard_is_refused`).
fn record_commit_tokens(
    system: &str,
    header: &str,
    by_shard: &mut BTreeMap<u32, String>,
) -> Result<(), LaneError> {
    for entry in header.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let token = CommitToken::decode(entry).map_err(|_| LaneError::InvalidCommitToken {
            system: system.to_string(),
            value: entry.to_string(),
        })?;
        if let Some(held_entry) = by_shard.get(&token.shard) {
            let held =
                CommitToken::decode(held_entry).map_err(|_| LaneError::InvalidCommitToken {
                    system: system.to_string(),
                    value: held_entry.clone(),
                })?;
            if held.writer_id != token.writer_id {
                return Err(LaneError::CommitTokenWriterConflict {
                    system: system.to_string(),
                    shard: token.shard,
                    held_writer: held.writer_id.to_string(),
                    new_writer: token.writer_id.to_string(),
                });
            }
            if (token.epoch, token.seq) <= (held.epoch, held.seq) {
                continue;
            }
        }
        by_shard.insert(token.shard, entry.to_string());
    }
    Ok(())
}

/// Where and how to read an HTTP row back.
#[derive(Clone, Debug)]
pub struct ReadbackConfig {
    /// Base URL of the receiver's Prometheus HTTP query API (the path
    /// prefix that `/api/v1/query` is appended to).
    pub query_url: String,
    /// Sent as `Authorization: Bearer` when set.
    pub bearer_token: Option<String>,
    /// Per-request timeout in seconds for each read-back query, the body
    /// read included (`metricsbench_ingest --readback-timeout-secs`).
    pub timeout_secs: u64,
    /// How long, in seconds, to keep re-reading a row whose figures do not
    /// yet match what was sent before reporting the mismatch. Zero reads
    /// once. The same window applies to every system.
    pub settle_secs: u64,
}

/// The [`Readback::query_ms`] key of the `/api/v1/series` cardinality read.
pub const READBACK_QUERY_SERIES: &str = "series";
/// The [`Readback::query_ms`] key prefix of each float probe's
/// `count_over_time`, suffixed `.0`, `.1`, ... in probe order.
pub const READBACK_QUERY_SAMPLES: &str = "samples";
/// The [`Readback::query_ms`] key of the native-histogram series read.
pub const READBACK_QUERY_HISTOGRAM_SERIES: &str = "histogram_series";
/// The [`Readback::query_ms`] key of the native-histogram probe's
/// `count_over_time`.
pub const READBACK_QUERY_HISTOGRAM_SAMPLES: &str = "histogram_samples";
/// The [`Readback::query_ms`] key of the stale marker probe's instant query.
pub const READBACK_QUERY_STALE_MARKER: &str = "stale_marker";

/// Pause between two read-backs of a row that has not settled.
const READBACK_SETTLE_POLL: Duration = Duration::from_millis(500);

/// Read `row` back from its receiver's query API and compare with what
/// `plan` says was sent: `/api/v1/series` cardinality over the replay window,
/// `count_over_time` over each float probe series, the native-histogram
/// series count and the histogram probe's sample count, and that the stale
/// marker probe ended its series. A Ravel row passes every token in
/// `commit_tokens_by_shard` as `min_commit_token`, so the read observes
/// every shard written. A receiver that acknowledges a write before it
/// makes the write queryable reads short at first, so a read that does not
/// match is repeated every 500 ms until one matches or `settle_secs` has
/// passed since the first; `attempts` and `settled_after_ms` record which
/// read is reported. Returns the figures with `matches_expected` set; a
/// mismatch is data, not an error.
pub async fn readback_over_http(
    config: &ReadbackConfig,
    row: &SystemIngestResult,
    plan: &ReadbackPlan,
) -> Result<Readback, LaneError> {
    // A plan with no window reads nothing, so a repeat cannot change it.
    let settle = match (plan.min_ts_ms, plan.max_ts_ms) {
        (Some(_), Some(_)) => Duration::from_secs(config.settle_secs),
        _ => Duration::ZERO,
    };
    let started = std::time::Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts = attempts.saturating_add(1);
        let mut readback = read_back_once(config, row, plan).await?;
        let elapsed = started.elapsed();
        if readback.matches_expected || elapsed >= settle {
            readback.attempts = attempts;
            readback.settled_after_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
            return Ok(readback);
        }
        tokio::time::sleep(READBACK_SETTLE_POLL.min(settle - elapsed)).await;
    }
}

/// One read-back of `row`, as [`readback_over_http`] describes it, with
/// `attempts` 1 and `settled_after_ms` 0.
async fn read_back_once(
    config: &ReadbackConfig,
    row: &SystemIngestResult,
    plan: &ReadbackPlan,
) -> Result<Readback, LaneError> {
    let expected = plan.expected();
    let system = row.system.clone();
    let fail = |detail: String| LaneError::Readback {
        system: system.clone(),
        detail,
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs.max(1)))
        .build()
        .map_err(|e| transport_setup_error(format!("building the HTTP client failed: {e}")))?;
    let tokens: Vec<String> = row
        .commit_tokens_by_shard
        .as_ref()
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    let reader = ApiReader {
        client: &client,
        base: config.query_url.trim_end_matches('/'),
        bearer: config.bearer_token.as_deref(),
        tokens: &tokens,
    };
    let (Some(min_ts), Some(max_ts)) = (plan.min_ts_ms, plan.max_ts_ms) else {
        return Ok(empty_window_readback(config.query_url.clone(), expected));
    };
    let window = [("start", seconds(min_ts)), ("end", seconds(max_ts))];
    let range_ms = max_ts - min_ts + 1;

    let mut query_ms = BTreeMap::new();
    // Each query's wall time goes into `query_ms` under `name`, and a failed
    // query's detail says how long it ran before it failed.
    let mut timed = |name: String, started: std::time::Instant, result: Result<u64, String>| {
        let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        query_ms.insert(name.clone(), ms);
        result.map_err(|detail| fail(format!("query {name} failed after {ms} ms: {detail}")))
    };

    let started = std::time::Instant::now();
    let result = reader
        .series_count(
            &format!(
                "{{__name__=~\"{}\"}}",
                promql_escape(&exact_alternation(&plan.metrics))
            ),
            &window,
        )
        .await;
    let series_count = timed(READBACK_QUERY_SERIES.to_string(), started, result)?;
    let mut sample_count = 0u64;
    for (index, (probe, _)) in plan.float_probes().into_iter().enumerate() {
        let started = std::time::Instant::now();
        let result = reader
            .instant_count(
                &format!("count_over_time({}[{range_ms}ms])", probe.selector),
                max_ts,
            )
            .await;
        sample_count += timed(format!("{READBACK_QUERY_SAMPLES}.{index}"), started, result)?;
    }
    let histogram_series_count = match expected.histogram_series_count {
        Some(_) => {
            let started = std::time::Instant::now();
            let result = reader
                .series_count(
                    &format!(
                        "{{__name__=~\"{}\"}}",
                        promql_escape(&exact_alternation(&plan.histogram_metrics))
                    ),
                    &window,
                )
                .await;
            Some(timed(
                READBACK_QUERY_HISTOGRAM_SERIES.to_string(),
                started,
                result,
            )?)
        }
        None => None,
    };
    let histogram_sample_count = match (
        expected.histogram_sample_count,
        plan.probes.get(&PayloadClass::NativeHistogram),
    ) {
        (Some(_), Some(probe)) => {
            let started = std::time::Instant::now();
            let result = reader
                .instant_count(
                    &format!("count_over_time({}[{range_ms}ms])", probe.selector),
                    max_ts,
                )
                .await;
            Some(timed(
                READBACK_QUERY_HISTOGRAM_SAMPLES.to_string(),
                started,
                result,
            )?)
        }
        _ => None,
    };
    let stale_marker_honored = match (
        expected.stale_marker_honored,
        plan.stale_probe,
        plan.probes.get(&PayloadClass::StaleMarker),
    ) {
        (Some(_), Some((marker_ts, _)), Some(probe)) => {
            let started = std::time::Instant::now();
            let result = reader.instant_series(&probe.selector, marker_ts).await;
            Some(timed(READBACK_QUERY_STALE_MARKER.to_string(), started, result)? == 0)
        }
        _ => None,
    };

    let mut mismatches = Vec::new();
    if series_count != expected.series_count {
        mismatches.push(format!(
            "series_count: read {series_count}, expected {}",
            expected.series_count
        ));
    }
    if sample_count != expected.sample_count {
        mismatches.push(format!(
            "sample_count: read {sample_count}, expected {}",
            expected.sample_count
        ));
    }
    if histogram_series_count != expected.histogram_series_count {
        mismatches.push(format!(
            "histogram_series_count: read {histogram_series_count:?}, expected {:?}",
            expected.histogram_series_count
        ));
    }
    if histogram_sample_count != expected.histogram_sample_count {
        mismatches.push(format!(
            "histogram_sample_count: read {histogram_sample_count:?}, expected {:?}",
            expected.histogram_sample_count
        ));
    }
    if stale_marker_honored != expected.stale_marker_honored {
        mismatches.push(format!(
            "stale_marker_honored: read {stale_marker_honored:?}, expected {:?}",
            expected.stale_marker_honored
        ));
    }
    Ok(Readback {
        query_url: config.query_url.clone(),
        series_count,
        sample_count,
        histogram_series_count,
        histogram_sample_count,
        stale_marker_honored,
        matches_expected: mismatches.is_empty(),
        mismatches,
        expected,
        attempts: 1,
        settled_after_ms: 0,
        query_ms,
    })
}

/// The read-back of a plan that holds no point, so has no window to query:
/// every figure reads zero, and an `expected` other than the default is
/// named as a mismatch, so `matches_expected` is false only with a reason
/// (`tests::an_empty_window_readback_names_why_it_does_not_match`).
fn empty_window_readback(query_url: String, expected: ReadbackExpected) -> Readback {
    let mut mismatches = Vec::new();
    if expected != ReadbackExpected::default() {
        mismatches.push(format!(
            "no point was sent, so no window was queried, but the plan expected {expected:?}"
        ));
    }
    Readback {
        query_url,
        series_count: 0,
        sample_count: 0,
        histogram_series_count: expected.histogram_series_count.map(|_| 0),
        histogram_sample_count: expected.histogram_sample_count.map(|_| 0),
        stale_marker_honored: None,
        matches_expected: mismatches.is_empty(),
        mismatches,
        expected,
        attempts: 1,
        settled_after_ms: 0,
        query_ms: BTreeMap::new(),
    }
}

/// A millisecond timestamp as the API's decimal seconds, a negative one
/// signed as a whole (`tests::seconds_renders_negative_milliseconds`).
fn seconds(ts_ms: i64) -> String {
    let sign = if ts_ms < 0 { "-" } else { "" };
    let abs = ts_ms.unsigned_abs();
    format!("{sign}{}.{:03}", abs / 1_000, abs % 1_000)
}

/// One receiver's Prometheus HTTP API, with Ravel's commit-token fence.
struct ApiReader<'a> {
    client: &'a reqwest::Client,
    base: &'a str,
    bearer: Option<&'a str>,
    tokens: &'a [String],
}

impl ApiReader<'_> {
    /// GET `path` with `params` and the commit-token fence; a non-2xx answer
    /// is an error carrying its body.
    async fn send(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<reqwest::Response, String> {
        let mut url = reqwest::Url::parse(&format!("{}{path}", self.base))
            .map_err(|e| format!("query URL {}{path} does not parse: {e}", self.base))?;
        {
            let mut pairs = url.query_pairs_mut();
            for (name, value) in params {
                pairs.append_pair(name, value);
            }
            for token in self.tokens {
                pairs.append_pair("min_commit_token", token);
            }
        }
        let mut request = self.client.get(url.clone());
        if let Some(token) = self.bearer {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("GET {path} failed: {e}"))?;
        let code = response.status();
        if !code.is_success() {
            let body = response
                .text()
                .await
                .map_err(|e| format!("GET {path} body read failed: {e}"))?;
            return Err(format!("GET {path} answered {code}: {body}"));
        }
        Ok(response)
    }

    async fn get(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<serde_json::Value, String> {
        let body = self
            .send(path, params)
            .await?
            .text()
            .await
            .map_err(|e| format!("GET {path} body read failed: {e}"))?;
        let value: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| format!("GET {path} answered non-JSON: {e}"))?;
        if value.get("status").and_then(|s| s.as_str()) != Some("success") {
            return Err(format!("GET {path} answered a non-success body: {body}"));
        }
        value
            .get("data")
            .cloned()
            .ok_or_else(|| format!("GET {path} answered no data: {body}"))
    }

    async fn series_count(&self, matcher: &str, window: &[(&str, String)]) -> Result<u64, String> {
        let mut params = vec![("match[]", matcher.to_string())];
        params.extend(window.iter().cloned());
        let path = "/api/v1/series";
        let response = self.send(path, &params).await?;
        count_series_body(path, response).await
    }

    /// The instant vector of `query` at `ts_ms`.
    async fn instant(&self, query: &str, ts_ms: i64) -> Result<Vec<serde_json::Value>, String> {
        let data = self
            .get(
                "/api/v1/query",
                &[("query", query.to_string()), ("time", seconds(ts_ms))],
            )
            .await?;
        data.get("result")
            .and_then(|r| r.as_array())
            .cloned()
            .ok_or_else(|| "query data has no result array".to_string())
    }

    /// The single value of a `count_over_time` instant query; an empty
    /// result is zero.
    async fn instant_count(&self, query: &str, ts_ms: i64) -> Result<u64, String> {
        let result = self.instant(query, ts_ms).await?;
        let mut total = 0u64;
        for element in &result {
            let text = element
                .get("value")
                .and_then(|v| v.get(1))
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("query result element has no value: {element}"))?;
            let value: f64 = text
                .parse()
                .map_err(|e| format!("query value {text} is not a number: {e}"))?;
            total += value as u64;
        }
        Ok(total)
    }

    /// How many series an instant query of `selector` at `ts_ms` returns.
    async fn instant_series(&self, selector: &str, ts_ms: i64) -> Result<u64, String> {
        Ok(self.instant(selector, ts_ms).await?.len() as u64)
    }
}

/// Body chunks in flight between the network and the series counter.
const SERIES_BODY_CHUNKS_IN_FLIGHT: usize = 4;

/// Count the elements of the `data` array of a `/api/v1/series` answer as
/// its body streams in, refusing a `status` other than `"success"`. The body
/// is never held whole: chunks pass through a channel of
/// [`SERIES_BODY_CHUNKS_IN_FLIGHT`] to a parser that discards each element
/// as [`serde::de::IgnoredAny`] once counted, so a 1,000,000-series answer
/// costs a few chunks and one element, not the body
/// (`tests::series_count_streams_a_200000_series_answer`).
async fn count_series_body(path: &str, mut response: reqwest::Response) -> Result<u64, String> {
    let (tx, rx) =
        tokio::sync::mpsc::channel::<Result<bytes::Bytes, String>>(SERIES_BODY_CHUNKS_IN_FLIGHT);
    let parser = tokio::task::spawn_blocking(move || {
        let reader = std::io::BufReader::new(ChunkReader {
            rx,
            current: bytes::Bytes::new(),
        });
        let mut de = serde_json::Deserializer::from_reader(reader);
        let body = SeriesBody::deserialize(&mut de)?;
        de.end()?;
        Ok::<_, serde_json::Error>(body)
    });
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => Ok(chunk),
            Ok(None) => break,
            Err(e) => Err(format!("GET {path} body read failed: {e}")),
        };
        let failed = chunk.is_err();
        // A closed channel means the parser already stopped; its error is
        // the one reported.
        if tx.send(chunk).await.is_err() || failed {
            break;
        }
    }
    drop(tx);
    let body = parser
        .await
        .map_err(|e| format!("GET {path} body parse task failed: {e}"))?
        .map_err(|e| format!("GET {path} answered a body that is not a series answer: {e}"))?;
    if body.status.as_deref() != Some("success") {
        return Err(format!(
            "GET {path} answered a non-success body: status {:?}, errorType {:?}, error {:?}",
            body.status, body.error_type, body.error
        ));
    }
    body.count
        .ok_or_else(|| format!("GET {path} answered no data"))
}

/// A blocking [`std::io::Read`] over body chunks sent from the async side.
struct ChunkReader {
    rx: tokio::sync::mpsc::Receiver<Result<bytes::Bytes, String>>,
    current: bytes::Bytes,
}

impl std::io::Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.current.is_empty() {
            match self.rx.blocking_recv() {
                Some(Ok(chunk)) => self.current = chunk,
                Some(Err(detail)) => return Err(std::io::Error::other(detail)),
                None => return Ok(0),
            }
        }
        let n = buf.len().min(self.current.len());
        buf[..n].copy_from_slice(&self.current.split_to(n));
        Ok(n)
    }
}

/// The fields of a `/api/v1/series` answer the read-back uses, with `data`
/// reduced to its element count.
#[derive(Default)]
struct SeriesBody {
    status: Option<String>,
    error_type: Option<String>,
    error: Option<String>,
    count: Option<u64>,
}

impl<'de> Deserialize<'de> for SeriesBody {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BodyVisitor;
        impl<'de> serde::de::Visitor<'de> for BodyVisitor {
            type Value = SeriesBody;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a Prometheus API answer object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<SeriesBody, A::Error> {
                let mut body = SeriesBody::default();
                while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                    match key.as_ref() {
                        "status" => body.status = map.next_value()?,
                        "errorType" => body.error_type = map.next_value()?,
                        "error" => body.error = map.next_value()?,
                        "data" => {
                            body.count = map.next_value::<Option<ElementCount>>()?.map(|c| c.0)
                        }
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(body)
            }
        }
        deserializer.deserialize_map(BodyVisitor)
    }
}

/// The length of a JSON array, read without keeping its elements.
struct ElementCount(u64);

impl<'de> Deserialize<'de> for ElementCount {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CountVisitor;
        impl<'de> serde::de::Visitor<'de> for CountVisitor {
            type Value = ElementCount;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("the series array")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<ElementCount, A::Error> {
                let mut n = 0u64;
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    n += 1;
                }
                Ok(ElementCount(n))
            }
        }
        deserializer.deserialize_seq(CountVisitor)
    }
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

    /// Group logical samples into `prometheus.TimeSeries` by canonical identity.
    fn write_request_from(samples: &[LogicalSample]) -> WriteRequest {
        let points: Vec<LogicalPoint> = samples.iter().cloned().map(LogicalPoint::Sample).collect();
        write_request_from_points(&points)
    }

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
            histograms_retry_exhausted_by_status: BTreeMap::new(),
            dropped_samples: 0,
            rejected_by_status: BTreeMap::new(),
            retry_exhausted_samples: 0,
            retry_exhausted_by_status: BTreeMap::new(),
            unsupported_samples: 0,
            unsupported_histograms: 0,
            expected_digests: ClassDigests::default(),
            accepted_digests: ClassDigests::default(),
            dropped_batches: 0,
            batches: 1,
            retries: 0,
            elapsed_secs: 1.0,
            elapsed_basis: ELAPSED_BASIS_TIMED_SEGMENTS.to_string(),
            generation_secs: 0.0,
            sender_bound: false,
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
            stored_bytes_per_point: 100.0,
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
    /// then share `Buffered`, `max_ack_p99_ms(&[IN_PROCESS_SYSTEM, "prometheus"])`
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
            report
                .row(IN_PROCESS_SYSTEM)
                .expect("ravel row")
                .ack_semantics,
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
            report
                .row(IN_PROCESS_SYSTEM)
                .expect("ravel row")
                .commit_tokens,
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
            .max_ack_p99_ms(&[IN_PROCESS_SYSTEM, "prometheus"])
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
                .max_ack_p99_ms(&[IN_PROCESS_SYSTEM])
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

    /// The report's `in_process_row` says whether [`IN_PROCESS_SYSTEM`] is
    /// among its rows, and serializes as `present` or `skipped`; a report
    /// with no such field reads as `present`.
    #[test]
    fn the_in_process_row_disclosure_follows_the_rows() {
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
        let both = MetricsIngestReport::new(vec![ravel, prometheus.clone()]);
        assert_eq!(both.in_process_row, InProcessRow::Present);
        let portable = MetricsIngestReport::new(vec![prometheus]);
        assert_eq!(portable.in_process_row, InProcessRow::Skipped);

        let mut json = serde_json::to_value(&portable).expect("serialize");
        assert_eq!(json["in_process_row"], "skipped");
        assert_eq!(
            serde_json::to_value(&both).expect("serialize")["in_process_row"],
            "present"
        );
        json.as_object_mut()
            .expect("object")
            .remove("in_process_row");
        let older: MetricsIngestReport = serde_json::from_value(json).expect("deserialize");
        assert_eq!(older.in_process_row, InProcessRow::Present);
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
        assert_eq!(outcome.result.system, IN_PROCESS_SYSTEM);
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
        points.push(LogicalPoint::Histogram(histogram(
            base + 15_000,
            vec![3, 1, 0],
        )));
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

        // The storage ratios cover the same records stored_bytes holds: five
        // samples at 16 logical bytes and two three-bucket histograms at
        // 24 + 3 * 8, so 7 points and 176 logical bytes. A ratio over samples
        // only (5 points, 80 bytes) fails both assertions.
        assert_eq!(logical_bytes_of(&points), 5 * 16 + 2 * (24 + 3 * 8));
        let storage = outcome.result.storage.as_ref().expect("storage present");
        assert!(storage.stored_bytes > 0);
        assert_eq!(
            storage.stored_bytes_per_point.to_bits(),
            (storage.stored_bytes as f64 / 7.0).to_bits(),
            "{storage:?}"
        );
        assert_eq!(
            storage.write_amplification.to_bits(),
            (storage.stored_bytes as f64 / 176.0).to_bits(),
            "{storage:?}"
        );
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

        assert_eq!(query.system, IN_PROCESS_SYSTEM);
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
        ingest.rejected_by_status = BTreeMap::from([(400, 1)]);
        ingest.offered_histograms = 3;
        ingest.accepted_histograms = 2;
        ingest.dropped_histograms = 1;
        ingest.histograms_rejected_by_status = BTreeMap::from([(400, 1)]);
        check_row_accounting(&ingest, &counts).expect("the row closes");

        // A row carrying named exclusions (the pre-change lane) still closes.
        let mut excluding = ingest.clone();
        excluding.offered_samples = 6;
        excluding.accepted_samples = 4;
        excluding.excluded_stale_marker = 1;
        excluding.offered_histograms = 0;
        excluding.accepted_histograms = 0;
        excluding.dropped_histograms = 0;
        excluding.histograms_rejected_by_status.clear();
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
            "6 offered against 4 accepted + 1 rejected + 1 dropped + 0 unsupported samples",
        ] {
            assert!(message.contains(figure), "missing `{figure}` in: {message}");
        }

        let mut unsplit = ingest.clone();
        unsplit.dropped_samples = 0;
        let err = check_row_accounting(&unsplit, &counts).expect_err("an open split");
        assert!(
            err.to_string().contains(
                "7 offered against 5 accepted + 1 rejected + 0 dropped + 0 unsupported samples"
            ),
            "{err}"
        );

        let mut unsplit_histograms = ingest.clone();
        unsplit_histograms.dropped_histograms = 0;
        let err = check_row_accounting(&unsplit_histograms, &counts)
            .expect_err("an open histogram split");
        assert!(
            err.to_string().contains(
                "3 offered against 2 accepted + 0 rejected + 0 dropped + 0 unsupported histograms"
            ),
            "{err}"
        );

        // Unsupported points close the split: a row that moves an accepted
        // histogram to `unsupported_histograms` still closes, and one that
        // counts it in neither does not.
        let mut unsupported = ingest.clone();
        unsupported.accepted_histograms = 1;
        unsupported.unsupported_histograms = 1;
        check_row_accounting(&unsupported, &counts).expect("unsupported closes the split");
        unsupported.unsupported_histograms = 0;
        let err = check_row_accounting(&unsupported, &counts).expect_err("a silent drop");
        assert!(
            err.to_string().contains(
                "3 offered against 1 accepted + 0 rejected + 1 dropped + 0 unsupported histograms"
            ),
            "{err}"
        );
    }

    /// Every dropped point is bucketed: `rejected_by_status` summed plus
    /// `retry_exhausted_*` equals `dropped_*`, for samples and for
    /// histograms, on every row (the in-process row included, whose maps are
    /// empty and whose drops are zero).
    ///
    /// TO SEE THIS FAIL: drop the status identity clauses from
    /// `check_row_accounting`; both `expect_err` calls below then pass the row.
    #[test]
    fn dropped_points_must_be_bucketed_by_status_or_retry_exhaustion() {
        let counts = StreamCounts {
            generated_lines: 10,
            scalar: 6,
            native_histogram: 3,
            stale_marker: 1,
            malformed: 0,
        };
        let mut ingest = ingest_phase(1.0);
        ingest.stream_counts = counts;
        ingest.offered_samples = 7;
        ingest.accepted_samples = 4;
        ingest.dropped_samples = 3;
        ingest.rejected_by_status = BTreeMap::from([(400, 1), (422, 1)]);
        ingest.retry_exhausted_samples = 1;
        ingest.retry_exhausted_by_status = BTreeMap::from([(503, 1)]);
        ingest.offered_histograms = 3;
        ingest.accepted_histograms = 1;
        ingest.dropped_histograms = 2;
        ingest.histograms_rejected_by_status = BTreeMap::from([(400, 1)]);
        ingest.retry_exhausted_histograms = 1;
        check_row_accounting(&ingest, &counts).expect("every drop is bucketed");

        let mut unbucketed = ingest.clone();
        unbucketed.retry_exhausted_samples = 0;
        let err = check_row_accounting(&unbucketed, &counts).expect_err("an unbucketed sample");
        assert_eq!(err.rejected_by_status, 2);
        assert_eq!(err.retry_exhausted, 0);
        assert!(
            err.to_string()
                .contains("2 rejected_by_status + 0 retry_exhausted against 3 dropped samples"),
            "{err}"
        );

        let mut unbucketed = ingest.clone();
        unbucketed.histograms_rejected_by_status.clear();
        let err = check_row_accounting(&unbucketed, &counts).expect_err("an unbucketed histogram");
        assert!(
            err.to_string().contains(
                "0 histograms_rejected_by_status + 1 retry_exhausted against 2 dropped histograms"
            ),
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
        assert_ne!(
            base,
            digest(&quiet_nan),
            "a stale marker is its bit pattern"
        );

        let edit = |f: &dyn Fn(&mut LogicalHistogram)| {
            let mut changed = points.clone();
            if let LogicalPoint::Histogram(h) = &mut changed[1] {
                f(h);
            }
            digest(&changed)
        };
        assert_ne!(
            base,
            edit(&|h| h.positive_deltas[2] = -1),
            "one bucket delta"
        );
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
            receiver: Receiver::Prometheus,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size: 1,
            max_retries: 2,
            timeout_secs: 5,
            shards: 1,
            stall_ceiling: None,
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
        assert_eq!(
            ingest.retry_exhausted_by_status,
            BTreeMap::from([(503u16, 1u64)]),
            "the exhausted batch is keyed by the status it ended on"
        );
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
        assert_eq!(
            row.last_commit_token, None,
            "a buffered row records no token"
        );
    }

    /// An encoded commit token for `shard` at `seq`.
    fn token(shard: u32, seq: u64) -> String {
        CommitToken {
            shard,
            writer_id: uuid::Uuid::from_u128(0x5eed),
            epoch: 1,
            seq,
            ingest_hour_bucket: 488_888,
        }
        .encode()
    }

    /// The `x-ravel-commit-token` header a Ravel-like stub answers request
    /// `n` (1-based) with: the requests touch different shards, and shard 0
    /// is written twice.
    fn ravel_stub_header(n: usize) -> String {
        match n {
            1 => format!("{},{}", token(0, 1), token(1, 1)),
            2 => token(0, 2),
            _ => token(2, 1),
        }
    }

    /// The portable Ravel row against a Ravel-like stub (a 2xx with
    /// per-shard tokens): every request carries `Authorization: Bearer
    /// <token>`, `commit_tokens_by_shard` keeps the latest token of every
    /// shard written, `last_commit_token` is those tokens comma-joined,
    /// histograms land in their own class, and the bearer text appears
    /// nowhere in the serialized report.
    ///
    /// TO SEE THIS FAIL: drop the `request.bearer_auth(token)` line in
    /// `replay_over_http_with_plan` (the header assertion fails), or keep
    /// only the last header instead of folding every header by shard in
    /// `record_commit_tokens` (the by-shard map then holds shard 2 alone).
    #[tokio::test]
    async fn the_portable_ravel_row_sends_the_bearer_and_keeps_the_latest_token_of_every_shard() {
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
                        [(COMMIT_TOKEN_HEADER, ravel_stub_header(n))],
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
        points.push(LogicalPoint::Histogram(histogram(
            base + 15_000,
            vec![2, 1],
        )));
        let config = HttpReplayConfig {
            receiver: Receiver::Ravel,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: Some(secret.to_string()),
            tenant: Some("metricsbench".to_string()),
            batch_size: 2,
            max_retries: 0,
            timeout_secs: 5,
            shards: 1,
            stall_ceiling: None,
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
        assert_eq!(row.acceptance_basis, Some(AcceptanceBasis::ReadBack));
        assert_eq!(row.tenant.as_deref(), Some("metricsbench"));
        let by_shard = BTreeMap::from([(0u32, token(0, 2)), (1, token(1, 1)), (2, token(2, 1))]);
        assert_eq!(row.commit_tokens_by_shard.as_ref(), Some(&by_shard));
        let joined = format!("{},{},{}", token(0, 2), token(1, 1), token(2, 1));
        assert_eq!(row.last_commit_token.as_deref(), Some(joined.as_str()));
        assert_eq!(row.ingest.offered_samples, 4);
        assert_eq!(row.ingest.accepted_samples, 4);
        assert_eq!(row.ingest.offered_histograms, 2);
        assert_eq!(row.ingest.accepted_histograms, 2);
        check_row_accounting(&row.ingest, &row.ingest.stream_counts).expect("closes");
        check_row_band6(&row).expect("a Ravel-like stub that keeps everything passes band 6");

        let report =
            serde_json::to_string(&MetricsIngestReport::new(vec![row])).expect("serialize report");
        assert!(
            report.contains(&format!("\"last_commit_token\":\"{joined}\"")),
            "{report}"
        );
        assert!(
            !report.contains(secret),
            "the bearer token leaked: {report}"
        );
    }

    /// A durable-on-ack row whose receiver acknowledges every request without
    /// ever returning a commit token (an endpoint that is not ravel-server) is
    /// refused rather than published as durable.
    ///
    /// TO SEE THIS FAIL: delete the `UnbackedDurability` check before the
    /// final `Ok` in `replay_over_http`; the replay then returns a row.
    #[tokio::test]
    async fn a_durable_row_without_any_commit_token_is_refused() {
        let app = axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let points: Vec<LogicalPoint> = valid_stream(4, 2)
            .into_iter()
            .map(LogicalPoint::Sample)
            .collect();
        let config = HttpReplayConfig {
            receiver: Receiver::Ravel,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size: 2,
            max_retries: 0,
            timeout_secs: 5,
            shards: 1,
            stall_ceiling: None,
        };
        let err = replay_over_http(&config, &mut SlicePoints::new(&points))
            .await
            .expect_err("an unbacked durable row must be refused");
        server.abort();
        assert!(
            matches!(
                err,
                LaneError::UnbackedDurability {
                    acknowledged: 4,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    /// A Ravel-like stub whose token header does not decode fails the replay
    /// rather than publishing an unusable fence.
    #[tokio::test]
    async fn an_undecodable_commit_token_is_refused() {
        let points = class_mix(base_ts_ms());
        let err = try_replay_stub(
            Receiver::Ravel,
            vec![Canned::ok(Some("tok-1".to_string()))],
            &points,
            9,
        )
        .await
        .expect_err("an undecodable token");
        assert!(
            matches!(&err, LaneError::InvalidCommitToken { value, .. } if value == "tok-1"),
            "{err:?}"
        );
    }

    fn scalar(instance: &str, ts_ms: i64, value: f64) -> LogicalPoint {
        LogicalPoint::Sample(LogicalSample {
            metric: "mb_gauge".to_string(),
            labels: vec![("instance".to_string(), instance.to_string())],
            ts_ms,
            value,
        })
    }

    /// Every class: series `a` with three scalars then a stale marker 15 s
    /// after the last, series `b` with three scalars, and two native
    /// histograms on `mb_native{instance="a"}`. Indexes 3, 5 and 8 are a
    /// scalar, a histogram and the stale marker.
    fn class_mix(base: i64) -> Vec<LogicalPoint> {
        vec![
            scalar("a", base, 1.0),
            scalar("b", base, 2.0),
            LogicalPoint::Histogram(histogram(base, vec![1, 1])),
            scalar("a", base + 15_000, 1.5),
            scalar("b", base + 15_000, 2.5),
            LogicalPoint::Histogram(histogram(base + 15_000, vec![2, 1])),
            scalar("a", base + 30_000, 1.75),
            scalar("b", base + 30_000, 2.75),
            scalar("a", base + 45_000, f64::from_bits(STALE_MARKER_BITS)),
        ]
    }

    /// One canned answer of a write stub.
    #[derive(Clone)]
    struct Canned {
        status: u16,
        body: String,
        token: Option<String>,
    }

    impl Canned {
        fn ok(token: Option<String>) -> Self {
            Canned {
                status: 204,
                body: String::new(),
                token,
            }
        }

        /// A Mimir 400 naming `point` as its rejected sample.
        fn mimir_400(point: &LogicalPoint) -> Self {
            Canned {
                status: 400,
                body: format!(
                    "send data to ingesters: failed pushing to ingester ingester-0: \
                     user=anonymous: the sample has been rejected because another sample with \
                     the same timestamp, but a different value, has already been ingested \
                     (err-mimir-sample-duplicate-timestamp). The affected sample has {}",
                    mimir_sample_needle(point)
                ),
                token: None,
            }
        }
    }

    async fn serve(app: axum::Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, server)
    }

    /// A write stub answering request `n` with `canned[n]`, the last answer
    /// repeating.
    fn write_stub(canned: Vec<Canned>) -> axum::Router {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let canned = Arc::new(canned);
        let requests = Arc::new(AtomicUsize::new(0));
        axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(move || {
                let canned = Arc::clone(&canned);
                let requests = Arc::clone(&requests);
                async move {
                    let i = requests
                        .fetch_add(1, Ordering::SeqCst)
                        .min(canned.len() - 1);
                    let answer = canned[i].clone();
                    let mut response =
                        axum::response::Response::new(axum::body::Body::from(answer.body));
                    *response.status_mut() =
                        axum::http::StatusCode::from_u16(answer.status).expect("status");
                    if let Some(token) = answer.token {
                        response.headers_mut().insert(
                            axum::http::HeaderName::from_static(COMMIT_TOKEN_HEADER),
                            axum::http::HeaderValue::from_str(&token).expect("header value"),
                        );
                    }
                    response
                }
            }),
        )
    }

    async fn try_replay_stub(
        receiver: Receiver,
        canned: Vec<Canned>,
        points: &[LogicalPoint],
        batch_size: usize,
    ) -> Result<HttpReplayOutcome, LaneError> {
        let (addr, server) = serve(write_stub(canned)).await;
        let config = HttpReplayConfig {
            receiver,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size,
            max_retries: 0,
            timeout_secs: 5,
            shards: 1,
            stall_ceiling: None,
        };
        let outcome = replay_over_http_with_plan(&config, &mut SlicePoints::new(points)).await;
        server.abort();
        outcome
    }

    async fn replay_stub(
        receiver: Receiver,
        canned: Vec<Canned>,
        points: &[LogicalPoint],
        batch_size: usize,
    ) -> SystemIngestResult {
        try_replay_stub(receiver, canned, points, batch_size)
            .await
            .expect("replay")
            .result
    }

    /// One 400 for a whole request, read two ways: Prometheus rejects every
    /// point of the request; Mimir rejects only the sample its body names
    /// and accepts the rest.
    ///
    /// TO SEE THIS FAIL: make `Receiver::soft_rejects_samples` false for
    /// Mimir (the Mimir row then drops all nine points), or true for
    /// Prometheus (its row then accepts eight).
    #[tokio::test]
    async fn prometheus_rejects_a_400_request_whole_and_mimir_only_the_named_sample() {
        let points = class_mix(base_ts_ms());
        let canned = vec![Canned::mimir_400(&points[3]), Canned::ok(None)];

        let prometheus = replay_stub(Receiver::Prometheus, canned.clone(), &points, 9).await;
        let ingest = &prometheus.ingest;
        assert_eq!(
            prometheus.acceptance_basis,
            Some(AcceptanceBasis::Acknowledged)
        );
        assert_eq!(ingest.offered_samples, 7);
        assert_eq!(ingest.accepted_samples, 0);
        assert_eq!(ingest.accepted_histograms, 0);
        assert_eq!(ingest.dropped_samples, 7);
        assert_eq!(ingest.dropped_histograms, 2);
        assert_eq!(ingest.rejected_by_status, BTreeMap::from([(400u16, 7u64)]));
        assert_eq!(
            ingest.histograms_rejected_by_status,
            BTreeMap::from([(400u16, 2u64)])
        );
        assert_eq!(ingest.dropped_batches, 1);
        check_row_accounting(ingest, &ingest.stream_counts).expect("band 4 closes");

        let mimir = replay_stub(Receiver::Mimir, canned, &points, 9).await;
        let ingest = &mimir.ingest;
        assert_eq!(mimir.acceptance_basis, Some(AcceptanceBasis::Acknowledged));
        assert_eq!(ingest.accepted_samples, 6);
        assert_eq!(ingest.accepted_histograms, 2);
        assert_eq!(ingest.dropped_samples, 1);
        assert_eq!(ingest.dropped_histograms, 0);
        assert_eq!(ingest.rejected_by_status, BTreeMap::from([(400u16, 1u64)]));
        assert!(ingest.histograms_rejected_by_status.is_empty());
        assert_eq!(ingest.dropped_batches, 1);
        check_row_accounting(ingest, &ingest.stream_counts).expect("band 4 closes");
    }

    /// A Mimir 400 whose body names no point of the batch rejects the batch
    /// whole: an unparseable body is never read as an acceptance.
    #[tokio::test]
    async fn a_mimir_400_naming_no_point_of_the_batch_rejects_it_whole() {
        let points = class_mix(base_ts_ms());
        let canned = vec![Canned {
            status: 400,
            body: "bad request".to_string(),
            token: None,
        }];
        let mimir = replay_stub(Receiver::Mimir, canned, &points, 9).await;
        assert_eq!(mimir.ingest.accepted_samples, 0);
        assert_eq!(mimir.ingest.dropped_samples, 7);
        assert_eq!(mimir.ingest.dropped_histograms, 2);
        assert_eq!(
            mimir.ingest.rejected_by_status,
            BTreeMap::from([(400u16, 7u64)])
        );
    }

    /// Mimir's rendering of a rejected sample: Go RFC3339Nano UTC with the
    /// fraction's trailing zeros trimmed, and the series as its name then
    /// sorted labels with Go-quoted values.
    #[test]
    fn mimir_needles_match_mimirs_rendering() {
        assert_eq!(go_rfc3339_nano_ms(0), "1970-01-01T00:00:00Z");
        assert_eq!(go_rfc3339_nano_ms(951_782_400_000), "2000-02-29T00:00:00Z");
        assert_eq!(
            go_rfc3339_nano_ms(1_760_000_010_000),
            "2025-10-09T08:53:30Z"
        );
        assert_eq!(
            go_rfc3339_nano_ms(1_760_000_010_123),
            "2025-10-09T08:53:30.123Z"
        );
        assert_eq!(
            go_rfc3339_nano_ms(1_760_000_010_100),
            "2025-10-09T08:53:30.1Z"
        );
        let point = LogicalPoint::Sample(LogicalSample {
            metric: "m".to_string(),
            labels: vec![
                ("zone".to_string(), "eu\"1\n".to_string()),
                ("instance".to_string(), "a".to_string()),
            ],
            ts_ms: 1_760_000_010_000,
            value: 1.0,
        });
        assert_eq!(
            mimir_sample_needle(&point),
            "timestamp 2025-10-09T08:53:30Z and is from series \
             m{instance=\"a\", zone=\"eu\\\"1\\n\"}"
        );
        let bare = LogicalPoint::Sample(LogicalSample {
            metric: "m".to_string(),
            labels: Vec::new(),
            ts_ms: 1_760_000_025_000,
            value: 1.0,
        });
        assert_eq!(
            mimir_sample_needle(&bare),
            "timestamp 2025-10-09T08:53:45Z and is from series m"
        );
    }

    /// A Mimir 400 body's text around one needle: the start of Mimir's
    /// `sampleError` message before it, and `rest` after it.
    fn mimir_body(needle: &str, rest: &str) -> String {
        format!(
            "failed pushing to ingester: user=anonymous: the sample has been rejected \
             because another sample with a more recent timestamp has already been \
             ingested and out-of-order samples are not allowed \
             (err-mimir-sample-out-of-order). The affected sample has {needle}{rest}"
        )
    }

    fn needle_at_ms(metric: &str, labels: &[(&str, &str)]) -> String {
        mimir_sample_needle(&LogicalPoint::Sample(LogicalSample {
            metric: metric.to_string(),
            labels: labels
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect(),
            ts_ms: 1_760_000_010_000,
            value: 1.0,
        }))
    }

    /// The label-less needle of `m` is a prefix of the needle of
    /// `m{instance="a"}` at the same millisecond, and a body naming only the
    /// labelled point does not name the label-less one.
    ///
    /// TO SEE THIS FAIL: make `mimir_body_names` return `body.contains(needle)`.
    #[test]
    fn a_bare_mimir_needle_is_not_named_by_its_labelled_series() {
        let bare = needle_at_ms("m", &[]);
        let labelled = needle_at_ms("m", &[("instance", "a")]);
        assert!(labelled.starts_with(&bare));
        for rest in ["", "\n"] {
            let body = mimir_body(&labelled, rest);
            assert!(!mimir_body_names(&body, &bare), "{body}");
        }
    }

    /// The needle of `m` is a prefix of the needle of `mx`, and a body naming
    /// only `mx` does not name `m`.
    ///
    /// TO SEE THIS FAIL: make `mimir_body_names` return `body.contains(needle)`,
    /// or refuse only a following `{`.
    #[test]
    fn a_mimir_needle_is_not_named_by_a_longer_metric_name() {
        let m = needle_at_ms("m", &[]);
        let longer = needle_at_ms("mx", &[]);
        assert!(longer.starts_with(&m));
        for rest in ["", "\n"] {
            let body = mimir_body(&longer, rest);
            assert!(!mimir_body_names(&body, &m), "{body}");
        }
    }

    /// Each needle is named when its sample is: at the end of the body,
    /// followed by the rest of Mimir's message, and after a prefix-sharing
    /// needle that does not name it.
    #[test]
    fn a_mimir_needle_is_named_when_its_sample_is() {
        let bare = needle_at_ms("m", &[]);
        let labelled = needle_at_ms("m", &[("instance", "a")]);
        let longer = needle_at_ms("mx", &[]);
        for needle in [&bare, &labelled, &longer] {
            for rest in ["", "\n", " (sample 1 of 2)\n"] {
                let body = mimir_body(needle, rest);
                assert!(mimir_body_names(&body, needle), "{body}");
            }
        }
        for decoy in [&labelled, &longer] {
            let body = format!("{}{}", mimir_body(decoy, "\n"), mimir_body(&bare, "\n"));
            assert!(mimir_body_names(&body, &bare), "{body}");
        }
    }

    /// The no-point read-back reports `matches_expected` false only with a
    /// mismatch line naming why.
    ///
    /// TO SEE THIS FAIL: set `matches_expected` to
    /// `expected == ReadbackExpected::default()` and leave `mismatches` empty.
    #[test]
    fn an_empty_window_readback_names_why_it_does_not_match() {
        let clean = empty_window_readback("http://q".to_string(), ReadbackExpected::default());
        assert!(clean.matches_expected);
        assert!(clean.mismatches.is_empty());
        let expected = ReadbackExpected {
            series_count: 2,
            sample_count: 3,
            histogram_series_count: Some(1),
            histogram_sample_count: Some(1),
            stale_marker_honored: Some(true),
        };
        let readback = empty_window_readback("http://q".to_string(), expected.clone());
        assert!(!readback.matches_expected);
        assert_eq!(readback.matches_expected, readback.mismatches.is_empty());
        assert_eq!(readback.mismatches.len(), 1);
        assert!(
            readback.mismatches[0].contains("series_count: 2"),
            "{:?}",
            readback.mismatches
        );
        assert_eq!(readback.expected, expected);
        assert_eq!(readback.histogram_series_count, Some(0));
    }

    #[test]
    fn seconds_renders_negative_milliseconds() {
        assert_eq!(seconds(-1), "-0.001");
        assert_eq!(seconds(-1_000), "-1.000");
        assert_eq!(seconds(-1_500), "-1.500");
        assert_eq!(seconds(0), "0.000");
        assert_eq!(seconds(1_500), "1.500");
    }

    /// A VictoriaMetrics 2xx acknowledges native histograms it cannot store:
    /// they land under `unsupported_histograms`, the row declares the class
    /// with its citation, and band 6 passes over the classes it stores. The
    /// same 2xx from Prometheus accepts the histograms.
    ///
    /// TO SEE THIS FAIL: make `Receiver::supports` return false for native
    /// histograms on every receiver; the Prometheus assertions then fail.
    #[tokio::test]
    async fn victoriametrics_acknowledges_histograms_it_cannot_store() {
        let points = class_mix(base_ts_ms());
        let vm = replay_stub(
            Receiver::VictoriaMetrics,
            vec![Canned::ok(None)],
            &points,
            4,
        )
        .await;
        assert_eq!(vm.ingest.offered_histograms, 2);
        assert_eq!(vm.ingest.accepted_histograms, 0);
        assert_eq!(vm.ingest.unsupported_histograms, 2, "every histogram sent");
        assert_eq!(vm.ingest.dropped_histograms, 0);
        assert_eq!(vm.ingest.excluded_native_histogram, 0);
        assert_eq!(vm.ingest.accepted_samples, 7);
        assert_eq!(vm.ingest.unsupported_samples, 0);
        assert_eq!(vm.unsupported_classes.len(), 1);
        assert_eq!(
            vm.unsupported_classes[0].class,
            PayloadClass::NativeHistogram
        );
        assert!(
            vm.unsupported_classes[0]
                .source
                .contains("lib/prompb/prompb.go")
                && vm.unsupported_classes[0].source.contains("v1.115.0"),
            "{:?}",
            vm.unsupported_classes
        );
        check_row_accounting(&vm.ingest, &vm.ingest.stream_counts).expect("band 4 closes");
        check_row_band6(&vm).expect("band 6 passes over the stored classes");
        assert_ne!(
            vm.ingest.accepted_digests.native_histogram,
            vm.ingest.expected_digests.native_histogram,
            "the excluded class is not compared, not faked"
        );

        let prometheus =
            replay_stub(Receiver::Prometheus, vec![Canned::ok(None)], &points, 4).await;
        assert_eq!(prometheus.ingest.accepted_histograms, 2);
        assert_eq!(prometheus.ingest.unsupported_histograms, 0);
        assert!(prometheus.unsupported_classes.is_empty());
        check_row_band6(&prometheus).expect("band 6 passes");

        assert_eq!(
            vm.ingest.expected_digests, prometheus.ingest.expected_digests,
            "a row with an excluded class is still offered every class"
        );
        check_expected_digests_agree(&[&vm, &prometheus], &PayloadClass::ALL)
            .expect("one expected set");
    }

    /// Band 6 fails when a receiver that stores every class drops one point
    /// of any one class, even though band 4 closes (the drop is counted).
    ///
    /// TO SEE THIS FAIL: feed `accepted_digests` from the offered points
    /// instead of the accepted ones (every case then passes band 6).
    #[tokio::test]
    async fn band6_fails_when_a_supporting_receiver_drops_one_point_of_any_class() {
        let points = class_mix(base_ts_ms());
        let clean = replay_stub(Receiver::Mimir, vec![Canned::ok(None)], &points, 9).await;
        check_row_band6(&clean).expect("nothing dropped");
        for (index, class) in [
            (3, PayloadClass::Scalar),
            (5, PayloadClass::NativeHistogram),
            (8, PayloadClass::StaleMarker),
        ] {
            assert_eq!(PayloadClass::of(&points[index]), class);
            let row = replay_stub(
                Receiver::Mimir,
                vec![Canned::mimir_400(&points[index]), Canned::ok(None)],
                &points,
                9,
            )
            .await;
            check_row_accounting(&row.ingest, &row.ingest.stream_counts)
                .expect("the drop is counted");
            let err = check_row_band6(&row).expect_err("one point dropped");
            assert!(
                matches!(&err, Band6Violation::AcceptedDiffers { class: c, .. } if *c == class),
                "{class:?}: {err}"
            );
            for other in PayloadClass::ALL.into_iter().filter(|c| *c != class) {
                assert_eq!(
                    row.ingest.accepted_digests.get(other),
                    row.ingest.expected_digests.get(other),
                    "{other:?} kept every point"
                );
            }
        }
    }

    /// Band 6 compares digests, not counts: a row that accepted as many
    /// scalars as it was sent, but not the same ones, fails.
    ///
    /// TO SEE THIS FAIL: compare `accepted_samples` with `offered_samples`
    /// in `check_row_band6` instead of the digests; the swapped row passes.
    #[test]
    fn band6_compares_digests_not_counts() {
        let base = 1_760_000_010_000;
        let (a, b, c) = (
            scalar("a", base, 1.0),
            scalar("b", base, 2.0),
            scalar("c", base, 3.0),
        );
        let digests = |points: &[&LogicalPoint]| {
            let mut digester = ClassDigester::default();
            for point in points {
                digester.update(point);
            }
            digester.finish()
        };
        let mut ingest = ingest_phase(1.0);
        ingest.offered_samples = 2;
        ingest.accepted_samples = 2;
        ingest.expected_digests = digests(&[&a, &b]);
        ingest.accepted_digests = digests(&[&a, &c]);
        let row = SystemIngestResult::buffered_http_row(
            "mimir".to_string(),
            "http://mimir.example/api/v1/push".to_string(),
            "stub".to_string(),
            ingest.clone(),
        );
        let err = check_row_band6(&row).expect_err("same count, different points");
        assert!(
            matches!(
                err,
                Band6Violation::AcceptedDiffers {
                    class: PayloadClass::Scalar,
                    ..
                }
            ),
            "{err}"
        );

        let mut same = row.clone();
        same.ingest.accepted_digests = digests(&[&a, &b]);
        check_row_band6(&same).expect("the same points pass");
    }

    /// Two portable rows offered different expected sets for one class fail
    /// the run, whichever class it is.
    #[test]
    fn portable_rows_with_different_expected_digests_fail() {
        let base = 1_760_000_010_000;
        let mut digester = ClassDigester::default();
        for point in class_mix(base) {
            digester.update(&point);
        }
        let expected = digester.finish();
        let mut ingest = ingest_phase(1.0);
        ingest.expected_digests = expected.clone();
        let first = SystemIngestResult::buffered_http_row(
            "prometheus".to_string(),
            "http://p/api/v1/write".to_string(),
            "stub".to_string(),
            ingest.clone(),
        );
        let mut second = first.clone();
        second.system = "victoriametrics".to_string();
        check_expected_digests_agree(&[&first, &second], &PayloadClass::ALL).expect("agree");

        let mut diverged = second.clone();
        let mut other = ClassDigester::default();
        other.update(&LogicalPoint::Histogram(histogram(base, vec![3])));
        diverged.ingest.expected_digests.native_histogram = other.finish().native_histogram;
        let err = check_expected_digests_agree(&[&first, &diverged], &PayloadClass::ALL)
            .expect_err("histogram sets differ");
        assert!(
            matches!(
                &err,
                Band6Violation::ExpectedDiverged {
                    class: PayloadClass::NativeHistogram,
                    second_system,
                    ..
                } if second_system == "victoriametrics"
            ),
            "{err}"
        );
        check_expected_digests_agree(&[&first, &diverged], &[PayloadClass::Scalar])
            .expect("only the named classes are compared");
    }

    /// The unsupported-class table has one entry, VictoriaMetrics plus
    /// native histograms, and every other receiver stores every class.
    ///
    /// TO SEE THIS FAIL: add a second entry to `UNSUPPORTED_CLASSES`, or
    /// remove the VictoriaMetrics one.
    #[test]
    fn the_unsupported_class_table_names_only_victoriametrics_histograms() {
        assert_eq!(UNSUPPORTED_CLASSES.len(), 1);
        for receiver in [
            Receiver::Ravel,
            Receiver::Prometheus,
            Receiver::VictoriaMetrics,
            Receiver::Mimir,
        ] {
            for class in PayloadClass::ALL {
                let unsupported =
                    receiver == Receiver::VictoriaMetrics && class == PayloadClass::NativeHistogram;
                assert_eq!(
                    receiver.supports(class),
                    !unsupported,
                    "{receiver:?} {class:?}"
                );
            }
            assert_eq!(
                receiver.unsupported_classes().len(),
                usize::from(receiver == Receiver::VictoriaMetrics)
            );
        }
    }

    /// The in-process row and a portable row replaying the same points carry
    /// equal expected digests for every class, the scalar one included.
    #[tokio::test]
    async fn the_in_process_and_portable_rows_carry_equal_expected_digests() {
        let points = class_mix(base_ts_ms());
        let in_process = replay_into_ravel(&memory_config("t", 4), &mut SlicePoints::new(&points))
            .await
            .expect("in-process replay");
        let portable = replay_stub(
            Receiver::Ravel,
            vec![Canned::ok(Some(token(0, 1)))],
            &points,
            4,
        )
        .await;
        assert_ne!(
            in_process.result.ingest.expected_digests.scalar,
            ClassDigests::default().scalar
        );
        assert_eq!(
            in_process.result.ingest.expected_digests,
            portable.ingest.expected_digests
        );
        assert_eq!(in_process.result.tenant.as_deref(), Some("t"));
        check_row_band6(&in_process.result).expect("the in-process row keeps every point");
    }

    /// What a read-back expects follows what was sent and what the receiver
    /// stores: VictoriaMetrics expects no histogram series and counts none.
    #[test]
    fn readback_expectations_follow_what_was_sent_and_what_the_receiver_stores() {
        let points = class_mix(1_760_000_010_000);
        let plan = |receiver| {
            let mut plan = ReadbackPlan::new(receiver);
            for point in &points {
                plan.observe(point);
            }
            plan.expected()
        };
        assert_eq!(
            plan(Receiver::Ravel),
            ReadbackExpected {
                series_count: 3,
                sample_count: 3,
                histogram_series_count: Some(1),
                histogram_sample_count: Some(2),
                stale_marker_honored: Some(true),
            }
        );
        assert_eq!(
            plan(Receiver::VictoriaMetrics),
            ReadbackExpected {
                series_count: 2,
                sample_count: 3,
                histogram_series_count: None,
                histogram_sample_count: None,
                stale_marker_honored: Some(true),
            }
        );
    }

    /// A query stub answering the series and query endpoints as a receiver
    /// that stored `class_mix` would, with `extra_series` added to the
    /// all-series cardinality; it records every query string it receives.
    /// Its first `hidden` requests, over both endpoints, answer as a receiver
    /// that has acknowledged the writes but not yet made them queryable.
    fn query_stub(
        extra_series: usize,
        hidden: usize,
        seen: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> axum::Router {
        let pairs = |raw: &Option<String>| -> Vec<(String, String)> {
            reqwest::Url::parse(&format!("http://stub/?{}", raw.clone().unwrap_or_default()))
                .expect("query parses")
                .query_pairs()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        };
        let seen_series = Arc::clone(&seen);
        axum::Router::new()
            .route(
                "/api/v1/series",
                axum::routing::get(
                    move |axum::extract::RawQuery(raw): axum::extract::RawQuery| {
                        let seen = Arc::clone(&seen_series);
                        async move {
                            let lagging = {
                                let mut seen = seen.lock().expect("lock");
                                seen.push(raw.clone().unwrap_or_default());
                                seen.len() <= hidden
                            };
                            let matcher = pairs(&raw)
                                .into_iter()
                                .find(|(k, _)| k == "match[]")
                                .map(|(_, v)| v)
                                .unwrap_or_default();
                            // The histogram probe names only `mb_native`;
                            // the cardinality read names every metric sent.
                            let n = if lagging {
                                0
                            } else if matcher.contains("mb_native") && !matcher.contains('|') {
                                1
                            } else {
                                3 + extra_series
                            };
                            axum::Json(serde_json::json!({
                                "status": "success",
                                "data": vec![serde_json::json!({}); n],
                            }))
                        }
                    },
                ),
            )
            .route(
                "/api/v1/query",
                axum::routing::get(
                    move |axum::extract::RawQuery(raw): axum::extract::RawQuery| {
                        let seen = Arc::clone(&seen);
                        async move {
                            let lagging = {
                                let mut seen = seen.lock().expect("lock");
                                seen.push(raw.clone().unwrap_or_default());
                                seen.len() <= hidden
                            };
                            let query = pairs(&raw)
                                .into_iter()
                                .find(|(k, _)| k == "query")
                                .map(|(_, v)| v)
                                .unwrap_or_default();
                            let result = if lagging || !query.starts_with("count_over_time(") {
                                serde_json::json!([])
                            } else {
                                let n = if query.contains("mb_native") {
                                    "2"
                                } else {
                                    "3"
                                };
                                serde_json::json!([{"metric": {}, "value": [0, n]}])
                            };
                            axum::Json(serde_json::json!({
                                "status": "success",
                                "data": {"resultType": "vector", "result": result},
                            }))
                        }
                    },
                ),
            )
    }

    /// A one-connection HTTP stub that answers any request 200 with a
    /// `Content-Length` of `declared` (the pieces' total when `None`) and
    /// writes `pieces` 20 ms apart, so the client reads them as separate
    /// chunks, then closes.
    async fn chunked_body_stub(
        pieces: Vec<Vec<u8>>,
        declared: Option<usize>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.expect("read request");
                assert!(n > 0, "the client closed before sending a request");
                request.extend_from_slice(&buf[..n]);
            }
            let len = declared.unwrap_or_else(|| pieces.iter().map(Vec::len).sum());
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {len}\r\nconnection: close\r\n\r\n"
            );
            socket.write_all(head.as_bytes()).await.expect("head");
            for piece in pieces {
                socket.write_all(&piece).await.expect("piece");
                socket.flush().await.expect("flush");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        (addr, server)
    }

    /// `body` cut into `n` pieces at even byte offsets, which land inside
    /// elements and strings.
    fn pieces_of(body: &[u8], n: usize) -> Vec<Vec<u8>> {
        body.chunks(body.len().div_ceil(n))
            .map(<[u8]>::to_vec)
            .collect()
    }

    fn series_answer(n: usize) -> Vec<u8> {
        let mut body = String::from(r#"{"status":"success","data":["#);
        for i in 0..n {
            if i > 0 {
                body.push(',');
            }
            body.push_str(&format!(
                r#"{{"__name__":"mb_cardinality","instance":"host-{i}","job":"j[{{,}}]\"q"}}"#
            ));
        }
        body.push_str("]}");
        body.into_bytes()
    }

    async fn stub_series_count(
        pieces: Vec<Vec<u8>>,
        declared: Option<usize>,
    ) -> Result<u64, String> {
        let (addr, server) = chunked_body_stub(pieces, declared).await;
        let client = reqwest::Client::new();
        let base = format!("http://{addr}");
        let reader = ApiReader {
            client: &client,
            base: &base,
            bearer: None,
            tokens: &[],
        };
        let result = reader
            .series_count("{__name__=~\"mb_cardinality\"}", &[])
            .await;
        server.abort();
        result
    }

    /// A 200,000-series answer delivered in eight pieces 20 ms apart counts
    /// 200,000. Each element carries braces, brackets, a comma and an
    /// escaped quote inside a string, and the cuts fall mid-element.
    ///
    /// TO SEE THIS FAIL: make `count_series_body` stop after the first
    /// `response.chunk()` (count only the first chunk): the first piece is
    /// an unterminated array, so the count is refused as not a series
    /// answer, and a count of whole elements in it would read about 25,000.
    #[tokio::test]
    async fn series_count_streams_a_200000_series_answer() {
        let body = series_answer(200_000);
        let pieces = pieces_of(&body, 8);
        assert_eq!(pieces.len(), 8);
        assert_eq!(stub_series_count(pieces, None).await, Ok(200_000));
    }

    /// A `status: error` answer is refused with its errorType and error,
    /// and so is a body cut short, whether the cut is inside the JSON or
    /// the connection closes before the declared length.
    #[tokio::test]
    async fn series_count_refuses_an_error_answer_and_a_truncated_body() {
        let error = br#"{"status":"error","errorType":"execution","error":"too many series"}"#;
        let refused = stub_series_count(vec![error.to_vec()], None)
            .await
            .expect_err("a status error answer is refused");
        assert!(
            refused.contains("non-success")
                && refused.contains("\"execution\"")
                && refused.contains("too many series"),
            "{refused}"
        );

        let body = series_answer(1_000);
        let cut = body[..body.len() / 2].to_vec();
        let refused = stub_series_count(pieces_of(&cut, 3), None)
            .await
            .expect_err("a body cut inside the JSON is refused");
        assert!(refused.contains("not a series answer"), "{refused}");

        let refused = stub_series_count(pieces_of(&cut, 3), Some(body.len()))
            .await
            .expect_err("a connection closed before the declared length is refused");
        assert!(refused.contains("/api/v1/series"), "{refused}");
    }

    /// The read-back of the portable Ravel row matches a receiver that
    /// stored what was sent, fences every query with every shard's token,
    /// and fails when the series count is off by one.
    ///
    /// TO SEE THIS FAIL: drop the `min_commit_token` loop in
    /// `ApiReader::get` (the fence assertion fails), or the
    /// `series_count` comparison in `readback_over_http` (the off-by-one
    /// read-back then matches).
    #[tokio::test]
    async fn readback_matches_what_was_stored_and_fails_one_series_off() {
        let points = class_mix(base_ts_ms());
        let (addr, server) = serve(write_stub(vec![
            Canned::ok(Some(ravel_stub_header(1))),
            Canned::ok(Some(ravel_stub_header(2))),
            Canned::ok(Some(ravel_stub_header(3))),
        ]))
        .await;
        let config = HttpReplayConfig {
            receiver: Receiver::Ravel,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size: 3,
            max_retries: 0,
            timeout_secs: 5,
            shards: 1,
            stall_ceiling: None,
        };
        let outcome = replay_over_http_with_plan(&config, &mut SlicePoints::new(&points))
            .await
            .expect("replay");
        server.abort();
        assert_eq!(
            outcome
                .result
                .commit_tokens_by_shard
                .as_ref()
                .map(BTreeMap::len),
            Some(3)
        );

        for (extra, matches) in [(0usize, true), (1, false)] {
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (addr, server) = serve(query_stub(extra, 0, Arc::clone(&seen))).await;
            let readback = readback_over_http(
                &ReadbackConfig {
                    query_url: format!("http://{addr}"),
                    bearer_token: None,
                    timeout_secs: 5,
                    settle_secs: 0,
                },
                &outcome.result,
                &outcome.plan,
            )
            .await
            .expect("read back");
            server.abort();
            assert_eq!(readback.matches_expected, matches, "{readback:?}");
            assert_eq!(readback.attempts, 1);
            assert_eq!(readback.series_count, 3 + extra as u64);
            assert_eq!(readback.sample_count, 3);
            assert_eq!(readback.histogram_series_count, Some(1));
            assert_eq!(readback.histogram_sample_count, Some(2));
            assert_eq!(readback.stale_marker_honored, Some(true));
            // One timing per query the stub saw (five, asserted below).
            assert_eq!(
                readback
                    .query_ms
                    .keys()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                [
                    READBACK_QUERY_HISTOGRAM_SAMPLES,
                    READBACK_QUERY_HISTOGRAM_SERIES,
                    "samples.0",
                    READBACK_QUERY_SERIES,
                    READBACK_QUERY_STALE_MARKER,
                ],
                "{readback:?}"
            );
            if !matches {
                assert_eq!(
                    readback.mismatches,
                    vec!["series_count: read 4, expected 3".to_string()]
                );
            }
            let seen = seen.lock().expect("lock").clone();
            assert_eq!(seen.len(), 5, "{seen:?}");
            for query in &seen {
                assert_eq!(query.matches("min_commit_token=").count(), 3, "{query}");
            }
            let matchers: Vec<String> = seen
                .iter()
                .flat_map(|raw| {
                    reqwest::Url::parse(&format!("http://stub/?{raw}"))
                        .expect("query parses")
                        .query_pairs()
                        .filter(|(k, _)| k == "match[]")
                        .map(|(_, v)| v.into_owned())
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(
                matchers,
                vec![
                    r#"{__name__=~"mb_gauge|mb_native"}"#.to_string(),
                    r#"{__name__=~"mb_native"}"#.to_string(),
                ],
                "the cardinality read names only the metrics sent"
            );
        }
    }

    /// A receiver that acknowledges writes before it makes them queryable
    /// reads short at first. The read-back repeats until a read matches
    /// inside the settle window; a receiver that never catches up is
    /// reported short once the window has passed; with no window the first
    /// read is the one reported.
    ///
    /// TO SEE THIS FAIL: return after the first read in
    /// `readback_over_http` (the lagging receiver reports a mismatch on
    /// attempt 1), drop its `elapsed >= settle` exit (the receiver that
    /// never catches up runs into the 20 s timeout), or ignore
    /// `settle_secs` in favour of a fixed window (the zero-window case
    /// reads more than once).
    #[tokio::test]
    async fn readback_settles_on_a_lagging_receiver_within_the_window_only() {
        let points = class_mix(base_ts_ms());
        let (addr, server) = serve(write_stub(vec![
            Canned::ok(Some(ravel_stub_header(1))),
            Canned::ok(Some(ravel_stub_header(2))),
            Canned::ok(Some(ravel_stub_header(3))),
        ]))
        .await;
        let config = HttpReplayConfig {
            receiver: Receiver::Ravel,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size: 3,
            max_retries: 0,
            timeout_secs: 5,
            shards: 1,
            stall_ceiling: None,
        };
        let outcome = replay_over_http_with_plan(&config, &mut SlicePoints::new(&points))
            .await
            .expect("replay");
        server.abort();

        // One read-back is five requests, so `hidden` 5 hides exactly the
        // first read.
        for (hidden, settle_secs) in [(5usize, 30u64), (usize::MAX, 1), (5, 0)] {
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (addr, server) = serve(query_stub(0, hidden, Arc::clone(&seen))).await;
            let readback = tokio::time::timeout(
                Duration::from_secs(20),
                readback_over_http(
                    &ReadbackConfig {
                        query_url: format!("http://{addr}"),
                        bearer_token: None,
                        timeout_secs: 5,
                        settle_secs,
                    },
                    &outcome.result,
                    &outcome.plan,
                ),
            )
            .await
            .expect("the settle window bounds the read-back")
            .expect("read back");
            server.abort();
            let requests = seen.lock().expect("lock").len();
            match (hidden, settle_secs) {
                (5, 30) => {
                    assert!(readback.matches_expected, "{readback:?}");
                    assert_eq!(readback.attempts, 2);
                    assert_eq!(requests, 10);
                    assert!(readback.settled_after_ms >= 500, "{readback:?}");
                    assert!(readback.settled_after_ms < 30_000, "{readback:?}");
                }
                (_, 1) => {
                    assert!(!readback.matches_expected, "{readback:?}");
                    assert!(readback.attempts >= 2, "{readback:?}");
                    assert_eq!(requests, 5 * readback.attempts as usize);
                    assert!(readback.settled_after_ms >= 1_000, "{readback:?}");
                    assert_eq!(
                        readback.mismatches.first().map(String::as_str),
                        Some("series_count: read 0, expected 3")
                    );
                }
                _ => {
                    assert!(!readback.matches_expected, "{readback:?}");
                    assert_eq!(readback.attempts, 1);
                    assert_eq!(requests, 5);
                }
            }
        }
    }

    /// A native histogram with a non-finite `sum` is refused, as a sample with
    /// a non-finite value is.
    #[test]
    fn a_histogram_with_a_non_finite_sum_is_refused() {
        for sum in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut h = histogram(base_ts_ms(), vec![1, 1]);
            h.sum = sum;
            assert_eq!(
                h.validate(),
                Err(RejectionReason::InvalidHistogram),
                "{sum}"
            );
        }
        assert_eq!(histogram(base_ts_ms(), vec![1, 1]).validate(), Ok(()));
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

        // `anchor_source` is the field #3298 removed: a run block written
        // before it carries the key, and must still load.
        let run: RunRecord = serde_json::from_str(
            r#"{"steps": 5, "total_series_created": 1000, "logical_input_bytes": 4096,
                "total_samples_generated": 5000, "scaling_label_cardinality": 1000,
                "anchor_source": "clock"}"#,
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

    /// The series and timestamp of every sample in one RW1.0 request body.
    fn decode_series_ts(body: &[u8]) -> Vec<(String, i64)> {
        let raw = snap::raw::Decoder::new()
            .decompress_vec(body)
            .expect("snappy");
        let req = WriteRequest::decode(raw.as_slice()).expect("prost decode");
        req.timeseries
            .iter()
            .flat_map(|ts| {
                let instance = ts
                    .labels
                    .iter()
                    .find(|l| l.name == "instance")
                    .map(|l| l.value.clone())
                    .unwrap_or_default();
                ts.samples
                    .iter()
                    .map(move |s| (instance.clone(), s.timestamp))
            })
            .collect()
    }

    /// The shard each of `valid_stream`'s first `series` series routes to.
    fn stream_shards(series: usize, shards: usize) -> Vec<usize> {
        valid_stream(series, series)
            .into_iter()
            .map(|s| series_shard(&LogicalPoint::Sample(s), shards))
            .collect()
    }

    /// Serve `app` on an ephemeral port.
    async fn serve_stub(app: axum::Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, server)
    }

    /// With several shards in flight, every series still reaches the
    /// receiver in offered order. The stub holds the first point of series
    /// `mb-instance-0` until either a later point of that series arrives or
    /// the other series' points the producer can send meanwhile have been
    /// processed: series 0's shard takes one request in flight and
    /// [`SHARD_QUEUE_BATCHES`] queued, after which the producer waits, so
    /// each other series sends that many too. Routed by series, the later
    /// point is queued behind the held one and cannot arrive; routed
    /// round-robin by batch (each batch to the next shard), it is sent on
    /// another shard while the first is held, is processed first, and the
    /// order assertion fails.
    #[tokio::test]
    async fn sharded_replay_keeps_every_series_in_offered_order() {
        const SERIES: usize = 3;
        const POINTS: usize = 30;
        // At least 4 shards, so round-robin puts point 3 (series 0 again) on
        // a different shard from point 0; and the series on distinct shards,
        // so the other two can finish while series 0's first point is held.
        let shards = (4..=16)
            .find(|&n| {
                stream_shards(SERIES, n)
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    == SERIES
            })
            .expect("a shard count in 4..=16 that routes the 3 series apart");

        let samples = valid_stream(POINTS, SERIES);
        let held_ts = samples[0].ts_ms;
        let others = (SERIES - 1) * (1 + SHARD_QUEUE_BATCHES);
        let processed: Arc<std::sync::Mutex<Vec<(String, i64)>>> = Arc::default();
        let changed = Arc::new(tokio::sync::Notify::new());
        let (log, notify) = (Arc::clone(&processed), Arc::clone(&changed));
        let app = axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(move |body: axum::body::Bytes| {
                let (log, notify) = (Arc::clone(&log), Arc::clone(&notify));
                async move {
                    let points = decode_series_ts(&body);
                    if points.iter().any(|(_, ts)| *ts == held_ts) {
                        let release = || {
                            let seen = log.lock().expect("lock");
                            seen.iter().filter(|(s, _)| s != "mb-instance-0").count() >= others
                                || seen.iter().any(|(s, _)| s == "mb-instance-0")
                        };
                        // A backstop only: a correct sender releases the hold
                        // through `release` well before it.
                        let _ = tokio::time::timeout(Duration::from_secs(10), async {
                            loop {
                                let woken = notify.notified();
                                if release() {
                                    break;
                                }
                                woken.await;
                            }
                        })
                        .await;
                    }
                    log.lock().expect("lock").extend(points);
                    notify.notify_waiters();
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        );
        let (addr, server) = serve_stub(app).await;
        let config = HttpReplayConfig {
            receiver: Receiver::Prometheus,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size: 1,
            max_retries: 0,
            timeout_secs: 30,
            shards,
            stall_ceiling: None,
        };
        let row = replay_over_http(&config, &mut SliceSamples::new(&samples))
            .await
            .expect("replay");
        server.abort();

        assert_eq!(row.ingest.accepted_samples, POINTS as u64);
        assert_eq!(row.ingest.batches, POINTS as u64);
        let processed = processed.lock().expect("lock").clone();
        assert_eq!(processed.len(), POINTS);
        let mut by_series: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        for (series, ts) in processed {
            by_series.entry(series).or_default().push(ts);
        }
        for (series, arrived) in &by_series {
            let mut offered = arrived.clone();
            offered.sort_unstable();
            assert_eq!(
                arrived, &offered,
                "{series} reached the receiver out of order"
            );
        }
    }

    /// At most one request per shard is in flight, and every shard with work
    /// gets a request in flight at once: the peak is `min(shards, batches)`.
    /// The stub holds each request until that many are in flight (a
    /// backstop timeout aside), so the sequential sender this replaced, one
    /// request at a time, peaks at 1 and fails the first assertion.
    #[tokio::test]
    async fn requests_in_flight_peak_at_min_of_shards_and_batches() {
        for (series, shards, points) in [(2usize, 2usize, 8usize), (2, 8, 2)] {
            let routed = stream_shards(series, shards);
            assert!(
                routed[0] != routed[1],
                "precondition: the two series route to distinct shards, got {routed:?}"
            );
            let expected = shards.min(points);
            let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let changed = Arc::new(tokio::sync::Notify::new());
            let (now, max, notify) = (
                Arc::clone(&in_flight),
                Arc::clone(&peak),
                Arc::clone(&changed),
            );
            let app = axum::Router::new().route(
                "/api/v1/write",
                axum::routing::post(move || {
                    let (now, max, notify) =
                        (Arc::clone(&now), Arc::clone(&max), Arc::clone(&notify));
                    async move {
                        use std::sync::atomic::Ordering;
                        let current = now.fetch_add(1, Ordering::SeqCst) + 1;
                        max.fetch_max(current, Ordering::SeqCst);
                        notify.notify_waiters();
                        let _ = tokio::time::timeout(Duration::from_secs(10), async {
                            loop {
                                let woken = notify.notified();
                                if max.load(Ordering::SeqCst) >= expected {
                                    break;
                                }
                                woken.await;
                            }
                        })
                        .await;
                        now.fetch_sub(1, Ordering::SeqCst);
                        axum::http::StatusCode::NO_CONTENT
                    }
                }),
            );
            let (addr, server) = serve_stub(app).await;
            let samples = valid_stream(points, series);
            let config = HttpReplayConfig {
                receiver: Receiver::Prometheus,
                endpoint: format!("http://{addr}/api/v1/write"),
                bearer_token: None,
                tenant: None,
                batch_size: 1,
                max_retries: 0,
                timeout_secs: 30,
                shards,
                stall_ceiling: None,
            };
            let row = replay_over_http(&config, &mut SliceSamples::new(&samples))
                .await
                .expect("replay");
            server.abort();
            assert_eq!(row.ingest.batches, points as u64);
            assert_eq!(
                peak.load(std::sync::atomic::Ordering::SeqCst),
                expected,
                "{shards} shards, {points} batches"
            );
        }
    }

    /// A shard sends its partial batch at stream end: 5 points of one series
    /// at batch size 2 are 3 requests, the last holding one point. The row is
    /// on the wall basis and its `client_behavior` discloses the shard model
    /// and the backoff.
    #[tokio::test]
    async fn a_partial_batch_is_flushed_at_stream_end() {
        let sizes: Arc<std::sync::Mutex<Vec<usize>>> = Arc::default();
        let log = Arc::clone(&sizes);
        let app = axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(move |body: axum::body::Bytes| {
                let log = Arc::clone(&log);
                async move {
                    log.lock()
                        .expect("lock")
                        .push(decode_series_ts(&body).len());
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        );
        let (addr, server) = serve_stub(app).await;
        let samples = valid_stream(5, 1);
        let config = HttpReplayConfig {
            receiver: Receiver::Prometheus,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size: 2,
            max_retries: 0,
            timeout_secs: 30,
            shards: 4,
            stall_ceiling: None,
        };
        let row = replay_over_http(&config, &mut SliceSamples::new(&samples))
            .await
            .expect("replay");
        server.abort();
        assert_eq!(*sizes.lock().expect("lock"), vec![2, 2, 1]);
        assert_eq!(row.ingest.accepted_samples, 5);
        assert_eq!(row.ingest.elapsed_basis, ELAPSED_BASIS_WALL_IN_FLIGHT);
        for disclosed in [
            "4 shards fixed for the whole replay (no resharding",
            "BLAKE3 hash of its sorted labels",
            "one request in flight per shard",
            "at 2 logical points",
            "backoff from 30 ms doubling to 5000 ms",
            "429 is retried although Prometheus' default retry_on_http_429 is false",
        ] {
            assert!(
                row.client_behavior.contains(disclosed),
                "client_behavior lacks `{disclosed}`: {}",
                row.client_behavior
            );
        }
    }

    /// Class digests are a multiset: the same points in another order give
    /// the same digest, and a point counted twice changes it. An ordered
    /// hasher fails the first assertion; an XOR of point hashes fails the
    /// last two, where a duplicated pair cancels out.
    #[test]
    fn class_digests_ignore_order_and_count_duplicates() {
        let points: Vec<LogicalPoint> = valid_stream(4, 2)
            .into_iter()
            .map(LogicalPoint::Sample)
            .collect();
        let digest = |p: &[&LogicalPoint]| {
            let mut d = ClassDigester::default();
            p.iter().for_each(|x| d.update(x));
            d.finish().scalar
        };
        let [a, b, c, d] = [&points[0], &points[1], &points[2], &points[3]];
        assert_eq!(digest(&[a, b, c, d]), digest(&[d, b, a, c]), "order");
        assert_ne!(digest(&[a, b]), digest(&[a, b, b]), "a duplicate");
        assert_ne!(digest(&[a]), digest(&[a, b, b]), "a duplicated pair");
        assert_ne!(digest(&[]), digest(&[a, a]), "a point counted twice");
        assert_eq!(digest(&[]), "0".repeat(64));

        let mut single = OfferedDigest::new();
        single.update_point(a);
        assert_eq!(digest(&[a]), single.finish(), "one point is its own hash");
    }

    /// The modular sum carries across limbs and wraps at 2^256.
    #[test]
    fn multiset_digest_carries_and_wraps_mod_2_256() {
        let mut d = MultisetDigest {
            limbs: [u64::MAX, u64::MAX, u64::MAX, u64::MAX],
        };
        let point = LogicalPoint::Sample(valid_stream(1, 1).remove(0));
        let mut alone = MultisetDigest::new();
        alone.add_point(&point);
        d.add_point(&point);
        // All-ones is -1 mod 2^256, so the sum is the point's hash minus one.
        let mut expected = alone.limbs;
        for limb in expected.iter_mut() {
            let (v, borrow) = limb.overflowing_sub(1);
            *limb = v;
            if !borrow {
                break;
            }
        }
        assert_eq!(d.limbs, expected);
    }

    /// A commit token for `shard` at `(epoch, seq)` from `writer`.
    fn token_of(shard: u32, writer: u128, epoch: u64, seq: u64) -> String {
        CommitToken {
            shard,
            writer_id: uuid::Uuid::from_u128(writer),
            epoch,
            seq,
            ingest_hour_bucket: 488_888,
        }
        .encode()
    }

    /// Responses from several shards arrive in any order, so an older token
    /// arriving last does not replace a newer one: seq 7 then seq 5 keeps 7,
    /// and a higher epoch wins over a higher seq. Overwriting with the latest
    /// arrival, the rule this replaced, keeps 5.
    #[test]
    fn commit_tokens_keep_the_newest_epoch_and_seq_per_shard() {
        let mut by_shard = BTreeMap::new();
        record_commit_tokens("ravel", &token_of(0, 1, 1, 7), &mut by_shard).expect("seq 7");
        record_commit_tokens("ravel", &token_of(0, 1, 1, 5), &mut by_shard).expect("seq 5");
        assert_eq!(by_shard.get(&0), Some(&token_of(0, 1, 1, 7)));
        record_commit_tokens("ravel", &token_of(0, 1, 2, 1), &mut by_shard).expect("epoch 2");
        assert_eq!(by_shard.get(&0), Some(&token_of(0, 1, 2, 1)));
        record_commit_tokens("ravel", &token_of(0, 1, 1, 9), &mut by_shard).expect("old epoch");
        assert_eq!(by_shard.get(&0), Some(&token_of(0, 1, 2, 1)));
    }

    /// Two writers on one shard are refused, naming the system, the shard
    /// and both writers.
    #[test]
    fn a_second_writer_on_one_shard_is_refused() {
        let mut by_shard = BTreeMap::new();
        record_commit_tokens("ravel", &token_of(3, 1, 1, 1), &mut by_shard).expect("first");
        let err = record_commit_tokens("ravel", &token_of(3, 2, 1, 2), &mut by_shard)
            .expect_err("a second writer");
        let (held, new) = (
            uuid::Uuid::from_u128(1).to_string(),
            uuid::Uuid::from_u128(2).to_string(),
        );
        assert!(
            matches!(
                &err,
                LaneError::CommitTokenWriterConflict { system, shard: 3, held_writer, new_writer }
                    if system == "ravel" && *held_writer == held && *new_writer == new
            ),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains(&held) && message.contains(&new),
            "{message}"
        );
        assert_eq!(by_shard.get(&3), Some(&token_of(3, 1, 1, 1)));
    }

    /// 30 ms doubling per retry, capped at 5 s, and no overflow however many
    /// retries.
    #[test]
    fn retry_delay_doubles_from_30ms_and_caps_at_5s() {
        let ms: Vec<u128> = (0..10).map(|r| retry_delay(r).as_millis()).collect();
        assert_eq!(ms, vec![30, 60, 120, 240, 480, 960, 1920, 3840, 5000, 5000]);
        assert_eq!(retry_delay(63), Duration::from_secs(5));
        assert_eq!(retry_delay(64), Duration::from_secs(5));
        assert_eq!(retry_delay(u32::MAX), Duration::from_secs(5));
    }

    /// The projection is elapsed time scaled by stream points over offered
    /// points, and nothing is projected before a point is offered.
    #[test]
    fn stall_projection_scales_elapsed_by_the_stream_left() {
        assert_eq!(project_ingest_secs(60.0, 0, 1_000), None);
        assert_eq!(project_ingest_secs(60.0, 250, 1_000), Some(240.0));
        assert_eq!(project_ingest_secs(60.0, 1_000, 1_000), Some(60.0));

        let ceiling = StallCeiling {
            max_ingest_secs: 600,
            stream_points: 1_000,
            grace: STALL_GRACE,
            recheck: STALL_RECHECK,
        };
        assert_eq!(
            stall_projection_over(&ceiling, 60.0, 100),
            None,
            "exactly 600 s"
        );
        assert_eq!(stall_projection_over(&ceiling, 60.0, 50), Some(1_200.0));
        assert_eq!(stall_projection_over(&ceiling, 60.0, 0), None);
    }

    /// The first check runs at 60 s of sending, later ones at most every 10 s.
    #[test]
    fn stall_checks_wait_60s_then_recur_every_10s() {
        let s = Duration::from_secs;
        let ceiling = StallCeiling {
            max_ingest_secs: 600,
            stream_points: 1_000,
            grace: STALL_GRACE,
            recheck: STALL_RECHECK,
        };
        assert!(!stall_check_due(&ceiling, s(59), None));
        assert!(stall_check_due(&ceiling, s(60), None));
        assert!(!stall_check_due(&ceiling, s(65), Some(s(5))));
        assert!(stall_check_due(&ceiling, s(70), Some(s(10))));
    }

    /// A stream smaller than the shard buffers is offered whole before the
    /// first response, so only the drain can stop it: with every response
    /// delayed and a ceiling the run cannot meet, the replay stops with
    /// `IngestStalled` after offering the whole stream instead of waiting the
    /// stream out.
    ///
    /// TO SEE THIS FAIL: in `replay_over_http_with_plan`, delete the
    /// `check_stall` call in the drain loop (the replay then returns `Ok`
    /// once the delayed response arrives), or the `sending_since` line in
    /// the final flush (no check ever runs, since the only batch is that
    /// flush).
    #[tokio::test]
    async fn the_drain_stops_a_row_whose_whole_stream_is_already_offered() {
        let points = class_mix(base_ts_ms());
        let app = axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(|| async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                axum::http::StatusCode::NO_CONTENT
            }),
        );
        let (addr, server) = serve(app).await;
        let config = HttpReplayConfig {
            receiver: Receiver::Prometheus,
            endpoint: format!("http://{addr}/api/v1/write"),
            bearer_token: None,
            tenant: None,
            batch_size: 100,
            max_retries: 0,
            timeout_secs: 5,
            shards: 1,
            stall_ceiling: Some(StallCeiling {
                max_ingest_secs: 0,
                stream_points: points.len() as u64,
                grace: Duration::ZERO,
                recheck: Duration::from_millis(20),
            }),
        };
        let outcome = replay_over_http_with_plan(&config, &mut SlicePoints::new(&points)).await;
        server.abort();
        match outcome {
            Err(LaneError::IngestStalled {
                offered_points,
                stream_points,
                ceiling_secs,
                ..
            }) => {
                assert_eq!(offered_points, points.len() as u64);
                assert_eq!(stream_points, points.len() as u64);
                assert_eq!(ceiling_secs, 0);
            }
            Err(other) => panic!("expected IngestStalled, got {other}"),
            Ok(_) => panic!("expected IngestStalled, the replay ran to its end"),
        }
    }

    /// A producer round adds its routing (plan observation, the shard
    /// function, the buffer push) to `generation`, not only the pull from
    /// the stream: with a clock that only the shard function advances, one
    /// second per point, `generation` is one second per routed point.
    ///
    /// TO SEE THIS FAIL: in `Producer::round`, add to `generation` right
    /// after `fill_valid` instead of after the routing loop (`generation` is
    /// then zero).
    #[test]
    fn a_producer_round_counts_routing_as_generation() {
        use std::cell::Cell;
        let points = class_mix(base_ts_ms());
        let clock = Cell::new(Duration::ZERO);
        let routed = Cell::new(0u64);
        let mut producer = Producer::new(Receiver::Prometheus, 1, 100);
        let mut source = SlicePoints::new(&points);
        let shard_of = |_: &LogicalPoint| {
            clock.set(clock.get() + Duration::from_secs(1));
            routed.set(routed.get() + 1);
            0
        };
        let full = producer
            .round(&mut source, shard_of, || clock.get())
            .expect("round")
            .expect("the stream has points");
        assert!(full.is_empty(), "9 points never fill a 100-point batch");
        let valid = producer.offer.samples + producer.offer.histograms
            - producer.offer.rejected_samples
            - producer.offer.rejected_histograms;
        assert!(valid > 0);
        assert_eq!(routed.get(), valid);
        assert_eq!(producer.buffers[0].len() as u64, valid);
        // hygiene-allow: wall-clock -- `generation` is read on the fake clock above, not the wall
        assert_eq!(producer.generation, Duration::from_secs(valid));
        assert!(
            producer
                .round(&mut source, |_| 0, || clock.get())
                .expect("round")
                .is_none()
        );
    }

    /// A row is sender bound only on the wall basis, with generation at
    /// least 90 % of its wall time.
    #[test]
    fn sender_bound_needs_wall_basis_and_ninety_percent_generation() {
        assert!(is_sender_bound(ELAPSED_BASIS_WALL_IN_FLIGHT, 9.0, 10.0));
        assert!(!is_sender_bound(ELAPSED_BASIS_WALL_IN_FLIGHT, 8.9, 10.0));
        assert!(!is_sender_bound(ELAPSED_BASIS_TIMED_SEGMENTS, 9.5, 10.0));
        assert!(!is_sender_bound(ELAPSED_BASIS_WALL_IN_FLIGHT, 0.0, 0.0));
    }
}

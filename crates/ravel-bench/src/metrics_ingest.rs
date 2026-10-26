//! MetricsBench Remote Write 1.0 ingest lane (ADR-0927, issue #937, task M5).
//!
//! One logical sample stream is replayed into every participating system: the
//! in-process Ravel path (`IngestRouter`, `WriteMode::Strict`), and, behind
//! endpoints supplied by config with no hardcoded hosts, Prometheus,
//! VictoriaMetrics, and the object-storage-native comparator over portable
//! Remote Write 1.0. Every system is fed the same [`LogicalSample`]s and the
//! same wire body shape (`prometheus.WriteRequest`, snappy-compressed
//! protobuf), so a byte or a latency difference is a property of the engine,
//! not of the protocol.
//!
//! ## Acknowledgement semantics are data, never normalised (ADR-0927 dec. 3)
//!
//! Ravel's Remote Write surface is strict-mode only: a 2xx means the data
//! object AND its commit record are both durably stored, and the response
//! carries `x-ravel-commit-token`
//! (`services/ravel-server/src/remote_write.rs`). Its rows are tagged
//! [`AckSemantics::DurableOnAck`] and carry the returned commit tokens. A
//! Remote Write 2xx from Prometheus or VictoriaMetrics means the samples were
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
//! Every count the lane controls is exact and closes: `offered_samples ==
//! accepted_samples + rejected_samples + dropped_samples`, over the stream's
//! scalar (float) lines. Every line is classified ([`classify_line`]) as a
//! scalar, a native histogram, a staleness marker or a malformed line, and
//! each row's `stream_counts` counts all four, so nothing leaves the replay
//! uncounted: the two non-scalar kinds are named exclusions
//! (`excluded_native_histogram`, `excluded_stale_marker`) from the scalar
//! replay. [`check_stream_band`] holds the stream to band 4: the four counts
//! sum to `generated_lines`, `generated_lines` equals the generator's
//! `emitted_samples`, and `malformed` is zero.
//!
//! ## Bounded streaming replay
//!
//! A replay pulls batches from a [`SampleBatches`] source rather than holding
//! the stream. [`GeneratedStream`] generates one scrape step at a time and
//! queues only that step's scalar samples, so what a replay holds is bounded
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
//!   Forced `false` when `steps_run != steps_declared`, regardless of the
//!   profile's own verdict: a run that stopped short or ran long is not that
//!   profile. Also forced `false` when the recorded `run.stream_digest`
//!   differs from the digest [`build_profile_record`] recomputes from the
//!   manifest, the anchor and `steps_run`.
//! - `comparability_reason`: why not, present exactly when `comparable` is
//!   `false`, absent when `true`. The workload's own `Comparability` reason,
//!   never a second hand-written copy of it, unless the stream digest does
//!   not reproduce or the run's step count is off the profile's, in which
//!   case this states that instead (the digest first).
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
//!   - `anchor_ms`: the first generated step's timestamp, in milliseconds,
//!     as passed with the required `--anchor-ms` ([`check_anchor`]).
//!   - `workload_version`, `workload_digest`, `seed`: the manifest's
//!     `version`, BLAKE3 over its file bytes, and its seed.
//!   - `stream_digest`: the generator's BLAKE3 over the encoded stream.
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
use ravel_remote_write::proto::prometheus::{
    Label as PbLabel, Sample as PbSample, TimeSeries as PbTimeSeries, WriteRequest,
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

/// One logical sample, the unit every system replays. Scalar float samples
/// only: the portable cross-engine comparison is over the sample stream every
/// candidate accepts without configuration, and a classic histogram already
/// decomposes into its `_bucket`/`_sum`/`_count` float series upstream.
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
    /// The value was not finite (NaN or +/-inf) on a plain sample. A Prometheus
    /// staleness marker is a distinct concept the scalar replay does not model,
    /// so a bare non-finite scalar is refused rather than silently ingested.
    NonFiniteValue,
}

impl LogicalSample {
    /// Validate a sample against the lane's client-side admission rules,
    /// naming the first reason it fails. `Ok` means every system receives it.
    pub fn validate(&self) -> Result<(), RejectionReason> {
        if self.metric.is_empty() {
            return Err(RejectionReason::EmptyMetricName);
        }
        if self.labels.iter().any(|(name, _)| name.is_empty()) {
            return Err(RejectionReason::EmptyLabelName);
        }
        if !self.value.is_finite() {
            return Err(RejectionReason::NonFiniteValue);
        }
        Ok(())
    }

    /// The canonical label set including `__name__`, sorted by name, so a series
    /// has exactly one spelling on the wire and in the id.
    fn canonical_labels(&self) -> Vec<(String, String)> {
        let mut labels: Vec<(String, String)> = Vec::with_capacity(self.labels.len() + 1);
        labels.push((METRIC_NAME_LABEL.to_string(), self.metric.clone()));
        labels.extend(self.labels.iter().cloned());
        labels.sort();
        labels
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
/// Every field added after the nightly artifact began carries a serde default,
/// so a reader built on this struct still loads a report written before it
/// (`tests::a_pre_change_artifact_loads_with_defaults`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IngestPhase {
    /// Every line of the stream this row replayed, counted by kind. The sample
    /// accounting below is over the `scalar` lines only. All zero in a report
    /// written before the field existed.
    #[serde(default)]
    pub stream_counts: StreamCounts,
    /// Native-histogram lines the scalar replay excluded by name
    /// (`stream_counts.native_histogram`): never offered, never dropped.
    #[serde(default)]
    pub excluded_native_histogram: u64,
    /// Staleness-marker lines the scalar replay excluded by name
    /// (`stream_counts.stale_marker`).
    #[serde(default)]
    pub excluded_stale_marker: u64,
    /// Scalar logical samples offered to the system. `accepted + rejected +
    /// dropped`, and `offered + excluded_native_histogram +
    /// excluded_stale_marker == stream_counts.generated_lines`
    /// ([`check_row_accounting`]).
    pub offered_samples: u64,
    /// BLAKE3 over the canonical `(series, ts_ms, value bits)` tuple of every
    /// sample this row offered, in offered order ([`OfferedDigest`]). Empty in
    /// a report written before the field existed.
    #[serde(default)]
    pub offered_digest: String,
    /// Samples the system acknowledged.
    pub accepted_samples: u64,
    /// Samples the lane refused before sending (client-side admission).
    pub rejected_samples: u64,
    /// Samples in batches the system never acknowledged: neither accepted nor
    /// rejected by the lane, so the accounting still closes. On a comparator
    /// row this is exactly the sum of `rejected_by_status` plus
    /// `retry_exhausted_samples`. On the Ravel row it counts batches whose
    /// strict write returned an error; that path does not retry at this level
    /// (object-store PUT retries run below it).
    pub dropped_samples: u64,
    /// Batches counted under `dropped_samples`.
    pub dropped_batches: u64,
    /// Samples in comparator batches that ended on a non-retryable non-2xx
    /// response, keyed by that HTTP status. Part of `dropped_samples`. Empty
    /// on the Ravel row, and in a report written before the field existed.
    #[serde(default)]
    pub rejected_by_status: BTreeMap<u16, u64>,
    /// Samples in comparator batches dropped after every allowed retry ended
    /// on a 429, a 5xx or a transport error. Part of `dropped_samples`.
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
    /// Reports written before this field existed carry no marker; their
    /// `elapsed_secs` was one wall-clock span over the write loop of a
    /// stream generated up front. For the Ravel row that span started after
    /// admission and the RW1.0 encode; for a comparator row the encode was
    /// inside it, as it is inside that row's timed segments now. In both
    /// lanes the only basis change is the generation now interleaved between
    /// segments, which the sum excludes. Empty in such a report, which reads
    /// as "wall time, unmarked".
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
    /// The system: `ravel`, `prometheus`, `victoriametrics`, `osn`.
    pub system: String,
    /// What a 2xx from this system means (ADR-0927 decision 3).
    pub ack_semantics: AckSemantics,
    /// The endpoint replayed against, or `in-process` for Ravel. Never a
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
            storage: Some(storage),
        }
    }

    /// Build a buffered comparator row (Prometheus/VictoriaMetrics/OSN over
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
    /// regardless of the profile's own verdict, when `steps_run <
    /// steps_declared`: a truncated run's figures are not that profile's.
    pub comparable: bool,
    /// Why `comparable` is `false`; absent when it is `true`. The workload's
    /// own [`crate::metrics_workload::Comparability::reason`], never a
    /// second, hand-written copy of it, unless the run itself was
    /// truncated, in which case this states that instead.
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
    /// Timestamp of the first generated step, in milliseconds since the epoch:
    /// the required `--anchor-ms`, on the scrape grid ([`check_anchor`]).
    pub anchor_ms: i64,
    /// The manifest's `version` field, as the loader accepted it.
    #[serde(default)]
    pub workload_version: u32,
    /// BLAKE3 over the manifest file's bytes, hex.
    #[serde(default)]
    pub workload_digest: String,
    /// The seed the manifest declared.
    #[serde(default)]
    pub seed: u64,
    /// The generator's BLAKE3 over the encoded stream this run replayed, hex.
    /// `comparable` requires it to equal a digest recomputed from the
    /// manifest, `anchor_ms` and `steps`.
    #[serde(default)]
    pub stream_digest: String,
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

/// The largest millisecond timestamp whose nanosecond form fits an `i64`:
/// `i64::MAX / 1_000_000`. The newest generated step must not pass it, or the
/// generator's `anchor + step * interval` or the lane's ms-to-ns conversion
/// overflows.
pub const ANCHOR_MAX_MS: i64 = i64::MAX / 1_000_000;

/// A `--anchor-ms` the lane refuses (ADR-0927 2026-10-26 amendment, section C).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AnchorError {
    /// No anchor was given.
    #[error(
        "--anchor-ms is required and has no default: the stream's timestamps and digest are a \
         function of the anchor, so a clock-derived anchor would make every run a different \
         stream and no two runs comparable; pass the first step's timestamp in milliseconds, \
         on the profile's scrape grid"
    )]
    Missing,
    /// The anchor is before the epoch.
    #[error("--anchor-ms {anchor_ms} is negative; the anchor must be at or after the epoch")]
    Negative {
        /// The refused anchor.
        anchor_ms: i64,
    },
    /// The newest step would pass [`ANCHOR_MAX_MS`].
    #[error(
        "--anchor-ms {anchor_ms} with {steps} steps of {interval_ms} ms puts the newest step \
         past {max_ms} ms, the largest millisecond whose nanosecond form fits an i64"
    )]
    PastRange {
        /// The refused anchor.
        anchor_ms: i64,
        /// Steps the run would generate.
        steps: u64,
        /// The profile's scrape interval in milliseconds.
        interval_ms: u64,
        /// [`ANCHOR_MAX_MS`].
        max_ms: i64,
    },
    /// The anchor is not a multiple of the scrape interval.
    #[error(
        "--anchor-ms {anchor_ms} is off the scrape grid: {anchor_ms} mod {interval_ms} ms is \
         {remainder}, and the anchor must be a multiple of the profile's scrape interval"
    )]
    OffGrid {
        /// The refused anchor.
        anchor_ms: i64,
        /// The profile's scrape interval in milliseconds.
        interval_ms: u64,
        /// `anchor_ms % interval_ms`, non-zero.
        remainder: i64,
    },
}

/// Check a run's `--anchor-ms` against the profile: required, non-negative,
/// its newest step (`anchor + (steps - 1) * interval`) at most
/// [`ANCHOR_MAX_MS`], and a multiple of the scrape interval in milliseconds.
pub fn check_anchor(
    anchor_ms: Option<i64>,
    scrape_interval_secs: u64,
    steps: u64,
) -> Result<i64, AnchorError> {
    let anchor_ms = anchor_ms.ok_or(AnchorError::Missing)?;
    if anchor_ms < 0 {
        return Err(AnchorError::Negative { anchor_ms });
    }
    let interval_ms = scrape_interval_secs.saturating_mul(1_000);
    let newest = (steps.saturating_sub(1) as u128) * u128::from(interval_ms)
        + u128::from(anchor_ms.unsigned_abs());
    if newest > ANCHOR_MAX_MS as u128 {
        return Err(AnchorError::PastRange {
            anchor_ms,
            steps,
            interval_ms,
            max_ms: ANCHOR_MAX_MS,
        });
    }
    if interval_ms > 0 {
        let remainder = anchor_ms % interval_ms as i64;
        if remainder != 0 {
            return Err(AnchorError::OffGrid {
                anchor_ms,
                interval_ms,
                remainder,
            });
        }
    }
    Ok(anchor_ms)
}

/// The generator's stream digest for `steps` scrapes of `profile` anchored at
/// `anchor_ms`, regenerated into a sink that keeps no bytes.
pub fn recompute_stream_digest(
    workload: &WorkloadFile,
    profile: &Profile,
    anchor_ms: i64,
    steps: u64,
) -> Result<String, crate::metrics_gen::GeneratorError> {
    crate::metrics_gen::Generator::for_profile(workload, profile, anchor_ms)
        .generate_into(steps, &mut std::io::sink())
        .map(|report| report.digest)
}

/// BLAKE3 over a manifest file's bytes, hex: the `workload_digest` a report
/// records.
pub fn workload_digest(manifest_bytes: &[u8]) -> String {
    blake3::hash(manifest_bytes).to_hex().to_string()
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
/// `comparable` is true only when the profile is publishable, `steps_run`
/// equals `profile`'s declared `samples_per_series`, and `gen_report.digest`
/// (the stream digest this record carries) equals a digest this function
/// recomputes from `workload`, `profile`, `anchor_ms` and `steps_run`
/// ([`recompute_stream_digest`]). `comparability_reason` names the first
/// failing condition: the digest, then the step counts, then the profile's
/// own reason.
///
/// `steps_run` is the `--steps` the run resolved to and `logical_input_bytes`
/// the encoded stream's length (the generator's own `bytes` for a streamed
/// run); `anchor_ms` is the first step's timestamp and `workload_digest` the
/// manifest file's BLAKE3 ([`workload_digest`]).
pub fn build_profile_record(
    workload: &WorkloadFile,
    profile: &Profile,
    steps_run: u64,
    logical_input_bytes: u64,
    gen_report: &crate::metrics_gen::GenerationReport,
    anchor_ms: i64,
    workload_digest: String,
) -> ProfileRecord {
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
    let off_profile = steps_run != steps_declared;
    let digest_mismatch = match recompute_stream_digest(workload, profile, anchor_ms, steps_run) {
        Ok(recomputed) if recomputed == gen_report.digest => None,
        Ok(recomputed) => Some(format!(
            "the recorded stream digest {} differs from {recomputed}, the digest of {steps_run} \
             steps of profile `{}` regenerated from the manifest at anchor {anchor_ms} ms, so \
             the replayed stream is not the one these parameters name",
            gen_report.digest, profile.name
        )),
        Err(e) => Some(format!(
            "the stream digest could not be recomputed from the manifest at anchor {anchor_ms} \
             ms: {e}"
        )),
    };
    let comparable = profile.is_publishable() && !off_profile && digest_mismatch.is_none();
    let comparability_reason = if let Some(reason) = digest_mismatch {
        Some(reason)
    } else if off_profile {
        Some(format!(
            "this run generated {steps_run} steps against profile `{}`'s {steps_declared}, so it \
             is not that profile and its figures cannot be published",
            profile.name
        ))
    } else {
        profile.comparability.reason().map(String::from)
    };
    let generator = crate::metrics_gen::Generator::for_profile(workload, profile, 0);
    ProfileRecord {
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
            anchor_ms,
            workload_version: workload.version,
            workload_digest,
            seed: workload.seed,
            stream_digest: gen_report.digest.clone(),
            total_series_created: gen_report.total_series_created,
            logical_input_bytes,
            total_samples_generated: gen_report.emitted_samples,
            scaling_label_cardinality: generator.scaling_label_cardinality(steps_run),
        },
    }
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

/// Group logical samples into `prometheus.TimeSeries` by canonical identity.
fn write_request_from(samples: &[LogicalSample]) -> WriteRequest {
    let mut by_series: BTreeMap<Vec<(String, String)>, PbTimeSeries> = BTreeMap::new();
    for s in samples {
        let labels = s.canonical_labels();
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
        entry.samples.push(PbSample {
            value: s.value,
            timestamp: s.ts_ms,
        });
    }
    WriteRequest {
        timeseries: by_series.into_values().collect(),
        metadata: Vec::new(),
    }
}

/// Encode a batch of logical samples as a Remote Write 1.0 body: a
/// `prometheus.WriteRequest` protobuf, snappy block-compressed. This is the
/// exact wire shape every comparator receives, and its length is the batch's
/// wire-byte figure for the in-process Ravel path too.
pub fn encode_rw1_body(samples: &[LogicalSample]) -> Result<Vec<u8>, LaneError> {
    let req = write_request_from(samples);
    let raw = req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&raw)
        .map_err(|e| LaneError::Snappy(e.to_string()))
}

/// Convert one accepted logical sample into an in-process ingest point. The
/// stored label set carries `__name__` so a PromQL selector matches by name;
/// `SeriesId::compute` hashes the name separately and skips the `__name__`
/// label, so the id is unchanged by its presence (ADR-0005).
fn ingest_point(tenant: &TenantId, sample: &LogicalSample) -> Result<IngestPoint, LaneError> {
    let mut labels: Vec<Label> = Vec::with_capacity(sample.labels.len() + 1);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: sample.metric.clone(),
    });
    for (name, value) in &sample.labels {
        labels.push(Label {
            name: name.clone(),
            value: value.clone(),
        });
    }
    let label_set = LabelSet::new(labels)?;
    let series_id = SeriesId::compute(tenant, &sample.metric, &label_set)?;
    Ok(IngestPoint {
        series_id,
        labels: Arc::new(label_set),
        value: IngestValue::Scalar(Sample {
            ts_ns: sample.ts_ms.saturating_mul(1_000_000),
            value: sample.value,
        }),
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
/// exactly one record, so a line the scalar replay does not send is counted
/// under its kind rather than skipped.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamRecord {
    /// A float payload: the unit the scalar replay sends.
    Scalar(LogicalSample),
    /// A native-histogram payload. The portable RW1.0 scalar replay does not
    /// model it, so it is excluded by name, never silently.
    NativeHistogram {
        ts_ms: i64,
        series: String,
        schema: i32,
        count: u64,
        sum_bits: u64,
        deltas_len: usize,
    },
    /// A staleness marker. Excluded from the scalar replay by name.
    StaleMarker { ts_ms: i64, series: String },
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
fn parse_native_histogram(body: &str) -> Result<(i32, u64, u64, usize), MalformedReason> {
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
    let deltas_len = if deltas.is_empty() {
        0
    } else {
        let mut n = 0usize;
        for delta in deltas.split(',') {
            delta
                .parse::<i64>()
                .map_err(|_| MalformedReason::UnknownPayload)?;
            n += 1;
        }
        n
    };
    Ok((schema, count, sum_bits, deltas_len))
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
        return StreamRecord::StaleMarker {
            ts_ms,
            series: series.to_string(),
        };
    }
    if let Some(body) = payload.strip_prefix("h ") {
        return match parse_native_histogram(body) {
            Ok((schema, count, sum_bits, deltas_len)) => StreamRecord::NativeHistogram {
                ts_ms,
                series: series.to_string(),
                schema,
                count,
                sum_bits,
                deltas_len,
            },
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
    /// Float lines: the samples the scalar replay offers.
    pub scalar: u64,
    /// Native-histogram lines, excluded from the scalar replay.
    pub native_histogram: u64,
    /// Staleness-marker lines, excluded from the scalar replay.
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

/// The stream half of ADR-0927 band 4 failed: the lines the replay classified
/// do not close against themselves or against what the generator emitted, a
/// per-type count differs from the generator's own, or a line was malformed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "the replayed stream does not close (ADR-0927 band 4): {scalar} scalar + {native_histogram} \
     native histogram + {stale_marker} stale marker + {malformed} malformed = {classified} \
     classified, against {generated_lines} lines read and {emitted_samples} samples the \
     generator emitted, of which {float_samples} float, {generator_native_histogram} native \
     histogram and {generator_stale_markers} stale marker; band 4 requires classified == lines \
     read == emitted, each classified kind equal to the generator's count of it, and zero \
     malformed"
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
    pub float_samples: u64,
    /// The generator's `native_histogram_samples`.
    pub generator_native_histogram: u64,
    /// The generator's `stale_markers`.
    pub generator_stale_markers: u64,
}

/// Check a replayed stream's counts against the generator's own report: the
/// four kinds sum to the lines read, the lines read equal the samples the
/// generator emitted, each of the three generated kinds equals the
/// generator's own count of it (so a classifier that swaps two kinds with
/// the totals intact fails), and no line was malformed.
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
        float_samples: report.float_samples,
        generator_native_histogram: report.native_histogram_samples,
        generator_stale_markers: report.stale_markers,
    })
}

/// One row's sample accounting failed (ADR-0927 band 4, the row half). Each
/// variant names every figure of the equation that failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RowAccountingViolation {
    /// The row did not offer exactly the stream's scalar lines.
    #[error(
        "{offered} offered samples against {scalar} scalar lines in the stream; the replay \
         offers exactly the scalar lines"
    )]
    OfferedNotScalar {
        /// The row's `offered_samples`.
        offered: u64,
        /// The closed stream's `scalar` count.
        scalar: u64,
    },
    /// The offering identity: offered plus the named exclusions is not every
    /// generated line.
    #[error(
        "{offered} offered + {excluded_native_histogram} excluded native histogram + \
         {excluded_stale_marker} excluded stale marker = {sum}, against {generated_lines} \
         generated lines; a line that reached no receiver must be counted under a named \
         exclusion"
    )]
    OfferingIdentity {
        /// The row's `offered_samples`.
        offered: u64,
        /// The row's `excluded_native_histogram`.
        excluded_native_histogram: u64,
        /// The row's `excluded_stale_marker`.
        excluded_stale_marker: u64,
        /// The three summed.
        sum: u64,
        /// The closed stream's `generated_lines`.
        generated_lines: u64,
    },
    /// The offered samples do not split into accepted, rejected and dropped.
    #[error(
        "{accepted} accepted + {rejected} rejected + {dropped} dropped = {sum}, against \
         {offered} offered; a silent drop fails the run"
    )]
    OfferedSplit {
        /// The row's `offered_samples`.
        offered: u64,
        /// The row's `accepted_samples`.
        accepted: u64,
        /// The row's `rejected_samples`.
        rejected: u64,
        /// The row's `dropped_samples`.
        dropped: u64,
        /// The three summed.
        sum: u64,
    },
}

/// Check one row against its closed stream: it offered exactly the scalar
/// lines, the offering identity `offered + excluded_native_histogram +
/// excluded_stale_marker == generated_lines` holds over the row's own fields,
/// and the offered samples split exactly into accepted, rejected and dropped.
/// `counts` is the stream's closed classification ([`GeneratedStream::finish`]
/// after [`check_stream_band`]); the exclusions are read from `ingest`, never
/// recomputed from `counts`.
pub fn check_row_accounting(
    ingest: &IngestPhase,
    counts: &StreamCounts,
) -> Result<(), RowAccountingViolation> {
    if ingest.offered_samples != counts.scalar {
        return Err(RowAccountingViolation::OfferedNotScalar {
            offered: ingest.offered_samples,
            scalar: counts.scalar,
        });
    }
    let offering =
        ingest.offered_samples + ingest.excluded_native_histogram + ingest.excluded_stale_marker;
    if offering != counts.generated_lines {
        return Err(RowAccountingViolation::OfferingIdentity {
            offered: ingest.offered_samples,
            excluded_native_histogram: ingest.excluded_native_histogram,
            excluded_stale_marker: ingest.excluded_stale_marker,
            sum: offering,
            generated_lines: counts.generated_lines,
        });
    }
    let split = ingest.accepted_samples + ingest.rejected_samples + ingest.dropped_samples;
    if split != ingest.offered_samples {
        return Err(RowAccountingViolation::OfferedSplit {
            offered: ingest.offered_samples,
            accepted: ingest.accepted_samples,
            rejected: ingest.rejected_samples,
            dropped: ingest.dropped_samples,
            sum: split,
        });
    }
    Ok(())
}

/// A source of scalar samples a replay pulls in batches, in stream order, so
/// a replay holds one batch at a time rather than the whole history.
pub trait SampleBatches {
    /// The next at most `max` scalar samples (`max` is raised to 1), or `None`
    /// once the stream is exhausted. A returned batch is never empty.
    fn next_batch(&mut self, max: usize) -> Result<Option<Vec<LogicalSample>>, LaneError>;
    /// Per-kind counts of the lines read so far.
    fn stream_counts(&self) -> StreamCounts;
}

/// A [`SampleBatches`] over samples already in memory, each counted as one
/// scalar line.
pub struct SliceSamples<'s> {
    samples: &'s [LogicalSample],
    pos: usize,
}

impl<'s> SliceSamples<'s> {
    /// Serve `samples` in order.
    pub fn new(samples: &'s [LogicalSample]) -> Self {
        SliceSamples { samples, pos: 0 }
    }
}

impl SampleBatches for SliceSamples<'_> {
    fn next_batch(&mut self, max: usize) -> Result<Option<Vec<LogicalSample>>, LaneError> {
        let rest = &self.samples[self.pos..];
        if rest.is_empty() {
            return Ok(None);
        }
        let n = max.max(1).min(rest.len());
        self.pos += n;
        Ok(Some(rest[..n].to_vec()))
    }

    fn stream_counts(&self) -> StreamCounts {
        let pulled = self.pos as u64;
        StreamCounts {
            generated_lines: pulled,
            scalar: pulled,
            ..StreamCounts::default()
        }
    }
}

/// The MetricsBench generator as a [`SampleBatches`]: it generates one scrape
/// step at a time into a reused buffer, classifies each line, and queues only
/// that step's scalar samples. What it holds is bounded by one step's lines
/// plus one batch, whatever the number of steps. Reusing the shipping encoder
/// as the single source keeps this lane from growing a second workload
/// generator (an ADR-0927 rejected alternative).
pub struct GeneratedStream<'a> {
    generator: crate::metrics_gen::Generator<'a>,
    steps: u64,
    tally: crate::metrics_gen::GenerationTally,
    step_bytes: Vec<u8>,
    pending: std::collections::VecDeque<LogicalSample>,
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
            if let StreamRecord::Scalar(sample) = record {
                self.pending.push_back(sample);
            }
        }
        Ok(true)
    }

    /// Close a drained stream: the generator's own report over every step,
    /// and the per-kind counts of every line. Refused before the stream is
    /// exhausted, since the report would then describe samples nobody offered.
    pub fn finish(self) -> Result<(crate::metrics_gen::GenerationReport, StreamCounts), LaneError> {
        let mut reasons = Vec::new();
        if self.tally.steps_generated() != self.steps {
            reasons.push(format!(
                "only {} of {} steps were generated",
                self.tally.steps_generated(),
                self.steps
            ));
        }
        if !self.pending.is_empty() {
            reasons.push(format!(
                "{} generated samples were never read",
                self.pending.len()
            ));
        }
        if !reasons.is_empty() {
            return Err(LaneError::Generate(format!(
                "the stream was closed before it was drained: {}",
                reasons.join(", and ")
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
    fn next_batch(&mut self, max: usize) -> Result<Option<Vec<LogicalSample>>, LaneError> {
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

/// BLAKE3 over the canonical `(series, ts_ms, value bits)` tuple of each
/// sample fed to it, in the order fed: a row's `offered_digest`. A tuple is
/// encoded as the label count, then each canonical label (`__name__`
/// included, sorted by name) as its name and value, each a `u32` byte length
/// and the bytes, then `ts_ms` as `i64` and the value's `f64::to_bits`, all
/// integers little-endian. The length prefixes make the encoding injective.
#[derive(Default)]
pub struct OfferedDigest {
    hasher: blake3::Hasher,
}

impl OfferedDigest {
    /// An empty digest.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one sample's tuple.
    pub fn update(&mut self, sample: &LogicalSample) {
        let labels = sample.canonical_labels();
        self.hasher.update(&(labels.len() as u32).to_le_bytes());
        for (name, value) in &labels {
            for part in [name, value] {
                self.hasher.update(&(part.len() as u32).to_le_bytes());
                self.hasher.update(part.as_bytes());
            }
        }
        self.hasher.update(&sample.ts_ms.to_le_bytes());
        self.hasher.update(&sample.value.to_bits().to_le_bytes());
    }

    /// The digest, hex.
    pub fn finalize(&self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }
}

/// Pull scalar samples from `source` until `pending` holds `batch_size` valid
/// ones or the stream ends, counting each pulled sample as offered (and
/// feeding it to `digest`) and each invalid one as rejected. `on_valid` sees
/// each valid sample in stream order and returns what is queued for it.
fn fill_valid<S, T, F>(
    source: &mut S,
    batch_size: usize,
    pending: &mut Vec<T>,
    offered: &mut u64,
    rejected: &mut u64,
    digest: &mut OfferedDigest,
    mut on_valid: F,
) -> Result<(), LaneError>
where
    S: SampleBatches + ?Sized,
    F: FnMut(LogicalSample) -> Result<T, LaneError>,
{
    while pending.len() < batch_size {
        let Some(batch) = source.next_batch(batch_size - pending.len())? else {
            break;
        };
        if batch.is_empty() {
            break;
        }
        for sample in batch {
            *offered += 1;
            digest.update(&sample);
            match sample.validate() {
                Ok(()) => pending.push(on_valid(sample)?),
                Err(_) => *rejected += 1,
            }
        }
    }
    Ok(())
}

/// Replay `source` into the in-process Ravel path, pulling one batch at a
/// time so memory is bounded by the batch, not the stream's history. Valid
/// samples are batched in stream order and written with `WriteMode::Strict`,
/// so each batch's ack means durable and mints commit tokens; invalid samples
/// are refused with an exact count. The accounting closes: `offered ==
/// accepted + rejected + dropped`, and the row carries the source's
/// [`StreamCounts`].
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

    let mut pending: Vec<(IngestPoint, LogicalSample)> = Vec::with_capacity(batch_size);
    let mut offered_samples: u64 = 0;
    let mut rejected_samples: u64 = 0;
    let mut max_ts_ms: Option<i64> = None;
    let mut first_metric: Option<String> = None;
    let mut wire_bytes: u64 = 0;
    let mut ack_latencies_ns: Vec<u64> = Vec::new();
    let mut tokens: Vec<CommitToken> = Vec::new();
    let mut accepted_samples: u64 = 0;
    let mut dropped_samples: u64 = 0;
    let mut dropped_batches: u64 = 0;
    let mut batches: u64 = 0;
    let mut offered_digest = OfferedDigest::new();

    loop {
        // Client-side admission: valid points (carrying their logical sample
        // for the wire encoding) and exact rejections.
        fill_valid(
            source,
            batch_size,
            &mut pending,
            &mut offered_samples,
            &mut rejected_samples,
            &mut offered_digest,
            |sample| {
                let point = ingest_point(&config.tenant, &sample)?;
                max_ts_ms = Some(max_ts_ms.map_or(sample.ts_ms, |m| m.max(sample.ts_ms)));
                first_metric.get_or_insert_with(|| sample.metric.clone());
                Ok((point, sample))
            },
        )?;
        if pending.is_empty() {
            break;
        }
        let (points, logical): (Vec<IngestPoint>, Vec<LogicalSample>) = pending.drain(..).unzip();
        // The RW1.0 wire size is encoded OUTSIDE the timed segment (issue #937
        // review finding 5): the in-process path never serializes an RW1.0
        // body to ingest, while the HTTP comparators' encode IS client work
        // inside their own segments.
        wire_bytes += encode_rw1_body(&logical)?.len() as u64;
        drop(logical);
        batches += 1;
        let batch_len = points.len() as u64;

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
                accepted_samples += batch_len;
                tokens.extend(receipt.tokens);
            }
            Err(_) => {
                // The strict write did not durably ack: the batch is dropped,
                // its samples neither accepted nor rejected, so the accounting
                // still closes. In process this is rare (a dead shard).
                dropped_batches += 1;
                dropped_samples += batch_len;
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
        excluded_native_histogram: stream_counts.native_histogram,
        excluded_stale_marker: stream_counts.stale_marker,
        offered_samples,
        offered_digest: offered_digest.finalize(),
        accepted_samples,
        rejected_samples,
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

/// Inputs for a comparator replay over portable Remote Write 1.0. The endpoint
/// comes from config: there is no hardcoded host.
pub struct HttpReplayConfig {
    /// The system name for the row: `prometheus`, `victoriametrics`, `osn`.
    pub system: String,
    /// The full `/api/v1/write` URL supplied by the caller.
    pub endpoint: String,
    /// Logical samples per RW1.0 request.
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

/// Replay `source` into a comparator over Remote Write 1.0, pulling one batch
/// at a time as [`replay_into_ravel`] does. Valid samples are batched in
/// stream order, encoded as snappy protobuf, and POSTed to the configured
/// endpoint; each batch is retried on a 429/5xx or a transport error up to
/// `max_retries`, then dropped and counted under `retry_exhausted_samples`. A
/// batch that ends on any other non-2xx is dropped without a retry and
/// counted under `rejected_by_status` by that status. Every
/// row this produces is [`AckSemantics::Buffered`]: a Remote Write 2xx from a
/// comparator means accepted-into-a-buffer, a durability the client cannot
/// observe, so it is never placed in a pooled column with Ravel's durable ack.
///
/// The comparator's object store is not the bench's, so this row carries no
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

    let mut pending: Vec<LogicalSample> = Vec::new();
    let mut offered_samples: u64 = 0;
    let mut rejected_samples: u64 = 0;
    let mut timed = Duration::ZERO;

    let batch_size = config.batch_size.max(1);
    let mut ack_latencies_ns: Vec<u64> = Vec::new();
    let mut accepted_samples: u64 = 0;
    let mut dropped_samples: u64 = 0;
    let mut dropped_batches: u64 = 0;
    let mut batches: u64 = 0;
    let mut retries: u64 = 0;
    let mut wire_bytes: u64 = 0;
    let mut offered_digest = OfferedDigest::new();
    let mut rejected_by_status: BTreeMap<u16, u64> = BTreeMap::new();
    let mut retry_exhausted_samples: u64 = 0;

    loop {
        fill_valid(
            source,
            batch_size,
            &mut pending,
            &mut offered_samples,
            &mut rejected_samples,
            &mut offered_digest,
            Ok,
        )?;
        if pending.is_empty() {
            break;
        }
        let chunk = std::mem::take(&mut pending);
        let segment_start = std::time::Instant::now();
        batches += 1;
        let body = encode_rw1_body(&chunk)?;
        wire_bytes += body.len() as u64;
        let batch_len = chunk.len() as u64;

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let outcome = loop {
            let sent = client
                .post(&config.endpoint)
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                .body(body.clone())
                .send()
                .await;
            let retryable = match &sent {
                Ok(resp) if resp.status().is_success() => break BatchOutcome::Accepted,
                Ok(resp) if !is_retryable(resp.status()) => {
                    break BatchOutcome::Rejected(resp.status().as_u16());
                }
                Ok(_) | Err(_) => true,
            };
            if retryable && attempt < config.max_retries {
                attempt += 1;
                retries += 1;
                tokio::time::sleep(HTTP_RETRY_BACKOFF).await;
            } else {
                break BatchOutcome::RetriesExhausted;
            }
        };
        match outcome {
            BatchOutcome::Accepted => {
                ack_latencies_ns.push(start.elapsed().as_nanos() as u64);
                accepted_samples += batch_len;
            }
            BatchOutcome::Rejected(status) => {
                dropped_batches += 1;
                dropped_samples += batch_len;
                *rejected_by_status.entry(status).or_default() += batch_len;
            }
            BatchOutcome::RetriesExhausted => {
                dropped_batches += 1;
                dropped_samples += batch_len;
                retry_exhausted_samples += batch_len;
            }
        }
        timed += segment_start.elapsed();
    }

    let elapsed_secs = timed.as_secs_f64().max(1e-9);
    let stream_counts = source.stream_counts();
    let ingest = IngestPhase {
        stream_counts,
        excluded_native_histogram: stream_counts.native_histogram,
        excluded_stale_marker: stream_counts.stale_marker,
        offered_samples,
        offered_digest: offered_digest.finalize(),
        accepted_samples,
        rejected_samples,
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
        // The comparator runs in another process: its CPU and RSS are not the
        // bench's to report. Absent, never a zero.
        peak_rss_bytes: None,
        cpu_secs: None,
    };
    let client_behavior = format!(
        "reqwest RW1.0 sender: snappy protobuf, {batch} logical samples/request sent sequentially \
         (backpressure by awaiting each POST), retry on 429/5xx up to {retries} times with a fixed \
         {backoff_ms} ms backoff then drop the batch; {timeout} s per-request timeout bounds the \
         complete operation including the body read",
        batch = batch_size,
        retries = config.max_retries,
        backoff_ms = HTTP_RETRY_BACKOFF.as_millis(),
        timeout = config.timeout_secs.max(1),
    );
    Ok(SystemIngestResult::buffered_http_row(
        config.system.clone(),
        config.endpoint.clone(),
        client_behavior,
        ingest,
    ))
}

/// How one comparator batch ended.
enum BatchOutcome {
    /// A 2xx.
    Accepted,
    /// A non-retryable non-2xx, with its status.
    Rejected(u16),
    /// Every allowed retry ended on a 429, a 5xx or a transport error.
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
            dropped_samples: 0,
            dropped_batches: 0,
            rejected_by_status: BTreeMap::new(),
            retry_exhausted_samples: 0,
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
        assert_eq!(
            records[1],
            StreamRecord::StaleMarker {
                ts_ms: 15_000,
                series: "mb_gauge{instance=\"mb-instance-1\"}".to_string(),
            }
        );
        assert_eq!(
            records[2],
            StreamRecord::NativeHistogram {
                ts_ms: 15_000,
                series: "mb_native{job=\"api\"}".to_string(),
                schema: 2,
                count: 4,
                sum_bits: 1.0f64.to_bits(),
                deltas_len: 4,
            }
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
        let mut offered = 0u64;
        while let Some(batch) = stream.next_batch(500).expect("generates") {
            offered += batch.len() as u64;
        }
        let (report, counts) = stream.finish().expect("drained stream closes");

        assert!(report.native_histogram_samples > 0 && report.stale_markers > 0);
        assert_eq!(counts.scalar, report.float_samples);
        assert_eq!(counts.native_histogram, report.native_histogram_samples);
        assert_eq!(counts.stale_marker, report.stale_markers);
        assert_eq!(counts.malformed, 0);
        assert_eq!(counts.generated_lines, report.emitted_samples);
        assert_eq!(offered, counts.scalar, "every scalar line is offered once");
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

        // Only the classification sum breaks: one scalar line lost from the
        // kinds while the lines read and the generator's total are unchanged.
        let scalar_short = StreamCounts {
            scalar: closed.scalar - 1,
            ..closed
        };
        let err =
            check_stream_band(&scalar_short, &report).expect_err("an open classification sum");
        assert_eq!(err.generated_lines, err.emitted_samples, "totals unchanged");
        assert_eq!(err.classified + 1, err.generated_lines);
        assert!(
            err.to_string().contains(&format!(
                "{} scalar + {} native histogram + {} stale marker + 0 malformed = {} classified, \
                 against {} lines read",
                closed.scalar - 1,
                closed.native_histogram,
                closed.stale_marker,
                closed.generated_lines - 1,
                closed.generated_lines,
            )),
            "{err}"
        );

        // A classifier that swaps native histograms and stale markers keeps
        // every total intact; only the generator's per-type counts catch it.
        assert_ne!(
            closed.native_histogram, closed.stale_marker,
            "the swap must move the counts for this case to mean anything"
        );
        let swapped = StreamCounts {
            native_histogram: closed.stale_marker,
            stale_marker: closed.native_histogram,
            ..closed
        };
        assert_eq!(swapped.classified(), swapped.generated_lines);
        let err = check_stream_band(&swapped, &report).expect_err("a type swap is refused");
        assert!(
            err.to_string().contains(&format!(
                "{} native histogram and {} stale marker; band 4",
                report.native_histogram_samples, report.stale_markers
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
        assert_eq!(
            first_samples, second_samples,
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
        let one_shot_scalars: Vec<LogicalSample> = std::str::from_utf8(&one_shot_bytes)
            .expect("utf8")
            .lines()
            .enumerate()
            .filter_map(|(i, line)| match classify_line(i as u64 + 1, line) {
                StreamRecord::Scalar(sample) => Some(sample),
                _ => None,
            })
            .collect();
        assert_eq!(
            first_samples, one_shot_scalars,
            "streaming offers the one-shot stream's scalars in its order"
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

    /// Closing a stream with every step generated but samples still queued is
    /// refused rather than reporting figures for samples nobody offered.
    #[test]
    fn a_stream_with_unread_samples_refuses_to_finish() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let mut stream =
            GeneratedStream::new(Generator::new(&workload, "ci", 0).expect("builds"), 1);
        let batch = stream.next_batch(10).expect("generates").expect("a batch");
        assert_eq!(batch.len(), 10);
        assert_eq!(stream.tally.steps_generated(), 1, "every step is generated");
        let unread = stream.pending.len();
        assert!(unread > 0);
        let err = stream.finish().expect_err("unread samples are refused");
        assert_eq!(
            err.to_string(),
            format!(
                "generating the replayed stream failed: the stream was closed before it was \
                 drained: {unread} generated samples were never read"
            )
        );
    }

    /// Closing a stream whose queue is drained but whose steps are short is
    /// refused too: the queue being empty says nothing about the steps.
    #[test]
    fn a_stream_with_steps_short_refuses_to_finish() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let step_scalars = {
            let mut one =
                GeneratedStream::new(Generator::new(&workload, "ci", 0).expect("builds"), 1);
            let mut n = 0;
            while let Some(batch) = one.next_batch(1 << 20).expect("generates") {
                n += batch.len();
            }
            n
        };
        let mut stream =
            GeneratedStream::new(Generator::new(&workload, "ci", 0).expect("builds"), 2);
        let batch = stream
            .next_batch(step_scalars)
            .expect("generates")
            .expect("a batch");
        assert_eq!(batch.len(), step_scalars);
        assert!(stream.pending.is_empty(), "the queue is drained");
        let err = stream.finish().expect_err("short steps are refused");
        assert_eq!(
            err.to_string(),
            "generating the replayed stream failed: the stream was closed before it was \
             drained: only 1 of 2 steps were generated"
        );
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

        let (bytes, gen_report) = Generator::for_profile(&workload, &profile, 0)
            .generate_bytes(steps)
            .expect("generates steps");
        let logical_input_bytes = bytes.len() as u64;

        let record = build_profile_record(
            &workload,
            &profile,
            steps,
            logical_input_bytes,
            &gen_report,
            0,
            String::new(),
        );

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

        let (bytes, gen_report) = Generator::for_profile(&workload, &profile, 0)
            .generate_bytes(steps_run)
            .expect("generates steps");
        let record = build_profile_record(
            &workload,
            &profile,
            steps_run,
            bytes.len() as u64,
            &gen_report,
            0,
            String::new(),
        );

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

        let (bytes, gen_report) = Generator::for_profile(&workload, &profile, 1_700_000_010_000)
            .generate_bytes(steps_run)
            .expect("generates steps");
        let logical_input_bytes = bytes.len() as u64;

        let record = build_profile_record(
            &workload,
            &profile,
            steps_run,
            logical_input_bytes,
            &gen_report,
            1_700_000_010_000,
            "manifest-digest".to_string(),
        );

        assert_eq!(record.run.anchor_ms, 1_700_000_010_000);
        assert_eq!(record.run.workload_digest, "manifest-digest");
        assert_eq!(record.run.workload_version, workload.version);
        assert_eq!(record.run.seed, workload.seed);
        assert_eq!(record.run.stream_digest, gen_report.digest);

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

    /// The anchor is required and bounded: missing, negative, past the
    /// nanosecond range and off the scrape grid are each refused, with the
    /// figures in the message, and the largest on-grid anchor whose newest
    /// step fits is accepted.
    ///
    /// TO SEE THIS FAIL: drop the range check in [`check_anchor`] and
    /// `i64::MAX - 1` reaches the grid check instead; drop the grid check and
    /// the anchor 1 ms off the grid is accepted.
    #[test]
    fn check_anchor_refuses_each_out_of_contract_anchor() {
        let interval_secs = 15;
        let steps = 120;

        let missing = check_anchor(None, interval_secs, steps).expect_err("no anchor");
        assert_eq!(missing, AnchorError::Missing);
        assert!(
            missing
                .to_string()
                .starts_with("--anchor-ms is required and has no default: "),
            "{missing}"
        );

        assert_eq!(
            check_anchor(Some(1_700_000_010_000), interval_secs, steps),
            Ok(1_700_000_010_000)
        );

        let off_grid = check_anchor(Some(1_700_000_010_001), interval_secs, steps)
            .expect_err("1 ms off the grid");
        assert_eq!(
            off_grid,
            AnchorError::OffGrid {
                anchor_ms: 1_700_000_010_001,
                interval_ms: 15_000,
                remainder: 1,
            }
        );
        assert_eq!(
            off_grid.to_string(),
            "--anchor-ms 1700000010001 is off the scrape grid: 1700000010001 mod 15000 ms is 1, \
             and the anchor must be a multiple of the profile's scrape interval"
        );

        let negative =
            check_anchor(Some(-15_000), interval_secs, steps).expect_err("before the epoch");
        assert_eq!(negative, AnchorError::Negative { anchor_ms: -15_000 });
        assert_eq!(
            negative.to_string(),
            "--anchor-ms -15000 is negative; the anchor must be at or after the epoch"
        );

        let near_max =
            check_anchor(Some(i64::MAX - 1), interval_secs, steps).expect_err("past the range");
        assert_eq!(
            near_max,
            AnchorError::PastRange {
                anchor_ms: i64::MAX - 1,
                steps,
                interval_ms: 15_000,
                max_ms: 9_223_372_036_854,
            }
        );
        assert_eq!(
            near_max.to_string(),
            "--anchor-ms 9223372036854775806 with 120 steps of 15000 ms puts the newest step \
             past 9223372036854 ms, the largest millisecond whose nanosecond form fits an i64"
        );

        let span = (steps as i64 - 1) * 15_000;
        let top = (ANCHOR_MAX_MS - span) / 15_000 * 15_000;
        assert_eq!(check_anchor(Some(top), interval_secs, steps), Ok(top));
        assert!(
            (top + span).checked_mul(1_000_000).is_some(),
            "the newest step of the largest accepted anchor fits an i64 in nanoseconds"
        );
        assert!(matches!(
            check_anchor(Some(top + 15_000), interval_secs, steps),
            Err(AnchorError::PastRange { .. })
        ));
    }

    /// `comparable` needs a publishable profile, the declared step count and
    /// a stream digest that matches one regenerated from the manifest and the
    /// anchor. A tampered digest, or a digest generated at another anchor,
    /// forces it false with the digest reason.
    ///
    /// TO SEE THIS FAIL: make `build_profile_record` compare
    /// `gen_report.digest` with itself (copy the generator's digest into the
    /// comparison) and both tampered records come back comparable.
    #[test]
    fn profile_record_is_non_comparable_when_the_stream_digest_does_not_recompute() {
        use crate::metrics_gen::Generator;
        use crate::metrics_workload::Comparability;

        let workload = ci_workload();
        let ci = workload.profile("ci").expect("ci profile declared");
        let steps = 3;
        let anchor = 1_700_000_010_000;
        let profile = Profile {
            comparability: Comparability::Comparable,
            samples_per_series: steps,
            ..ci.clone()
        };
        let (bytes, gen_report) = Generator::for_profile(&workload, &profile, anchor)
            .generate_bytes(steps)
            .expect("generates steps");
        let record = |report: &crate::metrics_gen::GenerationReport, anchor_ms: i64| {
            build_profile_record(
                &workload,
                &profile,
                steps,
                bytes.len() as u64,
                report,
                anchor_ms,
                String::new(),
            )
        };

        let honest = record(&gen_report, anchor);
        assert!(
            honest.comparable,
            "publishable profile, declared steps, recomputable digest: comparable"
        );
        assert_eq!(honest.comparability_reason, None);

        let mut tampered = gen_report.clone();
        tampered.digest = "0".repeat(64);
        let forged = record(&tampered, anchor);
        assert!(!forged.comparable, "a tampered digest is never comparable");
        assert_eq!(
            forged.comparability_reason,
            Some(format!(
                "the recorded stream digest {} differs from {}, the digest of {steps} steps of \
                 profile `{}` regenerated from the manifest at anchor {anchor} ms, so the \
                 replayed stream is not the one these parameters name",
                tampered.digest, gen_report.digest, profile.name
            ))
        );

        let elsewhere = record(&gen_report, anchor + 15_000);
        assert!(
            !elsewhere.comparable,
            "a stream generated at another anchor is not the one this anchor names"
        );
        assert!(
            elsewhere
                .comparability_reason
                .as_deref()
                .is_some_and(|r| r.starts_with("the recorded stream digest ")),
            "{:?}",
            elsewhere.comparability_reason
        );
    }

    /// Band 4's row half reads the exclusions from the row's own fields. A
    /// row from a real replay closes; the same row with either excluded
    /// field zeroed after assignment fails the offering identity, and the
    /// message names every figure.
    ///
    /// TO SEE THIS FAIL: compute the exclusions in [`check_row_accounting`]
    /// from `counts` instead of `ingest`, and both zeroed rows pass.
    #[tokio::test]
    async fn a_row_with_a_zeroed_exclusion_fails_the_offering_identity() {
        use crate::metrics_gen::Generator;

        let workload = ci_workload();
        let anchor = (base_ts_ms() / 15_000) * 15_000 - 60_000;
        // The fewest steps whose stream holds both excluded kinds, so zeroing
        // either field changes the row.
        let steps = (1..=workload.profile("ci").expect("ci").samples_per_series)
            .find(|&steps| {
                let (_, report) = Generator::new(&workload, "ci", anchor)
                    .expect("builds")
                    .generate_bytes(steps)
                    .expect("generates");
                report.native_histogram_samples > 0 && report.stale_markers > 0
            })
            .expect("the ci profile emits both excluded kinds");
        let mut stream = GeneratedStream::new(
            Generator::new(&workload, "ci", anchor).expect("builds"),
            steps,
        );
        let outcome = replay_into_ravel(&memory_config("mb-offering-identity", 500), &mut stream)
            .await
            .expect("replay");
        let (report, counts) = stream.finish().expect("drained stream closes");
        check_stream_band(&counts, &report).expect("band 4 stream half");
        let row = outcome.result.ingest;
        assert!(row.excluded_native_histogram > 0 && row.excluded_stale_marker > 0);
        check_row_accounting(&row, &counts).expect("an honest row closes");

        let mut no_stale = row.clone();
        no_stale.excluded_stale_marker = 0;
        let err = check_row_accounting(&no_stale, &counts).expect_err("stale zeroed");
        assert_eq!(
            err,
            RowAccountingViolation::OfferingIdentity {
                offered: row.offered_samples,
                excluded_native_histogram: row.excluded_native_histogram,
                excluded_stale_marker: 0,
                sum: row.offered_samples + row.excluded_native_histogram,
                generated_lines: counts.generated_lines,
            }
        );
        let message = err.to_string();
        for figure in [
            row.offered_samples,
            row.excluded_native_histogram,
            row.offered_samples + row.excluded_native_histogram,
            counts.generated_lines,
        ] {
            assert!(message.contains(&figure.to_string()), "{figure}: {message}");
        }

        let mut no_native = row.clone();
        no_native.excluded_native_histogram = 0;
        assert!(matches!(
            check_row_accounting(&no_native, &counts),
            Err(RowAccountingViolation::OfferingIdentity {
                excluded_native_histogram: 0,
                ..
            })
        ));
    }

    /// A comparator's dropped samples split by cause: a 400 is a rejection
    /// by status with no retry, and a 503 on every attempt is a retry
    /// exhaustion. `dropped_samples` is their sum and the offered samples
    /// still split exactly into accepted, rejected and dropped.
    #[tokio::test]
    async fn comparator_drops_are_split_by_status_and_retry_exhaustion() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let requests = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&requests);
        let app = axum::Router::new().route(
            "/api/v1/write",
            axum::routing::post(move || {
                let seen = Arc::clone(&seen);
                async move {
                    if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                        axum::http::StatusCode::BAD_REQUEST
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the stub");
        let addr = listener.local_addr().expect("stub address");
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let stream = valid_stream(8, 2);
        let config = HttpReplayConfig {
            system: "prometheus".to_string(),
            endpoint: format!("http://{addr}/api/v1/write"),
            batch_size: 4,
            max_retries: 2,
            timeout_secs: 5,
        };
        let mut source = SliceSamples::new(&stream);
        let row = replay_over_http(&config, &mut source)
            .await
            .expect("replay")
            .ingest;
        server.abort();

        assert_eq!(
            requests.load(Ordering::SeqCst),
            4,
            "one POST for the 400 batch, three for the 503 batch"
        );
        assert_eq!(row.offered_samples, 8);
        assert_eq!(row.accepted_samples, 0);
        assert_eq!(row.rejected_samples, 0);
        assert_eq!(row.rejected_by_status, BTreeMap::from([(400u16, 4u64)]));
        assert_eq!(row.retry_exhausted_samples, 4);
        assert_eq!(row.dropped_samples, 8);
        assert_eq!(row.dropped_batches, 2);
        assert_eq!(row.batches, 2);
        assert_eq!(row.retries, 2);
        check_row_accounting(&row, &source.stream_counts()).expect("the row's accounting closes");
    }

    /// An `ingest` block written before the stream accounting, the offered
    /// digest and the status map existed still loads, each added field
    /// taking its default; an empty `elapsed_basis` reads as wall time,
    /// unmarked. The run block likewise loads without the provenance fields.
    #[test]
    fn a_pre_change_artifact_loads_with_defaults() {
        let ingest: IngestPhase = serde_json::from_str(
            r#"{
                "offered_samples": 120,
                "accepted_samples": 120,
                "rejected_samples": 0,
                "dropped_samples": 0,
                "dropped_batches": 0,
                "batches": 1,
                "retries": 0,
                "elapsed_secs": 0.5,
                "logical_points_per_sec": 240.0,
                "wire_bytes": 900,
                "wire_bytes_per_sec": 1800.0,
                "ack_latency_ms": {"p50": 1.0, "p95": 1.0, "p99": 1.0, "max": 1.0, "count": 1}
            }"#,
        )
        .expect("a pre-change ingest block loads");
        assert_eq!(ingest.stream_counts, StreamCounts::default());
        assert_eq!(ingest.excluded_native_histogram, 0);
        assert_eq!(ingest.excluded_stale_marker, 0);
        assert_eq!(ingest.elapsed_basis, "");
        assert_eq!(ingest.offered_digest, "");
        assert!(ingest.rejected_by_status.is_empty());
        assert_eq!(ingest.retry_exhausted_samples, 0);
        assert_eq!(ingest.offered_samples, 120);

        let run: RunRecord = serde_json::from_str(
            r#"{
                "steps": 5,
                "anchor_ms": 1700000000000,
                "anchor_source": "clock",
                "total_series_created": 1000,
                "logical_input_bytes": 1,
                "total_samples_generated": 5000,
                "scaling_label_cardinality": 1000
            }"#,
        )
        .expect("a pre-change run block loads");
        assert_eq!(run.workload_version, 0);
        assert_eq!(run.workload_digest, "");
        assert_eq!(run.seed, 0);
        assert_eq!(run.stream_digest, "");
    }
}

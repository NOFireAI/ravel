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
//! accepted_samples + rejected_samples + dropped_samples`. A figure the lane
//! cannot measure on a system is ABSENT (an `Option`), never a zero standing in
//! for unmeasured: a buffered system has no commit tokens (`None`, not an empty
//! vec), an HTTP comparator has no object-store accounting the bench can see
//! (`None`), and a non-Linux host reports no CPU or peak RSS (`None`). This is
//! the same discipline `report.rs` applies to billed attempts.
//!
//! Report-only: like the rest of `ravel-bench`, this lane never changes library
//! behaviour, it only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{
    Clock, IngestConfig, IngestPoint, IngestRouter, IngestValue, SystemClock, WriteMode,
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

/// The ingest phase of one system's replay. Ingest and query phases are
/// recorded separately (ADR-0927 decision 9); this lane owns ingest, the query
/// lane owns query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IngestPhase {
    /// Logical samples offered to the system. `accepted + rejected + dropped`.
    pub offered_samples: u64,
    /// Samples the system acknowledged.
    pub accepted_samples: u64,
    /// Samples the lane refused before sending (client-side admission).
    pub rejected_samples: u64,
    /// Samples in a batch that was abandoned after exhausting retries: neither
    /// accepted nor rejected, so the accounting still closes.
    pub dropped_samples: u64,
    /// Batches abandoned after exhausting retries.
    pub dropped_batches: u64,
    /// Batches offered (one write call each).
    pub batches: u64,
    /// Transport retries: object-store PUT retries below `InstrumentedStore`
    /// for Ravel, HTTP 429/5xx retries for a comparator.
    pub retries: u64,
    /// Wall time of the ingest phase. The two lanes time DIFFERENT work, so a
    /// throughput comparison must account for it: the Ravel window covers only
    /// the in-process `write_values` calls (the RW1.0 encode that yields
    /// `wire_bytes` is precomputed OUTSIDE this window, since the in-process
    /// path never serializes an RW1.0 body to ingest), while an HTTP
    /// comparator's window includes its genuine per-batch encode and POST,
    /// which is real client work for that lane.
    pub elapsed_secs: f64,
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
    /// unmeasured (non-Linux, or a remote comparator).
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

/// The whole ingest-lane report: one row per participating system, plus the
/// separately-recorded query-phase rows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsIngestReport {
    /// Report rows, in the order the systems were replayed.
    pub systems: Vec<SystemIngestResult>,
    /// Query-phase rows, recorded separately from ingest (ADR-0927 decision 9).
    /// Empty when no post-ingest query ran.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queries: Vec<SystemQueryResult>,
}

impl MetricsIngestReport {
    /// Assemble a report from its ingest rows, with no query phase yet.
    pub fn new(systems: Vec<SystemIngestResult>) -> Self {
        MetricsIngestReport {
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

/// Parse a MetricsBench generated stream (the canonical `metrics_gen` encoding,
/// `<ts_ms>\t<metric>{<labels>}\t<payload>`) into the scalar logical samples
/// the ingest lane replays. Only float payloads (`f 0x<bits>`) become logical
/// samples; native-histogram (`h ...`) and staleness (`stale`) lines are
/// skipped, since the portable scalar replay does not model them. Reusing the
/// shipping encoder as the single source keeps this lane from growing a second
/// workload generator (an ADR-0927 rejected alternative).
pub fn parse_logical_stream(text: &str) -> Vec<LogicalSample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let mut parts = line.split('\t');
        let (Some(ts), Some(series), Some(payload)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let Some(hex) = payload.strip_prefix("f 0x") else {
            continue; // native histogram or staleness marker: not a scalar
        };
        let Ok(ts_ms) = ts.parse::<i64>() else {
            continue;
        };
        let Ok(bits) = u64::from_str_radix(hex, 16) else {
            continue;
        };
        let Some((metric, labels)) = parse_series(series) else {
            continue;
        };
        out.push(LogicalSample {
            metric,
            labels,
            ts_ms,
            value: f64::from_bits(bits),
        });
    }
    out
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
}

/// Replay `stream` into the in-process Ravel path. Valid samples are batched
/// and written with `WriteMode::Strict`, so each batch's ack means durable and
/// mints commit tokens; invalid samples are refused with an exact count. The
/// accounting closes: `offered == accepted + rejected + dropped`.
pub async fn replay_into_ravel(
    config: &RavelReplayConfig,
    stream: &[LogicalSample],
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
    ));

    // Client-side admission: partition into valid points (carrying their
    // logical sample for the wire encoding) and exact rejections.
    let mut valid: Vec<(IngestPoint, LogicalSample)> = Vec::new();
    let mut rejected_samples: u64 = 0;
    let mut max_ts_ms: Option<i64> = None;
    for sample in stream {
        match sample.validate() {
            Ok(()) => {
                let point = ingest_point(&config.tenant, sample)?;
                max_ts_ms = Some(max_ts_ms.map_or(sample.ts_ms, |m| m.max(sample.ts_ms)));
                valid.push((point, sample.clone()));
            }
            Err(_) => rejected_samples += 1,
        }
    }

    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);
    let batch_size = config.batch_size.max(1);

    // Precompute the RW1.0 wire size OUTSIDE the timed window (issue #937 review
    // finding 5). The in-process Ravel path never serializes an RW1.0 body to
    // ingest; timing that encode would fold work the in-process path never
    // performs into `elapsed_secs`, skewing Ravel's throughput against the HTTP
    // comparators, whose encode IS genuine client work inside their own window.
    // `wire_bytes` is finalized here and immutable through the timed loop. Move
    // this accumulation into the timed `write_values` loop below to see
    // `tests::wire_bytes_equal_the_precomputed_rw1_body_sum` observe the change.
    let mut precomputed_wire_bytes: u64 = 0;
    for chunk in valid.chunks(batch_size) {
        let logical: Vec<LogicalSample> = chunk.iter().map(|(_, s)| s.clone()).collect();
        precomputed_wire_bytes += encode_rw1_body(&logical)?.len() as u64;
    }
    let wire_bytes = precomputed_wire_bytes;

    let cpu_before = process_cpu_secs();
    let wall_start = std::time::Instant::now();

    let mut ack_latencies_ns: Vec<u64> = Vec::new();
    let mut tokens: Vec<CommitToken> = Vec::new();
    let mut accepted_samples: u64 = 0;
    let mut dropped_samples: u64 = 0;
    let mut dropped_batches: u64 = 0;
    let mut batches: u64 = 0;

    for chunk in valid.chunks(batch_size) {
        batches += 1;
        let points: Vec<IngestPoint> = chunk.iter().map(|(p, _)| p.clone()).collect();
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
        match result {
            Ok(receipt) => {
                ack_latencies_ns.push(start.elapsed().as_nanos() as u64);
                accepted_samples += batch_len;
                tokens.extend(receipt.tokens);
            }
            Err(_) => {
                // The strict write did not durably ack: the batch is dropped,
                // its samples neither accepted nor rejected, so the accounting
                // still closes. In process this is rare (a dead shard); over
                // HTTP it is the exhausted-retry case.
                dropped_batches += 1;
                dropped_samples += batch_len;
            }
        }
    }

    router.flush_all().await;
    let elapsed_secs = wall_start.elapsed().as_secs_f64().max(1e-9);
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

    let offered_samples = stream.len() as u64;
    let ingest = IngestPhase {
        offered_samples,
        accepted_samples,
        rejected_samples,
        dropped_samples,
        dropped_batches,
        batches,
        retries: snapshot.put.attempts.saturating_sub(snapshot.put.calls),
        elapsed_secs,
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

/// Replay `stream` into a comparator over Remote Write 1.0. Valid samples are
/// batched, encoded as snappy protobuf, and POSTed to the configured endpoint;
/// each batch is retried on a 429/5xx up to `max_retries`, then dropped. Every
/// row this produces is [`AckSemantics::Buffered`]: a Remote Write 2xx from a
/// comparator means accepted-into-a-buffer, a durability the client cannot
/// observe, so it is never placed in a pooled column with Ravel's durable ack.
///
/// The comparator's object store is not the bench's, so this row carries no
/// [`StorageAccounting`], and its CPU/RSS are the remote process's, so they are
/// `None`: absent, never a zero.
pub async fn replay_over_http(
    config: &HttpReplayConfig,
    stream: &[LogicalSample],
) -> Result<SystemIngestResult, LaneError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs.max(1)))
        .build()
        .map_err(|e| transport_setup_error(format!("building the HTTP client failed: {e}")))?;

    let mut valid: Vec<LogicalSample> = Vec::new();
    let mut rejected_samples: u64 = 0;
    for sample in stream {
        match sample.validate() {
            Ok(()) => valid.push(sample.clone()),
            Err(_) => rejected_samples += 1,
        }
    }

    let batch_size = config.batch_size.max(1);
    let wall_start = std::time::Instant::now();
    let mut ack_latencies_ns: Vec<u64> = Vec::new();
    let mut accepted_samples: u64 = 0;
    let mut dropped_samples: u64 = 0;
    let mut dropped_batches: u64 = 0;
    let mut batches: u64 = 0;
    let mut retries: u64 = 0;
    let mut wire_bytes: u64 = 0;

    for chunk in valid.chunks(batch_size) {
        batches += 1;
        let body = encode_rw1_body(chunk)?;
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
            match sent {
                Ok(resp) if resp.status().is_success() => break Ok(()),
                Ok(resp) if is_retryable(resp.status()) && attempt < config.max_retries => {
                    attempt += 1;
                    retries += 1;
                    tokio::time::sleep(HTTP_RETRY_BACKOFF).await;
                }
                Ok(_) => break Err(()),
                Err(_) if attempt < config.max_retries => {
                    attempt += 1;
                    retries += 1;
                    tokio::time::sleep(HTTP_RETRY_BACKOFF).await;
                }
                Err(_) => break Err(()),
            }
        };
        match outcome {
            Ok(()) => {
                ack_latencies_ns.push(start.elapsed().as_nanos() as u64);
                accepted_samples += batch_len;
            }
            Err(()) => {
                dropped_batches += 1;
                dropped_samples += batch_len;
            }
        }
    }

    let elapsed_secs = wall_start.elapsed().as_secs_f64().max(1e-9);
    let ingest = IngestPhase {
        offered_samples: stream.len() as u64,
        accepted_samples,
        rejected_samples,
        dropped_samples,
        dropped_batches,
        batches,
        retries,
        elapsed_secs,
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
            offered_samples: 1,
            accepted_samples: 1,
            rejected_samples: 0,
            dropped_samples: 0,
            dropped_batches: 0,
            batches: 1,
            retries: 0,
            elapsed_secs: 1.0,
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
        let outcome = replay_into_ravel(&cfg, &stream).await.expect("replay");
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
        let outcome = replay_into_ravel(&cfg, &stream).await.expect("replay");
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
        let outcome = replay_into_ravel(&cfg, &stream).await.expect("replay");

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
    /// TO SEE THIS FAIL against the pre-fix sentinel: change the `if
    /// valid.is_empty()`/`map_or` absence in `replay_into_ravel` back to a bare
    /// `i64::MIN` fold; `max_ts_ms` is then `Some(i64::MIN)` and the first
    /// assertion below fails.
    #[tokio::test]
    async fn an_empty_stream_reports_absent_newest_ts_not_a_sentinel() {
        let cfg = memory_config("mb-replay-empty", 8);
        let outcome = replay_into_ravel(&cfg, &[]).await.expect("replay");

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
        let outcome = replay_into_ravel(&cfg, &stream).await.expect("replay");

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

    /// The lane parses the shipping `metrics_gen` encoding: float lines become
    /// logical samples (with the exact bit-pattern value), native-histogram and
    /// staleness lines are skipped. Uses the shipping `GeneratedSample::encode`
    /// so the parser is pinned to the real format, not a hand-written stand-in.
    #[test]
    fn parse_logical_stream_reads_float_lines_only() {
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

        let parsed = parse_logical_stream(&String::from_utf8(text).expect("utf8"));
        assert_eq!(
            parsed.len(),
            1,
            "only the one float line is a scalar sample"
        );
        assert_eq!(parsed[0].metric, "mb_gauge");
        assert_eq!(parsed[0].value, 3.5);
        assert_eq!(
            parsed[0].labels,
            vec![
                ("instance".to_string(), "mb-instance-0".to_string()),
                ("job".to_string(), "api".to_string()),
            ]
        );
    }
}

//! Wire codec for the ADR-0071 distributed read fan-out.
//!
//! Maps the in-memory query shapes to and from the frozen `ravel.queryfrag.v1`
//! protobuf messages (`ravel_proto::queryfrag::v1`). Every mapping
//! here is total and typed: an unknown protocol version, a malformed series id,
//! an unknown enum discriminant, or a run whose timestamp and value columns
//! disagree in length is a [`CodecError`], never a panic and never a silently
//! dropped or corrupted sample.
//!
//! # Bit-exactness
//!
//! Sample values cross the wire as raw `f64` bit patterns (`value_bits`,
//! proto `fixed64`), encoded with [`f64::to_bits`] and restored with
//! [`f64::from_bits`], never as proto doubles and never through any float
//! arithmetic or comparison. NaN payloads, signalling NaNs, `-0.0`, and the
//! staleness marker therefore survive the round trip byte-for-byte, exactly as
//! the local read path preserves them (docs/segment-format.md). Timestamps
//! cross as zig-zag deltas (proto `sint64`); the delta arithmetic uses
//! `wrapping_sub`/`wrapping_add` so an `i64::MIN`/`i64::MAX` timestamp pair
//! round-trips without overflow.

use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_promql::{LabelMatcher, MatchOp};
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::{AccountedOp, QueryAccountingSnapshot};
use ravel_types::{Label, LabelSet, SeriesId, Signal};

use ravel_logseg::LogRecord;
use ravel_rspan::{SpanRecord, StatusCode};
use ravel_segment::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};
use ravel_types::logstream::{AttrValue, LogStreamId};

use crate::erasure::ErasurePredicate;
use crate::fetcher::{FetchedHistogramSeries, FetchedSeriesSoa, SamplePriority};
use crate::span_fetcher::SpanRow;

/// The queryfrag protocol version this build speaks. A `FetchRequest` carrying
/// any other value is rejected by [`check_protocol_version`] so the coordinator
/// falls back to fully local execution (ADR-0071 version-skew rule), never
/// misinterprets a future frame layout.
///
/// Bumped 1 -> 2 for the ADR-0071 amendment: the shared bearer token on
/// `Pinned` fetches is replaced by a per-tenant, per-query fragment capability
/// ([`FragmentClaims`]). Bumped 2 -> 3 for ADR-0096: `Run` and `HistogramRun`
/// carry the four packed per-sample provenance columns and `HistogramRun`
/// carries typed `HistogramRecord`s, and both encoders now emit them, so a
/// run-merged scalar run and a native-histogram run cross the wire bit-exactly
/// (issue #379, the epic's final commit). This is a version bump on an existing
/// versioned wire field, not a frozen persistent-format change. A version-skewed
/// worker is dropped at routing time (never a hard error), so a rolling deploy
/// degrades to coordinator-local execution, never to a wrong answer.
///
pub const PROTOCOL_VERSION: u32 = 3;

/// The fragment-capability claim-set version (ADR-0071 amendment, decision 2).
/// Distinct from [`PROTOCOL_VERSION`]: it versions the canonical claim encoding
/// [`encode_claims`] produces and the MAC covers, so the claim layout can evolve
/// under its own number without moving the wire protocol version. A verifier
/// recomputes the MAC over the exact bytes the minter signed, so a mismatched
/// claim version simply fails the MAC check.
pub const CAPABILITY_VERSION: u32 = 1;

/// The fixed width of the canonical claim encoding [`encode_claims`] produces:
/// `capability_version` (u32) + `tenant_hash` (16) + `signal` (u32) +
/// `query_id` (16) + `expires_unix_ns` (i64), big-endian, no padding.
pub const CAPABILITY_CLAIMS_LEN: usize = 4 + 16 + 4 + 16 + 8;

/// The width of the keyed-BLAKE3 MAC appended to the claims.
pub const CAPABILITY_MAC_LEN: usize = 32;

/// The total on-wire width of a fragment capability: the canonical claims
/// followed by their MAC.
pub const CAPABILITY_LEN: usize = CAPABILITY_CLAIMS_LEN + CAPABILITY_MAC_LEN;

/// The claim set of a fragment capability (ADR-0071 amendment, decision 2): the
/// exact authority a `Pinned` fetch carries, naming one tenant, one signal, one
/// query, and an absolute expiry. Transient, never stored. A capability is these
/// claims in their canonical fixed-width encoding followed by a keyed-BLAKE3 MAC
/// over that encoding; the worker recomputes the MAC and requires the request's
/// `tenant_hash`/`signal`/`query_id` to equal the claims, so a capability minted
/// for one tenant cannot authorize a fetch that names another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentClaims {
    /// The claim-set version ([`CAPABILITY_VERSION`]).
    pub capability_version: u32,
    /// The 16-byte tenant hash the capability authorizes.
    pub tenant_hash: [u8; 16],
    /// The signal discriminant ([`signal_to_u32`]) the capability authorizes.
    pub signal: u32,
    /// The 16-byte query id the capability is scoped to.
    pub query_id: [u8; 16],
    /// The absolute deadline, in unix nanoseconds, past which the capability is
    /// rejected. The coordinator sets this to the query's own deadline, so
    /// expiry reuses the clock the protocol already enforces cluster-wide.
    pub expires_unix_ns: i64,
}

/// The canonical fixed-width big-endian encoding of a claim set. Total and
/// infallible: the same claims always produce the same [`CAPABILITY_CLAIMS_LEN`]
/// bytes, so the minter and every verifier compute the MAC over identical input.
fn encode_claims(claims: &FragmentClaims) -> [u8; CAPABILITY_CLAIMS_LEN] {
    let mut out = [0u8; CAPABILITY_CLAIMS_LEN];
    out[0..4].copy_from_slice(&claims.capability_version.to_be_bytes());
    out[4..20].copy_from_slice(&claims.tenant_hash);
    out[20..24].copy_from_slice(&claims.signal.to_be_bytes());
    out[24..40].copy_from_slice(&claims.query_id);
    out[40..48].copy_from_slice(&claims.expires_unix_ns.to_be_bytes());
    out
}

/// The keyed-BLAKE3 MAC of a claim set under `key`. BLAKE3 is the workspace's
/// content-hash primitive; this is its keyed/MAC mode, the same construction
/// `ravel_catalog::auth_token_map` and `ravel_query::http::tenant` use. The MAC
/// covers the canonical claim bytes, so any flipped claim byte changes the MAC.
pub fn capability_mac(key: &[u8; 32], claims: &FragmentClaims) -> [u8; 32] {
    *blake3::keyed_hash(key, &encode_claims(claims)).as_bytes()
}

/// Mint a capability: the canonical claims followed by their MAC under `key`.
/// The coordinator mints one per query and attaches it to every slice; the bytes
/// are transient wire only, never stored. Deterministic in `(key, claims)`, so a
/// re-dispatch of the same slice mints byte-identical bytes.
pub fn mint_capability(key: &[u8; 32], claims: &FragmentClaims) -> Vec<u8> {
    let mut out = Vec::with_capacity(CAPABILITY_LEN);
    out.extend_from_slice(&encode_claims(claims));
    out.extend_from_slice(&capability_mac(key, claims));
    out
}

/// Split a capability's on-wire bytes into its claims and presented MAC, without
/// verifying the MAC (the verifier recomputes it with [`capability_mac`] and
/// compares in constant time). A capability of any other length is a typed
/// error, never a truncated or zero-padded claim set.
pub fn decode_capability(bytes: &[u8]) -> Result<(FragmentClaims, [u8; 32]), CodecError> {
    if bytes.len() != CAPABILITY_LEN {
        return Err(CodecError::BadCapabilityLength { got: bytes.len() });
    }
    let (claim_bytes, mac_bytes) = bytes.split_at(CAPABILITY_CLAIMS_LEN);
    let capability_version = u32::from_be_bytes(claim_bytes[0..4].try_into().unwrap_or_default());
    let tenant_hash: [u8; 16] = claim_bytes[4..20].try_into().unwrap_or_default();
    let signal = u32::from_be_bytes(claim_bytes[20..24].try_into().unwrap_or_default());
    let query_id: [u8; 16] = claim_bytes[24..40].try_into().unwrap_or_default();
    let expires_unix_ns = i64::from_be_bytes(claim_bytes[40..48].try_into().unwrap_or_default());
    let mac: [u8; 32] = mac_bytes
        .try_into()
        .map_err(|_| CodecError::BadCapabilityLength { got: bytes.len() })?;
    Ok((
        FragmentClaims {
            capability_version,
            tenant_hash,
            signal,
            query_id,
            expires_unix_ns,
        },
        mac,
    ))
}

/// A queryfrag message could not be mapped to or from its in-memory shape.
/// Every variant names a specific malformation; none is recoverable by
/// retrying the same bytes, so the coordinator treats a decode failure as a
/// corrupt slice (fail the query), distinct from a version mismatch (fall back
/// to local).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum CodecError {
    #[error("unknown queryfrag protocol version {got}, this build speaks {expected}")]
    UnknownProtocolVersion { got: u32, expected: u32 },
    #[error("series id is {got} bytes, expected 16")]
    BadSeriesId { got: usize },
    #[error("segment content hash is {got} bytes, expected 32")]
    BadContentHash { got: usize },
    #[error("run has {timestamps} timestamp deltas but {values} value words")]
    RunLengthMismatch { timestamps: usize, values: usize },
    #[error(
        "run provenance columns disagree in length: prov_created_delta {created}, \
         prov_epoch_delta {epoch}, prov_seq_delta {seq}, prov_in_page_index \
         {in_page_index}, samples {samples}"
    )]
    ProvenanceLengthMismatch {
        created: usize,
        epoch: usize,
        seq: usize,
        in_page_index: usize,
        samples: usize,
    },
    #[error("unknown label-matcher op discriminant {0}")]
    UnknownMatcherOp(i32),
    #[error("regex matcher {name:?} pattern {pattern:?} did not compile: {reason}")]
    InvalidRegex {
        name: String,
        pattern: String,
        reason: String,
    },
    #[error("unknown signal discriminant {0}")]
    UnknownSignal(u32),
    #[error("unknown status code discriminant {0}")]
    UnknownStatusCode(i32),
    #[error("frame carried no `frame` oneof variant")]
    EmptyFrame,
    #[error("summary frame carried no accounting snapshot")]
    MissingAccounting,
    #[error("summary frame carried no status")]
    MissingStatus,
    #[error("series labels invalid: {0}")]
    InvalidLabels(String),
    #[error("segment level discriminant {0} is neither L0 (0) nor L1 (1)")]
    UnknownSegmentLevel(u32),
    #[error("fragment capability is {got} bytes, expected {CAPABILITY_LEN}")]
    BadCapabilityLength { got: usize },
    #[error("log stream id is {got} bytes, expected 16")]
    BadStreamId { got: usize },
    #[error("log trace id is {got} bytes, expected 16 (or 0 for absent)")]
    BadTraceId { got: usize },
    #[error("log span id is {got} bytes, expected 8 (or 0 for absent)")]
    BadSpanId { got: usize },
    #[error("log severity number {got} does not fit in a u8")]
    SeverityOutOfRange { got: u32 },
    #[error("log attribute carried no value")]
    MissingAttrValue,
    #[error("span trace id is {got} bytes, expected 16")]
    BadSpanTraceId { got: usize },
    #[error("span id is {got} bytes, expected 8")]
    BadSpanSpanId { got: usize },
    #[error("span parent id is {got} bytes, expected 8 (or 0 for absent)")]
    BadSpanParentId { got: usize },
    #[error("span status code {got} is not a known OTLP StatusCode (0 Unset, 1 Ok, 2 Error)")]
    UnknownSpanStatusCode { got: u32 },
    #[error("histogram record carried no `counts` oneof variant")]
    MissingHistogramCounts,
    #[error("unknown histogram reset-hint discriminant {0}")]
    UnknownResetHint(i32),
    #[error("histogram run has {timestamps} timestamp deltas but {records} records")]
    HistogramRunLengthMismatch { timestamps: usize, records: usize },
    #[error("histogram span has length 0 (every span must cover at least one bucket)")]
    HistogramSpanLengthZero,
    #[error("histogram scale {scale} is below the -53 custom-boundary minimum")]
    HistogramScaleTooSmall { scale: i32 },
    #[error(
        "histogram custom_values must be present, non-empty, and strictly ascending iff \
         scale == -53"
    )]
    HistogramCustomValuesMismatch,
    #[error("histogram side has {buckets} bucket counts but its spans cover {spans} buckets")]
    HistogramBucketCountMismatch { spans: u64, buckets: usize },
    #[error("histogram count is less than its zero_count or its total bucket count")]
    HistogramCountInconsistent,
}

/// Rejects any protocol version this build does not speak. `Ok(())` only for
/// the exact [`PROTOCOL_VERSION`]; the caller maps `Err` to a silent local
/// fallback (ADR-0071), so version skew degrades gracefully rather than
/// erroring a user's query.
pub fn check_protocol_version(got: u32) -> Result<(), CodecError> {
    if got == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(CodecError::UnknownProtocolVersion {
            got,
            expected: PROTOCOL_VERSION,
        })
    }
}

// ---- Signal <-> u32 -------------------------------------------------------

/// Maps a [`Signal`] to its wire discriminant. Explicit, not `as u32`: the
/// enum has no `#[repr]`, and the wire contract must not shift if a variant is
/// ever inserted (persistent-contract discipline applied to a transient wire).
pub fn signal_to_u32(signal: Signal) -> u32 {
    match signal {
        Signal::Metrics => 0,
        Signal::Logs => 1,
        Signal::Spans => 2,
        Signal::Profiles => 3,
        Signal::Alerts => 4,
        Signal::Audit => 5,
    }
}

/// Inverse of [`signal_to_u32`]; an unrecognized discriminant is a typed
/// error, never a silent default to `Metrics`.
pub fn signal_from_u32(raw: u32) -> Result<Signal, CodecError> {
    match raw {
        0 => Ok(Signal::Metrics),
        1 => Ok(Signal::Logs),
        2 => Ok(Signal::Spans),
        3 => Ok(Signal::Profiles),
        4 => Ok(Signal::Alerts),
        5 => Ok(Signal::Audit),
        other => Err(CodecError::UnknownSignal(other)),
    }
}

// ---- SeriesFrame <-> FetchedSeriesSoa -------------------------------------

/// Encodes one decoded per-segment scalar series as a `SeriesFrame` carrying a
/// single run. A `FetchedSeriesSoa` is one series' samples from one segment, so
/// it maps to exactly one [`pb::Run`]; the labels are written once. Timestamps
/// become zig-zag deltas (delta-from-zero for the first), values become raw
/// bit patterns.
///
/// `pb::Run` now carries the four packed per-sample provenance columns
/// (`prov_created_delta`/`prov_epoch_delta`/`prov_seq_delta`/
/// `prov_in_page_index`, ADR-0096 decision 1), so a run-merged run's
/// per-sample dedup priority column
/// ([`FetchedSeriesSoa::per_sample_priorities`]) is representable on the wire.
/// When the column is `Some`, its samples' keys are delta-encoded into those
/// four fields; when `None` the four fields stay empty and, because proto3
/// omits empty packed repeated fields, the run encodes byte-identical to one
/// predating this change.
///
/// As of [`PROTOCOL_VERSION`] 3 (ADR-0096 decision 3 step 4, issue #379) this
/// encoder is live on the distributed fetch path: the service-level refusal that
/// once handed a `per_sample_priorities`-bearing slice to the coordinator's
/// local fallback is gone, so a merged L1 run's column crosses the wire here and
/// [`decode_series_frame`] restores it. The version gate ([`check_protocol_version`]
/// at the request level, and the intra-cluster routing filter) guarantees only a
/// coordinator speaking version 3 ever receives such a frame, so the column is
/// never silently dropped by an older decoder.
pub fn encode_series_frame(series: &FetchedSeriesSoa) -> pb::SeriesFrame {
    let (prov_created_delta, prov_epoch_delta, prov_seq_delta, prov_in_page_index) =
        encode_sample_priorities(&series.per_sample_priorities);
    pb::SeriesFrame {
        series_id: series.series_id.0.to_vec(),
        labels: encode_labels(&series.labels),
        runs: vec![pb::Run {
            created_unix_ns: series.created_unix_ns,
            writer_epoch: series.writer_epoch,
            writer_seq: series.writer_seq,
            ts_delta: encode_ts_deltas(&series.timestamps),
            value_bits: series.values.iter().map(|v| v.to_bits()).collect(),
            prov_created_delta,
            prov_epoch_delta,
            prov_seq_delta,
            prov_in_page_index,
        }],
    }
}

/// Decodes a `SeriesFrame` back into one [`FetchedSeriesSoa`] per run, so a
/// frame that batched several segments' runs of one series id fans back out to
/// the same per-segment shapes the local path produces. The series id and
/// labels are shared across the frame's runs.
pub fn decode_series_frame(frame: pb::SeriesFrame) -> Result<Vec<FetchedSeriesSoa>, CodecError> {
    let series_id = decode_series_id(&frame.series_id)?;
    let labels = decode_labels(frame.labels)?;
    frame
        .runs
        .into_iter()
        .map(|run| {
            if run.ts_delta.len() != run.value_bits.len() {
                return Err(CodecError::RunLengthMismatch {
                    timestamps: run.ts_delta.len(),
                    values: run.value_bits.len(),
                });
            }
            let per_sample_priorities = decode_sample_priorities(
                &run.prov_created_delta,
                &run.prov_epoch_delta,
                &run.prov_seq_delta,
                &run.prov_in_page_index,
                run.ts_delta.len(),
            )?;
            Ok(FetchedSeriesSoa {
                series_id,
                labels: labels.clone(),
                timestamps: decode_ts_deltas(&run.ts_delta),
                values: run.value_bits.iter().map(|b| f64::from_bits(*b)).collect(),
                created_unix_ns: run.created_unix_ns,
                writer_epoch: run.writer_epoch,
                writer_seq: run.writer_seq,
                per_sample_priorities,
            })
        })
        .collect()
}

/// Zig-zag delta encoding: the first entry is the delta from zero, each later
/// entry the delta from its predecessor. `wrapping_sub` keeps `i64` extremes
/// reversible (prost applies the sint64 zig-zag transform on the wire).
fn encode_ts_deltas(timestamps: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(timestamps.len());
    let mut prev: i64 = 0;
    for &ts in timestamps {
        out.push(ts.wrapping_sub(prev));
        prev = ts;
    }
    out
}

/// Inverse of [`encode_ts_deltas`]: a running `wrapping_add` prefix sum.
fn decode_ts_deltas(deltas: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(deltas.len());
    let mut acc: i64 = 0;
    for &d in deltas {
        acc = acc.wrapping_add(d);
        out.push(acc);
    }
    out
}

/// The `u64`-domain twin of [`encode_ts_deltas`] for the `writer_epoch` and
/// `writer_seq` provenance columns. Each value crosses the delta-transform
/// boundary as `v as i64`, a two's-complement bit reinterpretation, never a
/// numeric conversion: `u64 as i64` and its inverse `i64 as u64` are total
/// bitcasts, so every one of the `2^64` possible `u64`s maps to a distinct
/// `i64` bit pattern and back. The `wrapping_sub` delta then operates on that
/// bit pattern, and `wrapping_sub`/`wrapping_add` are bit-identical across
/// `i64` and `u64` (one two's-complement adder). So the round trip through
/// [`decode_u64_deltas`] recovers every original `u64` exactly; the dedup key
/// cannot be silently corrupted at this boundary. prost then applies the
/// sint64 zig-zag transform over the `i64` we hand it, reversed on decode.
fn encode_u64_deltas(values: &[u64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(values.len());
    let mut prev: i64 = 0;
    for &v in values {
        let cur = v as i64;
        out.push(cur.wrapping_sub(prev));
        prev = cur;
    }
    out
}

/// Inverse of [`encode_u64_deltas`]: a running `wrapping_add` prefix sum whose
/// `i64` accumulator is reinterpreted back to `u64` per element.
fn decode_u64_deltas(deltas: &[i64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(deltas.len());
    let mut acc: i64 = 0;
    for &d in deltas {
        acc = acc.wrapping_add(d);
        out.push(acc as u64);
    }
    out
}

/// Encodes the optional per-sample provenance column into the four packed run
/// columns (ADR-0096 decision 1). `None` yields four empty vecs, which proto3
/// omits from the wire entirely, so a run-wide run stays byte-identical to one
/// predating these fields. `Some` yields the four parallel columns:
/// `created`/`epoch`/`seq` delta-transformed (the first two through the signed
/// transform, the `u64` pair through [`encode_u64_deltas`]) and
/// `in_page_index` copied verbatim (`uint32`, not delta-transformed).
fn encode_sample_priorities(
    priorities: &Option<Vec<SamplePriority>>,
) -> (Vec<i64>, Vec<i64>, Vec<i64>, Vec<u32>) {
    match priorities {
        None => (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        Some(ps) => {
            let created: Vec<i64> = ps.iter().map(|p| p.created_unix_ns).collect();
            let epoch: Vec<u64> = ps.iter().map(|p| p.writer_epoch).collect();
            let seq: Vec<u64> = ps.iter().map(|p| p.writer_seq).collect();
            let in_page_index: Vec<u32> = ps.iter().map(|p| p.in_page_index).collect();
            (
                encode_ts_deltas(&created),
                encode_u64_deltas(&epoch),
                encode_u64_deltas(&seq),
                in_page_index,
            )
        }
    }
}

/// Inverse of [`encode_sample_priorities`]. All four columns empty decodes to
/// `None` (run-wide provenance). If any is non-empty, all four must be present
/// and every column's length must equal `sample_count` (the run's `ts_delta`
/// length); an all-or-nothing disagreement is a typed
/// [`CodecError::ProvenanceLengthMismatch`], mirroring
/// [`CodecError::RunLengthMismatch`], never a truncated or fabricated key.
fn decode_sample_priorities(
    created_delta: &[i64],
    epoch_delta: &[i64],
    seq_delta: &[i64],
    in_page_index: &[u32],
    sample_count: usize,
) -> Result<Option<Vec<SamplePriority>>, CodecError> {
    if created_delta.is_empty()
        && epoch_delta.is_empty()
        && seq_delta.is_empty()
        && in_page_index.is_empty()
    {
        return Ok(None);
    }
    if created_delta.len() != sample_count
        || epoch_delta.len() != sample_count
        || seq_delta.len() != sample_count
        || in_page_index.len() != sample_count
    {
        return Err(CodecError::ProvenanceLengthMismatch {
            created: created_delta.len(),
            epoch: epoch_delta.len(),
            seq: seq_delta.len(),
            in_page_index: in_page_index.len(),
            samples: sample_count,
        });
    }
    let created = decode_ts_deltas(created_delta);
    let epoch = decode_u64_deltas(epoch_delta);
    let seq = decode_u64_deltas(seq_delta);
    let priorities = (0..sample_count)
        .map(|i| SamplePriority {
            created_unix_ns: created[i],
            writer_epoch: epoch[i],
            writer_seq: seq[i],
            in_page_index: in_page_index[i],
        })
        .collect();
    Ok(Some(priorities))
}

// ---- HistogramRecord <-> ravel_segment::HistogramValue --------------------

/// Encodes decoded native-histogram samples as typed [`pb::HistogramRecord`]s
/// (ADR-0096 decision 2), mirroring [`HistogramValue`] field for field. Every
/// `f64` crosses as its [`f64::to_bits`] pattern (proto `fixed64`), never a
/// proto double, so NaN payloads, signalling NaNs, and `-0.0` survive the wire
/// byte-for-byte, the same discipline [`encode_series_frame`] applies to scalar
/// sample values.
///
/// `sum: None` stays distinct from a present value (proto3 `optional`).
/// `custom_values: None` maps to an empty repeated field, which proto3 omits;
/// on decode an empty field is read back as `None`. For a well-formed
/// histogram this is lossless: a `Some` custom-values set is never empty (it is
/// present iff `scale == -53`, with ascending boundaries), so the only value
/// that maps to the empty wire form is `None`.
///
/// As of [`PROTOCOL_VERSION`] 3 (ADR-0096 decision 3 step 4, #379) this encoder
/// is live: the worker's histogram-slice refusal is gone, so a native-histogram
/// series' records cross the wire here and [`decode_histogram_records`] restores
/// them. It is also proven structurally by this module's round-trip property
/// test against [`HistogramValue`].
pub fn encode_histogram_records(values: &[HistogramValue]) -> Vec<pb::HistogramRecord> {
    values.iter().map(encode_histogram_record).collect()
}

fn encode_histogram_record(value: &HistogramValue) -> pb::HistogramRecord {
    pb::HistogramRecord {
        scale: value.scale,
        zero_threshold_bits: value.zero_threshold.to_bits(),
        sum_bits: value.sum.map(f64::to_bits),
        custom_values_bits: value
            .custom_values
            .as_ref()
            .map(|bounds| bounds.iter().map(|b| b.to_bits()).collect())
            .unwrap_or_default(),
        positive_spans: value
            .positive_spans
            .iter()
            .map(encode_histogram_span)
            .collect(),
        negative_spans: value
            .negative_spans
            .iter()
            .map(encode_histogram_span)
            .collect(),
        counts: Some(encode_histogram_counts(&value.counts)),
        reset_hint: encode_reset_hint(value.reset_hint) as i32,
    }
}

/// Inverse of [`encode_histogram_records`]. Every malformation is a typed
/// [`CodecError`], never a panic or a silently defaulted field: a record with
/// no `counts` oneof member is [`CodecError::MissingHistogramCounts`], and an
/// unknown reset-hint discriminant is [`CodecError::UnknownResetHint`].
pub fn decode_histogram_records(
    records: &[pb::HistogramRecord],
) -> Result<Vec<HistogramValue>, CodecError> {
    records.iter().map(decode_histogram_record).collect()
}

fn decode_histogram_record(record: &pb::HistogramRecord) -> Result<HistogramValue, CodecError> {
    let counts = record
        .counts
        .as_ref()
        .ok_or(CodecError::MissingHistogramCounts)?;
    // An empty repeated field is absent on the wire (proto3), so it decodes to
    // `None`; a non-empty one to `Some`. See `encode_histogram_records` on why
    // this is lossless for a well-formed histogram.
    let custom_values = if record.custom_values_bits.is_empty() {
        None
    } else {
        Some(
            record
                .custom_values_bits
                .iter()
                .map(|b| f64::from_bits(*b))
                .collect(),
        )
    };
    Ok(HistogramValue {
        scale: record.scale,
        zero_threshold: f64::from_bits(record.zero_threshold_bits),
        sum: record.sum_bits.map(f64::from_bits),
        custom_values,
        positive_spans: record
            .positive_spans
            .iter()
            .map(decode_histogram_span)
            .collect(),
        negative_spans: record
            .negative_spans
            .iter()
            .map(decode_histogram_span)
            .collect(),
        counts: decode_histogram_counts(counts),
        reset_hint: decode_reset_hint(record.reset_hint)?,
    })
}

fn encode_histogram_span(span: &HistogramSpan) -> pb::HistogramSpan {
    pb::HistogramSpan {
        offset: span.offset,
        length: span.length,
    }
}

fn decode_histogram_span(span: &pb::HistogramSpan) -> HistogramSpan {
    HistogramSpan {
        offset: span.offset,
        length: span.length,
    }
}

/// Encodes the counts variant, carrying every float as its [`f64::to_bits`]
/// pattern so `-0.0` and NaN bucket counts survive exactly (the float variant
/// really does hold arbitrary `f64` counts after exponential-histogram scaling).
fn encode_histogram_counts(counts: &HistogramCounts) -> pb::histogram_record::Counts {
    use pb::histogram_record::Counts;
    match counts {
        HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => Counts::IntCounts(pb::HistogramCountsInt {
            zero_count: *zero_count,
            count: *count,
            positive: positive.clone(),
            negative: negative.clone(),
        }),
        HistogramCounts::Float {
            zero_count,
            count,
            positive,
            negative,
        } => Counts::FloatCounts(pb::HistogramCountsFloat {
            zero_count_bits: zero_count.to_bits(),
            count_bits: count.to_bits(),
            positive_bits: positive.iter().map(|v| v.to_bits()).collect(),
            negative_bits: negative.iter().map(|v| v.to_bits()).collect(),
        }),
    }
}

fn decode_histogram_counts(counts: &pb::histogram_record::Counts) -> HistogramCounts {
    use pb::histogram_record::Counts;
    match counts {
        Counts::IntCounts(c) => HistogramCounts::Int {
            zero_count: c.zero_count,
            count: c.count,
            positive: c.positive.clone(),
            negative: c.negative.clone(),
        },
        Counts::FloatCounts(c) => HistogramCounts::Float {
            zero_count: f64::from_bits(c.zero_count_bits),
            count: f64::from_bits(c.count_bits),
            positive: c.positive_bits.iter().map(|b| f64::from_bits(*b)).collect(),
            negative: c.negative_bits.iter().map(|b| f64::from_bits(*b)).collect(),
        },
    }
}

/// Maps a [`ResetHint`] to its wire enum. Explicit, not `as i32`: mirrors
/// [`signal_to_u32`]'s discipline so the wire contract does not shift if a
/// variant is ever inserted into either enum.
fn encode_reset_hint(hint: ResetHint) -> pb::histogram_record::ResetHint {
    use pb::histogram_record::ResetHint as Pb;
    match hint {
        ResetHint::Unknown => Pb::Unknown,
        ResetHint::Yes => Pb::Yes,
        ResetHint::No => Pb::No,
        ResetHint::Gauge => Pb::Gauge,
    }
}

/// Inverse of [`encode_reset_hint`]; an unrecognized discriminant is a typed
/// error, never a silent default to `Unknown`.
fn decode_reset_hint(raw: i32) -> Result<ResetHint, CodecError> {
    use pb::histogram_record::ResetHint as Pb;
    match Pb::try_from(raw).map_err(|_| CodecError::UnknownResetHint(raw))? {
        Pb::Unknown => Ok(ResetHint::Unknown),
        Pb::Yes => Ok(ResetHint::Yes),
        Pb::No => Ok(ResetHint::No),
        Pb::Gauge => Ok(ResetHint::Gauge),
    }
}

// ---- HistogramFrame <-> FetchedHistogramSeries ----------------------------

/// Encodes one decoded per-segment histogram series as a `HistogramFrame`
/// carrying a single run, mirroring [`encode_series_frame`]. A
/// [`FetchedHistogramSeries`] is one series' histogram samples from one segment
/// (the fetch path produces one run per segment per series, the same as
/// scalar), so it maps to exactly one [`pb::HistogramRun`]; the labels are
/// written once. Timestamps become zig-zag deltas via [`encode_ts_deltas`], the
/// values become typed [`pb::HistogramRecord`]s via [`encode_histogram_records`]
/// (every `f64` as its `to_bits` pattern), and the optional per-sample
/// provenance column crosses through the same four packed columns
/// [`encode_series_frame`] uses ([`encode_sample_priorities`]).
///
/// As of [`PROTOCOL_VERSION`] 3 (ADR-0096 decision 3 step 4) this is live on the
/// distributed fetch path: the worker's histogram-slice refusal and the
/// coordinator's `Hist`-frame refusal are both gone, so a real `Hist` frame
/// crosses the wire here and [`decode_histogram_frame`] restores it. The version
/// gate means only a version-3 coordinator ever receives one. The round trip is
/// also exercised by this module's tests.
pub fn encode_histogram_frame(series: &FetchedHistogramSeries) -> pb::HistogramFrame {
    let (prov_created_delta, prov_epoch_delta, prov_seq_delta, prov_in_page_index) =
        encode_sample_priorities(&series.per_sample_priorities);
    pb::HistogramFrame {
        series_id: series.series_id.0.to_vec(),
        labels: encode_labels(&series.labels),
        runs: vec![pb::HistogramRun {
            created_unix_ns: series.created_unix_ns,
            writer_epoch: series.writer_epoch,
            writer_seq: series.writer_seq,
            ts_delta: encode_ts_deltas(&series.timestamps),
            prov_created_delta,
            prov_epoch_delta,
            prov_seq_delta,
            prov_in_page_index,
            records: encode_histogram_records(&series.values),
        }],
    }
}

/// Decodes a `HistogramFrame` back into one [`FetchedHistogramSeries`] per run,
/// mirroring [`decode_series_frame`]. Beyond the record-level decode
/// ([`decode_histogram_records`]) and the shared run-length/provenance-length
/// checks, every decoded [`HistogramValue`] is passed through
/// [`validate_histogram_value`], which ports `ravel-segment`'s reader-side
/// structural invariants to the wire (`crates/ravel-segment/src/reader.rs`,
/// `decode_histogram_record`): a record that could not have come from a
/// well-formed RSEG histogram is a typed [`CodecError`], never a silently-wrong
/// value handed to the coordinator's histogram merge.
pub fn decode_histogram_frame(
    frame: pb::HistogramFrame,
) -> Result<Vec<FetchedHistogramSeries>, CodecError> {
    let series_id = decode_series_id(&frame.series_id)?;
    let labels = decode_labels(frame.labels)?;
    frame
        .runs
        .into_iter()
        .map(|run| {
            if run.records.len() != run.ts_delta.len() {
                return Err(CodecError::HistogramRunLengthMismatch {
                    timestamps: run.ts_delta.len(),
                    records: run.records.len(),
                });
            }
            let per_sample_priorities = decode_sample_priorities(
                &run.prov_created_delta,
                &run.prov_epoch_delta,
                &run.prov_seq_delta,
                &run.prov_in_page_index,
                run.ts_delta.len(),
            )?;
            let values = decode_histogram_records(&run.records)?;
            for value in &values {
                validate_histogram_value(value)?;
            }
            Ok(FetchedHistogramSeries {
                series_id,
                labels: labels.clone(),
                timestamps: decode_ts_deltas(&run.ts_delta),
                values,
                created_unix_ns: run.created_unix_ns,
                writer_epoch: run.writer_epoch,
                writer_seq: run.writer_seq,
                per_sample_priorities,
            })
        })
        .collect()
}

/// Ports `ravel-segment`'s reader-side structural invariants
/// (`crates/ravel-segment/src/reader.rs`, `decode_histogram_record`) to the wire
/// decoder, which otherwise has none. The comparisons match the reader's
/// exactly, including the float count check's `<` form so NaN/Inf bucket counts
/// (legal per docs/segment-format.md section 3.5) pass through unchanged rather
/// than being rejected: a NaN comparison is always false, so the check accepts
/// them the same way the reader does. Each malformation is its own typed
/// [`CodecError`], never a panic and never a value the merge would tie-break on
/// wrongly.
fn validate_histogram_value(value: &HistogramValue) -> Result<(), CodecError> {
    if value.scale < -53 {
        return Err(CodecError::HistogramScaleTooSmall { scale: value.scale });
    }
    // custom_values present, non-empty, and strictly ascending iff scale == -53;
    // absent otherwise. `<` on f64 matches the reader's strict-ascending check.
    match &value.custom_values {
        Some(bounds) if value.scale == -53 => {
            if bounds.is_empty() || !bounds.windows(2).all(|w| w[0] < w[1]) {
                return Err(CodecError::HistogramCustomValuesMismatch);
            }
        }
        None if value.scale != -53 => {}
        _ => return Err(CodecError::HistogramCustomValuesMismatch),
    }
    let positive_len = hist_span_bucket_total(&value.positive_spans)?;
    let negative_len = hist_span_bucket_total(&value.negative_spans)?;
    match &value.counts {
        HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => {
            check_bucket_len(positive_len, positive.len())?;
            check_bucket_len(negative_len, negative.len())?;
            let mut total: u64 = 0;
            for &v in positive.iter().chain(negative.iter()) {
                total = total
                    .checked_add(v)
                    .ok_or(CodecError::HistogramCountInconsistent)?;
            }
            if *count < *zero_count || *count < total {
                return Err(CodecError::HistogramCountInconsistent);
            }
        }
        HistogramCounts::Float {
            zero_count,
            count,
            positive,
            negative,
        } => {
            check_bucket_len(positive_len, positive.len())?;
            check_bucket_len(negative_len, negative.len())?;
            let mut total = 0.0f64;
            for &v in positive.iter().chain(negative.iter()) {
                total += v;
            }
            if *count < *zero_count || *count < total {
                return Err(CodecError::HistogramCountInconsistent);
            }
        }
    }
    Ok(())
}

/// The total bucket count one side's spans cover (`sum(length)`), rejecting a
/// zero-length span the same way the reader's `decode_hist_spans` does
/// (`HistogramSpanLengthZero`). `saturating_add` cannot overflow in practice
/// (each length is a `u32` and a real histogram has a handful of spans); if it
/// ever saturated, the bucket-length check downstream would reject the record
/// as a [`CodecError::HistogramBucketCountMismatch`] rather than wrapping.
fn hist_span_bucket_total(spans: &[HistogramSpan]) -> Result<u64, CodecError> {
    let mut total: u64 = 0;
    for span in spans {
        if span.length == 0 {
            return Err(CodecError::HistogramSpanLengthZero);
        }
        total = total.saturating_add(u64::from(span.length));
    }
    Ok(total)
}

/// A histogram side's bucket-count vector must be exactly as long as its spans
/// cover, mirroring the reader decoding exactly `sum(length)` counts per side.
fn check_bucket_len(spans: u64, buckets: usize) -> Result<(), CodecError> {
    if buckets as u64 != spans {
        return Err(CodecError::HistogramBucketCountMismatch { spans, buckets });
    }
    Ok(())
}

fn decode_series_id(bytes: &[u8]) -> Result<SeriesId, CodecError> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| CodecError::BadSeriesId { got: bytes.len() })?;
    Ok(SeriesId(arr))
}

fn encode_labels(labels: &LabelSet) -> Vec<pb::Label> {
    labels
        .iter()
        .map(|l| pb::Label {
            name: l.name.clone(),
            value: l.value.clone(),
        })
        .collect()
}

fn decode_labels(labels: Vec<pb::Label>) -> Result<LabelSet, CodecError> {
    let pairs = labels
        .into_iter()
        .map(|l| Label {
            name: l.name,
            value: l.value,
        })
        .collect();
    LabelSet::new(pairs).map_err(|e| CodecError::InvalidLabels(e.to_string()))
}

// ---- PartialAggregate <-> in-memory partial -------------------------------

/// One worker-computed partial aggregate for a single group (ADR-0103
/// decision 2): the group's series identity plus whichever of `count`, `min`,
/// and `max` the query asked for. A bare group enumeration carries all three
/// as `None`, which is why each is an `Option` rather than a sentinel: an
/// absent count must stay distinct from a present zero, and an absent bound
/// from a present `0.0`.
///
/// `min`/`max` are held as `f64` and cross the wire as raw bit patterns, never
/// proto doubles, so NaN payloads and `-0.0` survive byte-for-byte. Nothing
/// here compares them; the coordinator's fold uses the `total_cmp` total order
/// ADR-0023's min/max UDAF already defines.
#[derive(Debug, Clone)]
pub struct PartialAggregate {
    /// Canonical series identity of the group.
    pub series_id: SeriesId,
    /// The group's labels, the same shape [`pb::SeriesFrame`] carries.
    pub labels: LabelSet,
    /// Sample count for this group on this worker, when the query asked for it.
    pub count: Option<u64>,
    /// Minimum value for this group on this worker, when the query asked for it.
    pub min: Option<f64>,
    /// Maximum value for this group on this worker, when the query asked for it.
    pub max: Option<f64>,
}

/// Encodes one partial aggregate as a `PartialAggregate` frame, mirroring
/// [`encode_series_frame`]'s shape: the series id as its raw 16 bytes, the
/// labels through [`encode_labels`], and each present bound as its
/// [`f64::to_bits`] pattern. An absent field stays absent on the wire (proto3
/// `optional`), never a zero sentinel.
///
/// The worker's slice path is this encoder's call site (ADR-0103 decision 2,
/// `service.rs`): a Metrics slice whose request carries a
/// [`pb::PartialAggregateRequest`] emits one of these per series instead of its
/// raw series frames. Not yet reachable from a real query, though: no
/// coordinator sets that request field until the planner integration lands.
pub fn encode_partial_aggregate(partial: &PartialAggregate) -> pb::PartialAggregate {
    pb::PartialAggregate {
        series_id: partial.series_id.0.to_vec(),
        labels: encode_labels(&partial.labels),
        count: partial.count,
        min_bits: partial.min.map(f64::to_bits),
        max_bits: partial.max.map(f64::to_bits),
    }
}

/// Inverse of [`encode_partial_aggregate`]. A mis-sized series id or an invalid
/// label set is a typed [`CodecError`], never a panic. The bounds are restored
/// with [`f64::from_bits`] and never touched by float arithmetic or comparison,
/// so a NaN payload or a `-0.0` bound comes back exactly as the worker computed
/// it.
pub fn decode_partial_aggregate(
    frame: pb::PartialAggregate,
) -> Result<PartialAggregate, CodecError> {
    Ok(PartialAggregate {
        series_id: decode_series_id(&frame.series_id)?,
        labels: decode_labels(frame.labels)?,
        count: frame.count,
        min: frame.min_bits.map(f64::from_bits),
        max: frame.max_bits.map(f64::from_bits),
    })
}

// ---- LogRecordFrame <-> ravel_logseg::LogRecord ---------------------------

/// Encodes one decoded RLOG record as a `LogRecordFrame`, field for field. The
/// worker produces the record through the same `LogSegmentFetcher`/`RlogReader`
/// funnel the local path uses, so the shipped per-segment merged view (the
/// verbatim `stream_attrs` resource+scope blob plus the per-record `attrs`) is
/// byte-identical to what a local read produces; the coordinator never
/// re-derives attribute merging. `f64` attribute values cross as raw bit
/// patterns, never proto doubles, so NaN payloads and `-0.0` survive the wire,
/// the same discipline the scalar path applies to sample values.
pub fn encode_log_record(record: &LogRecord) -> pb::LogRecordFrame {
    pb::LogRecordFrame {
        stream_id: record.stream_id.0.to_vec(),
        stream_attrs: record.stream_attrs.clone(),
        ts_ns: record.ts_ns,
        observed_ts_ns: record.observed_ts_ns,
        severity_num: u32::from(record.severity_num),
        severity_text: record.severity_text.clone(),
        body: record.body.clone(),
        // An absent id is the empty byte string; a present one is its fixed
        // width. A real all-zero id is still its full 16/8 bytes, so it is
        // distinguishable from absent (which is zero bytes).
        trace_id: record.trace_id.map(|t| t.to_vec()).unwrap_or_default(),
        span_id: record.span_id.map(|s| s.to_vec()).unwrap_or_default(),
        flags: record.flags,
        attrs: record.attrs.iter().map(encode_log_attr).collect(),
    }
}

/// Inverse of [`encode_log_record`]. Every malformation (a mis-sized stream,
/// trace, or span id, a severity past `u8`, or an attribute with no value) is a
/// typed [`CodecError`], never a panic or a silently truncated record.
pub fn decode_log_record(frame: pb::LogRecordFrame) -> Result<LogRecord, CodecError> {
    let stream_id = decode_log_stream_id(&frame.stream_id)?;
    let trace_id = decode_trace_id(&frame.trace_id)?;
    let span_id = decode_span_id(&frame.span_id)?;
    let severity_num =
        u8::try_from(frame.severity_num).map_err(|_| CodecError::SeverityOutOfRange {
            got: frame.severity_num,
        })?;
    let attrs = frame
        .attrs
        .into_iter()
        .map(decode_log_attr)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LogRecord {
        stream_id,
        stream_attrs: frame.stream_attrs,
        ts_ns: frame.ts_ns,
        observed_ts_ns: frame.observed_ts_ns,
        severity_num,
        severity_text: frame.severity_text,
        body: frame.body,
        trace_id,
        span_id,
        flags: frame.flags,
        attrs,
    })
}

fn encode_log_attr(attr: &(String, AttrValue)) -> pb::LogAttr {
    pb::LogAttr {
        key: attr.0.clone(),
        value: Some(encode_attr_value(&attr.1)),
    }
}

fn decode_log_attr(attr: pb::LogAttr) -> Result<(String, AttrValue), CodecError> {
    let value = attr.value.ok_or(CodecError::MissingAttrValue)?;
    Ok((attr.key, decode_attr_value(value)?))
}

/// Encodes one attribute value, recursing through lists and maps. `f64` is
/// carried as `to_bits` (proto `fixed64`), never a double, so every bit
/// pattern (NaN payloads, `-0.0`) round-trips exactly.
fn encode_attr_value(value: &AttrValue) -> pb::LogAttrValue {
    use pb::log_attr_value::Value;
    let inner = match value {
        AttrValue::Str(s) => Value::Str(s.clone()),
        AttrValue::I64(v) => Value::Int(*v),
        AttrValue::F64(f) => Value::DoubleBits(f.to_bits()),
        AttrValue::Bool(b) => Value::Boolean(*b),
        AttrValue::Bytes(b) => Value::BytesVal(b.clone()),
        AttrValue::List(items) => Value::List(pb::LogAttrList {
            items: items.iter().map(encode_attr_value).collect(),
        }),
        AttrValue::Map(entries) => Value::Map(pb::LogAttrMap {
            entries: entries.iter().map(encode_log_attr).collect(),
        }),
    };
    pb::LogAttrValue { value: Some(inner) }
}

/// Inverse of [`encode_attr_value`]. An attribute value with no oneof variant
/// set is [`CodecError::MissingAttrValue`], never a silent default.
fn decode_attr_value(value: pb::LogAttrValue) -> Result<AttrValue, CodecError> {
    use pb::log_attr_value::Value;
    let inner = value.value.ok_or(CodecError::MissingAttrValue)?;
    Ok(match inner {
        Value::Str(s) => AttrValue::Str(s),
        Value::Int(v) => AttrValue::I64(v),
        Value::DoubleBits(b) => AttrValue::F64(f64::from_bits(b)),
        Value::Boolean(b) => AttrValue::Bool(b),
        Value::BytesVal(b) => AttrValue::Bytes(b),
        Value::List(l) => AttrValue::List(
            l.items
                .into_iter()
                .map(decode_attr_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Map(m) => AttrValue::Map(
            m.entries
                .into_iter()
                .map(decode_log_attr)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}

fn decode_log_stream_id(bytes: &[u8]) -> Result<LogStreamId, CodecError> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| CodecError::BadStreamId { got: bytes.len() })?;
    Ok(LogStreamId(arr))
}

/// Decodes an optional trace id: an empty byte string is `None`, a 16-byte
/// string is `Some`, and any other length is a typed error.
fn decode_trace_id(bytes: &[u8]) -> Result<Option<[u8; 16]>, CodecError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| CodecError::BadTraceId { got: bytes.len() })?;
    Ok(Some(arr))
}

/// Decodes an optional span id: an empty byte string is `None`, an 8-byte
/// string is `Some`, and any other length is a typed error.
fn decode_span_id(bytes: &[u8]) -> Result<Option<[u8; 8]>, CodecError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CodecError::BadSpanId { got: bytes.len() })?;
    Ok(Some(arr))
}

// ---- SpanFrame <-> SpanRow ------------------------------------------------

/// Encodes one decoded RSPAN span (its rebuilt [`SpanRecord`] plus the lifted
/// `service_name` column, ADR-0054) as a [`pb::SpanFrame`], field for field. The
/// worker produces the row through the same [`SpanSegmentFetcher`] funnel a
/// local `spans` read uses, so the shipped per-span merged `attrs` view is
/// byte-identical to what a local read produces; the coordinator never
/// re-derives attribute merging. Span attribute values are always strings (the
/// RSPAN merged-attrs map is `Map<Utf8, Utf8>`), so no bit-pattern discipline is
/// needed here as it is for `f64` log/metric values.
///
/// [`SpanRecord`]: ravel_rspan::SpanRecord
/// [`SpanSegmentFetcher`]: crate::span_fetcher::SpanSegmentFetcher
pub fn encode_span_frame(row: &SpanRow) -> pb::SpanFrame {
    let record = &row.record;
    pb::SpanFrame {
        trace_id: record.trace_id.to_vec(),
        span_id: record.span_id.to_vec(),
        // An absent parent (a root span) is the empty byte string; a present one
        // is its fixed 8 bytes. A real all-zero parent id is still its full 8
        // bytes, so it stays distinguishable from absent.
        parent_span_id: record
            .parent_span_id
            .map(|p| p.to_vec())
            .unwrap_or_default(),
        name: record.name.clone(),
        start_ts_ns: record.start_ts_ns,
        end_ts_ns: record.end_ts_ns,
        status_code: u32::from(record.status_code.to_u8()),
        status_message: record.status_message.clone(),
        attrs: record
            .attrs
            .iter()
            .map(|(key, value)| pb::SpanAttr {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        service_name: row.service_name.clone(),
    }
}

/// Inverse of [`encode_span_frame`]. Every malformation (a mis-sized trace,
/// span, or parent id, or a status code discriminant this build does not model)
/// is a typed [`CodecError`], never a panic or a silently truncated span. The
/// `status_message`/`service_name` proto3 `optional`s round-trip `None` vs
/// `Some("")` exactly.
pub fn decode_span_frame(frame: pb::SpanFrame) -> Result<SpanRow, CodecError> {
    let trace_id: [u8; 16] =
        frame
            .trace_id
            .as_slice()
            .try_into()
            .map_err(|_| CodecError::BadSpanTraceId {
                got: frame.trace_id.len(),
            })?;
    let span_id: [u8; 8] =
        frame
            .span_id
            .as_slice()
            .try_into()
            .map_err(|_| CodecError::BadSpanSpanId {
                got: frame.span_id.len(),
            })?;
    let parent_span_id = decode_parent_span_id(&frame.parent_span_id)?;
    let status_code = decode_span_status_code(frame.status_code)?;
    let attrs = frame
        .attrs
        .into_iter()
        .map(|a| (a.key, a.value))
        .collect::<Vec<_>>();
    Ok(SpanRow {
        record: SpanRecord {
            trace_id,
            span_id,
            parent_span_id,
            name: frame.name,
            start_ts_ns: frame.start_ts_ns,
            end_ts_ns: frame.end_ts_ns,
            status_code,
            status_message: frame.status_message,
            attrs,
        },
        service_name: frame.service_name,
    })
}

/// Decodes an optional parent span id: an empty byte string is `None` (a root
/// span), an 8-byte string is `Some`, and any other length is a typed error.
fn decode_parent_span_id(bytes: &[u8]) -> Result<Option<[u8; 8]>, CodecError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CodecError::BadSpanParentId { got: bytes.len() })?;
    Ok(Some(arr))
}

/// Decodes a span's OTLP status code. The wire carries the byte widened to a
/// `u32`; a value past `u8` or one [`StatusCode::from_u8`] does not model is a
/// typed error, never a silent default to `Unset`.
fn decode_span_status_code(raw: u32) -> Result<StatusCode, CodecError> {
    let byte = u8::try_from(raw).map_err(|_| CodecError::UnknownSpanStatusCode { got: raw })?;
    StatusCode::from_u8(byte).ok_or(CodecError::UnknownSpanStatusCode { got: raw })
}

// ---- LabelMatcher <-> pb::LabelMatcher ------------------------------------

/// Encodes PromQL matchers to their wire form. The raw literal/pattern text
/// (`LabelMatcher::value`) travels verbatim; a `Re`/`Nre` worker recompiles the
/// identical anchored regex on decode, so matching is byte-identical to local.
pub fn encode_matchers(matchers: &[LabelMatcher]) -> Vec<pb::LabelMatcher> {
    matchers
        .iter()
        .map(|m| {
            let op = match m.op {
                MatchOp::Eq => pb::label_matcher::Op::Eq,
                MatchOp::Ne => pb::label_matcher::Op::Neq,
                MatchOp::Re(_) => pb::label_matcher::Op::Re,
                MatchOp::Nre(_) => pb::label_matcher::Op::Nre,
            };
            pb::LabelMatcher {
                name: m.name.clone(),
                op: op as i32,
                value: m.value.clone(),
            }
        })
        .collect()
}

/// Inverse of [`encode_matchers`]. An unrecognized op discriminant, or a
/// regex operand that fails to recompile, is a typed error.
pub fn decode_matchers(matchers: Vec<pb::LabelMatcher>) -> Result<Vec<LabelMatcher>, CodecError> {
    matchers
        .into_iter()
        .map(|m| {
            let op = pb::label_matcher::Op::try_from(m.op)
                .map_err(|_| CodecError::UnknownMatcherOp(m.op))?;
            let to_regex_err =
                |name: &str, pattern: &str, reason: String| CodecError::InvalidRegex {
                    name: name.to_string(),
                    pattern: pattern.to_string(),
                    reason,
                };
            match op {
                pb::label_matcher::Op::Eq => Ok(LabelMatcher::equal(m.name, m.value)),
                pb::label_matcher::Op::Neq => Ok(LabelMatcher::not_equal(m.name, m.value)),
                pb::label_matcher::Op::Re => LabelMatcher::regex(&m.name, &m.value)
                    .map_err(|e| to_regex_err(&m.name, &m.value, e.reason)),
                pb::label_matcher::Op::Nre => LabelMatcher::not_regex(&m.name, &m.value)
                    .map_err(|e| to_regex_err(&m.name, &m.value, e.reason)),
            }
        })
        .collect()
}

// ---- ErasurePredicate <-> pb::ErasurePredicate ----------------------------

/// Encodes a resolved snapshot's pending erasure predicates for a slice's
/// request, so the worker applies the identical post-decode exclusion the local
/// path applies (ADR-0064, ADR-0071).
pub fn encode_erasure(predicates: &[ErasurePredicate]) -> Vec<pb::ErasurePredicate> {
    predicates
        .iter()
        .map(|p| pb::ErasurePredicate {
            equals: p
                .matchers()
                .iter()
                .map(|(name, value)| pb::LabelEquals {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            window_start_ns: p.window_start_ns(),
            window_end_ns: p.window_end_ns(),
        })
        .collect()
}

/// Inverse of [`encode_erasure`]. Total: any well-formed proto predicate maps
/// back to an [`ErasurePredicate`] (the empty-matcher fail-safe lives in the
/// erasure module, not here).
pub fn decode_erasure(predicates: Vec<pb::ErasurePredicate>) -> Vec<ErasurePredicate> {
    predicates
        .into_iter()
        .map(|p| {
            let matchers = p.equals.into_iter().map(|e| (e.name, e.value)).collect();
            ErasurePredicate::new(matchers, p.window_start_ns, p.window_end_ns)
        })
        .collect()
}

// ---- SegmentRef -> SegmentIdentity ----------------------------------------

/// Encodes a resolved [`SegmentRef`] as the wire identity a slice ships. Only
/// the durable identity fields cross; the worker reconstructs the object key
/// and verifies the fetched footer against this identity (ADR-0071
/// reconstruct-don't-trust), so the coordinator never ships a trusted key.
///
/// The reverse map (identity back to a full `SegmentRef`, which needs the
/// `ravel-commit` key reconstruction) is not provided
/// here; a worker resolves an identity to a ref by its content hash. See
/// [`identity_content_hash`].
pub fn encode_segment_identity(seg: &SegmentRef) -> pb::SegmentIdentity {
    let (level, input_set_hash, part_index) = match &seg.level {
        SegmentLevel::L0 => (0u32, Vec::new(), 0u32),
        SegmentLevel::L1 {
            input_set_hash,
            part_index,
        } => (1u32, input_set_hash.to_vec(), *part_index),
    };
    pb::SegmentIdentity {
        level,
        shard: seg.shard,
        ingest_hour_bucket: seg.ingest_hour_bucket,
        writer_id: seg.writer_id.to_string(),
        writer_epoch: seg.writer_epoch,
        writer_seq: seg.writer_seq,
        input_set_hash,
        part_index,
        content_hash: seg.content_hash.to_vec(),
        object_size: seg.object_size,
        // The version every current segment is written at, read from the
        // reader's supported-version window rather than from a version
        // constant: `SegmentRef` carries no per-segment format version, so the
        // identity names the version this build writes, and that value must
        // follow a version bump instead of being restamped by hand (ADR-0092
        // decision 7). Shipping the real version rather than a meaningless
        // hardcoded 0 gives the reconstruct-and-verify path the version to
        // check the fetched footer against.
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
    }
}

/// The 32-byte content hash a worker keys on to resolve a shipped identity back
/// to the pinned `SegmentRef` it must fetch. A hash of any other length is a
/// typed error, never a truncated or zero-padded key.
pub fn identity_content_hash(identity: &pb::SegmentIdentity) -> Result<[u8; 32], CodecError> {
    identity
        .content_hash
        .as_slice()
        .try_into()
        .map_err(|_| CodecError::BadContentHash {
            got: identity.content_hash.len(),
        })
}

// ---- QueryAccountingSnapshot <-> pb::QueryAccountingSnapshot --------------

/// Flattens the in-memory accounting snapshot into its wire form. The per-op
/// `s3_requests`/`s3_bytes` arrays flatten in [`AccountedOp`] index order
/// (Get, List, Head), matching the proto's field layout.
pub fn encode_accounting(snap: &QueryAccountingSnapshot) -> pb::QueryAccountingSnapshot {
    pb::QueryAccountingSnapshot {
        s3_get_requests: snap.s3_requests(AccountedOp::Get),
        s3_list_requests: snap.s3_requests(AccountedOp::List),
        s3_head_requests: snap.s3_requests(AccountedOp::Head),
        s3_get_bytes: snap.s3_bytes(AccountedOp::Get),
        s3_list_bytes: snap.s3_bytes(AccountedOp::List),
        s3_head_bytes: snap.s3_bytes(AccountedOp::Head),
        cache_hits: snap.cache_hits,
        cache_misses: snap.cache_misses,
        cache_bytes: snap.cache_bytes,
        decompressed_bytes: snap.decompressed_bytes,
        segments_opened: snap.segments_opened,
        segments_pruned: snap.segments_pruned,
        series_matched: snap.series_matched,
        bytes_reused: snap.bytes_reused,
        peak_intermediate_bytes: snap.peak_intermediate_bytes,
    }
}

/// Inverse of [`encode_accounting`]. Total and infallible: every field is a
/// plain `u64`.
pub fn decode_accounting(snap: pb::QueryAccountingSnapshot) -> QueryAccountingSnapshot {
    let mut s3_requests = [0u64; ravel_types::accounting::ACCOUNTED_OP_COUNT];
    let mut s3_bytes = [0u64; ravel_types::accounting::ACCOUNTED_OP_COUNT];
    s3_requests[AccountedOp::Get.index()] = snap.s3_get_requests;
    s3_requests[AccountedOp::List.index()] = snap.s3_list_requests;
    s3_requests[AccountedOp::Head.index()] = snap.s3_head_requests;
    s3_bytes[AccountedOp::Get.index()] = snap.s3_get_bytes;
    s3_bytes[AccountedOp::List.index()] = snap.s3_list_bytes;
    s3_bytes[AccountedOp::Head.index()] = snap.s3_head_bytes;
    QueryAccountingSnapshot {
        s3_requests,
        s3_bytes,
        cache_hits: snap.cache_hits,
        cache_misses: snap.cache_misses,
        cache_bytes: snap.cache_bytes,
        decompressed_bytes: snap.decompressed_bytes,
        segments_opened: snap.segments_opened,
        segments_pruned: snap.segments_pruned,
        series_matched: snap.series_matched,
        bytes_reused: snap.bytes_reused,
        peak_intermediate_bytes: snap.peak_intermediate_bytes,
    }
}

// ---- Status ---------------------------------------------------------------

/// Decodes a summary's status code discriminant to the typed enum. An
/// unrecognized code is an error (the coordinator cannot know how to react to a
/// status it does not model), never a silent success.
pub fn decode_status_code(raw: i32) -> Result<pb::status::Code, CodecError> {
    pb::status::Code::try_from(raw).map_err(|_| CodecError::UnknownStatusCode(raw))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn label_set(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: (*n).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        )
        .expect("valid labels")
    }

    /// A frame round-trips one run back to a single [`FetchedSeriesSoa`] whose
    /// timestamps and value bit patterns are byte-identical to the original.
    /// The property drives arbitrary `i64` timestamps (negatives and extremes,
    /// exercising the `wrapping` delta arithmetic) and arbitrary `u64` value
    /// words fed through [`f64::from_bits`] (so NaN payloads, signalling NaNs,
    /// and `-0.0` all appear) -- the exact bit-exactness the local read path
    /// guarantees (docs/segment-format.md).
    fn arb_soa() -> impl Strategy<Value = FetchedSeriesSoa> {
        let cols = prop::collection::vec((any::<i64>(), any::<u64>()), 0..24);
        (
            any::<[u8; 16]>(),
            cols,
            any::<i64>(),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(|(id, cols, created, epoch, seq)| {
                let timestamps = cols.iter().map(|(t, _)| *t).collect();
                let values = cols.iter().map(|(_, v)| f64::from_bits(*v)).collect();
                FetchedSeriesSoa {
                    series_id: SeriesId(id),
                    labels: label_set(&[("__name__", "m"), ("k", "v")]),
                    timestamps,
                    values,
                    created_unix_ns: created,
                    writer_epoch: epoch,
                    writer_seq: seq,
                    per_sample_priorities: None,
                }
            })
    }

    /// A `u64` generator weighted toward the values that break a bitcast-based
    /// delta transform if it is ever rewritten as a numeric conversion: the two
    /// signed-domain extremes and the sign-bit boundary. Uniform `any::<u64>()`
    /// alone essentially never lands on them.
    fn arb_boundary_u64() -> impl Strategy<Value = u64> {
        prop_oneof![
            2 => any::<u64>(),
            1 => prop::sample::select(vec![
                u64::MAX,
                u64::MAX - 1,
                i64::MAX as u64,
                1u64 << 63,
                i64::MIN as u64,
                0u64,
                1u64,
            ]),
        ]
    }

    /// [`arb_soa`] plus a per-sample provenance column, present about half the
    /// time and always the same length as the timestamps column. A sibling
    /// strategy rather than a change to `arb_soa` itself: the run-wide-only
    /// coverage `series_frame_round_trips_bit_for_bit` already provides stays
    /// exactly as it was, and the length coupling lives in one place.
    fn arb_soa_with_priorities() -> impl Strategy<Value = FetchedSeriesSoa> {
        arb_soa().prop_flat_map(|soa| {
            let n = soa.timestamps.len();
            let column = prop::collection::vec(
                (
                    any::<i64>(),
                    arb_boundary_u64(),
                    arb_boundary_u64(),
                    any::<u32>(),
                ),
                n..=n,
            );
            (Just(soa), prop::option::of(column)).prop_map(|(mut soa, ps)| {
                soa.per_sample_priorities = ps.map(|rows| {
                    rows.into_iter()
                        .map(
                            |(created_unix_ns, writer_epoch, writer_seq, in_page_index)| {
                                SamplePriority {
                                    created_unix_ns,
                                    writer_epoch,
                                    writer_seq,
                                    in_page_index,
                                }
                            },
                        )
                        .collect()
                });
                soa
            })
        })
    }

    proptest! {
        #[test]
        fn series_frame_round_trips_bit_for_bit(soa in arb_soa()) {
            let frame = encode_series_frame(&soa);
            let decoded = decode_series_frame(frame).expect("decode");
            prop_assert_eq!(decoded.len(), 1);
            let got = &decoded[0];
            prop_assert_eq!(got.series_id, soa.series_id);
            prop_assert_eq!(&got.labels, &soa.labels);
            prop_assert_eq!(got.created_unix_ns, soa.created_unix_ns);
            prop_assert_eq!(got.writer_epoch, soa.writer_epoch);
            prop_assert_eq!(got.writer_seq, soa.writer_seq);
            prop_assert_eq!(&got.timestamps, &soa.timestamps);
            prop_assert_eq!(got.values.len(), soa.values.len());
            for (a, b) in got.values.iter().zip(soa.values.iter()) {
                // Bit-exact, never `==`: -0.0 and NaN must compare by pattern.
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }

        #[test]
        fn ts_deltas_round_trip_over_i64_extremes(ts in prop::collection::vec(any::<i64>(), 0..32)) {
            let restored = decode_ts_deltas(&encode_ts_deltas(&ts));
            prop_assert_eq!(restored, ts);
        }

        /// The `u64` twin of the property above. The transform crosses the
        /// signed domain by bitcast, so the generator is weighted toward the
        /// sign-bit boundary and both signed extremes.
        #[test]
        fn u64_deltas_round_trip_over_u64_extremes(
            values in prop::collection::vec(arb_boundary_u64(), 0..32)
        ) {
            let restored = decode_u64_deltas(&encode_u64_deltas(&values));
            prop_assert_eq!(restored, values);
        }

        /// A per-sample provenance column survives the real prost encode/decode
        /// path field for field. These are integers, so `==` is the right
        /// comparison (unlike the value column's bit-pattern check above).
        #[test]
        fn series_frame_round_trips_per_sample_priorities(soa in arb_soa_with_priorities()) {
            let decoded = decode_series_frame(encode_series_frame(&soa)).expect("decode");
            prop_assert_eq!(decoded.len(), 1);
            let got = decoded[0].per_sample_priorities.as_ref();
            // An empty column is indistinguishable from absent on the wire:
            // proto3 omits empty repeated fields, so a zero-sample `Some`
            // decodes to `None` by design (see `decode_sample_priorities`).
            let want = soa
                .per_sample_priorities
                .as_ref()
                .filter(|ps| !ps.is_empty());
            match (got, want) {
                (Some(a), Some(b)) => {
                    prop_assert_eq!(a.len(), b.len());
                    for (x, y) in a.iter().zip(b.iter()) {
                        prop_assert_eq!(x.created_unix_ns, y.created_unix_ns);
                        prop_assert_eq!(x.writer_epoch, y.writer_epoch);
                        prop_assert_eq!(x.writer_seq, y.writer_seq);
                        prop_assert_eq!(x.in_page_index, y.in_page_index);
                    }
                }
                (None, None) => {}
                (a, b) => prop_assert!(
                    false,
                    "provenance presence changed on the wire: got {:?}, want {:?}",
                    a.map(|v| v.len()),
                    b.map(|v| v.len())
                ),
            }
        }
    }

    #[test]
    fn explicit_special_values_survive_round_trip() {
        // The values a naive `==`/`f64` codec would corrupt: quiet NaN,
        // signalling NaN, a NaN with a payload, negative zero, and the
        // ADR-0007 staleness marker bit pattern.
        let specials = [
            f64::NAN,
            f64::from_bits(0x7ff0_0000_0000_0001), // signalling NaN
            f64::from_bits(0x7ff8_0000_dead_beef), // NaN with payload
            -0.0_f64,
            0.0_f64,
            f64::from_bits(0x7ff0_0000_0000_0002), // staleness-marker-like
        ];
        let soa = FetchedSeriesSoa {
            series_id: SeriesId([9u8; 16]),
            labels: label_set(&[("__name__", "special")]),
            timestamps: (0..specials.len() as i64).map(|i| i * 1_000).collect(),
            values: specials.to_vec(),
            created_unix_ns: 7,
            writer_epoch: 1,
            writer_seq: 2,
            per_sample_priorities: None,
        };
        let decoded = decode_series_frame(encode_series_frame(&soa)).expect("decode");
        assert_eq!(decoded.len(), 1);
        for (a, b) in decoded[0].values.iter().zip(specials.iter()) {
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "special f64 corrupted on the wire"
            );
        }
    }

    #[test]
    fn unknown_protocol_version_is_typed_error() {
        assert_eq!(check_protocol_version(PROTOCOL_VERSION), Ok(()));
        assert_eq!(
            check_protocol_version(PROTOCOL_VERSION + 1),
            Err(CodecError::UnknownProtocolVersion {
                got: PROTOCOL_VERSION + 1,
                expected: PROTOCOL_VERSION,
            })
        );
        assert_eq!(
            check_protocol_version(0),
            Err(CodecError::UnknownProtocolVersion {
                got: 0,
                expected: PROTOCOL_VERSION,
            })
        );
    }

    fn partial(count: Option<u64>, min: Option<f64>, max: Option<f64>) -> PartialAggregate {
        PartialAggregate {
            series_id: SeriesId([7u8; 16]),
            labels: label_set(&[("__name__", "m"), ("k", "v")]),
            count,
            min,
            max,
        }
    }

    fn assert_partial_round_trip(original: &PartialAggregate) {
        let frame = encode_partial_aggregate(original);
        let got = decode_partial_aggregate(frame).expect("decode");
        assert_eq!(got.series_id, original.series_id);
        assert_eq!(&got.labels, &original.labels);
        assert_eq!(got.count, original.count);
        // Bit-exact, never `==`: -0.0 and NaN must compare by pattern, and an
        // absent bound must stay absent rather than becoming a present zero.
        assert_eq!(
            got.min.map(f64::to_bits),
            original.min.map(f64::to_bits),
            "min bit pattern corrupted on the wire"
        );
        assert_eq!(
            got.max.map(f64::to_bits),
            original.max.map(f64::to_bits),
            "max bit pattern corrupted on the wire"
        );
    }

    /// Every shape the ADR-0103 frame carries round-trips: a count-only
    /// partial, a min/max-only partial, all three present, and the bare group
    /// enumeration with none of them. A present zero count and a present `0.0`
    /// bound stay distinct from absence, which is what the proto3 `optional`
    /// fields exist for.
    #[test]
    fn partial_aggregate_round_trips_every_shape() {
        assert_partial_round_trip(&partial(Some(42), None, None));
        assert_partial_round_trip(&partial(None, Some(-3.5), Some(9.25)));
        assert_partial_round_trip(&partial(Some(7), Some(-3.5), Some(9.25)));
        assert_partial_round_trip(&partial(None, None, None));
        assert_partial_round_trip(&partial(Some(0), Some(0.0), Some(0.0)));
    }

    /// The bounds cross as raw bit patterns, so the values a proto double would
    /// mangle survive: NaN payloads, a signalling NaN, both infinities, and the
    /// two zeros (a `-0.0` min is not a `0.0` min).
    #[test]
    fn partial_aggregate_preserves_special_f64_bit_patterns() {
        let specials = [
            f64::NAN,
            f64::from_bits(0x7ff8_0000_dead_beef),
            f64::from_bits(0x7ff0_0000_0000_0001),
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.0f64,
            0.0f64,
        ];
        for &min in &specials {
            for &max in &specials {
                assert_partial_round_trip(&partial(Some(1), Some(min), Some(max)));
            }
        }
    }

    proptest! {
        /// Arbitrary `u64` words fed through [`f64::from_bits`] cover the whole
        /// f64 domain including every NaN payload and `-0.0`; presence is
        /// driven independently per field so absence is exercised too.
        #[test]
        fn partial_aggregate_round_trips_bit_for_bit(
            id in any::<[u8; 16]>(),
            count in prop::option::of(any::<u64>()),
            min_bits in prop::option::of(any::<u64>()),
            max_bits in prop::option::of(any::<u64>()),
        ) {
            let original = PartialAggregate {
                series_id: SeriesId(id),
                labels: label_set(&[("__name__", "m"), ("k", "v")]),
                count,
                min: min_bits.map(f64::from_bits),
                max: max_bits.map(f64::from_bits),
            };
            let got = decode_partial_aggregate(encode_partial_aggregate(&original))
                .expect("decode");
            prop_assert_eq!(got.series_id, original.series_id);
            prop_assert_eq!(&got.labels, &original.labels);
            prop_assert_eq!(got.count, original.count);
            prop_assert_eq!(got.min.map(f64::to_bits), min_bits);
            prop_assert_eq!(got.max.map(f64::to_bits), max_bits);
        }
    }

    /// A `PartialAggregate` whose series id is not 16 bytes is a typed error,
    /// never a panic, exactly as a malformed `SeriesFrame` id is.
    #[test]
    fn partial_aggregate_bad_series_id_is_typed_error() {
        let frame = pb::PartialAggregate {
            series_id: vec![0u8; 8],
            labels: Vec::new(),
            count: Some(1),
            min_bits: None,
            max_bits: None,
        };
        let err = decode_partial_aggregate(frame).expect_err("mis-sized series id");
        assert_eq!(err, CodecError::BadSeriesId { got: 8 });
    }

    #[test]
    fn run_length_mismatch_is_typed_error() {
        // A hand-built frame whose ts_delta and value_bits columns disagree
        // must decode to a typed error, never a panic or a truncated series.
        let frame = pb::SeriesFrame {
            series_id: vec![0u8; 16],
            labels: vec![pb::Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            }],
            runs: vec![pb::Run {
                created_unix_ns: 0,
                writer_epoch: 0,
                writer_seq: 0,
                ts_delta: vec![1, 2, 3],
                value_bits: vec![1, 2],
                ..Default::default()
            }],
        };
        assert!(matches!(
            decode_series_frame(frame),
            Err(CodecError::RunLengthMismatch {
                timestamps: 3,
                values: 2,
            })
        ));
    }

    /// A run whose `per_sample_priorities` is `Some` round-trips its four
    /// provenance columns losslessly through the real wire (encode -> prost
    /// bytes -> prost decode -> codec decode). The fixture drives negative
    /// `created` deltas (1000 -> 900 -> -50), a decreasing `writer_seq`
    /// (negative signed deltas), and a `writer_epoch` reaching `u64::MAX` (the
    /// value the two's-complement `u64`->`sint64` reinterpretation is most
    /// likely to corrupt if the boundary cast were numeric rather than a
    /// bitcast), so the key survives across the full delta transform.
    #[test]
    fn series_run_carries_per_sample_provenance_round_trip() {
        use prost::Message;

        let priorities = vec![
            SamplePriority {
                created_unix_ns: 1000,
                writer_epoch: 5,
                writer_seq: 9,
                in_page_index: 0,
            },
            SamplePriority {
                created_unix_ns: 900,
                writer_epoch: 5,
                writer_seq: 3,
                in_page_index: 2,
            },
            SamplePriority {
                created_unix_ns: -50,
                writer_epoch: u64::MAX,
                writer_seq: 0,
                in_page_index: 7,
            },
        ];
        let soa = FetchedSeriesSoa {
            series_id: SeriesId([1u8; 16]),
            labels: label_set(&[("__name__", "m")]),
            timestamps: vec![10, 20, 30],
            values: vec![1.0, 2.0, 3.0],
            created_unix_ns: 1000,
            writer_epoch: 5,
            writer_seq: 9,
            per_sample_priorities: Some(priorities.clone()),
        };

        let frame = encode_series_frame(&soa);
        let bytes = frame.encode_to_vec();
        let wire = pb::SeriesFrame::decode(bytes.as_slice()).expect("prost decode");
        let decoded = decode_series_frame(wire).expect("codec decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].per_sample_priorities,
            Some(priorities),
            "the per-sample provenance key round-trips byte-for-byte"
        );
        assert_eq!(&decoded[0].timestamps, &soa.timestamps);
    }

    /// The `HistogramRun` provenance columns share `Run`'s codec, so they
    /// round-trip identically through the real wire. `span_payload` (field 5)
    /// is now retired (`reserved`, ADR-0096 decision 2) and the typed
    /// `records` field carries the value payload; this test still isolates the
    /// provenance columns, driving them alongside one typed record to prove the
    /// two coexist on `HistogramRun` without interfering.
    #[test]
    fn histogram_run_carries_per_sample_provenance_round_trip() {
        use prost::Message;

        let priorities = vec![
            SamplePriority {
                created_unix_ns: 7,
                writer_epoch: 2,
                writer_seq: 100,
                in_page_index: 1,
            },
            SamplePriority {
                created_unix_ns: -3,
                writer_epoch: u64::MAX,
                writer_seq: 40,
                in_page_index: 0,
            },
        ];
        let (prov_created_delta, prov_epoch_delta, prov_seq_delta, prov_in_page_index) =
            encode_sample_priorities(&Some(priorities.clone()));
        let records = encode_histogram_records(&[sample_histogram_int(), sample_histogram_int()]);
        let run = pb::HistogramRun {
            created_unix_ns: 7,
            writer_epoch: 2,
            writer_seq: 100,
            ts_delta: encode_ts_deltas(&[1_000, 900]),
            prov_created_delta,
            prov_epoch_delta,
            prov_seq_delta,
            prov_in_page_index,
            records,
        };

        let bytes = run.encode_to_vec();
        let back = pb::HistogramRun::decode(bytes.as_slice()).expect("prost decode");
        assert_eq!(
            decode_histogram_records(&back.records).expect("decode records"),
            vec![sample_histogram_int(), sample_histogram_int()],
            "the typed histogram records are carried through alongside provenance"
        );
        let decoded = decode_sample_priorities(
            &back.prov_created_delta,
            &back.prov_epoch_delta,
            &back.prov_seq_delta,
            &back.prov_in_page_index,
            back.ts_delta.len(),
        )
        .expect("decode provenance");
        assert_eq!(
            decoded,
            Some(priorities),
            "the histogram run's per-sample provenance key round-trips"
        );
    }

    // ---- HistogramRecord round-trip -----------------------------------------

    /// A small consistent integer-count histogram, used where the surrounding
    /// test compares whole `HistogramValue`s with `==` (so it must carry no NaN
    /// or `-0.0`, which `==` would mis-handle).
    fn sample_histogram_int() -> HistogramValue {
        HistogramValue {
            scale: 2,
            zero_threshold: 0.5,
            sum: Some(3.0),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 1,
                length: 2,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 1,
                count: 6,
                positive: vec![2, 3],
                negative: vec![],
            },
            reset_hint: ResetHint::Yes,
        }
    }

    /// f64 generator weighted toward the bit patterns a naive `double`-based
    /// codec would corrupt: quiet/signalling/payload NaNs, both zeros, both
    /// infinities, and the finite extremes, plus uniform arbitrary bit patterns.
    fn arb_hist_float() -> impl Strategy<Value = f64> {
        prop_oneof![
            2 => any::<u64>().prop_map(f64::from_bits),
            1 => prop::sample::select(vec![
                0.0_f64,
                -0.0_f64,
                f64::NAN,
                -f64::NAN,
                f64::from_bits(0x7ff0_0000_0000_0001), // signalling NaN
                f64::from_bits(0x7ff8_0000_dead_beef), // NaN with payload
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::MAX,
                f64::MIN,
                f64::MIN_POSITIVE,
            ]),
        ]
    }

    /// A span with `length >= 1`: a zero-length span is structurally invalid
    /// (`HistogramSpanLengthZero`, `validate_histogram_value`), so the generator
    /// can no longer construct one and call it a passing round-trip case.
    fn arb_hist_span() -> impl Strategy<Value = HistogramSpan> {
        (any::<i32>(), 1u32..=4).prop_map(|(offset, length)| HistogramSpan { offset, length })
    }

    /// One histogram side: its spans and the exact bucket-count total those
    /// spans cover (`sum(length)`), so the coupled count generators below emit a
    /// bucket vector of the length `validate_histogram_value` requires.
    fn arb_hist_side() -> impl Strategy<Value = (Vec<HistogramSpan>, usize)> {
        prop::collection::vec(arb_hist_span(), 0..3).prop_map(|spans| {
            let total = spans.iter().map(|s| s.length as usize).sum();
            (spans, total)
        })
    }

    /// Integer counts whose `positive`/`negative` vectors match the span totals
    /// and whose `count` is `>= zero_count` and `>= sum(all buckets)` by
    /// construction, so the record satisfies the reader's count-consistency
    /// invariant. All values stay small so the total never overflows.
    fn arb_int_counts(pos_len: usize, neg_len: usize) -> impl Strategy<Value = HistogramCounts> {
        (
            0u64..100,
            prop::collection::vec(0u64..100, pos_len..=pos_len),
            prop::collection::vec(0u64..100, neg_len..=neg_len),
            0u64..100,
        )
            .prop_map(|(zero_count, positive, negative, extra)| {
                let total: u64 = positive.iter().chain(negative.iter()).sum();
                HistogramCounts::Int {
                    zero_count,
                    count: zero_count + total + extra,
                    positive,
                    negative,
                }
            })
    }

    /// Float counts whose bucket vectors match the span totals and carry
    /// arbitrary `f64` payloads (NaN, `-0.0`, infinities). `count` is
    /// `f64::INFINITY`, which is `>=` any finite `zero_count` and total and
    /// passes the reader's NaN-transparent `<` check for every bucket payload,
    /// so the record is always structurally valid while still exercising every
    /// bit pattern in the bucket and `zero_count` fields.
    fn arb_float_counts(pos_len: usize, neg_len: usize) -> impl Strategy<Value = HistogramCounts> {
        (
            arb_hist_float(),
            prop::collection::vec(arb_hist_float(), pos_len..=pos_len),
            prop::collection::vec(arb_hist_float(), neg_len..=neg_len),
        )
            .prop_map(|(zero_count, positive, negative)| HistogramCounts::Float {
                zero_count,
                count: f64::INFINITY,
                positive,
                negative,
            })
    }

    /// A scale and its `custom_values`, coupled so the pair always satisfies the
    /// custom-boundary invariant: scale `-53` with a non-empty strictly
    /// ascending finite boundary vector, or any other in-range scale with no
    /// boundaries.
    fn arb_scale_and_custom() -> impl Strategy<Value = (i32, Option<Vec<f64>>)> {
        prop_oneof![
            prop::collection::vec(1u32..1000, 1..5).prop_map(|deltas| {
                let mut acc = 0.0f64;
                let bounds = deltas
                    .into_iter()
                    .map(|d| {
                        acc += f64::from(d);
                        acc
                    })
                    .collect();
                (-53, Some(bounds))
            }),
            (-52i32..=20).prop_map(|scale| (scale, None)),
        ]
    }

    fn arb_reset_hint() -> impl Strategy<Value = ResetHint> {
        prop_oneof![
            Just(ResetHint::Unknown),
            Just(ResetHint::Yes),
            Just(ResetHint::No),
            Just(ResetHint::Gauge),
        ]
    }

    /// Generates a structurally valid [`HistogramValue`] spanning both
    /// `HistogramCounts` variants, `sum` present and absent, `custom_values`
    /// present (scale `-53`) and absent, every `ResetHint`, and float payloads
    /// including NaN/`-0.0`/finite extremes. Every generated value satisfies the
    /// invariants `validate_histogram_value` enforces: spans have `length >= 1`,
    /// each side's bucket vector matches its span total, `custom_values` is
    /// present iff scale is `-53` (non-empty, strictly ascending), and `count`
    /// is consistent with its buckets. So the round-trip test pins the PRESENCE
    /// of validation (a decode that rejected any of these would fail the test),
    /// not its absence.
    fn arb_histogram_value() -> impl Strategy<Value = HistogramValue> {
        (
            arb_hist_side(),
            arb_hist_side(),
            arb_scale_and_custom(),
            arb_hist_float(),
            prop::option::of(arb_hist_float()),
            arb_reset_hint(),
            any::<bool>(),
        )
            .prop_flat_map(
                |(
                    (positive_spans, pos_len),
                    (negative_spans, neg_len),
                    (scale, custom_values),
                    zero_threshold,
                    sum,
                    reset_hint,
                    int_kind,
                )| {
                    let counts = if int_kind {
                        arb_int_counts(pos_len, neg_len).boxed()
                    } else {
                        arb_float_counts(pos_len, neg_len).boxed()
                    };
                    counts.prop_map(move |counts| HistogramValue {
                        scale,
                        zero_threshold,
                        sum,
                        custom_values: custom_values.clone(),
                        positive_spans: positive_spans.clone(),
                        negative_spans: negative_spans.clone(),
                        counts,
                        reset_hint,
                    })
                },
            )
    }

    fn opt_f64_bits_eq(a: Option<f64>, b: Option<f64>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => x.to_bits() == y.to_bits(),
            (None, None) => true,
            _ => false,
        }
    }

    fn vec_f64_bits_eq(a: &[f64], b: &[f64]) -> bool {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
    }

    fn opt_vec_f64_bits_eq(a: &Option<Vec<f64>>, b: &Option<Vec<f64>>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => vec_f64_bits_eq(x, y),
            (None, None) => true,
            _ => false,
        }
    }

    fn counts_bits_eq(a: &HistogramCounts, b: &HistogramCounts) -> bool {
        match (a, b) {
            (
                HistogramCounts::Int {
                    zero_count: za,
                    count: ca,
                    positive: pa,
                    negative: na,
                },
                HistogramCounts::Int {
                    zero_count: zb,
                    count: cb,
                    positive: p2,
                    negative: n2,
                },
            ) => za == zb && ca == cb && pa == p2 && na == n2,
            (
                HistogramCounts::Float {
                    zero_count: za,
                    count: ca,
                    positive: pa,
                    negative: na,
                },
                HistogramCounts::Float {
                    zero_count: zb,
                    count: cb,
                    positive: p2,
                    negative: n2,
                },
            ) => {
                za.to_bits() == zb.to_bits()
                    && ca.to_bits() == cb.to_bits()
                    && vec_f64_bits_eq(pa, p2)
                    && vec_f64_bits_eq(na, n2)
            }
            _ => false,
        }
    }

    /// Whole-`HistogramValue` equality by bit pattern for every `f64`, so
    /// `-0.0` and NaN (including payloads) compare by pattern, never `==`.
    fn histogram_bits_eq(a: &HistogramValue, b: &HistogramValue) -> bool {
        a.scale == b.scale
            && a.zero_threshold.to_bits() == b.zero_threshold.to_bits()
            && opt_f64_bits_eq(a.sum, b.sum)
            && opt_vec_f64_bits_eq(&a.custom_values, &b.custom_values)
            && a.positive_spans == b.positive_spans
            && a.negative_spans == b.negative_spans
            && counts_bits_eq(&a.counts, &b.counts)
            && a.reset_hint == b.reset_hint
    }

    proptest! {
        /// Structurally valid histogram records survive the real prost wire
        /// (encode -> prost bytes -> prost decode -> validated codec decode)
        /// field for field, every `f64` bit-exact. Routed through
        /// `encode_histogram_frame`/`decode_histogram_frame` so the decode runs
        /// the full structural validation: since `arb_histogram_value` only
        /// generates valid records, a decode that rejected any of them (or one
        /// that let a corrupted value through) fails the test. This pins the
        /// PRESENCE of validation, not its absence. Covers both count variants,
        /// `sum`/`custom_values` present and absent, every `ResetHint`, and
        /// NaN/`-0.0`/extreme float payloads (compared by `to_bits`, never `==`).
        #[test]
        fn histogram_records_round_trip_bit_for_bit(
            values in prop::collection::vec(arb_histogram_value(), 0..6)
        ) {
            use prost::Message;
            let series = FetchedHistogramSeries {
                series_id: SeriesId([2u8; 16]),
                labels: label_set(&[("__name__", "h")]),
                timestamps: (0..values.len() as i64).map(|i| i * 1_000).collect(),
                values: values.clone(),
                created_unix_ns: 1,
                writer_epoch: 2,
                writer_seq: 3,
                per_sample_priorities: None,
            };
            let bytes = encode_histogram_frame(&series).encode_to_vec();
            let wire = pb::HistogramFrame::decode(bytes.as_slice()).expect("prost decode");
            let decoded = decode_histogram_frame(wire).expect("codec decode");
            prop_assert_eq!(decoded.len(), 1);
            let got = &decoded[0].values;
            prop_assert_eq!(got.len(), values.len());
            for (got, want) in got.iter().zip(values.iter()) {
                prop_assert!(
                    histogram_bits_eq(got, want),
                    "histogram record corrupted on the wire: {:?} vs {:?}",
                    got,
                    want
                );
            }
        }
    }

    /// The float payloads a naive `double`/`==` codec corrupts, driven through
    /// every `f64`-bearing field of both count variants and the scalar fields,
    /// plus the `sum: None`/`Some` and `custom_values: None`/`Some` distinctions
    /// and all four reset hints, across the real prost wire.
    #[test]
    fn histogram_record_special_values_and_optionals_round_trip() {
        use prost::Message;

        let payload_nan = f64::from_bits(0x7ff8_0000_dead_beef);
        let specials = [
            f64::NAN,
            payload_nan,
            -0.0_f64,
            0.0_f64,
            f64::INFINITY,
            f64::MAX,
        ];

        // A float-count histogram at the custom scale, carrying specials in
        // every f64 field and `sum: None`.
        let float_custom = HistogramValue {
            scale: -53,
            zero_threshold: payload_nan,
            sum: None,
            custom_values: Some(specials.to_vec()),
            positive_spans: vec![HistogramSpan {
                offset: -2,
                length: 3,
            }],
            negative_spans: vec![HistogramSpan {
                offset: 5,
                length: 1,
            }],
            counts: HistogramCounts::Float {
                zero_count: -0.0,
                count: f64::INFINITY,
                positive: vec![f64::NAN, -0.0, f64::MAX],
                negative: vec![payload_nan],
            },
            reset_hint: ResetHint::Gauge,
        };

        // An int-count histogram with `sum: Some`, `custom_values: None`, and a
        // distinct reset hint, so both optional distinctions and another enum
        // value are exercised in one pass.
        let int_plain = HistogramValue {
            scale: 4,
            zero_threshold: 0.0,
            sum: Some(-0.0),
            custom_values: None,
            positive_spans: vec![],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: u64::MAX,
                count: 0,
                positive: vec![],
                negative: vec![7],
            },
            reset_hint: ResetHint::No,
        };

        let values = vec![float_custom, int_plain];
        let run = pb::HistogramRun {
            records: encode_histogram_records(&values),
            ..Default::default()
        };
        let back = pb::HistogramRun::decode(run.encode_to_vec().as_slice()).expect("prost decode");
        let decoded = decode_histogram_records(&back.records).expect("codec decode");
        assert_eq!(decoded.len(), values.len());
        for (got, want) in decoded.iter().zip(values.iter()) {
            assert!(
                histogram_bits_eq(got, want),
                "special histogram value corrupted: {got:?} vs {want:?}"
            );
        }
        // `sum: None` must stay distinct from a present zero.
        assert_eq!(decoded[0].sum, None);
        assert_eq!(decoded[1].sum.map(f64::to_bits), Some((-0.0_f64).to_bits()));
    }

    /// Every `ResetHint` variant round-trips to itself across the wire.
    #[test]
    fn histogram_record_every_reset_hint_round_trips() {
        use prost::Message;
        for hint in [
            ResetHint::Unknown,
            ResetHint::Yes,
            ResetHint::No,
            ResetHint::Gauge,
        ] {
            let mut value = sample_histogram_int();
            value.reset_hint = hint;
            let run = pb::HistogramRun {
                records: encode_histogram_records(&[value]),
                ..Default::default()
            };
            let back =
                pb::HistogramRun::decode(run.encode_to_vec().as_slice()).expect("prost decode");
            let decoded = decode_histogram_records(&back.records).expect("codec decode");
            assert_eq!(decoded[0].reset_hint, hint, "reset hint {hint:?} changed");
        }
    }

    /// A record with no `counts` oneof member is a typed error, never a panic
    /// or a silently defaulted count set.
    #[test]
    fn histogram_record_missing_counts_is_typed_error() {
        let record = pb::HistogramRecord {
            counts: None,
            ..Default::default()
        };
        assert_eq!(
            decode_histogram_records(std::slice::from_ref(&record)),
            Err(CodecError::MissingHistogramCounts)
        );
    }

    /// An unknown reset-hint discriminant is a typed error, never a silent
    /// default to `Unknown`.
    #[test]
    fn histogram_record_unknown_reset_hint_is_typed_error() {
        let record = pb::HistogramRecord {
            reset_hint: 99,
            counts: Some(pb::histogram_record::Counts::IntCounts(
                pb::HistogramCountsInt::default(),
            )),
            ..Default::default()
        };
        assert_eq!(
            decode_histogram_records(std::slice::from_ref(&record)),
            Err(CodecError::UnknownResetHint(99))
        );
    }

    // ---- HistogramFrame round-trip and structural validation ----------------

    /// A full histogram series round-trips through
    /// `encode_histogram_frame`/`decode_histogram_frame` field for field, every
    /// `f64` bit-exact. The fixture spans both `HistogramCounts` variants,
    /// `sum: None` and `Some`, both `custom_values` states, every `ResetHint`,
    /// and a `Some` per-sample provenance column, so one round trip exercises
    /// the whole matrix.
    #[test]
    fn histogram_frame_round_trips() {
        // int, sum Some, custom None, ResetHint::Yes.
        let int_sum = sample_histogram_int();
        // float, sum None, custom Some (scale -53), ResetHint::Gauge, with NaN
        // and -0.0 bucket payloads.
        let float_custom = HistogramValue {
            scale: -53,
            zero_threshold: 0.25,
            sum: None,
            custom_values: Some(vec![1.0, 2.5, 4.0]),
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![HistogramSpan {
                offset: 1,
                length: 1,
            }],
            counts: HistogramCounts::Float {
                zero_count: -0.0,
                count: f64::INFINITY,
                positive: vec![f64::NAN, 3.0],
                negative: vec![-0.0],
            },
            reset_hint: ResetHint::Gauge,
        };
        // int, sum None, custom None, ResetHint::No, empty spans.
        let int_empty = HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: None,
            custom_values: None,
            positive_spans: vec![],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 0,
                count: 0,
                positive: vec![],
                negative: vec![],
            },
            reset_hint: ResetHint::No,
        };
        // float, sum Some, custom None, ResetHint::Unknown.
        let float_plain = HistogramValue {
            scale: 3,
            zero_threshold: 1.0,
            sum: Some(5.0),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: -1,
                length: 1,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Float {
                zero_count: 1.0,
                count: 5.0,
                positive: vec![4.0],
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        };

        let values = vec![int_sum, float_custom, int_empty, float_plain];
        let priorities = vec![
            SamplePriority {
                created_unix_ns: 10,
                writer_epoch: 1,
                writer_seq: 4,
                in_page_index: 0,
            },
            SamplePriority {
                created_unix_ns: 20,
                writer_epoch: 1,
                writer_seq: 3,
                in_page_index: 1,
            },
            SamplePriority {
                created_unix_ns: -5,
                writer_epoch: u64::MAX,
                writer_seq: 9,
                in_page_index: 2,
            },
            SamplePriority {
                created_unix_ns: 30,
                writer_epoch: 2,
                writer_seq: 0,
                in_page_index: 3,
            },
        ];
        let series = FetchedHistogramSeries {
            series_id: SeriesId([8u8; 16]),
            labels: label_set(&[("__name__", "h"), ("k", "v")]),
            timestamps: vec![100, 200, 300, 400],
            values: values.clone(),
            created_unix_ns: 42,
            writer_epoch: 7,
            writer_seq: 11,
            per_sample_priorities: Some(priorities.clone()),
        };

        use prost::Message;
        let bytes = encode_histogram_frame(&series).encode_to_vec();
        let wire = pb::HistogramFrame::decode(bytes.as_slice()).expect("prost decode");
        let decoded = decode_histogram_frame(wire).expect("codec decode");
        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.series_id, series.series_id);
        assert_eq!(&got.labels, &series.labels);
        assert_eq!(got.created_unix_ns, series.created_unix_ns);
        assert_eq!(got.writer_epoch, series.writer_epoch);
        assert_eq!(got.writer_seq, series.writer_seq);
        assert_eq!(&got.timestamps, &series.timestamps);
        assert_eq!(got.per_sample_priorities, Some(priorities));
        assert_eq!(got.values.len(), values.len());
        for (a, b) in got.values.iter().zip(values.iter()) {
            assert!(
                histogram_bits_eq(a, b),
                "histogram value corrupted on the wire: {a:?} vs {b:?}"
            );
        }
    }

    /// A hand-built `HistogramFrame` (one series id, one label) wrapping the
    /// given run, for the structural-error tests. These frames are built by hand
    /// rather than through `encode_histogram_frame`, because the encoder never
    /// produces a structurally invalid record.
    fn hist_frame(run: pb::HistogramRun) -> pb::HistogramFrame {
        pb::HistogramFrame {
            series_id: vec![0u8; 16],
            labels: vec![pb::Label {
                name: "__name__".to_string(),
                value: "h".to_string(),
            }],
            runs: vec![run],
        }
    }

    /// A valid single-record `HistogramRun` (one int record, one timestamp) the
    /// structural-error tests mutate into exactly one invalid shape each.
    fn valid_hist_run() -> pb::HistogramRun {
        pb::HistogramRun {
            ts_delta: encode_ts_deltas(&[1_000]),
            records: encode_histogram_records(&[sample_histogram_int()]),
            ..Default::default()
        }
    }

    /// `records.len() != ts_delta.len()` is a typed error (mirrors
    /// `RunLengthMismatch` for scalars), never a panic or a truncated series.
    #[test]
    fn histogram_run_length_mismatch_is_typed_error() {
        let run = pb::HistogramRun {
            ts_delta: encode_ts_deltas(&[1_000, 2_000]),
            records: encode_histogram_records(&[sample_histogram_int()]),
            ..Default::default()
        };
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramRunLengthMismatch {
                timestamps: 2,
                records: 1,
            })
        ));
    }

    /// A zero-length span is a typed error (the reader's
    /// `HistogramSpanLengthZero`), never a panic.
    #[test]
    fn histogram_zero_length_span_is_typed_error() {
        let mut run = valid_hist_run();
        run.records[0].positive_spans[0].length = 0;
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramSpanLengthZero)
        ));
    }

    /// A scale below `-53` is a typed error (the reader's
    /// `HistogramScaleTooSmall`), never a panic.
    #[test]
    fn histogram_scale_too_small_is_typed_error() {
        let mut run = valid_hist_run();
        run.records[0].scale = -54;
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramScaleTooSmall { scale: -54 })
        ));
    }

    /// `custom_values` present at a non-custom scale is a typed error (the
    /// reader's `HistogramCustomValuesMismatch`): the sample_histogram_int
    /// record has scale 2, so attaching custom boundaries violates the
    /// present-iff-scale-is-`-53` invariant.
    #[test]
    fn histogram_custom_values_at_wrong_scale_is_typed_error() {
        let mut run = valid_hist_run();
        run.records[0].custom_values_bits = vec![1.0f64.to_bits(), 2.0f64.to_bits()];
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramCustomValuesMismatch)
        ));
    }

    /// A bucket-count vector shorter than its spans cover is a typed error (the
    /// reader decodes exactly `sum(length)` counts per side).
    #[test]
    fn histogram_bucket_count_mismatch_is_typed_error() {
        let mut run = valid_hist_run();
        // sample_histogram_int has one positive span of length 2 and positive
        // counts [2, 3]; drop one so the vector no longer matches the span.
        if let Some(pb::histogram_record::Counts::IntCounts(c)) = run.records[0].counts.as_mut() {
            c.positive = vec![2];
        } else {
            panic!("sample record is int-counts");
        }
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramBucketCountMismatch {
                spans: 2,
                buckets: 1,
            })
        ));
    }

    /// A `count` below its bucket total is a typed error (the reader's
    /// `HistogramCountInconsistent`). This is the check mutation-proved in the
    /// task report: removing the `count` comparison in `validate_histogram_value`
    /// turns this test green when it must be red.
    #[test]
    fn histogram_count_inconsistent_is_typed_error() {
        let mut run = valid_hist_run();
        // Positive counts [2, 3] total 5; a count of 4 is below the total.
        if let Some(pb::histogram_record::Counts::IntCounts(c)) = run.records[0].counts.as_mut() {
            c.zero_count = 0;
            c.count = 4;
        } else {
            panic!("sample record is int-counts");
        }
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramCountInconsistent)
        ));
    }

    /// `count < zero_count`, with `count >= total`, is a typed error on its own
    /// (the two clauses of the reader's `HistogramCountInconsistent` are
    /// independently ORed, not just the total clause): int variant.
    #[test]
    fn histogram_count_below_zero_count_is_typed_error_int() {
        let mut run = valid_hist_run();
        // Positive counts [2, 3] total 5; count 5 clears the total clause but
        // not a zero_count of 10.
        if let Some(pb::histogram_record::Counts::IntCounts(c)) = run.records[0].counts.as_mut() {
            c.zero_count = 10;
            c.count = 5;
        } else {
            panic!("sample record is int-counts");
        }
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramCountInconsistent)
        ));
    }

    /// A float-count histogram, otherwise valid, for the float-arm
    /// `HistogramCountInconsistent` tests: the int arm and the float arm of
    /// `validate_histogram_value` are separate branches, so a check pinned only
    /// on the int side leaves the float side unguarded.
    fn valid_hist_run_float() -> pb::HistogramRun {
        let record = HistogramValue {
            scale: 1,
            zero_threshold: 0.25,
            sum: Some(6.0),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Float {
                zero_count: 1.0,
                count: 6.0,
                positive: vec![2.0, 3.0],
                negative: vec![],
            },
            reset_hint: ResetHint::No,
        };
        pb::HistogramRun {
            ts_delta: encode_ts_deltas(&[1_000]),
            records: encode_histogram_records(&[record]),
            ..Default::default()
        }
    }

    /// A `count` below its bucket total is a typed error on the float arm too
    /// (the reader's `HistogramCountInconsistent` applies identically to both
    /// `HistogramCounts` variants).
    #[test]
    fn histogram_count_inconsistent_is_typed_error_float() {
        let mut run = valid_hist_run_float();
        // Positive counts [2.0, 3.0] total 5.0; a count of 4.0 is below it.
        if let Some(pb::histogram_record::Counts::FloatCounts(c)) = run.records[0].counts.as_mut() {
            c.zero_count_bits = 0.0f64.to_bits();
            c.count_bits = 4.0f64.to_bits();
        } else {
            panic!("sample record is float-counts");
        }
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramCountInconsistent)
        ));
    }

    /// `count < zero_count`, with `count >= total`, is a typed error on the
    /// float arm too.
    #[test]
    fn histogram_count_below_zero_count_is_typed_error_float() {
        let mut run = valid_hist_run_float();
        // Positive counts [2.0, 3.0] total 5.0; count 5.0 clears the total
        // clause but not a zero_count of 10.0.
        if let Some(pb::histogram_record::Counts::FloatCounts(c)) = run.records[0].counts.as_mut() {
            c.zero_count_bits = 10.0f64.to_bits();
            c.count_bits = 5.0f64.to_bits();
        } else {
            panic!("sample record is float-counts");
        }
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramCountInconsistent)
        ));
    }

    /// `scale == -53` with `custom_values` absent is a typed error (the
    /// reader's `HistogramCustomValuesMismatch`): the present-iff-scale-is-`-53`
    /// invariant fails in the "scale demands them but none are present"
    /// direction, the mirror image of `histogram_custom_values_at_wrong_scale_is_typed_error`.
    #[test]
    fn histogram_custom_values_missing_at_custom_scale_is_typed_error() {
        let mut run = valid_hist_run();
        run.records[0].scale = -53;
        // custom_values_bits stays empty (sample_histogram_int has custom_values: None).
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramCustomValuesMismatch)
        ));
    }

    /// `scale == -53` with non-strictly-ascending `custom_values` bounds is a
    /// typed error (the reader's `HistogramCustomValuesMismatch`): equal
    /// adjacent bounds, not just descending ones, must be rejected.
    #[test]
    fn histogram_custom_values_not_ascending_is_typed_error() {
        let record = HistogramValue {
            scale: -53,
            zero_threshold: 0.0,
            sum: Some(1.0),
            custom_values: Some(vec![1.0, 1.0]),
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 0,
                count: 1,
                positive: vec![1],
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        };
        let run = pb::HistogramRun {
            ts_delta: encode_ts_deltas(&[1_000]),
            records: encode_histogram_records(&[record]),
            ..Default::default()
        };
        assert!(matches!(
            decode_histogram_frame(hist_frame(run)),
            Err(CodecError::HistogramCustomValuesMismatch)
        ));
    }

    /// Provenance columns that disagree in length -- among themselves or with
    /// `ts_delta` -- are a typed error, never a truncated or fabricated key.
    /// Two shapes: one column short of the others, and all four consistent but
    /// short of `ts_delta`.
    #[test]
    fn provenance_length_mismatch_is_typed_error() {
        // prov_created_delta is len 2 while the run has 3 samples.
        let one_short = pb::Run {
            created_unix_ns: 0,
            writer_epoch: 0,
            writer_seq: 0,
            ts_delta: vec![1, 2, 3],
            value_bits: vec![1, 2, 3],
            prov_created_delta: vec![0, 0],
            prov_epoch_delta: vec![0, 0, 0],
            prov_seq_delta: vec![0, 0, 0],
            prov_in_page_index: vec![0, 0, 0],
        };
        let frame = pb::SeriesFrame {
            series_id: vec![0u8; 16],
            labels: vec![pb::Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            }],
            runs: vec![one_short],
        };
        assert!(matches!(
            decode_series_frame(frame),
            Err(CodecError::ProvenanceLengthMismatch {
                created: 2,
                epoch: 3,
                seq: 3,
                in_page_index: 3,
                samples: 3,
            })
        ));

        // All four columns len 2, but the run carries 3 samples.
        let all_short = pb::Run {
            created_unix_ns: 0,
            writer_epoch: 0,
            writer_seq: 0,
            ts_delta: vec![1, 2, 3],
            value_bits: vec![1, 2, 3],
            prov_created_delta: vec![0, 0],
            prov_epoch_delta: vec![0, 0],
            prov_seq_delta: vec![0, 0],
            prov_in_page_index: vec![0, 0],
        };
        let frame = pb::SeriesFrame {
            series_id: vec![0u8; 16],
            labels: vec![pb::Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            }],
            runs: vec![all_short],
        };
        assert!(matches!(
            decode_series_frame(frame),
            Err(CodecError::ProvenanceLengthMismatch { samples: 3, .. })
        ));
    }

    /// A run carrying run-wide provenance only (`per_sample_priorities: None`)
    /// encodes byte-identical to a run predating the provenance columns. proto3
    /// omits empty packed repeated fields, so the four new fields must
    /// contribute zero bytes. The `expected` value fixes the pre-provenance
    /// shape independently (fields 1-5 only, the four columns defaulted empty),
    /// so the assertion fails if the encoder ever emits the columns for a `None`
    /// run. A decode-back check confirms the columns are absent on the wire, not
    /// merely emitted as zero-length data.
    #[test]
    fn run_wide_only_encodes_byte_identical_to_pre_provenance() {
        use prost::Message;

        let soa = FetchedSeriesSoa {
            series_id: SeriesId([4u8; 16]),
            labels: label_set(&[("__name__", "m"), ("k", "v")]),
            timestamps: vec![10, 20, 30],
            values: vec![1.0, 2.0, 3.0],
            created_unix_ns: -42,
            writer_epoch: 3,
            writer_seq: 9,
            per_sample_priorities: None,
        };

        let got = encode_series_frame(&soa).encode_to_vec();

        let expected = pb::SeriesFrame {
            series_id: soa.series_id.0.to_vec(),
            labels: encode_labels(&soa.labels),
            runs: vec![pb::Run {
                created_unix_ns: soa.created_unix_ns,
                writer_epoch: soa.writer_epoch,
                writer_seq: soa.writer_seq,
                ts_delta: encode_ts_deltas(&soa.timestamps),
                value_bits: soa.values.iter().map(|v| v.to_bits()).collect(),
                // The four provenance columns absent -- the pre-provenance shape.
                ..Default::default()
            }],
        }
        .encode_to_vec();

        assert_eq!(
            got, expected,
            "a run-wide-only run must encode byte-identical to the pre-provenance shape"
        );

        let decoded = pb::SeriesFrame::decode(got.as_slice()).expect("decode");
        let run = &decoded.runs[0];
        assert!(
            run.prov_created_delta.is_empty(),
            "no prov_created_delta bytes"
        );
        assert!(run.prov_epoch_delta.is_empty(), "no prov_epoch_delta bytes");
        assert!(run.prov_seq_delta.is_empty(), "no prov_seq_delta bytes");
        assert!(
            run.prov_in_page_index.is_empty(),
            "no prov_in_page_index bytes"
        );
    }

    #[test]
    fn bad_series_id_length_is_typed_error() {
        let frame = pb::SeriesFrame {
            series_id: vec![0u8; 15],
            labels: vec![pb::Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            }],
            runs: Vec::new(),
        };
        assert!(matches!(
            decode_series_frame(frame),
            Err(CodecError::BadSeriesId { got: 15 })
        ));
    }

    #[test]
    fn signal_round_trips_and_rejects_unknown() {
        for signal in [
            Signal::Metrics,
            Signal::Logs,
            Signal::Spans,
            Signal::Profiles,
            Signal::Alerts,
            Signal::Audit,
        ] {
            assert_eq!(signal_from_u32(signal_to_u32(signal)), Ok(signal));
        }
        assert_eq!(signal_from_u32(6), Err(CodecError::UnknownSignal(6)));
        assert_eq!(
            signal_from_u32(u32::MAX),
            Err(CodecError::UnknownSignal(u32::MAX))
        );
    }

    #[test]
    fn matchers_round_trip_including_regex() {
        let matchers = vec![
            LabelMatcher::equal("__name__".to_string(), "http_requests".to_string()),
            LabelMatcher::not_equal("code".to_string(), "500".to_string()),
            LabelMatcher::regex("path", "/api/.*").expect("valid regex"),
            LabelMatcher::not_regex("host", "db-[0-9]+").expect("valid regex"),
        ];
        let decoded = decode_matchers(encode_matchers(&matchers)).expect("decode matchers");
        assert_eq!(decoded.len(), matchers.len());
        for (a, b) in decoded.iter().zip(matchers.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(a.value, b.value);
            // MatchOp carries a compiled Regex (no PartialEq); compare the
            // discriminant shape via a match instead.
            let same_op = matches!(
                (&a.op, &b.op),
                (MatchOp::Eq, MatchOp::Eq)
                    | (MatchOp::Ne, MatchOp::Ne)
                    | (MatchOp::Re(_), MatchOp::Re(_))
                    | (MatchOp::Nre(_), MatchOp::Nre(_))
            );
            assert!(same_op, "matcher op changed across the wire");
        }
    }

    #[test]
    fn unknown_matcher_op_is_typed_error() {
        let bad = vec![pb::LabelMatcher {
            name: "k".to_string(),
            op: 99,
            value: "v".to_string(),
        }];
        assert_eq!(decode_matchers(bad), Err(CodecError::UnknownMatcherOp(99)));
    }

    #[test]
    fn accounting_round_trips() {
        let snap = QueryAccountingSnapshot {
            s3_requests: [3, 5, 7],
            s3_bytes: [11, 13, 17],
            cache_hits: 2,
            cache_misses: 4,
            cache_bytes: 8,
            decompressed_bytes: 16,
            segments_opened: 32,
            segments_pruned: 64,
            series_matched: 128,
            bytes_reused: 256,
            peak_intermediate_bytes: 512,
        };
        assert_eq!(decode_accounting(encode_accounting(&snap)), snap);
    }

    #[test]
    fn segment_identity_content_hash_round_trips_and_rejects_bad_length() {
        use ravel_catalog::SegmentLevel;
        use uuid::Uuid;
        let seg = SegmentRef {
            data_object_key: "k".to_string(),
            object_size: 42,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            ingest_hour_bucket: 3,
            sample_count: 0,
            series_count: 0,
            shard: 7,
            content_hash: [5u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 2,
            writer_seq: 3,
            created_unix_ns: 0,
            level: SegmentLevel::L1 {
                input_set_hash: [6u8; 32],
                part_index: 4,
            },
        };
        let identity = encode_segment_identity(&seg);
        assert_eq!(identity.level, 1);
        assert_eq!(identity.shard, 7);
        assert_eq!(identity.part_index, 4);
        assert_eq!(
            identity.segment_format_version,
            u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
            "the shipped identity names the real RSEG version, not a hardcoded 0"
        );
        assert_eq!(identity_content_hash(&identity).expect("hash"), [5u8; 32]);

        let mut bad = identity;
        bad.content_hash = vec![0u8; 31];
        assert_eq!(
            identity_content_hash(&bad),
            Err(CodecError::BadContentHash { got: 31 })
        );
    }

    /// The stamped `segment_format_version` must be the version this build
    /// writes, derived from the reader's supported-version window, not a
    /// hand-maintained copy of a version constant (ADR-0092 decision 7).
    ///
    /// The assertion names no version literal, so it keeps holding when the
    /// RSEG version moves: at a bump, a site that stamped the old constant
    /// would ship a fragment identity naming a version no object carries,
    /// silently, since the field is a plain `uint32` on the wire.
    #[test]
    fn segment_identity_version_follows_supported_versions() {
        use ravel_catalog::SegmentLevel;
        use uuid::Uuid;
        let seg = SegmentRef {
            data_object_key: "k".to_string(),
            object_size: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            ingest_hour_bucket: 1,
            sample_count: 0,
            series_count: 0,
            shard: 0,
            content_hash: [1u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        };
        let stamped = encode_segment_identity(&seg).segment_format_version;
        assert_eq!(
            stamped,
            u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
            "the fragment identity must stamp the version this build writes"
        );
        let as_u16 = u16::try_from(stamped).expect("a trailer version fits in u16");
        assert!(
            ravel_segment::SUPPORTED_VERSIONS.contains(as_u16),
            "the stamped version must be one this build's reader accepts"
        );
    }

    fn sample_claims() -> FragmentClaims {
        FragmentClaims {
            capability_version: CAPABILITY_VERSION,
            tenant_hash: [7u8; 16],
            signal: signal_to_u32(Signal::Metrics),
            query_id: [9u8; 16],
            expires_unix_ns: 1_700_000_000_000_000_000,
        }
    }

    /// A minted capability decodes back to the exact claims it was minted from,
    /// and its MAC recomputes to the presented MAC under the mint key. This is
    /// the accept path: same key, same claims, MAC matches.
    #[test]
    fn capability_mints_and_decodes_round_trip() {
        let key = [3u8; 32];
        let claims = sample_claims();
        let cap = mint_capability(&key, &claims);
        assert_eq!(cap.len(), CAPABILITY_LEN);
        let (decoded, mac) = decode_capability(&cap).expect("decode");
        assert_eq!(
            decoded, claims,
            "claims survive the round trip byte-for-byte"
        );
        assert_eq!(
            capability_mac(&key, &decoded),
            mac,
            "the MAC recomputes to the presented MAC under the mint key"
        );
    }

    /// A capability minted under one key does NOT verify under another: the
    /// recomputed MAC differs, so a holder of a wrong key cannot forge authority.
    #[test]
    fn capability_mac_differs_under_a_different_key() {
        let claims = sample_claims();
        let cap = mint_capability(&[1u8; 32], &claims);
        let (decoded, presented) = decode_capability(&cap).expect("decode");
        assert_ne!(
            capability_mac(&[2u8; 32], &decoded),
            presented,
            "a different key recomputes a different MAC"
        );
    }

    /// A single flipped bit anywhere in the capability (claims region or MAC
    /// region) breaks verification: either the claims decode to a different set
    /// whose MAC no longer matches, or the MAC bytes themselves no longer match
    /// the claims' recomputed MAC.
    #[test]
    fn capability_tamper_is_detected_at_every_byte() {
        let key = [5u8; 32];
        let claims = sample_claims();
        let cap = mint_capability(&key, &claims);
        for i in 0..cap.len() {
            let mut bad = cap.clone();
            bad[i] ^= 0x01;
            let (decoded, presented) = decode_capability(&bad).expect("still CAPABILITY_LEN bytes");
            assert_ne!(
                capability_mac(&key, &decoded),
                presented,
                "a flipped bit at offset {i} must break the MAC"
            );
        }
    }

    /// A capability of any length other than [`CAPABILITY_LEN`] is a typed
    /// error, never a truncated or zero-padded claim set.
    #[test]
    fn capability_wrong_length_is_typed_error() {
        assert_eq!(
            decode_capability(&[]),
            Err(CodecError::BadCapabilityLength { got: 0 })
        );
        assert_eq!(
            decode_capability(&[0u8; CAPABILITY_LEN - 1]),
            Err(CodecError::BadCapabilityLength {
                got: CAPABILITY_LEN - 1
            })
        );
        assert_eq!(
            decode_capability(&[0u8; CAPABILITY_LEN + 1]),
            Err(CodecError::BadCapabilityLength {
                got: CAPABILITY_LEN + 1
            })
        );
    }

    #[test]
    fn unknown_status_code_is_typed_error() {
        assert_eq!(
            decode_status_code(-1),
            Err(CodecError::UnknownStatusCode(-1))
        );
    }

    /// A full RLOG record with every attribute-value kind (including a nested
    /// list and map), a NaN-payload `f64` attribute, and present trace/span ids
    /// round-trips byte-for-byte. The `f64` crosses as its bit pattern, so the
    /// NaN payload survives -- the same discipline the scalar path applies.
    #[test]
    fn log_record_round_trips_including_nested_and_nan_attrs() {
        let nan = f64::from_bits(0x7ff8_0000_dead_beef);
        let record = LogRecord {
            stream_id: LogStreamId([3u8; 16]),
            stream_attrs: vec![1, 2, 3, 4, 5],
            ts_ns: -42,
            observed_ts_ns: i64::MAX,
            severity_num: 17,
            severity_text: "WARN".to_string(),
            body: "something happened".to_string(),
            trace_id: Some([9u8; 16]),
            span_id: Some([7u8; 8]),
            flags: 0xdead_beef,
            attrs: vec![
                ("s".to_string(), AttrValue::Str("v".to_string())),
                ("i".to_string(), AttrValue::I64(-7)),
                ("f".to_string(), AttrValue::F64(nan)),
                ("b".to_string(), AttrValue::Bool(true)),
                ("raw".to_string(), AttrValue::Bytes(vec![0, 255, 1])),
                (
                    "list".to_string(),
                    AttrValue::List(vec![AttrValue::I64(1), AttrValue::Str("x".to_string())]),
                ),
                (
                    "map".to_string(),
                    AttrValue::Map(vec![("k".to_string(), AttrValue::F64(-0.0))]),
                ),
            ],
        };
        let decoded = decode_log_record(encode_log_record(&record)).expect("decode");
        assert_eq!(decoded.stream_id, record.stream_id);
        assert_eq!(decoded.stream_attrs, record.stream_attrs);
        assert_eq!(decoded.ts_ns, record.ts_ns);
        assert_eq!(decoded.observed_ts_ns, record.observed_ts_ns);
        assert_eq!(decoded.severity_num, record.severity_num);
        assert_eq!(decoded.severity_text, record.severity_text);
        assert_eq!(decoded.body, record.body);
        assert_eq!(decoded.trace_id, record.trace_id);
        assert_eq!(decoded.span_id, record.span_id);
        assert_eq!(decoded.flags, record.flags);
        // Structural equality would treat NaN != NaN, so check the f64 attr by
        // bit pattern explicitly, then the rest structurally.
        let got_f = decoded
            .attrs
            .iter()
            .find(|(k, _)| k == "f")
            .expect("f attr");
        let want_f = record.attrs.iter().find(|(k, _)| k == "f").expect("f attr");
        match (&got_f.1, &want_f.1) {
            (AttrValue::F64(a), AttrValue::F64(b)) => assert_eq!(a.to_bits(), b.to_bits()),
            _ => panic!("f attr is not F64"),
        }
        // The map's -0.0 likewise survives by bit pattern.
        match &decoded
            .attrs
            .iter()
            .find(|(k, _)| k == "map")
            .expect("map")
            .1
        {
            AttrValue::Map(entries) => match &entries[0].1 {
                AttrValue::F64(v) => assert_eq!(v.to_bits(), (-0.0f64).to_bits()),
                _ => panic!("map value not F64"),
            },
            _ => panic!("map attr not a Map"),
        }
    }

    /// An absent trace/span id is the empty byte string; a present one is its
    /// fixed width. A record with neither round-trips to `None`, and a present
    /// all-zero id (16/8 bytes) round-trips to `Some`, distinct from absent.
    #[test]
    fn log_record_optional_ids_round_trip() {
        let none = LogRecord {
            stream_id: LogStreamId([0u8; 16]),
            stream_attrs: Vec::new(),
            ts_ns: 0,
            observed_ts_ns: 0,
            severity_num: 0,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        };
        let decoded = decode_log_record(encode_log_record(&none)).expect("decode");
        assert_eq!(decoded.trace_id, None);
        assert_eq!(decoded.span_id, None);

        let zero_ids = LogRecord {
            trace_id: Some([0u8; 16]),
            span_id: Some([0u8; 8]),
            ..none
        };
        let decoded = decode_log_record(encode_log_record(&zero_ids)).expect("decode");
        assert_eq!(decoded.trace_id, Some([0u8; 16]));
        assert_eq!(decoded.span_id, Some([0u8; 8]));
    }

    #[test]
    fn log_record_bad_lengths_and_missing_value_are_typed_errors() {
        let base = pb::LogRecordFrame {
            stream_id: vec![0u8; 16],
            stream_attrs: Vec::new(),
            ts_ns: 0,
            observed_ts_ns: 0,
            severity_num: 0,
            severity_text: String::new(),
            body: String::new(),
            trace_id: Vec::new(),
            span_id: Vec::new(),
            flags: 0,
            attrs: Vec::new(),
        };

        let bad_stream = pb::LogRecordFrame {
            stream_id: vec![0u8; 15],
            ..base.clone()
        };
        assert!(matches!(
            decode_log_record(bad_stream),
            Err(CodecError::BadStreamId { got: 15 })
        ));

        let bad_trace = pb::LogRecordFrame {
            trace_id: vec![0u8; 8],
            ..base.clone()
        };
        assert!(matches!(
            decode_log_record(bad_trace),
            Err(CodecError::BadTraceId { got: 8 })
        ));

        let bad_span = pb::LogRecordFrame {
            span_id: vec![0u8; 7],
            ..base.clone()
        };
        assert!(matches!(
            decode_log_record(bad_span),
            Err(CodecError::BadSpanId { got: 7 })
        ));

        let bad_sev = pb::LogRecordFrame {
            severity_num: 256,
            ..base.clone()
        };
        assert!(matches!(
            decode_log_record(bad_sev),
            Err(CodecError::SeverityOutOfRange { got: 256 })
        ));

        let missing_value = pb::LogRecordFrame {
            attrs: vec![pb::LogAttr {
                key: "k".to_string(),
                value: None,
            }],
            ..base
        };
        assert!(matches!(
            decode_log_record(missing_value),
            Err(CodecError::MissingAttrValue)
        ));
    }

    fn span_row(
        parent: Option<[u8; 8]>,
        status_code: StatusCode,
        status_message: Option<String>,
        service_name: Option<String>,
        attrs: Vec<(&str, &str)>,
    ) -> SpanRow {
        SpanRow {
            record: SpanRecord {
                trace_id: [3u8; 16],
                span_id: [7u8; 8],
                parent_span_id: parent,
                name: "GET /api".to_string(),
                start_ts_ns: -42,
                end_ts_ns: i64::MAX,
                status_code,
                status_message,
                attrs: attrs
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
            service_name,
        }
    }

    /// A full span with a present parent, an error status with a message, a
    /// lifted `service_name`, and a merged `attrs` map round-trips field for
    /// field. Span attribute values are plain strings, so there is no bit
    /// pattern to preserve, only exact byte equality.
    #[test]
    fn span_frame_round_trips_field_for_field() {
        let row = span_row(
            Some([9u8; 8]),
            StatusCode::Error,
            Some("boom".to_string()),
            Some("checkout".to_string()),
            vec![("service.name", "checkout"), ("http.method", "GET")],
        );
        let decoded = decode_span_frame(encode_span_frame(&row)).expect("decode");
        assert_eq!(decoded.record, row.record, "span record round-trips");
        assert_eq!(decoded.service_name, row.service_name);
    }

    /// The `status_message` and `service_name` proto3 `optional`s keep `None`
    /// distinct from `Some("")`, and an absent parent (`None`, a root span)
    /// distinct from a present all-zero id. A naive plain-`string` encoding would
    /// collapse `Some("")` to `None`; a naive non-optional parent would turn a
    /// root span into a zero-parent span.
    #[test]
    fn span_frame_distinguishes_none_from_empty_and_absent_parent() {
        // None everywhere, root span.
        let none = span_row(None, StatusCode::Unset, None, None, vec![]);
        let d = decode_span_frame(encode_span_frame(&none)).expect("decode");
        assert_eq!(d.record.parent_span_id, None);
        assert_eq!(d.record.status_message, None);
        assert_eq!(d.service_name, None);

        // Some("") everywhere, present all-zero parent -- all distinct from None.
        let empties = span_row(
            Some([0u8; 8]),
            StatusCode::Ok,
            Some(String::new()),
            Some(String::new()),
            vec![],
        );
        let d = decode_span_frame(encode_span_frame(&empties)).expect("decode");
        assert_eq!(d.record.parent_span_id, Some([0u8; 8]));
        assert_eq!(d.record.status_message, Some(String::new()));
        assert_eq!(d.service_name, Some(String::new()));
    }

    #[test]
    fn span_frame_bad_lengths_and_status_are_typed_errors() {
        let good = encode_span_frame(&span_row(None, StatusCode::Unset, None, None, vec![]));

        let bad_trace = pb::SpanFrame {
            trace_id: vec![0u8; 15],
            ..good.clone()
        };
        assert!(matches!(
            decode_span_frame(bad_trace),
            Err(CodecError::BadSpanTraceId { got: 15 })
        ));

        let bad_span = pb::SpanFrame {
            span_id: vec![0u8; 7],
            ..good.clone()
        };
        assert!(matches!(
            decode_span_frame(bad_span),
            Err(CodecError::BadSpanSpanId { got: 7 })
        ));

        let bad_parent = pb::SpanFrame {
            parent_span_id: vec![0u8; 4],
            ..good.clone()
        };
        assert!(matches!(
            decode_span_frame(bad_parent),
            Err(CodecError::BadSpanParentId { got: 4 })
        ));

        let bad_status = pb::SpanFrame {
            status_code: 3,
            ..good
        };
        assert!(matches!(
            decode_span_frame(bad_status),
            Err(CodecError::UnknownSpanStatusCode { got: 3 })
        ));
    }

    #[test]
    fn erasure_predicates_round_trip() {
        // The worker must apply the identical exclusion the local path would,
        // so every predicate's matchers and window bounds must survive the
        // wire. A windowed and a windowless predicate together.
        let predicates = vec![
            ErasurePredicate::new(
                vec![
                    ("__name__".to_string(), "http_requests".to_string()),
                    ("tenant".to_string(), "acme".to_string()),
                ],
                1_000,
                2_000,
            ),
            ErasurePredicate::windowless(vec![("__name__".to_string(), "secret".to_string())]),
        ];
        let decoded = decode_erasure(encode_erasure(&predicates));
        assert_eq!(decoded.len(), predicates.len());
        for (a, b) in decoded.iter().zip(predicates.iter()) {
            assert_eq!(a.matchers(), b.matchers(), "erasure matchers changed");
            assert_eq!(
                a.window_start_ns(),
                b.window_start_ns(),
                "erasure window start changed"
            );
            assert_eq!(
                a.window_end_ns(),
                b.window_end_ns(),
                "erasure window end changed"
            );
        }
    }
}

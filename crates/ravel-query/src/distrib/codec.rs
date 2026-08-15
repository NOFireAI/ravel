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

use crate::erasure::ErasurePredicate;
use crate::fetcher::FetchedSeriesSoa;

/// The queryfrag protocol version this build speaks. A `FetchRequest` carrying
/// any other value is rejected by [`check_protocol_version`] so the coordinator
/// falls back to fully local execution (ADR-0071 version-skew rule), never
/// misinterprets a future frame layout.
///
/// Bumped 1 -> 2 for the ADR-0071 amendment (epic #1055 finding F-1): the shared
/// bearer token on `Pinned` fetches is replaced by a per-tenant, per-query
/// fragment capability ([`FragmentClaims`]). This is a version bump on an
/// existing versioned wire field, not a frozen persistent-format change. A
/// version-skewed worker is dropped at routing time (never a hard error), so a
/// rolling deploy degrades to coordinator-local execution, never to a wrong
/// answer.
pub const PROTOCOL_VERSION: u32 = 2;

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
pub fn encode_series_frame(series: &FetchedSeriesSoa) -> pb::SeriesFrame {
    pb::SeriesFrame {
        series_id: series.series_id.0.to_vec(),
        labels: encode_labels(&series.labels),
        runs: vec![pb::Run {
            created_unix_ns: series.created_unix_ns,
            writer_epoch: series.writer_epoch,
            writer_seq: series.writer_seq,
            ts_delta: encode_ts_deltas(&series.timestamps),
            value_bits: series.values.iter().map(|v| v.to_bits()).collect(),
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
            Ok(FetchedSeriesSoa {
                series_id,
                labels: labels.clone(),
                timestamps: decode_ts_deltas(&run.ts_delta),
                values: run.value_bits.iter().map(|b| f64::from_bits(*b)).collect(),
                created_unix_ns: run.created_unix_ns,
                writer_epoch: run.writer_epoch,
                writer_seq: run.writer_seq,
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
        // The single readable/writable RSEG version (ADR-0027). `SegmentRef`
        // carries no per-segment format version, so every current segment is a
        // VERSION_V6 object; shipping the real constant rather than a
        // meaningless hardcoded 0 gives the reconstruct-and-verify path the
        // version to check the fetched footer against.
        segment_format_version: u32::from(ravel_segment::VERSION_V6),
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
                }
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
            u32::from(ravel_segment::VERSION_V6),
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

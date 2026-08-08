//! Selective-erasure record construction, encoding/decoding, and validation
//! (ADR-0064, docs/catalog-and-mvcc.md). Three durable record types:
//!
//! - [`ErasureRequest`] (`.dreq`): a durable, immutable erasure predicate. It
//!   necessarily names the subject, so it is deletable after completion plus
//!   the protection horizon (ADR-0064 decision 5).
//! - [`ErasureCompletion`] (`.done`): permanent, PII-free audit evidence. By
//!   construction it carries no predicate key or value, only a blake3 hash of
//!   the canonical predicate encoding, per-bucket dropped counts, timestamps,
//!   and a closed-enum deferral cause.
//! - [`RewriteRecord`] (`rw.<hash16>.cmt`): the publish record for one bucket
//!   rewrite that physically drops matching records (ADR-0064 decision 3).
//!
//! Every timestamp is caller-supplied: this crate never reads the system
//! clock. Validation on decode matches the rigor of [`crate::record`]: a
//! `format_version` check, a `tenant_hash` length check, and the additional
//! frozen-format invariants each message carries.

use prost::Message;
use ravel_proto::commit::v1::{
    CompactionInputIdentity, ErasureCompletion, ErasureDeferralCause, ErasureRequest, RewriteRecord,
};
use uuid::Uuid;

use crate::signal;

/// The only supported `format_version` for every selective-erasure record.
pub const FORMAT_VERSION: u32 = 1;

/// Errors building, validating, or decoding a selective-erasure record.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ErasureError {
    #[error("invalid tenant_hash length: expected 16 bytes, got {0}")]
    InvalidTenantHashLen(usize),
    #[error("unsupported format_version: expected {expected}, got {actual}")]
    UnsupportedFormatVersion { expected: u32, actual: u32 },
    #[error("unknown or unspecified signal enum value {0}")]
    UnknownSignal(i32),
    #[error("invalid request id {0:?}: not a UUID")]
    InvalidRequestId(String),
    #[error("empty predicate: an erasure predicate must have at least one matcher")]
    EmptyPredicate,
    #[error("empty predicate matcher key: every matcher must name a label/attribute key")]
    EmptyPredicateMatcherKey,
    #[error("event-time window out of order: window_start_ns {start} > window_end_ns {end}")]
    WindowOutOfOrder { start: i64, end: i64 },
    #[error("invalid predicate_hash length: expected 32 bytes, got {0}")]
    InvalidPredicateHashLen(usize),
    #[error(
        "erasure timestamps out of order: requested_unix_ns {requested} > completed_unix_ns {completed}"
    )]
    TimestampsOutOfOrder { requested: i64, completed: i64 },
    #[error("unknown deferral cause enum value {0}")]
    UnknownDeferralCause(i32),
    #[error("invalid input_set_hash length: expected 32 bytes, got {0}")]
    InvalidInputSetHashLen(usize),
    #[error("empty input set: a rewrite record must supersede at least one input")]
    EmptyInputs,
    #[error(
        "rewrite record sets both `inputs` and `superseded_record_key`: exactly one may be set"
    )]
    BothInputsAndSuperseded,
    #[error(
        "rewrite record sets neither `inputs` nor `superseded_record_key`: exactly one must be set"
    )]
    NeitherInputsNorSuperseded,
    #[error(
        "input_set_hash mismatch: the record's input_set_hash is not the hash computed from its own inputs/superseded_record_key and applied request ids"
    )]
    InputSetHashMismatch,
    #[error(
        "bucket drop signal {bucket_drop} does not match the completion's top-level signal {completion}"
    )]
    BucketDropSignalMismatch { completion: i32, bucket_drop: i32 },
    #[error("empty drops: a rewrite record must apply at least one erasure request")]
    EmptyDrops,
    #[error("invalid output part content_hash length: expected 32 bytes, got {0}")]
    InvalidPartContentHashLen(usize),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
}

fn check_tenant_hash(bytes: &[u8]) -> Result<(), ErasureError> {
    if bytes.len() == 16 {
        Ok(())
    } else {
        Err(ErasureError::InvalidTenantHashLen(bytes.len()))
    }
}

fn check_format_version(actual: u32) -> Result<(), ErasureError> {
    if actual == FORMAT_VERSION {
        Ok(())
    } else {
        Err(ErasureError::UnsupportedFormatVersion {
            expected: FORMAT_VERSION,
            actual,
        })
    }
}

fn check_signal(raw: i32) -> Result<(), ErasureError> {
    signal::from_proto(raw)
        .map(|_| ())
        .map_err(|_| ErasureError::UnknownSignal(raw))
}

fn check_request_id(s: &str) -> Result<(), ErasureError> {
    Uuid::parse_str(s)
        .map(|_| ())
        .map_err(|_| ErasureError::InvalidRequestId(s.to_string()))
}

// --- ErasureRequest ---

/// Structural invariants every [`ErasureRequest`] must satisfy. Rejects the
/// empty predicate (an empty conjunction matches every record, which for an
/// irreversible erasure is catastrophic) and an out-of-order event-time
/// window. Zero/zero window means "no range restriction" and is valid.
pub fn validate_request(record: &ErasureRequest) -> Result<(), ErasureError> {
    check_format_version(record.format_version)?;
    check_tenant_hash(&record.tenant_hash)?;
    check_signal(record.signal)?;
    check_request_id(&record.request_id)?;
    if record.predicate.is_empty() {
        return Err(ErasureError::EmptyPredicate);
    }
    if record.predicate.iter().any(|m| m.key.is_empty()) {
        return Err(ErasureError::EmptyPredicateMatcherKey);
    }
    // Zero on either bound means "unset" (no restriction on that side); only a
    // fully-specified window is order-checked, matching this file's
    // zero-as-unset convention for scalar timestamp fields.
    if record.window_start_ns != 0
        && record.window_end_ns != 0
        && record.window_start_ns > record.window_end_ns
    {
        return Err(ErasureError::WindowOutOfOrder {
            start: record.window_start_ns,
            end: record.window_end_ns,
        });
    }
    Ok(())
}

/// Serialize an [`ErasureRequest`]. Infallible: encoding a well-formed message
/// cannot fail.
pub fn encode_request(record: &ErasureRequest) -> bytes::Bytes {
    record.encode_to_vec().into()
}

/// Deserialize and validate an [`ErasureRequest`].
pub fn decode_request(bytes: &[u8]) -> Result<ErasureRequest, ErasureError> {
    let record = ErasureRequest::decode(bytes)?;
    validate_request(&record)?;
    Ok(record)
}

// --- ErasureCompletion ---

/// Structural invariants every [`ErasureCompletion`] must satisfy. The
/// predicate is represented only by its 32-byte blake3 hash; there is no
/// field here that could hold a plaintext predicate key or value, so the
/// PII-exclusion guarantee is structural (see the module docs and the
/// `completion_encoding_carries_no_predicate_plaintext` test).
pub fn validate_completion(record: &ErasureCompletion) -> Result<(), ErasureError> {
    check_format_version(record.format_version)?;
    check_tenant_hash(&record.tenant_hash)?;
    check_signal(record.signal)?;
    check_request_id(&record.request_id)?;
    if record.predicate_hash.len() != 32 {
        return Err(ErasureError::InvalidPredicateHashLen(
            record.predicate_hash.len(),
        ));
    }
    // Reject unknown enum values, so a completion written by a future writer
    // with a deferral cause this version cannot interpret is a typed error
    // rather than a silently-misread integer.
    ErasureDeferralCause::try_from(record.deferral_cause)
        .map_err(|_| ErasureError::UnknownDeferralCause(record.deferral_cause))?;
    // Every per-bucket drop belongs to this completion's own request, so its
    // signal must equal the completion's top-level signal. A mismatch means a
    // bucket drop for the wrong signal was folded in and is rejected, never
    // silently accepted (ADR-0064 decision 4, amended).
    for drop in &record.bucket_drops {
        if drop.signal != record.signal {
            return Err(ErasureError::BucketDropSignalMismatch {
                completion: record.signal,
                bucket_drop: drop.signal,
            });
        }
    }
    if record.requested_unix_ns != 0
        && record.completed_unix_ns != 0
        && record.requested_unix_ns > record.completed_unix_ns
    {
        return Err(ErasureError::TimestampsOutOfOrder {
            requested: record.requested_unix_ns,
            completed: record.completed_unix_ns,
        });
    }
    Ok(())
}

/// Serialize an [`ErasureCompletion`]. Infallible.
pub fn encode_completion(record: &ErasureCompletion) -> bytes::Bytes {
    record.encode_to_vec().into()
}

/// Deserialize and validate an [`ErasureCompletion`].
pub fn decode_completion(bytes: &[u8]) -> Result<ErasureCompletion, ErasureError> {
    let record = ErasureCompletion::decode(bytes)?;
    validate_completion(&record)?;
    Ok(record)
}

// --- RewriteRecord ---

/// Domain-separation prefix for [`compute_rewrite_input_set_hash`]. Distinct
/// from ravel-maintain's compaction `input_set_hash` domain
/// (`ravel-compaction-input-set-v1\0`) so the two hashes can never collide
/// even if the same input byte sequence were fed to both.
pub const REWRITE_INPUT_SET_HASH_DOMAIN: &[u8] = b"ravel-rewrite-input-set-v1\0";

/// Compute the canonical `input_set_hash` for a [`RewriteRecord`]
/// (ADR-0064 decision 3, amended). This hash names the rewrite by exactly
/// what it supersedes and which erasure requests it applied, and is what the
/// rewrite record is keyed by (`rw.<hash16>.cmt`) under `CreateIfAbsent`.
///
/// The preimage, in order, is:
///
/// 1. the [`REWRITE_INPUT_SET_HASH_DOMAIN`] tag;
/// 2. exactly one of
///    - (inputs) if `inputs` is non-empty: a little-endian `u64` count of the
///      inputs, then for each input its `writer_id` UTF-8 bytes
///      (length-prefixed by a little-endian `u64`), then its `writer_epoch`
///      and `writer_seq` as little-endian `u64` — the same shape ravel-maintain's
///      compaction `input_set_hash` uses; or
///    - (superseded) if `superseded_record_key` is `Some`: its UTF-8 bytes
///      prefixed by their length as a little-endian `u64`;
/// 3. a little-endian `u64` count of `applied_request_ids`, then each
///    request id's UTF-8 bytes (length-prefixed the same way).
///
/// The length-prefix framing of the one-of case makes the two cases produce
/// distinct preimages by construction, so no `inputs` set can ever hash the
/// same as a `superseded_record_key` string.
///
/// This function does NOT sort. Inputs must already arrive sorted canonically
/// by `(writer_id, writer_epoch, writer_seq)` (per the proto comment on
/// `RewriteRecord.inputs`), matching how the sibling compaction hash works.
/// Callers MUST likewise sort `applied_request_ids` lexicographically before
/// calling. That sorting is exactly what makes an identical-batch retry
/// idempotent: the same sorted set yields the same hash, hence the same
/// `CreateIfAbsent` key, so a retry addresses the same object; two batches
/// that applied a different set of requests hash differently and diverge into
/// distinct records.
///
/// # Panics
///
/// Panics unless exactly one of `inputs` (non-empty) or
/// `superseded_record_key` (`Some` and non-empty) holds. Both-set or
/// neither-set is a caller contract violation the type system cannot express;
/// this is a library invariant check on malformed caller input, not a
/// data-path unwrap on untrusted external bytes.
pub fn compute_rewrite_input_set_hash(
    inputs: &[CompactionInputIdentity],
    superseded_record_key: Option<&str>,
    applied_request_ids: &[String],
) -> [u8; 32] {
    let has_inputs = !inputs.is_empty();
    let superseded = superseded_record_key.filter(|k| !k.is_empty());
    assert!(
        has_inputs != superseded.is_some(),
        "compute_rewrite_input_set_hash: exactly one of `inputs` (non-empty) or \
         `superseded_record_key` (non-empty) must be set \
         (inputs_present={has_inputs}, superseded_present={})",
        superseded.is_some()
    );

    let mut hasher = blake3::Hasher::new();
    hasher.update(REWRITE_INPUT_SET_HASH_DOMAIN);
    if has_inputs {
        hasher.update(&(inputs.len() as u64).to_le_bytes());
        for input in inputs {
            let wid = input.writer_id.as_bytes();
            hasher.update(&(wid.len() as u64).to_le_bytes());
            hasher.update(wid);
            hasher.update(&input.writer_epoch.to_le_bytes());
            hasher.update(&input.writer_seq.to_le_bytes());
        }
    } else if let Some(key) = superseded {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
    }
    hasher.update(&(applied_request_ids.len() as u64).to_le_bytes());
    for id in applied_request_ids {
        hasher.update(&(id.len() as u64).to_le_bytes());
        hasher.update(id.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

/// Extract and lexicographically sort the applied request ids from a rewrite
/// record's drops, the canonical order [`compute_rewrite_input_set_hash`]
/// expects.
fn sorted_applied_request_ids(record: &RewriteRecord) -> Vec<String> {
    let mut ids: Vec<String> = record.drops.iter().map(|d| d.request_id.clone()).collect();
    ids.sort();
    ids
}

/// Structural invariants every [`RewriteRecord`] must satisfy. A rewrite
/// supersedes at least one input and applies at least one erasure request;
/// its output parts may be empty (a bucket whose every record matched is
/// rewritten to nothing). Each output part's `content_hash` must be 32 bytes
/// because the part key is reconstructed from it (ADR-0010 §7 discipline).
pub fn validate_rewrite(record: &RewriteRecord) -> Result<(), ErasureError> {
    check_format_version(record.format_version)?;
    check_tenant_hash(&record.tenant_hash)?;
    check_signal(record.signal)?;
    if record.input_set_hash.len() != 32 {
        return Err(ErasureError::InvalidInputSetHashLen(
            record.input_set_hash.len(),
        ));
    }
    // Exactly one of `inputs` (raw L0/L1 identities) or `superseded_record_key`
    // (a whole live compaction/rewrite record superseded as a unit) names what
    // this rewrite replaces (ADR-0064 decision 3, amended). Both-set or
    // neither-set is malformed.
    let has_inputs = !record.inputs.is_empty();
    let superseded =
        (!record.superseded_record_key.is_empty()).then_some(record.superseded_record_key.as_str());
    match (has_inputs, superseded.is_some()) {
        (true, true) => return Err(ErasureError::BothInputsAndSuperseded),
        (false, false) => return Err(ErasureError::NeitherInputsNorSuperseded),
        _ => {}
    }
    if record.drops.is_empty() {
        return Err(ErasureError::EmptyDrops);
    }
    for drop in &record.drops {
        check_request_id(&drop.request_id)?;
    }
    for part in &record.parts {
        if part.content_hash.len() != 32 {
            return Err(ErasureError::InvalidPartContentHashLen(
                part.content_hash.len(),
            ));
        }
    }
    // Verify the stored input_set_hash actually is the canonical hash of this
    // record's own inputs/superseded key and its sorted applied request ids,
    // not merely a well-formed 32-byte value (ADR-0064 decision 3, amended).
    // This is what makes an identical-batch retry idempotent under
    // CreateIfAbsent and what prevents two logically different rewrites from
    // colliding on one key.
    let expected = compute_rewrite_input_set_hash(
        &record.inputs,
        superseded,
        &sorted_applied_request_ids(record),
    );
    if record.input_set_hash.as_slice() != expected.as_slice() {
        return Err(ErasureError::InputSetHashMismatch);
    }
    Ok(())
}

/// Serialize a [`RewriteRecord`]. Infallible.
pub fn encode_rewrite(record: &RewriteRecord) -> bytes::Bytes {
    record.encode_to_vec().into()
}

/// Deserialize and validate a [`RewriteRecord`].
pub fn decode_rewrite(bytes: &[u8]) -> Result<RewriteRecord, ErasureError> {
    let record = RewriteRecord::decode(bytes)?;
    validate_rewrite(&record)?;
    Ok(record)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ravel_proto::commit::v1::{
        CompactionInputIdentity, CompactionPart, ErasureBucketDrop, ErasurePredicateMatcher,
        RewriteDrop,
    };

    const VALID_SIGNALS: [i32; 6] = [1, 2, 3, 4, 5, 6]; // METRICS..AUDIT

    fn uuid_string() -> impl Strategy<Value = String> {
        any::<u128>().prop_map(|n| Uuid::from_u128(n).to_string())
    }

    fn matcher_strategy() -> impl Strategy<Value = ErasurePredicateMatcher> {
        ("[a-zA-Z0-9_.-]{1,12}", "[a-zA-Z0-9_.:-]{0,16}")
            .prop_map(|(key, value)| ErasurePredicateMatcher { key, value })
    }

    prop_compose! {
        fn request_strategy()(
            tenant_hash in prop::collection::vec(any::<u8>(), 16),
            signal in prop::sample::select(VALID_SIGNALS.to_vec()),
            request_id in uuid_string(),
            created_unix_ns in any::<i64>(),
            predicate in prop::collection::vec(matcher_strategy(), 1..6),
            has_window in any::<bool>(),
            window_start in 1i64..1_000_000,
            window_len in 0i64..1_000_000,
            reason in "[a-zA-Z0-9 ._-]{0,24}",
        ) -> ErasureRequest {
            let (window_start_ns, window_end_ns) = if has_window {
                (window_start, window_start + window_len)
            } else {
                (0, 0)
            };
            ErasureRequest {
                format_version: 1,
                tenant_hash,
                signal,
                request_id,
                created_unix_ns,
                predicate,
                window_start_ns,
                window_end_ns,
                reason,
            }
        }
    }

    fn bucket_drop_strategy() -> impl Strategy<Value = ErasureBucketDrop> {
        (
            prop::sample::select(VALID_SIGNALS.to_vec()),
            any::<u32>(),
            any::<u32>(),
            any::<u64>(),
        )
            .prop_map(|(signal, shard, ingest_hour_bucket, dropped_count)| {
                ErasureBucketDrop {
                    signal,
                    shard,
                    ingest_hour_bucket,
                    dropped_count,
                }
            })
    }

    prop_compose! {
        fn completion_strategy()(
            tenant_hash in prop::collection::vec(any::<u8>(), 16),
            signal in prop::sample::select(VALID_SIGNALS.to_vec()),
            request_id in uuid_string(),
            predicate_hash in prop::collection::vec(any::<u8>(), 32),
            bucket_drops in prop::collection::vec(bucket_drop_strategy(), 0..5),
            has_ts in any::<bool>(),
            requested in 1i64..1_000_000,
            elapsed in 0i64..1_000_000,
            deferral_cause in prop::sample::select(vec![0i32, 1]),
        ) -> ErasureCompletion {
            let (requested_unix_ns, completed_unix_ns) = if has_ts {
                (requested, requested + elapsed)
            } else {
                (0, 0)
            };
            // Every bucket drop belongs to this completion's request, so its
            // signal must equal the top-level signal (validate_completion now
            // enforces this); align the generated drops accordingly.
            let bucket_drops = bucket_drops
                .into_iter()
                .map(|mut d| {
                    d.signal = signal;
                    d
                })
                .collect();
            ErasureCompletion {
                format_version: 1,
                tenant_hash,
                signal,
                request_id,
                predicate_hash,
                bucket_drops,
                requested_unix_ns,
                completed_unix_ns,
                deferral_cause,
            }
        }
    }

    fn input_strategy() -> impl Strategy<Value = CompactionInputIdentity> {
        (uuid_string(), any::<u64>(), any::<u64>()).prop_map(
            |(writer_id, writer_epoch, writer_seq)| CompactionInputIdentity {
                writer_id,
                writer_epoch,
                writer_seq,
            },
        )
    }

    fn part_strategy() -> impl Strategy<Value = CompactionPart> {
        (
            any::<u32>(),
            prop::collection::vec(any::<u8>(), 32),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(|(part_index, content_hash, object_size, sample_count)| {
                CompactionPart {
                    part_index,
                    first_series_id: vec![0; 16],
                    last_series_id: vec![0xff; 16],
                    content_hash,
                    object_size,
                    sample_count,
                    series_count: 1,
                    run_count: 1,
                    min_event_ts_ns: 0,
                    max_event_ts_ns: 0,
                    segment_format_version: 3,
                }
            })
    }

    fn drop_strategy() -> impl Strategy<Value = RewriteDrop> {
        (uuid_string(), any::<u64>()).prop_map(|(request_id, dropped_count)| RewriteDrop {
            request_id,
            dropped_count,
        })
    }

    prop_compose! {
        fn rewrite_strategy()(
            tenant_hash in prop::collection::vec(any::<u8>(), 16),
            signal in prop::sample::select(VALID_SIGNALS.to_vec()),
            shard in any::<u32>(),
            ingest_hour_bucket in any::<u32>(),
            inputs in prop::collection::vec(input_strategy(), 1..5),
            parts in prop::collection::vec(part_strategy(), 0..4),
            drops in prop::collection::vec(drop_strategy(), 1..4),
            created_unix_ns in any::<i64>(),
        ) -> RewriteRecord {
            // A valid rewrite carries the canonical hash of its own inputs and
            // sorted applied request ids, so it survives validate_rewrite's
            // recomputation check on decode.
            let mut request_ids: Vec<String> =
                drops.iter().map(|d| d.request_id.clone()).collect();
            request_ids.sort();
            let input_set_hash =
                compute_rewrite_input_set_hash(&inputs, None, &request_ids).to_vec();
            RewriteRecord {
                format_version: 1,
                tenant_hash,
                signal,
                shard,
                ingest_hour_bucket,
                inputs,
                input_set_hash,
                parts,
                drops,
                created_unix_ns,
                superseded_record_key: String::new(),
            }
        }
    }

    proptest! {
        #[test]
        fn request_round_trips(record in request_strategy()) {
            let bytes = encode_request(&record);
            let decoded = decode_request(&bytes).expect("decode");
            prop_assert_eq!(decoded, record);
        }

        #[test]
        fn completion_round_trips(record in completion_strategy()) {
            let bytes = encode_completion(&record);
            let decoded = decode_completion(&bytes).expect("decode");
            prop_assert_eq!(decoded, record);
        }

        #[test]
        fn rewrite_round_trips(record in rewrite_strategy()) {
            let bytes = encode_rewrite(&record);
            let decoded = decode_rewrite(&bytes).expect("decode");
            prop_assert_eq!(decoded, record);
        }

        // Truncated bytes must produce a typed error, never a panic or wrong
        // data (repo corrupt-input rule).
        #[test]
        fn truncated_request_bytes_never_panic(record in request_strategy(), cut in 0usize..64) {
            let bytes = encode_request(&record);
            let n = cut.min(bytes.len());
            let _ = decode_request(&bytes[..n]); // must not panic
        }

        #[test]
        fn arbitrary_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..128)) {
            let _ = decode_request(&raw);
            let _ = decode_completion(&raw);
            let _ = decode_rewrite(&raw);
        }
    }

    fn valid_request() -> ErasureRequest {
        ErasureRequest {
            format_version: 1,
            tenant_hash: vec![0x11; 16],
            signal: 1,
            request_id: Uuid::from_u128(1).to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: "user_id".to_string(),
                value: "u123".to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        }
    }

    fn valid_completion() -> ErasureCompletion {
        ErasureCompletion {
            format_version: 1,
            tenant_hash: vec![0x11; 16],
            signal: 1,
            request_id: Uuid::from_u128(1).to_string(),
            predicate_hash: vec![0x22; 32],
            bucket_drops: vec![],
            requested_unix_ns: 0,
            completed_unix_ns: 0,
            deferral_cause: 0,
        }
    }

    fn valid_rewrite() -> RewriteRecord {
        let inputs = vec![CompactionInputIdentity {
            writer_id: Uuid::from_u128(9).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }];
        let drops = vec![RewriteDrop {
            request_id: Uuid::from_u128(1).to_string(),
            dropped_count: 5,
        }];
        let mut request_ids: Vec<String> = drops.iter().map(|d| d.request_id.clone()).collect();
        request_ids.sort();
        let input_set_hash = compute_rewrite_input_set_hash(&inputs, None, &request_ids).to_vec();
        RewriteRecord {
            format_version: 1,
            tenant_hash: vec![0x11; 16],
            signal: 1,
            shard: 0,
            ingest_hour_bucket: 0,
            inputs,
            input_set_hash,
            parts: vec![],
            drops,
            created_unix_ns: 0,
            superseded_record_key: String::new(),
        }
    }

    /// A valid rewrite that supersedes a whole live record key instead of a
    /// raw input set (the amended second case): `inputs` empty,
    /// `superseded_record_key` set, hash recomputed from that key.
    fn valid_superseding_rewrite() -> RewriteRecord {
        let superseded = "t/aabb/m/c/0001/20260726T14/l1.0011223344556677.cmt".to_string();
        let drops = vec![RewriteDrop {
            request_id: Uuid::from_u128(1).to_string(),
            dropped_count: 5,
        }];
        let mut request_ids: Vec<String> = drops.iter().map(|d| d.request_id.clone()).collect();
        request_ids.sort();
        let input_set_hash =
            compute_rewrite_input_set_hash(&[], Some(&superseded), &request_ids).to_vec();
        RewriteRecord {
            format_version: 1,
            tenant_hash: vec![0x11; 16],
            signal: 1,
            shard: 1,
            ingest_hour_bucket: 0,
            inputs: vec![],
            input_set_hash,
            parts: vec![],
            drops,
            created_unix_ns: 0,
            superseded_record_key: superseded,
        }
    }

    #[test]
    fn request_rejects_wrong_format_version() {
        let mut r = valid_request();
        r.format_version = 2;
        assert_eq!(
            validate_request(&r),
            Err(ErasureError::UnsupportedFormatVersion {
                expected: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn request_rejects_bad_tenant_hash_len() {
        let mut r = valid_request();
        r.tenant_hash = vec![0; 15];
        assert_eq!(
            validate_request(&r),
            Err(ErasureError::InvalidTenantHashLen(15))
        );
    }

    #[test]
    fn request_rejects_empty_predicate() {
        let mut r = valid_request();
        r.predicate.clear();
        assert_eq!(validate_request(&r), Err(ErasureError::EmptyPredicate));
    }

    #[test]
    fn request_rejects_empty_matcher_key() {
        let mut r = valid_request();
        r.predicate[0].key = String::new();
        assert_eq!(
            validate_request(&r),
            Err(ErasureError::EmptyPredicateMatcherKey)
        );
    }

    #[test]
    fn request_rejects_out_of_order_window() {
        let mut r = valid_request();
        r.window_start_ns = 100;
        r.window_end_ns = 50;
        assert_eq!(
            validate_request(&r),
            Err(ErasureError::WindowOutOfOrder {
                start: 100,
                end: 50
            })
        );
    }

    #[test]
    fn request_rejects_unspecified_signal() {
        let mut r = valid_request();
        r.signal = 0; // SIGNAL_UNSPECIFIED
        assert_eq!(validate_request(&r), Err(ErasureError::UnknownSignal(0)));
    }

    #[test]
    fn request_rejects_non_uuid_request_id() {
        let mut r = valid_request();
        r.request_id = "not-a-uuid".to_string();
        assert!(matches!(
            validate_request(&r),
            Err(ErasureError::InvalidRequestId(_))
        ));
    }

    #[test]
    fn completion_rejects_bad_predicate_hash_len() {
        let mut c = valid_completion();
        c.predicate_hash = vec![0; 31];
        assert_eq!(
            validate_completion(&c),
            Err(ErasureError::InvalidPredicateHashLen(31))
        );
    }

    #[test]
    fn completion_rejects_out_of_order_timestamps() {
        let mut c = valid_completion();
        c.requested_unix_ns = 100;
        c.completed_unix_ns = 50;
        assert_eq!(
            validate_completion(&c),
            Err(ErasureError::TimestampsOutOfOrder {
                requested: 100,
                completed: 50
            })
        );
    }

    #[test]
    fn completion_rejects_unknown_deferral_cause() {
        let mut c = valid_completion();
        c.deferral_cause = 99;
        assert_eq!(
            validate_completion(&c),
            Err(ErasureError::UnknownDeferralCause(99))
        );
    }

    #[test]
    fn rewrite_rejects_neither_inputs_nor_superseded() {
        // inputs empty and superseded_record_key empty: the amended one-of
        // invariant is violated in the neither direction.
        let mut rw = valid_rewrite();
        rw.inputs.clear();
        assert!(rw.superseded_record_key.is_empty());
        assert_eq!(
            validate_rewrite(&rw),
            Err(ErasureError::NeitherInputsNorSuperseded)
        );
    }

    #[test]
    fn rewrite_rejects_both_inputs_and_superseded() {
        // inputs non-empty AND superseded_record_key non-empty: violated in
        // the both direction.
        let mut rw = valid_rewrite();
        rw.superseded_record_key = "t/aa/m/c/0001/20260726T14/l1.0011223344556677.cmt".to_string();
        assert!(!rw.inputs.is_empty());
        assert_eq!(
            validate_rewrite(&rw),
            Err(ErasureError::BothInputsAndSuperseded)
        );
    }

    #[test]
    fn rewrite_accepts_superseded_record_key() {
        let rw = valid_superseding_rewrite();
        assert!(rw.inputs.is_empty());
        assert!(!rw.superseded_record_key.is_empty());
        assert_eq!(validate_rewrite(&rw), Ok(()));
        // And it round-trips through encode/decode under the new validation.
        let bytes = encode_rewrite(&rw);
        assert_eq!(decode_rewrite(&bytes).expect("decode"), rw);
    }

    #[test]
    fn rewrite_rejects_input_set_hash_mismatch() {
        // A well-formed 32-byte hash that is not the canonical hash of this
        // record's own inputs/drops is rejected (regression: validation used
        // to accept any 32-byte value).
        let mut rw = valid_rewrite();
        rw.input_set_hash = vec![0x33; 32];
        assert_eq!(
            validate_rewrite(&rw),
            Err(ErasureError::InputSetHashMismatch)
        );
    }

    #[test]
    fn rewrite_rejects_empty_drops() {
        let mut rw = valid_rewrite();
        rw.drops.clear();
        assert_eq!(validate_rewrite(&rw), Err(ErasureError::EmptyDrops));
    }

    #[test]
    fn compute_rewrite_input_set_hash_is_deterministic() {
        // Same inputs, same (already sorted) request ids: identical hash. This
        // is what makes an identical-batch retry idempotent under
        // CreateIfAbsent.
        let inputs = vec![CompactionInputIdentity {
            writer_id: Uuid::from_u128(9).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }];
        let ids = vec!["req-a".to_string(), "req-b".to_string()];
        let h1 = compute_rewrite_input_set_hash(&inputs, None, &ids);
        let h2 = compute_rewrite_input_set_hash(&inputs, None, &ids);
        assert_eq!(h1, h2);
    }

    #[test]
    fn compute_rewrite_input_set_hash_differs_on_request_ids() {
        // The collision the checkpoint reproduced: two batches over the same
        // inputs that applied a different set of requests must diverge.
        let inputs = vec![CompactionInputIdentity {
            writer_id: Uuid::from_u128(9).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }];
        let one = vec!["req-a".to_string()];
        let two = vec!["req-a".to_string(), "req-b".to_string()];
        assert_ne!(
            compute_rewrite_input_set_hash(&inputs, None, &one),
            compute_rewrite_input_set_hash(&inputs, None, &two)
        );
    }

    #[test]
    fn compute_rewrite_input_set_hash_no_cross_case_collision() {
        // The same logical value fed through the `inputs` case versus the
        // `superseded_record_key` case must not collide: the length-prefix
        // framing separates the two cases by construction, not by luck.
        let writer = Uuid::from_u128(9).to_string();
        let inputs = vec![CompactionInputIdentity {
            writer_id: writer.clone(),
            writer_epoch: 0,
            writer_seq: 0,
        }];
        let ids = vec!["req-a".to_string()];
        let via_inputs = compute_rewrite_input_set_hash(&inputs, None, &ids);
        let via_superseded = compute_rewrite_input_set_hash(&[], Some(&writer), &ids);
        assert_ne!(via_inputs, via_superseded);
    }

    #[test]
    #[should_panic(expected = "exactly one")]
    fn compute_rewrite_input_set_hash_panics_on_neither() {
        let _ = compute_rewrite_input_set_hash(&[], None, &["req-a".to_string()]);
    }

    #[test]
    #[should_panic(expected = "exactly one")]
    fn compute_rewrite_input_set_hash_panics_on_both() {
        let inputs = vec![CompactionInputIdentity {
            writer_id: Uuid::from_u128(9).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }];
        let _ = compute_rewrite_input_set_hash(&inputs, Some("some-key"), &["req-a".to_string()]);
    }

    #[test]
    fn completion_rejects_bucket_drop_signal_mismatch() {
        let mut c = valid_completion();
        c.signal = 1; // METRICS
        c.bucket_drops = vec![ErasureBucketDrop {
            signal: 2, // LOGS: differs from the completion's top-level signal
            shard: 0,
            ingest_hour_bucket: 0,
            dropped_count: 3,
        }];
        assert_eq!(
            validate_completion(&c),
            Err(ErasureError::BucketDropSignalMismatch {
                completion: 1,
                bucket_drop: 2,
            })
        );
    }

    #[test]
    fn rewrite_rejects_bad_input_set_hash_len() {
        let mut rw = valid_rewrite();
        rw.input_set_hash = vec![0; 16];
        assert_eq!(
            validate_rewrite(&rw),
            Err(ErasureError::InvalidInputSetHashLen(16))
        );
    }

    #[test]
    fn rewrite_allows_empty_parts() {
        // A bucket whose every record matched is rewritten to nothing.
        let rw = valid_rewrite();
        assert!(rw.parts.is_empty());
        assert!(validate_rewrite(&rw).is_ok());
    }

    /// Structural PII-exclusion (ADR-0064 decision 1): the `.done` completion
    /// record must carry no plaintext predicate value. Proven structurally,
    /// not by convention: a subject sentinel present in the `.dreq` encoding
    /// is absent from the `.done` encoding built for the same request.
    #[test]
    fn completion_encoding_carries_no_predicate_plaintext() {
        const SUBJECT: &str = "SUBJECT-SENTINEL-u123-should-never-appear";
        let request = ErasureRequest {
            predicate: vec![ErasurePredicateMatcher {
                key: "user_id".to_string(),
                value: SUBJECT.to_string(),
            }],
            ..valid_request()
        };
        let request_bytes = encode_request(&request);
        // The request necessarily contains the subject identifier.
        assert!(contains_subsequence(&request_bytes, SUBJECT.as_bytes()));

        // The completion for the same request carries only a hash of the
        // predicate, never its plaintext.
        let completion = ErasureCompletion {
            request_id: request.request_id.clone(),
            predicate_hash: vec![0xab; 32],
            deferral_cause: ErasureDeferralCause::LegalHold as i32,
            ..valid_completion()
        };
        let completion_bytes = encode_completion(&completion);
        assert!(!contains_subsequence(&completion_bytes, SUBJECT.as_bytes()));
    }

    fn contains_subsequence(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }
}

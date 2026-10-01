//! `CommitRecord` construction, encoding/decoding, and validation
//! (docs/catalog-and-mvcc.md, ADR-0010 §1).

use prost::Message;
use ravel_proto::commit::v1::{CommitRecord, CompactionRecord, RetentionTombstone};
use ravel_types::{CommitToken, Signal, TenantHash};
use uuid::Uuid;

use crate::keys::{self, KeyError};
use crate::{erasure, signal};

/// The only supported `CommitRecord.format_version`.
pub const FORMAT_VERSION: u32 = 1;
/// The `CompactionRecord.format_version` of a record that supersedes nothing,
/// and the floor of the supported range (ADR-0066 decision 2).
pub const COMPACTION_FORMAT_VERSION: u32 = 1;
/// The `CompactionRecord.format_version` of a record that names, in
/// `superseded_record_key`, the compaction record it re-encodes, and the
/// ceiling of the supported range (ADR-0066, force 2 amendment).
pub const COMPACTION_SUPERSEDING_FORMAT_VERSION: u32 = 2;
/// The only supported `RetentionTombstone.format_version` (ADR-0066 decision 2).
pub const TOMBSTONE_FORMAT_VERSION: u32 = 1;
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// A durable commit-protocol record kind that carries a `format_version`.
/// Every variant here must have a decode-and-validate pair that rejects an
/// out-of-range version through [`check_format_version`]; the enumeration
/// guard test asserts it, so a fourth kind cannot ship ungated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    Commit,
    Compaction,
    RetentionTombstone,
}

impl RecordKind {
    fn as_str(self) -> &'static str {
        match self {
            RecordKind::Commit => "commit record",
            RecordKind::Compaction => "compaction record",
            RecordKind::RetentionTombstone => "retention tombstone",
        }
    }
}

/// The shared supported-set gate for every versioned record kind: `actual`
/// must fall within the inclusive `[min, max]` band. A version below the
/// floor (a writer that failed to stamp, leaving proto3's default 0) and a
/// version above the ceiling (a future writer whose meaning this reader does
/// not know) are both refused with the typed error naming the kind and the
/// version seen, never read as the supported version.
fn check_format_version(
    kind: RecordKind,
    actual: u32,
    min: u32,
    max: u32,
) -> Result<(), RecordError> {
    if actual < min || actual > max {
        return Err(RecordError::UnsupportedRecordFormatVersion {
            kind,
            min,
            max,
            actual,
        });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RecordError {
    #[error("invalid tenant_hash length: expected 16 bytes, got {0}")]
    InvalidTenantHashLen(usize),
    #[error("invalid content_hash length: expected 32 bytes, got {0}")]
    InvalidContentHashLen(usize),
    #[error("unsupported format_version: expected {expected}, got {actual}")]
    UnsupportedFormatVersion { expected: u32, actual: u32 },
    #[error(
        "unsupported {kind} format_version: supported {min}..={max}, got {actual}",
        kind = kind.as_str()
    )]
    UnsupportedRecordFormatVersion {
        kind: RecordKind,
        min: u32,
        max: u32,
        actual: u32,
    },
    #[error("event ts out of order: min_event_ts_ns {min} > max_event_ts_ns {max}")]
    EventTsOutOfOrder { min: i64, max: i64 },
    #[error("ingest ts out of order: min_ingest_ts_ns {min} > max_ingest_ts_ns {max}")]
    IngestTsOutOfOrder { min: i64, max: i64 },
    #[error(
        "ingest_hour_bucket {ingest_hour_bucket} is after created_unix_ns's hour bucket {created_hour_bucket} (a flush cannot open after it was recorded as created)"
    )]
    IngestHourInconsistent {
        ingest_hour_bucket: u32,
        created_hour_bucket: i64,
    },
    #[error("invalid writer id {0:?}")]
    InvalidWriterId(String),
    #[error(
        "format_version 1 compaction record sets superseded_record_key {0:?}: only a format_version 2 record may"
    )]
    SupersededRecordKeyOnVersionOne(String),
    #[error("format_version 2 compaction record has an empty superseded_record_key")]
    MissingSupersededRecordKey,
    #[error(
        "superseded_record_key {0:?} of a format_version 2 compaction record does not parse as a compaction record key"
    )]
    InvalidSupersededRecordKey(String),
    #[error(
        "superseded_record_key {0:?} of a format_version 2 compaction record is not the canonical rendering of the key it parses to"
    )]
    NonCanonicalSupersededRecordKey(String),
    #[error(
        "superseded_record_key names a different bucket than this compaction record: key names \
         (tenant={key_tenant_hex}, signal={key_signal}, shard={key_shard}, hour={key_hour}), \
         record is (tenant={record_tenant_hex}, signal={record_signal}, shard={record_shard}, hour={record_hour})"
    )]
    SupersededRecordKeyBucketMismatch {
        key_tenant_hex: String,
        key_signal: i32,
        key_shard: u32,
        key_hour: u32,
        record_tenant_hex: String,
        record_signal: i32,
        record_shard: u32,
        record_hour: u32,
    },
    #[error(
        "format_version 2 compaction record's input_set_hash is not the version 2 hash over its own inputs and superseded_record_key"
    )]
    SupersedingInputSetHashMismatch,
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Inputs to [`build`]. Every timestamp is caller-supplied: this crate never
/// reads the system clock (ADR-0010 §1); the pinned flush identity is the
/// writer's responsibility.
#[derive(Debug, Clone)]
pub struct NewCommitRecord {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub writer_id: Uuid,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub object_size: u64,
    pub content_hash: [u8; 32],
    pub sample_count: u64,
    pub series_count: u64,
    pub min_event_ts_ns: i64,
    pub max_event_ts_ns: i64,
    pub min_ingest_ts_ns: i64,
    pub max_ingest_ts_ns: i64,
    pub segment_format_version: u32,
    pub created_unix_ns: i64,
    pub ingest_hour_bucket: u32,
}

/// Build a `CommitRecord`, computing `object_key` from the identity fields
/// (never taken from caller input, ADR-0010 §7) and validating the result
/// before returning it.
pub fn build(input: NewCommitRecord) -> Result<CommitRecord, RecordError> {
    let object_key = keys::data_key(
        &input.tenant_hash,
        input.signal,
        input.shard,
        input.writer_id,
        input.writer_epoch,
        input.writer_seq,
        &input.content_hash,
    )?;
    let record = CommitRecord {
        format_version: FORMAT_VERSION,
        tenant_hash: input.tenant_hash.0.to_vec(),
        signal: signal::to_proto(input.signal) as i32,
        shard: input.shard,
        writer_id: input.writer_id.to_string(),
        writer_epoch: input.writer_epoch,
        writer_seq: input.writer_seq,
        object_key,
        object_size: input.object_size,
        content_hash: input.content_hash.to_vec(),
        sample_count: input.sample_count,
        series_count: input.series_count,
        min_event_ts_ns: input.min_event_ts_ns,
        max_event_ts_ns: input.max_event_ts_ns,
        min_ingest_ts_ns: input.min_ingest_ts_ns,
        max_ingest_ts_ns: input.max_ingest_ts_ns,
        segment_format_version: input.segment_format_version,
        created_unix_ns: input.created_unix_ns,
        ingest_hour_bucket: input.ingest_hour_bucket,
        // Stamped by the writer that encoded the object, through
        // [`crate::declared_stats::stamp_commit_record`] (ADR-0873 decision
        // 3), never derived here. Empty is a permanently legal state.
        declared_column_stats: Vec::new(),
    };
    validate(&record)?;
    Ok(record)
}

/// Structural invariants every `CommitRecord` must satisfy, regardless of
/// how it was constructed. Deliberately does NOT check `object_key`: that is
/// a distinct, fatal-on-mismatch check performed by readers against the
/// record's own identity fields (see [`crate::keys::verify_object_key`]),
/// not a self-contained property of the record.
///
/// The `ingest_hour_bucket`/`created_unix_ns` cross-check treats an all-zero
/// value on either field as "not meaningfully set" and skips the check
/// (proto3 scalars have no true presence bit). When both carry a real
/// value, a flush cannot have opened strictly *after* the hour it was
/// ultimately recorded as created; there is deliberately no upper bound, so
/// a flush that spans into a later hour (bounded elsewhere by
/// `max_flush_lifetime`) still validates.
pub fn validate(record: &CommitRecord) -> Result<(), RecordError> {
    // First: a newer version may change any field's shape, so a record it
    // wrote must fail as a newer version, not as a corrupt field.
    if record.format_version != FORMAT_VERSION {
        return Err(RecordError::UnsupportedFormatVersion {
            expected: FORMAT_VERSION,
            actual: record.format_version,
        });
    }
    if record.tenant_hash.len() != 16 {
        return Err(RecordError::InvalidTenantHashLen(record.tenant_hash.len()));
    }
    if record.content_hash.len() != 32 {
        return Err(RecordError::InvalidContentHashLen(
            record.content_hash.len(),
        ));
    }
    if record.min_event_ts_ns > record.max_event_ts_ns {
        return Err(RecordError::EventTsOutOfOrder {
            min: record.min_event_ts_ns,
            max: record.max_event_ts_ns,
        });
    }
    if record.min_ingest_ts_ns > record.max_ingest_ts_ns {
        return Err(RecordError::IngestTsOutOfOrder {
            min: record.min_ingest_ts_ns,
            max: record.max_ingest_ts_ns,
        });
    }
    if record.created_unix_ns != 0 && record.ingest_hour_bucket != 0 {
        let created_hour_bucket = record.created_unix_ns.div_euclid(NS_PER_HOUR);
        if i64::from(record.ingest_hour_bucket) > created_hour_bucket {
            return Err(RecordError::IngestHourInconsistent {
                ingest_hour_bucket: record.ingest_hour_bucket,
                created_hour_bucket,
            });
        }
    }
    Ok(())
}

/// Serialize a `CommitRecord`. Infallible: protobuf-encoding a well-formed
/// message cannot fail.
pub fn encode(record: &CommitRecord) -> bytes::Bytes {
    record.encode_to_vec().into()
}

/// Deserialize and validate a `CommitRecord`.
pub fn decode(bytes: &[u8]) -> Result<CommitRecord, RecordError> {
    let record = CommitRecord::decode(bytes)?;
    validate(&record)?;
    Ok(record)
}

/// Derive the v2 commit token for a record (ADR-0010 §2): the token that
/// fully determines this record's own commit key.
pub fn token_for(record: &CommitRecord) -> Result<CommitToken, RecordError> {
    let writer_id = Uuid::parse_str(&record.writer_id)
        .map_err(|_| RecordError::InvalidWriterId(record.writer_id.clone()))?;
    Ok(CommitToken {
        shard: record.shard,
        writer_id,
        epoch: record.writer_epoch,
        seq: record.writer_seq,
        ingest_hour_bucket: record.ingest_hour_bucket,
    })
}

/// Structural invariants every `CompactionRecord` must satisfy on read. The
/// primary gate is the supported-set `format_version` check (ADR-0066
/// decision 2): a record stamped with an unknown version is refused, never
/// read as version 1. Mirrors [`validate`] for `CommitRecord`; like it, the
/// distinct `object_key`/identity reconstruction is a separate reader check
/// (see [`crate::keys::verify_compaction_record_key`]), not repeated here.
///
/// `superseded_record_key` is set exactly when the record is version 2. A
/// version 2 record's key must be the canonical rendering of a compaction
/// record key (never a rewrite record key) in this record's own bucket, and
/// its `input_set_hash` must be
/// [`crate::erasure::compute_superseding_compaction_input_set_hash`] over its
/// own inputs and that key. No selector honours the key yet; once one does it
/// will exclude whatever a present version 2 record names (ADR-0066 force 2
/// amendment, item 3), so an unchecked value would then drop a live record.
pub fn validate_compaction(record: &CompactionRecord) -> Result<(), RecordError> {
    check_format_version(
        RecordKind::Compaction,
        record.format_version,
        COMPACTION_FORMAT_VERSION,
        COMPACTION_SUPERSEDING_FORMAT_VERSION,
    )?;
    if record.tenant_hash.len() != 16 {
        return Err(RecordError::InvalidTenantHashLen(record.tenant_hash.len()));
    }
    if record.format_version == COMPACTION_SUPERSEDING_FORMAT_VERSION {
        validate_superseding_compaction(record)
    } else if !record.superseded_record_key.is_empty() {
        Err(RecordError::SupersededRecordKeyOnVersionOne(
            record.superseded_record_key.clone(),
        ))
    } else {
        Ok(())
    }
}

fn validate_superseding_compaction(record: &CompactionRecord) -> Result<(), RecordError> {
    let key = record.superseded_record_key.as_str();
    if key.is_empty() {
        return Err(RecordError::MissingSupersededRecordKey);
    }
    let parsed = keys::parse_compaction_record_key(key)
        .map_err(|_| RecordError::InvalidSupersededRecordKey(key.to_string()))?;
    // The parser accepts either hex case; only the builder's own rendering
    // string-equals the key a string-based exclusion will compare against.
    let canonical = keys::compaction_record_key(
        &parsed.tenant_hash,
        parsed.signal,
        parsed.shard,
        parsed.ingest_hour_bucket,
        &parsed.input_set_hash16.to_ascii_lowercase(),
    )
    .map_err(|_| RecordError::InvalidSupersededRecordKey(key.to_string()))?;
    if canonical != key {
        return Err(RecordError::NonCanonicalSupersededRecordKey(
            key.to_string(),
        ));
    }
    let key_signal = signal::to_proto(parsed.signal) as i32;
    if parsed.tenant_hash.0.as_slice() != record.tenant_hash.as_slice()
        || key_signal != record.signal
        || parsed.shard != record.shard
        || parsed.ingest_hour_bucket != record.ingest_hour_bucket
    {
        return Err(RecordError::SupersededRecordKeyBucketMismatch {
            key_tenant_hex: hex::encode(parsed.tenant_hash.0),
            key_signal,
            key_shard: parsed.shard,
            key_hour: parsed.ingest_hour_bucket,
            record_tenant_hex: hex::encode(&record.tenant_hash),
            record_signal: record.signal,
            record_shard: record.shard,
            record_hour: record.ingest_hour_bucket,
        });
    }
    let expected = erasure::compute_superseding_compaction_input_set_hash(&record.inputs, key);
    if record.input_set_hash.as_slice() != expected.as_slice() {
        return Err(RecordError::SupersedingInputSetHashMismatch);
    }
    Ok(())
}

/// Serialize a `CompactionRecord`. Infallible, like [`encode`].
pub fn encode_compaction(record: &CompactionRecord) -> bytes::Bytes {
    record.encode_to_vec().into()
}

/// Deserialize and validate a `CompactionRecord`. The only decode path
/// production code may use: the raw prost `Message::decode` skips the
/// `format_version` gate and reads a future record as version 1.
pub fn decode_compaction(bytes: &[u8]) -> Result<CompactionRecord, RecordError> {
    let record = CompactionRecord::decode(bytes)?;
    validate_compaction(&record)?;
    Ok(record)
}

/// Structural invariants every `RetentionTombstone` must satisfy on read.
/// Same supported-set `format_version` gate and rationale as
/// [`validate_compaction`].
pub fn validate_tombstone(record: &RetentionTombstone) -> Result<(), RecordError> {
    check_format_version(
        RecordKind::RetentionTombstone,
        record.format_version,
        TOMBSTONE_FORMAT_VERSION,
        TOMBSTONE_FORMAT_VERSION,
    )?;
    if record.tenant_hash.len() != 16 {
        return Err(RecordError::InvalidTenantHashLen(record.tenant_hash.len()));
    }
    Ok(())
}

/// Serialize a `RetentionTombstone`. Infallible, like [`encode`].
pub fn encode_tombstone(record: &RetentionTombstone) -> bytes::Bytes {
    record.encode_to_vec().into()
}

/// Deserialize and validate a `RetentionTombstone`. The only decode path
/// production code may use, for the same reason as [`decode_compaction`].
pub fn decode_tombstone(bytes: &[u8]) -> Result<RetentionTombstone, RecordError> {
    let record = RetentionTombstone::decode(bytes)?;
    validate_tombstone(&record)?;
    Ok(record)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn base_input() -> NewCommitRecord {
        NewCommitRecord {
            tenant_hash: TenantHash([0x11; 16]),
            signal: Signal::Metrics,
            shard: 1,
            writer_id: Uuid::new_v4(),
            writer_epoch: 100,
            writer_seq: 1,
            object_size: 1024,
            content_hash: [0x22; 32],
            sample_count: 10,
            series_count: 2,
            min_event_ts_ns: 1_000,
            max_event_ts_ns: 2_000,
            min_ingest_ts_ns: 1_500,
            max_ingest_ts_ns: 2_500,
            segment_format_version: 1,
            created_unix_ns: 495_734 * NS_PER_HOUR + 30 * 60_000_000_000,
            ingest_hour_bucket: 495_734,
        }
    }

    #[test]
    fn build_computes_object_key_and_validates() {
        let record = build(base_input()).expect("valid record");
        let expected_key = keys::reconstruct_data_key(&record).expect("reconstruct");
        assert_eq!(record.object_key, expected_key);
        assert_eq!(record.format_version, FORMAT_VERSION);
    }

    #[test]
    fn encode_decode_round_trips() {
        let record = build(base_input()).expect("valid record");
        let bytes = encode(&record);
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn token_for_round_trips_identity_fields() {
        let record = build(base_input()).expect("valid record");
        let token = token_for(&record).expect("token");
        assert_eq!(token.shard, record.shard);
        assert_eq!(token.writer_id.to_string(), record.writer_id);
        assert_eq!(token.epoch, record.writer_epoch);
        assert_eq!(token.seq, record.writer_seq);
        assert_eq!(token.ingest_hour_bucket, record.ingest_hour_bucket);
    }

    #[test]
    fn validate_rejects_bad_tenant_hash_len() {
        let mut record = build(base_input()).expect("valid record");
        record.tenant_hash = vec![0; 15];
        assert_eq!(
            validate(&record),
            Err(RecordError::InvalidTenantHashLen(15))
        );
    }

    #[test]
    fn validate_rejects_bad_content_hash_len() {
        let mut record = build(base_input()).expect("valid record");
        record.content_hash = vec![0; 31];
        assert_eq!(
            validate(&record),
            Err(RecordError::InvalidContentHashLen(31))
        );
    }

    #[test]
    fn validate_rejects_wrong_format_version() {
        let mut record = build(base_input()).expect("valid record");
        record.format_version = 2;
        assert_eq!(
            validate(&record),
            Err(RecordError::UnsupportedFormatVersion {
                expected: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn validate_checks_format_version_before_hash_lengths() {
        let mut record = build(base_input()).expect("valid record");
        record.format_version = 2;
        record.tenant_hash = vec![0; 15];
        record.content_hash = vec![0; 31];
        assert_eq!(
            validate(&record),
            Err(RecordError::UnsupportedFormatVersion {
                expected: 1,
                actual: 2
            })
        );
        let bytes = record.encode_to_vec();
        assert_eq!(
            decode(&bytes),
            Err(RecordError::UnsupportedFormatVersion {
                expected: 1,
                actual: 2
            })
        );

        record.tenant_hash = vec![0; 16];
        assert_eq!(
            validate(&record),
            Err(RecordError::UnsupportedFormatVersion {
                expected: 1,
                actual: 2
            })
        );

        // A below-floor version (proto3's default 0) is refused first too.
        record.format_version = 0;
        record.tenant_hash = vec![0; 15];
        assert_eq!(
            validate(&record),
            Err(RecordError::UnsupportedFormatVersion {
                expected: 1,
                actual: 0
            })
        );
    }

    #[test]
    fn validate_rejects_event_ts_out_of_order() {
        let mut record = build(base_input()).expect("valid record");
        record.min_event_ts_ns = 5_000;
        record.max_event_ts_ns = 1_000;
        assert_eq!(
            validate(&record),
            Err(RecordError::EventTsOutOfOrder {
                min: 5_000,
                max: 1_000
            })
        );
    }

    #[test]
    fn validate_rejects_ingest_ts_out_of_order() {
        let mut record = build(base_input()).expect("valid record");
        record.min_ingest_ts_ns = 5_000;
        record.max_ingest_ts_ns = 1_000;
        assert_eq!(
            validate(&record),
            Err(RecordError::IngestTsOutOfOrder {
                min: 5_000,
                max: 1_000
            })
        );
    }

    #[test]
    fn validate_allows_hour_bucket_at_or_before_created_hour() {
        let mut input = base_input();
        // Bucket several hours before creation: a long-running flush.
        input.ingest_hour_bucket = 495_730;
        let record = build(input).expect("valid: bucket before creation is fine");
        assert!(validate(&record).is_ok());
    }

    #[test]
    fn validate_rejects_hour_bucket_after_created_hour() {
        let mut input = base_input();
        // Bucket after the hour it was created in: impossible, flush opens
        // before it is committed.
        input.ingest_hour_bucket = 495_740;
        let err = build(input).expect_err("bucket after creation is inconsistent");
        assert!(matches!(err, RecordError::IngestHourInconsistent { .. }));
    }

    #[test]
    fn validate_skips_hour_check_when_created_unix_ns_is_zero() {
        let mut input = base_input();
        input.created_unix_ns = 0;
        input.ingest_hour_bucket = u32::MAX;
        let record = build(input).expect("zero created_unix_ns skips the cross-check");
        assert!(validate(&record).is_ok());
    }

    fn sample_compaction() -> CompactionRecord {
        CompactionRecord {
            format_version: COMPACTION_FORMAT_VERSION,
            tenant_hash: vec![0x33; 16],
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard: 1,
            ingest_hour_bucket: 495_734,
            level: 1,
            inputs: Vec::new(),
            input_set_hash: vec![0x44; 32],
            parts: Vec::new(),
            created_unix_ns: 495_734 * NS_PER_HOUR,
            superseded_record_key: String::new(),
        }
    }

    fn sample_tombstone() -> RetentionTombstone {
        RetentionTombstone {
            format_version: TOMBSTONE_FORMAT_VERSION,
            tenant_hash: vec![0x55; 16],
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard: 1,
            ingest_hour_bucket: 495_734,
            retired_at_ns: 495_734 * NS_PER_HOUR,
            retention_window_ns: 720 * NS_PER_HOUR as u64,
            record_count_observed: 3,
        }
    }

    #[test]
    fn compaction_version_one_accepted_and_round_trips() {
        let record = sample_compaction();
        assert!(validate_compaction(&record).is_ok());
        let bytes = encode_compaction(&record);
        let decoded = decode_compaction(&bytes).expect("decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn compaction_version_three_refused_with_typed_error() {
        let mut record = superseding_compaction();
        record.format_version = 3;
        let bytes = record.encode_to_vec();
        assert_eq!(
            decode_compaction(&bytes),
            Err(RecordError::UnsupportedRecordFormatVersion {
                kind: RecordKind::Compaction,
                min: 1,
                max: 2,
                actual: 3,
            })
        );
    }

    #[test]
    fn compaction_version_zero_refused_with_typed_error() {
        let mut record = sample_compaction();
        record.format_version = 0;
        let bytes = record.encode_to_vec();
        assert_eq!(
            decode_compaction(&bytes),
            Err(RecordError::UnsupportedRecordFormatVersion {
                kind: RecordKind::Compaction,
                min: 1,
                max: 2,
                actual: 0,
            })
        );
    }

    #[test]
    fn compaction_checks_format_version_before_tenant_hash_len() {
        let mut record = sample_compaction();
        record.format_version = 3;
        record.tenant_hash = vec![0; 15];
        assert_eq!(
            validate_compaction(&record),
            Err(RecordError::UnsupportedRecordFormatVersion {
                kind: RecordKind::Compaction,
                min: 1,
                max: 2,
                actual: 3,
            })
        );
    }

    /// A version 1 compaction record with every field set to a non-default
    /// value, so each field's tag and value appear in the encoding.
    fn full_v1_compaction() -> CompactionRecord {
        use ravel_proto::commit::v1::{
            CompactionInputIdentity, CompactionPart, DeclaredColumnMinMax, DeclaredColumnStatValue,
            declared_column_stat_value,
        };
        CompactionRecord {
            format_version: COMPACTION_FORMAT_VERSION,
            tenant_hash: (0u8..16).collect(),
            signal: signal::to_proto(Signal::Logs) as i32,
            shard: 7,
            ingest_hour_bucket: 495_734,
            level: 1,
            inputs: vec![
                CompactionInputIdentity {
                    writer_id: "00000000-0000-4000-8000-000000000001".to_string(),
                    writer_epoch: 3,
                    writer_seq: 9,
                },
                CompactionInputIdentity {
                    writer_id: "00000000-0000-4000-8000-000000000002".to_string(),
                    writer_epoch: 4,
                    writer_seq: 1,
                },
            ],
            input_set_hash: vec![0xa5; 32],
            parts: vec![CompactionPart {
                part_index: 2,
                first_series_id: vec![0x01; 16],
                last_series_id: vec![0xfe; 16],
                content_hash: vec![0x5a; 32],
                object_size: 4096,
                sample_count: 100,
                series_count: 5,
                run_count: 6,
                min_event_ts_ns: -1_000,
                max_event_ts_ns: 2_000,
                segment_format_version: 3,
                declared_column_stats: vec![DeclaredColumnMinMax {
                    name: "status".to_string(),
                    declared_type: 1,
                    min: Some(DeclaredColumnStatValue {
                        kind: Some(declared_column_stat_value::Kind::I64(-5)),
                    }),
                    max: Some(DeclaredColumnStatValue {
                        kind: Some(declared_column_stat_value::Kind::I64(500)),
                    }),
                    null_count: 2,
                }],
            }],
            created_unix_ns: 495_734 * NS_PER_HOUR + 17,
            superseded_record_key: String::new(),
        }
    }

    /// The encoding of [`full_v1_compaction`], produced by the tree before
    /// `superseded_record_key` existed (the parent of the commit adding it).
    const FULL_V1_COMPACTION_HEX: &str = concat!(
        "08011210000102030405060708090a0b0c0d0e0f1802200728f6a01e30013a2a0a24",
        "30303030303030302d303030302d343030302d383030302d30303030303030303030",
        "3031100318093a2a0a2430303030303030302d303030302d343030302d383030302d",
        "303030303030303030303032100418014220a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5",
        "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a54a7c08021210010101010101010101010101",
        "010101011a10fefefefefefefefefefefefefefefefe22205a5a5a5a5a5a5a5a5a5a",
        "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a2880203064380540064918fc",
        "ffffffffffff51d007000000000000580362150a0673746174757310011a02080922",
        "0308e80728025111c019afce52c418",
    );

    #[test]
    fn compaction_v1_encoding_is_byte_identical_to_the_pre_version_two_format() {
        let record = full_v1_compaction();
        let bytes = encode_compaction(&record);
        assert_eq!(hex::encode(&bytes), FULL_V1_COMPACTION_HEX);
        assert_eq!(decode_compaction(&bytes).expect("decode v1"), record);
    }

    /// A version 2 record re-encoding [`full_v1_compaction`]: same bucket and
    /// inputs, the predecessor's key in `superseded_record_key`, and the
    /// version 2 hash over both.
    fn superseding_compaction() -> CompactionRecord {
        let predecessor = full_v1_compaction();
        let key = keys::compaction_record_key_for(&predecessor).expect("predecessor key");
        reseal(CompactionRecord {
            format_version: COMPACTION_SUPERSEDING_FORMAT_VERSION,
            superseded_record_key: key,
            created_unix_ns: predecessor.created_unix_ns + 1,
            ..predecessor
        })
    }

    /// Recompute the version 2 hash after a field change, so a test isolates
    /// the one check it names instead of tripping the hash check.
    fn reseal(mut record: CompactionRecord) -> CompactionRecord {
        record.input_set_hash = erasure::compute_superseding_compaction_input_set_hash(
            &record.inputs,
            &record.superseded_record_key,
        )
        .to_vec();
        record
    }

    /// The key of a compaction record in `record`'s bucket with one
    /// component swapped by `alter`.
    fn compaction_key_in(
        record: &CompactionRecord,
        alter: impl FnOnce(&mut keys::ParsedCompactionRecordKey),
    ) -> String {
        let mut parsed =
            keys::parse_compaction_record_key(&record.superseded_record_key).expect("parse");
        alter(&mut parsed);
        keys::compaction_record_key(
            &parsed.tenant_hash,
            parsed.signal,
            parsed.shard,
            parsed.ingest_hour_bucket,
            &parsed.input_set_hash16,
        )
        .expect("key")
    }

    fn decode_superseding_with_key(key: String) -> Result<CompactionRecord, RecordError> {
        let record = reseal(CompactionRecord {
            superseded_record_key: key,
            ..superseding_compaction()
        });
        decode_compaction(&encode_compaction(&record))
    }

    #[test]
    fn compaction_v2_round_trips_and_keys_away_from_its_predecessor() {
        let record = superseding_compaction();
        let bytes = encode_compaction(&record);
        let decoded = decode_compaction(&bytes).expect("decode v2");
        assert_eq!(decoded, record);
        let key = keys::compaction_record_key_for(&decoded).expect("v2 key");
        assert_ne!(key, decoded.superseded_record_key);
        let parsed = keys::parse_compaction_record_key(&key).expect("v2 key parses");
        assert_eq!(
            parsed.input_set_hash16,
            hex::encode(&record.input_set_hash[..8])
        );
    }

    #[test]
    fn compaction_v2_with_empty_superseded_record_key_rejected() {
        let record = reseal(CompactionRecord {
            superseded_record_key: String::new(),
            ..superseding_compaction()
        });
        assert_eq!(
            validate_compaction(&record),
            Err(RecordError::MissingSupersededRecordKey)
        );
    }

    /// proto3 does not encode an empty string, so a version 2 record with the
    /// field absent on the wire is the same bytes as one with it empty; this
    /// builds it from the version 1 bytes with only the version byte changed.
    #[test]
    fn compaction_v2_without_the_field_rejected() {
        let mut bytes = encode_compaction(&full_v1_compaction()).to_vec();
        // Field 1 (format_version), varint: tag 0x08, then the value.
        assert_eq!(bytes[..2], [0x08, 0x01]);
        bytes[1] = 0x02;
        assert_eq!(
            decode_compaction(&bytes),
            Err(RecordError::MissingSupersededRecordKey)
        );
    }

    #[test]
    fn compaction_v2_naming_a_rewrite_record_key_rejected() {
        let record = superseding_compaction();
        let parsed =
            keys::parse_compaction_record_key(&record.superseded_record_key).expect("parse");
        let rewrite_key = keys::rewrite_record_key(
            &parsed.tenant_hash,
            parsed.signal,
            parsed.shard,
            parsed.ingest_hour_bucket,
            &parsed.input_set_hash16,
        )
        .expect("rewrite key");
        assert_eq!(
            decode_superseding_with_key(rewrite_key.clone()),
            Err(RecordError::InvalidSupersededRecordKey(rewrite_key))
        );
    }

    #[test]
    fn compaction_v2_naming_an_unparseable_key_rejected() {
        assert_eq!(
            decode_superseding_with_key("not-a-key".to_string()),
            Err(RecordError::InvalidSupersededRecordKey(
                "not-a-key".to_string()
            ))
        );
    }

    fn assert_bucket_mismatch(result: Result<CompactionRecord, RecordError>) {
        assert!(
            matches!(
                result,
                Err(RecordError::SupersededRecordKeyBucketMismatch { .. })
            ),
            "expected a bucket mismatch, got {result:?}"
        );
    }

    #[test]
    fn compaction_v2_naming_a_different_tenant_rejected() {
        let record = superseding_compaction();
        let key = compaction_key_in(&record, |p| p.tenant_hash = TenantHash([0xee; 16]));
        assert_bucket_mismatch(decode_superseding_with_key(key));
    }

    #[test]
    fn compaction_v2_naming_a_different_signal_rejected() {
        let record = superseding_compaction();
        let key = compaction_key_in(&record, |p| p.signal = Signal::Metrics);
        assert_bucket_mismatch(decode_superseding_with_key(key));
    }

    #[test]
    fn compaction_v2_naming_a_different_shard_rejected() {
        let record = superseding_compaction();
        let key = compaction_key_in(&record, |p| p.shard += 1);
        assert_bucket_mismatch(decode_superseding_with_key(key));
    }

    #[test]
    fn compaction_v2_naming_a_different_hour_rejected() {
        let record = superseding_compaction();
        let key = compaction_key_in(&record, |p| p.ingest_hour_bucket -= 1);
        assert_bucket_mismatch(decode_superseding_with_key(key));
    }

    /// `superseding_compaction`'s key with the path segment at `segment`
    /// uppercased; the tenant (segment 1) and the filename (segment 6) are the
    /// ones whose hex the parser accepts in either case.
    fn superseding_key_with_uppercased_segment(segment: usize) -> String {
        let key = superseding_compaction().superseded_record_key;
        let mut parts: Vec<String> = key.split('/').map(str::to_string).collect();
        parts[segment] = if segment == 6 {
            let file: Vec<&str> = parts[6].split('.').collect();
            format!("{}.{}.{}", file[0], file[1].to_ascii_uppercase(), file[2])
        } else {
            parts[segment].to_ascii_uppercase()
        };
        let altered = parts.join("/");
        assert_ne!(altered, key, "the fixture must contain lowercase hex");
        assert!(keys::parse_compaction_record_key(&altered).is_ok());
        altered
    }

    #[test]
    fn compaction_v2_naming_an_uppercase_tenant_key_rejected() {
        let key = superseding_key_with_uppercased_segment(1);
        assert_eq!(
            decode_superseding_with_key(key.clone()),
            Err(RecordError::NonCanonicalSupersededRecordKey(key))
        );
    }

    #[test]
    fn compaction_v2_naming_an_uppercase_hash16_key_rejected() {
        let key = superseding_key_with_uppercased_segment(6);
        assert_eq!(
            decode_superseding_with_key(key.clone()),
            Err(RecordError::NonCanonicalSupersededRecordKey(key))
        );
    }

    #[test]
    fn compaction_v2_with_tampered_input_set_hash_rejected() {
        let mut record = superseding_compaction();
        record.input_set_hash[0] ^= 0x01;
        assert_eq!(
            decode_compaction(&encode_compaction(&record)),
            Err(RecordError::SupersedingInputSetHashMismatch)
        );
    }

    #[test]
    fn compaction_v2_carrying_the_version_one_hash_rejected() {
        let mut record = superseding_compaction();
        record.input_set_hash = erasure::compute_compaction_input_set_hash(&record.inputs).to_vec();
        assert_eq!(
            validate_compaction(&record),
            Err(RecordError::SupersedingInputSetHashMismatch)
        );
    }

    #[test]
    fn compaction_v1_carrying_superseded_record_key_rejected() {
        let v2 = superseding_compaction();
        let record = CompactionRecord {
            superseded_record_key: v2.superseded_record_key.clone(),
            ..full_v1_compaction()
        };
        assert_eq!(
            decode_compaction(&encode_compaction(&record)),
            Err(RecordError::SupersededRecordKeyOnVersionOne(
                v2.superseded_record_key
            ))
        );
    }

    mod compaction_codec_props {
        use super::*;
        use proptest::prelude::*;
        use ravel_proto::commit::v1::CompactionInputIdentity;

        const VALID_SIGNALS: [Signal; 6] = [
            Signal::Metrics,
            Signal::Logs,
            Signal::Spans,
            Signal::Profiles,
            Signal::Alerts,
            Signal::Audit,
        ];

        prop_compose! {
            fn v1_strategy()(
                tenant in any::<[u8; 16]>(),
                signal in prop::sample::select(VALID_SIGNALS.to_vec()),
                shard in 0u32..10_000,
                hour in 0u32..2_000_000,
                inputs in prop::collection::vec(
                    (any::<u128>(), any::<u64>(), any::<u64>()),
                    0..5,
                ),
                input_set_hash in any::<[u8; 32]>(),
                created_unix_ns in any::<i64>(),
            ) -> CompactionRecord {
                let mut inputs: Vec<CompactionInputIdentity> = inputs
                    .into_iter()
                    .map(|(id, epoch, seq)| CompactionInputIdentity {
                        writer_id: Uuid::from_u128(id).to_string(),
                        writer_epoch: epoch,
                        writer_seq: seq,
                    })
                    .collect();
                inputs.sort_by(|a, b| {
                    (a.writer_id.as_str(), a.writer_epoch, a.writer_seq)
                        .cmp(&(b.writer_id.as_str(), b.writer_epoch, b.writer_seq))
                });
                CompactionRecord {
                    format_version: COMPACTION_FORMAT_VERSION,
                    tenant_hash: tenant.to_vec(),
                    signal: signal::to_proto(signal) as i32,
                    shard,
                    ingest_hour_bucket: hour,
                    level: 1,
                    inputs,
                    input_set_hash: input_set_hash.to_vec(),
                    parts: Vec::new(),
                    created_unix_ns,
                    superseded_record_key: String::new(),
                }
            }
        }

        fn v2_from(predecessor: CompactionRecord) -> CompactionRecord {
            let key = keys::compaction_record_key_for(&predecessor).expect("predecessor key");
            reseal(CompactionRecord {
                format_version: COMPACTION_SUPERSEDING_FORMAT_VERSION,
                superseded_record_key: key,
                ..predecessor
            })
        }

        proptest! {
            #[test]
            fn v1_round_trips(record in v1_strategy()) {
                let decoded = decode_compaction(&encode_compaction(&record)).expect("decode v1");
                prop_assert_eq!(decoded, record);
            }

            #[test]
            fn v2_round_trips(record in v1_strategy().prop_map(v2_from)) {
                let decoded = decode_compaction(&encode_compaction(&record)).expect("decode v2");
                prop_assert_eq!(decoded, record);
            }

            #[test]
            fn corrupt_bytes_never_panic(
                record in prop_oneof![v1_strategy(), v1_strategy().prop_map(v2_from)],
                flips in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..6),
                cut in any::<prop::sample::Index>(),
            ) {
                let mut bytes = encode_compaction(&record).to_vec();
                for (at, value) in flips {
                    let i = at.index(bytes.len());
                    bytes[i] = value;
                }
                let len = cut.index(bytes.len() + 1);
                let _ = decode_compaction(&bytes[..len]);
            }

            #[test]
            fn arbitrary_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..256)) {
                let _ = decode_compaction(&raw);
            }
        }
    }

    #[test]
    fn tombstone_version_one_accepted_and_round_trips() {
        let record = sample_tombstone();
        assert!(validate_tombstone(&record).is_ok());
        let bytes = encode_tombstone(&record);
        let decoded = decode_tombstone(&bytes).expect("decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn tombstone_version_two_refused_with_typed_error() {
        let mut record = sample_tombstone();
        record.format_version = 2;
        let bytes = record.encode_to_vec();
        assert_eq!(
            decode_tombstone(&bytes),
            Err(RecordError::UnsupportedRecordFormatVersion {
                kind: RecordKind::RetentionTombstone,
                min: 1,
                max: 1,
                actual: 2,
            })
        );
    }

    #[test]
    fn tombstone_version_zero_refused_with_typed_error() {
        let mut record = sample_tombstone();
        record.format_version = 0;
        let bytes = record.encode_to_vec();
        assert_eq!(
            decode_tombstone(&bytes),
            Err(RecordError::UnsupportedRecordFormatVersion {
                kind: RecordKind::RetentionTombstone,
                min: 1,
                max: 1,
                actual: 0,
            })
        );
    }

    #[test]
    fn tombstone_checks_format_version_before_tenant_hash_len() {
        let mut record = sample_tombstone();
        record.format_version = 2;
        record.tenant_hash = vec![0; 15];
        assert_eq!(
            validate_tombstone(&record),
            Err(RecordError::UnsupportedRecordFormatVersion {
                kind: RecordKind::RetentionTombstone,
                min: 1,
                max: 1,
                actual: 2,
            })
        );
    }

    /// Enumeration guard: every `RecordKind` must have a decode-and-validate
    /// pair that refuses an out-of-range `format_version`. The exhaustive
    /// match makes a fourth kind fail to compile here until it is wired to a
    /// validate pair, so no versioned record can ship ungated (ADR-0066).
    #[test]
    fn every_record_kind_has_a_versioned_validate_pair() {
        for kind in [
            RecordKind::Commit,
            RecordKind::Compaction,
            RecordKind::RetentionTombstone,
        ] {
            match kind {
                RecordKind::Commit => {
                    let mut record = build(base_input()).expect("valid");
                    record.format_version = 2;
                    assert!(matches!(
                        validate(&record),
                        Err(RecordError::UnsupportedFormatVersion { .. })
                    ));
                }
                RecordKind::Compaction => {
                    let mut record = superseding_compaction();
                    record.format_version = 3;
                    assert!(matches!(
                        validate_compaction(&record),
                        Err(RecordError::UnsupportedRecordFormatVersion {
                            kind: RecordKind::Compaction,
                            ..
                        })
                    ));
                }
                RecordKind::RetentionTombstone => {
                    let mut record = sample_tombstone();
                    record.format_version = 2;
                    assert!(matches!(
                        validate_tombstone(&record),
                        Err(RecordError::UnsupportedRecordFormatVersion {
                            kind: RecordKind::RetentionTombstone,
                            ..
                        })
                    ));
                }
            }
        }
    }
}

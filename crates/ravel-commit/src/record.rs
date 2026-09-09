//! `CommitRecord` construction, encoding/decoding, and validation
//! (docs/catalog-and-mvcc.md, ADR-0010 §1).

use prost::Message;
use ravel_proto::commit::v1::{CommitRecord, CompactionRecord, RetentionTombstone};
use ravel_types::{CommitToken, Signal, TenantHash};
use uuid::Uuid;

use crate::keys::{self, KeyError};
use crate::signal;

/// The only supported `CommitRecord.format_version`.
pub const FORMAT_VERSION: u32 = 1;
/// The only supported `CompactionRecord.format_version` (ADR-0066 decision 2).
pub const COMPACTION_FORMAT_VERSION: u32 = 1;
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
    if record.tenant_hash.len() != 16 {
        return Err(RecordError::InvalidTenantHashLen(record.tenant_hash.len()));
    }
    if record.content_hash.len() != 32 {
        return Err(RecordError::InvalidContentHashLen(
            record.content_hash.len(),
        ));
    }
    if record.format_version != FORMAT_VERSION {
        return Err(RecordError::UnsupportedFormatVersion {
            expected: FORMAT_VERSION,
            actual: record.format_version,
        });
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
pub fn validate_compaction(record: &CompactionRecord) -> Result<(), RecordError> {
    check_format_version(
        RecordKind::Compaction,
        record.format_version,
        COMPACTION_FORMAT_VERSION,
        COMPACTION_FORMAT_VERSION,
    )?;
    if record.tenant_hash.len() != 16 {
        return Err(RecordError::InvalidTenantHashLen(record.tenant_hash.len()));
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
    fn compaction_version_two_refused_with_typed_error() {
        let mut record = sample_compaction();
        record.format_version = 2;
        let bytes = record.encode_to_vec();
        assert_eq!(
            decode_compaction(&bytes),
            Err(RecordError::UnsupportedRecordFormatVersion {
                kind: RecordKind::Compaction,
                min: 1,
                max: 1,
                actual: 2,
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
                max: 1,
                actual: 0,
            })
        );
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
                    let mut record = sample_compaction();
                    record.format_version = 2;
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

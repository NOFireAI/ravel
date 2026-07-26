//! Object key layout for data and commit-record objects
//! (docs/catalog-and-mvcc.md, ADR-0002, ADR-0010 §1/§2/§7).
//!
//! Builders are pure string formatting: no I/O, no system clock reads. Hour
//! buckets are always supplied by the caller (pinned at flush open) and
//! turned into calendar text with a table-free Gregorian conversion
//! (Howard Hinnant's `civil_from_days`/`days_from_civil`), so this module
//! never depends on a clock or a timezone database.

use ravel_types::{CommitToken, Signal, TenantHash};
use uuid::Uuid;

use crate::signal;

/// Data object suffix (RSEG v1).
pub const DATA_SUFFIX: &str = "rseg";
/// Commit record object suffix.
pub const COMMIT_SUFFIX: &str = "cmt";

/// Errors building or parsing a key. All are caller-input problems (bad
/// shard, malformed key text); none indicate a system fault.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    #[error("shard {0} exceeds the 4-digit key width (max 9999)")]
    ShardOutOfRange(u32),
    #[error("malformed key {key:?}: {reason}")]
    Malformed { key: String, reason: String },
    #[error("invalid tenant hash: {0}")]
    InvalidTenantHash(String),
    #[error("unknown signal prefix {0:?}")]
    UnknownSignal(String),
    #[error("invalid writer id {0:?}")]
    InvalidWriterId(String),
    #[error("invalid content hash length: expected 32 bytes, got {0}")]
    InvalidContentHashLen(usize),
    #[error("invalid ingest hour bucket text {0:?}")]
    InvalidIngestHour(String),
}

/// Fatal: the record's `object_key` field does not match the key
/// reconstructed from its own identity fields (ADR-0010 §7). Readers MUST
/// treat this as an invariant breach, never silently prefer either value.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReconstructionError {
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(
        "commit record object_key mismatch (fatal invariant breach): expected {expected:?}, record has {actual:?}"
    )]
    ObjectKeyMismatch { expected: String, actual: String },
}

fn format_shard(shard: u32) -> Result<String, KeyError> {
    if shard > 9999 {
        return Err(KeyError::ShardOutOfRange(shard));
    }
    Ok(format!("{shard:04}"))
}

fn tenant_hash_from_bytes(bytes: &[u8]) -> Result<TenantHash, KeyError> {
    let arr: [u8; 16] = bytes.try_into().map_err(|_| {
        KeyError::InvalidTenantHash(format!("expected 16 bytes, got {}", bytes.len()))
    })?;
    Ok(TenantHash(arr))
}

fn content_hash_from_bytes(bytes: &[u8]) -> Result<[u8; 32], KeyError> {
    bytes
        .try_into()
        .map_err(|_| KeyError::InvalidContentHashLen(bytes.len()))
}

/// Days since the Unix epoch (1970-01-01) for a given proleptic Gregorian
/// civil date. Howard Hinnant's `days_from_civil` algorithm (public domain).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let m = i64::from(m);
    let d = i64::from(d);
    let doy = ((153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1) as u64; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe as i64 - 719_468
}

/// Inverse of [`days_from_civil`]: civil (year, month, day) for a day count
/// since the Unix epoch.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Format an ingest-hour bucket (unix hours, pinned at flush open) as the
/// `YYYYMMDDTHH` UTC text used in commit keys.
pub fn ingest_hour_string(ingest_hour_bucket: u32) -> String {
    let total_hours = i64::from(ingest_hour_bucket);
    let days = total_hours.div_euclid(24);
    let hour = total_hours.rem_euclid(24);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}{m:02}{d:02}T{hour:02}")
}

/// Parse an ingest-hour bucket string back into unix hours. Inverse of
/// [`ingest_hour_string`]; used by key parsers, never by writers (writers
/// always carry the numeric bucket from the pinned flush identity).
pub fn parse_ingest_hour_string(s: &str) -> Result<u32, KeyError> {
    let bad = || KeyError::InvalidIngestHour(s.to_string());
    if s.len() != 11 || s.as_bytes()[8] != b'T' {
        return Err(bad());
    }
    let (date, hour) = s.split_at(8);
    let hour = &hour[1..];
    if !date.bytes().all(|b| b.is_ascii_digit()) || !hour.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let y: i64 = date[0..4].parse().map_err(|_| bad())?;
    let m: u32 = date[4..6].parse().map_err(|_| bad())?;
    let d: u32 = date[6..8].parse().map_err(|_| bad())?;
    let h: i64 = hour.parse().map_err(|_| bad())?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || !(0..=23).contains(&h) {
        return Err(bad());
    }
    let days = days_from_civil(y, m, d);
    // Round-trip guard: reject calendar dates like 2026-02-30 that parse as
    // digits but do not exist (civil_from_days would normalize them).
    if civil_from_days(days) != (y, m, d) {
        return Err(bad());
    }
    let total_hours = days * 24 + h;
    u32::try_from(total_hours).map_err(|_| bad())
}

/// Build the data-object key for a pinned flush identity (ADR-0010 §1/§7).
///
/// `t/<tenant_hash_hex>/<signal>/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg`
pub fn data_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    writer_id: Uuid,
    epoch: u64,
    seq: u64,
    content_hash: &[u8; 32],
) -> Result<String, KeyError> {
    let shard_s = format_shard(shard)?;
    let hash16 = hex::encode(&content_hash[..8]);
    Ok(format!(
        "t/{}/{}/l0/{}/{}.{}.{:020}.{}.{}",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        shard_s,
        writer_id,
        epoch,
        seq,
        hash16,
        DATA_SUFFIX
    ))
}

/// Build the commit-record key for a pinned flush identity (ADR-0010 §1/§2).
///
/// `t/<tenant_hash_hex>/<signal>/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt`
pub fn commit_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    ingest_hour_bucket: u32,
    writer_id: Uuid,
    epoch: u64,
    seq: u64,
) -> Result<String, KeyError> {
    let prefix = commit_shard_hour_prefix(tenant_hash, signal, shard, ingest_hour_bucket)?;
    Ok(format!(
        "{prefix}{writer_id}.{epoch}.{seq:020}.{COMMIT_SUFFIX}"
    ))
}

/// Build the exact commit key a [`CommitToken`] addresses. A token fully
/// determines its commit key (ADR-0010 §2); resolvers use this to GET
/// directly, never to re-list.
pub fn commit_key_for_token(
    tenant_hash: &TenantHash,
    signal: Signal,
    token: &CommitToken,
) -> Result<String, KeyError> {
    commit_key(
        tenant_hash,
        signal,
        token.shard,
        token.ingest_hour_bucket,
        token.writer_id,
        token.epoch,
        token.seq,
    )
}

/// Prefix covering every commit record for one (tenant, signal, shard),
/// across all ingest hours.
pub fn commit_shard_prefix(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<String, KeyError> {
    Ok(format!(
        "t/{}/{}/c/{}/",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        format_shard(shard)?
    ))
}

/// Prefix covering every commit record for one (tenant, signal, shard,
/// ingest-hour bucket). This is the unit the catalog lists (docs/catalog-and-mvcc.md).
pub fn commit_shard_hour_prefix(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    ingest_hour_bucket: u32,
) -> Result<String, KeyError> {
    Ok(format!(
        "{}{}/",
        commit_shard_prefix(tenant_hash, signal, shard)?,
        ingest_hour_string(ingest_hour_bucket)
    ))
}

/// Parsed form of a data-object key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDataKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub writer_id: Uuid,
    pub epoch: u64,
    pub seq: u64,
    pub hash16: String,
}

/// Parsed form of a commit-record key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommitKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
    pub writer_id: Uuid,
    pub epoch: u64,
    pub seq: u64,
}

fn malformed(key: &str, reason: impl Into<String>) -> KeyError {
    KeyError::Malformed {
        key: key.to_string(),
        reason: reason.into(),
    }
}

fn parse_shard_component(key: &str, s: &str) -> Result<u32, KeyError> {
    if s.len() != 4 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed(
            key,
            format!("shard component {s:?} is not 4 digits"),
        ));
    }
    s.parse()
        .map_err(|_| malformed(key, "shard component overflow"))
}

fn parse_seq_component(key: &str, s: &str) -> Result<u64, KeyError> {
    if s.len() != 20 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed(
            key,
            format!("seq component {s:?} is not 20 digits"),
        ));
    }
    s.parse()
        .map_err(|_| malformed(key, "seq component overflow"))
}

/// Parse a data-object key produced by [`data_key`], validating every
/// component.
pub fn parse_data_key(key: &str) -> Result<ParsedDataKey, KeyError> {
    let parts: Vec<&str> = key.split('/').collect();
    let [root, tenant_hex, signal_s, l0, shard_s, filename] = parts.as_slice() else {
        return Err(malformed(key, "expected 6 path segments"));
    };
    if *root != "t" {
        return Err(malformed(key, "expected key to start with \"t/\""));
    }
    if *l0 != "l0" {
        return Err(malformed(key, "expected \"l0\" segment"));
    }
    let tenant_hash = TenantHash::from_hex(tenant_hex)
        .map_err(|_| KeyError::InvalidTenantHash(tenant_hex.to_string()))?;
    let signal =
        signal::from_prefix(signal_s).map_err(|_| KeyError::UnknownSignal(signal_s.to_string()))?;
    let shard = parse_shard_component(key, shard_s)?;

    let file_parts: Vec<&str> = filename.split('.').collect();
    let [writer_id_s, epoch_s, seq_s, hash16, suffix] = file_parts.as_slice() else {
        return Err(malformed(
            key,
            "expected \"writer.epoch.seq.hash16.rseg\" filename",
        ));
    };
    if *suffix != DATA_SUFFIX {
        return Err(malformed(key, format!("expected suffix {DATA_SUFFIX:?}")));
    }
    if hash16.len() != 16 || !hash16.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(malformed(
            key,
            format!("hash16 component {hash16:?} is not 16 hex chars"),
        ));
    }
    let writer_id = Uuid::parse_str(writer_id_s)
        .map_err(|_| KeyError::InvalidWriterId(writer_id_s.to_string()))?;
    let epoch: u64 = epoch_s
        .parse()
        .map_err(|_| malformed(key, "invalid epoch"))?;
    let seq = parse_seq_component(key, seq_s)?;

    Ok(ParsedDataKey {
        tenant_hash,
        signal,
        shard,
        writer_id,
        epoch,
        seq,
        hash16: hash16.to_string(),
    })
}

/// Parse a commit-record key produced by [`commit_key`], validating every
/// component.
pub fn parse_commit_key(key: &str) -> Result<ParsedCommitKey, KeyError> {
    let parts: Vec<&str> = key.split('/').collect();
    let [root, tenant_hex, signal_s, c, shard_s, hour_s, filename] = parts.as_slice() else {
        return Err(malformed(key, "expected 7 path segments"));
    };
    if *root != "t" {
        return Err(malformed(key, "expected key to start with \"t/\""));
    }
    if *c != "c" {
        return Err(malformed(key, "expected \"c\" segment"));
    }
    let tenant_hash = TenantHash::from_hex(tenant_hex)
        .map_err(|_| KeyError::InvalidTenantHash(tenant_hex.to_string()))?;
    let signal =
        signal::from_prefix(signal_s).map_err(|_| KeyError::UnknownSignal(signal_s.to_string()))?;
    let shard = parse_shard_component(key, shard_s)?;
    let ingest_hour_bucket = parse_ingest_hour_string(hour_s)?;

    let file_parts: Vec<&str> = filename.split('.').collect();
    let [writer_id_s, epoch_s, seq_s, suffix] = file_parts.as_slice() else {
        return Err(malformed(key, "expected \"writer.epoch.seq.cmt\" filename"));
    };
    if *suffix != COMMIT_SUFFIX {
        return Err(malformed(key, format!("expected suffix {COMMIT_SUFFIX:?}")));
    }
    let writer_id = Uuid::parse_str(writer_id_s)
        .map_err(|_| KeyError::InvalidWriterId(writer_id_s.to_string()))?;
    let epoch: u64 = epoch_s
        .parse()
        .map_err(|_| malformed(key, "invalid epoch"))?;
    let seq = parse_seq_component(key, seq_s)?;

    Ok(ParsedCommitKey {
        tenant_hash,
        signal,
        shard,
        ingest_hour_bucket,
        writer_id,
        epoch,
        seq,
    })
}

/// Reconstruct the data-object key implied by a commit record's own
/// identity fields (tenant_hash, signal, shard, writer_id, epoch, seq,
/// content_hash). Never trust `record.object_key` directly: it is
/// informational only (ADR-0010 §7); use [`verify_object_key`] to check it.
pub fn reconstruct_data_key(
    record: &ravel_proto::commit::v1::CommitRecord,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    let writer_id = Uuid::parse_str(&record.writer_id)
        .map_err(|_| KeyError::InvalidWriterId(record.writer_id.clone()))?;
    let content_hash = content_hash_from_bytes(&record.content_hash)?;
    data_key(
        &tenant_hash,
        signal,
        record.shard,
        writer_id,
        record.writer_epoch,
        record.writer_seq,
        &content_hash,
    )
}

/// The commit-record's own key, reconstructed from its identity fields
/// (never trust a stored key string for this; commit records are addressed
/// by construction, exactly like data objects).
pub fn commit_key_for_record(
    record: &ravel_proto::commit::v1::CommitRecord,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    let writer_id = Uuid::parse_str(&record.writer_id)
        .map_err(|_| KeyError::InvalidWriterId(record.writer_id.clone()))?;
    commit_key(
        &tenant_hash,
        signal,
        record.shard,
        record.ingest_hour_bucket,
        writer_id,
        record.writer_epoch,
        record.writer_seq,
    )
}

/// Reconstruct the data key from the record's identity fields and compare it
/// against the stored `object_key`. Any mismatch is a fatal invariant breach
/// (ADR-0010 §7): readers must crash loudly, never silently prefer either
/// value. Returns the verified (reconstructed) key on success.
pub fn verify_object_key(
    record: &ravel_proto::commit::v1::CommitRecord,
) -> Result<String, ReconstructionError> {
    let expected = reconstruct_data_key(record)?;
    if expected == record.object_key {
        Ok(expected)
    } else {
        Err(ReconstructionError::ObjectKeyMismatch {
            expected,
            actual: record.object_key.clone(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn tenant_hash() -> TenantHash {
        TenantHash([0xab; 16])
    }

    fn hash32(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn data_key_round_trips() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        let key =
            data_key(&th, Signal::Metrics, 7, writer_id, 42, 99, &hash32(0xcd)).expect("build");
        assert_eq!(
            key,
            format!(
                "t/{}/m/l0/0007/{}.42.{:020}.{}.rseg",
                th.to_hex(),
                writer_id,
                99,
                hex::encode([0xcd; 8])
            )
        );
        let parsed = parse_data_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Metrics);
        assert_eq!(parsed.shard, 7);
        assert_eq!(parsed.writer_id, writer_id);
        assert_eq!(parsed.epoch, 42);
        assert_eq!(parsed.seq, 99);
        assert_eq!(parsed.hash16, hex::encode([0xcd; 8]));
    }

    #[test]
    fn commit_key_round_trips() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        // 2026-07-26T14:00:00Z: 20655 days since epoch * 24 + 14 hours.
        let hour_bucket = 495_734u32;
        let key = commit_key(&th, Signal::Logs, 3, hour_bucket, writer_id, 7, 12).expect("build");
        let parsed = parse_commit_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Logs);
        assert_eq!(parsed.shard, 3);
        assert_eq!(parsed.ingest_hour_bucket, hour_bucket);
        assert_eq!(parsed.writer_id, writer_id);
        assert_eq!(parsed.epoch, 7);
        assert_eq!(parsed.seq, 12);
    }

    #[test]
    fn parse_data_key_rejects_malformed_input() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        let good =
            data_key(&th, Signal::Metrics, 1, writer_id, 2, 3, &hash32(0xaa)).expect("build");

        // Wrong number of path segments.
        assert!(parse_data_key("t/only/three").is_err());
        assert!(parse_data_key(&format!("{good}/extra")).is_err());

        // Bad root segment.
        let bad_root = good.replacen("t/", "x/", 1);
        assert!(parse_data_key(&bad_root).is_err());

        // "c" where "l0" belongs.
        let bad_l0 = good.replacen("/l0/", "/c/", 1);
        assert!(parse_data_key(&bad_l0).is_err());

        // Shard not exactly 4 digits.
        let bad_shard = good.replacen("/0001/", "/001/", 1);
        assert!(parse_data_key(&bad_shard).is_err());

        // Seq not exactly 20 digits.
        let bad_seq = good.replacen(
            &format!("{writer_id}.2.{:020}.", 3),
            &format!("{writer_id}.2.3."),
            1,
        );
        assert!(parse_data_key(&bad_seq).is_err());

        // hash16 wrong length.
        let bad_hash_len = good.replace(&hex::encode([0xaa; 8]), &hex::encode([0xaa; 8])[..15]);
        assert!(parse_data_key(&bad_hash_len).is_err());

        // hash16 not hex.
        let bad_hash_hex = good.replace(&hex::encode([0xaa; 8]), "zzzzzzzzzzzzzzzz");
        assert!(parse_data_key(&bad_hash_hex).is_err());

        // Wrong suffix (looks like a commit key).
        let bad_suffix = good.replacen(".rseg", ".cmt", 1);
        assert!(parse_data_key(&bad_suffix).is_err());

        // Non-UUID writer id.
        let bad_writer = good.replacen(&writer_id.to_string(), "not-a-uuid", 1);
        assert!(parse_data_key(&bad_writer).is_err());

        // Non-numeric signal prefix.
        let bad_signal = good.replacen("/m/", "/q/", 1);
        assert!(parse_data_key(&bad_signal).is_err());
    }

    #[test]
    fn parse_commit_key_rejects_malformed_input() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        let hour_bucket = 495_734u32;
        let good = commit_key(&th, Signal::Logs, 3, hour_bucket, writer_id, 7, 12).expect("build");

        // Wrong number of path segments.
        assert!(parse_commit_key("t/only/three").is_err());
        assert!(parse_commit_key(&format!("{good}/extra")).is_err());

        // "l0" where "c" belongs.
        let bad_c = good.replacen("/c/", "/l0/", 1);
        assert!(parse_commit_key(&bad_c).is_err());

        // Shard not exactly 4 digits.
        let bad_shard = good.replacen("/0003/", "/03/", 1);
        assert!(parse_commit_key(&bad_shard).is_err());

        // Malformed hour segment (missing the "T" separator).
        let bad_hour = good.replacen(&ingest_hour_string(hour_bucket), "2026072614", 1);
        assert!(parse_commit_key(&bad_hour).is_err());

        // Seq not exactly 20 digits.
        let bad_seq = good.replacen(
            &format!("{writer_id}.7.{:020}.", 12),
            &format!("{writer_id}.7.12."),
            1,
        );
        assert!(parse_commit_key(&bad_seq).is_err());

        // Wrong suffix (looks like a data key).
        let bad_suffix = good.replacen(".cmt", ".rseg", 1);
        assert!(parse_commit_key(&bad_suffix).is_err());

        // Non-UUID writer id.
        let bad_writer = good.replacen(&writer_id.to_string(), "not-a-uuid", 1);
        assert!(parse_commit_key(&bad_writer).is_err());
    }

    #[test]
    fn shard_out_of_range_is_rejected() {
        let th = tenant_hash();
        let err = data_key(
            &th,
            Signal::Metrics,
            10_000,
            Uuid::new_v4(),
            0,
            0,
            &hash32(1),
        );
        assert_eq!(err, Err(KeyError::ShardOutOfRange(10_000)));
    }

    #[test]
    fn ingest_hour_string_exact_hour_boundary() {
        // 1970-01-01T00:00:00Z is hour bucket 0.
        assert_eq!(ingest_hour_string(0), "19700101T00");
        // Exactly one hour later.
        assert_eq!(ingest_hour_string(1), "19700101T01");
        // 23:00 on day 0, then day rollover at hour 24.
        assert_eq!(ingest_hour_string(23), "19700101T23");
        assert_eq!(ingest_hour_string(24), "19700102T00");
    }

    #[test]
    fn ingest_hour_string_month_and_year_rollover() {
        // 1970-01-31T23 -> next hour rolls into February.
        let jan_31_23 = 30 * 24 + 23;
        assert_eq!(ingest_hour_string(jan_31_23), "19700131T23");
        assert_eq!(ingest_hour_string(jan_31_23 + 1), "19700201T00");
        // End of a non-leap year: 1970-12-31T23 -> 1971-01-01T00.
        let dec_31_23 = 364 * 24 + 23;
        assert_eq!(ingest_hour_string(dec_31_23), "19701231T23");
        assert_eq!(ingest_hour_string(dec_31_23 + 1), "19710101T00");
    }

    #[test]
    fn ingest_hour_round_trips_through_parse() {
        for hour_bucket in [0u32, 1, 23, 24, 8_760, 495_734, 1_000_000] {
            let s = ingest_hour_string(hour_bucket);
            assert_eq!(parse_ingest_hour_string(&s).expect("parse"), hour_bucket);
        }
    }

    #[test]
    fn parse_ingest_hour_rejects_bad_calendar_dates() {
        assert!(parse_ingest_hour_string("20260230T00").is_err()); // Feb 30 does not exist
        assert!(parse_ingest_hour_string("20260101T24").is_err()); // hour out of range
        assert!(parse_ingest_hour_string("garbage").is_err());
    }

    #[test]
    fn reconstruct_data_key_matches_data_key() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        let content_hash = hash32(0x11);
        let expected =
            data_key(&th, Signal::Metrics, 2, writer_id, 5, 6, &content_hash).expect("build");
        let record = ravel_proto::commit::v1::CommitRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: ravel_proto::commit::v1::Signal::Metrics as i32,
            shard: 2,
            writer_id: writer_id.to_string(),
            writer_epoch: 5,
            writer_seq: 6,
            object_key: expected.clone(),
            object_size: 0,
            content_hash: content_hash.to_vec(),
            sample_count: 0,
            series_count: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        };
        assert_eq!(
            reconstruct_data_key(&record).expect("reconstruct"),
            expected
        );
        assert_eq!(verify_object_key(&record).expect("verified"), expected);
    }

    #[test]
    fn verify_object_key_detects_mismatch() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        let content_hash = hash32(0x22);
        let record = ravel_proto::commit::v1::CommitRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: ravel_proto::commit::v1::Signal::Metrics as i32,
            shard: 2,
            writer_id: writer_id.to_string(),
            writer_epoch: 5,
            writer_seq: 6,
            object_key: "t/wrong/key".to_string(),
            object_size: 0,
            content_hash: content_hash.to_vec(),
            sample_count: 0,
            series_count: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        };
        let err = verify_object_key(&record).expect_err("must be fatal mismatch");
        assert!(matches!(err, ReconstructionError::ObjectKeyMismatch { .. }));
    }

    #[test]
    fn commit_key_for_token_matches_commit_key() {
        let th = tenant_hash();
        let token = CommitToken {
            shard: 4,
            writer_id: Uuid::new_v4(),
            epoch: 9,
            seq: 20,
            ingest_hour_bucket: 495_734,
        };
        let expected = commit_key(
            &th,
            Signal::Spans,
            token.shard,
            token.ingest_hour_bucket,
            token.writer_id,
            token.epoch,
            token.seq,
        )
        .expect("build");
        assert_eq!(
            commit_key_for_token(&th, Signal::Spans, &token).expect("token key"),
            expected
        );
    }

    #[test]
    fn prefixes_are_prefixes_of_the_full_key() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        let hour_bucket = 495_734u32;
        let key = commit_key(&th, Signal::Metrics, 1, hour_bucket, writer_id, 0, 0).expect("build");
        let shard_prefix = commit_shard_prefix(&th, Signal::Metrics, 1).expect("prefix");
        let shard_hour_prefix =
            commit_shard_hour_prefix(&th, Signal::Metrics, 1, hour_bucket).expect("prefix");
        assert!(key.starts_with(&shard_prefix));
        assert!(key.starts_with(&shard_hour_prefix));
    }
}

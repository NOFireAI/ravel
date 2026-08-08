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

/// Data object suffix (RSEG v1). L1 parts share this suffix (ADR-0018 §2):
/// the page grammar is unchanged, only the trailer version and catalog
/// differ.
pub const DATA_SUFFIX: &str = "rseg";
/// Commit record object suffix. Compaction records share this suffix
/// (docs/compaction-retention-plan.md §3.1): they live in the same `c/`
/// prefix and are told apart from L0 commit records by filename shape.
pub const COMMIT_SUFFIX: &str = "cmt";
/// Retention tombstone object suffix.
pub const TOMBSTONE_SUFFIX: &str = "tmb";
/// L1 part key directory segment.
pub const L1_DIR: &str = "l1";
/// Compaction record filename tag: `l1.<input_set_hash16>.cmt`.
pub const COMPACTION_RECORD_TAG: &str = "l1";
/// Retention tombstone filename: `retire.tmb`, fixed per bucket.
pub const TOMBSTONE_FILENAME: &str = "retire.tmb";
/// Advisory maintenance-cursor directory segment.
pub const MAINT_DIR: &str = "maint";
/// Advisory maintenance-cursor filename, fixed per (tenant, signal, shard).
pub const CURSOR_FILENAME: &str = "cursor";
/// Selective-erasure directory segment (ADR-0064): holds `.dreq` requests
/// and `.done` completion records for one (tenant, signal).
pub const DEL_DIR: &str = "del";
/// Erasure request object suffix: `<request_id>.dreq`.
pub const DREQ_SUFFIX: &str = "dreq";
/// Erasure completion object suffix: `<request_id>.done`.
pub const DONE_SUFFIX: &str = "done";
/// Rewrite record filename tag: `rw.<input_set_hash16>.cmt`. Shares the `c/`
/// prefix and `.cmt` suffix with compaction records (ADR-0064 decision 3);
/// told apart by this tag, exactly as compaction records use [`COMPACTION_RECORD_TAG`].
pub const REWRITE_RECORD_TAG: &str = "rw";

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
    #[error("part index {0} exceeds the 4-digit key width (max 9999)")]
    PartIndexOutOfRange(u32),
    #[error("invalid hash16 {0:?}: expected 16 lowercase hex chars")]
    InvalidHash16(String),
    #[error(
        "key {0:?} matches no known bucket-entry shape (commit record, compaction record, tombstone)"
    )]
    UnknownBucketEntryShape(String),
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

fn format_part_index(part_index: u32) -> Result<String, KeyError> {
    if part_index > 9999 {
        return Err(KeyError::PartIndexOutOfRange(part_index));
    }
    Ok(format!("{part_index:04}"))
}

/// Validate a caller-supplied hash16 argument to a key builder (as opposed
/// to [`parse_hash16_component`], which validates one already-split
/// filename component while parsing).
fn validate_hash16_arg(s: &str) -> Result<(), KeyError> {
    if s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(KeyError::InvalidHash16(s.to_string()))
    }
}

fn parse_hash16_component(key: &str, s: &str) -> Result<String, KeyError> {
    if s.len() != 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(malformed(
            key,
            format!("hash16 component {s:?} is not 16 hex chars"),
        ));
    }
    Ok(s.to_string())
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

// --- Compaction and retention key shapes (docs/compaction-retention-plan.md
// §3.1, ADR-0018, ADR-0019). All four are additive: new prefixes only,
// existing key shapes above are untouched. ---

/// Build the L1 part key for a compacted sealed bucket (plan §3.1).
///
/// `t/<tenant_hash_hex>/<signal>/l1/<shard>/<ingest_hour>/<input_set_hash16>.<part:04>.<hash16>.rseg`
pub fn l1_part_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    ingest_hour_bucket: u32,
    input_set_hash16: &str,
    part_index: u32,
    hash16: &str,
) -> Result<String, KeyError> {
    validate_hash16_arg(input_set_hash16)?;
    validate_hash16_arg(hash16)?;
    let shard_s = format_shard(shard)?;
    let part_s = format_part_index(part_index)?;
    Ok(format!(
        "t/{}/{}/{}/{}/{}/{}.{}.{}.{}",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        L1_DIR,
        shard_s,
        ingest_hour_string(ingest_hour_bucket),
        input_set_hash16,
        part_s,
        hash16,
        DATA_SUFFIX
    ))
}

/// Build the compaction record key for a sealed bucket (plan §3.1).
///
/// `t/<tenant_hash_hex>/<signal>/c/<shard>/<ingest_hour>/l1.<input_set_hash16>.cmt`
pub fn compaction_record_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    ingest_hour_bucket: u32,
    input_set_hash16: &str,
) -> Result<String, KeyError> {
    validate_hash16_arg(input_set_hash16)?;
    let prefix = commit_shard_hour_prefix(tenant_hash, signal, shard, ingest_hour_bucket)?;
    Ok(format!(
        "{prefix}{COMPACTION_RECORD_TAG}.{input_set_hash16}.{COMMIT_SUFFIX}"
    ))
}

/// Build the retention tombstone key for a sealed bucket (ADR-0019 §Decision 2).
///
/// `t/<tenant_hash_hex>/<signal>/c/<shard>/<ingest_hour>/retire.tmb`
pub fn retention_tombstone_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    ingest_hour_bucket: u32,
) -> Result<String, KeyError> {
    let prefix = commit_shard_hour_prefix(tenant_hash, signal, shard, ingest_hour_bucket)?;
    Ok(format!("{prefix}{TOMBSTONE_FILENAME}"))
}

/// Build the advisory maintenance-scan cursor key (ADR-0018 §Decision 7).
/// CAS-updated mutable state, exempt from the immutability rule the same
/// way the ADR-0003 catalog HEAD pointer is: losing or corrupting it costs
/// a rescan, never correctness.
///
/// `t/<tenant_hash_hex>/<signal>/maint/<shard>/cursor`
pub fn maint_cursor_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<String, KeyError> {
    let shard_s = format_shard(shard)?;
    Ok(format!(
        "t/{}/{}/{}/{}/{}",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        MAINT_DIR,
        shard_s,
        CURSOR_FILENAME
    ))
}

// --- Selective-erasure key shapes (ADR-0064 decision 1 and 3). All additive:
// new `del/` prefix plus a new `rw.` tag in the existing `c/` prefix; every
// key shape above is untouched. ---

/// Prefix covering every selective-erasure record (`.dreq` and `.done`) for
/// one (tenant, signal). The resolver LISTs this per resolve to attach
/// pending predicates (ADR-0064 decision 2); it is empty for any tenant with
/// no erasure requests.
///
/// `t/<tenant_hash_hex>/<signal>/del/`
pub fn del_prefix(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!(
        "t/{}/{}/{}/",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        DEL_DIR
    )
}

/// Build the erasure request key for one request id (ADR-0064 decision 1).
/// CreateIfAbsent, immutable; deleted after completion plus horizon (§5).
///
/// `t/<tenant_hash_hex>/<signal>/del/<request_id>.dreq`
pub fn erasure_request_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    request_id: Uuid,
) -> Result<String, KeyError> {
    Ok(format!(
        "{}{}.{}",
        del_prefix(tenant_hash, signal),
        request_id,
        DREQ_SUFFIX
    ))
}

/// Build the erasure completion key for one request id (ADR-0064 decision 1).
/// CreateIfAbsent, immutable, permanent (PII-free audit evidence).
///
/// `t/<tenant_hash_hex>/<signal>/del/<request_id>.done`
pub fn erasure_completion_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    request_id: Uuid,
) -> Result<String, KeyError> {
    Ok(format!(
        "{}{}.{}",
        del_prefix(tenant_hash, signal),
        request_id,
        DONE_SUFFIX
    ))
}

/// Build the rewrite record key for a sealed bucket (ADR-0064 decision 3).
/// Same `c/` prefix and `.cmt` suffix as a compaction record, told apart by
/// the `rw.` tag, so the existing single per-bucket LIST discovers it.
///
/// `t/<tenant_hash_hex>/<signal>/c/<shard>/<ingest_hour>/rw.<input_set_hash16>.cmt`
pub fn rewrite_record_key(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    ingest_hour_bucket: u32,
    input_set_hash16: &str,
) -> Result<String, KeyError> {
    validate_hash16_arg(input_set_hash16)?;
    let prefix = commit_shard_hour_prefix(tenant_hash, signal, shard, ingest_hour_bucket)?;
    Ok(format!(
        "{prefix}{REWRITE_RECORD_TAG}.{input_set_hash16}.{COMMIT_SUFFIX}"
    ))
}

/// Parsed form of an erasure request (`.dreq`) key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedErasureRequestKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub request_id: Uuid,
}

/// Parsed form of an erasure completion (`.done`) key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedErasureCompletionKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub request_id: Uuid,
}

/// Parsed form of a rewrite record (`rw.<hash16>.cmt`) key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRewriteRecordKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
    pub input_set_hash16: String,
}

/// Parse the `t/<hex>/<sig>/del/<request_id>.<suffix>` shape shared by
/// `.dreq` and `.done` keys, validating every component and the suffix.
fn parse_del_key(key: &str, expected_suffix: &str) -> Result<(TenantHash, Signal, Uuid), KeyError> {
    let parts: Vec<&str> = key.split('/').collect();
    let [root, tenant_hex, signal_s, del, filename] = parts.as_slice() else {
        return Err(malformed(key, "expected 5 path segments"));
    };
    if *root != "t" {
        return Err(malformed(key, "expected key to start with \"t/\""));
    }
    if *del != DEL_DIR {
        return Err(malformed(key, format!("expected {DEL_DIR:?} segment")));
    }
    let tenant_hash = TenantHash::from_hex(tenant_hex)
        .map_err(|_| KeyError::InvalidTenantHash(tenant_hex.to_string()))?;
    let signal =
        signal::from_prefix(signal_s).map_err(|_| KeyError::UnknownSignal(signal_s.to_string()))?;

    let file_parts: Vec<&str> = filename.split('.').collect();
    let [request_id_s, suffix] = file_parts.as_slice() else {
        return Err(malformed(
            key,
            format!("expected \"<request_id>.{expected_suffix}\" filename"),
        ));
    };
    if *suffix != expected_suffix {
        return Err(malformed(
            key,
            format!("expected suffix {expected_suffix:?}"),
        ));
    }
    let request_id = Uuid::parse_str(request_id_s)
        .map_err(|_| KeyError::InvalidWriterId(request_id_s.to_string()))?;
    Ok((tenant_hash, signal, request_id))
}

/// Parse an erasure request key produced by [`erasure_request_key`].
pub fn parse_erasure_request_key(key: &str) -> Result<ParsedErasureRequestKey, KeyError> {
    let (tenant_hash, signal, request_id) = parse_del_key(key, DREQ_SUFFIX)?;
    Ok(ParsedErasureRequestKey {
        tenant_hash,
        signal,
        request_id,
    })
}

/// Parse an erasure completion key produced by [`erasure_completion_key`].
pub fn parse_erasure_completion_key(key: &str) -> Result<ParsedErasureCompletionKey, KeyError> {
    let (tenant_hash, signal, request_id) = parse_del_key(key, DONE_SUFFIX)?;
    Ok(ParsedErasureCompletionKey {
        tenant_hash,
        signal,
        request_id,
    })
}

/// Parse a rewrite record key produced by [`rewrite_record_key`], validating
/// every component. Structurally identical to a compaction record key except
/// for the leading filename tag ([`REWRITE_RECORD_TAG`] vs
/// [`COMPACTION_RECORD_TAG`]).
pub fn parse_rewrite_record_key(key: &str) -> Result<ParsedRewriteRecordKey, KeyError> {
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
    let [tag, hash16, suffix] = file_parts.as_slice() else {
        return Err(malformed(key, "expected \"rw.hash16.cmt\" filename"));
    };
    if *tag != REWRITE_RECORD_TAG {
        return Err(malformed(
            key,
            format!("expected tag {REWRITE_RECORD_TAG:?}"),
        ));
    }
    if *suffix != COMMIT_SUFFIX {
        return Err(malformed(key, format!("expected suffix {COMMIT_SUFFIX:?}")));
    }
    let input_set_hash16 = parse_hash16_component(key, hash16)?;

    Ok(ParsedRewriteRecordKey {
        tenant_hash,
        signal,
        shard,
        ingest_hour_bucket,
        input_set_hash16,
    })
}

/// The erasure request's own key, reconstructed from its identity fields.
/// Never trust a stored key string (ADR-0010 §7 discipline); callers verify
/// an observed key with [`verify_erasure_request_key`].
pub fn erasure_request_key_for(
    record: &ravel_proto::commit::v1::ErasureRequest,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    let request_id = Uuid::parse_str(&record.request_id)
        .map_err(|_| KeyError::InvalidWriterId(record.request_id.clone()))?;
    erasure_request_key(&tenant_hash, signal, request_id)
}

/// Verify an observed key against the key reconstructed from an
/// `ErasureRequest`'s own identity fields. Any mismatch is a fatal invariant
/// breach (ADR-0010 §7).
pub fn verify_erasure_request_key(
    record: &ravel_proto::commit::v1::ErasureRequest,
    observed_key: &str,
) -> Result<String, ReconstructionError> {
    let expected = erasure_request_key_for(record)?;
    if expected == observed_key {
        Ok(expected)
    } else {
        Err(ReconstructionError::ObjectKeyMismatch {
            expected,
            actual: observed_key.to_string(),
        })
    }
}

/// The erasure completion's own key, reconstructed from its identity fields.
pub fn erasure_completion_key_for(
    record: &ravel_proto::commit::v1::ErasureCompletion,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    let request_id = Uuid::parse_str(&record.request_id)
        .map_err(|_| KeyError::InvalidWriterId(record.request_id.clone()))?;
    erasure_completion_key(&tenant_hash, signal, request_id)
}

/// Verify an observed key against the key reconstructed from an
/// `ErasureCompletion`'s own identity fields.
pub fn verify_erasure_completion_key(
    record: &ravel_proto::commit::v1::ErasureCompletion,
    observed_key: &str,
) -> Result<String, ReconstructionError> {
    let expected = erasure_completion_key_for(record)?;
    if expected == observed_key {
        Ok(expected)
    } else {
        Err(ReconstructionError::ObjectKeyMismatch {
            expected,
            actual: observed_key.to_string(),
        })
    }
}

/// The rewrite record's own key, reconstructed from its identity fields (the
/// same first-8-bytes-of-`input_set_hash` convention as
/// [`compaction_record_key_for`]).
pub fn rewrite_record_key_for(
    record: &ravel_proto::commit::v1::RewriteRecord,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    let input_set_hash = content_hash_from_bytes(&record.input_set_hash)?;
    let hash16 = hex::encode(&input_set_hash[..8]);
    rewrite_record_key(
        &tenant_hash,
        signal,
        record.shard,
        record.ingest_hour_bucket,
        &hash16,
    )
}

/// Verify an observed key against the key reconstructed from a
/// `RewriteRecord`'s own identity fields.
pub fn verify_rewrite_record_key(
    record: &ravel_proto::commit::v1::RewriteRecord,
    observed_key: &str,
) -> Result<String, ReconstructionError> {
    let expected = rewrite_record_key_for(record)?;
    if expected == observed_key {
        Ok(expected)
    } else {
        Err(ReconstructionError::ObjectKeyMismatch {
            expected,
            actual: observed_key.to_string(),
        })
    }
}

/// Parsed form of an L1 part key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedL1PartKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
    pub input_set_hash16: String,
    pub part_index: u32,
    pub hash16: String,
}

/// Parse an L1 part key produced by [`l1_part_key`], validating every
/// component.
pub fn parse_l1_part_key(key: &str) -> Result<ParsedL1PartKey, KeyError> {
    let parts: Vec<&str> = key.split('/').collect();
    let [root, tenant_hex, signal_s, l1, shard_s, hour_s, filename] = parts.as_slice() else {
        return Err(malformed(key, "expected 7 path segments"));
    };
    if *root != "t" {
        return Err(malformed(key, "expected key to start with \"t/\""));
    }
    if *l1 != L1_DIR {
        return Err(malformed(key, format!("expected {L1_DIR:?} segment")));
    }
    let tenant_hash = TenantHash::from_hex(tenant_hex)
        .map_err(|_| KeyError::InvalidTenantHash(tenant_hex.to_string()))?;
    let signal =
        signal::from_prefix(signal_s).map_err(|_| KeyError::UnknownSignal(signal_s.to_string()))?;
    let shard = parse_shard_component(key, shard_s)?;
    let ingest_hour_bucket = parse_ingest_hour_string(hour_s)?;

    let file_parts: Vec<&str> = filename.split('.').collect();
    let [input_set_hash16, part_s, hash16, suffix] = file_parts.as_slice() else {
        return Err(malformed(
            key,
            "expected \"input_set_hash16.part.hash16.rseg\" filename",
        ));
    };
    if *suffix != DATA_SUFFIX {
        return Err(malformed(key, format!("expected suffix {DATA_SUFFIX:?}")));
    }
    let input_set_hash16 = parse_hash16_component(key, input_set_hash16)?;
    let hash16 = parse_hash16_component(key, hash16)?;
    if part_s.len() != 4 || !part_s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed(
            key,
            format!("part component {part_s:?} is not 4 digits"),
        ));
    }
    let part_index: u32 = part_s
        .parse()
        .map_err(|_| malformed(key, "part component overflow"))?;

    Ok(ParsedL1PartKey {
        tenant_hash,
        signal,
        shard,
        ingest_hour_bucket,
        input_set_hash16,
        part_index,
        hash16,
    })
}

/// Parsed form of a compaction record key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCompactionRecordKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
    pub input_set_hash16: String,
}

/// Parse a compaction record key produced by [`compaction_record_key`],
/// validating every component.
pub fn parse_compaction_record_key(key: &str) -> Result<ParsedCompactionRecordKey, KeyError> {
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
    let [tag, hash16, suffix] = file_parts.as_slice() else {
        return Err(malformed(key, "expected \"l1.hash16.cmt\" filename"));
    };
    if *tag != COMPACTION_RECORD_TAG {
        return Err(malformed(
            key,
            format!("expected tag {COMPACTION_RECORD_TAG:?}"),
        ));
    }
    if *suffix != COMMIT_SUFFIX {
        return Err(malformed(key, format!("expected suffix {COMMIT_SUFFIX:?}")));
    }
    let input_set_hash16 = parse_hash16_component(key, hash16)?;

    Ok(ParsedCompactionRecordKey {
        tenant_hash,
        signal,
        shard,
        ingest_hour_bucket,
        input_set_hash16,
    })
}

/// Parsed form of a retention tombstone key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRetentionTombstoneKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
}

/// Parse a retention tombstone key produced by [`retention_tombstone_key`],
/// validating every component.
pub fn parse_retention_tombstone_key(key: &str) -> Result<ParsedRetentionTombstoneKey, KeyError> {
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
    if *filename != TOMBSTONE_FILENAME {
        return Err(malformed(
            key,
            format!("expected filename {TOMBSTONE_FILENAME:?}"),
        ));
    }
    let tenant_hash = TenantHash::from_hex(tenant_hex)
        .map_err(|_| KeyError::InvalidTenantHash(tenant_hex.to_string()))?;
    let signal =
        signal::from_prefix(signal_s).map_err(|_| KeyError::UnknownSignal(signal_s.to_string()))?;
    let shard = parse_shard_component(key, shard_s)?;
    let ingest_hour_bucket = parse_ingest_hour_string(hour_s)?;

    Ok(ParsedRetentionTombstoneKey {
        tenant_hash,
        signal,
        shard,
        ingest_hour_bucket,
    })
}

/// Parsed form of an advisory maintenance-scan cursor key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMaintCursorKey {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
}

/// Parse a maintenance cursor key produced by [`maint_cursor_key`],
/// validating every component.
pub fn parse_maint_cursor_key(key: &str) -> Result<ParsedMaintCursorKey, KeyError> {
    let parts: Vec<&str> = key.split('/').collect();
    let [root, tenant_hex, signal_s, maint, shard_s, filename] = parts.as_slice() else {
        return Err(malformed(key, "expected 6 path segments"));
    };
    if *root != "t" {
        return Err(malformed(key, "expected key to start with \"t/\""));
    }
    if *maint != MAINT_DIR {
        return Err(malformed(key, format!("expected {MAINT_DIR:?} segment")));
    }
    if *filename != CURSOR_FILENAME {
        return Err(malformed(
            key,
            format!("expected filename {CURSOR_FILENAME:?}"),
        ));
    }
    let tenant_hash = TenantHash::from_hex(tenant_hex)
        .map_err(|_| KeyError::InvalidTenantHash(tenant_hex.to_string()))?;
    let signal =
        signal::from_prefix(signal_s).map_err(|_| KeyError::UnknownSignal(signal_s.to_string()))?;
    let shard = parse_shard_component(key, shard_s)?;

    Ok(ParsedMaintCursorKey {
        tenant_hash,
        signal,
        shard,
    })
}

/// The compaction record's own key, reconstructed from its identity fields.
/// Never trust a stored key string for this (ADR-0010 §7 discipline);
/// callers verify it against the key they listed or fetched from with
/// [`verify_compaction_record_key`].
pub fn compaction_record_key_for(
    record: &ravel_proto::commit::v1::CompactionRecord,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    let input_set_hash = content_hash_from_bytes(&record.input_set_hash)?;
    let hash16 = hex::encode(&input_set_hash[..8]);
    compaction_record_key(
        &tenant_hash,
        signal,
        record.shard,
        record.ingest_hour_bucket,
        &hash16,
    )
}

/// Verify an observed key (the key a `CompactionRecord` was listed or
/// fetched at) against the key reconstructed from its own identity fields.
/// Any mismatch is a fatal invariant breach, never silently preferred
/// either way (ADR-0010 §7).
pub fn verify_compaction_record_key(
    record: &ravel_proto::commit::v1::CompactionRecord,
    observed_key: &str,
) -> Result<String, ReconstructionError> {
    let expected = compaction_record_key_for(record)?;
    if expected == observed_key {
        Ok(expected)
    } else {
        Err(ReconstructionError::ObjectKeyMismatch {
            expected,
            actual: observed_key.to_string(),
        })
    }
}

/// The retention tombstone's own key, reconstructed from its identity
/// fields. Never trust a stored key string for this; callers verify it
/// against the key they listed or fetched from with
/// [`verify_retention_tombstone_key`].
pub fn retention_tombstone_key_for(
    record: &ravel_proto::commit::v1::RetentionTombstone,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    retention_tombstone_key(
        &tenant_hash,
        signal,
        record.shard,
        record.ingest_hour_bucket,
    )
}

/// Verify an observed key (the key a `RetentionTombstone` was listed or
/// fetched at) against the key reconstructed from its own identity fields.
pub fn verify_retention_tombstone_key(
    record: &ravel_proto::commit::v1::RetentionTombstone,
    observed_key: &str,
) -> Result<String, ReconstructionError> {
    let expected = retention_tombstone_key_for(record)?;
    if expected == observed_key {
        Ok(expected)
    } else {
        Err(ReconstructionError::ObjectKeyMismatch {
            expected,
            actual: observed_key.to_string(),
        })
    }
}

/// Reconstruct one part's key from its parent `CompactionRecord`'s identity
/// fields plus the part's own `part_index` and `content_hash`. Never trust
/// a stored key string for this (plan §3.5: "reconstruct the part key
/// ADR-0010 §7 style"); callers verify it against an observed key with
/// [`verify_l1_part_key`].
pub fn reconstruct_l1_part_key(
    record: &ravel_proto::commit::v1::CompactionRecord,
    part: &ravel_proto::commit::v1::CompactionPart,
) -> Result<String, KeyError> {
    let tenant_hash = tenant_hash_from_bytes(&record.tenant_hash)?;
    let signal = signal::from_proto(record.signal)
        .map_err(|_| KeyError::UnknownSignal(record.signal.to_string()))?;
    let input_set_hash = content_hash_from_bytes(&record.input_set_hash)?;
    let input_set_hash16 = hex::encode(&input_set_hash[..8]);
    let content_hash = content_hash_from_bytes(&part.content_hash)?;
    let hash16 = hex::encode(&content_hash[..8]);
    l1_part_key(
        &tenant_hash,
        signal,
        record.shard,
        record.ingest_hour_bucket,
        &input_set_hash16,
        part.part_index,
        &hash16,
    )
}

/// Verify an observed key (the key one part object was found at) against
/// the key reconstructed from its parent record's and its own identity
/// fields.
pub fn verify_l1_part_key(
    record: &ravel_proto::commit::v1::CompactionRecord,
    part: &ravel_proto::commit::v1::CompactionPart,
    observed_key: &str,
) -> Result<String, ReconstructionError> {
    let expected = reconstruct_l1_part_key(record, part)?;
    if expected == observed_key {
        Ok(expected)
    } else {
        Err(ReconstructionError::ObjectKeyMismatch {
            expected,
            actual: observed_key.to_string(),
        })
    }
}

/// One key from a `c/<shard>/<hour>/` bucket listing, classified by shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketEntry {
    CommitRecord(ParsedCommitKey),
    CompactionRecord(ParsedCompactionRecordKey),
    Tombstone(ParsedRetentionTombstoneKey),
}

// NOTE (ADR-0064, follow-up): `partition_bucket_entry` does NOT yet classify
// `rw.<hash16>.cmt` rewrite record keys. They end with `.cmt` but do not start
// with `l1.`, so today they fall through to the `else if filename.ends_with(".cmt")`
// commit-record branch and fail to parse (a UUID-shaped writer id is expected,
// `rw` is not one). This must be fixed before ravel-catalog's snapshot
// resolution can see rewrite records, at three known call sites:
//   - crates/ravel-catalog/src/catalog.rs: propagates the parse error via `?`,
//     so a live rewrite record hard-errors resolution today.
//   - crates/ravel-catalog/src/fold.rs: catches the error and silently skips
//     the entry with a warning. This one is the dangerous case: a silently
//     skipped rewrite record means its supersession of the erased inputs is
//     ignored by the index fold, so an erased subject's pre-rewrite records
//     can reappear in a folded snapshot.
//   - crates/ravel-maintain/src/read.rs (`list_bucket`): also routes through
//     `partition_bucket_entry` and hard-errors on an `rw.` key today, so
//     compacting any bucket containing a rewrite record currently fails.
// Fix `partition_bucket_entry` to emit a `BucketEntry::RewriteRecord` (via
// `parse_rewrite_record_key`) before wiring any of the three to act on it.
// Classifying `rw.` keys and teaching ravel-catalog to act on them is out of
// scope for this task; this comment is the pointer for whoever does it next.
/// Classify one key from a `c/<shard>/<hour>/` bucket listing by filename
/// shape. Name patterns are disjoint by construction (plan §3.1): a
/// tombstone is exactly `retire.tmb`, a compaction record's filename starts
/// with `l1.` where an L0 commit record's filename is always a UUID (never
/// `l1`). An unrecognized shape is a hard error, never silently skipped, so
/// layout drift surfaces to metrics instead of vanishing.
pub fn partition_bucket_entry(key: &str) -> Result<BucketEntry, KeyError> {
    let filename = key.rsplit('/').next().unwrap_or(key);
    if filename == TOMBSTONE_FILENAME {
        parse_retention_tombstone_key(key).map(BucketEntry::Tombstone)
    } else if filename.starts_with("l1.") && filename.ends_with(".cmt") {
        parse_compaction_record_key(key).map(BucketEntry::CompactionRecord)
    } else if filename.ends_with(".cmt") {
        parse_commit_key(key).map(BucketEntry::CommitRecord)
    } else {
        Err(KeyError::UnknownBucketEntryShape(key.to_string()))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_proto::commit::v1::{
        ErasureCompletion, ErasurePredicateMatcher, ErasureRequest, RewriteRecord,
    };

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

    fn hash16_hex(byte: u8) -> String {
        hex::encode([byte; 8])
    }

    #[test]
    fn l1_part_key_round_trips() {
        let th = tenant_hash();
        let hour_bucket = 495_734u32;
        let input_set_hash16 = hash16_hex(0x11);
        let hash16 = hash16_hex(0x22);
        let key = l1_part_key(
            &th,
            Signal::Metrics,
            7,
            hour_bucket,
            &input_set_hash16,
            3,
            &hash16,
        )
        .expect("build");
        assert_eq!(
            key,
            format!(
                "t/{}/m/l1/0007/{}/{}.0003.{}.rseg",
                th.to_hex(),
                ingest_hour_string(hour_bucket),
                input_set_hash16,
                hash16
            )
        );
        let parsed = parse_l1_part_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Metrics);
        assert_eq!(parsed.shard, 7);
        assert_eq!(parsed.ingest_hour_bucket, hour_bucket);
        assert_eq!(parsed.input_set_hash16, input_set_hash16);
        assert_eq!(parsed.part_index, 3);
        assert_eq!(parsed.hash16, hash16);
    }

    #[test]
    fn l1_part_key_rejects_bad_input() {
        let th = tenant_hash();
        assert_eq!(
            l1_part_key(&th, Signal::Metrics, 1, 0, "not-hex", 0, &hash16_hex(1)),
            Err(KeyError::InvalidHash16("not-hex".to_string()))
        );
        assert_eq!(
            l1_part_key(
                &th,
                Signal::Metrics,
                1,
                0,
                &hash16_hex(1),
                10_000,
                &hash16_hex(1)
            ),
            Err(KeyError::PartIndexOutOfRange(10_000))
        );
    }

    #[test]
    fn parse_l1_part_key_rejects_malformed_input() {
        let th = tenant_hash();
        let good = l1_part_key(
            &th,
            Signal::Metrics,
            1,
            0,
            &hash16_hex(1),
            2,
            &hash16_hex(2),
        )
        .expect("build");

        assert!(parse_l1_part_key("t/only/three").is_err());
        assert!(parse_l1_part_key(&format!("{good}/extra")).is_err());
        let bad_dir = good.replacen("/l1/", "/l0/", 1);
        assert!(parse_l1_part_key(&bad_dir).is_err());
        let bad_suffix = good.replacen(".rseg", ".cmt", 1);
        assert!(parse_l1_part_key(&bad_suffix).is_err());
        let bad_part = good.replacen(".0002.", "..", 1);
        assert!(parse_l1_part_key(&bad_part).is_err());
    }

    #[test]
    fn compaction_record_key_round_trips() {
        let th = tenant_hash();
        let hour_bucket = 495_734u32;
        let input_set_hash16 = hash16_hex(0x33);
        let key = compaction_record_key(&th, Signal::Logs, 3, hour_bucket, &input_set_hash16)
            .expect("build");
        assert_eq!(
            key,
            format!(
                "t/{}/l/c/0003/{}/l1.{}.cmt",
                th.to_hex(),
                ingest_hour_string(hour_bucket),
                input_set_hash16
            )
        );
        let parsed = parse_compaction_record_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Logs);
        assert_eq!(parsed.shard, 3);
        assert_eq!(parsed.ingest_hour_bucket, hour_bucket);
        assert_eq!(parsed.input_set_hash16, input_set_hash16);
    }

    #[test]
    fn parse_compaction_record_key_rejects_malformed_input() {
        let th = tenant_hash();
        let good = compaction_record_key(&th, Signal::Logs, 3, 0, &hash16_hex(4)).expect("build");

        assert!(parse_compaction_record_key("t/only/three").is_err());
        let bad_tag = good.replacen("l1.", "l2.", 1);
        assert!(parse_compaction_record_key(&bad_tag).is_err());
        let bad_suffix = good.replacen(".cmt", ".rseg", 1);
        assert!(parse_compaction_record_key(&bad_suffix).is_err());
        // Looks like an L0 commit key instead (four dot components).
        assert!(parse_compaction_record_key(&good.replacen("l1.", "", 1)).is_err());
    }

    #[test]
    fn retention_tombstone_key_round_trips() {
        let th = tenant_hash();
        let hour_bucket = 495_734u32;
        let key = retention_tombstone_key(&th, Signal::Spans, 9, hour_bucket).expect("build");
        assert_eq!(
            key,
            format!(
                "t/{}/s/c/0009/{}/retire.tmb",
                th.to_hex(),
                ingest_hour_string(hour_bucket)
            )
        );
        let parsed = parse_retention_tombstone_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Spans);
        assert_eq!(parsed.shard, 9);
        assert_eq!(parsed.ingest_hour_bucket, hour_bucket);
    }

    #[test]
    fn parse_retention_tombstone_key_rejects_malformed_input() {
        let th = tenant_hash();
        let good = retention_tombstone_key(&th, Signal::Spans, 9, 0).expect("build");
        assert!(parse_retention_tombstone_key("t/only/three").is_err());
        let bad_filename = good.replacen("retire.tmb", "retire.cmt", 1);
        assert!(parse_retention_tombstone_key(&bad_filename).is_err());
        let bad_c = good.replacen("/c/", "/l0/", 1);
        assert!(parse_retention_tombstone_key(&bad_c).is_err());
    }

    #[test]
    fn maint_cursor_key_round_trips() {
        let th = tenant_hash();
        let key = maint_cursor_key(&th, Signal::Profiles, 2).expect("build");
        assert_eq!(key, format!("t/{}/p/maint/0002/cursor", th.to_hex()));
        let parsed = parse_maint_cursor_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Profiles);
        assert_eq!(parsed.shard, 2);
    }

    #[test]
    fn parse_maint_cursor_key_rejects_malformed_input() {
        let th = tenant_hash();
        let good = maint_cursor_key(&th, Signal::Profiles, 2).expect("build");
        assert!(parse_maint_cursor_key("t/only/three").is_err());
        let bad_dir = good.replacen("/maint/", "/l0/", 1);
        assert!(parse_maint_cursor_key(&bad_dir).is_err());
        let bad_filename = good.replacen("cursor", "cursors", 1);
        assert!(parse_maint_cursor_key(&bad_filename).is_err());
    }

    fn sample_compaction_record(
        th: &TenantHash,
        signal: Signal,
        shard: u32,
        hour_bucket: u32,
        input_set_hash: [u8; 32],
        part_content_hash: [u8; 32],
    ) -> ravel_proto::commit::v1::CompactionRecord {
        ravel_proto::commit::v1::CompactionRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: signal::to_proto(signal) as i32,
            shard,
            ingest_hour_bucket: hour_bucket,
            level: 1,
            inputs: vec![],
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![ravel_proto::commit::v1::CompactionPart {
                part_index: 0,
                first_series_id: vec![0; 16],
                last_series_id: vec![0xff; 16],
                content_hash: part_content_hash.to_vec(),
                object_size: 0,
                sample_count: 0,
                series_count: 0,
                run_count: 0,
                min_event_ts_ns: 0,
                max_event_ts_ns: 0,
                segment_format_version: 3,
            }],
            created_unix_ns: 0,
        }
    }

    #[test]
    fn compaction_record_key_for_matches_compaction_record_key() {
        let th = tenant_hash();
        let input_set_hash = hash32(0x55);
        let record =
            sample_compaction_record(&th, Signal::Metrics, 1, 0, input_set_hash, hash32(0x66));
        let expected = compaction_record_key(
            &th,
            Signal::Metrics,
            1,
            0,
            &hex::encode(&input_set_hash[..8]),
        )
        .expect("build");
        assert_eq!(
            compaction_record_key_for(&record).expect("reconstruct"),
            expected
        );
        assert_eq!(
            verify_compaction_record_key(&record, &expected).expect("verified"),
            expected
        );
    }

    #[test]
    fn verify_compaction_record_key_detects_mismatch() {
        let th = tenant_hash();
        let record =
            sample_compaction_record(&th, Signal::Metrics, 1, 0, hash32(0x77), hash32(0x88));
        let err = verify_compaction_record_key(&record, "t/wrong/key")
            .expect_err("must be fatal mismatch");
        assert!(matches!(err, ReconstructionError::ObjectKeyMismatch { .. }));
    }

    #[test]
    fn reconstruct_l1_part_key_matches_l1_part_key() {
        let th = tenant_hash();
        let input_set_hash = hash32(0x99);
        let part_content_hash = hash32(0xaa);
        let record =
            sample_compaction_record(&th, Signal::Logs, 4, 0, input_set_hash, part_content_hash);
        let expected = l1_part_key(
            &th,
            Signal::Logs,
            4,
            0,
            &hex::encode(&input_set_hash[..8]),
            0,
            &hex::encode(&part_content_hash[..8]),
        )
        .expect("build");
        let part = &record.parts[0];
        assert_eq!(
            reconstruct_l1_part_key(&record, part).expect("reconstruct"),
            expected
        );
        assert_eq!(
            verify_l1_part_key(&record, part, &expected).expect("verified"),
            expected
        );
    }

    fn sample_tombstone(
        th: &TenantHash,
        signal: Signal,
        shard: u32,
        hour_bucket: u32,
    ) -> ravel_proto::commit::v1::RetentionTombstone {
        ravel_proto::commit::v1::RetentionTombstone {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: signal::to_proto(signal) as i32,
            shard,
            ingest_hour_bucket: hour_bucket,
            retired_at_ns: 0,
            retention_window_ns: 0,
            record_count_observed: 0,
        }
    }

    #[test]
    fn retention_tombstone_key_for_matches_retention_tombstone_key() {
        let th = tenant_hash();
        let record = sample_tombstone(&th, Signal::Spans, 5, 495_734);
        let expected = retention_tombstone_key(&th, Signal::Spans, 5, 495_734).expect("build");
        assert_eq!(
            retention_tombstone_key_for(&record).expect("reconstruct"),
            expected
        );
        assert_eq!(
            verify_retention_tombstone_key(&record, &expected).expect("verified"),
            expected
        );
    }

    #[test]
    fn verify_retention_tombstone_key_detects_mismatch() {
        let th = tenant_hash();
        let record = sample_tombstone(&th, Signal::Spans, 5, 495_734);
        let err = verify_retention_tombstone_key(&record, "t/wrong/key")
            .expect_err("must be fatal mismatch");
        assert!(matches!(err, ReconstructionError::ObjectKeyMismatch { .. }));
    }

    #[test]
    fn partition_bucket_entry_classifies_each_shape() {
        let th = tenant_hash();
        let writer_id = Uuid::new_v4();
        let hour_bucket = 495_734u32;

        let commit = commit_key(&th, Signal::Metrics, 1, hour_bucket, writer_id, 0, 0)
            .expect("build commit key");
        assert!(matches!(
            partition_bucket_entry(&commit).expect("classify"),
            BucketEntry::CommitRecord(_)
        ));

        let compaction =
            compaction_record_key(&th, Signal::Metrics, 1, hour_bucket, &hash16_hex(1))
                .expect("build compaction key");
        assert!(matches!(
            partition_bucket_entry(&compaction).expect("classify"),
            BucketEntry::CompactionRecord(_)
        ));

        let tombstone = retention_tombstone_key(&th, Signal::Metrics, 1, hour_bucket)
            .expect("build tombstone key");
        assert!(matches!(
            partition_bucket_entry(&tombstone).expect("classify"),
            BucketEntry::Tombstone(_)
        ));
    }

    #[test]
    fn partition_bucket_entry_fails_loud_on_unknown_shape() {
        let th = tenant_hash();
        let hour_bucket = 495_734u32;
        let prefix =
            commit_shard_hour_prefix(&th, Signal::Metrics, 1, hour_bucket).expect("prefix");
        let err = partition_bucket_entry(&format!("{prefix}unexpected.file"))
            .expect_err("unknown shape must be an error, never silently skipped");
        assert!(matches!(err, KeyError::UnknownBucketEntryShape(_)));
    }

    // --- Selective-erasure key shapes (ADR-0064). ---

    fn request_id() -> Uuid {
        Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0)
    }

    #[test]
    fn erasure_request_key_round_trips() {
        let th = tenant_hash();
        let rid = request_id();
        let key = erasure_request_key(&th, Signal::Metrics, rid).expect("build");
        assert_eq!(key, format!("t/{}/m/del/{}.dreq", th.to_hex(), rid));
        let parsed = parse_erasure_request_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Metrics);
        assert_eq!(parsed.request_id, rid);
    }

    #[test]
    fn erasure_completion_key_round_trips() {
        let th = tenant_hash();
        let rid = request_id();
        let key = erasure_completion_key(&th, Signal::Logs, rid).expect("build");
        assert_eq!(key, format!("t/{}/l/del/{}.done", th.to_hex(), rid));
        let parsed = parse_erasure_completion_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Logs);
        assert_eq!(parsed.request_id, rid);
    }

    #[test]
    fn dreq_and_done_keys_are_distinct_for_one_request() {
        let th = tenant_hash();
        let rid = request_id();
        let dreq = erasure_request_key(&th, Signal::Spans, rid).expect("build");
        let done = erasure_completion_key(&th, Signal::Spans, rid).expect("build");
        assert_ne!(dreq, done);
        // Only the suffix differs; the del/ prefix is shared, so one LIST of
        // del_prefix returns both.
        assert!(dreq.starts_with(&del_prefix(&th, Signal::Spans)));
        assert!(done.starts_with(&del_prefix(&th, Signal::Spans)));
    }

    #[test]
    fn parse_del_keys_reject_malformed_input() {
        let th = tenant_hash();
        let rid = request_id();
        let dreq = erasure_request_key(&th, Signal::Metrics, rid).expect("build");
        let done = erasure_completion_key(&th, Signal::Metrics, rid).expect("build");

        // Wrong number of path segments.
        assert!(parse_erasure_request_key("t/only/three").is_err());
        assert!(parse_erasure_request_key(&format!("{dreq}/extra")).is_err());
        // A .done is not a .dreq and vice versa: suffix is checked.
        assert!(parse_erasure_request_key(&done).is_err());
        assert!(parse_erasure_completion_key(&dreq).is_err());
        // Wrong directory segment.
        assert!(parse_erasure_request_key(&dreq.replacen("/del/", "/c/", 1)).is_err());
        // Non-UUID request id.
        assert!(
            parse_erasure_request_key(&dreq.replacen(&rid.to_string(), "not-a-uuid", 1)).is_err()
        );
        // Unknown signal prefix.
        assert!(parse_erasure_request_key(&dreq.replacen("/m/", "/q/", 1)).is_err());
    }

    #[test]
    fn rewrite_record_key_round_trips() {
        let th = tenant_hash();
        let hour_bucket = 495_734u32;
        let input_set_hash16 = hash16_hex(0x33);
        let key = rewrite_record_key(&th, Signal::Logs, 3, hour_bucket, &input_set_hash16)
            .expect("build");
        assert_eq!(
            key,
            format!(
                "t/{}/l/c/0003/{}/rw.{}.cmt",
                th.to_hex(),
                ingest_hour_string(hour_bucket),
                input_set_hash16
            )
        );
        let parsed = parse_rewrite_record_key(&key).expect("parse");
        assert_eq!(parsed.tenant_hash, th);
        assert_eq!(parsed.signal, Signal::Logs);
        assert_eq!(parsed.shard, 3);
        assert_eq!(parsed.ingest_hour_bucket, hour_bucket);
        assert_eq!(parsed.input_set_hash16, input_set_hash16);
    }

    #[test]
    fn rewrite_record_key_shares_bucket_prefix_with_compaction() {
        // Both records live in the same c/<shard>/<hour>/ prefix so one LIST
        // discovers both (ADR-0064 decision 3).
        let th = tenant_hash();
        let hour_bucket = 495_734u32;
        let hash16 = hash16_hex(0x44);
        let prefix =
            commit_shard_hour_prefix(&th, Signal::Metrics, 2, hour_bucket).expect("prefix");
        let rw = rewrite_record_key(&th, Signal::Metrics, 2, hour_bucket, &hash16).expect("build");
        let cp =
            compaction_record_key(&th, Signal::Metrics, 2, hour_bucket, &hash16).expect("build");
        assert!(rw.starts_with(&prefix));
        assert!(cp.starts_with(&prefix));
        assert_ne!(rw, cp);
    }

    #[test]
    fn parse_rewrite_record_key_rejects_malformed_input() {
        let th = tenant_hash();
        let good = rewrite_record_key(&th, Signal::Logs, 3, 0, &hash16_hex(4)).expect("build");
        assert!(parse_rewrite_record_key("t/only/three").is_err());
        // A compaction record's tag is not a rewrite's tag.
        assert!(parse_rewrite_record_key(&good.replacen("rw.", "l1.", 1)).is_err());
        let bad_suffix = good.replacen(".cmt", ".rseg", 1);
        assert!(parse_rewrite_record_key(&bad_suffix).is_err());
        // Looks like an L0 commit key instead (four dot components).
        assert!(parse_rewrite_record_key(&good.replacen("rw.", "", 1)).is_err());
    }

    fn sample_erasure_request(th: &TenantHash, signal: Signal, rid: Uuid) -> ErasureRequest {
        ErasureRequest {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: signal::to_proto(signal) as i32,
            request_id: rid.to_string(),
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

    #[test]
    fn erasure_request_key_for_matches_and_verifies() {
        let th = tenant_hash();
        let rid = request_id();
        let record = sample_erasure_request(&th, Signal::Metrics, rid);
        let expected = erasure_request_key(&th, Signal::Metrics, rid).expect("build");
        assert_eq!(
            erasure_request_key_for(&record).expect("reconstruct"),
            expected
        );
        assert_eq!(
            verify_erasure_request_key(&record, &expected).expect("verified"),
            expected
        );
        let err =
            verify_erasure_request_key(&record, "t/wrong/key").expect_err("must be fatal mismatch");
        assert!(matches!(err, ReconstructionError::ObjectKeyMismatch { .. }));
    }

    #[test]
    fn erasure_completion_key_for_matches_and_verifies() {
        let th = tenant_hash();
        let rid = request_id();
        let record = ErasureCompletion {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: signal::to_proto(Signal::Logs) as i32,
            request_id: rid.to_string(),
            predicate_hash: hash32(0x11).to_vec(),
            bucket_drops: vec![],
            requested_unix_ns: 0,
            completed_unix_ns: 0,
            deferral_cause: 0,
        };
        let expected = erasure_completion_key(&th, Signal::Logs, rid).expect("build");
        assert_eq!(
            erasure_completion_key_for(&record).expect("reconstruct"),
            expected
        );
        assert_eq!(
            verify_erasure_completion_key(&record, &expected).expect("verified"),
            expected
        );
        let err = verify_erasure_completion_key(&record, "t/wrong/key")
            .expect_err("must be fatal mismatch");
        assert!(matches!(err, ReconstructionError::ObjectKeyMismatch { .. }));
    }

    #[test]
    fn rewrite_record_key_for_matches_and_verifies() {
        let th = tenant_hash();
        let input_set_hash = hash32(0x55);
        let record = RewriteRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard: 1,
            ingest_hour_bucket: 495_734,
            inputs: vec![],
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![],
            drops: vec![],
            created_unix_ns: 0,
            superseded_record_key: String::new(),
        };
        let expected = rewrite_record_key(
            &th,
            Signal::Metrics,
            1,
            495_734,
            &hex::encode(&input_set_hash[..8]),
        )
        .expect("build");
        assert_eq!(
            rewrite_record_key_for(&record).expect("reconstruct"),
            expected
        );
        assert_eq!(
            verify_rewrite_record_key(&record, &expected).expect("verified"),
            expected
        );
        let err =
            verify_rewrite_record_key(&record, "t/wrong/key").expect_err("must be fatal mismatch");
        assert!(matches!(err, ReconstructionError::ObjectKeyMismatch { .. }));
    }
}

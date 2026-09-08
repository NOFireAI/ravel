//! The D5 cursor and evidence-reference codec (ADR-1374).
//!
//! [`Cursor`] and [`EvidenceRef`] are opaque, MAC'd, self-describing wire
//! tokens in the same pattern as `ravel_sql::flight_ticket`'s Flight-ticket
//! codec: magic, version byte, payload, trailing keyed BLAKE3-256 MAC,
//! base64url on the wire. A cursor pins the snapshot a paginated tool call
//! must keep reading against (the segment set, any pending erasure
//! predicates, and the tenant's declared columns at mint time) plus the
//! caller's position in the result order, so page 2 of a query answers
//! against exactly the same snapshot page 1 planned against, never a
//! re-resolution. An evidence reference pins a single row for later
//! recall by a `ravel_get_trace`-style follow-up.
//!
//! # The pinned snapshot is `ravel_sql::SegmentPin`
//!
//! A cursor's segment set is a `Vec<ravel_sql::SegmentPin>` encoded through
//! that crate's own per-pin codec (ADR-1374 decision 9), not a local
//! projection of the fields the pagination path happens to branch on. The two
//! tokens pin a snapshot for the same reason and must reconstruct the same
//! `SegmentRef`, so they share the layout and the field list; a narrower
//! mirror silently drops the pruning bounds, the routing fields, and each
//! segment's on-object format version.
//!
//! # The D5 wrong-tenant rule
//!
//! A cursor minted for tenant A that is redeemed by tenant B must decode as
//! [`CursorError::Invalid`], the same error a corrupt or tampered token
//! produces -- never a distinct "wrong tenant" signal, which would let one
//! tenant learn that a token it holds was minted for someone else. Redeeming
//! code MUST call [`Cursor::redeem`] (or [`EvidenceRef::redeem`]), never
//! [`Cursor::decode`] directly, so the tenant check happens on every call.
//!
//! # Deviation from the wire field name `sha256`
//!
//! ADR-1374's evidence-reference shape names a `sha256` field. This crate has
//! no `sha2` dependency (only `rmcp` is an authorized new dependency for this
//! task; the workspace-root edit scope is the `ravel-mcp` member plus the
//! `rmcp` workspace dependency only), and `blake3` is already the codebase's
//! universal hashing and MAC primitive (see `ravel_sql::flight_ticket`).
//! [`EvidenceRef`] hashes the referenced row with BLAKE3-256 while keeping the
//! wire field named `sha256` for D5 compatibility. This is flagged here, in
//! the crate rustdoc, and in the task's final report as an ADR ambiguity
//! rather than silently resolved.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ravel_query::erasure::ErasurePredicate;
use ravel_sql::{DeclaredColumn, DeclaredType, FlightTicketError, SegmentPin};
use ravel_types::TenantHash;

/// Length in bytes of the trailing keyed-MAC tag.
const MAC_LEN: usize = 32;

/// Length in bytes of the secret key [`Cursor::encode`]/[`Cursor::decode`]
/// (and the [`EvidenceRef`] equivalents) are keyed by. Deliberately the same
/// length as `ravel_sql::TICKET_KEY_LEN`, but this crate mints its own
/// process-local key: a cursor and a Flight ticket are different trust
/// domains, so they are never verified with the same secret.
pub const CURSOR_KEY_LEN: usize = 32;

/// The secret MAC key a [`Cursor`]/[`EvidenceRef`] is signed and verified
/// with. Generated once per process, held only in memory, never logged, sent
/// to a client, or persisted -- a process restart mints a fresh key, which is
/// safe because every token here is bounded by `deadline_ns` and never
/// expected to outlive the process that minted it.
pub type CursorKey = [u8; CURSOR_KEY_LEN];

const CURSOR_MAGIC: [u8; 4] = *b"RMC1";
/// Version 2 pins each segment as a whole `ravel_sql::SegmentPin` (all 15
/// fields, through that crate's own codec) instead of the five-field local
/// projection version 1 carried. A version 1 token decodes as
/// [`CursorError::Invalid`] like any other unsupported version, which is
/// correct and costs nothing: these tokens are process-local and
/// deadline-bounded, so none survives the deploy that changes the number.
const CURSOR_VERSION: u8 = 2;
const EVIDENCE_MAGIC: [u8; 4] = *b"RME1";
const EVIDENCE_VERSION: u8 = 1;

/// Typed failure for both [`Cursor`] and [`EvidenceRef`]. The two decode
/// outcomes are the ones ADR-1374 D5 names, and they stay
/// undifferentiated on purpose: every malformed, truncated, tampered, or
/// wrong-tenant token is [`CursorError::Invalid`], so a caller cannot
/// distinguish "not yours" from "corrupt", and only a structurally valid,
/// correctly-tenanted token past its deadline is [`CursorError::Expired`].
/// [`CursorError::FieldTooLong`] is not a third decode outcome: it is a
/// length that does not fit the wire layout at all, and a caller that hits it
/// is never a client holding a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    /// The token is malformed, truncated, tampered with (MAC mismatch), an
    /// unsupported version, or minted for a different tenant than the caller
    /// resolves to.
    #[error("cursor is invalid")]
    Invalid,
    /// The token is structurally valid and tenant-correct but its
    /// `deadline_ns` has passed.
    #[error("cursor has expired")]
    Expired,
    /// A length the wire layout cannot carry: at mint time a field longer
    /// than the `u32` its length prefix is written as. Never returned for a
    /// token a client could hold, which is why it is not one of the two D5
    /// decode outcomes above.
    #[error("cursor field length {len} exceeds the {max}-byte cap")]
    FieldTooLong { len: usize, max: usize },
}

/// Where a paginated result left off. A keyset position is an opaque,
/// tool-defined ordering key (e.g. the last row's sort tuple, encoded by the
/// tool); a row range is used by tools that page by row index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorPosition {
    Keyset(Vec<u8>),
    RowRange { start: u64, end: u64 },
}

/// The D5 cursor: an opaque, snapshot-pinning pagination token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub tenant: TenantHash,
    pub tool: String,
    /// BLAKE3-256 hash of the tool's canonicalized argument set at mint time,
    /// so a cursor cannot be redeemed against a call with different
    /// arguments than the one that minted it.
    pub argument_hash: [u8; 32],
    /// The pinned snapshot, one entry per resolved segment.
    ///
    /// This is `ravel_sql::SegmentPin` itself, not a narrower local mirror
    /// (ADR-1374 decision 9): a cursor pins a snapshot for exactly the reason
    /// a Flight ticket does, so it carries the same 15 fields through the
    /// same codec. A projection of "the fields pagination branches on" drops
    /// the pruning bounds, the routing fields, and the on-object format
    /// version, and page 2 then cannot rebuild the `SegmentRef` page 1
    /// planned against.
    pub segments: Vec<SegmentPin>,
    pub pending_erasure: Vec<ErasurePredicate>,
    pub declared_columns: Vec<DeclaredColumn>,
    pub position: CursorPosition,
    pub mint_ns: i64,
    pub deadline_ns: i64,
}

/// The D5 evidence reference: an opaque, single-row recall token (e.g. a
/// `ravel_search_logs` hit later redeemed by `ravel_get_trace`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRef {
    pub tenant: TenantHash,
    pub tool: String,
    /// BLAKE3-256 hash of the referenced row. Wire field name `sha256`; see
    /// this module's "Deviation from the wire field name `sha256`" docs.
    pub sha256: [u8; 32],
    pub mint_ns: i64,
    pub deadline_ns: i64,
}

impl Cursor {
    /// Encode to the wire layout, sign with `key`, and base64url the result.
    ///
    /// Fails with [`CursorError::FieldTooLong`] if any length prefix would
    /// not fit in a `u32` (not reachable with real object keys, tool names,
    /// or positions). A silent `as u32` truncation here would mint a token
    /// whose own length prefix disagrees with its payload.
    pub fn encode(&self, key: &CursorKey) -> Result<String, CursorError> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&CURSOR_MAGIC);
        buf.push(CURSOR_VERSION);
        buf.extend_from_slice(&self.tenant.0);
        write_len_prefixed(&mut buf, self.tool.as_bytes())?;
        buf.extend_from_slice(&self.argument_hash);

        write_count(&mut buf, self.segments.len())?;
        for seg in &self.segments {
            seg.encode_into(&mut buf)?;
        }

        write_count(&mut buf, self.pending_erasure.len())?;
        for predicate in &self.pending_erasure {
            write_count(&mut buf, predicate.matchers().len())?;
            for (k, v) in predicate.matchers() {
                write_len_prefixed(&mut buf, k.as_bytes())?;
                write_len_prefixed(&mut buf, v.as_bytes())?;
            }
            buf.extend_from_slice(&predicate.window_start_ns().to_le_bytes());
            buf.extend_from_slice(&predicate.window_end_ns().to_le_bytes());
        }

        write_count(&mut buf, self.declared_columns.len())?;
        for column in &self.declared_columns {
            write_len_prefixed(&mut buf, column.key.as_bytes())?;
            buf.push(declared_type_tag(column.ty));
        }

        match &self.position {
            CursorPosition::Keyset(bytes) => {
                buf.push(0);
                write_len_prefixed(&mut buf, bytes)?;
            }
            CursorPosition::RowRange { start, end } => {
                buf.push(1);
                buf.extend_from_slice(&start.to_le_bytes());
                buf.extend_from_slice(&end.to_le_bytes());
            }
        }

        buf.extend_from_slice(&self.mint_ns.to_le_bytes());
        buf.extend_from_slice(&self.deadline_ns.to_le_bytes());

        let tag = mac(key, &buf);
        buf.extend_from_slice(&tag);
        Ok(URL_SAFE_NO_PAD.encode(buf))
    }

    /// Decode and MAC-verify a wire token. Does NOT check the tenant field
    /// against a caller's resolved tenant -- that is [`Cursor::redeem`]'s job.
    /// Every malformed, truncated, or tampered input is
    /// [`CursorError::Invalid`], never a panic.
    pub fn decode(token: &str, key: &CursorKey) -> Result<Cursor, CursorError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| CursorError::Invalid)?;
        if bytes.len() < MAC_LEN {
            return Err(CursorError::Invalid);
        }
        let split = bytes.len() - MAC_LEN;
        let (payload, stored) = bytes.split_at(split);
        if !ct_eq(&mac(key, payload), stored) {
            return Err(CursorError::Invalid);
        }

        let mut cur = ByteReader::new(payload);
        if cur.read_array::<4>()? != CURSOR_MAGIC {
            return Err(CursorError::Invalid);
        }
        if cur.read_u8()? != CURSOR_VERSION {
            return Err(CursorError::Invalid);
        }
        let tenant = TenantHash(cur.read_array::<16>()?);
        let tool = read_string(&mut cur)?;
        let argument_hash = cur.read_array::<32>()?;

        let seg_count = cur.read_u32()?;
        // Never pre-allocate from the untrusted count; push and grow.
        let mut segments = Vec::new();
        for _ in 0..seg_count {
            segments.push(cur.read_segment_pin()?);
        }

        let erasure_count = cur.read_u32()?;
        let mut pending_erasure = Vec::new();
        for _ in 0..erasure_count {
            let matcher_count = cur.read_u32()?;
            let mut matchers = Vec::new();
            for _ in 0..matcher_count {
                let k = read_string(&mut cur)?;
                let v = read_string(&mut cur)?;
                matchers.push((k, v));
            }
            let window_start_ns = i64::from_le_bytes(cur.read_array::<8>()?);
            let window_end_ns = i64::from_le_bytes(cur.read_array::<8>()?);
            pending_erasure.push(ErasurePredicate::new(
                matchers,
                window_start_ns,
                window_end_ns,
            ));
        }

        let declared_count = cur.read_u32()?;
        let mut declared_columns = Vec::new();
        for _ in 0..declared_count {
            let key = read_string(&mut cur)?;
            let ty = declared_type_from_tag(cur.read_u8()?)?;
            declared_columns.push(DeclaredColumn::new(key, ty));
        }

        let position = match cur.read_u8()? {
            0 => CursorPosition::Keyset(read_bytes_vec(&mut cur)?),
            1 => {
                let start = u64::from_le_bytes(cur.read_array::<8>()?);
                let end = u64::from_le_bytes(cur.read_array::<8>()?);
                CursorPosition::RowRange { start, end }
            }
            _ => return Err(CursorError::Invalid),
        };

        let mint_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        let deadline_ns = i64::from_le_bytes(cur.read_array::<8>()?);

        if !cur.is_empty() {
            return Err(CursorError::Invalid);
        }

        Ok(Cursor {
            tenant,
            tool,
            argument_hash,
            segments,
            pending_erasure,
            declared_columns,
            position,
            mint_ns,
            deadline_ns,
        })
    }

    /// Decode, verify the MAC, check the tenant against `caller_tenant`, and
    /// check the deadline against `now_ns` -- the full redemption sequence
    /// every call site MUST use instead of [`Cursor::decode`]. A tenant
    /// mismatch decodes as [`CursorError::Invalid`], identically to
    /// corruption (the D5 wrong-tenant rule): no separate signal is ever
    /// returned for "this cursor belongs to someone else."
    pub fn redeem(
        token: &str,
        key: &CursorKey,
        caller_tenant: TenantHash,
        now_ns: i64,
    ) -> Result<Cursor, CursorError> {
        let cursor = Self::decode(token, key)?;
        if cursor.tenant != caller_tenant {
            return Err(CursorError::Invalid);
        }
        if now_ns >= cursor.deadline_ns {
            return Err(CursorError::Expired);
        }
        Ok(cursor)
    }
}

impl EvidenceRef {
    /// See [`Cursor::encode`], including the [`CursorError::FieldTooLong`]
    /// condition on the tool name's length prefix.
    pub fn encode(&self, key: &CursorKey) -> Result<String, CursorError> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&EVIDENCE_MAGIC);
        buf.push(EVIDENCE_VERSION);
        buf.extend_from_slice(&self.tenant.0);
        write_len_prefixed(&mut buf, self.tool.as_bytes())?;
        buf.extend_from_slice(&self.sha256);
        buf.extend_from_slice(&self.mint_ns.to_le_bytes());
        buf.extend_from_slice(&self.deadline_ns.to_le_bytes());

        let tag = mac(key, &buf);
        buf.extend_from_slice(&tag);
        Ok(URL_SAFE_NO_PAD.encode(buf))
    }

    pub fn decode(token: &str, key: &CursorKey) -> Result<EvidenceRef, CursorError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| CursorError::Invalid)?;
        if bytes.len() < MAC_LEN {
            return Err(CursorError::Invalid);
        }
        let split = bytes.len() - MAC_LEN;
        let (payload, stored) = bytes.split_at(split);
        if !ct_eq(&mac(key, payload), stored) {
            return Err(CursorError::Invalid);
        }

        let mut cur = ByteReader::new(payload);
        if cur.read_array::<4>()? != EVIDENCE_MAGIC {
            return Err(CursorError::Invalid);
        }
        if cur.read_u8()? != EVIDENCE_VERSION {
            return Err(CursorError::Invalid);
        }
        let tenant = TenantHash(cur.read_array::<16>()?);
        let tool = read_string(&mut cur)?;
        let sha256 = cur.read_array::<32>()?;
        let mint_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        let deadline_ns = i64::from_le_bytes(cur.read_array::<8>()?);

        if !cur.is_empty() {
            return Err(CursorError::Invalid);
        }

        Ok(EvidenceRef {
            tenant,
            tool,
            sha256,
            mint_ns,
            deadline_ns,
        })
    }

    /// See [`Cursor::redeem`]: same wrong-tenant-decodes-as-Invalid rule.
    pub fn redeem(
        token: &str,
        key: &CursorKey,
        caller_tenant: TenantHash,
        now_ns: i64,
    ) -> Result<EvidenceRef, CursorError> {
        let evidence = Self::decode(token, key)?;
        if evidence.tenant != caller_tenant {
            return Err(CursorError::Invalid);
        }
        if now_ns >= evidence.deadline_ns {
            return Err(CursorError::Expired);
        }
        Ok(evidence)
    }
}

fn declared_type_tag(ty: DeclaredType) -> u8 {
    match ty {
        DeclaredType::Str => 1,
        DeclaredType::I64 => 2,
        DeclaredType::Bool => 3,
        DeclaredType::Bytes => 4,
    }
}

fn declared_type_from_tag(tag: u8) -> Result<DeclaredType, CursorError> {
    match tag {
        1 => Ok(DeclaredType::Str),
        2 => Ok(DeclaredType::I64),
        3 => Ok(DeclaredType::Bool),
        4 => Ok(DeclaredType::Bytes),
        _ => Err(CursorError::Invalid),
    }
}

/// Keyed BLAKE3-256 MAC, in the same construction as
/// `ravel_sql::flight_ticket`'s ticket MAC, but under this crate's own
/// process-local [`CursorKey`]: a cursor and a Flight ticket are separate
/// trust domains and are never verified with the same secret.
fn mac(key: &CursorKey, bytes: &[u8]) -> [u8; MAC_LEN] {
    *blake3::keyed_hash(key, bytes).as_bytes()
}

/// Constant-time byte-slice comparison, so verifying a MAC does not leak how
/// many leading bytes matched through a timing side channel.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Writes a count or length as the `u32` the wire layout reads it back as,
/// refusing rather than truncating one that does not fit.
fn write_count(buf: &mut Vec<u8>, len: usize) -> Result<(), CursorError> {
    let v = u32::try_from(len).map_err(|_| CursorError::FieldTooLong {
        len,
        max: u32::MAX as usize,
    })?;
    write_u32(buf, v);
    Ok(())
}

fn write_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CursorError> {
    write_count(buf, bytes.len())?;
    buf.extend_from_slice(bytes);
    Ok(())
}

fn read_string(cur: &mut ByteReader<'_>) -> Result<String, CursorError> {
    let bytes = read_bytes_vec(cur)?;
    String::from_utf8(bytes).map_err(|_| CursorError::Invalid)
}

fn read_bytes_vec(cur: &mut ByteReader<'_>) -> Result<Vec<u8>, CursorError> {
    let len = cur.read_u32()? as usize;
    Ok(cur.read_bytes(len)?.to_vec())
}

/// A bounds-checked forward reader over the MAC-verified payload. Every read
/// that would run past the end returns [`CursorError::Invalid`]. Named
/// distinctly from [`Cursor`] (the public D5 token type this module exports)
/// to avoid the name collision.
struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        ByteReader { buf, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos == self.buf.len()
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], CursorError> {
        let end = self.pos.checked_add(n).ok_or(CursorError::Invalid)?;
        let slice = self.buf.get(self.pos..end).ok_or(CursorError::Invalid)?;
        self.pos = end;
        Ok(slice)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CursorError> {
        let slice = self.read_bytes(N)?;
        slice.try_into().map_err(|_| CursorError::Invalid)
    }

    fn read_u8(&mut self) -> Result<u8, CursorError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CursorError> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    /// Reads one segment pin through `ravel_sql`'s own codec, which owns the
    /// layout, and advances by however many bytes it consumed. A typed
    /// `FlightTicketError` from there (truncation, bad UTF-8, an invalid
    /// level tag) becomes [`CursorError::Invalid`] like any other malformed
    /// input: a cursor exposes exactly the two D5 decode outcomes and never
    /// reports which field of which pin was wrong.
    fn read_segment_pin(&mut self) -> Result<SegmentPin, CursorError> {
        let rest = self.buf.get(self.pos..).ok_or(CursorError::Invalid)?;
        let (pin, consumed) = SegmentPin::decode_from(rest).map_err(|_| CursorError::Invalid)?;
        self.pos = self.pos.checked_add(consumed).ok_or(CursorError::Invalid)?;
        Ok(pin)
    }
}

impl From<FlightTicketError> for CursorError {
    /// The pin codec's encode-side length refusal is this codec's own; every
    /// other variant is a decode failure, which is [`CursorError::Invalid`].
    fn from(error: FlightTicketError) -> Self {
        match error {
            FlightTicketError::FieldTooLong(len) => CursorError::FieldTooLong {
                len,
                max: u32::MAX as usize,
            },
            _ => CursorError::Invalid,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_catalog::SegmentLevel;
    use uuid::Uuid;

    use super::*;

    fn test_key() -> CursorKey {
        [0x11u8; CURSOR_KEY_LEN]
    }

    /// A pin with every one of its 15 fields set to a distinct non-default
    /// value, so a field dropped anywhere in the cursor's codec changes the
    /// decoded struct rather than round-tripping a zero through a zero.
    fn every_field_pin() -> SegmentPin {
        SegmentPin {
            data_object_key: "t/aa/logs/l1/2026090812/w.7.9.deadbeef.rseg".to_owned(),
            object_size: 4_194_304,
            min_event_ts_ns: 1_700_000_000_111_111_111,
            max_event_ts_ns: 1_700_000_003_222_222_222,
            ingest_hour_bucket: 472_222,
            sample_count: 123_456,
            series_count: 789,
            shard: 5,
            content_hash: [0xC3u8; 32],
            writer_id: Uuid::from_bytes([0xD7u8; 16]),
            writer_epoch: 11,
            writer_seq: 22,
            created_unix_ns: 1_700_000_004_333_333_333,
            level: SegmentLevel::L1 {
                input_set_hash: [0xE5u8; 32],
                part_index: 3,
            },
            segment_format_version: 4,
        }
    }

    fn sample_cursor(tenant: TenantHash) -> Cursor {
        Cursor {
            tenant,
            tool: "ravel_search_logs".to_owned(),
            argument_hash: [9u8; 32],
            segments: vec![SegmentPin {
                data_object_key: "t/aa/logs/l0/0000/w.1.2.abc.rseg".to_owned(),
                object_size: 65_536,
                min_event_ts_ns: 1_700_000_000_000_000_000,
                max_event_ts_ns: 1_700_000_001_000_000_000,
                ingest_hour_bucket: 472_222,
                sample_count: 10,
                series_count: 4,
                shard: 0,
                content_hash: [3u8; 32],
                writer_id: Uuid::from_bytes([4u8; 16]),
                writer_epoch: 1,
                writer_seq: 2,
                created_unix_ns: 1_700_000_000_000_000_000,
                level: SegmentLevel::L0,
                segment_format_version: 3,
            }],
            pending_erasure: vec![ErasurePredicate::windowless(vec![(
                "region".to_owned(),
                "us-east".to_owned(),
            )])],
            declared_columns: vec![DeclaredColumn::new("http.status_code", DeclaredType::I64)],
            position: CursorPosition::Keyset(vec![1, 2, 3]),
            mint_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
        }
    }

    /// D5 wrong-tenant rule: a cursor minted for tenant A, redeemed by tenant
    /// B, decodes as `Invalid` -- the same error class as tampering, never a
    /// distinct "wrong tenant" signal. Two distinct tenant hashes are used
    /// (never the same literal twice), so this actually exercises a mismatch.
    #[test]
    fn cursor_minted_for_tenant_a_is_refused_for_tenant_b() {
        let tenant_a = TenantHash([0xAAu8; 16]);
        let tenant_b = TenantHash([0xBBu8; 16]);
        assert_ne!(tenant_a, tenant_b, "test must use two distinct tenants");

        let key = test_key();
        let token = sample_cursor(tenant_a).encode(&key).expect("encodes");

        let err = Cursor::redeem(&token, &key, tenant_b, 1_700_000_000_500_000_000)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// A single flipped byte in the MAC-verified region is a MAC mismatch,
    /// which decodes as `Invalid`.
    #[test]
    fn tampered_mac_is_cursor_invalid() {
        let tenant = TenantHash([0x42u8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = URL_SAFE_NO_PAD.decode(&token).expect("decode base64");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let tampered = URL_SAFE_NO_PAD.encode(bytes);

        let err = Cursor::redeem(&tampered, &key, tenant, 1_700_000_000_500_000_000)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// A structurally valid, correctly-tenanted cursor past its
    /// `deadline_ns` is `Expired`, distinct from `Invalid`.
    #[test]
    fn expired_cursor_is_cursor_expired() {
        let tenant = TenantHash([0x7Cu8; 16]);
        let key = test_key();
        let cursor = sample_cursor(tenant);
        let deadline = cursor.deadline_ns;
        let token = cursor.encode(&key).expect("encodes");

        let err = Cursor::redeem(&token, &key, tenant, deadline)
            .expect_err("must be expired at exactly the deadline");
        assert_eq!(err, CursorError::Expired);
    }

    /// The whole point of pinning `ravel_sql::SegmentPin` itself: every one
    /// of its 15 fields survives the cursor round trip. Asserted on the whole
    /// struct, so a field the codec forgets to write fails here whether or
    /// not this test is updated to name it.
    #[test]
    fn cursor_round_trips_every_segment_pin_field() {
        let tenant = TenantHash([0x5Eu8; 16]);
        let key = test_key();
        let pin = every_field_pin();
        let mut cursor = sample_cursor(tenant);
        cursor.segments = vec![pin.clone(), every_field_pin()];

        let token = cursor.encode(&key).expect("encodes");
        let decoded = Cursor::redeem(&token, &key, tenant, 1_700_000_000_500_000_000)
            .expect("round-trips through its own codec");

        assert_eq!(decoded.segments.len(), 2);
        assert_eq!(decoded.segments[0], pin);
        assert_eq!(decoded.segments[1], pin);
        assert_eq!(decoded, cursor);
    }
}

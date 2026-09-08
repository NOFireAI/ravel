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
//! # A token is bound to the call that minted it
//!
//! The tenant is not the only thing a token is scoped to. [`Cursor::redeem`]
//! also takes the redeeming call's tool name and argument hash, and refuses a
//! mismatch as [`CursorError::Invalid`] -- the same variant, and therefore the
//! same message, a forged token produces. Without that check the two fields
//! are decoration: a `ravel_search_logs` cursor could be handed to
//! `ravel_query_promql`, or the same cursor replayed against a different
//! predicate set, and page 2 would answer against page 1's pinned snapshot
//! under arguments that snapshot was never planned for.
//!
//! # A foreign token is `Expired`, a tampered one is `Invalid`
//!
//! Every token carries a process nonce in the clear, ahead of the MAC'd body:
//! [`process_nonce`] is a keyed BLAKE3 tag over a fixed context string, so it
//! commits to the process-local [`CursorKey`] without revealing it. Decode
//! compares it before it verifies the MAC, which is what separates the two
//! outcomes a client can actually act on: a token minted by a process that
//! has since restarted (a different key, hence a different nonce) is
//! [`CursorError::Expired`], the honest answer for a token whose pinned
//! snapshot no longer exists anywhere, while a token tampered with under this
//! process's own key stays [`CursorError::Invalid`]. Without the nonce both
//! collapse into `Invalid`, and a client cannot tell "restart, mint a fresh
//! page" from "this token is corrupt". The nonce is also inside the MAC'd
//! region, so swapping it cannot forge anything: the worst a caller can do by
//! editing it is force its own token to read as expired.
//!
//! # The wire length cap
//!
//! Both codecs refuse a token longer than [`MAX_TOKEN_BYTES`] before they
//! base64-decode it, with the typed [`CursorError::TokenTooLong`]. The cap is
//! not a third D5 decode outcome: it is a resource guard on the one input a
//! caller controls the size of, and it reports back only the length the caller
//! itself sent.
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
/// Version 2 pinned each segment as a whole `ravel_sql::SegmentPin` (all 15
/// fields, through that crate's own codec) instead of the five-field local
/// projection version 1 carried; version 3 adds the plaintext process nonce
/// after the version byte. A token of any earlier version decodes as
/// [`CursorError::Invalid`] like any other unsupported version, which is
/// correct and costs nothing: these tokens are process-local and
/// deadline-bounded, so none survives the deploy that changes the number.
const CURSOR_VERSION: u8 = 3;
const EVIDENCE_MAGIC: [u8; 4] = *b"RME1";
/// Version 2 adds the same plaintext process nonce [`CURSOR_VERSION`] 3 does.
const EVIDENCE_VERSION: u8 = 2;

/// Length in bytes of the plaintext process nonce both tokens carry directly
/// after their version byte.
const NONCE_LEN: usize = 8;

/// Bytes both codecs read in the clear before they verify anything: the
/// 4-byte magic, the version byte, and the process nonce.
const HEADER_LEN: usize = 4 + 1 + NONCE_LEN;

/// Domain-separation context for [`process_nonce`], so the nonce a token
/// carries in the clear can never collide with a MAC tag over token bytes.
const NONCE_CONTEXT: &[u8] = b"ravel-mcp cursor process nonce v1";

/// Largest wire token, in base64url characters, either codec will look at.
/// 1 MiB of base64url decodes to at most 786,432 payload bytes, which at the
/// ~250 B one segment pin costs is roughly 3,000 pinned segments: far above
/// any real snapshot, and a bound on what a caller can make this process
/// allocate from one token.
pub const MAX_TOKEN_BYTES: usize = 1024 * 1024;

/// Typed failure for both [`Cursor`] and [`EvidenceRef`]. The two decode
/// outcomes are the ones ADR-1374 D5 names, and they stay
/// undifferentiated on purpose: every malformed, truncated, tampered, or
/// wrong-tenant token is [`CursorError::Invalid`], so a caller cannot
/// distinguish "not yours" from "corrupt", and only a structurally valid,
/// correctly-tenanted token past its deadline is [`CursorError::Expired`].
/// Neither [`CursorError::FieldTooLong`] nor [`CursorError::TokenTooLong`] is
/// a third decode outcome: the first is a length that does not fit the wire
/// layout at all (a mint-time refusal), and the second is a resource guard
/// that reports back only the length the caller itself sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    /// The token is malformed, truncated, tampered with (MAC mismatch), an
    /// unsupported version, or minted for a different tenant, tool, or
    /// argument set than the redeeming call.
    #[error("cursor is invalid")]
    Invalid,
    /// The token is structurally valid and tenant-correct but its
    /// `deadline_ns` has passed, or it was minted by a process whose key no
    /// longer exists (a nonce mismatch), which puts its pinned snapshot just
    /// as far out of reach.
    #[error("cursor has expired")]
    Expired,
    /// The wire token is longer than [`MAX_TOKEN_BYTES`], refused before it is
    /// base64-decoded or allocated. Reports the length the caller sent, which
    /// the caller already knows.
    #[error("cursor token length {len} exceeds the {max}-byte cap")]
    TokenTooLong { len: usize, max: usize },
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
        buf.extend_from_slice(&process_nonce(key));
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

    /// Decode and MAC-verify a wire token. Does NOT check the tenant, tool, or
    /// argument hash against the redeeming call -- that is [`Cursor::redeem`]'s
    /// job. Every malformed, truncated, or tampered input is
    /// [`CursorError::Invalid`], never a panic; a token from another process is
    /// [`CursorError::Expired`], and one over [`MAX_TOKEN_BYTES`] is
    /// [`CursorError::TokenTooLong`].
    pub fn decode(token: &str, key: &CursorKey) -> Result<Cursor, CursorError> {
        let body = open_token(token, key, CURSOR_MAGIC, CURSOR_VERSION)?;

        let mut cur = ByteReader::new(&body);
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

    /// Decode, verify the MAC, bind the token to the redeeming call, and check
    /// the deadline against `now_ns` -- the full redemption sequence every
    /// call site MUST use instead of [`Cursor::decode`].
    ///
    /// The token must have been minted for `caller_tenant`, for `tool`, and
    /// for `argument_hash`. Any of those three mismatching is
    /// [`CursorError::Invalid`], identically to corruption (the D5
    /// wrong-tenant rule extended to the other two bindings): no separate
    /// signal is ever returned for "this cursor belongs to someone else", to
    /// another tool, or to another argument set.
    pub fn redeem(
        token: &str,
        key: &CursorKey,
        caller_tenant: TenantHash,
        tool: &str,
        argument_hash: &[u8; 32],
        now_ns: i64,
    ) -> Result<Cursor, CursorError> {
        let cursor = Self::decode(token, key)?;
        if cursor.tenant != caller_tenant {
            return Err(CursorError::Invalid);
        }
        if cursor.tool != tool {
            return Err(CursorError::Invalid);
        }
        if !ct_eq(&cursor.argument_hash, argument_hash) {
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
        buf.extend_from_slice(&process_nonce(key));
        buf.extend_from_slice(&self.tenant.0);
        write_len_prefixed(&mut buf, self.tool.as_bytes())?;
        buf.extend_from_slice(&self.sha256);
        buf.extend_from_slice(&self.mint_ns.to_le_bytes());
        buf.extend_from_slice(&self.deadline_ns.to_le_bytes());

        let tag = mac(key, &buf);
        buf.extend_from_slice(&tag);
        Ok(URL_SAFE_NO_PAD.encode(buf))
    }

    /// See [`Cursor::decode`], including the nonce and length-cap outcomes.
    pub fn decode(token: &str, key: &CursorKey) -> Result<EvidenceRef, CursorError> {
        let body = open_token(token, key, EVIDENCE_MAGIC, EVIDENCE_VERSION)?;

        let mut cur = ByteReader::new(&body);
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

    /// See [`Cursor::redeem`]: same wrong-tenant-decodes-as-Invalid rule, and
    /// the same binding to the redeeming `tool`. An evidence reference carries
    /// no argument hash (it pins one row, not a query), so the tool name is
    /// the whole call binding here.
    pub fn redeem(
        token: &str,
        key: &CursorKey,
        caller_tenant: TenantHash,
        tool: &str,
        now_ns: i64,
    ) -> Result<EvidenceRef, CursorError> {
        let evidence = Self::decode(token, key)?;
        if evidence.tenant != caller_tenant {
            return Err(CursorError::Invalid);
        }
        if evidence.tool != tool {
            return Err(CursorError::Invalid);
        }
        if now_ns >= evidence.deadline_ns {
            return Err(CursorError::Expired);
        }
        Ok(evidence)
    }
}

/// Checks the length cap, base64-decodes, checks magic and version, compares
/// the plaintext process nonce, verifies the MAC, and returns the body after
/// the header.
///
/// The nonce comparison sits before the MAC check on purpose: a token minted
/// under another process's key fails both, and the nonce is what lets this
/// report [`CursorError::Expired`] (the snapshot is gone with the process that
/// pinned it) rather than the [`CursorError::Invalid`] a tampered token gets.
/// The MAC still covers the nonce, so editing it forges nothing.
fn open_token(
    token: &str,
    key: &CursorKey,
    magic: [u8; 4],
    version: u8,
) -> Result<Vec<u8>, CursorError> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(CursorError::TokenTooLong {
            len: token.len(),
            max: MAX_TOKEN_BYTES,
        });
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| CursorError::Invalid)?;
    if bytes.len() < HEADER_LEN + MAC_LEN {
        return Err(CursorError::Invalid);
    }

    let mut head = ByteReader::new(&bytes);
    if head.read_array::<4>()? != magic {
        return Err(CursorError::Invalid);
    }
    if head.read_u8()? != version {
        return Err(CursorError::Invalid);
    }
    if head.read_array::<NONCE_LEN>()? != process_nonce(key) {
        return Err(CursorError::Expired);
    }

    let split = bytes.len().saturating_sub(MAC_LEN);
    let (payload, stored) = bytes.split_at(split);
    if !ct_eq(&mac(key, payload), stored) {
        return Err(CursorError::Invalid);
    }
    Ok(payload
        .get(HEADER_LEN..)
        .ok_or(CursorError::Invalid)?
        .to_vec())
}

/// The plaintext nonce every token carries: a keyed BLAKE3 tag over a fixed
/// context string.
///
/// It is a commitment to the process-local [`CursorKey`], not a secret and not
/// a second MAC. Deriving it from the key rather than from an entropy source
/// is what makes it a process nonce with no clock and no randomness in library
/// code: the key is minted once per process, so a restart changes the nonce,
/// and BLAKE3's keyed hash is a PRF, so publishing this tag reveals nothing
/// about the key.
fn process_nonce(key: &CursorKey) -> [u8; NONCE_LEN] {
    let tag = blake3::keyed_hash(key, NONCE_CONTEXT);
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&tag.as_bytes()[..NONCE_LEN]);
    nonce
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

    /// The tool and argument hash every sample token below is minted for, and
    /// a time inside its deadline. Redemption binds all three, so the tests
    /// name them once instead of repeating literals that must agree.
    const SAMPLE_TOOL: &str = "ravel_search_logs";
    const SAMPLE_ARGS: [u8; 32] = [9u8; 32];
    const NOW_NS: i64 = 1_700_000_000_500_000_000;

    fn test_key() -> CursorKey {
        [0x11u8; CURSOR_KEY_LEN]
    }

    /// Re-signs mutated token bytes with `key`, so the decode failure a test
    /// asserts on is attributable to the field it edited rather than to the
    /// MAC check that would otherwise fire first. The nonce is unchanged, so
    /// the re-minted token is one this process would accept but for the edit.
    fn remint(mut bytes: Vec<u8>, key: &CursorKey) -> String {
        let split = bytes.len().saturating_sub(MAC_LEN);
        bytes.truncate(split);
        let tag = mac(key, &bytes);
        bytes.extend_from_slice(&tag);
        URL_SAFE_NO_PAD.encode(bytes)
    }

    fn token_bytes(token: &str) -> Vec<u8> {
        URL_SAFE_NO_PAD.decode(token).expect("decode base64")
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

    fn sample_evidence(tenant: TenantHash) -> EvidenceRef {
        EvidenceRef {
            tenant,
            tool: SAMPLE_TOOL.to_owned(),
            sha256: [0x8Au8; 32],
            mint_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
        }
    }

    fn sample_cursor(tenant: TenantHash) -> Cursor {
        Cursor {
            tenant,
            tool: SAMPLE_TOOL.to_owned(),
            argument_hash: SAMPLE_ARGS,
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

        let err = Cursor::redeem(&token, &key, tenant_b, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
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

        let mut bytes = token_bytes(&token);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let tampered = URL_SAFE_NO_PAD.encode(bytes);

        let err = Cursor::redeem(&tampered, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The MAC covers the payload, not just the tag: a byte flipped INSIDE the
    /// MAC'd region is `Invalid` too. Flipping only the tag would pass on a
    /// codec that MAC'd nothing but itself.
    ///
    /// The byte edited belongs to a pinned segment's object key, which is the
    /// one thing a forger would actually want to change and which nothing
    /// downstream re-checks: editing the tenant, tool, or argument hash would
    /// be refused a second time by the bindings in `redeem`, and the assertion
    /// would hold even with no MAC at all.
    #[test]
    fn tampered_payload_byte_is_cursor_invalid() {
        let tenant = TenantHash([0x42u8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        let needle = b"t/aa/logs/l0/0000/w.1.2.abc.rseg";
        let at = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("the pinned object key is in the payload");
        assert!(
            at > HEADER_LEN && at < bytes.len() - MAC_LEN,
            "must edit inside the MAC'd payload"
        );
        bytes[at] ^= 0x20;
        let tampered = URL_SAFE_NO_PAD.encode(bytes);

        let err = Cursor::redeem(&tampered, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
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

        let err = Cursor::redeem(&token, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, deadline)
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
        let decoded = Cursor::redeem(&token, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect("round-trips through its own codec");

        assert_eq!(decoded.segments.len(), 2);
        assert_eq!(decoded.segments[0], pin);
        assert_eq!(decoded.segments[1], pin);
        assert_eq!(decoded, cursor);
    }

    /// A cursor is bound to the tool that minted it: handing a
    /// `ravel_search_logs` cursor to `ravel_query_promql` is `Invalid`, the
    /// same variant (and therefore the same message) a forged token gets.
    #[test]
    fn cursor_redeemed_by_another_tool_is_refused() {
        let tenant = TenantHash([0x3Au8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let err = Cursor::redeem(
            &token,
            &key,
            tenant,
            "ravel_query_promql",
            &SAMPLE_ARGS,
            NOW_NS,
        )
        .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
        assert_eq!(err.to_string(), CursorError::Invalid.to_string());
    }

    /// And to the arguments that minted it: replaying page 1's cursor under a
    /// different predicate set would answer the new question against the old
    /// question's pinned snapshot. One flipped bit in the hash is enough.
    #[test]
    fn cursor_redeemed_with_other_arguments_is_refused() {
        let tenant = TenantHash([0x3Bu8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut other = SAMPLE_ARGS;
        other[31] ^= 0x01;
        let err = Cursor::redeem(&token, &key, tenant, SAMPLE_TOOL, &other, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// A token minted under another process's key carries that process's
    /// nonce, which decode compares before the MAC: the snapshot it pins died
    /// with the process, so this is `Expired` and not `Invalid`. A client can
    /// act on that (mint a fresh page) where `Invalid` tells it nothing.
    #[test]
    fn token_minted_by_another_process_is_cursor_expired() {
        let tenant = TenantHash([0x6Du8; 16]);
        let minting_key = test_key();
        let redeeming_key: CursorKey = [0x22u8; CURSOR_KEY_LEN];
        assert_ne!(
            process_nonce(&minting_key),
            process_nonce(&redeeming_key),
            "two keys must produce two nonces"
        );

        let token = sample_cursor(tenant).encode(&minting_key).expect("encodes");
        let err = Cursor::redeem(
            &token,
            &redeeming_key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
        )
        .expect_err("must be refused");
        assert_eq!(err, CursorError::Expired);

        let evidence = sample_evidence(tenant)
            .encode(&minting_key)
            .expect("encodes");
        let err = EvidenceRef::redeem(&evidence, &redeeming_key, tenant, SAMPLE_TOOL, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Expired);
    }

    /// The length cap is checked before the base64 decode, so an oversized
    /// token is refused without allocating its payload. Exactly at the cap is
    /// not refused by the cap (it fails later, as a malformed token), which is
    /// what pins the comparison as `>` and not `>=`.
    #[test]
    fn oversized_token_is_refused_before_it_is_decoded() {
        let tenant = TenantHash([0x1Fu8; 16]);
        let key = test_key();

        let over = "A".repeat(MAX_TOKEN_BYTES + 1);
        let err = Cursor::redeem(&over, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(
            err,
            CursorError::TokenTooLong {
                len: MAX_TOKEN_BYTES + 1,
                max: MAX_TOKEN_BYTES,
            }
        );
        assert_eq!(MAX_TOKEN_BYTES, 1024 * 1024);

        let at_cap = "A".repeat(MAX_TOKEN_BYTES);
        let err = Cursor::redeem(&at_cap, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// Truncation at any point is `Invalid`, never a panic and never a partial
    /// decode: below the fixed header plus tag, and at half a real token. So
    /// is the other direction, a token carrying bytes past the last field it
    /// declares, which is re-MAC'd here so the trailing-bytes check is what
    /// refuses it rather than the MAC.
    #[test]
    fn truncated_or_extended_token_is_cursor_invalid() {
        let tenant = TenantHash([0x2Eu8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");
        let bytes = token_bytes(&token);

        for cut in [
            0usize,
            HEADER_LEN,
            HEADER_LEN + MAC_LEN - 1,
            bytes.len() / 2,
            bytes.len() - 1,
        ] {
            let mut truncated = bytes.clone();
            truncated.truncate(cut);
            let token = URL_SAFE_NO_PAD.encode(truncated);
            let err = Cursor::redeem(&token, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
                .expect_err("a truncated token must be refused");
            assert_eq!(err, CursorError::Invalid, "cut at {cut}");
        }

        let mut extended = bytes.clone();
        let tag_at = extended.len() - MAC_LEN;
        extended.insert(tag_at, 0x00);
        let reminted = remint(extended, &key);
        let err = Cursor::redeem(&reminted, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect_err("a token with trailing bytes must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The version byte is checked, so a token in the version 2 layout (a
    /// whole-`SegmentPin` cursor without the process nonce) is `Invalid`
    /// rather than parsed as if the nonce were tenant bytes. Re-MAC'd, so the
    /// failure is the version check and not the MAC.
    #[test]
    fn wrong_version_token_is_cursor_invalid() {
        let tenant = TenantHash([0x4Du8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        bytes[4] = CURSOR_VERSION - 1;
        let reminted = remint(bytes, &key);

        let err = Cursor::redeem(&reminted, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The magic is checked, so an evidence reference cannot be redeemed as a
    /// cursor even under this process's own key.
    #[test]
    fn wrong_magic_token_is_cursor_invalid() {
        let tenant = TenantHash([0x5Du8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        bytes[..4].copy_from_slice(&EVIDENCE_MAGIC);
        let reminted = remint(bytes, &key);

        let err = Cursor::redeem(&reminted, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// A string field whose bytes are not UTF-8 is `Invalid`, not a lossy
    /// decode and not a panic. 0xFF is never a valid UTF-8 leading byte.
    ///
    /// The field edited is a declared column key, not the tool name: a lossy
    /// decode of the tool name would be caught a second time by the tool
    /// binding in `redeem`, and the assertion would hold for the wrong reason.
    /// Nothing downstream re-checks a column key.
    #[test]
    fn non_utf8_string_field_is_cursor_invalid() {
        let tenant = TenantHash([0x7Eu8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        let needle = b"http.status_code";
        let at = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("the declared column key is in the payload");
        bytes[at] = 0xFF;
        let reminted = remint(bytes, &key);

        let err = Cursor::redeem(&reminted, &key, tenant, SAMPLE_TOOL, &SAMPLE_ARGS, NOW_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The evidence reference round-trips every field, and carries the same
    /// bindings a cursor does: wrong tenant and wrong tool are `Invalid`, and
    /// its own deadline is `Expired`.
    #[test]
    fn evidence_ref_round_trips_and_binds_its_call() {
        let tenant = TenantHash([0x9Cu8; 16]);
        let other = TenantHash([0x9Du8; 16]);
        let key = test_key();
        let evidence = sample_evidence(tenant);
        let deadline = evidence.deadline_ns;
        let token = evidence.encode(&key).expect("encodes");

        let decoded = EvidenceRef::redeem(&token, &key, tenant, SAMPLE_TOOL, NOW_NS)
            .expect("round-trips through its own codec");
        assert_eq!(decoded, evidence);
        assert_eq!(decoded.sha256, [0x8Au8; 32]);
        assert_eq!(decoded.tool, SAMPLE_TOOL);
        assert_eq!(decoded.mint_ns, 1_700_000_000_000_000_000);
        assert_eq!(decoded.deadline_ns, deadline);

        let err = EvidenceRef::redeem(&token, &key, other, SAMPLE_TOOL, NOW_NS)
            .expect_err("wrong tenant must be refused");
        assert_eq!(err, CursorError::Invalid);

        let err = EvidenceRef::redeem(&token, &key, tenant, "ravel_get_trace", NOW_NS)
            .expect_err("wrong tool must be refused");
        assert_eq!(err, CursorError::Invalid);

        let err = EvidenceRef::redeem(&token, &key, tenant, SAMPLE_TOOL, deadline)
            .expect_err("must be expired at exactly the deadline");
        assert_eq!(err, CursorError::Expired);
    }
}

//! The D5 cursor and evidence-reference codec (ADR-1374).
//!
//! [`Cursor`] and [`EvidenceRef`] are opaque, MAC'd, self-describing wire
//! tokens in the same pattern as `ravel_sql::flight_ticket`'s Flight-ticket
//! codec: magic, version byte, payload, trailing keyed BLAKE3-256 MAC,
//! base64url on the wire. A cursor pins the snapshot a paginated tool call
//! must keep reading against, as the inputs that resolve it rather than as
//! its result, plus the caller's position in the result order, so page 2 of
//! a query answers against the same snapshot page 1 planned against. An
//! evidence reference pins a single row for later recall by a
//! `ravel_get_trace`-style follow-up.
//!
//! # The pinned snapshot is a set of resolve inputs
//!
//! A cursor carries the signal, the half-open time range, the minimum
//! commit-token watermark the page was resolved against, the pending erasure
//! predicates in force, and the declared column set (the 2026-09-09 amendment
//! to ADR-1374 D5). Redemption re-resolves the snapshot from those inputs
//! deterministically; when the re-resolve cannot reproduce the pinned
//! watermark, the cursor is [`CursorError::Expired`] and the caller re-runs
//! the query.
//!
//! Enumerating the pinned segments instead is what the amendment replaced. One
//! `ravel_sql::SegmentPin` costs about 250 B plus base64, so the 2,000-segment
//! admission ceiling of D6 mints a token of several hundred KiB: past the
//! cursor's own 4 KiB bound in the envelope, and past the whole 256 KiB
//! response floor. Evidence references are unchanged and keep that per-pin
//! codec, which is why this module still converts a `FlightTicketError`.
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
//! # An evidence reference past its pin redeems as unpinned
//!
//! A cursor whose pin is gone has nothing left to offer: page 2 of a
//! paginated result means something only against page 1's snapshot, so
//! [`Cursor::redeem`] reports [`CursorError::Expired`] for a passed deadline
//! or a foreign nonce. An evidence reference is different. It names one row,
//! and every field a fresh re-execution needs (the tenant, the tool, the
//! argument hash, and the row digest to compare the re-read row against) is
//! in the token itself. [`EvidenceRef::redeem`] therefore answers
//! [`Redeemed::Unpinned`] instead of an error once the deadline has passed or
//! the minting process is gone, and the caller re-runs the reference's own
//! call and reports whether the digest still matches.
//!
//! [`Redeemed::Unpinned`] is the one outcome that does not rest on this
//! process's MAC. A token minted under another process's key cannot verify
//! under this one's, so its body is parsed unverified. That is sound because
//! nothing pinned is being reused: the tenant is still compared against the
//! authenticated caller's, and every other field the outcome carries is one
//! the caller could have passed in directly, so a forged unpinned reference
//! buys a caller nothing it could not ask for outright. Every malformed body
//! is still [`CursorError::Invalid`]. [`Redeemed::Pinned`], the outcome that
//! does reuse the pin, is returned only for a token carrying this process's
//! nonce whose MAC verified under this process's key.
//!
//! # The wire length cap
//!
//! Both codecs refuse a token longer than [`MAX_TOKEN_BYTES`] before they
//! base64-decode it, with the typed [`CursorError::TokenTooLong`]. The cap is
//! not a third D5 decode outcome: it is a resource guard on the one input a
//! caller controls the size of, and it reports back only the length the caller
//! itself sent.
//!
//! # The digest is BLAKE3-256, and the field says so
//!
//! ADR-1374's evidence-reference shape names a `sha256` field. This crate has
//! no `sha2` dependency (only `rmcp` is an authorized new dependency for this
//! task; the workspace-root edit scope is the `ravel-mcp` member plus the
//! `rmcp` workspace dependency only), and `blake3` is already the codebase's
//! universal hashing and MAC primitive (see `ravel_sql::flight_ticket`). So
//! [`EvidenceRef`] hashes the referenced row with BLAKE3-256, and both the
//! field here and the `evidence[].blake3_256` field of the D4 envelope carry
//! that name: a field called `sha256` holding a BLAKE3 digest cannot be
//! verified by a client that believes the name, which is worse than a name
//! the ADR did not anticipate. The 2026-09-09 amendment to
//! docs/adrs/1374-agent-mcp-server.md records the rename.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ravel_query::erasure::ErasurePredicate;
use ravel_sql::{DeclaredColumn, DeclaredType, FlightTicketError};
use ravel_types::{CommitToken, Signal, TenantHash};

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
///
/// The field is private and [`CursorKey::from_process_secret`] is the only
/// constructor, so the contract that a key is process-local is stated at the
/// one place a key can come into existence.
#[derive(Clone)]
pub struct CursorKey([u8; CURSOR_KEY_LEN]);

impl CursorKey {
    /// Wrap the process's own freshly generated key bytes.
    ///
    /// The adapter generates `bytes` at process start from the OS entropy
    /// source and never derives them from a shared, configured, or persisted
    /// secret. A derived key would be the same key in two processes, so a
    /// cursor minted by a process that has since died would carry this
    /// process's nonce and verify under this process's key: it would decode as
    /// live and be read against a pinned snapshot no live process is holding
    /// open, which is exactly what ADR-1374 D5 makes the process nonce
    /// prevent.
    pub fn from_process_secret(bytes: [u8; CURSOR_KEY_LEN]) -> CursorKey {
        CursorKey(bytes)
    }
}

/// Redacted: a key must not reach a log line or an error body through a
/// derived `Debug`.
impl std::fmt::Debug for CursorKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CursorKey(<redacted>)")
    }
}

const CURSOR_MAGIC: [u8; 4] = *b"RMC1";
/// Version 2 pinned each segment as a whole `ravel_sql::SegmentPin` (all 15
/// fields, through that crate's own codec) instead of the five-field local
/// projection version 1 carried; version 3 added the plaintext process nonce
/// after the version byte; version 4 drops the segment enumeration for the
/// resolve inputs of the 2026-09-09 amendment (signal, time range, commit-token
/// watermark, pending erasure, declared columns, keyset position). A token of
/// any earlier version decodes as [`CursorError::Invalid`] like any other
/// unsupported version, which is correct and costs nothing: these tokens are
/// process-local and deadline-bounded, so none survives the deploy that
/// changes the number.
const CURSOR_VERSION: u8 = 4;
const EVIDENCE_MAGIC: [u8; 4] = *b"RME1";
/// Version 2 adds the same plaintext process nonce [`CURSOR_VERSION`] 3 does;
/// version 3 adds the argument hash, which an unpinned redemption needs to
/// re-execute the call the reference was minted by.
const EVIDENCE_VERSION: u8 = 3;

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
/// 1 MiB of base64url decodes to at most 786,432 payload bytes, far above
/// what any real cursor or evidence reference carries, and a bound on what a
/// caller can make this process allocate from one token. It is not the bound
/// a minted cursor is held to: the envelope bounds `presentation.cursor` at
/// 4 KiB and reports a larger one as an `internal` failure.
pub const MAX_TOKEN_BYTES: usize = 1024 * 1024;

/// The `grace` component of a deployment's GC protection horizon, 24 h
/// (`ravel_catalog::DEFAULT_PROTECTION_HORIZON_NS` is `max_query_duration` 1 h
/// + `grace` 24 h + `clock_skew_allowance` 5 m).
///
/// A redemption never spends it: the grace exists so a sweep that observes a
/// stale frontier still cannot delete a segment a running query holds, not to
/// extend how long a token may name that segment. Subtracting it leaves
/// `max_query_duration + clock_skew_allowance`, which is the part of the
/// window a redemption starting now may still use.
pub const GRACE_NS: i64 = 24 * 3_600 * 1_000_000_000;

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
    /// as far out of reach. Only cursors report this: an evidence reference
    /// in either state redeems as [`Redeemed::Unpinned`] instead.
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

/// Where a paginated result left off.
///
/// A keyset position is the last tuple of the page (opaque here, encoded by
/// the tool that ordered the rows) together with the `ORDER BY` it was taken
/// under, which the amendment names as part of the position: the same tuple
/// under a different ordering resumes somewhere else entirely, so the two
/// travel as one value rather than as a tuple the redeeming call is trusted
/// to pair correctly. A row range is used by tools that page by row index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorPosition {
    Keyset {
        /// The last tuple of the page, in the tool's own encoding.
        tuple: Vec<u8>,
        /// The `ORDER BY` terms the tuple was taken under, in order.
        order_by: Vec<String>,
    },
    RowRange {
        start: u64,
        end: u64,
    },
}

/// The D5 cursor: an opaque, snapshot-pinning pagination token.
///
/// The pin is the resolve inputs below, not an enumeration of what they
/// resolved to (the 2026-09-09 amendment to ADR-1374 D5). See this module's
/// "The pinned snapshot is a set of resolve inputs" docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub tenant: TenantHash,
    pub tool: String,
    /// BLAKE3-256 hash of the tool's canonicalized argument set at mint time,
    /// so a cursor cannot be redeemed against a call with different
    /// arguments than the one that minted it.
    pub argument_hash: [u8; 32],
    /// The signal the page was resolved over.
    pub signal: Signal,
    /// Start of the half-open time range the page was resolved over,
    /// inclusive.
    pub range_start_ns: i64,
    /// End of that range, exclusive.
    pub range_end_ns: i64,
    /// The minimum commit-token watermark the page was resolved against, one
    /// token per shard the read-your-write lower bound covers.
    ///
    /// This is what makes the re-resolve deterministic rather than a fresh
    /// resolution: page 2 resolves against this same watermark, and a
    /// re-resolve that cannot reproduce it is [`CursorError::Expired`]
    /// (see [`Cursor::redeem`]).
    pub min_commit_watermark: Vec<CommitToken>,
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
    /// BLAKE3-256 hash of the tool's canonicalized argument set at mint time.
    /// Unlike a cursor's, this is not bound against a redeeming call's own
    /// hash: an evidence reference is redeemed by re-executing the call it
    /// was minted by, so the hash is what a redeemer needs back, not
    /// something it supplies.
    pub argument_hash: [u8; 32],
    /// BLAKE3-256 hash of the referenced row, the same digest the envelope's
    /// `evidence[].blake3_256` field carries in hex. See this module's "The
    /// digest is BLAKE3-256, and the field says so" docs.
    pub blake3_256: [u8; 32],
    pub mint_ns: i64,
    pub deadline_ns: i64,
}

/// The outcome of [`EvidenceRef::redeem`].
///
/// Two states, not an error and a success: an evidence reference past its pin
/// is still usable, just at a higher cost. See this module's "An evidence
/// reference past its pin redeems as unpinned" docs for why, and for why
/// [`Redeemed::Unpinned`] is not a MAC-authenticated outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Redeemed {
    /// The pin is live: the reference was minted by this process, its MAC
    /// verified, and its deadline has not passed. The pinned row can be read
    /// back without re-executing anything.
    Pinned(EvidenceRef),
    /// The pin is gone -- the deadline has passed, or the minting process is
    /// gone (its nonce is foreign). These are the fields a fresh re-execution of
    /// the reference's own call needs; the redeemer re-runs it and reports
    /// whether the row it reads still hashes to `digest`.
    Unpinned {
        /// BLAKE3-256 hash of the row as it was when the reference was
        /// minted, to compare the re-read row against.
        digest: [u8; 32],
        tool: String,
        argument_hash: [u8; 32],
        /// Always equal to the redeeming caller's tenant: an unpinned
        /// outcome is returned only after the tenant check passes.
        tenant: TenantHash,
    },
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

        buf.push(signal_tag(self.signal));
        buf.extend_from_slice(&self.range_start_ns.to_le_bytes());
        buf.extend_from_slice(&self.range_end_ns.to_le_bytes());

        write_count(&mut buf, self.min_commit_watermark.len())?;
        for token in &self.min_commit_watermark {
            // The token's own codec, not a local field-by-field mirror: a
            // commit token is a frozen contract (ADR-0010) and a cursor is
            // not a second place to define its layout.
            write_len_prefixed(&mut buf, token.encode().as_bytes())?;
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
            CursorPosition::Keyset { tuple, order_by } => {
                buf.push(0);
                write_len_prefixed(&mut buf, tuple)?;
                write_count(&mut buf, order_by.len())?;
                for term in order_by {
                    write_len_prefixed(&mut buf, term.as_bytes())?;
                }
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
        let body = match open_token(token, key, CURSOR_MAGIC, CURSOR_VERSION)? {
            Opened::Local(body) => body,
            // A cursor's whole value is its pin, and the process that pinned
            // it is gone; there is nothing to answer with but `Expired`.
            Opened::Foreign(_) => return Err(CursorError::Expired),
        };

        let mut cur = ByteReader::new(&body);
        let tenant = TenantHash(cur.read_array::<16>()?);
        let tool = read_string(&mut cur)?;
        let argument_hash = cur.read_array::<32>()?;

        let signal = signal_from_tag(cur.read_u8()?)?;
        let range_start_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        let range_end_ns = i64::from_le_bytes(cur.read_array::<8>()?);

        let watermark_count = cur.read_u32()?;
        // Never pre-allocate from the untrusted count; push and grow.
        let mut min_commit_watermark = Vec::new();
        for _ in 0..watermark_count {
            let encoded = read_string(&mut cur)?;
            min_commit_watermark
                .push(CommitToken::decode(&encoded).map_err(|_| CursorError::Invalid)?);
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
            0 => {
                let tuple = read_bytes_vec(&mut cur)?;
                let term_count = cur.read_u32()?;
                let mut order_by = Vec::new();
                for _ in 0..term_count {
                    order_by.push(read_string(&mut cur)?);
                }
                CursorPosition::Keyset { tuple, order_by }
            }
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
            signal,
            range_start_ns,
            range_end_ns,
            min_commit_watermark,
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
    ///
    /// The deadline checked is [`effective_deadline_ns`] of the embedded one
    /// and `protection_horizon_ns`, not the embedded one alone, and the
    /// returned cursor carries that clamped value in `deadline_ns` so a caller
    /// that bounds its own work by the field cannot read past the pin's
    /// protection either.
    pub fn redeem(
        token: &str,
        key: &CursorKey,
        caller_tenant: TenantHash,
        tool: &str,
        argument_hash: &[u8; 32],
        now_ns: i64,
        protection_horizon_ns: i64,
    ) -> Result<Cursor, CursorError> {
        let mut cursor = Self::decode(token, key)?;
        if cursor.tenant != caller_tenant {
            return Err(CursorError::Invalid);
        }
        if cursor.tool != tool {
            return Err(CursorError::Invalid);
        }
        if !ct_eq(&cursor.argument_hash, argument_hash) {
            return Err(CursorError::Invalid);
        }
        cursor.deadline_ns = effective_deadline_ns(cursor.deadline_ns, protection_horizon_ns);
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
        buf.extend_from_slice(&self.argument_hash);
        buf.extend_from_slice(&self.blake3_256);
        buf.extend_from_slice(&self.mint_ns.to_le_bytes());
        buf.extend_from_slice(&self.deadline_ns.to_le_bytes());

        let tag = mac(key, &buf);
        buf.extend_from_slice(&tag);
        Ok(URL_SAFE_NO_PAD.encode(buf))
    }

    /// See [`Cursor::decode`], including the nonce and length-cap outcomes: a
    /// token from another process is [`CursorError::Expired`] here too.
    /// [`EvidenceRef::redeem`] is the entry point that reads such a token as
    /// [`Redeemed::Unpinned`] instead.
    pub fn decode(token: &str, key: &CursorKey) -> Result<EvidenceRef, CursorError> {
        match open_token(token, key, EVIDENCE_MAGIC, EVIDENCE_VERSION)? {
            Opened::Local(body) => Self::parse(&body),
            Opened::Foreign(_) => Err(CursorError::Expired),
        }
    }

    /// Parses the header-stripped body. Shared by [`EvidenceRef::decode`] and
    /// [`EvidenceRef::redeem`], which differ only in whether the bytes were
    /// MAC-verified first.
    fn parse(body: &[u8]) -> Result<EvidenceRef, CursorError> {
        let mut cur = ByteReader::new(body);
        let tenant = TenantHash(cur.read_array::<16>()?);
        let tool = read_string(&mut cur)?;
        let argument_hash = cur.read_array::<32>()?;
        let blake3_256 = cur.read_array::<32>()?;
        let mint_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        let deadline_ns = i64::from_le_bytes(cur.read_array::<8>()?);

        if !cur.is_empty() {
            return Err(CursorError::Invalid);
        }

        Ok(EvidenceRef {
            tenant,
            tool,
            argument_hash,
            blake3_256,
            mint_ns,
            deadline_ns,
        })
    }

    /// Redeem a reference: same wrong-tenant-decodes-as-Invalid rule as
    /// [`Cursor::redeem`], and the same binding to the redeeming `tool`.
    ///
    /// Unlike a cursor this never reports [`CursorError::Expired`]. A live
    /// pin answers [`Redeemed::Pinned`]; a passed deadline or a foreign
    /// process nonce answers [`Redeemed::Unpinned`] with the fields a fresh
    /// re-execution needs. The argument hash is returned rather than checked,
    /// since the redeemer re-runs the minting call rather than supplying its
    /// own arguments. Tampering under this process's own key, a wrong tenant,
    /// a wrong tool, and any malformed body are all still
    /// [`CursorError::Invalid`].
    ///
    /// The deadline is re-clamped exactly as in [`Cursor::redeem`]: a
    /// reference whose embedded deadline outlives `protection_horizon_ns`
    /// minus [`GRACE_NS`] redeems as [`Redeemed::Unpinned`], since the pin it
    /// names is no longer protected even though the token itself is intact.
    pub fn redeem(
        token: &str,
        key: &CursorKey,
        caller_tenant: TenantHash,
        tool: &str,
        now_ns: i64,
        protection_horizon_ns: i64,
    ) -> Result<Redeemed, CursorError> {
        let (mut evidence, pin_verified) =
            match open_token(token, key, EVIDENCE_MAGIC, EVIDENCE_VERSION)? {
                Opened::Local(body) => (Self::parse(&body)?, true),
                Opened::Foreign(body) => (Self::parse(&body)?, false),
            };
        if evidence.tenant != caller_tenant {
            return Err(CursorError::Invalid);
        }
        if evidence.tool != tool {
            return Err(CursorError::Invalid);
        }
        evidence.deadline_ns = effective_deadline_ns(evidence.deadline_ns, protection_horizon_ns);
        if pin_verified && now_ns < evidence.deadline_ns {
            return Ok(Redeemed::Pinned(evidence));
        }
        Ok(Redeemed::Unpinned {
            digest: evidence.blake3_256,
            tool: evidence.tool,
            argument_hash: evidence.argument_hash,
            tenant: evidence.tenant,
        })
    }
}

/// What [`open_token`] recovered: the header-stripped body, and whether the
/// token's process nonce was this process's.
enum Opened {
    /// This process's nonce, and the MAC verified under this process's key.
    Local(Vec<u8>),
    /// Another process's nonce. The body is NOT MAC-verified -- it cannot be,
    /// since the key that signed it is gone. Only an unpinned evidence
    /// redemption may read these bytes; see the module docs.
    Foreign(Vec<u8>),
}

/// Checks the length cap, base64-decodes, checks magic and version, compares
/// the plaintext process nonce, verifies the MAC when the nonce is this
/// process's, and returns the body after the header.
///
/// The nonce comparison sits before the MAC check on purpose: a token minted
/// under another process's key fails both, and the nonce is what separates
/// "the process that pinned this is gone" from the [`CursorError::Invalid`] a
/// tampered token gets. The MAC still covers the nonce, so editing it forges
/// nothing: the worst it can do is downgrade the holder's own token to
/// [`Opened::Foreign`], which no caller can pin against.
fn open_token(
    token: &str,
    key: &CursorKey,
    magic: [u8; 4],
    version: u8,
) -> Result<Opened, CursorError> {
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
    let foreign = head.read_array::<NONCE_LEN>()? != process_nonce(key);

    let split = bytes.len().saturating_sub(MAC_LEN);
    let (payload, stored) = bytes.split_at(split);
    if !foreign && !ct_eq(&mac(key, payload), stored) {
        return Err(CursorError::Invalid);
    }
    let body = payload
        .get(HEADER_LEN..)
        .ok_or(CursorError::Invalid)?
        .to_vec();
    if foreign {
        return Ok(Opened::Foreign(body));
    }
    Ok(Opened::Local(body))
}

/// The deadline a redemption may actually use: the embedded one, re-clamped
/// to the deployment's pin protection.
///
/// `protection_horizon_ns` is the absolute epoch-ns instant through which the
/// caller's deployment protects a pinned snapshot from the sweeper (the caller
/// computes it as `now + protection_horizon`, the same duration
/// `ravel_catalog::DEFAULT_PROTECTION_HORIZON_NS` names). This mirrors
/// `ravel_sql::FlightSqlConfig::clamp_ticket_deadline_ns`: an embedded
/// deadline may only shorten the effective one, never lengthen it, so a token
/// minted with an honest but over-long deadline (a misconfigured adapter, a
/// horizon shortened after the mint) cannot outlive the protection its pin
/// depends on.
fn effective_deadline_ns(embedded_deadline_ns: i64, protection_horizon_ns: i64) -> i64 {
    embedded_deadline_ns.min(protection_horizon_ns.saturating_sub(GRACE_NS))
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
    let tag = blake3::keyed_hash(&key.0, NONCE_CONTEXT);
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&tag.as_bytes()[..NONCE_LEN]);
    nonce
}

/// The wire tag for a signal. Written out here rather than taken from the
/// enum's discriminant: this is a token layout under [`CURSOR_VERSION`], so a
/// reordering of `ravel_types::Signal` must not silently change what a minted
/// token means.
fn signal_tag(signal: Signal) -> u8 {
    match signal {
        Signal::Metrics => 1,
        Signal::Logs => 2,
        Signal::Spans => 3,
        Signal::Profiles => 4,
        Signal::Alerts => 5,
        Signal::Audit => 6,
    }
}

fn signal_from_tag(tag: u8) -> Result<Signal, CursorError> {
    match tag {
        1 => Ok(Signal::Metrics),
        2 => Ok(Signal::Logs),
        3 => Ok(Signal::Spans),
        4 => Ok(Signal::Profiles),
        5 => Ok(Signal::Alerts),
        6 => Ok(Signal::Audit),
        _ => Err(CursorError::Invalid),
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
    *blake3::keyed_hash(&key.0, bytes).as_bytes()
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
    use uuid::Uuid;

    use super::*;

    /// The tool and argument hash every sample token below is minted for, and
    /// a time inside its deadline. Redemption binds all three, so the tests
    /// name them once instead of repeating literals that must agree.
    const SAMPLE_TOOL: &str = "ravel_search_logs";
    const SAMPLE_ARGS: [u8; 32] = [9u8; 32];
    const NOW_NS: i64 = 1_700_000_000_500_000_000;

    /// A protection horizon a year past every deadline below, so the
    /// re-clamp is inert in every test but the one that exercises it: those
    /// tests assert the embedded deadline's own behavior.
    const FAR_HORIZON_NS: i64 = NOW_NS + 365 * 24 * 3_600 * 1_000_000_000 + GRACE_NS;

    fn test_key() -> CursorKey {
        CursorKey::from_process_secret([0x11u8; CURSOR_KEY_LEN])
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

    fn sample_evidence(tenant: TenantHash) -> EvidenceRef {
        EvidenceRef {
            tenant,
            tool: SAMPLE_TOOL.to_owned(),
            argument_hash: SAMPLE_ARGS,
            blake3_256: [0x8Au8; 32],
            mint_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
        }
    }

    fn sample_cursor(tenant: TenantHash) -> Cursor {
        Cursor {
            tenant,
            tool: SAMPLE_TOOL.to_owned(),
            argument_hash: SAMPLE_ARGS,
            signal: Signal::Logs,
            range_start_ns: 1_699_999_000_000_000_000,
            range_end_ns: 1_700_000_000_000_000_000,
            min_commit_watermark: vec![sample_token(0)],
            pending_erasure: vec![ErasurePredicate::windowless(vec![(
                "region".to_owned(),
                "us-east".to_owned(),
            )])],
            declared_columns: vec![DeclaredColumn::new("http.status_code", DeclaredType::I64)],
            position: CursorPosition::Keyset {
                tuple: vec![1, 2, 3],
                order_by: vec!["timestamp desc".to_owned()],
            },
            mint_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
        }
    }

    fn sample_token(shard: u32) -> CommitToken {
        CommitToken {
            shard,
            writer_id: Uuid::from_bytes([4u8; 16]),
            epoch: 1,
            seq: 2,
            ingest_hour_bucket: 472_222,
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

        let err = Cursor::redeem(
            &token,
            &key,
            tenant_b,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
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

        let err = Cursor::redeem(
            &tampered,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The MAC covers the payload, not just the tag: a byte flipped INSIDE the
    /// MAC'd region is `Invalid` too. Flipping only the tag would pass on a
    /// codec that MAC'd nothing but itself.
    ///
    /// The byte edited belongs to a pending erasure predicate's matcher value,
    /// which is the kind of thing a forger would actually want to change and
    /// which nothing downstream re-checks: editing the tenant, tool, or
    /// argument hash would be refused a second time by the bindings in
    /// `redeem`, and the assertion would hold even with no MAC at all.
    #[test]
    fn tampered_payload_byte_is_cursor_invalid() {
        let tenant = TenantHash([0x42u8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        let needle = b"us-east";
        let at = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("the pending erasure matcher value is in the payload");
        assert!(
            at > HEADER_LEN && at < bytes.len() - MAC_LEN,
            "must edit inside the MAC'd payload"
        );
        bytes[at] ^= 0x20;
        let tampered = URL_SAFE_NO_PAD.encode(bytes);

        let err = Cursor::redeem(
            &tampered,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
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

        let err = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            deadline,
            FAR_HORIZON_NS,
        )
        .expect_err("must be expired at exactly the deadline");
        assert_eq!(err, CursorError::Expired);
    }

    /// Every resolve input the amendment names survives the round trip, each
    /// set to a distinct non-default value so a field the codec forgets to
    /// write changes the decoded struct rather than round-tripping a zero
    /// through a zero. Asserted on the whole struct, so a new field fails
    /// here whether or not this test is updated to name it.
    #[test]
    fn cursor_round_trips_every_resolve_input() {
        let tenant = TenantHash([0x5Eu8; 16]);
        let key = test_key();
        let cursor = Cursor {
            tenant,
            tool: SAMPLE_TOOL.to_owned(),
            argument_hash: SAMPLE_ARGS,
            signal: Signal::Audit,
            range_start_ns: 1_700_000_000_111_111_111,
            range_end_ns: 1_700_000_003_222_222_222,
            min_commit_watermark: vec![
                CommitToken {
                    shard: 5,
                    writer_id: Uuid::from_bytes([0xD7u8; 16]),
                    epoch: 11,
                    seq: 22,
                    ingest_hour_bucket: 472_222,
                },
                CommitToken {
                    shard: 6,
                    writer_id: Uuid::from_bytes([0xE5u8; 16]),
                    epoch: 33,
                    seq: 44,
                    ingest_hour_bucket: 472_223,
                },
            ],
            pending_erasure: vec![
                ErasurePredicate::new(
                    vec![("region".to_owned(), "us-east".to_owned())],
                    1_699_000_000_000_000_000,
                    1_699_500_000_000_000_000,
                ),
                ErasurePredicate::windowless(vec![
                    ("service.name".to_owned(), "checkout".to_owned()),
                    ("user.id".to_owned(), "u-42".to_owned()),
                ]),
            ],
            declared_columns: vec![
                DeclaredColumn::new("http.status_code", DeclaredType::I64),
                DeclaredColumn::new("http.route", DeclaredType::Str),
                DeclaredColumn::new("error", DeclaredType::Bool),
                DeclaredColumn::new("trace_id", DeclaredType::Bytes),
            ],
            position: CursorPosition::Keyset {
                tuple: vec![7, 8, 9, 10],
                order_by: vec!["timestamp desc".to_owned(), "trace_id asc".to_owned()],
            },
            mint_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
        };

        let token = cursor.encode(&key).expect("encodes");
        let decoded = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect("round-trips through its own codec");
        assert_eq!(decoded, cursor);

        // The other position arm, through the same codec.
        let mut ranged = cursor.clone();
        ranged.position = CursorPosition::RowRange {
            start: 4_000,
            end: 4_500,
        };
        let token = ranged.encode(&key).expect("encodes");
        let decoded = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect("round-trips through its own codec");
        assert_eq!(decoded, ranged);
    }

    /// The amendment changed what a cursor pins, so a token minted by the
    /// previous version is not readable under the new layout: its version byte
    /// is refused at the header, before any field is parsed. Reminted under
    /// this process's key so the refusal is attributable to the version byte
    /// rather than to the MAC.
    #[test]
    fn a_version_3_token_is_invalid() {
        let tenant = TenantHash([0x5Fu8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        assert_eq!(bytes[CURSOR_MAGIC.len()], CURSOR_VERSION);
        bytes[CURSOR_MAGIC.len()] = 3;
        let stale = remint(bytes, &key);

        let err = Cursor::decode(&stale, &key).expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
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
            FAR_HORIZON_NS,
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
        let err = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &other,
            NOW_NS,
            FAR_HORIZON_NS,
        )
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
        let redeeming_key = CursorKey::from_process_secret([0x22u8; CURSOR_KEY_LEN]);
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
            FAR_HORIZON_NS,
        )
        .expect_err("must be refused");
        assert_eq!(err, CursorError::Expired);

        // The evidence leg is the exception: a foreign reference is unpinned,
        // not refused. `NOW_NS` is inside its deadline, so the foreign nonce
        // is the only thing that can produce this outcome.
        let evidence = sample_evidence(tenant)
            .encode(&minting_key)
            .expect("encodes");
        let redeemed = EvidenceRef::redeem(
            &evidence,
            &redeeming_key,
            tenant,
            SAMPLE_TOOL,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect("a foreign reference redeems unpinned");
        assert_eq!(
            redeemed,
            Redeemed::Unpinned {
                digest: [0x8Au8; 32],
                tool: SAMPLE_TOOL.to_owned(),
                argument_hash: SAMPLE_ARGS,
                tenant,
            }
        );

        // The lower-level `decode` still reports the process as gone; only
        // `redeem` reads a foreign reference as unpinned.
        let err = EvidenceRef::decode(&evidence, &redeeming_key).expect_err("must be refused");
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
        let err = Cursor::redeem(
            &over,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
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
        let err = Cursor::redeem(
            &at_cap,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
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
            let err = Cursor::redeem(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                NOW_NS,
                FAR_HORIZON_NS,
            )
            .expect_err("a truncated token must be refused");
            assert_eq!(err, CursorError::Invalid, "cut at {cut}");
        }

        let mut extended = bytes.clone();
        let tag_at = extended.len() - MAC_LEN;
        extended.insert(tag_at, 0x00);
        let reminted = remint(extended, &key);
        let err = Cursor::redeem(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("a token with trailing bytes must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The version byte is checked against this build's own version, not for
    /// being no newer than it: a token claiming a future layout is `Invalid`
    /// rather than parsed with the fields this build happens to know.
    /// Re-MAC'd, so the failure is the version check and not the MAC.
    /// [`a_version_3_token_is_invalid`] covers the previous layout.
    #[test]
    fn wrong_version_token_is_cursor_invalid() {
        let tenant = TenantHash([0x4Du8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        bytes[4] = CURSOR_VERSION + 1;
        let reminted = remint(bytes, &key);

        let err = Cursor::redeem(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The version byte is checked for an evidence token the same way it is
    /// for a cursor: a token in the version 2 layout is `Invalid` rather than
    /// parsed as a later version. Re-MAC'd, so the failure is the version
    /// check and not the MAC.
    #[test]
    fn wrong_version_evidence_token_is_cursor_invalid() {
        let tenant = TenantHash([0x4Eu8; 16]);
        let key = test_key();
        let token = sample_evidence(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        bytes[4] = EVIDENCE_VERSION - 1;
        let reminted = remint(bytes, &key);

        let err = EvidenceRef::redeem(&reminted, &key, tenant, SAMPLE_TOOL, NOW_NS, FAR_HORIZON_NS)
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

        let err = Cursor::redeem(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
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

        let err = Cursor::redeem(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The evidence reference round-trips every field, and carries the same
    /// bindings a cursor does: wrong tenant and wrong tool are `Invalid`. Past
    /// its deadline it is not an error at all but `Unpinned`, carrying the
    /// exact digest and the fields a fresh re-execution needs.
    #[test]
    fn evidence_ref_round_trips_and_binds_its_call() {
        let tenant = TenantHash([0x9Cu8; 16]);
        let other = TenantHash([0x9Du8; 16]);
        let key = test_key();
        let evidence = sample_evidence(tenant);
        let deadline = evidence.deadline_ns;
        let token = evidence.encode(&key).expect("encodes");

        let redeemed =
            EvidenceRef::redeem(&token, &key, tenant, SAMPLE_TOOL, NOW_NS, FAR_HORIZON_NS)
                .expect("round-trips through its own codec");
        assert_eq!(redeemed, Redeemed::Pinned(evidence.clone()));
        let decoded = EvidenceRef::decode(&token, &key).expect("decodes");
        assert_eq!(decoded, evidence);
        assert_eq!(decoded.blake3_256, [0x8Au8; 32]);
        assert_eq!(decoded.argument_hash, SAMPLE_ARGS);
        assert_eq!(decoded.tool, SAMPLE_TOOL);
        assert_eq!(decoded.mint_ns, 1_700_000_000_000_000_000);
        assert_eq!(decoded.deadline_ns, deadline);

        let err = EvidenceRef::redeem(&token, &key, other, SAMPLE_TOOL, NOW_NS, FAR_HORIZON_NS)
            .expect_err("wrong tenant must be refused");
        assert_eq!(err, CursorError::Invalid);

        let err = EvidenceRef::redeem(
            &token,
            &key,
            tenant,
            "ravel_get_trace",
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("wrong tool must be refused");
        assert_eq!(err, CursorError::Invalid);

        // At exactly the deadline the pin is gone, and the reference is
        // unpinned rather than expired.
        let redeemed =
            EvidenceRef::redeem(&token, &key, tenant, SAMPLE_TOOL, deadline, FAR_HORIZON_NS)
                .expect("past its pin, still redeemable");
        assert_eq!(
            redeemed,
            Redeemed::Unpinned {
                digest: [0x8Au8; 32],
                tool: SAMPLE_TOOL.to_owned(),
                argument_hash: SAMPLE_ARGS,
                tenant,
            }
        );
    }

    /// The tenant check is not skipped on the unpinned path, in either of the
    /// two ways a reference can become unpinned. An unpinned redemption is
    /// the one outcome that reads bytes no MAC vouched for, so this is what
    /// keeps it from being a cross-tenant read of a token someone else holds.
    #[test]
    fn unpinned_evidence_still_enforces_the_tenant_check() {
        let tenant = TenantHash([0xA1u8; 16]);
        let other = TenantHash([0xA2u8; 16]);
        assert_ne!(tenant, other, "test must use two distinct tenants");
        let key = test_key();
        let foreign_key = CursorKey::from_process_secret([0x33u8; CURSOR_KEY_LEN]);
        let evidence = sample_evidence(tenant);
        let deadline = evidence.deadline_ns;
        let token = evidence.encode(&key).expect("encodes");

        let err = EvidenceRef::redeem(&token, &key, other, SAMPLE_TOOL, deadline, FAR_HORIZON_NS)
            .expect_err("past the deadline, wrong tenant is still refused");
        assert_eq!(err, CursorError::Invalid);

        let err = EvidenceRef::redeem(
            &token,
            &foreign_key,
            other,
            SAMPLE_TOOL,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("foreign process, wrong tenant is still refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// A reference tampered with under this process's own key is `Invalid`,
    /// not `Unpinned`: the unpinned path exists for a token this process
    /// cannot verify, and must not become a way to launder a forged one that
    /// it can. The edited byte is the row digest, which is the field an
    /// unpinned outcome hands back and which nothing else re-checks.
    #[test]
    fn tampered_evidence_ref_is_invalid_not_unpinned() {
        let tenant = TenantHash([0xA3u8; 16]);
        let key = test_key();
        let token = sample_evidence(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        let digest_at = bytes.len() - MAC_LEN - 16 - 32;
        assert_eq!(
            bytes.get(digest_at),
            Some(&0x8Au8),
            "must edit the row digest itself"
        );
        bytes[digest_at] ^= 0x01;
        let tampered = URL_SAFE_NO_PAD.encode(bytes);

        let err = EvidenceRef::redeem(&tampered, &key, tenant, SAMPLE_TOOL, NOW_NS, FAR_HORIZON_NS)
            .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// The process nonce is a function of the key and nothing else: two keys
    /// give two nonces (which is what makes a restart's tokens foreign), and
    /// one key gives one nonce every time (which is what makes this process's
    /// own tokens redeemable). The exact bytes are pinned so a change to
    /// [`NONCE_CONTEXT`], to the derivation, or to the truncation length is a
    /// test failure and not a silent wire change.
    #[test]
    fn two_keys_produce_two_process_nonces() {
        let key = test_key();
        let other = CursorKey::from_process_secret([0x12u8; CURSOR_KEY_LEN]);

        assert_eq!(process_nonce(&key), [93, 236, 37, 126, 78, 123, 230, 12]);
        assert_eq!(process_nonce(&other), [54, 26, 121, 90, 155, 156, 167, 55]);
        assert_ne!(process_nonce(&key), process_nonce(&other));
        assert_eq!(
            process_nonce(&key),
            process_nonce(&CursorKey::from_process_secret([0x11u8; CURSOR_KEY_LEN])),
            "one key must give one nonce"
        );
    }

    /// A token's embedded deadline may only shorten the effective one. A
    /// deadline 10 days out, redeemed against a 25h05m protection horizon,
    /// stops being redeemable 1h05m in: the horizon minus the 24 h grace a
    /// redemption may not spend. Without the re-clamp the cursor would still
    /// be live 10 days later, resolving against a watermark whose objects the
    /// sweeper is free to delete.
    #[test]
    fn redeem_reclamps_the_deadline_to_the_protection_horizon() {
        let tenant = TenantHash([0xB7u8; 16]);
        let key = test_key();

        // `ravel_catalog::DEFAULT_PROTECTION_HORIZON_NS`: max_query_duration
        // 1 h + grace 24 h + clock_skew_allowance 5 m.
        let horizon = NOW_NS + 25 * 3_600 * 1_000_000_000 + 5 * 60 * 1_000_000_000;
        let embedded = NOW_NS + 10 * 24 * 3_600 * 1_000_000_000;
        let clamped = NOW_NS + 3_900 * 1_000_000_000;
        assert_eq!(effective_deadline_ns(embedded, horizon), clamped);
        assert_eq!(
            effective_deadline_ns(embedded, FAR_HORIZON_NS),
            embedded,
            "the clamp never lengthens a deadline"
        );

        let mut cursor = sample_cursor(tenant);
        cursor.deadline_ns = embedded;
        let token = cursor.encode(&key).expect("encodes");

        let redeemed = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            horizon,
        )
        .expect("inside the clamped deadline");
        assert_eq!(redeemed.deadline_ns, clamped);
        assert_ne!(redeemed.deadline_ns, embedded);

        let err = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            clamped,
            horizon,
        )
        .expect_err("the clamped deadline has passed");
        assert_eq!(err, CursorError::Expired);

        let still_live = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            clamped,
            FAR_HORIZON_NS,
        )
        .expect("the same instant is inside the embedded deadline");
        assert_eq!(still_live.deadline_ns, embedded);

        // The evidence leg re-clamps the same way, and answers `Unpinned`
        // where the cursor answers `Expired`.
        let mut evidence = sample_evidence(tenant);
        evidence.deadline_ns = embedded;
        let token = evidence.encode(&key).expect("encodes");

        let redeemed = EvidenceRef::redeem(&token, &key, tenant, SAMPLE_TOOL, NOW_NS, horizon)
            .expect("inside the clamped deadline");
        assert_eq!(
            redeemed,
            Redeemed::Pinned(EvidenceRef {
                tenant,
                tool: SAMPLE_TOOL.to_owned(),
                argument_hash: SAMPLE_ARGS,
                blake3_256: [0x8Au8; 32],
                mint_ns: 1_700_000_000_000_000_000,
                deadline_ns: clamped,
            })
        );

        let redeemed = EvidenceRef::redeem(&token, &key, tenant, SAMPLE_TOOL, clamped, horizon)
            .expect("past its pin, still redeemable");
        assert_eq!(
            redeemed,
            Redeemed::Unpinned {
                digest: [0x8Au8; 32],
                tool: SAMPLE_TOOL.to_owned(),
                argument_hash: SAMPLE_ARGS,
                tenant,
            }
        );

        let redeemed =
            EvidenceRef::redeem(&token, &key, tenant, SAMPLE_TOOL, clamped, FAR_HORIZON_NS)
                .expect("the same instant is inside the embedded deadline");
        assert_eq!(
            redeemed,
            Redeemed::Pinned(EvidenceRef {
                tenant,
                tool: SAMPLE_TOOL.to_owned(),
                argument_hash: SAMPLE_ARGS,
                blake3_256: [0x8Au8; 32],
                mint_ns: 1_700_000_000_000_000_000,
                deadline_ns: embedded,
            }),
            "the horizon, not the clock, produced the unpinned outcome above"
        );
    }
}

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
//! to ADR-1374 D5, as corrected by the 2026-09-10 one). Redemption re-resolves
//! the snapshot from those inputs deterministically, at the cursor's own
//! `mint_ns` (the 2026-09-12 amendment, item 4, and the 2026-09-13 one, item
//! 5), and refuses the cursor as [`CursorError::Expired`] in the three cases
//! where that re-resolve would no longer answer with the page's own snapshot:
//! its deadline has fallen outside the protection horizon less the grace,
//! which is where a compaction becomes free to take the pin apart; an erasure
//! predicate is in force that the cursor did not pin and that reaches its
//! signal and time range; or the declared column set differs from the pinned
//! one (the 2026-09-12 amendment, item 3).
//!
//! The commit-token watermark is one of those inputs and nothing more. It is
//! what page 2 resolves against, not a test run against whatever watermark the
//! redeeming call happens to observe: ADR-0010 gives no ordering to run such a
//! test with, since `seq` is monotonic only per (writer_id, epoch, shard) with
//! gaps carrying no meaning and `epoch` is informational only. The catalog is
//! what answers whether a commit token can be satisfied, and it answers by
//! resolving the token's own identity rather than by comparing positions.
//!
//! Enumerating the pinned segments instead is what the amendment replaced. One
//! `ravel_sql::SegmentPin` costs about 250 B plus base64, so the 2,000-segment
//! admission ceiling of D6 mints a token of several hundred KiB: past the
//! cursor's own 4 KiB bound in the envelope, and past the whole 256 KiB
//! response floor.
//!
//! # Redemption is two stages around the caller's resolve
//!
//! The last two cases are facts about the redeeming resolve, so redemption is
//! split around it. [`Cursor::redeem`] is stage one: it decodes the token,
//! verifies its MAC, version and deadline, and binds it to the redeeming
//! tenant, tool and argument hash. It takes no store, catalog or erasure set,
//! so it reads nothing, and it returns a [`PendingRedemption`] carrying the
//! resolve inputs the caller resolves with, the resolve instant included. The
//! caller resolves, and then [`PendingRedemption::confirm`], stage two, checks
//! what that resolve observed (its pending erasure predicates and its declared
//! column set) against the pin and only then hands back the [`Cursor`] with
//! its position.
//!
//! The erasure check is sound for the set the redeeming resolve observed, and
//! for no set read earlier. Stage one takes no erasure set, so the only one
//! the check can be given is a resolve's, and the redeeming resolve runs after
//! stage one. That resolve runs at `mint_ns`, and the instant bounds which
//! ingest-hour buckets it lists, not which erasure requests it reports:
//! ravel-catalog lists the requests under `del/` on every resolve with no
//! instant among its arguments (`Catalog::list_pending_erasure` in
//! `crates/ravel-catalog/src/catalog.rs`), so a request durable before the
//! redeeming resolve and still pending is in the set `confirm` receives. A
//! caller that filtered that set down to what was pending at `mint_ns`, or
//! passed one read before stage one, would hide exactly the predicates the
//! check exists to see.
//!
//! Those three are what a redemption can detect. A fourth case exists that it
//! cannot: an erasure that arrives after the mint and completes before the
//! redeeming resolve lists `del/` is neither pinned nor pending. It stays
//! empty only while the time from the mint to that listing is shorter than an
//! erasure's completion time; that time is the cursor's lifetime plus however
//! long the caller takes between stage one and its resolve.
//! [`PendingRedemption::confirm`] states the assumption with both quantities
//! named.
//!
//! # The pinned ingest watermark and the snapshot id
//!
//! A cursor also pins page one's freshness watermark,
//! [`Cursor::ingest_watermark_hour`] (the 2026-09-13 amendment, items 3 and
//! 4), so every page of a sequence can report the value page one measured. It
//! is a measurement carried forward, not a resolve input: neither stage of
//! redemption takes or compares it, and [`snapshot_id`] does not hash it.
//!
//! [`snapshot_id`] is the `visibility.snapshot_id` derivation of the same
//! amendment's item 6: a hash over the seven resolve inputs, the resolve
//! instant among them. A cursor pins six of them, and its `mint_ns` is the
//! seventh as [`PendingRedemption::resolve_at_ns`], so a redemption that
//! derives its id from the pinned inputs and that instant reports page one's
//! id.
//!
//! # The D5 wrong-tenant rule
//!
//! A cursor minted for tenant A that is redeemed by tenant B must decode as
//! [`CursorError::Invalid`], the same error a corrupt or tampered token
//! produces -- never a distinct "wrong tenant" signal, which would let one
//! tenant learn that a token it holds was minted for someone else. Redeeming
//! code MUST call [`Cursor::redeem`] and then [`PendingRedemption::confirm`]
//! (or [`EvidenceRef::redeem`]), never [`Cursor::decode`] directly, so the
//! tenant check happens on every call. On that path the position is reachable
//! only through `confirm`'s result, since [`PendingRedemption`] has no
//! accessor for it, so a caller that follows the rule cannot page from a
//! cursor whose stage two it skipped.
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
//! or a foreign nonce, and [`PendingRedemption::confirm`] for a newer erasure
//! over its range or a changed declared column set. An evidence reference
//! is different. It names one row,
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
use ravel_sql::{DeclaredColumn, DeclaredType};
use ravel_types::{CommitToken, Signal, TenantHash};

use crate::envelope::{Failure, FailureClass};

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
/// watermark, pending erasure, declared columns, keyset position); version 5
/// adds the pinned ingest watermark hour of the 2026-09-13 amendment. A token
/// of any earlier version decodes as [`CursorError::Invalid`] like any other
/// unsupported version, which is correct and costs nothing: these tokens are
/// process-local and deadline-bounded, so none survives the deploy that
/// changes the number.
const CURSOR_VERSION: u8 = 5;
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
///
/// This constant also enters the bound on a cursor's lifetime, and through
/// that what keeps [`PendingRedemption::confirm`]'s erasure check sound,
/// through the clamp [`Cursor::mint`] applies. Raising a
/// deployment's protection horizon without raising this value lengthens that
/// lifetime; [`PendingRedemption::confirm`] states the bound the lifetime must
/// stay under.
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
    /// as far out of reach; or, from [`PendingRedemption::confirm`], the
    /// redeeming resolve observed a newer erasure over its scope or a
    /// different declared column set. Only cursors report this: an evidence
    /// reference past its deadline or from a foreign process redeems as
    /// [`Redeemed::Unpinned`] instead.
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

impl CursorError {
    /// The D4 failure a tool reports for this error, carrying its message.
    ///
    /// [`CursorError::Expired`] is `cursor_expired`, and that includes a token
    /// minted by a process that has since restarted: its nonce is foreign, so
    /// it decodes as expired, not invalid. [`CursorError::Invalid`] and
    /// [`CursorError::TokenTooLong`] are `cursor_invalid`: both are a token
    /// the caller sent that no redemption can use, and the second only names
    /// the length the caller already knows. [`CursorError::FieldTooLong`] is
    /// `internal`: it is only ever returned while minting, so it is this
    /// process failing to build a token, never anything about one a caller
    /// holds. The match is exhaustive, so a new variant must be classed here
    /// before it compiles.
    pub fn failure(self) -> Failure {
        let class = match self {
            CursorError::Expired => FailureClass::CursorExpired,
            CursorError::Invalid | CursorError::TokenTooLong { .. } => FailureClass::CursorInvalid,
            CursorError::FieldTooLong { .. } => FailureClass::Internal,
        };
        Failure {
            class,
            message: self.to_string(),
            counter: None,
        }
    }
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
    /// This is a resolve input, not a validity test. It is what makes the
    /// re-resolve deterministic rather than a fresh resolution: page 2
    /// resolves against this same watermark instead of against whatever the
    /// redeeming call observes. Neither stage of redemption compares it with
    /// anything, because ADR-0010 defines no ordering over commit tokens to
    /// compare it with: `seq` is monotonic only per (writer_id, epoch,
    /// shard), gaps in it carry no meaning, and `epoch` is informational
    /// only. Whether these tokens can still be satisfied is the catalog's
    /// answer to give at resolve time, not this codec's.
    pub min_commit_watermark: Vec<CommitToken>,
    pub pending_erasure: Vec<ErasurePredicate>,
    /// The declared column set page one resolved with. Compared as a set
    /// against the redeeming resolve's own by [`PendingRedemption::confirm`].
    pub declared_columns: Vec<DeclaredColumn>,
    /// Page one's freshness watermark: the greatest `ingest_hour_bucket`
    /// among the segments its resolve returned, in whole hours since the
    /// epoch (the 2026-09-13 amendment, items 3 and 4). `None` when that
    /// resolve returned no segment. Carried forward so every later page
    /// reports page one's value; never compared with anything.
    pub ingest_watermark_hour: Option<u32>,
    pub position: CursorPosition,
    /// The instant page one resolved at, and the instant every redeeming
    /// resolve runs at ([`PendingRedemption::resolve_at_ns`]).
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

        match self.ingest_watermark_hour {
            None => buf.push(0),
            Some(hour) => {
                buf.push(1);
                buf.extend_from_slice(&hour.to_le_bytes());
            }
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

    /// Mint the wire token a tool hands out: [`Cursor::encode`] after holding
    /// `deadline_ns` to the cursor lifetime bound.
    ///
    /// The bound is `mint_ns + horizon - GRACE_NS`, where `horizon` is the
    /// deployment's protection horizon as a duration. This crate never sees
    /// that duration directly; the minting call's [`ToolContext`] carries it
    /// as the pair `now_ns` and `protection_horizon_ns` (`now_ns + horizon`),
    /// so the horizon is their difference and the bound is measured from this
    /// cursor's own `mint_ns`, not from the minting call's clock.
    ///
    /// A `deadline_ns` past the bound is clamped to it, not refused. The bound
    /// is the deployment's, not something the tool asked for, and a clamped
    /// cursor is exactly as sound as one minted at the bound: stage one of a
    /// redemption re-clamps the same way through [`effective_deadline_ns`]
    /// and only ever shortens a deadline. Refusing would end pagination for
    /// every tool whose requested lifetime is longer than the deployment
    /// allows, with no gain in what a redemption can trust. A tool that has
    /// no lifetime of its own passes `i64::MAX` and gets the bound.
    ///
    /// Refused with [`CursorError::Expired`] when the clamped deadline is not
    /// after `now_ns`: such a token would be expired at its first redemption,
    /// which happens when the horizon is no longer than [`GRACE_NS`] or the
    /// resolve instant lies more than `horizon - GRACE_NS` in the past.
    /// Every [`Cursor::encode`] failure passes through unchanged.
    ///
    /// [`ToolContext`]: crate::tools::ToolContext
    pub fn mint(
        &self,
        key: &CursorKey,
        now_ns: i64,
        protection_horizon_ns: i64,
    ) -> Result<String, CursorError> {
        let horizon_ns = protection_horizon_ns.saturating_sub(now_ns);
        let bound_ns = self
            .mint_ns
            .saturating_add(horizon_ns)
            .saturating_sub(GRACE_NS);
        let deadline_ns = self.deadline_ns.min(bound_ns);
        if deadline_ns <= now_ns {
            return Err(CursorError::Expired);
        }
        Cursor {
            deadline_ns,
            ..self.clone()
        }
        .encode(key)
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

        let ingest_watermark_hour = match cur.read_u8()? {
            0 => None,
            1 => Some(u32::from_le_bytes(cur.read_array::<4>()?)),
            _ => return Err(CursorError::Invalid),
        };

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
            ingest_watermark_hour,
            position,
            mint_ns,
            deadline_ns,
        })
    }

    /// Stage one of a redemption: decode, verify the MAC and version, bind the
    /// token to the redeeming call, and check the deadline against `now_ns`.
    /// Every call site MUST use this, then [`PendingRedemption::confirm`],
    /// instead of [`Cursor::decode`].
    ///
    /// It takes no store, catalog or erasure set, so it reads nothing: the
    /// caller resolves the snapshot after this returns, from the inputs the
    /// [`PendingRedemption`] exposes and at its
    /// [`resolve_at_ns`](PendingRedemption::resolve_at_ns), and passes what
    /// that resolve observed to [`PendingRedemption::confirm`]. `now_ns` is the
    /// redeeming call's clock and is used for the deadline check alone; it
    /// never becomes the resolve instant.
    ///
    /// The token must have been minted for `caller_tenant`, for `tool`, and
    /// for `argument_hash`. Any of those three mismatching is
    /// [`CursorError::Invalid`], identically to corruption (the D5
    /// wrong-tenant rule extended to the other two bindings): no separate
    /// signal is ever returned for "this cursor belongs to someone else", to
    /// another tool, or to another argument set.
    ///
    /// The deadline checked is [`effective_deadline_ns`] of the embedded one
    /// and `protection_horizon_ns`, not the embedded one alone. The pending
    /// redemption reports that clamped value as
    /// [`PendingRedemption::deadline_ns`], and the cursor `confirm` returns
    /// carries it in `deadline_ns`, so a caller that bounds its own work by
    /// either cannot read past the pin's protection.
    ///
    /// That clamp is also the compaction check of the 2026-09-10 amendment to
    /// ADR-1374 D5, and the only one. `protection_horizon_ns` minus
    /// [`GRACE_NS`] is the instant past which a sweep may compact away what
    /// the pin resolves to, so a cursor whose pin is no longer protected is
    /// [`CursorError::Expired`] before anything else here runs. Nothing else
    /// in this codec can observe a compaction, and a second mechanism for it
    /// would only be a second thing to keep in agreement with this one.
    ///
    /// The erasure and declared-column checks are not here: both compare the
    /// pin with what the redeeming resolve observed, which does not exist yet.
    /// They are [`PendingRedemption::confirm`].
    //
    // Every parameter is a distinct precondition of a single redemption, and
    // a caller that omits one has skipped a check. Grouping them into a
    // struct would let a call site leave a field at its default.
    #[allow(clippy::too_many_arguments)]
    pub fn redeem(
        token: &str,
        key: &CursorKey,
        caller_tenant: TenantHash,
        tool: &str,
        argument_hash: &[u8; 32],
        now_ns: i64,
        protection_horizon_ns: i64,
    ) -> Result<PendingRedemption, CursorError> {
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
        Ok(PendingRedemption { cursor })
    }

    /// Whether an erasure predicate is in force at redemption that this
    /// cursor did not pin and that reaches into what its page read.
    ///
    /// Scope is the cursor's signal and its half-open `[range_start_ns,
    /// range_end_ns)`, and nothing else. A predicate's matchers are tested
    /// against a record's labels or attributes, and this codec holds no
    /// records, so no matcher-level analysis is possible here: two predicates
    /// that name different services are both treated as reaching the cursor's
    /// rows. Overrefusing that way costs a re-run of the query, where missing
    /// a predicate would page against a snapshot ADR-0064 requires the erasure
    /// to have already left.
    ///
    /// A predicate for another signal, or one whose window lies entirely
    /// outside the cursor's range, cannot touch a row this page returned, so
    /// it leaves the cursor redeemable. A windowless predicate (both bounds
    /// unset, `ravel_query::erasure`'s zero-as-unset convention) erases
    /// regardless of event time and so intersects every range.
    ///
    /// Predicates that were in force at mint and are not in force now are not
    /// a mismatch either: an erasure that completed took its rows out of the
    /// pinned snapshot before the page was built, and the pin already names it.
    ///
    /// An erasure that both arrived and completed between mint and redemption
    /// is in neither set and is invisible here. What keeps that case empty is
    /// a timing relationship this method cannot check;
    /// [`PendingRedemption::confirm`] states it and names both quantities.
    fn newer_erasure_intersects(&self, in_force: &[(Signal, ErasurePredicate)]) -> bool {
        in_force.iter().any(|(signal, predicate)| {
            *signal == self.signal
                && !self.pending_erasure.contains(predicate)
                && window_intersects(predicate, self.range_start_ns, self.range_end_ns)
        })
    }
}

/// A cursor that passed stage one of a redemption ([`Cursor::redeem`]) and
/// waits for the caller's resolve.
///
/// It exposes the pinned resolve inputs and the resolve instant, and nothing
/// a page can be served from: the position comes back only from
/// [`PendingRedemption::confirm`], once the resolve's observations have been
/// checked against the pin.
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// use ravel_mcp::cursor::{Cursor, CursorError, CursorKey};
/// use ravel_types::TenantHash;
///
/// fn page_two(token: &str, key: &CursorKey) -> Result<(), CursorError> {
///     // Dropping the pending redemption skips stage two.
///     Cursor::redeem(token, key, TenantHash([0; 16]), "ravel_search_logs", &[0; 32], 0, 0)?;
///     Ok(())
/// }
/// # fn main() {}
/// ```
///
/// The same call compiles once the pending redemption is confirmed, so the
/// refusal above is the `#[must_use]` on this type and nothing else:
///
/// ```
/// #![deny(unused_must_use)]
/// use ravel_mcp::cursor::{Cursor, CursorError, CursorKey};
/// use ravel_types::TenantHash;
///
/// fn page_two(token: &str, key: &CursorKey) -> Result<(), CursorError> {
///     let pending =
///         Cursor::redeem(token, key, TenantHash([0; 16]), "ravel_search_logs", &[0; 32], 0, 0)?;
///     pending.confirm(0, &[], &[])?;
///     Ok(())
/// }
/// # fn main() {}
/// ```
#[derive(Debug)]
#[must_use = "a pending redemption has not checked erasure or declared columns; resolve, then confirm"]
pub struct PendingRedemption {
    cursor: Cursor,
}

impl PendingRedemption {
    /// The pinned signal to resolve over.
    pub fn signal(&self) -> Signal {
        self.cursor.signal
    }

    /// Start of the pinned half-open time range, inclusive.
    pub fn range_start_ns(&self) -> i64 {
        self.cursor.range_start_ns
    }

    /// End of the pinned half-open time range, exclusive.
    pub fn range_end_ns(&self) -> i64 {
        self.cursor.range_end_ns
    }

    /// The pinned minimum commit-token watermark to resolve against.
    pub fn min_commit_watermark(&self) -> &[CommitToken] {
        &self.cursor.min_commit_watermark
    }

    /// The instant the redeeming resolve runs at: the cursor's own `mint_ns`,
    /// never the redeeming call's clock (the 2026-09-12 amendment, item 4, and
    /// the 2026-09-13 one, item 5). Resolving at a later instant would list
    /// buckets page one never saw, so the sequence would walk a moving
    /// snapshot under a pinned watermark.
    pub fn resolve_at_ns(&self) -> i64 {
        self.cursor.mint_ns
    }

    /// The deadline stage one checked, already clamped to the protection
    /// horizon less [`GRACE_NS`].
    pub fn deadline_ns(&self) -> i64 {
        self.cursor.deadline_ns
    }

    /// Stage two of a redemption: compare what the redeeming resolve observed
    /// with the pin, and hand back the cursor and its position only when
    /// nothing moved.
    ///
    /// `now_ns` is the redeeming call's clock read when its resolve finished.
    /// The deadline stage one checked is tested again against it, so a
    /// resolve that ran past the pin's protection is [`CursorError::Expired`]
    /// even though stage one accepted the token: the deadline is what bounds
    /// the time the erasure check below depends on, and stage one cannot see
    /// how long the resolve took.
    ///
    /// `observed_erasure` is the pending erasure predicate set that resolve
    /// reported, each paired with the signal it applies to. A predicate that
    /// the cursor did not pin, whose signal is the cursor's own and whose
    /// event-time window overlaps the cursor's half-open range, means the page
    /// came from a snapshot that no longer reproduces, so the cursor is
    /// [`CursorError::Expired`]. A predicate outside that scope leaves it
    /// redeemable (see `Cursor::newer_erasure_intersects`).
    ///
    /// `observed_declared_columns` is the declared column set that resolve
    /// reported. Compared with the pinned set as a set of (key, type) pairs:
    /// an added column, a removed one, or one whose type changed is
    /// [`CursorError::Expired`] (the 2026-09-12 amendment, item 3), and the
    /// order the two lists name their columns in does not matter.
    ///
    /// The set passed must be the one the redeeming resolve listed. That
    /// resolve runs at [`resolve_at_ns`](Self::resolve_at_ns), but the
    /// catalog lists pending erasure requests with no instant among its
    /// arguments (see this module's "Redemption is two stages around the
    /// caller's resolve" docs), so the set holds every request durable before
    /// that resolve's listing and still pending then.
    ///
    /// The erasure check is sound only while the time from the mint to the
    /// redeeming resolve's listing is shorter than the minimum time an
    /// erasure takes to complete. `observed_erasure` carries the predicates
    /// still pending at that listing. An erasure that arrived after the
    /// cursor was minted and whose rewrite completed before the listing is in
    /// neither set: the cursor did not pin it and it is no longer pending, so
    /// nothing here can observe it, and page two is served from a snapshot it
    /// changed. That time is the cursor's lifetime plus the caller's own gap
    /// between stage one and its resolve, so a caller resolves straight after
    /// stage one. The two quantities are configured in different crates and
    /// nothing asserts the relationship between them:
    ///
    /// - Maximum cursor lifetime is `protection_horizon_ns` minus
    ///   [`GRACE_NS`], measured from the mint. [`Cursor::mint`] enforces it,
    ///   clamping the embedded deadline to `mint_ns` plus the horizon less
    ///   [`GRACE_NS`]. Stage one's own clamp, [`effective_deadline_ns`], cannot:
    ///   its horizon argument is the redeeming call's `now +
    ///   protection_horizon`, so it never expires a cursor on its own, and a
    ///   token built with [`Cursor::encode`] directly carries whatever deadline
    ///   its builder chose. The horizon is the deployment's own
    ///   (`CatalogConfig::protection_horizon_ns` in
    ///   `crates/ravel-catalog/src/config.rs`, default
    ///   `DEFAULT_PROTECTION_HORIZON_NS` = 25 h 05 m), set by an operator
    ///   through `ravel-cli gc-config set --protection-horizon`, which writes
    ///   durable `sys/gc`. [`GRACE_NS`] is a constant of this crate, 24 h,
    ///   that no configuration moves. With the default horizon, a cursor
    ///   [`Cursor::mint`] minted lives at most 1 h 05 m.
    /// - Minimum erasure completion is the seal wait,
    ///   `erasure_seal_wait_bound_ns` in
    ///   `crates/ravel-maintain/src/erasure_rewrite.rs`: `max_ingest_lag`
    ///   (2 h by `ravel_catalog::DEFAULT_MAX_INGEST_LAG_NS`, also operator-set)
    ///   plus one 1 h bucket span plus the seal margin (`max_flush_lifetime`
    ///   1 h + `clock_skew_allowance` 5 m), 4 h 05 m with defaults. A rewrite
    ///   cannot acknowledge before the bucket open at the request seals.
    ///
    /// 1 h 05 m under 4 h 05 m is what closes the window, for a cursor minted
    /// through [`Cursor::mint`] and a caller that resolves promptly. The operator
    /// action that opens it is raising `--protection-horizon`, because
    /// [`GRACE_NS`] cannot be raised with it. `gc-config set` accepts any
    /// horizon at or above `max_query_duration + grace +
    /// clock_skew_allowance`, so a 30 h horizon is a valid deployment; it
    /// leaves a 6 h cursor lifetime, above the 4 h 05 m floor, and an erasure
    /// can then complete inside one cursor's life. Lowering `max_ingest_lag`
    /// moves the same floor down.
    ///
    /// The relationship is not expressible as an assertion in this crate. The
    /// crate never sees the horizon as a duration: `protection_horizon_ns`
    /// arrives per call as an absolute instant, the crate has no startup step
    /// of its own, and it holds no ravel-catalog or ravel-maintain dependency
    /// to read either configured value from.
    pub fn confirm(
        self,
        now_ns: i64,
        observed_erasure: &[(Signal, ErasurePredicate)],
        observed_declared_columns: &[DeclaredColumn],
    ) -> Result<Cursor, CursorError> {
        if now_ns >= self.cursor.deadline_ns {
            return Err(CursorError::Expired);
        }
        if self.cursor.newer_erasure_intersects(observed_erasure) {
            return Err(CursorError::Expired);
        }
        if canonical_set(
            self.cursor
                .declared_columns
                .iter()
                .map(declared_column_bytes),
        ) != canonical_set(observed_declared_columns.iter().map(declared_column_bytes))
        {
            return Err(CursorError::Expired);
        }
        Ok(self.cursor)
    }
}

/// Domain-separation context for [`snapshot_id`], in BLAKE3's derive-key mode.
const SNAPSHOT_ID_CONTEXT: &str = "ravel-mcp visibility.snapshot_id v1";

/// `visibility.snapshot_id`: a BLAKE3-256 hash over the seven resolve inputs of
/// the 2026-09-13 amendment to ADR-1374, item 6 -- the tenant hash, the signal,
/// the half-open time range, the minimum commit-token watermark, the pending
/// erasure predicates, the declared column set, and the resolve instant. Two
/// resolves over the same seven derive the same id; it identifies those
/// inputs, not the segments they resolved to.
///
/// Unkeyed (BLAKE3's derive-key mode under [`SNAPSHOT_ID_CONTEXT`]), so the id
/// does not depend on the process that derived it. Every variable-length
/// value is written behind a `u64` little-endian length, and every list
/// behind its element count, so inputs that differ other than in list order
/// or repetition never encode to the same bytes. The three lists are sets: each element is encoded on its own and
/// the encodings are sorted and deduplicated, so the order a resolve happened
/// to list them in does not change the id. A redemption derives its page's id
/// from the cursor's pinned inputs with `resolve_at_ns` set to the cursor's
/// `mint_ns`, which is what keeps one id across a page sequence.
// The seven inputs are the amendment's own list, the time range taken as its
// two bounds; a struct would let a call site leave one at its default.
#[allow(clippy::too_many_arguments)]
pub fn snapshot_id(
    tenant: TenantHash,
    signal: Signal,
    range_start_ns: i64,
    range_end_ns: i64,
    min_commit_watermark: &[CommitToken],
    pending_erasure: &[ErasurePredicate],
    declared_columns: &[DeclaredColumn],
    resolve_at_ns: i64,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(SNAPSHOT_ID_CONTEXT);
    hasher.update(&tenant.0);
    hasher.update(&[signal_tag(signal)]);
    hasher.update(&range_start_ns.to_le_bytes());
    hasher.update(&range_end_ns.to_le_bytes());
    hash_set(
        &mut hasher,
        canonical_set(
            min_commit_watermark
                .iter()
                .map(|token| token.encode().into_bytes()),
        ),
    );
    hash_set(
        &mut hasher,
        canonical_set(pending_erasure.iter().map(erasure_predicate_bytes)),
    );
    hash_set(
        &mut hasher,
        canonical_set(declared_columns.iter().map(declared_column_bytes)),
    );
    hasher.update(&resolve_at_ns.to_le_bytes());
    *hasher.finalize().as_bytes()
}

/// Sorts and deduplicates per-element encodings, so a list compares and
/// hashes as the set it names.
fn canonical_set(items: impl Iterator<Item = Vec<u8>>) -> Vec<Vec<u8>> {
    let mut items: Vec<Vec<u8>> = items.collect();
    items.sort();
    items.dedup();
    items
}

/// Feeds a canonical set to `hasher` as its element count followed by each
/// element behind its own length.
fn hash_set(hasher: &mut blake3::Hasher, items: Vec<Vec<u8>>) {
    hash_u64(hasher, items.len());
    for item in &items {
        hash_u64(hasher, item.len());
        hasher.update(item);
    }
}

fn hash_u64(hasher: &mut blake3::Hasher, len: usize) {
    hasher.update(&(len as u64).to_le_bytes());
}

/// One erasure predicate as self-delimiting bytes: its matcher count, each
/// key and value behind a `u64` length, then both window bounds.
fn erasure_predicate_bytes(predicate: &ErasurePredicate) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, predicate.matchers().len());
    for (k, v) in predicate.matchers() {
        push_u64(&mut out, k.len());
        out.extend_from_slice(k.as_bytes());
        push_u64(&mut out, v.len());
        out.extend_from_slice(v.as_bytes());
    }
    out.extend_from_slice(&predicate.window_start_ns().to_le_bytes());
    out.extend_from_slice(&predicate.window_end_ns().to_le_bytes());
    out
}

/// One declared column as self-delimiting bytes: its key behind a `u64`
/// length, then its type's wire tag.
fn declared_column_bytes(column: &DeclaredColumn) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, column.key.len());
    out.extend_from_slice(column.key.as_bytes());
    out.push(declared_type_tag(column.ty));
    out
}

fn push_u64(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(&(len as u64).to_le_bytes());
}

/// Whether an erasure predicate's half-open event-time window overlaps the
/// half-open `[start_ns, end_ns)` a cursor was resolved over.
///
/// A bound of `0` means unset on that side, the convention
/// `ravel_query::erasure` reads a predicate's window under, so an unset bound
/// is unbounded rather than the epoch.
fn window_intersects(predicate: &ErasurePredicate, start_ns: i64, end_ns: i64) -> bool {
    (predicate.window_end_ns() == 0 || predicate.window_end_ns() > start_ns)
        && (predicate.window_start_ns() == 0 || predicate.window_start_ns() < end_ns)
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
///
/// This clamp is relative to the redeeming call's clock, so it never bounds a
/// cursor's lifetime from its mint. [`Cursor::mint`] does that, by clamping
/// the embedded deadline to `mint_ns + horizon - GRACE_NS` before encoding.
/// Clamping rather than refusing there matches this function: both only ever
/// shorten a deadline, a cursor at the bound is as sound as any shorter one,
/// and a refusal would end pagination over a deadline the deployment, not the
/// tool, is responsible for.
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

    /// No erasure predicate in force at redemption, so the newer-erasure
    /// check is inert in every test but the three that exercise it.
    const NO_ERASURE: &[(Signal, ErasurePredicate)] = &[];

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

    /// Both stages of a redemption, with the resolve between them observing
    /// `erasure` and exactly the declared column set the cursor pinned, so a
    /// test of either stage's other checks is not perturbed by this one.
    #[allow(clippy::too_many_arguments)]
    fn redeem_both(
        token: &str,
        key: &CursorKey,
        tenant: TenantHash,
        tool: &str,
        argument_hash: &[u8; 32],
        erasure: &[(Signal, ErasurePredicate)],
        now_ns: i64,
        protection_horizon_ns: i64,
    ) -> Result<Cursor, CursorError> {
        let pending = Cursor::redeem(
            token,
            key,
            tenant,
            tool,
            argument_hash,
            now_ns,
            protection_horizon_ns,
        )?;
        let declared = pending.cursor.declared_columns.clone();
        pending.confirm(now_ns, erasure, &declared)
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
            ingest_watermark_hour: Some(472_222),
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

        let err = redeem_both(
            &token,
            &key,
            tenant_b,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

        let err = redeem_both(
            &tampered,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

        let err = redeem_both(
            &tampered,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

        let err = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
            deadline,
            FAR_HORIZON_NS,
        )
        .expect_err("must be expired at exactly the deadline");
        assert_eq!(err, CursorError::Expired);
    }

    /// [`sample_cursor`]'s own pinned erasure set, paired with the signal it
    /// was pinned for, which is what "unchanged" means at redemption.
    fn pinned_erasure(cursor: &Cursor) -> Vec<(Signal, ErasurePredicate)> {
        cursor
            .pending_erasure
            .iter()
            .map(|predicate| (cursor.signal, predicate.clone()))
            .collect()
    }

    /// An erasure predicate in force at redemption that the cursor did not
    /// pin, over the cursor's own signal and reaching into its time range,
    /// is `Expired`: the snapshot the page came from is one ADR-0064 requires
    /// those rows to have already left, so it cannot be re-resolved.
    ///
    /// Five shapes of reaching in, each the pinned set plus exactly one new
    /// predicate: windowless (no event-time restriction at all), fully inside
    /// the range, straddling each end of it, and open-ended above from inside.
    /// A sixth case drops the pinned predicate from the in-force set as well,
    /// so the refusal is attributable to the predicate that was added rather
    /// than to set equality.
    #[test]
    fn an_intersecting_new_erasure_is_cursor_expired() {
        let tenant = TenantHash([0x9Du8; 16]);
        let key = test_key();
        let cursor = sample_cursor(tenant);
        let token = cursor.encode(&key).expect("encodes");
        assert_eq!(cursor.signal, Signal::Logs);
        assert_eq!(cursor.range_start_ns, 1_699_999_000_000_000_000);
        assert_eq!(cursor.range_end_ns, 1_700_000_000_000_000_000);
        assert_eq!(cursor.pending_erasure.len(), 1);

        let user = vec![("user.id".to_owned(), "u-42".to_owned())];
        let newer = [
            ErasurePredicate::windowless(user.clone()),
            ErasurePredicate::new(
                user.clone(),
                cursor.range_start_ns + 1,
                cursor.range_end_ns - 1,
            ),
            ErasurePredicate::new(
                user.clone(),
                cursor.range_start_ns - 3_600_000_000_000,
                cursor.range_start_ns + 1,
            ),
            ErasurePredicate::new(
                user.clone(),
                cursor.range_end_ns - 1,
                cursor.range_end_ns + 3_600_000_000_000,
            ),
            ErasurePredicate::new(user.clone(), cursor.range_start_ns + 1, 0),
        ];
        assert_eq!(newer.len(), 5);

        for predicate in &newer {
            let mut in_force = pinned_erasure(&cursor);
            in_force.push((cursor.signal, predicate.clone()));
            assert_eq!(in_force.len(), 2);

            let err = redeem_both(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                &in_force,
                NOW_NS,
                FAR_HORIZON_NS,
            )
            .expect_err("a newer erasure over the cursor's range must be refused");
            assert_eq!(err, CursorError::Expired);
        }

        let only_new = [(cursor.signal, newer[0].clone())];
        let err = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            &only_new,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("a newer erasure must be refused whatever else is in force");
        assert_eq!(err, CursorError::Expired);

        // The bindings still answer first: a newer erasure does not turn a
        // wrong-tenant token into `Expired`, which would tell its holder the
        // token itself was well-formed.
        let err = redeem_both(
            &token,
            &key,
            TenantHash([0x9Cu8; 16]),
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            &only_new,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
    }

    /// An erasure the cursor did not pin that cannot touch a row the page
    /// returned leaves the cursor redeemable. Three ways to miss: a window
    /// that ends exactly where the cursor's half-open range starts, one that
    /// starts exactly where it ends, and a predicate over another signal
    /// entirely (windowless, so only the signal keeps it out of scope).
    ///
    /// The two boundary cases are then moved one nanosecond into the range,
    /// which must refuse: that is what pins the comparison as strict overlap
    /// of half-open intervals rather than as touching endpoints.
    #[test]
    fn a_non_intersecting_new_erasure_still_redeems() {
        let tenant = TenantHash([0x9Eu8; 16]);
        let key = test_key();
        let cursor = sample_cursor(tenant);
        let token = cursor.encode(&key).expect("encodes");
        assert_ne!(cursor.signal, Signal::Metrics);

        let user = vec![("user.id".to_owned(), "u-42".to_owned())];
        let outside = [
            (
                cursor.signal,
                ErasurePredicate::new(
                    user.clone(),
                    cursor.range_start_ns - 3_600_000_000_000,
                    cursor.range_start_ns,
                ),
            ),
            (
                cursor.signal,
                ErasurePredicate::new(
                    user.clone(),
                    cursor.range_end_ns,
                    cursor.range_end_ns + 3_600_000_000_000,
                ),
            ),
            (Signal::Metrics, ErasurePredicate::windowless(user.clone())),
        ];
        assert_eq!(outside.len(), 3);

        for entry in &outside {
            let mut in_force = pinned_erasure(&cursor);
            in_force.push(entry.clone());
            let redeemed = redeem_both(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                &in_force,
                NOW_NS,
                FAR_HORIZON_NS,
            )
            .expect("an erasure outside the cursor's scope must redeem");
            assert_eq!(redeemed, cursor);
        }

        let just_inside = [
            ErasurePredicate::new(
                user.clone(),
                cursor.range_start_ns - 3_600_000_000_000,
                cursor.range_start_ns + 1,
            ),
            ErasurePredicate::new(
                user.clone(),
                cursor.range_end_ns - 1,
                cursor.range_end_ns + 3_600_000_000_000,
            ),
        ];
        assert_eq!(just_inside.len(), 2);

        for predicate in &just_inside {
            let in_force = [(cursor.signal, predicate.clone())];
            let err = redeem_both(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                &in_force,
                NOW_NS,
                FAR_HORIZON_NS,
            )
            .expect_err("one nanosecond inside the range is inside it");
            assert_eq!(err, CursorError::Expired);
        }
    }

    /// The erasure check refuses a predicate the cursor did not pin, not the
    /// presence of one: the pinned set still in force redeems, and so does an
    /// in-force set that has lost a predicate the cursor pinned, since an
    /// erasure that completed already took its rows out of what the page read.
    /// The cursor comes back whole, carrying the pinned watermark and the
    /// pinned erasure set unchanged, so page 2 resolves from the pin rather
    /// than from what the redeeming call happened to observe.
    #[test]
    fn an_unchanged_erasure_set_redeems() {
        let tenant = TenantHash([0x9Fu8; 16]);
        let key = test_key();
        let cursor = sample_cursor(tenant);
        let token = cursor.encode(&key).expect("encodes");

        let mut duplicated = pinned_erasure(&cursor);
        duplicated.extend(pinned_erasure(&cursor));
        let in_force_sets = [pinned_erasure(&cursor), Vec::new(), duplicated];
        assert_eq!(in_force_sets.len(), 3);

        for in_force in &in_force_sets {
            let redeemed = redeem_both(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                in_force,
                NOW_NS,
                FAR_HORIZON_NS,
            )
            .expect("an unchanged erasure set must redeem");
            assert_eq!(redeemed, cursor);
            assert_eq!(redeemed.min_commit_watermark, cursor.min_commit_watermark);
            assert_eq!(redeemed.pending_erasure, cursor.pending_erasure);
        }
    }

    /// The compaction case of the amendment, through the deadline re-clamp
    /// that already implements it and not through a second mechanism. A pin
    /// is protected until `protection_horizon - grace`; past that instant a
    /// sweep may take apart what the cursor resolves to, so the cursor is
    /// `Expired` however live its own embedded deadline still is.
    ///
    /// The embedded deadline is 29.5 s past `NOW_NS` here and is asserted so,
    /// which is what makes the horizon the only thing that can produce the
    /// refusal. One nanosecond of horizon either side of the boundary
    /// separates the two outcomes.
    #[test]
    fn a_cursor_past_the_protection_horizon_is_expired() {
        let tenant = TenantHash([0xA0u8; 16]);
        let key = test_key();
        let cursor = sample_cursor(tenant);
        let token = cursor.encode(&key).expect("encodes");
        assert_eq!(cursor.deadline_ns, NOW_NS + 29_500_000_000);

        let horizon = NOW_NS + GRACE_NS;
        assert_eq!(effective_deadline_ns(cursor.deadline_ns, horizon), NOW_NS);
        let err = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
            NOW_NS,
            horizon,
        )
        .expect_err("a pin no longer inside the protection horizon must be refused");
        assert_eq!(err, CursorError::Expired);

        let redeemed = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
            NOW_NS,
            horizon + 1,
        )
        .expect("one nanosecond of horizon later, the pin is still protected");
        assert_eq!(redeemed.deadline_ns, NOW_NS + 1);
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
            ingest_watermark_hour: Some(472_223),
            position: CursorPosition::Keyset {
                tuple: vec![7, 8, 9, 10],
                order_by: vec!["timestamp desc".to_owned(), "trace_id asc".to_owned()],
            },
            mint_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
        };

        let token = cursor.encode(&key).expect("encodes");
        let decoded = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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
        let decoded = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

    /// Version 5 added the pinned ingest watermark hour, so a version 4 token
    /// lacks a field this layout reads: it is refused at the header, as
    /// [`a_version_3_token_is_invalid`] refuses the layout before it. Reminted
    /// under this process's key, so the refusal is the version byte's and not
    /// the MAC's, and checked through both stages' entry point as well as
    /// `decode`.
    ///
    /// FLIP: with `CURSOR_VERSION` left at 4 (the pre-change code) the token
    /// decodes. FLIP: a version check that accepts any version up to its own
    /// (`> version` in `open_token`) decodes it too.
    #[test]
    fn a_version_4_token_is_invalid() {
        let tenant = TenantHash([0x60u8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");
        assert!(
            Cursor::decode(&token, &key).is_ok(),
            "the unedited token decodes, so the edit below is what refuses it"
        );

        let mut bytes = token_bytes(&token);
        assert_eq!(bytes[CURSOR_MAGIC.len()], CURSOR_VERSION);
        bytes[CURSOR_MAGIC.len()] = 4;
        let stale = remint(bytes, &key);

        let err = Cursor::decode(&stale, &key).expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);
        let err = Cursor::redeem(
            &stale,
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

    /// A cursor is bound to the tool that minted it: handing a
    /// `ravel_search_logs` cursor to `ravel_query_promql` is `Invalid`, the
    /// same variant (and therefore the same message) a forged token gets.
    #[test]
    fn cursor_redeemed_by_another_tool_is_refused() {
        let tenant = TenantHash([0x3Au8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let err = redeem_both(
            &token,
            &key,
            tenant,
            "ravel_query_promql",
            &SAMPLE_ARGS,
            NO_ERASURE,
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
        let err = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &other,
            NO_ERASURE,
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
        let err = redeem_both(
            &token,
            &redeeming_key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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
        let err = redeem_both(
            &over,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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
        let err = redeem_both(
            &at_cap,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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
            let err = redeem_both(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                NO_ERASURE,
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
        let err = redeem_both(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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
    /// [`a_version_4_token_is_invalid`] and [`a_version_3_token_is_invalid`]
    /// cover the earlier layouts.
    #[test]
    fn wrong_version_token_is_cursor_invalid() {
        let tenant = TenantHash([0x4Du8; 16]);
        let key = test_key();
        let token = sample_cursor(tenant).encode(&key).expect("encodes");

        let mut bytes = token_bytes(&token);
        bytes[4] = CURSOR_VERSION + 1;
        let reminted = remint(bytes, &key);

        let err = redeem_both(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

        let err = redeem_both(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

        let err = redeem_both(
            &reminted,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

        let redeemed = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
            NOW_NS,
            horizon,
        )
        .expect("inside the clamped deadline");
        assert_eq!(redeemed.deadline_ns, clamped);
        assert_ne!(redeemed.deadline_ns, embedded);
        let pending = Cursor::redeem(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            horizon,
        )
        .expect("inside the clamped deadline");
        assert_eq!(
            pending.deadline_ns(),
            clamped,
            "stage one already reports the clamped deadline"
        );

        let err = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
            clamped,
            horizon,
        )
        .expect_err("the clamped deadline has passed");
        assert_eq!(err, CursorError::Expired);

        let still_live = redeem_both(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NO_ERASURE,
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

    /// Stage one for a token minted with the sample tool and arguments, at
    /// `NOW_NS` and far inside the protection horizon.
    fn pending_for(token: &str, key: &CursorKey, tenant: TenantHash) -> PendingRedemption {
        Cursor::redeem(
            token,
            key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect("stage one passes")
    }

    /// The pinned ingest watermark hour survives the round trip in all three
    /// shapes: a real hour, hour 0 (which a zero-as-unset encoding would read
    /// back as `None`), and `None` (an empty resolve); `Some` costs exactly
    /// its tag and four bytes over `None`. A byte of the hour flipped without
    /// re-signing is `Invalid`; re-signed, the same edit decodes as the
    /// neighbouring hour, so the MAC and nothing in the field's own parse is
    /// what refused it.
    ///
    /// FLIP: an encode that never writes the field (decode always `None`)
    /// fails the `Some` cases. FLIP: a bare `u32` with zero as unset fails the
    /// `Some(0)` case. FLIP: the field appended after the MAC tag as a fixed
    /// five-byte trailer that `decode` strips before verifying decodes the
    /// tampered token, even with the length assertion removed.
    #[test]
    fn the_pinned_ingest_watermark_round_trips_and_is_under_the_mac() {
        let tenant = TenantHash([0x61u8; 16]);
        let key = test_key();
        for hour in [Some(472_222u32), Some(0), None] {
            let mut cursor = sample_cursor(tenant);
            cursor.ingest_watermark_hour = hour;
            let token = cursor.encode(&key).expect("encodes");
            assert!(token.len() <= 4096, "inside the envelope's cursor bound");
            let redeemed = redeem_both(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                NO_ERASURE,
                NOW_NS,
                FAR_HORIZON_NS,
            )
            .expect("round-trips through its own codec");
            assert_eq!(redeemed.ingest_watermark_hour, hour);
            assert_eq!(redeemed, cursor);
        }

        let encoded_len = |hour: Option<u32>| {
            let mut cursor = sample_cursor(tenant);
            cursor.ingest_watermark_hour = hour;
            token_bytes(&cursor.encode(&key).expect("encodes")).len()
        };
        assert_eq!(encoded_len(Some(0)), encoded_len(None) + 4);

        let cursor = sample_cursor(tenant);
        assert_eq!(cursor.ingest_watermark_hour, Some(472_222));
        let token = cursor.encode(&key).expect("encodes");
        let mut bytes = token_bytes(&token);
        let mut needle = vec![1u8];
        needle.extend_from_slice(&472_222u32.to_le_bytes());
        let at = bytes
            .windows(needle.len())
            .position(|window| window == needle.as_slice())
            .expect("the present tag and the pinned hour are in the payload");
        assert!(at > HEADER_LEN, "must edit the body, not the header");
        bytes[at + 1] ^= 0x01;

        let tampered = URL_SAFE_NO_PAD.encode(&bytes);
        let err = Cursor::decode(&tampered, &key).expect_err("must be refused");
        assert_eq!(err, CursorError::Invalid);

        let reminted = remint(bytes, &key);
        let decoded = Cursor::decode(&reminted, &key).expect("re-signed, the edit parses");
        assert_eq!(decoded.ingest_watermark_hour, Some(472_223));
    }

    /// Stage one reads nothing, and the proof is its signature: the coercion
    /// below names every parameter it takes (the token, the key, the three
    /// bindings, a clock and a horizon) and fails to compile if a store, a
    /// catalog or an erasure set is added, and this crate has no object-store
    /// or catalog dependency to read through. Its result carries the pinned
    /// resolve inputs, and stage two hands back the whole cursor when the
    /// resolve observed what was pinned.
    ///
    /// FLIP: the pre-change single-stage `redeem`, which takes
    /// `erasure_in_force` before any resolve, fails the coercion with E0308.
    /// FLIP: a stage one returning the `Cursor` itself instead of the pending
    /// wrapper fails it the same way.
    #[test]
    fn stage_one_takes_no_store_and_exposes_the_resolve_inputs() {
        type StageOne = fn(
            &str,
            &CursorKey,
            TenantHash,
            &str,
            &[u8; 32],
            i64,
            i64,
        ) -> Result<PendingRedemption, CursorError>;
        let stage_one: StageOne = Cursor::redeem;

        let tenant = TenantHash([0x62u8; 16]);
        let key = test_key();
        let cursor = sample_cursor(tenant);
        let token = cursor.encode(&key).expect("encodes");

        let pending = stage_one(
            &token,
            &key,
            tenant,
            SAMPLE_TOOL,
            &SAMPLE_ARGS,
            NOW_NS,
            FAR_HORIZON_NS,
        )
        .expect("stage one passes");
        assert_eq!(pending.signal(), cursor.signal);
        assert_eq!(pending.range_start_ns(), cursor.range_start_ns);
        assert_eq!(pending.range_end_ns(), cursor.range_end_ns);
        assert_eq!(
            pending.min_commit_watermark(),
            cursor.min_commit_watermark.as_slice()
        );
        assert_eq!(pending.deadline_ns(), cursor.deadline_ns);

        let redeemed = pending
            .confirm(NOW_NS, &pinned_erasure(&cursor), &cursor.declared_columns)
            .expect("nothing moved");
        assert_eq!(redeemed, cursor);
    }

    /// Stage two's erasure check, called directly on a pending redemption:
    /// a newer windowless predicate paired with the cursor's own signal is
    /// `Expired`; the same predicate paired with any other signal redeems, and
    /// so does a newer one on the cursor's signal whose window ends where the
    /// range starts. The pinned predicate still pending redeems too.
    ///
    /// FLIP: a check that ignores the signal (drop `*signal == self.signal`
    /// in `newer_erasure_intersects`) expires the other-signal entries. FLIP:
    /// a check that ignores the window (`window_intersects` always true)
    /// expires the out-of-range one. FLIP: a check that ignores the pin (drop
    /// the `contains`) expires the pinned one.
    #[test]
    fn stage_two_expires_only_a_newer_erasure_over_the_pinned_scope() {
        let tenant = TenantHash([0x63u8; 16]);
        let key = test_key();
        let cursor = sample_cursor(tenant);
        let token = cursor.encode(&key).expect("encodes");
        assert_eq!(cursor.signal, Signal::Logs);

        let user = vec![("user.id".to_owned(), "u-42".to_owned())];
        let newer = ErasurePredicate::windowless(user.clone());

        let err = pending_for(&token, &key, tenant)
            .confirm(
                NOW_NS,
                &[(Signal::Logs, newer.clone())],
                &cursor.declared_columns,
            )
            .expect_err("a newer erasure over the pinned signal and range");
        assert_eq!(err, CursorError::Expired);

        for other in [
            Signal::Metrics,
            Signal::Spans,
            Signal::Profiles,
            Signal::Alerts,
            Signal::Audit,
        ] {
            assert_ne!(other, cursor.signal);
            let redeemed = pending_for(&token, &key, tenant)
                .confirm(NOW_NS, &[(other, newer.clone())], &cursor.declared_columns)
                .expect("an erasure on another signal cannot touch this page");
            assert_eq!(redeemed, cursor);
        }

        let before = ErasurePredicate::new(
            user.clone(),
            cursor.range_start_ns - 3_600_000_000_000,
            cursor.range_start_ns,
        );
        let redeemed = pending_for(&token, &key, tenant)
            .confirm(NOW_NS, &[(cursor.signal, before)], &cursor.declared_columns)
            .expect("an erasure wholly before the range cannot touch this page");
        assert_eq!(redeemed, cursor);

        let redeemed = pending_for(&token, &key, tenant)
            .confirm(NOW_NS, &pinned_erasure(&cursor), &cursor.declared_columns)
            .expect("the pinned predicate still pending is not newer");
        assert_eq!(redeemed, cursor);
    }

    /// A declared column set observed at redemption that differs from the
    /// pinned one is `Expired` (the 2026-09-12 amendment, item 3): one column
    /// added, one removed, one retyped, one renamed. The same set in another
    /// order, and the pinned set itself, redeem.
    ///
    /// FLIP: comparing by count passes the retyped and renamed sets. FLIP:
    /// comparing keys only passes the retyped set. FLIP: comparing the lists
    /// in order (plain `Vec` equality) refuses the reordered set.
    #[test]
    fn a_changed_declared_column_set_is_expired() {
        let tenant = TenantHash([0x64u8; 16]);
        let key = test_key();
        let status = DeclaredColumn::new("http.status_code", DeclaredType::I64);
        let route = DeclaredColumn::new("http.route", DeclaredType::Str);
        let mut cursor = sample_cursor(tenant);
        cursor.declared_columns = vec![status.clone(), route.clone()];
        let token = cursor.encode(&key).expect("encodes");

        let changed = [
            (
                "added",
                vec![
                    status.clone(),
                    route.clone(),
                    DeclaredColumn::new("error", DeclaredType::Bool),
                ],
            ),
            ("removed", vec![status.clone()]),
            (
                "retyped",
                vec![
                    DeclaredColumn::new("http.status_code", DeclaredType::Str),
                    route.clone(),
                ],
            ),
            (
                "renamed",
                vec![
                    status.clone(),
                    DeclaredColumn::new("http.method", DeclaredType::Str),
                ],
            ),
        ];
        for (what, observed) in &changed {
            let err = pending_for(&token, &key, tenant)
                .confirm(NOW_NS, &pinned_erasure(&cursor), observed)
                .expect_err(what);
            assert_eq!(err, CursorError::Expired, "{what}");
        }

        for observed in [
            vec![route.clone(), status.clone()],
            vec![status.clone(), route.clone()],
        ] {
            let redeemed = pending_for(&token, &key, tenant)
                .confirm(NOW_NS, &pinned_erasure(&cursor), &observed)
                .expect("the same set redeems in any order");
            assert_eq!(redeemed, cursor);
        }
    }

    /// The redeeming resolve runs at the cursor's `mint_ns`, whatever the
    /// redeeming call's clock reads: two redemptions a second apart report
    /// the same instant. `mint_ns` is moved off every other instant the
    /// cursor carries (in `sample_cursor` it equals `range_end_ns`), so only
    /// the mint instant can satisfy the assertion.
    ///
    /// FLIP: a `resolve_at_ns` returning stage one's `now_ns` reads `NOW_NS`.
    /// FLIP: one returning `range_end_ns` or `deadline_ns` reads that instead.
    #[test]
    fn the_redeeming_resolve_runs_at_the_mint_instant() {
        let tenant = TenantHash([0x65u8; 16]);
        let key = test_key();
        let mut cursor = sample_cursor(tenant);
        cursor.mint_ns = 1_700_000_000_250_000_000;
        let token = cursor.encode(&key).expect("encodes");
        for other in [
            cursor.range_start_ns,
            cursor.range_end_ns,
            cursor.deadline_ns,
            NOW_NS,
        ] {
            assert_ne!(cursor.mint_ns, other);
        }

        for now_ns in [NOW_NS, NOW_NS + 1_000_000_000] {
            let pending = Cursor::redeem(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                now_ns,
                FAR_HORIZON_NS,
            )
            .expect("stage one passes");
            assert_eq!(pending.resolve_at_ns(), cursor.mint_ns, "now {now_ns}");
        }
    }

    /// The seven inputs of [`snapshot_id`], held together so a test can vary
    /// exactly one.
    #[derive(Clone)]
    struct SnapshotInputs {
        tenant: TenantHash,
        signal: Signal,
        range_start_ns: i64,
        range_end_ns: i64,
        watermark: Vec<CommitToken>,
        erasure: Vec<ErasurePredicate>,
        declared: Vec<DeclaredColumn>,
        resolve_at_ns: i64,
    }

    impl SnapshotInputs {
        fn id(&self) -> [u8; 32] {
            snapshot_id(
                self.tenant,
                self.signal,
                self.range_start_ns,
                self.range_end_ns,
                &self.watermark,
                &self.erasure,
                &self.declared,
                self.resolve_at_ns,
            )
        }
    }

    fn sample_snapshot_inputs() -> SnapshotInputs {
        let cursor = sample_cursor(TenantHash([0x70u8; 16]));
        SnapshotInputs {
            tenant: cursor.tenant,
            signal: cursor.signal,
            range_start_ns: cursor.range_start_ns,
            range_end_ns: cursor.range_end_ns,
            watermark: cursor.min_commit_watermark,
            erasure: cursor.pending_erasure,
            declared: cursor.declared_columns,
            resolve_at_ns: cursor.mint_ns,
        }
    }

    /// Equal inputs derive equal ids, and changing any one of the seven
    /// inputs changes the id (the time range checked at each bound).
    ///
    /// FLIP: a derivation that omits the tenant fails the tenant assertion,
    /// which is two tenants sharing an id over disjoint data. FLIP: one that
    /// omits the resolve instant fails that assertion, which is two calls a
    /// day apart sharing an id.
    #[test]
    fn snapshot_id_is_deterministic_and_separates_each_input() {
        let base = sample_snapshot_inputs();
        assert_eq!(base.id(), base.clone().id());
        assert_eq!(base.id(), sample_snapshot_inputs().id());

        let mut tenant = base.clone();
        tenant.tenant = TenantHash([0x71u8; 16]);
        assert_ne!(tenant.id(), base.id(), "tenant");

        let mut signal = base.clone();
        signal.signal = Signal::Metrics;
        assert_ne!(signal.id(), base.id(), "signal");

        let mut start = base.clone();
        start.range_start_ns += 1;
        assert_ne!(start.id(), base.id(), "range start");
        let mut end = base.clone();
        end.range_end_ns += 1;
        assert_ne!(end.id(), base.id(), "range end");

        let mut watermark = base.clone();
        watermark.watermark[0].seq += 1;
        assert_ne!(watermark.id(), base.id(), "commit-token watermark");

        let mut erasure = base.clone();
        erasure.erasure.push(ErasurePredicate::windowless(vec![(
            "user.id".to_owned(),
            "u-42".to_owned(),
        )]));
        assert_ne!(erasure.id(), base.id(), "pending erasure");

        let mut declared = base.clone();
        declared.declared[0].ty = DeclaredType::Str;
        assert_ne!(declared.id(), base.id(), "declared columns");

        let mut resolve_at = base.clone();
        resolve_at.resolve_at_ns += 24 * 3_600 * 1_000_000_000;
        assert_ne!(resolve_at.id(), base.id(), "resolve instant");
    }

    /// A redemption that derives its id from the confirmed cursor's pinned
    /// inputs and [`PendingRedemption::resolve_at_ns`] reports the id page one
    /// derived at its mint, whatever the redeeming clock reads.
    ///
    /// FLIP: a `resolve_at_ns` returning stage one's `now_ns` derives a
    /// different id. FLIP: one returning `deadline_ns` does too. FLIP: a
    /// `confirm` that drops the pinned erasure set from the cursor it hands
    /// back derives a different id.
    #[test]
    fn a_redemption_reports_page_ones_snapshot_id() {
        let tenant = TenantHash([0x66u8; 16]);
        let key = test_key();
        let mut cursor = sample_cursor(tenant);
        cursor.mint_ns = 1_700_000_000_250_000_000;
        let page_one = snapshot_id(
            cursor.tenant,
            cursor.signal,
            cursor.range_start_ns,
            cursor.range_end_ns,
            &cursor.min_commit_watermark,
            &cursor.pending_erasure,
            &cursor.declared_columns,
            cursor.mint_ns,
        );
        let token = cursor.encode(&key).expect("encodes");

        let pending = pending_for(&token, &key, tenant);
        assert_ne!(NOW_NS, cursor.mint_ns);
        let resolve_at_ns = pending.resolve_at_ns();
        let redeemed = pending
            .confirm(NOW_NS, &pinned_erasure(&cursor), &cursor.declared_columns)
            .expect("nothing moved");
        let page_two = |c: &Cursor| {
            snapshot_id(
                c.tenant,
                c.signal,
                c.range_start_ns,
                c.range_end_ns,
                &c.min_commit_watermark,
                &c.pending_erasure,
                &c.declared_columns,
                resolve_at_ns,
            )
        };
        assert_eq!(page_two(&redeemed), page_one);
    }

    /// Every variable-length value is length-prefixed: two inputs whose
    /// values concatenate to the same bytes derive different ids. Each pair
    /// is asserted to collide under its naive encoding first (plain
    /// concatenation for the first matcher pair and the declared sets, a NUL
    /// separator for the second matcher pair), so the pair is a real
    /// collision and not one the length prefix is never needed for.
    ///
    /// FLIP: plain concatenation (drop the length writes in
    /// `erasure_predicate_bytes` and `declared_column_bytes`) collides every
    /// pair. FLIP: joining the matcher key and value with a NUL byte instead
    /// of prefixing them collides the second pair.
    #[test]
    fn snapshot_id_length_prefixes_every_variable_length_value() {
        let base = sample_snapshot_inputs();
        let matcher = |k: &str, v: &str| ErasurePredicate::windowless(vec![(k.into(), v.into())]);

        for (left, right, separator) in [
            (("ab", "c"), ("a", "bc"), ""),
            (("a\0b", "c"), ("a", "b\0c"), "\0"),
        ] {
            assert_eq!(
                [left.0, separator, left.1].concat(),
                [right.0, separator, right.1].concat(),
                "joining with {separator:?} collides"
            );
            let mut a = base.clone();
            a.erasure = vec![matcher(left.0, left.1)];
            let mut b = base.clone();
            b.erasure = vec![matcher(right.0, right.1)];
            assert_ne!(a.id(), b.id(), "{left:?} against {right:?}");
        }

        let two = vec![
            DeclaredColumn::new("a", DeclaredType::Str),
            DeclaredColumn::new("b", DeclaredType::I64),
        ];
        let one = vec![DeclaredColumn::new("a\u{1}b", DeclaredType::I64)];
        let naive = |columns: &[DeclaredColumn]| -> Vec<u8> {
            columns
                .iter()
                .flat_map(|c| {
                    let mut out = c.key.as_bytes().to_vec();
                    out.push(declared_type_tag(c.ty));
                    out
                })
                .collect()
        };
        assert_eq!(naive(&two), naive(&one), "plain concatenation collides");
        let mut a = base.clone();
        a.declared = two;
        let mut b = base.clone();
        b.declared = one;
        assert_ne!(a.id(), b.id());
    }

    /// The three lists are sets: reordering one, or naming an element twice,
    /// does not change the id, so two resolves that list the same inputs in
    /// different orders report one snapshot.
    ///
    /// FLIP: hashing each list in the order given fails the reordered cases.
    /// FLIP: sorting without deduplicating fails the duplicated one.
    #[test]
    fn snapshot_id_reads_each_list_as_a_set() {
        let mut base = sample_snapshot_inputs();
        base.watermark = vec![sample_token(0), sample_token(1)];
        base.erasure.push(ErasurePredicate::new(
            vec![("user.id".to_owned(), "u-42".to_owned())],
            1,
            2,
        ));
        base.declared
            .push(DeclaredColumn::new("http.route", DeclaredType::Str));

        let mut reordered = base.clone();
        reordered.watermark.reverse();
        reordered.erasure.reverse();
        reordered.declared.reverse();
        assert_ne!(reordered.watermark, base.watermark);
        assert_ne!(reordered.erasure, base.erasure);
        assert_ne!(reordered.declared, base.declared);
        assert_eq!(reordered.id(), base.id());

        let mut duplicated = base.clone();
        duplicated.declared.push(base.declared[0].clone());
        assert_eq!(duplicated.id(), base.id());
    }

    /// Stage one accepts a token whose deadline is still ahead; a resolve
    /// that finishes at or past that deadline is refused by stage two, and
    /// one that finishes a nanosecond before it is not.
    ///
    /// FLIP: a `confirm` that ignores `now_ns` (stage two trusting stage
    /// one's deadline check) returns the cursor on the late resolve.
    /// FLIP: testing `now_ns > deadline_ns` instead of `>=` accepts the
    /// resolve that lands exactly on the deadline, which stage one refuses.
    #[test]
    fn confirm_refuses_a_resolve_that_finished_past_the_deadline() {
        let key = test_key();
        let tenant = TenantHash([0xAAu8; 16]);
        let cursor = sample_cursor(tenant);
        let token = cursor.encode(&key).expect("encodes");
        let declared = cursor.declared_columns.clone();
        let redeem = || {
            Cursor::redeem(
                &token,
                &key,
                tenant,
                SAMPLE_TOOL,
                &SAMPLE_ARGS,
                NOW_NS,
                FAR_HORIZON_NS,
            )
            .expect("stage one accepts a token before its deadline")
        };
        assert!(NOW_NS < cursor.deadline_ns);

        assert_eq!(
            redeem()
                .confirm(cursor.deadline_ns, NO_ERASURE, &declared)
                .expect_err("a resolve finishing on the deadline is refused"),
            CursorError::Expired
        );
        assert_eq!(
            redeem()
                .confirm(cursor.deadline_ns + 1, NO_ERASURE, &declared)
                .expect_err("a resolve finishing past the deadline is refused"),
            CursorError::Expired
        );
        assert_eq!(
            redeem()
                .confirm(cursor.deadline_ns - 1, NO_ERASURE, &declared)
                .expect("a resolve finishing before the deadline confirms"),
            cursor
        );
    }

    /// `Cursor::mint` clamps the embedded deadline to `mint_ns` plus the
    /// configured horizon less `GRACE_NS`, keeps a shorter one as it is, and
    /// refuses a cursor whose clamped deadline is already behind `now_ns`.
    /// The horizon here is 25 h 05 m measured from `now_ns`, the default, so
    /// the bound is 1 h 05 m after the mint.
    ///
    /// FLIP: a `mint` that encodes `deadline_ns` unchanged decodes the
    /// year-long deadline in the second case.
    /// FLIP: a bound measured from `now_ns` rather than from `mint_ns`
    /// decodes to a deadline 500 ms later than the one asserted.
    /// FLIP: a `mint` that clamps every deadline to the bound, rather than
    /// taking the smaller of the two, lengthens the first case's.
    /// FLIP: a `mint` that never refuses returns a token that stage one
    /// would refuse on its first redemption.
    #[test]
    fn mint_enforces_the_cursor_lifetime_bound() {
        let key = test_key();
        let tenant = TenantHash([0xAAu8; 16]);
        let horizon_ns = 25 * 3_600 * 1_000_000_000 + 5 * 60 * 1_000_000_000;
        let horizon_at = NOW_NS + horizon_ns;
        let lifetime_ns = horizon_ns - GRACE_NS;
        assert_eq!(lifetime_ns, 65 * 60 * 1_000_000_000);

        let short = sample_cursor(tenant);
        assert!(short.deadline_ns < short.mint_ns + lifetime_ns);
        let token = short.mint(&key, NOW_NS, horizon_at).expect("mints");
        assert_eq!(
            Cursor::decode(&token, &key).expect("decodes").deadline_ns,
            short.deadline_ns,
            "a deadline inside the bound is kept"
        );

        let long = Cursor {
            deadline_ns: short.mint_ns + 365 * 24 * 3_600 * 1_000_000_000,
            ..short.clone()
        };
        let token = long.mint(&key, NOW_NS, horizon_at).expect("mints");
        let decoded = Cursor::decode(&token, &key).expect("decodes");
        assert_eq!(decoded.deadline_ns, long.mint_ns + lifetime_ns);
        assert_eq!(
            Cursor {
                deadline_ns: long.deadline_ns,
                ..decoded
            },
            long,
            "only the deadline changes"
        );

        let stale = Cursor {
            mint_ns: NOW_NS - lifetime_ns,
            deadline_ns: i64::MAX,
            ..short.clone()
        };
        assert_eq!(
            stale.mint(&key, NOW_NS, horizon_at),
            Err(CursorError::Expired),
            "a bound that is already behind now is refused"
        );
        assert_eq!(
            short.mint(&key, NOW_NS, NOW_NS + GRACE_NS),
            Err(CursorError::Expired),
            "a horizon no longer than the grace leaves no lifetime"
        );
    }

    /// Every `CursorError` variant maps to the D4 class its doc states, and
    /// carries its own message. The match below has no wildcard, so a new
    /// variant fails to compile here until it is given a row.
    ///
    /// FLIP: mapping `Expired` to `cursor_invalid` (the adapter doc's old
    /// claim for a restarted process's token) fails the first row.
    /// FLIP: mapping `FieldTooLong` to `cursor_invalid` blames the caller for
    /// a mint-time failure and fails the last row.
    #[test]
    fn cursor_error_failure_mapping_is_exhaustive() {
        let all = [
            CursorError::Expired,
            CursorError::Invalid,
            CursorError::TokenTooLong { len: 9, max: 8 },
            CursorError::FieldTooLong { len: 9, max: 8 },
        ];
        for error in all {
            let expected = match error {
                CursorError::Expired => FailureClass::CursorExpired,
                CursorError::Invalid => FailureClass::CursorInvalid,
                CursorError::TokenTooLong { .. } => FailureClass::CursorInvalid,
                CursorError::FieldTooLong { .. } => FailureClass::Internal,
            };
            let failure = error.failure();
            assert_eq!(failure.class, expected, "{error:?}");
            assert_eq!(failure.message, error.to_string());
            assert_eq!(failure.counter, None);
        }

        let key = test_key();
        let foreign = sample_cursor(TenantHash([0xAAu8; 16]))
            .encode(&CursorKey::from_process_secret([0x22u8; CURSOR_KEY_LEN]))
            .expect("encodes");
        let restarted = Cursor::decode(&foreign, &key).expect_err("foreign nonce");
        assert_eq!(restarted.failure().class, FailureClass::CursorExpired);
    }
}

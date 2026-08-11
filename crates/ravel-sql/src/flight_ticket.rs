//! Flight SQL snapshot-pinning ticket codec (ticket C1b, issue #150).
//!
//! # Why this exists (review F18, docs/arrow-datafusion-plan.md Phase C)
//!
//! Flight SQL splits one query across two RPCs: `GetFlightInfo` plans the
//! query and hands the client an opaque ticket; `DoGet` later redeems that
//! ticket to stream results. If `DoGet` re-resolved the snapshot it would
//! observe a *different* set of committed segments than `GetFlightInfo`
//! planned against, so the same query could return two different answers
//! across its own two RPCs. F18 fixes this by making the ticket carry the
//! exact resolved snapshot identity: `DoGet` executes against precisely the
//! snapshot `GetFlightInfo` pinned, never a re-resolution.
//!
//! This module is only the wire contract. It does not resolve snapshots,
//! implement `FlightSqlService`, or compare tenants; those live in
//! [`crate::flight`], which binds to this format (ticket C1d, issue #152).
//!
//! # Deadline and the GC protection horizon
//!
//! [`FlightTicket::deadline_ns`] is an absolute wall-clock nanosecond
//! timestamp past which the ticket is invalid ([`FlightTicket::is_expired`]).
//! The pin is only safe while the pinned segments still physically exist, so
//! the caller that mints a ticket MUST set the deadline no later than the GC
//! protection horizon (`protection_horizon >= max_query_duration + grace`,
//! docs/consistency-model.md "Deletion and GC"): a pinned segment is
//! guaranteed present until that horizon, so a ticket redeemed before its
//! deadline never races superseded-input or retention GC. This module does
//! not look up that horizon or any config; enforcing `deadline_ns <=
//! now_ns + protection_horizon` is the minting caller's job (the later C1
//! ticket owns `EngineConfig`). Here the deadline is an opaque bound the
//! codec round-trips and [`FlightTicket::is_expired`] checks.
//!
//! # Tenancy is not trusted from the ticket
//!
//! The ticket carries [`FlightTicket::tenant`] so `DoGet` can compare it
//! against the tenant it resolves from authoritative gRPC metadata and
//! reject on mismatch. The embedded field is the value to check against, not
//! a source of authority: the ticket is never a tenancy trust boundary. That
//! comparison is out of scope for this codec.
//!
//! # Encoding choice: manual little-endian byte layout, not prost
//!
//! A hand-rolled layout is used rather than a prost message. Rationale:
//!
//! - The ticket is ephemeral (bounded by `deadline_ns`), never a durable
//!   persisted object, so it is deliberately NOT a frozen format contract in
//!   the sense of docs (no ADR / `.proto` schema needed). A version byte
//!   allows a future field addition without a prost schema.
//! - It keeps this format out of `proto/`, where every schema is a frozen
//!   contract, and reuses [`ravel_types::CommitToken`]'s own canonical string
//!   codec for the min-commit-token inputs rather than restating it. (`prost`
//!   and `uuid` did arrive in the crate later, with the `flight-sql` feature's
//!   service and this codec's version 2 respectively; the layout stays
//!   hand-rolled because the version byte, not a schema registry, is what
//!   governs it.)
//! - A trailing keyed BLAKE3-256 MAC makes accidental corruption (a single
//!   flipped byte anywhere) a typed decode error rather than a silently
//!   different pin, and -- unlike a plain checksum -- also makes the ticket
//!   unforgeable by anyone who does not hold the minting process's secret key
//!   (issue #185; version 2 used an unkeyed FNV-1a-64 checksum any client
//!   could recompute after tampering with a field).
//!
//! Layout (all integers little-endian, lengths are `u32`):
//!
//! ```text
//! magic         4   b"RFT1"
//! version       1   = 4
//! tenant       16   TenantHash bytes
//! now_ns        8   i64
//! deadline_ns   8   i64
//! slice_index   4   u32   (this slice's 0-based index in the fan-out)
//! slice_count   4   u32   (number of slices; 1 for a whole-snapshot ticket)
//! token_count   4   u32
//!   per token:  4 + N   len-prefixed CommitToken::encode() (ASCII)
//! seg_count     4   u32
//!   per segment:
//!     writer_epoch       8   u64
//!     writer_seq         8   u64
//!     created_unix_ns    8   i64
//!     object_size        8   u64
//!     min_event_ts_ns    8   i64
//!     max_event_ts_ns    8   i64
//!     sample_count       8   u64
//!     series_count       8   u64
//!     ingest_hour_bucket 4   u32
//!     shard              4   u32
//!     content_hash      32   [u8; 32]
//!     writer_id         16   Uuid bytes
//!     key_len            4   u32
//!     key                N   data_object_key (UTF-8)
//!     level_tag          1   0 = L0, 1 = L1
//!       if L1:
//!         input_set_hash 32   [u8; 32]
//!         part_index      4   u32
//! stmt_len      4   u32   (<= MAX_STATEMENT_LEN)
//! stmt          N   statement text (UTF-8)
//! mac          32   keyed BLAKE3-256 over every preceding byte
//! ```
//!
//! # Integrity: a keyed MAC, not a checksum
//!
//! [`FlightTicket::encode`] and [`FlightTicket::decode`] both take a
//! [`TicketKey`]: a 32-byte secret generated once, in memory, when the
//! `FlightSqlService` is constructed, and never sent to a client or persisted.
//! The trailing tag is `blake3::keyed_hash(key, payload)`, not a plain hash of
//! the payload, so recomputing it requires the key. A client can still read
//! and replay a ticket verbatim (that is the protocol), but cannot flip a
//! single field -- extend `deadline_ns`, swap a `data_object_key`, change the
//! `tenant` -- and produce a tag the minting process will accept, which the
//! version-2 FNV-1a-64 checksum never prevented.
//!
//! # Segment identity fields (the judgment call from issue #150, settled by C1)
//!
//! Version 1 of this codec carried only `data_object_key`, `writer_epoch`,
//! and `content_hash`, and left open "whether `DoGet` re-resolves the full
//! `Snapshot` or reconstructs it from the ticket is the later C1 ticket's
//! decision. If it chooses full reconstruction, those fields are added under
//! a bumped `version` byte; the codec is built for that extension."
//!
//! C1 (issue #152) chose full reconstruction, so version 2 carries every
//! [`SegmentRef`] field and [`SegmentPin::to_segment_ref`] rebuilds the
//! resolved `Snapshot` exactly. The alternative -- re-resolving at `DoGet`
//! and intersecting against the pinned keys -- would have put a catalog LIST
//! on the redemption path and made the pin depend on a second resolve
//! observing the same committed state, which is the coupling F18 exists to
//! remove. The three original fields keep their original roles inside the
//! larger set:
//!
//! - `data_object_key` (required): locates the immutable object to fetch.
//! - `content_hash` (`[u8; 32]`): the stale/tampered-ticket signal. Data
//!   objects are immutable, so a segment that hashes differently than the
//!   ticket recorded means a stale or tampered ticket.
//! - `writer_epoch`: a provenance witness and the second component of the
//!   cross-segment dedup total order (`created_unix_ns`, `writer_epoch`,
//!   `writer_seq`, in-page index) the rebuilt snapshot must reproduce
//!   byte-for-byte, which is why the remaining provenance fields are now
//!   carried too: a snapshot missing them would dedup differently than the
//!   HTTP path did over the same segments.
//!
//! Version 1 and version 2 tickets are rejected
//! ([`FlightTicketError::UnsupportedVersion`] or, for version 2's
//! differently-shaped trailing checksum, a MAC or length mismatch), not
//! upgraded. They are ephemeral by construction -- no ticket outlives its
//! `deadline_ns`, and nothing on any released path ever minted one -- so
//! there is no compatibility window to preserve. Version 3 (issue #185)
//! replaces the unkeyed FNV-1a-64 checksum with a keyed BLAKE3 MAC; see
//! "Integrity: a keyed MAC, not a checksum" above.
//!
//! Version 4 (issue #866, ADR-0071 distributed read fan-out) carries a
//! [`slice_index`](FlightTicket::slice_index) /
//! [`slice_count`](FlightTicket::slice_count) pair so a ticket can pin a
//! *slice* of the resolved snapshot (a subset of segments) rather than the
//! whole set: `GetFlightInfo` fans a distributed query out to N endpoints,
//! each carrying one slice's segments and its `(index, count)` position in
//! the fan-out. A whole-snapshot ticket sets `slice_index = 0`,
//! `slice_count = 1`. The pair also gives a mixed-version rolling deploy the
//! reject-unknown safety ADR-0071 requires: a coordinator minting v4 slice
//! tickets and a worker still on the v3 codec cannot silently misread one
//! layout as the other, because the version byte differs and each side
//! rejects the other's version rather than reinterpreting its bytes. The
//! envelope stays a transient wire token bounded by `deadline_ns`, never a
//! persisted format, so the field is threaded into the layout under a bumped
//! version byte exactly as the earlier extensions were.
//!
//! Because a ticket is ephemeral and never persisted, a new [`SegmentRef`]
//! field is threaded into the current version's layout in place rather than
//! behind a fresh version byte: there is no older-version ticket in flight to
//! stay compatible with, and any that somehow were would fail the MAC or
//! length check regardless. Issue #394 added the per-segment `level_tag`
//! (L0 vs L1, with an L1 part's `input_set_hash`/`part_index`) this way, so
//! [`SegmentPin::to_segment_ref`] reconstructs the level and a rebuilt L1 part
//! is verified against the v4 footer contract, not read as an L0 segment.

use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_types::{CommitToken, TenantHash};
use uuid::Uuid;

/// Maximum accepted SQL statement length, in bytes. 64 KiB. Longer
/// statements are rejected at [`FlightTicket::encode`] time and refused at
/// [`FlightTicket::decode`] time.
pub const MAX_STATEMENT_LEN: usize = 64 * 1024;

const MAGIC: [u8; 4] = *b"RFT1";
const VERSION: u8 = 4;

/// Length in bytes of the trailing keyed-MAC tag ([`mac`]).
const MAC_LEN: usize = 32;

/// Length in bytes of the secret key [`FlightTicket::encode`] and
/// [`FlightTicket::decode`] are keyed by.
pub const TICKET_KEY_LEN: usize = 32;

/// The secret, in-process MAC key a ticket is signed and verified with.
///
/// Generated once when the `FlightSqlService` is constructed (see
/// `crate::flight::service`) and held only in memory: it is never logged,
/// sent to a client, or persisted. A process restart mints a fresh key, which
/// is safe because a ticket is ephemeral by construction and never expected
/// to outlive the process that minted it.
pub type TicketKey = [u8; TICKET_KEY_LEN];

/// Smallest possible encoded ticket: the fixed header (including the
/// `slice_index`/`slice_count` pair) plus the trailing MAC, with zero tokens,
/// zero segments, and an empty statement.
const MIN_ENCODED_LEN: usize = 4 + 1 + 16 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + MAC_LEN;

/// One pinned segment inside a [`FlightTicket`]: the wire mirror of a
/// resolved [`SegmentRef`].
///
/// Deliberately a separate type rather than `SegmentRef` itself. This is a
/// wire layout with its own version byte; `SegmentRef` is an in-memory
/// catalog struct that may gain or reorder fields. Keeping them distinct
/// means a `SegmentRef` change surfaces as a compile error in
/// [`SegmentPin::from_segment_ref`]/[`SegmentPin::to_segment_ref`] and a
/// deliberate version bump, never as a silently different pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPin {
    /// Data-object key, as reconstructed by the catalog into
    /// [`SegmentRef::data_object_key`].
    pub data_object_key: String,
    /// Encoded object size in bytes.
    pub object_size: u64,
    /// Smallest event timestamp in the segment. Feeds segment pruning.
    pub min_event_ts_ns: i64,
    /// Largest event timestamp in the segment. Feeds segment pruning.
    pub max_event_ts_ns: i64,
    /// Ingest-hour bucket pinned at flush open (unix hours).
    pub ingest_hour_bucket: u32,
    /// Sample count recorded in the commit record.
    pub sample_count: u64,
    /// Series count recorded in the commit record.
    pub series_count: u64,
    /// Ingest shard that produced the segment.
    pub shard: u32,
    /// Whole-object content hash recorded in the commit record. Verified
    /// against the fetched object to detect a stale or tampered ticket.
    pub content_hash: [u8; 32],
    /// Writer that produced the segment. Final tiebreak of the snapshot's
    /// deterministic iteration order.
    pub writer_id: Uuid,
    /// Writer epoch that produced the segment. Provenance witness and second
    /// component of the dedup total order.
    pub writer_epoch: u64,
    /// Writer sequence number. Third component of the dedup total order.
    pub writer_seq: u64,
    /// Wall-clock the commit record was created. First component of the
    /// dedup total order.
    pub created_unix_ns: i64,
    /// L0 vs L1 discriminator, mirrored from [`SegmentRef::level`]. Determines
    /// how `DoGet` verifies the segment footer and how the ref sorts into the
    /// mixed-level snapshot order, so it must be pinned like every other
    /// identity field: rebuilding the snapshot without it would reconstruct an
    /// L1 part as if it were L0 (or vice versa) and read it against the wrong
    /// footer contract.
    pub level: SegmentLevel,
}

impl SegmentPin {
    /// Project a resolved [`SegmentRef`] onto the wire layout.
    pub fn from_segment_ref(seg: &SegmentRef) -> Self {
        SegmentPin {
            data_object_key: seg.data_object_key.clone(),
            object_size: seg.object_size,
            min_event_ts_ns: seg.min_event_ts_ns,
            max_event_ts_ns: seg.max_event_ts_ns,
            ingest_hour_bucket: seg.ingest_hour_bucket,
            sample_count: seg.sample_count,
            series_count: seg.series_count,
            shard: seg.shard,
            content_hash: seg.content_hash,
            writer_id: seg.writer_id,
            writer_epoch: seg.writer_epoch,
            writer_seq: seg.writer_seq,
            created_unix_ns: seg.created_unix_ns,
            level: seg.level.clone(),
        }
    }

    /// Rebuild the [`SegmentRef`] this pin was projected from.
    ///
    /// Total and lossless: version 2 carries every `SegmentRef` field, which
    /// is what lets `DoGet` reconstruct the resolved `Snapshot` without a
    /// second `Catalog::resolve`.
    pub fn to_segment_ref(&self) -> SegmentRef {
        SegmentRef {
            data_object_key: self.data_object_key.clone(),
            object_size: self.object_size,
            min_event_ts_ns: self.min_event_ts_ns,
            max_event_ts_ns: self.max_event_ts_ns,
            ingest_hour_bucket: self.ingest_hour_bucket,
            sample_count: self.sample_count,
            series_count: self.series_count,
            shard: self.shard,
            content_hash: self.content_hash,
            writer_id: self.writer_id,
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
            created_unix_ns: self.created_unix_ns,
            level: self.level.clone(),
        }
    }
}

/// A self-describing, snapshot-pinning Flight SQL ticket.
///
/// Round-trips bit-for-bit through [`FlightTicket::encode`] /
/// [`FlightTicket::decode`]. See the module docs for the wire layout and the
/// security posture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightTicket {
    /// Tenant the ticket was minted for. Compared against the authoritative
    /// gRPC-metadata tenant at `DoGet`; never trusted as authority here.
    pub tenant: TenantHash,
    /// The single read-only SQL statement, capped at [`MAX_STATEMENT_LEN`].
    pub statement: String,
    /// The pinned segment set: exactly the snapshot `GetFlightInfo` resolved.
    pub segments: Vec<SegmentPin>,
    /// `min_commit_token` read-your-write inputs passed to `Catalog::resolve`.
    pub min_commit_tokens: Vec<CommitToken>,
    /// Injected `now_ns` that bounded the resolve listing window.
    pub now_ns: i64,
    /// Absolute wall-clock nanosecond deadline; at or after this the ticket
    /// is expired. Always set by the minter to at most the GC protection
    /// horizon (see module docs).
    pub deadline_ns: i64,
    /// This ticket's 0-based position in a distributed fan-out (ADR-0071,
    /// issue #866). A whole-snapshot ticket carries `0`. Carried so a
    /// coordinator/worker can identify and log which slice a ticket pins; it
    /// is not a trust boundary and `snapshot()` ignores it.
    pub slice_index: u32,
    /// Number of slices the resolved snapshot was fanned out into; `1` for a
    /// whole-snapshot ticket. With [`slice_index`](Self::slice_index) this
    /// makes `segments` an identified *slice* of the pinned set rather than
    /// the whole set.
    pub slice_count: u32,
}

impl FlightTicket {
    /// Encode to the wire layout documented on this module, signed with
    /// `key`.
    ///
    /// Returns [`FlightTicketError::StatementTooLong`] if the statement
    /// exceeds [`MAX_STATEMENT_LEN`], and [`FlightTicketError::FieldTooLong`]
    /// if any single length field would not fit in a `u32` (not reachable
    /// with real keys or tokens).
    pub fn encode(&self, key: &TicketKey) -> Result<Vec<u8>, FlightTicketError> {
        if self.statement.len() > MAX_STATEMENT_LEN {
            return Err(FlightTicketError::StatementTooLong {
                len: self.statement.len(),
                max: MAX_STATEMENT_LEN,
            });
        }

        let mut buf = Vec::with_capacity(MIN_ENCODED_LEN + self.statement.len());
        buf.extend_from_slice(&MAGIC);
        buf.push(VERSION);
        buf.extend_from_slice(&self.tenant.0);
        buf.extend_from_slice(&self.now_ns.to_le_bytes());
        buf.extend_from_slice(&self.deadline_ns.to_le_bytes());
        write_u32(&mut buf, self.slice_index);
        write_u32(&mut buf, self.slice_count);

        write_u32(&mut buf, u32_len(self.min_commit_tokens.len())?);
        for token in &self.min_commit_tokens {
            write_len_prefixed(&mut buf, token.encode().as_bytes())?;
        }

        write_u32(&mut buf, u32_len(self.segments.len())?);
        for seg in &self.segments {
            buf.extend_from_slice(&seg.writer_epoch.to_le_bytes());
            buf.extend_from_slice(&seg.writer_seq.to_le_bytes());
            buf.extend_from_slice(&seg.created_unix_ns.to_le_bytes());
            buf.extend_from_slice(&seg.object_size.to_le_bytes());
            buf.extend_from_slice(&seg.min_event_ts_ns.to_le_bytes());
            buf.extend_from_slice(&seg.max_event_ts_ns.to_le_bytes());
            buf.extend_from_slice(&seg.sample_count.to_le_bytes());
            buf.extend_from_slice(&seg.series_count.to_le_bytes());
            buf.extend_from_slice(&seg.ingest_hour_bucket.to_le_bytes());
            buf.extend_from_slice(&seg.shard.to_le_bytes());
            buf.extend_from_slice(&seg.content_hash);
            buf.extend_from_slice(seg.writer_id.as_bytes());
            write_len_prefixed(&mut buf, seg.data_object_key.as_bytes())?;
            write_segment_level(&mut buf, &seg.level);
        }

        write_len_prefixed(&mut buf, self.statement.as_bytes())?;

        let tag = mac(key, &buf);
        buf.extend_from_slice(&tag);
        Ok(buf)
    }

    /// Decode from the wire layout, verifying the trailing MAC against `key`.
    /// Every malformed, truncated, corrupt, tampered, or trailing-garbage
    /// input yields a typed [`FlightTicketError`], never a panic.
    pub fn decode(bytes: &[u8], key: &TicketKey) -> Result<FlightTicket, FlightTicketError> {
        if bytes.len() < MIN_ENCODED_LEN {
            return Err(FlightTicketError::Truncated);
        }
        // Split off and verify the trailing MAC before parsing, so a corrupt
        // length field cannot drive parsing at all, and so a tampered field
        // is rejected before any of it is trusted.
        let split = bytes.len() - MAC_LEN;
        let (payload, stored) = bytes.split_at(split);
        if !ct_eq(&mac(key, payload), stored) {
            return Err(FlightTicketError::MacMismatch);
        }

        let mut cur = Cursor::new(payload);
        if cur.read_array::<4>()? != MAGIC {
            return Err(FlightTicketError::BadMagic);
        }
        let version = cur.read_u8()?;
        if version != VERSION {
            return Err(FlightTicketError::UnsupportedVersion(version));
        }
        let tenant = TenantHash(cur.read_array::<16>()?);
        let now_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        let deadline_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        let slice_index = cur.read_u32()?;
        let slice_count = cur.read_u32()?;

        let token_count = cur.read_u32()?;
        // Do not pre-allocate from the untrusted count; push and grow.
        let mut min_commit_tokens = Vec::new();
        for _ in 0..token_count {
            let raw = cur.read_len_prefixed()?;
            let s = std::str::from_utf8(raw).map_err(|_| FlightTicketError::InvalidUtf8)?;
            let token =
                CommitToken::decode(s).map_err(|_| FlightTicketError::InvalidCommitToken)?;
            min_commit_tokens.push(token);
        }

        let seg_count = cur.read_u32()?;
        let mut segments = Vec::new();
        for _ in 0..seg_count {
            let writer_epoch = u64::from_le_bytes(cur.read_array::<8>()?);
            let writer_seq = u64::from_le_bytes(cur.read_array::<8>()?);
            let created_unix_ns = i64::from_le_bytes(cur.read_array::<8>()?);
            let object_size = u64::from_le_bytes(cur.read_array::<8>()?);
            let min_event_ts_ns = i64::from_le_bytes(cur.read_array::<8>()?);
            let max_event_ts_ns = i64::from_le_bytes(cur.read_array::<8>()?);
            let sample_count = u64::from_le_bytes(cur.read_array::<8>()?);
            let series_count = u64::from_le_bytes(cur.read_array::<8>()?);
            let ingest_hour_bucket = u32::from_le_bytes(cur.read_array::<4>()?);
            let shard = u32::from_le_bytes(cur.read_array::<4>()?);
            let content_hash = cur.read_array::<32>()?;
            let writer_id = Uuid::from_bytes(cur.read_array::<16>()?);
            let key = cur.read_len_prefixed()?;
            let data_object_key =
                std::str::from_utf8(key).map_err(|_| FlightTicketError::InvalidUtf8)?;
            let level = read_segment_level(&mut cur)?;
            segments.push(SegmentPin {
                data_object_key: data_object_key.to_owned(),
                object_size,
                min_event_ts_ns,
                max_event_ts_ns,
                ingest_hour_bucket,
                sample_count,
                series_count,
                shard,
                content_hash,
                writer_id,
                writer_epoch,
                writer_seq,
                created_unix_ns,
                level,
            });
        }

        let stmt_len = cur.read_u32()? as usize;
        if stmt_len > MAX_STATEMENT_LEN {
            return Err(FlightTicketError::StatementTooLong {
                len: stmt_len,
                max: MAX_STATEMENT_LEN,
            });
        }
        let stmt_bytes = cur.read_bytes(stmt_len)?;
        let statement =
            std::str::from_utf8(stmt_bytes).map_err(|_| FlightTicketError::InvalidUtf8)?;

        if !cur.is_empty() {
            return Err(FlightTicketError::TrailingBytes);
        }

        Ok(FlightTicket {
            tenant,
            statement: statement.to_owned(),
            segments,
            min_commit_tokens,
            now_ns,
            deadline_ns,
            slice_index,
            slice_count,
        })
    }

    /// Whether the ticket's validity has ended: `true` at and after
    /// `deadline_ns`. `DoGet` rejects an expired ticket with
    /// `SnapshotInvalidated` (out of scope for this codec).
    pub fn is_expired(&self, now_ns: i64) -> bool {
        now_ns >= self.deadline_ns
    }

    /// Rebuild the resolved `Snapshot` this ticket pinned.
    ///
    /// This is the whole point of the pin: `DoGet` executes against the
    /// snapshot `GetFlightInfo` resolved, never a re-resolution (review F18).
    /// Segment order is preserved from the resolve, which is already the
    /// catalog's deterministic provenance order.
    ///
    /// `segments_pruned` is 0: the ticket carries the segments that survived
    /// the original resolve, and redemption never re-resolves or re-prunes,
    /// so this snapshot excludes nothing of its own.
    pub fn snapshot(&self) -> ravel_catalog::Snapshot {
        ravel_catalog::Snapshot {
            segments: self
                .segments
                .iter()
                .map(SegmentPin::to_segment_ref)
                .collect(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        }
    }
}

/// Typed decode/encode failure. Corrupt input never panics; it surfaces here.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FlightTicketError {
    /// Statement exceeds [`MAX_STATEMENT_LEN`] (at encode or decode).
    #[error("statement is {len} bytes, exceeds the {max}-byte cap")]
    StatementTooLong { len: usize, max: usize },
    /// A length field would not fit in a `u32` at encode time.
    #[error("field length {0} exceeds u32::MAX")]
    FieldTooLong(usize),
    /// Input ended before a field could be fully read.
    #[error("ticket bytes are truncated")]
    Truncated,
    /// Trailing magic bytes did not match.
    #[error("ticket magic bytes are wrong")]
    BadMagic,
    /// Version byte is not one this codec understands.
    #[error("unsupported ticket version {0}")]
    UnsupportedVersion(u8),
    /// The trailing MAC did not match the body (corruption, tampering, or a
    /// key other than the one it was signed with).
    #[error("ticket MAC mismatch")]
    MacMismatch,
    /// A statement or object-key field was not valid UTF-8.
    #[error("ticket contains invalid UTF-8")]
    InvalidUtf8,
    /// An embedded commit token failed [`CommitToken::decode`].
    #[error("ticket contains an invalid commit token")]
    InvalidCommitToken,
    /// A segment's level tag byte was neither L0 (0) nor L1 (1).
    #[error("ticket contains an invalid segment level tag {0}")]
    InvalidSegmentLevel(u8),
    /// Bytes remained after the last field was read.
    #[error("ticket has trailing bytes")]
    TrailingBytes,
}

/// Keyed BLAKE3-256 MAC over `bytes`. Deterministic across processes and
/// platforms for a given key, which the ticket requires: `GetFlightInfo` and
/// `DoGet` may run on different nodes sharing the same in-process key only
/// when they are, in fact, the same process (see [`TicketKey`] docs).
/// Forging a tag without `key` is a BLAKE3 key-recovery / preimage problem,
/// not a recomputation any client can do, which is the property version 2's
/// unkeyed FNV-1a-64 checksum lacked.
fn mac(key: &TicketKey, bytes: &[u8]) -> [u8; MAC_LEN] {
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

fn u32_len(len: usize) -> Result<u32, FlightTicketError> {
    u32::try_from(len).map_err(|_| FlightTicketError::FieldTooLong(len))
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<(), FlightTicketError> {
    write_u32(buf, u32_len(bytes.len())?);
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Per-segment wire encoding of [`SegmentLevel`]: a single tag byte (0 = L0,
/// 1 = L1) followed, for L1, by the part's `input_set_hash` (`[u8; 32]`) and
/// `part_index` (`u32`). These bytes land in the payload before the trailing
/// MAC is computed, so the level is covered by the tag like every other field
/// and cannot be flipped without invalidating it. `ravel-catalog` keeps its
/// own snapshot-format encoding of the level internal, so no public codec is
/// reused; this hand-rolled layout matches the rest of this ephemeral ticket.
fn write_segment_level(buf: &mut Vec<u8>, level: &SegmentLevel) {
    match level {
        SegmentLevel::L0 => buf.push(0),
        SegmentLevel::L1 {
            input_set_hash,
            part_index,
        } => {
            buf.push(1);
            buf.extend_from_slice(input_set_hash);
            buf.extend_from_slice(&part_index.to_le_bytes());
        }
    }
}

fn read_segment_level(cur: &mut Cursor<'_>) -> Result<SegmentLevel, FlightTicketError> {
    match cur.read_u8()? {
        0 => Ok(SegmentLevel::L0),
        1 => {
            let input_set_hash = cur.read_array::<32>()?;
            let part_index = u32::from_le_bytes(cur.read_array::<4>()?);
            Ok(SegmentLevel::L1 {
                input_set_hash,
                part_index,
            })
        }
        other => Err(FlightTicketError::InvalidSegmentLevel(other)),
    }
}

/// A bounds-checked forward reader over the checksum-verified payload. Every
/// read that would run past the end returns [`FlightTicketError::Truncated`].
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos == self.buf.len()
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], FlightTicketError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(FlightTicketError::Truncated)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(FlightTicketError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], FlightTicketError> {
        let slice = self.read_bytes(N)?;
        slice.try_into().map_err(|_| FlightTicketError::Truncated)
    }

    fn read_u8(&mut self) -> Result<u8, FlightTicketError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u32(&mut self) -> Result<u32, FlightTicketError> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    fn read_len_prefixed(&mut self) -> Result<&'a [u8], FlightTicketError> {
        let len = self.read_u32()? as usize;
        self.read_bytes(len)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use uuid::Uuid;

    /// A fixed key for tests that don't care about key material itself, only
    /// that encode/decode agree on one.
    fn test_key() -> TicketKey {
        [0x42u8; TICKET_KEY_LEN]
    }

    fn sample_token(seed: u64) -> CommitToken {
        CommitToken {
            shard: (seed % 64) as u32,
            writer_id: Uuid::from_u128(u128::from(seed).wrapping_mul(0x9e37_79b9)),
            epoch: seed.wrapping_add(1),
            seq: seed.wrapping_mul(7),
            ingest_hour_bucket: (seed % 10_000) as u32,
        }
    }

    /// A pin whose every field is distinct, so a codec that swapped two of
    /// them fails the round trip instead of silently agreeing. The level
    /// alternates by seed parity so a mixed L0/L1 pin set exercises both
    /// wire encodings of [`SegmentLevel`].
    fn sample_pin(seed: u64, key: &str) -> SegmentPin {
        let level = if seed.is_multiple_of(2) {
            SegmentLevel::L0
        } else {
            SegmentLevel::L1 {
                input_set_hash: [(seed % 241) as u8; 32],
                part_index: seed as u32 + 11,
            }
        };
        SegmentPin {
            data_object_key: key.to_owned(),
            object_size: seed * 1_000 + 1,
            min_event_ts_ns: seed as i64 * 1_000 + 2,
            max_event_ts_ns: seed as i64 * 1_000 + 3,
            ingest_hour_bucket: seed as u32 + 4,
            sample_count: seed * 1_000 + 5,
            series_count: seed * 1_000 + 6,
            shard: seed as u32 + 7,
            content_hash: [(seed % 251) as u8; 32],
            writer_id: Uuid::from_u128(u128::from(seed).wrapping_mul(0x1234_5679)),
            writer_epoch: seed * 1_000 + 8,
            writer_seq: seed * 1_000 + 9,
            created_unix_ns: seed as i64 * 1_000 + 10,
            level,
        }
    }

    fn sample_ticket() -> FlightTicket {
        FlightTicket {
            tenant: TenantHash([7u8; 16]),
            statement: "SELECT * FROM samples WHERE ts >= 1 AND ts < 2".to_owned(),
            segments: vec![
                // Odd seed -> L1, even seed -> L0: the fixed ticket carries one
                // of each so the generic round-trip and flip tests exercise both
                // level encodings.
                sample_pin(3, "t/aa/metrics/l1/0000/w.1.2.abc.rseg"),
                sample_pin(8, "t/aa/metrics/l0/0001/w.4.5.def.rseg"),
            ],
            min_commit_tokens: vec![sample_token(1), sample_token(2)],
            now_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
            slice_index: 0,
            slice_count: 1,
        }
    }

    /// The pin is the wire mirror of `SegmentRef`: projecting and rebuilding
    /// must be lossless, or `DoGet` would execute over a snapshot that
    /// dedups differently than the one `GetFlightInfo` resolved.
    #[test]
    fn segment_ref_round_trips_through_the_pin() {
        // Cover both level variants through the real ticket encode/decode
        // path: the pin is only lossless if L0 and an L1 part's
        // input_set_hash/part_index both survive the wire and rebuild.
        for level in [
            SegmentLevel::L0,
            SegmentLevel::L1 {
                input_set_hash: [0xcdu8; 32],
                part_index: 7,
            },
        ] {
            let seg = SegmentRef {
                data_object_key: "t/aa/metrics/l0/0000/w.1.2.abc.rseg".to_owned(),
                object_size: 4096,
                min_event_ts_ns: -17,
                max_event_ts_ns: 1_700_000_000_000_000_000,
                ingest_hour_bucket: 471_000,
                sample_count: 9_999,
                series_count: 12,
                shard: 63,
                content_hash: [0xabu8; 32],
                writer_id: Uuid::from_u128(0x9e37_79b9_7f4a_7c15),
                writer_epoch: 7,
                writer_seq: 4_294_967_296,
                created_unix_ns: 1_699_999_999_999_999_999,
                level: level.clone(),
            };
            let pin = SegmentPin::from_segment_ref(&seg);
            assert_eq!(pin.to_segment_ref(), seg);

            let ticket = FlightTicket {
                segments: vec![pin],
                ..sample_ticket()
            };
            let bytes = ticket.encode(&test_key()).expect("encode");
            let decoded = FlightTicket::decode(&bytes, &test_key()).expect("decode");
            // Reconstruct the SegmentRef through the full mint/decode/rebuild
            // path, not just the in-memory struct conversion.
            assert_eq!(decoded.snapshot().segments, vec![seg]);
        }
    }

    /// A redeemed ticket reports no pruning of its own. The pin already holds
    /// the post-prune segment set from `GetFlightInfo`, so a nonzero count
    /// here would double-count segments the original resolve already dropped.
    #[test]
    fn rebuilt_snapshot_reports_no_pruning() {
        let ticket = sample_ticket();
        let bytes = ticket.encode(&test_key()).expect("encode");
        let decoded = FlightTicket::decode(&bytes, &test_key()).expect("decode");

        let snapshot = decoded.snapshot();
        assert_eq!(
            snapshot.segments,
            ticket
                .segments
                .iter()
                .map(SegmentPin::to_segment_ref)
                .collect::<Vec<_>>()
        );
        assert_eq!(snapshot.segments_pruned, 0);
    }

    /// A version byte this codec does not implement is refused, never
    /// reinterpreted under the current layout.
    #[test]
    fn a_foreign_version_byte_is_refused() {
        let key = test_key();
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.push(VERSION.wrapping_add(1));
        body.extend_from_slice(&[0u8; 16]);
        body.extend_from_slice(&0i64.to_le_bytes());
        body.extend_from_slice(&0i64.to_le_bytes());
        write_u32(&mut body, 0); // slice_index
        write_u32(&mut body, 0); // slice_count
        write_u32(&mut body, 0);
        write_u32(&mut body, 0);
        write_u32(&mut body, 0);
        // A valid MAC under the real key: the version check, not the MAC,
        // must be what rejects this.
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert!(matches!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::UnsupportedVersion(_))
        ));
    }

    /// The predecessor envelope version (v3, before ADR-0071 added the slice
    /// pair) is rejected with the existing typed `UnsupportedVersion`, never
    /// reinterpreted under the v4 layout. This is the rolling-deploy safety a
    /// coordinator minting v4 slice tickets relies on: a v3 ticket carrying a
    /// valid MAC under this process's key (so the MAC is not what rejects it)
    /// still fails on the version byte, so no v3 bytes are ever read as a v4
    /// slice.
    #[test]
    fn a_v3_envelope_is_rejected_as_unsupported_version() {
        let key = test_key();
        // A v4-sized body (>= MIN_ENCODED_LEN) so the length guard passes and
        // the version check is what fires, but with the v3 version byte.
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.push(3); // the predecessor version
        body.extend_from_slice(&[0u8; 16]);
        body.extend_from_slice(&0i64.to_le_bytes());
        body.extend_from_slice(&0i64.to_le_bytes());
        write_u32(&mut body, 0); // slice_index
        write_u32(&mut body, 0); // slice_count
        write_u32(&mut body, 0); // tokens
        write_u32(&mut body, 0); // segments
        write_u32(&mut body, 0); // stmt_len
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert_eq!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::UnsupportedVersion(3))
        );
    }

    /// A slice ticket (a subset of the pinned set with a non-trivial
    /// `(slice_index, slice_count)`) round-trips bit-for-bit, and `snapshot()`
    /// rebuilds exactly that slice's segments: the slice fields are carried but
    /// do not perturb the reconstructed segment set.
    #[test]
    fn a_slice_ticket_round_trips_and_rebuilds_its_slice() {
        let ticket = FlightTicket {
            segments: vec![
                sample_pin(5, "t/aa/metrics/l0/0002/w.7.8.ghi.rseg"),
                sample_pin(6, "t/aa/metrics/l0/0003/w.9.a.jkl.rseg"),
            ],
            slice_index: 2,
            slice_count: 5,
            ..sample_ticket()
        };
        let bytes = ticket.encode(&test_key()).expect("encode");
        let decoded = FlightTicket::decode(&bytes, &test_key()).expect("decode");
        assert_eq!(decoded, ticket);
        assert_eq!(decoded.slice_index, 2);
        assert_eq!(decoded.slice_count, 5);
        assert_eq!(
            decoded.snapshot().segments,
            ticket
                .segments
                .iter()
                .map(SegmentPin::to_segment_ref)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn round_trip_fixed_ticket() {
        let ticket = sample_ticket();
        let bytes = ticket.encode(&test_key()).expect("encode");
        let decoded = FlightTicket::decode(&bytes, &test_key()).expect("decode");
        assert_eq!(ticket, decoded);
    }

    #[test]
    fn round_trip_empty_collections() {
        let ticket = FlightTicket {
            tenant: TenantHash([0u8; 16]),
            statement: String::new(),
            segments: vec![],
            min_commit_tokens: vec![],
            now_ns: 0,
            deadline_ns: 0,
            slice_index: 0,
            slice_count: 1,
        };
        let bytes = ticket.encode(&test_key()).expect("encode");
        assert_eq!(bytes.len(), MIN_ENCODED_LEN);
        assert_eq!(
            FlightTicket::decode(&bytes, &test_key()).expect("decode"),
            ticket
        );
    }

    #[test]
    fn round_trip_max_segments() {
        // The worst-case 1024-segment snapshot (max_segments) must round-trip
        // and stay well under gRPC's default message-size limit.
        let segments: Vec<SegmentPin> = (0..1024)
            .map(|i| {
                sample_pin(
                    i as u64,
                    &format!(
                        "t/00112233445566778899aabbccddeeff/metrics/l0/{:04}/\
                         3f8a1c2d-4e5f-6071-8293-a4b5c6d7e8f9.7.{:020}.0123456789abcdef.rseg",
                        i % 256,
                        i
                    ),
                )
            })
            .collect();
        let ticket = FlightTicket {
            tenant: TenantHash([9u8; 16]),
            statement: "x".repeat(1024),
            segments,
            min_commit_tokens: (0..8).map(sample_token).collect(),
            now_ns: 42,
            deadline_ns: 99,
            slice_index: 3,
            slice_count: 8,
        };
        let bytes = ticket.encode(&test_key()).expect("encode");
        assert_eq!(
            FlightTicket::decode(&bytes, &test_key()).expect("decode"),
            ticket
        );
        // Comfortably inside gRPC's default 4 MiB receive cap.
        assert!(bytes.len() < 4 * 1024 * 1024, "size {}", bytes.len());
    }

    #[test]
    fn statement_at_cap_ok_over_cap_rejected() {
        let key = test_key();
        let mut ticket = sample_ticket();
        ticket.statement = "s".repeat(MAX_STATEMENT_LEN);
        let bytes = ticket.encode(&key).expect("at-cap encodes");
        assert_eq!(FlightTicket::decode(&bytes, &key).expect("decode"), ticket);

        ticket.statement = "s".repeat(MAX_STATEMENT_LEN + 1);
        match ticket.encode(&key) {
            Err(FlightTicketError::StatementTooLong { len, max }) => {
                assert_eq!(len, MAX_STATEMENT_LEN + 1);
                assert_eq!(max, MAX_STATEMENT_LEN);
            }
            other => panic!("expected StatementTooLong, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_oversized_statement_claim() {
        // Hand-build a body claiming a stmt_len above the cap; the MAC must
        // be valid so the length check (not the MAC) is what rejects.
        let key = test_key();
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&[0u8; 16]); // tenant
        body.extend_from_slice(&0i64.to_le_bytes()); // now
        body.extend_from_slice(&0i64.to_le_bytes()); // deadline
        write_u32(&mut body, 0); // slice_index
        write_u32(&mut body, 0); // slice_count
        write_u32(&mut body, 0); // tokens
        write_u32(&mut body, 0); // segments
        write_u32(&mut body, (MAX_STATEMENT_LEN + 1) as u32); // stmt_len
        // No stmt bytes follow, but the length check fires before the read.
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert!(matches!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::StatementTooLong { .. })
        ));
    }

    #[test]
    fn empty_input_is_typed_error() {
        assert_eq!(
            FlightTicket::decode(&[], &test_key()),
            Err(FlightTicketError::Truncated)
        );
    }

    #[test]
    fn truncated_input_is_typed_error() {
        let key = test_key();
        let bytes = sample_ticket().encode(&key).expect("encode");
        for cut in 0..bytes.len() {
            // Every prefix shorter than the whole is rejected, never panics.
            assert!(FlightTicket::decode(&bytes[..cut], &key).is_err());
        }
    }

    #[test]
    fn bad_magic_is_typed_error() {
        let key = test_key();
        let mut bytes = sample_ticket().encode(&key).expect("encode");
        bytes[0] ^= 0xff;
        // Flipping a body byte trips the MAC first; the point is a typed
        // error, never a panic.
        assert!(FlightTicket::decode(&bytes, &key).is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        let key = test_key();
        let mut bytes = sample_ticket().encode(&key).expect("encode");
        bytes.push(0);
        // The appended byte is now read as part of the MAC tail, so the
        // stored MAC no longer matches the body.
        assert!(matches!(
            FlightTicket::decode(&bytes, &key),
            Err(FlightTicketError::MacMismatch)
        ));
    }

    #[test]
    fn every_single_flip_is_detected() {
        let key = test_key();
        let bytes = sample_ticket().encode(&key).expect("encode");
        for i in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[i] ^= 0x01;
            assert!(
                FlightTicket::decode(&corrupt, &key).is_err(),
                "flip at {i} decoded successfully"
            );
        }
    }

    /// The vulnerability issue #185 fixes: tampering with a field (here, the
    /// deadline the redemption path trusts) must be rejected as a MAC
    /// mismatch, not silently accepted because the tamperer recomputed some
    /// self-consistent checksum -- there is no key-independent way to make
    /// the tag agree again.
    #[test]
    fn tampering_with_a_field_is_rejected_as_a_mac_mismatch() {
        let key = test_key();
        let ticket = sample_ticket();
        let mut bytes = ticket.encode(&key).expect("encode");
        // Offset of the first byte of `deadline_ns`: magic + version +
        // tenant + now_ns.
        let deadline_offset = 4 + 1 + 16 + 8;
        bytes[deadline_offset] ^= 0x01;
        assert_eq!(
            FlightTicket::decode(&bytes, &key),
            Err(FlightTicketError::MacMismatch)
        );
    }

    /// The precise defect in the version-2 unkeyed FNV-1a-64 checksum: any
    /// holder of a ticket could tamper with a field and recompute a checksum
    /// that decode would accept, because the checksum needed no secret. A
    /// keyed MAC closes exactly this: recomputing the tag under any key other
    /// than the minting process's own is rejected, even though the tag is
    /// self-consistent under the attacker's own (wrong) key.
    /// The level is an identity field the redemption path trusts to pick the
    /// footer contract (L0 flush vs L1 v4 part), so flipping only the level
    /// tag on an otherwise-valid ticket must be rejected as a MAC mismatch,
    /// exactly like flipping the deadline. If `level` were added to the struct
    /// but left out of the MACed bytes, this flip would be silently accepted.
    #[test]
    fn flipping_the_level_tag_is_rejected_as_a_mac_mismatch() {
        let key = test_key();
        let object_key = "t/aa/metrics/l0/0000/w.1.2.abc.rseg";
        let ticket = FlightTicket {
            tenant: TenantHash([1u8; 16]),
            statement: String::new(),
            // Even seed -> L0, so the level tag byte is 0.
            segments: vec![sample_pin(2, object_key)],
            min_commit_tokens: vec![],
            now_ns: 5,
            deadline_ns: 6,
            slice_index: 0,
            slice_count: 1,
        };
        let mut bytes = ticket.encode(&key).expect("encode");
        // The level tag sits right after the per-segment fixed fields and the
        // length-prefixed key: header (magic 4 + version 1 + tenant 16 + now 8
        // + deadline 8 + slice_index 4 + slice_count 4 = 45) + token_count (4)
        // + seg_count (4) + the segment's 120 fixed bytes + key_len (4) + the
        // key bytes.
        let level_offset = 45 + 4 + 4 + 120 + 4 + object_key.len();
        assert_eq!(bytes[level_offset], 0, "expected the L0 level tag here");
        bytes[level_offset] ^= 0x01;
        assert_eq!(
            FlightTicket::decode(&bytes, &key),
            Err(FlightTicketError::MacMismatch)
        );
    }

    #[test]
    fn recomputing_the_tag_under_the_wrong_key_does_not_forge_a_valid_ticket() {
        let real_key = test_key();
        let mut ticket = sample_ticket();
        ticket.deadline_ns += 1_000_000_000; // an attacker extending its budget
        let attacker_key = [0x99u8; TICKET_KEY_LEN];
        let bytes = ticket.encode(&attacker_key).expect("encode under any key");
        assert_eq!(
            FlightTicket::decode(&bytes, &real_key),
            Err(FlightTicketError::MacMismatch)
        );
    }

    #[test]
    fn is_expired_boundaries() {
        let ticket = FlightTicket {
            deadline_ns: 100,
            ..sample_ticket()
        };
        assert!(!ticket.is_expired(99));
        assert!(ticket.is_expired(100));
        assert!(ticket.is_expired(101));
        assert!(!ticket.is_expired(i64::MIN));
        assert!(ticket.is_expired(i64::MAX));
    }

    fn segment_level_strategy() -> impl Strategy<Value = SegmentLevel> {
        prop_oneof![
            Just(SegmentLevel::L0),
            (any::<[u8; 32]>(), any::<u32>()).prop_map(|(input_set_hash, part_index)| {
                SegmentLevel::L1 {
                    input_set_hash,
                    part_index,
                }
            }),
        ]
    }

    fn segment_pin_strategy() -> impl Strategy<Value = SegmentPin> {
        (
            ".{0,80}",
            any::<[u8; 32]>(),
            any::<u128>(),
            (any::<u64>(), any::<u64>(), any::<i64>(), any::<u64>()),
            (any::<i64>(), any::<i64>(), any::<u64>(), any::<u64>()),
            (any::<u32>(), any::<u32>()),
            segment_level_strategy(),
        )
            .prop_map(
                |(
                    data_object_key,
                    content_hash,
                    writer_id,
                    (writer_epoch, writer_seq, created_unix_ns, object_size),
                    (min_event_ts_ns, max_event_ts_ns, sample_count, series_count),
                    (ingest_hour_bucket, shard),
                    level,
                )| SegmentPin {
                    data_object_key,
                    object_size,
                    min_event_ts_ns,
                    max_event_ts_ns,
                    ingest_hour_bucket,
                    sample_count,
                    series_count,
                    shard,
                    content_hash,
                    writer_id: Uuid::from_u128(writer_id),
                    writer_epoch,
                    writer_seq,
                    created_unix_ns,
                    level,
                },
            )
    }

    fn token_strategy() -> impl Strategy<Value = CommitToken> {
        (
            any::<u32>(),
            any::<u128>(),
            any::<u64>(),
            any::<u64>(),
            any::<u32>(),
        )
            .prop_map(|(shard, wid, epoch, seq, ingest_hour_bucket)| CommitToken {
                shard,
                writer_id: Uuid::from_u128(wid),
                epoch,
                seq,
                ingest_hour_bucket,
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // decode(encode(x)) == x bit-for-bit across arbitrary tenants,
        // statements up to a bound, 0..1024 segments, and arbitrary
        // now_ns/deadline. (The exact-cap statement is pinned separately in
        // statement_at_cap_ok_over_cap_rejected to keep proptest fast.)
        #[test]
        fn prop_round_trip(
            tenant in any::<[u8; 16]>(),
            statement in ".{0,3000}",
            segments in prop::collection::vec(segment_pin_strategy(), 0..1024),
            tokens in prop::collection::vec(token_strategy(), 0..8),
            now_ns in any::<i64>(),
            deadline_ns in any::<i64>(),
            slice_index in any::<u32>(),
            slice_count in any::<u32>(),
        ) {
            let ticket = FlightTicket {
                tenant: TenantHash(tenant),
                statement,
                segments,
                min_commit_tokens: tokens,
                now_ns,
                deadline_ns,
                slice_index,
                slice_count,
            };
            let key = test_key();
            let bytes = ticket.encode(&key).expect("encode");
            let decoded = FlightTicket::decode(&bytes, &key).expect("decode");
            prop_assert_eq!(ticket, decoded);
        }

        // Arbitrary bytes never panic: decode returns Ok or a typed Err.
        #[test]
        fn prop_arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = FlightTicket::decode(&bytes, &test_key());
        }

        // Any single-byte flip of a valid ticket is caught.
        #[test]
        fn prop_single_flip_detected(idx in any::<prop::sample::Index>()) {
            let key = test_key();
            let bytes = sample_ticket().encode(&key).expect("encode");
            let i = idx.index(bytes.len());
            let mut corrupt = bytes;
            corrupt[i] ^= 0x80;
            prop_assert!(FlightTicket::decode(&corrupt, &key).is_err());
        }
    }
}

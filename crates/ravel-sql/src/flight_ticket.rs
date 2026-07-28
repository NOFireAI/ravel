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
//! implement `FlightSqlService`, or compare tenants; those belong to the
//! later C1 ticket that binds to this format.
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
//! - It keeps `ravel-sql` free of a new `prost`/`.proto` dependency for a
//!   handful of fields, and reuses [`ravel_types::CommitToken`]'s own canonical
//!   string codec for the min-commit-token inputs, so no `uuid` dependency is
//!   pulled in either.
//! - A trailing 64-bit checksum makes accidental corruption (a single
//!   flipped byte anywhere) a typed decode error rather than a silently
//!   different pin.
//!
//! Layout (all integers little-endian, lengths are `u32`):
//!
//! ```text
//! magic         4   b"RFT1"
//! version       1   = 1
//! tenant       16   TenantHash bytes
//! now_ns        8   i64
//! deadline_ns   8   i64
//! token_count   4   u32
//!   per token:  4 + N   len-prefixed CommitToken::encode() (ASCII)
//! seg_count     4   u32
//!   per segment:
//!     writer_epoch  8   u64
//!     content_hash 32   [u8; 32]
//!     key_len       4   u32
//!     key           N   data_object_key (UTF-8)
//! stmt_len      4   u32   (<= MAX_STATEMENT_LEN)
//! stmt          N   statement text (UTF-8)
//! checksum      8   FNV-1a-64 over every preceding byte
//! ```
//!
//! # Segment identity fields (the judgment call from issue #150)
//!
//! Each [`SegmentPin`] carries `data_object_key`, `writer_epoch`, and
//! `content_hash`. The reasoning, since the issue left this open:
//!
//! - `data_object_key` (required): locates the immutable object to fetch.
//! - `content_hash` (`[u8; 32]`): the primary stale/tampered-ticket signal.
//!   Data objects are immutable, so if a segment `DoGet` fetches under a
//!   pinned key hashes differently than the ticket recorded, the ticket is
//!   stale or tampered and execution must reject rather than serve foreign
//!   bytes. This is defense in depth for the pin, cheap at 32 bytes/segment.
//! - `writer_epoch`: a cheap provenance witness (a fenced/superseded writer
//!   epoch is a strong stale-snapshot signal) and the first component of the
//!   cross-segment dedup total order the later executor reconstructs.
//!
//! The other `SegmentRef` fields (`object_size`, ts bounds, `sample_count`,
//! `series_count`, `shard`, `writer_id`, `writer_seq`, `created_unix_ns`,
//! `ingest_hour_bucket`) are intentionally omitted: whether `DoGet`
//! re-resolves the full `Snapshot` or reconstructs it from the ticket is the
//! later C1 ticket's decision. If it chooses full reconstruction, those
//! fields are added under a bumped `version` byte; the codec is built for
//! that extension.

use ravel_catalog::SegmentRef;
use ravel_types::{CommitToken, TenantHash};

/// Maximum accepted SQL statement length, in bytes. 64 KiB. Longer
/// statements are rejected at [`FlightTicket::encode`] time and refused at
/// [`FlightTicket::decode`] time.
pub const MAX_STATEMENT_LEN: usize = 64 * 1024;

const MAGIC: [u8; 4] = *b"RFT1";
const VERSION: u8 = 1;

/// Smallest possible encoded ticket: the fixed header plus the trailing
/// checksum, with zero tokens, zero segments, and an empty statement.
const MIN_ENCODED_LEN: usize = 4 + 1 + 16 + 8 + 8 + 4 + 4 + 4 + 8;

/// One pinned segment's identity inside a [`FlightTicket`]. See the module
/// docs for why exactly these three fields are carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPin {
    /// Data-object key, as reconstructed by the catalog into
    /// [`SegmentRef::data_object_key`].
    pub data_object_key: String,
    /// Writer epoch that produced the segment. Provenance witness and first
    /// component of the dedup total order.
    pub writer_epoch: u64,
    /// Whole-object content hash recorded in the commit record. Verified
    /// against the fetched object to detect a stale or tampered ticket.
    pub content_hash: [u8; 32],
}

impl SegmentPin {
    /// Project the pinned identity fields out of a resolved [`SegmentRef`].
    pub fn from_segment_ref(seg: &SegmentRef) -> Self {
        SegmentPin {
            data_object_key: seg.data_object_key.clone(),
            writer_epoch: seg.writer_epoch,
            content_hash: seg.content_hash,
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
}

impl FlightTicket {
    /// Encode to the wire layout documented on this module.
    ///
    /// Returns [`FlightTicketError::StatementTooLong`] if the statement
    /// exceeds [`MAX_STATEMENT_LEN`], and [`FlightTicketError::FieldTooLong`]
    /// if any single length field would not fit in a `u32` (not reachable
    /// with real keys or tokens).
    pub fn encode(&self) -> Result<Vec<u8>, FlightTicketError> {
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

        write_u32(&mut buf, u32_len(self.min_commit_tokens.len())?);
        for token in &self.min_commit_tokens {
            write_len_prefixed(&mut buf, token.encode().as_bytes())?;
        }

        write_u32(&mut buf, u32_len(self.segments.len())?);
        for seg in &self.segments {
            buf.extend_from_slice(&seg.writer_epoch.to_le_bytes());
            buf.extend_from_slice(&seg.content_hash);
            write_len_prefixed(&mut buf, seg.data_object_key.as_bytes())?;
        }

        write_len_prefixed(&mut buf, self.statement.as_bytes())?;

        let checksum = fnv1a64(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        Ok(buf)
    }

    /// Decode from the wire layout. Every malformed, truncated, corrupt, or
    /// trailing-garbage input yields a typed [`FlightTicketError`], never a
    /// panic.
    pub fn decode(bytes: &[u8]) -> Result<FlightTicket, FlightTicketError> {
        if bytes.len() < MIN_ENCODED_LEN {
            return Err(FlightTicketError::Truncated);
        }
        // Split off and verify the trailing checksum before parsing, so a
        // corrupt length field cannot drive parsing at all.
        let split = bytes.len() - 8;
        let (payload, stored) = bytes.split_at(split);
        let stored = u64::from_le_bytes(
            stored
                .try_into()
                .map_err(|_| FlightTicketError::Truncated)?,
        );
        if fnv1a64(payload) != stored {
            return Err(FlightTicketError::ChecksumMismatch);
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
            let content_hash = cur.read_array::<32>()?;
            let key = cur.read_len_prefixed()?;
            let data_object_key =
                std::str::from_utf8(key).map_err(|_| FlightTicketError::InvalidUtf8)?;
            segments.push(SegmentPin {
                data_object_key: data_object_key.to_owned(),
                writer_epoch,
                content_hash,
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
        })
    }

    /// Whether the ticket's validity has ended: `true` at and after
    /// `deadline_ns`. `DoGet` rejects an expired ticket with
    /// `SnapshotInvalidated` (out of scope for this codec).
    pub fn is_expired(&self, now_ns: i64) -> bool {
        now_ns >= self.deadline_ns
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
    /// The trailing checksum did not match the body (corruption/tampering).
    #[error("ticket checksum mismatch")]
    ChecksumMismatch,
    /// A statement or object-key field was not valid UTF-8.
    #[error("ticket contains invalid UTF-8")]
    InvalidUtf8,
    /// An embedded commit token failed [`CommitToken::decode`].
    #[error("ticket contains an invalid commit token")]
    InvalidCommitToken,
    /// Bytes remained after the last field was read.
    #[error("ticket has trailing bytes")]
    TrailingBytes,
}

/// FNV-1a 64-bit hash. Chosen over a `std::hash` hasher because it is
/// deterministic across processes and platforms, which the ticket requires:
/// `GetFlightInfo` and `DoGet` may run on different nodes.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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

    fn sample_token(seed: u64) -> CommitToken {
        CommitToken {
            shard: (seed % 64) as u32,
            writer_id: Uuid::from_u128(u128::from(seed).wrapping_mul(0x9e37_79b9)),
            epoch: seed.wrapping_add(1),
            seq: seed.wrapping_mul(7),
            ingest_hour_bucket: (seed % 10_000) as u32,
        }
    }

    fn sample_ticket() -> FlightTicket {
        FlightTicket {
            tenant: TenantHash([7u8; 16]),
            statement: "SELECT * FROM samples WHERE ts >= 1 AND ts < 2".to_owned(),
            segments: vec![
                SegmentPin {
                    data_object_key: "t/aa/metrics/l0/0000/w.1.2.abc.rseg".to_owned(),
                    writer_epoch: 3,
                    content_hash: [1u8; 32],
                },
                SegmentPin {
                    data_object_key: "t/aa/metrics/l0/0001/w.4.5.def.rseg".to_owned(),
                    writer_epoch: 9,
                    content_hash: [2u8; 32],
                },
            ],
            min_commit_tokens: vec![sample_token(1), sample_token(2)],
            now_ns: 1_700_000_000_000_000_000,
            deadline_ns: 1_700_000_030_000_000_000,
        }
    }

    #[test]
    fn round_trip_fixed_ticket() {
        let ticket = sample_ticket();
        let bytes = ticket.encode().expect("encode");
        let decoded = FlightTicket::decode(&bytes).expect("decode");
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
        };
        let bytes = ticket.encode().expect("encode");
        assert_eq!(bytes.len(), MIN_ENCODED_LEN);
        assert_eq!(FlightTicket::decode(&bytes).expect("decode"), ticket);
    }

    #[test]
    fn round_trip_max_segments() {
        // The worst-case 1024-segment snapshot (max_segments) must round-trip
        // and stay well under gRPC's default message-size limit.
        let segments: Vec<SegmentPin> = (0..1024)
            .map(|i| SegmentPin {
                data_object_key: format!(
                    "t/00112233445566778899aabbccddeeff/metrics/l0/{:04}/\
                     3f8a1c2d-4e5f-6071-8293-a4b5c6d7e8f9.7.{:020}.0123456789abcdef.rseg",
                    i % 256,
                    i
                ),
                writer_epoch: i as u64,
                content_hash: [(i % 251) as u8; 32],
            })
            .collect();
        let ticket = FlightTicket {
            tenant: TenantHash([9u8; 16]),
            statement: "x".repeat(1024),
            segments,
            min_commit_tokens: (0..8).map(sample_token).collect(),
            now_ns: 42,
            deadline_ns: 99,
        };
        let bytes = ticket.encode().expect("encode");
        assert_eq!(FlightTicket::decode(&bytes).expect("decode"), ticket);
        // Comfortably inside gRPC's default 4 MiB receive cap.
        assert!(bytes.len() < 4 * 1024 * 1024, "size {}", bytes.len());
    }

    #[test]
    fn statement_at_cap_ok_over_cap_rejected() {
        let mut ticket = sample_ticket();
        ticket.statement = "s".repeat(MAX_STATEMENT_LEN);
        let bytes = ticket.encode().expect("at-cap encodes");
        assert_eq!(FlightTicket::decode(&bytes).expect("decode"), ticket);

        ticket.statement = "s".repeat(MAX_STATEMENT_LEN + 1);
        match ticket.encode() {
            Err(FlightTicketError::StatementTooLong { len, max }) => {
                assert_eq!(len, MAX_STATEMENT_LEN + 1);
                assert_eq!(max, MAX_STATEMENT_LEN);
            }
            other => panic!("expected StatementTooLong, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_oversized_statement_claim() {
        // Hand-build a body claiming a stmt_len above the cap; checksum must
        // be valid so the length check (not the checksum) is what rejects.
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&[0u8; 16]); // tenant
        body.extend_from_slice(&0i64.to_le_bytes()); // now
        body.extend_from_slice(&0i64.to_le_bytes()); // deadline
        write_u32(&mut body, 0); // tokens
        write_u32(&mut body, 0); // segments
        write_u32(&mut body, (MAX_STATEMENT_LEN + 1) as u32); // stmt_len
        // No stmt bytes follow, but the length check fires before the read.
        let checksum = fnv1a64(&body);
        body.extend_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            FlightTicket::decode(&body),
            Err(FlightTicketError::StatementTooLong { .. })
        ));
    }

    #[test]
    fn empty_input_is_typed_error() {
        assert_eq!(FlightTicket::decode(&[]), Err(FlightTicketError::Truncated));
    }

    #[test]
    fn truncated_input_is_typed_error() {
        let bytes = sample_ticket().encode().expect("encode");
        for cut in 0..bytes.len() {
            // Every prefix shorter than the whole is rejected, never panics.
            assert!(FlightTicket::decode(&bytes[..cut]).is_err());
        }
    }

    #[test]
    fn bad_magic_is_typed_error() {
        let mut bytes = sample_ticket().encode().expect("encode");
        bytes[0] ^= 0xff;
        // Flipping a body byte trips the checksum first; the point is a
        // typed error, never a panic.
        assert!(FlightTicket::decode(&bytes).is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut bytes = sample_ticket().encode().expect("encode");
        bytes.push(0);
        // The appended byte is now read as the checksum tail, so the stored
        // checksum no longer matches the body.
        assert!(matches!(
            FlightTicket::decode(&bytes),
            Err(FlightTicketError::ChecksumMismatch)
        ));
    }

    #[test]
    fn every_single_flip_is_detected() {
        let bytes = sample_ticket().encode().expect("encode");
        for i in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[i] ^= 0x01;
            assert!(
                FlightTicket::decode(&corrupt).is_err(),
                "flip at {i} decoded successfully"
            );
        }
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

    fn segment_pin_strategy() -> impl Strategy<Value = SegmentPin> {
        (".{0,80}", any::<u64>(), any::<[u8; 32]>()).prop_map(
            |(data_object_key, writer_epoch, content_hash)| SegmentPin {
                data_object_key,
                writer_epoch,
                content_hash,
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
        ) {
            let ticket = FlightTicket {
                tenant: TenantHash(tenant),
                statement,
                segments,
                min_commit_tokens: tokens,
                now_ns,
                deadline_ns,
            };
            let bytes = ticket.encode().expect("encode");
            let decoded = FlightTicket::decode(&bytes).expect("decode");
            prop_assert_eq!(ticket, decoded);
        }

        // Arbitrary bytes never panic: decode returns Ok or a typed Err.
        #[test]
        fn prop_arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = FlightTicket::decode(&bytes);
        }

        // Any single-byte flip of a valid ticket is caught.
        #[test]
        fn prop_single_flip_detected(idx in any::<prop::sample::Index>()) {
            let bytes = sample_ticket().encode().expect("encode");
            let i = idx.index(bytes.len());
            let mut corrupt = bytes;
            corrupt[i] ^= 0x80;
            prop_assert!(FlightTicket::decode(&corrupt).is_err());
        }
    }
}

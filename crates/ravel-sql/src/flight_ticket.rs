//! Flight SQL snapshot-pinning ticket codec.
//!
//! # Why this exists
//!
//! Flight SQL splits one query across two RPCs: `GetFlightInfo` plans the
//! query and hands the client an opaque ticket; `DoGet` later redeems that
//! ticket to stream results. If `DoGet` re-resolved the snapshot it would
//! observe a *different* set of committed segments than `GetFlightInfo`
//! planned against, so the same query could return two different answers
//! across its own two RPCs. The pin fixes this by making the ticket carry the
//! exact resolved snapshot identity: `DoGet` executes against precisely the
//! snapshot `GetFlightInfo` pinned, never a re-resolution.
//!
//! This module is only the wire contract. It does not resolve snapshots,
//! implement `FlightSqlService`, or compare tenants; those live in
//! [`crate::flight`], which binds to this format (ticket C1d).
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
//! # Tenancy depends on the surface
//!
//! A client whole-set ticket carries [`FlightTicket::tenant`] so `DoGet` can
//! compare it against the tenant it resolves from authoritative gRPC metadata
//! and reject on mismatch. There the embedded field is the value to check
//! against, not a source of authority.
//!
//! A slice ticket is different (ADR-1689 decision 2): it is the capability. A
//! coordinator mints it under the slice key after resolving the client's
//! tenant itself, no client credential travels with it, and the worker
//! executes the slice under the ticket's tenant. The two surfaces are MAC'd
//! under different keys ([`SqlTicketKeys`]), so a client ticket can never be
//! presented as a slice capability. Both checks live in [`crate::flight`], not
//! in this codec.
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
//!   (version 2 used an unkeyed FNV-1a-64 checksum any client
//!   could recompute after tampering with a field).
//!
//! Layout (all integers little-endian, lengths are `u32`):
//!
//! ```text
//! magic         4   b"RFT1"
//! version       1   = 8; 9 for a ticket carrying the pushdown block; 10 for
//!                   a ticket carrying segment groups (see below); 11 when
//!                   the pushdown block is a logs block with exact-key
//!                   equalities
//! tenant       16   TenantHash bytes
//! now_ns        8   i64
//! deadline_ns   8   i64
//! slice_index   4   u32   (this slice's 0-based index in the fan-out)
//! slice_count   4   u32   (number of slices; 1 for a whole-snapshot ticket)
//! token_count   4   u32
//!   per token:  4 + N   len-prefixed CommitToken::encode() (ASCII)
//! [pins]            versions 8, 9 and 11: seg_count through erasure_count
//!                   below, once. Version 10 instead:
//!   group_count  4   u32   (>= 2, one per signal table the statement reads)
//!   per group, in lane order samples, logs, spans, alerts, audit:
//!     signal_tag   1   1 = samples, 2 = logs, 3 = spans, 4 = alerts,
//!                      5 = audit; no duplicates
//!     seg_count through erasure_count below, for that table alone
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
//!     segment_format_version 4   u32
//! erasure_count 4   u32   (pending erasure predicates, ADR-0064 decision 3)
//!   per predicate:
//!     matcher_count      4   u32
//!       per matcher:
//!         key_len        4   u32
//!         key            N   matcher key (UTF-8)
//!         value_len      4   u32
//!         value          N   matcher value (UTF-8)
//!     window_start_ns    8   i64
//!     window_end_ns      8   i64
//! declared_count 4  u32   (declared typed attribute columns, ADR-0090)
//!   per column:
//!     key_len        4   u32
//!     key            N   attribute key (UTF-8)
//!     type_tag       1   1=Str 2=I64 3=Bool 4=Bytes
//! parquet_count 4   u32   (Parquet tables pinned, <= MAX_STATEMENT_TABLE_NAMES)
//!   per table:
//!     name_len       4   u32
//!     name           N   table name (UTF-8, a valid table name)
//!     version        8   u64  (the manifest object's version)
//! budgets_flag  1   0 = none, 1 = the three limits below follow
//!   if 1, three limits in this order: max_bytes_scanned, max_store_requests,
//!   max_segments; each is:
//!     tag            1   0 = absent, 1 = unlimited (not max_segments), 2 = bounded
//!       if 2:
//!         limit      8   u64
//! stmt_len      4   u32   (<= MAX_STATEMENT_LEN)
//! stmt          N   statement text (UTF-8)
//! [pushdown]        versions 9 and 11 only: the slice pushdown block a
//!                   coordinator attached (see below)
//!   kind         1   1 = spans, 2 = logs
//!   ts_lo        1 (+8)  flag 0 = absent, 1 = present, then i64
//!   ts_hi        1 (+8)  same
//!   if spans:
//!     trace_flag 1   0 = absent, 1 = the trace id follows
//!       trace_id 16
//!   if logs:
//!     prune_count 4  u32
//!       per prune predicate:
//!         tag     1   1 = attribute equality, 2 = attribute numeric range
//!         key_len 4   u32
//!         key     N   attribute key (UTF-8)
//!         type    1   ravel_logseg FieldType byte: equality 1 = Str or
//!                     5 = Bytes; range 2 = I64, 3 = F64 or 4 = Bool
//!         if equality:
//!           value_len 4  u32
//!           value     N  UTF-8 for Str, raw for Bytes
//!         if range:
//!           min    1 (+8)  flag, then the NumStat bit pattern
//!           max    1 (+8)  same
//!     version 11 only:
//!     key_count    4  u32 (>= 1)
//!       per exact-key equality (ravel_query::KeyEquals):
//!         field_len 4  u32
//!         field     N  field name (UTF-8)
//!         key_type  1  KEY_IDX key type byte: 1 = Id16, 2 = I64, 3 = Str,
//!                      4 = Bytes
//!         key8      8  the compared literal's key8
//! mac          32   keyed BLAKE3-256 over every preceding byte
//! ```
//!
//! The pushdown block (issue #2755) is how a distributed SQL coordinator
//! hands a worker the trace id, the time bounds and the prune comparisons it
//! extracted, so the worker's fragment reads only the blocks they admit. See
//! [`SlicePushdown`]. A ticket without it is the no-pushdown query, and
//! [`FlightTicket::encode`] never writes it. Only the slice surface carries
//! it. It sits under the MAC like every other field. A ticket that carries
//! the block is version 9, or 11 when it is a logs block with exact-key
//! equalities; one without it is still version 8, byte for byte,
//! so a metrics slice or a coordinator built before the block mints exactly
//! what it always did. See "Version history" below for why the block needs
//! the bump.
//!
//! Segment groups (ADR-2904 decision 10) are how a statement naming two or
//! more signal tables, such as `samples JOIN logs`, keeps the two-RPC pin.
//! `GetFlightInfo` resolves one snapshot per signal table, as the HTTP
//! endpoint does, and each table's segments and pending erasure become one
//! group, so `DoGet` rebuilds every table's own snapshot without resolving
//! again. A flat pin list could not say which segments belong to which
//! table. [`FlightTicket::signal_groups`] non-empty makes the ticket version
//! 10, with both flat lists empty; empty keeps a statement over at most one
//! signal table on version 8 or 9, byte for byte. A version 10 ticket is
//! client-surface only: the distributed slice path splits one snapshot, so
//! a slice ticket never carries groups and the slice surface refuses version
//! 10. The slice surface's own newer version is 11, the pushdown block with
//! a logs key section; the client surface refuses it in turn.
//!
//! # Integrity: a keyed MAC, not a checksum
//!
//! [`FlightTicket::encode`] and [`FlightTicket::decode`] both take a
//! [`TicketKey`]: a 32-byte secret, never sent to a client or persisted. The
//! Flight service holds one per surface per file key ([`SqlTicketKeys`]): a
//! single-process deployment generates its file key once in memory; a
//! distributed deployment shares it so a coordinator and a worker verify with
//! the same keys (see [`TicketKey`] and [`derive_ticket_key`]).
//! The trailing tag is `blake3::keyed_hash(key, payload)`, not a plain hash of
//! the payload, so recomputing it requires the key. A client can still read
//! and replay a ticket verbatim (that is the protocol), but cannot flip a
//! single field -- extend `deadline_ns`, swap a `data_object_key`, change the
//! `tenant` -- and produce a tag the minting process will accept, which the
//! version-2 FNV-1a-64 checksum never prevented.
//!
//! # Segment identity fields
//!
//! Version 1 of this codec carried only `data_object_key`, `writer_epoch`,
//! and `content_hash`, and left open "whether `DoGet` re-resolves the full
//! `Snapshot` or reconstructs it from the ticket is the later C1 ticket's
//! decision. If it chooses full reconstruction, those fields are added under
//! a bumped `version` byte; the codec is built for that extension."
//!
//! C1 chose full reconstruction, so version 2 carries every
//! [`SegmentRef`] field and [`SegmentPin::to_segment_ref`] rebuilds the
//! resolved `Snapshot` exactly. The alternative -- re-resolving at `DoGet`
//! and intersecting against the pinned keys -- would have put a catalog LIST
//! on the redemption path and made the pin depend on a second resolve
//! observing the same committed state, which is the coupling the pin exists to
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
//! there is no compatibility window to preserve. Version 3
//! replaces the unkeyed FNV-1a-64 checksum with a keyed BLAKE3 MAC; see
//! "Integrity: a keyed MAC, not a checksum" above.
//!
//! Version 4 (ADR-0071 distributed read fan-out) carries a
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
//! length check regardless. The per-segment `level_tag` is added
//! (L0 vs L1, with an L1 part's `input_set_hash`/`part_index`) this way, so
//! [`SegmentPin::to_segment_ref`] reconstructs the level and a rebuilt L1 part
//! is verified against the v4 footer contract, not read as an L0 segment.
//!
//! Version 5 (ADR-0064 decision 3) carries the resolved
//! snapshot's pending selective-erasure predicates
//! ([`FlightTicket::pending_erasure`]). Earlier versions minted `DoGet`
//! against a snapshot rebuilt with an always-empty predicate set
//! ([`FlightTicket::snapshot`] hardcoded `pending_erasure: Vec::new()`),
//! which meant a query whose snapshot had a pending erasure request still
//! returned the erased rows over Flight SQL, in violation of ADR-0064's
//! visibility bound; the HTTP `/api/v1/sql` path, which resolves and scans
//! without going through this codec, was never affected. This is a version
//! bump, not a `.proto` schema change and not an ADR of its own: as stated
//! above, the ticket is an ephemeral MAC'd blob bounded by `deadline_ns`, not
//! one of the frozen persistent contracts, so a new field under a bumped
//! version byte is the correct and sufficient way to add it, exactly as
//! version 4 added the slice pair. A v4 (or earlier) ticket is rejected with
//! [`FlightTicketError::UnsupportedVersion`] at the existing version check,
//! never reinterpreted as a v5 ticket with no erasure predicates -- there is
//! no silent-downgrade path here. The predicate shape mirrors, at the level
//! of matchers plus an optional half-open window, the one
//! `ravel_query::distrib::codec::encode_erasure` already carries in a
//! distributed `FetchRequest` (`ravel-query/src/distrib/mod.rs`); this codec
//! stays hand-rolled rather than reusing that proto message, consistent with
//! this codec avoiding prost by design.
//!
//! Version 6 (ADR-0090) carries the tenant's declared typed attribute columns
//! ([`FlightTicket::declared_columns`]). The `logs` table's declared schema is
//! resolved once at `GetFlightInfo` from a `DeclaredColumnSource` whose real
//! implementation is a cache-aside overlay, so its result is not a pure
//! function of `(tenant, now_ns)`: a refresh between `GetFlightInfo` and
//! `DoGet` could otherwise make `DoGet` plan against a different declared
//! schema than the one the `FlightInfo` advertised, and the streamed batch
//! schema would disagree with it. Pinning the resolved list here makes `DoGet`
//! plan against exactly the schema `GetFlightInfo` advertised, for the same
//! reason the segment set and the pending-erasure set are pinned. A v5 (or
//! earlier) ticket is rejected with [`FlightTicketError::UnsupportedVersion`],
//! never reinterpreted under the v6 layout. Consistent with every earlier
//! extension, this is a version bump on an ephemeral MAC'd blob, not a `.proto`
//! schema change and not an ADR of its own.
//!
//! Version 7 (issue #862) carries each segment's on-object format version
//! ([`SegmentPin::segment_format_version`], mirroring the field added to
//! [`SegmentRef`]). The whole-segment fast path routes the ranged column-chunk
//! read on that version -- it is a v4 capability (ADR-0699 decision 5) -- so a
//! worker reconstructing the snapshot from the ticket must see each segment's
//! real version, or it would misroute a v3 logs segment onto the ranged path
//! and pay a probe to fetch the same whole-object bytes. A v6 (or earlier)
//! ticket is rejected with [`FlightTicketError::UnsupportedVersion`], never
//! reinterpreted under the v7 layout. Consistent with every earlier extension,
//! this is a version bump on an ephemeral MAC'd blob, not a `.proto` schema
//! change and not an ADR of its own.
//!
//! Version 8 (issue #2240, ADR-2040 D1 and D3) carries the Parquet tables the
//! statement reads ([`FlightTicket::parquet_tables`]): for each, the table
//! name and the version of the immutable manifest object `GetFlightInfo`
//! resolved. A Parquet table has no segment set, so before this version `DoGet`
//! resolved each table's newest manifest again, and a table replaced between
//! the two RPCs streamed a schema other than the one `FlightInfo` advertised.
//! The `flight` module doc, "The two-RPC problem, and the pin", states how
//! `DoGet` redeems these pins. Version 8 also carries the request's lowered
//! budgets ([`FlightTicket::budgets`]), which `GetFlightInfo` applied while
//! resolving and planning and which bind `DoGet`'s scans the same way. A v7 (or
//! earlier) ticket is rejected with [`FlightTicketError::UnsupportedVersion`],
//! never reinterpreted under the v8 layout. Consistent with every earlier
//! extension, this is a version bump on an ephemeral MAC'd blob, not a `.proto`
//! schema change and not an ADR of its own.
//!
//! # Version history
//!
//! - 1: segment key, writer epoch and content hash only.
//! - 2: every [`SegmentRef`] field, FNV-1a-64 checksum.
//! - 3: keyed BLAKE3 MAC replaces the checksum.
//! - 4: `slice_index`/`slice_count` (ADR-0071).
//! - 5: pending erasure predicates (ADR-0064).
//! - 6: declared attribute columns (ADR-0090).
//! - 7: per-segment on-object format version (issue #862).
//! - 8: pinned Parquet tables and lowered budgets (issue #2240).
//! - 9: the slice pushdown block after the statement (issue #2755). Written
//!   only when a pushdown is attached; a ticket without one stays 8, and this
//!   codec decodes both.
//! - 10: one segment group per signal table in place of the flat pins, for a
//!   statement naming two or more signal tables (ADR-2904 decision 10).
//!   Written only then and only on the client surface; every other ticket
//!   stays 8 or 9.
//! - 11: a logs pushdown block ends with the exact-key equalities the
//!   worker's plan probes KEY_IDX with (ADR-2707 decision 5, issue #2762).
//!   Written only when the block carries at least one; every other slice
//!   ticket keeps version 8 or 9. Version 11 is a slice-surface version: it
//!   follows 9's layout, not 10's, so the slice surface decodes 8, 9 and 11
//!   and the client surface 8, 9 and 10. A version 9 worker refuses a
//!   version 11 ticket on its version byte.
//!
//! Every layout change bumps the byte, even though a ticket is ephemeral,
//! because the build that mints a ticket and the build that redeems it can
//! differ: a slice ticket is minted by a coordinator and redeemed by a worker
//! in another process, and during a rolling upgrade the two run different
//! releases. A decoder that meets a version its surface does not accept,
//! including one above its own, returns
//! [`FlightTicketError::UnsupportedVersion`] after the MAC verifies, so skew
//! reads as skew. Without the bump a version-8 decoder parses up to the
//! statement and refuses the block as [`FlightTicketError::TrailingBytes`],
//! which the slice surface could not tell apart from a forged ticket.

use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::key_index::KeyType;
use ravel_logseg::{AttrValue, FieldSel, FieldType, Predicate};
use ravel_pqtable::names::validate_table;
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{ByteLimit, KeyEquals, RequestBudgets, RequestLimit};
use ravel_types::{CommitToken, TenantHash};
use uuid::Uuid;

use crate::declared::{DeclaredColumn, DeclaredType};
use crate::error::SqlError;
use crate::executor::{SIGNAL_LANE_ORDER, SqlExecutor, TargetSignal, pinned_signals_mismatch};
use crate::logs_pushdown::LogsPushdown;
use crate::parquet::{MAX_STATEMENT_TABLE_NAMES, ParquetPin};
use crate::spans_pushdown::SpansPushdown;

/// Maximum accepted SQL statement length, in bytes. 64 KiB. Longer
/// statements are rejected at [`FlightTicket::encode`] time and refused at
/// [`FlightTicket::decode`] time.
pub const MAX_STATEMENT_LEN: usize = 64 * 1024;

const MAGIC: [u8; 4] = *b"RFT1";
/// Version of a ticket without the slice pushdown block, and the oldest this
/// codec decodes.
const VERSION: u8 = 8;
/// Version of a ticket carrying the slice pushdown block with no exact-key
/// equality.
pub(crate) const VERSION_PUSHDOWN: u8 = 9;
/// Version of a ticket carrying one segment group per signal table (ADR-2904
/// decision 10), and the newest the client surface decodes. A slice ticket is
/// never this version.
pub(crate) const VERSION_GROUPS: u8 = 10;
/// Version of a ticket whose logs pushdown block carries exact-key
/// equalities (ADR-2707 decision 5), and the newest the slice surface
/// decodes. Its layout is version 9's plus the key section, so a client
/// ticket is never this version.
pub(crate) const VERSION_KEY_EQUALS: u8 = 11;

/// The versions the client surface decodes.
const CLIENT_VERSIONS: &[u8] = &[VERSION, VERSION_PUSHDOWN, VERSION_GROUPS];
/// The versions the slice surface decodes. Version 10 is a client ticket.
const SLICE_VERSIONS: &[u8] = &[VERSION, VERSION_PUSHDOWN, VERSION_KEY_EQUALS];
/// Every version this codec decodes when no surface is named.
const ALL_VERSIONS: &[u8] = &[
    VERSION,
    VERSION_PUSHDOWN,
    VERSION_GROUPS,
    VERSION_KEY_EQUALS,
];

/// Wire tag of each signal table's group in a version 10 ticket. Parquet
/// tables have no segment set and so no tag.
const SIGNAL_TAG_SAMPLES: u8 = 1;
const SIGNAL_TAG_LOGS: u8 = 2;
const SIGNAL_TAG_SPANS: u8 = 3;
const SIGNAL_TAG_ALERTS: u8 = 4;
const SIGNAL_TAG_AUDIT: u8 = 5;

fn signal_tag(signal: TargetSignal) -> Result<u8, FlightTicketError> {
    match signal {
        TargetSignal::Metrics => Ok(SIGNAL_TAG_SAMPLES),
        TargetSignal::Logs => Ok(SIGNAL_TAG_LOGS),
        TargetSignal::Spans => Ok(SIGNAL_TAG_SPANS),
        TargetSignal::Alerts => Ok(SIGNAL_TAG_ALERTS),
        TargetSignal::Audit => Ok(SIGNAL_TAG_AUDIT),
        TargetSignal::Parquet => Err(FlightTicketError::ParquetSignalGroup),
    }
}

fn signal_from_tag(tag: u8) -> Result<TargetSignal, FlightTicketError> {
    match tag {
        SIGNAL_TAG_SAMPLES => Ok(TargetSignal::Metrics),
        SIGNAL_TAG_LOGS => Ok(TargetSignal::Logs),
        SIGNAL_TAG_SPANS => Ok(TargetSignal::Spans),
        SIGNAL_TAG_ALERTS => Ok(TargetSignal::Alerts),
        SIGNAL_TAG_AUDIT => Ok(TargetSignal::Audit),
        other => Err(FlightTicketError::InvalidSignalTag(other)),
    }
}

/// Check a group list against the rules both encode and decode enforce: at
/// least two groups, no signal twice, and the groups in [`SIGNAL_LANE_ORDER`].
fn check_signal_order(signals: &[TargetSignal]) -> Result<(), FlightTicketError> {
    if signals.len() < 2 {
        return Err(FlightTicketError::TooFewSignalGroups(signals.len()));
    }
    let mut last = None;
    for (index, signal) in signals.iter().enumerate() {
        let tag = signal_tag(*signal)?;
        if signals[..index].contains(signal) {
            return Err(FlightTicketError::DuplicateSignalGroup(tag));
        }
        let position = SIGNAL_LANE_ORDER
            .iter()
            .position(|lane| lane == signal)
            .ok_or(FlightTicketError::ParquetSignalGroup)?;
        if last.is_some_and(|last| position < last) {
            return Err(FlightTicketError::SignalGroupsOutOfOrder);
        }
        last = Some(position);
    }
    Ok(())
}

/// Length in bytes of the trailing keyed-MAC tag ([`mac`]).
const MAC_LEN: usize = 32;

/// Length in bytes of the secret key [`FlightTicket::encode`] and
/// [`FlightTicket::decode`] are keyed by.
pub const TICKET_KEY_LEN: usize = 32;

/// The secret MAC key a ticket is signed and verified with.
///
/// In a single-process deployment this is generated once when the
/// `FlightSqlService` is constructed (see `crate::flight::service`) and held
/// only in memory: it is never logged, sent to a client, or persisted. A
/// process restart mints a fresh key, which is safe because a ticket is
/// ephemeral by construction and never expected to outlive the process that
/// minted it.
///
/// A distributed deployment (ADR-0071) cannot use a per-process
/// random key: a coordinator mints a slice ticket that a *different* worker
/// process must verify, so every process in the cluster must agree on the key.
/// There the key is derived deterministically from the shared cluster secret
/// with [`derive_ticket_key`]; see its docs.
pub type TicketKey = [u8; TICKET_KEY_LEN];

/// BLAKE3 domain-separation context for [`derive_ticket_key`]. Changing this
/// string rotates every cluster's derived key; it is versioned with the codec.
const TICKET_KEY_DERIVATION_CONTEXT: &str =
    "ravel-sql flight ticket MAC key 2026-08 (RFT1 v4, ADR-0071)";

/// Derive a deterministic [`TicketKey`] from a cluster-shared secret.
///
/// A single-process deployment mints a random key. A distributed deployment
/// cannot: a coordinator signs a slice ticket that a *different* worker process
/// redeems, so all processes must key their MAC identically. Before ADR-1689,
/// every process in an ADR-0071 cluster derived the ticket key from one shared
/// secret, the lowercase hex of the first key of its `--fragment-key-file`.
/// `blake3::derive_key` with a fixed context is a proper KDF: it maps the
/// arbitrary-length secret to a 32-byte key and never uses the secret as a MAC
/// key directly, so the fragment key and the ticket key are cryptographically
/// independent. The result is still secret (never logged, sent, or persisted);
/// it is merely reproducible across processes and restarts, which a distributed
/// ticket requires.
///
/// ADR-1689 decision 2 moves the ticket MAC onto per-surface keys
/// ([`SqlTicketKeys`]) read from `--sql-ticket-key-file`, and from release B
/// (decision 4) the server derives nothing from the fragment key file. A
/// caller may still pass this function's output as
/// `DistributedFlightConfig::shared_ticket_key`, which the Flight service
/// treats as one file key and derives both surface keys from. The deployment
/// guide's recipe computes the same key, for a one-line key file that agrees
/// with a node that derived its key this way.
pub fn derive_ticket_key(shared_secret: &[u8]) -> TicketKey {
    blake3::derive_key(TICKET_KEY_DERIVATION_CONTEXT, shared_secret)
}

/// BLAKE3 context for the MAC key of client whole-set tickets (ADR-1689
/// decision 2). Distinct from [`SLICE_KEY_CONTEXT`], so a ticket minted for one
/// surface fails the MAC on the other.
const CLIENT_KEY_CONTEXT: &str =
    "ravel-sql flight client whole-set ticket MAC key 2026-09 (RFT1, ADR-1689)";

/// BLAKE3 context for the MAC key of coordinator-minted slice capabilities
/// (ADR-1689 decision 2).
const SLICE_KEY_CONTEXT: &str =
    "ravel-sql flight slice capability MAC key 2026-09 (RFT1, ADR-1689)";

/// Which Flight surface a ticket is minted for and verified on (ADR-1689
/// decision 2). The ticket layout is the same on both; only the MAC key
/// differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketSurface {
    /// The whole-set ticket `GetFlightInfo` hands an external client, redeemed
    /// with that client's own credential.
    Client,
    /// A slice ticket a coordinator mints for a worker. It is the capability
    /// itself: no client credential travels with it.
    Slice,
}

/// Derive one surface's MAC key from a file key: `blake3::derive_key` under a
/// context that names the surface.
pub fn derive_surface_key(file_key: &[u8], surface: TicketSurface) -> TicketKey {
    let context = match surface {
        TicketSurface::Client => CLIENT_KEY_CONTEXT,
        TicketSurface::Slice => SLICE_KEY_CONTEXT,
    };
    blake3::derive_key(context, file_key)
}

/// The Flight SQL ticket keys of one process (ADR-1689 decision 2): two MAC
/// keys per file key, one per [`TicketSurface`]. The first file key mints; a
/// ticket verifies under any of them, so a key file can rotate without a flag
/// day, the same rule `--fragment-key-file` follows.
#[derive(Clone)]
pub struct SqlTicketKeys {
    client_mint: TicketKey,
    slice_mint: TicketKey,
    client: Vec<TicketKey>,
    slice: Vec<TicketKey>,
}

impl std::fmt::Debug for SqlTicketKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Key material is secret; print only how many keys are configured.
        f.debug_struct("SqlTicketKeys")
            .field("file_keys", &self.client.len())
            .finish_non_exhaustive()
    }
}

impl SqlTicketKeys {
    /// Derive both surface keys from one file key.
    pub fn from_file_key(file_key: &[u8]) -> Self {
        let client_mint = derive_surface_key(file_key, TicketSurface::Client);
        let slice_mint = derive_surface_key(file_key, TicketSurface::Slice);
        SqlTicketKeys {
            client_mint,
            slice_mint,
            client: vec![client_mint],
            slice: vec![slice_mint],
        }
    }

    /// Derive both surface keys from each file key, in order. Returns `None`
    /// for an empty key list: a process with no key can neither mint nor
    /// verify, and refusing here keeps that from surfacing as a MAC failure on
    /// every ticket.
    pub fn from_file_keys<K: AsRef<[u8]>>(file_keys: impl IntoIterator<Item = K>) -> Option<Self> {
        let (client, slice): (Vec<_>, Vec<_>) = file_keys
            .into_iter()
            .map(|key| {
                (
                    derive_surface_key(key.as_ref(), TicketSurface::Client),
                    derive_surface_key(key.as_ref(), TicketSurface::Slice),
                )
            })
            .unzip();
        let client_mint = *client.first()?;
        let slice_mint = *slice.first()?;
        Some(SqlTicketKeys {
            client_mint,
            slice_mint,
            client,
            slice,
        })
    }

    fn keys(&self, surface: TicketSurface) -> &[TicketKey] {
        match surface {
            TicketSurface::Client => &self.client,
            TicketSurface::Slice => &self.slice,
        }
    }

    /// The key `surface`'s tickets are minted with: the one derived from the
    /// first file key.
    pub fn mint_key(&self, surface: TicketSurface) -> &TicketKey {
        match surface {
            TicketSurface::Client => &self.client_mint,
            TicketSurface::Slice => &self.slice_mint,
        }
    }

    /// Sign `ticket` for `surface` with its mint key. A ticket carrying signal
    /// groups is refused on the slice surface
    /// ([`FlightTicketError::SignalGroupsOnSlice`]).
    pub fn encode(
        &self,
        ticket: &FlightTicket,
        surface: TicketSurface,
    ) -> Result<Vec<u8>, FlightTicketError> {
        self.encode_with_pushdown(ticket, surface, None)
    }

    /// [`Self::encode`] with a slice pushdown block
    /// ([`FlightTicket::encode_with_pushdown`]).
    pub fn encode_with_pushdown(
        &self,
        ticket: &FlightTicket,
        surface: TicketSurface,
        pushdown: Option<&SlicePushdown>,
    ) -> Result<Vec<u8>, FlightTicketError> {
        if surface == TicketSurface::Slice && !ticket.signal_groups.is_empty() {
            return Err(FlightTicketError::SignalGroupsOnSlice);
        }
        ticket.encode_with_pushdown(self.mint_key(surface), pushdown)
    }

    /// Decode `bytes` as a `surface` ticket, accepting a MAC under any of that
    /// surface's keys. [`FlightTicketError::MacMismatch`] when none verifies;
    /// any other error is the first key's structural refusal, which does not
    /// depend on the key. The client surface decodes versions 8, 9 and 10,
    /// the slice surface 8, 9 and 11: a version 10 ticket on the slice
    /// surface, or a version 11 ticket on the client surface, is
    /// [`FlightTicketError::UnsupportedVersion`].
    pub fn decode(
        &self,
        bytes: &[u8],
        surface: TicketSurface,
    ) -> Result<FlightTicket, FlightTicketError> {
        self.decode_with_pushdown(bytes, surface)
            .map(|(ticket, _)| ticket)
    }

    /// [`Self::decode`], also returning the ticket's slice pushdown block
    /// ([`FlightTicket::decode_with_pushdown`]).
    pub fn decode_with_pushdown(
        &self,
        bytes: &[u8],
        surface: TicketSurface,
    ) -> Result<(FlightTicket, Option<SlicePushdown>), FlightTicketError> {
        let accepted = match surface {
            TicketSurface::Client => CLIENT_VERSIONS,
            TicketSurface::Slice => SLICE_VERSIONS,
        };
        for key in self.keys(surface) {
            match FlightTicket::decode_accepting(bytes, key, accepted) {
                Err(FlightTicketError::MacMismatch) => continue,
                other => return other,
            }
        }
        Err(FlightTicketError::MacMismatch)
    }
}

/// Smallest possible encoded ticket: the fixed header (including the
/// `slice_index`/`slice_count` pair) plus the trailing MAC, with zero tokens,
/// zero segments, zero pending-erasure predicates, zero declared columns, zero
/// Parquet tables, no budgets, and an empty statement.
const MIN_ENCODED_LEN: usize = 4 + 1 + 16 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 1 + MAC_LEN;

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
    /// On-object format version, mirrored from
    /// [`SegmentRef::segment_format_version`]. Pinned so `DoGet` reconstructs
    /// the same read-shape routing the coordinator's snapshot carried: the
    /// whole-segment fast path keeps the ranged column-chunk read on RLOG v4
    /// objects and off v3 (issue #862). Without it a worker would reconstruct
    /// every segment at a default version and misroute a v3 logs segment.
    pub segment_format_version: u32,
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
            segment_format_version: seg.segment_format_version,
        }
    }

    /// Rebuild the [`SegmentRef`] this pin was projected from.
    ///
    /// Version 2 carries every identity, pruning, and routing field, which is
    /// what lets `DoGet` reconstruct the resolved `Snapshot` without a second
    /// `Catalog::resolve`. It does not carry
    /// [`SegmentRef::declared_column_stats`] (ADR-0873): the pin's wire layout
    /// has its own version byte, so adding them is a ticket format change of
    /// its own. A rebuilt ref is therefore uncovered for every declared
    /// column, which costs `DoGet` the statistics shortcut and never a wrong
    /// answer.
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
            segment_format_version: self.segment_format_version,
            // Not carried by the pin's wire layout (see this method's docs):
            // uncovered, which is a legal permanent state for any segment.
            declared_column_stats: Default::default(),
        }
    }

    /// Append this pin's wire layout to `buf`: the same bytes
    /// [`FlightTicket::encode`] writes for each of its own segments, in the
    /// same order.
    ///
    /// Reachable under the `pin-codec` feature so a crate that pins a
    /// resolved snapshot in a token of its own (`ravel-mcp`'s cursors,
    /// ADR-1374 decision 9) reuses this layout rather than defining a second,
    /// narrower one that would silently drop identity, pruning, or routing
    /// fields. The bytes carry no magic, no version, and no MAC of their own:
    /// the enclosing token supplies all three, and both callers here cover
    /// these bytes with their own MAC.
    ///
    /// Returns [`FlightTicketError::FieldTooLong`] if the object key's length
    /// would not fit in a `u32`.
    pub fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), FlightTicketError> {
        write_segment_pin(buf, self)
    }

    /// Decode one pin from the front of `bytes`, returning it and the number
    /// of bytes it consumed, so a caller reading a sequence of pins knows
    /// where the next one starts. Trailing bytes are left to the caller and
    /// are not an error here.
    ///
    /// Every malformed or truncated input yields a typed
    /// [`FlightTicketError`], never a panic.
    pub fn decode_from(bytes: &[u8]) -> Result<(SegmentPin, usize), FlightTicketError> {
        let mut cur = Cursor::new(bytes);
        let pin = read_segment_pin(&mut cur)?;
        Ok((pin, cur.pos))
    }
}

/// One signal table's pinned segments and pending erasure inside a version 10
/// [`FlightTicket`] (ADR-2904 decision 10): what `GetFlightInfo` resolved for
/// that table, which `DoGet` rebuilds into that table's own snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalGroup {
    /// The signal table the group pins. Never [`TargetSignal::Parquet`].
    pub signal: TargetSignal,
    /// The segments that table's resolve returned, in resolve order.
    pub segments: Vec<SegmentPin>,
    /// The pending selective-erasure predicates that table's resolve saw.
    pub pending_erasure: Vec<ErasurePredicate>,
}

impl SignalGroup {
    /// Project one signal's resolved snapshot onto the wire layout.
    pub fn from_snapshot(signal: TargetSignal, snapshot: &ravel_catalog::Snapshot) -> Self {
        SignalGroup {
            signal,
            segments: snapshot
                .segments
                .iter()
                .map(SegmentPin::from_segment_ref)
                .collect(),
            pending_erasure: ravel_query::erasure::snapshot_pending_erasure_predicates(snapshot),
        }
    }

    /// Rebuild the snapshot this group pinned, the same way
    /// [`FlightTicket::snapshot`] rebuilds a single-signal ticket's.
    pub fn snapshot(&self) -> ravel_catalog::Snapshot {
        rebuild_snapshot(&self.segments, &self.pending_erasure)
    }
}

fn rebuild_snapshot(
    segments: &[SegmentPin],
    pending_erasure: &[ErasurePredicate],
) -> ravel_catalog::Snapshot {
    ravel_catalog::Snapshot {
        segments: segments.iter().map(SegmentPin::to_segment_ref).collect(),
        segments_pruned: 0,
        pending_erasure: pending_erasure.iter().map(to_erasure_request).collect(),
    }
}

/// A self-describing, snapshot-pinning Flight SQL ticket.
///
/// Round-trips bit-for-bit through [`FlightTicket::encode`] /
/// [`FlightTicket::decode`]. See the module docs for the wire layout and the
/// security posture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightTicket {
    /// Tenant the ticket was minted for. On a client ticket, compared against
    /// the authoritative gRPC-metadata tenant at `DoGet`; on a slice ticket,
    /// the tenant the worker executes under (see the module docs).
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
    /// This ticket's 0-based position in a distributed fan-out
    /// (ADR-0071). A whole-snapshot ticket carries `0`. Carried so a
    /// coordinator/worker can identify and log which slice a ticket pins; it
    /// is not a trust boundary and `snapshot()` ignores it.
    pub slice_index: u32,
    /// Number of slices the resolved snapshot was fanned out into; `1` for a
    /// whole-snapshot ticket. With [`slice_index`](Self::slice_index) this
    /// makes `segments` an identified *slice* of the pinned set rather than
    /// the whole set.
    pub slice_count: u32,
    /// Pending selective-erasure predicates from the resolved snapshot
    /// (ADR-0064 decision 3): [`FlightTicket::snapshot`] carries
    /// this set into the rebuilt `Snapshot.pending_erasure` so `DoGet`
    /// excludes exactly what `GetFlightInfo`'s resolve saw pending, never a
    /// re-resolution and never an empty set.
    pub pending_erasure: Vec<ErasurePredicate>,
    /// The tenant's declared typed attribute columns resolved at
    /// `GetFlightInfo` (ADR-0090 decision 2). Pinned into the ticket so `DoGet`
    /// plans the `logs` table against the exact same declared schema
    /// `GetFlightInfo` advertised, never a re-resolution: the real
    /// `DeclaredColumnSource` is a cache-aside overlay whose result is not a
    /// pure function of `(tenant, now_ns)`, so a concurrent refresh between the
    /// two RPCs could otherwise stream a batch schema that disagrees with the
    /// advertised `FlightInfo` schema. Empty for a metrics/spans query or a
    /// tenant with no declared columns.
    pub declared_columns: Vec<DeclaredColumn>,
    /// The Parquet tables the statement reads, each at the manifest version
    /// `GetFlightInfo` resolved (ADR-2040 D1, D3), redeemed as the `flight`
    /// module doc states. At most [`MAX_STATEMENT_TABLE_NAMES`]; empty
    /// for a statement over a signal table. A slice ticket carries none.
    pub parquet_tables: Vec<ParquetPin>,
    /// The request's lowered budgets (ADR-1374 decision 3), which can only
    /// lower the executor's ceilings. `None` runs under the executor's own
    /// configuration.
    pub budgets: Option<RequestBudgets>,
    /// One group per signal table the statement names, in
    /// [`SIGNAL_LANE_ORDER`], when it names two or more (ADR-2904 decision
    /// 10). Non-empty makes the ticket version 10, which carries these
    /// instead of [`Self::segments`] and [`Self::pending_erasure`], both then
    /// empty. Empty for a statement over at most one signal table, whose
    /// ticket keeps the version 8 or 9 encoding byte for byte.
    pub signal_groups: Vec<SignalGroup>,
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
        self.encode_with_pushdown(key, None)
    }

    /// [`Self::encode`], plus the coordinator's scan pushdown for a
    /// distributed slice as the optional trailing block the module doc
    /// describes. `None` produces exactly the bytes [`Self::encode`] does.
    pub fn encode_with_pushdown(
        &self,
        key: &TicketKey,
        pushdown: Option<&SlicePushdown>,
    ) -> Result<Vec<u8>, FlightTicketError> {
        if self.statement.len() > MAX_STATEMENT_LEN {
            return Err(FlightTicketError::StatementTooLong {
                len: self.statement.len(),
                max: MAX_STATEMENT_LEN,
            });
        }

        let grouped = !self.signal_groups.is_empty();
        if grouped {
            if !self.segments.is_empty() || !self.pending_erasure.is_empty() {
                return Err(FlightTicketError::SignalGroupsBesideFlatPins);
            }
            if pushdown.is_some() {
                return Err(FlightTicketError::SignalGroupsWithPushdown);
            }
            let signals: Vec<TargetSignal> = self
                .signal_groups
                .iter()
                .map(|group| group.signal)
                .collect();
            check_signal_order(&signals)?;
        }

        let mut buf = Vec::with_capacity(MIN_ENCODED_LEN + self.statement.len());
        buf.extend_from_slice(&MAGIC);
        buf.push(match pushdown {
            _ if grouped => VERSION_GROUPS,
            Some(p) if p.carries_key_equals() => VERSION_KEY_EQUALS,
            Some(_) => VERSION_PUSHDOWN,
            None => VERSION,
        });
        buf.extend_from_slice(&self.tenant.0);
        buf.extend_from_slice(&self.now_ns.to_le_bytes());
        buf.extend_from_slice(&self.deadline_ns.to_le_bytes());
        write_u32(&mut buf, self.slice_index);
        write_u32(&mut buf, self.slice_count);

        write_u32(&mut buf, u32_len(self.min_commit_tokens.len())?);
        for token in &self.min_commit_tokens {
            write_len_prefixed(&mut buf, token.encode().as_bytes())?;
        }

        if grouped {
            write_u32(&mut buf, u32_len(self.signal_groups.len())?);
            for group in &self.signal_groups {
                buf.push(signal_tag(group.signal)?);
                write_pins(&mut buf, &group.segments, &group.pending_erasure)?;
            }
        } else {
            write_pins(&mut buf, &self.segments, &self.pending_erasure)?;
        }

        write_u32(&mut buf, u32_len(self.declared_columns.len())?);
        for column in &self.declared_columns {
            write_len_prefixed(&mut buf, column.key.as_bytes())?;
            buf.push(declared_type_tag(column.ty));
        }

        if self.parquet_tables.len() > MAX_STATEMENT_TABLE_NAMES {
            return Err(FlightTicketError::TooManyParquetTables {
                count: self.parquet_tables.len(),
                max: MAX_STATEMENT_TABLE_NAMES,
            });
        }
        write_u32(&mut buf, u32_len(self.parquet_tables.len())?);
        for pin in &self.parquet_tables {
            if validate_table(&pin.table).is_err() {
                return Err(FlightTicketError::InvalidParquetTable);
            }
            if pin.version == 0 {
                return Err(FlightTicketError::InvalidParquetVersion);
            }
            write_len_prefixed(&mut buf, pin.table.as_bytes())?;
            buf.extend_from_slice(&pin.version.to_le_bytes());
        }

        write_budgets(&mut buf, self.budgets.as_ref());

        write_len_prefixed(&mut buf, self.statement.as_bytes())?;

        if let Some(pushdown) = pushdown {
            write_pushdown(&mut buf, pushdown)?;
        }

        let tag = mac(key, &buf);
        buf.extend_from_slice(&tag);
        Ok(buf)
    }

    /// Decode from the wire layout, verifying the trailing MAC against `key`.
    /// Every malformed, truncated, corrupt, tampered, or trailing-garbage
    /// input yields a typed [`FlightTicketError`], never a panic.
    ///
    /// A ticket carrying a slice pushdown block decodes too: the block is
    /// validated and dropped. A caller that builds a scan from the ticket
    /// uses [`Self::decode_with_pushdown`] instead.
    pub fn decode(bytes: &[u8], key: &TicketKey) -> Result<FlightTicket, FlightTicketError> {
        Self::decode_with_pushdown(bytes, key).map(|(ticket, _)| ticket)
    }

    /// [`Self::decode`], also returning the slice pushdown block when the
    /// ticket carries one. A version 8 ticket has no block and decodes to
    /// `None`, which is the no-pushdown query.
    pub fn decode_with_pushdown(
        bytes: &[u8],
        key: &TicketKey,
    ) -> Result<(FlightTicket, Option<SlicePushdown>), FlightTicketError> {
        Self::decode_accepting(bytes, key, ALL_VERSIONS)
    }

    /// The decoder for the versions in `accepted`; any other version is
    /// [`FlightTicketError::UnsupportedVersion`]. Pinned at `[VERSION]` it is
    /// the decoder from before the pushdown block, which is how the tests
    /// reproduce an older worker.
    fn decode_accepting(
        bytes: &[u8],
        key: &TicketKey,
        accepted: &[u8],
    ) -> Result<(FlightTicket, Option<SlicePushdown>), FlightTicketError> {
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
        if !accepted.contains(&version) {
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

        // A version 10 ticket has no flat segment or erasure list: the group
        // list stands in their place, so both stay empty.
        let (segments, pending_erasure, signal_groups) = if version == VERSION_GROUPS {
            let group_count = cur.read_u32()?;
            let mut signal_groups = Vec::new();
            let mut signals = Vec::new();
            for _ in 0..group_count {
                let signal = signal_from_tag(cur.read_u8()?)?;
                signals.push(signal);
                // Refuse a repeat or an out-of-order group as soon as it is
                // read, before its pins are parsed.
                if signals.len() >= 2 {
                    check_signal_order(&signals)?;
                }
                let (segments, pending_erasure) = read_pins(&mut cur)?;
                signal_groups.push(SignalGroup {
                    signal,
                    segments,
                    pending_erasure,
                });
            }
            check_signal_order(&signals)?;
            (Vec::new(), Vec::new(), signal_groups)
        } else {
            let (segments, pending_erasure) = read_pins(&mut cur)?;
            (segments, pending_erasure, Vec::new())
        };

        let declared_count = cur.read_u32()?;
        let mut declared_columns = Vec::new();
        for _ in 0..declared_count {
            let key = cur.read_len_prefixed()?;
            let key = std::str::from_utf8(key).map_err(|_| FlightTicketError::InvalidUtf8)?;
            let ty = declared_type_from_tag(cur.read_u8()?)?;
            declared_columns.push(DeclaredColumn::new(key.to_owned(), ty));
        }

        let parquet_count = cur.read_u32()? as usize;
        if parquet_count > MAX_STATEMENT_TABLE_NAMES {
            return Err(FlightTicketError::TooManyParquetTables {
                count: parquet_count,
                max: MAX_STATEMENT_TABLE_NAMES,
            });
        }
        let mut parquet_tables = Vec::with_capacity(parquet_count);
        for _ in 0..parquet_count {
            let table = cur.read_len_prefixed()?;
            let table = std::str::from_utf8(table).map_err(|_| FlightTicketError::InvalidUtf8)?;
            if validate_table(table).is_err() {
                return Err(FlightTicketError::InvalidParquetTable);
            }
            let version = u64::from_le_bytes(cur.read_array::<8>()?);
            if version == 0 {
                return Err(FlightTicketError::InvalidParquetVersion);
            }
            parquet_tables.push(ParquetPin {
                table: table.to_owned(),
                version,
            });
        }

        let budgets = read_budgets(&mut cur)?;

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

        // Version 10 carries groups and no pushdown block.
        let pushdown = if version == VERSION_PUSHDOWN || version == VERSION_KEY_EQUALS {
            Some(read_pushdown(&mut cur, version)?)
        } else {
            None
        };

        if !cur.is_empty() {
            return Err(FlightTicketError::TrailingBytes);
        }

        let ticket = FlightTicket {
            tenant,
            statement: statement.to_owned(),
            segments,
            min_commit_tokens,
            now_ns,
            deadline_ns,
            slice_index,
            slice_count,
            pending_erasure,
            declared_columns,
            parquet_tables,
            budgets,
            signal_groups,
        };
        Ok((ticket, pushdown))
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
    /// snapshot `GetFlightInfo` resolved, never a re-resolution.
    /// Segment order is preserved from the resolve, which is already the
    /// catalog's deterministic provenance order.
    ///
    /// `segments_pruned` is 0: the ticket carries the segments that survived
    /// the original resolve, and redemption never re-resolves or re-prunes,
    /// so this snapshot excludes nothing of its own.
    ///
    /// `pending_erasure` carries [`Self::pending_erasure`] back into the
    /// proto-shaped `Snapshot` field (ADR-0064 decision 3), so
    /// `RavelTableProvider::new` derives the same predicate set from a
    /// redeemed ticket that `GetFlightInfo`'s resolve saw pending -- never an
    /// empty set regardless of what the resolve actually found.
    ///
    /// A version 10 ticket pins one snapshot per signal table and has no
    /// single one: [`Self::signal_snapshots`] rebuilds those, and this method
    /// returns [`FlightTicketError::SignalGroupsHaveNoSingleSnapshot`] for it
    /// rather than an empty snapshot.
    pub fn snapshot(&self) -> Result<ravel_catalog::Snapshot, FlightTicketError> {
        if !self.signal_groups.is_empty() {
            return Err(FlightTicketError::SignalGroupsHaveNoSingleSnapshot);
        }
        Ok(rebuild_snapshot(&self.segments, &self.pending_erasure))
    }

    /// One rebuilt snapshot per signal group, in the groups' lane order:
    /// what `DoGet` plans a version 10 ticket against. Empty for a version 8
    /// or 9 ticket.
    pub fn signal_snapshots(&self) -> Vec<(TargetSignal, ravel_catalog::Snapshot)> {
        self.signal_groups
            .iter()
            .map(|group| (group.signal, group.snapshot()))
            .collect()
    }

    /// The snapshots `DoGet` plans this ticket's statement against, one per
    /// signal lane, checked against the statement before any store request.
    ///
    /// A version 8 or 9 ticket pins one snapshot, so its statement must name
    /// at most one signal table ([`SqlError::TicketPinsOneSignal`]
    /// otherwise). A version 10 ticket's groups must name exactly the signal
    /// tables the statement reads ([`SqlError::PinnedSignalsMismatch`]
    /// otherwise). The statement's tables come from the same parse every plan
    /// takes ([`SqlExecutor::statement_tables`]).
    pub(crate) fn redemption_lanes(
        &self,
    ) -> Result<Vec<(TargetSignal, ravel_catalog::Snapshot)>, SqlError> {
        let tables = SqlExecutor::statement_tables(&self.statement)?;
        if self.signal_groups.is_empty() {
            if tables.signals.len() > 1 {
                return Err(SqlError::TicketPinsOneSignal);
            }
            return Ok(vec![(
                tables.first_lane(),
                rebuild_snapshot(&self.segments, &self.pending_erasure),
            )]);
        }
        let pinned: Vec<TargetSignal> = self.signal_groups.iter().map(|g| g.signal).collect();
        if pinned != tables.signals {
            return Err(pinned_signals_mismatch(&pinned, &tables.signals));
        }
        Ok(self.signal_snapshots())
    }
}

/// The coordinator's extracted scan pushdown for one distributed slice
/// (issue #2755): what lets a worker decode only the blocks the statement's
/// predicate admits instead of every block of every pinned object.
///
/// It carries a fixed subset of what the extractors find. For spans: the
/// time bounds and the `trace_id =` equality. For logs: the time bounds and
/// the prune-only attribute comparisons (`attrs['k'] = 'v'` and the declared
/// column comparisons of ADR-0093). Every carried field only narrows which
/// blocks a worker reads; the coordinator still evaluates the full filter
/// above the merged slices, because the providers report every filter
/// `Inexact` in distributed mode. A worker that ignores the block therefore
/// returns the same rows, only after decoding more.
///
/// The constructors keep exactly that subset, so a value always round-trips
/// through [`FlightTicket::encode_with_pushdown`] and
/// [`FlightTicket::decode_with_pushdown`] unchanged.
#[derive(Debug, Clone, PartialEq)]
pub struct SlicePushdown(SlicePushdownKind);

#[derive(Debug, Clone, PartialEq)]
enum SlicePushdownKind {
    Spans(SpansPushdown),
    Logs(LogsPushdown),
}

impl SlicePushdown {
    /// The spans subset of `pushdown`: `ts_lo`, `ts_hi` and `trace_id`.
    pub fn spans(pushdown: &SpansPushdown) -> Self {
        SlicePushdown(SlicePushdownKind::Spans(SpansPushdown {
            ts_lo: pushdown.ts_lo,
            ts_hi: pushdown.ts_hi,
            trace_id: pushdown.trace_id,
            ..SpansPushdown::default()
        }))
    }

    /// The logs subset of `pushdown`: `ts_lo`, `ts_hi`, the prune
    /// predicates the layout can carry, and the exact-key equalities. A prune
    /// predicate outside the layout is dropped, which only widens what the
    /// worker reads; the content channel is not carried.
    pub fn logs(pushdown: &LogsPushdown) -> Self {
        SlicePushdown(SlicePushdownKind::Logs(LogsPushdown {
            ts_lo: pushdown.ts_lo,
            ts_hi: pushdown.ts_hi,
            content: Vec::new(),
            prune: pushdown
                .prune
                .iter()
                .filter(|p| prune_is_carried(p))
                .cloned()
                .collect(),
            key_equals: pushdown.key_equals.clone(),
        }))
    }

    /// Whether this is a logs block carrying exact-key equalities, which only
    /// a version [`VERSION_KEY_EQUALS`] ticket can hold.
    fn carries_key_equals(&self) -> bool {
        matches!(&self.0, SlicePushdownKind::Logs(p) if !p.key_equals.is_empty())
    }

    /// The spans pushdown this block carries, or `None` for a logs block.
    pub fn as_spans(&self) -> Option<&SpansPushdown> {
        match &self.0 {
            SlicePushdownKind::Spans(p) => Some(p),
            SlicePushdownKind::Logs(_) => None,
        }
    }

    /// The logs pushdown this block carries, or `None` for a spans block.
    pub fn as_logs(&self) -> Option<&LogsPushdown> {
        match &self.0 {
            SlicePushdownKind::Logs(p) => Some(p),
            SlicePushdownKind::Spans(_) => None,
        }
    }
}

/// Whether the slice pushdown layout carries `p`: an equality on an
/// attribute against a string or bytes value, or a numeric range on an
/// attribute. These are the only shapes `extract_logs` puts in its prune
/// channel.
fn prune_is_carried(p: &Predicate) -> bool {
    matches!(
        p,
        Predicate::Equals {
            field: FieldSel::Attr(_),
            value: AttrValue::Str(_) | AttrValue::Bytes(_),
        } | Predicate::NumRange {
            field: FieldSel::Attr(_),
            ty: FieldType::I64 | FieldType::F64 | FieldType::Bool,
            ..
        }
    )
}

/// Adapt the ticket's leaner [`ErasurePredicate`] (matchers plus an optional
/// window) into the proto-shaped [`ErasureRequest`] `Snapshot.pending_erasure`
/// is typed as. Every other `ErasureRequest` field (`request_id`,
/// `created_unix_ns`, `reason`, ...) is durable-record metadata that
/// `ravel_query::erasure::snapshot_pending_erasure_predicates` itself
/// documents as "play no part in filtering," so this only ever needs to
/// populate `predicate` and the window bounds.
fn to_erasure_request(predicate: &ErasurePredicate) -> ErasureRequest {
    ErasureRequest {
        predicate: predicate
            .matchers()
            .iter()
            .map(|(key, value)| ErasurePredicateMatcher {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        window_start_ns: predicate.window_start_ns(),
        window_end_ns: predicate.window_end_ns(),
        ..Default::default()
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
    /// A declared column's type tag byte was not one of the four declarable
    /// types (ADR-0090).
    #[error("ticket contains an invalid declared column type tag {0}")]
    InvalidDeclaredType(u8),
    /// The ticket pins more Parquet tables than a statement may name
    /// ([`MAX_STATEMENT_TABLE_NAMES`]), at encode or decode.
    #[error("ticket pins {count} Parquet tables, more than the {max} a statement may name")]
    TooManyParquetTables { count: usize, max: usize },
    /// A pinned Parquet table's name is not a valid table name.
    #[error("ticket contains an invalid Parquet table name")]
    InvalidParquetTable,
    /// A pinned Parquet table's manifest version is 0; manifest versions
    /// start at 1.
    #[error("ticket pins Parquet manifest version 0; versions start at 1")]
    InvalidParquetVersion,
    /// A budget limit's tag byte, or the budgets flag, was not one the layout
    /// defines.
    #[error("ticket contains an invalid budget tag {0}")]
    InvalidBudgetTag(u8),
    /// A tag byte in the slice pushdown block (its kind, an optional-bound
    /// flag, a predicate tag, a value tag, or a range type) was not one the
    /// layout defines.
    #[error("ticket contains an invalid slice pushdown tag {0}")]
    InvalidPushdownTag(u8),
    /// A slice pushdown held a prune predicate the layout has no encoding
    /// for. [`SlicePushdown::logs`] drops those, so this marks a value built
    /// some other way.
    #[error("slice pushdown holds a predicate the ticket layout cannot carry")]
    UncarriedPushdown,
    /// Bytes remained after the last field was read.
    #[error("ticket has trailing bytes")]
    TrailingBytes,
    /// A ticket set both signal groups and the flat segment or erasure list;
    /// a version 10 ticket carries its pins in the groups alone.
    #[error("ticket sets both signal groups and a flat segment or erasure list")]
    SignalGroupsBesideFlatPins,
    /// A ticket set both signal groups and a slice pushdown block; a version
    /// 10 ticket carries no pushdown.
    #[error("ticket sets both signal groups and a slice pushdown block")]
    SignalGroupsWithPushdown,
    /// A ticket carrying signal groups was minted for the slice surface,
    /// which never carries one.
    #[error("a slice ticket cannot carry signal groups")]
    SignalGroupsOnSlice,
    /// A version 10 ticket carried fewer than two signal groups.
    #[error("ticket carries {0} signal groups; a grouped ticket carries at least two")]
    TooFewSignalGroups(usize),
    /// A signal group's tag byte names no signal table.
    #[error("ticket contains an invalid signal group tag {0}")]
    InvalidSignalTag(u8),
    /// A signal group named the Parquet target, which has no segment set.
    #[error("a signal group cannot pin Parquet tables")]
    ParquetSignalGroup,
    /// Two signal groups carried the same signal tag.
    #[error("ticket carries signal group tag {0} twice")]
    DuplicateSignalGroup(u8),
    /// The signal groups were not in the statement lane order (samples, logs,
    /// spans, alerts, audit).
    #[error("ticket signal groups are out of lane order")]
    SignalGroupsOutOfOrder,
    /// [`FlightTicket::snapshot`] was asked for the single snapshot of a
    /// version 10 ticket, which pins one per signal group.
    #[error("a ticket with signal groups pins one snapshot per signal, not one")]
    SignalGroupsHaveNoSingleSnapshot,
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

/// The per-segment field order of the ticket's wire layout, in one place so
/// [`FlightTicket::encode`] and [`SegmentPin::encode_into`] cannot drift
/// apart: the two must produce identical bytes for the same pin, since the
/// same reader below parses both.
fn write_segment_pin(buf: &mut Vec<u8>, seg: &SegmentPin) -> Result<(), FlightTicketError> {
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
    write_len_prefixed(buf, seg.data_object_key.as_bytes())?;
    write_segment_level(buf, &seg.level);
    write_u32(buf, seg.segment_format_version);
    Ok(())
}

/// Inverse of [`write_segment_pin`], reading one pin from `cur`.
fn read_segment_pin(cur: &mut Cursor<'_>) -> Result<SegmentPin, FlightTicketError> {
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
    let data_object_key = std::str::from_utf8(key).map_err(|_| FlightTicketError::InvalidUtf8)?;
    let level = read_segment_level(cur)?;
    let segment_format_version = cur.read_u32()?;
    Ok(SegmentPin {
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
        segment_format_version,
    })
}

/// A segment list then an erasure list, each count-prefixed: the flat lists
/// of a version 8 or 9 ticket and the body of each version 10 group.
fn write_pins(
    buf: &mut Vec<u8>,
    segments: &[SegmentPin],
    pending_erasure: &[ErasurePredicate],
) -> Result<(), FlightTicketError> {
    write_u32(buf, u32_len(segments.len())?);
    for seg in segments {
        write_segment_pin(buf, seg)?;
    }
    write_u32(buf, u32_len(pending_erasure.len())?);
    for predicate in pending_erasure {
        write_u32(buf, u32_len(predicate.matchers().len())?);
        for (key, value) in predicate.matchers() {
            write_len_prefixed(buf, key.as_bytes())?;
            write_len_prefixed(buf, value.as_bytes())?;
        }
        buf.extend_from_slice(&predicate.window_start_ns().to_le_bytes());
        buf.extend_from_slice(&predicate.window_end_ns().to_le_bytes());
    }
    Ok(())
}

/// Inverse of [`write_pins`].
fn read_pins(
    cur: &mut Cursor<'_>,
) -> Result<(Vec<SegmentPin>, Vec<ErasurePredicate>), FlightTicketError> {
    let seg_count = cur.read_u32()?;
    // Do not pre-allocate from the untrusted count; push and grow.
    let mut segments = Vec::new();
    for _ in 0..seg_count {
        segments.push(read_segment_pin(cur)?);
    }
    let erasure_count = cur.read_u32()?;
    let mut pending_erasure = Vec::new();
    for _ in 0..erasure_count {
        let matcher_count = cur.read_u32()?;
        let mut matchers = Vec::new();
        for _ in 0..matcher_count {
            let key = cur.read_len_prefixed()?;
            let key = std::str::from_utf8(key).map_err(|_| FlightTicketError::InvalidUtf8)?;
            let value = cur.read_len_prefixed()?;
            let value = std::str::from_utf8(value).map_err(|_| FlightTicketError::InvalidUtf8)?;
            matchers.push((key.to_owned(), value.to_owned()));
        }
        let window_start_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        let window_end_ns = i64::from_le_bytes(cur.read_array::<8>()?);
        pending_erasure.push(ErasurePredicate::new(
            matchers,
            window_start_ns,
            window_end_ns,
        ));
    }
    Ok((segments, pending_erasure))
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

/// Wire tag for a [`DeclaredType`] (ADR-0090). A single byte, its own small
/// enumeration distinct from `ravel_logseg`'s `FieldType` byte so a change to
/// either surfaces as a compile error here rather than a silently different
/// pin. `f64`/`List`/`Map` have no declared type, so only these four exist.
fn declared_type_tag(ty: DeclaredType) -> u8 {
    match ty {
        DeclaredType::Str => 1,
        DeclaredType::I64 => 2,
        DeclaredType::Bool => 3,
        DeclaredType::Bytes => 4,
    }
}

fn declared_type_from_tag(tag: u8) -> Result<DeclaredType, FlightTicketError> {
    match tag {
        1 => Ok(DeclaredType::Str),
        2 => Ok(DeclaredType::I64),
        3 => Ok(DeclaredType::Bool),
        4 => Ok(DeclaredType::Bytes),
        other => Err(FlightTicketError::InvalidDeclaredType(other)),
    }
}

const LIMIT_ABSENT: u8 = 0;
const LIMIT_UNLIMITED: u8 = 1;
const LIMIT_BOUNDED: u8 = 2;

/// A request budget's one limit: absent, unlimited, or bounded to a value.
type WireLimit = Option<Option<u64>>;

fn write_limit(buf: &mut Vec<u8>, limit: WireLimit) {
    match limit {
        None => buf.push(LIMIT_ABSENT),
        Some(None) => buf.push(LIMIT_UNLIMITED),
        Some(Some(value)) => {
            buf.push(LIMIT_BOUNDED);
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }
}

/// `unlimited_allowed` is false for `max_segments`, which has no unlimited
/// spelling.
fn read_limit(
    cur: &mut Cursor<'_>,
    unlimited_allowed: bool,
) -> Result<WireLimit, FlightTicketError> {
    match cur.read_u8()? {
        LIMIT_ABSENT => Ok(None),
        LIMIT_UNLIMITED if unlimited_allowed => Ok(Some(None)),
        LIMIT_BOUNDED => Ok(Some(Some(u64::from_le_bytes(cur.read_array::<8>()?)))),
        other => Err(FlightTicketError::InvalidBudgetTag(other)),
    }
}

/// The request budgets: a presence byte, then the three limits in a fixed
/// order. The bytes land in the payload before the MAC is computed, so a
/// client cannot drop or raise a limit.
fn write_budgets(buf: &mut Vec<u8>, budgets: Option<&RequestBudgets>) {
    let Some(budgets) = budgets else {
        buf.push(0);
        return;
    };
    buf.push(1);
    write_limit(
        buf,
        budgets.max_bytes_scanned.map(|limit| match limit {
            ByteLimit::Bounded(value) => Some(value),
            ByteLimit::Unlimited => None,
        }),
    );
    write_limit(
        buf,
        budgets.max_store_requests.map(|limit| match limit {
            RequestLimit::Bounded(value) => Some(value),
            RequestLimit::Unlimited => None,
        }),
    );
    write_limit(
        buf,
        budgets
            .max_segments
            .map(|segments| Some(u64::try_from(segments).unwrap_or(u64::MAX))),
    );
}

fn read_budgets(cur: &mut Cursor<'_>) -> Result<Option<RequestBudgets>, FlightTicketError> {
    match cur.read_u8()? {
        0 => Ok(None),
        1 => {
            let max_bytes_scanned = read_limit(cur, true)?.map(|limit| match limit {
                Some(value) => ByteLimit::Bounded(value),
                None => ByteLimit::Unlimited,
            });
            let max_store_requests = read_limit(cur, true)?.map(|limit| match limit {
                Some(value) => RequestLimit::Bounded(value),
                None => RequestLimit::Unlimited,
            });
            // A limit past `usize` only ever lowers a ceiling by less than it
            // could, so saturating is safe.
            let max_segments = read_limit(cur, false)?
                .flatten()
                .map(|value| usize::try_from(value).unwrap_or(usize::MAX));
            Ok(Some(RequestBudgets {
                max_bytes_scanned,
                max_store_requests,
                max_segments,
            }))
        }
        other => Err(FlightTicketError::InvalidBudgetTag(other)),
    }
}

const PUSHDOWN_SPANS: u8 = 1;
const PUSHDOWN_LOGS: u8 = 2;
const PRUNE_EQUALS: u8 = 1;
const PRUNE_NUM_RANGE: u8 = 2;

/// The slice pushdown block. The bytes land in the payload before the MAC is
/// computed, so a client cannot narrow or widen a worker's read.
fn write_pushdown(buf: &mut Vec<u8>, pushdown: &SlicePushdown) -> Result<(), FlightTicketError> {
    match &pushdown.0 {
        SlicePushdownKind::Spans(p) => {
            buf.push(PUSHDOWN_SPANS);
            write_opt_u64(buf, p.ts_lo.map(|v| v as u64));
            write_opt_u64(buf, p.ts_hi.map(|v| v as u64));
            match p.trace_id {
                Some(tid) => {
                    buf.push(1);
                    buf.extend_from_slice(&tid);
                }
                None => buf.push(0),
            }
        }
        SlicePushdownKind::Logs(p) => {
            buf.push(PUSHDOWN_LOGS);
            write_opt_u64(buf, p.ts_lo.map(|v| v as u64));
            write_opt_u64(buf, p.ts_hi.map(|v| v as u64));
            write_u32(buf, u32_len(p.prune.len())?);
            for predicate in &p.prune {
                match predicate {
                    Predicate::Equals {
                        field: FieldSel::Attr(key),
                        value,
                    } => {
                        let (ty, bytes) = match value {
                            AttrValue::Str(s) => (FieldType::Str, s.as_bytes()),
                            AttrValue::Bytes(b) => (FieldType::Bytes, b.as_slice()),
                            _ => return Err(FlightTicketError::UncarriedPushdown),
                        };
                        buf.push(PRUNE_EQUALS);
                        write_len_prefixed(buf, key.as_bytes())?;
                        buf.push(ty.to_u8());
                        write_len_prefixed(buf, bytes)?;
                    }
                    Predicate::NumRange {
                        field: FieldSel::Attr(key),
                        ty: ty @ (FieldType::I64 | FieldType::F64 | FieldType::Bool),
                        min,
                        max,
                    } => {
                        buf.push(PRUNE_NUM_RANGE);
                        write_len_prefixed(buf, key.as_bytes())?;
                        buf.push(ty.to_u8());
                        write_opt_u64(buf, *min);
                        write_opt_u64(buf, *max);
                    }
                    _ => return Err(FlightTicketError::UncarriedPushdown),
                }
            }
            if !p.key_equals.is_empty() {
                write_u32(buf, u32_len(p.key_equals.len())?);
                for k in &p.key_equals {
                    write_len_prefixed(buf, k.field.as_bytes())?;
                    buf.push(k.key_type.as_u8());
                    buf.extend_from_slice(&k.key8);
                }
            }
        }
    }
    Ok(())
}

fn read_pushdown(cur: &mut Cursor<'_>, version: u8) -> Result<SlicePushdown, FlightTicketError> {
    match cur.read_u8()? {
        // Version 11 exists only for a logs block's key section; the encoder
        // never writes a spans block under it.
        PUSHDOWN_SPANS if version == VERSION_KEY_EQUALS => {
            Err(FlightTicketError::InvalidPushdownTag(PUSHDOWN_SPANS))
        }
        PUSHDOWN_SPANS => {
            let ts_lo = read_opt_u64(cur)?.map(|v| v as i64);
            let ts_hi = read_opt_u64(cur)?.map(|v| v as i64);
            let trace_id = match cur.read_u8()? {
                0 => None,
                1 => Some(cur.read_array::<16>()?),
                other => return Err(FlightTicketError::InvalidPushdownTag(other)),
            };
            Ok(SlicePushdown(SlicePushdownKind::Spans(SpansPushdown {
                ts_lo,
                ts_hi,
                trace_id,
                ..SpansPushdown::default()
            })))
        }
        PUSHDOWN_LOGS => {
            let ts_lo = read_opt_u64(cur)?.map(|v| v as i64);
            let ts_hi = read_opt_u64(cur)?.map(|v| v as i64);
            let count = cur.read_u32()?;
            // Do not pre-allocate from the untrusted count; push and grow.
            let mut prune = Vec::new();
            for _ in 0..count {
                let tag = cur.read_u8()?;
                let key = cur.read_len_prefixed()?;
                let key = std::str::from_utf8(key)
                    .map_err(|_| FlightTicketError::InvalidUtf8)?
                    .to_owned();
                let ty_byte = cur.read_u8()?;
                let ty = FieldType::from_u8(ty_byte)
                    .ok_or(FlightTicketError::InvalidPushdownTag(ty_byte))?;
                let predicate = match (tag, ty) {
                    (PRUNE_EQUALS, FieldType::Str) => {
                        let value = cur.read_len_prefixed()?;
                        let value = std::str::from_utf8(value)
                            .map_err(|_| FlightTicketError::InvalidUtf8)?;
                        Predicate::Equals {
                            field: FieldSel::Attr(key),
                            value: AttrValue::Str(value.to_owned()),
                        }
                    }
                    (PRUNE_EQUALS, FieldType::Bytes) => Predicate::Equals {
                        field: FieldSel::Attr(key),
                        value: AttrValue::Bytes(cur.read_len_prefixed()?.to_vec()),
                    },
                    (PRUNE_NUM_RANGE, FieldType::I64 | FieldType::F64 | FieldType::Bool) => {
                        Predicate::NumRange {
                            field: FieldSel::Attr(key),
                            ty,
                            min: read_opt_u64(cur)?,
                            max: read_opt_u64(cur)?,
                        }
                    }
                    (PRUNE_EQUALS | PRUNE_NUM_RANGE, _) => {
                        return Err(FlightTicketError::InvalidPushdownTag(ty_byte));
                    }
                    (other, _) => return Err(FlightTicketError::InvalidPushdownTag(other)),
                };
                prune.push(predicate);
            }
            let mut key_equals = Vec::new();
            if version == VERSION_KEY_EQUALS {
                let count = cur.read_u32()?;
                if count == 0 {
                    return Err(FlightTicketError::InvalidPushdownTag(0));
                }
                for _ in 0..count {
                    let field = cur.read_len_prefixed()?;
                    let field = std::str::from_utf8(field)
                        .map_err(|_| FlightTicketError::InvalidUtf8)?
                        .to_owned();
                    let ty_byte = cur.read_u8()?;
                    let key_type = KeyType::from_u8(ty_byte)
                        .map_err(|_| FlightTicketError::InvalidPushdownTag(ty_byte))?;
                    key_equals.push(KeyEquals::new(field, key_type, cur.read_array::<8>()?));
                }
            }
            Ok(SlicePushdown(SlicePushdownKind::Logs(LogsPushdown {
                ts_lo,
                ts_hi,
                content: Vec::new(),
                prune,
                key_equals,
            })))
        }
        other => Err(FlightTicketError::InvalidPushdownTag(other)),
    }
}

/// An optional 64-bit value: a flag byte (0 absent, 1 present), then the
/// value when present. An `i64` travels as its two's-complement `u64`.
fn write_opt_u64(buf: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        None => buf.push(0),
    }
}

fn read_opt_u64(cur: &mut Cursor<'_>) -> Result<Option<u64>, FlightTicketError> {
    match cur.read_u8()? {
        0 => Ok(None),
        1 => Ok(Some(u64::from_le_bytes(cur.read_array::<8>()?))),
        other => Err(FlightTicketError::InvalidPushdownTag(other)),
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
    use ravel_logseg::key_index::{TRACE_ID_FIELD, key8_bytes, key8_i64, key8_id16, key8_str};
    use uuid::Uuid;

    /// A fixed key for tests that don't care about key material itself, only
    /// that encode/decode agree on one.
    fn test_key() -> TicketKey {
        [0x42u8; TICKET_KEY_LEN]
    }

    /// The derived key is a deterministic function of the shared secret, so two
    /// processes deriving from the same cluster secret key their MAC identically
    /// (the property ADR-0071 cross-process slice tickets require); a different
    /// secret yields a different key; and the key is never the raw secret bytes
    /// (BLAKE3 `derive_key` is a real KDF, not a copy).
    #[test]
    fn derive_ticket_key_is_deterministic_and_domain_separated() {
        let a = derive_ticket_key(b"cluster-secret");
        let b = derive_ticket_key(b"cluster-secret");
        assert_eq!(
            a, b,
            "same secret must derive the same key across processes"
        );

        let other = derive_ticket_key(b"cluster-secret-2");
        assert_ne!(a, other, "a different secret must derive a different key");

        assert_ne!(
            &a[..],
            b"cluster-secret".as_slice(),
            "the key must be derived, not the raw secret"
        );

        // A ticket signed with a derived key verifies with a key derived from the
        // same secret and is rejected by one derived from a different secret.
        let ticket = FlightTicket {
            tenant: TenantHash([7u8; 16]),
            statement: "SELECT 1".to_string(),
            segments: Vec::new(),
            min_commit_tokens: Vec::new(),
            now_ns: 1,
            deadline_ns: 2,
            slice_index: 0,
            slice_count: 1,
            pending_erasure: Vec::new(),
            declared_columns: Vec::new(),
            parquet_tables: Vec::new(),
            budgets: None,
            signal_groups: Vec::new(),
        };
        let encoded = ticket.encode(&a).expect("encode");
        assert!(FlightTicket::decode(&encoded, &b).is_ok());
        assert!(FlightTicket::decode(&encoded, &other).is_err());
    }

    /// ADR-1689 decision 2: a ticket minted for one surface fails the MAC on
    /// the other, under the same file key, in both directions.
    #[test]
    fn a_ticket_minted_for_one_surface_fails_the_mac_on_the_other() {
        let keys = SqlTicketKeys::from_file_keys([b"file-key".as_slice()]).expect("one key");
        assert_ne!(
            keys.mint_key(TicketSurface::Client),
            keys.mint_key(TicketSurface::Slice),
            "the two surfaces derive distinct keys from one file key"
        );
        let ticket = sample_ticket();
        for (mint, other) in [
            (TicketSurface::Client, TicketSurface::Slice),
            (TicketSurface::Slice, TicketSurface::Client),
        ] {
            let bytes = keys.encode(&ticket, mint).expect("encode");
            assert_eq!(keys.decode(&bytes, mint), Ok(ticket.clone()));
            assert_eq!(
                keys.decode(&bytes, other),
                Err(FlightTicketError::MacMismatch),
                "a {mint:?} ticket must not verify as {other:?}"
            );
        }
    }

    /// The first file key mints; every configured key verifies, so a ticket
    /// minted before a rotation still redeems after it. A key that was never
    /// configured does not verify.
    #[test]
    fn the_first_file_key_mints_and_every_file_key_verifies() {
        let old = SqlTicketKeys::from_file_keys([b"old".as_slice()]).expect("one key");
        let rotated =
            SqlTicketKeys::from_file_keys([b"new".as_slice(), b"old".as_slice()]).expect("keys");
        let stranger = SqlTicketKeys::from_file_keys([b"other".as_slice()]).expect("one key");
        let ticket = sample_ticket();
        for surface in [TicketSurface::Client, TicketSurface::Slice] {
            assert_eq!(
                rotated.mint_key(surface),
                &derive_surface_key(b"new", surface),
                "the first file key mints"
            );
            let minted_before = old.encode(&ticket, surface).expect("encode");
            assert_eq!(rotated.decode(&minted_before, surface), Ok(ticket.clone()));
            let minted_after = rotated.encode(&ticket, surface).expect("encode");
            assert_eq!(
                old.decode(&minted_after, surface),
                Err(FlightTicketError::MacMismatch)
            );
            assert_eq!(
                stranger.decode(&minted_before, surface),
                Err(FlightTicketError::MacMismatch)
            );
        }
        assert!(SqlTicketKeys::from_file_keys(std::iter::empty::<&[u8]>()).is_none());
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
            segment_format_version: seed as u32 + 12,
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
            pending_erasure: vec![
                ErasurePredicate::windowless(vec![("__name__".to_owned(), "erase_me".to_owned())]),
                ErasurePredicate::new(vec![("region".to_owned(), "us-east".to_owned())], 100, 200),
            ],
            // One declared column of each type, so the round-trip and
            // single-flip tests exercise every DeclaredType wire tag.
            declared_columns: vec![
                DeclaredColumn::new("http.status_code", DeclaredType::I64),
                DeclaredColumn::new("k8s.namespace.name", DeclaredType::Str),
                DeclaredColumn::new("ok", DeclaredType::Bool),
                DeclaredColumn::new("payload", DeclaredType::Bytes),
            ],
            parquet_tables: vec![
                ParquetPin {
                    table: "hits".to_owned(),
                    version: 7,
                },
                ParquetPin {
                    table: "_orders_2".to_owned(),
                    version: u64::MAX,
                },
            ],
            // Every limit kind, so the round-trip and single-flip tests
            // exercise each budget wire tag.
            budgets: Some(RequestBudgets {
                max_bytes_scanned: Some(ByteLimit::Bounded(1 << 30)),
                max_store_requests: Some(RequestLimit::Unlimited),
                max_segments: Some(12),
            }),
            signal_groups: Vec::new(),
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
                segment_format_version: 4,
                declared_column_stats: Default::default(),
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
            assert_eq!(
                decoded.snapshot().expect("a one-snapshot ticket").segments,
                vec![seg]
            );
        }
    }

    /// A pin with every field set to a distinct non-default value, for
    /// [`segment_pin_wire_bytes_are_pinned_inside_a_flight_ticket`].
    fn golden_pin() -> SegmentPin {
        SegmentPin {
            data_object_key: "t/aa/metrics/l1/0002/w.7.8.abcdef.rseg".to_owned(),
            object_size: 123_456,
            min_event_ts_ns: 1_700_000_000_000_000_001,
            max_event_ts_ns: 1_700_000_000_100_000_002,
            ingest_hour_bucket: 472_183,
            sample_count: 9_001,
            series_count: 42,
            shard: 7,
            content_hash: [0xabu8; 32],
            writer_id: Uuid::from_u128(0x0123_4567_89ab_cdef_0011_2233_4455_6677),
            writer_epoch: 3,
            writer_seq: 99,
            created_unix_ns: 1_700_000_000_200_000_003,
            level: SegmentLevel::L1 {
                input_set_hash: [0xcdu8; 32],
                part_index: 5,
            },
            segment_format_version: 4,
        }
    }

    /// The frozen wire form of [`golden_pin`]: the exact bytes
    /// `SegmentPin::encode_into` writes for it, as hex. This literal changes
    /// only with an ADR and a version bump to the pin's wire layout
    /// (ADR-1374 decision 9): a codec change that reorders, widens, or drops
    /// a field must show up here as a diff a reviewer has to approve, not as
    /// a silently different set of bytes an older worker can no longer read.
    const GOLDEN_PIN_HEX: &str = "0300000000000000630000000000000003c21542fe9c971740e2010\
        00000000001002a36fe9c971702e11f3cfe9c971729230000000000002a0000000000000077340700070\
        00000abababababababababababababababababababababababababababababababab0123456789abcde\
        f001122334455667726000000742f61612f6d6574726963732f6c312f303030322f772e372e382e616263\
        6465662e7273656701cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd05000\
        00004000000";

    /// [`SegmentPin::encode_into`] must produce the same bytes
    /// [`FlightTicket::encode`] writes for the identical pin inside a
    /// ticket (both call the same private `write_segment_pin`, but this
    /// checks the public API contract, not the implementation sharing), the
    /// exact wire form is pinned as a golden hex literal so a drift in the
    /// frozen layout is caught here, and [`SegmentPin::decode_from`] must
    /// report a consumed length equal to the encoded pin's own length,
    /// leaving trailing bytes for the caller rather than erroring on them.
    #[test]
    fn segment_pin_wire_bytes_are_pinned_inside_a_flight_ticket() {
        let pin = golden_pin();

        let mut pin_bytes = Vec::new();
        pin.encode_into(&mut pin_bytes).expect("encode_into");
        assert_eq!(hex::encode(&pin_bytes), GOLDEN_PIN_HEX);

        let ticket = FlightTicket {
            segments: vec![pin.clone()],
            ..sample_ticket()
        };
        let encoded = ticket.encode(&test_key()).expect("ticket encode");
        let offset = encoded
            .windows(pin_bytes.len())
            .position(|window| window == pin_bytes.as_slice())
            .expect("the pin's own bytes appear verbatim inside the encoded ticket");
        assert_eq!(
            &encoded[offset..offset + pin_bytes.len()],
            pin_bytes.as_slice()
        );

        let (decoded, consumed) = SegmentPin::decode_from(&pin_bytes).expect("decode_from");
        assert_eq!(decoded, pin);
        assert_eq!(consumed, pin_bytes.len());

        let mut with_trailing_garbage = pin_bytes.clone();
        with_trailing_garbage.extend_from_slice(&[0xffu8; 16]);
        let (decoded_with_garbage, consumed_with_garbage) =
            SegmentPin::decode_from(&with_trailing_garbage).expect("decode_from with trailing");
        assert_eq!(decoded_with_garbage, pin);
        assert_eq!(
            consumed_with_garbage,
            pin_bytes.len(),
            "trailing garbage must not be consumed"
        );
    }

    /// A redeemed ticket reports no pruning of its own. The pin already holds
    /// the post-prune segment set from `GetFlightInfo`, so a nonzero count
    /// here would double-count segments the original resolve already dropped.
    #[test]
    fn rebuilt_snapshot_reports_no_pruning() {
        let ticket = sample_ticket();
        let bytes = ticket.encode(&test_key()).expect("encode");
        let decoded = FlightTicket::decode(&bytes, &test_key()).expect("decode");

        let snapshot = decoded.snapshot().expect("a one-snapshot ticket");
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
        body.push(VERSION_KEY_EQUALS.wrapping_add(1));
        body.extend_from_slice(&[0u8; 16]);
        body.extend_from_slice(&0i64.to_le_bytes());
        body.extend_from_slice(&0i64.to_le_bytes());
        write_u32(&mut body, 0); // slice_index
        write_u32(&mut body, 0); // slice_count
        write_u32(&mut body, 0);
        write_u32(&mut body, 0);
        write_u32(&mut body, 0);
        write_u32(&mut body, 0); // erasure_count
        write_u32(&mut body, 0); // declared_count
        pad_to_min_encoded_len(&mut body); // clears MIN_ENCODED_LEN
        // A valid MAC under the real key: the version check, not the MAC,
        // must be what rejects this.
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert!(matches!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::UnsupportedVersion(_))
        ));
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// A ticket over one signal encodes to exactly the bytes it did before
    /// signal groups existed: version 8 without a pushdown block, version 9
    /// with one. Both literals were generated on the tree before version 10,
    /// so a ticket in flight across an upgrade still verifies and decodes.
    /// A grouped ticket encodes to exactly the version 10 bytes it did before
    /// version 11 existed; that literal was generated on the tree before
    /// version 11.
    #[test]
    fn single_signal_tickets_are_byte_identical_to_before() {
        let key = test_key();
        let ticket = sample_ticket();
        let v8 = ticket.encode(&key).expect("encode v8");
        let v9 = ticket
            .encode_with_pushdown(&key, Some(&sample_logs_pushdown()))
            .expect("encode v9");
        let v10 = grouped_ticket().encode(&key).expect("encode v10");
        assert_eq!(v8[4], 8);
        assert_eq!(v9[4], 9);
        assert_eq!(v10[4], 10);
        assert_eq!(to_hex(&v8), V8_GOLDEN_HEX);
        assert_eq!(to_hex(&v9), V9_GOLDEN_HEX);
        assert_eq!(to_hex(&v10), V10_GOLDEN_HEX);
    }

    const V8_GOLDEN_HEX: &str = "52465431080707070707070707070707070707070700002a36fe9c971700ac4d32059d97170000000001000000020000003f000000646a49364d546f774d4441774d4441774d4330774d4441774c5441774d4441744d4441774d4330774d4441774f57557a4e7a6335596a6b364d6a6f334f6a4540000000646a49364d6a6f774d4441774d4441774d4330774d4441774c5441774d4441744d4441774d4330774d4441784d324d325a57597a4e7a49364d7a6f784e446f7902000000c00b000000000000c10b000000000000c20b000000000000b90b000000000000ba0b000000000000bb0b000000000000bd0b000000000000be0b000000000000070000000a0000000303030303030303030303030303030303030303030303030303030303030303000000000000000000000000369d036b23000000742f61612f6d6574726963732f6c312f303030302f772e312e322e6162632e727365670103030303030303030303030303030303030303030303030303030303030303030e0000000f000000481f000000000000491f0000000000004a1f000000000000411f000000000000421f000000000000431f000000000000451f000000000000461f0000000000000c0000000f000000080808080808080808080808080808080808080808080808080808080808080800000000000000000000000091a2b3c823000000742f61612f6d6574726963732f6c302f303030312f772e342e352e6465662e7273656700140000000200000001000000080000005f5f6e616d655f5f0800000065726173655f6d65000000000000000000000000000000000100000006000000726567696f6e0700000075732d656173746400000000000000c8000000000000000400000010000000687474702e7374617475735f636f646502120000006b38732e6e616d6573706163652e6e616d6501020000006f6b03070000007061796c6f6164040200000004000000686974730700000000000000090000005f6f72646572735f32ffffffffffffffff0102000000400000000001020c000000000000002e00000053454c454354202a2046524f4d2073616d706c6573205748455245207473203e3d203120414e44207473203c2032c98d62930dedceb80ed7eebb08d798b38446f1b2e88bae028f7620a50ae0557c";
    const V9_GOLDEN_HEX: &str = "52465431090707070707070707070707070707070700002a36fe9c971700ac4d32059d97170000000001000000020000003f000000646a49364d546f774d4441774d4441774d4330774d4441774c5441774d4441744d4441774d4330774d4441774f57557a4e7a6335596a6b364d6a6f334f6a4540000000646a49364d6a6f774d4441774d4441774d4330774d4441774c5441774d4441744d4441774d4330774d4441784d324d325a57597a4e7a49364d7a6f784e446f7902000000c00b000000000000c10b000000000000c20b000000000000b90b000000000000ba0b000000000000bb0b000000000000bd0b000000000000be0b000000000000070000000a0000000303030303030303030303030303030303030303030303030303030303030303000000000000000000000000369d036b23000000742f61612f6d6574726963732f6c312f303030302f772e312e322e6162632e727365670103030303030303030303030303030303030303030303030303030303030303030e0000000f000000481f000000000000491f0000000000004a1f000000000000411f000000000000421f000000000000431f000000000000451f000000000000461f0000000000000c0000000f000000080808080808080808080808080808080808080808080808080808080808080800000000000000000000000091a2b3c823000000742f61612f6d6574726963732f6c302f303030312f772e342e352e6465662e7273656700140000000200000001000000080000005f5f6e616d655f5f0800000065726173655f6d65000000000000000000000000000000000100000006000000726567696f6e0700000075732d656173746400000000000000c8000000000000000400000010000000687474702e7374617475735f636f646502120000006b38732e6e616d6573706163652e6e616d6501020000006f6b03070000007061796c6f6164040200000004000000686974730700000000000000090000005f6f72646572735f32ffffffffffffffff0102000000400000000001020c000000000000002e00000053454c454354202a2046524f4d2073616d706c6573205748455245207473203e3d203120414e44207473203c2032020001ffffffffffffff7f050000000107000000736572766963650108000000636865636b6f75740104000000626c6f6205030000000001ff020b0000007374617475735f636f64650201fdffffffffffffff00020700000073616d706c6564040101000000000000000101000000000000000205000000726174696f030001000000000000e03f895fac53726f191762670f3474908a66d704122b1993e82dbb260302de63501b";
    const V10_GOLDEN_HEX: &str = "524654310a0707070707070707070707070707070700002a36fe9c971700ac4d32059d97170000000001000000020000003f000000646a49364d546f774d4441774d4441774d4330774d4441774c5441774d4441744d4441774d4330774d4441774f57557a4e7a6335596a6b364d6a6f334f6a4540000000646a49364d6a6f774d4441774d4441774d4330774d4441774c5441774d4441744d4441774d4330774d4441784d324d325a57597a4e7a49364d7a6f784e446f79020000000102000000c00b000000000000c10b000000000000c20b000000000000b90b000000000000ba0b000000000000bb0b000000000000bd0b000000000000be0b000000000000070000000a0000000303030303030303030303030303030303030303030303030303030303030303000000000000000000000000369d036b23000000742f61612f6d6574726963732f6c312f303030302f772e312e322e6162632e727365670103030303030303030303030303030303030303030303030303030303030303030e0000000f000000481f000000000000491f0000000000004a1f000000000000411f000000000000421f000000000000431f000000000000451f000000000000461f0000000000000c0000000f000000080808080808080808080808080808080808080808080808080808080808080800000000000000000000000091a2b3c823000000742f61612f6d6574726963732f6c302f303030312f772e342e352e6465662e7273656700140000000100000001000000080000005f5f6e616d655f5f0800000065726173655f6d6500000000000000000000000000000000020100000090130000000000009113000000000000921300000000000089130000000000008a130000000000008b130000000000008d130000000000008e13000000000000090000000c00000005050505050505050505050505050505050505050505050505050505050505050000000000000000000000005b05b05d20000000742f61612f6c6f67732f6c312f303030322f772e362e372e6665642e726c6f67010505050505050505050505050505050505050505050505050505050505050505100000001100000001000000010000000c000000736572766963652e6e616d6508000000636865636b6f75746400000000000000c8000000000000000400000010000000687474702e7374617475735f636f646502120000006b38732e6e616d6573706163652e6e616d6501020000006f6b03070000007061796c6f6164040200000004000000686974730700000000000000090000005f6f72646572735f32ffffffffffffffff0102000000400000000001020c000000000000005200000053454c45435420732e74732046524f4d2073616d706c65732073204a4f494e206c6f6773206c204f4e20732e7473203d206c2e747320574845524520732e7473203e3d203120414e4420732e7473203c2032af2c7423fb4aaa9bd16a527cf71f4f6e960a4776650b3858340d7e86415845a4";

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
        // A v5-sized body (>= MIN_ENCODED_LEN) so the length guard passes and
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
        write_u32(&mut body, 0); // erasure_count
        write_u32(&mut body, 0); // declared_count
        write_u32(&mut body, 0); // stmt_len
        pad_to_min_encoded_len(&mut body);
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert_eq!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::UnsupportedVersion(3))
        );
    }

    /// v4 (the predecessor of this ticket's `pending_erasure` field, ADR-0064
    /// decision 3) is rejected the same way v3 is: a v4-shaped
    /// envelope carrying a valid MAC under this process's key still fails on
    /// the version byte, never reinterpreted under the v5 layout -- which
    /// would otherwise silently read v4's `stmt_len` as `erasure_count` and
    /// desync every field after it instead of refusing outright.
    #[test]
    fn a_v4_envelope_is_rejected_as_unsupported_version() {
        let key = test_key();
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.push(4); // the predecessor version, before pending_erasure
        body.extend_from_slice(&[0u8; 16]);
        body.extend_from_slice(&0i64.to_le_bytes());
        body.extend_from_slice(&0i64.to_le_bytes());
        write_u32(&mut body, 0); // slice_index
        write_u32(&mut body, 0); // slice_count
        write_u32(&mut body, 0); // tokens
        write_u32(&mut body, 0); // segments
        write_u32(&mut body, 0); // erasure_count
        write_u32(&mut body, 0); // declared_count, padding to v6's MIN_ENCODED_LEN
        write_u32(&mut body, 0); // stmt_len
        pad_to_min_encoded_len(&mut body);
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert_eq!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::UnsupportedVersion(4))
        );
    }

    /// v5 (the predecessor of this ticket's `declared_columns` field, ADR-0090)
    /// is rejected the same way v3 and v4 are: a v5-shaped envelope carrying a
    /// valid MAC under this process's key still fails on the version byte, never
    /// reinterpreted under the v6 layout -- which would otherwise silently read
    /// v5's `stmt_len` as `declared_count` and desync every field after it
    /// instead of refusing outright.
    #[test]
    fn a_v5_envelope_is_rejected_as_unsupported_version() {
        let key = test_key();
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.push(5); // the predecessor version, before declared_columns
        body.extend_from_slice(&[0u8; 16]);
        body.extend_from_slice(&0i64.to_le_bytes());
        body.extend_from_slice(&0i64.to_le_bytes());
        write_u32(&mut body, 0); // slice_index
        write_u32(&mut body, 0); // slice_count
        write_u32(&mut body, 0); // tokens
        write_u32(&mut body, 0); // segments
        write_u32(&mut body, 0); // erasure_count
        write_u32(&mut body, 0); // declared_count, padding to v6's MIN_ENCODED_LEN
        write_u32(&mut body, 0); // stmt_len
        pad_to_min_encoded_len(&mut body);
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert_eq!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::UnsupportedVersion(5))
        );
    }

    /// Pad a hand-built predecessor-version body to the current smallest
    /// ticket, so the length guard passes and the version check is what
    /// refuses it. The padding's content is irrelevant: the version byte is
    /// read before anything after the header.
    fn pad_to_min_encoded_len(body: &mut Vec<u8>) {
        let want = MIN_ENCODED_LEN - MAC_LEN;
        if body.len() < want {
            body.resize(want, 0);
        }
    }

    /// The v7 layout, which has no `parquet_tables` or `budgets` (issue
    /// #2240): a v7 ticket carrying a valid MAC under this process's key is
    /// refused on its version byte. Read under the v8 layout, its `stmt_len`
    /// would be taken as `parquet_count` and every later field would desync.
    /// The body is a real v7 ticket: the current ticket's bytes up to the end
    /// of the declared columns, then the statement, with the version byte
    /// changed.
    #[test]
    fn a_v7_envelope_is_rejected_as_unsupported_version() {
        let key = test_key();
        let ticket = FlightTicket {
            parquet_tables: vec![],
            budgets: None,
            ..sample_ticket()
        };
        let v8 = ticket.encode(&key).expect("encode");
        let payload = &v8[..v8.len() - MAC_LEN];
        // v8 adds `parquet_count` (4) and `budgets_flag` (1) between the
        // declared columns and the statement: lift them out to get v7's body.
        let stmt_at = payload.len() - 4 - ticket.statement.len();
        let added = 4 + 1;
        let mut body = payload[..stmt_at - added].to_vec();
        body.extend_from_slice(&payload[stmt_at..]);
        body[MAGIC.len()] = 7;
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert_eq!(
            FlightTicket::decode(&body, &key),
            Err(FlightTicketError::UnsupportedVersion(7))
        );
        // Non-vacuity: the same ticket as v8 decodes.
        assert_eq!(FlightTicket::decode(&v8, &key), Ok(ticket));
    }

    /// A signed ticket whose `parquet_count` section and budgets section are
    /// exactly `parquet` and `budgets`, with everything before them empty and
    /// an empty statement after them: the way to put a malformed pin list or
    /// budget under a valid MAC, which `encode` itself refuses to write.
    fn spliced(parquet: &[u8], budgets: &[u8]) -> Vec<u8> {
        let key = test_key();
        let minimal = FlightTicket {
            tenant: TenantHash([0u8; 16]),
            statement: String::new(),
            segments: vec![],
            min_commit_tokens: vec![],
            now_ns: 0,
            deadline_ns: 0,
            slice_index: 0,
            slice_count: 1,
            pending_erasure: vec![],
            declared_columns: vec![],
            parquet_tables: vec![],
            budgets: None,
            signal_groups: Vec::new(),
        }
        .encode(&key)
        .expect("encode");
        // parquet_count (4) + budgets_flag (1) + stmt_len (4) close the body.
        let section_at = minimal.len() - MAC_LEN - 4 - 1 - 4;
        let mut body = minimal[..section_at].to_vec();
        body.extend_from_slice(parquet);
        body.extend_from_slice(budgets);
        write_u32(&mut body, 0); // stmt_len
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        body
    }

    fn pin_section(pins: &[(&[u8], u64)]) -> Vec<u8> {
        let mut section = Vec::new();
        write_u32(&mut section, pins.len() as u32);
        for (name, version) in pins {
            write_u32(&mut section, name.len() as u32);
            section.extend_from_slice(name);
            section.extend_from_slice(&version.to_le_bytes());
        }
        section
    }

    /// The pinned Parquet tables round-trip with their versions, and they sit
    /// inside the signed payload: flipping any bit of a pin's table name or
    /// version is a MAC mismatch, and a ticket signed under another key does
    /// not verify.
    #[test]
    fn parquet_pins_round_trip_and_are_covered_by_the_mac() {
        let key = test_key();
        let ticket = sample_ticket();
        assert_eq!(ticket.parquet_tables.len(), 2);
        let bytes = ticket.encode(&key).expect("encode");
        let decoded = FlightTicket::decode(&bytes, &key).expect("decode");
        assert_eq!(decoded.parquet_tables, ticket.parquet_tables);
        assert_eq!(decoded.parquet_tables[1].version, u64::MAX);

        let mut wire = Vec::new();
        for pin in &ticket.parquet_tables {
            write_len_prefixed(&mut wire, pin.table.as_bytes()).expect("len");
            wire.extend_from_slice(&pin.version.to_le_bytes());
        }
        let at = bytes
            .windows(wire.len())
            .position(|window| window == wire.as_slice())
            .expect("the pins' bytes appear verbatim in the ticket");
        for offset in 0..wire.len() {
            for bit in 0..8 {
                let mut flipped = bytes.clone();
                flipped[at + offset] ^= 1 << bit;
                assert_eq!(
                    FlightTicket::decode(&flipped, &key),
                    Err(FlightTicketError::MacMismatch),
                    "byte {offset} bit {bit} of the pin list"
                );
            }
        }
        assert_eq!(
            FlightTicket::decode(&bytes, &[0x43u8; TICKET_KEY_LEN]),
            Err(FlightTicketError::MacMismatch)
        );
    }

    /// More pins than a statement may name is refused at encode, and at
    /// decode before any entry is read, with the codec's typed error. The
    /// exact cap encodes.
    #[test]
    fn an_oversize_pin_list_is_refused_both_ways() {
        let key = test_key();
        let pins = |count: u64| -> Vec<ParquetPin> {
            (0..count)
                .map(|i| ParquetPin {
                    table: format!("t{i}"),
                    version: i + 1,
                })
                .collect()
        };
        let at_cap = FlightTicket {
            parquet_tables: pins(MAX_STATEMENT_TABLE_NAMES as u64),
            ..sample_ticket()
        };
        let bytes = at_cap.encode(&key).expect("the cap encodes");
        assert_eq!(FlightTicket::decode(&bytes, &key), Ok(at_cap));

        let over = FlightTicket {
            parquet_tables: pins(MAX_STATEMENT_TABLE_NAMES as u64 + 1),
            ..sample_ticket()
        };
        assert_eq!(
            over.encode(&key),
            Err(FlightTicketError::TooManyParquetTables {
                count: MAX_STATEMENT_TABLE_NAMES + 1,
                max: MAX_STATEMENT_TABLE_NAMES,
            })
        );

        // Under a valid MAC, with no entries behind the count: the count alone
        // is refused, and a count of u32::MAX drives no allocation.
        for count in [MAX_STATEMENT_TABLE_NAMES as u32 + 1, u32::MAX] {
            let mut section = Vec::new();
            write_u32(&mut section, count);
            assert_eq!(
                FlightTicket::decode(&spliced(&section, &[0]), &key),
                Err(FlightTicketError::TooManyParquetTables {
                    count: count as usize,
                    max: MAX_STATEMENT_TABLE_NAMES,
                })
            );
        }
    }

    /// A pin whose name is not a table name, or whose bytes are cut short or
    /// not UTF-8, is a typed error at decode, never a panic or a pin.
    #[test]
    fn a_malformed_pin_is_a_typed_error() {
        let key = test_key();
        let long = "a".repeat(64);
        for name in [
            b"".as_slice(),
            b"Hits",
            b"has-dash",
            b"1starts_with_digit",
            b"logs",
            long.as_bytes(),
        ] {
            assert_eq!(
                FlightTicket::decode(&spliced(&pin_section(&[(name, 1)]), &[0]), &key),
                Err(FlightTicketError::InvalidParquetTable),
                "{:?}",
                String::from_utf8_lossy(name)
            );
        }
        assert_eq!(
            FlightTicket::decode(
                &spliced(&pin_section(&[([0xffu8, 0xfe].as_slice(), 1)]), &[0]),
                &key
            ),
            Err(FlightTicketError::InvalidUtf8)
        );
        // A name length that runs past the payload.
        let mut section = Vec::new();
        write_u32(&mut section, 1);
        write_u32(&mut section, u32::MAX);
        assert_eq!(
            FlightTicket::decode(&spliced(&section, &[0]), &key),
            Err(FlightTicketError::Truncated)
        );
        // A valid name with its version cut off.
        let mut section = pin_section(&[(b"hits".as_slice(), 1)]);
        section.truncate(section.len() - 3);
        assert_eq!(
            FlightTicket::decode(&spliced(&section, &[0]), &key),
            Err(FlightTicketError::Truncated)
        );
        // Encode refuses the same names.
        let bad = FlightTicket {
            parquet_tables: vec![ParquetPin {
                table: "Hits".to_owned(),
                version: 1,
            }],
            ..sample_ticket()
        };
        assert_eq!(
            bad.encode(&key),
            Err(FlightTicketError::InvalidParquetTable)
        );
        // Non-vacuity: a well-formed pin in the same splice decodes.
        let ok = FlightTicket::decode(
            &spliced(&pin_section(&[(b"hits".as_slice(), 9)]), &[0]),
            &key,
        )
        .expect("a valid pin");
        assert_eq!(
            ok.parquet_tables,
            vec![ParquetPin {
                table: "hits".to_owned(),
                version: 9,
            }]
        );
    }

    /// A pin of manifest version 0 is a typed error at decode and at encode:
    /// manifest versions start at 1, so no resolve can have produced it.
    #[test]
    fn a_pin_of_manifest_version_zero_is_refused() {
        let key = test_key();
        assert_eq!(
            FlightTicket::decode(
                &spliced(&pin_section(&[(b"hits".as_slice(), 0)]), &[0]),
                &key
            ),
            Err(FlightTicketError::InvalidParquetVersion)
        );
        let bad = FlightTicket {
            parquet_tables: vec![ParquetPin {
                table: "hits".to_owned(),
                version: 0,
            }],
            ..sample_ticket()
        };
        assert_eq!(
            bad.encode(&key),
            Err(FlightTicketError::InvalidParquetVersion)
        );
        // Non-vacuity: version 1 in the same splice decodes.
        let ok = FlightTicket::decode(
            &spliced(&pin_section(&[(b"hits".as_slice(), 1)]), &[0]),
            &key,
        )
        .expect("a valid pin");
        assert_eq!(ok.parquet_tables[0].version, 1);
    }

    /// Budgets round-trip in every shape, and a flag or limit tag the layout
    /// does not define is a typed error.
    #[test]
    fn budgets_round_trip_and_bad_tags_are_typed_errors() {
        let key = test_key();
        for budgets in [
            None,
            Some(RequestBudgets::default()),
            Some(RequestBudgets {
                max_bytes_scanned: Some(ByteLimit::Unlimited),
                max_store_requests: Some(RequestLimit::Bounded(0)),
                max_segments: Some(0),
            }),
            Some(RequestBudgets {
                max_bytes_scanned: Some(ByteLimit::Bounded(u64::MAX)),
                max_store_requests: None,
                max_segments: Some(usize::MAX),
            }),
        ] {
            let ticket = FlightTicket {
                budgets,
                ..sample_ticket()
            };
            let bytes = ticket.encode(&key).expect("encode");
            assert_eq!(FlightTicket::decode(&bytes, &key), Ok(ticket));
        }

        let none = pin_section(&[]);
        assert_eq!(
            FlightTicket::decode(&spliced(&none, &[2]), &key),
            Err(FlightTicketError::InvalidBudgetTag(2)),
            "a flag that is neither 0 nor 1"
        );
        assert_eq!(
            FlightTicket::decode(&spliced(&none, &[1, 3, 0, 0]), &key),
            Err(FlightTicketError::InvalidBudgetTag(3)),
            "a limit tag past bounded"
        );
        assert_eq!(
            FlightTicket::decode(&spliced(&none, &[1, 0, 0, 1]), &key),
            Err(FlightTicketError::InvalidBudgetTag(1)),
            "max_segments has no unlimited spelling"
        );
        assert_eq!(
            FlightTicket::decode(&spliced(&none, &[1, 0, 0]), &key),
            Err(FlightTicketError::Truncated),
            "the third limit is missing, so the read runs into stmt_len and \
             then past the payload"
        );
        // Non-vacuity: every limit absent decodes to the empty budgets.
        assert_eq!(
            FlightTicket::decode(&spliced(&none, &[1, 0, 0, 0]), &key)
                .expect("decode")
                .budgets,
            Some(RequestBudgets::default())
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
            decoded.snapshot().expect("a one-snapshot ticket").segments,
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
            pending_erasure: vec![],
            declared_columns: vec![],
            parquet_tables: vec![],
            budgets: None,
            signal_groups: Vec::new(),
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
            pending_erasure: (0..8)
                .map(|i| {
                    ErasurePredicate::new(
                        vec![(format!("k{i}"), format!("v{i}"))],
                        i as i64,
                        i as i64 + 1,
                    )
                })
                .collect(),
            declared_columns: (0..8)
                .map(|i| {
                    let ty = match i % 4 {
                        0 => DeclaredType::Str,
                        1 => DeclaredType::I64,
                        2 => DeclaredType::Bool,
                        _ => DeclaredType::Bytes,
                    };
                    DeclaredColumn::new(format!("declared.{i}"), ty)
                })
                .collect(),
            parquet_tables: (0..MAX_STATEMENT_TABLE_NAMES as u64)
                .map(|i| ParquetPin {
                    table: format!("table_{i}"),
                    version: i + 1,
                })
                .collect(),
            budgets: None,
            signal_groups: Vec::new(),
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
        write_u32(&mut body, 0); // erasure_count
        write_u32(&mut body, 0); // declared_count
        write_u32(&mut body, 0); // parquet_count
        body.push(0); // budgets_flag
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

    /// The vulnerability the keyed MAC closes: tampering with a field (here, the
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
            pending_erasure: vec![],
            declared_columns: vec![],
            parquet_tables: vec![],
            budgets: None,
            signal_groups: Vec::new(),
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
            (any::<u32>(), any::<u32>(), any::<u32>()),
            segment_level_strategy(),
        )
            .prop_map(
                |(
                    data_object_key,
                    content_hash,
                    writer_id,
                    (writer_epoch, writer_seq, created_unix_ns, object_size),
                    (min_event_ts_ns, max_event_ts_ns, sample_count, series_count),
                    (ingest_hour_bucket, shard, segment_format_version),
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
                    segment_format_version,
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

    fn erasure_predicate_strategy() -> impl Strategy<Value = ErasurePredicate> {
        (
            prop::collection::vec((".{0,16}", ".{0,16}"), 1..4),
            any::<i64>(),
            any::<i64>(),
        )
            .prop_map(|(matchers, window_start_ns, window_end_ns)| {
                ErasurePredicate::new(matchers, window_start_ns, window_end_ns)
            })
    }

    fn declared_type_strategy() -> impl Strategy<Value = DeclaredType> {
        prop_oneof![
            Just(DeclaredType::Str),
            Just(DeclaredType::I64),
            Just(DeclaredType::Bool),
            Just(DeclaredType::Bytes),
        ]
    }

    fn declared_column_strategy() -> impl Strategy<Value = DeclaredColumn> {
        (".{0,32}", declared_type_strategy()).prop_map(|(key, ty)| DeclaredColumn::new(key, ty))
    }

    fn parquet_pin_strategy() -> impl Strategy<Value = ParquetPin> {
        ("[a-z_][a-z0-9_]{0,62}", 1..=u64::MAX)
            .prop_filter("a reserved name is no table name", |(table, _)| {
                validate_table(table).is_ok()
            })
            .prop_map(|(table, version)| ParquetPin { table, version })
    }

    fn limit_strategy() -> impl Strategy<Value = Option<Option<u64>>> {
        prop_oneof![
            Just(None),
            Just(Some(None)),
            any::<u64>().prop_map(|value| Some(Some(value))),
        ]
    }

    fn budgets_strategy() -> impl Strategy<Value = Option<RequestBudgets>> {
        prop::option::of(
            (
                limit_strategy(),
                limit_strategy(),
                prop::option::of(any::<u32>()),
            )
                .prop_map(|(bytes, requests, segments)| RequestBudgets {
                    max_bytes_scanned: bytes.map(|limit| match limit {
                        Some(value) => ByteLimit::Bounded(value),
                        None => ByteLimit::Unlimited,
                    }),
                    max_store_requests: requests.map(|limit| match limit {
                        Some(value) => RequestLimit::Bounded(value),
                        None => RequestLimit::Unlimited,
                    }),
                    max_segments: segments.map(|value| value as usize),
                }),
        )
    }

    fn sample_spans_pushdown() -> SlicePushdown {
        SlicePushdown::spans(&SpansPushdown {
            ts_lo: Some(-5),
            ts_hi: Some(1_700_000_000_000_000_000),
            trace_id: Some([0xabu8; 16]),
            ..SpansPushdown::default()
        })
    }

    fn sample_logs_pushdown() -> SlicePushdown {
        SlicePushdown::logs(&LogsPushdown {
            ts_lo: None,
            ts_hi: Some(i64::MAX),
            content: Vec::new(),
            prune: vec![
                Predicate::Equals {
                    field: FieldSel::Attr("service".to_owned()),
                    value: AttrValue::Str("checkout".to_owned()),
                },
                Predicate::Equals {
                    field: FieldSel::Attr("blob".to_owned()),
                    value: AttrValue::Bytes(vec![0, 1, 0xff]),
                },
                Predicate::NumRange {
                    field: FieldSel::Attr("status_code".to_owned()),
                    ty: FieldType::I64,
                    min: Some(-3i64 as u64),
                    max: None,
                },
                Predicate::NumRange {
                    field: FieldSel::Attr("sampled".to_owned()),
                    ty: FieldType::Bool,
                    min: Some(1),
                    max: Some(1),
                },
                Predicate::NumRange {
                    field: FieldSel::Attr("ratio".to_owned()),
                    ty: FieldType::F64,
                    min: None,
                    max: Some(0.5f64.to_bits()),
                },
            ],
            key_equals: Vec::new(),
        })
    }

    fn sample_key_equals() -> Vec<KeyEquals> {
        vec![
            KeyEquals::new("status_code", KeyType::I64, key8_i64(-3)),
            KeyEquals::new("service", KeyType::Str, key8_str("checkout")),
            KeyEquals::new("blob", KeyType::Bytes, key8_bytes(&[0, 1, 0xff])),
            KeyEquals::new(TRACE_ID_FIELD, KeyType::Id16, key8_id16(&[0xab; 16])),
        ]
    }

    /// A logs block carrying exact-key equalities (ADR-2707 decision 5) is
    /// version 11 and survives the codec in order, beside the prune
    /// predicates; a version 9 decoder refuses it on its version byte.
    #[test]
    fn logs_key_equals_round_trip_as_version_11() {
        let key = test_key();
        let ticket = sample_ticket();
        let mut logs = sample_logs_pushdown().as_logs().expect("logs").clone();
        logs.key_equals = sample_key_equals();
        let pushdown = SlicePushdown::logs(&logs);
        let bytes = ticket
            .encode_with_pushdown(&key, Some(&pushdown))
            .expect("encode");
        assert_eq!(bytes[4], VERSION_KEY_EQUALS);
        let (decoded, carried) = FlightTicket::decode_with_pushdown(&bytes, &key).expect("decode");
        assert_eq!(decoded, ticket);
        let carried = carried.expect("pushdown");
        assert_eq!(carried, pushdown);
        assert_eq!(
            carried.as_logs().map(|p| p.key_equals.clone()),
            Some(sample_key_equals())
        );
        assert_eq!(bytes[4], 11);
        assert_eq!(
            FlightTicket::decode_accepting(&bytes, &key, &[VERSION, VERSION_PUSHDOWN]),
            Err(FlightTicketError::UnsupportedVersion(VERSION_KEY_EQUALS))
        );
    }

    /// The key section is present exactly when the version is 11: a signed
    /// version 11 ticket that ends at the prune list is truncated, one with
    /// an empty key list is refused, and an unknown key type is refused.
    #[test]
    fn the_key_section_is_present_exactly_when_the_version_is_11() {
        let key = test_key();
        let ticket = sample_ticket();
        let resign = |mut body: Vec<u8>| {
            let tag = mac(&key, &body);
            body.extend_from_slice(&tag);
            body
        };
        let v9 = ticket
            .encode_with_pushdown(&key, Some(&sample_logs_pushdown()))
            .expect("encode");
        assert_eq!(v9[4], VERSION_PUSHDOWN);
        let mut no_section = v9[..v9.len() - MAC_LEN].to_vec();
        no_section[4] = VERSION_KEY_EQUALS;
        assert_eq!(
            FlightTicket::decode_with_pushdown(&resign(no_section.clone()), &key),
            Err(FlightTicketError::Truncated)
        );
        let mut empty = no_section.clone();
        write_u32(&mut empty, 0);
        assert_eq!(
            FlightTicket::decode_with_pushdown(&resign(empty), &key),
            Err(FlightTicketError::InvalidPushdownTag(0))
        );
        let mut bad_type = no_section;
        write_u32(&mut bad_type, 1);
        write_len_prefixed(&mut bad_type, b"k").expect("field");
        bad_type.push(9);
        bad_type.extend_from_slice(&[0u8; 8]);
        assert_eq!(
            FlightTicket::decode_with_pushdown(&resign(bad_type), &key),
            Err(FlightTicketError::InvalidPushdownTag(9))
        );
    }

    /// A signed version 11 ticket carrying a spans block is refused: the
    /// encoder writes a spans block only under version 9, so the version byte
    /// and the block kind disagree.
    #[test]
    fn a_version_11_ticket_with_a_spans_block_is_refused() {
        let key = test_key();
        let ticket = sample_ticket();
        let spans = ticket
            .encode_with_pushdown(&key, Some(&sample_spans_pushdown()))
            .expect("encode");
        assert_eq!(spans[4], VERSION_PUSHDOWN);
        assert!(FlightTicket::decode_with_pushdown(&spans, &key).is_ok());
        let mut body = spans[..spans.len() - MAC_LEN].to_vec();
        body[4] = VERSION_KEY_EQUALS;
        let tag = mac(&key, &body);
        body.extend_from_slice(&tag);
        assert_eq!(
            FlightTicket::decode_with_pushdown(&body, &key),
            Err(FlightTicketError::InvalidPushdownTag(PUSHDOWN_SPANS))
        );
    }

    /// The spans pushdown (time bounds and trace id) survives the ticket
    /// codec bit-for-bit, alongside the unchanged ticket.
    #[test]
    fn spans_pushdown_round_trips() {
        let key = test_key();
        let ticket = sample_ticket();
        let pushdown = sample_spans_pushdown();
        let bytes = ticket
            .encode_with_pushdown(&key, Some(&pushdown))
            .expect("encode");
        let (decoded, carried) = FlightTicket::decode_with_pushdown(&bytes, &key).expect("decode");
        assert_eq!(decoded, ticket);
        assert_eq!(carried.as_ref(), Some(&pushdown));
        let spans = carried
            .as_ref()
            .and_then(SlicePushdown::as_spans)
            .expect("spans block");
        assert_eq!(spans.trace_id, Some([0xabu8; 16]));
        assert_eq!(spans.ts_lo, Some(-5));
        assert!(carried.as_ref().and_then(SlicePushdown::as_logs).is_none());
    }

    /// Every prune shape `extract_logs` produces (string and bytes
    /// equality, I64, Bool and F64 ranges) survives the codec, in order.
    #[test]
    fn logs_pushdown_round_trips() {
        let key = test_key();
        let ticket = sample_ticket();
        let pushdown = sample_logs_pushdown();
        assert_eq!(pushdown.as_logs().map(|p| p.prune.len()), Some(5));
        let bytes = ticket
            .encode_with_pushdown(&key, Some(&pushdown))
            .expect("encode");
        let (decoded, carried) = FlightTicket::decode_with_pushdown(&bytes, &key).expect("decode");
        assert_eq!(decoded, ticket);
        assert_eq!(carried, Some(pushdown));
    }

    /// The spans constructor keeps only the carried fields, and the logs
    /// constructor drops the content channel and any prune shape outside the
    /// layout, so a constructed value always encodes.
    #[test]
    fn slice_pushdown_constructors_keep_the_carried_subset() {
        let spans = SlicePushdown::spans(&SpansPushdown {
            trace_id: Some([1u8; 16]),
            duration_lo: Some(10),
            status_mask: Some(1),
            ..SpansPushdown::default()
        });
        assert_eq!(
            spans.as_spans(),
            Some(&SpansPushdown {
                trace_id: Some([1u8; 16]),
                ..SpansPushdown::default()
            })
        );
        let logs = SlicePushdown::logs(&LogsPushdown {
            ts_lo: Some(1),
            ts_hi: None,
            content: vec![Predicate::Equals {
                field: FieldSel::Body,
                value: AttrValue::Str("x".to_owned()),
            }],
            prune: vec![
                Predicate::Equals {
                    field: FieldSel::Attr("k".to_owned()),
                    value: AttrValue::I64(4),
                },
                Predicate::Equals {
                    field: FieldSel::Attr("k".to_owned()),
                    value: AttrValue::Str("v".to_owned()),
                },
            ],
            key_equals: sample_key_equals(),
        });
        let carried = logs.as_logs().expect("logs block");
        assert!(carried.content.is_empty());
        assert_eq!(carried.prune.len(), 1);
        assert_eq!(carried.key_equals, sample_key_equals());
        assert_eq!(carried.ts_lo, Some(1));
        FlightTicket::encode_with_pushdown(&sample_ticket(), &test_key(), Some(&logs))
            .expect("a constructed pushdown always encodes");
    }

    /// A ticket without the pushdown block is the same bytes `encode` has
    /// always produced, and decodes to the no-pushdown query.
    #[test]
    fn ticket_without_pushdown_decodes_to_none() {
        let key = test_key();
        let ticket = sample_ticket();
        let plain = ticket.encode(&key).expect("encode");
        assert_eq!(
            ticket.encode_with_pushdown(&key, None).expect("encode"),
            plain
        );
        let (decoded, carried) = FlightTicket::decode_with_pushdown(&plain, &key).expect("decode");
        assert_eq!(decoded, ticket);
        assert_eq!(carried, None);
    }

    /// A worker built before the pushdown block decodes only version 8. A
    /// ticket carrying the block is version 9, so that decoder refuses it on
    /// its version byte, after the MAC verifies, rather than parsing up to the
    /// statement and calling the block trailing garbage; a ticket without the
    /// block is still version 8 and that decoder serves it unchanged.
    #[test]
    fn a_pre_pushdown_decoder_refuses_a_pushdown_ticket_on_its_version() {
        let key = test_key();
        let ticket = sample_ticket();
        for pushdown in [sample_spans_pushdown(), sample_logs_pushdown()] {
            let bytes = ticket
                .encode_with_pushdown(&key, Some(&pushdown))
                .expect("encode");
            assert_eq!(bytes[4], 9);
            assert_eq!(
                FlightTicket::decode_accepting(&bytes, &key, &[VERSION]),
                Err(FlightTicketError::UnsupportedVersion(9))
            );
        }

        let plain = ticket.encode(&key).expect("encode");
        assert_eq!(plain[4], 8);
        assert_eq!(
            FlightTicket::decode_accepting(&plain, &key, &[VERSION]),
            Ok((ticket, None))
        );
    }

    /// The current decoder takes a version 8 ticket as the no-pushdown
    /// query, and the block's presence must match the version byte: a
    /// signed version 8 ticket with a block appended is trailing bytes, and
    /// a signed version 9 ticket that ends at the statement is truncated.
    #[test]
    fn the_pushdown_block_is_present_exactly_when_the_version_is_9() {
        let key = test_key();
        let ticket = sample_ticket();
        let resign = |mut body: Vec<u8>| {
            let tag = mac(&key, &body);
            body.extend_from_slice(&tag);
            body
        };

        let plain = ticket.encode(&key).expect("encode");
        assert_eq!(plain[4], 8);
        assert_eq!(
            FlightTicket::decode_with_pushdown(&plain, &key),
            Ok((ticket.clone(), None))
        );

        let with_block = ticket
            .encode_with_pushdown(&key, Some(&sample_spans_pushdown()))
            .expect("encode");
        let mut v8_with_block = with_block[..with_block.len() - MAC_LEN].to_vec();
        v8_with_block[4] = VERSION;
        assert_eq!(
            FlightTicket::decode_with_pushdown(&resign(v8_with_block), &key),
            Err(FlightTicketError::TrailingBytes)
        );

        let mut v9_without_block = plain[..plain.len() - MAC_LEN].to_vec();
        v9_without_block[4] = VERSION_PUSHDOWN;
        assert_eq!(
            FlightTicket::decode_with_pushdown(&resign(v9_without_block), &key),
            Err(FlightTicketError::Truncated)
        );
    }

    /// Plain `decode` accepts a ticket that carries a pushdown and returns
    /// the same ticket, so a surface that ignores the pushdown still serves.
    #[test]
    fn plain_decode_accepts_a_pushdown_ticket() {
        let key = test_key();
        let ticket = sample_ticket();
        let bytes = ticket
            .encode_with_pushdown(&key, Some(&sample_spans_pushdown()))
            .expect("encode");
        assert_eq!(FlightTicket::decode(&bytes, &key).expect("decode"), ticket);
    }

    /// The pushdown is inside the MAC: flipping a pushdown byte without the
    /// key is a MAC mismatch, never a different predicate.
    #[test]
    fn pushdown_bytes_are_authenticated() {
        let key = test_key();
        let ticket = sample_ticket();
        let plain_len = ticket.encode(&key).expect("encode").len() - MAC_LEN;
        let mut bytes = ticket
            .encode_with_pushdown(&key, Some(&sample_spans_pushdown()))
            .expect("encode");
        // The last trace id byte.
        let at = bytes.len() - MAC_LEN - 1;
        assert!(at > plain_len);
        bytes[at] ^= 0x01;
        assert!(matches!(
            FlightTicket::decode_with_pushdown(&bytes, &key),
            Err(FlightTicketError::MacMismatch)
        ));
    }

    /// A correctly signed block with an unknown kind tag, an unknown prune
    /// tag, or a truncated tail is a typed error, never a guessed predicate.
    #[test]
    fn malformed_pushdown_is_a_typed_error() {
        let key = test_key();
        let ticket = sample_ticket();
        let plain_len = ticket.encode(&key).expect("encode").len() - MAC_LEN;
        let resign = |mut body: Vec<u8>| {
            let tag = mac(&key, &body);
            body.extend_from_slice(&tag);
            body
        };

        let spans = ticket
            .encode_with_pushdown(&key, Some(&sample_spans_pushdown()))
            .expect("encode");
        let mut body = spans[..spans.len() - MAC_LEN].to_vec();
        assert_eq!(body[plain_len], PUSHDOWN_SPANS);
        body[plain_len] = 9;
        assert!(matches!(
            FlightTicket::decode_with_pushdown(&resign(body), &key),
            Err(FlightTicketError::InvalidPushdownTag(9))
        ));

        let logs = ticket
            .encode_with_pushdown(&key, Some(&sample_logs_pushdown()))
            .expect("encode");
        let mut body = logs[..logs.len() - MAC_LEN].to_vec();
        // kind, two opt-u64 (absent lo: 1 byte, present hi: 9), the count.
        let first_tag = plain_len + 1 + 1 + 9 + 4;
        assert_eq!(body[first_tag], PRUNE_EQUALS);
        body[first_tag] = 7;
        assert!(matches!(
            FlightTicket::decode_with_pushdown(&resign(body), &key),
            Err(FlightTicketError::InvalidPushdownTag(7))
        ));

        let body = spans[..spans.len() - MAC_LEN - 3].to_vec();
        assert!(FlightTicket::decode_with_pushdown(&resign(body), &key).is_err());
    }

    /// A hand-built pushdown outside the layout refuses to encode rather
    /// than being silently dropped from a signed ticket.
    #[test]
    fn uncarried_pushdown_refuses_to_encode() {
        let pushdown = SlicePushdown(SlicePushdownKind::Logs(LogsPushdown {
            ts_lo: None,
            ts_hi: None,
            content: Vec::new(),
            prune: vec![Predicate::Equals {
                field: FieldSel::Attr("k".to_owned()),
                value: AttrValue::I64(4),
            }],
            key_equals: Vec::new(),
        }));
        assert!(matches!(
            sample_ticket().encode_with_pushdown(&test_key(), Some(&pushdown)),
            Err(FlightTicketError::UncarriedPushdown)
        ));
    }

    /// The keyring surface carries the pushdown the same way, and the slice
    /// key a coordinator mints with is the one the worker verifies with.
    #[test]
    fn sql_ticket_keys_round_trip_the_pushdown() {
        let keys = SqlTicketKeys::from_file_keys([b"file-key".as_slice()]).expect("one key");
        let ticket = sample_ticket();
        let pushdown = sample_logs_pushdown();
        let bytes = keys
            .encode_with_pushdown(&ticket, TicketSurface::Slice, Some(&pushdown))
            .expect("encode");
        let (decoded, carried) = keys
            .decode_with_pushdown(&bytes, TicketSurface::Slice)
            .expect("decode");
        assert_eq!(decoded, ticket);
        assert_eq!(carried, Some(pushdown));
        assert!(
            keys.decode_with_pushdown(&bytes, TicketSurface::Client)
                .is_err()
        );
    }

    /// Version 11 is a slice-surface version. A key-equals ticket signed
    /// under the client key, so the MAC verifies, is refused on the client
    /// surface by its version byte, never read under version 10's grouped
    /// layout; the same ticket signed for the slice surface decodes there.
    #[test]
    fn a_version_11_ticket_on_the_client_surface_is_wrong_surface() {
        let keys = SqlTicketKeys::from_file_keys([b"file-key".as_slice()]).expect("one key");
        let ticket = sample_ticket();
        let mut logs = sample_logs_pushdown().as_logs().expect("logs").clone();
        logs.key_equals = sample_key_equals();
        let pushdown = SlicePushdown::logs(&logs);
        let client = keys
            .encode_with_pushdown(&ticket, TicketSurface::Client, Some(&pushdown))
            .expect("encode");
        assert_eq!(client[4], 11);
        assert_eq!(
            keys.decode_with_pushdown(&client, TicketSurface::Client),
            Err(FlightTicketError::UnsupportedVersion(11))
        );
        let slice = keys
            .encode_with_pushdown(&ticket, TicketSurface::Slice, Some(&pushdown))
            .expect("encode");
        assert_eq!(
            keys.decode_with_pushdown(&slice, TicketSurface::Slice),
            Ok((ticket, Some(pushdown)))
        );
    }

    const TWO_SIGNAL_STATEMENT: &str =
        "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts WHERE s.ts >= 1 AND s.ts < 2";

    /// [`sample_ticket`] over samples JOIN logs: its flat pins moved into one
    /// group per signal, each with its own segments and erasure predicates.
    fn grouped_ticket() -> FlightTicket {
        FlightTicket {
            statement: TWO_SIGNAL_STATEMENT.to_owned(),
            segments: Vec::new(),
            pending_erasure: Vec::new(),
            signal_groups: vec![
                SignalGroup {
                    signal: TargetSignal::Metrics,
                    segments: vec![
                        sample_pin(3, "t/aa/metrics/l1/0000/w.1.2.abc.rseg"),
                        sample_pin(8, "t/aa/metrics/l0/0001/w.4.5.def.rseg"),
                    ],
                    pending_erasure: vec![ErasurePredicate::windowless(vec![(
                        "__name__".to_owned(),
                        "erase_me".to_owned(),
                    )])],
                },
                SignalGroup {
                    signal: TargetSignal::Logs,
                    segments: vec![sample_pin(5, "t/aa/logs/l1/0002/w.6.7.fed.rlog")],
                    pending_erasure: vec![ErasurePredicate::new(
                        vec![("service.name".to_owned(), "checkout".to_owned())],
                        100,
                        200,
                    )],
                },
            ],
            ..sample_ticket()
        }
    }

    /// A signed version 10 body with `groups` (tag, segment pins) in the
    /// given order and no erasure predicates, so a decode refusal is reached
    /// with a valid MAC.
    fn signed_v10(key: &TicketKey, groups: &[(u8, Vec<SegmentPin>)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.push(VERSION_GROUPS);
        body.extend_from_slice(&[7u8; 16]);
        body.extend_from_slice(&1i64.to_le_bytes());
        body.extend_from_slice(&2i64.to_le_bytes());
        write_u32(&mut body, 0); // slice_index
        write_u32(&mut body, 1); // slice_count
        write_u32(&mut body, 0); // token_count
        write_u32(&mut body, groups.len() as u32);
        for (tag, segments) in groups {
            body.push(*tag);
            write_pins(&mut body, segments, &[]).expect("pins");
        }
        write_u32(&mut body, 0); // declared_count
        write_u32(&mut body, 0); // parquet_count
        write_budgets(&mut body, None);
        write_len_prefixed(&mut body, TWO_SIGNAL_STATEMENT.as_bytes()).expect("statement");
        let tag = mac(key, &body);
        body.extend_from_slice(&tag);
        body
    }

    #[test]
    fn v10_round_trips_two_signal_groups() {
        let key = test_key();
        let ticket = grouped_ticket();
        let bytes = ticket.encode(&key).expect("encode");
        assert_eq!(bytes[4], VERSION_GROUPS);
        let decoded = FlightTicket::decode(&bytes, &key).expect("decode");
        assert_eq!(decoded, ticket);
        assert!(decoded.segments.is_empty() && decoded.pending_erasure.is_empty());

        let snapshots = decoded.signal_snapshots();
        assert_eq!(snapshots.len(), 2);
        for ((signal, snapshot), group) in snapshots.iter().zip(&ticket.signal_groups) {
            assert_eq!(*signal, group.signal);
            assert_eq!(
                snapshot.segments,
                group
                    .segments
                    .iter()
                    .map(SegmentPin::to_segment_ref)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                ravel_query::erasure::snapshot_pending_erasure_predicates(snapshot),
                group.pending_erasure
            );
        }
        assert_eq!(
            decoded.snapshot(),
            Err(FlightTicketError::SignalGroupsHaveNoSingleSnapshot)
        );

        let keys = SqlTicketKeys::from_file_key(&[0x42; 32]);
        let client = keys
            .encode(&ticket, TicketSurface::Client)
            .expect("client encode");
        assert_eq!(keys.decode(&client, TicketSurface::Client), Ok(ticket));
    }

    #[test]
    fn encode_refuses_a_grouped_ticket_it_cannot_write() {
        let key = test_key();
        let beside_segments = FlightTicket {
            segments: vec![sample_pin(1, "t/aa/metrics/l0/0000/w.1.1.a.rseg")],
            ..grouped_ticket()
        };
        assert_eq!(
            beside_segments.encode(&key),
            Err(FlightTicketError::SignalGroupsBesideFlatPins)
        );
        let beside_erasure = FlightTicket {
            pending_erasure: vec![ErasurePredicate::windowless(vec![(
                "a".to_owned(),
                "b".to_owned(),
            )])],
            ..grouped_ticket()
        };
        assert_eq!(
            beside_erasure.encode(&key),
            Err(FlightTicketError::SignalGroupsBesideFlatPins)
        );
        assert_eq!(
            grouped_ticket().encode_with_pushdown(&key, Some(&sample_spans_pushdown())),
            Err(FlightTicketError::SignalGroupsWithPushdown)
        );

        let with_groups = |signals: &[TargetSignal]| FlightTicket {
            signal_groups: signals
                .iter()
                .map(|signal| SignalGroup {
                    signal: *signal,
                    segments: Vec::new(),
                    pending_erasure: Vec::new(),
                })
                .collect(),
            ..grouped_ticket()
        };
        assert_eq!(
            with_groups(&[TargetSignal::Logs]).encode(&key),
            Err(FlightTicketError::TooFewSignalGroups(1))
        );
        assert_eq!(
            with_groups(&[TargetSignal::Logs, TargetSignal::Logs]).encode(&key),
            Err(FlightTicketError::DuplicateSignalGroup(SIGNAL_TAG_LOGS))
        );
        assert_eq!(
            with_groups(&[TargetSignal::Spans, TargetSignal::Logs]).encode(&key),
            Err(FlightTicketError::SignalGroupsOutOfOrder)
        );
        assert_eq!(
            with_groups(&[TargetSignal::Metrics, TargetSignal::Parquet]).encode(&key),
            Err(FlightTicketError::ParquetSignalGroup)
        );

        let keys = SqlTicketKeys::from_file_key(&[0x42; 32]);
        assert_eq!(
            keys.encode(&grouped_ticket(), TicketSurface::Slice),
            Err(FlightTicketError::SignalGroupsOnSlice)
        );
    }

    /// The tags are the wire contract: samples 1, logs 2, spans 3, alerts 4,
    /// audit 5, in that lane order.
    #[test]
    fn signal_tags_are_pinned_in_lane_order() {
        let tags: Vec<u8> = SIGNAL_LANE_ORDER
            .iter()
            .map(|signal| signal_tag(*signal).expect("a signal table"))
            .collect();
        assert_eq!(tags, vec![1, 2, 3, 4, 5]);
        for (tag, signal) in tags.iter().zip(SIGNAL_LANE_ORDER) {
            assert_eq!(signal_from_tag(*tag), Ok(signal));
        }
        assert_eq!(
            signal_tag(TargetSignal::Parquet),
            Err(FlightTicketError::ParquetSignalGroup)
        );
    }

    #[test]
    fn a_v10_ticket_with_fewer_than_two_groups_is_refused() {
        let key = test_key();
        let one = signed_v10(&key, &[(SIGNAL_TAG_LOGS, Vec::new())]);
        assert_eq!(
            FlightTicket::decode(&one, &key),
            Err(FlightTicketError::TooFewSignalGroups(1))
        );
        let none = signed_v10(&key, &[]);
        assert_eq!(
            FlightTicket::decode(&none, &key),
            Err(FlightTicketError::TooFewSignalGroups(0))
        );
    }

    #[test]
    fn a_v10_ticket_with_an_unknown_signal_tag_is_refused() {
        let key = test_key();
        for tag in [0u8, 6, 0xff] {
            let bytes = signed_v10(&key, &[(SIGNAL_TAG_SAMPLES, Vec::new()), (tag, Vec::new())]);
            assert_eq!(
                FlightTicket::decode(&bytes, &key),
                Err(FlightTicketError::InvalidSignalTag(tag)),
                "tag {tag}"
            );
        }
    }

    #[test]
    fn a_v10_ticket_naming_a_signal_twice_is_refused() {
        let key = test_key();
        let bytes = signed_v10(
            &key,
            &[
                (
                    SIGNAL_TAG_LOGS,
                    vec![sample_pin(2, "t/aa/logs/l0/0000/w.1.1.a.rlog")],
                ),
                (SIGNAL_TAG_LOGS, Vec::new()),
            ],
        );
        assert_eq!(
            FlightTicket::decode(&bytes, &key),
            Err(FlightTicketError::DuplicateSignalGroup(SIGNAL_TAG_LOGS))
        );
    }

    #[test]
    fn a_v10_ticket_with_groups_out_of_lane_order_is_refused() {
        let key = test_key();
        let bytes = signed_v10(
            &key,
            &[
                (SIGNAL_TAG_LOGS, Vec::new()),
                (SIGNAL_TAG_SAMPLES, Vec::new()),
            ],
        );
        assert_eq!(
            FlightTicket::decode(&bytes, &key),
            Err(FlightTicketError::SignalGroupsOutOfOrder)
        );
    }

    /// Every structural refusal above sits behind the MAC: the same bodies
    /// signed under another key are a MAC mismatch, never parsed.
    #[test]
    fn v10_refusals_are_reached_only_after_the_mac_verifies() {
        let key = test_key();
        let other = [0x43u8; TICKET_KEY_LEN];
        for groups in [
            vec![(SIGNAL_TAG_LOGS, Vec::new())],
            vec![(SIGNAL_TAG_SAMPLES, Vec::new()), (6, Vec::new())],
            vec![(SIGNAL_TAG_LOGS, Vec::new()), (SIGNAL_TAG_LOGS, Vec::new())],
            vec![
                (SIGNAL_TAG_LOGS, Vec::new()),
                (SIGNAL_TAG_SAMPLES, Vec::new()),
            ],
        ] {
            let bytes = signed_v10(&other, &groups);
            assert_eq!(
                FlightTicket::decode(&bytes, &key),
                Err(FlightTicketError::MacMismatch)
            );
        }
    }

    /// A version 8 or 9 ticket pins one snapshot. Its statement naming two
    /// signal tables decodes, and redemption refuses it before any plan.
    #[test]
    fn a_v8_ticket_naming_two_signals_is_refused() {
        let key = test_key();
        let ticket = FlightTicket {
            statement: TWO_SIGNAL_STATEMENT.to_owned(),
            ..sample_ticket()
        };
        let v8 = ticket.encode(&key).expect("encode");
        assert_eq!(v8[4], VERSION);
        let decoded = FlightTicket::decode(&v8, &key).expect("decode");
        assert!(matches!(
            decoded.redemption_lanes(),
            Err(SqlError::TicketPinsOneSignal)
        ));

        let v9 = ticket
            .encode_with_pushdown(&key, Some(&sample_logs_pushdown()))
            .expect("encode");
        assert_eq!(v9[4], VERSION_PUSHDOWN);
        let decoded = FlightTicket::decode(&v9, &key).expect("decode");
        assert!(matches!(
            decoded.redemption_lanes(),
            Err(SqlError::TicketPinsOneSignal)
        ));
    }

    /// A version 10 ticket's groups must be exactly the statement's signal
    /// tables, in lane order.
    #[test]
    fn a_v10_ticket_whose_groups_are_not_the_statements_tables_is_refused() {
        let key = test_key();
        let cases = [
            (
                "SELECT s.ts FROM samples s JOIN spans t ON s.ts = t.ts",
                "samples, spans",
            ),
            ("SELECT ts FROM samples", "samples"),
            (
                "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts \
                 JOIN spans t ON s.ts = t.ts",
                "samples, logs, spans",
            ),
        ];
        for (statement, tables) in cases {
            let ticket = FlightTicket {
                statement: statement.to_owned(),
                ..grouped_ticket()
            };
            let decoded =
                FlightTicket::decode(&ticket.encode(&key).expect("encode"), &key).expect("decode");
            match decoded.redemption_lanes() {
                Err(SqlError::PinnedSignalsMismatch {
                    pinned,
                    tables: got,
                }) => {
                    assert_eq!(pinned, "samples, logs", "{statement}");
                    assert_eq!(got, tables, "{statement}");
                }
                other => panic!("{statement}: expected a mismatch, got {other:?}"),
            }
        }
    }

    #[test]
    fn redemption_lanes_rebuild_every_pinned_snapshot() {
        let key = test_key();
        let ticket = grouped_ticket();
        let decoded =
            FlightTicket::decode(&ticket.encode(&key).expect("encode"), &key).expect("decode");
        let lanes = decoded.redemption_lanes().expect("lanes");
        assert_eq!(lanes, ticket.signal_snapshots());
        assert_eq!(
            lanes.iter().map(|(signal, _)| *signal).collect::<Vec<_>>(),
            vec![TargetSignal::Metrics, TargetSignal::Logs]
        );

        let single = sample_ticket();
        let decoded =
            FlightTicket::decode(&single.encode(&key).expect("encode"), &key).expect("decode");
        let lanes = decoded.redemption_lanes().expect("lanes");
        assert_eq!(
            lanes,
            vec![(
                TargetSignal::Metrics,
                single.snapshot().expect("a one-snapshot ticket")
            )]
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // decode(encode(x)) == x for 2 to 5 groups in lane order, each with
        // its own segments and erasure predicates.
        #[test]
        fn prop_v10_round_trip(
            lanes in prop::sample::subsequence(SIGNAL_LANE_ORDER.to_vec(), 2..=5),
            groups in prop::collection::vec(
                (
                    prop::collection::vec(segment_pin_strategy(), 0..64),
                    prop::collection::vec(erasure_predicate_strategy(), 0..4),
                ),
                5,
            ),
            tokens in prop::collection::vec(token_strategy(), 0..8),
            statement in ".{0,512}",
            budgets in budgets_strategy(),
        ) {
            let signal_groups = lanes
                .iter()
                .zip(groups)
                .map(|(signal, (segments, pending_erasure))| SignalGroup {
                    signal: *signal,
                    segments,
                    pending_erasure,
                })
                .collect();
            let ticket = FlightTicket {
                statement,
                min_commit_tokens: tokens,
                budgets,
                signal_groups,
                ..grouped_ticket()
            };
            let key = test_key();
            let bytes = ticket.encode(&key).expect("encode");
            prop_assert_eq!(bytes[4], VERSION_GROUPS);
            let decoded = FlightTicket::decode(&bytes, &key).expect("decode");
            prop_assert_eq!(ticket, decoded);
        }

        // Arbitrary correctly signed bytes after a version 10 header never
        // panic: decode returns Ok or a typed Err.
        #[test]
        fn prop_signed_v10_bytes_never_panic(
            tail in prop::collection::vec(any::<u8>(), 0..512),
        ) {
            let key = test_key();
            let mut body = MAGIC.to_vec();
            body.push(VERSION_GROUPS);
            body.extend_from_slice(&tail);
            pad_to_min_encoded_len(&mut body);
            let tag = mac(&key, &body);
            body.extend_from_slice(&tag);
            let _ = FlightTicket::decode(&body, &key);
        }

        // Any single-byte flip of a valid version 10 ticket is caught.
        #[test]
        fn prop_v10_single_flip_detected(idx in any::<prop::sample::Index>()) {
            let key = test_key();
            let bytes = grouped_ticket().encode(&key).expect("encode");
            let i = idx.index(bytes.len());
            let mut corrupt = bytes;
            corrupt[i] ^= 0x80;
            prop_assert!(FlightTicket::decode(&corrupt, &key).is_err());
        }

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
            pending_erasure in prop::collection::vec(erasure_predicate_strategy(), 0..8),
            declared_columns in prop::collection::vec(declared_column_strategy(), 0..8),
            parquet_tables in prop::collection::vec(
                parquet_pin_strategy(),
                0..=MAX_STATEMENT_TABLE_NAMES,
            ),
            budgets in budgets_strategy(),
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
                pending_erasure,
                declared_columns,
                parquet_tables,
                budgets,
                signal_groups: Vec::new(),
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

        // decode(encode(x)) == x for arbitrary spans and logs pushdowns.
        #[test]
        fn prop_pushdown_round_trip(
            ts_lo in proptest::option::of(any::<i64>()),
            ts_hi in proptest::option::of(any::<i64>()),
            trace_id in proptest::option::of(any::<[u8; 16]>()),
            spans in any::<bool>(),
            prune in prop::collection::vec(
                (
                    "[a-z_.]{0,12}",
                    0u8..5,
                    prop::collection::vec(any::<u8>(), 0..8),
                    proptest::option::of(any::<u64>()),
                    proptest::option::of(any::<u64>()),
                ),
                0..6,
            ),
            keys in prop::collection::vec(("[a-z_.]{0,12}", 1u8..=4, any::<[u8; 8]>()), 0..4),
        ) {
            let pushdown = if spans {
                SlicePushdown::spans(&SpansPushdown {
                    ts_lo,
                    ts_hi,
                    trace_id,
                    ..SpansPushdown::default()
                })
            } else {
                let prune = prune
                    .into_iter()
                    .map(|(key, shape, bytes, min, max)| {
                        let field = FieldSel::Attr(key);
                        match shape {
                            0 => Predicate::Equals {
                                field,
                                value: AttrValue::Str(String::from_utf8_lossy(&bytes).into_owned()),
                            },
                            1 => Predicate::Equals { field, value: AttrValue::Bytes(bytes) },
                            2 => Predicate::NumRange { field, ty: FieldType::I64, min, max },
                            3 => Predicate::NumRange { field, ty: FieldType::F64, min, max },
                            _ => Predicate::NumRange { field, ty: FieldType::Bool, min, max },
                        }
                    })
                    .collect();
                let key_equals = keys
                    .into_iter()
                    .map(|(field, ty, key8)| {
                        let key_type = KeyType::from_u8(ty).expect("1..=4 is a key type");
                        KeyEquals::new(field, key_type, key8)
                    })
                    .collect();
                SlicePushdown::logs(&LogsPushdown {
                    ts_lo,
                    ts_hi,
                    content: Vec::new(),
                    prune,
                    key_equals,
                })
            };
            let key = test_key();
            let ticket = sample_ticket();
            let bytes = ticket.encode_with_pushdown(&key, Some(&pushdown)).expect("encode");
            let (decoded, carried) =
                FlightTicket::decode_with_pushdown(&bytes, &key).expect("decode");
            prop_assert_eq!(decoded, ticket);
            prop_assert_eq!(carried, Some(pushdown));
        }

        // Arbitrary correctly signed bytes in the pushdown position never
        // panic: a slice-surface decode returns Ok or a typed Err for every
        // version that surface accepts.
        #[test]
        fn prop_signed_pushdown_tail_never_panics(
            tail in prop::collection::vec(any::<u8>(), 1..96),
            version in prop::sample::select(SLICE_VERSIONS.to_vec()),
        ) {
            let key = test_key();
            let plain = sample_ticket().encode(&key).expect("encode");
            let mut body = plain[..plain.len() - MAC_LEN].to_vec();
            body[4] = version;
            body.extend_from_slice(&tail);
            let tag = mac(&key, &body);
            body.extend_from_slice(&tag);
            let _ = FlightTicket::decode_accepting(&body, &key, SLICE_VERSIONS);
        }
    }
}

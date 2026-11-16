//! `compat certify` for a class-A family, and the member probe the certificate
//! census and the retention version holds share (ADR-4233 decision 7 and hazard
//! H5).
//!
//! # The member probe
//!
//! [`probe_member`] classifies one data or part object by its own bytes:
//! absent, structurally corrupt, trailer version outside this build's reader
//! window, or readable, in which case it returns the trailer version and the
//! grammar version of any nested section this build cannot read. A `.rlog`
//! object's trailer can be in window while a nested POSTINGS or KEY_IDX section
//! carries a grammar version outside this build's read set (ADR-4233
//! inventory: both read sets are `{1,2}`), and a `.rspan` object's trailer can
//! be in window over a KEY_IDX section outside `{1,2}`. A trailer-only gate
//! never sees either. The probe reads each section's version through a ranged
//! GET and classifies it against the same read set the reader applies when it
//! opens the section (`ravel_logseg::postings::PostingsSection::parse`,
//! `ravel_logseg::key_index` and `ravel_codec::key_idx::KeyIdxPrefix::parse`,
//! each of which refuses a version outside its set with
//! `UnsupportedSectionVersion`).
//!
//! Cost per object:
//!
//! - RSEG: one 16-byte suffix GET of the trailer.
//! - RLOG: one 64 KiB suffix GET of the footer (a second ranged GET only when
//!   the footer did not fit), plus one 1-byte ranged GET of the POSTINGS header
//!   when the object carries a POSTINGS section, plus one ranged GET of the
//!   KEY_IDX prefix when it carries a KEY_IDX section: at most four GETs.
//! - RSPAN: the same footer read (one or two GETs), plus one 1-byte ranged GET
//!   of the mandatory KEY_IDX section's version byte: at most three GETs.
//!
//! The retention holds treat an absent or structurally corrupt object as not
//! held (corrupt is never a version hold). The certificate census refuses on
//! either: it cannot confirm such a member is at or above the target.
//!
//! # The certify cursor
//!
//! `sys/maintain/certify/<family>/cursor` persists `S`, the target version and
//! activation epoch it was taken for, the family's certificate identity and
//! `contradicted` flag as the first run read them, and per `(tenant, signal)`
//! how many shards the census finished with their entry counts and newest
//! creation time. A resumed run keeps `S` and skips only the shards a key in
//! the cursor names, so a tenant or shard that appears between runs is still
//! censused. A cursor whose target, epoch, certificate identity or
//! `contradicted` flag no longer matches `sys/compat` is stale and starts a
//! fresh census with a new `S`; the certificate CAS changes the identity, so a
//! cursor left behind by a completed certification (a failed delete) is never
//! resumed.

use std::collections::BTreeMap;
use std::ops::Range;

use bytes::Bytes;
use ravel_catalog::compat::{
    COMPAT_KEY, CompatError, CompatKeys, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, read_compat,
    update_compat,
};
use ravel_catalog::{
    CompatFenceOutcome, FloorBasis, ProvisioningError, provisioning_key, raise_compat_fence,
    raise_format_floor, read_generations_from_store,
};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_logseg::LogSegError;
use ravel_logseg::footer::{LogFooter, kind};
use ravel_logseg::key_index;
use ravel_logseg::postings::{POSTINGS_VERSION, POSTINGS_VERSION_V1};
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version, list_all_reporting,
};
use ravel_proto::sys::v1 as sysproto;
use ravel_rspan::footer::SpanFooter;
use ravel_types::{Signal, TenantHash};

use crate::discover::discover_tenants;
use crate::error::{MaintainError, Result};

/// The staleness horizon `H`: a writer stops starting to encode a
/// pre-activation object within this of an activation (ADR-4233 decision 3).
const H_NS: i64 = ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS;
/// Three hours, the deliberate-margin term of `G_pre` (ADR-4233 decision 4).
const G_PRE_BASE_NS: i64 = 3 * 3_600 * 1_000_000_000;

/// The timing terms the certificate preconditions need that are not on
/// `sys/compat`: the store request timeout `T_req`, the writers'
/// `max_flush_lifetime` (the `2 x` term of `G_pre`), and the clock-skew
/// allowance. `P` is read from the record, `H` is [`H_NS`]. `certify` raises
/// `max_flush_lifetime_ns` to the value `sys/gc` records when that is larger.
#[derive(Debug, Clone, Copy)]
pub struct CertifyTimings {
    pub t_req_ns: i64,
    pub max_flush_lifetime_ns: i64,
    pub clock_skew_allowance_ns: i64,
}

impl CertifyTimings {
    /// `G_pre = 3h + 2 x max_flush_lifetime + P + T_req` (ADR-4233 decision 4).
    fn g_pre_ns(&self, p_ns: i64) -> i64 {
        G_PRE_BASE_NS
            .saturating_add(self.max_flush_lifetime_ns.saturating_mul(2))
            .saturating_add(p_ns)
            .saturating_add(self.t_req_ns)
    }
}

/// Why `certify` did not produce a certificate, short of a store or decode
/// fault ([`MaintainError`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertifyRefusal {
    /// The family is not one of the class-A data families.
    NotClassAFamily { family: String },
    /// `sys/compat` is absent; `compat init` creates it.
    CompatRecordAbsent,
    /// The family has no entry in `sys/compat`.
    FamilyNotInRecord { family: String },
    /// `sys/compat` records no `max_publish_lifetime_ns` (`P`). The time
    /// precondition depends on it, and no default stands in for it.
    PublishLifetimeAbsent,
    /// The target version is above the family's active write version.
    TargetAboveActive { target: u32, active: u32 },
    /// The activation's eager fence walk has not finished.
    FenceWalkIncomplete,
    /// The time precondition `S` is gated on has not passed.
    TimeGate { now_ns: i64, not_before_ns: i64 },
    /// A provisioned bucket was fenced too recently for its own `G_pre` to
    /// have passed; the certificate waits for it (rechecked before the CAS).
    BucketRecentlyFenced {
        tenant_hash: String,
        signal: Signal,
        not_before_ns: i64,
    },
    /// A provisioned bucket is fenced below the family's activation epoch at
    /// the recheck before the CAS: a writer that never saw the activation can
    /// still publish into it.
    BucketUnfenced {
        tenant_hash: String,
        signal: Signal,
        compat_epoch: u64,
        activated_epoch: u64,
    },
    /// The tenant set at the recheck before the CAS differs from the set the
    /// census covered.
    TenantSetChanged,
    /// A listing was incomplete: an unaddressable key under a census prefix.
    IncompleteListing { prefix: String, unaddressable: u64 },
    /// An entry directly under a bucket's `l0/`, `l1/` or `c/` prefix is not a
    /// shard directory, so no shard census would cover what it holds.
    UnexpectedShardEntry { key: String },
    /// The family carries a certificate above the target version. A
    /// certificate is replaced only by one of a higher version, or of the
    /// same version after a late write (ADR-4233 decision 7).
    TargetBelowCertificate { target: u32, certified: u32 },
    /// The family carries an uncontradicted certificate at the target
    /// version. A same-version certificate replaces it only once a late write
    /// set `contradicted` and was remedied (ADR-4233 decision 7).
    AlreadyCertified { version: u32 },
    /// A record names a member object that is absent, or a listed object was
    /// gone by the time its trailer was read.
    MemberAbsent { member_key: String },
    /// A member's trailer is structurally corrupt.
    MemberCorrupt { member_key: String },
    /// A member's trailer version differs from the version its record stamps.
    StampTrailerMismatch {
        member_key: String,
        stamp: u32,
        trailer: u16,
    },
    /// The family's certificate or `contradicted` flag changed after the
    /// census read `sys/compat` (a late-write pass or another certification
    /// ran meanwhile). Nothing was written; a later run starts a fresh census.
    CompatChangedDuringCensus,
    /// `sys/compat` was replaced between the read and the write of the
    /// certificate CAS, without changing this family's certificate or
    /// `contradicted` flag. Nothing was written; the census cursor is kept,
    /// so a later run resumes it and retries the CAS.
    CompatRecordRaced,
    /// Another certify run created or replaced the cursor first.
    ConcurrentCertify,
}

/// What one `certify` run concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertifyReport {
    /// The census passed and a certificate was written under CAS. The counts
    /// cover every run of a resumed census, not only the last.
    Certified {
        version: u32,
        member_count: u64,
        buckets_scanned: u64,
        newest_created_unix_ns: i64,
    },
    /// A member object below the target version was found: the result is
    /// contradicted, not certified, and no certificate is written.
    Contradicted {
        member_key: String,
        found_version: u32,
    },
    /// A member object this build cannot read blocks certification: its
    /// trailer version (reported here) is outside the window, or a nested
    /// section's grammar version (reported here) is outside the read set.
    HeldMember { member_key: String, version: u16 },
    /// The budget stopped the census before it covered every bucket. The
    /// cursor keeps `S`; a later run resumes from it.
    Incomplete { units_examined: usize },
    /// A precondition refused the run before any certificate was written.
    Refused(CertifyRefusal),
}

/// The certify cursor payload tag. Distinct from the compaction and migrate
/// cursor tags, and from this cursor's earlier index-based layout (tag 1), so
/// any other payload is a fresh census, never a misread.
const CERTIFY_CURSOR_TAG: u8 = 2;
/// tag(1) + target u32(4) + activated_epoch u64(8) + S i64(8) +
/// has_cert u8(1) + cert version u32(4) + certified_unix_ns i64(8) +
/// contradicted u8(1) + finalizing u8(1) + finalizing_unix_ns i64(8) +
/// bucket count u32(4).
const CURSOR_HEADER_LEN: usize = 48;
/// tenant(16) + signal index u8(1) + shards_done u32(4) + entries u64(8) +
/// newest_created i64(8).
const CURSOR_BUCKET_LEN: usize = 37;

/// The store-wide certify cursor key for one family. Disjoint from the
/// per-tenant compaction and migrate cursors: it lives under `sys/maintain`,
/// never under a tenant's `maint/` directory.
fn certify_cursor_key(family: &str) -> String {
    format!("sys/maintain/certify/{family}/cursor")
}

/// The family's certificate identity and `contradicted` flag, as one census
/// read them. The certificate CAS refuses when the record no longer matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PriorState {
    /// `(version, certified_unix_ns)` of the certificate, if any.
    certificate: Option<(u32, i64)>,
    contradicted: bool,
}

impl PriorState {
    fn of(entry: &sysproto::CompatFamily) -> Self {
        PriorState {
            certificate: entry
                .certificate
                .as_ref()
                .map(|c| (c.version, c.certified_unix_ns)),
            contradicted: entry.contradicted,
        }
    }
}

/// The census progress of one `(tenant, signal)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BucketProgress {
    /// Shards `0..shards_done` are censused.
    shards_done: u32,
    /// Recorded members (commit-family entries) in those shards.
    entries: u64,
    /// Newest member creation time in those shards.
    newest_created_unix_ns: i64,
}

/// The certify cursor. Buckets are keyed by tenant and the signal's index in
/// [`family_signals`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct CertifyCursor {
    target_version: u32,
    activated_epoch: u64,
    start_unix_ns: i64,
    prior: PriorState,
    /// Set just before the certificate CAS to the `certified_unix_ns` that
    /// CAS writes. A cursor whose marker matches the family's certificate
    /// belongs to a certification whose CAS succeeded and whose floor raises
    /// may not have finished; the next run completes them.
    finalizing_unix_ns: Option<i64>,
    buckets: BTreeMap<(TenantHash, u8), BucketProgress>,
}

fn encode_cursor(cursor: &CertifyCursor) -> Vec<u8> {
    let mut out = Vec::with_capacity(CURSOR_HEADER_LEN + cursor.buckets.len() * CURSOR_BUCKET_LEN);
    out.push(CERTIFY_CURSOR_TAG);
    out.extend_from_slice(&cursor.target_version.to_le_bytes());
    out.extend_from_slice(&cursor.activated_epoch.to_le_bytes());
    out.extend_from_slice(&cursor.start_unix_ns.to_le_bytes());
    let (has_cert, cert_version, cert_ns) = match cursor.prior.certificate {
        Some((version, ns)) => (1u8, version, ns),
        None => (0u8, 0u32, 0i64),
    };
    out.push(has_cert);
    out.extend_from_slice(&cert_version.to_le_bytes());
    out.extend_from_slice(&cert_ns.to_le_bytes());
    out.push(u8::from(cursor.prior.contradicted));
    out.push(u8::from(cursor.finalizing_unix_ns.is_some()));
    out.extend_from_slice(&cursor.finalizing_unix_ns.unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&(cursor.buckets.len() as u32).to_le_bytes());
    for ((tenant, signal_index), progress) in &cursor.buckets {
        out.extend_from_slice(&tenant.0);
        out.push(*signal_index);
        out.extend_from_slice(&progress.shards_done.to_le_bytes());
        out.extend_from_slice(&progress.entries.to_le_bytes());
        out.extend_from_slice(&progress.newest_created_unix_ns.to_le_bytes());
    }
    out
}

/// A little-endian reader over a cursor payload; every read is bounds-checked.
struct CursorReader<'a> {
    data: &'a [u8],
}

impl CursorReader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.data.split_at_checked(N)?;
        self.data = rest;
        head.try_into().ok()
    }
    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|b| b[0])
    }
    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_le_bytes)
    }
    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_le_bytes)
    }
    fn i64(&mut self) -> Option<i64> {
        self.take().map(i64::from_le_bytes)
    }
}

/// Decode a cursor payload, or `None` for anything that is not exactly a
/// well-formed tag-2 cursor for a family with `signal_count` signals.
fn decode_cursor(data: &[u8], signal_count: usize) -> Option<CertifyCursor> {
    let mut r = CursorReader { data };
    if r.u8()? != CERTIFY_CURSOR_TAG {
        return None;
    }
    let target_version = r.u32()?;
    let activated_epoch = r.u64()?;
    let start_unix_ns = r.i64()?;
    let has_cert = r.u8()?;
    let cert_version = r.u32()?;
    let cert_ns = r.i64()?;
    let certificate = match has_cert {
        1 => Some((cert_version, cert_ns)),
        0 if cert_version == 0 && cert_ns == 0 => None,
        _ => return None,
    };
    let contradicted = match r.u8()? {
        0 => false,
        1 => true,
        _ => return None,
    };
    let has_finalizing = r.u8()?;
    let finalizing_ns = r.i64()?;
    let finalizing_unix_ns = match has_finalizing {
        1 => Some(finalizing_ns),
        0 if finalizing_ns == 0 => None,
        _ => return None,
    };
    let count = r.u32()? as usize;
    if r.data.len() != count.checked_mul(CURSOR_BUCKET_LEN)? {
        return None;
    }
    let mut buckets = BTreeMap::new();
    for _ in 0..count {
        let tenant = TenantHash(r.take::<16>()?);
        let signal_index = r.u8()?;
        if usize::from(signal_index) >= signal_count {
            return None;
        }
        let progress = BucketProgress {
            shards_done: r.u32()?,
            entries: r.u64()?,
            newest_created_unix_ns: r.i64()?,
        };
        if buckets.insert((tenant, signal_index), progress).is_some() {
            return None;
        }
    }
    Some(CertifyCursor {
        target_version,
        activated_epoch,
        start_unix_ns,
        prior: PriorState {
            certificate,
            contradicted,
        },
        finalizing_unix_ns,
        buckets,
    })
}

/// What the cursor key held.
enum StoredCursor {
    Absent,
    /// A payload this build does not read as a cursor; overwritten by CAS.
    Unrecognized(Version),
    Present(CertifyCursor, Version),
}

async fn read_certify_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
    signal_count: usize,
) -> Result<StoredCursor> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => Ok(match decode_cursor(got.data.as_ref(), signal_count) {
            Some(cursor) => StoredCursor::Present(cursor, got.version),
            None => StoredCursor::Unrecognized(got.version),
        }),
        Err(StoreError::NotFound) => Ok(StoredCursor::Absent),
        Err(err) => Err(MaintainError::Store(err)),
    }
}

/// Write the cursor: create it when `prev` is `None`, else CAS on `prev`.
/// `None` when another run created or replaced it first.
async fn put_certify_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
    cursor: &CertifyCursor,
    prev: Option<&Version>,
) -> Result<Option<Version>> {
    let opts = match prev {
        None => PutOptions::create_if_absent(),
        Some(version) => PutOptions {
            mode: PutMode::CasVersion(version.clone()),
            checksum: None,
        },
    };
    match store
        .put(key, Bytes::from(encode_cursor(cursor)), opts)
        .await
    {
        Ok(outcome) => Ok(Some(outcome.version)),
        Err(StoreError::AlreadyExists) | Err(StoreError::PreconditionFailed) => Ok(None),
        Err(err) => Err(MaintainError::Store(err)),
    }
}

/// The signals a class-A family's objects live under (ADR-4233 inventory).
pub(crate) fn family_signals(family: &str) -> Option<&'static [Signal]> {
    match family {
        FAMILY_RSEG => Some(&[Signal::Metrics]),
        FAMILY_RLOG => Some(&[Signal::Logs, Signal::Alerts, Signal::Audit]),
        FAMILY_RSPAN => Some(&[Signal::Spans]),
        _ => None,
    }
}

fn prov_err(err: ProvisioningError) -> MaintainError {
    MaintainError::Invariant(format!("provisioning: {err}"))
}

/// The shard count to scan for a `(tenant, signal)`: the widest the generation
/// history ever used, widened to cover every shard directory present under
/// `l0/`, `l1/` and `c/`, and floored at the signal's pinned read shards and at
/// one. The directories count because a bucket can hold data with no
/// provisioning record (adopted data, alerts, audit), and its generation
/// history then says nothing about how many shards it spans. Each listing is
/// strict: an unaddressable entry, or an entry that is not a shard directory,
/// refuses rather than narrowing the range.
pub(crate) async fn scan_shards_for(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> std::result::Result<u32, CensusStop> {
    let generations = read_generations_from_store(store, tenant, signal)
        .await
        .map_err(|e| CensusStop::Fatal(prov_err(e)))?;
    let widest = generations
        .map(|gens| gens.iter().map(|g| g.shard_count).max().unwrap_or(0))
        .unwrap_or(0);
    let mut shards = widest.max(signal.fixed_read_shards()).max(1);
    for dir in ["l0", keys::L1_DIR, "c"] {
        let prefix = format!("t/{}/{}/{dir}/", tenant.to_hex(), signal.key_prefix());
        let listing = store
            .list_delimited(&prefix)
            .await
            .map_err(|e| CensusStop::Fatal(MaintainError::Store(e)))?;
        let unaddressable = listing.unaddressable.len() + listing.unaddressable_prefixes.len();
        if unaddressable != 0 {
            return Err(CensusStop::Refused(CertifyRefusal::IncompleteListing {
                prefix,
                unaddressable: unaddressable as u64,
            }));
        }
        if let Some(object) = listing.objects.first() {
            return Err(CensusStop::Refused(CertifyRefusal::UnexpectedShardEntry {
                key: object.key.clone(),
            }));
        }
        for common in &listing.common_prefixes {
            let Some(index) = shard_index_of(common, &prefix) else {
                return Err(CensusStop::Refused(CertifyRefusal::UnexpectedShardEntry {
                    key: common.clone(),
                }));
            };
            shards = shards.max(index.saturating_add(1));
        }
    }
    Ok(shards)
}

/// The shard index of a common prefix `<listed>/<digits>/`, or `None` when the
/// segment is not a plain decimal shard number.
fn shard_index_of(common_prefix: &str, listed_prefix: &str) -> Option<u32> {
    let segment = common_prefix
        .strip_prefix(listed_prefix)?
        .strip_suffix('/')?;
    if segment.is_empty() || !segment.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    segment.parse().ok()
}

/// One data or part object the census checks. `stamp` is the version its
/// record stamps; `None` for an object found under `l0/` or `l1/` that no
/// record names (a held or tombstoned bucket's object, or a superseded input
/// whose record is gone), which is checked by its trailer alone and counts in
/// neither the member count nor the newest creation time: those are the floor
/// basis, which counts commit-family entries.
struct Member {
    key: String,
    stamp: Option<u32>,
    created_unix_ns: i64,
}

/// Every key under `prefix`, by STRICT listing: an unaddressable key refuses
/// the run rather than shortening the population (ADR-4233 decision 6).
async fn strict_list(
    store: &dyn ObjectStoreBackend,
    prefix: String,
) -> std::result::Result<Vec<String>, CensusStop> {
    let listing = list_all_reporting(store, &prefix)
        .await
        .map_err(|e| CensusStop::Fatal(MaintainError::Store(e)))?;
    if listing.unaddressable.count != 0 {
        return Err(CensusStop::Refused(CertifyRefusal::IncompleteListing {
            prefix,
            unaddressable: listing.unaddressable.count,
        }));
    }
    Ok(listing.objects.into_iter().map(|meta| meta.key).collect())
}

/// Enumerate the member objects of one `(tenant, signal, shard)`: every L0
/// commit record's data object and every compaction and rewrite record's
/// parts, by strict listing of the shard's commit prefix, then every object
/// under the shard's `l0/` and `l1/` prefixes that no record names.
async fn census_shard(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> std::result::Result<Vec<Member>, CensusStop> {
    let prefix = keys::commit_shard_prefix(tenant, signal, shard).map_err(CensusStop::fatal)?;
    let mut members = Vec::new();
    for key in strict_list(store, prefix).await? {
        let entry = keys::partition_bucket_entry(&key).map_err(CensusStop::fatal)?;
        if matches!(entry, keys::BucketEntry::Tombstone(_)) {
            continue;
        }
        let got = store
            .get(&key, GetRange::Full)
            .await
            .map_err(|e| CensusStop::Fatal(MaintainError::Store(e)))?;
        match entry {
            keys::BucketEntry::CommitRecord(_) => {
                let rec = record::decode(&got.data).map_err(CensusStop::fatal)?;
                members.push(Member {
                    key: keys::reconstruct_data_key(&rec).map_err(CensusStop::fatal)?,
                    stamp: Some(rec.segment_format_version),
                    created_unix_ns: rec.created_unix_ns,
                });
            }
            keys::BucketEntry::CompactionRecord(_) => {
                let rec = record::decode_compaction(&got.data).map_err(CensusStop::fatal)?;
                for part in &rec.parts {
                    members.push(Member {
                        key: keys::reconstruct_l1_part_key(&rec, part)
                            .map_err(CensusStop::fatal)?,
                        stamp: Some(part.segment_format_version),
                        created_unix_ns: rec.created_unix_ns,
                    });
                }
            }
            keys::BucketEntry::RewriteRecord(_) => {
                let rec = sysproto_rewrite_decode(&got.data).map_err(CensusStop::fatal)?;
                for part in &rec.parts {
                    members.push(Member {
                        key: keys::reconstruct_rewrite_part_key(&rec, part)
                            .map_err(CensusStop::fatal)?,
                        stamp: Some(part.segment_format_version),
                        created_unix_ns: rec.created_unix_ns,
                    });
                }
            }
            keys::BucketEntry::Tombstone(_) => {}
        }
    }

    let recorded: std::collections::HashSet<String> =
        members.iter().map(|m| m.key.clone()).collect();
    for dir in ["l0", keys::L1_DIR] {
        let prefix = format!(
            "t/{}/{}/{dir}/{shard:04}/",
            tenant.to_hex(),
            signal.key_prefix()
        );
        for key in strict_list(store, prefix).await? {
            if !recorded.contains(&key) {
                members.push(Member {
                    key,
                    stamp: None,
                    created_unix_ns: 0,
                });
            }
        }
    }
    Ok(members)
}

fn sysproto_rewrite_decode(
    bytes: &[u8],
) -> std::result::Result<ravel_proto::commit::v1::RewriteRecord, MaintainError> {
    use prost::Message;
    ravel_proto::commit::v1::RewriteRecord::decode(bytes)
        .map_err(|e| MaintainError::Invariant(format!("rewrite record decode: {e}")))
}

/// A reason the census stopped before covering a shard.
pub(crate) enum CensusStop {
    Fatal(MaintainError),
    Refused(CertifyRefusal),
}

impl CensusStop {
    fn fatal(err: impl Into<MaintainError>) -> Self {
        CensusStop::Fatal(err.into())
    }
}

/// Check one member against `target`: its stamp, then its bytes through
/// [`probe_member`]. `None` when it passes.
async fn check_member(
    store: &dyn ObjectStoreBackend,
    family: &str,
    member: Member,
    target: u32,
) -> Result<Option<CertifyReport>> {
    if let Some(stamp) = member.stamp
        && stamp < target
    {
        return Ok(Some(CertifyReport::Contradicted {
            member_key: member.key,
            found_version: stamp,
        }));
    }
    let report = match probe_member(store, family, &member.key).await? {
        MemberProbe::Absent => CertifyReport::Refused(CertifyRefusal::MemberAbsent {
            member_key: member.key,
        }),
        MemberProbe::Corrupt => CertifyReport::Refused(CertifyRefusal::MemberCorrupt {
            member_key: member.key,
        }),
        MemberProbe::OutOfWindow(version) => CertifyReport::HeldMember {
            member_key: member.key,
            version,
        },
        MemberProbe::Readable {
            trailer,
            section_hold,
        } => {
            if let Some(stamp) = member.stamp
                && stamp != u32::from(trailer)
            {
                CertifyReport::Refused(CertifyRefusal::StampTrailerMismatch {
                    member_key: member.key,
                    stamp,
                    trailer,
                })
            } else if u32::from(trailer) < target {
                CertifyReport::Contradicted {
                    member_key: member.key,
                    found_version: u32::from(trailer),
                }
            } else if let Some(version) = section_hold {
                CertifyReport::HeldMember {
                    member_key: member.key,
                    version,
                }
            } else {
                return Ok(None);
            }
        }
    };
    Ok(Some(report))
}

fn compat_err(err: CompatError) -> MaintainError {
    MaintainError::Invariant(format!("sys/compat: {err}"))
}

/// `timings` with `max_flush_lifetime_ns` raised to the value `sys/gc`
/// records, when that is larger: `G_pre` must cover the longest flush any
/// writer was configured for, not only this process's configuration.
async fn effective_timings(
    store: &dyn ObjectStoreBackend,
    timings: CertifyTimings,
) -> Result<CertifyTimings> {
    let recorded = crate::gc_config::read_gc_config(store)
        .await
        .map_err(|e| MaintainError::Invariant(format!("sys/gc: {e}")))?;
    let mut effective = timings;
    if let Some((values, _version)) = recorded {
        effective.max_flush_lifetime_ns = effective
            .max_flush_lifetime_ns
            .max(values.max_flush_lifetime_ns);
    }
    Ok(effective)
}

/// Certify `target_version` for class-A `family` (ADR-4233 decision 7).
///
/// Refuses before the family's activation fence walk is complete, when
/// `sys/compat` records no `P`, and before the time precondition `S` is gated
/// on has passed. The first run records `S` (now) in the cursor; a resumed run
/// keeps it, so the late-write pass examines records that landed after it. The
/// census is a strict listing of every member object of the family: each
/// recorded member's stamp is compared with the target and its trailer version
/// is read and required to equal the stamp, and each unrecorded object under
/// `l0/` or `l1/` is checked by its trailer. A member below the target makes
/// the result contradicted, an unreadable member holds it, and an absent
/// member, a corrupt trailer or a stamp the trailer disagrees with refuses it.
///
/// Before the CAS every provisioned bucket of every tenant is rechecked: its
/// `compat_epoch` must be at or above the family's `activated_epoch` and its
/// own `G_pre` must have passed. The certificate CAS sets the watermark to `S`
/// and clears `contradicted`, and refuses when the certificate or
/// `contradicted` changed since the census read them. The format floors are
/// raised with the basis of every bucket the census covered, across runs.
///
/// `budget` caps the `(tenant, signal, shard)` units one run covers; `None` is
/// no cap. A run stopped by its budget returns [`CertifyReport::Incomplete`]
/// and leaves the cursor for the next run.
pub async fn certify(
    store: &dyn ObjectStoreBackend,
    keys_compat: &CompatKeys,
    now_ns: i64,
    family: &str,
    target_version: u32,
    timings: CertifyTimings,
    budget: Option<usize>,
) -> Result<CertifyReport> {
    let Some(signals) = family_signals(family) else {
        return Ok(CertifyReport::Refused(CertifyRefusal::NotClassAFamily {
            family: family.to_string(),
        }));
    };

    let Some((record, _version)) = read_compat(store, keys_compat).await.map_err(compat_err)?
    else {
        return Ok(CertifyReport::Refused(CertifyRefusal::CompatRecordAbsent));
    };
    let Some(entry) = record.families.iter().find(|f| f.family == family) else {
        return Ok(CertifyReport::Refused(CertifyRefusal::FamilyNotInRecord {
            family: family.to_string(),
        }));
    };

    // A certification whose CAS succeeded but whose floor raises did not all
    // finish left its cursor marked with the certificate it wrote: finish
    // the raises first. A contradicted certificate gets none, so `migrate`
    // still rewrites the bucket holding the late write.
    let cursor_key = certify_cursor_key(family);
    let mut stored = read_certify_cursor(store, &cursor_key, signals.len()).await?;
    if let StoredCursor::Present(pending, _) = &stored
        && let Some(certificate) = entry.certificate.as_ref()
        && pending.finalizing_unix_ns == Some(certificate.certified_unix_ns)
        && pending.target_version == certificate.version
    {
        if !entry.contradicted {
            let bases = match floor_bases(store, signals, pending).await? {
                Ok(bases) => bases,
                Err(refusal) => return Ok(CertifyReport::Refused(refusal)),
            };
            raise_floors(store, family, certificate.version, &bases, now_ns).await?;
        }
        delete_cursor(store, &cursor_key).await;
        if !entry.contradicted && certificate.version == target_version {
            return Ok(CertifyReport::Certified {
                version: certificate.version,
                member_count: certificate.member_count,
                buckets_scanned: certificate.buckets_scanned,
                newest_created_unix_ns: certificate.newest_created_unix_ns,
            });
        }
        stored = read_certify_cursor(store, &cursor_key, signals.len()).await?;
    }

    if target_version > entry.active_write_version {
        return Ok(CertifyReport::Refused(CertifyRefusal::TargetAboveActive {
            target: target_version,
            active: entry.active_write_version,
        }));
    }
    if let Some(refusal) = replacement_refusal(entry, target_version) {
        return Ok(CertifyReport::Refused(refusal));
    }
    if entry.fence_complete_unix_ns < entry.activated_unix_ns {
        return Ok(CertifyReport::Refused(CertifyRefusal::FenceWalkIncomplete));
    }
    let p_ns = record.max_publish_lifetime_ns;
    if p_ns <= 0 {
        return Ok(CertifyReport::Refused(
            CertifyRefusal::PublishLifetimeAbsent,
        ));
    }
    let activated_epoch = entry.activated_epoch;
    let prior = PriorState::of(entry);

    let timings = effective_timings(store, timings).await?;
    let activation_term = entry
        .activated_unix_ns
        .saturating_add(H_NS)
        .saturating_add(p_ns)
        .saturating_add(timings.t_req_ns);
    let g_pre = timings.g_pre_ns(p_ns);

    // The tenant set, fixed before the census so the digest, the census and
    // the recheck use the same population.
    let mut tenants = discover_tenants(store).await?;
    tenants.sort();

    // The newest non-born fence over every provisioned bucket: the second term
    // of the time precondition (a born-fenced bucket, time 0, is left out).
    let mut newest_fence = 0i64;
    for tenant in &tenants {
        for signal in signals {
            if let Some(fence) = provisioning_fence(store, tenant, *signal).await?
                && fence.fenced_unix_ns != 0
            {
                newest_fence = newest_fence.max(fence.fenced_unix_ns);
            }
        }
    }
    let not_before = activation_term
        .max(if newest_fence == 0 {
            i64::MIN
        } else {
            newest_fence.saturating_add(g_pre)
        })
        .saturating_add(timings.clock_skew_allowance_ns);
    if now_ns <= not_before {
        return Ok(CertifyReport::Refused(CertifyRefusal::TimeGate {
            now_ns,
            not_before_ns: not_before,
        }));
    }

    // `S` and the progress: kept from a cursor taken for this target, epoch and
    // certificate state; a fresh census, with `S` now, otherwise.
    let fresh = CertifyCursor {
        target_version,
        activated_epoch,
        start_unix_ns: now_ns,
        prior,
        finalizing_unix_ns: None,
        buckets: BTreeMap::new(),
    };
    let (mut cursor, prev) = match stored {
        StoredCursor::Present(cursor, version)
            if cursor.target_version == target_version
                && cursor.activated_epoch == activated_epoch
                && cursor.prior == prior =>
        {
            (cursor, Some(version))
        }
        StoredCursor::Present(_, version) | StoredCursor::Unrecognized(version) => {
            let Some(version) =
                put_certify_cursor(store, &cursor_key, &fresh, Some(&version)).await?
            else {
                return Ok(CertifyReport::Refused(CertifyRefusal::ConcurrentCertify));
            };
            (fresh, Some(version))
        }
        StoredCursor::Absent => {
            let Some(version) = put_certify_cursor(store, &cursor_key, &fresh, None).await? else {
                return Ok(CertifyReport::Refused(CertifyRefusal::ConcurrentCertify));
            };
            (fresh, Some(version))
        }
    };
    let mut cursor_version = prev;

    let mut processed_this_run = 0usize;
    for tenant in &tenants {
        for (signal_index, signal) in signals.iter().enumerate() {
            let bucket_key = (*tenant, signal_index as u8);
            let shards = match scan_shards_for(store, tenant, *signal).await {
                Ok(shards) => shards,
                Err(CensusStop::Fatal(err)) => return Err(err),
                Err(CensusStop::Refused(refusal)) => {
                    return Ok(CertifyReport::Refused(refusal));
                }
            };
            let done = cursor
                .buckets
                .get(&bucket_key)
                .map_or(0, |progress| progress.shards_done);
            for shard in done..shards {
                if let Some(cap) = budget
                    && processed_this_run >= cap
                {
                    if put_certify_cursor(store, &cursor_key, &cursor, cursor_version.as_ref())
                        .await?
                        .is_none()
                    {
                        return Ok(CertifyReport::Refused(CertifyRefusal::ConcurrentCertify));
                    }
                    return Ok(CertifyReport::Incomplete {
                        units_examined: processed_this_run,
                    });
                }

                // Fence the provisioned bucket to the family's activation
                // epoch, as the writers do, so a bucket provisioned at v3 after
                // the eager walk is caught by the recheck below.
                fence_bucket(store, tenant, *signal, activated_epoch, now_ns).await?;

                let members = match census_shard(store, tenant, *signal, shard).await {
                    Ok(members) => members,
                    Err(CensusStop::Fatal(err)) => return Err(err),
                    Err(CensusStop::Refused(refusal)) => {
                        return Ok(CertifyReport::Refused(refusal));
                    }
                };
                let mut entries = 0u64;
                let mut newest = 0i64;
                for member in members {
                    if member.stamp.is_some() {
                        entries += 1;
                        newest = newest.max(member.created_unix_ns);
                    }
                    if let Some(report) =
                        check_member(store, family, member, target_version).await?
                    {
                        return Ok(report);
                    }
                }
                let progress = cursor.buckets.entry(bucket_key).or_default();
                progress.shards_done = shard.saturating_add(1);
                progress.entries = progress.entries.saturating_add(entries);
                progress.newest_created_unix_ns = progress.newest_created_unix_ns.max(newest);
                processed_this_run += 1;
            }
        }
    }
    if processed_this_run > 0 {
        // Keep the finished census, so a recheck refusal below does not cost
        // the next run a rescan.
        cursor_version =
            put_certify_cursor(store, &cursor_key, &cursor, cursor_version.as_ref()).await?;
        if cursor_version.is_none() {
            return Ok(CertifyReport::Refused(CertifyRefusal::ConcurrentCertify));
        }
    }

    // The recheck immediately before the CAS, over every provisioned bucket of
    // every tenant whichever run censused it: the tenant set is the one the
    // census covered, every bucket is fenced at or above the activation epoch,
    // and every bucket's own G_pre has passed.
    let mut tenants_now = discover_tenants(store).await?;
    tenants_now.sort();
    if tenants_now != tenants {
        return Ok(CertifyReport::Refused(CertifyRefusal::TenantSetChanged));
    }
    for tenant in &tenants {
        for signal in signals {
            let Some(fence) = provisioning_fence(store, tenant, *signal).await? else {
                continue;
            };
            if fence.compat_epoch < activated_epoch {
                return Ok(CertifyReport::Refused(CertifyRefusal::BucketUnfenced {
                    tenant_hash: tenant.to_hex(),
                    signal: *signal,
                    compat_epoch: fence.compat_epoch,
                    activated_epoch,
                }));
            }
            let not_before_ns = fence
                .fenced_unix_ns
                .saturating_add(g_pre)
                .saturating_add(timings.clock_skew_allowance_ns);
            if fence.fenced_unix_ns != 0 && now_ns <= not_before_ns {
                return Ok(CertifyReport::Refused(
                    CertifyRefusal::BucketRecentlyFenced {
                        tenant_hash: tenant.to_hex(),
                        signal: *signal,
                        not_before_ns,
                    },
                ));
            }
        }
    }

    // The totals over every bucket the census covered, in this run or earlier
    // ones, restricted to the tenant set the digest names.
    cursor
        .buckets
        .retain(|(tenant, _), _| tenants.binary_search(tenant).is_ok());
    let mut member_count = 0u64;
    let mut buckets_scanned = 0u64;
    let mut newest_created = 0i64;
    for progress in cursor.buckets.values() {
        member_count = member_count.saturating_add(progress.entries);
        buckets_scanned = buckets_scanned.saturating_add(u64::from(progress.shards_done));
        newest_created = newest_created.max(progress.newest_created_unix_ns);
    }
    let bases = match floor_bases(store, signals, &cursor).await? {
        Ok(bases) => bases,
        Err(refusal) => return Ok(CertifyReport::Refused(refusal)),
    };

    // The tenant-set digest: blake3 over the sorted tenant hashes.
    let mut hasher = blake3::Hasher::new();
    for tenant in &tenants {
        hasher.update(&tenant.0);
    }
    let digest = hasher.finalize().as_bytes().to_vec();

    // The certificate, with the watermark reset to `S` and `contradicted`
    // cleared in the same CAS. A late-write pass that advanced the watermark
    // past `S` checked records against the previous certificate, so the new
    // certificate's pass starts again at `S`.
    let certificate = sysproto::Certificate {
        version: target_version,
        certified_unix_ns: now_ns,
        member_count,
        newest_created_unix_ns: newest_created,
        buckets_scanned,
        tenant_set_digest: digest,
    };
    let start_unix_ns = cursor.start_unix_ns;

    // Mark the cursor with the certificate the CAS is about to write, so a
    // run that stops between the CAS and the last floor raise is completed by
    // the next one.
    cursor.finalizing_unix_ns = Some(now_ns);
    if put_certify_cursor(store, &cursor_key, &cursor, cursor_version.as_ref())
        .await?
        .is_none()
    {
        return Ok(CertifyReport::Refused(CertifyRefusal::ConcurrentCertify));
    }

    let mut refusal = None;
    let written = update_compat(store, keys_compat, |record| {
        let entry = record
            .families
            .iter_mut()
            .find(|f| f.family == family)
            .ok_or_else(|| CompatError::FamilyNotInRecord {
                family: family.to_string(),
            })?;
        let conflict = || CompatError::CasConflict {
            key: COMPAT_KEY.to_string(),
        };
        if let Some(found) = replacement_refusal(entry, target_version) {
            refusal = Some(found);
            return Err(conflict());
        }
        if PriorState::of(entry) != prior {
            refusal = Some(CertifyRefusal::CompatChangedDuringCensus);
            return Err(conflict());
        }
        entry.certificate = Some(certificate);
        entry.contradicted = false;
        entry.late_writes_verified_through_unix_ns = start_unix_ns;
        Ok(())
    })
    .await;
    match written {
        Ok(_) => {}
        Err(CompatError::CasConflict { .. }) => {
            return Ok(CertifyReport::Refused(
                refusal.unwrap_or(CertifyRefusal::CompatRecordRaced),
            ));
        }
        Err(err) => return Err(compat_err(err)),
    }

    // Only now that the certificate is written: a floor raised for a census
    // the CAS then refused would make `migrate` skip a bucket that may hold
    // the late write the refusal is about.
    raise_floors(store, family, target_version, &bases, now_ns).await?;

    // A cursor this delete leaves behind is never resumed as a census: its
    // marker matches the certificate, so the next run repeats the floor
    // raises (each already at the target is a no-op) and deletes it.
    delete_cursor(store, &cursor_key).await;

    Ok(CertifyReport::Certified {
        version: target_version,
        member_count,
        buckets_scanned,
        newest_created_unix_ns: newest_created,
    })
}

/// Why the family's existing certificate may not be replaced by one at
/// `target`: one above the target is never replaced by a lower one, and one
/// at the target only once a late write set `contradicted` (ADR-4233
/// decision 7). `None` when the replacement is allowed.
fn replacement_refusal(entry: &sysproto::CompatFamily, target: u32) -> Option<CertifyRefusal> {
    let certified = entry.certificate.as_ref()?.version;
    if certified > target {
        Some(CertifyRefusal::TargetBelowCertificate { target, certified })
    } else if certified == target && !entry.contradicted {
        Some(CertifyRefusal::AlreadyCertified { version: target })
    } else {
        None
    }
}

/// The floor basis of every bucket in `cursor`, with the shard range each
/// covers now.
async fn floor_bases(
    store: &dyn ObjectStoreBackend,
    signals: &[Signal],
    cursor: &CertifyCursor,
) -> Result<std::result::Result<Vec<(TenantHash, Signal, FloorBasis)>, CertifyRefusal>> {
    let mut bases = Vec::with_capacity(cursor.buckets.len());
    for ((tenant, signal_index), progress) in &cursor.buckets {
        let Some(signal) = signals.get(usize::from(*signal_index)).copied() else {
            continue;
        };
        let observed_shards = match scan_shards_for(store, tenant, signal).await {
            Ok(shards) => shards,
            Err(CensusStop::Fatal(err)) => return Err(err),
            Err(CensusStop::Refused(refusal)) => return Ok(Err(refusal)),
        };
        bases.push((
            *tenant,
            signal,
            FloorBasis {
                observed_entries: progress.entries,
                observed_newest_created_unix_ns: progress.newest_created_unix_ns,
                observed_shards,
            },
        ));
    }
    Ok(Ok(bases))
}

/// Raise each bucket's `family` floor to `version` with its basis.
async fn raise_floors(
    store: &dyn ObjectStoreBackend,
    family: &str,
    version: u32,
    bases: &[(TenantHash, Signal, FloorBasis)],
    now_ns: i64,
) -> Result<()> {
    for (tenant, signal, basis) in bases {
        match raise_format_floor(
            store,
            tenant,
            *signal,
            family,
            version,
            *basis,
            "compat-certify",
            now_ns,
        )
        .await
        {
            Ok(_) => {}
            // Already at or above the version, or no record to floor: neither
            // blocks a certificate whose census the bucket passed.
            Err(ProvisioningError::FloorNotAboveCurrent { .. })
            | Err(ProvisioningError::NoRecordForFloor { .. }) => {}
            Err(err) => return Err(prov_err(err)),
        }
    }
    Ok(())
}

/// Delete the certify cursor. A failure is logged, not returned: every caller
/// has already written what the cursor tracked, and a cursor left behind is
/// either completed or found stale by the next run.
async fn delete_cursor(store: &dyn ObjectStoreBackend, cursor_key: &str) {
    match store.delete(cursor_key).await {
        Ok(()) | Err(StoreError::NotFound) => {}
        Err(err) => tracing::warn!(
            key = %cursor_key,
            error = %err,
            "compat certify: could not delete the completed cursor"
        ),
    }
}

/// The fence state of one provisioned bucket.
#[derive(Debug, Clone, Copy)]
struct BucketFence {
    compat_epoch: u64,
    fenced_unix_ns: i64,
}

/// The fence state of a `(tenant, signal)` provisioning record, or `None`
/// when the record is absent.
async fn provisioning_fence(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<BucketFence>> {
    use prost::Message;
    let key = provisioning_key(tenant, signal);
    match store.get(&key, GetRange::Full).await {
        Ok(got) => {
            let record = sysproto::ProvisioningRecord::decode(got.data.as_ref())
                .map_err(|e| MaintainError::Invariant(format!("{key}: {e}")))?;
            Ok(Some(BucketFence {
                compat_epoch: record.compat_epoch,
                fenced_unix_ns: record.compat_fenced_unix_ns,
            }))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(MaintainError::Store(err)),
    }
}

/// Fence a provisioned bucket to `epoch`; a bucket with no record is left
/// alone (audit and alerts are never provisioned).
async fn fence_bucket(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    epoch: u64,
    now_ns: i64,
) -> Result<()> {
    match raise_compat_fence(store, tenant, signal, epoch, now_ns).await {
        Ok(CompatFenceOutcome::Raised { .. }) | Ok(CompatFenceOutcome::AlreadyFenced { .. }) => {
            Ok(())
        }
        Err(ProvisioningError::NoRecordToFence { .. }) => Ok(()),
        Err(err) => Err(prov_err(err)),
    }
}

/// Suffix size probed to locate a footer, matching the scrub and compaction
/// readers' first GET. A footer larger than this falls back to a second ranged
/// GET.
const FOOTER_PROBE_BYTES: u64 = 64 * 1024;
/// The RLOG and RSPAN trailer length; the version sits at bytes 8..10 of it.
const TRAILER_BYTES: usize = 16;

const _: () = assert!(
    ravel_logseg::footer::TRAILER_LEN == TRAILER_BYTES
        && ravel_rspan::footer::TRAILER_LEN == TRAILER_BYTES
);

/// What probing one member object's bytes found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberProbe {
    /// The object is absent.
    Absent,
    /// The trailer or footer is structurally corrupt.
    Corrupt,
    /// The trailer version is outside this build's reader window.
    OutOfWindow(u16),
    /// The trailer version is in window. `section_hold` is the grammar version
    /// of a nested section outside this build's read set, if any.
    Readable {
        trailer: u16,
        section_hold: Option<u16>,
    },
}

impl MemberProbe {
    /// The version a retention hold keeps the object on: an out-of-window
    /// trailer or an unreadable section. Absent and corrupt are not holds.
    pub(crate) fn held_version(self) -> Option<u16> {
        match self {
            MemberProbe::OutOfWindow(version) => Some(version),
            MemberProbe::Readable { section_hold, .. } => section_hold,
            MemberProbe::Absent | MemberProbe::Corrupt => None,
        }
    }
}

/// Probe one object of class-A `family` (RSEG, RLOG or RSPAN); see the module
/// doc for the requests each family costs.
pub(crate) async fn probe_member(
    store: &dyn ObjectStoreBackend,
    family: &str,
    key: &str,
) -> Result<MemberProbe> {
    match family {
        FAMILY_RSEG => probe_rseg(store, key).await,
        FAMILY_RLOG => probe_rlog(store, key).await,
        FAMILY_RSPAN => probe_rspan(store, key).await,
        other => Err(MaintainError::Invariant(format!(
            "member probe: {other} is not a class-A family"
        ))),
    }
}

async fn probe_rseg(store: &dyn ObjectStoreBackend, key: &str) -> Result<MemberProbe> {
    let got = match store
        .get(key, GetRange::Suffix(ravel_segment::TRAILER_LEN))
        .await
    {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(MemberProbe::Absent),
        Err(err) => return Err(MaintainError::Store(err)),
    };
    Ok(
        match ravel_segment::classify_trailer(got.total_size, got.data.as_ref()) {
            ravel_segment::TrailerClass::Readable(version) => MemberProbe::Readable {
                trailer: version.number(),
                section_hold: None,
            },
            ravel_segment::TrailerClass::OutsideVersionWindow(version) => {
                MemberProbe::OutOfWindow(version)
            }
            ravel_segment::TrailerClass::Corrupt(_) => MemberProbe::Corrupt,
        },
    )
}

/// A ranged GET, with an absent object reported as `None`.
async fn get_range(
    store: &dyn ObjectStoreBackend,
    key: &str,
    start: u64,
    end: u64,
) -> Result<Option<Bytes>> {
    match store.get(key, GetRange::Range(start, end)).await {
        Ok(got) => Ok(Some(got.data)),
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(MaintainError::Store(err)),
    }
}

/// One step of a format's suffix open.
enum SuffixStep<F> {
    Ready(F),
    NeedRange { offset: u64, len: u64 },
    OutOfWindow(u16),
    Corrupt,
}

fn rlog_step(suffix: &[u8], total: u64) -> SuffixStep<LogFooter> {
    match ravel_logseg::open_from_suffix(suffix, total) {
        Ok(ravel_logseg::footer::SuffixOutcome::Ready(footer)) => SuffixStep::Ready(footer),
        Ok(ravel_logseg::footer::SuffixOutcome::NeedRange { offset, len }) => {
            SuffixStep::NeedRange { offset, len }
        }
        Err(LogSegError::UnsupportedVersion(version)) => SuffixStep::OutOfWindow(version),
        Err(_) => SuffixStep::Corrupt,
    }
}

fn rspan_step(suffix: &[u8], total: u64) -> SuffixStep<SpanFooter> {
    match ravel_rspan::open_from_suffix(suffix, total) {
        Ok(ravel_rspan::footer::SuffixOutcome::Ready(footer)) => SuffixStep::Ready(footer),
        Ok(ravel_rspan::footer::SuffixOutcome::NeedRange { offset, len }) => {
            SuffixStep::NeedRange { offset, len }
        }
        Err(ravel_rspan::SpanSegError::UnsupportedVersion(version)) => {
            SuffixStep::OutOfWindow(version)
        }
        Err(_) => SuffixStep::Corrupt,
    }
}

/// What a footer read found.
enum FooterRead<F> {
    Absent,
    Corrupt,
    OutOfWindow(u16),
    Ready { footer: Box<F>, trailer: u16 },
}

/// The trailer version of a suffix that covers the trailer.
fn trailer_version_of(tail: &[u8]) -> Option<u16> {
    let at = tail.len().checked_sub(TRAILER_BYTES)?;
    let bytes = tail.get(at + 8..at + 10)?;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

/// Read a footer through the suffix-probe-then-range-chase protocol: ONE
/// suffix GET, and a second ranged GET only when the footer did not fit. The
/// suffix open checks the trailer version before the footer bounds, so an
/// out-of-window trailer comes back from the first read.
async fn read_footer<F>(
    store: &dyn ObjectStoreBackend,
    key: &str,
    step: fn(&[u8], u64) -> SuffixStep<F>,
) -> Result<FooterRead<F>> {
    let probe = match store.get(key, GetRange::Suffix(FOOTER_PROBE_BYTES)).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(FooterRead::Absent),
        Err(err) => return Err(MaintainError::Store(err)),
    };
    let total = probe.total_size;
    let (footer, tail) = match step(probe.data.as_ref(), total) {
        SuffixStep::Ready(footer) => (footer, probe.data),
        SuffixStep::NeedRange { offset, len } => {
            let Some(tail) = get_range(store, key, offset, offset.saturating_add(len)).await?
            else {
                return Ok(FooterRead::Absent);
            };
            match step(tail.as_ref(), total) {
                SuffixStep::Ready(footer) => (footer, tail),
                SuffixStep::OutOfWindow(version) => return Ok(FooterRead::OutOfWindow(version)),
                SuffixStep::NeedRange { .. } | SuffixStep::Corrupt => {
                    return Ok(FooterRead::Corrupt);
                }
            }
        }
        SuffixStep::OutOfWindow(version) => return Ok(FooterRead::OutOfWindow(version)),
        SuffixStep::Corrupt => return Ok(FooterRead::Corrupt),
    };
    Ok(match trailer_version_of(tail.as_ref()) {
        Some(trailer) => FooterRead::Ready {
            footer: Box::new(footer),
            trailer,
        },
        None => FooterRead::Corrupt,
    })
}

/// What a nested-section probe found.
enum SectionProbe {
    Clear,
    Held(u16),
    /// The object was deleted between the footer read and the section read.
    Absent,
}

async fn probe_rlog(store: &dyn ObjectStoreBackend, key: &str) -> Result<MemberProbe> {
    let (footer, trailer) = match read_footer(store, key, rlog_step).await? {
        FooterRead::Absent => return Ok(MemberProbe::Absent),
        FooterRead::Corrupt => return Ok(MemberProbe::Corrupt),
        FooterRead::OutOfWindow(version) => return Ok(MemberProbe::OutOfWindow(version)),
        FooterRead::Ready { footer, trailer } => (footer, trailer),
    };
    Ok(match rlog_sections(store, key, &footer).await? {
        SectionProbe::Clear => MemberProbe::Readable {
            trailer,
            section_hold: None,
        },
        SectionProbe::Held(version) => MemberProbe::Readable {
            trailer,
            section_hold: Some(version),
        },
        SectionProbe::Absent => MemberProbe::Absent,
    })
}

/// The POSTINGS and KEY_IDX grammar versions of a `.rlog` object.
async fn rlog_sections(
    store: &dyn ObjectStoreBackend,
    key: &str,
    footer: &LogFooter,
) -> Result<SectionProbe> {
    // POSTINGS: the section is stored raw at the section-comp level, so its
    // first byte is the grammar version the reader gates on.
    if let Some(desc) = footer.section(kind::POSTINGS) {
        let Some(head) = get_range(store, key, desc.offset, desc.offset + 1).await? else {
            return Ok(SectionProbe::Absent);
        };
        if let Some(&version) = head.first()
            && version != POSTINGS_VERSION
            && version != POSTINGS_VERSION_V1
        {
            return Ok(SectionProbe::Held(u16::from(version)));
        }
    }

    // KEY_IDX: read the section prefix, then its header version through the
    // same path the reader uses. A version outside the set is a typed
    // `UnsupportedSectionVersion`; a corrupt prefix is not a hold.
    if let Ok(Some(range)) = key_index::key_probe_prefix_range(footer) {
        let Some(prefix) = get_range(store, key, range.start, range.end).await? else {
            return Ok(SectionProbe::Absent);
        };
        if let Err(LogSegError::UnsupportedSectionVersion { version, .. }) =
            key_index::key_index_version(footer, |_requested: Range<u64>| {
                Ok::<Bytes, LogSegError>(prefix.clone())
            })
        {
            return Ok(SectionProbe::Held(version));
        }
    }
    Ok(SectionProbe::Clear)
}

async fn probe_rspan(store: &dyn ObjectStoreBackend, key: &str) -> Result<MemberProbe> {
    let (footer, trailer) = match read_footer(store, key, rspan_step).await? {
        FooterRead::Absent => return Ok(MemberProbe::Absent),
        FooterRead::Corrupt => return Ok(MemberProbe::Corrupt),
        FooterRead::OutOfWindow(version) => return Ok(MemberProbe::OutOfWindow(version)),
        FooterRead::Ready { footer, trailer } => (footer, trailer),
    };
    // KEY_IDX is stored raw and its first byte is the grammar version the
    // reader checks against the codec's read set when it opens the section
    // (`KeyIdxPrefix::parse`), the same set RLOG's KEY_IDX uses.
    let mut section_hold = None;
    if let Some(desc) = footer.section(ravel_rspan::footer::kind::KEY_IDX) {
        let Some(head) = get_range(store, key, desc.offset, desc.offset + 1).await? else {
            return Ok(MemberProbe::Absent);
        };
        if let Some(&version) = head.first()
            && !key_index::KEY_IDX_VERSIONS.contains(&version)
        {
            section_hold = Some(u16::from(version));
        }
    }
    Ok(MemberProbe::Readable {
        trailer,
        section_hold,
    })
}

/// The version an RLOG object is held on: its trailer version when outside this
/// build's reader window, else the grammar version of a nested POSTINGS or
/// KEY_IDX section outside this build's read set (ADR-4233 H5). `None` when the
/// object is readable here, absent, or structurally corrupt (corrupt is never a
/// version hold). Up to four GETs; see the module doc.
pub(crate) async fn rlog_held_version(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<u16>> {
    Ok(probe_member(store, FAMILY_RLOG, key).await?.held_version())
}

/// The version an RSPAN object is held on: its trailer version when outside
/// this build's reader window, else the grammar version of its KEY_IDX section
/// when outside the read set (ADR-4233 H5). `None` when the object is readable
/// here, absent, or structurally corrupt. Up to three GETs; see the module doc.
pub(crate) async fn rspan_held_version(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<u16>> {
    Ok(probe_member(store, FAMILY_RSPAN, key).await?.held_version())
}

/// A one-record `.rlog` object with a POSTINGS section (an indexed field) and
/// a KEY_IDX section (a key field). When `bump` is set, the nested POSTINGS
/// grammar version byte is raised one above the read set while the trailer
/// version stays in window: an H5 object. Shared by this crate's retention
/// tests, which need the same fixture against the three RLOG retention paths.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) fn rlog_object_with_sections(bump: bool) -> Vec<u8> {
    use ravel_logseg::{LogRecord, ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_types::logstream::{AttrValue, log_stream_id};

    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc0".to_string()),
    )];
    let record = LogRecord {
        stream_id: log_stream_id(&res, "scope", "1", &[]),
        stream_attrs: ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]),
        ts_ns: 1_000,
        observed_ts_ns: 1_000,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "alpha".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("service.name".to_string(), AttrValue::Str("svc0".into()))],
    };
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: [7; 16],
            shard: 0,
            writer_id: [1; 16],
            writer_epoch: 1,
            writer_seq: 1,
        },
    )
    .with_indexed_fields(vec!["service.name".to_string()])
    .with_key_index_fields(vec![(
        "service.name".to_string(),
        ravel_logseg::key_index::KeyType::Str,
    )]);
    writer.push(record).expect("push log record");
    let mut bytes = writer.finish().expect("finish rlog");
    if bump {
        let at = ravel_logseg::footer::open(&bytes)
            .expect("open")
            .section(kind::POSTINGS)
            .expect("postings section")
            .offset as usize;
        bytes[at] = POSTINGS_VERSION + 1;
    }
    bytes
}

/// A one-span `.rspan` object. When `bump` is set, its KEY_IDX section's
/// version byte is raised one above the read set while the trailer version
/// stays in window: an H5 object.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) fn rspan_object_with_key_idx(bump: bool) -> Vec<u8> {
    use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};

    let mut writer = RspanWriter::new(
        RspanConfig::default(),
        ObjectIdentity {
            tenant_hash: [7; 16],
            shard: 0,
            writer_id: [1; 16],
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    writer.push(SpanRecord {
        trace_id: [1; 16],
        span_id: [1; 8],
        parent_span_id: None,
        name: "op".into(),
        start_ts_ns: 1,
        end_ts_ns: 2,
        status_code: StatusCode::Ok,
        status_message: None,
        attrs: vec![("service".to_string(), "checkout".to_string())],
    });
    let mut bytes = writer.finish().expect("finish rspan");
    if bump {
        let at = ravel_rspan::footer::open(&bytes)
            .expect("open")
            .section(ravel_rspan::footer::kind::KEY_IDX)
            .expect("key_idx section")
            .offset as usize;
        let above = key_index::KEY_IDX_VERSIONS
            .iter()
            .max()
            .copied()
            .expect("non-empty read set");
        bytes[at] = above + 1;
    }
    bytes
}

/// A one-series `.rseg` object at the current write version.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) fn rseg_object() -> Vec<u8> {
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};

    let tenant = TenantId::new("acme");
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "m".to_string(),
    }])
    .expect("labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(&tenant, "m", &labels).expect("series id"),
        labels,
        samples: vec![Sample {
            ts_ns: 1,
            value: 1.0,
        }],
    }];
    SegmentWriter::write(
        series,
        SegmentIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: uuid::Uuid::from_u128(1).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        },
        IngestBounds {
            min_ingest_ts_ns: 1,
            max_ingest_ts_ns: 1,
        },
    )
    .expect("write rseg")
    .bytes
    .to_vec()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use ravel_catalog::compat::{COMPAT_FORMAT_VERSION, create_compat};
    use ravel_catalog::{AbsentPolicy, validate_or_adopt};
    use ravel_commit::record::NewCommitRecord;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetOutcome, ObjectMeta, PutOptions, PutOutcome};
    use uuid::Uuid;

    use super::*;
    use crate::late_writes::late_write_pass;

    const HOUR: i64 = 3_600_000_000_000;
    const ACTIVATED: i64 = 1_000;
    /// Far enough past `ACTIVATED` to clear the activation and `G_pre` terms.
    const NOW1: i64 = 400_000 * HOUR;
    /// The RLOG and RSPAN writers' trailer version, and the RSEG writer's.
    const V_LOG: u32 = 5;
    const V_SEG: u32 = 7;

    fn ckeys() -> CompatKeys {
        CompatKeys::from_file_keys(&[[7; 32]]).expect("one key")
    }

    fn timings() -> CertifyTimings {
        CertifyTimings {
            t_req_ns: 30 * 1_000_000_000,
            max_flush_lifetime_ns: HOUR,
            clock_skew_allowance_ns: 60 * 1_000_000_000,
        }
    }

    fn skew() -> i64 {
        timings().clock_skew_allowance_ns
    }

    fn tenant() -> TenantHash {
        TenantHash([5; 16])
    }

    /// Every census listing in these tests pages at two keys.
    fn new_store() -> Arc<MemoryStore> {
        Arc::new(MemoryStore::with_page_size(2))
    }

    fn ms(ns: i64) -> u64 {
        (ns / 1_000_000) as u64
    }

    /// Create `sys/compat` with `family` active at `active`, activated at
    /// `ACTIVATED` in epoch 1, `fence_complete` for the fence-walk
    /// precondition, and `P = p_ns`.
    async fn seed_compat_with(
        store: &dyn ObjectStoreBackend,
        family: &str,
        active: u32,
        fence_complete: i64,
        p_ns: i64,
    ) {
        create_compat(
            store,
            &ckeys(),
            sysproto::CompatRecord {
                format_version: COMPAT_FORMAT_VERSION,
                baseline_release_major: 0,
                baseline_release_minor: 34,
                families: vec![sysproto::CompatFamily {
                    family: family.to_string(),
                    active_write_version: active,
                    activated_epoch: 1,
                    activated_unix_ns: ACTIVATED,
                    activated_by: "test".to_string(),
                    certificate: None,
                    fence_complete_unix_ns: fence_complete,
                    late_writes_verified_through_unix_ns: 0,
                    contradicted: false,
                }],
                compat_epoch: 1,
                max_publish_lifetime_ns: p_ns,
                adopted: Vec::new(),
                mac: Vec::new(),
            },
        )
        .await
        .expect("create sys/compat");
    }

    async fn seed_compat(store: &dyn ObjectStoreBackend, family: &str, active: u32) {
        seed_compat_with(store, family, active, ACTIVATED, HOUR).await;
    }

    /// Provision `(tenant, signal)` at `shards`, unfenced (a v3 record).
    async fn provision_unfenced(
        store: &dyn ObjectStoreBackend,
        tenant: TenantHash,
        signal: Signal,
        shards: u32,
    ) {
        validate_or_adopt(
            store,
            &tenant,
            signal,
            shards,
            ACTIVATED,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
    }

    /// Provision `(tenant, signal)` at `shards` and fence it to epoch 1 at
    /// `fenced_at`.
    async fn provision_fenced_at(
        store: &dyn ObjectStoreBackend,
        tenant: TenantHash,
        signal: Signal,
        shards: u32,
        fenced_at: i64,
    ) {
        provision_unfenced(store, tenant, signal, shards).await;
        raise_compat_fence(store, &tenant, signal, 1, fenced_at)
            .await
            .expect("fence");
    }

    /// Provision and fence at `ACTIVATED`, an old time, so the bucket's `G_pre`
    /// has long passed and the time gate and recheck admit it.
    async fn provision_fenced(
        store: &dyn ObjectStoreBackend,
        tenant: TenantHash,
        signal: Signal,
        shards: u32,
    ) {
        provision_fenced_at(store, tenant, signal, shards, ACTIVATED).await;
    }

    /// One commit record of `(tenant, signal, shard)` stamped `stamp`, with
    /// store `LastModified` `modified_ms`, and `data` (when given) put at the
    /// data key the record names. Returns `(commit_key, data_key)`.
    #[allow(clippy::too_many_arguments)]
    async fn seed_member(
        store: &MemoryStore,
        tenant: TenantHash,
        signal: Signal,
        shard: u32,
        seq: u64,
        stamp: u32,
        modified_ms: u64,
        data: Option<Vec<u8>>,
    ) -> (String, String) {
        store.set_clock_ms(modified_ms);
        let ts_ns = i64::from(shard) * HOUR + (seq as i64) * 1_000_000;
        let commit = record::build(NewCommitRecord {
            tenant_hash: tenant,
            signal,
            shard,
            writer_id: Uuid::from_u128(u128::from(seq) + 1),
            writer_epoch: 1,
            writer_seq: seq,
            object_size: data.as_ref().map_or(1, |d| d.len() as u64),
            content_hash: [seq as u8; 32],
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: ts_ns,
            max_event_ts_ns: ts_ns,
            min_ingest_ts_ns: ts_ns,
            max_ingest_ts_ns: ts_ns,
            segment_format_version: stamp,
            created_unix_ns: ts_ns,
            ingest_hour_bucket: u32::try_from(ts_ns.div_euclid(HOUR)).expect("hour"),
        })
        .expect("build commit");
        let key = ravel_commit::keys::commit_key_for_record(&commit).expect("commit key");
        store
            .put(&key, record::encode(&commit), PutOptions::default())
            .await
            .expect("put commit");
        let data_key = ravel_commit::keys::reconstruct_data_key(&commit).expect("data key");
        if let Some(bytes) = data {
            put(store, &data_key, bytes).await;
        }
        (key, data_key)
    }

    /// A stamp-5 logs commit with a readable `.rlog` data object.
    async fn seed_log(
        store: &MemoryStore,
        tenant: TenantHash,
        shard: u32,
        seq: u64,
        modified_ms: u64,
    ) -> (String, String) {
        seed_member(
            store,
            tenant,
            Signal::Logs,
            shard,
            seq,
            V_LOG,
            modified_ms,
            Some(rlog_object()),
        )
        .await
    }

    /// The `family` entry of `sys/compat`.
    async fn family_entry(store: &dyn ObjectStoreBackend, family: &str) -> sysproto::CompatFamily {
        read_compat(store, &ckeys())
            .await
            .expect("read")
            .expect("present")
            .0
            .families
            .into_iter()
            .find(|f| f.family == family)
            .expect("family entry")
    }

    async fn rlog_entry(store: &dyn ObjectStoreBackend) -> sysproto::CompatFamily {
        family_entry(store, FAMILY_RLOG).await
    }

    /// The `rlog` floor of `(tenant, signal)`.
    async fn rlog_floor(
        store: &dyn ObjectStoreBackend,
        tenant: TenantHash,
        signal: Signal,
    ) -> Option<u32> {
        ravel_catalog::current_floor_from_store(store, &tenant, signal, FAMILY_RLOG)
            .await
            .expect("floor")
    }

    fn rlog_object() -> Vec<u8> {
        rlog_object_with_sections(false)
    }

    /// `bytes` with its trailer's version field (bytes 8..10 of the last 16, in
    /// all three formats) set to `version`.
    fn with_trailer_version(mut bytes: Vec<u8>, version: u16) -> Vec<u8> {
        let at = bytes.len() - TRAILER_BYTES + 8;
        bytes[at..at + 2].copy_from_slice(&version.to_le_bytes());
        bytes
    }

    /// `bytes` with the last trailer magic byte flipped.
    fn with_corrupt_trailer(mut bytes: Vec<u8>) -> Vec<u8> {
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        bytes
    }

    async fn put(store: &dyn ObjectStoreBackend, key: &str, bytes: Vec<u8>) {
        store
            .put(key, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put object");
    }

    async fn cursor_present(store: &dyn ObjectStoreBackend) -> bool {
        store
            .get(&certify_cursor_key(FAMILY_RLOG), GetRange::Full)
            .await
            .is_ok()
    }

    async fn certify_rlog(
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
        target: u32,
        budget: Option<usize>,
    ) -> CertifyReport {
        certify(
            store,
            &ckeys(),
            now_ns,
            FAMILY_RLOG,
            target,
            timings(),
            budget,
        )
        .await
        .expect("certify")
    }

    type Hook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

    /// A store that records every GET key and runs `hook` once, before the
    /// first GET whose key contains `trigger`.
    struct HookStore {
        inner: Arc<MemoryStore>,
        trigger: String,
        hook: Mutex<Option<Hook>>,
        gets: Mutex<Vec<String>>,
    }

    impl HookStore {
        fn new(inner: Arc<MemoryStore>, trigger: &str, hook: Option<Hook>) -> Self {
            HookStore {
                inner,
                trigger: trigger.to_string(),
                hook: Mutex::new(hook),
                gets: Mutex::new(Vec::new()),
            }
        }

        fn gets_of(&self, key: &str) -> usize {
            self.gets
                .lock()
                .expect("gets")
                .iter()
                .filter(|k| *k == key)
                .count()
        }

        fn hook_fired(&self) -> bool {
            self.hook.lock().expect("hook").is_none()
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for HookStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> std::result::Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> std::result::Result<GetOutcome, StoreError> {
            self.gets.lock().expect("gets").push(key.to_string());
            if key.contains(&self.trigger) {
                let hook = self.hook.lock().expect("hook").take();
                if let Some(hook) = hook {
                    hook().await;
                }
            }
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> std::result::Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> std::result::Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> std::result::Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    // ---- the member probe ----

    #[tokio::test]
    async fn a_readable_object_is_not_held() {
        let store = MemoryStore::new();
        put(&store, "obj", rlog_object()).await;
        assert_eq!(rlog_held_version(&store, "obj").await.expect("probe"), None);
        assert_eq!(
            probe_member(&store, FAMILY_RLOG, "obj")
                .await
                .expect("probe"),
            MemberProbe::Readable {
                trailer: V_LOG as u16,
                section_hold: None
            }
        );
    }

    #[tokio::test]
    async fn an_unreadable_postings_version_is_held() {
        let store = MemoryStore::new();
        // The whole object's trailer version stays in window; only the nested
        // POSTINGS grammar version moves above the read set.
        put(&store, "obj", rlog_object_with_sections(true)).await;
        assert_eq!(
            rlog_held_version(&store, "obj").await.expect("probe"),
            Some(u16::from(POSTINGS_VERSION + 1))
        );
    }

    #[tokio::test]
    async fn an_absent_object_is_not_held() {
        let store = MemoryStore::new();
        assert_eq!(
            rlog_held_version(&store, "missing").await.expect("probe"),
            None
        );
        for family in [FAMILY_RSEG, FAMILY_RLOG, FAMILY_RSPAN] {
            assert_eq!(
                probe_member(&store, family, "missing")
                    .await
                    .expect("probe"),
                MemberProbe::Absent,
                "{family}"
            );
        }
    }

    /// The trailer version is untouched; the hold is on the section version
    /// alone. A reader confirms the trailer is still in window (so the object
    /// would pass the trailer-only gate and be deleted without this probe).
    #[tokio::test]
    async fn the_trailer_stays_in_window() {
        let store = MemoryStore::new();
        put(&store, "obj", rlog_object_with_sections(true)).await;
        put(&store, "span", rspan_object_with_key_idx(true)).await;
        let tail = store
            .get(
                "obj",
                GetRange::Suffix(ravel_logseg::footer::TRAILER_LEN as u64),
            )
            .await
            .expect("suffix");
        assert!(
            !matches!(
                ravel_logseg::open_from_suffix(tail.data.as_ref(), tail.total_size),
                Err(ravel_logseg::LogSegError::UnsupportedVersion(_))
            ),
            "trailer version must stay in window for this to be an H5 case"
        );
        let tail = store
            .get(
                "span",
                GetRange::Suffix(ravel_rspan::footer::TRAILER_LEN as u64),
            )
            .await
            .expect("suffix");
        assert!(
            !matches!(
                ravel_rspan::open_from_suffix(tail.data.as_ref(), tail.total_size),
                Err(ravel_rspan::SpanSegError::UnsupportedVersion(_))
            ),
            "trailer version must stay in window for this to be an H5 case"
        );
    }

    /// ADR-4233 H5 for RSPAN: a `.rspan` object whose trailer is in window but
    /// whose KEY_IDX section is at a version outside the read set is held on
    /// that version; the readable control is not.
    #[tokio::test]
    async fn an_unreadable_rspan_key_idx_version_is_held() {
        let store = MemoryStore::new();
        put(&store, "held", rspan_object_with_key_idx(true)).await;
        put(&store, "readable", rspan_object_with_key_idx(false)).await;
        let above = u16::from(*key_index::KEY_IDX_VERSIONS.iter().max().expect("set")) + 1;
        assert_eq!(
            rspan_held_version(&store, "held").await.expect("probe"),
            Some(above)
        );
        assert_eq!(
            probe_member(&store, FAMILY_RSPAN, "readable")
                .await
                .expect("probe"),
            MemberProbe::Readable {
                trailer: V_LOG as u16,
                section_hold: None
            }
        );
    }

    /// The probe decodes each family's trailer: the real version when in
    /// window, the real version when out of it (RSEG included), and Corrupt
    /// for a bad trailer.
    #[tokio::test]
    async fn the_probe_reports_each_familys_trailer() {
        let store = MemoryStore::new();
        put(&store, "seg", rseg_object()).await;
        put(&store, "seg6", with_trailer_version(rseg_object(), 6)).await;
        put(&store, "seg-bad", with_corrupt_trailer(rseg_object())).await;
        put(&store, "log4", with_trailer_version(rlog_object(), 4)).await;
        put(&store, "log-bad", with_corrupt_trailer(rlog_object())).await;
        put(
            &store,
            "span-bad",
            with_corrupt_trailer(rspan_object_with_key_idx(false)),
        )
        .await;
        let probe = |family, key| {
            let store = &store;
            async move { probe_member(store, family, key).await.expect("probe") }
        };
        assert_eq!(
            probe(FAMILY_RSEG, "seg").await,
            MemberProbe::Readable {
                trailer: V_SEG as u16,
                section_hold: None
            }
        );
        assert_eq!(
            probe(FAMILY_RSEG, "seg6").await,
            MemberProbe::OutOfWindow(6)
        );
        assert_eq!(probe(FAMILY_RSEG, "seg-bad").await, MemberProbe::Corrupt);
        assert_eq!(
            probe(FAMILY_RLOG, "log4").await,
            MemberProbe::OutOfWindow(4)
        );
        assert_eq!(probe(FAMILY_RLOG, "log-bad").await, MemberProbe::Corrupt);
        assert_eq!(probe(FAMILY_RSPAN, "span-bad").await, MemberProbe::Corrupt);
    }

    /// The per-object request cost the module doc states: RSEG one GET; an
    /// RLOG object with POSTINGS and KEY_IDX sections and a footer under 64 KiB
    /// three (footer suffix, POSTINGS byte, KEY_IDX prefix); an RSPAN object
    /// two (footer suffix, KEY_IDX byte).
    #[tokio::test]
    async fn the_probe_costs_the_documented_gets() {
        let inner = new_store();
        put(&*inner, "seg", rseg_object()).await;
        put(&*inner, "log", rlog_object()).await;
        put(&*inner, "span", rspan_object_with_key_idx(false)).await;
        let store = HookStore::new(inner, "\u{0}", None);
        probe_member(&store, FAMILY_RSEG, "seg").await.expect("seg");
        probe_member(&store, FAMILY_RLOG, "log").await.expect("log");
        probe_member(&store, FAMILY_RSPAN, "span")
            .await
            .expect("span");
        assert_eq!(store.gets_of("seg"), 1);
        assert_eq!(store.gets_of("log"), 3);
        assert_eq!(store.gets_of("span"), 2);
    }

    // ---- the cursor ----

    #[test]
    fn the_cursor_round_trips_and_refuses_other_payloads() {
        let mut buckets = BTreeMap::new();
        buckets.insert(
            (TenantHash([3; 16]), 2),
            BucketProgress {
                shards_done: 2,
                entries: 9,
                newest_created_unix_ns: 77,
            },
        );
        let cursor = CertifyCursor {
            target_version: 5,
            activated_epoch: 3,
            start_unix_ns: 1234,
            prior: PriorState {
                certificate: Some((4, 99)),
                contradicted: true,
            },
            finalizing_unix_ns: Some(4321),
            buckets,
        };
        let bytes = encode_cursor(&cursor);
        assert_eq!(decode_cursor(&bytes, 3), Some(cursor.clone()));
        let unmarked = CertifyCursor {
            finalizing_unix_ns: None,
            ..cursor.clone()
        };
        assert_eq!(decode_cursor(&encode_cursor(&unmarked), 3), Some(unmarked));
        // A signal index outside the family, a trailing byte, a truncation, and
        // the index-based tag-1 layout are all unrecognized.
        assert_eq!(decode_cursor(&bytes, 2), None);
        let mut long = bytes.clone();
        long.push(0);
        assert_eq!(decode_cursor(&long, 3), None);
        assert_eq!(decode_cursor(&bytes[..bytes.len() - 1], 3), None);
        let mut tag1 = bytes;
        tag1[0] = 1;
        assert_eq!(decode_cursor(&tag1, 3), None);
    }

    // ---- certify preconditions ----

    #[tokio::test]
    async fn certify_refuses_before_fence_complete() {
        let store = new_store();
        seed_compat_with(&*store, FAMILY_RLOG, V_LOG, 0, HOUR).await;
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::FenceWalkIncomplete)
        );
    }

    /// certify refuses, and records no `S`, before the time precondition passes.
    #[tokio::test]
    async fn certify_refuses_before_the_time_gate() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        // `now` just above activation but below activation + H + P + T_req.
        let report = certify_rlog(&*store, ACTIVATED + 1, V_LOG, None).await;
        assert!(
            matches!(
                report,
                CertifyReport::Refused(CertifyRefusal::TimeGate { .. })
            ),
            "{report:?}"
        );
        assert!(
            !cursor_present(&*store).await,
            "S is gated on the time gate"
        );
    }

    /// With no `P` on `sys/compat` the time precondition has no publish term,
    /// so certify refuses rather than treating it as zero.
    #[tokio::test]
    async fn certify_refuses_without_a_publish_lifetime() {
        let store = new_store();
        seed_compat_with(&*store, FAMILY_RLOG, V_LOG, ACTIVATED, 0).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::PublishLifetimeAbsent)
        );
    }

    /// `G_pre` takes the larger of the configured and the `sys/gc`
    /// `max_flush_lifetime`: a bucket fenced 10 h ago clears `G_pre` at the
    /// configured 1 h (3 + 2 + 1 h plus `T_req`) but not at the recorded 10 h
    /// (3 + 20 + 1 h).
    #[tokio::test]
    async fn the_recorded_max_flush_lifetime_widens_g_pre() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced_at(&*store, tenant(), Signal::Logs, 1, NOW1 - 10 * HOUR).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        crate::gc_config::bootstrap_gc_config(
            &*store,
            crate::gc_config::GcConfigValues {
                max_flush_lifetime_ns: 10 * HOUR,
                ..crate::gc_config::GcConfigValues::maintain_defaults()
            },
            ACTIVATED,
        )
        .await
        .expect("sys/gc");
        let report = certify_rlog(&*store, NOW1, V_LOG, None).await;
        assert!(
            matches!(
                report,
                CertifyReport::Refused(CertifyRefusal::TimeGate { .. })
            ),
            "{report:?}"
        );
    }

    // ---- member verdicts ----

    /// A member stamped below the target makes the result contradicted.
    #[tokio::test]
    async fn a_lower_version_stamp_yields_contradicted() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        let (_, data_key) = seed_member(&store, tenant(), Signal::Logs, 0, 1, 4, 1_000, None).await;
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::Contradicted {
                member_key: data_key,
                found_version: 4
            }
        );
    }

    /// A member whose nested section is unreadable holds the certificate,
    /// reported with its key and the section's version.
    #[tokio::test]
    async fn a_held_member_holds_the_certificate() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        let (_, data_key) = seed_member(
            &store,
            tenant(),
            Signal::Logs,
            0,
            1,
            V_LOG,
            1_000,
            Some(rlog_object_with_sections(true)),
        )
        .await;
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::HeldMember {
                member_key: data_key,
                version: u16::from(POSTINGS_VERSION + 1)
            }
        );
        assert!(rlog_entry(&*store).await.certificate.is_none());
    }

    /// A held RSEG member is reported on its real trailer version.
    #[tokio::test]
    async fn a_held_rseg_member_reports_its_trailer_version() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RSEG, V_SEG).await;
        provision_fenced(&*store, tenant(), Signal::Metrics, 1).await;
        let (_, data_key) = seed_member(
            &store,
            tenant(),
            Signal::Metrics,
            0,
            1,
            V_SEG,
            1_000,
            Some(with_trailer_version(rseg_object(), 6)),
        )
        .await;
        let report = certify(&*store, &ckeys(), NOW1, FAMILY_RSEG, V_SEG, timings(), None)
            .await
            .expect("certify");
        assert_eq!(
            report,
            CertifyReport::HeldMember {
                member_key: data_key,
                version: 6
            }
        );
    }

    /// A member whose stamp is one above its trailer version refuses: the
    /// stamp is a claim the bytes contradict.
    #[tokio::test]
    async fn a_stamp_above_the_trailer_refuses() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        let (_, data_key) = seed_member(
            &store,
            tenant(),
            Signal::Logs,
            0,
            1,
            V_LOG + 1,
            1_000,
            Some(rlog_object()),
        )
        .await;
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::StampTrailerMismatch {
                member_key: data_key,
                stamp: V_LOG + 1,
                trailer: V_LOG as u16
            })
        );
    }

    #[tokio::test]
    async fn an_absent_member_refuses() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        let (_, data_key) =
            seed_member(&store, tenant(), Signal::Logs, 0, 1, V_LOG, 1_000, None).await;
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::MemberAbsent {
                member_key: data_key
            })
        );
    }

    #[tokio::test]
    async fn a_corrupt_member_trailer_refuses() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        let (_, data_key) = seed_member(
            &store,
            tenant(),
            Signal::Logs,
            0,
            1,
            V_LOG,
            1_000,
            Some(with_corrupt_trailer(rlog_object())),
        )
        .await;
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::MemberCorrupt {
                member_key: data_key
            })
        );
    }

    /// Objects under `l0/` and `l1/` that no record names are census members:
    /// an out-of-window one in each holds the certificate.
    #[tokio::test]
    async fn unrecorded_l0_and_l1_objects_are_censused() {
        for dir in ["l0/0000/stray.rseg", "l1/0000/000123/stray.rseg"] {
            let store = new_store();
            seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
            provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
            seed_log(&store, tenant(), 0, 1, 1_000).await;
            let stray = format!("t/{}/l/{dir}", tenant().to_hex());
            put(&*store, &stray, with_trailer_version(rlog_object(), 4)).await;
            assert_eq!(
                certify_rlog(&*store, NOW1, V_LOG, None).await,
                CertifyReport::HeldMember {
                    member_key: stray,
                    version: 4
                }
            );
        }
    }

    /// Alerts (fixed shard 0) and audit (fixed shards 0 and 1) are rlog
    /// members though never provisioned.
    #[tokio::test]
    async fn alerts_and_audit_members_are_censused() {
        for (signal, shard) in [(Signal::Alerts, 0), (Signal::Audit, 1)] {
            let store = new_store();
            seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
            provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
            seed_log(&store, tenant(), 0, 1, 1_000).await;
            let (_, data_key) =
                seed_member(&store, tenant(), signal, shard, 2, 4, 1_000, None).await;
            assert_eq!(
                certify_rlog(&*store, NOW1, V_LOG, None).await,
                CertifyReport::Contradicted {
                    member_key: data_key,
                    found_version: 4
                },
                "{signal:?}"
            );
        }
    }

    /// DISTINGUISHING (rules out a shard range taken from the provisioning
    /// record alone, and one widened by the `l0/` and `c/` directories but not
    /// `l1/`): an unprovisioned logs bucket spans two shards, as adopted data
    /// with no record can, and shard 1 holds a below-target member reachable
    /// only through its commit record, only under `l0/`, or only under `l1/`.
    /// Each is found; none certifies.
    #[tokio::test]
    async fn an_unprovisioned_bucket_is_censused_over_every_present_shard() {
        let unrecorded = |dir: &str| format!("t/{}/l/{dir}", tenant().to_hex());
        for case in ["c", "l0", "l1"] {
            let store = new_store();
            seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
            seed_log(&store, tenant(), 0, 1, 1_000).await;
            let expected = match case {
                "c" => {
                    let (_, data_key) =
                        seed_member(&store, tenant(), Signal::Logs, 1, 2, 4, 1_000, None).await;
                    CertifyReport::Contradicted {
                        member_key: data_key,
                        found_version: 4,
                    }
                }
                _ => {
                    let key = unrecorded(if case == "l0" {
                        "l0/0001/stray.rlog"
                    } else {
                        "l1/0001/000123/stray.rlog"
                    });
                    put(&*store, &key, with_trailer_version(rlog_object(), 4)).await;
                    CertifyReport::HeldMember {
                        member_key: key,
                        version: 4,
                    }
                }
            };
            assert!(
                read_generations_from_store(&*store, &tenant(), Signal::Logs)
                    .await
                    .expect("generations")
                    .is_none(),
                "the bucket has no provisioning record"
            );
            assert_eq!(
                certify_rlog(&*store, NOW1, V_LOG, None).await,
                expected,
                "{case}"
            );
            assert!(rlog_entry(&*store).await.certificate.is_none(), "{case}");
        }
    }

    /// An unrecorded object has no stamp, so its trailer alone decides: with
    /// `V_LOG + 1` active and certified, an in-window `V_LOG` `.rlog` under
    /// `l0/` that no record names contradicts the certificate.
    #[tokio::test]
    async fn an_unrecorded_in_window_object_below_the_target_contradicts() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG + 1).await;
        let key = format!("t/{}/l/l0/0000/stray.rlog", tenant().to_hex());
        put(&*store, &key, rlog_object()).await;
        assert!(
            matches!(
                probe_member(&*store, FAMILY_RLOG, &key)
                    .await
                    .expect("probe"),
                MemberProbe::Readable {
                    trailer,
                    section_hold: None
                } if u32::from(trailer) == V_LOG
            ),
            "the object must be readable and in window for the trailer check to decide"
        );
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG + 1, None).await,
            CertifyReport::Contradicted {
                member_key: key,
                found_version: V_LOG
            }
        );
        assert!(rlog_entry(&*store).await.certificate.is_none());
    }

    /// An entry under a bucket's `l0/`, `l1/` or `c/` that is not a shard
    /// directory refuses: no shard census would cover it.
    #[tokio::test]
    async fn a_non_shard_entry_refuses() {
        for stray in ["l0/stray.rlog", "l1/abc/stray.rlog", "c/0x01/stray.cmt"] {
            let store = new_store();
            seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
            seed_log(&store, tenant(), 0, 1, 1_000).await;
            let key = format!("t/{}/l/{stray}", tenant().to_hex());
            put(&*store, &key, rlog_object()).await;
            let expected = match stray.matches('/').count() {
                1 => key,
                _ => key[..=key.rfind('/').expect("dir")].to_string(),
            };
            assert_eq!(
                certify_rlog(&*store, NOW1, V_LOG, None).await,
                CertifyReport::Refused(CertifyRefusal::UnexpectedShardEntry { key: expected }),
                "{stray}"
            );
        }
    }

    /// An incomplete listing (an unaddressable key under a census prefix)
    /// refuses with the typed refusal naming the prefix.
    #[tokio::test]
    async fn an_incomplete_listing_blocks() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        let prefix =
            ravel_commit::keys::commit_shard_prefix(&tenant(), Signal::Logs, 0).expect("prefix");
        store.insert_foreign(
            &format!("{prefix}a#unaddressable"),
            Bytes::from_static(b"x"),
        );
        assert_eq!(
            certify_rlog(&*store, NOW1, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::IncompleteListing {
                prefix,
                unaddressable: 1
            })
        );
    }

    /// A faulted LIST page blocks certification, and the fault is shown to
    /// have fired.
    #[tokio::test]
    async fn a_faulted_listing_blocks_certification() {
        let inner = new_store();
        seed_compat(&*inner, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*inner, tenant(), Signal::Logs, 1).await;
        seed_log(&inner, tenant(), 0, 1, 1_000).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Permanent("list down".to_string()))
                .with_key_contains("/c/"),
        );
        let store = FaultStore::new(inner.clone(), plan);
        let result = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, V_LOG, timings(), None).await;
        assert!(result.is_err(), "a faulted listing must block: {result:?}");
        assert_eq!(store.fault_count(Op::List, FaultKind::Permanent), 1);
        assert!(rlog_entry(&*inner).await.certificate.is_none());
    }

    /// certify fences a bucket provisioned at version 3 after the eager walk,
    /// and the per-bucket `G_pre` recheck then holds the certificate until that
    /// fresh fence's own `G_pre` has passed.
    #[tokio::test]
    async fn the_per_bucket_g_pre_recheck_refuses_a_recently_fenced_bucket() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_unfenced(&*store, tenant(), Signal::Logs, 1).await;
        let report = certify_rlog(&*store, NOW1, V_LOG, None).await;
        assert!(
            matches!(
                report,
                CertifyReport::Refused(CertifyRefusal::BucketRecentlyFenced { .. })
            ),
            "{report:?}"
        );
    }

    // ---- certified censuses ----

    /// A clean census certifies over real data objects, writing the
    /// certificate, the watermark `S`, the floor, and clearing the cursor.
    #[tokio::test]
    async fn a_clean_census_certifies() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        let report = certify_rlog(&*store, NOW1, V_LOG, None).await;
        // logs 1 shard + alerts 1 + audit 2.
        assert_eq!(
            report,
            CertifyReport::Certified {
                version: V_LOG,
                member_count: 1,
                buckets_scanned: 4,
                newest_created_unix_ns: 1_000_000,
            }
        );
        let entry = rlog_entry(&*store).await;
        assert_eq!(entry.certificate.expect("cert").version, V_LOG);
        assert_eq!(
            entry.late_writes_verified_through_unix_ns, NOW1,
            "watermark is S"
        );
        assert_eq!(
            ravel_catalog::current_floor_from_store(&*store, &tenant(), Signal::Logs, FAMILY_RLOG)
                .await
                .expect("floor read"),
            Some(V_LOG),
        );
        assert!(!cursor_present(&*store).await);
    }

    /// RSEG and RSPAN censuses certify over real objects, three per shard so
    /// every listing pages.
    #[tokio::test]
    async fn rseg_and_rspan_censuses_certify() {
        for (family, signal, version) in [
            (FAMILY_RSEG, Signal::Metrics, V_SEG),
            (FAMILY_RSPAN, Signal::Spans, V_LOG),
        ] {
            let store = new_store();
            seed_compat(&*store, family, version).await;
            provision_fenced(&*store, tenant(), signal, 1).await;
            for seq in 1..=3 {
                let bytes = if signal == Signal::Metrics {
                    rseg_object()
                } else {
                    rspan_object_with_key_idx(false)
                };
                seed_member(
                    &store,
                    tenant(),
                    signal,
                    0,
                    seq,
                    version,
                    1_000,
                    Some(bytes),
                )
                .await;
            }
            let report = certify(&*store, &ckeys(), NOW1, family, version, timings(), None)
                .await
                .expect("certify");
            assert_eq!(
                report,
                CertifyReport::Certified {
                    version,
                    member_count: 3,
                    buckets_scanned: 1,
                    newest_created_unix_ns: 3_000_000,
                },
                "{family}"
            );
        }
    }

    /// A held RSPAN member (KEY_IDX outside the read set) holds certify.
    #[tokio::test]
    async fn a_held_rspan_member_holds_the_certificate() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RSPAN, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Spans, 1).await;
        let (_, data_key) = seed_member(
            &store,
            tenant(),
            Signal::Spans,
            0,
            1,
            V_LOG,
            1_000,
            Some(rspan_object_with_key_idx(true)),
        )
        .await;
        let report = certify(
            &*store,
            &ckeys(),
            NOW1,
            FAMILY_RSPAN,
            V_LOG,
            timings(),
            None,
        )
        .await
        .expect("certify");
        let above = u16::from(*key_index::KEY_IDX_VERSIONS.iter().max().expect("set")) + 1;
        assert_eq!(
            report,
            CertifyReport::HeldMember {
                member_key: data_key,
                version: above
            }
        );
    }

    /// The certificate's totals and the floors cover every run of a resumed
    /// census, not only the last.
    #[tokio::test]
    async fn totals_cover_every_run() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 2).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        seed_log(&store, tenant(), 1, 2, 1_000).await;
        let run1 = certify_rlog(&*store, NOW1, V_LOG, Some(1)).await;
        assert_eq!(run1, CertifyReport::Incomplete { units_examined: 1 });
        let run2 = certify_rlog(&*store, NOW1 + HOUR, V_LOG, None).await;
        // logs 2 shards + alerts 1 + audit 2.
        assert_eq!(
            run2,
            CertifyReport::Certified {
                version: V_LOG,
                member_count: 2,
                buckets_scanned: 5,
                newest_created_unix_ns: HOUR + 2_000_000,
            }
        );
    }

    // ---- resume ----

    /// DISTINGUISHING (rules out an index-based resume): a tenant that sorts
    /// before every tenant the first run covered appears between runs with a
    /// below-target member. An index cursor would count it as done; the
    /// key-based cursor has no progress for it and censuses it.
    #[tokio::test]
    async fn a_tenant_appearing_in_the_done_range_is_censused() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        for t in [tenant(), TenantHash([9; 16])] {
            provision_fenced(&*store, t, Signal::Logs, 1).await;
            seed_log(&store, t, 0, 1, 1_000).await;
        }
        // Tenant 5's four units (logs, alerts, audit x2), then the budget.
        let run1 = certify_rlog(&*store, NOW1, V_LOG, Some(4)).await;
        assert_eq!(run1, CertifyReport::Incomplete { units_examined: 4 });

        let early = TenantHash([1; 16]);
        provision_fenced(&*store, early, Signal::Logs, 1).await;
        let (_, data_key) = seed_member(&store, early, Signal::Logs, 0, 3, 4, 1_000, None).await;
        assert_eq!(
            certify_rlog(&*store, NOW1 + HOUR, V_LOG, None).await,
            CertifyReport::Contradicted {
                member_key: data_key,
                found_version: 4
            }
        );
    }

    /// DISTINGUISHING (rules out a recheck limited to buckets the last run
    /// fenced): a bucket provisioned unfenced between runs, under a shard the
    /// first run already censused, with a below-target member there. The
    /// resumed run never revisits the shard, so only the every-bucket
    /// `compat_epoch` recheck stands between it and a certificate.
    #[tokio::test]
    async fn a_bucket_provisioned_unfenced_between_runs_refuses() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        let run1 = certify_rlog(&*store, NOW1, V_LOG, Some(1)).await;
        assert_eq!(run1, CertifyReport::Incomplete { units_examined: 1 });

        provision_unfenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_member(&store, tenant(), Signal::Logs, 0, 2, 4, 1_000, None).await;
        assert_eq!(
            certify_rlog(&*store, NOW1 + HOUR, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::BucketUnfenced {
                tenant_hash: tenant().to_hex(),
                signal: Signal::Logs,
                compat_epoch: 0,
                activated_epoch: 1,
            })
        );
    }

    /// A cursor taken for another target is stale: the run starts a fresh
    /// census with its own `S`.
    #[tokio::test]
    async fn a_cursor_for_another_target_starts_a_fresh_census() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        let run1 = certify_rlog(&*store, NOW1, V_LOG, Some(1)).await;
        assert!(matches!(run1, CertifyReport::Incomplete { .. }), "{run1:?}");

        let now2 = NOW1 + HOUR;
        let run2 = certify_rlog(&*store, now2, V_LOG - 1, None).await;
        assert!(
            matches!(
                run2,
                CertifyReport::Certified {
                    buckets_scanned: 4,
                    ..
                }
            ),
            "{run2:?}"
        );
        assert_eq!(
            rlog_entry(&*store)
                .await
                .late_writes_verified_through_unix_ns,
            now2,
            "the target-4 census took its own S"
        );
    }

    /// A cursor taken under another activation epoch is stale.
    #[tokio::test]
    async fn a_cursor_from_another_epoch_starts_a_fresh_census() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        let run1 = certify_rlog(&*store, NOW1, V_LOG, Some(1)).await;
        assert!(matches!(run1, CertifyReport::Incomplete { .. }), "{run1:?}");

        update_compat(&*store, &ckeys(), |record| {
            record.compat_epoch = 2;
            let entry = record
                .families
                .iter_mut()
                .find(|f| f.family == FAMILY_RLOG)
                .expect("rlog");
            entry.activated_epoch = 2;
            Ok(())
        })
        .await
        .expect("bump epoch");
        // Fenced to epoch 2 at the old time, so the recheck admits it.
        raise_compat_fence(&*store, &tenant(), Signal::Logs, 2, ACTIVATED)
            .await
            .expect("fence 2");

        let now2 = NOW1 + HOUR;
        let run2 = certify_rlog(&*store, now2, V_LOG, None).await;
        assert!(matches!(run2, CertifyReport::Certified { .. }), "{run2:?}");
        assert_eq!(
            rlog_entry(&*store)
                .await
                .late_writes_verified_through_unix_ns,
            now2,
            "the epoch-2 census took its own S"
        );
    }

    /// A completed certification whose cursor delete failed leaves a cursor
    /// naming the pre-certificate state, so the next certification does not
    /// resume it as a census: a below-target member planted in its done shard
    /// is found.
    #[tokio::test]
    async fn a_cursor_left_by_a_failed_delete_is_not_resumed() {
        let inner = new_store();
        seed_compat(&*inner, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*inner, tenant(), Signal::Logs, 1).await;
        seed_log(&inner, tenant(), 0, 1, 1_000).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Delete,
                ScriptedFault::Permanent("delete down".to_string()),
            )
            .with_key_contains("sys/maintain/certify/"),
        );
        let faulted = FaultStore::new(inner.clone(), plan);
        let report = certify(
            &faulted,
            &ckeys(),
            NOW1,
            FAMILY_RLOG,
            V_LOG,
            timings(),
            None,
        )
        .await
        .expect("certify");
        assert!(
            matches!(report, CertifyReport::Certified { .. }),
            "{report:?}"
        );
        assert_eq!(faulted.fault_count(Op::Delete, FaultKind::Permanent), 1);
        assert!(cursor_present(&*inner).await, "the delete failed");

        // A late write contradicts the certificate, which is what lets a
        // same-version certification run at all.
        let (_, data_key) = seed_member(
            &inner,
            tenant(),
            Signal::Logs,
            0,
            2,
            4,
            ms(NOW1 + HOUR),
            None,
        )
        .await;
        let pass = late_write_pass(&*inner, &ckeys(), NOW1 + 2 * HOUR, FAMILY_RLOG, skew())
            .await
            .expect("pass");
        assert_eq!(pass.late_writes.len(), 1, "{pass:?}");
        assert_eq!(
            certify_rlog(&*inner, NOW1 + 3 * HOUR, V_LOG, None).await,
            CertifyReport::Contradicted {
                member_key: data_key,
                found_version: 4
            }
        );
    }

    // ---- the certificate CAS and the late-write pass ----

    /// DISTINGUISHING (rules out a CAS that ignores what changed under the
    /// census, and floors raised before the CAS): a certification at
    /// `V_LOG` over a certificate at `V_LOG - 1` is in flight when a late
    /// write lands in a shard it already censused and a late-write pass
    /// contradicts the current certificate. The CAS must refuse, since
    /// clearing `contradicted` would lose the late write, and the refusal
    /// leaves every floor where the first certificate put it: a floor at
    /// `V_LOG` would make `migrate` skip the bucket holding the late write.
    #[tokio::test]
    async fn a_late_write_pass_during_the_census_refuses_the_cas() {
        let inner = new_store();
        seed_compat(&*inner, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*inner, tenant(), Signal::Logs, 1).await;
        provision_fenced(&*inner, TenantHash([6; 16]), Signal::Logs, 1).await;
        seed_log(&inner, tenant(), 0, 1, 1_000).await;
        seed_log(&inner, TenantHash([6; 16]), 0, 3, 1_000).await;
        seed_member(
            &inner,
            tenant(),
            Signal::Audit,
            0,
            2,
            V_LOG,
            1_000,
            Some(rlog_object()),
        )
        .await;
        let first = certify_rlog(&*inner, NOW1, V_LOG - 1, None).await;
        assert!(
            matches!(first, CertifyReport::Certified { .. }),
            "{first:?}"
        );
        for t in [tenant(), TenantHash([6; 16])] {
            assert_eq!(rlog_floor(&*inner, t, Signal::Logs).await, Some(V_LOG - 1));
        }

        let now2 = NOW1 + 10 * HOUR;
        let hook_store = inner.clone();
        let hook: Hook = Box::new(move || {
            Box::pin(async move {
                seed_member(
                    &hook_store,
                    tenant(),
                    Signal::Logs,
                    0,
                    9,
                    V_LOG - 2,
                    ms(NOW1 + HOUR),
                    None,
                )
                .await;
                let pass = late_write_pass(&*hook_store, &ckeys(), now2, FAMILY_RLOG, skew())
                    .await
                    .expect("late-write pass");
                assert_eq!(pass.late_writes.len(), 1, "{pass:?}");
            })
        });
        let audit_prefix =
            ravel_commit::keys::commit_shard_prefix(&tenant(), Signal::Audit, 0).expect("prefix");
        let store = HookStore::new(inner.clone(), &audit_prefix, Some(hook));
        let report = certify_rlog(&store, now2 + HOUR, V_LOG, None).await;
        assert!(store.hook_fired());
        assert_eq!(
            report,
            CertifyReport::Refused(CertifyRefusal::CompatChangedDuringCensus)
        );
        let entry = rlog_entry(&*inner).await;
        assert!(entry.contradicted, "the late write's verdict stands");
        let cert = entry.certificate.expect("cert");
        assert_eq!((cert.version, cert.certified_unix_ns), (V_LOG - 1, NOW1));
        for t in [tenant(), TenantHash([6; 16])] {
            assert_eq!(
                rlog_floor(&*inner, t, Signal::Logs).await,
                Some(V_LOG - 1),
                "a refused CAS raises no floor"
            );
        }
    }

    /// DISTINGUISHING (rules out a monotone watermark across certificates): a
    /// late-write pass between two runs of a certification at a higher
    /// version advances the old certificate's watermark past the new census's
    /// `S`. The new
    /// certificate sets it back to `S`, so its first pass re-examines what
    /// landed after its census began.
    #[tokio::test]
    async fn a_new_certificate_sets_the_watermark_to_its_s() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        let first = certify_rlog(&*store, NOW1, V_LOG - 1, None).await;
        assert!(
            matches!(first, CertifyReport::Certified { .. }),
            "{first:?}"
        );

        let now2 = NOW1 + 10 * HOUR;
        let run1 = certify_rlog(&*store, now2, V_LOG, Some(1)).await;
        assert!(matches!(run1, CertifyReport::Incomplete { .. }), "{run1:?}");
        let now3 = now2 + 10 * HOUR;
        let pass = late_write_pass(&*store, &ckeys(), now3, FAMILY_RLOG, skew())
            .await
            .expect("pass");
        assert_eq!(pass.watermark_unix_ns, now3);

        let run2 = certify_rlog(&*store, now3 + HOUR, V_LOG, None).await;
        assert!(matches!(run2, CertifyReport::Certified { .. }), "{run2:?}");
        assert_eq!(
            rlog_entry(&*store)
                .await
                .late_writes_verified_through_unix_ns,
            now2
        );
    }

    /// DISTINGUISHING (rules out a certify that ignores the existing
    /// certificate, and one that admits any same-version replacement): over a
    /// certificate at `V_LOG`, a lower target and an uncontradicted same
    /// target both refuse before any census, leaving the certificate, its
    /// watermark and the cursor untouched. Once a late write contradicts it
    /// and is remedied, a same-version certification replaces it.
    #[tokio::test]
    async fn a_certificate_is_replaced_only_upward_or_after_a_late_write() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        let first = certify_rlog(&*store, NOW1, V_LOG, None).await;
        assert!(
            matches!(first, CertifyReport::Certified { .. }),
            "{first:?}"
        );

        let now2 = NOW1 + 10 * HOUR;
        assert_eq!(
            certify_rlog(&*store, now2, V_LOG - 1, None).await,
            CertifyReport::Refused(CertifyRefusal::TargetBelowCertificate {
                target: V_LOG - 1,
                certified: V_LOG
            })
        );
        assert_eq!(
            certify_rlog(&*store, now2, V_LOG, None).await,
            CertifyReport::Refused(CertifyRefusal::AlreadyCertified { version: V_LOG })
        );
        let entry = rlog_entry(&*store).await;
        let cert = entry.certificate.expect("cert");
        assert_eq!((cert.version, cert.certified_unix_ns), (V_LOG, NOW1));
        assert_eq!(entry.late_writes_verified_through_unix_ns, NOW1);
        assert!(!cursor_present(&*store).await, "no census started");

        // A late write contradicts the certificate; `migrate` (here, removing
        // the late record) remedies it, and the same version re-certifies.
        let (late_key, _) =
            seed_member(&store, tenant(), Signal::Logs, 0, 9, 4, ms(now2), None).await;
        let pass = late_write_pass(&*store, &ckeys(), now2 + HOUR, FAMILY_RLOG, skew())
            .await
            .expect("pass");
        assert_eq!(pass.late_writes.len(), 1, "{pass:?}");
        store.delete(&late_key).await.expect("remedy");
        let now3 = now2 + 2 * HOUR;
        let again = certify_rlog(&*store, now3, V_LOG, None).await;
        assert!(
            matches!(again, CertifyReport::Certified { .. }),
            "{again:?}"
        );
        let entry = rlog_entry(&*store).await;
        assert!(!entry.contradicted);
        assert_eq!(entry.certificate.expect("cert").certified_unix_ns, now3);
        assert_eq!(entry.late_writes_verified_through_unix_ns, now3);
    }

    /// A certificate above the target written while the census runs refuses
    /// inside the CAS with the typed refusal, not as a generic change.
    #[tokio::test]
    async fn a_higher_certificate_written_during_the_census_refuses_the_cas() {
        let inner = new_store();
        seed_compat(&*inner, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*inner, tenant(), Signal::Logs, 1).await;
        seed_log(&inner, tenant(), 0, 1, 1_000).await;
        seed_member(
            &inner,
            tenant(),
            Signal::Audit,
            0,
            2,
            V_LOG,
            1_000,
            Some(rlog_object()),
        )
        .await;
        let hook_store = inner.clone();
        let hook: Hook = Box::new(move || {
            Box::pin(async move {
                update_compat(&*hook_store, &ckeys(), |record| {
                    let entry = record
                        .families
                        .iter_mut()
                        .find(|f| f.family == FAMILY_RLOG)
                        .expect("rlog");
                    entry.certificate = Some(sysproto::Certificate {
                        version: V_LOG,
                        certified_unix_ns: NOW1,
                        member_count: 2,
                        newest_created_unix_ns: 1,
                        buckets_scanned: 4,
                        tenant_set_digest: vec![0; 32],
                    });
                    Ok(())
                })
                .await
                .expect("write certificate");
            })
        });
        let audit_prefix =
            ravel_commit::keys::commit_shard_prefix(&tenant(), Signal::Audit, 0).expect("prefix");
        let store = HookStore::new(inner.clone(), &audit_prefix, Some(hook));
        let report = certify_rlog(&store, NOW1 + HOUR, V_LOG - 1, None).await;
        assert!(store.hook_fired());
        assert_eq!(
            report,
            CertifyReport::Refused(CertifyRefusal::TargetBelowCertificate {
                target: V_LOG - 1,
                certified: V_LOG
            })
        );
        let cert = rlog_entry(&*inner).await.certificate.expect("cert");
        assert_eq!((cert.version, cert.certified_unix_ns), (V_LOG, NOW1));
        assert_eq!(rlog_floor(&*inner, tenant(), Signal::Logs).await, None);
    }

    /// A run that stops after the certificate CAS and before its floor raises
    /// finish is completed by the next run: the floor is raised and the
    /// cursor removed, and the same-version run reports the certificate the
    /// first run wrote rather than refusing it as already certified.
    #[tokio::test]
    async fn floor_raises_interrupted_after_the_cas_are_completed() {
        let inner = new_store();
        seed_compat(&*inner, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*inner, tenant(), Signal::Logs, 1).await;
        seed_log(&inner, tenant(), 0, 1, 1_000).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("put down".to_string()))
                .with_key_contains(provisioning_key(&tenant(), Signal::Logs)),
        );
        let faulted = FaultStore::new(inner.clone(), plan);
        let result = certify(
            &faulted,
            &ckeys(),
            NOW1,
            FAMILY_RLOG,
            V_LOG,
            timings(),
            None,
        )
        .await;
        assert!(result.is_err(), "{result:?}");
        assert_eq!(faulted.fault_count(Op::Put, FaultKind::Permanent), 1);
        let cert = rlog_entry(&*inner)
            .await
            .certificate
            .expect("CAS succeeded");
        assert_eq!(cert.certified_unix_ns, NOW1);
        assert_eq!(rlog_floor(&*inner, tenant(), Signal::Logs).await, None);
        assert!(cursor_present(&*inner).await);

        let report = certify_rlog(&*inner, NOW1 + HOUR, V_LOG, None).await;
        assert_eq!(
            report,
            CertifyReport::Certified {
                version: V_LOG,
                member_count: cert.member_count,
                buckets_scanned: cert.buckets_scanned,
                newest_created_unix_ns: cert.newest_created_unix_ns,
            }
        );
        assert_eq!(
            rlog_floor(&*inner, tenant(), Signal::Logs).await,
            Some(V_LOG)
        );
        assert!(!cursor_present(&*inner).await);
        let entry = rlog_entry(&*inner).await;
        assert_eq!(entry.certificate.expect("cert").certified_unix_ns, NOW1);
    }

    /// A late-write pass whose certificate is replaced while it runs writes
    /// nothing: its finding was judged against a certificate that is gone.
    #[tokio::test]
    async fn a_late_write_pass_aborts_when_the_certificate_is_replaced() {
        let inner = new_store();
        seed_compat(&*inner, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*inner, tenant(), Signal::Logs, 1).await;
        seed_log(&inner, tenant(), 0, 1, 1_000).await;
        certify_rlog(&*inner, NOW1, V_LOG, None).await;
        let (late_key, _) = seed_member(
            &inner,
            tenant(),
            Signal::Logs,
            0,
            9,
            4,
            ms(NOW1 + HOUR),
            None,
        )
        .await;

        let hook_store = inner.clone();
        let hook: Hook = Box::new(move || {
            Box::pin(async move {
                update_compat(&*hook_store, &ckeys(), |record| {
                    let entry = record
                        .families
                        .iter_mut()
                        .find(|f| f.family == FAMILY_RLOG)
                        .expect("rlog");
                    if let Some(cert) = entry.certificate.as_mut() {
                        cert.certified_unix_ns += 1;
                    }
                    Ok(())
                })
                .await
                .expect("replace certificate");
            })
        });
        let store = HookStore::new(inner.clone(), &late_key, Some(hook));
        let report = late_write_pass(&store, &ckeys(), NOW1 + 10 * HOUR, FAMILY_RLOG, skew())
            .await
            .expect("pass");
        assert!(store.hook_fired());
        assert!(report.certificate_replaced, "{report:?}");
        assert_eq!(report.late_writes.len(), 1);
        assert_eq!(report.watermark_unix_ns, NOW1);
        let entry = rlog_entry(&*inner).await;
        assert!(
            !entry.contradicted,
            "no verdict against the new certificate"
        );
        assert_eq!(entry.late_writes_verified_through_unix_ns, NOW1);
    }

    /// DISTINGUISHING (rules out S recomputed on resume): a late lower-version
    /// record lands in a shard the first run already read, between two
    /// resumed runs. The resumed run keeps the first run's `S`, so the
    /// late-write pass examines the record by its `LastModified` and finds it.
    #[tokio::test]
    async fn late_write_between_resumed_runs_is_found_by_the_late_write_pass() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 2).await;
        let old_ms = ms(NOW1) - 10_000;
        seed_log(&store, tenant(), 0, 1, old_ms).await;
        seed_log(&store, tenant(), 1, 2, old_ms).await;

        let run1 = certify_rlog(&*store, NOW1, V_LOG, Some(1)).await;
        assert!(matches!(run1, CertifyReport::Incomplete { .. }), "{run1:?}");

        let late_ms = ms(NOW1) + 3_600_000; // S + 1h
        let (late_key, _) =
            seed_member(&store, tenant(), Signal::Logs, 0, 9, 4, late_ms, None).await;

        let now2 = NOW1 + 10 * HOUR;
        let run2 = certify_rlog(&*store, now2, V_LOG, None).await;
        assert!(matches!(run2, CertifyReport::Certified { .. }), "{run2:?}");
        assert_eq!(
            rlog_entry(&*store)
                .await
                .late_writes_verified_through_unix_ns,
            NOW1,
            "the resumed run kept the first run's S"
        );
        assert!(
            (late_ms as i64) * 1_000_000 < now2 - skew(),
            "the late write predates a recomputed-S watermark, so only keeping S finds it"
        );

        let report = late_write_pass(&*store, &ckeys(), now2 + HOUR, FAMILY_RLOG, skew())
            .await
            .expect("late-write pass");
        assert!(
            report
                .late_writes
                .iter()
                .any(|lw| lw.record_key == late_key),
            "the late write was found: {report:?}"
        );
        assert!(rlog_entry(&*store).await.contradicted);
    }

    /// The late-write pass covers every shard directory present, not only the
    /// shards a provisioning record names: a late write in shard 1 of an
    /// unprovisioned two-shard logs bucket is found.
    #[tokio::test]
    async fn the_late_write_pass_covers_an_unprovisioned_buckets_shards() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        seed_log(&store, tenant(), 1, 2, 1_000).await;
        let first = certify_rlog(&*store, NOW1, V_LOG, None).await;
        assert!(
            matches!(
                first,
                CertifyReport::Certified {
                    member_count: 2,
                    ..
                }
            ),
            "{first:?}"
        );
        let (late_key, _) = seed_member(
            &store,
            tenant(),
            Signal::Logs,
            1,
            9,
            4,
            ms(NOW1 + HOUR),
            None,
        )
        .await;
        let report = late_write_pass(&*store, &ckeys(), NOW1 + 2 * HOUR, FAMILY_RLOG, skew())
            .await
            .expect("pass");
        assert_eq!(
            report.late_writes,
            vec![crate::late_writes::LateWrite {
                record_key: late_key,
                version: 4
            }]
        );
        assert!(rlog_entry(&*store).await.contradicted);
    }

    /// A later pass that starts earlier than a prior pass's start never lowers
    /// the watermark.
    #[tokio::test]
    async fn the_watermark_never_moves_backward() {
        let store = new_store();
        seed_compat(&*store, FAMILY_RLOG, V_LOG).await;
        provision_fenced(&*store, tenant(), Signal::Logs, 1).await;
        seed_log(&store, tenant(), 0, 1, 1_000).await;
        certify_rlog(&*store, NOW1, V_LOG, None).await;

        let later = NOW1 + 100 * HOUR;
        late_write_pass(&*store, &ckeys(), later, FAMILY_RLOG, skew())
            .await
            .expect("later pass");
        assert_eq!(
            rlog_entry(&*store)
                .await
                .late_writes_verified_through_unix_ns,
            later
        );

        let earlier = NOW1 + 10 * HOUR;
        let report = late_write_pass(&*store, &ckeys(), earlier, FAMILY_RLOG, skew())
            .await
            .expect("earlier pass");
        assert_eq!(
            report.watermark_unix_ns, later,
            "watermark did not move back"
        );
        assert_eq!(
            rlog_entry(&*store)
                .await
                .late_writes_verified_through_unix_ns,
            later
        );
    }
}

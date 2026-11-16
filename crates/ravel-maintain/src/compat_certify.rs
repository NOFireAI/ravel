//! `compat certify` for a class-A family, and the nested-section-version probe
//! the certificate census and the RLOG retention holds share (ADR-4233
//! decision 7 and hazard H5).
//!
//! # The H5 section-version probe
//!
//! A `.rlog` object's trailer version can be inside this build's reader window
//! while a nested POSTINGS or KEY_IDX section carries a grammar version this
//! build cannot read (ADR-4233 inventory: POSTINGS and KEY_IDX read sets are
//! `{1,2}`). The trailer-only version gate the retention holds and the census
//! trailer check use never sees that, because it reads only the 16-byte
//! trailer suffix. [`rlog_held_version`] closes it: it reads the
//! footer through the suffix-probe-then-range-chase protocol, then reads each
//! nested section's version through a ranged GET of its header and classifies
//! it against this build's read set, exactly as the reader does when it opens
//! the section for a query (`ravel_logseg::postings::PostingsSection::parse`
//! and `ravel_logseg::key_index`, both of which return
//! `LogSegError::UnsupportedSectionVersion` for a version outside their set).
//!
//! Cost per RLOG member: one suffix GET of the footer (a second ranged GET
//! only when the footer did not fit the suffix), plus one 1-byte ranged GET of
//! the POSTINGS header when the object carries a POSTINGS section, plus one
//! ranged GET of the KEY_IDX prefix when it carries a KEY_IDX section. An
//! object with neither section costs only the footer read; an object with both
//! costs at most four small GETs. An absent object, a structurally corrupt
//! object, and a section whose version is readable all return `None`: corrupt
//! is never a version hold, the same rule the trailer gate applies.

use std::ops::Range;

use bytes::Bytes;
use ravel_catalog::compat::{
    CompatError, CompatKeys, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, read_compat, update_compat,
};
use ravel_catalog::{
    CompatFenceOutcome, FloorBasis, ProvisioningError, provisioning_key, raise_compat_fence,
    raise_format_floor, read_generations_from_store,
};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_logseg::LogSegError;
use ravel_logseg::footer::{LogFooter, SuffixOutcome, kind};
use ravel_logseg::key_index;
use ravel_logseg::postings::{POSTINGS_VERSION, POSTINGS_VERSION_V1};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all_reporting};
use ravel_proto::sys::v1 as sysproto;
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
/// allowance. `P` is read from the record, `H` is [`H_NS`].
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
    /// A listing was incomplete: an unaddressable key under a census prefix.
    IncompleteListing { prefix: String, unaddressable: u64 },
}

/// What one `certify` run concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertifyReport {
    /// The census passed and a certificate was written under CAS.
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
    /// A member object this build cannot read blocks certification (its trailer
    /// version is outside the window, or a nested section is unreadable).
    HeldMember { member_key: String, version: u16 },
    /// The budget stopped the census before it covered every bucket. The
    /// cursor keeps `S`; a later run resumes from it.
    Incomplete { units_examined: usize },
    /// A precondition refused the run before any certificate was written.
    Refused(CertifyRefusal),
}

/// The certify cursor payload tag. Distinct from the compaction and migrate
/// cursor tags so a foreign payload is a rescan, never a misread.
const CERTIFY_CURSOR_TAG: u8 = 1;
/// tag(1) + S: i64 LE(8) + units_done: u32 LE(4).
const CERTIFY_CURSOR_LEN: usize = 13;

/// The store-wide certify cursor key for one family. Disjoint from the
/// per-tenant compaction and migrate cursors: it lives under `sys/maintain`,
/// never under a tenant's `maint/` directory.
fn certify_cursor_key(family: &str) -> String {
    format!("sys/maintain/certify/{family}/cursor")
}

/// What the certify cursor held: `S` and how many census units a prior run
/// finished. An absent or unrecognized cursor is a fresh census.
#[derive(Debug, Clone, Copy)]
struct CertifyCursor {
    start_unix_ns: i64,
    units_done: u32,
}

async fn read_certify_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<(CertifyCursor, ravel_object_store::Version)>> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let data = got.data;
            if data.len() == CERTIFY_CURSOR_LEN && data[0] == CERTIFY_CURSOR_TAG {
                let mut start_bytes = [0u8; 8];
                start_bytes.copy_from_slice(&data[1..9]);
                let mut units_bytes = [0u8; 4];
                units_bytes.copy_from_slice(&data[9..13]);
                let start = i64::from_le_bytes(start_bytes);
                let units = u32::from_le_bytes(units_bytes);
                Ok(Some((
                    CertifyCursor {
                        start_unix_ns: start,
                        units_done: units,
                    },
                    got.version,
                )))
            } else {
                // A foreign or truncated payload: rescan from the start, leaving
                // the stored cursor in place (ADR-4233 decision 4, H9).
                Ok(None)
            }
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(MaintainError::Store(err)),
    }
}

async fn write_certify_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
    cursor: CertifyCursor,
    prev: Option<ravel_object_store::Version>,
) -> Result<()> {
    let mut payload = Vec::with_capacity(CERTIFY_CURSOR_LEN);
    payload.push(CERTIFY_CURSOR_TAG);
    payload.extend_from_slice(&cursor.start_unix_ns.to_le_bytes());
    payload.extend_from_slice(&cursor.units_done.to_le_bytes());
    let opts = match prev {
        None => ravel_object_store::PutOptions::create_if_absent(),
        Some(version) => ravel_object_store::PutOptions {
            mode: ravel_object_store::PutMode::CasVersion(version),
            checksum: None,
        },
    };
    match store.put(key, Bytes::from(payload), opts).await {
        Ok(_) => Ok(()),
        // Advisory: a lost race means another pass advanced it; converge.
        Err(StoreError::AlreadyExists) | Err(StoreError::PreconditionFailed) => Ok(()),
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
/// history ever used, floored at the signal's pinned read shards and at one.
pub(crate) async fn scan_shards_for(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<u32> {
    let generations = read_generations_from_store(store, tenant, signal)
        .await
        .map_err(prov_err)?;
    let widest = generations
        .map(|gens| gens.iter().map(|g| g.shard_count).max().unwrap_or(0))
        .unwrap_or(0);
    Ok(widest.max(signal.fixed_read_shards()).max(1))
}

/// One data or part object the census checks, and the version its record
/// stamps for it.
struct Member {
    key: String,
    stamp: u32,
    created_unix_ns: i64,
}

/// Enumerate the member objects of one `(tenant, signal, shard)` by STRICT
/// listing of its commit prefix: every L0 commit record's data object, and
/// every compaction and rewrite record's parts. An unaddressable key under the
/// prefix refuses the run rather than shortening the population (ADR-4233
/// decision 6).
async fn census_shard(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> std::result::Result<Vec<Member>, CensusStop> {
    let prefix = keys::commit_shard_prefix(tenant, signal, shard).map_err(CensusStop::fatal)?;
    let listing = list_all_reporting(store, &prefix)
        .await
        .map_err(|e| CensusStop::Fatal(MaintainError::Store(e)))?;
    if listing.unaddressable.count != 0 {
        return Err(CensusStop::Refused(CertifyRefusal::IncompleteListing {
            prefix,
            unaddressable: listing.unaddressable.count,
        }));
    }
    let mut members = Vec::new();
    for meta in listing.objects {
        let entry = keys::partition_bucket_entry(&meta.key).map_err(CensusStop::fatal)?;
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .map_err(|e| CensusStop::Fatal(MaintainError::Store(e)))?;
        match entry {
            keys::BucketEntry::CommitRecord(_) => {
                let rec = record::decode(&got.data).map_err(CensusStop::fatal)?;
                members.push(Member {
                    key: keys::reconstruct_data_key(&rec).map_err(CensusStop::fatal)?,
                    stamp: rec.segment_format_version,
                    created_unix_ns: rec.created_unix_ns,
                });
            }
            keys::BucketEntry::CompactionRecord(_) => {
                let rec = record::decode_compaction(&got.data).map_err(CensusStop::fatal)?;
                for part in &rec.parts {
                    members.push(Member {
                        key: keys::reconstruct_l1_part_key(&rec, part)
                            .map_err(CensusStop::fatal)?,
                        stamp: part.segment_format_version,
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
                        stamp: part.segment_format_version,
                        created_unix_ns: rec.created_unix_ns,
                    });
                }
            }
            keys::BucketEntry::Tombstone(_) => {}
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
enum CensusStop {
    Fatal(MaintainError),
    Refused(CertifyRefusal),
}

impl CensusStop {
    fn fatal(err: impl Into<MaintainError>) -> Self {
        CensusStop::Fatal(err.into())
    }
}

/// The trailer version of a member when it is outside this build's reader
/// window, or, for RLOG, the grammar version of an unreadable nested section:
/// such a member cannot be confirmed at or above the target, so it holds the
/// certificate (ADR-4233 decision 7). `None` when the object is readable here.
async fn member_held_version(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    key: &str,
) -> Result<Option<u16>> {
    // RLOG also probes its nested section versions; one footer-suffix GET
    // serves both the trailer and the sections.
    if part_family_for(signal) == Some(FAMILY_RLOG) {
        return rlog_held_version(store, key).await;
    }
    // RSEG and RSPAN have no version-gated nested sections: a 16-byte trailer
    // suffix GET confirms the member is readable here.
    let got = match store.get(key, GetRange::Suffix(16)).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(None),
        Err(err) => return Err(MaintainError::Store(err)),
    };
    let tail = got.data;
    let total = got.total_size;
    let held = match part_family_for(signal) {
        Some(FAMILY_RSEG) => matches!(
            ravel_segment::open_from_suffix(
                tail.as_ref(),
                total,
                ravel_segment::ReaderLimits::default()
            ),
            Err(ravel_segment::SegmentError::UnsupportedVersion { .. })
        )
        .then_some(0u16),
        Some(FAMILY_RSPAN) => match ravel_rspan::open_from_suffix(tail.as_ref(), total) {
            Err(ravel_rspan::SpanSegError::UnsupportedVersion(v)) => Some(v),
            _ => None,
        },
        _ => None,
    };
    Ok(held)
}

/// The class-A family a signal's parts belong to.
pub(crate) fn part_family_for(signal: Signal) -> Option<&'static str> {
    match signal {
        Signal::Metrics => Some(FAMILY_RSEG),
        Signal::Logs | Signal::Alerts | Signal::Audit => Some(FAMILY_RLOG),
        Signal::Spans => Some(FAMILY_RSPAN),
        Signal::Profiles => None,
    }
}

fn compat_err(err: CompatError) -> MaintainError {
    MaintainError::Invariant(format!("sys/compat: {err}"))
}

/// Certify `target_version` for class-A `family` (ADR-4233 decision 7).
///
/// Refuses before the family's activation fence walk is complete and before
/// the time precondition `S` is gated on has passed. On the first run it
/// records `S` (now) in the resumable cursor; a resumed run keeps the stored
/// `S`, so the late-write pass examines records that landed after it. The
/// census is a strict listing of every member object of the family, each
/// member's stamp compared with the target and a 16-byte trailer suffix GET
/// confirming it is readable here; a member below the target makes the result
/// contradicted, an unreadable member holds it. On success the certificate is
/// written under CAS and the format floors are raised with their basis.
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
    if target_version > entry.active_write_version {
        return Ok(CertifyReport::Refused(CertifyRefusal::TargetAboveActive {
            target: target_version,
            active: entry.active_write_version,
        }));
    }
    if entry.fence_complete_unix_ns < entry.activated_unix_ns {
        return Ok(CertifyReport::Refused(CertifyRefusal::FenceWalkIncomplete));
    }

    let p_ns = if record.max_publish_lifetime_ns > 0 {
        record.max_publish_lifetime_ns
    } else {
        crate::compat_policy::DEFAULT_PUBLISH_LIFETIME_NS
    };
    let activation_term = entry
        .activated_unix_ns
        .saturating_add(H_NS)
        .saturating_add(p_ns)
        .saturating_add(timings.t_req_ns);
    let g_pre = timings.g_pre_ns(p_ns);

    // The tenant set, fixed before the census so the digest and the per-bucket
    // fence reads use the same population.
    let mut tenants = discover_tenants(store).await?;
    tenants.sort();

    // The newest non-born fence over every provisioned bucket: the second term
    // of the time precondition (a born-fenced bucket, time 0, is left out).
    let mut newest_fence = 0i64;
    for tenant in &tenants {
        for signal in signals {
            if let Some(fenced) = provisioning_fence(store, tenant, *signal).await?
                && fenced != 0
            {
                newest_fence = newest_fence.max(fenced);
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

    // `S`: kept from the cursor on resume, recorded now on the first run.
    let cursor_key = certify_cursor_key(family);
    let (start_unix_ns, mut units_done) = match read_certify_cursor(store, &cursor_key).await? {
        Some((cursor, _version)) => (cursor.start_unix_ns, cursor.units_done),
        None => {
            write_certify_cursor(
                store,
                &cursor_key,
                CertifyCursor {
                    start_unix_ns: now_ns,
                    units_done: 0,
                },
                None,
            )
            .await?;
            (now_ns, 0u32)
        }
    };

    // The ordered census units: (tenant, signal, shard), skipping what a prior
    // run finished.
    let mut units: Vec<(TenantHash, Signal, u32)> = Vec::new();
    for tenant in &tenants {
        for signal in signals {
            let shards = scan_shards_for(store, tenant, *signal).await?;
            for shard in 0..shards {
                units.push((*tenant, *signal, shard));
            }
        }
    }

    let mut member_count = 0u64;
    let mut newest_created = 0i64;
    let mut buckets_scanned = 0u64;
    // Per (tenant, signal) floor basis, accumulated over the census. The key
    // holds the signal's prefix because `Signal` is not `Ord`; the value keeps
    // the signal itself for the floor raise.
    let mut basis: std::collections::BTreeMap<(TenantHash, String), (Signal, u64, i64)> =
        std::collections::BTreeMap::new();
    let mut processed_this_run = 0usize;

    for (idx, (tenant, signal, shard)) in units.iter().enumerate() {
        if (idx as u32) < units_done {
            continue;
        }
        if let Some(cap) = budget
            && processed_this_run >= cap
        {
            // Persist progress and stop; the cursor keeps `S`. Re-read the
            // cursor version so the CAS overwrites the record created at the
            // run start (a stale `CreateIfAbsent` would silently keep
            // `units_done` at 0 and make the next run rescan from the start).
            let version = read_certify_cursor(store, &cursor_key)
                .await?
                .map(|(_, v)| v);
            write_certify_cursor(
                store,
                &cursor_key,
                CertifyCursor {
                    start_unix_ns,
                    units_done,
                },
                version,
            )
            .await?;
            return Ok(CertifyReport::Incomplete {
                units_examined: processed_this_run,
            });
        }

        // Fence the provisioned bucket to the family's activation epoch, as the
        // writers do, so a bucket provisioned at v3 after the eager walk is
        // caught by the per-bucket G_pre recheck below.
        fence_bucket(store, tenant, *signal, entry.activated_epoch, now_ns).await?;

        let members = match census_shard(store, tenant, *signal, *shard).await {
            Ok(members) => members,
            Err(CensusStop::Fatal(err)) => return Err(err),
            Err(CensusStop::Refused(refusal)) => return Ok(CertifyReport::Refused(refusal)),
        };
        buckets_scanned += 1;
        let bucket_basis = basis
            .entry((*tenant, signal.key_prefix().to_string()))
            .or_insert((*signal, 0, 0));
        for member in members {
            member_count += 1;
            bucket_basis.1 += 1;
            bucket_basis.2 = bucket_basis.2.max(member.created_unix_ns);
            newest_created = newest_created.max(member.created_unix_ns);
            if member.stamp < target_version {
                return Ok(CertifyReport::Contradicted {
                    member_key: member.key,
                    found_version: member.stamp,
                });
            }
            if let Some(version) = member_held_version(store, *signal, &member.key).await? {
                return Ok(CertifyReport::HeldMember {
                    member_key: member.key,
                    version,
                });
            }
        }
        units_done = (idx as u32).saturating_add(1);
        processed_this_run += 1;
    }

    // Per-bucket G_pre recheck immediately before the CAS: a bucket fenced too
    // recently (including one this run just fenced) holds the certificate.
    for tenant in &tenants {
        for signal in signals {
            if let Some(fenced) = provisioning_fence(store, tenant, *signal).await?
                && fenced != 0
                && now_ns
                    <= fenced
                        .saturating_add(g_pre)
                        .saturating_add(timings.clock_skew_allowance_ns)
            {
                return Ok(CertifyReport::Refused(
                    CertifyRefusal::BucketRecentlyFenced {
                        tenant_hash: tenant.to_hex(),
                        signal: *signal,
                        not_before_ns: fenced
                            .saturating_add(g_pre)
                            .saturating_add(timings.clock_skew_allowance_ns),
                    },
                ));
            }
        }
    }

    // Raise the format floors with their basis, as `migrate` does.
    for ((tenant, _sig_key), (signal, entries, newest)) in &basis {
        let family_str = part_family_for(*signal).unwrap_or(family);
        match raise_format_floor(
            store,
            tenant,
            *signal,
            family_str,
            target_version,
            FloorBasis {
                observed_entries: *entries,
                observed_newest_created_unix_ns: *newest,
                observed_shards: scan_shards_for(store, tenant, *signal).await?,
            },
            "compat-certify",
            now_ns,
        )
        .await
        {
            Ok(_) => {}
            // Already at or above the target, or no record to floor: neither
            // blocks a certificate whose census the bucket passed.
            Err(ProvisioningError::FloorNotAboveCurrent { .. })
            | Err(ProvisioningError::NoRecordForFloor { .. }) => {}
            Err(err) => return Err(prov_err(err)),
        }
    }

    // The tenant-set digest: blake3 over the sorted tenant hashes.
    let mut hasher = blake3::Hasher::new();
    for tenant in &tenants {
        hasher.update(&tenant.0);
    }
    let digest = hasher.finalize().as_bytes().to_vec();

    // Write the certificate and the watermark `S` under CAS.
    let certificate = sysproto::Certificate {
        version: target_version,
        certified_unix_ns: now_ns,
        member_count,
        newest_created_unix_ns: newest_created,
        buckets_scanned,
        tenant_set_digest: digest,
    };
    let family_owned = family.to_string();
    update_compat(store, keys_compat, |record| {
        let entry = record
            .families
            .iter_mut()
            .find(|f| f.family == family_owned)
            .ok_or_else(|| CompatError::FamilyNotInRecord {
                family: family_owned.clone(),
            })?;
        entry.certificate = Some(certificate.clone());
        entry.contradicted = false;
        if start_unix_ns > entry.late_writes_verified_through_unix_ns {
            entry.late_writes_verified_through_unix_ns = start_unix_ns;
        }
        Ok(())
    })
    .await
    .map_err(compat_err)?;

    // The certification completed: clear the cursor so a later re-certify of
    // this or a higher version starts from a fresh `S`.
    let _ = store.delete(&cursor_key).await;

    Ok(CertifyReport::Certified {
        version: target_version,
        member_count,
        buckets_scanned,
        newest_created_unix_ns: newest_created,
    })
}

/// The `compat_fenced_unix_ns` of a `(tenant, signal)` provisioning record, or
/// `None` when the record is absent.
async fn provisioning_fence(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<i64>> {
    use prost::Message;
    let key = provisioning_key(tenant, signal);
    match store.get(&key, GetRange::Full).await {
        Ok(got) => {
            let record = sysproto::ProvisioningRecord::decode(got.data.as_ref())
                .map_err(|e| MaintainError::Invariant(format!("{key}: {e}")))?;
            Ok(Some(record.compat_fenced_unix_ns))
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

/// A ranged GET, with an absent object reported as `None` (a delete another
/// pass completed between the footer read and this read is not a hold).
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

/// Open a `.rlog` object's footer through the suffix-probe-then-range-chase
/// protocol. `None` when the object is absent or structurally corrupt; neither
/// is a version hold.
/// What a footer probe found: an out-of-window trailer (a version hold on its
/// own), a decoded footer to probe sections against, or nothing holdable (the
/// object is absent or structurally corrupt).
enum FooterProbe {
    OutOfWindow(u16),
    Footer(Box<LogFooter>),
    Unreadable,
}

/// Open a `.rlog` object's footer with ONE suffix GET (a second ranged GET only
/// when the footer did not fit), classifying the trailer version in the same
/// read. `open_from_suffix` checks the trailer version before the footer
/// bounds, so an out-of-window trailer comes back as
/// [`FooterProbe::OutOfWindow`] from the first read.
async fn open_rlog_footer(store: &dyn ObjectStoreBackend, key: &str) -> Result<FooterProbe> {
    let probe = match store.get(key, GetRange::Suffix(FOOTER_PROBE_BYTES)).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(FooterProbe::Unreadable),
        Err(err) => return Err(MaintainError::Store(err)),
    };
    let total = probe.total_size;
    match ravel_logseg::open_from_suffix(probe.data.as_ref(), total) {
        Ok(SuffixOutcome::Ready(footer)) => Ok(FooterProbe::Footer(Box::new(footer))),
        Ok(SuffixOutcome::NeedRange { offset, len }) => {
            let Some(tail) = get_range(store, key, offset, offset + len).await? else {
                return Ok(FooterProbe::Unreadable);
            };
            match ravel_logseg::open_from_suffix(tail.as_ref(), total) {
                Ok(SuffixOutcome::Ready(footer)) => Ok(FooterProbe::Footer(Box::new(footer))),
                Err(LogSegError::UnsupportedVersion(version)) => {
                    Ok(FooterProbe::OutOfWindow(version))
                }
                _ => Ok(FooterProbe::Unreadable),
            }
        }
        Err(LogSegError::UnsupportedVersion(version)) => Ok(FooterProbe::OutOfWindow(version)),
        Err(_) => Ok(FooterProbe::Unreadable),
    }
}

/// The version an RLOG object is held on: its trailer version when outside this
/// build's reader window, else the grammar version of a nested POSTINGS or
/// KEY_IDX section outside this build's read set (ADR-4233 H5). `None` when the
/// object is readable here, absent, or structurally corrupt (corrupt is never a
/// version hold). One suffix GET serves both the trailer and the footer, so the
/// section probe adds no request for an object whose footer fits the suffix and
/// carries no nested section; the per-member cost in the module doc counts from
/// there.
pub(crate) async fn rlog_held_version(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<u16>> {
    let footer = match open_rlog_footer(store, key).await? {
        FooterProbe::OutOfWindow(version) => return Ok(Some(version)),
        FooterProbe::Footer(footer) => footer,
        FooterProbe::Unreadable => return Ok(None),
    };

    // POSTINGS: the section is stored raw at the section-comp level, so its
    // first byte is the grammar version the reader gates on.
    if let Some(desc) = footer.section(kind::POSTINGS)
        && let Some(head) = get_range(store, key, desc.offset, desc.offset + 1).await?
        && let Some(&version) = head.first()
        && version != POSTINGS_VERSION
        && version != POSTINGS_VERSION_V1
    {
        return Ok(Some(u16::from(version)));
    }

    // KEY_IDX: read the section prefix, then read its header version through
    // the same path the reader uses. A version outside the set is a typed
    // `UnsupportedSectionVersion`; a corrupt prefix is not a hold.
    if let Ok(Some(range)) = key_index::key_probe_prefix_range(&footer) {
        let Some(prefix) = get_range(store, key, range.start, range.end).await? else {
            return Ok(None);
        };
        if let Err(LogSegError::UnsupportedSectionVersion { version, .. }) =
            key_index::key_index_version(&footer, |_requested: Range<u64>| {
                Ok::<Bytes, LogSegError>(prefix.clone())
            })
        {
            return Ok(Some(version));
        }
    }

    Ok(None)
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_catalog::compat::{COMPAT_FORMAT_VERSION, FAMILY_RLOG, create_compat, read_compat};
    use ravel_catalog::{AbsentPolicy, raise_compat_fence, validate_or_adopt};
    use ravel_commit::record::NewCommitRecord;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
    use uuid::Uuid;

    use super::*;
    use crate::late_writes::late_write_pass;

    const HOUR: i64 = 3_600_000_000_000;
    const ACTIVATED: i64 = 1_000;
    /// Far enough past `ACTIVATED` to clear the activation and `G_pre` terms.
    const NOW1: i64 = 400_000 * HOUR;

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

    fn tenant() -> TenantHash {
        TenantHash([5; 16])
    }

    /// Create `sys/compat` with `rlog` active at `active`, activated at
    /// `ACTIVATED`, and `fence_complete` for the fence-walk precondition.
    async fn seed_compat(store: &dyn ObjectStoreBackend, active: u32, fence_complete: i64) {
        create_compat(
            store,
            &ckeys(),
            sysproto::CompatRecord {
                format_version: COMPAT_FORMAT_VERSION,
                baseline_release_major: 0,
                baseline_release_minor: 34,
                families: vec![sysproto::CompatFamily {
                    family: FAMILY_RLOG.to_string(),
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
                max_publish_lifetime_ns: HOUR,
                adopted: Vec::new(),
                mac: Vec::new(),
            },
        )
        .await
        .expect("create sys/compat");
    }

    /// Provision the logs bucket at `shards` and fence it at `ACTIVATED`, an old
    /// time, so its `G_pre` has long passed and the time gate and recheck admit
    /// it.
    async fn provision_logs_fenced(store: &dyn ObjectStoreBackend, shards: u32) {
        validate_or_adopt(
            store,
            &tenant(),
            Signal::Logs,
            shards,
            ACTIVATED,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision logs");
        raise_compat_fence(store, &tenant(), Signal::Logs, 1, ACTIVATED)
            .await
            .expect("fence logs");
    }

    /// Seed one logs commit record (no data object) at `shard`, stamped
    /// `stamp`, with store `LastModified` set to `modified_ms`.
    async fn seed_commit(
        store: &MemoryStore,
        shard: u32,
        seq: u64,
        stamp: u32,
        modified_ms: u64,
    ) -> String {
        store.set_clock_ms(modified_ms);
        let ts_ns = i64::from(shard) * HOUR + (seq as i64) * 1_000_000;
        let commit = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Logs,
            shard,
            writer_id: Uuid::from_u128(u128::from(seq) + 1),
            writer_epoch: 1,
            writer_seq: seq,
            object_size: 1,
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
        key
    }

    /// The `rlog` family entry of `sys/compat`.
    async fn rlog_entry(store: &dyn ObjectStoreBackend) -> sysproto::CompatFamily {
        read_compat(store, &ckeys())
            .await
            .expect("read")
            .expect("present")
            .0
            .families
            .into_iter()
            .find(|f| f.family == FAMILY_RLOG)
            .expect("rlog entry")
    }

    fn rlog_object_bytes() -> Vec<u8> {
        rlog_object_with_sections(false)
    }

    async fn put(store: &dyn ObjectStoreBackend, key: &str, bytes: Vec<u8>) {
        store
            .put(key, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put object");
    }

    #[tokio::test]
    async fn a_readable_object_is_not_held() {
        let store = MemoryStore::new();
        put(&store, "obj", rlog_object_bytes()).await;
        assert_eq!(rlog_held_version(&store, "obj").await.expect("probe"), None);
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
    }

    /// The trailer version is untouched; the hold is on the section version
    /// alone. A reader confirms the trailer is still in window (so the object
    /// would pass the trailer-only gate and be deleted without this probe).
    #[tokio::test]
    async fn the_trailer_stays_in_window() {
        let store = MemoryStore::new();
        put(&store, "obj", rlog_object_with_sections(true)).await;
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
    }

    /// certify refuses before the activation's fence walk is recorded complete.
    #[tokio::test]
    async fn certify_refuses_before_fence_complete() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, 0).await;
        let report = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None)
            .await
            .expect("certify");
        assert_eq!(
            report,
            CertifyReport::Refused(CertifyRefusal::FenceWalkIncomplete)
        );
    }

    /// certify refuses, and records no `S`, before the time precondition passes.
    #[tokio::test]
    async fn certify_refuses_before_the_time_gate() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        // `now` just above activation but below activation + H + P + T_req.
        let report = certify(
            &store,
            &ckeys(),
            ACTIVATED + 1,
            FAMILY_RLOG,
            6,
            timings(),
            None,
        )
        .await
        .expect("certify");
        assert!(matches!(
            report,
            CertifyReport::Refused(CertifyRefusal::TimeGate { .. })
        ));
        // No cursor was written: `S` is gated on the time precondition.
        assert!(
            store
                .get(&certify_cursor_key(FAMILY_RLOG), GetRange::Full)
                .await
                .is_err()
        );
    }

    /// A member object below the target makes the result contradicted.
    #[tokio::test]
    async fn a_lower_version_object_yields_contradicted() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        provision_logs_fenced(&store, 1).await;
        seed_commit(&store, 0, 1, 5, 1_000).await; // stamp 5, below target 6
        let report = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None)
            .await
            .expect("certify");
        assert!(
            matches!(
                report,
                CertifyReport::Contradicted {
                    found_version: 5,
                    ..
                }
            ),
            "{report:?}"
        );
    }

    /// A member whose nested section is unreadable holds the certificate.
    #[tokio::test]
    async fn a_held_member_is_counted() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        provision_logs_fenced(&store, 1).await;
        let commit_key = seed_commit(&store, 0, 1, 6, 1_000).await;
        // Place the stamped-6 commit's data object with a bumped POSTINGS
        // section, so its stamp is in range but the object is unreadable.
        let commit = record::decode(
            store
                .get(&commit_key, GetRange::Full)
                .await
                .expect("get commit")
                .data
                .as_ref(),
        )
        .expect("decode");
        let data_key = ravel_commit::keys::reconstruct_data_key(&commit).expect("data key");
        store
            .put(
                &data_key,
                Bytes::from(rlog_object_with_sections(true)),
                PutOptions::default(),
            )
            .await
            .expect("put data");
        let report = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None)
            .await
            .expect("certify");
        assert!(
            matches!(report, CertifyReport::HeldMember { .. }),
            "{report:?}"
        );
    }

    /// An incomplete listing (an unaddressable key under a census prefix)
    /// refuses the run: a strict census never shortens its population.
    #[tokio::test]
    async fn an_incomplete_listing_blocks() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        provision_logs_fenced(&store, 1).await;
        let prefix =
            ravel_commit::keys::commit_shard_prefix(&tenant(), Signal::Logs, 0).expect("prefix");
        // A key under the census prefix the census cannot address: a strict
        // census blocks on it rather than certifying over a shortened
        // population. A non-strict census would skip it and certify.
        store.insert_foreign(
            &format!("{prefix}!!unaddressable"),
            Bytes::from_static(b"x"),
        );
        let result = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None).await;
        assert!(
            !matches!(result, Ok(CertifyReport::Certified { .. })),
            "a strict census must block an unaddressable census key: {result:?}"
        );
    }

    /// certify fences a bucket provisioned at version 3 after the eager walk,
    /// and the per-bucket `G_pre` recheck then holds the certificate until that
    /// fresh fence's own `G_pre` has passed.
    #[tokio::test]
    async fn the_per_bucket_g_pre_recheck_refuses_a_recently_fenced_bucket() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        // Provisioned at version 3, unfenced: the initial time gate sees no
        // fence, but certify fences it at `now`, and the recheck refuses.
        validate_or_adopt(
            &store,
            &tenant(),
            Signal::Logs,
            1,
            ACTIVATED,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
        let report = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None)
            .await
            .expect("certify");
        assert!(
            matches!(
                report,
                CertifyReport::Refused(CertifyRefusal::BucketRecentlyFenced { .. })
            ),
            "{report:?}"
        );
    }

    /// A clean census certifies, writing the certificate and seeding the
    /// late-write watermark with `S`.
    #[tokio::test]
    async fn a_clean_census_certifies() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        provision_logs_fenced(&store, 1).await;
        seed_commit(&store, 0, 1, 6, 1_000).await;
        let report = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None)
            .await
            .expect("certify");
        assert!(
            matches!(report, CertifyReport::Certified { version: 6, .. }),
            "{report:?}"
        );
        let entry = rlog_entry(&store).await;
        assert_eq!(entry.certificate.expect("cert").version, 6);
        assert_eq!(
            entry.late_writes_verified_through_unix_ns, NOW1,
            "watermark is S"
        );
        // The format floor was raised to the certified version.
        assert_eq!(
            ravel_catalog::current_floor_from_store(&store, &tenant(), Signal::Logs, FAMILY_RLOG)
                .await
                .expect("floor read"),
            Some(6),
            "certify raised the rlog floor to the certified version"
        );
        // The completed certification cleared its cursor.
        assert!(
            store
                .get(&certify_cursor_key(FAMILY_RLOG), GetRange::Full)
                .await
                .is_err()
        );
    }

    /// DISTINGUISHING (rules out S recomputed on resume, and a watermark that
    /// moves backward): a late lower-version record lands in a shard-hour the
    /// first certify run already read, between two resumed runs. The resumed
    /// run keeps the first run's `S`, so the certificate's watermark is `S`
    /// (the earlier time), and the late-write pass examines the record by its
    /// `LastModified` and finds it. Had the resumed run recomputed `S` to its
    /// own later start, the watermark would be past the late write's
    /// `LastModified` and the pass would miss it.
    #[tokio::test]
    async fn late_write_between_resumed_runs_is_found_by_the_late_write_pass() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        provision_logs_fenced(&store, 2).await;

        // The census-time population: one stamp-6 record in each of the two
        // logs shards. Shard 0 is census unit 0, shard 1 is unit 1.
        let old_ms = (NOW1 / 1_000_000) as u64 - 10_000;
        seed_commit(&store, 0, 1, 6, old_ms).await;
        seed_commit(&store, 1, 2, 6, old_ms).await;

        // Run 1 covers only unit 0 (shard 0), records S = NOW1, and stops.
        let run1 = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), Some(1))
            .await
            .expect("run 1");
        assert!(matches!(run1, CertifyReport::Incomplete { .. }), "{run1:?}");

        // A late write lands in shard 0 (already read by run 1) below the
        // target, with a LastModified after S.
        let late_ms = (NOW1 / 1_000_000) as u64 + 3_600_000; // S + 1h
        let late_key = seed_commit(&store, 0, 9, 5, late_ms).await;

        // Run 2 resumes from unit 1 (shard 1), skips shard 0, and certifies.
        // It keeps run 1's S as the watermark.
        let now2 = NOW1 + 10 * HOUR;
        let run2 = certify(&store, &ckeys(), now2, FAMILY_RLOG, 6, timings(), None)
            .await
            .expect("run 2");
        assert!(matches!(run2, CertifyReport::Certified { .. }), "{run2:?}");
        let entry = rlog_entry(&store).await;
        assert_eq!(
            entry.late_writes_verified_through_unix_ns, NOW1,
            "the resumed run kept the first run's S"
        );

        // Had S been recomputed to run 2's start, the watermark would be past
        // the late write's LastModified, and the pass below would skip it.
        let late_modified_ns = (late_ms as i64) * 1_000_000;
        assert!(
            late_modified_ns < now2 - timings().clock_skew_allowance_ns,
            "the late write predates a recomputed-S watermark, so only keeping S finds it"
        );

        // The late-write pass, at the kept watermark S, examines the late write
        // and contradicts the certificate.
        let report = late_write_pass(
            &store,
            &ckeys(),
            now2 + HOUR,
            FAMILY_RLOG,
            timings().clock_skew_allowance_ns,
        )
        .await
        .expect("late-write pass");
        assert!(
            report
                .late_writes
                .iter()
                .any(|lw| lw.record_key == late_key),
            "the late write was found: {report:?}"
        );
        assert!(
            rlog_entry(&store).await.contradicted,
            "certificate contradicted"
        );
    }

    /// DISTINGUISHING (rules out a watermark that moves backward): a later pass
    /// that starts earlier than a prior pass's start never lowers the
    /// watermark.
    #[tokio::test]
    async fn the_watermark_never_moves_backward() {
        let store = MemoryStore::new();
        seed_compat(&store, 6, ACTIVATED).await;
        provision_logs_fenced(&store, 1).await;
        seed_commit(&store, 0, 1, 6, 1_000).await;
        certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None)
            .await
            .expect("certify");

        // A pass at a later now advances the watermark.
        let later = NOW1 + 100 * HOUR;
        late_write_pass(
            &store,
            &ckeys(),
            later,
            FAMILY_RLOG,
            timings().clock_skew_allowance_ns,
        )
        .await
        .expect("later pass");
        assert_eq!(
            rlog_entry(&store)
                .await
                .late_writes_verified_through_unix_ns,
            later
        );

        // A pass at an earlier now (out of order) must not lower it.
        let earlier = NOW1 + 10 * HOUR;
        let report = late_write_pass(
            &store,
            &ckeys(),
            earlier,
            FAMILY_RLOG,
            timings().clock_skew_allowance_ns,
        )
        .await
        .expect("earlier pass");
        assert_eq!(
            report.watermark_unix_ns, later,
            "watermark did not move back"
        );
        assert_eq!(
            rlog_entry(&store)
                .await
                .late_writes_verified_through_unix_ns,
            later
        );
    }

    /// A truncated listing (a faulted LIST page) blocks certification rather
    /// than certifying over a shortened population.
    #[tokio::test]
    async fn a_faulted_listing_blocks_certification() {
        let inner = MemoryStore::new();
        seed_compat(&inner, 6, ACTIVATED).await;
        provision_logs_fenced(&inner, 1).await;
        seed_commit(&inner, 0, 1, 6, 1_000).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Permanent("list down".to_string()))
                .with_key_contains("/c/"),
        );
        let store = FaultStore::new(inner, plan);
        let result = certify(&store, &ckeys(), NOW1, FAMILY_RLOG, 6, timings(), None).await;
        assert!(
            result.is_err(),
            "a faulted listing must block, got {result:?}"
        );
    }
}

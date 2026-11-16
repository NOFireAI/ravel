//! The late-write pass that keeps a certificate checkable (ADR-4233 decision
//! 7).
//!
//! A certificate proves every live member of a class-A family was at or above
//! the certified version at census time `S`. After certification a paused
//! in-contract writer, or a pre-baseline process that outlived the rollout, can
//! still land an object below that version. The late-write pass detects it: it
//! lists the commit, compaction and rewrite records of every signal of the
//! family and GETs only the records whose store `LastModified` is after the
//! persisted watermark minus the clock-skew allowance; any such record below
//! the certified version is a late write, which CAS-sets `contradicted` on the
//! family entry.
//!
//! The watermark (`late_writes_verified_through_unix_ns`) is monotone across
//! runs: a completed pass advances it to its own start only when that is later
//! than the stored value, so two passes finishing out of order never move it
//! back. `certify` seeds it with `S`, the start of the first census run, so the
//! first late-write pass examines every record that landed after the census
//! began even on a resumed certification that kept the earlier `S`.

use prost::Message;
use ravel_catalog::compat::{CompatError, CompatKeys, read_compat, update_compat};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all_reporting};

use crate::compat_certify::{family_signals, scan_shards_for};
use crate::discover::discover_tenants;
use crate::error::{MaintainError, Result};

/// One record found below the certified version after certification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LateWrite {
    pub record_key: String,
    pub version: u32,
}

/// What one late-write pass concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LateWriteReport {
    /// Records GETted because their `LastModified` was after the watermark
    /// minus the skew allowance.
    pub examined: usize,
    /// Late writes found: records below the certified version. When non-empty
    /// the family entry's `contradicted` flag was set.
    pub late_writes: Vec<LateWrite>,
    /// The watermark after this pass. Never below the value it read.
    pub watermark_unix_ns: i64,
    /// Whether the family carried a certificate to verify against. With none,
    /// the pass examines nothing and leaves the watermark untouched.
    pub certified: bool,
}

fn compat_err(err: CompatError) -> MaintainError {
    MaintainError::Invariant(format!("sys/compat: {err}"))
}

/// The version a commit-family record stamps: a commit record's segment format
/// version, or the lowest part version of a compaction or rewrite record (the
/// lowest is what a certificate at or above it would be contradicted by).
fn record_version(key: &str, bytes: &[u8]) -> Result<Option<u32>> {
    use ravel_commit::keys::{BucketEntry, partition_bucket_entry};
    match partition_bucket_entry(key)? {
        BucketEntry::CommitRecord(_) => Ok(Some(record::decode(bytes)?.segment_format_version)),
        BucketEntry::CompactionRecord(_) => {
            let rec = record::decode_compaction(bytes)?;
            Ok(rec.parts.iter().map(|p| p.segment_format_version).min())
        }
        BucketEntry::RewriteRecord(_) => {
            let rec = ravel_proto::commit::v1::RewriteRecord::decode(bytes)
                .map_err(|e| MaintainError::Invariant(format!("rewrite record decode: {e}")))?;
            Ok(rec.parts.iter().map(|p| p.segment_format_version).min())
        }
        BucketEntry::Tombstone(_) => Ok(None),
    }
}

/// Run the late-write pass for class-A `family` at `now_ns`, examining records
/// landed since the certificate's watermark minus `clock_skew_allowance_ns`.
///
/// Reads `sys/compat`; with no certificate for the family the pass is a no-op.
/// A found record below the certified version CAS-sets `contradicted`. The
/// watermark is advanced to `now_ns` only when that is later than the stored
/// value, so it never moves backward.
pub async fn late_write_pass(
    store: &dyn ObjectStoreBackend,
    keys_compat: &CompatKeys,
    now_ns: i64,
    family: &str,
    clock_skew_allowance_ns: i64,
) -> Result<LateWriteReport> {
    let Some(signals) = family_signals(family) else {
        return Err(MaintainError::Invariant(format!(
            "late-write pass: {family} is not a class-A family"
        )));
    };

    let Some((record, _version)) = read_compat(store, keys_compat).await.map_err(compat_err)?
    else {
        return Ok(LateWriteReport {
            examined: 0,
            late_writes: Vec::new(),
            watermark_unix_ns: 0,
            certified: false,
        });
    };
    let Some(entry) = record.families.iter().find(|f| f.family == family) else {
        return Ok(LateWriteReport {
            examined: 0,
            late_writes: Vec::new(),
            watermark_unix_ns: 0,
            certified: false,
        });
    };
    let Some(certificate) = entry.certificate.as_ref() else {
        return Ok(LateWriteReport {
            examined: 0,
            late_writes: Vec::new(),
            watermark_unix_ns: entry.late_writes_verified_through_unix_ns,
            certified: false,
        });
    };
    let certified_version = certificate.version;
    let watermark = entry.late_writes_verified_through_unix_ns;
    let threshold_ns = watermark.saturating_sub(clock_skew_allowance_ns);

    let mut tenants = discover_tenants(store).await?;
    tenants.sort();

    let mut examined = 0usize;
    let mut late_writes = Vec::new();
    for tenant in &tenants {
        for signal in signals {
            let shards = scan_shards_for(store, tenant, *signal).await?;
            for shard in 0..shards {
                let prefix = ravel_commit::keys::commit_shard_prefix(tenant, *signal, shard)?;
                let listing = list_all_reporting(store, &prefix).await?;
                if listing.unaddressable.count != 0 {
                    return Err(MaintainError::Invariant(format!(
                        "late-write pass: {} unaddressable keys under {prefix}",
                        listing.unaddressable.count
                    )));
                }
                for meta in listing.objects {
                    // `LastModified` is milliseconds; the watermark is
                    // nanoseconds. A record whose millisecond stamp falls on or
                    // after the threshold is examined.
                    let modified_ns = meta.last_modified_unix_ms.saturating_mul(1_000_000);
                    if modified_ns <= threshold_ns {
                        continue;
                    }
                    let got = store.get(&meta.key, GetRange::Full).await?;
                    if let Some(version) = record_version(&meta.key, got.data.as_ref())? {
                        examined += 1;
                        if version < certified_version {
                            late_writes.push(LateWrite {
                                record_key: meta.key.clone(),
                                version,
                            });
                        }
                    }
                }
            }
        }
    }

    let contradicted = !late_writes.is_empty();
    let family_owned = family.to_string();
    let (updated, _) = update_compat(store, keys_compat, |record| {
        let entry = record
            .families
            .iter_mut()
            .find(|f| f.family == family_owned)
            .ok_or_else(|| CompatError::FamilyNotInRecord {
                family: family_owned.clone(),
            })?;
        if contradicted {
            entry.contradicted = true;
        }
        if now_ns > entry.late_writes_verified_through_unix_ns {
            entry.late_writes_verified_through_unix_ns = now_ns;
        }
        Ok(())
    })
    .await
    .map_err(compat_err)?;

    let watermark_unix_ns = updated
        .families
        .iter()
        .find(|f| f.family == family)
        .map(|f| f.late_writes_verified_through_unix_ns)
        .unwrap_or(watermark);

    Ok(LateWriteReport {
        examined,
        late_writes,
        watermark_unix_ns,
        certified: true,
    })
}

//! The format-floor check a maintenance run makes before it writes (ADR-1746
//! decision 4).
//!
//! Compaction, the erasure rewrite, a format migration and a re-encode each
//! write parts at their compat run's version for the bucket's signal, or at
//! [`crate::rewrite::current_part_version`] with no run. Before any of them
//! takes the bucket's claim, builds a part or PUTs anything, it reads the
//! bucket's provisioning record once and compares that version with the
//! recorded floor for the signal's family, through the same
//! [`FormatFloorPolicy`] ingest uses: below the floor the run refuses with
//! [`MaintainError::BelowFormatFloor`], or, under `--write-below-format-floor`,
//! writes and the policy counts it. The read is fresh, not cached, and is
//! counted under [`RequestPhase::FloorRead`] when a ledger is installed.

use prost::Message;
use ravel_catalog::{current_floor, provisioning_key, read_floors_checked};
use ravel_ingest::{FloorAdmission, FormatFamily, FormatFloorPolicy};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;

use crate::bucket::Bucket;
use crate::compat_policy::{CompatRun, part_write_version};
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::request_ledger::{RequestLedger, RequestPhase, note_get};

/// Check the floor before a `run` (named in the log line) writes into
/// `bucket`, with `config`'s policy; a no-op when none is installed. `compat`
/// is the run's compat run, whose part version is the one written.
pub(crate) async fn check_output_floor(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    run: &'static str,
    compat: Option<&CompatRun>,
) -> Result<()> {
    match config.format_floor.as_ref() {
        Some(policy) => {
            let writing = part_write_version(compat, bucket.signal)?;
            check_with_policy(
                store,
                config.request_ledger.as_ref(),
                policy,
                bucket,
                run,
                writing,
            )
            .await
        }
        None => Ok(()),
    }
}

/// [`check_output_floor`] with an explicit policy and part version.
pub(crate) async fn check_with_policy(
    store: &dyn ObjectStoreBackend,
    ledger: Option<&RequestLedger>,
    policy: &FormatFloorPolicy,
    bucket: &Bucket,
    run: &'static str,
    writing: u32,
) -> Result<()> {
    let Some(family) = FormatFamily::for_signal(bucket.signal) else {
        return Ok(());
    };
    let floor = read_floor(store, ledger, bucket, family).await?;
    match policy.admit(family, floor, writing) {
        Ok(FloorAdmission::AtOrAboveFloor) => Ok(()),
        Ok(FloorAdmission::BelowFloorOverridden(below)) => {
            tracing::warn!(
                run,
                tenant_hash = %bucket.tenant_hash.to_hex(),
                signal = ?bucket.signal,
                shard = bucket.shard,
                ingest_hour_bucket = bucket.ingest_hour_bucket,
                family = below.family.as_str(),
                floor = below.floor,
                writing = below.writing,
                "maintenance: writing below the bucket's recorded format floor under \
                 --write-below-format-floor; the floor is now contradicted (ADR-1746 decision 5)"
            );
            Ok(())
        }
        Err(refused) => {
            tracing::warn!(
                run,
                tenant_hash = %bucket.tenant_hash.to_hex(),
                signal = ?bucket.signal,
                shard = bucket.shard,
                ingest_hour_bucket = bucket.ingest_hour_bucket,
                family = refused.family.as_str(),
                floor = refused.floor,
                writing = refused.writing,
                "maintenance: refusing to write below the bucket's recorded format floor; \
                 nothing built or published (ADR-1746 decision 4)"
            );
            Err(MaintainError::BelowFormatFloor(refused))
        }
    }
}

/// The bucket's recorded floor for `family`, read fresh: `None` when the
/// record is absent or records no floor for the family. A store error is
/// [`MaintainError::Store`]; a record that does not decode, is misfiled or has
/// a corrupt floor history is [`MaintainError::FormatFloorRead`].
async fn read_floor(
    store: &dyn ObjectStoreBackend,
    ledger: Option<&RequestLedger>,
    bucket: &Bucket,
    family: FormatFamily,
) -> Result<Option<u32>> {
    let key = provisioning_key(&bucket.tenant_hash, bucket.signal);
    let got = store.get(&key, GetRange::Full).await;
    note_get(ledger, RequestPhase::FloorRead, &got);
    match got {
        Ok(outcome) => {
            let record = sysproto::ProvisioningRecord::decode(outcome.data.as_ref())
                .map_err(|err| MaintainError::FormatFloorRead(format!("{key}: {err}")))?;
            let floors = read_floors_checked(&record, &key, &bucket.tenant_hash, bucket.signal)
                .map_err(|err| MaintainError::FormatFloorRead(err.to_string()))?;
            Ok(current_floor(&floors, family.as_str()))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(MaintainError::Store(err)),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use ravel_catalog::{AbsentPolicy, FloorBasis, raise_format_floor, validate_or_adopt};
    use ravel_ingest::{BelowFormatFloor, FormatFloorMetrics};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::{Signal, TenantHash};

    use super::*;
    use crate::compat_policy::FamilyVersions;
    use crate::compat_policy::test_support::{ManualClock, T0, family, policy, write_compat};
    use ravel_catalog::compat::FAMILY_RSEG;

    fn current() -> u32 {
        u32::from(ravel_segment::SegmentVersion::CURRENT.number())
    }

    fn bucket() -> Bucket {
        Bucket::new(TenantHash([5; 16]), Signal::Metrics, 0, 1)
    }

    /// A metrics bucket whose RSEG floor is one above the build's current
    /// version.
    async fn floored_store() -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        let bucket = bucket();
        validate_or_adopt(
            store.as_ref(),
            &bucket.tenant_hash,
            bucket.signal,
            1,
            1,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
        raise_format_floor(
            store.as_ref(),
            &bucket.tenant_hash,
            bucket.signal,
            FormatFamily::Rseg.as_str(),
            current() + 1,
            FloorBasis {
                observed_entries: 0,
                observed_newest_created_unix_ns: 0,
                observed_shards: 1,
            },
            "test",
            2,
        )
        .await
        .expect("raise floor");
        store
    }

    fn refusing() -> FormatFloorPolicy {
        FormatFloorPolicy::new(false, Arc::new(FormatFloorMetrics::default()))
    }

    fn assert_below(err: &MaintainError, writing: u32) {
        assert!(
            matches!(
                err,
                MaintainError::BelowFormatFloor(BelowFormatFloor {
                    family: FormatFamily::Rseg,
                    floor,
                    writing: w,
                }) if *floor == current() + 1 && *w == writing
            ),
            "{err:?}"
        );
    }

    /// The floor is compared with the version the caller writes, not the
    /// build's current one: one above current passes a floor that refuses
    /// current.
    #[tokio::test]
    async fn the_floor_is_checked_against_the_given_version() {
        let store = floored_store().await;
        let policy = refusing();

        let err = check_with_policy(store.as_ref(), None, &policy, &bucket(), "test", current())
            .await
            .expect_err("current is below the floor");
        assert_below(&err, current());
        check_with_policy(
            store.as_ref(),
            None,
            &policy,
            &bucket(),
            "test",
            current() + 1,
        )
        .await
        .expect("the run's version is at the floor");
    }

    /// `check_output_floor` checks the part version the compat run chose,
    /// and the build's current one with no run.
    #[tokio::test]
    async fn the_output_floor_uses_the_runs_version() {
        let store = floored_store().await;
        let next = current() + 1;
        let next_u16 = u16::try_from(next).expect("u16");
        write_compat(
            store.as_ref(),
            vec![family(FAMILY_RSEG, next)],
            3_600_000_000_000,
        )
        .await;
        let compat = policy(store.clone(), ManualClock::at(T0))
            .await
            .with_family(
                FAMILY_RSEG,
                FamilyVersions {
                    writable: vec![ravel_segment::SegmentVersion::CURRENT.number(), next_u16],
                    baseline: ravel_segment::SegmentVersion::CURRENT.number(),
                },
            );
        let bucket = bucket();
        let run = compat
            .begin(
                "test",
                &[FAMILY_RSEG],
                Some((bucket.tenant_hash, bucket.signal)),
                None,
                0,
            )
            .expect("begin");
        assert_eq!(run.version(FAMILY_RSEG), Some(next_u16));
        let config = CompactorConfig {
            format_floor: Some(refusing()),
            ..CompactorConfig::default()
        };

        check_output_floor(store.as_ref(), &config, &bucket, "test", Some(&run))
            .await
            .expect("the run writes at the floor");
        let err = check_output_floor(store.as_ref(), &config, &bucket, "test", None)
            .await
            .expect_err("no run writes current, below the floor");
        assert_below(&err, current());
    }
}

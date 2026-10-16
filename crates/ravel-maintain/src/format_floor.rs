//! The format-floor check a maintenance run makes before it writes (ADR-1746
//! decision 4).
//!
//! Compaction, the erasure rewrite, a format migration and a re-encode each
//! write parts at [`crate::rewrite::current_part_version`] for the bucket's
//! signal. Before any of them takes the bucket's claim, builds a part or PUTs
//! anything, it reads the bucket's provisioning record once and compares that
//! version with the recorded floor for the signal's family, through the same
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
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::request_ledger::{RequestLedger, RequestPhase, note_get};

/// Check the floor before a `run` (named in the log line) writes into
/// `bucket`, with `config`'s policy; a no-op when none is installed.
pub(crate) async fn check_output_floor(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    run: &'static str,
) -> Result<()> {
    match config.format_floor.as_ref() {
        Some(policy) => {
            check_with_policy(store, config.request_ledger.as_ref(), policy, bucket, run).await
        }
        None => Ok(()),
    }
}

/// [`check_output_floor`] with an explicit policy.
pub(crate) async fn check_with_policy(
    store: &dyn ObjectStoreBackend,
    ledger: Option<&RequestLedger>,
    policy: &FormatFloorPolicy,
    bucket: &Bucket,
    run: &'static str,
) -> Result<()> {
    let Some(family) = FormatFamily::for_signal(bucket.signal) else {
        return Ok(());
    };
    let writing = crate::rewrite::current_part_version(bucket.signal)?;
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

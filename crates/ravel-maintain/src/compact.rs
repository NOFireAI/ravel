//! The per-bucket compaction driver: seal + trigger checks, then the
//! plan-build-publish pipeline (docs/compaction-retention-plan.md §3.2-§3.4).
//! Stateless and idempotent: a crashed run re-run from scratch reuses
//! content-addressed part keys and converges at the record's
//! `CreateIfAbsent` (plan §3.6).

use ravel_object_store::ObjectStoreBackend;

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::Result;
use crate::publish::{PublishOutcome, publish_record};
use crate::read::{input_set_hash, list_bucket, load_input_catalog, load_inputs};
use crate::{build, read};

/// The result of a `compact_bucket` call. Every variant except
/// [`CompactionOutcome::Compacted`] means the bucket was left untouched, with
/// the reason; the scan driver treats all of them except `NotSealed` as
/// "this hour is done" for cursor advancement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionOutcome {
    /// Not yet sealed: the writer interlock does not yet guarantee a complete
    /// input set (plan §3.2). Later hours are also unsealed.
    NotSealed,
    /// A retention tombstone is present: the bucket contributes nothing and is
    /// never compacted (ADR-0019, plan §3.2).
    Tombstoned,
    /// A compaction record already exists; nothing to do.
    AlreadyCompacted,
    /// Fewer than `min_compaction_inputs` L0 records; not worth compacting.
    BelowMinInputs { count: usize },
    /// Built and published (or converged / abandoned): `parts` parts written,
    /// `publish` records how the record PUT resolved.
    Compacted {
        parts: usize,
        publish: PublishOutcome,
    },
}

/// Compact one sealed bucket end to end (plan §3.2-§3.4). Safe to call
/// concurrently with other compactors over the same bucket: the record's
/// `CreateIfAbsent` picks a single winner and losers converge.
pub async fn compact_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> Result<CompactionOutcome> {
    let start_ns = clock.now_ns();
    if !bucket.is_sealed(start_ns, config) {
        return Ok(CompactionOutcome::NotSealed);
    }

    let listing = list_bucket(store, bucket).await?;
    if listing.tombstone_key.is_some() {
        return Ok(CompactionOutcome::Tombstoned);
    }
    if !listing.compaction_record_keys.is_empty() {
        return Ok(CompactionOutcome::AlreadyCompacted);
    }
    if listing.commit_keys.len() < config.min_compaction_inputs {
        return Ok(CompactionOutcome::BelowMinInputs {
            count: listing.commit_keys.len(),
        });
    }

    let inputs = load_inputs(store, bucket, &listing.commit_keys).await?;
    let hash = input_set_hash(&inputs);

    // Catalogs aligned one-to-one with `inputs` (canonical order): the merge
    // relies on that alignment for deterministic run tie-breaking.
    let mut catalogs = Vec::with_capacity(inputs.len());
    for input in &inputs {
        catalogs.push(load_input_catalog(store, config, input).await?);
    }

    let parts = build::build_parts(store, config, bucket, &inputs, &catalogs, &hash).await?;

    let publish = publish_record(
        store, config, clock, bucket, &inputs, &hash, &parts, start_ns,
    )
    .await?;

    Ok(CompactionOutcome::Compacted {
        parts: parts.len(),
        publish,
    })
}

// Re-export the input-listing type so callers (and tests) can inspect a
// bucket without reaching into the module.
pub use read::BucketListing;

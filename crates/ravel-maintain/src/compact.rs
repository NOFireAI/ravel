//! The per-bucket compaction driver: seal + trigger checks, then the
//! plan-build-publish pipeline (docs/compaction-retention-plan.md §3.2-§3.4).
//! Stateless and idempotent: a crashed run re-run from scratch reuses
//! content-addressed part keys and converges at the record's
//! `CreateIfAbsent` (plan §3.6).

use ravel_object_store::ObjectStoreBackend;
use ravel_types::Signal;

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::codec::{RsegCodec, SegmentCodec};
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::publish::{PublishOutcome, publish_record};
use crate::read;
use crate::read::{input_set_hash, list_bucket, load_inputs};
use crate::rlog::RlogCodec;
use crate::rspan_codec::SpanCodec;

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

    // Everything up to here is signal-generic (seal, tombstone, already-done,
    // and input-count gates on the bucket listing). The plan-build step is the
    // only signal-specific part: dispatch it to the codec for this bucket's
    // signal and run the identical shared pipeline (canonical ordering,
    // input_set_hash, publish) around it (ADR-0032).
    match bucket.signal {
        Signal::Metrics => {
            run_pipeline::<RsegCodec>(store, clock, config, bucket, &listing.commit_keys, start_ns)
                .await
        }
        Signal::Logs => {
            run_pipeline::<RlogCodec>(store, clock, config, bucket, &listing.commit_keys, start_ns)
                .await
        }
        Signal::Spans => {
            run_pipeline::<SpanCodec>(store, clock, config, bucket, &listing.commit_keys, start_ns)
                .await
        }
        other => Err(MaintainError::Invariant(format!(
            "compaction is not implemented for signal {other:?}"
        ))),
    }
}

/// The signal-generic plan-build-publish pipeline, parameterized over the
/// per-signal [`SegmentCodec`]. Loads and canonically orders the inputs,
/// derives the `input_set_hash`, decodes each input's catalog metadata through
/// the codec, streams the merge into size-capped parts through the codec, and
/// publishes the record. Only the two `C::` calls know the on-object format;
/// everything else is identical for every signal.
async fn run_pipeline<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    commit_keys: &[String],
    start_ns: i64,
) -> Result<CompactionOutcome> {
    let inputs = load_inputs(store, bucket, commit_keys).await?;
    let hash = input_set_hash(&inputs);

    // Catalogs aligned one-to-one with `inputs` (canonical order): the merge
    // relies on that alignment for deterministic tie-breaking.
    let mut catalogs = Vec::with_capacity(inputs.len());
    for input in &inputs {
        catalogs.push(C::load_input_catalog(store, config, input).await?);
    }

    let parts = C::build_parts(store, config, bucket, &inputs, &catalogs, &hash).await?;

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

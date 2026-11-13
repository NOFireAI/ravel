//! A migrate walk over a bucket whose RLOG merge is refused for its part
//! indexes (ADR-3853, issue #4127). The refusal is terminal for that bucket
//! the way it is for compaction and retention: the walk records it as not
//! migrated, releases its claim, and goes on to migrate the later buckets.
//!
//! The refusal comes from the real build: at a range quantum of one record a
//! unit of more than 10,000 records needs more ranges than the part key's
//! 10,000 indexes give windows, so `RlogCodec::build_parts` refuses it with
//! [`MaintainError::PartKeySpace`] before encoding a part.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use prost::Message;
use ravel_commit::keys;
use ravel_fleet::claim::COMPACTION_CLAIMS_PREFIX;
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    Bucket, ClaimParticipant, Clock, CompactorConfig, Coordination, FixedClock, MaintainError,
    MigrateBudget, MigrationPath, NotMigratedBucket, NotMigratedReason, PartIndexRefusalKind,
    Verification, migrate_family,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_proto::sys::v1::{ClaimState, CompactionClaim};
use ravel_types::Signal;
use uuid::Uuid;

const FAMILY: &str = "rlog";
/// The part key's indexes (`rlog::PART_KEYS`): one record past them at a
/// quantum of one is refused.
const REFUSED_RECORDS: u64 = 10_001;

/// A [`ClaimSleeper`] that returns immediately.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// Every range closes at its first record, so [`REFUSED_RECORDS`] records
/// pass the part key space.
fn quantum_one(config: CompactorConfig) -> CompactorConfig {
    CompactorConfig {
        rlog_range_records: 1,
        ..config
    }
}

/// [`quantum_one`], claiming every bucket as participant `id` on `clock`.
fn claiming(clock: &FixedClock, id: u128) -> CompactorConfig {
    quantum_one(CompactorConfig {
        coordination: Coordination::On,
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(id),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        ..CompactorConfig::default()
    })
}

fn logs_bucket_at(hour: u32) -> Bucket {
    Bucket::new(tenant_hash(), Signal::Logs, SHARD, hour)
}

/// Seeds three below-target buckets of [`SHARD`]: `HOUR - 2` holds one input
/// of [`REFUSED_RECORDS`] records, `HOUR - 1` and `HOUR` two one-record
/// inputs each.
async fn seed(store: &dyn ObjectStoreBackend, base: u128) {
    let below = ravel_maintain::rlog::OUTPUT_FORMAT_VERSION - 1;
    let hour_ns = |hour: u32| i64::from(hour) * 3_600_000_000_000;
    let dense: Vec<_> = (0..REFUSED_RECORDS)
        .map(|n| log_record(0, hour_ns(HOUR - 2) + n as i64, "dense"))
        .collect();
    seed_rlog_input_at_hour(
        store,
        Uuid::from_u128(base),
        10,
        1,
        HOUR - 2,
        &dense,
        false,
        below,
    )
    .await;
    let mut seq = 1u64;
    for hour in [HOUR - 1, HOUR] {
        for n in 0..2u64 {
            seq += 1;
            seed_rlog_input_at_hour(
                store,
                Uuid::from_u128(base + u128::from(seq)),
                10,
                seq,
                hour,
                &[log_record(0, hour_ns(hour) + n as i64, "alpha")],
                false,
                below,
            )
            .await;
        }
    }
}

/// `(compaction records, commit records)` listed in the bucket at `hour`.
async fn bucket_entries(store: &dyn ObjectStoreBackend, hour: u32) -> (usize, usize) {
    let prefix =
        keys::commit_shard_hour_prefix(&tenant_hash(), Signal::Logs, SHARD, hour).expect("prefix");
    let mut counts = (0, 0);
    for meta in list_all(store, &prefix).await.expect("list") {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(keys::BucketEntry::CompactionRecord(_)) => counts.0 += 1,
            Ok(keys::BucketEntry::CommitRecord(_)) => counts.1 += 1,
            _ => {}
        }
    }
    counts
}

async fn walk(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
) -> ravel_maintain::Result<ravel_maintain::FamilyMigrateReport> {
    migrate_family(
        store,
        clock,
        config,
        tenant_hash(),
        Signal::Logs,
        FAMILY,
        ravel_maintain::rlog::OUTPUT_FORMAT_VERSION,
        SHARD + 1,
        MigrateBudget::unlimited(),
        "migrate_part_index_refusal test",
    )
    .await
}

fn refused_bucket() -> NotMigratedBucket {
    NotMigratedBucket {
        shard: SHARD,
        ingest_hour: HOUR - 2,
        path: MigrationPath::L0Migration,
        reason: NotMigratedReason::PartIndexRefusal {
            refusal: PartIndexRefusalKind::KeySpace,
        },
    }
}

/// The refused bucket is the walk's first; both later buckets are migrated,
/// their records published under a compaction record, and the refused bucket
/// is named once with its refusal while its inputs stay live and raw.
///
/// Before the walk's part-index-refusal arm in `migrate_family` existed, the
/// refusal reached `other => Some(other?)` and the walk returned
/// `Err(PartKeySpace)` at the first bucket.
#[tokio::test]
async fn a_part_index_refusal_is_not_migrated_and_the_walk_goes_on() {
    let store = MemoryStore::new();
    seed(&store, 0x100).await;
    let clock = FixedClock::new(sealed_now_ns());

    let report = walk(&store, &clock, &quantum_one(CompactorConfig::default()))
        .await
        .expect("the walk finishes past the refused bucket");

    assert_eq!(report.not_migrated, vec![refused_bucket()], "{report:?}");
    assert_eq!(report.buckets_examined, 3, "{report:?}");
    assert_eq!(report.buckets_migrated, 2, "{report:?}");
    assert_eq!(report.records_migrated, 4, "{report:?}");
    assert!(report.unwritable_skipped.is_empty(), "{report:?}");
    assert!(report.walk_complete, "{report:?}");
    assert_eq!(
        report.verification,
        Some(Verification::Stragglers {
            l0: 1,
            l1: 0,
            rewrite_parts: 0,
            blocked: Vec::new(),
        }),
        "the refused bucket's one input holds the floor down, and nothing else does"
    );
    assert_eq!(
        bucket_entries(&store, HOUR - 2).await,
        (0, 1),
        "nothing published over the refused bucket's input"
    );
    for hour in [HOUR - 1, HOUR] {
        assert_eq!(bucket_entries(&store, hour).await.0, 1, "hour {hour}");
        let record = fetch_compaction_record(&store, &logs_bucket_at(hour)).await;
        assert_eq!(record.inputs.len(), 2, "hour {hour}");
        assert_eq!(
            record.parts.iter().map(|p| p.sample_count).sum::<u64>(),
            2,
            "hour {hour}: both records published"
        );
    }
}

/// The refused migration marks its bucket claim completed, as compaction
/// does for the same refusal, rather than leaving it running until its lease
/// expires: the walk's three claims, the refused bucket's among them, are all
/// completed.
#[tokio::test]
async fn a_part_index_refusal_completes_the_bucket_claim() {
    let store = MemoryStore::new();
    seed(&store, 0x200).await;
    let clock = FixedClock::new(sealed_now_ns());

    let report = walk(&store, &clock, &claiming(&clock, 77))
        .await
        .expect("the walk finishes past the refused bucket");
    assert_eq!(report.not_migrated, vec![refused_bucket()], "{report:?}");
    assert_eq!(report.buckets_migrated, 2, "{report:?}");

    let mut states = Vec::new();
    for meta in list_all(&store, COMPACTION_CLAIMS_PREFIX)
        .await
        .expect("list claims")
    {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get claim");
        let claim = CompactionClaim::decode(got.data.as_ref()).expect("claim decodes");
        states.push(ClaimState::try_from(claim.state).expect("known state"));
    }
    assert_eq!(states, vec![ClaimState::Completed; 3]);
}

/// Only a part-index refusal is held: the walk's arm leaves every other
/// error to end the walk. A refused range writer config (a quantum of zero)
/// still returns `Err` from the first bucket.
#[tokio::test]
async fn a_walk_still_fails_on_another_build_error() {
    let store = MemoryStore::new();
    seed(&store, 0x300).await;
    let err = walk(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig {
            rlog_range_records: 0,
            ..CompactorConfig::default()
        },
    )
    .await
    .expect_err("a refused writer config ends the walk");
    assert!(!err.is_part_index_refusal(), "{err:?}");
    assert!(
        !matches!(err, MaintainError::UnwritableInput { .. }),
        "{err:?}"
    );
}

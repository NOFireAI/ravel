//! A log object already in storage whose `stream_attrs` blob the RLOG writer
//! refuses (issue #2554). Such an object predates the writer's validation
//! (issue #2548); compacting a bucket that holds one hands the blob back to the
//! writer at finish.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_commit::{keys, record};
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    ClaimParticipant, Clock, CompactorConfig, Coordination, FixedClock, MaintainError,
    MaintainMemo, compact_bucket,
};
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use uuid::Uuid;

/// Stream `n`'s record with its scope name's first byte replaced by `0xFF`:
/// well-formed framing whose scope name is not UTF-8, which the reader's
/// decoder refuses and so the writer refuses too. Its stream id is stream
/// `n`'s, so `n` must not be a stream any healthy input carries.
fn refused_record(stream_n: u32, ts: i64) -> ravel_logseg::LogRecord {
    let mut r = log_record(stream_n, ts, "refused");
    let at = r
        .stream_attrs
        .windows(5)
        .position(|w| w == b"scope")
        .expect("scope name in blob");
    r.stream_attrs[at] = 0xFF;
    assert!(ravel_logseg::stream_attr_pairs(&r.stream_attrs).is_err());
    r
}

/// The data-object key a commit record names.
async fn data_key_of(store: &dyn ObjectStoreBackend, commit_key: &str) -> String {
    let got = store
        .get(commit_key, GetRange::Full)
        .await
        .expect("get commit record");
    let rec = record::decode(&got.data).expect("decode commit record");
    keys::reconstruct_data_key(&rec).expect("data key")
}

/// Seeds two healthy inputs and one refused input into the logs bucket and
/// returns the refused input's (commit key, data key).
async fn seed_bucket(store: &dyn ObjectStoreBackend) -> (String, String) {
    seed_rlog_input(
        store,
        Uuid::from_u128(1),
        10,
        1,
        &[log_record(0, 10, "alpha"), log_record(1, 20, "bravo")],
    )
    .await;
    seed_rlog_input(
        store,
        Uuid::from_u128(2),
        10,
        2,
        &[log_record(0, 15, "charlie"), log_record(2, 5, "delta")],
    )
    .await;
    let refused_commit =
        seed_rlog_input_unchecked(store, Uuid::from_u128(3), 10, 3, &[refused_record(9, 12)]).await;
    let refused_data = data_key_of(store, &refused_commit).await;
    (refused_commit, refused_data)
}

/// `mem` behind a [`FaultStore`] whose sequence 0 passes every GET of `key`
/// through and counts it: `sequence_progress(0)` is the number of reads of
/// that object.
fn counting_reads_of(mem: &Arc<MemoryStore>, key: &str) -> FaultStore<Arc<MemoryStore>> {
    FaultStore::new(
        Arc::clone(mem),
        FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(key)
                .with_steps(vec![SequenceStep::Passthrough; 1024]),
        ),
    )
}

async fn compaction_record_count(store: &dyn ObjectStoreBackend) -> usize {
    let b = logs_bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .expect("prefix");
    list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .filter(|m| {
            matches!(
                keys::partition_bucket_entry(&m.key),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .count()
}

/// A [`ClaimSleeper`] that returns immediately.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A config that claims every bucket as one process on `clock`, as the
/// background supervisor does.
fn claiming_config(clock: &FixedClock) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(77),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        ..CompactorConfig::default()
    }
}

/// One maintenance tick over the logs bucket's shard.
async fn tick(
    memo: &mut MaintainMemo,
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
) -> ravel_maintain::Result<ravel_maintain::MaintainReport> {
    let b = logs_bucket();
    ravel_maintain::scan::scan_and_maintain_with_memo(
        memo,
        store,
        clock,
        config,
        &ravel_maintain::RetentionConfig::default(),
        &ravel_maintain::NoLeases,
        b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
}

/// Today's behaviour at the maintenance tick, pinned before the fix: the
/// refusal fails the whole shard's tick (every later hour of the shard goes
/// unmaintained with it), every tick re-reads the refused object the same
/// number of times, and the bucket's claim is never marked completed, so it
/// stays on the store and this process re-takes it each tick.
#[tokio::test]
async fn today_a_refused_blob_fails_every_tick_of_its_shard() {
    let mem = Arc::new(MemoryStore::new());
    let now_ns = sealed_now_ns();
    mem.set_clock_ms((now_ns / 1_000_000) as u64);
    let (_, refused_data) = seed_bucket(&mem).await;
    let store = counting_reads_of(&mem, &refused_data);
    let clock = FixedClock::new(now_ns);
    let config = claiming_config(&clock);
    let mut memo = MaintainMemo::with_default_interval();

    let mut reads = Vec::new();
    for n in 1..=3 {
        let err = tick(&mut memo, &store, &clock, &config)
            .await
            .expect_err("the tick fails");
        assert!(
            matches!(
                err,
                MaintainError::LogSeg(ravel_logseg::LogSegError::Corrupted(_))
            ),
            "tick {n}: {err:?}"
        );
        reads.push(store.sequence_progress(0));
        let claims = list_all(&*mem, "sys/maintain/claims/compaction/")
            .await
            .expect("list claims");
        assert_eq!(claims.len(), 1, "tick {n} leaves the claim behind");
    }
    assert_eq!(
        reads,
        vec![6, 12, 18],
        "six reads of the refused object per tick"
    );
    assert_eq!(compaction_record_count(&store).await, 0);
}

/// Today's behaviour, pinned before the fix: the writer's refusal surfaces as
/// the run's error, nothing is published, and the next run reads the refused
/// object again and fails the same way.
#[tokio::test]
async fn today_a_refused_blob_fails_every_compaction_of_its_bucket() {
    let mem = Arc::new(MemoryStore::new());
    let (_, refused_data) = seed_bucket(&mem).await;
    let store = counting_reads_of(&mem, &refused_data);
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();

    for tick in 1..=2u64 {
        let err = compact_bucket(&store, &clock, &config, &logs_bucket())
            .await
            .expect_err("the bucket fails to compact");
        assert!(
            matches!(
                err,
                MaintainError::LogSeg(ravel_logseg::LogSegError::Corrupted(_))
            ),
            "tick {tick}: {err:?}"
        );
        assert_eq!(compaction_record_count(&store).await, 0);
        assert!(
            store.sequence_progress(0) >= tick,
            "tick {tick} read the refused object"
        );
    }
}

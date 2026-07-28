//! Sealed-bucket scan and trigger (plan section 3.2), walking hours
//! upward from the advisory cursor. Retention is checked first so
//! tombstoned buckets are treated as already done, never re-compacted.

use ravel_commit::keys::BucketEntry;
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash};

use crate::clock::Clock;
use crate::config::MaintainConfig;
use crate::cursor::{self, CursorState};
use crate::error::MaintainError;
use crate::input::{self, CompactionInput};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// A sealed bucket whose L0 inputs need compaction: no compaction record,
/// no tombstone, at least `min_compaction_inputs` L0 records present.
#[derive(Debug, Clone)]
pub struct SealedBucket {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
    pub inputs: Vec<CompactionInput>,
}

fn hour_bucket_end_ns(hour: u32) -> Result<i64, MaintainError> {
    i64::from(hour)
        .checked_add(1)
        .and_then(|h| h.checked_mul(NS_PER_HOUR))
        .ok_or(MaintainError::HourBucketOverflow(hour))
}

/// Advance the in-memory cursor position and best-effort persist it via
/// CAS. A lost race is not an error (the cursor is advisory); the scan
/// keeps going from its own in-memory position either way, only dropping
/// the stale `Version` so a further advance in the same call does not
/// reuse it (see the cursor-chaining note in `cursor.rs`).
async fn advance(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    state: &mut CursorState,
    next_hour_bucket: u32,
) -> Result<(), MaintainError> {
    let new_version =
        cursor::advance_cursor(store, tenant_hash, signal, shard, state, next_hour_bucket).await?;
    state.next_hour_bucket = next_hour_bucket;
    state.version = new_version;
    Ok(())
}

/// Scan (tenant, signal, shard) upward from the advisory cursor for the
/// next bucket needing compaction. Buckets confirmed trivially done
/// (unsealed encountered, empty, single-record, already compacted, or
/// tombstoned) are folded into the cursor as the scan walks past them.
/// Returns `None` once an unsealed bucket is reached: there is nothing
/// more to do right now, and the cursor was never advanced past it.
///
/// The cursor is only ever advanced past a bucket confirmed trivially
/// done. A bucket that needs compaction is returned to the caller and the
/// cursor is left pointing at it; it only advances once that compaction
/// is durably published (plan section 3.1: losing the cursor costs a
/// rescan, never correctness, but advancing it past pending work would
/// silently skip that work forever).
pub async fn scan_next(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &MaintainConfig,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<Option<SealedBucket>, MaintainError> {
    let mut state = cursor::read_cursor(store, tenant_hash, signal, shard).await?;

    loop {
        let hour = state.next_hour_bucket;
        let bucket_end_ns = hour_bucket_end_ns(hour)?;
        if !config.is_sealed(clock.now_ns(), bucket_end_ns) {
            return Ok(None);
        }

        let prefix =
            ravel_commit::keys::commit_shard_hour_prefix(tenant_hash, signal, shard, hour)?;
        let listed = ravel_object_store::list_all(store, &prefix).await?;

        let mut inputs_keys = Vec::new();
        let mut has_compaction_record = false;
        let mut has_tombstone = false;
        for meta in &listed {
            match ravel_commit::keys::partition_bucket_entry(&meta.key)? {
                BucketEntry::CommitRecord(_) => inputs_keys.push(meta.key.clone()),
                BucketEntry::CompactionRecord(_) => has_compaction_record = true,
                BucketEntry::Tombstone(_) => has_tombstone = true,
            }
        }

        let next_hour = hour
            .checked_add(1)
            .ok_or(MaintainError::HourBucketOverflow(hour))?;

        // Retention is checked first (plan section 3.2): a tombstoned
        // bucket is done regardless of whether a compaction record also
        // exists.
        if has_tombstone
            || has_compaction_record
            || inputs_keys.len() < config.min_compaction_inputs
        {
            advance(store, tenant_hash, signal, shard, &mut state, next_hour).await?;
            continue;
        }

        let mut inputs = Vec::with_capacity(inputs_keys.len());
        for key in &inputs_keys {
            inputs.push(input::fetch_input(store, key).await?);
        }
        let inputs = input::sort_inputs_canonically(inputs)?;

        return Ok(Some(SealedBucket {
            tenant_hash: *tenant_hash,
            signal,
            shard,
            ingest_hour_bucket: hour,
            inputs,
        }));
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use ravel_commit::record::{NewCommitRecord, build};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{PutMode, PutOptions};
    use uuid::Uuid;

    fn tenant_hash() -> TenantHash {
        TenantHash([0x77; 16])
    }

    fn far_future_clock() -> FakeClock {
        // Comfortably past the seal threshold for hour 0.
        FakeClock::new(NS_PER_HOUR * 10)
    }

    async fn put_commit_record(
        store: &MemoryStore,
        tenant_hash: &TenantHash,
        shard: u32,
        writer_id: Uuid,
        seq: u64,
    ) {
        let record = build(NewCommitRecord {
            tenant_hash: *tenant_hash,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: 100,
            content_hash: [0x9; 32],
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 1,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 1,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("valid record");
        let key = ravel_commit::keys::commit_key_for_record(&record).expect("commit key");
        store
            .put(
                &key,
                ravel_commit::record::encode(&record),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put commit record");
    }

    #[tokio::test]
    async fn unsealed_bucket_returns_none_without_advancing_cursor() {
        let store = MemoryStore::new();
        let clock = FakeClock::new(0); // hour 0 not sealed at time 0
        let config = MaintainConfig::default();
        let th = tenant_hash();

        let outcome = scan_next(&store, &clock, &config, &th, Signal::Metrics, 1)
            .await
            .expect("scan");
        assert!(outcome.is_none());

        let state = cursor::read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        assert_eq!(state.next_hour_bucket, 0);
    }

    #[tokio::test]
    async fn empty_bucket_is_skipped_and_cursor_advances() {
        let store = MemoryStore::new();
        let clock = far_future_clock();
        let config = MaintainConfig::default();
        let th = tenant_hash();

        let outcome = scan_next(&store, &clock, &config, &th, Signal::Metrics, 1)
            .await
            .expect("scan");
        assert!(outcome.is_none());

        let state = cursor::read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        assert!(state.next_hour_bucket > 0);
    }

    #[tokio::test]
    async fn below_threshold_bucket_is_skipped() {
        let store = MemoryStore::new();
        let clock = far_future_clock();
        let config = MaintainConfig::default(); // min_compaction_inputs = 2
        let th = tenant_hash();
        put_commit_record(&store, &th, 1, Uuid::new_v4(), 1).await;

        let outcome = scan_next(&store, &clock, &config, &th, Signal::Metrics, 1)
            .await
            .expect("scan");
        assert!(outcome.is_none());
    }

    #[tokio::test]
    async fn bucket_at_threshold_triggers_compaction_and_cursor_does_not_advance_past_it() {
        let store = MemoryStore::new();
        let clock = far_future_clock();
        let config = MaintainConfig::default();
        let th = tenant_hash();
        let writer_a = Uuid::new_v4();
        let writer_b = Uuid::new_v4();
        put_commit_record(&store, &th, 1, writer_a, 1).await;
        put_commit_record(&store, &th, 1, writer_b, 1).await;

        let outcome = scan_next(&store, &clock, &config, &th, Signal::Metrics, 1)
            .await
            .expect("scan");
        let bucket = outcome.expect("needs compaction");
        assert_eq!(bucket.ingest_hour_bucket, 0);
        assert_eq!(bucket.inputs.len(), 2);

        let state = cursor::read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        assert_eq!(
            state.next_hour_bucket, 0,
            "cursor must not advance past a bucket pending compaction"
        );
    }

    #[tokio::test]
    async fn compaction_record_present_marks_bucket_done() {
        let store = MemoryStore::new();
        let clock = far_future_clock();
        let config = MaintainConfig::default();
        let th = tenant_hash();
        put_commit_record(&store, &th, 1, Uuid::new_v4(), 1).await;
        put_commit_record(&store, &th, 1, Uuid::new_v4(), 2).await;

        let key = ravel_commit::keys::compaction_record_key(
            &th,
            Signal::Metrics,
            1,
            0,
            "deadbeefdeadbeef",
        )
        .expect("key");
        store
            .put(
                &key,
                vec![].into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put stub compaction record");

        let outcome = scan_next(&store, &clock, &config, &th, Signal::Metrics, 1)
            .await
            .expect("scan");
        assert!(outcome.is_none());

        let state = cursor::read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        assert!(state.next_hour_bucket > 0);
    }

    #[tokio::test]
    async fn tombstone_present_wins_over_pending_compaction() {
        let store = MemoryStore::new();
        let clock = far_future_clock();
        let config = MaintainConfig::default();
        let th = tenant_hash();
        put_commit_record(&store, &th, 1, Uuid::new_v4(), 1).await;
        put_commit_record(&store, &th, 1, Uuid::new_v4(), 2).await;

        let key =
            ravel_commit::keys::retention_tombstone_key(&th, Signal::Metrics, 1, 0).expect("key");
        store
            .put(
                &key,
                vec![].into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put stub tombstone");

        let outcome = scan_next(&store, &clock, &config, &th, Signal::Metrics, 1)
            .await
            .expect("scan");
        assert!(outcome.is_none(), "tombstoned bucket must not be compacted");

        let state = cursor::read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        assert!(state.next_hour_bucket > 0);
    }

    #[tokio::test]
    async fn scan_uses_paginated_listing() {
        // with_page_size(2) forces multi-page LIST; scan_next must still
        // see every commit record via list_all's pagination loop.
        let store = MemoryStore::with_page_size(2);
        let clock = far_future_clock();
        let config = MaintainConfig::default();
        let th = tenant_hash();
        for i in 0..5u64 {
            put_commit_record(&store, &th, 1, Uuid::new_v4(), i).await;
        }

        let outcome = scan_next(&store, &clock, &config, &th, Signal::Metrics, 1)
            .await
            .expect("scan");
        let bucket = outcome.expect("needs compaction");
        assert_eq!(bucket.inputs.len(), 5);
    }

    #[tokio::test]
    async fn fetch_input_round_trips_through_listing() {
        // Sanity check that fetch_input's GetRange::Full path round-trips
        // through MemoryStore for a bucket already exercised above.
        let store = MemoryStore::new();
        let th = tenant_hash();
        put_commit_record(&store, &th, 1, Uuid::new_v4(), 1).await;
        let prefix = ravel_commit::keys::commit_shard_hour_prefix(&th, Signal::Metrics, 1, 0)
            .expect("prefix");
        let listed = ravel_object_store::list_all(&store, &prefix)
            .await
            .expect("list");
        let commit_key = listed
            .iter()
            .find_map(|m| {
                if let Ok(BucketEntry::CommitRecord(_)) =
                    ravel_commit::keys::partition_bucket_entry(&m.key)
                {
                    Some(m.key.clone())
                } else {
                    None
                }
            })
            .expect("commit record listed");
        let fetched = input::fetch_input(&store, &commit_key)
            .await
            .expect("fetch");
        assert_eq!(fetched.record.writer_seq, 1);
    }
}

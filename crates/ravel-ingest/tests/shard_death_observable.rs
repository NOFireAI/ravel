//! Regression test: a shard actor that dies mid-flush must become observable,
//! not silently partial.
//!
//! Routing to a dead shard still yields
//! the typed `WriteError::ShardUnavailable` (not silence, not a hang), the
//! surviving shards keep acking, and the router now counts the death exactly
//! once in `IngestMetricsSnapshot::shard_deaths`.
//!
//! docs/consistency-model.md makes the router the component that decides
//! whether a write was acknowledged; docs/ingest.md "Metrics
//! (self-observability)" requires per-shard failure counters.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::{TestClock, make_point, tenant};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_ingest::{IngestConfig, IngestRouter, WriteError, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_types::{Signal, TenantId, shard_for};
use uuid::Uuid;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first point (`target_bytes: 8`) and never on age, so a
/// strict write drives one complete flush inline and returns its outcome.
fn flush_on_first_point(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// Lands a *different*, structurally valid commit record at the exact key the
/// first flush targets and then reports `AlreadyExists`, which is precisely
/// the state `publish::resolve_already_exists` classifies as split-brain. That
/// drives the `SplitBrain` panic inside the shard actor, killing that task.
struct SplitBrainOnFirstCommit {
    inner: MemoryStore,
    poisoned: AtomicBool,
}

impl SplitBrainOnFirstCommit {
    fn new() -> Self {
        SplitBrainOnFirstCommit {
            inner: MemoryStore::new(),
            poisoned: AtomicBool::new(false),
        }
    }

    /// A structurally valid `CommitRecord` whose `content_hash` differs from
    /// anything the writer could have produced for this flush.
    fn conflicting_record(&self) -> Bytes {
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant("acme").hash(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::nil(),
            writer_epoch: 1,
            writer_seq: 0,
            object_size: 1,
            content_hash: [0xAA; 32],
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("valid conflicting record");
        record::encode(&record)
    }
}

#[async_trait]
impl ObjectStoreBackend for SplitBrainOnFirstCommit {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if key.contains("/c/") && !self.poisoned.swap(true, Ordering::SeqCst) {
            self.inner
                .put(key, self.conflicting_record(), PutOptions::default())
                .await?;
            return Err(StoreError::AlreadyExists);
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// First `host` label value whose point routes to `want_shard`.
fn point_on_shard(
    tenant: &TenantId,
    want_shard: u32,
    shard_count: u32,
    ts_ns: i64,
) -> ravel_otlp::NormalizedPoint {
    for i in 0..10_000u32 {
        let point = make_point(tenant, "cpu_usage", &[("host", &i.to_string())], ts_ns, 1.0);
        if shard_for(&point.series_id, shard_count) == want_shard {
            return point;
        }
    }
    panic!("no series found for shard {want_shard} of {shard_count}");
}

#[tokio::test]
async fn shard_actor_death_is_observable_and_isolated() {
    let shard_count = 4;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainOnFirstCommit::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first_point(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let victim_shard = 0;
    let survivor_shard = 1;

    // This flush hits the poisoned commit key and panics the shard-0 actor
    // mid-flush. The waiter's ack sender dies with it, so instead of silence
    // or a hang the caller gets the typed ShardUnavailable, and the router
    // counts the death.
    let err = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, victim_shard, shard_count, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the split-brain panic takes the shard actor down mid-flush");
    assert!(
        matches!(err, WriteError::ShardUnavailable),
        "a dead shard is reported as the typed ShardUnavailable, got {err}"
    );
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        1,
        "the death of the victim shard's actor is counted exactly once"
    );

    // The router keeps serving every other shard: a survivor write still acks
    // durably, exactly as before the death.
    let receipt = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, survivor_shard, shard_count, 2_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("surviving shards keep acking after a sibling shard dies");
    assert_eq!(receipt.tokens.len(), 1);

    // Shard 0 stays dead (no restart): a later write to it fails at the send
    // half with the same typed error, and the death is not double-counted.
    let again = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, victim_shard, shard_count, 3_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the dead shard never comes back");
    assert!(matches!(again, WriteError::ShardUnavailable));
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        1,
        "a permanently dead shard is counted once, not once per routed write"
    );

    router.shutdown().await;
}

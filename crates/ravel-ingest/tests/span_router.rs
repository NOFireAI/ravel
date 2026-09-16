//! `SpanIngestRouter` shard-death observability: a split-brain panic takes a
//! span shard actor down mid-flush, the caller sees the typed
//! `ShardUnavailable`, and because the span router never respawns, that first
//! death condemns the shard. The condemned count (which drives `/readyz` 503)
//! moves exactly once and `ready()` turns false. Mirrors the log router's own
//! `dead_shard_is_observable_and_counted_once` in `log_router.rs`, swapping
//! `NormalizedSpan`s and the span commit keyspace for the log ones.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::{TestClock, tenant};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_ingest::{IngestConfig, SpanIngestRouter, SpanWriteError, WriteMode, shard_for_span};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::Signal;
use uuid::Uuid;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first span (`target_bytes: 1`) and never on age, so a strict
/// write drives one complete flush inline and returns its outcome.
fn flush_on_first(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// A span routed to `want_shard` by its `trace_id` (the span router shards on
/// the trace id). Varies the first four bytes until the fixture lands.
fn span_on_shard(want_shard: u32, shard_count: u32, start_ns: i64) -> NormalizedSpan {
    for i in 0..100_000u32 {
        let mut trace_id = [0u8; 16];
        trace_id[..4].copy_from_slice(&i.to_be_bytes());
        if shard_for_span(&trace_id, shard_count) == want_shard {
            return NormalizedSpan {
                trace_id,
                span_id: [1u8; 8],
                parent_span_id: None,
                name: "handle".to_string(),
                start_ts_ns: start_ns,
                end_ts_ns: start_ns + 100,
                status_code: StatusCode::Unset,
                status_message: None,
                attrs: vec![("service.name".to_string(), "checkout".to_string())],
            };
        }
    }
    panic!("no trace_id found for shard {want_shard} of {shard_count}");
}

#[tokio::test]
async fn dead_shard_is_observable_and_condemns_on_first_death() {
    let shard_count = 4;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainOnFirstCommit::new());
    let clock = TestClock::new(BASE_NS);
    let router = SpanIngestRouter::new(
        flush_on_first(shard_count),
        Arc::clone(&store),
        clock.clone(),
    );

    let tenant = tenant("acme");
    let victim = 0;
    let survivor = 1;

    // The victim flush hits the poisoned commit key and panics its actor
    // mid-flush; the waiter's ack sender dies with it, so the caller gets the
    // typed ShardUnavailable and the router counts the death.
    let err = router
        .write(
            tenant.clone(),
            vec![span_on_shard(victim, shard_count, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the split-brain panic takes the shard actor down mid-flush");
    assert!(
        matches!(err, SpanWriteError::ShardUnavailable),
        "a dead shard is reported as the typed ShardUnavailable, got {err}"
    );
    assert_eq!(router.metrics().snapshot().shard_deaths, 1);
    // The span router never respawns, so the first death condemns the shard:
    // the counter that drives /readyz 503 moves on this same death, and the
    // router reports not-ready.
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        1,
        "the first shard death condemns the shard (spans never respawn)"
    );
    assert_eq!(router.metrics().condemned_shards(), 1);
    assert!(
        !router.ready(),
        "a condemned shard makes the router report not-ready"
    );

    // A survivor shard still acks durably.
    let receipt = router
        .write(
            tenant.clone(),
            vec![span_on_shard(survivor, shard_count, 2_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("surviving shards keep acking after a sibling dies");
    assert_eq!(receipt.tokens.len(), 1);

    // The dead shard never comes back; a later write to it fails at the send
    // half with the same typed error, not double-counted.
    let again = router
        .write(
            tenant.clone(),
            vec![span_on_shard(victim, shard_count, 3_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the dead shard never comes back");
    assert!(matches!(again, SpanWriteError::ShardUnavailable));
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        1,
        "a permanently dead shard is counted once, not once per routed write"
    );
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        1,
        "condemnation is counted once per shard, not once per routed write"
    );
    assert!(
        !router.ready(),
        "the condemned shard keeps the router not-ready"
    );

    router.shutdown().await;
}

/// Lands a different, structurally valid commit record at the exact key the
/// first span flush targets and then reports `AlreadyExists`, the state
/// `publish` classifies as split-brain. That drives the `SplitBrain` panic
/// inside the span shard actor, killing that task. Keyed on the span commit
/// keyspace (`/s/c/`) so only a span flush trips it. Mirrors
/// `log_router.rs`'s `SplitBrainOnFirstCommit` for the log router.
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

    fn conflicting_record(&self) -> Bytes {
        let rec = record::build(NewCommitRecord {
            tenant_hash: tenant("acme").hash(),
            signal: Signal::Spans,
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
        record::encode(&rec)
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
        if key.contains("/s/c/") && !self.poisoned.swap(true, Ordering::SeqCst) {
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

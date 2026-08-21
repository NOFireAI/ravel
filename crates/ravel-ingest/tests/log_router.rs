//! `LogIngestRouter` end to end over `MemoryStore`: single-shard round-trip
//! to a readable RLOG object, multi-shard fan-out producing one token per
//! involved shard, buffered mode never blocking past enqueue, and a dead
//! shard surfacing as the typed `ShardUnavailable` with the death counted
//! exactly once. Mirrors the metrics router's own tests
//! (`shard_death_observable.rs`), swapping points for `NormalizedLogRecord`s
//! and RSEG read-back for RLOG.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::{TestClock, tenant};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_ingest::{IngestConfig, LogIngestRouter, LogWriteError, WriteMode};
use ravel_logseg::{Predicate, RlogConfig, RlogReader, stream_attrs_bytes};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantHash, shard_for_log};
use uuid::Uuid;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first record (`target_bytes: 1`) and never on age, so a
/// strict write drives one complete flush inline and returns its outcome.
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

/// A consistently-built record: `stream_id` and `stream_attrs` share the same
/// resource+scope inputs, so `RlogWriter::finish`'s collision check passes.
fn norm_record(resource: &[(&str, &str)], ts_ns: i64, body: &str) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = resource
        .iter()
        .map(|(k, v)| ((*k).to_string(), AttrValue::Str((*v).to_string())))
        .collect();
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&res, "scope", "", &scope_attrs);
    let stream_attrs = stream_attrs_bytes(&res, "scope", "", &scope_attrs);
    NormalizedLogRecord {
        stream_id,
        stream_attrs,
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// First record (by an incrementing `host` label) whose `stream_id` routes to
/// `want_shard`.
fn record_on_shard(
    want_shard: u32,
    shard_count: u32,
    ts_ns: i64,
    body: &str,
) -> NormalizedLogRecord {
    for i in 0..100_000u32 {
        let host = i.to_string();
        let rec = norm_record(&[("service.name", "api"), ("host", &host)], ts_ns, body);
        if shard_for_log(&rec.stream_id, shard_count) == want_shard {
            return rec;
        }
    }
    panic!("no stream found for shard {want_shard} of {shard_count}");
}

/// Follows a commit token to its RLOG object and returns every record an
/// unfiltered scan yields.
async fn read_back(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    token: &CommitToken,
) -> Vec<ravel_logseg::LogRecord> {
    let commit_key =
        keys::commit_key_for_token(tenant_hash, Signal::Logs, token).expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let rec = record::decode(&commit_bytes).expect("decode commit record");
    let data_bytes = store
        .get(&rec.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data;
    let reader = RlogReader::new(&data_bytes, &RlogConfig::default()).expect("open rlog");
    let (records, _stats) = reader
        .scan(&Predicate::And(Vec::new()))
        .expect("unfiltered scan");
    records
}

#[tokio::test]
async fn single_shard_write_round_trips_to_a_readable_rlog_object() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(flush_on_first(1), Arc::clone(&store), clock.clone());

    let tenant = tenant("acme");
    let receipt = router
        .write(
            tenant.clone(),
            vec![norm_record(&[("service.name", "api")], 1_000, "hello")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("strict write commits");
    assert_eq!(receipt.tokens.len(), 1);

    let records = read_back(store.as_ref(), &tenant.hash(), &receipt.tokens[0]).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, "hello");

    router.shutdown().await;
}

#[tokio::test]
async fn multi_shard_write_produces_one_token_per_involved_shard() {
    let shard_count = 4;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(
        flush_on_first(shard_count),
        Arc::clone(&store),
        clock.clone(),
    );

    let tenant = tenant("acme");
    let on_zero = record_on_shard(0, shard_count, 1_000, "a");
    let on_one = record_on_shard(1, shard_count, 2_000, "b");
    assert_ne!(
        shard_for_log(&on_zero.stream_id, shard_count),
        shard_for_log(&on_one.stream_id, shard_count),
        "the fixture must span two shards"
    );

    let receipt = router
        .write(
            tenant.clone(),
            vec![on_zero, on_one],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("both shards commit");
    assert_eq!(
        receipt.tokens.len(),
        2,
        "one commit token per involved shard"
    );

    // Both objects are readable, one record each.
    for token in &receipt.tokens {
        let records = read_back(store.as_ref(), &tenant.hash(), token).await;
        assert_eq!(records.len(), 1);
    }

    router.shutdown().await;
}

#[tokio::test]
async fn buffered_write_returns_immediately_with_no_tokens() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        shard_count: 1,
        // Neither size nor age can fire during this test, proving the return
        // is the enqueue, not a flush.
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        ..IngestConfig::default()
    };
    let router = LogIngestRouter::new(config, Arc::clone(&store), clock.clone());

    let tenant = tenant("acme");
    let receipt = router
        .write(
            tenant.clone(),
            vec![norm_record(&[("service.name", "api")], 1_000, "x")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write is acknowledged at enqueue");
    assert!(
        receipt.tokens.is_empty(),
        "buffered mode returns no commit tokens"
    );

    router.shutdown().await;
}

/// Lands a different, structurally valid commit record at the exact key the
/// first flush targets and then reports `AlreadyExists`, the state
/// `publish` classifies as split-brain. That drives the `SplitBrain` panic
/// inside the log shard actor, killing that task. Mirrors
/// `shard_death_observable.rs`'s store for the metrics router.
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
            signal: Signal::Logs,
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

#[tokio::test]
async fn dead_shard_is_observable_and_counted_once() {
    let shard_count = 4;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainOnFirstCommit::new());
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(
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
            vec![record_on_shard(victim, shard_count, 1_000, "a")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the split-brain panic takes the shard actor down mid-flush");
    assert!(
        matches!(err, LogWriteError::ShardUnavailable),
        "a dead shard is reported as the typed ShardUnavailable, got {err}"
    );
    assert_eq!(router.metrics().snapshot().shard_deaths, 1);

    // A survivor shard still acks durably.
    let receipt = router
        .write(
            tenant.clone(),
            vec![record_on_shard(survivor, shard_count, 2_000, "b")],
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
            vec![record_on_shard(victim, shard_count, 3_000, "c")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the dead shard never comes back");
    assert!(matches!(again, LogWriteError::ShardUnavailable));
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        1,
        "a permanently dead shard is counted once, not once per routed write"
    );

    router.shutdown().await;
}

/// Issue #296: a multi-shard Strict write where one shard's flush is abandoned
/// (its data-object PUT fails permanently) while a sibling shard commits
/// durably in the same `write()` call. The failure must still surface as an
/// error, and the sibling's commit token must be recoverable from that error
/// via `LogWriteError::durable_tokens` rather than being silently discarded.
///
/// Non-vacuity (prove-the-test): the failing shard (shard 0) sorts *first* in
/// the router's ack-collection loop, so before the fix the loop's `inner?`
/// returned at shard 0 and never looked at shard 1's successful ack. Against
/// that unfixed loop this test fails at the `durable_tokens().len() == 1`
/// assertion (the recovered list is empty). The injected fault is asserted via
/// the `FaultStore` counter so the test proves the abandonment actually fired.
#[tokio::test]
async fn partial_failure_recovers_the_surviving_shards_token() {
    let shard_count = 4;
    // Fail every data-object PUT for shard 0 (`/l0/0000/`) permanently: a
    // non-retryable store error abandons that flush on the first attempt with
    // no backoff, so the outcome is deterministic and needs no clock advance.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
        )
        .with_key_contains("/l0/0000/")
        .with_occurrence(Occurrence::Always),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(
        flush_on_first(shard_count),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let victim = record_on_shard(0, shard_count, 1_000, "victim");
    let survivor = record_on_shard(1, shard_count, 2_000, "survivor");
    assert_ne!(
        shard_for_log(&victim.stream_id, shard_count),
        shard_for_log(&survivor.stream_id, shard_count),
        "the fixture must span two shards"
    );

    let err = router
        .write(
            tenant.clone(),
            vec![victim, survivor],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("one shard's flush was abandoned, so the write is a failure");

    // The failure is still a failure, and still classifies as its underlying
    // (retryable) abandonment cause.
    assert!(
        err.is_retryable(),
        "an abandoned-flush partial write stays retryable, got {err}"
    );

    // The surviving shard's durable token is recoverable from the error.
    let recovered = err.durable_tokens();
    assert_eq!(
        recovered.len(),
        1,
        "exactly the surviving shard's token is recoverable, got {recovered:?}"
    );
    let records = read_back(store.as_ref(), &tenant.hash(), &recovered[0]).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].body, "survivor",
        "the recovered token points at the shard that actually committed"
    );

    // Prove the injected fault fired: the victim shard's data PUT was rejected.
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the permanent data-object PUT fault fired exactly once (shard 0, no retry)"
    );

    router.shutdown().await;
}

/// Lands a conflicting commit record at shard 0's commit key and reports
/// `AlreadyExists`, driving the pinned-identity split-brain panic in *that
/// specific* shard actor. Unlike `SplitBrainOnFirstCommit`, it targets shard 0
/// only (`/c/0000/`), so a sibling shard's commit is untouched and the panic is
/// deterministic regardless of inter-actor flush ordering.
struct SplitBrainOnShardZeroCommit {
    inner: MemoryStore,
    poisoned: AtomicBool,
}

impl SplitBrainOnShardZeroCommit {
    fn new() -> Self {
        SplitBrainOnShardZeroCommit {
            inner: MemoryStore::new(),
            poisoned: AtomicBool::new(false),
        }
    }

    fn conflicting_record(&self) -> Bytes {
        let rec = record::build(NewCommitRecord {
            tenant_hash: tenant("acme").hash(),
            signal: Signal::Logs,
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
impl ObjectStoreBackend for SplitBrainOnShardZeroCommit {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if key.contains("/c/0000/") && !self.poisoned.swap(true, Ordering::SeqCst) {
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
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Issue #296: a shard whose ack never resolves (its actor panics mid-flush,
/// so the ack sender is dropped) must contribute NO token to the recovered
/// partial-success list, while a sibling that did commit durably is still
/// recovered. Reporting the panicked shard's records as durable would be worse
/// than the original bug, since nothing was acked.
#[tokio::test]
async fn an_unresolved_ack_contributes_no_token() {
    let shard_count = 4;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainOnShardZeroCommit::new());
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(
        flush_on_first(shard_count),
        Arc::clone(&store),
        clock.clone(),
    );

    let tenant = tenant("acme");
    let victim = record_on_shard(0, shard_count, 1_000, "victim");
    let survivor = record_on_shard(1, shard_count, 2_000, "survivor");

    let err = router
        .write(
            tenant.clone(),
            vec![victim, survivor],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the split-brain panic takes shard 0's actor down mid-flush");

    // The write still fails as ShardUnavailable, and the death is counted once.
    assert!(
        err.is_retryable(),
        "ShardUnavailable is retryable, got {err}"
    );
    assert_eq!(router.metrics().snapshot().shard_deaths, 1);

    // Only the surviving shard's token is recovered; the panicked shard, whose
    // ack never resolved, contributes none.
    let recovered = err.durable_tokens();
    assert_eq!(
        recovered.len(),
        1,
        "only the committed sibling's token is recovered, not the panicked shard's, got {recovered:?}"
    );
    let records = read_back(store.as_ref(), &tenant.hash(), &recovered[0]).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, "survivor");

    router.shutdown().await;
}

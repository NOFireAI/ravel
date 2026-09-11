//! Regression test: a shard actor that dies mid-flush must become observable
//! and must not stay permanently dead for the process lifetime (issue #1299).
//!
//! Routing to a shard whose actor just died still yields the typed
//! `WriteError::ShardUnavailable` (not silence, not a hang), the surviving
//! shards keep acking, and the router counts each death exactly once in
//! `IngestMetricsSnapshot::shard_deaths`. The router then respawns the dead
//! shard with a fresh writer identity, so a later write to the same shard
//! reaches a live actor again (the buffered points the dead actor held are lost;
//! a respawn restores capacity, not the buffer). A shard that keeps dying is
//! respawned only up to `IngestRouter::MAX_SHARD_RESPAWNS`, after which it is
//! condemned: writes to it keep failing and the router reports not-ready
//! (`IngestRouter::ready`) so the orchestrator replaces the replica.
//!
//! docs/consistency-model.md makes the router the component that decides
//! whether a write was acknowledged; docs/ingest.md "Metrics
//! (self-observability)" requires per-shard failure counters.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// Lands a *different*, structurally valid commit record at the exact key a
/// flush targets and then reports `AlreadyExists`, which is precisely the state
/// `publish::resolve_already_exists` classifies as split-brain. That drives the
/// `SplitBrain` panic inside the shard actor, killing that task. It does this
/// for the first `remaining` commit PUTs it sees; once that budget is spent
/// every commit passes through, so a respawned actor can commit durably.
///
/// Each respawn mints a fresh `writer_id`, so its flush targets a new commit
/// key; this store keys the poison off "any commit PUT while budget remains",
/// not a fixed key, so it fires once per successive incarnation regardless.
struct SplitBrainNTimes {
    inner: MemoryStore,
    remaining: AtomicUsize,
}

impl SplitBrainNTimes {
    fn new(deaths: usize) -> Self {
        SplitBrainNTimes {
            inner: MemoryStore::new(),
            remaining: AtomicUsize::new(deaths),
        }
    }

    /// Consume one unit of poison budget, returning whether this call should
    /// poison. Decrements only while positive, so it never underflows.
    fn take_poison(&self) -> bool {
        self.remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 { Some(n - 1) } else { None }
            })
            .is_ok()
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
impl ObjectStoreBackend for SplitBrainNTimes {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if key.contains("/c/") && self.take_poison() {
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

/// A death is observable (typed `ShardUnavailable`, counted exactly once) and
/// then recovered: the router respawns the dead shard, so the next write to the
/// same shard reaches a live actor and commits durably. The surviving shards
/// are never touched.
#[tokio::test]
async fn a_dead_shard_is_respawned_and_serves_the_next_write() {
    let shard_count = 4;
    // Exactly one death on the victim shard, then the poison is spent.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainNTimes::new(1));
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
    // mid-flush. Instead of silence or a hang the caller gets the typed
    // ShardUnavailable, and the router counts the death exactly once.
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
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        0,
        "one death within the respawn budget does not condemn the shard"
    );
    assert!(
        router.ready(),
        "a shard respawned within budget leaves the router ready"
    );

    // A survivor write still acks durably: the death was isolated to shard 0.
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

    // The key claim of issue #1299: a later write to the SAME shard now reaches
    // the respawned actor. The poison is spent, so its flush commits durably.
    let recovered = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, victim_shard, shard_count, 3_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the respawned shard actor serves the next write to the same shard");
    assert_eq!(recovered.tokens.len(), 1);
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        1,
        "the respawn's success is not a new death, and the first was not \
         double-counted across the respawn"
    );
    assert!(
        router.ready(),
        "the router stays ready after a successful respawn"
    );

    router.shutdown().await;
}

/// A shard that keeps dying is respawned only up to `MAX_SHARD_RESPAWNS`; the
/// death that spends the last respawn condemns the shard. From then on writes to
/// it keep failing, no further death is counted, and the router reports
/// not-ready. Each death across the respawns is counted exactly once, and a live
/// sibling shard is unaffected.
#[tokio::test]
async fn exhausting_the_respawn_budget_condemns_the_shard_and_drops_readiness() {
    let shard_count = 4;
    // One more death than the budget: the last one condemns the shard.
    let deaths_to_exhaust = IngestRouter::MAX_SHARD_RESPAWNS + 1;
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(SplitBrainNTimes::new(deaths_to_exhaust as usize));
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

    // Each write to shard 0 dies mid-flush; the router respawns until the budget
    // is spent. Drive exactly MAX_SHARD_RESPAWNS + 1 deaths, one per write.
    for i in 0..deaths_to_exhaust {
        let ts = 1_000 + i64::from(i);
        let err = router
            .write(
                tenant.clone(),
                vec![point_on_shard(&tenant, victim_shard, shard_count, ts)],
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
            .expect_err("each successive incarnation dies on the poisoned commit");
        assert!(
            matches!(err, WriteError::ShardUnavailable),
            "death {i} surfaces as the typed ShardUnavailable, got {err}"
        );
    }

    // Each death was counted exactly once across the respawns: the exact bound,
    // not merely > 0, and never double-counted.
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        u64::from(deaths_to_exhaust),
        "each of the {deaths_to_exhaust} deaths counts exactly once across respawns"
    );
    // The shard is condemned exactly once, and the router is now not-ready.
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        1,
        "the shard is condemned exactly once, on the death that spends the last respawn"
    );
    assert!(
        !router.ready(),
        "a condemned shard makes the router report not-ready"
    );

    // A further write to the condemned shard still fails with the typed error,
    // and it is NOT a new death: a condemned incarnation is neither respawned
    // nor re-counted.
    let again = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, victim_shard, shard_count, 9_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a condemned shard keeps failing");
    assert!(matches!(again, WriteError::ShardUnavailable));
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        u64::from(deaths_to_exhaust),
        "a write to an already-condemned shard is not a new death"
    );
    assert!(
        !router.ready(),
        "the router stays not-ready once a shard is condemned"
    );

    // The condemned shard does not take its siblings down: a live sibling still
    // acks durably (its commit is past the spent poison budget).
    let survivor = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, survivor_shard, shard_count, 10_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("a live sibling shard keeps acking after another shard is condemned");
    assert_eq!(survivor.tokens.len(), 1);

    router.shutdown().await;
}

/// A shard whose actor never dies is entirely unaffected by the death
/// machinery: its writes ack durably and the death counter stays exactly 0.
#[tokio::test]
async fn a_shard_that_never_dies_is_unaffected_and_counts_zero_deaths() {
    let shard_count = 4;
    // A store that never poisons: no flush ever hits split-brain.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainNTimes::new(0));
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first_point(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");

    for (shard, ts) in [(1u32, 1_000i64), (2, 2_000)] {
        let receipt = router
            .write(
                tenant.clone(),
                vec![point_on_shard(&tenant, shard, shard_count, ts)],
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
            .expect("a shard that never dies acks durably");
        assert_eq!(receipt.tokens.len(), 1);
    }

    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        0,
        "a shard that never dies is counted exactly 0 deaths"
    );
    assert_eq!(router.metrics().snapshot().shards_condemned, 0);
    assert!(router.ready(), "no death means the router stays ready");

    router.shutdown().await;
}

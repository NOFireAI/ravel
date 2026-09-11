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
//! respawned only up to `IngestRouter::MAX_SHARD_RESPAWNS` within one decay
//! window, after which it is condemned: writes to it keep failing and the
//! router reports not-ready (`IngestRouter::ready`), which sheds traffic from
//! this replica but does not replace it.
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
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_ingest::{
    IngestConfig, IngestRouter, MAX_FLUSH_CLOCK_HOLD_NS, WriteError, WriteMode, WriteReceipt,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_types::{CommitToken, Signal, TenantId, shard_for};
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

/// Buffers every write instead of flushing it: the byte target and both age
/// thresholds are far larger than a test's span, and the age tick never fires
/// under a frozen [`TestClock`], so nothing leaves the buffer until `flush_all`
/// asks for it. That lets a test park two strict writers in one tenant buffer
/// and then drive the single flush both of them wait on.
fn flush_on_demand(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 64 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_secs(3600),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// Lands a *different*, structurally valid commit record at the exact key a
/// flush targets and then reports `AlreadyExists`, which is precisely the state
/// `publish::resolve_already_exists` classifies as split-brain. That drives the
/// `SplitBrain` panic inside the shard actor, killing that task. It lets the
/// first `skip` commit PUTs through untouched, then poisons the next
/// `remaining`; once that budget is spent every commit passes through again, so
/// a respawned actor can commit durably.
///
/// Each respawn mints a fresh `writer_id`, so its flush targets a new commit
/// key; this store keys the poison off "any commit PUT while budget remains",
/// not a fixed key, so it fires once per successive incarnation regardless.
struct SplitBrainNTimes {
    inner: MemoryStore,
    skip: AtomicUsize,
    remaining: AtomicUsize,
}

impl SplitBrainNTimes {
    fn new(deaths: usize) -> Self {
        SplitBrainNTimes::after_clean_commits(0, deaths)
    }

    /// Poison only after `skip` commit PUTs have landed cleanly, so a test can
    /// establish durable state (and a flush-open stamp) before the first death.
    fn after_clean_commits(skip: usize, deaths: usize) -> Self {
        SplitBrainNTimes {
            inner: MemoryStore::new(),
            skip: AtomicUsize::new(skip),
            remaining: AtomicUsize::new(deaths),
        }
    }

    /// Consume one unit of poison budget, returning whether this call should
    /// poison. Both counters decrement only while positive, so neither
    /// underflows, and the skip budget is spent first.
    fn take_poison(&self) -> bool {
        let spent_skip = self
            .skip
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 { Some(n - 1) } else { None }
            })
            .is_ok();
        if spent_skip {
            return false;
        }
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

/// One strict write to `shard`, paired with the `flush_all` that drives it.
/// Under [`flush_on_demand`] the write parks in the tenant buffer until that
/// flush runs, so the pair is one complete flush with one waiter on it.
///
/// The yields before `flush_all` hand the write future its first polls:
/// `tokio::join!` polls in argument order and a send into a channel with free
/// capacity completes without yielding, so the point is already queued ahead of
/// the `FlushNow`, but yielding first makes that independent of how many polls
/// the write's admission path takes.
async fn write_and_flush(
    router: &IngestRouter,
    tenant: &TenantId,
    shard: u32,
    shard_count: u32,
    ts_ns: i64,
) -> Result<WriteReceipt, WriteError> {
    let (result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            vec![point_on_shard(tenant, shard, shard_count, ts_ns)],
            WriteMode::Strict,
            Duration::from_secs(5),
        ),
        async {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            router.flush_all().await;
        },
    );
    result
}

/// Reads back the flush-open stamp (`created_unix_ns`) a commit token
/// addresses. That stamp is the primary key of the query-time
/// duplicate-resolution order (docs/catalog-and-mvcc.md "Cross-segment
/// duplicate samples"), so it is what the ADR-1307 monotonic floor protects.
async fn created_unix_ns_of(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    token: &CommitToken,
) -> i64 {
    let commit_key =
        keys::commit_key_for_token(&tenant.hash(), Signal::Metrics, token).expect("commit key");
    let bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    record::decode(&bytes)
        .expect("decode commit record")
        .created_unix_ns
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

/// Two writers waiting on the SAME flush both observe that flush's death, and
/// the router must count it once and charge one respawn for it, not two. The
/// incarnation each writer captured when it routed is what settles this: the
/// first observer respawns the actor and bumps the incarnation, so the second
/// observer's stale incarnation no longer matches and it returns without
/// counting anything.
///
/// Without that check a single death costs two respawns, so a shard is
/// condemned after roughly half its budget whenever writes are concurrent,
/// which is the normal case in the gateway.
#[tokio::test]
async fn concurrent_observers_of_one_death_count_it_once() {
    let shard_count = 4;
    // One death for the concurrent pair, then enough budget to show the shard
    // is still respawnable exactly twice more before the death that condemns.
    let deaths = IngestRouter::MAX_SHARD_RESPAWNS as usize + 1;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainNTimes::new(deaths));
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_demand(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let victim_shard = 0;

    // Both writes are routed on incarnation 0 and land in one tenant buffer,
    // so the single flush that follows carries both waiters. Its split-brain
    // panic drops both ack senders at once: two observers, one death.
    let (first, second, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            vec![point_on_shard(&tenant, victim_shard, shard_count, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        ),
        router.write(
            tenant.clone(),
            vec![point_on_shard(&tenant, victim_shard, shard_count, 2_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        ),
        async {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            router.flush_all().await;
        },
    );

    for (label, result) in [("first", first), ("second", second)] {
        let err = result.expect_err("both writers were waiting on the flush that died");
        assert!(
            matches!(err, WriteError::ShardUnavailable),
            "the {label} concurrent writer sees the typed ShardUnavailable, got {err}"
        );
    }
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        1,
        "two observers of one death count it exactly once"
    );
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        0,
        "one death is well within the respawn budget"
    );

    // The respawn budget was charged once, not twice: the shard survives
    // exactly MAX_SHARD_RESPAWNS - 1 further deaths and is condemned by the
    // next one. If the pair had cost two respawns, the first iteration here
    // would already condemn it.
    for i in 0..(IngestRouter::MAX_SHARD_RESPAWNS - 1) {
        let ts = 3_000 + i64::from(i);
        let err = write_and_flush(&router, &tenant, victim_shard, shard_count, ts)
            .await
            .expect_err("the respawned incarnation dies on the next poisoned commit");
        assert!(matches!(err, WriteError::ShardUnavailable));
        assert_eq!(
            router.metrics().snapshot().shard_deaths,
            u64::from(i) + 2,
            "death {} counts exactly once",
            i + 2
        );
        assert_eq!(
            router.metrics().snapshot().shards_condemned,
            0,
            "respawn {} of {} is still within budget",
            i + 2,
            IngestRouter::MAX_SHARD_RESPAWNS
        );
        assert!(
            router.ready(),
            "a shard within budget leaves the router ready"
        );
    }

    // The death that spends the last respawn condemns the shard.
    let err = write_and_flush(&router, &tenant, victim_shard, shard_count, 8_000)
        .await
        .expect_err("the death that spends the last respawn still fails the write");
    assert!(matches!(err, WriteError::ShardUnavailable));
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        u64::from(IngestRouter::MAX_SHARD_RESPAWNS) + 1,
        "one death for the concurrent pair plus one per respawn"
    );
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        1,
        "the shard is condemned exactly once, on the death past the budget"
    );
    assert!(
        !router.ready(),
        "a condemned shard makes the router not-ready"
    );

    router.shutdown().await;
}

/// The respawn budget is a rate, not a process-lifetime allowance: a shard that
/// spends its whole budget and then runs clean for a decay window gets the
/// budget back. docs/guides/observability.md describes a low steady
/// `ravel_ingest_shard_deaths_total` rate as transient respawn recovery, which
/// is only true if the budget decays; without decay the fourth death of a
/// process's life condemns the shard however far apart the deaths were.
///
/// The decay window is `IngestConfig::max_flush_lifetime`. Decay restores the
/// budget, it does not lift the bound: after the window the shard is condemned
/// by the next run of `MAX_SHARD_RESPAWNS + 1` deaths, which the second half of
/// this test drives without advancing the clock.
///
/// Entirely on the injected clock: no wall-clock sleep, no real elapsed time.
#[tokio::test]
async fn a_respawn_budget_decays_after_a_clean_run() {
    let shard_count = 4;
    let config = flush_on_demand(shard_count);
    let decay_window_ns =
        i64::try_from(config.max_flush_lifetime.as_nanos()).expect("window fits in i64");
    // Two full runs of the budget: MAX_SHARD_RESPAWNS deaths before the decay,
    // then MAX_SHARD_RESPAWNS + 1 after it, the last of which condemns.
    let deaths = (IngestRouter::MAX_SHARD_RESPAWNS * 2 + 1) as usize;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainNTimes::new(deaths));
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let victim_shard = 0;

    // Spend the whole budget: MAX_SHARD_RESPAWNS deaths, all respawned. One
    // more death right now would condemn the shard.
    for i in 0..IngestRouter::MAX_SHARD_RESPAWNS {
        let err = write_and_flush(
            &router,
            &tenant,
            victim_shard,
            shard_count,
            1_000 + i64::from(i),
        )
        .await
        .expect_err("each incarnation dies on the poisoned commit");
        assert!(matches!(err, WriteError::ShardUnavailable));
    }
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        u64::from(IngestRouter::MAX_SHARD_RESPAWNS),
        "the budget is spent exactly, one death per respawn"
    );
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        0,
        "spending the budget does not itself condemn the shard"
    );

    // A clean run of exactly one decay window on the injected clock. The
    // boundary is inclusive, so this is the shortest run that restores the
    // budget.
    clock.advance_ns(decay_window_ns);

    // The death that would have condemned the shard a moment ago now respawns
    // it: the budget decayed to zero and this death is the first of a new run.
    let err = write_and_flush(&router, &tenant, victim_shard, shard_count, 5_000)
        .await
        .expect_err("the write still fails; the shard is respawned, not rescued");
    assert!(matches!(err, WriteError::ShardUnavailable));
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        u64::from(IngestRouter::MAX_SHARD_RESPAWNS) + 1,
        "the post-decay death is counted like any other"
    );
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        0,
        "after a decay window the shard is respawned, not condemned"
    );
    assert!(
        router.ready(),
        "a shard respawned after the decay window leaves the router ready"
    );

    // Decay restores the budget; it does not remove the bound. Without
    // advancing the clock again, the rest of the new run condemns the shard.
    for i in 0..IngestRouter::MAX_SHARD_RESPAWNS {
        let err = write_and_flush(
            &router,
            &tenant,
            victim_shard,
            shard_count,
            6_000 + i64::from(i),
        )
        .await
        .expect_err("the new run of deaths keeps failing writes");
        assert!(matches!(err, WriteError::ShardUnavailable));
    }
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        u64::from(IngestRouter::MAX_SHARD_RESPAWNS * 2) + 1,
        "every death in both runs counted exactly once"
    );
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        1,
        "the second run spends the restored budget and condemns the shard"
    );
    assert!(
        !router.ready(),
        "the decayed budget is still a bound: the router ends not-ready"
    );

    router.shutdown().await;
}

/// The ADR-1307 monotonic flush-open floor belongs to the shard, not to one
/// incarnation of its actor. A respawn mints a fresh `writer_id`, but
/// `writer_id` is not part of the query-time duplicate-resolution comparator
/// (docs/catalog-and-mvcc.md "Cross-segment duplicate samples"), so a respawned
/// actor starting from a zero floor would stamp a post-respawn flush below a
/// pre-respawn one after a backwards clock step and let the stale flush outrank
/// its own correction: exactly the defect ADR-1307 closes, reopened by the
/// respawn path.
///
/// The floor is a per-shard `AtomicI64` the router hands to every incarnation,
/// so the backwards step below is absorbed (counted as `clock_regressions`) and
/// the post-respawn commit carries the pre-respawn stamp.
#[tokio::test]
async fn the_flush_open_floor_survives_a_respawn() {
    // One shard, so every point lands on the victim and the commit records are
    // unambiguous.
    let shard_count = 1;
    // The first commit lands cleanly and sets the floor; the second is
    // poisoned and kills the actor; everything after it commits.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(SplitBrainNTimes::after_clean_commits(1, 1));
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first_point(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");

    // A clean flush at BASE_NS raises this shard's floor to BASE_NS.
    let before = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, 0, shard_count, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the first flush commits cleanly");
    assert_eq!(before.tokens.len(), 1);
    assert_eq!(
        created_unix_ns_of(store.as_ref(), &tenant, &before.tokens[0]).await,
        BASE_NS,
        "the first flush is stamped from the clock"
    );

    // The next flush dies and the router respawns the actor with a fresh
    // writer identity.
    let err = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, 0, shard_count, 2_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the poisoned commit kills the actor mid-flush");
    assert!(matches!(err, WriteError::ShardUnavailable));
    assert_eq!(router.metrics().snapshot().shard_deaths, 1);

    // A backwards clock step within the production hold bound, the kind
    // ADR-1307 absorbs rather than refuses.
    clock.set_ns(BASE_NS - MAX_FLUSH_CLOCK_HOLD_NS / 2);

    let after = router
        .write(
            tenant.clone(),
            vec![point_on_shard(&tenant, 0, shard_count, 3_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the respawned actor commits durably");
    assert_eq!(after.tokens.len(), 1);
    assert_eq!(
        created_unix_ns_of(store.as_ref(), &tenant, &after.tokens[0]).await,
        BASE_NS,
        "the respawned actor is held to the shard's floor, not to its own zero"
    );

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.clock_regressions, 1,
        "the backwards step is absorbed exactly once, by the respawned actor"
    );
    assert_eq!(
        snapshot.clock_regressions_refused, 0,
        "a step within the hold bound is never refused"
    );

    router.shutdown().await;
}

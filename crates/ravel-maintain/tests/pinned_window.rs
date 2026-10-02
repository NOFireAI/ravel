//! The unnamed-since marker gate (ADR-1133): a retention or superseded-input
//! candidate HEAD no longer names is deleted only once its marker satisfies
//! `observed_unix_ns + max_query_duration + head_cache_ttl +
//! 4 * clock_skew_allowance <= now_ns` on the deleting sweeper's clock.
//!
//! Every other maintain test runs with the window zeroed; these pin it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common::*;
use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::{
    Bucket, Clock, CompactionOutcome, CompactorConfig, FixedClock, MarkerAnchor, MarkerKind,
    NoLeases, RetentionConfig, RetentionOutcome, RetentionPolicy, SnapshotBlock, UnnamedMarker,
    compact_bucket, reap_orphan_unnamed_markers, retention_sweep_bucket, sweep_superseded,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_types::Signal;
use uuid::Uuid;

const SECOND: i64 = 1_000_000_000;

/// The default window: 1 h + 30 s + 4 * 5 min.
fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

fn window_ns(config: &CompactorConfig) -> i64 {
    config.max_query_duration_ns + config.head_cache_ttl_ns + 4 * config.clock_skew_allowance_ns
}

fn retention_at_floor(config: &CompactorConfig) -> RetentionConfig {
    let floor = config.retention_floor_ns(DEFAULT_MAX_INGEST_LAG_NS);
    RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(TENANT.to_string(), floor)],
        },
        config,
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("valid retention config")
}

fn metrics_specs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series(
                "m",
                &[("k", "a")],
                &[(1_000, 1.0), (2_000, 2.0)],
            )],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ]
}

/// A live input in `HOUR + 1`, so HEAD still names a part once the fold drops
/// the tombstoned `HOUR`.
fn live_spec(seq: u64) -> InputSpec {
    InputSpec::new_at(
        HOUR + 1,
        Uuid::from_u128(0xC3),
        1,
        seq,
        vec![raw_series(
            "m",
            &[("k", "live")],
            &[(i64::from(HOUR + 1) * NS_PER_HOUR + seq as i64, seq as f64)],
        )],
    )
}

async fn seed_bucket(store: &dyn ObjectStoreBackend) -> Bucket {
    for spec in metrics_specs() {
        seed_input(store, &spec).await;
    }
    bucket()
}

fn tombstone_key(b: &Bucket) -> String {
    keys::retention_tombstone_key(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
        .expect("tombstone key")
}

fn retention_marker_key(b: &Bucket) -> String {
    keys::retention_unnamed_marker_key(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
        .expect("marker key")
}

fn head_key() -> String {
    format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash().to_hex(),
        Signal::Metrics.key_prefix()
    )
}

async fn read_marker(store: &dyn ObjectStoreBackend, key: &str) -> Option<UnnamedMarker> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => Some(UnnamedMarker::decode(got.data.as_ref()).expect("marker decodes")),
        Err(StoreError::NotFound) => None,
        Err(e) => panic!("marker GET {key}: {e}"),
    }
}

async fn marker_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    list_all(
        store,
        &keys::unnamed_marker_prefix(&tenant_hash(), Signal::Metrics),
    )
    .await
    .expect("list markers")
    .into_iter()
    .map(|m| m.key)
    .collect()
}

async fn bucket_commit_keys(store: &dyn ObjectStoreBackend, b: &Bucket) -> BTreeSet<String> {
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .expect("prefix");
    list_all(store, &prefix)
        .await
        .expect("list bucket")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

/// Only the tombstone left: every other commit-prefix key of the bucket is gone.
async fn swept_but_tombstone(store: &dyn ObjectStoreBackend, b: &Bucket) -> bool {
    bucket_commit_keys(store, b)
        .await
        .iter()
        .all(|k| *k == tombstone_key(b))
}

async fn superseded_at(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    b: &Bucket,
    now_ns: i64,
) -> ravel_maintain::SupersededSweepOutcome {
    sweep_superseded(
        store,
        &FixedClock::new(now_ns),
        config,
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 2")
}

async fn retention_pass(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    b: &Bucket,
) -> RetentionOutcome {
    retention_sweep_bucket(
        store,
        clock,
        config,
        &retention_at_floor(config),
        &NoLeases,
        b,
    )
    .await
    .expect("retention pass")
}

async fn retention_at(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    b: &Bucket,
    now_ns: i64,
) -> RetentionOutcome {
    retention_pass(store, &FixedClock::new(now_ns), config, b).await
}

/// Seed the bucket, tombstone it at [`sealed_now_ns`], and return the first
/// instant past the tombstone's protection horizon.
async fn tombstoned(store: &dyn ObjectStoreBackend, config: &CompactorConfig) -> (Bucket, i64) {
    let b = seed_bucket(store).await;
    let created = sealed_now_ns();
    assert_eq!(
        retention_at(store, config, &b, created).await,
        RetentionOutcome::Tombstoned
    );
    (b, created + config.protection_horizon_ns + 1)
}

const PINNED: RetentionOutcome = RetentionOutcome::BlockedBySnapshot(SnapshotBlock::PinnedWindow);
const UNREADABLE: RetentionOutcome = RetentionOutcome::BlockedBySnapshot(SnapshotBlock::Unreadable);

async fn fold_head_with(store: Arc<dyn ObjectStoreBackend>, folder: Uuid, now_ns: i64) -> bool {
    let catalog = ravel_catalog::Catalog::new(
        store,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    catalog
        .fold(&tenant_hash(), Signal::Metrics, folder, now_ns, &[], None)
        .await
        .is_ok()
}

async fn fold_head(store: &Arc<MemoryStore>, now_ns: i64) {
    assert!(
        fold_head_with(store.clone(), Uuid::new_v4(), now_ns).await,
        "fold"
    );
}

fn decode_head(bytes: &[u8]) -> ravel_proto::catalog::v1::SnapshotHead {
    ravel_catalog::decode_head(bytes).expect("HEAD decodes")
}

// --- decision 3: the condition, term by term -------------------------------

/// Each term of the condition, alone, pinned one nanosecond each side: the
/// marker is written at `t1`, the bucket is held at `t1 + window - 1` and swept
/// at `t1 + window`. The skew-only case is what makes `4 *` load-bearing: with
/// `3 *` it would sweep at `t1 + 3 * skew`, inside the held instant below, and
/// `<` for `<=` would hold at the swept instant. The default case runs all
/// three together.
#[tokio::test]
async fn each_window_term_is_pinned_one_nanosecond_each_side() {
    let zero = CompactorConfig {
        max_query_duration_ns: 0,
        head_cache_ttl_ns: 0,
        clock_skew_allowance_ns: 0,
        ..CompactorConfig::default()
    };
    let cases = [
        (
            "max_query_duration",
            CompactorConfig {
                max_query_duration_ns: 3_600 * SECOND,
                ..zero.clone()
            },
            3_600 * SECOND,
        ),
        (
            "head_cache_ttl",
            CompactorConfig {
                head_cache_ttl_ns: 30 * SECOND,
                ..zero.clone()
            },
            30 * SECOND,
        ),
        (
            "4 * clock_skew_allowance",
            CompactorConfig {
                clock_skew_allowance_ns: 300 * SECOND,
                ..zero.clone()
            },
            1_200 * SECOND,
        ),
        ("all three", cfg(), (3_600 + 30 + 1_200) * SECOND),
    ];
    for (term, config, window) in cases {
        assert_eq!(window_ns(&config), window, "{term}");
        let store = MemoryStore::new();
        let (b, t1) = tombstoned(&store, &config).await;

        assert_eq!(
            retention_at(&store, &config, &b, t1).await,
            PINNED,
            "{term}"
        );
        let marker = read_marker(&store, &retention_marker_key(&b))
            .await
            .expect("the first unnamed pass writes the marker");
        assert_eq!(marker.observed_unix_ns, t1, "{term}");

        assert_eq!(
            retention_at(&store, &config, &b, t1 + window - 1).await,
            PINNED,
            "{term}: one nanosecond inside the window holds"
        );
        assert!(!swept_but_tombstone(&store, &b).await, "{term}");
        assert_eq!(
            retention_at(&store, &config, &b, t1 + window).await,
            RetentionOutcome::Swept,
            "{term}: the instant the window ends sweeps (<=)"
        );
        assert!(bucket_commit_keys(&store, &b).await.is_empty(), "{term}");
        assert!(
            marker_keys(&store).await.is_empty(),
            "{term}: marker retired"
        );
    }
}

/// The superseded-input sweep: the group of a compaction record past its
/// horizon is held while its marker ages, and deleted at the instant the
/// window ends. The marker is keyed by the record, which survives the group.
#[tokio::test]
async fn superseded_group_is_held_until_its_marker_ages() {
    let store = MemoryStore::new();
    let config = cfg();
    let b = seed_bucket(&store).await;
    let compacted_at = sealed_now_ns();
    let outcome = compact_bucket(&store, &FixedClock::new(compacted_at), &config, &b)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    let record_key = bucket_commit_keys(&store, &b)
        .await
        .into_iter()
        .find(|k| keys::parse_compaction_record_key(k).is_ok())
        .expect("compaction record");
    let marker_key = keys::record_unnamed_marker_key(&record_key).expect("marker key");

    let t1 = compacted_at + config.protection_horizon_ns + 1;
    let sweep = |now: i64| {
        let store = &store;
        let config = config.clone();
        let b = b.clone();
        async move {
            sweep_superseded(
                store,
                &FixedClock::new(now),
                &config,
                &NoLeases,
                &b.tenant_hash,
                b.signal,
                b.shard,
            )
            .await
            .expect("rule 2")
        }
    };

    let first = sweep(t1).await;
    assert_eq!((first.records_deleted, first.data_deleted), (0, 0));
    assert_eq!(
        first.held_by_pinned_window, 4,
        "two input commit records and two data objects"
    );
    assert_eq!(first.unnamed_markers.written, 1);
    assert_eq!(first.unnamed_markers.put_requests, 1);
    let marker = read_marker(&store, &marker_key).await.expect("marker");
    assert_eq!(marker.observed_unix_ns, t1);
    assert_eq!(marker.anchor.kind, MarkerKind::Superseded);
    assert_eq!(marker.anchor.key, record_key);

    let held = sweep(t1 + window_ns(&config) - 1).await;
    assert_eq!(held.held_by_pinned_window, 4);
    assert_eq!(
        held.unnamed_markers.get_requests, 1,
        "one GET of the marker body per pass while the window runs"
    );
    assert_eq!(held.unnamed_markers.put_requests, 0, "written once");

    let cleared = sweep(t1 + window_ns(&config)).await;
    assert_eq!((cleared.records_deleted, cleared.data_deleted), (2, 2));
    assert_eq!(cleared.unnamed_markers.retired, 1);
    assert!(read_marker(&store, &marker_key).await.is_none());
    assert!(
        store.head(&record_key).await.is_ok(),
        "the record the marker is keyed by survives its group"
    );
}

// --- why it cannot stall, and why HEAD and part timestamps are not used ---

/// Folds that rewrite HEAD and its one part every hour do not stall the
/// sweep: the marker is never rewritten, so the bucket goes once it has aged.
/// A gate anchored on HEAD's own timestamp would never open here, since every
/// pass reads a HEAD less than an hour old and the window is longer.
#[tokio::test]
async fn hourly_folds_rewriting_head_and_its_one_part_do_not_stall_the_sweep() {
    let store = Arc::new(MemoryStore::new());
    let config = cfg();
    let b = seed_bucket(store.as_ref()).await;
    seed_input(store.as_ref(), &live_spec(1)).await;
    let created = sealed_now_ns();
    fold_head(&store, created).await;
    assert_eq!(
        retention_at(store.as_ref(), &config, &b, created).await,
        RetentionOutcome::Tombstoned
    );

    let t1 = created + config.protection_horizon_ns + 1;
    let mut heads: Vec<(i64, Vec<String>)> = Vec::new();
    let mut outcomes = Vec::new();
    for hour in 0..3_i64 {
        let now = t1 + hour * NS_PER_HOUR;
        // A new live write, then the hourly fold: HEAD and its one part are
        // both rewritten.
        seed_input(store.as_ref(), &live_spec(2 + hour as u64)).await;
        fold_head(&store, now).await;
        let head = decode_head(&get_full(store.as_ref(), &head_key()).await);
        heads.push((
            head.created_unix_ns,
            head.parts.iter().map(|p| p.key.clone()).collect(),
        ));
        outcomes.push(retention_at(store.as_ref(), &config, &b, now).await);
    }
    for (created_ns, parts) in &heads {
        assert_eq!(parts.len(), 1, "a single-part HEAD: {heads:?}");
        assert!(*created_ns > 0);
    }
    assert!(
        heads
            .windows(2)
            .all(|w| w[0].0 < w[1].0 && w[0].1 != w[1].1),
        "every fold rewrote HEAD and its part: {heads:?}"
    );
    assert_eq!(
        outcomes,
        vec![PINNED, PINNED, RetentionOutcome::Swept],
        "held at the first two hourly passes, swept once t1 + window has passed"
    );
    assert!(bucket_commit_keys(store.as_ref(), &b).await.is_empty());
}

/// Rejected alternative 1: a fold PUTs its part, dies before the HEAD CAS, and
/// a later fold adopts those bytes in the first HEAD that drops the bucket.
/// The adopted part's `last_modified` is from the dead fold, a day before the
/// drop. The marker is written by the first pass after the drop, so the
/// bucket stays held for a whole window after it.
#[tokio::test]
async fn a_part_adopted_from_a_dead_fold_does_not_open_the_gate_early() {
    let mem = Arc::new(MemoryStore::new());
    let config = cfg();
    let b = seed_bucket(mem.as_ref()).await;
    seed_input(mem.as_ref(), &live_spec(1)).await;
    let created = sealed_now_ns();
    mem.set_clock_ms((created / 1_000_000) as u64);
    fold_head(&mem, created).await;
    assert_eq!(
        retention_at(mem.as_ref(), &config, &b, created).await,
        RetentionOutcome::Tombstoned
    );

    // The dead fold: its part PUTs land, its HEAD write does not.
    let folder = Uuid::from_u128(0xF01D);
    let dead_at = created + 2 * NS_PER_HOUR;
    mem.set_clock_ms((dead_at / 1_000_000) as u64);
    let faulting = Arc::new(FaultStore::new(
        mem.clone(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("lost HEAD write".into()))
                .with_key_contains("/HEAD"),
        ),
    ));
    assert!(
        !fold_head_with(faulting.clone(), folder, dead_at).await,
        "the fold dies at its HEAD write"
    );
    assert!(faulting.fault_count(Op::Put, FaultKind::Permanent) >= 1);

    // A day later the same fold content is published, adopting the part.
    let t1 = created + config.protection_horizon_ns + 1;
    mem.set_clock_ms((t1 / 1_000_000) as u64);
    assert!(fold_head_with(mem.clone(), folder, dead_at).await);
    let head = decode_head(&get_full(mem.as_ref(), &head_key()).await);
    let part = mem.head(&head.parts[0].key).await.expect("part");
    assert_eq!(
        part.last_modified_unix_ms,
        dead_at / 1_000_000,
        "the published HEAD names the part the dead fold wrote"
    );
    assert!(t1 - dead_at > window_ns(&config));

    assert_eq!(retention_at(mem.as_ref(), &config, &b, t1).await, PINNED);
    assert_eq!(
        retention_at(mem.as_ref(), &config, &b, t1 + window_ns(&config) - 1).await,
        PINNED,
        "held for a whole window after the drop"
    );
    assert_eq!(
        retention_at(mem.as_ref(), &config, &b, t1 + window_ns(&config)).await,
        RetentionOutcome::Swept
    );
}

// --- the clock-reading amendment -------------------------------------------

/// A store that records every mutating operation in order, and can step a
/// shared clock forward when HEAD is read, so a test can place a drop between
/// a pass's start and the moment its HEAD GET returns.
struct LoggingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: Mutex<Vec<(&'static str, String)>>,
    on_head_get: Option<(Arc<FixedClock>, i64)>,
}

impl LoggingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Self {
        Self {
            inner,
            log: Mutex::new(Vec::new()),
            on_head_get: None,
        }
    }

    fn deletes(&self) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, _)| *op == "delete")
            .map(|(_, k)| k.clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for LoggingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.log.lock().unwrap().push(("put", key.to_string()));
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let got = self.inner.get(key, range).await;
        if key.ends_with("/HEAD")
            && let Some((clock, at)) = &self.on_head_get
        {
            clock.set(*at);
        }
        got
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
        self.log.lock().unwrap().push(("delete", key.to_string()));
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// The pass starts before the fold drops the bucket and reads HEAD after it.
/// The marker's `observed_unix_ns` is the sweeper's clock after that HEAD GET
/// returned, never the pass's start reading, which predates the drop.
#[tokio::test]
async fn the_marker_is_stamped_after_the_head_get_not_at_the_pass_start() {
    let mem = Arc::new(MemoryStore::new());
    let config = cfg();
    let b = seed_bucket(mem.as_ref()).await;
    seed_input(mem.as_ref(), &live_spec(1)).await;
    let created = sealed_now_ns();
    fold_head(&mem, created).await;
    assert_eq!(
        retention_at(mem.as_ref(), &config, &b, created).await,
        RetentionOutcome::Tombstoned
    );

    let pass_start = created + config.protection_horizon_ns + 1;
    let drop_at = pass_start + 10 * SECOND;
    let head_returned = pass_start + 20 * SECOND;
    fold_head(&mem, drop_at).await;

    let clock = Arc::new(FixedClock::new(pass_start));
    let mut store = LoggingStore::new(mem.clone());
    store.on_head_get = Some((clock.clone(), head_returned));
    assert_eq!(
        retention_pass(&store, clock.as_ref(), &config, &b).await,
        PINNED
    );

    let marker = read_marker(mem.as_ref(), &retention_marker_key(&b))
        .await
        .expect("marker");
    assert!(pass_start < drop_at, "the pass started before the drop");
    assert_eq!(
        marker.observed_unix_ns, head_returned,
        "stamped after the HEAD GET, not at the pass start"
    );
}

// --- decisions 2 and 5 -----------------------------------------------------

/// Decision 2: a marker left behind by an older sweeper, which deleted the
/// tombstone it was written for, counts as absent for the replacement
/// tombstone. It is deleted, a fresh one is written, and the window restarts.
#[tokio::test]
async fn a_stale_marker_does_not_count_for_a_replacement_tombstone() {
    let store = MemoryStore::new();
    let config = cfg();
    let (b, t1) = tombstoned(&store, &config).await;
    assert_eq!(retention_at(&store, &config, &b, t1).await, PINNED);
    let stale = read_marker(&store, &retention_marker_key(&b))
        .await
        .expect("marker");

    // The older sweeper deletes the tombstone and leaves the marker; a later
    // pass retires the bucket again.
    store.delete(&tombstone_key(&b)).await.expect("delete");
    let t2 = t1 + 10 * NS_PER_HOUR;
    assert_eq!(
        retention_at(&store, &config, &b, t2).await,
        RetentionOutcome::Tombstoned
    );
    let t3 = t2 + config.protection_horizon_ns + 1;
    assert!(stale.observed_unix_ns + window_ns(&config) <= t3, "aged");

    assert_eq!(
        retention_at(&store, &config, &b, t3).await,
        PINNED,
        "the aged marker was written for the tombstone that is gone"
    );
    let fresh = read_marker(&store, &retention_marker_key(&b))
        .await
        .expect("fresh marker");
    assert_eq!(fresh.observed_unix_ns, t3, "the window restarted");
    assert_ne!(fresh.anchor, stale.anchor);
    assert_eq!(
        retention_at(&store, &config, &b, t3 + window_ns(&config)).await,
        RetentionOutcome::Swept
    );
}

/// The superseded half of decision 2: a marker at the record's key whose
/// anchor identity is another record's is replaced, not used.
#[tokio::test]
async fn a_marker_for_another_record_is_replaced_and_restarts_the_window() {
    let store = MemoryStore::new();
    let config = cfg();
    let b = seed_bucket(&store).await;
    let compacted_at = sealed_now_ns();
    compact_bucket(&store, &FixedClock::new(compacted_at), &config, &b)
        .await
        .expect("compact");
    let record_key = bucket_commit_keys(&store, &b)
        .await
        .into_iter()
        .find(|k| keys::parse_compaction_record_key(k).is_ok())
        .expect("record");
    let got = store
        .get(&record_key, GetRange::Full)
        .await
        .expect("record");
    let marker_key = keys::record_unnamed_marker_key(&record_key).expect("key");
    let stale = UnnamedMarker {
        observed_unix_ns: 0,
        anchor: MarkerAnchor {
            kind: MarkerKind::Superseded,
            key: record_key.clone(),
            anchor_unix_ns: compacted_at,
            version: format!("{}-older", got.version.0),
        },
        head_version: String::new(),
    };
    store
        .put(&marker_key, stale.encode().into(), PutOptions::default())
        .await
        .expect("seed stale marker");

    let t1 = compacted_at + config.protection_horizon_ns + 1;
    let out = sweep_superseded(
        &store,
        &FixedClock::new(t1),
        &config,
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 2");
    assert_eq!((out.records_deleted, out.data_deleted), (0, 0));
    assert_eq!(out.held_by_pinned_window, 4);
    assert_eq!(out.unnamed_markers.reset_mismatched, 1);
    assert_eq!(out.unnamed_markers.written, 1);
    let fresh = read_marker(&store, &marker_key).await.expect("fresh");
    assert_eq!(fresh.observed_unix_ns, t1);
    assert_eq!(fresh.anchor.version, got.version.0);
}

/// Decision 5: a candidate HEAD names again has its marker deleted, and the
/// next unnamed observation starts a fresh window.
#[tokio::test]
async fn a_renamed_candidate_restarts_its_window() {
    let store = Arc::new(MemoryStore::new());
    let config = cfg();
    let b = seed_bucket(store.as_ref()).await;
    seed_input(store.as_ref(), &live_spec(1)).await;
    let created = sealed_now_ns();
    fold_head(&store, created).await;
    let naming_head = get_full(store.as_ref(), &head_key()).await;
    assert_eq!(
        retention_at(store.as_ref(), &config, &b, created).await,
        RetentionOutcome::Tombstoned
    );
    let t1 = created + config.protection_horizon_ns + 1;
    fold_head(&store, t1).await;
    let dropping_head = get_full(store.as_ref(), &head_key()).await;
    assert_eq!(retention_at(store.as_ref(), &config, &b, t1).await, PINNED);

    let put_head = |bytes: Bytes| {
        let store = store.clone();
        async move {
            store
                .put(&head_key(), bytes, PutOptions::default())
                .await
                .expect("put HEAD");
        }
    };
    let t2 = t1 + window_ns(&config);
    put_head(naming_head).await;
    assert_eq!(
        retention_at(store.as_ref(), &config, &b, t2).await,
        RetentionOutcome::BlockedBySnapshot(SnapshotBlock::Named)
    );
    assert!(
        read_marker(store.as_ref(), &retention_marker_key(&b))
            .await
            .is_none(),
        "the re-named candidate's marker is deleted"
    );

    put_head(dropping_head).await;
    assert_eq!(
        retention_at(store.as_ref(), &config, &b, t2).await,
        PINNED,
        "the first marker would have aged by now; the fresh one has not"
    );
    let fresh = read_marker(store.as_ref(), &retention_marker_key(&b))
        .await
        .expect("fresh marker");
    assert_eq!(fresh.observed_unix_ns, t2);
    assert_eq!(
        retention_at(store.as_ref(), &config, &b, t2 + window_ns(&config)).await,
        RetentionOutcome::Swept
    );
}

// --- decision 6: any doubt blocks ------------------------------------------

fn faulting_on(op: Op, fault: ScriptedFault, key: &str) -> FaultStore<MemoryStore> {
    FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(Rule::new(op, fault).with_key_contains(key.to_string())),
    )
}

#[tokio::test]
async fn a_marker_put_error_blocks_unreadable() {
    let config = cfg();
    let store = faulting_on(
        Op::Put,
        ScriptedFault::Permanent("denied".into()),
        "/maint/unn/",
    );
    let (b, t1) = tombstoned(&store, &config).await;
    assert_eq!(retention_at(&store, &config, &b, t1).await, UNREADABLE);
    assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 1);
    assert!(marker_keys(&store).await.is_empty());
    assert!(!swept_but_tombstone(&store, &b).await, "nothing deleted");
}

#[tokio::test]
async fn a_marker_get_error_blocks_unreadable() {
    let config = cfg();
    let store = faulting_on(
        Op::Get,
        ScriptedFault::Transient("get refused".into()),
        "/maint/unn/",
    );
    let (b, t1) = tombstoned(&store, &config).await;
    // The PUT goes through; the next pass must GET the body.
    assert_eq!(retention_at(&store, &config, &b, t1).await, PINNED);
    assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 0);
    assert_eq!(
        retention_at(&store, &config, &b, t1 + window_ns(&config)).await,
        UNREADABLE,
        "an aged marker that cannot be read does not permit"
    );
    assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
    assert!(!swept_but_tombstone(&store, &b).await, "nothing deleted");
}

/// A failed marker delete at retirement keeps the tombstone, so the bucket
/// stays excluded and a later pass retries both.
#[tokio::test]
async fn a_marker_delete_error_keeps_the_tombstone() {
    let config = cfg();
    let store = faulting_on(
        Op::Delete,
        ScriptedFault::Permanent("denied".into()),
        "/maint/unn/",
    );
    let (b, t1) = tombstoned(&store, &config).await;
    assert_eq!(retention_at(&store, &config, &b, t1).await, PINNED);
    assert_eq!(
        retention_at(&store, &config, &b, t1 + window_ns(&config)).await,
        RetentionOutcome::SweptPartial
    );
    assert_eq!(store.fault_count(Op::Delete, FaultKind::Permanent), 1);
    assert!(
        store.head(&tombstone_key(&b)).await.is_ok(),
        "tombstone kept"
    );
    assert!(swept_but_tombstone(&store, &b).await);
    assert_eq!(marker_keys(&store).await.len(), 1);
}

/// The other two marker deletes, on the re-name and the mismatch paths, block
/// the same way.
#[tokio::test]
async fn a_marker_delete_error_on_a_mismatch_blocks_unreadable() {
    let config = cfg();
    let store = faulting_on(
        Op::Delete,
        ScriptedFault::Permanent("denied".into()),
        "/maint/unn/",
    );
    let (b, t1) = tombstoned(&store, &config).await;
    assert_eq!(retention_at(&store, &config, &b, t1).await, PINNED);
    store.delete(&tombstone_key(&b)).await.expect("delete");
    let t2 = t1 + NS_PER_HOUR;
    assert_eq!(
        retention_at(&store, &config, &b, t2).await,
        RetentionOutcome::Tombstoned
    );
    assert_eq!(
        retention_at(&store, &config, &b, t2 + config.protection_horizon_ns + 1).await,
        UNREADABLE
    );
    assert_eq!(store.fault_count(Op::Delete, FaultKind::Permanent), 1);
}

#[tokio::test]
async fn an_undecodable_marker_body_blocks_unreadable() {
    let store = MemoryStore::new();
    let config = cfg();
    let (b, t1) = tombstoned(&store, &config).await;
    store
        .put(
            &retention_marker_key(&b),
            Bytes::from_static(&[0xff, 0xff, 0xff]),
            PutOptions::default(),
        )
        .await
        .expect("garbage marker");
    assert_eq!(
        retention_at(&store, &config, &b, t1 + window_ns(&config)).await,
        UNREADABLE
    );
    assert!(!swept_but_tombstone(&store, &b).await);
}

/// The anchor itself cannot be read: the pass fails rather than deleting, and
/// writes no marker.
#[tokio::test]
async fn an_unreadable_anchor_blocks() {
    let config = cfg();
    let store = faulting_on(
        Op::Get,
        ScriptedFault::Transient("get refused".into()),
        "retire.tmb",
    );
    let b = seed_bucket(&store).await;
    let created = sealed_now_ns();
    assert_eq!(
        retention_at(&store, &config, &b, created).await,
        RetentionOutcome::Tombstoned
    );
    let t1 = created + config.protection_horizon_ns + 1;
    let result = retention_sweep_bucket(
        &store,
        &FixedClock::new(t1),
        &config,
        &retention_at_floor(&config),
        &NoLeases,
        &b,
    )
    .await;
    assert!(result.is_err(), "{result:?}");
    assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
    assert!(marker_keys(&store).await.is_empty());
    assert!(!swept_but_tombstone(&store, &b).await);
}

/// The superseded sweep under a marker PUT error: every object of the group
/// is held as unreadable, and nothing is deleted.
#[tokio::test]
async fn a_superseded_marker_put_error_holds_the_group_unreadable() {
    let config = cfg();
    let store = faulting_on(
        Op::Put,
        ScriptedFault::Permanent("denied".into()),
        "/maint/unn/",
    );
    let b = seed_bucket(&store).await;
    let compacted_at = sealed_now_ns();
    compact_bucket(&store, &FixedClock::new(compacted_at), &config, &b)
        .await
        .expect("compact");
    let out = sweep_superseded(
        &store,
        &FixedClock::new(compacted_at + config.protection_horizon_ns + 1),
        &config,
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 2");
    assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 1);
    assert_eq!((out.records_deleted, out.data_deleted), (0, 0));
    assert_eq!(out.held_by_unreadable_head, 4);
}

// --- scope: dry runs ------------------------------------------------------

/// A dry run writes no marker. With a nonzero window it reports what the
/// deleting pass would do at that instant, which is to hold.
#[tokio::test]
async fn a_dry_run_writes_no_marker() {
    let store = MemoryStore::new();
    let config = cfg();
    let dry = CompactorConfig {
        dry_run: true,
        ..config.clone()
    };
    let (b, t1) = tombstoned(&store, &config).await;
    assert_eq!(retention_at(&store, &dry, &b, t1).await, PINNED);
    assert!(marker_keys(&store).await.is_empty());

    let store = MemoryStore::new();
    let b = seed_bucket(&store).await;
    let compacted_at = sealed_now_ns();
    compact_bucket(&store, &FixedClock::new(compacted_at), &config, &b)
        .await
        .expect("compact");
    let out = sweep_superseded(
        &store,
        &FixedClock::new(compacted_at + config.protection_horizon_ns + 1),
        &dry,
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("dry rule 2");
    assert_eq!((out.records_deleted, out.data_deleted), (0, 0));
    assert_eq!(out.held_by_pinned_window, 4);
    assert_eq!(out.unnamed_markers.put_requests, 0);
    assert_eq!(out.unnamed_markers.delete_requests, 0);
    assert!(marker_keys(&store).await.is_empty());
}

// --- delete order --------------------------------------------------------

/// Retention deletes the bucket's objects, then the marker, then the
/// tombstone last.
#[tokio::test]
async fn retention_deletes_objects_then_marker_then_tombstone() {
    let mem = Arc::new(MemoryStore::new());
    let config = cfg();
    let (b, t1) = tombstoned(mem.as_ref(), &config).await;
    assert_eq!(retention_at(mem.as_ref(), &config, &b, t1).await, PINNED);

    let store = LoggingStore::new(mem.clone());
    assert_eq!(
        retention_at(&store, &config, &b, t1 + window_ns(&config)).await,
        RetentionOutcome::Swept
    );
    let deletes = store.deletes();
    let marker = retention_marker_key(&b);
    let tombstone = tombstone_key(&b);
    let marker_at = deletes.iter().position(|k| *k == marker).expect("marker");
    let tombstone_at = deletes.iter().position(|k| *k == tombstone).expect("tmb");
    assert_eq!(
        tombstone_at,
        deletes.len() - 1,
        "tombstone last: {deletes:?}"
    );
    assert_eq!(
        marker_at,
        deletes.len() - 2,
        "marker just before: {deletes:?}"
    );
    assert!(
        marker_at >= 3,
        "two data objects and two records first: {deletes:?}"
    );
}

/// The superseded sweep deletes the group's objects, then the marker; the
/// record the marker is keyed by is not in its own group and survives.
#[tokio::test]
async fn superseded_deletes_objects_then_the_marker() {
    let mem = Arc::new(MemoryStore::new());
    let config = cfg();
    let b = seed_bucket(mem.as_ref()).await;
    let compacted_at = sealed_now_ns();
    compact_bucket(mem.as_ref(), &FixedClock::new(compacted_at), &config, &b)
        .await
        .expect("compact");
    let t1 = compacted_at + config.protection_horizon_ns + 1;
    superseded_at(mem.as_ref(), &config, &b, t1).await;
    let store = LoggingStore::new(mem.clone());
    let out = superseded_at(&store, &config, &b, t1 + window_ns(&config)).await;
    assert_eq!((out.records_deleted, out.data_deleted), (2, 2));
    let deletes = store.deletes();
    assert_eq!(deletes.len(), 5, "{deletes:?}");
    assert!(
        deletes[4].contains("/maint/unn/"),
        "the marker goes last: {deletes:?}"
    );
    assert!(deletes[..4].iter().all(|k| !k.contains("/maint/")));
}

// --- the orphan reaper ---------------------------------------------------

fn marker_body(key: &str, observed_unix_ns: i64) -> Bytes {
    UnnamedMarker {
        observed_unix_ns,
        anchor: MarkerAnchor {
            kind: MarkerKind::Retention,
            key: key.to_string(),
            anchor_unix_ns: 1,
            version: "1".to_string(),
        },
        head_version: String::new(),
    }
    .encode()
    .into()
}

/// The reaper deletes a marker only once its anchor is gone and its
/// `observed_unix_ns` is older than the protection horizon, lists across pages
/// and shards, and counts and skips a key it cannot parse.
#[tokio::test]
async fn the_orphan_reaper_needs_a_gone_anchor_and_an_old_marker() {
    let store = MemoryStore::with_page_size(2);
    let config = cfg();
    let now = sealed_now_ns() + 10 * config.protection_horizon_ns;
    let old = now - config.protection_horizon_ns - 1;
    let at_horizon = now - config.protection_horizon_ns;
    let th = tenant_hash();
    let marker = |shard: u32, hour: u32| {
        keys::retention_unnamed_marker_key(&th, Signal::Metrics, shard, hour).expect("key")
    };

    // Gone anchor and old: reaped, on two shards.
    let reaped = [marker(1, HOUR), marker(9, HOUR)];
    // Gone anchor, observed exactly one horizon ago: kept.
    let young = marker(2, HOUR);
    // Old, anchor present: kept.
    let anchored = marker(3, HOUR);
    let anchor = keys::retention_tombstone_key(&th, Signal::Metrics, 3, HOUR).expect("tmb");
    store
        .put(&anchor, Bytes::from_static(b"tmb"), PutOptions::default())
        .await
        .expect("anchor");
    for key in &reaped {
        store
            .put(key, marker_body(key, old), PutOptions::default())
            .await
            .expect("seed");
    }
    store
        .put(
            &young,
            marker_body(&young, at_horizon),
            PutOptions::default(),
        )
        .await
        .expect("seed");
    store
        .put(
            &anchored,
            marker_body(&anchored, old),
            PutOptions::default(),
        )
        .await
        .expect("seed");
    let junk = format!(
        "{}0004/not-a-marker",
        keys::unnamed_marker_prefix(&th, Signal::Metrics)
    );
    store
        .put(&junk, Bytes::from_static(b"?"), PutOptions::default())
        .await
        .expect("junk");

    let out =
        reap_orphan_unnamed_markers(&store, &FixedClock::new(now), &config, &th, Signal::Metrics)
            .await
            .expect("reap");
    assert_eq!(out.listed, 5, "every page of the signal-wide LIST");
    assert_eq!(out.reaped, 2);
    assert_eq!(out.unparseable, 1);
    assert_eq!(out.unreadable, 0);
    assert_eq!(out.delete_requests, 2);
    assert_eq!(out.head_requests, 4, "one anchor HEAD per parsed marker");
    let left: BTreeSet<String> = marker_keys(&store).await.into_iter().collect();
    assert_eq!(left, BTreeSet::from([young, anchored, junk]));
}

/// A deleting pass that gates a candidate also runs the reaper: an orphan from
/// another shard is gone after it.
#[tokio::test]
async fn a_gating_pass_reaps_orphans() {
    let store = MemoryStore::new();
    let config = cfg();
    let (b, t1) = tombstoned(&store, &config).await;
    let orphan =
        keys::retention_unnamed_marker_key(&tenant_hash(), Signal::Metrics, 2, HOUR).expect("key");
    store
        .put(
            &orphan,
            marker_body(&orphan, t1 - config.protection_horizon_ns - 1),
            PutOptions::default(),
        )
        .await
        .expect("orphan");
    assert_eq!(retention_at(&store, &config, &b, t1).await, PINNED);
    assert_eq!(marker_keys(&store).await, vec![retention_marker_key(&b)]);
}

// --- what markers do not disturb ----------------------------------------

/// A marker sits outside every commit prefix: the bucket and shard listings
/// never return one, a scan pass over the shard runs clean beside it, and the
/// tenant stays discoverable while one is left.
#[tokio::test]
async fn markers_stay_out_of_commit_listings_and_keep_the_tenant_discoverable() {
    let store = MemoryStore::new();
    let config = cfg();
    let (b, t1) = tombstoned(&store, &config).await;
    assert_eq!(retention_at(&store, &config, &b, t1).await, PINNED);
    let marker = retention_marker_key(&b);
    assert!(store.head(&marker).await.is_ok());

    let shard_prefix =
        keys::commit_shard_prefix(&b.tenant_hash, b.signal, b.shard).expect("prefix");
    for meta in list_all(&store, &shard_prefix).await.expect("list") {
        assert!(!meta.key.contains("/maint/"), "{}", meta.key);
        keys::partition_bucket_entry(&meta.key).expect("every c/ key classifies");
    }
    ravel_maintain::scan::scan_and_maintain(
        &store,
        &FixedClock::new(t1 + 1),
        &config,
        &retention_at_floor(&config),
        &NoLeases,
        b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("a scan pass runs beside a marker");

    // Everything but the marker gone: the tenant is still discovered.
    for meta in list_all(&store, "t/").await.expect("list") {
        if meta.key != marker {
            store.delete(&meta.key).await.expect("delete");
        }
    }
    assert_eq!(
        ravel_maintain::discover::discover_tenants(&store)
            .await
            .expect("discover"),
        vec![b.tenant_hash]
    );
}

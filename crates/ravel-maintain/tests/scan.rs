//! Sealed-bucket scan and advisory CAS cursor (plan §3.2). Covers multi-hour
//! walking, cursor advancement, stopping at the first unsealed hour, and the
//! cursor's re-scan-avoidance on a second pass.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::scan::{
    DEFAULT_MEMO_REVERIFY_INTERVAL_NS, DEFAULT_MEMO_SNAPSHOT_STALENESS_NS, MaintainMemo,
    TerminalState, Zone, classify_zone, scan_and_maintain_with_memo,
};
use ravel_maintain::{
    Bucket, CompactorConfig, FixedClock, NoLeases, RetentionConfig, RetentionPolicy,
    scan_and_compact,
};
use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::Signal;
use uuid::Uuid;

fn two_at(hour: u32, base: u128) -> Vec<InputSpec> {
    vec![
        InputSpec::new_at(
            hour,
            Uuid::from_u128(base),
            1,
            1,
            vec![raw_series("m", &[("k", "a")], &[(1_000, 1.0)])],
        ),
        InputSpec::new_at(
            hour,
            Uuid::from_u128(base + 1),
            1,
            2,
            vec![raw_series("m", &[("k", "a")], &[(2_000, 2.0)])],
        ),
    ]
}

#[tokio::test]
async fn scan_compacts_all_sealed_hours_and_advances_cursor() {
    let store = MemoryStore::new();
    for s in two_at(HOUR, 10).into_iter().chain(two_at(HOUR + 1, 20)) {
        seed_input(&store, &s).await;
    }
    let clock = FixedClock::new((i64::from(HOUR + 1) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR);

    let report = scan_and_compact(
        &store,
        &clock,
        &CompactorConfig::default(),
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan");
    assert_eq!(report.compacted, 2);
    assert_eq!(report.cursor_advanced_to, Some(HOUR + 1));

    // Both buckets now carry a compaction record.
    let record_a = fetch_compaction_record(&store, &bucket_at(HOUR)).await;
    let record_b = fetch_compaction_record(&store, &bucket_at(HOUR + 1)).await;
    assert_eq!(record_a.inputs.len(), 2);
    assert_eq!(record_b.inputs.len(), 2);

    // The advisory cursor object exists.
    let cursor_key = keys::maint_cursor_key(&tenant_hash(), Signal::Metrics, SHARD).unwrap();
    assert!(store.get(&cursor_key, GetRange::Full).await.is_ok());

    // Second pass: cursor skips both done hours; nothing new compacted.
    let report2 = scan_and_compact(
        &store,
        &clock,
        &CompactorConfig::default(),
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan 2");
    assert_eq!(report2.compacted, 0);
    assert_eq!(report2.already_done, 0, "cursor skips already-done hours");
}

#[tokio::test]
async fn scan_stops_at_first_unsealed_hour() {
    let store = MemoryStore::new();
    // HOUR is sealed; a far-future hour is not.
    let future = HOUR + 5;
    for s in two_at(HOUR, 10).into_iter().chain(two_at(future, 20)) {
        seed_input(&store, &s).await;
    }
    // now seals HOUR but not `future`.
    let clock = FixedClock::new((i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR);

    let report = scan_and_compact(
        &store,
        &clock,
        &CompactorConfig::default(),
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan");
    assert_eq!(report.compacted, 1);
    assert_eq!(report.not_sealed, 1);
    assert_eq!(report.cursor_advanced_to, Some(HOUR));
    // The unsealed future bucket is untouched.
    assert_eq!(
        ravel_maintain::compact_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket_at(future)
        )
        .await
        .expect("check"),
        ravel_maintain::CompactionOutcome::NotSealed
    );
}

// --- Terminal-bucket memo (issue #280) -------------------------------------
//
// The maintain loop re-lists and re-reads every retained bucket on every tick.
// A per-worker `MaintainMemo` records buckets already known terminal so
// steady-state ticks skip re-listing them, with a periodic full re-verify that
// catches staleness by design. These tests count store calls through an
// `InstrumentedStore` to prove the reduction, prove a fresh (restarted) memo
// re-evaluates everything, and prove the re-verify interval re-lists a memoized
// bucket and catches a since-appeared retention expiry.

const DAY_NS: i64 = 24 * NS_PER_HOUR;

/// Two sealed, compactable L0 inputs for `hour`, their samples placed inside the
/// ingest hour so the bucket's `max_event_ts` is ~the hour start (so retention
/// math against a multi-day window is intuitive).
fn compactable_at(hour: u32, base: u128) -> Vec<InputSpec> {
    let t = i64::from(hour) * NS_PER_HOUR;
    vec![
        InputSpec::new_at(
            hour,
            Uuid::from_u128(base),
            1,
            1,
            vec![raw_series("m", &[("k", "a")], &[(t + 1_000_000, 1.0)])],
        ),
        InputSpec::new_at(
            hour,
            Uuid::from_u128(base + 1),
            1,
            2,
            vec![raw_series("m", &[("k", "a")], &[(t + 2_000_000, 2.0)])],
        ),
    ]
}

/// A retention config with one default window applied to every tenant.
fn retention_window(window_ns: i64) -> RetentionConfig {
    RetentionConfig::from_policy(
        RetentionPolicy {
            default: Some(window_ns),
            tenants: Vec::new(),
        },
        &CompactorConfig::default(),
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("retention config within floor")
}

fn list_delta(before: &StoreMetricsSnapshot, after: &StoreMetricsSnapshot) -> u64 {
    after.list.calls - before.list.calls
}

fn list_delimited_delta(before: &StoreMetricsSnapshot, after: &StoreMetricsSnapshot) -> u64 {
    after.list_delimited.calls - before.list_delimited.calls
}

fn get_delta(before: &StoreMetricsSnapshot, after: &StoreMetricsSnapshot) -> u64 {
    after.get.calls - before.get.calls
}

async fn has_tombstone(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> bool {
    let key = keys::retention_tombstone_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .expect("tombstone key");
    store.head(&key).await.is_ok()
}

/// After the memo warms up, a steady-state tick over an all-terminal population
/// skips every bucket and issues strictly fewer LIST and GET calls than the cold
/// tick that populated the memo. The cold tick pays 2 LISTs + 3 GETs per bucket
/// (retention's and compaction's `list_bucket`, plus the two commit records and
/// one compaction record retention reads to evaluate expiry); the warm tick pays
/// only the single shard-level `list_delimited`.
#[tokio::test]
async fn warm_memo_skips_terminal_buckets_with_fewer_list_and_get_calls() {
    const N: u32 = 5;
    let store = InstrumentedStore::new(MemoryStore::new());
    for h in 0..N {
        for s in compactable_at(HOUR + h, 100 + u128::from(h) * 10) {
            seed_input(&store, &s).await;
        }
    }
    let now = (i64::from(HOUR + N) + 3) * NS_PER_HOUR;
    let clock = FixedClock::new(now);
    let config = CompactorConfig::default();
    // 30-day window: a few-hour-old bucket is retained (not expired), so every
    // cold tick's retention pass actually lists and GETs the bucket's records.
    let retention = retention_window(30 * DAY_NS);
    let mut memo = MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS);

    // Tick 1: compacts all N buckets. A just-compacted bucket is not memoized
    // until it reaches the stable AlreadyCompacted state.
    let r1 = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("tick 1");
    assert_eq!(r1.compacted, N as usize);
    assert_eq!(r1.skipped_terminal, 0);
    assert_eq!(memo.len(), 0, "a just-compacted bucket is not yet memoized");

    // Tick 2 (cold): every bucket is now AlreadyCompacted + retained, so it is
    // terminal and gets memoized. This tick still does the full per-bucket work.
    let before_cold = store.metrics().snapshot();
    let r2 = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("tick 2");
    let after_cold = store.metrics().snapshot();
    assert_eq!(r2.already_done, N as usize);
    assert_eq!(r2.skipped_terminal, 0);
    assert_eq!(memo.len(), N as usize);
    for h in 0..N {
        assert_eq!(
            memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR + h),
            Some(TerminalState::Compacted),
        );
    }
    let cold_list = list_delta(&before_cold, &after_cold);
    let cold_get = get_delta(&before_cold, &after_cold);
    assert_eq!(list_delimited_delta(&before_cold, &after_cold), 1);
    assert_eq!(
        cold_list,
        2 * u64::from(N),
        "2 list_bucket LISTs per bucket"
    );
    assert_eq!(
        cold_get,
        3 * u64::from(N),
        "2 commit + 1 compaction GET per bucket"
    );

    // Tick 3 (warm): every bucket is skipped straight from the memo.
    let before_warm = store.metrics().snapshot();
    let r3 = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("tick 3");
    let after_warm = store.metrics().snapshot();
    assert_eq!(r3.skipped_terminal, N as usize);
    assert_eq!(r3.already_done, 0);
    // The warm steady-state tick issues only the single shard-level
    // list_delimited: no per-bucket List, no GET.
    assert_eq!(list_delimited_delta(&before_warm, &after_warm), 1);
    assert_eq!(
        list_delta(&before_warm, &after_warm),
        0,
        "no per-bucket LISTs"
    );
    assert_eq!(get_delta(&before_warm, &after_warm), 0, "no GETs");
    assert!(0 < cold_list && 0 < cold_get, "cold tick did real work");
}

/// A worker restart drops the in-memory memo. The fresh memo's first tick must
/// skip nothing and redo the full per-bucket evaluation (identical to the
/// pre-memo behavior), even though a carried-over warm memo would have skipped
/// every bucket.
#[tokio::test]
async fn fresh_memo_after_restart_re_evaluates_and_skips_nothing() {
    const N: u32 = 3;
    let store = InstrumentedStore::new(MemoryStore::new());
    for h in 0..N {
        for s in compactable_at(HOUR + h, 200 + u128::from(h) * 10) {
            seed_input(&store, &s).await;
        }
    }
    let now = (i64::from(HOUR + N) + 3) * NS_PER_HOUR;
    let clock = FixedClock::new(now);
    let config = CompactorConfig::default();
    let retention = retention_window(30 * DAY_NS);

    // Warm a memo: compact, then reach the stable terminal state.
    let mut warm = MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS);
    for _ in 0..2 {
        scan_and_maintain_with_memo(
            &mut warm,
            &store,
            &clock,
            &config,
            &retention,
            &NoLeases,
            tenant_hash(),
            Signal::Metrics,
            SHARD,
        )
        .await
        .expect("warm-up tick");
    }
    assert_eq!(warm.len(), N as usize);
    let r_warm = scan_and_maintain_with_memo(
        &mut warm,
        &store,
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("warm tick");
    assert_eq!(
        r_warm.skipped_terminal, N as usize,
        "the warm memo would skip every bucket"
    );

    // Restart: a fresh, cold memo. Its first tick skips nothing and redoes the
    // full per-bucket work (2 LISTs + 3 GETs per bucket).
    let mut fresh = MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS);
    let before = store.metrics().snapshot();
    let r_fresh = scan_and_maintain_with_memo(
        &mut fresh,
        &store,
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("restart tick");
    let after = store.metrics().snapshot();
    assert_eq!(
        r_fresh.skipped_terminal, 0,
        "a cold memo skips nothing on the first tick"
    );
    assert_eq!(
        r_fresh.already_done, N as usize,
        "every bucket re-evaluated"
    );
    assert_eq!(list_delta(&before, &after), 2 * u64::from(N));
    assert_eq!(get_delta(&before, &after), 3 * u64::from(N));
    assert_eq!(fresh.len(), N as usize, "the fresh memo re-learned the set");
}

/// The periodic full re-verify re-lists a bucket the memo marked terminal. A
/// bucket memoized as compacted-and-not-expired is skipped while its entry is
/// fresh, but once the entry ages past the re-verify interval it is re-evaluated
/// against the store, catching a retention expiry that appeared in the meantime
/// (the memo could not have known) and tombstoning the bucket. This is what
/// keeps the memo from trusting a stale terminal verdict forever.
#[tokio::test]
async fn periodic_reverify_relists_terminal_bucket_and_catches_expiry() {
    let hour = HOUR;
    let store = InstrumentedStore::new(MemoryStore::new());
    for s in compactable_at(hour, 300) {
        seed_input(&store, &s).await;
    }
    let config = CompactorConfig::default();
    let window = 30 * DAY_NS;
    let retention = retention_window(window);
    let reverify = NS_PER_HOUR;
    let mut memo = MaintainMemo::new(reverify);

    let hour_start = i64::from(hour) * NS_PER_HOUR;
    // Sealed, bucket ~3h old, far from expiry under a 30-day window.
    let sealed = hour_start + 3 * NS_PER_HOUR;

    // Compact, then memoize the terminal (compacted, not-expired) bucket.
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(sealed),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("compact");
    let memoize_at = sealed + 1_000_000;
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(memoize_at),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("memoize");
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, hour),
        Some(TerminalState::Compacted),
    );

    // Within the re-verify interval: the fresh entry is skipped, no per-bucket
    // LIST, no tombstone.
    let fresh_at = memoize_at + reverify / 2;
    let before_fresh = store.metrics().snapshot();
    let r_fresh = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(fresh_at),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("fresh skip");
    let after_fresh = store.metrics().snapshot();
    assert_eq!(r_fresh.skipped_terminal, 1);
    assert_eq!(
        list_delta(&before_fresh, &after_fresh),
        0,
        "a fresh entry issues no per-bucket LIST"
    );
    assert!(
        !has_tombstone(&store, &bucket_at(hour)).await,
        "not tombstoned while skipped"
    );

    // Past both the re-verify interval and the retention window: the stale entry
    // forces a full re-evaluation that re-lists the bucket and catches its
    // now-elapsed expiry, tombstoning it.
    let reverify_at = hour_start + window + NS_PER_HOUR;
    let before_rev = store.metrics().snapshot();
    let r_rev = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(reverify_at),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("reverify");
    let after_rev = store.metrics().snapshot();
    assert_eq!(
        r_rev.skipped_terminal, 0,
        "a stale entry forces a full re-evaluation"
    );
    assert_eq!(
        r_rev.retired, 1,
        "the re-verify catches the now-expired bucket"
    );
    assert!(
        list_delta(&before_rev, &after_rev) >= 1,
        "the terminal bucket was re-listed"
    );
    assert!(
        has_tombstone(&store, &bucket_at(hour)).await,
        "the re-verify tombstoned the expired bucket"
    );
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, hour),
        None,
        "the no-longer-terminal bucket was forgotten"
    );
}

// --- Durable memo snapshot: warm start and handoff (ADR-0065 decision 3,
//     issue #747) --------------------------------------------------------------
//
// A worker persists its terminal-bucket memo to a durable snapshot and, on
// startup or after ownership handoff, seeds a fresh in-memory memo from it. The
// seeded worker must then skip the buckets the snapshot marks terminal without
// re-listing or re-reading them -- the durable analogue of the in-memory warm
// tick above. These tests seed from *serialized-then-deserialized* snapshot
// bytes (not a carried-over in-memory memo), proving the durability path.

/// Warm the given memo over `n` sealed compactable buckets from `HOUR` and
/// return the reached-terminal memo (two ticks: compact, then stabilize to
/// AlreadyCompacted so the buckets are memoized terminal).
async fn warm_terminal_memo(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
    retention: &RetentionConfig,
    n: u32,
) -> MaintainMemo {
    let mut memo = MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS);
    for _ in 0..2 {
        scan_and_maintain_with_memo(
            &mut memo,
            store,
            clock,
            config,
            retention,
            &NoLeases,
            tenant_hash(),
            Signal::Metrics,
            SHARD,
        )
        .await
        .expect("warm-up tick");
    }
    assert_eq!(memo.len(), n as usize, "the warm memo learned every bucket");
    memo
}

/// The named acceptance test (ADR-0065 decision 3): a worker seeded from a fresh
/// durable memo snapshot skips re-scanning the buckets its memo already marks
/// terminal, issuing no per-bucket LIST and no GET -- only the single shard-level
/// `list_delimited`. This is the durable warm start: the seed comes from encoded
/// snapshot bytes, exactly as a restarted process reads them back from storage,
/// not from a memo kept alive in memory.
#[tokio::test]
async fn warm_start_skips_terminal_buckets_without_reads() {
    const N: u32 = 6;
    let store = InstrumentedStore::new(MemoryStore::new());
    for h in 0..N {
        for s in compactable_at(HOUR + h, 400 + u128::from(h) * 10) {
            seed_input(&store, &s).await;
        }
    }
    let now = (i64::from(HOUR + N) + 3) * NS_PER_HOUR;
    let clock = FixedClock::new(now);
    let config = CompactorConfig::default();
    let retention = retention_window(30 * DAY_NS);

    // A first worker warms its memo and persists a durable snapshot.
    let warm = warm_terminal_memo(&store, &clock, &config, &retention, N).await;
    let snapshot_bytes = warm.encode_snapshot(now);

    // A second worker starts cold and warm-starts purely from the snapshot
    // bytes (the durability path: no in-memory memo carried over).
    let mut seeded = MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS);
    let stats = seeded
        .seed_from_snapshot(
            &snapshot_bytes,
            now,
            DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
            |_, _, _| true,
        )
        .expect("seed from durable snapshot");
    assert!(!stats.stale, "a fresh snapshot is trusted");
    assert_eq!(
        stats.seeded_buckets, N as usize,
        "every terminal bucket seeded"
    );
    assert_eq!(seeded.len(), N as usize);
    for h in 0..N {
        assert_eq!(
            seeded.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR + h),
            Some(TerminalState::Compacted),
            "the seeded memo carries the exact terminal verdict"
        );
    }

    // The seeded worker's first tick skips every bucket straight from the
    // seed: no redundant per-bucket LIST, no GET, only the shard listing.
    let before = store.metrics().snapshot();
    let report = scan_and_maintain_with_memo(
        &mut seeded,
        &store,
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("warm-started tick");
    let after = store.metrics().snapshot();

    assert_eq!(
        report.skipped_terminal, N as usize,
        "the warm-started worker skips every seeded terminal bucket"
    );
    assert_eq!(report.already_done, 0, "no per-bucket work redone");
    assert_eq!(
        list_delimited_delta(&before, &after),
        1,
        "only the single shard-level list_delimited"
    );
    assert_eq!(
        list_delta(&before, &after),
        0,
        "no per-bucket LISTs after a durable warm start"
    );
    assert_eq!(
        get_delta(&before, &after),
        0,
        "no GETs after a durable warm start"
    );
}

/// A stale durable snapshot (older than the staleness bound) is ignored: the
/// seeded worker gets nothing and cold-starts, re-evaluating every bucket. This
/// is the durable analogue of the periodic re-verify: a snapshot too old to hold
/// any still-fresh entry is never trusted.
#[tokio::test]
async fn stale_memo_snapshot_is_ignored_and_forces_cold_start() {
    const N: u32 = 3;
    let store = InstrumentedStore::new(MemoryStore::new());
    for h in 0..N {
        for s in compactable_at(HOUR + h, 500 + u128::from(h) * 10) {
            seed_input(&store, &s).await;
        }
    }
    let warm_now = (i64::from(HOUR + N) + 3) * NS_PER_HOUR;
    let config = CompactorConfig::default();
    let retention = retention_window(30 * DAY_NS);
    let warm = warm_terminal_memo(&store, &FixedClock::new(warm_now), &config, &retention, N).await;
    let snapshot_bytes = warm.encode_snapshot(warm_now);

    // Read the snapshot one nanosecond past the staleness bound: ignored.
    let read_now = warm_now + DEFAULT_MEMO_SNAPSHOT_STALENESS_NS + 1;
    let mut seeded = MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS);
    let stats = seeded
        .seed_from_snapshot(
            &snapshot_bytes,
            read_now,
            DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
            |_, _, _| true,
        )
        .expect("decode");
    assert!(stats.stale, "a snapshot past the bound is not trusted");
    assert!(seeded.is_empty(), "nothing seeded from a stale snapshot");

    // The cold worker's tick re-evaluates every bucket (does real per-bucket
    // work), skipping nothing.
    let before = store.metrics().snapshot();
    let report = scan_and_maintain_with_memo(
        &mut seeded,
        &store,
        &FixedClock::new(read_now),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("cold tick");
    let after = store.metrics().snapshot();
    assert_eq!(report.skipped_terminal, 0, "a stale snapshot skips nothing");
    assert!(
        get_delta(&before, &after) > 0,
        "the cold worker did real per-bucket reads"
    );
}

/// Ownership handoff (ADR-0065 decision 3): a unit moves from worker A to worker
/// B. B seeds from A's durable snapshot -- filtered to the units B now owns --
/// and skips A's terminal buckets rather than cold-starting them. Modeled by
/// seeding B's fresh memo from A's snapshot bytes with an ownership predicate
/// that accepts the handed-over unit, then proving the skip.
#[tokio::test]
async fn ownership_handoff_seeds_successor_from_predecessor_snapshot() {
    const N: u32 = 4;
    let store = InstrumentedStore::new(MemoryStore::new());
    for h in 0..N {
        for s in compactable_at(HOUR + h, 600 + u128::from(h) * 10) {
            seed_input(&store, &s).await;
        }
    }
    let now = (i64::from(HOUR + N) + 3) * NS_PER_HOUR;
    let config = CompactorConfig::default();
    let retention = retention_window(30 * DAY_NS);

    // Worker A owned (tenant, Metrics, SHARD), warmed its memo, and left a
    // durable snapshot before dropping out of the live set.
    let warm_a = warm_terminal_memo(&store, &FixedClock::new(now), &config, &retention, N).await;
    let a_snapshot = warm_a.encode_snapshot(now);

    // Worker B newly owns the unit after the handoff. It starts cold and seeds
    // from A's snapshot, keeping only units it now owns (here, the moved unit).
    let mut memo_b = MaintainMemo::new(DEFAULT_MEMO_REVERIFY_INTERVAL_NS);
    let stats = memo_b
        .seed_from_snapshot(
            &a_snapshot,
            now,
            DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
            |_, signal, shard| signal == Signal::Metrics && shard == SHARD,
        )
        .expect("B seeds from A's snapshot");
    assert_eq!(stats.seeded_units, 1, "B seeds the one unit it took over");
    assert_eq!(stats.seeded_buckets, N as usize);

    // B's first tick over the handed-over unit skips every bucket from the
    // seed: the handoff did not go cold.
    let before = store.metrics().snapshot();
    let report = scan_and_maintain_with_memo(
        &mut memo_b,
        &store,
        &FixedClock::new(now),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("B's first tick");
    let after = store.metrics().snapshot();
    assert_eq!(
        report.skipped_terminal, N as usize,
        "B skips A's terminal buckets after the handoff (not a cold rescan)"
    );
    assert_eq!(
        get_delta(&before, &after),
        0,
        "no per-bucket GETs on handoff warm start"
    );
    assert_eq!(
        list_delta(&before, &after),
        0,
        "no per-bucket LISTs on handoff warm start"
    );
}

// --- Zone-scheduled maintenance (ADR-0065 decision 3, issue #748) ----------
//
// The unit scan splits a unit's ingest hours into head/tail (evaluated every
// tick, exactly as before this split existed) and interior (memo-gated,
// re-verified only on `CompactorConfig::interior_reverify_ns`, default 6h, or
// immediately on the hour's own zone transition into `Tail` at its computed
// retention expiry). These tests pin the differentiator from the flat,
// pre-split re-verify interval (`DEFAULT_MEMO_REVERIFY_INTERVAL_NS`, 1h): an
// interior bucket memoized terminal must survive well past that 1h bound
// without being re-listed, skipping only while the interior zone's own 6h
// cadence has not yet elapsed.

/// The named acceptance test (ADR-0065 decision 3, issue #748): an interior
/// hour memoized terminal is NOT re-listed on a tick 3h later, even though
/// that is past the old flat 1h re-verify interval -- only the interior
/// zone's own 6h cadence governs it now. Proven by an exact LIST-count
/// assertion (`list_delta == 0`), not just the report's `skipped_terminal`
/// count.
#[tokio::test]
async fn interior_zone_scheduled_not_rescanned() {
    let store = InstrumentedStore::new(MemoryStore::new());
    for s in compactable_at(HOUR, 700) {
        seed_input(&store, &s).await;
    }
    let config = CompactorConfig::default();
    let retention = retention_window(30 * DAY_NS);
    let mut memo = MaintainMemo::new(config.interior_reverify_ns);

    let t1 = sealed_now_ns(); // hour_start + 3h: sealed, still Head.
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(t1),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("compact");
    let t2 = t1 + 1_000_000;
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(t2),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("memoize");
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR),
        Some(TerminalState::Compacted),
    );

    // t3 is 3h past t2: past the old flat 1h re-verify interval, but well
    // inside the interior zone's 6h cadence. The hour has also left Head by
    // now (it's been ~6h since the hour started).
    let t3 = t2 + 3 * NS_PER_HOUR;
    assert_eq!(
        classify_zone(HOUR, t3, &config, retention.window_for(&tenant_hash())),
        Zone::Interior,
        "3h past sealing, 30 days from expiry: neither head nor tail"
    );
    let before = store.metrics().snapshot();
    let report = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(t3),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("interior tick");
    let after = store.metrics().snapshot();
    assert_eq!(report.skipped_terminal, 1, "the interior hour is skipped");
    assert_eq!(
        list_delta(&before, &after),
        0,
        "an interior hour within its 6h cadence issues no per-bucket LIST"
    );
    assert!(
        report.head_tail_hours.is_empty(),
        "the hour is Interior, not Head or Tail"
    );
}

/// Interior -> Tail zone transition forces re-evaluation before the interior
/// cadence would have: even though the memoized entry is still well within
/// `interior_reverify_ns` of its last verification, the bucket's own
/// retention expiry moves it into the tail window, and tail hours bypass the
/// memo unconditionally (ADR-0065 decision 3). This is what catches an
/// expiry the interior cadence alone would have deferred.
#[tokio::test]
async fn interior_zone_expiry_transition_forces_reevaluation() {
    let store = InstrumentedStore::new(MemoryStore::new());
    for s in compactable_at(HOUR, 710) {
        seed_input(&store, &s).await;
    }
    let config = CompactorConfig::default();
    let window = 5 * NS_PER_HOUR; // above the default retention floor (~4h05m)
    let retention = retention_window(window);
    let mut memo = MaintainMemo::new(config.interior_reverify_ns);
    let hour_start = i64::from(HOUR) * NS_PER_HOUR;

    // now1: sealed, past Head (>~3h05m old), well before expiry (hour_start +
    // 5h).
    let now1 = hour_start + 4 * NS_PER_HOUR;
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(now1),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("compact");
    let now2 = now1 + 1_000_000;
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(now2),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("memoize");
    assert_eq!(
        classify_zone(HOUR, now2, &config, retention.window_for(&tenant_hash())),
        Zone::Interior,
    );
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR),
        Some(TerminalState::Compacted),
    );

    // now3: ~2h after now2 -- well inside the 6h interior cadence, so a
    // cadence-only check would still call this entry fresh -- but past the
    // bucket's computed expiry (hour_start + window), so the zone is now
    // Tail.
    let now3 = hour_start + window + NS_PER_HOUR;
    assert!(
        now3 - now2 < config.interior_reverify_ns,
        "well inside the interior cadence"
    );
    assert_eq!(
        classify_zone(HOUR, now3, &config, retention.window_for(&tenant_hash())),
        Zone::Tail,
        "past the computed expiry, within the protection horizon"
    );
    let before = store.metrics().snapshot();
    let report = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(now3),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("tail tick");
    let after = store.metrics().snapshot();
    assert_eq!(
        report.retired, 1,
        "the tail-zone re-evaluation catches the now-expired bucket"
    );
    assert!(
        list_delta(&before, &after) >= 1,
        "the tail zone bypasses the memo and re-lists the bucket"
    );
    assert_eq!(
        report.head_tail_hours,
        vec![HOUR],
        "the hour is classified Tail this tick"
    );
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR),
        None,
        "a tombstoned-pending bucket is not memoized terminal"
    );
    assert!(has_tombstone(&store, &bucket_at(HOUR)).await);
}

/// `MaintainMemo::invalidate` (ADR-0065 decision 3, issue #748) marks an
/// interior hour due for re-evaluation on the next tick regardless of the
/// interior cadence, and the invalidation survives a memo snapshot save/load
/// round trip: because it works by forgetting the entry (there is nothing to
/// encode), a snapshot taken after `invalidate()` carries no terminal verdict
/// for that hour, so a fresh memo seeded from it treats the hour as due, not
/// as freshly terminal. `invalidate()` has no production caller yet in this
/// crate: EJ-T4 (issue #754) is what wires the erasure-rewrite work orders to
/// call it.
#[tokio::test]
async fn invalidate_forces_reevaluation_and_survives_snapshot_round_trip() {
    let store = InstrumentedStore::new(MemoryStore::new());
    for s in compactable_at(HOUR, 720) {
        seed_input(&store, &s).await;
    }
    let config = CompactorConfig::default();
    let retention = retention_window(30 * DAY_NS);
    let mut memo = MaintainMemo::new(config.interior_reverify_ns);

    let t1 = sealed_now_ns();
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(t1),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("compact");
    let t2 = t1 + 1_000_000;
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(t2),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("memoize");
    let t3 = t2 + 3 * NS_PER_HOUR;
    assert_eq!(
        classify_zone(HOUR, t3, &config, retention.window_for(&tenant_hash())),
        Zone::Interior,
    );
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR),
        Some(TerminalState::Compacted),
    );

    // Invalidate at t3, still well inside the 6h interior cadence -- a
    // cadence-only check would skip this hour.
    memo.invalidate(tenant_hash(), Signal::Metrics, SHARD, &[HOUR]);
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR),
        None,
        "invalidate forgets the memoized verdict"
    );

    // The very next tick re-evaluates the hour: no skip, a real LIST.
    let before = store.metrics().snapshot();
    let report = scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(t3),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("post-invalidate tick");
    let after = store.metrics().snapshot();
    assert_eq!(
        report.skipped_terminal, 0,
        "the invalidated hour is not skipped"
    );
    assert!(
        list_delta(&before, &after) >= 1,
        "the invalidated hour is re-listed"
    );

    // Snapshot round trip: invalidate again on a freshly re-memoized entry,
    // then encode and seed a fresh memo from the snapshot bytes. The
    // invalidated hour must come back due, not freshly terminal.
    let t4 = t3 + 1_000_000;
    scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &FixedClock::new(t4),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("re-memoize");
    assert_eq!(
        memo.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR),
        Some(TerminalState::Compacted),
    );
    memo.invalidate(tenant_hash(), Signal::Metrics, SHARD, &[HOUR]);
    let snapshot_bytes = memo.encode_snapshot(t4);
    let mut seeded = MaintainMemo::new(config.interior_reverify_ns);
    let stats = seeded
        .seed_from_snapshot(
            &snapshot_bytes,
            t4,
            DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
            |_, _, _| true,
        )
        .expect("seed from post-invalidate snapshot");
    assert_eq!(
        stats.seeded_buckets, 0,
        "the invalidated hour carries no terminal verdict to seed"
    );
    assert_eq!(
        seeded.terminal_state(tenant_hash(), Signal::Metrics, SHARD, HOUR),
        None,
        "the invalidated hour stays due after a snapshot save/load round trip"
    );

    // The warm-started worker's first tick re-evaluates the hour rather than
    // skipping it straight from the seed.
    let before2 = store.metrics().snapshot();
    let report2 = scan_and_maintain_with_memo(
        &mut seeded,
        &store,
        &FixedClock::new(t4),
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("warm-started tick after invalidate");
    let after2 = store.metrics().snapshot();
    assert_eq!(report2.skipped_terminal, 0);
    assert!(list_delta(&before2, &after2) >= 1);
}

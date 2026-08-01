//! Sealed-bucket scan and advisory CAS cursor (plan §3.2). Covers multi-hour
//! walking, cursor advancement, stopping at the first unsealed hour, and the
//! cursor's re-scan-avoidance on a second pass.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::scan::{
    DEFAULT_MEMO_REVERIFY_INTERVAL_NS, MaintainMemo, TerminalState, scan_and_maintain_with_memo,
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

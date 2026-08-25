//! ADR-0046's read cache: a RAM tier and a local-disk tier
//! (docs/adrs/0046-read-cache-tier.md, decisions 1-7). This crate is not
//! wired into anything yet: it does not depend on `ravel-types`, does not
//! touch `ravel-catalog` or `ravel-query`, and does not reference
//! `QueryAccounting`. Later tasks in this epic connect it to those.
//!
//! This crate is consulted at a read funnel; it is never a store
//! decorator (decision 1).
//!
//! [`CacheKey`] is content-addressed (decision 2): `(tenant_hash,
//! content_hash, offset, len)`, with no constructor from an object key
//! string, because the mutable objects that own such strings (the
//! catalog HEAD, the maintenance cursor) have no content hash and must
//! stay unrepresentable rather than merely un-cached-by-convention.
//!
//! [`Cache`] is the RAM tier. It bounds total bytes, entry count, and
//! maximum single-entry size (deliverable 4); evicts with S3-FIFO, not
//! LRU, because the compactor and the folder scan cold data in the same
//! process as queries and a single scan must not evict the query working
//! set (decision 6, see [`s3fifo`]); collapses concurrent misses on one
//! key into a single upstream call (decision 5, see [`single_flight`]);
//! and has a constructor, [`Cache::with_corruption`], that makes every
//! hit return deliberately corrupted bytes -- the supported
//! acceptance-gate mode for the whole epic (decision 4), not a test
//! fixture. Like the disk tier, each RAM entry carries a stamped write
//! time and a configured per-entry max-age ([`DEFAULT_MAX_ENTRY_AGE_NS`]):
//! an entry older than the max-age is treated as a miss on `get`, and a
//! background sweep drops over-age idle entries on [`DEFAULT_SWEEP_INTERVAL_NS`]
//! regardless of access, so raw bytes of a subject erased by ADR-0064's sweep
//! persist in a query node's RAM at most the max-age plus one sweep interval
//! past the sweep, a sum the defaults keep within 24 h.
//! The maintain node's erasure sweep cannot reach a query node's memory, so the
//! bound is enforced locally, the same way it is for the disk tier.
//!
//! [`disk::DiskCache`] is the local-disk tier: content-addressed raw byte
//! ranges under a configured directory, opt-in (no directory, no disk
//! tier, behavior unchanged), plaintext (decision 7 -- **with SSE-KMS
//! configured, cached bytes on local disk are not protected by that
//! key**), evicted with the same [`s3fifo`] policy as [`Cache`] rather than
//! a second implementation (decision 6, amended 2026-08-02 -- scan
//! resistance matters more on disk than in RAM, since a disk miss costs an
//! S3 fetch and disk is the tier that holds the working set), and built so
//! every failure degrades to a miss rather than an error (see the
//! [`disk`] module docs for the crash-safety mechanism and what crc32c and
//! the header actually prove on a hit, per decision 4 as amended
//! 2026-08-02). A disk entry also carries a stamped write time and a
//! configured per-entry max-age (default 23 h,
//! [`DEFAULT_MAX_ENTRY_AGE_NS`]): an entry older than the max-age is treated
//! as a miss and dropped, so raw bytes of a subject erased by ADR-0064's
//! sweep persist on a node's disposable local disk at most the max-age plus
//! one sweep interval past the sweep -- the defaults are tuned to keep that
//! sum within ADR-0064's 24 h bound. Time is injected through
//! [`Clock`] so this ageing is deterministic under test.
//!
//! [`tiered::TieredCache`] composes the two tiers behind one handle the three
//! ADR-0046 read funnels can hold (decision 3). It is read-through: a RAM miss
//! consults the disk tier before the caller's upstream fetch is allowed to
//! run, and a successful upstream fetch populates *both* tiers, not only RAM,
//! so a later RAM eviction is served from disk instead of paying an S3 round
//! trip again. Single-flight spans both tiers: concurrent callers for one key
//! collapse to a single disk consult and, on a disk miss, a single upstream
//! fetch. When its RAM tier is in [`Cache::with_corruption`] mode, a
//! disk-served hit is corrupted by the identical transform a RAM hit uses, so
//! ADR-0046 decision 4's acceptance gate reaches the disk tier the moment the
//! funnels start using this handle. A caller that cannot express its miss
//! handling as one upstream-fetch closure instead has [`tiered::TieredCache::get`]
//! and [`tiered::TieredCache::insert`]: the same read-through and dual-tier
//! admission, with no fetch and no single-flight participation.

mod cache;
mod clock;
pub mod disk;
mod key;
mod limits;
mod metrics;
mod s3fifo;
mod single_flight;
pub mod tiered;

pub use cache::Cache;
pub use clock::{Clock, SystemClock};
pub use disk::DiskCache;
pub use key::CacheKey;
pub use limits::{CacheLimits, DEFAULT_MAX_ENTRY_AGE_NS, DEFAULT_SWEEP_INTERVAL_NS};
pub use metrics::{CacheMetrics, CacheMetricsSnapshot};
pub use single_flight::{Role, SingleFlight, SingleFlightError};
pub use tiered::{Source, TieredCache};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::timeout;

    use super::*;

    fn test_key(n: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([7u8; 16], content_hash, 0, 0)
    }

    #[tokio::test]
    async fn single_flight_collapses_concurrent_misses_and_propagates_errors() {
        let flight: Arc<SingleFlight<CacheKey, Bytes, &'static str>> =
            Arc::new(SingleFlight::new());
        let key = test_key(1);

        // N concurrent misses on one key produce exactly one upstream
        // call, and every waiter receives the same bytes.
        let upstream_calls = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let flight = flight.clone();
            let upstream_calls = upstream_calls.clone();
            handles.push(tokio::spawn(async move {
                flight
                    .run(key, move || {
                        let upstream_calls = upstream_calls.clone();
                        async move {
                            upstream_calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok::<Bytes, &'static str>(Bytes::from_static(b"payload"))
                        }
                    })
                    .await
            }));
        }
        for handle in handles {
            let (result, _role) = handle.await.unwrap();
            assert_eq!(result.unwrap().as_ref(), b"payload".as_slice());
        }
        assert_eq!(
            upstream_calls.load(Ordering::SeqCst),
            1,
            "N concurrent misses on one key must produce exactly one upstream call"
        );

        // A failing upstream call reaches every waiter as an error,
        // rather than hanging any of them.
        let key2 = test_key(2);
        let upstream_calls2 = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let flight = flight.clone();
            let upstream_calls2 = upstream_calls2.clone();
            handles.push(tokio::spawn(async move {
                flight
                    .run(key2, move || {
                        let upstream_calls2 = upstream_calls2.clone();
                        async move {
                            upstream_calls2.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Err::<Bytes, &'static str>("upstream exploded")
                        }
                    })
                    .await
            }));
        }
        for handle in handles {
            let (result, _role) = handle.await.unwrap();
            match result {
                Err(SingleFlightError::Upstream(message)) => {
                    assert_eq!(message, "upstream exploded")
                }
                other => panic!("expected every waiter to see the upstream error, got {other:?}"),
            }
        }
        assert_eq!(upstream_calls2.load(Ordering::SeqCst), 1);

        // A second, later miss on the same key after the first completed
        // does not reuse the completed slot: it starts a fresh leader.
        let third_call_ran = Arc::new(AtomicUsize::new(0));
        let third_call_ran_clone = third_call_ran.clone();
        let (third_result, third_role) = flight
            .run(key2, move || async move {
                third_call_ran_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"fresh"))
            })
            .await;
        assert_eq!(
            third_role,
            Role::Leader,
            "a miss after completion must start a fresh leader"
        );
        assert_eq!(third_result.unwrap().as_ref(), b"fresh".as_slice());
        assert_eq!(third_call_ran.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn single_flight_panicking_or_cancelled_leader_unblocks_waiters() {
        let flight: Arc<SingleFlight<CacheKey, Bytes, &'static str>> =
            Arc::new(SingleFlight::new());

        // A leader that panics before producing a result must not leave a
        // concurrent follower waiting forever.
        let key = test_key(100);
        let leader = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run(key, || async {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        panic!("leader blew up before producing a result");
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(2)).await;
        let follower_ran = Arc::new(AtomicUsize::new(0));
        let follower = {
            let flight = flight.clone();
            let follower_ran = follower_ran.clone();
            tokio::spawn(async move {
                flight
                    .run(key, move || {
                        let follower_ran = follower_ran.clone();
                        async move {
                            follower_ran.fetch_add(1, Ordering::SeqCst);
                            Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
                        }
                    })
                    .await
            })
        };

        assert!(
            leader.await.is_err(),
            "the leader task itself should have panicked"
        );
        let (follower_result, follower_role) = timeout(Duration::from_secs(2), follower)
            .await
            .expect("a panicking leader must not leave a follower parked forever")
            .expect("follower task must not itself panic");
        assert!(matches!(
            follower_result,
            Err(SingleFlightError::LeaderLost)
        ));
        assert_eq!(follower_role, Role::Follower);
        assert_eq!(
            follower_ran.load(Ordering::SeqCst),
            0,
            "a follower never runs its own fetch"
        );

        // A leader whose task is cancelled before it produces a result
        // must not leave a concurrent follower waiting forever either.
        let key2 = test_key(101);
        let leader2 = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run(key2, || async {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"unreachable"))
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        let follower2 = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run(key2, || async {
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        leader2.abort();

        let (follower2_result, _role) = timeout(Duration::from_secs(2), follower2)
            .await
            .expect("a cancelled leader must not leave a follower parked forever")
            .expect("follower task must not itself panic");
        assert!(matches!(
            follower2_result,
            Err(SingleFlightError::LeaderLost)
        ));
    }

    #[test]
    fn s3_fifo_scan_resistance_keeps_working_set_resident() {
        let entry_size = 1024u64;
        let working_set_len = 20u64;
        let limits = CacheLimits::new(entry_size * 100, 1000, entry_size * 10);
        let cache: Cache<&'static str> = Cache::new(limits);

        let working_set: Vec<CacheKey> = (0..working_set_len).map(test_key).collect();
        for key in &working_set {
            cache.insert(*key, Bytes::from(vec![0xAAu8; entry_size as usize]));
        }
        // Access each working-set entry a second time so S3-FIFO promotes
        // it out of the small probation queue into main.
        for key in &working_set {
            assert!(cache.get(key).is_some());
        }

        // A long one-shot scan: far more distinct keys than the cache
        // holds, each admitted and never touched again -- exactly the
        // compactor/folder access pattern ADR-0046 decision 6 is written
        // against.
        for i in 1_000..1_500u64 {
            cache.insert(test_key(i), Bytes::from(vec![0xEEu8; entry_size as usize]));
        }

        let resident = working_set
            .iter()
            .filter(|key| cache.get(key).is_some())
            .count();
        assert!(
            resident as f64 >= working_set_len as f64 * 0.9,
            "expected the working set to stay resident under a one-shot scan, only {resident}/{working_set_len} survived (a plain-LRU cache would evict nearly all of it)"
        );
    }

    /// Scan resistance must hold across repeated scans, not just the first.
    ///
    /// ADR-0046 decision 6 exists because the compactor and the folder scan
    /// cold data *continuously*. An earlier version sized the ghost queue from
    /// `max_entries` rather than from resident capacity, which made the ghost
    /// many times wider than the cache: every key of a finished scan was still
    /// remembered when the next began, so each was re-admitted straight to
    /// main, bypassing probation, and the second pass evicted everything the
    /// first pass protected. That version passes the single-pass test above
    /// and fails this one at pass 1.
    #[test]
    fn s3_fifo_scan_resistance_survives_repeated_scans() {
        let entry_size = 1024u64;
        let working_set_len = 20u64;
        let limits = CacheLimits::new(entry_size * 100, 1000, entry_size * 10);
        let cache: Cache<&'static str> = Cache::new(limits);

        let working_set: Vec<CacheKey> = (0..working_set_len).map(test_key).collect();
        for key in &working_set {
            cache.insert(*key, Bytes::from(vec![0xAAu8; entry_size as usize]));
        }
        for key in &working_set {
            assert!(cache.get(key).is_some());
        }

        for pass in 0..3u64 {
            let base = 1_000 + pass * 500;
            for i in base..base + 500 {
                cache.insert(test_key(i), Bytes::from(vec![0xEEu8; entry_size as usize]));
            }
            let resident = working_set
                .iter()
                .filter(|key| cache.get(key).is_some())
                .count();
            assert!(
                resident as f64 >= working_set_len as f64 * 0.9,
                "working set collapsed on scan pass {pass}: only {resident}/{working_set_len} resident"
            );
        }
    }

    #[test]
    fn bounds_respected_under_insertion_pressure_and_oversized_entry_not_admitted() {
        let limits = CacheLimits::new(10 * 1024, 5, 4096);
        let cache: Cache<&'static str> = Cache::new(limits);

        for i in 0..50u64 {
            cache.insert(test_key(i), Bytes::from(vec![0u8; 512]));
        }
        assert!(
            cache.len() <= 5,
            "entry-count bound violated: {} entries resident",
            cache.len()
        );
        assert!(
            cache.total_bytes() <= 10 * 1024,
            "byte bound violated: {} bytes resident",
            cache.total_bytes()
        );

        let oversized_key = test_key(9_999);
        cache.insert(oversized_key, Bytes::from(vec![0u8; 8192]));
        assert!(
            cache.get(&oversized_key).is_none(),
            "an oversized entry must not be admitted"
        );
        assert_eq!(cache.metrics().snapshot().admissions_rejected_size, 1);
    }

    #[test]
    fn corruption_mode_returns_bytes_that_differ_on_every_hit() {
        let limits = CacheLimits::new(1024 * 1024, 100, 1024 * 1024);
        let cache: Cache<&'static str> = Cache::with_corruption(limits);
        let key = test_key(1);
        let original = Bytes::from_static(b"trust no cached byte");
        cache.insert(key, original.clone());

        for _ in 0..5 {
            let hit = cache.get(&key).expect("entry should be resident");
            assert_ne!(hit.as_ref(), original.as_ref());
            assert_eq!(hit.len(), original.len());
        }
    }

    #[tokio::test]
    async fn counters_match_hand_computed_sequence() {
        let limits = CacheLimits::new(1024 * 1024, 1, 1024 * 1024);
        let cache: Cache<&'static str> = Cache::new(limits);
        let a = test_key(1);
        let b = test_key(2);

        assert!(cache.get(&a).is_none()); // miss 1
        cache.insert(a, Bytes::from_static(b"aaa"));
        assert!(cache.get(&a).is_some()); // hit 1

        // max_entries == 1, so admitting `b` must make room. The `get` above
        // bumped `a`'s freq to 1, so `a` is promoted out of the probation
        // queue into main and then evicted from main in the same pass; it does
        // not survive either way. (An earlier comment here claimed `a` was
        // evicted from probation with freq == 0, which is not what happens.)
        cache.insert(b, Bytes::from_static(b"bb"));

        assert!(cache.get(&a).is_none()); // miss 2 (evicted)
        assert!(cache.get(&b).is_some()); // hit 2

        let snap = cache.metrics().snapshot();
        assert_eq!(snap.hits, 2);
        assert_eq!(snap.misses, 2);
        assert_eq!(snap.evictions, 1);
        assert_eq!(snap.bytes_admitted, 5); // "aaa" (3) + "bb" (2)
        assert_eq!(snap.bytes_served, 5); // "aaa" (3) + "bb" (2)
        assert_eq!(snap.admissions_rejected_size, 0);
        assert_eq!(snap.single_flight_collapses, 0);

        // Single-flight collapse: two concurrent get_or_fetch calls for a
        // new key collapse into one upstream call and one collapse.
        let limits2 = CacheLimits::new(1024 * 1024, 10, 1024 * 1024);
        let cache = Arc::new(Cache::<&'static str>::new(limits2));
        let c = test_key(3);
        let (first, second) = tokio::join!(
            {
                let cache = cache.clone();
                async move {
                    cache
                        .get_or_fetch(c, || async {
                            tokio::time::sleep(Duration::from_millis(15)).await;
                            Ok::<Bytes, &'static str>(Bytes::from_static(b"c"))
                        })
                        .await
                }
            },
            {
                let cache = cache.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    cache
                        .get_or_fetch(c, || async {
                            Ok::<Bytes, &'static str>(Bytes::from_static(b"c"))
                        })
                        .await
                }
            }
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(cache.metrics().snapshot().single_flight_collapses, 1);
    }
}

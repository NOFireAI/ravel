//! The tiered read-through handle of ADR-0046's read cache
//! (docs/adrs/0046-read-cache-tier.md, decision 3's two-tier composition,
//! decision 4's acceptance gate, and decision 5's single-flight). It holds a
//! RAM [`Cache`] over a local-disk [`DiskCache`] and presents one interface
//! the three ADR-0046 read funnels can hold, so no funnel has to know there
//! are two tiers or in what order they are consulted.
//!
//! **Read-through, and admission populates both tiers.** A lookup checks RAM
//! first. On a RAM miss it consults the disk tier before signalling an
//! upstream miss to the caller, and a disk hit repopulates RAM so the next
//! read of that key is a RAM hit rather than a second disk consult. The
//! caller still owns the upstream fetch -- it is passed in as a closure, the
//! same division of responsibility [`Cache::get_or_fetch`] already uses,
//! because only the funnel holds the `SegmentRef` a range came from and must
//! be the one that never admits a payload under a key that does not describe
//! it (decision 4's amendment). A successful upstream fetch populates *both*
//! tiers (decision 3), not RAM alone, so a later RAM eviction is served from
//! disk instead of re-paying the S3 round trip this epic exists to remove.
//!
//! **Single-flight spans both tiers** (decision 5). A RAM hit is the fast
//! path and needs no coordination. Every RAM miss for one key collapses onto
//! a single leader that consults disk once and, only on a disk miss, runs the
//! caller's upstream fetch once; concurrent callers for that key are followers
//! that neither consult disk nor fetch. Two callers therefore never both read
//! disk, and never both fall through to a separate upstream fetch.
//!
//! **The corruption gate reaches the disk tier.** When the RAM tier is built
//! with [`Cache::with_corruption`] (ADR-0046 decision 4's acceptance-gate
//! mode), a disk-served hit is corrupted by the identical byte transform a
//! RAM hit uses (see [`crate::cache::corrupt_bytes`]), applied at serve time
//! only -- the bytes admitted to either tier stay clean, so corruption is a
//! read-time view, never stored. Without this, the epic's "correctness never
//! depends on cached state" gate would silently stop covering the disk tier
//! the moment the funnels started serving hits from it. Clean bytes freshly
//! returned by an upstream fetch are never corrupted: they did not come from a
//! cache tier, exactly as a store read is never corrupted.

use std::sync::Arc;

use bytes::Bytes;

use crate::cache::Cache;
use crate::disk::DiskCache;
use crate::key::CacheKey;
use crate::metrics::CacheMetrics;
use crate::single_flight::{Role, SingleFlight, SingleFlightError};

/// Where the bytes a [`TieredCache::get_or_fetch`] call returned came from,
/// so the caller can account for it (ADR-0044): a cache hit crossed no
/// network, an upstream fetch did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A tier already populated before this call served the bytes: the RAM
    /// tier (the fast path), or the disk tier consulted on a RAM miss. No
    /// upstream round trip happened on this call's behalf. In corruption mode
    /// these bytes are corrupted.
    Cache,
    /// The single-flight leader ran the caller's upstream `fetch` to satisfy a
    /// miss in both tiers; this call was that leader, or a follower that rode
    /// its one fetch. A store round trip happened, so the bytes are the fresh
    /// upstream bytes and are never corrupted.
    Upstream,
}

/// A RAM [`Cache`] over a local-disk [`DiskCache`], composed as one
/// read-through handle (ADR-0046 decision 3). `E` is the caller's upstream
/// fetch error type, threaded through the single-flight exactly as
/// [`Cache`] threads it, so this crate still defines no error type of its own
/// and has no opinion on what a miss's upstream call looks like.
///
/// See the [module docs](self) for the read-through, dual-tier admission,
/// cross-tier single-flight, and corruption-gate behavior.
pub struct TieredCache<E> {
    ram: Cache<E>,
    disk: DiskCache,
    /// Coalesces concurrent RAM misses on one key across *both* tiers. Its
    /// value is `(clean_bytes, from_cache)`: `from_cache` records whether the
    /// leader satisfied the miss from the disk tier (`true`, corruptible) or
    /// from the upstream fetch (`false`, never corrupted). Bytes on the wire
    /// are always clean; corruption is applied per-caller at serve time.
    single_flight: SingleFlight<CacheKey, (Bytes, bool), E>,
}

impl<E> TieredCache<E>
where
    E: Clone + Send + Sync + 'static,
{
    /// Composes an existing RAM `ram` tier over a disk `disk` tier. The
    /// corruption mode is inherited from `ram`: build `ram` with
    /// [`Cache::with_corruption`] to put the whole handle -- disk-served hits
    /// included -- into ADR-0046 decision 4's acceptance-gate mode.
    pub fn new(ram: Cache<E>, disk: DiskCache) -> Self {
        TieredCache {
            ram,
            disk,
            single_flight: SingleFlight::new(),
        }
    }

    /// A cloneable handle to the RAM tier's counters, independent of this
    /// handle's lifetime. Single-flight collapses (across both tiers) are
    /// recorded here.
    pub fn ram_metrics(&self) -> Arc<CacheMetrics> {
        self.ram.metrics()
    }

    /// A cloneable handle to the disk tier's counters, independent of this
    /// handle's lifetime.
    pub fn disk_metrics(&self) -> Arc<CacheMetrics> {
        self.disk.metrics()
    }

    /// Read `key` through both tiers, fetching upstream only if both miss.
    ///
    /// Returns the served bytes and the [`Source`] they came from. Order:
    ///
    /// 1. **RAM hit** -- returned immediately, [`Source::Cache`], without
    ///    touching disk or the single-flight. [`Cache::get`] applies
    ///    corruption and records the hit, so a RAM hit here is byte-for-byte
    ///    what the RAM tier alone would return.
    /// 2. **RAM miss** -- the call joins the single-flight for `key`. The
    ///    leader consults the disk tier; a disk hit repopulates RAM
    ///    (read-through) and yields [`Source::Cache`]. On a disk miss the
    ///    leader runs `fetch`, admits the result to *both* tiers, and yields
    ///    [`Source::Upstream`]. Followers ride the leader's single disk
    ///    consult / upstream fetch.
    ///
    /// In corruption mode a [`Source::Cache`] result (RAM or disk) is
    /// corrupted; a [`Source::Upstream`] result is the clean fetched bytes.
    pub async fn get_or_fetch<F, Fut>(
        &self,
        key: CacheKey,
        fetch: F,
    ) -> Result<(Bytes, Source), SingleFlightError<E>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<Bytes, E>> + Send,
    {
        // Fast path: a RAM hit needs no cross-tier coordination and never
        // touches disk. `Cache::get` records the hit and, in corruption mode,
        // corrupts the served bytes.
        if let Some(bytes) = self.ram.get(&key) {
            return Ok((bytes, Source::Cache));
        }

        // RAM miss: collapse concurrent callers for this key onto one leader
        // that consults disk and, only on a disk miss, runs the upstream
        // fetch. Followers neither consult disk nor fetch (decision 5,
        // extended across both tiers).
        let (outcome, role) = self
            .single_flight
            .run(key, move || async move {
                if let Some(bytes) = self.disk.get(&key) {
                    // Read-through: repopulate RAM so the next read is a RAM
                    // hit, not another disk consult. Clean bytes are admitted;
                    // corruption, if on, is a serve-time transform below.
                    self.ram.insert(key, bytes.clone());
                    return Ok((bytes, true));
                }
                // Both tiers missed: the caller owns the upstream fetch. On
                // success admit to BOTH tiers (decision 3), not RAM alone, so
                // a later RAM eviction is served from disk. `DiskCache::insert`
                // silently declines bytes whose length disagrees with
                // `key.len`, so a well-formed funnel key admits cleanly.
                let bytes = fetch().await?;
                self.ram.insert(key, bytes.clone());
                self.disk.insert(key, &bytes);
                Ok((bytes, false))
            })
            .await;

        let (bytes, from_cache) = match outcome {
            Ok(value) => value,
            Err(err) => return Err(err),
        };
        if role == Role::Follower {
            // A follower rode the leader's single disk consult / upstream
            // fetch: count the collapse on the same counter the RAM tier uses,
            // feeding ADR-0046's single-flight-collapse SLI.
            self.ram.metrics().record_collapse();
        }
        let source = if from_cache {
            Source::Cache
        } else {
            Source::Upstream
        };
        Ok((self.maybe_corrupt(bytes, from_cache), source))
    }

    /// Corrupts `bytes` iff they came from a cache tier and the RAM tier is in
    /// corruption mode, using the exact transform a RAM hit uses so the gate
    /// covers disk-served hits identically. Upstream (`from_cache == false`)
    /// bytes are the fresh fetch and are never corrupted.
    fn maybe_corrupt(&self, bytes: Bytes, from_cache: bool) -> Bytes {
        if from_cache && self.ram.corrupts_hits() {
            crate::cache::corrupt_bytes(&bytes)
        } else {
            bytes
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod fixtures {
    use super::*;

    pub(super) fn test_key(n: u64, len: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([7u8; 16], content_hash, 0, len)
    }

    pub(super) fn generous_limits() -> crate::CacheLimits {
        crate::CacheLimits::new(64 * 1024 * 1024, 10_000, 16 * 1024 * 1024)
    }
}

/// The named acceptance test for issue #1135, at the exact path the task
/// specifies (`ravel_cache::tiered::corrupted_disk_hit_is_corrupted_through_ram_readthrough`):
/// a key resident only on disk, RAM empty, corruption mode on. The bytes it
/// serves must arrive corrupted -- not the clean disk bytes -- proving
/// ADR-0046 decision 4's gate reaches a disk-served hit read through this
/// handle and not just a RAM hit. Placed at module scope (not under a `tests`
/// child) so its path is exactly the one the epic's gate cites.
#[cfg(test)]
#[tokio::test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn corrupted_disk_hit_is_corrupted_through_ram_readthrough() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tempfile::TempDir;

    use fixtures::{generous_limits, test_key};

    let tmp = TempDir::new().unwrap();
    let clean = Bytes::from_static(b"disk-resident payload; trust no cached byte");
    let key = test_key(1, clean.len() as u64);

    // Disk tier populated, RAM tier empty: exactly the state a read-through
    // must fall through RAM into disk to serve.
    let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
    disk.insert(key, &clean);
    assert!(
        disk.get(&key).is_some(),
        "precondition: entry lives on disk"
    );

    let ram: Cache<&'static str> = Cache::with_corruption(generous_limits());
    let tiered = TieredCache::new(ram, disk);

    // The upstream fetch must never run: the bytes are on disk already.
    let upstream_calls = Arc::new(AtomicUsize::new(0));
    let calls = upstream_calls.clone();
    let (served, source) = tiered
        .get_or_fetch(key, move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"UPSTREAM MUST NOT RUN"))
            }
        })
        .await
        .expect("a disk-resident key resolves without error");

    assert_eq!(
        source,
        Source::Cache,
        "a disk hit is a cache hit, not an upstream fetch"
    );
    assert_eq!(
        upstream_calls.load(Ordering::SeqCst),
        0,
        "a disk hit must not trigger an upstream fetch"
    );
    assert_eq!(served.len(), clean.len(), "corruption preserves length");
    assert_ne!(
        served.as_ref(),
        clean.as_ref(),
        "a disk-served hit in corruption mode must arrive corrupted, proving \
         ADR-0046 decision 4's gate reaches the disk tier"
    );

    // The corruption a RAM hit would apply to the same bytes, computed by an
    // independent RAM tier in the same mode: proves the disk-served hit is
    // corrupted by the identical transform, not merely "some" mutation.
    let ram_probe: Cache<&'static str> = Cache::with_corruption(generous_limits());
    ram_probe.insert(key, clean.clone());
    let ram_corrupted = ram_probe.get(&key).expect("probe RAM hit");
    assert_eq!(
        served, ram_corrupted,
        "a disk-served hit must be corrupted exactly like a RAM hit"
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::fixtures::{generous_limits, test_key};
    use super::*;
    use crate::CacheLimits;

    /// A RAM hit is the fast path: it returns the RAM bytes and never consults
    /// the disk tier at all. Proven via the disk tier's own counters -- a
    /// consulted disk records a hit or a miss, and both must stay unchanged.
    #[tokio::test]
    async fn ram_hit_short_circuits_without_touching_disk() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let disk_metrics = disk.metrics();
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let clean = Bytes::from_static(b"resident in RAM");
        let key = test_key(1, clean.len() as u64);
        // Prime the RAM tier directly (a module-child test may reach the
        // private field), leaving disk untouched.
        tiered.ram.insert(key, clean.clone());

        let before = disk_metrics.snapshot();
        let upstream_calls = Arc::new(AtomicUsize::new(0));
        let calls = upstream_calls.clone();
        let (served, source) = tiered
            .get_or_fetch(key, move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<Bytes, &'static str>(Bytes::from_static(b"upstream"))
                }
            })
            .await
            .unwrap();

        assert_eq!(source, Source::Cache);
        assert_eq!(served, clean, "the RAM bytes are served verbatim");
        assert_eq!(
            upstream_calls.load(Ordering::SeqCst),
            0,
            "no upstream on a RAM hit"
        );
        let after = disk_metrics.snapshot();
        assert_eq!(after.hits, before.hits, "a RAM hit reads no disk hit");
        assert_eq!(
            after.misses, before.misses,
            "a RAM hit does not consult the disk tier at all"
        );
    }

    /// Concurrent RAM+disk misses on one key collapse to a single upstream
    /// fetch (ADR-0046 decision 5, spanning both tiers): every waiter gets the
    /// same bytes, and the fetch closure runs exactly once.
    #[tokio::test]
    async fn single_flight_collapses_concurrent_misses_to_one_upstream() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = Arc::new(TieredCache::new(ram, disk));

        let payload = Bytes::from_static(b"fetched once");
        let key = test_key(1, payload.len() as u64);
        let upstream_calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let tiered = tiered.clone();
            let calls = upstream_calls.clone();
            let payload = payload.clone();
            handles.push(tokio::spawn(async move {
                tiered
                    .get_or_fetch(key, move || {
                        let calls = calls.clone();
                        let payload = payload.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            // Hold the slot so the other callers arrive as
                            // followers rather than each starting a fresh
                            // leader after the first completes.
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok::<Bytes, &'static str>(payload)
                        }
                    })
                    .await
            }));
        }
        for handle in handles {
            let (bytes, _source) = handle.await.unwrap().unwrap();
            assert_eq!(
                bytes, payload,
                "every waiter receives the one fetched value"
            );
        }
        assert_eq!(
            upstream_calls.load(Ordering::SeqCst),
            1,
            "8 concurrent misses on one key must produce exactly one upstream fetch"
        );
        assert_eq!(
            tiered.ram_metrics().snapshot().single_flight_collapses,
            7,
            "the 7 followers each record one collapse"
        );
    }

    /// A successful upstream fetch populates BOTH tiers (ADR-0046 decision 3),
    /// not RAM alone. Proven by evicting the RAM entry and reading again: the
    /// key is served from disk with no second upstream fetch, the exact cold
    /// -path round trip this epic removes.
    #[tokio::test]
    async fn upstream_fetch_populates_both_tiers_and_disk_serves_after_ram_eviction() {
        let tmp = TempDir::new().unwrap();
        // A one-slot RAM tier so a second distinct key deterministically
        // evicts the first; the disk tier stays generous and keeps both.
        let ram: Cache<&'static str> = Cache::new(CacheLimits::new(64 * 1024, 1, 64 * 1024));
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let a = test_key(1, 4);
        let b = test_key(2, 4);
        let a_bytes = Bytes::from_static(b"aaaa");
        let b_bytes = Bytes::from_static(b"bbbb");
        let calls = Arc::new(AtomicUsize::new(0));

        // First read of A: both tiers miss, the upstream fetch runs once, and
        // both tiers are populated.
        let calls_a = calls.clone();
        let bytes_a = a_bytes.clone();
        let (got, source) = tiered
            .get_or_fetch(a, move || {
                let calls_a = calls_a.clone();
                async move {
                    calls_a.fetch_add(1, Ordering::SeqCst);
                    Ok::<Bytes, &'static str>(bytes_a)
                }
            })
            .await
            .unwrap();
        assert_eq!(source, Source::Upstream);
        assert_eq!(got, a_bytes);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            tiered.disk.get(&a).is_some(),
            "the upstream fetch must have populated the disk tier, not RAM alone"
        );

        // Read B: the one-slot RAM tier evicts A while B is fetched upstream.
        let calls_b = calls.clone();
        let bytes_b = b_bytes.clone();
        let _ = tiered
            .get_or_fetch(b, move || {
                let calls_b = calls_b.clone();
                async move {
                    calls_b.fetch_add(1, Ordering::SeqCst);
                    Ok::<Bytes, &'static str>(bytes_b)
                }
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            tiered.ram.get(&a).is_none(),
            "A must have been evicted from the one-slot RAM tier"
        );

        // Read A again: RAM misses, disk hits, so A is served from disk with
        // no upstream re-fetch.
        let before_disk = tiered.disk.metrics().snapshot();
        let calls_a2 = calls.clone();
        let (got2, source2) = tiered
            .get_or_fetch(a, move || {
                let calls_a2 = calls_a2.clone();
                async move {
                    calls_a2.fetch_add(1, Ordering::SeqCst);
                    Ok::<Bytes, &'static str>(Bytes::from_static(b"XXXX"))
                }
            })
            .await
            .unwrap();
        assert_eq!(
            source2,
            Source::Cache,
            "a RAM-evicted key is served from the disk tier, not re-fetched"
        );
        assert_eq!(
            got2, a_bytes,
            "the disk tier serves the originally-cached bytes"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "no upstream re-fetch: the disk tier absorbed the RAM eviction"
        );
        let after_disk = tiered.disk.metrics().snapshot();
        assert_eq!(
            after_disk.hits,
            before_disk.hits + 1,
            "the second read of A was a disk hit"
        );
    }
}

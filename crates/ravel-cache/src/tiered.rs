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
//! disk instead of re-paying the S3 round trip the disk tier exists to remove.
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
//! read-time view, never stored. Without this, ADR-0046's "correctness never
//! depends on cached state" gate would silently stop covering the disk tier
//! the moment the funnels started serving hits from it. Clean bytes freshly
//! returned by an upstream fetch are never corrupted: they did not come from a
//! cache tier, exactly as a store read is never corrupted.
//!
//! **A second, fetch-free access path exists for a caller that cannot
//! express its miss handling as one upstream-fetch closure.**
//! [`TieredCache::get`] and [`TieredCache::insert`] give the same RAM-then-
//! disk read-through and dual-tier admission as `get_or_fetch`, but neither
//! runs a fetch or joins single-flight -- `get` returns `None` on a genuine
//! both-tier miss rather than fetching, and `insert` admits bytes the caller
//! already holds. A caller mixing this path with `get_or_fetch` on the same
//! key must still account for exactly one miss per logical request; see
//! [`TieredCache::get`]'s own docstring for the double-counting pitfall this
//! crate has already shipped and fixed once on the closure-based path.
//!
//! **A third path resolves a peek-then-defer miss under single-flight.**
//! [`TieredCache::resolve_peeked_miss`] is for a caller that already peeked
//! both tiers with `get`, saw a confirmed miss, and now wants to resolve it --
//! but coalesced, so concurrent callers for one key collapse onto a single
//! upstream fetch. Unlike `get`/`insert` it joins the single-flight (the same
//! field `get_or_fetch` uses), and unlike `get_or_fetch` it consults neither
//! tier before fetching and records no miss of its own, because the caller's
//! `get` already accounted the one miss. `BlockRangeFetcher`'s per-extent
//! peek-then-defer uses it so N cross-partition callers striping one segment
//! issue one GET, not N.

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
    ///
    /// **Error-path accounting (issue #656).** When `fetch` fails this method
    /// records neither a hit nor a miss: it threads the error straight out
    /// (the follower's single-flight collapse is still counted, because a
    /// collapse happened regardless of the leader's outcome). Hit/miss
    /// accounting is the caller's, keyed off the returned [`Source`] on the
    /// success path -- this handle has no `QueryAccounting` and takes no
    /// opinion on whether a faulted upstream GET should count as a miss. A
    /// caller that records a miss on the success `Source::Upstream` branch must
    /// therefore decide explicitly whether the *error* branch records one too;
    /// a caller whose success and error paths disagree (one counts the miss,
    /// the other returns before any accounting) makes a faulted GET count
    /// differently than a served miss, the exact divergence #656 tracks.
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

    /// Resolve a miss the caller ALREADY confirmed with [`get`](Self::get):
    /// run `fetch` once behind single-flight and admit its result to **both**
    /// tiers, **without** re-consulting either tier and **without** recording a
    /// miss of this method's own.
    ///
    /// This is the coalesced companion to [`get`](Self::get)'s peek-then-defer
    /// discipline (ADR-0046 decision 5), and it exists apart from
    /// [`get_or_fetch`](Self::get_or_fetch) for one reason: the caller has
    /// already peeked both tiers with `get` and seen a confirmed both-tier miss,
    /// so `get_or_fetch`'s internal tier consultation would be redundant work
    /// here and, worse, would count a SECOND miss on a key `get` already
    /// accounted for -- exactly the double-count [`get`](Self::get)'s docstring
    /// warns a peek-then-defer caller against. `BlockRangeFetcher` is that
    /// caller: it peeks each candidate block with `get`, defers the miss, and
    /// then resolves the deferred run through this method so N concurrent
    /// cross-partition callers striping one RSEG/RLOG extent collapse onto one
    /// upstream fetch instead of each issuing its own.
    ///
    /// It joins the **same** [`single_flight`](Self::single_flight) field
    /// `get_or_fetch` uses, not a second coordinator, so a concurrent
    /// `get_or_fetch` and a `resolve_peeked_miss` on the same key still coalesce
    /// onto each other correctly. The stored single-flight value is
    /// `(clean_bytes, false)`: this method's own leader never serves from a tier
    /// (`false` = "not from cache", so the bytes are the fresh upstream fetch and
    /// are never corrupted). A follower may instead ride a concurrent
    /// `get_or_fetch` leader that served from disk (`true`); that flag is honored
    /// on the way out so a disk-served ride is corruption-gated identically to
    /// `get_or_fetch`.
    ///
    /// Accounting discipline (the invariant this method depends on): it records
    /// **no miss** on either tier -- the caller's earlier `get` already recorded
    /// the one accounted miss for this logical request, and layering a second
    /// here would corrupt the request-hit-rate SLI, the bug this crate shipped
    /// and fixed once already (see [`get`](Self::get)'s docstring). It **does**
    /// record a single-flight collapse when a caller rides another caller's
    /// leader (`self.ram.metrics().record_collapse()`, the same counter
    /// `get_or_fetch`'s follower branch feeds), since that is real coalescing
    /// worth surfacing on the SLI. On a `fetch` error it records neither a hit
    /// nor a miss and threads the error out unchanged, exactly as
    /// [`get_or_fetch`](Self::get_or_fetch) does: the caller's earlier `get`
    /// already accounted the one miss for this logical request, so the error
    /// path adds nothing here regardless (issue #656).
    ///
    /// On a fetch success the leader admits to **both** tiers (decision 3) via
    /// the same dual-tier admission [`insert`](Self::insert) uses, so a later RAM
    /// eviction is served from disk rather than re-paying the S3 round trip.
    pub async fn resolve_peeked_miss<F, Fut>(
        &self,
        key: CacheKey,
        fetch: F,
    ) -> Result<Bytes, SingleFlightError<E>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<Bytes, E>> + Send,
    {
        // No tier consultation: the caller peeked both tiers with `get` and got
        // a confirmed miss, so re-reading here would be redundant and could
        // count a second, unaccounted miss. The leader runs the upstream fetch
        // once and, on success, admits to BOTH tiers (decision 3). `false`
        // records that these bytes are the fresh upstream fetch, never a cache
        // serve, so they are never corrupted.
        let (outcome, role) = self
            .single_flight
            .run(key, move || async move {
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
            // A follower rode another caller's leader (this method's or a
            // concurrent `get_or_fetch`'s -- both share `single_flight`): count
            // the collapse on the same SLI `get_or_fetch` feeds. This is the
            // ONLY `metrics()` call this method makes; it records no miss of its
            // own, because the caller's earlier `get` already recorded the one
            // accounted miss for this logical request.
            self.ram.metrics().record_collapse();
        }
        // `from_cache` is `false` for this method's own leader (it consults no
        // tier), but a follower may have ridden a concurrent `get_or_fetch`
        // leader that served from disk (`true`); honor it so a disk-served ride
        // is corruption-gated exactly as `get_or_fetch`'s is.
        Ok(self.maybe_corrupt(bytes, from_cache))
    }

    /// Read `key` through both tiers with **no** upstream fetch and **no**
    /// single-flight: a plain hit-or-miss peek the caller owns entirely.
    ///
    /// RAM-first, falling through to disk on a RAM miss, in the exact
    /// tier-consulting order [`TieredCache::get_or_fetch`] uses: the RAM tier's
    /// [`Cache::get`] first and, only on its `None`, the disk tier's
    /// [`DiskCache::get`]. A disk hit repopulates RAM read-through, identical to
    /// `get_or_fetch`'s disk-hit branch, so the next read of `key` is a RAM hit
    /// and this method leaves both tiers in the same state that path would.
    ///
    /// This is the behavioral difference from [`TieredCache::get_or_fetch`] a
    /// future reader must not miss: `get_or_fetch` collapses a both-tier miss
    /// onto a single-flight leader that runs the caller's upstream fetch, while
    /// `get` runs no fetch and joins no single-flight. A `None` return is a
    /// genuine miss in both tiers, and the caller owns all of its miss handling
    /// (`BlockRangeFetcher` admits already-verified bytes via
    /// [`TieredCache::insert`] with no fetch at all, and peeks a key with this
    /// method to defer a miss to a later coalesced GET rather than fetch here).
    /// Concurrent `get` calls for one key each consult disk independently
    /// rather than riding one leader: single-flight exists only to protect the
    /// upstream fetch this method never performs.
    ///
    /// **A `None` from this method still records a real miss on both tiers'
    /// [`CacheMetrics`]** ([`Cache::get`] and [`DiskCache::get`] each count
    /// their own call). A caller using the peek-then-defer pattern above must
    /// not also call `get_or_fetch` (or `get` again) for the same logical
    /// miss expecting only one miss to be counted: that double-counts one
    /// request as two misses, corrupting the request-hit-rate SLI ADR-0046
    /// depends on -- this crate has shipped and fixed exactly this bug once
    /// already (see [`TieredCache::get_or_fetch`]'s own docstring). A caller
    /// that peeks with `get` and, on a miss, later resolves the value some
    /// other way (a coalesced fetch elsewhere, as `BlockRangeFetcher` does)
    /// should treat this method's miss as the query's ONLY accounted miss for
    /// that key, not layer a second accounted miss on top when the deferred
    /// fetch later runs.
    ///
    /// In corruption mode (`ram` built with [`Cache::with_corruption`]) a hit
    /// from either tier is corrupted at serve time exactly as a
    /// [`Source::Cache`] `get_or_fetch` result is: a RAM hit through
    /// [`Cache::get`]'s own transform, and a disk hit through the identical
    /// [`maybe_corrupt`](Self::maybe_corrupt) call `get_or_fetch` applies. The
    /// bytes admitted to either tier stay clean; corruption is a read-time
    /// view. Without this, ADR-0046 decision 4's "correctness never depends on
    /// cached state" gate would stop covering a disk-served hit read through
    /// this new access path.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        // Fast path: a RAM hit is served verbatim (already corrupted, in
        // corruption mode, by `Cache::get`) and never consults disk, exactly
        // like `get_or_fetch`'s RAM fast path.
        if let Some(bytes) = self.ram.get(key) {
            return Some(bytes);
        }
        // RAM miss: consult disk. A disk hit repopulates RAM read-through with
        // the clean bytes, then is corrupted per-caller at serve time -- the
        // same order `get_or_fetch`'s disk-hit branch uses, so the gate reaches
        // a disk-served hit here identically.
        let bytes = self.disk.get(key)?;
        self.ram.insert(*key, bytes.clone());
        Some(self.maybe_corrupt(bytes, true))
    }

    /// Admit `value` under `key` into **both** tiers with no upstream fetch,
    /// matching `get_or_fetch`'s dual-tier admission policy on a fetch success
    /// (ADR-0046 decision 3): the RAM tier via [`Cache::insert`] (which takes
    /// [`Bytes`]) and the disk tier via [`DiskCache::insert`] (which takes a
    /// borrowed slice). This is the plain-admission half a caller uses to cache
    /// bytes it already fetched and verified elsewhere (`BlockRangeFetcher`),
    /// so a later RAM eviction is served from disk rather than re-paying the S3
    /// round trip the disk tier exists to remove. Like [`TieredCache::get`], it
    /// touches no single-flight. `DiskCache::insert` silently declines bytes
    /// whose length disagrees with `key.len`, so a well-formed funnel key
    /// admits to both tiers cleanly.
    pub fn insert(&self, key: CacheKey, value: Bytes) {
        self.ram.insert(key, value.clone());
        self.disk.insert(key, &value);
    }

    /// Whether the **RAM tier** holds no resident entry.
    ///
    /// "Empty" is defined as the RAM tier alone, not both tiers, because every
    /// pre-existing caller of an `is_empty` on this cache (`cache_warm.rs`)
    /// asks one question: did cache warming populate the in-RAM working set?
    /// A warm goes through [`TieredCache::get_or_fetch`], which admits to both
    /// tiers, so before any warm both tiers are empty and after one both are
    /// populated -- the RAM-tier answer and the both-tier answer agree in that
    /// caller's case. They diverge only when RAM is empty over a non-empty disk
    /// tier (a fresh process reopening a populated cache directory, or a RAM
    /// eviction), and there the caller's actual concern -- whether *this
    /// process's* RAM warming did anything -- is answered by the RAM tier, not
    /// by disk residue a prior process left. See [`Cache::is_empty`] and
    /// [`DiskCache::is_empty`] for a single-tier check when a caller needs one.
    pub fn is_empty(&self) -> bool {
        self.ram.is_empty()
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

/// The named acceptance test, at the exact required path
/// (`ravel_cache::tiered::corrupted_disk_hit_is_corrupted_through_ram_readthrough`):
/// a key resident only on disk, RAM empty, corruption mode on. The bytes it
/// serves must arrive corrupted -- not the clean disk bytes -- proving
/// ADR-0046 decision 4's gate reaches a disk-served hit read through this
/// handle and not just a RAM hit. Placed at module scope (not under a `tests`
/// child) so its path is exactly the one the acceptance gate cites.
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
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use bytes::Bytes;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

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
    ///
    /// The leader is held in its fetch by a `oneshot` this test releases, and
    /// each follower's future is polled once before that release. That first
    /// poll is where `SingleFlight::run` takes the in-flight map lock, so
    /// "the followers joined while the leader was still in flight" is an
    /// ordering this test enforces rather than a wall-clock race it can lose.
    #[tokio::test]
    async fn single_flight_collapses_concurrent_misses_to_one_upstream() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = Arc::new(TieredCache::new(ram, disk));

        let payload = Bytes::from_static(b"fetched once");
        let key = test_key(1, payload.len() as u64);
        let upstream_calls = Arc::new(AtomicUsize::new(0));
        let follower_fetches = Arc::new(AtomicUsize::new(0));

        const CALLERS: usize = 8;
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let leader = {
            let tiered = tiered.clone();
            let calls = upstream_calls.clone();
            let payload = payload.clone();
            tokio::spawn(async move {
                tiered
                    .get_or_fetch(key, move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<Bytes, &'static str>(payload)
                    })
                    .await
            })
        };
        entered_rx.await.expect("the leader reaches its fetch");

        let mut followers = Vec::new();
        for _ in 1..CALLERS {
            let ran = follower_fetches.clone();
            followers.push(Box::pin(tiered.get_or_fetch(key, move || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
            })));
        }
        for follower in &mut followers {
            let first = std::future::poll_fn(|cx| Poll::Ready(follower.as_mut().poll(cx))).await;
            assert!(
                first.is_pending(),
                "a follower parks on the held leader instead of completing on its own"
            );
        }
        release_tx.send(()).expect("the leader is still parked");

        for follower in followers {
            let (bytes, _source) = follower.await.unwrap();
            assert_eq!(
                bytes, payload,
                "every waiter receives the one fetched value"
            );
        }
        let (bytes, _source) = leader.await.unwrap().unwrap();
        assert_eq!(bytes, payload, "the leader receives its own fetched value");

        assert_eq!(
            upstream_calls.load(Ordering::SeqCst),
            1,
            "8 concurrent misses on one key must produce exactly one upstream fetch"
        );
        assert_eq!(
            follower_fetches.load(Ordering::SeqCst),
            0,
            "a follower never runs its own fetch"
        );
        assert_eq!(
            tiered.ram_metrics().snapshot().single_flight_collapses,
            (CALLERS - 1) as u64,
            "the 7 followers each record one collapse"
        );
    }

    /// Eight callers that each already peeked-and-missed the same key (via
    /// `get`, the one accounted miss per caller) then resolve that miss through
    /// [`TieredCache::resolve_peeked_miss`]: the upstream `fetch` runs exactly
    /// once (they collapse onto one leader), every caller receives identical
    /// bytes, the seven followers each record one collapse, and -- critically --
    /// the method records NO miss of its own on either tier, so the peek stays
    /// the single accounted miss. A successful resolve admits to both tiers.
    ///
    /// This is the crate-level proof of #662's contract. To watch single-flight
    /// bite, revert `resolve_peeked_miss` to run `fetch().await` directly
    /// without the `self.single_flight.run(...)` wrapper (the pre-fix
    /// `ReadCache::fetch_peeked` Tiered behavior): `upstream_calls` then reads 8,
    /// not 1, and the exactly-one-fetch assertion fails.
    ///
    /// The leader is held in its fetch by a `oneshot` this test releases, and
    /// each follower's future is polled once (the poll that takes the
    /// in-flight map lock) before that release, so no wall-clock hold decides
    /// whether a caller lands as a follower or as a second leader.
    #[tokio::test]
    async fn resolve_peeked_miss_collapses_concurrent_callers_to_one_fetch() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let ram_metrics = ram.metrics();
        let tiered = Arc::new(TieredCache::new(ram, disk));

        let payload = Bytes::from_static(b"resolved once");
        let key = test_key(1, payload.len() as u64);

        // Every caller peeks first and genuinely misses both tiers -- exactly
        // the BlockRangeFetcher peek-then-defer entry condition. These `get`
        // calls are the one accounted miss per caller.
        const CALLERS: usize = 8;
        for _ in 0..CALLERS {
            assert!(
                tiered.get(&key).is_none(),
                "precondition: a caller peeks and misses both tiers"
            );
        }
        let misses_after_peeks = ram_metrics.snapshot().misses;

        let upstream_calls = Arc::new(AtomicUsize::new(0));
        let follower_fetches = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let leader = {
            let tiered = tiered.clone();
            let calls = upstream_calls.clone();
            let payload = payload.clone();
            tokio::spawn(async move {
                tiered
                    .resolve_peeked_miss(key, move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<Bytes, &'static str>(payload)
                    })
                    .await
            })
        };
        entered_rx.await.expect("the leader reaches its fetch");

        let mut followers = Vec::new();
        for _ in 1..CALLERS {
            let ran = follower_fetches.clone();
            followers.push(Box::pin(tiered.resolve_peeked_miss(
                key,
                move || async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
                },
            )));
        }
        for follower in &mut followers {
            let first = std::future::poll_fn(|cx| Poll::Ready(follower.as_mut().poll(cx))).await;
            assert!(
                first.is_pending(),
                "a follower parks on the held leader instead of completing on its own"
            );
        }
        release_tx.send(()).expect("the leader is still parked");

        for follower in followers {
            let bytes = follower.await.unwrap();
            assert_eq!(
                bytes, payload,
                "every caller receives the one fetched value"
            );
        }
        let bytes = leader.await.unwrap().unwrap();
        assert_eq!(bytes, payload, "the leader receives its own fetched value");

        assert_eq!(
            upstream_calls.load(Ordering::SeqCst),
            1,
            "8 concurrent resolve_peeked_miss callers on one key must produce exactly one fetch"
        );
        assert_eq!(
            follower_fetches.load(Ordering::SeqCst),
            0,
            "a follower never runs its own fetch"
        );
        assert_eq!(
            ram_metrics.snapshot().single_flight_collapses,
            (CALLERS - 1) as u64,
            "the 7 followers each record one collapse"
        );
        assert_eq!(
            ram_metrics.snapshot().misses,
            misses_after_peeks,
            "resolve_peeked_miss records NO miss of its own: the peek was the one accounted miss"
        );
        assert!(
            tiered.disk.get(&key).is_some(),
            "a successful resolve admits to the disk tier, not RAM alone"
        );
    }

    /// A successful upstream fetch populates BOTH tiers (ADR-0046 decision 3),
    /// not RAM alone. Proven by evicting the RAM entry and reading again: the
    /// key is served from disk with no second upstream fetch, the exact cold
    /// -path round trip the disk tier removes.
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

    /// The plain `get` returns `None` on a genuine miss in BOTH tiers, and
    /// runs no upstream fetch (it takes no fetch closure: a miss is the
    /// caller's to handle). The disk tier is consulted, proven by its miss
    /// counter, so the fall-through order matches `get_or_fetch`.
    #[tokio::test]
    async fn plain_get_double_miss_returns_none_without_fetching() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let disk_metrics = disk.metrics();
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let key = test_key(1, 5);
        let before = disk_metrics.snapshot();
        assert!(
            tiered.get(&key).is_none(),
            "a key resident in neither tier is a plain miss"
        );
        let after = disk_metrics.snapshot();
        assert_eq!(
            after.misses,
            before.misses + 1,
            "a RAM miss must fall through and consult the disk tier"
        );
        assert_eq!(after.hits, before.hits, "a double miss records no disk hit");
    }

    /// The plain `get` fast path: a RAM hit is served without consulting the
    /// disk tier at all, mirroring `ram_hit_short_circuits_without_touching_disk`
    /// but for the fetch-free `get`. Proven via the disk tier's own counters.
    #[tokio::test]
    async fn plain_get_ram_hit_short_circuits_without_touching_disk() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let disk_metrics = disk.metrics();
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let clean = Bytes::from_static(b"resident in RAM");
        let key = test_key(1, clean.len() as u64);
        tiered.ram.insert(key, clean.clone());

        let before = disk_metrics.snapshot();
        let served = tiered.get(&key);
        assert_eq!(
            served.as_deref(),
            Some(clean.as_ref()),
            "the RAM bytes are served verbatim"
        );
        let after = disk_metrics.snapshot();
        assert_eq!(after.hits, before.hits, "a RAM hit reads no disk hit");
        assert_eq!(
            after.misses, before.misses,
            "a RAM hit does not consult the disk tier at all"
        );
    }

    /// The plain `get` on a disk-only key repopulates RAM read-through, exactly
    /// as `get_or_fetch`'s disk-hit branch does: the next read of the key is a
    /// RAM hit. Mirrors the `get_or_fetch` disk-repopulation proof.
    #[tokio::test]
    async fn plain_get_disk_only_key_repopulates_ram() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let clean = Bytes::from_static(b"disk resident");
        let key = test_key(1, clean.len() as u64);
        // Populate disk only; RAM stays empty.
        tiered.disk.insert(key, &clean);
        assert!(
            tiered.ram.get(&key).is_none(),
            "precondition: the key is not in RAM"
        );

        let served = tiered.get(&key);
        assert_eq!(
            served.as_deref(),
            Some(clean.as_ref()),
            "a disk-only key is served from disk"
        );
        // Read-through: the disk hit repopulated RAM, so a direct RAM read now
        // hits without any further disk consult.
        assert_eq!(
            tiered.ram.get(&key).as_deref(),
            Some(clean.as_ref()),
            "a disk hit must repopulate RAM so the next read is a RAM hit"
        );
    }

    /// The plain `insert` admits to BOTH tiers (ADR-0046 decision 3's admission
    /// policy, no upstream fetch involved). Proven by reading the disk tier
    /// directly after the insert: the bytes are present there, not RAM alone.
    #[tokio::test]
    async fn plain_insert_populates_both_tiers() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let value = Bytes::from_static(b"admitted to both");
        let key = test_key(1, value.len() as u64);
        tiered.insert(key, value.clone());

        assert_eq!(
            tiered.ram.get(&key).as_deref(),
            Some(value.as_ref()),
            "insert must admit to the RAM tier"
        );
        // Read the disk tier directly (RAM bypassed): the bytes must be there
        // too, so a later RAM eviction is served from disk.
        assert_eq!(
            tiered.disk.get(&key).as_deref(),
            Some(value.as_ref()),
            "insert must admit to the disk tier, not RAM alone"
        );
    }

    /// #656: the disk tier's own metrics accessor surfaces a disk-served hit.
    /// A key resident only on disk is read through the tiered handle; the hit
    /// registers on [`TieredCache::disk_metrics`], while the RAM-tier view
    /// ([`TieredCache::ram_metrics`]) records the same read as a *miss* -- which
    /// is exactly why a RAM-only accessor under-reports disk-served hits and the
    /// disk accessor is needed. This is the crate-level primitive behind
    /// `ravel_catalog::Catalog::byte_cache_disk_metrics`.
    ///
    /// To watch it bite, read only `ram_metrics()`: `disk_after.hits` would be
    /// invisible there and the disk-served hit would read as a pure miss.
    #[tokio::test]
    async fn disk_served_hit_is_visible_on_disk_metrics() {
        let tmp = TempDir::new().unwrap();
        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        let ram: Cache<&'static str> = Cache::new(generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let clean = Bytes::from_static(b"disk resident");
        let key = test_key(1, clean.len() as u64);
        // Populate disk only; RAM stays empty so the read falls through to disk.
        tiered.disk.insert(key, &clean);

        let disk_before = tiered.disk_metrics().snapshot();
        let ram_before = tiered.ram_metrics().snapshot();

        let served = tiered.get(&key).expect("a disk-resident key resolves");
        assert_eq!(served, clean);

        let disk_after = tiered.disk_metrics().snapshot();
        let ram_after = tiered.ram_metrics().snapshot();
        assert_eq!(
            disk_after.hits,
            disk_before.hits + 1,
            "a disk-served hit must read as a hit on the disk-tier metrics"
        );
        assert_eq!(
            ram_after.misses,
            ram_before.misses + 1,
            "the same read is a miss on the RAM-tier metrics: a RAM-only \
             accessor would report the disk-served hit as a miss (#656)"
        );
    }

    /// The corruption gate (ADR-0046 decision 4) reaches a disk-served hit read
    /// through the fetch-free `get`, identically to the `get_or_fetch` proof at
    /// module scope. A key resident only on disk, RAM in corruption mode: the
    /// plain `get` must return corrupted bytes, not the clean disk bytes.
    ///
    /// To watch it bite, in `TieredCache::get`'s disk-hit branch replace
    /// `Some(self.maybe_corrupt(bytes, true))` with `Some(bytes)`: the
    /// `assert_ne!` below then fails because the clean disk bytes are served
    /// through a path the acceptance gate must corrupt.
    #[tokio::test]
    async fn plain_get_disk_hit_is_corrupted_in_corruption_mode() {
        let tmp = TempDir::new().unwrap();
        let clean = Bytes::from_static(b"disk-resident payload; trust no cached byte");
        let key = test_key(1, clean.len() as u64);

        let disk = DiskCache::new(tmp.path().to_path_buf(), generous_limits());
        disk.insert(key, &clean);
        assert!(
            disk.get(&key).is_some(),
            "precondition: entry lives on disk"
        );

        let ram: Cache<&'static str> = Cache::with_corruption(generous_limits());
        let tiered = TieredCache::new(ram, disk);

        let served = tiered.get(&key).expect("a disk-resident key resolves");
        assert_eq!(served.len(), clean.len(), "corruption preserves length");
        assert_ne!(
            served.as_ref(),
            clean.as_ref(),
            "a disk-served hit through the plain get must arrive corrupted, \
             proving ADR-0046 decision 4's gate reaches this access path"
        );

        // The corruption an independent RAM tier in the same mode applies to
        // the same bytes: proves the identical transform, not merely "some"
        // mutation.
        let ram_probe: Cache<&'static str> = Cache::with_corruption(generous_limits());
        ram_probe.insert(key, clean.clone());
        let ram_corrupted = ram_probe.get(&key).expect("probe RAM hit");
        assert_eq!(
            served, ram_corrupted,
            "a disk-served hit must be corrupted exactly like a RAM hit"
        );
    }
}

//! Worker membership and rendezvous-hash work partitioning (ADR-0065
//! decisions 1 and 2, epic EI).
//!
//! # Membership via self-owned heartbeat keys (decision 1)
//!
//! Each maintain-role process writes, every heartbeat interval `H`
//! (default 60s), a [`WorkerHeartbeat`] to a key it alone ever writes:
//!
//! ```text
//! sys/maintain/workers/<process_id>
//! ```
//!
//! `process_id` is a UUID generated once at construction (the ADR-0057
//! convention every other self-owned-snapshot mechanism in this codebase uses).
//! The write is [`PutMode::Overwrite`] -- single writer per key, no CAS, no
//! contention. On the same cadence each process lists `sys/maintain/workers/`
//! and GETs siblings; the **live set** is itself plus every sibling whose
//! `heartbeat_unix_ns` is within `liveness_factor * H` (default `3 * H`) of the
//! reader's own clock. A stale worker is treated as gone -- its work is taken
//! over, and if it was merely slow the overlap is idempotent (the same
//! fail-open direction ADR-0057 chose, so a slow heartbeat anywhere never
//! freezes maintenance everywhere).
//!
//! # Work partitioning via rendezvous hash (decision 2)
//!
//! The unit of ownership is `(tenant_hash, signal, shard)` -- the exact unit
//! `run_tick` iterates. For each unit every worker independently computes
//!
//! ```text
//! owner(unit) = argmax over w in live_set of blake3(unit_key || w.as_bytes())
//! ```
//!
//! and the supervisor's discovery cycle simply skips units it does not own.
//! Rendezvous (highest-random-weight) hashing needs no coordination state, no
//! assignment object, and no leader; every worker with the same live set
//! computes the same owner, and a membership change moves only the departed or
//! arrived worker's units. With one replica the live set is `{self}` and
//! behavior is byte-for-byte the pre-ADR-0065 unconditional walk.
//!
//! This is deliberately **not** called a lease: the existing
//! [`crate::sweep::LeaseCheck`] trait is the GC reader-protection gate, a
//! different concept, and the two must not blur (ADR-0065 decision 1). The
//! vocabulary here is `WorkerSet`, `live_set`, `owner`, `owns`.

use std::time::Duration;

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use ravel_proto::sys::v1::WorkerHeartbeat;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use futures::stream::{self, StreamExt};

/// Default heartbeat interval `H` (ADR-0065 decision 1): 60 seconds.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Default liveness factor: a sibling whose heartbeat is older than
/// `LIVENESS_FACTOR * H` at read time is treated as gone (ADR-0065 decision 1,
/// "within `3 * H`"). One factor wider than ADR-0057's `2 * R` to absorb skew
/// plus write jitter; a config knob, not a contract.
pub const DEFAULT_LIVENESS_FACTOR: u32 = 3;

/// Default bounded intra-process unit concurrency (ADR-0065 decision 2's
/// stuck-owner mitigation): at most this many owned units are maintained at
/// once, so one pathological unit cannot starve the rest of a process's
/// ownership.
pub const DEFAULT_UNIT_CONCURRENCY: usize = 4;

/// Format floor for [`WorkerHeartbeat`] (matches the guard every other `sys/`
/// message uses). A sibling heartbeat advertising a higher floor is one this
/// reader does not understand, so it is skipped (treated as absent) rather than
/// misread.
const HEARTBEAT_FORMAT_VERSION: u32 = 1;

/// The prefix every maintain process's heartbeat key lives under (ADR-0065
/// decision 1): a new additive control-plane prefix under `sys/maintain/`.
const WORKERS_PREFIX: &str = "sys/maintain/workers/";

/// The heartbeat key one maintain process owns and alone ever writes.
pub fn heartbeat_key(process_id: &Uuid) -> String {
    format!("{WORKERS_PREFIX}{process_id}")
}

/// Extract the `<process_id>` from a heartbeat key
/// (`sys/maintain/workers/<uuid>`). Returns `None` for a key that does not
/// parse as a UUID (a defensive guard; every key this module writes matches).
fn process_id_of(key: &str) -> Option<Uuid> {
    let raw = key.strip_prefix(WORKERS_PREFIX)?;
    Uuid::parse_str(raw).ok()
}

/// Stable, collision-free byte encoding of one `(tenant_hash, signal, shard)`
/// unit of ownership (ADR-0065 decision 2). Fixed-width fields: the 16-byte
/// tenant hash, then the signal's one-byte key-prefix discriminator (part of
/// the frozen object-key layout), then the shard as 4 big-endian bytes. Two
/// different triples always differ in at least one field, so their encodings
/// never collide.
pub fn unit_key(tenant_hash: &TenantHash, signal: Signal, shard: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + 1 + 4);
    buf.extend_from_slice(&tenant_hash.0);
    buf.extend_from_slice(signal.key_prefix().as_bytes());
    buf.extend_from_slice(&shard.to_be_bytes());
    buf
}

/// The rendezvous weight `blake3(unit_key || process_id_bytes)`.
fn weight(unit_key: &[u8], process_id: &Uuid) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(unit_key);
    hasher.update(process_id.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Rendezvous (highest-random-weight) owner of `unit_key` among `live_set`
/// (ADR-0065 decision 2): `argmax over w of blake3(unit_key || w.as_bytes())`.
///
/// Returns `None` only for an empty `live_set` -- which never occurs in
/// practice, since a process's own live set always includes itself; callers
/// that only need the boolean use [`owns`]. Ties (astronomically improbable at
/// 256 bits) break toward the larger `process_id`, so the choice is fully
/// deterministic given the same inputs and carries no hidden randomness.
pub fn owner(unit_key: &[u8], live_set: &[Uuid]) -> Option<Uuid> {
    live_set
        .iter()
        .copied()
        .map(|w| (weight(unit_key, &w), w))
        .max_by(|(wa, a), (wb, b)| wa.cmp(wb).then_with(|| a.as_bytes().cmp(b.as_bytes())))
        .map(|(_, w)| w)
}

/// Whether `process_id` is the rendezvous owner of `unit_key` under `live_set`
/// (a convenience over [`owner`] for the common gate-this-unit check).
pub fn owns(unit_key: &[u8], process_id: Uuid, live_set: &[Uuid]) -> bool {
    owner(unit_key, live_set) == Some(process_id)
}

/// Whether a sibling heartbeat is stale at read time: older than the liveness
/// window. Exactly the window old is still live (inclusive on the fresh side);
/// a future-dated heartbeat (negative age from clock skew) is treated as live,
/// the same fail-open direction ADR-0057 uses.
fn is_stale(now_ns: i64, heartbeat_unix_ns: i64, liveness_window_ns: i64) -> bool {
    now_ns.saturating_sub(heartbeat_unix_ns) > liveness_window_ns
}

/// Run `f` over `items` with at most `concurrency` futures in flight,
/// preserving input order in the returned results (ADR-0065 decision 2's
/// bounded intra-process unit concurrency, mirroring ravel-catalog's
/// `fold_bucket_concurrency` buffered fan-out from epic EH). `concurrency` is
/// clamped to at least 1, so a cap of 0 degrades to a sequential walk rather
/// than deadlocking.
pub async fn run_bounded<I, T, Fut, F>(concurrency: usize, items: I, f: F) -> Vec<T>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    stream::iter(items)
        .map(f)
        .buffered(concurrency.max(1))
        .collect()
        .await
}

/// One maintain-role process's membership handle (ADR-0065 decision 1): a
/// stable `process_id`, the heartbeat/liveness timing, and the bounded
/// unit-concurrency cap. Held once for the whole process and shared (via `Arc`)
/// by the maintenance supervisor and the scrub loop, so the process presents a
/// single worker identity to the fleet rather than one per background loop.
#[derive(Debug)]
pub struct WorkerSet {
    process_id: Uuid,
    started_unix_ns: i64,
    heartbeat_interval: Duration,
    liveness_factor: u32,
    unit_concurrency: usize,
}

impl WorkerSet {
    /// A worker with an explicit timing and concurrency configuration.
    /// `liveness_factor` and `unit_concurrency` are clamped to at least 1.
    pub fn new(
        started_unix_ns: i64,
        heartbeat_interval: Duration,
        liveness_factor: u32,
        unit_concurrency: usize,
    ) -> Self {
        WorkerSet {
            process_id: Uuid::new_v4(),
            started_unix_ns,
            heartbeat_interval,
            liveness_factor: liveness_factor.max(1),
            unit_concurrency: unit_concurrency.max(1),
        }
    }

    /// A worker with the ADR-0065 defaults (`H = 60s`, `3 * H` liveness window,
    /// 4 units in flight), started at `started_unix_ns`.
    pub fn with_defaults(started_unix_ns: i64) -> Self {
        Self::new(
            started_unix_ns,
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_LIVENESS_FACTOR,
            DEFAULT_UNIT_CONCURRENCY,
        )
    }

    /// This process's stable membership id (ADR-0057 section 1). Names the one
    /// heartbeat key this process owns and is the identity the rendezvous hash
    /// resolves ownership against.
    pub fn process_id(&self) -> Uuid {
        self.process_id
    }

    /// The heartbeat interval `H` this worker writes on.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// The bounded intra-process unit-concurrency cap (ADR-0065 decision 2).
    pub fn unit_concurrency(&self) -> usize {
        self.unit_concurrency
    }

    /// The staleness window in nanoseconds: `liveness_factor * H`.
    fn liveness_window_ns(&self) -> i64 {
        let h = i64::try_from(self.heartbeat_interval.as_nanos()).unwrap_or(i64::MAX);
        h.saturating_mul(i64::from(self.liveness_factor))
    }

    /// Write this process's heartbeat (`Overwrite`: single writer per key, no
    /// CAS). A failed write self-corrects on the next interval; the caller logs
    /// it. `now_ns` is the injected clock reading stamped as `heartbeat_unix_ns`.
    pub async fn write_heartbeat(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<(), StoreError> {
        let heartbeat = WorkerHeartbeat {
            format_version: HEARTBEAT_FORMAT_VERSION,
            process_id: self.process_id.as_bytes().to_vec(),
            started_unix_ns: self.started_unix_ns,
            heartbeat_unix_ns: now_ns,
        };
        let key = heartbeat_key(&self.process_id);
        store
            .put(
                &key,
                heartbeat.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await?;
        Ok(())
    }

    /// Compute the live worker set (ADR-0065 decision 1): this process plus
    /// every non-stale sibling under `sys/maintain/workers/`. Self is always
    /// included, even if its own heartbeat object is missing or stale, so a
    /// process never disowns its own units. The returned set is sorted by
    /// `process_id` and deduplicated, for a deterministic order (rendezvous
    /// ownership is order-independent; sorting only stabilizes tests and logs).
    ///
    /// A corrupt or future-version sibling is skipped (treated as absent,
    /// self-correcting next interval); only a failed LIST or GET is an `Err`,
    /// which the caller treats fail-open (keep the last-known live set or fall
    /// back to `{self}`), never freezing maintenance on a transient read fault.
    pub async fn live_set(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<Vec<Uuid>, StoreError> {
        let window = self.liveness_window_ns();
        let objects = list_all(store, WORKERS_PREFIX).await?;
        let mut live = vec![self.process_id];
        for meta in objects {
            let Some(pid) = process_id_of(&meta.key) else {
                continue;
            };
            if pid == self.process_id {
                continue;
            }
            let got = store.get(&meta.key, GetRange::Full).await?;
            let Ok(heartbeat) = WorkerHeartbeat::decode(got.data.as_ref()) else {
                tracing::debug!(
                    key = %meta.key,
                    "worker_set: skipping an undecodable sibling heartbeat"
                );
                continue;
            };
            if heartbeat.format_version > HEARTBEAT_FORMAT_VERSION {
                tracing::debug!(
                    key = %meta.key,
                    version = heartbeat.format_version,
                    "worker_set: skipping a future-version sibling heartbeat"
                );
                continue;
            }
            if is_stale(now_ns, heartbeat.heartbeat_unix_ns, window) {
                continue;
            }
            live.push(pid);
        }
        live.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        live.dedup();
        Ok(live)
    }

    /// Whether this process owns `(tenant, signal, shard)` under `live_set`
    /// (ADR-0065 decision 2).
    pub fn owns_unit(
        &self,
        live_set: &[Uuid],
        tenant_hash: &TenantHash,
        signal: Signal,
        shard: u32,
    ) -> bool {
        owns(&unit_key(tenant_hash, signal, shard), self.process_id, live_set)
    }

    /// The single-replica live set (`{self}`): every unit owned by self, so
    /// maintenance behaves byte-for-byte as the pre-ADR-0065 unconditional walk.
    /// Used as the fail-open fallback when a live-set read fails and no prior
    /// set exists.
    pub fn solo_live_set(&self) -> Vec<Uuid> {
        vec![self.process_id]
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    use super::*;

    const H: Duration = Duration::from_secs(60);
    const H_NS: i64 = 60 * 1_000_000_000;

    fn worker(now_ns: i64) -> WorkerSet {
        WorkerSet::new(now_ns, H, DEFAULT_LIVENESS_FACTOR, DEFAULT_UNIT_CONCURRENCY)
    }

    /// The rendezvous hash is deterministic: same unit key and live set yields
    /// the same owner every call, with no hidden randomness.
    #[test]
    fn owner_is_deterministic() {
        let live: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let key = unit_key(&TenantId::new("acme").hash(), Signal::Metrics, 3);
        let first = owner(&key, &live).expect("non-empty live set has an owner");
        for _ in 0..16 {
            assert_eq!(owner(&key, &live), Some(first), "owner must not vary");
        }
    }

    /// Ownership is independent of the order the live set is presented in: the
    /// argmax is over a set, so a permuted live set resolves the same owner.
    #[test]
    fn owner_is_order_independent() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let key = unit_key(&TenantId::new("acme").hash(), Signal::Logs, 1);
        let forward = owner(&key, &[a, b, c]);
        let reversed = owner(&key, &[c, b, a]);
        assert_eq!(forward, reversed);
    }

    /// A single-replica live set (`{self}`) owns every unit: `owns` is true for
    /// every triple, so maintenance runs unconditionally as it did pre-ADR-0065.
    #[test]
    fn solo_live_set_owns_every_unit() {
        let w = worker(0);
        let live = w.solo_live_set();
        let tenant = TenantId::new("acme").hash();
        for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
            for shard in 0..8u32 {
                assert!(w.owns_unit(&live, &tenant, signal, shard));
            }
        }
    }

    /// Two workers over one live set partition the unit space disjointly and
    /// completely: every unit resolves to exactly one of the two, and (for a
    /// non-trivial unit count) both get a non-empty share.
    #[test]
    fn two_workers_partition_units_disjointly_and_completely() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let live = {
            let mut v = vec![a, b];
            v.sort_unstable_by(|x, y| x.as_bytes().cmp(y.as_bytes()));
            v
        };
        let tenant = TenantId::new("acme").hash();
        let mut owned_by_a = 0;
        let mut owned_by_b = 0;
        for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
            for shard in 0..16u32 {
                let key = unit_key(&tenant, signal, shard);
                let o = owner(&key, &live).expect("owner");
                assert!(o == a || o == b, "owner is one of the live workers");
                if o == a {
                    owned_by_a += 1;
                } else {
                    owned_by_b += 1;
                }
                // Disjoint: exactly one of the two owns it.
                assert_ne!(owns(&key, a, &live), owns(&key, b, &live));
            }
        }
        assert_eq!(owned_by_a + owned_by_b, 3 * 16, "every unit is owned");
        assert!(owned_by_a > 0 && owned_by_b > 0, "both workers get a share");
    }

    /// `unit_key` never collides across distinct triples.
    #[test]
    fn unit_key_is_collision_free() {
        let t1 = TenantId::new("acme").hash();
        let t2 = TenantId::new("globex").hash();
        let mut seen = std::collections::HashSet::new();
        for tenant in [t1, t2] {
            for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
                for shard in 0..4u32 {
                    assert!(
                        seen.insert(unit_key(&tenant, signal, shard)),
                        "distinct triples must produce distinct unit keys"
                    );
                }
            }
        }
    }

    /// Heartbeats round-trip through a store: two workers each write their own
    /// key, and each computes a live set that converges to include both.
    #[tokio::test]
    async fn live_sets_converge_to_include_both_workers() {
        let store = MemoryStore::new();
        let now = 1_000 * H_NS;
        let a = worker(now);
        let b = worker(now);

        a.write_heartbeat(&store, now).await.expect("a heartbeat");
        b.write_heartbeat(&store, now).await.expect("b heartbeat");

        let live_a = a.live_set(&store, now).await.expect("a live set");
        let live_b = b.live_set(&store, now).await.expect("b live set");

        assert!(live_a.contains(&a.process_id()) && live_a.contains(&b.process_id()));
        assert_eq!(live_a, live_b, "both processes see the same sorted live set");
        assert_eq!(live_a.len(), 2);
    }

    /// A sibling whose heartbeat is older than `3 * H` is excluded from the live
    /// set (its units get taken over), while one exactly at the boundary stays.
    #[tokio::test]
    async fn stale_sibling_is_excluded_at_three_h() {
        let store = MemoryStore::new();
        let a = worker(0);
        let b = worker(0);

        // `b` last beat at t=0; `a` reads at various later times.
        b.write_heartbeat(&store, 0).await.expect("b heartbeat");
        a.write_heartbeat(&store, 3 * H_NS).await.expect("a heartbeat");

        // Exactly 3*H old: still live (inclusive on the fresh side).
        let live = a.live_set(&store, 3 * H_NS).await.expect("live set");
        assert!(live.contains(&b.process_id()), "3*H is still live");

        // One nanosecond past 3*H: excluded.
        let live = a.live_set(&store, 3 * H_NS + 1).await.expect("live set");
        assert!(!live.contains(&b.process_id()), "past 3*H is stale");
        assert_eq!(live, vec![a.process_id()], "only self survives");
    }

    /// Losing a live worker moves its owned units to the survivor: units `a`
    /// owned while both were live are taken over by `b` once `a` drops out.
    #[test]
    fn dropped_worker_units_move_to_survivor() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let both = {
            let mut v = vec![a, b];
            v.sort_unstable_by(|x, y| x.as_bytes().cmp(y.as_bytes()));
            v
        };
        let solo_b = vec![b];
        let tenant = TenantId::new("acme").hash();
        // Find a unit `a` owned while both were live.
        let a_owned = (0..64u32)
            .map(|shard| unit_key(&tenant, Signal::Metrics, shard))
            .find(|key| owner(key, &both) == Some(a))
            .expect("a owns at least one unit among 64");
        // With `a` gone, `b` (the only survivor) owns it.
        assert_eq!(owner(&a_owned, &solo_b), Some(b));
    }

    /// `run_bounded` never exceeds the configured cap of concurrent futures in
    /// flight, proven by a counting test double (not just by the final result).
    #[tokio::test]
    async fn run_bounded_respects_the_concurrency_cap() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cap = 3usize;
        let units = 8usize;
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let results = run_bounded(cap, 0..units, |i| {
            let in_flight = in_flight.clone();
            let peak = peak.clone();
            async move {
                let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(cur, Ordering::SeqCst);
                // Yield enough times that every admitted future reaches its peak
                // before any releases, so the observed peak is the true cap.
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
                in_flight.fetch_sub(1, Ordering::SeqCst);
                i
            }
        })
        .await;

        assert_eq!(results, (0..units).collect::<Vec<_>>(), "order preserved");
        assert_eq!(
            peak.load(Ordering::SeqCst),
            cap,
            "at most, and exactly, the cap runs concurrently"
        );
    }
}

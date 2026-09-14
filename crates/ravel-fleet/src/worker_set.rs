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
//! `ravel_maintain::sweep::LeaseCheck` trait is the GC reader-protection gate, a
//! different concept, and the two must not blur (ADR-0065 decision 1). The
//! vocabulary here is `WorkerSet`, `live_set`, `owner`, `owns`.
//!
//! # Bounding the prefix (issue #1679)
//!
//! A heartbeat key is named by a `process_id` generated fresh per process and
//! nothing deleted it, so `sys/maintain/workers/` accumulated one key for every
//! maintain process that ever ran and [`WorkerSet::live_set`] paid a GET for
//! each of them on every heartbeat. Two bounds keep that proportional to the
//! live fleet, matching what `ravel_ingest::reconcile` does for the admission
//! snapshots:
//!
//! - `live_set` skips the GET for a key the LIST result already shows as older
//!   than the liveness window ([`mtime_stale`]). A heartbeat's
//!   `heartbeat_unix_ns` is stamped no later than the write that set the
//!   modification time, so a key that looks past the window by its modification
//!   time can only hold a stamp at least as old.
//! - A key past the *reap horizon* ([`reap_horizon_ns`]: the liveness window
//!   widened by [`REAP_WINDOW_FACTOR`]) is deleted, which bounds the LIST
//!   itself. The extra width is the clock-skew margin between the object
//!   store's clock (which sets the modification time) and the reader's.
//!   [`WorkerSet::live_set_read`] returns those keys from the SAME listing it
//!   makes for the live set, and the maintain tier's heartbeat tick deletes
//!   them through [`WorkerSet::reap_keys`], so the reap costs no listing of its
//!   own. [`WorkerSet::reap_dead_workers`] is the standalone form for a caller
//!   that is not already reading the live set, and it does pay one listing.
//!   Reaping is idempotent and costs a live-but-skewed worker at most one
//!   heartbeat interval of invisibility, since it rewrites its key every `H`.
//!
//! A backend reporting no usable modification time (`<= 0`) gets neither
//! treatment: its keys are read as before and never reaped.

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

/// Whether a sibling heartbeat is stale at read time: further than the
/// liveness window from `now_ns` in EITHER direction. Exactly the window old
/// (or young) is still live (inclusive on the fresh side).
///
/// A far-future-dated heartbeat is excluded, not treated as live. This is the
/// opposite of ADR-0057's admission-control fail-open direction, and
/// deliberately so: there, a future-dated phantom sibling only *adds* to a
/// conservative sum, so treating it as live costs nothing but a slightly
/// wider self-throttle. Here, a live-forever phantom process_id can *win* the
/// rendezvous argmax for a unit and never relinquish it, permanently starving
/// every real worker of that unit's maintenance -- the opposite of ADR-0065's
/// stated direction ("a stale worker is treated as gone... the overlap is
/// idempotent"), which wants MORE claimants on doubt, not fewer. Excluding an
/// implausible heartbeat (whether corrupt, future-version, or impossibly
/// future-dated) is the one direction that is safe under every other
/// exclusion this module already makes.
pub(crate) fn is_stale(now_ns: i64, heartbeat_unix_ns: i64, liveness_window_ns: i64) -> bool {
    let too_old = now_ns.saturating_sub(heartbeat_unix_ns) > liveness_window_ns;
    let too_future = heartbeat_unix_ns.saturating_sub(now_ns) > liveness_window_ns;
    too_old || too_future
}

/// How much wider than the liveness window the reap horizon is (issue #1679): a
/// heartbeat key is deleted only once it is `REAP_WINDOW_FACTOR *
/// liveness_factor * H` old. A factor rather than a second absolute duration,
/// so the configured `H` stays the only knob and the horizon keeps being
/// derived from the window [`is_stale`] already judges against.
pub const REAP_WINDOW_FACTOR: i64 = 2;

/// The reap horizon for a liveness window: the window widened by
/// [`REAP_WINDOW_FACTOR`], which is the clock-skew margin between the object
/// store's clock (which stamps the modification time reaping reads) and this
/// reader's.
pub(crate) fn reap_horizon_ns(window_ns: i64) -> i64 {
    window_ns.saturating_mul(REAP_WINDOW_FACTOR)
}

/// Whether a listed key is already past `window_ns` by the LIST result's
/// modification time alone, so its GET can be skipped (or, at the reap horizon,
/// the key deleted).
///
/// Unlike [`is_stale`] this is one-directional: a modification time in the
/// future means the object store's clock runs ahead of this reader's, which is
/// a reason to read the key normally rather than to skip or delete it. The
/// symmetric exclusion still applies to the `heartbeat_unix_ns` a GET returns,
/// which is what can carry an implausible stamp.
///
/// `last_modified_unix_ms <= 0` means the backend reported no usable
/// modification time (the in-memory oracle's default clock does exactly this).
/// That is "unknown", never "ancient": the key is read normally and never
/// reaped.
pub(crate) fn mtime_stale(now_ns: i64, last_modified_unix_ms: i64, window_ns: i64) -> bool {
    if last_modified_unix_ms <= 0 {
        return false;
    }
    let last_modified_ns = last_modified_unix_ms.saturating_mul(1_000_000);
    now_ns.saturating_sub(last_modified_ns) > window_ns
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

/// What one listing of `sys/maintain/workers/` yields: the live set, and the
/// keys past the reap horizon. Returned together because the maintain tick
/// needs both on the same cadence over the same prefix, so a second listing
/// would be pure cost. Mirrors `read_siblings` on the admission path.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LiveSetRead {
    /// Workers inside the liveness window, including this process.
    pub live: Vec<Uuid>,
    /// Keys past the reap horizon, for [`WorkerSet::reap_keys`] to delete.
    pub reapable: Vec<String>,
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

    /// The same worker under a caller-supplied `process_id` instead of the
    /// freshly generated one.
    ///
    /// Rendezvous ownership is a function of the `process_id`, so a test that
    /// needs a specific owner for a specific unit must be able to pin the id
    /// the same way this crate's clocks are injected: a random draw inside the
    /// constructor is the identity equivalent of `SystemTime::now()` in library
    /// logic, and a test that searches for a peer id which outweighs a random
    /// own id fails whenever that draw lands near the top of the weight range.
    ///
    /// Production callers never use this: they take [`WorkerSet::new`]'s fresh
    /// UUID, so every process keeps a distinct membership identity. Two live
    /// processes sharing one `process_id` would share one heartbeat key and one
    /// rendezvous weight, which is exactly the collision decision 1 rules out.
    pub fn with_process_id(mut self, process_id: Uuid) -> Self {
        self.process_id = process_id;
        self
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
        Ok(self.live_set_read(store, now_ns).await?.live)
    }

    /// The live set AND the keys past the reap horizon, from ONE listing.
    ///
    /// The maintain tick needs both every heartbeat, and the prefix is the
    /// same. Returning them together is what makes the reap free rather than a
    /// second LIST: `read_siblings` on the admission path is the same shape for
    /// the same reason. A caller that wants only one of the two still pays one
    /// listing, never two.
    pub async fn live_set_read(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<LiveSetRead, StoreError> {
        let window = self.liveness_window_ns();
        let horizon = reap_horizon_ns(window);
        let objects = list_all(store, WORKERS_PREFIX).await?;
        let mut live = vec![self.process_id];
        let mut reapable = Vec::new();
        for meta in objects {
            let Some(pid) = process_id_of(&meta.key) else {
                continue;
            };
            if pid == self.process_id {
                continue;
            }
            if mtime_stale(now_ns, meta.last_modified_unix_ms, window) {
                // Already past the liveness window by the LIST's own metadata:
                // the body could only be older still, so it costs no GET
                // (issue #1679). Past the wider reap horizon it is also this
                // listing's reap candidate, collected here so the delete needs
                // no listing of its own.
                if mtime_stale(now_ns, meta.last_modified_unix_ms, horizon) {
                    reapable.push(meta.key.clone());
                }
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
        Ok(LiveSetRead { live, reapable })
    }

    /// Delete the keys a [`live_set_read`] already identified. Issues no
    /// listing of its own, which is the whole point of taking them as an
    /// argument.
    ///
    /// `delete` is idempotent, so several maintain processes reaping the same
    /// dead key concurrently is not a race. A failed individual delete is
    /// logged and retried by whichever process next lists the prefix, since a
    /// key that outlives one tick costs one listing entry and nothing else.
    ///
    /// [`live_set_read`]: Self::live_set_read
    pub async fn reap_keys(&self, store: &dyn ObjectStoreBackend, keys: &[String]) -> u64 {
        let mut reaped = 0;
        for key in keys {
            match store.delete(key).await {
                Ok(()) => reaped += 1,
                Err(err) => tracing::warn!(
                    key = %key,
                    error = %err,
                    "worker_set: reaping a dead worker heartbeat failed; retried next tick"
                ),
            }
        }
        reaped
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
        owns(
            &unit_key(tenant_hash, signal, shard),
            self.process_id,
            live_set,
        )
    }

    /// Delete every heartbeat key under `sys/maintain/workers/` that is past
    /// the reap horizon (issue #1679), judged from the modification time the
    /// LIST result already carries, and return how many were deleted.
    ///
    /// This is the standalone form and it costs one listing. The maintain tick
    /// does NOT use it: that tick already calls [`live_set_read`], which returns
    /// the same candidates from the listing it was making anyway, so it reaps
    /// through [`reap_keys`] and pays nothing extra. Use this where no live-set
    /// read is happening.
    ///
    /// [`live_set_read`]: Self::live_set_read
    /// [`reap_keys`]: Self::reap_keys
    ///
    /// Never deletes this process's own key, never deletes a key that does not
    /// parse as `sys/maintain/workers/<uuid>` (something else's key under a
    /// prefix this process does not own is not this function's to reap), and
    /// never deletes a key the backend reports no modification time for. The
    /// horizon is [`reap_horizon_ns`] of the liveness window, so a worker whose
    /// key merely aged out of the live set keeps a full extra window of grace
    /// against clock skew before it is reaped.
    ///
    /// `delete` is idempotent, so several maintain processes reaping the same
    /// dead key concurrently is not a race. A failed LIST is an `Err` the
    /// caller can treat fail-open; a failed individual delete is logged and
    /// retried by whichever process next lists the prefix, since a key that
    /// outlives one tick costs one listing entry and nothing else.
    pub async fn reap_dead_workers(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<u64, StoreError> {
        let read = self.live_set_read(store, now_ns).await?;
        Ok(self.reap_keys(store, &read.reapable).await)
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
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    use super::*;

    const H: Duration = Duration::from_secs(60);
    const H_NS: i64 = 60 * 1_000_000_000;
    /// `3 * H` in milliseconds: the default liveness window.
    const H_MS: u64 = 60 * 1_000;

    fn worker(now_ns: i64) -> WorkerSet {
        WorkerSet::new(now_ns, H, DEFAULT_LIVENESS_FACTOR, DEFAULT_UNIT_CONCURRENCY)
    }

    /// Write `worker`'s heartbeat so the object carries a *store* modification
    /// time of `mtime_ms`, which is what the LIST result reports and therefore
    /// what the read-side skip and the reaper judge against. The oracle's clock
    /// is left at `restore_ms`, so anything written afterwards lands current.
    async fn beat_with_mtime(
        memory: &MemoryStore,
        store: &dyn ObjectStoreBackend,
        worker: &WorkerSet,
        mtime_ms: u64,
        restore_ms: u64,
        stamp_ns: i64,
    ) {
        memory.set_clock_ms(mtime_ms);
        worker
            .write_heartbeat(store, stamp_ns)
            .await
            .expect("seed heartbeat");
        memory.set_clock_ms(restore_ms);
    }

    /// Every key currently under `sys/maintain/workers/`, sorted.
    async fn worker_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
        let mut keys: Vec<String> = list_all(store, WORKERS_PREFIX)
            .await
            .expect("list workers prefix")
            .into_iter()
            .map(|meta| meta.key)
            .collect();
        keys.sort();
        keys
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

    /// Two workers constructed without an explicit id get DIFFERENT ids. The
    /// `with_process_id` seam must stay an override, never a shared default: two
    /// live processes on one id would write one heartbeat key and carry one
    /// rendezvous weight, colliding on ownership of every unit.
    #[test]
    fn default_process_ids_are_distinct() {
        let a = worker(0);
        let b = worker(0);
        assert_ne!(a.process_id(), b.process_id());
        assert_ne!(
            WorkerSet::with_defaults(0).process_id(),
            WorkerSet::with_defaults(0).process_id()
        );
    }

    /// `with_process_id` pins the identity the rendezvous hash resolves against,
    /// leaving the rest of the configuration alone.
    #[test]
    fn with_process_id_pins_the_identity() {
        let pinned = Uuid::from_u128(0xABCD);
        let w = worker(0).with_process_id(pinned);
        assert_eq!(w.process_id(), pinned);
        assert_eq!(w.solo_live_set(), vec![pinned]);
        assert_eq!(w.unit_concurrency(), DEFAULT_UNIT_CONCURRENCY);
        assert_eq!(w.heartbeat_interval(), H);
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
        assert_eq!(
            live_a, live_b,
            "both processes see the same sorted live set"
        );
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
        a.write_heartbeat(&store, 3 * H_NS)
            .await
            .expect("a heartbeat");

        // Exactly 3*H old: still live (inclusive on the fresh side).
        let live = a.live_set(&store, 3 * H_NS).await.expect("live set");
        assert!(live.contains(&b.process_id()), "3*H is still live");

        // One nanosecond past 3*H: excluded.
        let live = a.live_set(&store, 3 * H_NS + 1).await.expect("live set");
        assert!(!live.contains(&b.process_id()), "past 3*H is stale");
        assert_eq!(live, vec![a.process_id()], "only self survives");
    }

    /// A far-future-dated heartbeat (clock skew, or a stuck writer that wrote
    /// its own future timestamp) must be excluded exactly like a far-past one,
    /// not treated as live forever. A live-forever phantom can win the
    /// rendezvous argmax for a unit and never relinquish it, permanently
    /// starving every real worker of that unit -- the opposite of a fail-open
    /// direction, since fail-open here means MORE claimants on doubt, not a
    /// single unkillable one.
    #[test]
    fn future_dated_sibling_is_excluded_at_three_h() {
        // Symmetric to `stale_sibling_is_excluded_at_three_h`: exactly 3*H in
        // the future is still live (inclusive), one nanosecond past that is
        // excluded.
        assert!(
            !is_stale(0, 3 * H_NS, 3 * H_NS),
            "exactly 3*H in the future is still live"
        );
        assert!(
            is_stale(0, 3 * H_NS + 1, 3 * H_NS),
            "past 3*H in the future must be excluded, not treated as live forever"
        );
    }

    /// The same exclusion, exercised through `live_set` end to end: a sibling
    /// whose stored heartbeat is impossibly far in the future never appears
    /// in the reader's live set, so it can never win ownership of any unit.
    #[tokio::test]
    async fn future_dated_sibling_never_wins_ownership() {
        let store = MemoryStore::new();
        let a = worker(0);
        let phantom = worker(0);

        // The phantom's heartbeat claims a timestamp far past the liveness
        // window into the future relative to every real reader's clock.
        phantom
            .write_heartbeat(&store, 100 * H_NS)
            .await
            .expect("phantom heartbeat");
        a.write_heartbeat(&store, 0).await.expect("a heartbeat");

        let live = a.live_set(&store, 0).await.expect("live set");
        assert!(
            !live.contains(&phantom.process_id()),
            "a far-future-dated heartbeat must not read as live"
        );
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

    /// Test debt (formal/tla/TRACEABILITY.md, `HeartbeatAndMemoNeverCas`): the
    /// row asks for a test that both a heartbeat PUT and a "memo" PUT use
    /// `PutMode::Overwrite`, not CAS. `write_heartbeat` issues exactly one PUT;
    /// no "memo" write exists anywhere in `ravel-fleet`. The memo write is
    /// `ravel_maintain::memo_snapshot::write_memo_snapshot`, in a different
    /// crate, and the two are only ever driven together from
    /// `services/ravel-server`'s maintain loop -- outside this task's scope.
    /// This test pins what is true and testable here: the heartbeat PUT is an
    /// unconditional overwrite. Proven behaviorally, since no store wrapper in
    /// this workspace records the `PutMode` a call used: a second heartbeat
    /// write to an already-populated key succeeds and its content wins, which
    /// neither CAS variant (`CreateIfAbsent`, `CasVersion`) can do without the
    /// caller reading and passing a precondition -- and `write_heartbeat` never
    /// reads the key before writing it.
    #[tokio::test]
    async fn heartbeat_and_memo_puts_use_overwrite_not_cas() {
        let store = MemoryStore::new();
        let w = worker(0);

        w.write_heartbeat(&store, 1_000)
            .await
            .expect("first heartbeat write succeeds");
        w.write_heartbeat(&store, 2_000)
            .await
            .expect("second heartbeat write to the same key succeeds unconditionally");

        let key = heartbeat_key(&w.process_id());
        let got = store
            .get(&key, GetRange::Full)
            .await
            .expect("get heartbeat");
        let heartbeat = WorkerHeartbeat::decode(got.data.as_ref()).expect("decode heartbeat");
        assert_eq!(
            heartbeat.heartbeat_unix_ns, 2_000,
            "the second write's content wins: no CAS precondition blocked it"
        );
    }

    /// The reaper deletes exactly the keys past the reap horizon (issue #1679):
    /// dead workers go, a worker inside the clock-skew margin stays even though
    /// it is already out of the live set, a fresh worker stays, this process's
    /// own key stays, and a key that is not `sys/maintain/workers/<uuid>` is
    /// left alone.
    async fn reap_case(memory: MemoryStore) {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let store = memory;
        store.set_clock_ms(now_ms);

        let me = worker(now_ns);
        let fresh = worker(now_ns);
        // 4 * H old: past the 3 * H liveness window, inside the 6 * H horizon.
        let skewed = worker(now_ns);
        let dead: Vec<WorkerSet> = (0..3).map(|_| worker(now_ns)).collect();

        beat_with_mtime(&store, &store, &me, now_ms, now_ms, now_ns).await;
        beat_with_mtime(&store, &store, &fresh, now_ms, now_ms, now_ns).await;
        beat_with_mtime(
            &store,
            &store,
            &skewed,
            now_ms - 4 * H_MS,
            now_ms,
            now_ns - 4 * H_NS,
        )
        .await;
        for d in &dead {
            beat_with_mtime(
                &store,
                &store,
                d,
                now_ms - 10 * H_MS,
                now_ms,
                now_ns - 10 * H_NS,
            )
            .await;
        }
        // A key under the same prefix that this mechanism does not own: never
        // reaped, however old it looks.
        store.set_clock_ms(now_ms - 100 * H_MS);
        store
            .put(
                &format!("{WORKERS_PREFIX}not-a-uuid"),
                b"x".to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed foreign key");
        store.set_clock_ms(now_ms);

        let reaped = me.reap_dead_workers(&store, now_ns).await.expect("reap");
        assert_eq!(reaped, 3, "exactly the three keys past the horizon");

        let mut expected = vec![
            heartbeat_key(&me.process_id()),
            heartbeat_key(&fresh.process_id()),
            heartbeat_key(&skewed.process_id()),
            format!("{WORKERS_PREFIX}not-a-uuid"),
        ];
        expected.sort();
        assert_eq!(
            worker_keys(&store).await,
            expected,
            "own, fresh, inside-the-margin and foreign keys all survive"
        );

        // Reaping again is idempotent: nothing left is past the horizon.
        assert_eq!(
            me.reap_dead_workers(&store, now_ns)
                .await
                .expect("second reap"),
            0
        );
    }

    /// The tick's listing serves both purposes: `live_set_read` returns the
    /// live set AND the reap candidates from ONE listing, so reaping costs no
    /// second LIST of the same prefix. The previous shape called `live_set`
    /// and then `reap_dead_workers`, each with its own `list_all`, while three
    /// doc sites claimed the reap added no listing of its own.
    ///
    /// Asserted through the FaultStore's LIST counter, because that is the
    /// claim: one listing, not two.
    ///
    /// Flip to watch it fail: have the body call `live_set` and then
    /// `reap_dead_workers` instead of `live_set_read` plus `reap_keys`. The
    /// LIST count goes to 2.
    #[tokio::test]
    async fn one_listing_serves_both_the_live_set_and_the_reap() {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let memory = MemoryStore::new();
        memory.set_clock_ms(now_ms);

        let me = worker(now_ns);
        let dead = worker(now_ns);

        // Seed through the memory store directly: the rule below matches
        // `list`, and seeding is `put`, but going through the plain store
        // keeps the scripted occurrence counting only the read path.
        beat_with_mtime(&memory, &memory, &me, now_ms, now_ms, now_ns).await;
        beat_with_mtime(
            &memory,
            &memory,
            &dead,
            now_ms - 10 * H_MS,
            now_ms,
            now_ns - 10 * H_NS,
        )
        .await;

        // Fault the SECOND listing of this prefix. The previous shape called
        // `live_set` and then `reap_dead_workers`, each with its own
        // `list_all`, so it tripped this; one listing never reaches it.
        let store = FaultStore::new(
            memory,
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::List,
                    ScriptedFault::Transient("a second listing".into()),
                )
                .with_occurrence(Occurrence::Nth(2)),
            ),
        );

        let read = me.live_set_read(&store, now_ns).await.expect("one read");
        let reaped = me.reap_keys(&store, &read.reapable).await;

        assert_eq!(
            store.fault_count(Op::List, FaultKind::Transient),
            0,
            "the live set and the reap must come from one listing, not two"
        );
        assert_eq!(reaped, 1, "the dead worker's key was reaped");
        assert_eq!(read.live, vec![me.process_id], "only this process is live");
    }

    #[tokio::test]
    async fn dead_worker_heartbeats_are_reaped_past_the_horizon() {
        reap_case(MemoryStore::new()).await;
    }

    /// The same case over a listing that pages: a reaper that only ever sees
    /// the first page leaves most of a long-lived prefix behind.
    #[tokio::test]
    async fn dead_worker_heartbeats_are_reaped_across_list_pages() {
        reap_case(MemoryStore::with_page_size(2)).await;
    }

    /// `live_set` pays one GET per LIVE sibling, not per listed key (issue
    /// #1679): the 2nd GET under the workers prefix is scripted to fail, so a
    /// cycle that fetches any key the LIST already showed as stale surfaces as
    /// a fired fault and as a live set missing the one live sibling.
    #[tokio::test]
    async fn live_set_costs_no_get_for_a_key_the_list_shows_stale() {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("a 2nd workers-prefix GET".into()),
            )
            .with_key_contains(WORKERS_PREFIX)
            .with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        store.inner().set_clock_ms(now_ms);

        let me = worker(now_ns);
        let live = worker(now_ns);
        beat_with_mtime(store.inner(), &store, &me, now_ms, now_ms, now_ns).await;
        beat_with_mtime(store.inner(), &store, &live, now_ms, now_ms, now_ns).await;
        for _ in 0..8 {
            beat_with_mtime(
                store.inner(),
                &store,
                &worker(now_ns),
                now_ms - 10 * H_MS,
                now_ms,
                now_ns - 10 * H_NS,
            )
            .await;
        }

        let set = me.live_set(&store, now_ns).await.expect("live set");
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Transient),
            0,
            "a 2nd GET under the workers prefix never happened: at most 1"
        );
        let mut expected = vec![me.process_id(), live.process_id()];
        expected.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(
            set, expected,
            "self plus the one live sibling, and nobody else"
        );
    }

    /// The modification-time judgement the skip and the reaper share: unknown
    /// is never ancient, the boundary is inclusive on the fresh side, a future
    /// modification time is read rather than reaped, and the reap horizon is
    /// the same window widened by the factor.
    #[test]
    fn mtime_staleness_is_one_directional_and_treats_unknown_as_fresh() {
        let now_ns = 1_000 * H_NS;
        let now_ms = now_ns / 1_000_000;
        let window = 3 * H_NS;
        let h_ms = H_MS as i64;

        assert!(!mtime_stale(now_ns, 0, window), "unknown, so read the key");
        assert!(!mtime_stale(now_ns, -1, window));
        assert!(
            !mtime_stale(now_ns, now_ms - 3 * h_ms, window),
            "exactly the window old is still live, as is_stale judges it"
        );
        assert!(mtime_stale(now_ns, now_ms - 3 * h_ms - 1, window));
        assert!(
            !mtime_stale(now_ns, now_ms + 100 * h_ms, window),
            "a store clock running ahead is a reason to read the key, not to reap it"
        );
        assert_eq!(reap_horizon_ns(window), 6 * H_NS);
        assert!(!mtime_stale(
            now_ns,
            now_ms - 6 * h_ms,
            reap_horizon_ns(window)
        ));
        assert!(mtime_stale(
            now_ns,
            now_ms - 6 * h_ms - 1,
            reap_horizon_ns(window)
        ));
    }
}

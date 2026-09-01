//! Bounded-cardinality per-tenant PUT attribution (ADR-0076 decision 2).
//!
//! # Why this is not a Prometheus label
//!
//! [`crate::IngestMetrics`] and its log/span counterparts are label-free by a
//! deliberate cardinality choice: a single instance is shared across every
//! shard and every tenant of a process, and each counter is a sum-only
//! process-global total (see that module's docs). ADR-0076 decision 2 makes
//! per-tenant shard count the primary operator-facing cost control, and needs
//! per-tenant PUT volume for an operator to choose a target or verify a
//! saving. The ADR rules out an unbounded per-tenant Prometheus label
//! explicitly and names two sanctioned shapes: a top-K attribution or a
//! dedicated accounting endpoint. This structure is the top-K shape: it tracks
//! at most [`MAX_TRACKED_TENANTS`] distinct tenants no matter how many flush,
//! so its memory and cardinality are bounded by a compile-time constant rather
//! than by the tenant count of the process.
//!
//! # Counting convention
//!
//! One unit is one completed flush's object-store PUTs. ADR-0076's cost model
//! charges [`PUTS_PER_FLUSH`] PUTs per flush (the data object, then the commit
//! record). Attribution is recorded once, at flush *completion* -- the
//! terminal success path of each shard actor's `run_flush`, after both PUTs
//! have landed -- never at flush open. A flush abandoned before its commit
//! record lands contributes no PUTs here, and a tenant that never completes a
//! flush has no entry at all, which reads as zero. This is success-time
//! accounting, unlike `record_flush`'s attempt-time trigger counters.
//!
//! # Admission and eviction (Space-Saving)
//!
//! The map holds at most [`MAX_TRACKED_TENANTS`] entries. Recording PUTs for a
//! tenant already tracked adds to its count. A new tenant is admitted into a
//! free slot with an overestimate error of 0. When the map is full, the new
//! tenant evicts the entry with the smallest count (ties broken by tenant hash
//! so eviction is deterministic, not dependent on hash-map iteration order),
//! and *inherits that evicted count* as its own baseline, recording the
//! inherited value in [`TenantPutCount::error`] as the bound on how far its
//! reported count may exceed its true count.
//!
//! This is the Space-Saving heavy-hitters algorithm. Its guarantee is that any
//! tenant whose true PUT count exceeds `total_puts / MAX_TRACKED_TENANTS` is
//! always present in the table, so the reported top-K reflects the true top-K.
//! Two simpler policies do not hold this and are rejected for it:
//!
//! - **FIFO / seen-first eviction** drops whichever entry was inserted
//!   earliest, which can be a genuine heavy hitter, so the table degrades to
//!   "the K tenants seen most recently" rather than the K largest.
//! - **Reset-on-evict min-eviction** (evict the min, admit the newcomer at its
//!   own increment) starves a rising tenant that keeps arriving one flush at a
//!   time into a full table: its per-step increment never beats the standing
//!   minimum, so it is never admitted even as its true volume grows.
//!   Inheriting the evicted baseline is what lets a newcomer overtake and stay.

use std::collections::HashMap;
use std::sync::Mutex;

use ravel_types::TenantHash;

/// Maximum number of distinct tenants tracked at once. The hard cardinality
/// and memory bound for the whole structure: at most this many `(TenantHash,
/// count, error)` entries exist regardless of how many tenants flush, which is
/// what keeps this attribution bounded where an unbounded per-tenant
/// Prometheus label would not be (ADR-0076 decision 2). Sized to comfortably
/// cover the distinct-tenant count of a busy ingest process while costing on
/// the order of tens of KiB per signal.
pub const MAX_TRACKED_TENANTS: usize = 1024;

/// Object-store PUTs charged to a tenant per completed flush, per ADR-0076's
/// cost model: the data-object PUT and the commit-record PUT.
pub const PUTS_PER_FLUSH: u64 = 2;

/// One tracked tenant's attributed PUT count. `error` is the Space-Saving
/// overestimate bound: this tenant's true count lies in `[puts - error,
/// puts]`. `error` is 0 for a tenant admitted into a free slot and never
/// evicted-and-readmitted, i.e. an exact count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantPutCount {
    pub tenant: TenantHash,
    pub puts: u64,
    pub error: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Entry {
    count: u64,
    error: u64,
}

/// Bounded-cardinality top-K per-tenant PUT attribution. Shared across all
/// shard actors of one signal through an `Arc`, exactly like the counters in
/// [`crate::IngestMetrics`], so recorded PUTs for one tenant sum across every
/// shard that flushed for it. See the [module docs](self) for the counting
/// convention and the admission/eviction policy.
#[derive(Debug)]
pub struct TenantPutAttribution {
    cap: usize,
    entries: Mutex<HashMap<TenantHash, Entry>>,
}

impl Default for TenantPutAttribution {
    fn default() -> Self {
        Self::with_capacity(MAX_TRACKED_TENANTS)
    }
}

impl TenantPutAttribution {
    /// A structure tracking at most `cap` distinct tenants. Production code
    /// uses [`Default`] ([`MAX_TRACKED_TENANTS`]); `cap` is a knob for tests
    /// that need to drive eviction without pushing thousands of tenants.
    /// `cap` must be at least 1.
    pub(crate) fn with_capacity(cap: usize) -> Self {
        debug_assert!(cap >= 1, "attribution capacity must be at least 1");
        Self {
            cap: cap.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Attribute one completed flush's [`PUTS_PER_FLUSH`] PUTs to `tenant`.
    /// Called from each shard actor's `run_flush` terminal success path.
    pub(crate) fn record_flush(&self, tenant: TenantHash) {
        self.record_puts(tenant, PUTS_PER_FLUSH);
    }

    /// Add `puts` to `tenant`'s attributed count, applying the Space-Saving
    /// admission/eviction policy documented on the module when the tenant is
    /// new and the table is full.
    fn record_puts(&self, tenant: TenantHash, puts: u64) {
        let mut map = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = map.get_mut(&tenant) {
            entry.count = entry.count.saturating_add(puts);
            return;
        }
        if map.len() < self.cap {
            map.insert(
                tenant,
                Entry {
                    count: puts,
                    error: 0,
                },
            );
            return;
        }
        // Table full and `tenant` is new: evict the smallest-count entry (ties
        // broken by tenant hash for determinism) and admit `tenant` inheriting
        // the evicted count as its Space-Saving baseline and overestimate
        // bound.
        let victim = map
            .iter()
            .min_by(|a, b| a.1.count.cmp(&b.1.count).then_with(|| a.0.cmp(b.0)))
            .map(|(&t, e)| (t, e.count));
        if let Some((victim_tenant, victim_count)) = victim {
            map.remove(&victim_tenant);
            map.insert(
                tenant,
                Entry {
                    count: victim_count.saturating_add(puts),
                    error: victim_count,
                },
            );
        }
    }

    /// The `n` tenants with the most attributed PUTs, descending (ties broken
    /// by tenant hash for a stable order). Returns fewer than `n` when fewer
    /// are tracked, and an empty vector when none are. This is the read API a
    /// later operator-facing endpoint (ADR-0076 T4) consumes.
    pub fn top_n(&self, n: usize) -> Vec<TenantPutCount> {
        let map = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let mut all: Vec<TenantPutCount> = map
            .iter()
            .map(|(&tenant, e)| TenantPutCount {
                tenant,
                puts: e.count,
                error: e.error,
            })
            .collect();
        all.sort_unstable_by(|a, b| b.puts.cmp(&a.puts).then_with(|| a.tenant.cmp(&b.tenant)));
        all.truncate(n);
        all
    }

    /// Number of tenants currently tracked. Never exceeds
    /// [`MAX_TRACKED_TENANTS`] (or the `with_capacity` cap in tests).
    pub fn tracked_len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn th(n: u8) -> TenantHash {
        TenantHash([n; 16])
    }

    #[test]
    fn record_flush_charges_two_puts() {
        let attr = TenantPutAttribution::default();
        attr.record_flush(th(1));
        attr.record_flush(th(1));
        let top = attr.top_n(10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].tenant, th(1));
        assert_eq!(top[0].puts, 2 * PUTS_PER_FLUSH);
        assert_eq!(top[0].error, 0);
    }

    #[test]
    fn untracked_tenant_contributes_zero_not_an_entry() {
        let attr = TenantPutAttribution::default();
        // Nothing recorded: no entries at all.
        assert!(attr.top_n(10).is_empty());
        assert_eq!(attr.tracked_len(), 0);

        // Record only for tenant 1. Tenant 2 never flushed and must be absent,
        // not present with a zero count.
        attr.record_flush(th(1));
        let top = attr.top_n(10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].tenant, th(1));
        assert!(top.iter().all(|t| t.tenant != th(2)));
        assert_eq!(attr.tracked_len(), 1);
    }

    #[test]
    fn concurrent_flushes_for_one_tenant_across_shards_sum() {
        // The structure is keyed by tenant only, so "same tenant, different
        // shard" is exactly concurrent record_flush calls from different
        // threads against one shared Arc, as the three shard actors of a
        // signal do through their shared metrics Arc.
        let attr = Arc::new(TenantPutAttribution::default());
        let threads = 4;
        let per_thread = 250;
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let attr = Arc::clone(&attr);
                thread::spawn(move || {
                    for _ in 0..per_thread {
                        attr.record_flush(th(9));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("thread");
        }
        let top = attr.top_n(10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].tenant, th(9));
        assert_eq!(top[0].puts, threads * per_thread * PUTS_PER_FLUSH);
        // A single, never-evicted tenant carries an exact count.
        assert_eq!(top[0].error, 0);
    }

    #[test]
    fn eviction_drops_the_lowest_count_entry_not_an_arbitrary_one() {
        let attr = TenantPutAttribution::with_capacity(3);
        attr.record_puts(th(1), 100);
        attr.record_puts(th(2), 50);
        attr.record_puts(th(3), 30);
        assert_eq!(attr.tracked_len(), 3);

        // New tenant into a full table: the smallest entry (tenant 3, 30) must
        // be the one evicted, not tenant 1 or 2.
        attr.record_puts(th(4), 1);
        assert_eq!(attr.tracked_len(), 3);
        let tracked: Vec<TenantHash> = attr.top_n(10).into_iter().map(|t| t.tenant).collect();
        assert!(tracked.contains(&th(1)));
        assert!(tracked.contains(&th(2)));
        assert!(tracked.contains(&th(4)));
        assert!(
            !tracked.contains(&th(3)),
            "lowest-count entry must be dropped"
        );

        // The newcomer inherited the evicted count (30) as baseline and error.
        let newcomer = attr
            .top_n(10)
            .into_iter()
            .find(|t| t.tenant == th(4))
            .expect("newcomer present");
        assert_eq!(newcomer.puts, 31);
        assert_eq!(newcomer.error, 30);
    }

    /// Eviction correctness (ADR-0076 top-K soundness): a true heavy hitter
    /// must survive a new low-volume tenant arriving into a full table. This
    /// test FAILS against a FIFO / insertion-order eviction shape, which would
    /// evict `heavy` because it was inserted first, and PASSES against the
    /// Space-Saving min-eviction implemented here, which evicts the standing
    /// minimum instead. It is the "would fail against FIFO" test the task asks
    /// for.
    #[test]
    fn eviction_preserves_true_top_k_not_insertion_order() {
        let attr = TenantPutAttribution::with_capacity(2);
        attr.record_puts(th(1), 100); // heavy, inserted first
        attr.record_puts(th(2), 40); // mid
        assert_eq!(attr.tracked_len(), 2);

        // Newcomer into the full table. Min-eviction drops `mid` (40); FIFO
        // would drop `heavy` (oldest).
        attr.record_puts(th(3), 1);
        assert_eq!(attr.tracked_len(), 2);

        let top = attr.top_n(2);
        // Under FIFO, th(1) would be gone here and this first assertion fails.
        assert!(
            top.iter().any(|t| t.tenant == th(1) && t.puts == 100),
            "the true heavy hitter must survive insertion-order eviction"
        );
        assert_eq!(top[0].tenant, th(1), "heavy hitter is still the top entry");
        assert!(
            !top.iter().any(|t| t.tenant == th(2)),
            "the minimum was evicted"
        );
    }

    #[test]
    fn cardinality_cap_never_exceeded_under_load() {
        let cap = 4;
        let attr = TenantPutAttribution::with_capacity(cap);
        // Far more distinct tenants than the cap, each with a distinct count.
        for i in 0..100u8 {
            attr.record_puts(th(i), u64::from(i) + 1);
            assert!(
                attr.tracked_len() <= cap,
                "tracked set must never exceed the cap"
            );
        }
        assert_eq!(attr.tracked_len(), cap);
    }

    #[test]
    fn top_n_orders_by_count_descending() {
        let attr = TenantPutAttribution::default();
        attr.record_puts(th(1), 10);
        attr.record_puts(th(2), 30);
        attr.record_puts(th(3), 20);
        let top = attr.top_n(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].tenant, th(2));
        assert_eq!(top[0].puts, 30);
        assert_eq!(top[1].tenant, th(3));
        assert_eq!(top[1].puts, 20);
    }
}

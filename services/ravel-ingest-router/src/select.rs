//! Subset selection and the local round-robin pick (deliverable 4).
//!
//! [`pick`] is a pure function of the HRW-ranked order, the Ready dial set, the
//! subset size, and a round-robin offset, so it is unit-testable with no watcher
//! and no HTTP server. [`RoundRobin`] holds the bounded per-tenant offset state.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;

use ravel_affinity::ReplicaId;

/// A routing decision the proxy layer turns into a status code.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RouteError {
    /// The watcher has not completed its first sync yet (cold start). 503.
    #[error("router not ready: endpoint watcher has not completed its first sync")]
    NotSynced,
    /// Canonical-tenant resolution failed. 401. Carries no key bytes.
    #[error("unauthenticated")]
    Unauthenticated,
    /// Every ranked replica was exhausted with none Ready. 503.
    #[error("no ready gateway replica available")]
    Exhausted,
    /// The upstream dial or forward failed. 502.
    #[error("bad gateway")]
    Upstream,
}

/// The chosen replica and the address to dial it at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Selection {
    pub id: ReplicaId,
    pub addr: SocketAddr,
}

/// Pick one replica for a tenant from the HRW-ranked order.
///
/// `ranked` is the full [`ravel_affinity::rank`] order over the current
/// membership. `ready_addr` maps a replica to its dial address iff it is Ready.
/// `subset_size` is `S`; `rr_offset` rotates the pick across repeated calls for
/// the same tenant.
///
/// The subset is `ranked[0..S]`. The pick rotates round-robin across the subset
/// starting at `rr_offset % S`, taking the first member that is Ready. If no
/// subset member is Ready, it walks the HRW order past position `S`
/// (`S`, `S+1`, ...) in order, rather than narrowing to a smaller subset or
/// re-querying. If every ranked replica is exhausted, it returns
/// [`RouteError::Exhausted`] (never a panic).
pub(crate) fn pick(
    ranked: &[&ReplicaId],
    ready_addr: &HashMap<ReplicaId, SocketAddr>,
    subset_size: usize,
    rr_offset: u64,
) -> Result<Selection, RouteError> {
    let n = ranked.len();
    if n == 0 {
        return Err(RouteError::Exhausted);
    }
    // The subset window, clamped to the membership and to at least one so the
    // modulo below is well-defined.
    let s = subset_size.min(n).max(1);
    let start = (rr_offset % s as u64) as usize;

    // Round-robin within the subset: the first Ready member starting at `start`.
    for i in 0..s {
        let idx = (start + i) % s;
        if let Some(addr) = ready_addr.get(ranked[idx]) {
            return Ok(Selection {
                id: ranked[idx].clone(),
                addr: *addr,
            });
        }
    }
    // No Ready subset member: fall through the HRW order past position `S`.
    for r in ranked.iter().take(n).skip(s) {
        if let Some(addr) = ready_addr.get(*r) {
            return Ok(Selection {
                id: (*r).clone(),
                addr: *addr,
            });
        }
    }
    Err(RouteError::Exhausted)
}

/// One per-tenant round-robin slot: the next offset and when it was last used.
#[derive(Clone, Copy)]
struct Slot {
    counter: u64,
    last_used_ns: i64,
}

/// Number of entries sampled per eviction to approximate least-recently-used
/// without scanning the whole map. The map's keys are blake3 hashes, so a
/// HashMap iteration prefix is an unbiased sample uncorrelated with recency;
/// the oldest of the sample is a good-enough eviction victim (Redis-style
/// sampled LRU). Small so a single eviction is O(1), not O(n).
const EVICTION_SAMPLE_SIZE: usize = 8;

/// Hard cap on the number of entries a single `make_room` call may evict.
/// Bounds the per-`tick` (per-request) eviction work to O(1). In steady state
/// the map sits exactly at `max_entries`, so a new-key insertion evicts one
/// entry and this cap is never approached; it only bites when the map is
/// genuinely over the cap (e.g. after a burst under lock contention), letting
/// the map converge back toward `max_entries` over successive calls (each
/// over-cap call nets `1 - MAX_EVICTIONS_PER_CALL` entries) without any single
/// call paying the full O(n) drain.
const MAX_EVICTIONS_PER_CALL: usize = 8;

/// Bounded per-tenant round-robin offsets (ADR-0069 idle-eviction shape).
///
/// Keyed by a blake3 hash of the tenant key (never the raw bytes). Bounded two
/// ways so client-controlled key churn (a rotating bearer token under
/// `authorization-header`) cannot grow it without limit: entries idle past
/// `idle_ttl_ns` are swept by the background task, and a hard `max_entries` cap
/// evicts an approximately-least-recently-used entry on overflow. Overflow
/// eviction is amortized and bounded (see [`RoundRobin::make_room`]): it never
/// scans the whole map on the request path, so sustained new-key churn once the
/// cap is reached cannot turn the memory bound into a per-request CPU cliff. A
/// dropped entry only restarts that tenant's rotation at offset 0, so eviction
/// is never correctness-bearing.
pub(crate) struct RoundRobin {
    slots: Mutex<HashMap<[u8; 32], Slot>>,
    max_entries: usize,
    idle_ttl_ns: i64,
}

impl RoundRobin {
    pub(crate) fn new(max_entries: usize, idle_ttl_ns: i64) -> Self {
        RoundRobin {
            slots: Mutex::new(HashMap::new()),
            max_entries: max_entries.max(1),
            idle_ttl_ns: idle_ttl_ns.max(1),
        }
    }

    /// Return this tenant's current round-robin offset, then advance it. Records
    /// `now_ns` as the entry's last use for idle eviction.
    pub(crate) fn tick(&self, key_hash: [u8; 32], now_ns: i64) -> u64 {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        if !slots.contains_key(&key_hash) {
            self.make_room(&mut slots);
        }
        let slot = slots.entry(key_hash).or_insert(Slot {
            counter: 0,
            last_used_ns: now_ns,
        });
        let offset = slot.counter;
        slot.counter = slot.counter.wrapping_add(1);
        slot.last_used_ns = now_ns;
        offset
    }

    /// Evict entries idle longer than the TTL. Returns the number removed.
    /// Driven by the background sweep and by unit tests.
    pub(crate) fn evict_idle(&self, now_ns: i64) -> usize {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let before = slots.len();
        slots.retain(|_, slot| now_ns.saturating_sub(slot.last_used_ns) < self.idle_ttl_ns);
        before - slots.len()
    }

    /// Current entry count (tests).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Make room for one more entry when the map is at or over `max_entries`.
    ///
    /// Eviction is amortized and bounded: this never scans the whole map. While
    /// the map is at or over the cap it removes up to [`MAX_EVICTIONS_PER_CALL`]
    /// approximately-least-recently-used entries, each chosen as the oldest of a
    /// bounded [`EVICTION_SAMPLE_SIZE`] sample (a HashMap iteration prefix, which
    /// for blake3-hashed keys is an unbiased sample uncorrelated with recency).
    /// The loop condition still gates on the cap, so in steady state (map at the
    /// cap) a single new-key insertion evicts exactly one entry; the batch only
    /// does more work when the map is genuinely over the cap, so it converges
    /// back toward `max_entries` without any single call paying the O(n) cost.
    ///
    /// The idle-TTL `retain` sweep is deliberately NOT run here: it is an O(n)
    /// scan, and running it on the request path once the cap is reached (under
    /// the held lock, on every `tick` from a churning client) is exactly the CPU
    /// cliff this strategy replaces. `spawn_round_robin_sweep` in `lib.rs` runs
    /// [`RoundRobin::evict_idle`] on a timer off the request path for that.
    fn make_room(&self, slots: &mut HashMap<[u8; 32], Slot>) {
        for _ in 0..MAX_EVICTIONS_PER_CALL {
            if slots.len() < self.max_entries {
                return;
            }
            let Some(victim) = slots
                .iter()
                .take(EVICTION_SAMPLE_SIZE)
                .min_by_key(|(_, slot)| slot.last_used_ns)
                .map(|(k, _)| *k)
            else {
                return;
            };
            slots.remove(&victim);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn reps(ids: &[&str]) -> Vec<ReplicaId> {
        ids.iter().map(|s| ReplicaId::new(s.as_bytes())).collect()
    }

    fn refs(reps: &[ReplicaId]) -> Vec<&ReplicaId> {
        reps.iter().collect()
    }

    fn addr(n: u8) -> SocketAddr {
        format!("10.0.0.{n}:8080").parse().expect("addr")
    }

    #[test]
    fn empty_ranking_is_exhausted() {
        let ready = HashMap::new();
        assert!(matches!(
            pick(&[], &ready, 2, 0),
            Err(RouteError::Exhausted)
        ));
    }

    #[test]
    fn picks_a_ready_subset_member() {
        let reps = reps(&["a", "b", "c"]);
        let mut ready = HashMap::new();
        ready.insert(reps[0].clone(), addr(1));
        ready.insert(reps[1].clone(), addr(2));
        ready.insert(reps[2].clone(), addr(3));
        let sel = pick(&refs(&reps), &ready, 2, 0).expect("pick");
        assert_eq!(sel.id, reps[0]);
    }

    #[test]
    fn round_robin_rotates_within_the_subset() {
        let reps = reps(&["a", "b", "c"]);
        let mut ready = HashMap::new();
        ready.insert(reps[0].clone(), addr(1));
        ready.insert(reps[1].clone(), addr(2));
        ready.insert(reps[2].clone(), addr(3));
        // Subset size 2 -> alternates between ranked[0] and ranked[1].
        let s0 = pick(&refs(&reps), &ready, 2, 0).expect("0").id;
        let s1 = pick(&refs(&reps), &ready, 2, 1).expect("1").id;
        let s2 = pick(&refs(&reps), &ready, 2, 2).expect("2").id;
        assert_eq!(s0, reps[0]);
        assert_eq!(s1, reps[1]);
        assert_eq!(s2, reps[0], "wraps back around the subset");
        assert_ne!(s0, s1, "consecutive picks rotate, not repeat");
    }

    #[test]
    fn unready_subset_member_falls_through_past_position_s() {
        let reps = reps(&["a", "b", "c"]);
        // ranked[0] ("a") is NOT ready; subset size 1 means the subset is {a}.
        let mut ready = HashMap::new();
        ready.insert(reps[1].clone(), addr(2));
        ready.insert(reps[2].clone(), addr(3));
        let sel = pick(&refs(&reps), &ready, 1, 0).expect("falls through");
        assert_eq!(
            sel.id, reps[1],
            "falls to the next-ranked Ready replica past position S"
        );
    }

    #[test]
    fn all_unready_is_exhausted_not_a_panic() {
        let reps = reps(&["a", "b"]);
        let ready = HashMap::new();
        assert!(matches!(
            pick(&refs(&reps), &ready, 2, 0),
            Err(RouteError::Exhausted)
        ));
    }

    #[test]
    fn round_robin_tick_advances_and_is_deterministic() {
        let rr = RoundRobin::new(1024, 1_000_000);
        let key = [7u8; 32];
        assert_eq!(rr.tick(key, 0), 0);
        assert_eq!(rr.tick(key, 1), 1);
        assert_eq!(rr.tick(key, 2), 2);
        // A different key has its own independent counter.
        assert_eq!(rr.tick([9u8; 32], 3), 0);
    }

    #[test]
    fn round_robin_evicts_idle_entries() {
        let ttl = 100;
        let rr = RoundRobin::new(1024, ttl);
        rr.tick([1u8; 32], 0);
        rr.tick([2u8; 32], 50);
        assert_eq!(rr.len(), 2);
        // At t=120, entry 1 (idle 120 >= ttl 100) is evicted; entry 2 (idle 70)
        // is retained.
        let evicted = rr.evict_idle(120);
        assert_eq!(evicted, 1);
        assert_eq!(rr.len(), 1);
    }

    #[test]
    fn round_robin_is_bounded_by_max_entries() {
        let rr = RoundRobin::new(4, i64::MAX);
        // Insert far more distinct keys than the cap, each newer than the last,
        // mimicking a stream of rotating bearer tokens.
        for i in 0..1000u64 {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&i.to_le_bytes());
            rr.tick(key, i as i64);
        }
        assert!(
            rr.len() <= 4,
            "map must stay bounded by max_entries, got {}",
            rr.len()
        );
    }

    /// A distinct 32-byte key derived from `i`, mimicking a rotating token hash.
    fn key_of(i: u64) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[..8].copy_from_slice(&i.to_le_bytes());
        key
    }

    #[test]
    fn eviction_under_cap_pressure_does_not_scan_the_whole_map() {
        // Force the map WELL over the cap directly (the public API keeps it at
        // the cap, so we cannot reach an over-cap state through `tick` alone).
        // A single `tick` must then evict only a bounded batch, not drain the
        // whole overflow back to the cap in one O(n) pass the way the old
        // full-scan `while` loop did.
        let cap = 10;
        let rr = RoundRobin::new(cap, i64::MAX);
        {
            let mut slots = rr.slots.lock().expect("lock");
            for i in 0..100u64 {
                slots.insert(
                    key_of(i),
                    Slot {
                        counter: 0,
                        last_used_ns: i as i64,
                    },
                );
            }
        }
        assert_eq!(rr.len(), 100, "precondition: map forced over the cap");

        // One request for a new key.
        rr.tick(key_of(1000), 1000);

        let len = rr.len();
        // Bounded work: at most MAX_EVICTIONS_PER_CALL removed, plus the one
        // insert, so the map barely moved rather than collapsing toward the cap.
        assert!(
            len >= 100 - MAX_EVICTIONS_PER_CALL,
            "a single tick evicted more than the per-call bound: {len}"
        );
        // It did do *some* eviction (proving the batch fired, not that it grew
        // unbounded), and it did NOT shrink to 1 (the old full-scan tell).
        assert!(
            len < 100,
            "eviction must make progress each call, got {len}"
        );
        assert!(
            len > 1,
            "a single call must not drain the whole map, got {len}"
        );
    }

    #[test]
    fn round_robin_converges_toward_cap_under_sustained_pressure() {
        // Start well over the cap, then hammer with distinct new keys. Bounded
        // per-call eviction must still drive the map back down toward the cap
        // over many calls (each over-cap call nets 1 insert minus up to
        // MAX_EVICTIONS_PER_CALL evictions), not merely keep each call cheap.
        let cap = 10;
        let rr = RoundRobin::new(cap, i64::MAX);
        {
            let mut slots = rr.slots.lock().expect("lock");
            for i in 0..500u64 {
                slots.insert(
                    key_of(i),
                    Slot {
                        counter: 0,
                        last_used_ns: i as i64,
                    },
                );
            }
        }
        assert_eq!(rr.len(), 500, "precondition: map forced far over the cap");

        for i in 0..500u64 {
            rr.tick(key_of(1_000 + i), (1_000 + i) as i64);
        }

        // Converged: each new-key `tick` at the cap evicts one and inserts one,
        // so the steady state is exactly the cap, never above it.
        assert!(
            rr.len() <= cap,
            "map did not converge back toward the cap, got {}",
            rr.len()
        );
        assert!(
            rr.len() >= cap - MAX_EVICTIONS_PER_CALL,
            "map shrank below the expected steady state, got {}",
            rr.len()
        );
    }
}

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

/// Bounded per-tenant round-robin offsets (ADR-0069 idle-eviction shape).
///
/// Keyed by a blake3 hash of the tenant key (never the raw bytes). Bounded two
/// ways so client-controlled key churn (a rotating bearer token under
/// `authorization-header`) cannot grow it without limit: entries idle past
/// `idle_ttl_ns` are swept, and a hard `max_entries` cap evicts the
/// least-recently-used entry on overflow. A dropped entry only restarts that
/// tenant's rotation at offset 0, so eviction is never correctness-bearing.
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
            self.make_room(&mut slots, now_ns);
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

    /// Ensure there is room for one more entry: first sweep idle entries, then,
    /// if still at the cap, evict the single least-recently-used entry.
    fn make_room(&self, slots: &mut HashMap<[u8; 32], Slot>, now_ns: i64) {
        if slots.len() < self.max_entries {
            return;
        }
        slots.retain(|_, slot| now_ns.saturating_sub(slot.last_used_ns) < self.idle_ttl_ns);
        while slots.len() >= self.max_entries {
            let Some(oldest) = slots
                .iter()
                .min_by_key(|(_, slot)| slot.last_used_ns)
                .map(|(k, _)| *k)
            else {
                break;
            };
            slots.remove(&oldest);
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
}

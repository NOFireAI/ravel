//! Subset selection and the local round-robin pick (deliverable 4).
//!
//! [`pick`] is a pure function of the HRW-ranked order, the Ready dial set, the
//! subset size, and a round-robin offset, so it is unit-testable with no watcher
//! and no HTTP server. [`RoundRobin`] holds the bounded per-tenant offset state.

use std::collections::{HashMap, VecDeque};
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

/// Hard cap on the number of entries a single `make_room` call may evict.
/// Bounds the per-`tick` (per-request) eviction work to O(1). In steady state
/// the map sits exactly at `max_entries`, so a new-key insertion evicts one
/// entry and this cap is never approached; it only bites when the map is
/// genuinely over the cap (e.g. after a burst under lock contention), letting
/// the map converge back toward `max_entries` over successive calls (each
/// over-cap call nets `1 - MAX_EVICTIONS_PER_CALL` entries) without any single
/// call paying the full O(n) drain.
const MAX_EVICTIONS_PER_CALL: usize = 8;

/// Hard cap on the number of *stale* FIFO entries a single `make_room` call may
/// discard: keys popped off the insertion-order queue that are no longer in the
/// map because the background idle sweep removed them independently. Without
/// this bound, a large burst of idle-sweep removals between two `tick` calls
/// could make one `tick` walk an arbitrarily long run of stale queue entries,
/// which is the same unbounded-per-call work the FIFO is here to remove. On
/// exhausting this budget `make_room` gives up for the call and leaves the map
/// transiently over the cap; the next call (or the next sweep) continues.
const MAX_STALE_DISCARDS_PER_CALL: usize = 32;

/// The `RoundRobin` mutable state: the offset map plus its insertion-order FIFO.
///
/// Both live under one mutex because every mutation touches both and they must
/// not diverge across a lock boundary.
struct State {
    slots: HashMap<[u8; 32], Slot>,
    /// Keys in the order they were inserted into `slots`, oldest at the front.
    /// A key is pushed only when it is newly inserted, never on a repeat `tick`
    /// of a key already present, so a hot key cannot accumulate duplicate
    /// entries and grow this queue without bound. Entries can go stale when the
    /// background idle sweep removes a key from `slots`; `make_room` discards
    /// those on pop (within [`MAX_STALE_DISCARDS_PER_CALL`]) and
    /// [`RoundRobin::evict_idle`] prunes them in bulk off the request path.
    order: VecDeque<[u8; 32]>,
}

/// Bounded per-tenant round-robin offsets (ADR-0069 idle-eviction shape).
///
/// Keyed by a blake3 hash of the tenant key (never the raw bytes). Bounded two
/// ways so client-controlled key churn (a rotating bearer token under
/// `authorization-header`) cannot grow it without limit: entries idle past
/// `idle_ttl_ns` are swept by the background task, and a hard `max_entries` cap
/// evicts the oldest-inserted entry on overflow. Overflow eviction is amortized
/// and bounded (see [`RoundRobin::make_room`]): its cost per call depends on
/// neither `max_entries` nor the map's occupancy pattern, so sustained new-key
/// churn once the cap is reached cannot turn the memory bound into a
/// per-request CPU cliff.
///
/// Overflow eviction order is insertion-order FIFO, not use-recency LRU: a
/// frequently used ("hot") key inserted early is still evicted once
/// `max_entries` newer keys have churned past it. That is an accepted trade for
/// a genuinely constant per-call bound, because a dropped entry only restarts
/// that tenant's rotation at offset 0, so eviction is never
/// correctness-bearing. It is also strictly better than an eviction order
/// biased by the hash table's internal bucket layout, which can never evict a
/// hot key at all while repeatedly re-scanning the same region of the table.
pub(crate) struct RoundRobin {
    state: Mutex<State>,
    max_entries: usize,
    idle_ttl_ns: i64,
}

impl RoundRobin {
    pub(crate) fn new(max_entries: usize, idle_ttl_ns: i64) -> Self {
        RoundRobin {
            state: Mutex::new(State {
                slots: HashMap::new(),
                order: VecDeque::new(),
            }),
            max_entries: max_entries.max(1),
            idle_ttl_ns: idle_ttl_ns.max(1),
        }
    }

    /// Return this tenant's current round-robin offset, then advance it. Records
    /// `now_ns` as the entry's last use for idle eviction.
    pub(crate) fn tick(&self, key_hash: [u8; 32], now_ns: i64) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.slots.contains_key(&key_hash) {
            self.make_room(&mut state);
            // Newly inserted below, so it joins the back of the FIFO exactly
            // once. Repeat ticks of a present key skip this branch entirely.
            state.order.push_back(key_hash);
        }
        let slot = state.slots.entry(key_hash).or_insert(Slot {
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
    ///
    /// This is an O(n) scan, which is fine: `spawn_round_robin_sweep` in
    /// `lib.rs` runs it on a timer, off the request path. It also prunes the
    /// insertion-order FIFO of the keys it just removed, so the FIFO cannot
    /// accumulate stale entries indefinitely when the map churns below the cap
    /// (where `make_room` never runs and so never discards them on pop).
    pub(crate) fn evict_idle(&self, now_ns: i64) -> usize {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let State { slots, order } = &mut *state;
        let before = slots.len();
        slots.retain(|_, slot| now_ns.saturating_sub(slot.last_used_ns) < self.idle_ttl_ns);
        let removed = before - slots.len();
        if removed > 0 {
            order.retain(|key| slots.contains_key(key));
        }
        removed
    }

    /// Current entry count (tests).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .slots
            .len()
    }

    /// Make room for one more entry when the map is at or over `max_entries`.
    ///
    /// Eviction pops the oldest-inserted key off the front of the
    /// insertion-order FIFO and removes it from the map. The FIFO is maintained
    /// independently of the hash table's internal layout, so the per-call cost
    /// is a constant number of `VecDeque::pop_front` plus `HashMap::remove`
    /// operations, independent of `max_entries` and of which buckets happen to
    /// be occupied. Nothing here iterates the map.
    ///
    /// Two independent budgets bound the call: at most
    /// [`MAX_EVICTIONS_PER_CALL`] actual removals, and at most
    /// [`MAX_STALE_DISCARDS_PER_CALL`] popped keys that were already gone from
    /// the map (removed by the background idle sweep). Exhausting either budget
    /// ends the call with the map possibly still over the cap; the cap is a
    /// steady-state bound, not an every-instant invariant, and the next `tick`
    /// or sweep continues the work.
    ///
    /// The idle-TTL `retain` sweep is deliberately NOT run here: it is an O(n)
    /// scan, and running it on the request path once the cap is reached (under
    /// the held lock, on every `tick` from a churning client) is exactly the CPU
    /// cliff this strategy replaces.
    fn make_room(&self, state: &mut State) {
        let mut stale = 0usize;
        let mut evicted = 0usize;
        while evicted < MAX_EVICTIONS_PER_CALL {
            if state.slots.len() < self.max_entries {
                return;
            }
            let Some(victim) = state.order.pop_front() else {
                // FIFO empty with the map at the cap: nothing to evict by
                // insertion order. Cannot happen while the two stay in sync,
                // and returning keeps the call bounded if they ever do not.
                return;
            };
            if state.slots.remove(&victim).is_some() {
                evicted += 1;
            } else {
                // Stale front entry: the idle sweep already removed this key.
                stale += 1;
                if stale >= MAX_STALE_DISCARDS_PER_CALL {
                    return;
                }
            }
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

    /// Force the map (and its FIFO) over the cap directly. The public API keeps
    /// the map at the cap, so an over-cap state is not reachable through `tick`
    /// alone. Keys `0..n` are inserted in order, oldest first.
    fn force_over_cap(rr: &RoundRobin, n: u64) {
        let mut state = rr.state.lock().expect("lock");
        for i in 0..n {
            state.slots.insert(
                key_of(i),
                Slot {
                    counter: 0,
                    last_used_ns: i as i64,
                },
            );
            state.order.push_back(key_of(i));
        }
    }

    /// Per-call `tick` cost under sustained churn at a given cap, in nanoseconds
    /// per call: churn `churn` distinct new keys to reach steady state, then time
    /// `measure` further distinct-key calls and return the mean.
    fn steady_state_ns_per_tick(cap: usize, churn: u64, measure: u64) -> f64 {
        let rr = RoundRobin::new(cap, i64::MAX);
        for i in 0..churn {
            rr.tick(key_of(i), i as i64);
        }
        let start = std::time::Instant::now();
        for i in churn..(churn + measure) {
            rr.tick(key_of(i), i as i64);
        }
        start.elapsed().as_nanos() as f64 / measure as f64
    }

    #[test]
    fn surviving_entries_are_recent_under_sustained_cap_pressure() {
        // The property the sampled-LRU predecessor violated. It sampled a fixed
        // prefix of `HashMap::iter()`, which always starts at the same low
        // bucket, so it evicted from one region of the table forever: entries in
        // higher buckets survived indefinitely regardless of age (the review
        // measured 1016/1024 residents last used more than `cap` ticks earlier,
        // with the very first key ever inserted still resident after 100k
        // ticks), and walking the hollowed-out prefix made each call O(n).
        //
        // Under insertion-order FIFO with every `tick` using a fresh key, the
        // residents are exactly the last `cap` keys inserted, so every resident
        // must have `last_used_ns >= ticks - cap`. The assertion allows 2x that
        // slack to stay robust against the bounded-batch behaviour, and is still
        // ~100x tighter than what the predecessor produced.
        let cap = 1024usize;
        let ticks = 100_000u64;
        let rr = RoundRobin::new(cap, i64::MAX);
        for i in 0..ticks {
            rr.tick(key_of(i), i as i64);
        }

        let state = rr.state.lock().expect("lock");
        assert_eq!(state.slots.len(), cap, "steady state sits at the cap");
        assert!(
            !state.slots.contains_key(&key_of(0)),
            "the first key ever inserted must not survive 100k ticks of churn"
        );
        let floor = ticks as i64 - 2 * cap as i64;
        let oldest = state
            .slots
            .values()
            .map(|slot| slot.last_used_ns)
            .min()
            .expect("non-empty");
        assert!(
            oldest >= floor,
            "a resident entry is far older than the eviction order allows: \
             last_used_ns {oldest} < floor {floor}"
        );
        // The FIFO tracks the map, so it cannot grow unboundedly either.
        assert!(
            state.order.len() <= cap + MAX_EVICTIONS_PER_CALL,
            "insertion-order FIFO drifted from the map: {} entries",
            state.order.len()
        );
    }

    #[test]
    fn per_tick_cost_is_independent_of_max_entries() {
        // The bound the predecessor failed: its per-call cost grew with the
        // hollowed-out bucket prefix, measured at ~3,700-6,800ns per `tick` at
        // cap=100,000 versus a few tens of ns at cap=100. FIFO eviction pops one
        // deque entry and removes one map entry regardless of cap.
        //
        // Both caps are churned to several multiples of capacity first, then a
        // batch of single calls is timed. Best-of-3 trials filters scheduler
        // noise. The threshold is deliberately loose (3x plus a 200ns additive
        // floor, so a sub-100ns baseline cannot make ordinary jitter fail the
        // test) because the point is to catch an O(n) blowup - three orders of
        // magnitude in the review's numbers - not to police cache-miss-scale
        // differences between a 100-entry and a 100,000-entry table.
        let trials = 3;
        let small = (0..trials)
            .map(|_| steady_state_ns_per_tick(100, 10_000, 5_000))
            .fold(f64::INFINITY, f64::min);
        let big = (0..trials)
            .map(|_| steady_state_ns_per_tick(100_000, 300_000, 5_000))
            .fold(f64::INFINITY, f64::min);
        assert!(
            big <= small * 3.0 + 200.0,
            "per-tick cost scales with max_entries: {small:.0}ns at cap=100 vs \
             {big:.0}ns at cap=100_000"
        );
    }

    #[test]
    fn a_hot_key_is_still_evicted_under_sustained_churn() {
        // FIFO evicts by insertion order, not by use recency, so a hot key IS
        // evicted once `cap` newer keys have churned past it - and its rotation
        // restarts at offset 0. That is the accepted trade documented on
        // `RoundRobin`: eviction is never correctness-bearing. The predecessor's
        // bucket-biased sampling never evicted an always-active early key at all
        // (the review measured 0 restarts over 8 x 50,000 requests), so it was
        // not approximating any eviction policy.
        let cap = 64usize;
        let rounds = 10_000u64;
        let rr = RoundRobin::new(cap, i64::MAX);
        let hot = [0xABu8; 32];
        let mut restarts = 0usize;
        for i in 0..rounds {
            // The hot tenant keeps requesting throughout.
            if rr.tick(hot, i as i64) == 0 && i > 0 {
                restarts += 1;
            }
            // A churning client rotates its token every request.
            rr.tick(key_of(i), i as i64);
        }
        // Roughly one restart per `cap` new keys inserted; assert only that the
        // hot key is not starved from eviction forever.
        assert!(
            restarts > 0,
            "a hot key was never evicted under {rounds} rounds of churn at cap {cap}"
        );
    }

    #[test]
    fn over_cap_tick_evicts_only_a_bounded_batch() {
        // A single `tick` must evict only a bounded batch, not drain the whole
        // overflow back to the cap in one O(n) pass.
        let cap = 10;
        let rr = RoundRobin::new(cap, i64::MAX);
        force_over_cap(&rr, 100);
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
        // The oldest-inserted keys are the ones that went.
        let state = rr.state.lock().expect("lock");
        assert!(
            !state.slots.contains_key(&key_of(0)),
            "FIFO must evict from the front (oldest insert) first"
        );
        assert!(
            state.slots.contains_key(&key_of(99)),
            "FIFO must not evict the newest insert"
        );
    }

    #[test]
    fn stale_fifo_entries_are_discarded_within_a_bounded_budget() {
        // The background idle sweep can remove keys from the map, leaving their
        // FIFO entries stale. `make_room` discards stale front entries on pop,
        // but only up to MAX_STALE_DISCARDS_PER_CALL of them, so one `tick`
        // cannot be made to walk an unbounded run of them.
        let cap = 16usize;
        let rr = RoundRobin::new(cap, i64::MAX);
        {
            let mut state = rr.state.lock().expect("lock");
            // A long run of stale front entries: in the FIFO, absent from the map.
            for i in 0..10_000u64 {
                state.order.push_back(key_of(i));
            }
            // Then `cap` live entries behind them.
            for i in 10_000..(10_000 + cap as u64) {
                state.slots.insert(
                    key_of(i),
                    Slot {
                        counter: 0,
                        last_used_ns: i as i64,
                    },
                );
                state.order.push_back(key_of(i));
            }
        }

        rr.tick(key_of(999_999), 999_999);

        let state = rr.state.lock().expect("lock");
        // Exactly the stale budget was discarded, and the call gave up rather
        // than walking to the live entries, so nothing live was evicted.
        assert_eq!(
            state.order.len(),
            10_000 + cap + 1 - MAX_STALE_DISCARDS_PER_CALL,
            "stale discards were not bounded by MAX_STALE_DISCARDS_PER_CALL"
        );
        assert_eq!(
            state.slots.len(),
            cap + 1,
            "map may sit transiently over the cap when the stale budget is spent"
        );
    }

    #[test]
    fn idle_sweep_prunes_the_insertion_order_fifo() {
        // Below the cap `make_room` never runs, so nothing would ever discard
        // stale FIFO entries on pop. The sweep prunes them instead, off the
        // request path, so repeated sweep-and-reinsert churn cannot grow the
        // FIFO without bound.
        let ttl = 100;
        let rr = RoundRobin::new(100_000, ttl);
        for round in 0..50i64 {
            let base = round * 1_000;
            for k in 0..10u64 {
                rr.tick(key_of(k), base);
            }
            // Every key is now idle past the TTL and swept away.
            assert_eq!(rr.evict_idle(base + ttl), 10);
            assert_eq!(rr.len(), 0);
        }
        let state = rr.state.lock().expect("lock");
        assert!(
            state.order.is_empty(),
            "FIFO grew across sweep-and-reinsert rounds: {} entries",
            state.order.len()
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
        force_over_cap(&rr, 500);
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

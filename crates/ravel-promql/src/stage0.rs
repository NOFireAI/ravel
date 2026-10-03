//! Stage 0 measurement scaffolding for issue #2443 (epic #2425). Never
//! merged to `main`: a measurement branch read directly by the orchestrator.
//!
//! `MODE` gates all of it: 0 is the unmodified evaluator, byte-for-byte; 1
//! adds hit/miss counting on the real index probes (a cheap branch plus an
//! atomic increment on an existing match arm) and, once the real pass
//! finishes, a replay of the same key-build/index-operation sequence against
//! a fresh map of the same type, timed with [`std::time::Instant`]. The
//! replay is additional work layered on top of the real pass, not a
//! substitute for it: mode 1's own measured operator time is the real work
//! plus the replay, which is what lets `stage0_maps.rs` recover an estimate
//! of the real (non-replayed) operator time as
//! `mode_1_time - replayed_index_time`.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

/// 0 = unmodified code. 1 = unmodified code plus measurement.
#[doc(hidden)]
pub static MODE: AtomicU8 = AtomicU8::new(0);

#[doc(hidden)]
pub fn set_mode(m: u8) {
    MODE.store(m, Ordering::SeqCst);
}

#[doc(hidden)]
pub fn mode() -> u8 {
    MODE.load(Ordering::Relaxed)
}

// --- aggregate::group_by (the grouping index) ---

#[doc(hidden)]
pub static AGG_INDEX_NS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static AGG_KEY_BUILD_NS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static AGG_PROBE_HITS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static AGG_PROBE_MISSES: AtomicU64 = AtomicU64::new(0);

// --- binop::one_to_one (vector matching): three map/set touches ---
// (rhs_map build, lhs-probes-rhs_map, matched_sigs dedup insert), key build
// timed once per touch (matching_signature is called once per rhs sample and
// once per lhs sample; the lhs-computed key is reused for both the probe and
// the dedup insert, exactly as the real code reuses it).

#[doc(hidden)]
pub static MATCH_KEY_BUILD_NS: AtomicU64 = AtomicU64::new(0);

#[doc(hidden)]
pub static MATCH_RHS_BUILD_NS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static MATCH_RHS_BUILD_HITS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static MATCH_RHS_BUILD_MISSES: AtomicU64 = AtomicU64::new(0);

/// The probing side: lhs looking up its signature in `rhs_map`. This is the
/// counter the MATCH_HALF pre-registered assertion is about.
#[doc(hidden)]
pub static MATCH_PROBE_NS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static MATCH_PROBE_HITS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static MATCH_PROBE_MISSES: AtomicU64 = AtomicU64::new(0);

#[doc(hidden)]
pub static MATCH_DEDUP_NS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static MATCH_DEDUP_HITS: AtomicU64 = AtomicU64::new(0);
#[doc(hidden)]
pub static MATCH_DEDUP_MISSES: AtomicU64 = AtomicU64::new(0);

#[doc(hidden)]
pub fn reset() {
    for c in [
        &AGG_INDEX_NS,
        &AGG_KEY_BUILD_NS,
        &AGG_PROBE_HITS,
        &AGG_PROBE_MISSES,
        &MATCH_KEY_BUILD_NS,
        &MATCH_RHS_BUILD_NS,
        &MATCH_RHS_BUILD_HITS,
        &MATCH_RHS_BUILD_MISSES,
        &MATCH_PROBE_NS,
        &MATCH_PROBE_HITS,
        &MATCH_PROBE_MISSES,
        &MATCH_DEDUP_NS,
        &MATCH_DEDUP_HITS,
        &MATCH_DEDUP_MISSES,
    ] {
        c.store(0, Ordering::SeqCst);
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Snapshot {
    pub agg_index_ns: u64,
    pub agg_key_build_ns: u64,
    pub agg_hits: u64,
    pub agg_misses: u64,
    pub match_key_build_ns: u64,
    pub match_rhs_build_ns: u64,
    pub match_rhs_build_hits: u64,
    pub match_rhs_build_misses: u64,
    pub match_probe_ns: u64,
    pub match_probe_hits: u64,
    pub match_probe_misses: u64,
    pub match_dedup_ns: u64,
    pub match_dedup_hits: u64,
    pub match_dedup_misses: u64,
}

impl Snapshot {
    /// Total replayed index time across all three `one_to_one` map/set
    /// touches (build + probe + dedup), excluding key-build time.
    pub fn match_index_ns(&self) -> u64 {
        self.match_rhs_build_ns + self.match_probe_ns + self.match_dedup_ns
    }
}

#[doc(hidden)]
pub fn snapshot() -> Snapshot {
    Snapshot {
        agg_index_ns: AGG_INDEX_NS.load(Ordering::SeqCst),
        agg_key_build_ns: AGG_KEY_BUILD_NS.load(Ordering::SeqCst),
        agg_hits: AGG_PROBE_HITS.load(Ordering::SeqCst),
        agg_misses: AGG_PROBE_MISSES.load(Ordering::SeqCst),
        match_key_build_ns: MATCH_KEY_BUILD_NS.load(Ordering::SeqCst),
        match_rhs_build_ns: MATCH_RHS_BUILD_NS.load(Ordering::SeqCst),
        match_rhs_build_hits: MATCH_RHS_BUILD_HITS.load(Ordering::SeqCst),
        match_rhs_build_misses: MATCH_RHS_BUILD_MISSES.load(Ordering::SeqCst),
        match_probe_ns: MATCH_PROBE_NS.load(Ordering::SeqCst),
        match_probe_hits: MATCH_PROBE_HITS.load(Ordering::SeqCst),
        match_probe_misses: MATCH_PROBE_MISSES.load(Ordering::SeqCst),
        match_dedup_ns: MATCH_DEDUP_NS.load(Ordering::SeqCst),
        match_dedup_hits: MATCH_DEDUP_HITS.load(Ordering::SeqCst),
        match_dedup_misses: MATCH_DEDUP_MISSES.load(Ordering::SeqCst),
    }
}

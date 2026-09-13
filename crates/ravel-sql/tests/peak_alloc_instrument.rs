//! High-water-mark instrument for issue #1582's re-measurement round.
//!
//! `stats_alloc` (`tests/dedup_finalize_allocation*.rs`) reports only
//! cumulative allocation churn (total bytes ever requested, never
//! decremented on free); it has no notion of peak resident bytes. Getting
//! peak requires tracking live bytes per alloc/dealloc/realloc call and
//! recording a high-water mark, which needs a `GlobalAlloc` impl of its own.
//! The workspace forbids `unsafe_code` everywhere, including tests
//! (`unsafe_code = "forbid"` in the workspace `[lints]`), so this crate
//! cannot write that impl itself; `peak_alloc::PeakAlloc` is the dependency
//! that owns the `unsafe impl GlobalAlloc`, the same division of labor
//! `stats_alloc` is used for elsewhere in this crate (see its Cargo.toml
//! comment).
//!
//! One test per binary: `PeakAlloc`'s counters are one pair of
//! process-global atomics (`CURRENT`/`PEAK`), so a second test running
//! concurrently in this binary would land its own allocations in the same
//! counters. Same constraint `tests/dedup_finalize_allocation.rs` documents
//! for `stats_alloc`.

use peak_alloc::PeakAlloc;

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

/// A known allocation pattern exercising alloc, dealloc, a growing realloc,
/// and a shrinking realloc, each against a hand-computed exact byte delta so
/// the wrapper's own bookkeeping is checked rather than assumed. Every
/// `Vec<u8>` capacity below becomes an exact `Layout` size request to the
/// global allocator (no padding: `u8` has alignment 1, and `Global`'s stable
/// alloc/realloc path never reports back a larger usable size than
/// requested), so every delta asserted here is exact, not a bound.
///
/// `peak_alloc::PeakAlloc::realloc` does not resize in place: it allocates
/// the new layout, copies, then frees the old block, so old and new are both
/// live for the space of that call. A growing realloc's *live* total settles
/// to the new size, but its transient *peak* is the pre-realloc live total
/// plus the full new size (old and new momentarily coexist), not just the
/// new size on its own -- confirmed against this exact pattern below rather
/// than assumed, since it is the one place this instrument could plausibly
/// double-count.
///
///   1. alloc   A: 100 bytes             live  100   peak  100
///   2. alloc   B: 300 bytes             live  400   peak  400
///   3. dealloc A: -100 bytes            live  300   peak  400  (below the mark, must not move it)
///   4. alloc   C: 50 bytes              live  350   peak  400
///   5. grow    B: 300 -> 1000            live 1050   peak 1350  (peak = pre-grow live 350 + new size 1000, old and new momentarily both counted live)
///   6. shrink  B: 1000 -> 200            live  250   peak 1350  (shrink's own transient, pre-shrink live 1050 + new size 200 = 1250, stays below the grow's peak)
///
/// Final live is 250 (50 from C + 200 from shrunk B); final peak is 1350
/// (step 5's transient high point, never reached again).
#[test]
fn high_water_mark_matches_a_known_allocation_pattern() {
    let baseline_current = PEAK_ALLOC.current_usage();
    PEAK_ALLOC.reset_peak_usage();

    let a: Vec<u8> = Vec::with_capacity(100);
    let mut b: Vec<u8> = Vec::with_capacity(300);
    assert_eq!(
        PEAK_ALLOC.current_usage() - baseline_current,
        400,
        "two fresh allocations of 100 and 300 bytes must add up exactly"
    );
    assert_eq!(
        PEAK_ALLOC.peak_usage() - baseline_current,
        400,
        "the high-water mark after only growth must equal the current total"
    );

    drop(a);
    assert_eq!(
        PEAK_ALLOC.current_usage() - baseline_current,
        300,
        "dropping A must free exactly its own 100 bytes, not more or less"
    );
    assert_eq!(
        PEAK_ALLOC.peak_usage() - baseline_current,
        400,
        "a deallocation below the high-water mark must not move it"
    );

    let c: Vec<u8> = Vec::with_capacity(50);
    assert_eq!(PEAK_ALLOC.current_usage() - baseline_current, 350);

    // `reserve_exact(additional)` reserves for `len() + additional`; `b` is
    // still empty (`len() == 0`), so the additional amount IS the target
    // capacity, not the difference from the current capacity.
    b.reserve_exact(1000);
    assert_eq!(
        b.capacity(),
        1000,
        "test assumption: reserve_exact grows to exactly the requested capacity"
    );
    assert_eq!(
        PEAK_ALLOC.current_usage() - baseline_current,
        1050,
        "growing B by +700 (300 -> 1000) must add exactly the 700-byte \
         difference, not double-count the already-live 300 bytes on top of \
         a fresh 1000-byte allocation"
    );
    assert_eq!(
        PEAK_ALLOC.peak_usage() - baseline_current,
        1350,
        "PeakAlloc::realloc allocates the new block before freeing the old \
         one, so a grow's transient peak is pre-grow live (350) + the new \
         size (1000) = 1350, not just the settled post-grow total of 1050"
    );

    b.shrink_to(200);
    assert_eq!(
        b.capacity(),
        200,
        "test assumption: shrink_to shrinks to exactly the requested capacity"
    );
    assert_eq!(
        PEAK_ALLOC.current_usage() - baseline_current,
        250,
        "shrinking B by -800 (1000 -> 200) must subtract exactly the \
         800-byte difference, not underflow past the true remaining total"
    );
    assert_eq!(
        PEAK_ALLOC.peak_usage() - baseline_current,
        1350,
        "the shrink's own transient (pre-shrink live 1050 + new size 200 = \
         1250) stays below the grow's 1350 and must not raise the mark"
    );

    drop(c);
    drop(b);
}

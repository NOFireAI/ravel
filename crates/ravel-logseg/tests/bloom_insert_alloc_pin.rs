//! Allocation pin for the writer's bloom text insert (issue #3499): tokens are
//! yielded from a stack buffer by `tokenizer::for_each_token`, not collected
//! into one `Vec` per token, so inserting text whose tokens the bloom already
//! staged allocates nothing at all.
//!
//! This file contains EXACTLY ONE test on purpose: the instrumented global
//! allocator counts every allocation in the process, so a second test running
//! concurrently in the same binary would pollute the count, as in
//! `stamp_alloc_pin.rs`.
//!
//! The test harness's own thread can still allocate while the counter runs,
//! and `unsafe_code` is forbidden, so the count cannot be limited to the
//! measuring thread. The loop is measured in [`WINDOWS`] separate windows and
//! the quietest one is asserted: a per-token allocation shows in every window,
//! a stray harness allocation in at most a few.

#![allow(clippy::expect_used)]

use std::alloc::System;

use ravel_logseg::bloom::BloomBuilder;
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::writer::bloom_probe::insert_text;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Bodies of the ingest shape's kind: ASCII words and ids, one non-ASCII value
/// (the Unicode tokenizer path), and one past the exact-insert cap.
const TEXTS: [&str; 5] = [
    "GET /api/v1/orders/48213 completed in 12ms status=200 region=us-east-1",
    "connection reset by peer while reading response header from upstream",
    "user 7f3a9c2e retried checkout after timeout",
    "Größe überschritten: İstanbul café naïve",
    "short",
];
const COLUMN: u32 = 4;
/// Passes over [`TEXTS`] under the counter, per window.
const MEASURED: usize = 1_000;
/// Separately counted windows of [`MEASURED`] passes each.
const WINDOWS: usize = 5;

/// Each window's counts, in order, from running `measured` once per window.
fn windows(mut measured: impl FnMut()) -> Vec<Stats> {
    let mut counts = Vec::with_capacity(WINDOWS);
    for _ in 0..WINDOWS {
        let region = Region::new(GLOBAL);
        measured();
        counts.push(region.change());
    }
    counts
}

/// The window with the fewest allocations and reallocations.
fn quietest(counts: &[Stats]) -> Stats {
    counts
        .iter()
        .copied()
        .min_by_key(|stats| stats.allocations + stats.reallocations)
        .expect("at least one window")
}

#[test]
fn bloom_text_insert_allocates_nothing_per_token() {
    let token_count: usize = TEXTS.iter().map(|t| tokens(t).len()).sum();
    assert_eq!(token_count, 36, "the shape inserts a fixed token set");

    // Stage every key first: the bloom's own staging of a new key is a
    // one-off allocation per distinct key, not a per-token one.
    let mut builder = BloomBuilder::new(7);
    for text in TEXTS {
        insert_text(&mut builder, COLUMN, text.as_bytes());
    }

    let counts = windows(|| {
        for _ in 0..MEASURED {
            for text in TEXTS {
                insert_text(&mut builder, COLUMN, text.as_bytes());
            }
        }
    });
    let stats = quietest(&counts);

    assert_eq!(
        stats.allocations,
        0,
        "inserting {} tokens allocated {} times ({} bytes) in the quietest window; \
         allocations per window: {:?}",
        MEASURED * token_count,
        stats.allocations,
        stats.bytes_allocated,
        counts.iter().map(|s| s.allocations).collect::<Vec<_>>()
    );
    assert_eq!(stats.reallocations, 0);

    // The insert still staged what it always did: the filter is a pure
    // function of the distinct keys.
    let mut want = BloomBuilder::new(7);
    for text in TEXTS {
        for tok in tokens(text) {
            want.insert(COLUMN, &tok);
        }
        if text.len() <= 64 {
            want.insert(COLUMN, text.as_bytes());
        }
    }
    assert_eq!(builder.finish_exact(), want.finish_exact());
}

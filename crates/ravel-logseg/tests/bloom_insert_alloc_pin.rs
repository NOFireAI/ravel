//! Allocation pin for the writer's bloom text insert (issue #3499): tokens are
//! yielded from a stack buffer by `tokenizer::for_each_token`, not collected
//! into one `Vec` per token, so inserting text whose tokens the bloom already
//! staged allocates nothing at all.
//!
//! This file contains EXACTLY ONE test on purpose: the instrumented global
//! allocator counts every allocation in the process, so a second test running
//! concurrently in the same binary would pollute the count, as in
//! `stamp_alloc_pin.rs`.

#![allow(clippy::expect_used)]

use std::alloc::System;

use ravel_logseg::bloom::BloomBuilder;
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::writer::bloom_probe::insert_text;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

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
/// Passes over [`TEXTS`] under the counter.
const MEASURED: usize = 1_000;

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

    let region = Region::new(GLOBAL);
    for _ in 0..MEASURED {
        for text in TEXTS {
            insert_text(&mut builder, COLUMN, text.as_bytes());
        }
    }
    let stats = region.change();

    assert_eq!(
        stats.allocations,
        0,
        "inserting {} tokens allocated {} times ({} bytes)",
        MEASURED * token_count,
        stats.allocations,
        stats.bytes_allocated
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

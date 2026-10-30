//! Allocation pin for the word tokenizer (ADR-3492 D4): tokenizing text
//! through `for_each_token` allocates nothing, on the ASCII path and on the
//! Unicode path, and re-inserting tokens the bloom builder has already staged
//! allocates nothing.
//!
//! This file contains EXACTLY ONE test on purpose: it installs the instrumented
//! global allocator, which counts every allocation in the process, so a second
//! test running concurrently in the same binary would pollute the count. The
//! workspace forbids `unsafe_code`, so `stats_alloc`'s instrumented allocator
//! is used instead of a hand-rolled one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use ravel_codec::bloom::BloomBuilder;
use ravel_codec::tokenizer::{for_each_token, tokens};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Lines tokenized under the counter.
const MEASURED: usize = 1_000;

fn line(i: usize) -> String {
    format!(
        "2026-10-30T12:{:02}:{:02}Z INFO GET /api/v{}/Users/{} status={} latency_ms={} \
         trace_id=4bf92f3577b34da6a3ce929d0e0e{:04} {}",
        i % 60,
        i * 7 % 60,
        i % 3,
        i % 17,
        [200, 404, 500][i % 3],
        i % 23,
        i % 13,
        "x".repeat(70 + i % 5),
    )
}

#[test]
fn tokens_allocate_nothing_per_token() {
    let lines: Vec<String> = (0..MEASURED).map(line).collect();
    assert!(lines.iter().all(|l| l.is_ascii()));

    // The sink's figures, computed through the allocating `tokens` outside
    // the counted region.
    let want_tokens: usize = lines.iter().map(|l| tokens(l).len()).sum();
    let want_bytes: usize = lines.iter().flat_map(|l| tokens(l)).map(|t| t.len()).sum();
    assert_eq!(want_tokens, MEASURED * 20);

    // Warm-up: the builder stages every distinct key and grows its scratch
    // buffer before the counter starts.
    let mut builder = BloomBuilder::new(7);
    for l in &lines {
        for_each_token(l, |t| builder.insert(0, t));
    }

    let mut got_tokens = 0usize;
    let mut got_bytes = 0usize;
    let region = Region::new(GLOBAL);
    for l in &lines {
        for_each_token(l, |t| {
            got_tokens += 1;
            got_bytes += t.len();
        });
    }
    let tokenize = region.change();

    // The Unicode path: the same lines with a word that needs full case
    // mapping, a multi-byte lowercase and a non-ASCII separator.
    let unicode: Vec<String> = lines
        .iter()
        .map(|l| format!("{l} İstanbul\u{3000}ΑΒΓ"))
        .collect();
    let want_unicode: usize = unicode.iter().map(|l| tokens(l).len()).sum();
    assert_eq!(want_unicode, MEASURED * 22);
    let mut got_unicode = 0usize;
    let region = Region::new(GLOBAL);
    for l in &unicode {
        for_each_token(l, |_| got_unicode += 1);
    }
    let tokenize_unicode = region.change();

    let region = Region::new(GLOBAL);
    for l in &lines {
        for_each_token(l, |t| builder.insert(0, t));
    }
    let reinsert = region.change();

    assert_eq!((got_tokens, got_bytes), (want_tokens, want_bytes));
    assert_eq!(
        (tokenize.allocations, tokenize.reallocations),
        (0, 0),
        "tokenizing {got_tokens} ASCII tokens allocated: {tokenize:?}"
    );
    assert_eq!(got_unicode, want_unicode);
    assert_eq!(
        (tokenize_unicode.allocations, tokenize_unicode.reallocations),
        (0, 0),
        "tokenizing {got_unicode} tokens of non-ASCII text allocated: {tokenize_unicode:?}"
    );
    assert_eq!(
        (reinsert.allocations, reinsert.reallocations),
        (0, 0),
        "re-inserting {got_tokens} staged tokens allocated: {reinsert:?}"
    );
    assert!(!builder.finish().is_empty());
}

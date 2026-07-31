#![no_main]

//! Coverage-guided fuzz target for the RSEG v5 sparse point-lookup grammar
//! (issue #48).
//!
//! The v5 analog of a filtered/point catalog read is the sparse probe: parse
//! the SERIES_IDX directory, then binary-search a SERIES_IDS window for a
//! target 16-byte series id. `parse_series_idx` and `find_index_in_window`
//! each take only byte slices (no `Footer`), so this target exercises the
//! whole probe grammar directly on unstructured input. Every offset/length in
//! SERIES_IDX and every window bound is untrusted and must yield a typed
//! `SegmentError`, never a panic or out-of-bounds read.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use ravel_segment::{find_index_in_window, parse_series_idx};

#[derive(Arbitrary, Debug)]
struct Input {
    series_idx: Vec<u8>,
    window: Vec<u8>,
    first_index: u64,
    target: [u8; 16],
}

fuzz_target!(|input: Input| {
    // SERIES_IDX directory parse: bounds-check the chunk/window directory.
    let _ = parse_series_idx(&input.series_idx);

    // In-window binary search: fuzz the window walk independently of a valid
    // directory, since its bounds logic must stand alone against garbage.
    let _ = find_index_in_window(&input.window, input.first_index, &input.target);
});

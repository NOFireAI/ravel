#![no_main]

//! Coverage-guided fuzz target for the RSPAN decode path (issue #352).
//!
//! ADR-0041 and docs/span-segment-format.md mandate that every RSPAN decode
//! path treat stored lengths, offsets, and section descriptors as untrusted and
//! reject malformed input with a typed [`ravel_rspan::SpanSegError`], never a
//! panic or out-of-bounds access. Per CLAUDE.md's corrupt-input testing
//! pattern, feeding arbitrary bytes to the reader entry points must never
//! panic. This target asserts exactly that by driving the whole-object reader,
//! the pruned scan, and the footer/suffix parsers with unstructured input.
//!
//! It is the unbounded, coverage-guided complement to the crate's bounded
//! `decode_never_panics`-style proptests: the proptests replay a fixed catalog
//! of small mutations, while libFuzzer explores the input space on its own
//! under coverage feedback.

use libfuzzer_sys::fuzz_target;
use ravel_rspan::{RspanConfig, RspanReader, SpanQuery, open, open_from_suffix};

fuzz_target!(|data: &[u8]| {
    let cfg = RspanConfig::default();

    // Whole-object open + scan: `RspanReader::new` validates the trailer,
    // footer crc, section table, and decodes the skip index; `scan` then reads,
    // checksum-verifies, and re-evaluates every surviving block. Both a bare
    // time-range query and a trace-id lookup drive the pruning and block-decode
    // paths. Any rejection must be `Err`, never a panic.
    if let Ok(reader) = RspanReader::new(data, &cfg) {
        let _ = reader.scan(&SpanQuery::ts_range(i64::MIN, i64::MAX));
        let _ = reader.scan(&SpanQuery::trace([0u8; 16], i64::MIN, i64::MAX));
    }

    // Footer + section table parse in isolation over the whole object.
    let _ = open(data);

    // Suffix reader protocol: split the input into a declared total object size
    // and the tail bytes so the fuzzer can reach both the `NeedRange` path
    // (declared size larger than the supplied suffix) and the `Ready` path (the
    // suffix covers the whole footer).
    let (total_size, suffix) = if data.len() >= 8 {
        let mut size_bytes = [0u8; 8];
        size_bytes.copy_from_slice(&data[..8]);
        (u64::from_le_bytes(size_bytes), &data[8..])
    } else {
        (data.len() as u64, data)
    };
    let _ = open_from_suffix(suffix, total_size);

    // A self-consistent object where the declared size equals the suffix length,
    // covering the full-suffix `Ready` branch more often.
    let _ = open_from_suffix(suffix, suffix.len() as u64);
});

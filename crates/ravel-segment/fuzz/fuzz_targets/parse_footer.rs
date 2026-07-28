#![no_main]

//! Coverage-guided fuzz target for RSEG footer/section parsing (issue #48).
//!
//! ADR-0004 mandates that parsers treat all stored lengths and offsets as
//! untrusted and that fuzz targets exercise them. Per CLAUDE.md's corrupt-input
//! testing pattern, feeding arbitrary bytes to the reader entry points must
//! never panic: every rejection is a typed `SegmentError`. This target asserts
//! exactly that by driving the trailer/footer reader and the section validator
//! with unstructured input.

use libfuzzer_sys::fuzz_target;
use ravel_segment::{ReaderLimits, open_from_suffix, parse_footer};

fuzz_target!(|data: &[u8]| {
    // Split the input into a declared object size and the tail bytes so the
    // fuzzer can reach both the `NeedRange` path (declared size larger than the
    // supplied tail) and the `Ready` path (tail contains the whole footer).
    let (total_size, tail) = if data.len() >= 8 {
        let mut size_bytes = [0u8; 8];
        size_bytes.copy_from_slice(&data[..8]);
        (u64::from_le_bytes(size_bytes), &data[8..])
    } else {
        (data.len() as u64, data)
    };

    // Trailer + footer parse in isolation: must return Ok/Err, never panic.
    let _ = parse_footer(total_size, tail);

    // Also exercise the section reader: on a `Ready` footer this chains into
    // `validate_sections`, so bounds-checking of section descriptors is fuzzed
    // through the same untrusted bytes.
    let _ = open_from_suffix(tail, total_size, ReaderLimits::default());

    // A self-consistent object where the declared size equals the byte length,
    // covering the full-suffix `Ready` branch more often.
    let _ = open_from_suffix(tail, tail.len() as u64, ReaderLimits::default());
});

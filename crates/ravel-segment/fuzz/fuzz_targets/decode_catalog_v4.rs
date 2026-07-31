#![no_main]

//! Coverage-guided fuzz target for the RSEG v4 (run-major) whole-catalog
//! decoder (issue #48).
//!
//! `decode_catalog_v4` is handed an untrusted `Footer` plus the three raw
//! section bodies (LABEL_DICT, SERIES_IDS, SERIES_META) that its section
//! descriptors point at. Per ADR-0004 and CLAUDE.md's corrupt-input pattern
//! every stored offset/length/count is untrusted: any combination of a
//! malformed footer and arbitrary section bytes must resolve to a typed
//! `SegmentError`, never a panic, out-of-bounds read, or wrong decode.
//!
//! The `Footer` is itself decoded from fuzzer bytes with `prost`, so the
//! fuzzer explores both malformed protobuf and (as the corpus evolves)
//! footers carrying section descriptors that then drive the bounds-checking
//! logic against independent, fuzzer-chosen section bodies.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use prost::Message;
use ravel_segment::{Footer, ReaderLimits, decode_catalog_v4};

#[derive(Arbitrary, Debug)]
struct Input {
    footer: Vec<u8>,
    label_dict: Vec<u8>,
    series_ids: Vec<u8>,
    series_meta: Vec<u8>,
}

fuzz_target!(|input: Input| {
    let Ok(footer) = Footer::decode(input.footer.as_slice()) else {
        return;
    };
    let _ = decode_catalog_v4(
        &footer,
        &input.label_dict,
        &input.series_ids,
        &input.series_meta,
        ReaderLimits::default(),
    );
});

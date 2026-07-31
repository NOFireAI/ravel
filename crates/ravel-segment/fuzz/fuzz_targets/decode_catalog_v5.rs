#![no_main]

//! Coverage-guided fuzz target for the RSEG v5 (sparse) whole-catalog decoder
//! (issue #48).
//!
//! Unlike the v4 decoders, `decode_catalog_v5` takes the whole object bytes,
//! because a v5 chunked catalog spans several sections it locates itself
//! (SERIES_IDX's chunk directory, SERIES_META_CHUNKS frames). Every offset it
//! follows comes from the untrusted footer/index, so arbitrary footer and
//! object bytes must resolve to a typed `SegmentError`, never a panic or
//! out-of-bounds read. Below the sparse threshold this path delegates to the
//! v4 core, so the fuzzer also reaches that fallback here.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use prost::Message;
use ravel_segment::{Footer, ReaderLimits, decode_catalog_v5};

#[derive(Arbitrary, Debug)]
struct Input {
    footer: Vec<u8>,
    object: Vec<u8>,
}

fuzz_target!(|input: Input| {
    let Ok(footer) = Footer::decode(input.footer.as_slice()) else {
        return;
    };
    let _ = decode_catalog_v5(&footer, &input.object, ReaderLimits::default());
});

#![no_main]

//! Coverage-guided fuzz target for the RSEG v4 filtered catalog decoder.
//!
//! `decode_catalog_matching_v4` is `decode_catalog_v4` plus an equality
//! predicate list applied against the label dictionary while decoding. It has
//! its own label-resolution and matching path on top of the shared section
//! bounds-checking, so it is fuzzed separately with fuzzer-chosen `(name,
//! value)` filters. As with the unfiltered decoder, arbitrary footer and
//! section bytes must only ever yield `Ok`/typed `SegmentError`, never a panic.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use prost::Message;
use ravel_segment::{Footer, ReaderLimits, decode_catalog_matching_v4};

#[derive(Arbitrary, Debug)]
struct Input {
    footer: Vec<u8>,
    label_dict: Vec<u8>,
    series_ids: Vec<u8>,
    series_meta: Vec<u8>,
    equals: Vec<(String, String)>,
}

fuzz_target!(|input: Input| {
    let Ok(footer) = Footer::decode(input.footer.as_slice()) else {
        return;
    };
    let equals: Vec<(&str, &str)> = input
        .equals
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let _ = decode_catalog_matching_v4(
        &footer,
        &input.label_dict,
        &input.series_ids,
        &input.series_meta,
        &equals,
        ReaderLimits::default(),
    );
});

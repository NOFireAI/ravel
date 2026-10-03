#![no_main]

//! Coverage-guided fuzz target for the RSPAN decode path.
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
use ravel_rspan::skip_index::SkipIndex;
use ravel_rspan::{
    BloomPredicate, BloomSection, COL_NAME, COL_SERVICE_NAME, RspanConfig, RspanReader, SpanQuery,
    open, open_from_suffix,
};

fuzz_target!(|data: &[u8]| {
    let cfg = RspanConfig::default();

    // Direct SKIP_IDX decode: the whole-object path above only reaches
    // BlockEntry's v2 duration/status_mask fields (ADR-0045) when a section
    // table happens to parse far enough to hand the section bytes to
    // `SkipIndex::decode`, which is rare with unstructured input. Decoding
    // fuzz bytes as a raw skip-index section directly exercises the new
    // fields' truncation and reserved-status-bit rejection paths without
    // needing a well-formed trailer/footer/section table around them.
    let _ = SkipIndex::decode(data, 1 << 16);

    // Direct BLOOM container parse (v3, ADR-0054): feed fuzz bytes straight to
    // the shared bloom section framing so its truncation, overlong-entry, and
    // trailing-bytes rejection paths are reached without needing a well-formed
    // object around them. A parsed container is then probed to drive the
    // per-entry crc and BloomView paths.
    if let Ok(section) = BloomSection::parse(data) {
        for i in 0..section.len() {
            if let Ok(view) = section.entry(i) {
                let _ = view.may_contain(COL_SERVICE_NAME, b"probe");
                let _ = view.may_contain(COL_NAME, b"probe");
            }
        }
    }

    // Whole-object open + scan: `RspanReader::new` validates the trailer,
    // footer crc, section table (BLOOM now mandatory: a missing BLOOM is a
    // typed error here, not a panic), and decodes the skip index; `scan` then
    // reads, checksum-verifies, and re-evaluates every surviving block. Both a
    // bare time-range query and a trace-id lookup drive the pruning and
    // block-decode paths. Any rejection must be `Err`, never a panic.
    if let Ok(reader) = RspanReader::new(data, &cfg) {
        let _ = reader.scan(&SpanQuery::ts_range(i64::MIN, i64::MAX));
        let _ = reader.scan(&SpanQuery::trace([0u8; 16], i64::MIN, i64::MAX));

        // Bloom-backed pruning over the opened object: parse the BLOOM section
        // and prune with a service_name and a name predicate. Corrupt or
        // count-mismatched blooms must return `Err`, never panic.
        if let Ok(bloom) = reader.bloom() {
            let preds = [
                BloomPredicate {
                    field_id: COL_SERVICE_NAME,
                    literal: "checkout",
                },
                BloomPredicate {
                    field_id: COL_NAME,
                    literal: "get",
                },
            ];
            let _ = reader.skip_index().candidate_blocks_with_bloom(
                None,
                i64::MIN,
                i64::MAX,
                None,
                None,
                &bloom,
                &preds,
            );
        }
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

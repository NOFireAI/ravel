//! Allocation regression test for the SoA decode path
//! (docs/arrow-datafusion-plan.md ticket A1a). A global counting allocator
//! would need an `unsafe impl GlobalAlloc`, which this workspace denies
//! (`unsafe` is forbidden workspace-wide, repo CLAUDE.md); instead this
//! proves zero-reallocation the observable way: `Vec::capacity` only grows
//! on reallocation and never shrinks on `clear`, so a `decode_pages_soa`
//! call that leaves `scratch`/`timestamps`/`values` capacity unchanged
//! after a warm-up pass made zero heap allocations for that call. The AoS
//! `decode_pages` path has no such caller-supplied buffers to warm up: by
//! construction (see `decode_pages`'s delegation to `decode_pages_soa` with
//! fresh `Vec`s) it allocates scratch, timestamps, values, and the merged
//! `Sample` vec fresh on every single call.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntry, SeriesInput,
    decode_catalog, decode_pages_soa, open_from_full, plan_ranges,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

const SERIES_COUNT: usize = 50;
const SAMPLES_PER_SERIES: usize = 30;

fn build_test_segment() -> bytes::Bytes {
    let tenant_id = TenantId::new("alloc-test-tenant".to_string());
    let mut series = Vec::with_capacity(SERIES_COUNT);
    for i in 0..SERIES_COUNT {
        let metric = format!("alloc_metric_{i}");
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.clone(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, &metric, &labels).expect("series id");
        // Same sample count for every series so a capacity warmed up on the
        // first series is guaranteed sufficient for every later one.
        let samples = (0..SAMPLES_PER_SERIES)
            .map(|j| Sample {
                ts_ns: 1_000_000_000 * (j as i64),
                value: 100.0 + (j as f64) * 0.5 + (i as f64) * 0.001,
            })
            .collect();
        series.push(SeriesInput {
            series_id,
            labels,
            samples,
        });
    }
    let identity = SegmentIdentity {
        tenant_hash: [2u8; 16],
        shard: 0,
        writer_id: "alloc-test-writer".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    SegmentWriter::write(series, identity, bounds)
        .expect("write segment")
        .bytes
}

fn section_bytes(footer: &ravel_segment::Footer, kind: u32) -> std::ops::Range<usize> {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    s.offset as usize..(s.offset + s.len) as usize
}

fn decode_entries(bytes: &bytes::Bytes) -> (ravel_segment::Footer, Vec<SeriesEntry>) {
    const LABEL_DICT: u32 = 1;
    const SERIES_TABLE: u32 = 2;
    let limits = ReaderLimits::default();
    let footer = open_from_full(bytes, limits).expect("open segment").footer;
    let label_dict = &bytes[section_bytes(&footer, LABEL_DICT)];
    let series_table = &bytes[section_bytes(&footer, SERIES_TABLE)];
    let entries = decode_catalog(&footer, label_dict, series_table, limits).expect("catalog");
    (footer, entries)
}

#[test]
fn soa_reused_buffers_stop_reallocating_after_warm_up() {
    let bytes = build_test_segment();
    let (footer, entries) = decode_entries(&bytes);
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let planned = plan_ranges(&footer, &selected).expect("plan ranges");
    let limits = ReaderLimits::default();

    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();

    let decode_all = |scratch: &mut Vec<u8>, timestamps: &mut Vec<i64>, values: &mut Vec<f64>| {
        for (entry, plan) in entries.iter().zip(planned.iter()) {
            let ts_bytes =
                &bytes[plan.ts_range.0 as usize..(plan.ts_range.0 + plan.ts_range.1) as usize];
            let val_bytes =
                &bytes[plan.val_range.0 as usize..(plan.val_range.0 + plan.val_range.1) as usize];
            decode_pages_soa(
                entry, ts_bytes, val_bytes, limits, scratch, timestamps, values,
            )
            .expect("decode");
        }
    };

    // Warm-up pass: buffers start empty, so this pass grows them to their
    // steady-state capacity.
    decode_all(&mut scratch, &mut timestamps, &mut values);
    let (scratch_cap, ts_cap, val_cap) =
        (scratch.capacity(), timestamps.capacity(), values.capacity());

    // Measured pass: every series in this segment has the same sample and
    // page-byte-length shape as the warm-up pass, so a correctly-reusing
    // decode should need no further growth.
    decode_all(&mut scratch, &mut timestamps, &mut values);

    eprintln!(
        "alloc_accounting: {SERIES_COUNT} series x {SAMPLES_PER_SERIES} samples: \
         steady-state capacities scratch={scratch_cap} timestamps={ts_cap} values={val_cap}, \
         unchanged after second decode pass (zero reallocations)"
    );
    assert_eq!(
        scratch.capacity(),
        scratch_cap,
        "scratch buffer reallocated"
    );
    assert_eq!(
        timestamps.capacity(),
        ts_cap,
        "timestamps buffer reallocated"
    );
    assert_eq!(values.capacity(), val_cap, "values buffer reallocated");
}

// ===========================================================================
// P4 additions: v2 decode-path coverage (docs/rseg-v2-plan.md phase P4,
// issue #32). Everything below is additive; the v1 test and helpers above
// are unmodified.
//
// `decode_catalog_v2` takes no caller-supplied scratch buffers (unlike
// `decode_pages_soa`) -- it always returns a freshly allocated
// `Vec<SeriesEntry>`, so there is no warm-up/measured capacity comparison
// to make for the catalog decode step itself the way the test above does
// for `decode_pages_soa`. This section instead covers:
//
//   1. The same warm-up/measured `decode_pages_soa` zero-reallocation
//      proof as the v1 test above, fed by a v2-written segment whose
//      catalog came from `decode_catalog_v2` -- genuine v2 pipeline
//      coverage of the one buffer-reuse decode path that exists in both
//      versions (docs/segment-format.md: page format is unchanged in v2).
//   2. An exact-preallocation assertion on `decode_catalog_v2` itself: it
//      builds its output via `Vec::with_capacity(series_ids.len())` then
//      pushes exactly that many entries (reader.rs), so `capacity()` must
//      equal `len()` with zero growth reallocations.
//   3. A bounds-safety test: a hostile SERIES_IDS `count` (u32::MAX) far
//      exceeding what the actual section bytes could hold must fail fast
//      with a typed error, proving the reader's preallocation is capped by
//      the real buffer size (docs/segment-format.md v2 amendment:
//      "Pre-allocation ... is capped by remaining input size"), never by
//      the untrusted declared count -- a bounds-safety test, not a literal
//      allocation-byte measurement.
//
// Note on scope: the task brief for this ticket guessed this file "likely"
// uses `stats_alloc` the way crates/ravel-otap/benches/decode.rs does. It
// does not -- this file's own header comment above explains why (a global
// counting allocator needs `unsafe impl GlobalAlloc`, and while that impl
// lives inside the `stats_alloc` crate rather than this one, introducing a
// second, allocator-instrumentation-based accounting pattern into this one
// test file, alongside the capacity-based pattern every existing test here
// uses, was judged not worth the inconsistency for this ticket; see the
// final report for the full reasoning.

use ravel_proto::segment::v1::Section;

const SERIES_COUNT_V2: usize = SERIES_COUNT;
const SAMPLES_PER_SERIES_V2: usize = SAMPLES_PER_SERIES;

fn build_test_segment_v2() -> bytes::Bytes {
    let tenant_id = TenantId::new("alloc-test-tenant-v2".to_string());
    let mut series = Vec::with_capacity(SERIES_COUNT_V2);
    for i in 0..SERIES_COUNT_V2 {
        let metric = format!("alloc_metric_v2_{i}");
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.clone(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, &metric, &labels).expect("series id");
        let samples = (0..SAMPLES_PER_SERIES_V2)
            .map(|j| Sample {
                ts_ns: 1_000_000_000 * (j as i64),
                value: 100.0 + (j as f64) * 0.5 + (i as f64) * 0.001,
            })
            .collect();
        series.push(SeriesInput {
            series_id,
            labels,
            samples,
        });
    }
    let identity = SegmentIdentity {
        tenant_hash: [3u8; 16],
        shard: 0,
        writer_id: "alloc-test-writer-v2".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    SegmentWriter::write_v2(series, identity, bounds)
        .expect("write v2 segment")
        .bytes
}

fn decode_entries_v2(bytes: &bytes::Bytes) -> (ravel_segment::Footer, Vec<SeriesEntry>) {
    const LABEL_DICT: u32 = 1;
    const SERIES_IDS: u32 = 5;
    const SERIES_META: u32 = 6;
    let limits = ReaderLimits::default();
    let footer = open_from_full(bytes, limits).expect("open segment").footer;
    let dict = &bytes[section_bytes(&footer, LABEL_DICT)];
    let ids = &bytes[section_bytes(&footer, SERIES_IDS)];
    let meta = &bytes[section_bytes(&footer, SERIES_META)];
    let entries =
        ravel_segment::decode_catalog_v2(&footer, dict, ids, meta, limits).expect("v2 catalog");
    (footer, entries)
}

#[test]
fn v2_soa_reused_buffers_stop_reallocating_after_warm_up() {
    let bytes = build_test_segment_v2();
    let (footer, entries) = decode_entries_v2(&bytes);
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let planned = plan_ranges(&footer, &selected).expect("plan ranges");
    let limits = ReaderLimits::default();

    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();

    let decode_all = |scratch: &mut Vec<u8>, timestamps: &mut Vec<i64>, values: &mut Vec<f64>| {
        for (entry, plan) in entries.iter().zip(planned.iter()) {
            let ts_bytes =
                &bytes[plan.ts_range.0 as usize..(plan.ts_range.0 + plan.ts_range.1) as usize];
            let val_bytes =
                &bytes[plan.val_range.0 as usize..(plan.val_range.0 + plan.val_range.1) as usize];
            decode_pages_soa(
                entry, ts_bytes, val_bytes, limits, scratch, timestamps, values,
            )
            .expect("decode");
        }
    };

    // Warm-up pass, then measured pass, exactly as the v1 test above.
    decode_all(&mut scratch, &mut timestamps, &mut values);
    let (scratch_cap, ts_cap, val_cap) =
        (scratch.capacity(), timestamps.capacity(), values.capacity());
    decode_all(&mut scratch, &mut timestamps, &mut values);

    eprintln!(
        "alloc_accounting (v2): {SERIES_COUNT_V2} series x {SAMPLES_PER_SERIES_V2} samples: \
         steady-state capacities scratch={scratch_cap} timestamps={ts_cap} values={val_cap}, \
         unchanged after second decode pass (zero reallocations)"
    );
    assert_eq!(
        scratch.capacity(),
        scratch_cap,
        "scratch buffer reallocated (v2)"
    );
    assert_eq!(
        timestamps.capacity(),
        ts_cap,
        "timestamps buffer reallocated (v2)"
    );
    assert_eq!(values.capacity(), val_cap, "values buffer reallocated (v2)");
}

#[test]
fn v2_decode_catalog_preallocates_entries_exactly() {
    let bytes = build_test_segment_v2();
    let (_, entries) = decode_entries_v2(&bytes);
    assert_eq!(entries.len(), SERIES_COUNT_V2);
    assert_eq!(
        entries.capacity(),
        entries.len(),
        "decode_catalog_v2 pushes exactly series_ids.len() entries into a \
         Vec::with_capacity(series_ids.len()) (reader.rs); any mismatch means \
         it grew and reallocated"
    );
}

/// Bounds-safety test: SERIES_IDS declares `count: u32` up front
/// (docs/segment-format.md v2 amendment), and the reader must never trust
/// it blindly for allocation size -- `parse_series_ids_v2` (reader.rs)
/// preallocates via `count.min(bytes.len() / 16)`, capped by what the
/// actual buffer could possibly hold, exactly as v1's `scan_series_table`
/// already does for its own count field. A `count` of `u32::MAX` (would
/// demand roughly 64 GiB if honored literally) against a handful of real
/// bytes must fail fast with a typed error, never hang or attempt that
/// allocation.
#[test]
fn hostile_series_ids_count_fails_fast_without_huge_allocation() {
    // SERIES_IDS: count = u32::MAX, but only one real 16-byte id follows --
    // nowhere near enough bytes to satisfy the declared count.
    let mut ids_bytes = Vec::new();
    ids_bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    ids_bytes.extend_from_slice(&[0x01u8; 16]);

    // A trivially well-formed, empty LABEL_DICT and SERIES_META so the
    // failure is pinned to SERIES_IDS parsing specifically, not an earlier
    // section crc or decompression error (both sections precede SERIES_IDS
    // parsing in decode_catalog_v2, but not in the mandatory-section scan
    // order that matters here: dict/ids/meta crc verification all happen
    // up front, so both must be independently well-formed).
    let dict_bytes = 0u32.to_le_bytes().to_vec();
    let mut meta_bytes = Vec::new();
    meta_bytes.extend_from_slice(&0u32.to_le_bytes()); // count
    meta_bytes.extend_from_slice(&0u32.to_le_bytes()); // schema_count
    meta_bytes.extend(std::iter::repeat_n(0x00u8, 9)); // 9 empty blocks (varint 0)

    let section = |kind: u32, bytes: &[u8]| Section {
        kind,
        offset: 0,
        len: bytes.len() as u64,
        crc32c: crc32c::crc32c(bytes),
        comp: 0,
        uncompressed_len: bytes.len() as u64,
    };

    let footer = ravel_segment::Footer {
        tenant_hash: vec![0u8; 16],
        shard: 0,
        writer_id: "alloc-bounds-test".to_string(),
        writer_epoch: 0,
        writer_seq: 0,
        min_event_ts_ns: 0,
        max_event_ts_ns: 0,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
        sample_count: 0,
        series_count: 0,
        sections: vec![
            section(1, &dict_bytes), // LABEL_DICT
            section(5, &ids_bytes),  // SERIES_IDS
            section(6, &meta_bytes), // SERIES_META
            Section {
                kind: 3, // TS_PAGES
                offset: 0,
                len: 0,
                crc32c: 0,
                comp: 0,
                uncompressed_len: 0,
            },
            Section {
                kind: 4, // VAL_PAGES
                offset: 0,
                len: 0,
                crc32c: 0,
                comp: 0,
                uncompressed_len: 0,
            },
        ],
    };

    let start = std::time::Instant::now();
    let result = ravel_segment::decode_catalog_v2(
        &footer,
        &dict_bytes,
        &ids_bytes,
        &meta_bytes,
        ReaderLimits::default(),
    );
    let elapsed = start.elapsed();

    assert_eq!(result, Err(ravel_segment::SegmentError::Truncated));
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "hostile count took {elapsed:?} -- looks like it tried to honor the \
         declared count instead of bounding preallocation by the actual \
         buffer size"
    );
}

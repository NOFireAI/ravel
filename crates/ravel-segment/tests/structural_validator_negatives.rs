//! Negative-path coverage for the RSEG structural footer/section validator.
//! A byte-flip test cannot reach these branches: a flip in a length/offset/
//! count field also invalidates the covering crc and is caught earlier. Each
//! test below violates exactly one structural rule of `validate_sections_v6`
//! (the only version ADR-0047 keeps, superseding ADR-0027's v5) and asserts
//! the exact typed corruption error, never a panic and never wrong data. The
//! test name names the precise rule pinned, so an editor who weakens that
//! branch sees one red test.
#![allow(clippy::expect_used)]

use ravel_segment::{
    Footer, IngestBounds, ReaderLimits, SegmentError, SegmentIdentity, SegmentWriter, SeriesInput,
    VERSION_V6, open_from_full, validate_sections,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

// Section kinds (docs/segment-format.md; crate-internal `format::section_kind`
// is not public, so the wire values are named locally here).
const LABEL_DICT: u32 = 1;
const TS_PAGES: u32 = 3;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;
const SERIES_IDX: u32 = 8;

fn labels(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::new(
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: (*n).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
    )
    .expect("valid labels")
}

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [3u8; 16],
        shard: 1,
        writer_id: "test-writer".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 1000,
    }
}

/// A minimal real, valid v6 object (one series, one sample) -- below the
/// sparse threshold, so LABEL_DICT/SERIES_IDS/SERIES_META/TS_PAGES/VAL_PAGES.
fn one_sample_object() -> bytes::Bytes {
    let series = vec![SeriesInput {
        series_id: SeriesId([0x7F; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "lonely_metric")]),
        samples: vec![Sample {
            ts_ns: 123,
            value: 9.75,
        }],
    }];
    SegmentWriter::write(series, identity(), bounds())
        .expect("writes")
        .bytes
}

/// A real, valid v6 footer plus its `page_region_end` (the footer offset,
/// exactly what `open_from_full` passes to `validate_sections`).
fn valid_footer() -> (Footer, u64) {
    let object = one_sample_object();
    let loc = open_from_full(&object, ReaderLimits::default()).expect("opens");
    (loc.footer, loc.footer_offset)
}

/// Control: a real, unmutated v6 footer validates.
#[test]
fn valid_v6_footer_accepted() {
    let (footer, region) = valid_footer();
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Ok(())
    );
}

/// Pins: every mandatory v6 kind must be present. SERIES_IDS is a kind that
/// did not exist in the retired v1 layout; dropping it is rejected.
#[test]
fn missing_mandatory_section_rejected() {
    let (mut footer, region) = valid_footer();
    footer.sections.retain(|s| s.kind != SERIES_IDS);
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Err(SegmentError::MissingSection("SERIES_IDS"))
    );
}

/// Pins: at most one section per known kind (`seen[idx]` branch). Duplicate
/// LABEL_DICT.
#[test]
fn duplicate_known_kind_rejected() {
    let (mut footer, region) = valid_footer();
    let dup = footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT)
        .copied()
        .expect("LABEL_DICT present");
    footer.sections.push(dup);
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Err(SegmentError::DuplicateSection(LABEL_DICT))
    );
}

/// Pins: `offset + len` is computed with checked arithmetic, distinct from the
/// range-vs-region comparison below. `u64::MAX + 1` overflows.
#[test]
fn section_offset_len_overflow_rejected() {
    let (mut footer, region) = valid_footer();
    let s = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == TS_PAGES)
        .expect("TS_PAGES present");
    s.offset = u64::MAX;
    s.len = 1;
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Err(SegmentError::SectionOutOfBounds)
    );
}

/// Pins: a section range must lie within `[0, page_region_end)`, the
/// non-overflowing sibling of the check above.
#[test]
fn section_range_past_region_rejected() {
    let (mut footer, region) = valid_footer();
    let s = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == TS_PAGES)
        .expect("TS_PAGES present");
    s.offset = 0;
    s.len = region + 1;
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Err(SegmentError::SectionOutOfBounds)
    );
}

/// Pins: `uncompressed_len` is capped before any allocation.
#[test]
fn section_uncompressed_len_over_cap_rejected() {
    let (mut footer, region) = valid_footer();
    let limits = ReaderLimits::default();
    let s = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == LABEL_DICT)
        .expect("LABEL_DICT present");
    s.uncompressed_len = limits.max_section_uncompressed_bytes + 1;
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, limits),
        Err(SegmentError::SectionTooLarge {
            len: limits.max_section_uncompressed_bytes + 1,
            cap: limits.max_section_uncompressed_bytes,
        })
    );
}

/// Pins: unknown section kinds are skipped, not rejected (docs/segment-format.md
/// "unknown section kinds MUST be skipped"). Guards against a forward-compat
/// regression that makes unknown kinds an error.
#[test]
fn unknown_kind_is_skipped_not_rejected() {
    let (mut footer, region) = valid_footer();
    let mut unknown = footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT)
        .copied()
        .expect("LABEL_DICT present");
    unknown.kind = 0xBEEF;
    footer.sections.push(unknown);
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Ok(())
    );
}

/// Pins: a whole SERIES_META and the sparse SERIES_IDX must not both appear
/// (the "exactly one catalog body" rule). Adding SERIES_IDX to a below-
/// threshold footer that already has the whole SERIES_META is rejected.
#[test]
fn both_whole_and_sparse_catalog_rejected() {
    let (mut footer, region) = valid_footer();
    let mut idx = footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT)
        .copied()
        .expect("LABEL_DICT present");
    idx.kind = SERIES_IDX;
    footer.sections.push(idx);
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Err(SegmentError::DuplicateSection(SERIES_META))
    );
}

/// Pins: the sparse catalog form requires BOTH SERIES_IDX and
/// SERIES_META_CHUNKS. A footer with SERIES_IDX but no whole SERIES_META and
/// no SERIES_META_CHUNKS is an incomplete sparse catalog.
#[test]
fn incomplete_sparse_catalog_rejected() {
    let (mut footer, region) = valid_footer();
    let mut idx = footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT)
        .copied()
        .expect("LABEL_DICT present");
    idx.kind = SERIES_IDX;
    footer.sections.push(idx);
    footer.sections.retain(|s| s.kind != SERIES_META);
    assert_eq!(
        validate_sections(&footer, VERSION_V6, region, ReaderLimits::default()),
        Err(SegmentError::SparseSectionsIncomplete)
    );
}

/// Pins: `validate_sections` rejects any trailer version other than 6
/// (ADR-0047), matching `parse_footer`'s accepted set.
#[test]
fn unsupported_version_rejected() {
    let (footer, region) = valid_footer();
    // v5 (ADR-0026) is retired by the v6 EXEMPLARS bump; rejected the same
    // way as any other non-6 version.
    assert_eq!(
        validate_sections(&footer, 5, region, ReaderLimits::default()),
        Err(SegmentError::UnsupportedVersion(5))
    );
    // A version that never existed is rejected the same way.
    assert_eq!(
        validate_sections(&footer, 1, region, ReaderLimits::default()),
        Err(SegmentError::UnsupportedVersion(1))
    );
}

//! Negative-path coverage for finding a1-F02 of the 2026-07-27 storage-engine
//! quality audit (issue #50, parent #35): the structural footer/section and
//! grammar validators in the RSEG v1 read path had no test that feeds them a
//! crc-consistent but structurally-invalid object, so weakening or deleting
//! any single rejection branch passed the whole suite (the byte-flip test in
//! roundtrip.rs cannot reach these branches: a flip in a length/offset/count
//! field also invalidates the covering crc and is caught earlier).
//!
//! Each test below violates exactly one structural rule at a time and asserts
//! the exact typed corruption error, never a panic and never wrong data. The
//! test name and its comment name the precise rule pinned, so an editor who
//! weakens that branch sees one red test pointing straight at it.
//!
//! Two construction strategies are used:
//!   * `validate_sections` is a pure `&Footer` check, so its branches need no
//!     crc setup at all -- mutate a real footer's `sections` vector.
//!   * the grammar validators (`scan_series_table`, `parse_label_dict`,
//!     `index_label_dict`, the `decode_catalog` ordinal checks) read section
//!     payload bytes, so those tests hand-build section bytes and stamp a
//!     matching `comp = NONE` / `len` / `uncompressed_len` / `crc32c` onto the
//!     footer's section descriptor (crc computed the same way the reader
//!     verifies it), then drive `decode_catalog` / `decode_catalog_matching`.
//!
//! Scope: RSEG v1 read path (crates/ravel-segment). The two `validate_sections`
//! v2-dispatch tests exercise only the pure footer-level mandatory-kind logic
//! (no v2 payload decode exists yet; issue #31).
#![allow(clippy::expect_used)]

use ravel_segment::{
    Footer, IngestBounds, ReaderLimits, SegmentError, SegmentIdentity, SegmentWriter, SeriesInput,
    decode_catalog, decode_catalog_matching, open_from_full, validate_sections,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

// Section kinds (docs/segment-format.md; crate-internal `format::section_kind`
// is not public, so the wire values are named locally here).
const LABEL_DICT: u32 = 1;
const SERIES_TABLE: u32 = 2;
const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;

// Trailer version values (docs/segment-format.md).
const VERSION_V1: u16 = 1;
const VERSION_V2: u16 = 2;

const COMP_NONE: i32 = 0;

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
        writer_id: "audit-a1".to_string(),
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

/// A minimal real, valid v1 object (one series, one sample).
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

/// A real, valid footer plus its `page_region_end` (the footer offset, exactly
/// what `open_from_full` passes to `validate_sections`).
fn valid_footer() -> (Footer, u64) {
    let object = one_sample_object();
    let loc = open_from_full(&object, ReaderLimits::default()).expect("opens");
    (loc.footer, loc.footer_offset)
}

// ---------------------------------------------------------------------------
// validate_sections (footer-level, pure &Footer check). No crc setup needed.
// ---------------------------------------------------------------------------

/// Pins: every mandatory v1 kind must be present (reader.rs
/// `validate_sections_v1` mandatory loop). Drop VAL_PAGES; the other three
/// mandatory kinds stay present, unique, and in bounds.
#[test]
fn v1_missing_mandatory_section_rejected() {
    let (mut footer, region) = valid_footer();
    footer.sections.retain(|s| s.kind != VAL_PAGES);
    assert_eq!(
        validate_sections(&footer, VERSION_V1, region, ReaderLimits::default()),
        Err(SegmentError::MissingSection("VAL_PAGES"))
    );
}

/// Pins: at most one section per known kind (reader.rs `validate_sections_v1`
/// `seen[idx]` branch). Duplicate LABEL_DICT.
#[test]
fn v1_duplicate_known_kind_rejected() {
    let (mut footer, region) = valid_footer();
    let dup = footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT)
        .copied()
        .expect("LABEL_DICT present");
    footer.sections.push(dup);
    assert_eq!(
        validate_sections(&footer, VERSION_V1, region, ReaderLimits::default()),
        Err(SegmentError::DuplicateSection(LABEL_DICT))
    );
}

/// Pins: `offset + len` is computed with checked arithmetic (reader.rs
/// `checked_add ... ok_or(SectionOutOfBounds)`), distinct from the
/// range-vs-region comparison below. `u64::MAX + 1` overflows.
#[test]
fn v1_section_offset_len_overflow_rejected() {
    let (mut footer, region) = valid_footer();
    let s = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == TS_PAGES)
        .expect("TS_PAGES present");
    s.offset = u64::MAX;
    s.len = 1;
    assert_eq!(
        validate_sections(&footer, VERSION_V1, region, ReaderLimits::default()),
        Err(SegmentError::SectionOutOfBounds)
    );
}

/// Pins: a section range must lie within `[0, page_region_end)` (reader.rs
/// `end > page_region_end` branch), the non-overflowing sibling of the check
/// above. `offset = 0`, `len = region + 1` puts `end` one byte past the region
/// with no arithmetic overflow.
#[test]
fn v1_section_range_past_region_rejected() {
    let (mut footer, region) = valid_footer();
    let s = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == TS_PAGES)
        .expect("TS_PAGES present");
    s.offset = 0;
    s.len = region + 1;
    assert_eq!(
        validate_sections(&footer, VERSION_V1, region, ReaderLimits::default()),
        Err(SegmentError::SectionOutOfBounds)
    );
}

/// Pins: `uncompressed_len` is capped before any allocation (reader.rs
/// `uncompressed_len > limits.max_section_uncompressed_bytes` branch).
#[test]
fn v1_section_uncompressed_len_over_cap_rejected() {
    let (mut footer, region) = valid_footer();
    let limits = ReaderLimits::default();
    let s = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == LABEL_DICT)
        .expect("LABEL_DICT present");
    s.uncompressed_len = limits.max_section_uncompressed_bytes + 1;
    assert_eq!(
        validate_sections(&footer, VERSION_V1, region, limits),
        Err(SegmentError::SectionTooLarge {
            len: limits.max_section_uncompressed_bytes + 1,
            cap: limits.max_section_uncompressed_bytes,
        })
    );
}

/// Pins: unknown section kinds are skipped, not rejected (reader.rs `_ =>
/// continue` arm; docs/segment-format.md "unknown section kinds MUST be
/// skipped"). An extra in-bounds unknown-kind section alongside all mandatory
/// kinds must still validate. Guards against a future edit that makes unknown
/// kinds an error (a forward-compatibility regression).
#[test]
fn v1_unknown_kind_is_skipped_not_rejected() {
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
        validate_sections(&footer, VERSION_V1, region, ReaderLimits::default()),
        Ok(())
    );
}

/// Pins: `validate_sections` rejects any trailer version other than 1-5
/// (reader.rs `other => UnsupportedVersion(other)` dispatch arm), matching
/// `parse_footer`'s accepted set.
#[test]
fn unsupported_version_rejected() {
    let (footer, region) = valid_footer();
    assert_eq!(
        validate_sections(&footer, 6, region, ReaderLimits::default()),
        Err(SegmentError::UnsupportedVersion(6))
    );
}

// ---------------------------------------------------------------------------
// validate_sections v2 dispatch (pure footer-level mandatory-kind set).
// Built by remapping a v1 footer's kinds; no v2 payload decode is invoked.
// ---------------------------------------------------------------------------

/// Remaps a real v1 footer into the v2 mandatory kind set (LABEL_DICT,
/// SERIES_IDS, SERIES_META, TS_PAGES, VAL_PAGES): reuse LABEL_DICT/TS_PAGES/
/// VAL_PAGES, retag SERIES_TABLE (kind 2) as SERIES_IDS (kind 5), and clone it
/// as SERIES_META (kind 6). All ranges stay in-bounds and each kind is unique.
fn v2_footer() -> (Footer, u64) {
    let (mut footer, region) = valid_footer();
    for s in footer.sections.iter_mut() {
        if s.kind == SERIES_TABLE {
            s.kind = SERIES_IDS;
        }
    }
    let mut meta = footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_IDS)
        .copied()
        .expect("SERIES_IDS present");
    meta.kind = SERIES_META;
    footer.sections.push(meta);
    (footer, region)
}

/// Pins: the v2 dispatch selects the v2 mandatory-kind set (reader.rs
/// `VERSION_V2 => validate_sections_v2`). A footer carrying exactly the five
/// v2 mandatory kinds validates under version 2.
#[test]
fn v2_valid_mandatory_kinds_accepted() {
    let (footer, region) = v2_footer();
    assert_eq!(
        validate_sections(&footer, VERSION_V2, region, ReaderLimits::default()),
        Ok(())
    );
}

/// Pins: the v2 mandatory set includes SERIES_IDS (reader.rs
/// `validate_sections_v2` mandatory loop), a kind that does not exist in v1.
/// Dropping it is rejected only on the v2 path.
#[test]
fn v2_missing_series_ids_rejected() {
    let (mut footer, region) = v2_footer();
    footer.sections.retain(|s| s.kind != SERIES_IDS);
    assert_eq!(
        validate_sections(&footer, VERSION_V2, region, ReaderLimits::default()),
        Err(SegmentError::MissingSection("SERIES_IDS"))
    );
}

// ---------------------------------------------------------------------------
// Grammar validators: crc-consistent crafted section bytes fed to
// decode_catalog / decode_catalog_matching.
// ---------------------------------------------------------------------------

fn push_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// LABEL_DICT uncompressed form: `count: u32`, then `count` `len:varint` +
/// UTF-8 bytes (docs/segment-format.md).
fn label_dict(strings: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    for s in strings {
        push_uvarint(&mut out, s.len() as u64);
        out.extend_from_slice(s);
    }
    out
}

/// One SERIES_TABLE entry (docs/segment-format.md SERIES_TABLE grammar).
struct TableEntry {
    id: [u8; 16],
    labels: Vec<(u64, u64)>,
}

/// SERIES_TABLE uncompressed form: `count: u32`, then `count` entries.
fn series_table(entries: &[TableEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        out.extend_from_slice(&e.id);
        out.extend_from_slice(&(e.labels.len() as u16).to_le_bytes());
        for &(name_ord, value_ord) in &e.labels {
            push_uvarint(&mut out, name_ord);
            push_uvarint(&mut out, value_ord);
        }
        out.extend_from_slice(&1u32.to_le_bytes()); // sample_count
        out.extend_from_slice(&0i64.to_le_bytes()); // min_ts_ns
        out.extend_from_slice(&0i64.to_le_bytes()); // max_ts_ns
        push_uvarint(&mut out, 0); // ts_page offset
        push_uvarint(&mut out, 0); // ts_page len
        push_uvarint(&mut out, 0); // val_page offset
        push_uvarint(&mut out, 0); // val_page len
    }
    out
}

/// Stamps a `comp = NONE`, crc-consistent section descriptor for `bytes` onto
/// the footer's `kind` section, exactly as the reader verifies it
/// (`crc32c` over stored bytes, `len == uncompressed_len == bytes.len()`).
fn set_section_bytes(footer: &mut Footer, kind: u32, bytes: &[u8]) {
    let s = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == kind)
        .expect("section present");
    s.comp = COMP_NONE;
    s.len = bytes.len() as u64;
    s.uncompressed_len = bytes.len() as u64;
    s.crc32c = crc32c::crc32c(bytes);
}

/// A footer whose LABEL_DICT and SERIES_TABLE descriptors match the supplied
/// crafted section bytes; other sections are left as the real object's (unused
/// by `decode_catalog`).
fn catalog_footer(dict: &[u8], table: &[u8]) -> Footer {
    let (mut footer, _region) = valid_footer();
    set_section_bytes(&mut footer, LABEL_DICT, dict);
    set_section_bytes(&mut footer, SERIES_TABLE, table);
    footer
}

/// A two-string dictionary ("__name__", "v") that resolves the `(0, 1)` label
/// pair used by the structurally-valid entries below.
fn dict_two() -> Vec<u8> {
    label_dict(&[b"__name__", b"v"])
}

/// Control: the crafting harness itself produces a byte-valid catalog that
/// decodes cleanly. Makes each negative below meaningful -- the injected
/// violation is the only reason it is rejected.
#[test]
fn crafted_valid_catalog_decodes() {
    let dict = dict_two();
    let table = series_table(&[TableEntry {
        id: [1u8; 16],
        labels: vec![(0, 1)],
    }]);
    let footer = catalog_footer(&dict, &table);
    let entries = decode_catalog(&footer, &dict, &table, ReaderLimits::default())
        .expect("crafted catalog decodes");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].series_id, SeriesId([1u8; 16]));
}

/// Pins: SERIES_TABLE ids must be strictly ascending by byte comparison
/// (reader.rs `scan_series_table` `series_id <= prev` branch). Second id is
/// below the first.
#[test]
fn series_table_unsorted_ids_rejected() {
    let dict = dict_two();
    let table = series_table(&[
        TableEntry {
            id: [2u8; 16],
            labels: vec![(0, 1)],
        },
        TableEntry {
            id: [1u8; 16],
            labels: vec![(0, 1)],
        },
    ]);
    let footer = catalog_footer(&dict, &table);
    assert_eq!(
        decode_catalog(&footer, &dict, &table, ReaderLimits::default()),
        Err(SegmentError::SeriesTableUnsorted)
    );
}

/// Pins: no trailing bytes past the last SERIES_TABLE entry (reader.rs
/// `scan_series_table` `pos != bytes.len()` branch). One valid entry plus a
/// stray byte.
#[test]
fn series_table_trailing_bytes_rejected() {
    let dict = dict_two();
    let mut table = series_table(&[TableEntry {
        id: [1u8; 16],
        labels: vec![(0, 1)],
    }]);
    table.push(0x00);
    let footer = catalog_footer(&dict, &table);
    assert_eq!(
        decode_catalog(&footer, &dict, &table, ReaderLimits::default()),
        Err(SegmentError::TrailingBytes)
    );
}

/// Pins: label ordinals must be within the LABEL_DICT (reader.rs
/// `decode_catalog` `dict.get(idx).ok_or(BadOrdinal)`). A `name_ord` of 9 in a
/// two-string dictionary is out of range.
#[test]
fn out_of_range_label_ordinal_rejected() {
    let dict = dict_two();
    let table = series_table(&[TableEntry {
        id: [1u8; 16],
        labels: vec![(9, 1)],
    }]);
    let footer = catalog_footer(&dict, &table);
    assert_eq!(
        decode_catalog(&footer, &dict, &table, ReaderLimits::default()),
        Err(SegmentError::BadOrdinal(9))
    );
}

/// Pins: no trailing bytes past the LABEL_DICT strings (reader.rs
/// `parse_label_dict` `pos != bytes.len()` branch). A valid one-string
/// dictionary plus a stray byte; the table is well-formed so the dictionary is
/// what fails.
#[test]
fn label_dict_trailing_bytes_rejected() {
    let mut dict = label_dict(&[b"__name__"]);
    dict.push(0x00);
    let table = series_table(&[]);
    let footer = catalog_footer(&dict, &table);
    assert_eq!(
        decode_catalog(&footer, &dict, &table, ReaderLimits::default()),
        Err(SegmentError::TrailingBytes)
    );
}

/// Pins: LABEL_DICT strings must be valid UTF-8 (reader.rs `parse_label_dict`
/// `from_utf8 ... BadUtf8`). A one-string dictionary whose single string is a
/// lone `0xFF` byte.
#[test]
fn label_dict_non_utf8_rejected() {
    let dict = label_dict(&[&[0xFFu8]]);
    let table = series_table(&[]);
    let footer = catalog_footer(&dict, &table);
    assert_eq!(
        decode_catalog(&footer, &dict, &table, ReaderLimits::default()),
        Err(SegmentError::BadUtf8)
    );
}

/// Pins: the lazy matcher path indexes LABEL_DICT with its own trailing-bytes
/// check (reader.rs `index_label_dict` `pos != bytes.len()` branch), a
/// distinct code path from `parse_label_dict`. An empty matcher still forces
/// the dictionary index, so a trailing-byte dictionary is rejected there too.
#[test]
fn matching_label_dict_trailing_bytes_rejected() {
    let mut dict = label_dict(&[b"__name__"]);
    dict.push(0x00);
    let table = series_table(&[]);
    let footer = catalog_footer(&dict, &table);
    assert_eq!(
        decode_catalog_matching(&footer, &dict, &table, &[], ReaderLimits::default()),
        Err(SegmentError::TrailingBytes)
    );
}

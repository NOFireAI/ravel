//! RSEG v1 reader: footer-first parsing, LABEL_DICT/SERIES_TABLE decode,
//! series selection, byte-range planning, and page decode. Every offset,
//! length, count, and ordinal read from stored bytes is untrusted
//! (docs/segment-format.md); every fallible path returns a typed error.

use ravel_proto::segment::v1::{Footer, Section};
use ravel_types::{Label, LabelSet, Sample, SeriesId};

use crate::crc::page_crc;
use crate::error::SegmentError;
use crate::format::{
    MAGIC, RESERVED, ReaderLimits, SIGNAL_METRICS, VERSION, VERSION_V2, VERSION_V3, compression,
    page_comp, page_enc, section_kind,
};
use crate::histogram::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};
use crate::varint::{read_uvarint, read_zigzag_varint};
use crate::writer::HistogramSample;

const TRAILER_LEN_USIZE: usize = crate::format::TRAILER_LEN as usize;

/// A successfully located and decoded footer, plus the absolute byte
/// offsets needed to plan further reads.
#[derive(Debug, Clone)]
pub struct FooterLocation {
    pub footer: Footer,
    /// Absolute offset of the footer protobuf bytes within the object.
    pub footer_offset: u64,
    /// Absolute offset of the 16-byte trailer within the object.
    pub trailer_offset: u64,
    pub total_size: u64,
    /// Trailer format version (1, 2, or 3; `parse_footer` rejects any other
    /// value). Determines which catalog decode path
    /// (`decode_catalog`/`decode_catalog_matching` for v1,
    /// `decode_catalog_v2`/`decode_catalog_matching_v2` for v2,
    /// `decode_catalog_v3`/`decode_catalog_matching_v3` for v3) and which
    /// `validate_sections` mandatory-kind set applies.
    pub version: u16,
}

/// Result of attempting to locate the footer from a (possibly partial)
/// suffix of the object.
#[derive(Debug, Clone)]
pub enum FooterOutcome {
    Ready(FooterLocation),
    /// Fetch `object[offset .. offset + len)` (a suffix ending at the
    /// object's last byte) and retry with those bytes as `tail`.
    NeedRange {
        offset: u64,
        len: u64,
    },
}

/// Parses the trailer and footer given `tail`, a suffix of the object
/// ending at its last byte (`tail == object[total_size - tail.len() ..
/// total_size]`). Does not validate section bounds/caps; call
/// [`validate_sections`] on the result before trusting section descriptors.
pub fn parse_footer(total_size: u64, tail: &[u8]) -> Result<FooterOutcome, SegmentError> {
    let trailer_len = crate::format::TRAILER_LEN;
    if total_size < trailer_len {
        return Err(SegmentError::TooSmall { size: total_size });
    }
    let tail_len = tail.len() as u64;
    if tail_len < trailer_len {
        return Ok(FooterOutcome::NeedRange {
            offset: total_size - trailer_len,
            len: trailer_len,
        });
    }

    let trailer = &tail[tail.len() - TRAILER_LEN_USIZE..];
    let footer_len = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    let footer_crc32c = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);
    let version = u16::from_le_bytes([trailer[8], trailer[9]]);
    let signal = trailer[10];
    let reserved = trailer[11];
    let magic = &trailer[12..16];

    if magic != MAGIC {
        return Err(SegmentError::BadMagic);
    }
    if version != VERSION && version != VERSION_V2 && version != VERSION_V3 {
        return Err(SegmentError::UnsupportedVersion(version));
    }
    if signal != SIGNAL_METRICS {
        return Err(SegmentError::UnsupportedSignal(signal));
    }
    if reserved != RESERVED {
        return Err(SegmentError::ReservedNonZero(reserved));
    }
    if footer_len == 0 {
        return Err(SegmentError::InvalidFooterLen);
    }

    let footer_len_u64 = u64::from(footer_len);
    let footer_start_abs = total_size
        .checked_sub(trailer_len)
        .and_then(|v| v.checked_sub(footer_len_u64))
        .ok_or(SegmentError::InvalidFooterLen)?;

    let needed_from_tail = trailer_len + footer_len_u64;
    if tail_len < needed_from_tail {
        return Ok(FooterOutcome::NeedRange {
            offset: footer_start_abs,
            len: needed_from_tail,
        });
    }

    let needed_usize = to_usize(needed_from_tail)?;
    let footer_start_in_tail = tail.len() - needed_usize;
    let footer_end_in_tail = tail.len() - TRAILER_LEN_USIZE;
    let footer_bytes = &tail[footer_start_in_tail..footer_end_in_tail];

    let expected_crc = crate::crc::footer_crc(footer_bytes, footer_len, version, signal, reserved);
    if expected_crc != footer_crc32c {
        return Err(SegmentError::FooterCrcMismatch);
    }

    let footer = <Footer as prost::Message>::decode(footer_bytes)
        .map_err(|e| SegmentError::FooterDecode(e.to_string()))?;

    Ok(FooterOutcome::Ready(FooterLocation {
        footer,
        footer_offset: footer_start_abs,
        trailer_offset: total_size - trailer_len,
        total_size,
        version,
    }))
}

/// Validates footer-level section invariants (docs/segment-format.md),
/// dispatching on the trailer `version` (1, 2, or 3; anything else is
/// `UnsupportedVersion`, matching `parse_footer`'s accepted set): at most
/// one section per known kind, every mandatory kind for that version
/// present, every section range within `[0, page_region_end)` with checked
/// arithmetic, and section `uncompressed_len` within `limits`.
///
/// The v2 amendment's extra count-equality check (SERIES_IDS `count`,
/// SERIES_META `count`, and `Footer.series_count` must all be equal) is
/// deliberately NOT performed here: `Section` (proto/ravel/segment.proto)
/// carries no `count` field, so that check needs each section's decoded
/// payload bytes, which this function -- called immediately after
/// `parse_footer`, before any section byte GET -- never has. It is
/// performed in `decode_catalog_v2` and `decode_catalog_matching_v2`
/// instead, which do have those bytes. Same deferral applies to v3's
/// `value_kind`-vs-page-presence check (`decode_catalog_v3`,
/// `decode_catalog_matching_v3`).
pub fn validate_sections(
    footer: &Footer,
    version: u16,
    page_region_end: u64,
    limits: ReaderLimits,
) -> Result<(), SegmentError> {
    match version {
        VERSION => validate_sections_v1(footer, page_region_end, limits),
        VERSION_V2 => validate_sections_v2(footer, page_region_end, limits),
        VERSION_V3 => validate_sections_v3(footer, page_region_end, limits),
        other => Err(SegmentError::UnsupportedVersion(other)),
    }
}

/// v1 mandatory-kind validation: LABEL_DICT, SERIES_TABLE, TS_PAGES,
/// VAL_PAGES. Body unchanged from the pre-v2 `validate_sections`.
fn validate_sections_v1(
    footer: &Footer,
    page_region_end: u64,
    limits: ReaderLimits,
) -> Result<(), SegmentError> {
    let mut seen = [false; 4];
    for section in &footer.sections {
        let end = section
            .offset
            .checked_add(section.len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if end > page_region_end {
            return Err(SegmentError::SectionOutOfBounds);
        }
        let idx = match section.kind {
            section_kind::LABEL_DICT => 0,
            section_kind::SERIES_TABLE => 1,
            section_kind::TS_PAGES => 2,
            section_kind::VAL_PAGES => 3,
            _ => continue,
        };
        if seen[idx] {
            return Err(SegmentError::DuplicateSection(section.kind));
        }
        seen[idx] = true;
        if section.uncompressed_len > limits.max_section_uncompressed_bytes {
            return Err(SegmentError::SectionTooLarge {
                len: section.uncompressed_len,
                cap: limits.max_section_uncompressed_bytes,
            });
        }
    }
    const NAMES: [&str; 4] = ["LABEL_DICT", "SERIES_TABLE", "TS_PAGES", "VAL_PAGES"];
    for (i, name) in NAMES.iter().enumerate() {
        if !seen[i] {
            return Err(SegmentError::MissingSection(name));
        }
    }
    Ok(())
}

/// v2 mandatory-kind validation (docs/segment-format.md "RSEG v2
/// amendment"): LABEL_DICT, SERIES_IDS, SERIES_META, TS_PAGES, VAL_PAGES.
/// SERIES_TABLE (kind 2) is not mandatory for v2 and is not specially
/// checked if present -- it falls through the same as any other
/// non-mandatory kind, exactly as an unknown kind would. Bytes between
/// sections are permitted (v2 amendment "Bytes between sections are
/// permitted, exactly as in v1"); this function does not require
/// adjacency between sections.
fn validate_sections_v2(
    footer: &Footer,
    page_region_end: u64,
    limits: ReaderLimits,
) -> Result<(), SegmentError> {
    let mut seen = [false; 5];
    for section in &footer.sections {
        let end = section
            .offset
            .checked_add(section.len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if end > page_region_end {
            return Err(SegmentError::SectionOutOfBounds);
        }
        let idx = match section.kind {
            section_kind::LABEL_DICT => 0,
            section_kind::SERIES_IDS => 1,
            section_kind::SERIES_META => 2,
            section_kind::TS_PAGES => 3,
            section_kind::VAL_PAGES => 4,
            _ => continue,
        };
        if seen[idx] {
            return Err(SegmentError::DuplicateSection(section.kind));
        }
        seen[idx] = true;
        if section.uncompressed_len > limits.max_section_uncompressed_bytes {
            return Err(SegmentError::SectionTooLarge {
                len: section.uncompressed_len,
                cap: limits.max_section_uncompressed_bytes,
            });
        }
    }
    const NAMES: [&str; 5] = [
        "LABEL_DICT",
        "SERIES_IDS",
        "SERIES_META",
        "TS_PAGES",
        "VAL_PAGES",
    ];
    for (i, name) in NAMES.iter().enumerate() {
        if !seen[i] {
            return Err(SegmentError::MissingSection(name));
        }
    }
    Ok(())
}

/// v3 mandatory-kind validation (docs/rseg-v3-plan.md section 3.2):
/// LABEL_DICT, SERIES_IDS, SERIES_META, TS_PAGES are always mandatory;
/// VAL_PAGES and HIST_PAGES are each optional, but at least one of the two
/// must be present whenever the segment has any series at all
/// (`footer.series_count > 0`) -- an empty segment (zero series) omits
/// both, per the merged `write_v3` behavior
/// (`empty_segment_omits_val_and_hist_sections`).
///
/// Whether a *present* VAL_PAGES/HIST_PAGES section actually matches the
/// segment's scalar/histogram series counts (the "value_kind count must
/// equal non-empty page count" rule, and "a section for a kind whose
/// mandatory count is zero is Corrupted") needs each series' decoded
/// `value_kind`, which this function does not have; that check is
/// performed in `decode_catalog_v3`/`decode_catalog_matching_v3`, mirroring
/// v2's count-equality deferral above.
fn validate_sections_v3(
    footer: &Footer,
    page_region_end: u64,
    limits: ReaderLimits,
) -> Result<(), SegmentError> {
    let mut seen = [false; 6];
    for section in &footer.sections {
        let end = section
            .offset
            .checked_add(section.len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if end > page_region_end {
            return Err(SegmentError::SectionOutOfBounds);
        }
        let idx = match section.kind {
            section_kind::LABEL_DICT => 0,
            section_kind::SERIES_IDS => 1,
            section_kind::SERIES_META => 2,
            section_kind::TS_PAGES => 3,
            section_kind::VAL_PAGES => 4,
            section_kind::HIST_PAGES => 5,
            _ => continue,
        };
        if seen[idx] {
            return Err(SegmentError::DuplicateSection(section.kind));
        }
        seen[idx] = true;
        if section.uncompressed_len > limits.max_section_uncompressed_bytes {
            return Err(SegmentError::SectionTooLarge {
                len: section.uncompressed_len,
                cap: limits.max_section_uncompressed_bytes,
            });
        }
    }
    const NAMES: [&str; 4] = ["LABEL_DICT", "SERIES_IDS", "SERIES_META", "TS_PAGES"];
    for (i, name) in NAMES.iter().enumerate() {
        if !seen[i] {
            return Err(SegmentError::MissingSection(name));
        }
    }
    if footer.series_count > 0 && !seen[4] && !seen[5] {
        return Err(SegmentError::MissingSection("VAL_PAGES or HIST_PAGES"));
    }
    Ok(())
}

/// Convenience: parse and validate a footer from the complete object bytes.
pub fn open_from_full(bytes: &[u8], limits: ReaderLimits) -> Result<FooterLocation, SegmentError> {
    match parse_footer(bytes.len() as u64, bytes)? {
        FooterOutcome::Ready(loc) => {
            validate_sections(&loc.footer, loc.version, loc.footer_offset, limits)?;
            Ok(loc)
        }
        FooterOutcome::NeedRange { .. } => Err(SegmentError::Truncated),
    }
}

/// Convenience: parse a footer from a suffix of the object. If the suffix
/// doesn't cover the footer, returns `NeedRange` for the caller to fetch and
/// retry (docs/segment-format.md reader protocol step 2-3).
pub fn open_from_suffix(
    suffix: &[u8],
    total_size: u64,
    limits: ReaderLimits,
) -> Result<FooterOutcome, SegmentError> {
    match parse_footer(total_size, suffix)? {
        FooterOutcome::Ready(loc) => {
            validate_sections(&loc.footer, loc.version, loc.footer_offset, limits)?;
            Ok(FooterOutcome::Ready(loc))
        }
        need @ FooterOutcome::NeedRange { .. } => Ok(need),
    }
}

/// A series' value model, from SERIES_META column 10 in v3
/// (docs/rseg-v3-plan.md section 3.4). v1/v2 series are always `Scalar`
/// (the column does not exist pre-v3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Scalar,
    Histogram,
}

/// A decoded SERIES_TABLE/SERIES_META entry with its labels materialized
/// from LABEL_DICT. Page offsets/lengths are relative to their respective
/// section's start. `value_kind`/`hist_page` are v3-only (section 3.4);
/// v1/v2 entries always carry `ValueKind::Scalar` and `hist_page: (0, 0)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesEntry {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub sample_count: u32,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub ts_page: (u64, u64),
    pub val_page: (u64, u64),
    pub value_kind: ValueKind,
    pub hist_page: (u64, u64),
}

fn find_section(footer: &Footer, kind: u32) -> Option<&Section> {
    footer.sections.iter().find(|s| s.kind == kind)
}

fn to_usize(v: u64) -> Result<usize, SegmentError> {
    usize::try_from(v).map_err(|_| SegmentError::SectionOutOfBounds)
}

/// Decompresses and crc-verifies a whole section's stored bytes, per
/// docs/segment-format.md ("Section crc32c covers the stored bytes";
/// "uncompressed_len must match the decompressed size exactly").
fn decode_section_bytes(
    section: &Section,
    stored: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<u8>, SegmentError> {
    if stored.len() as u64 != section.len {
        return Err(SegmentError::SectionOutOfBounds);
    }
    if crc32c::crc32c(stored) != section.crc32c {
        return Err(SegmentError::SectionCrcMismatch);
    }
    if section.uncompressed_len > limits.max_section_uncompressed_bytes {
        return Err(SegmentError::SectionTooLarge {
            len: section.uncompressed_len,
            cap: limits.max_section_uncompressed_bytes,
        });
    }
    let capacity = to_usize(section.uncompressed_len)?;

    let decompressed = if section.comp == compression::NONE {
        if stored.len() as u64 != section.uncompressed_len {
            return Err(SegmentError::DecompressedLenMismatch {
                expected: section.uncompressed_len,
                actual: stored.len() as u64,
            });
        }
        stored.to_vec()
    } else if section.comp == compression::LZ4 {
        if stored.len() < 4 {
            return Err(SegmentError::Truncated);
        }
        let prefix = u32::from_le_bytes([stored[0], stored[1], stored[2], stored[3]]);
        if u64::from(prefix) > limits.max_section_uncompressed_bytes {
            return Err(SegmentError::SectionTooLarge {
                len: u64::from(prefix),
                cap: limits.max_section_uncompressed_bytes,
            });
        }
        lz4_flex::decompress_size_prepended(stored)
            .map_err(|e| SegmentError::Decompress(e.to_string()))?
    } else if section.comp == compression::ZSTD {
        zstd::bulk::decompress(stored, capacity)
            .map_err(|e| SegmentError::Decompress(e.to_string()))?
    } else {
        return Err(SegmentError::InvalidSectionCompression(section.comp));
    };

    if decompressed.len() as u64 != section.uncompressed_len {
        return Err(SegmentError::DecompressedLenMismatch {
            expected: section.uncompressed_len,
            actual: decompressed.len() as u64,
        });
    }
    Ok(decompressed)
}

fn take_bytes<'a>(bytes: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], SegmentError> {
    let end = pos.checked_add(n).ok_or(SegmentError::Truncated)?;
    let slice = bytes.get(*pos..end).ok_or(SegmentError::Truncated)?;
    *pos = end;
    Ok(slice)
}

fn take_u16_le(bytes: &[u8], pos: &mut usize) -> Result<u16, SegmentError> {
    let s = take_bytes(bytes, pos, 2)?;
    Ok(u16::from_le_bytes([s[0], s[1]]))
}

fn take_u32_le(bytes: &[u8], pos: &mut usize) -> Result<u32, SegmentError> {
    let s = take_bytes(bytes, pos, 4)?;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn take_i64_le(bytes: &[u8], pos: &mut usize) -> Result<i64, SegmentError> {
    let s = take_bytes(bytes, pos, 8)?;
    let arr: [u8; 8] = s.try_into().map_err(|_| SegmentError::Truncated)?;
    Ok(i64::from_le_bytes(arr))
}

fn take_array16(bytes: &[u8], pos: &mut usize) -> Result<[u8; 16], SegmentError> {
    let s = take_bytes(bytes, pos, 16)?;
    s.try_into().map_err(|_| SegmentError::Truncated)
}

fn parse_label_dict(bytes: &[u8]) -> Result<Vec<String>, SegmentError> {
    let mut pos = 0usize;
    let count = take_u32_le(bytes, &mut pos)?;
    // Each string costs at least its 1-byte length varint, so `count` can
    // never validly exceed the remaining bytes; capping the pre-allocation
    // keeps a corrupt count from forcing a huge reservation.
    let mut out = Vec::with_capacity((count as usize).min(bytes.len()));
    for _ in 0..count {
        let len = read_uvarint(bytes, &mut pos)?;
        let len = to_usize(len)?;
        let slice = take_bytes(bytes, &mut pos, len)?;
        let s = std::str::from_utf8(slice).map_err(|_| SegmentError::BadUtf8)?;
        out.push(s.to_string());
    }
    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(out)
}

/// Byte ranges of each dictionary string within the uncompressed
/// LABEL_DICT payload, in ordinal order. Bounds are validated during the
/// indexing pass; UTF-8 validation is deferred until a string is actually
/// materialized.
fn index_label_dict(bytes: &[u8]) -> Result<Vec<(usize, usize)>, SegmentError> {
    let mut pos = 0usize;
    let count = take_u32_le(bytes, &mut pos)?;
    let mut out = Vec::with_capacity((count as usize).min(bytes.len()));
    for _ in 0..count {
        let len = read_uvarint(bytes, &mut pos)?;
        let len = to_usize(len)?;
        let start = pos;
        take_bytes(bytes, &mut pos, len)?;
        out.push((start, len));
    }
    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(out)
}

fn dict_str<'a>(
    dict_bytes: &'a [u8],
    index: &[(usize, usize)],
    ord: u64,
) -> Result<&'a str, SegmentError> {
    let i = to_usize(ord)?;
    let (start, len) = *index.get(i).ok_or(SegmentError::BadOrdinal(ord))?;
    let slice = dict_bytes
        .get(start..start + len)
        .ok_or(SegmentError::Truncated)?;
    std::str::from_utf8(slice).map_err(|_| SegmentError::BadUtf8)
}

/// Resolves LABEL_DICT ordinals to `&str`, memoizing the bounds check and
/// UTF-8 validation so each distinct ordinal pays them at most once no
/// matter how many series reference it.
///
/// The v2 catalog decoders materialize labels ordinal by ordinal through
/// the deferred-validation LABEL_DICT index (`index_label_dict` +
/// `dict_str`), which the lazy path needs so a matcher can reject a series
/// without ever UTF-8-validating strings it will not return. The eager
/// path materializes every series, so every distinct name/value string is
/// validated anyway, but a bare `dict_str` re-validates on every one of the
/// many references (in the 10k-series/6-label bench shape, ~120k
/// references over ~500 distinct strings). Profiling attributed that
/// repeated `str::from_utf8` as the RSEG v2 eager-decode regression
/// (issue #94). Memoizing collapses it to one validation per referenced
/// ordinal.
///
/// Behavior is identical to calling `dict_str` per reference: ordinals are
/// resolved in the same order, only referenced ordinals are ever validated
/// (an unreferenced entry with bad UTF-8 stays unvalidated, exactly as
/// before), and the first `BadOrdinal`/`Truncated`/`BadUtf8` encountered is
/// the same one `dict_str` would return, because a cached hit only ever
/// replaces a repeat of an already-successful validation.
struct DictResolver<'a> {
    dict_bytes: &'a [u8],
    index: &'a [(usize, usize)],
    cache: Vec<Option<&'a str>>,
}

impl<'a> DictResolver<'a> {
    fn new(dict_bytes: &'a [u8], index: &'a [(usize, usize)]) -> Self {
        Self {
            dict_bytes,
            index,
            cache: vec![None; index.len()],
        }
    }

    fn get(&mut self, ord: u64) -> Result<&'a str, SegmentError> {
        let i = to_usize(ord)?;
        let (start, len) = *self.index.get(i).ok_or(SegmentError::BadOrdinal(ord))?;
        // `i < index.len() == cache.len()`, so indexing `cache[i]` after a
        // successful `index.get(i)` cannot panic.
        if let Some(s) = self.cache[i] {
            return Ok(s);
        }
        let slice = self
            .dict_bytes
            .get(start..start + len)
            .ok_or(SegmentError::Truncated)?;
        let s = std::str::from_utf8(slice).map_err(|_| SegmentError::BadUtf8)?;
        self.cache[i] = Some(s);
        Ok(s)
    }
}

/// One SERIES_TABLE entry as scanned in place: label pairs live in a
/// scratch buffer reused across entries.
struct RawEntryView<'a> {
    series_id: [u8; 16],
    label_pairs: &'a [(u64, u64)],
    sample_count: u32,
    min_ts_ns: i64,
    max_ts_ns: i64,
    ts_page: (u64, u64),
    val_page: (u64, u64),
}

/// Walks every SERIES_TABLE entry, enforcing the structural grammar
/// (bounds, varints, strictly ascending series ids, no trailing bytes)
/// and handing each entry to `visit` without any per-entry allocation.
fn scan_series_table(
    bytes: &[u8],
    mut visit: impl FnMut(&RawEntryView<'_>) -> Result<(), SegmentError>,
) -> Result<(), SegmentError> {
    let mut pos = 0usize;
    let count = take_u32_le(bytes, &mut pos)?;
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    let mut prev_id: Option<[u8; 16]> = None;
    for _ in 0..count {
        let series_id = take_array16(bytes, &mut pos)?;
        if let Some(prev) = prev_id
            && series_id <= prev
        {
            return Err(SegmentError::SeriesTableUnsorted);
        }
        prev_id = Some(series_id);

        let label_count = take_u16_le(bytes, &mut pos)?;
        pairs.clear();
        pairs.reserve(usize::from(label_count));
        for _ in 0..label_count {
            let name_ord = read_uvarint(bytes, &mut pos)?;
            let value_ord = read_uvarint(bytes, &mut pos)?;
            pairs.push((name_ord, value_ord));
        }
        let sample_count = take_u32_le(bytes, &mut pos)?;
        let min_ts_ns = take_i64_le(bytes, &mut pos)?;
        let max_ts_ns = take_i64_le(bytes, &mut pos)?;
        let ts_offset = read_uvarint(bytes, &mut pos)?;
        let ts_len = read_uvarint(bytes, &mut pos)?;
        let val_offset = read_uvarint(bytes, &mut pos)?;
        let val_len = read_uvarint(bytes, &mut pos)?;
        visit(&RawEntryView {
            series_id,
            label_pairs: &pairs,
            sample_count,
            min_ts_ns,
            max_ts_ns,
            ts_page: (ts_offset, ts_len),
            val_page: (val_offset, val_len),
        })?;
    }
    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(())
}

/// Minimum encoded size of one SERIES_TABLE entry (id + label_count +
/// sample_count + min/max ts + four 1-byte varints), used only to cap
/// pre-allocations against a corrupt declared count.
const MIN_TABLE_ENTRY_BYTES: usize = 16 + 2 + 4 + 8 + 8 + 4;

/// Decodes LABEL_DICT and SERIES_TABLE (verifying section crcs) into
/// [`SeriesEntry`] values with materialized [`LabelSet`]s.
pub fn decode_catalog(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_table_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntry>, SegmentError> {
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let series_table_section = find_section(footer, section_kind::SERIES_TABLE)
        .ok_or(SegmentError::MissingSection("SERIES_TABLE"))?;

    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let table_bytes = decode_section_bytes(series_table_section, series_table_bytes, limits)?;

    let dict = parse_label_dict(&dict_bytes)?;

    let mut entries = Vec::with_capacity(table_bytes.len() / MIN_TABLE_ENTRY_BYTES);
    scan_series_table(&table_bytes, |raw| {
        let mut labels = Vec::with_capacity(raw.label_pairs.len());
        for &(name_ord, value_ord) in raw.label_pairs {
            let name_idx = to_usize(name_ord)?;
            let value_idx = to_usize(value_ord)?;
            let name = dict
                .get(name_idx)
                .ok_or(SegmentError::BadOrdinal(name_ord))?;
            let value = dict
                .get(value_idx)
                .ok_or(SegmentError::BadOrdinal(value_ord))?;
            labels.push(Label {
                name: name.clone(),
                value: value.clone(),
            });
        }
        let label_set = LabelSet::new(labels)?;

        entries.push(SeriesEntry {
            series_id: SeriesId(raw.series_id),
            labels: label_set,
            sample_count: raw.sample_count,
            min_ts_ns: raw.min_ts_ns,
            max_ts_ns: raw.max_ts_ns,
            ts_page: raw.ts_page,
            val_page: raw.val_page,
            value_kind: ValueKind::Scalar,
            hist_page: (0, 0),
        });
        Ok(())
    })?;
    Ok(entries)
}

/// Decodes only the series whose labels satisfy every `(name, value)`
/// equality in `equals`, matching on dictionary ordinals so that
/// non-matching series never materialize a [`LabelSet`] or any `String`.
///
/// Semantics match [`decode_catalog`] followed by [`select`] with the same
/// pairs (and no predicate): the whole SERIES_TABLE is still structurally
/// validated (bounds, sortedness, trailing bytes) and both section crcs
/// are verified, so corrupt input fails with the same typed errors. An
/// empty `equals` selects every series. Dictionary strings of series that
/// never match are not UTF-8-validated here, because they are never
/// materialized; [`decode_catalog`] remains the eager, fully-validating
/// path.
pub fn decode_catalog_matching(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_table_bytes: &[u8],
    equals: &[(&str, &str)],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntry>, SegmentError> {
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let series_table_section = find_section(footer, section_kind::SERIES_TABLE)
        .ok_or(SegmentError::MissingSection("SERIES_TABLE"))?;

    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let table_bytes = decode_section_bytes(series_table_section, series_table_bytes, limits)?;

    let dict_index = index_label_dict(&dict_bytes)?;

    // Resolve each matcher pair to dictionary ordinals by raw byte
    // equality (equal bytes imply equal UTF-8). A matcher string absent
    // from the dictionary can never match any entry.
    let find_ordinal = |needle: &str| -> Option<u64> {
        let needle = needle.as_bytes();
        dict_index
            .iter()
            .position(|&(start, len)| {
                dict_bytes
                    .get(start..start + len)
                    .is_some_and(|s| s == needle)
            })
            .map(|i| i as u64)
    };
    let mut matcher_ords: Vec<(u64, u64)> = Vec::with_capacity(equals.len());
    let mut resolvable = true;
    for (name, value) in equals {
        match (find_ordinal(name), find_ordinal(value)) {
            (Some(n), Some(v)) => matcher_ords.push((n, v)),
            _ => {
                resolvable = false;
                break;
            }
        }
    }

    let mut entries = Vec::new();
    scan_series_table(&table_bytes, |raw| {
        if !resolvable
            || !matcher_ords
                .iter()
                .all(|needed| raw.label_pairs.contains(needed))
        {
            return Ok(());
        }
        let mut labels = Vec::with_capacity(raw.label_pairs.len());
        for &(name_ord, value_ord) in raw.label_pairs {
            labels.push(Label {
                name: dict_str(&dict_bytes, &dict_index, name_ord)?.to_string(),
                value: dict_str(&dict_bytes, &dict_index, value_ord)?.to_string(),
            });
        }
        let label_set = LabelSet::new(labels)?;
        entries.push(SeriesEntry {
            series_id: SeriesId(raw.series_id),
            labels: label_set,
            sample_count: raw.sample_count,
            min_ts_ns: raw.min_ts_ns,
            max_ts_ns: raw.max_ts_ns,
            ts_page: raw.ts_page,
            val_page: raw.val_page,
            value_kind: ValueKind::Scalar,
            hist_page: (0, 0),
        });
        Ok(())
    })?;
    Ok(entries)
}

/// Filters `entries` by exact label equality pairs and/or a predicate over
/// the full label set.
pub fn select<'a>(
    entries: &'a [SeriesEntry],
    equals: &[(&str, &str)],
    predicate: Option<&dyn Fn(&LabelSet) -> bool>,
) -> Vec<&'a SeriesEntry> {
    entries
        .iter()
        .filter(|e| {
            equals
                .iter()
                .all(|(name, value)| e.labels.get(name) == Some(*value))
                && predicate.is_none_or(|p| p(&e.labels))
        })
        .collect()
}

/// Absolute (offset, len) byte ranges within the object for one series'
/// TS, VAL, and (v3 only) HIST pages. `val_range`/`hist_range` are `(0, 0)`
/// for the page kind a series doesn't have (v1/v2 entries are always
/// scalar, so `hist_range` is always `(0, 0)` for them; a v3 histogram-kind
/// entry has no VAL page, so `val_range` is `(0, 0)` for it), mirroring the
/// same "(0, 0) means not applicable" convention SERIES_META's own
/// `val_page`/`hist_page` columns already use (docs/rseg-v3-plan.md
/// section 3.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedRange {
    pub series_id: SeriesId,
    pub ts_range: (u64, u64),
    pub val_range: (u64, u64),
    pub hist_range: (u64, u64),
}

/// Computes exact absolute byte ranges for the TS/VAL pages of `selected`
/// series: `absolute = section.offset + page.offset`, bounds-checked
/// against each section's own length.
pub fn plan_ranges(
    footer: &Footer,
    selected: &[&SeriesEntry],
) -> Result<Vec<PlannedRange>, SegmentError> {
    let ts_section = find_section(footer, section_kind::TS_PAGES)
        .ok_or(SegmentError::MissingSection("TS_PAGES"))?;
    let val_section = find_section(footer, section_kind::VAL_PAGES)
        .ok_or(SegmentError::MissingSection("VAL_PAGES"))?;

    let mut out = Vec::new();
    for entry in selected {
        let (ts_off, ts_len) = entry.ts_page;
        let ts_end = ts_off
            .checked_add(ts_len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if ts_end > ts_section.len {
            return Err(SegmentError::SectionOutOfBounds);
        }
        let ts_abs = ts_section
            .offset
            .checked_add(ts_off)
            .ok_or(SegmentError::SectionOutOfBounds)?;

        let (val_off, val_len) = entry.val_page;
        let val_end = val_off
            .checked_add(val_len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if val_end > val_section.len {
            return Err(SegmentError::SectionOutOfBounds);
        }
        let val_abs = val_section
            .offset
            .checked_add(val_off)
            .ok_or(SegmentError::SectionOutOfBounds)?;

        out.push(PlannedRange {
            series_id: entry.series_id,
            ts_range: (ts_abs, ts_len),
            val_range: (val_abs, val_len),
            hist_range: (0, 0),
        });
    }
    Ok(out)
}

/// v3 counterpart of [`plan_ranges`]: computes TS/VAL/HIST ranges for
/// `selected` series, looking up VAL_PAGES only if at least one selected
/// entry is `ValueKind::Scalar` and HIST_PAGES only if at least one is
/// `ValueKind::Histogram` -- a segment legitimately omits either section
/// when no series needs it (docs/rseg-v3-plan.md section 3.2), so an
/// unconditional `find_section` lookup (as `plan_ranges` does for v1/v2,
/// where every series is scalar) would wrongly fail a valid all-histogram
/// or all-scalar v3 segment.
pub fn plan_ranges_v3(
    footer: &Footer,
    selected: &[&SeriesEntry],
) -> Result<Vec<PlannedRange>, SegmentError> {
    let ts_section = find_section(footer, section_kind::TS_PAGES)
        .ok_or(SegmentError::MissingSection("TS_PAGES"))?;

    let mut out = Vec::new();
    for entry in selected {
        let (ts_off, ts_len) = entry.ts_page;
        let ts_end = ts_off
            .checked_add(ts_len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if ts_end > ts_section.len {
            return Err(SegmentError::SectionOutOfBounds);
        }
        let ts_abs = ts_section
            .offset
            .checked_add(ts_off)
            .ok_or(SegmentError::SectionOutOfBounds)?;

        let val_range = match entry.value_kind {
            ValueKind::Scalar => {
                let val_section = find_section(footer, section_kind::VAL_PAGES)
                    .ok_or(SegmentError::MissingSection("VAL_PAGES"))?;
                let (val_off, val_len) = entry.val_page;
                let val_end = val_off
                    .checked_add(val_len)
                    .ok_or(SegmentError::SectionOutOfBounds)?;
                if val_end > val_section.len {
                    return Err(SegmentError::SectionOutOfBounds);
                }
                let val_abs = val_section
                    .offset
                    .checked_add(val_off)
                    .ok_or(SegmentError::SectionOutOfBounds)?;
                (val_abs, val_len)
            }
            ValueKind::Histogram => (0, 0),
        };

        let hist_range = match entry.value_kind {
            ValueKind::Histogram => {
                let hist_section = find_section(footer, section_kind::HIST_PAGES)
                    .ok_or(SegmentError::MissingSection("HIST_PAGES"))?;
                let (hist_off, hist_len) = entry.hist_page;
                let hist_end = hist_off
                    .checked_add(hist_len)
                    .ok_or(SegmentError::SectionOutOfBounds)?;
                if hist_end > hist_section.len {
                    return Err(SegmentError::SectionOutOfBounds);
                }
                let hist_abs = hist_section
                    .offset
                    .checked_add(hist_off)
                    .ok_or(SegmentError::SectionOutOfBounds)?;
                (hist_abs, hist_len)
            }
            ValueKind::Scalar => (0, 0),
        };

        out.push(PlannedRange {
            series_id: entry.series_id,
            ts_range: (ts_abs, ts_len),
            val_range,
            hist_range,
        });
    }
    Ok(out)
}

fn split_page_header<'a>(
    series_id: &SeriesId,
    page: &'a [u8],
) -> Result<(u8, u8, &'a [u8]), SegmentError> {
    if page.len() < 6 {
        return Err(SegmentError::Truncated);
    }
    let enc = page[0];
    let comp = page[1];
    let crc = u32::from_le_bytes([page[2], page[3], page[4], page[5]]);
    let payload = &page[6..];
    let expected = page_crc(&series_id.0, enc, comp, payload);
    if expected != crc {
        return Err(SegmentError::PageCrcMismatch);
    }
    Ok((enc, comp, payload))
}

/// Decompresses one page payload into a caller-supplied buffer: `out` is
/// cleared, then filled. On error `out`'s contents are unspecified. Lets a
/// caller decoding many pages (one segment fetch) reuse one `Vec<u8>`
/// scratch buffer across pages instead of allocating a fresh decompression
/// buffer per page.
fn decompress_page_payload_into(
    comp: u8,
    payload: &[u8],
    limits: ReaderLimits,
    out: &mut Vec<u8>,
) -> Result<(), SegmentError> {
    match comp {
        page_comp::NONE => {
            // A comp=NONE page's uncompressed size is its stored payload
            // length. Enforce the same per-page cap the LZ4 branch applies to
            // its declared prefix, before copying into `out`, so an oversized
            // page is rejected rather than materialized.
            if payload.len() as u64 > limits.max_page_uncompressed_bytes {
                return Err(SegmentError::PageTooLarge {
                    len: payload.len() as u64,
                    cap: limits.max_page_uncompressed_bytes,
                });
            }
            out.clear();
            out.extend_from_slice(payload);
            Ok(())
        }
        page_comp::LZ4 => {
            if payload.len() < 4 {
                return Err(SegmentError::Truncated);
            }
            let prefix = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if u64::from(prefix) > limits.max_page_uncompressed_bytes {
                return Err(SegmentError::PageTooLarge {
                    len: u64::from(prefix),
                    cap: limits.max_page_uncompressed_bytes,
                });
            }
            let expected = to_usize(u64::from(prefix))?;
            out.clear();
            out.resize(expected, 0);
            let n = lz4_flex::decompress_into(&payload[4..], out)
                .map_err(|e| SegmentError::Decompress(e.to_string()))?;
            if n != expected {
                return Err(SegmentError::DecompressedLenMismatch {
                    expected: u64::from(prefix),
                    actual: n as u64,
                });
            }
            Ok(())
        }
        other => Err(SegmentError::InvalidCompression(other)),
    }
}

fn decode_ts_page_into(
    entry: &SeriesEntry,
    page: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    out: &mut Vec<i64>,
) -> Result<(), SegmentError> {
    let (enc, comp, payload) = split_page_header(&entry.series_id, page)?;
    if enc != page_enc::TS_DELTA_VARINT {
        return Err(SegmentError::InvalidEncoding(enc));
    }
    decompress_page_payload_into(comp, payload, limits, scratch)?;
    let count = to_usize(u64::from(entry.sample_count))?;
    crate::ts_delta::decode_ts_deltas_into(scratch, count, entry.min_ts_ns, entry.max_ts_ns, out)
}

fn decode_raw_f64_into(bytes: &[u8], count: usize, out: &mut Vec<f64>) -> Result<(), SegmentError> {
    let expected_len = count.checked_mul(8).ok_or(SegmentError::FieldOverflow)?;
    if bytes.len() != expected_len {
        return Err(SegmentError::Truncated);
    }
    out.clear();
    out.reserve(count);
    for chunk in bytes.chunks_exact(8) {
        let arr: [u8; 8] = chunk.try_into().map_err(|_| SegmentError::Truncated)?;
        out.push(f64::from_le_bytes(arr));
    }
    Ok(())
}

/// Which encoding a VAL page was actually stored with. Surfaced by the SoA
/// decode path so callers can account for encoding mix
/// (docs/arrow-datafusion-plan.md hop 7: the RSEG v2 alignment decision
/// needs a measured raw-f64 fraction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValPageKind {
    Gorilla,
    RawF64,
}

fn decode_val_page_into(
    entry: &SeriesEntry,
    page: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    out: &mut Vec<f64>,
) -> Result<ValPageKind, SegmentError> {
    let (enc, comp, payload) = split_page_header(&entry.series_id, page)?;
    decompress_page_payload_into(comp, payload, limits, scratch)?;
    let count = to_usize(u64::from(entry.sample_count))?;
    match enc {
        page_enc::VAL_GORILLA => {
            crate::gorilla::decode_gorilla_into(scratch, count, out)?;
            Ok(ValPageKind::Gorilla)
        }
        page_enc::VAL_RAW_F64 => {
            decode_raw_f64_into(scratch, count, out)?;
            Ok(ValPageKind::RawF64)
        }
        other => Err(SegmentError::InvalidEncoding(other)),
    }
}

/// Decodes one series' TS and VAL pages into samples, verifying page crc
/// (with the series_id prefix binding), enc/comp validity, timestamp
/// accumulation (overflow-checked, bounds-checked against the entry).
/// Preserves on-disk order, including duplicate timestamps.
pub fn decode_pages(
    entry: &SeriesEntry,
    ts_page_bytes: &[u8],
    val_page_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<Sample>, SegmentError> {
    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    decode_pages_soa(
        entry,
        ts_page_bytes,
        val_page_bytes,
        limits,
        &mut scratch,
        &mut timestamps,
        &mut values,
    )?;
    Ok(timestamps
        .into_iter()
        .zip(values)
        .map(|(ts_ns, value)| Sample { ts_ns, value })
        .collect())
}

/// Decodes one series' TS and VAL pages directly into separate
/// timestamp/value vecs (SoA) instead of `Vec<Sample>` (AoS). Same
/// validation contract as [`decode_pages`] (page crc, enc/comp validity,
/// overflow- and bounds-checked timestamp accumulation, on-disk order
/// including duplicate timestamps preserved) and produces bit-identical
/// output to it for the same input (verified by proptest in this crate's
/// test suite).
///
/// `scratch` is an internal decompression buffer, cleared and refilled for
/// the TS page and again for the VAL page; its contents never outlive one
/// call, so a caller decoding many series in one segment fetch should pass
/// the same `scratch` buffer to every call and let it reuse its allocation
/// instead of allocating a fresh decompression buffer per page.
/// `timestamps`/`values` are the per-series decode *output*: they are
/// cleared, then filled by this call, and the caller owns them from
/// there (typically moving them into that series' result) rather than
/// reusing them across series. On error, `timestamps`/`values` contents
/// are unspecified.
///
/// This is a committed public API: crates/ravel-sql (Phase B of
/// docs/arrow-datafusion-plan.md) and later consumers build Arrow arrays
/// directly off `timestamps`/`values` by buffer adoption, so its signature
/// and semantics are stable.
///
/// Returns the [`ValPageKind`] the VAL page was actually encoded with, for
/// fetch-stats accounting.
pub fn decode_pages_soa(
    entry: &SeriesEntry,
    ts_page_bytes: &[u8],
    val_page_bytes: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> Result<ValPageKind, SegmentError> {
    decode_ts_page_into(entry, ts_page_bytes, limits, scratch, timestamps)?;
    let val_kind = decode_val_page_into(entry, val_page_bytes, limits, scratch, values)?;
    if timestamps.len() != values.len() {
        return Err(SegmentError::Truncated);
    }
    Ok(val_kind)
}

// --- RSEG v3 HIST_PAGES decode (ADR-0017, docs/rseg-v3-plan.md section
// 3.5). Byte-for-byte inverse of `encode_histogram_record_into`
// (histogram.rs); every business-logic check that function's doc comment
// defers to "the future reader" is enforced here, since every byte read
// here is untrusted.

fn take_u8(bytes: &[u8], pos: &mut usize) -> Result<u8, SegmentError> {
    let s = take_bytes(bytes, pos, 1)?;
    Ok(s[0])
}

fn take_f64_le(bytes: &[u8], pos: &mut usize) -> Result<f64, SegmentError> {
    let s = take_bytes(bytes, pos, 8)?;
    let arr: [u8; 8] = s.try_into().map_err(|_| SegmentError::Truncated)?;
    Ok(f64::from_le_bytes(arr))
}

/// Decodes one histogram side's spans (docs/rseg-v3-plan.md section 3.5):
/// `uvarint span_count`, then `span_count` pairs of `(zigzag varint offset,
/// uvarint length)`. Every `length` MUST be `> 0`. Returns the spans and
/// the side's total bucket count (`sum(length)`, overflow-checked).
fn decode_hist_spans(
    bytes: &[u8],
    pos: &mut usize,
) -> Result<(Vec<HistogramSpan>, u64), SegmentError> {
    let span_count = to_usize(read_uvarint(bytes, pos)?)?;
    let mut spans = Vec::with_capacity(span_count.min(bytes.len()));
    let mut total: u64 = 0;
    for _ in 0..span_count {
        let offset = read_zigzag_varint(bytes, pos)?;
        let offset = i32::try_from(offset).map_err(|_| SegmentError::FieldOverflow)?;
        let length = read_uvarint(bytes, pos)?;
        if length == 0 {
            return Err(SegmentError::HistogramSpanLengthZero);
        }
        let length_u32 = u32::try_from(length).map_err(|_| SegmentError::FieldOverflow)?;
        total = total
            .checked_add(length)
            .ok_or(SegmentError::FieldOverflow)?;
        spans.push(HistogramSpan {
            offset,
            length: length_u32,
        });
    }
    Ok((spans, total))
}

fn decode_hist_int_counts(bytes: &[u8], pos: &mut usize, n: u64) -> Result<Vec<u64>, SegmentError> {
    let n = to_usize(n)?;
    let mut out = Vec::with_capacity(n.min(bytes.len()));
    for _ in 0..n {
        out.push(read_uvarint(bytes, pos)?);
    }
    Ok(out)
}

fn decode_hist_float_counts(
    bytes: &[u8],
    pos: &mut usize,
    n: u64,
) -> Result<Vec<f64>, SegmentError> {
    let n = to_usize(n)?;
    let mut out = Vec::with_capacity(n.min(bytes.len()));
    for _ in 0..n {
        out.push(take_f64_le(bytes, pos)?);
    }
    Ok(out)
}

/// Decodes one HIST_SPANS record (docs/rseg-v3-plan.md section 3.5),
/// enforcing every Corrupted rule: reserved flag bits zero, `scale >=
/// -53`, `custom_values` present (non-empty, strictly ascending) iff
/// `scale == -53`, every span `length > 0`, and `count >= zero_count`
/// and `>= sum(all bucket_counts, both sides)`. The count check uses
/// `<` (not `!(>=)`) for the float case so NaN/Inf payloads -- legal
/// per section 3.5 -- pass through unchanged rather than being
/// rejected (NaN comparisons are always false, matching the Gorilla
/// page's NaN/-0.0 transparency rule).
fn decode_histogram_record(bytes: &[u8], pos: &mut usize) -> Result<HistogramValue, SegmentError> {
    let flags = take_u8(bytes, pos)?;
    if flags >> 4 != 0 {
        return Err(SegmentError::HistogramReservedFlagsNonZero);
    }
    let count_kind = flags & 0b1;
    let has_sum = (flags >> 1) & 0b1 == 1;
    let reset_hint = match (flags >> 2) & 0b11 {
        0 => ResetHint::Unknown,
        1 => ResetHint::Yes,
        2 => ResetHint::No,
        _ => ResetHint::Gauge,
    };

    let scale =
        i32::try_from(read_zigzag_varint(bytes, pos)?).map_err(|_| SegmentError::FieldOverflow)?;
    if scale < -53 {
        return Err(SegmentError::HistogramScaleTooSmall(scale));
    }
    let zero_threshold = take_f64_le(bytes, pos)?;

    let (zero_count_u64, count_u64, zero_count_f64, count_f64) = if count_kind == 0 {
        (
            read_uvarint(bytes, pos)?,
            read_uvarint(bytes, pos)?,
            0.0,
            0.0,
        )
    } else {
        (0, 0, take_f64_le(bytes, pos)?, take_f64_le(bytes, pos)?)
    };

    let sum = if has_sum {
        Some(take_f64_le(bytes, pos)?)
    } else {
        None
    };

    let custom_values = if scale == -53 {
        let n = to_usize(read_uvarint(bytes, pos)?)?;
        if n == 0 {
            return Err(SegmentError::HistogramCustomValuesMismatch);
        }
        let mut bounds = Vec::with_capacity(n.min(bytes.len()));
        for _ in 0..n {
            bounds.push(take_f64_le(bytes, pos)?);
        }
        if !bounds.windows(2).all(|w| w[0] < w[1]) {
            return Err(SegmentError::HistogramCustomValuesMismatch);
        }
        Some(bounds)
    } else {
        None
    };

    let (positive_spans, positive_len) = decode_hist_spans(bytes, pos)?;
    let (counts, negative_spans) = if count_kind == 0 {
        let positive = decode_hist_int_counts(bytes, pos, positive_len)?;
        let (negative_spans, negative_len) = decode_hist_spans(bytes, pos)?;
        let negative = decode_hist_int_counts(bytes, pos, negative_len)?;
        let mut total: u64 = 0;
        for &v in positive.iter().chain(negative.iter()) {
            total = total
                .checked_add(v)
                .ok_or(SegmentError::HistogramCountInconsistent)?;
        }
        if count_u64 < zero_count_u64 || count_u64 < total {
            return Err(SegmentError::HistogramCountInconsistent);
        }
        (
            HistogramCounts::Int {
                zero_count: zero_count_u64,
                count: count_u64,
                positive,
                negative,
            },
            negative_spans,
        )
    } else {
        let positive = decode_hist_float_counts(bytes, pos, positive_len)?;
        let (negative_spans, negative_len) = decode_hist_spans(bytes, pos)?;
        let negative = decode_hist_float_counts(bytes, pos, negative_len)?;
        let mut total = 0.0f64;
        for &v in positive.iter().chain(negative.iter()) {
            total += v;
        }
        if count_f64 < zero_count_f64 || count_f64 < total {
            return Err(SegmentError::HistogramCountInconsistent);
        }
        (
            HistogramCounts::Float {
                zero_count: zero_count_f64,
                count: count_f64,
                positive,
                negative,
            },
            negative_spans,
        )
    };

    Ok(HistogramValue {
        scale,
        zero_threshold,
        sum,
        custom_values,
        positive_spans,
        negative_spans,
        counts,
        reset_hint,
    })
}

/// Decodes one HIST_SPANS page (docs/rseg-v3-plan.md section 3.5): verifies
/// the page header/crc, requires `enc == HIST_SPANS`, decompresses via the
/// generic page-payload path (`comp` is writer policy, not a format
/// restriction -- a HIST page may legally use the same `comp` enum as
/// VAL/TS pages), then decodes exactly `entry.sample_count` records and
/// requires the payload to end exactly there.
fn decode_hist_page_into(
    entry: &SeriesEntry,
    page: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    out: &mut Vec<HistogramValue>,
) -> Result<(), SegmentError> {
    let (enc, comp, payload) = split_page_header(&entry.series_id, page)?;
    if enc != page_enc::HIST_SPANS {
        return Err(SegmentError::InvalidEncoding(enc));
    }
    decompress_page_payload_into(comp, payload, limits, scratch)?;
    let count = to_usize(u64::from(entry.sample_count))?;
    out.clear();
    out.reserve(count.min(scratch.len()));
    let mut pos = 0usize;
    for _ in 0..count {
        out.push(decode_histogram_record(scratch, &mut pos)?);
    }
    if pos != scratch.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(())
}

/// Decodes one histogram series' TS and HIST pages into samples, verifying
/// page crc (with the series_id prefix binding), enc/comp validity, and
/// every HIST_SPANS record's Corrupted rule (docs/rseg-v3-plan.md section
/// 3.5). Preserves on-disk order, including duplicate timestamps. The
/// histogram counterpart of [`decode_pages`].
pub fn decode_histogram_pages(
    entry: &SeriesEntry,
    ts_page_bytes: &[u8],
    hist_page_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<HistogramSample>, SegmentError> {
    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    decode_ts_page_into(entry, ts_page_bytes, limits, &mut scratch, &mut timestamps)?;
    let mut values = Vec::new();
    decode_hist_page_into(entry, hist_page_bytes, limits, &mut scratch, &mut values)?;
    if timestamps.len() != values.len() {
        return Err(SegmentError::Truncated);
    }
    Ok(timestamps
        .into_iter()
        .zip(values)
        .map(|(ts_ns, value)| HistogramSample { ts_ns, value })
        .collect())
}

// --- RSEG v2 decode path (ADR-0014, docs/segment-format.md "RSEG v2
// amendment", docs/rseg-v2-plan.md phase P3, issue #31). SERIES_IDS +
// SERIES_META decode producing the same `SeriesEntry` shape v1's
// `decode_catalog`/`decode_catalog_matching` already produce.
//
// Reuse boundary: this path calls none of v1's *catalog decoders*
// (`decode_catalog`, `decode_catalog_matching`, `scan_series_table`) --
// those stay untouched and v1 behavior stays provable by inspection, same
// discipline as P2's writer split. It DOES reuse the version-agnostic
// primitives already shared by every section kind in both versions today
// (`find_section`, `decode_section_bytes`, `index_label_dict`, `dict_str`,
// `take_*`, `to_usize`): the LABEL_DICT grammar and the section
// compression envelope are unchanged in v2 (only LABEL_DICT's ordering
// *rule* is relaxed, which none of these functions assume), so reusing
// them cannot let a v2 change leak into v1 behavior, and duplicating them
// would only invite drift. This is looser than P2's writer, which
// duplicated even behavior-identical helpers (`append_ts_page_v2`,
// `zstd_compress_v2`) for the same page/section-envelope logic; the
// difference is that those write TS_PAGES/VAL_PAGES bytes (a place a
// future v2-only change is plausible), while the primitives reused here
// are pure decode-side envelope/parsing code with no v1/v2 branch inside
// them to ever diverge.

/// One SERIES_META schema entry: LABEL_DICT ordinals for this schema's
/// label names, validated at decode time to be strictly ascending by the
/// referenced dictionary string's bytes (docs/segment-format.md v2
/// amendment).
struct SchemaMetaV2 {
    name_ords: Vec<u64>,
}

/// Per-series output of SERIES_META's blocks 3-9 (docs/segment-format.md
/// v2 amendment): identical work for the eager and lazy decode paths (no
/// per-series skipping is possible here, unlike block 2 / value_ord, since
/// every column is exactly one varint per series regardless of schema).
struct SeriesMetaTailV2 {
    sample_count: Vec<u32>,
    min_ts_ns: Vec<i64>,
    max_ts_ns: Vec<i64>,
    ts_page: Vec<(u64, u64)>,
    val_page: Vec<(u64, u64)>,
}

/// Reads a `block_len: varint` prefix, then slices out exactly that many
/// bytes (`Truncated` if the section doesn't have that many bytes left).
fn take_block<'a>(bytes: &'a [u8], pos: &mut usize) -> Result<&'a [u8], SegmentError> {
    let block_len = read_uvarint(bytes, pos)?;
    let block_len = to_usize(block_len)?;
    take_bytes(bytes, pos, block_len)
}

/// Decodes SERIES_IDS (kind 5, v2 only, docs/segment-format.md): `count:
/// u32` then `count` 16-byte ids, strictly ascending by byte comparison.
/// Rejects non-ascending ids and any trailing bytes (equivalently, any
/// section length other than exactly `4 + 16*count`).
fn parse_series_ids_v2(bytes: &[u8]) -> Result<Vec<[u8; 16]>, SegmentError> {
    let mut pos = 0usize;
    let count = take_u32_le(bytes, &mut pos)?;
    let mut out = Vec::with_capacity((count as usize).min(bytes.len() / 16));
    let mut prev: Option<[u8; 16]> = None;
    for _ in 0..count {
        let id = take_array16(bytes, &mut pos)?;
        if let Some(p) = prev
            && id <= p
        {
            return Err(SegmentError::SeriesIdsUnsorted);
        }
        prev = Some(id);
        out.push(id);
    }
    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(out)
}

/// Reads SERIES_META's `count: u32`, `schema_count: u32`, and the schema
/// dictionary (docs/segment-format.md v2 amendment), returning `count` and
/// the decoded schemas. Each schema's `name_ord` sequence is validated to
/// be in-range LABEL_DICT ordinals, strictly ascending by the *referenced
/// dictionary string's bytes* (not by ordinal value -- v2's LABEL_DICT
/// ordering is unspecified past ordinal 0, so ordinal order and name-byte
/// order do not coincide the way they do in v1). Byte comparison (not
/// `dict_str`) so this never UTF-8-validates a name that a caller doesn't
/// go on to materialize (matches v1's `decode_catalog_matching` contract
/// that non-materialized dictionary strings are never UTF-8-checked).
fn parse_schema_list_v2(
    meta_bytes: &[u8],
    pos: &mut usize,
    dict_bytes: &[u8],
    dict_index: &[(usize, usize)],
) -> Result<(u32, Vec<SchemaMetaV2>), SegmentError> {
    let count = take_u32_le(meta_bytes, pos)?;
    let schema_count = take_u32_le(meta_bytes, pos)?;
    let mut schemas = Vec::with_capacity((schema_count as usize).min(meta_bytes.len()));
    for _ in 0..schema_count {
        let name_count = read_uvarint(meta_bytes, pos)?;
        if name_count > 65_535 {
            return Err(SegmentError::SchemaNameCountTooLarge(name_count));
        }
        let name_count_usize = to_usize(name_count)?;
        let mut name_ords = Vec::with_capacity(name_count_usize.min(meta_bytes.len()));
        let mut prev_range: Option<(usize, usize)> = None;
        for _ in 0..name_count_usize {
            let name_ord = read_uvarint(meta_bytes, pos)?;
            let idx = to_usize(name_ord)?;
            let range = *dict_index
                .get(idx)
                .ok_or(SegmentError::BadOrdinal(name_ord))?;
            if let Some(prev) = prev_range {
                let prev_bytes = dict_bytes
                    .get(prev.0..prev.0 + prev.1)
                    .ok_or(SegmentError::Truncated)?;
                let cur_bytes = dict_bytes
                    .get(range.0..range.0 + range.1)
                    .ok_or(SegmentError::Truncated)?;
                if cur_bytes <= prev_bytes {
                    return Err(SegmentError::SchemaNamesUnsorted);
                }
            }
            prev_range = Some(range);
            name_ords.push(name_ord);
        }
        schemas.push(SchemaMetaV2 { name_ords });
    }
    Ok((count, schemas))
}

/// Checks the v2 amendment's count-equality rule: SERIES_IDS `count`,
/// SERIES_META `count`, and `Footer.series_count` must all be equal.
fn check_series_counts_v2(
    series_ids: u64,
    series_meta: u64,
    footer_count: u64,
) -> Result<(), SegmentError> {
    if series_ids != series_meta || series_ids != footer_count {
        return Err(SegmentError::SeriesCountMismatch {
            series_ids,
            series_meta,
            footer: footer_count,
        });
    }
    Ok(())
}

/// SERIES_META block 1 (`schema_ref`, docs/segment-format.md v2
/// amendment): `count` varints, each `< schema_count`.
fn parse_schema_ref_block_v2(
    meta_bytes: &[u8],
    pos: &mut usize,
    count: u32,
    schema_count: u32,
) -> Result<Vec<u32>, SegmentError> {
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut out = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        let v = read_uvarint(block, &mut bpos)?;
        let idx = u32::try_from(v)
            .ok()
            .filter(|&i| i < schema_count)
            .ok_or(SegmentError::SchemaRefOutOfRange(v))?;
        out.push(idx);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    Ok(out)
}

/// SERIES_META block 2 (`value_ord`, docs/segment-format.md v2 amendment),
/// eager form: decodes every series' value ordinals into one flat vector
/// (series-major, `name_count(schema)` varints per series, concatenated).
/// Used by [`decode_catalog_v2`], which always materializes every series.
///
/// Flat rather than `Vec<Vec<u64>>`: the eager path materializes all
/// `count` series, so a per-series inner vector would be one heap
/// allocation per series (10k allocations in the bench shape). A single
/// flat vector, walked at materialization time with a running cursor over
/// each series' `name_ords.len()` slice, removes those allocations while
/// preserving the exact series-major order the block already stores. This
/// was the residual half of the RSEG v2 eager-decode regression (issue #94)
/// after per-reference dictionary UTF-8 revalidation was fixed.
fn parse_value_ord_block_all_v2(
    meta_bytes: &[u8],
    pos: &mut usize,
    count: u32,
    schema_ref: &[u32],
    schemas: &[SchemaMetaV2],
) -> Result<Vec<u64>, SegmentError> {
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    // Each ordinal is at least one varint byte, so the flat count can never
    // validly exceed the block length; cap the single reservation there.
    let mut out = Vec::with_capacity(block.len());
    for &sref in schema_ref.iter().take(count as usize) {
        let schema = &schemas[sref as usize];
        for _ in 0..schema.name_ords.len() {
            out.push(read_uvarint(block, &mut bpos)?);
        }
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    Ok(out)
}

/// SERIES_META block 2 (`value_ord`), selective/lazy form: for a series
/// whose `schema_plausible[schema_ref[i]]` is `false`, the value ordinals
/// are walked (to keep the shared byte position correct) but never stored
/// or dictionary-resolved -- the "column skipping" docs/rseg-v2-plan.md
/// describes, computed once per schema by the caller rather than once per
/// series. Returns `None` for skipped series, `Some(vals)` otherwise.
fn parse_value_ord_block_selective_v2(
    meta_bytes: &[u8],
    pos: &mut usize,
    count: u32,
    schema_ref: &[u32],
    schemas: &[SchemaMetaV2],
    schema_plausible: &[bool],
) -> Result<Vec<Option<Vec<u64>>>, SegmentError> {
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut out = Vec::with_capacity((count as usize).min(block.len()));
    for &sref_u32 in schema_ref.iter().take(count as usize) {
        let sref = sref_u32 as usize;
        let schema = &schemas[sref];
        if schema_plausible[sref] {
            let mut vals = Vec::with_capacity(schema.name_ords.len().min(block.len()));
            for _ in 0..schema.name_ords.len() {
                vals.push(read_uvarint(block, &mut bpos)?);
            }
            out.push(Some(vals));
        } else {
            for _ in 0..schema.name_ords.len() {
                read_uvarint(block, &mut bpos)?;
            }
            out.push(None);
        }
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    Ok(out)
}

/// Reconstructs `(offset, len)` ranges from a gap/len running-sum column
/// pair (docs/segment-format.md v2 amendment): `offset_i = end_{i-1} +
/// gap_i`, `end_i = offset_i + len_i`, checked, `end_i <= section_len`.
/// Shared by the TS (blocks 6/7 over TS_PAGES) and VAL (blocks 8/9 over
/// VAL_PAGES) column pairs. Error choice mirrors `plan_ranges`' precedent
/// for v1: `SectionOutOfBounds` for both arithmetic overflow and a range
/// exceeding its section's length.
fn reconstruct_ranges_v2(
    gaps: &[u64],
    lens: &[u64],
    section_len: u64,
) -> Result<Vec<(u64, u64)>, SegmentError> {
    let mut out = Vec::with_capacity(gaps.len());
    let mut end = 0u64;
    for (&gap, &len) in gaps.iter().zip(lens) {
        let offset = end
            .checked_add(gap)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        let new_end = offset
            .checked_add(len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if new_end > section_len {
            return Err(SegmentError::SectionOutOfBounds);
        }
        out.push((offset, len));
        end = new_end;
    }
    Ok(out)
}

/// SERIES_META blocks 3-9 (docs/segment-format.md v2 amendment):
/// `sample_count` (must be non-zero, must fit u32), `min_ts_delta`/
/// `ts_span` (reconstructed into absolute `min_ts_ns`/`max_ts_ns` via
/// `footer.min_event_ts_ns`, i128 intermediate so the check is exactly
/// "does the reconstructed i64 fit", not an artifact of doing the addition
/// in a narrower type), and the TS/VAL page gap/len column pairs
/// (reconstructed via [`reconstruct_ranges_v2`] against each section's own
/// `len`). Also performs the final "no trailing bytes past block 9" check
/// for the whole SERIES_META payload. Identical work regardless of which
/// series end up materialized, so this is shared by the eager and lazy
/// decode paths.
fn parse_series_meta_tail_v2(
    meta_bytes: &[u8],
    pos: &mut usize,
    footer: &Footer,
    count: u32,
) -> Result<SeriesMetaTailV2, SegmentError> {
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut sample_count = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        let v = read_uvarint(block, &mut bpos)?;
        let v = u32::try_from(v).map_err(|_| SegmentError::FieldOverflow)?;
        if v == 0 {
            return Err(SegmentError::ZeroSampleCount);
        }
        sample_count.push(v);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut min_ts_ns = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        let delta = read_uvarint(block, &mut bpos)?;
        let sum = i128::from(footer.min_event_ts_ns) + i128::from(delta);
        min_ts_ns.push(i64::try_from(sum).map_err(|_| SegmentError::TimestampBoundsOverflow)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut max_ts_ns = Vec::with_capacity((count as usize).min(block.len()));
    for &min_ts in min_ts_ns.iter().take(count as usize) {
        let span = read_uvarint(block, &mut bpos)?;
        let sum = i128::from(min_ts) + i128::from(span);
        max_ts_ns.push(i64::try_from(sum).map_err(|_| SegmentError::TimestampBoundsOverflow)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let ts_section = find_section(footer, section_kind::TS_PAGES)
        .ok_or(SegmentError::MissingSection("TS_PAGES"))?;
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut ts_gap = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        ts_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut ts_len = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        ts_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let ts_page = reconstruct_ranges_v2(&ts_gap, &ts_len, ts_section.len)?;

    let val_section = find_section(footer, section_kind::VAL_PAGES)
        .ok_or(SegmentError::MissingSection("VAL_PAGES"))?;
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut val_gap = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        val_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut val_len = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        val_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let val_page = reconstruct_ranges_v2(&val_gap, &val_len, val_section.len)?;

    if *pos != meta_bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }

    Ok(SeriesMetaTailV2 {
        sample_count,
        min_ts_ns,
        max_ts_ns,
        ts_page,
        val_page,
    })
}

/// Decodes LABEL_DICT + SERIES_IDS + SERIES_META (verifying section crcs)
/// into [`SeriesEntry`] values with materialized [`LabelSet`]s. The v2
/// counterpart of [`decode_catalog`]: same output shape, same eager
/// (everyone materialized) semantics.
pub fn decode_catalog_v2(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_ids_bytes: &[u8],
    series_meta_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntry>, SegmentError> {
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let series_ids_section = find_section(footer, section_kind::SERIES_IDS)
        .ok_or(SegmentError::MissingSection("SERIES_IDS"))?;
    let series_meta_section = find_section(footer, section_kind::SERIES_META)
        .ok_or(SegmentError::MissingSection("SERIES_META"))?;

    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let ids_bytes = decode_section_bytes(series_ids_section, series_ids_bytes, limits)?;
    let meta_bytes = decode_section_bytes(series_meta_section, series_meta_bytes, limits)?;

    let dict_index = index_label_dict(&dict_bytes)?;
    let series_ids = parse_series_ids_v2(&ids_bytes)?;
    let series_ids_count = series_ids.len() as u64;

    let mut pos = 0usize;
    let (meta_count, schemas) =
        parse_schema_list_v2(&meta_bytes, &mut pos, &dict_bytes, &dict_index)?;
    check_series_counts_v2(series_ids_count, u64::from(meta_count), footer.series_count)?;

    let schema_count = schemas.len() as u32;
    let schema_ref = parse_schema_ref_block_v2(&meta_bytes, &mut pos, meta_count, schema_count)?;
    let value_ord =
        parse_value_ord_block_all_v2(&meta_bytes, &mut pos, meta_count, &schema_ref, &schemas)?;
    let tail = parse_series_meta_tail_v2(&meta_bytes, &mut pos, footer, meta_count)?;

    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let mut entries = Vec::with_capacity(series_ids.len());
    // Running cursor over the flat value_ord vector: series `i` owns the
    // next `schema.name_ords.len()` ordinals, in the same series-major order
    // the block stores. `parse_value_ord_block_all_v2` consumed exactly the
    // sum of these lengths, so the slicing below stays in bounds for every
    // series and ends exactly at `value_ord.len()`.
    let mut voff = 0usize;
    for i in 0..series_ids.len() {
        let schema = &schemas[schema_ref[i] as usize];
        let n = schema.name_ords.len();
        let vals = &value_ord[voff..voff + n];
        voff += n;
        let mut label_pairs = Vec::with_capacity(n);
        for (&name_ord, &val_ord) in schema.name_ords.iter().zip(vals) {
            let name = resolver.get(name_ord)?;
            let value = resolver.get(val_ord)?;
            label_pairs.push(Label {
                name: name.to_string(),
                value: value.to_string(),
            });
        }
        let label_set = LabelSet::new(label_pairs)?;
        entries.push(SeriesEntry {
            series_id: SeriesId(series_ids[i]),
            labels: label_set,
            sample_count: tail.sample_count[i],
            min_ts_ns: tail.min_ts_ns[i],
            max_ts_ns: tail.max_ts_ns[i],
            ts_page: tail.ts_page[i],
            val_page: tail.val_page[i],
            value_kind: ValueKind::Scalar,
            hist_page: (0, 0),
        });
    }
    Ok(entries)
}

/// Decodes only the series whose labels satisfy every `(name, value)`
/// equality in `equals`, the v2 counterpart of [`decode_catalog_matching`].
///
/// Matcher name/value strings are resolved to LABEL_DICT ordinals once, up
/// front, by the same linear byte-equality scan
/// [`decode_catalog_matching`] uses (`dict_index.iter().position(...)`):
/// that scan never assumed any dictionary ordering (it's a raw byte-equal
/// search over every entry), so it translates directly to v2's
/// first-occurrence-order dictionary with no change in approach.
///
/// Column skipping (docs/rseg-v2-plan.md §3.4): a schema is "plausible"
/// only if every matcher name ordinal appears somewhere in that schema's
/// `name_ords` -- computed once per distinct schema, not once per series.
/// For series whose schema is implausible, block 2 (`value_ord`) is walked
/// but never stored or dictionary-resolved. Schema-plausible series still
/// need a per-series value check (existence of the right name only proves
/// the label *could* be set to the right value, not that it is), and the
/// whole structure (blocks 1, 3-9) is still parsed and validated for every
/// series regardless of match, exactly as [`decode_catalog_matching`]
/// still structurally validates every v1 row.
pub fn decode_catalog_matching_v2(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_ids_bytes: &[u8],
    series_meta_bytes: &[u8],
    equals: &[(&str, &str)],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntry>, SegmentError> {
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let series_ids_section = find_section(footer, section_kind::SERIES_IDS)
        .ok_or(SegmentError::MissingSection("SERIES_IDS"))?;
    let series_meta_section = find_section(footer, section_kind::SERIES_META)
        .ok_or(SegmentError::MissingSection("SERIES_META"))?;

    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let ids_bytes = decode_section_bytes(series_ids_section, series_ids_bytes, limits)?;
    let meta_bytes = decode_section_bytes(series_meta_section, series_meta_bytes, limits)?;

    let dict_index = index_label_dict(&dict_bytes)?;
    let series_ids = parse_series_ids_v2(&ids_bytes)?;
    let series_ids_count = series_ids.len() as u64;

    let mut pos = 0usize;
    let (meta_count, schemas) =
        parse_schema_list_v2(&meta_bytes, &mut pos, &dict_bytes, &dict_index)?;
    check_series_counts_v2(series_ids_count, u64::from(meta_count), footer.series_count)?;

    // Resolve each matcher pair to dictionary ordinals by raw byte
    // equality, identical in approach to decode_catalog_matching's
    // find_ordinal (equal bytes imply equal UTF-8; a matcher string absent
    // from the dictionary can never match any entry).
    let find_ordinal = |needle: &str| -> Option<u64> {
        let needle = needle.as_bytes();
        dict_index
            .iter()
            .position(|&(start, len)| {
                dict_bytes
                    .get(start..start + len)
                    .is_some_and(|s| s == needle)
            })
            .map(|i| i as u64)
    };
    let mut matcher_ords: Vec<(u64, u64)> = Vec::with_capacity(equals.len());
    let mut resolvable = true;
    for (name, value) in equals {
        match (find_ordinal(name), find_ordinal(value)) {
            (Some(n), Some(v)) => matcher_ords.push((n, v)),
            _ => {
                resolvable = false;
                break;
            }
        }
    }

    let schema_count = schemas.len() as u32;
    let schema_ref = parse_schema_ref_block_v2(&meta_bytes, &mut pos, meta_count, schema_count)?;

    let schema_plausible: Vec<bool> = if resolvable {
        schemas
            .iter()
            .map(|schema| {
                matcher_ords
                    .iter()
                    .all(|(name_ord, _)| schema.name_ords.contains(name_ord))
            })
            .collect()
    } else {
        vec![false; schemas.len()]
    };

    let value_ord = parse_value_ord_block_selective_v2(
        &meta_bytes,
        &mut pos,
        meta_count,
        &schema_ref,
        &schemas,
        &schema_plausible,
    )?;
    let tail = parse_series_meta_tail_v2(&meta_bytes, &mut pos, footer, meta_count)?;

    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let mut entries = Vec::new();
    for i in 0..series_ids.len() {
        let Some(vals) = &value_ord[i] else {
            continue;
        };
        let schema = &schemas[schema_ref[i] as usize];
        let is_match = matcher_ords.iter().all(|(name_ord, wanted_val_ord)| {
            schema
                .name_ords
                .iter()
                .position(|n| n == name_ord)
                .is_some_and(|j| vals[j] == *wanted_val_ord)
        });
        if !is_match {
            continue;
        }
        let mut label_pairs = Vec::with_capacity(schema.name_ords.len());
        for (&name_ord, &val_ord) in schema.name_ords.iter().zip(vals) {
            let name = resolver.get(name_ord)?;
            let value = resolver.get(val_ord)?;
            label_pairs.push(Label {
                name: name.to_string(),
                value: value.to_string(),
            });
        }
        let label_set = LabelSet::new(label_pairs)?;
        entries.push(SeriesEntry {
            series_id: SeriesId(series_ids[i]),
            labels: label_set,
            sample_count: tail.sample_count[i],
            min_ts_ns: tail.min_ts_ns[i],
            max_ts_ns: tail.max_ts_ns[i],
            ts_page: tail.ts_page[i],
            val_page: tail.val_page[i],
            value_kind: ValueKind::Scalar,
            hist_page: (0, 0),
        });
    }
    Ok(entries)
}

// --- RSEG v3 SERIES_IDS/SERIES_META decode (ADR-0017, docs/rseg-v3-plan.md
// section 3.4). Reuses every version-agnostic v2 primitive (`take_block`,
// `parse_series_ids_v2`, `parse_schema_list_v2`, `check_series_counts_v2`,
// `parse_schema_ref_block_v2`, `parse_value_ord_block_all_v2`/
// `_selective_v2`, `reconstruct_ranges_v2`) exactly as v2's SERIES_IDS/
// LABEL_DICT grammar and section-compression envelope are unchanged in v3;
// only SERIES_META's tail (blocks 3-12, vs. v2's blocks 3-9) and the
// catalog decoders themselves are new, mirroring the reuse boundary v2's
// own decode path documents above.

/// Per-series output of SERIES_META's v3 blocks 3-12 (docs/rseg-v3-plan.md
/// section 3.4): the shared blocks 3-9 (identical grammar to v2's
/// `SeriesMetaTailV2`) plus the three v3-only blocks (`value_kind`,
/// `hist_page_gap`, `hist_page_len`).
struct SeriesMetaTailV3 {
    sample_count: Vec<u32>,
    min_ts_ns: Vec<i64>,
    max_ts_ns: Vec<i64>,
    ts_page: Vec<(u64, u64)>,
    val_page: Vec<(u64, u64)>,
    value_kind: Vec<ValueKind>,
    hist_page: Vec<(u64, u64)>,
}

/// SERIES_META blocks 3-12 (docs/rseg-v3-plan.md section 3.4): identical
/// work to [`parse_series_meta_tail_v2`] for blocks 3-9, then the three
/// v3-only columns. VAL_PAGES/HIST_PAGES are each optional in v3 (section
/// 3.2), so their gap/len columns are reconstructed against the section's
/// `len` when present, or against `0` when absent -- a series with no
/// bytes in that kind (gap=0, len=0) reconstructs to `(0, 0)` either way,
/// while a series claiming bytes in a wholly absent section fails the
/// existing `SectionOutOfBounds` bounds check. The value_kind-vs-page
/// cross-check (`ScalarSeriesHasHistPage`, `HistogramSeriesHasValPage`,
/// `ZeroHistPageLen`) runs after all three new columns are parsed, moving
/// the final trailing-bytes check to after block 12. The aggregate
/// value_kind-count-vs-page-count equality check and the
/// section-present-but-unneeded check (`ValueKindPageCountMismatch`,
/// `UnexpectedSectionPresent`) need the footer's section list alongside
/// the fully parsed tail, so those run in the caller (`decode_catalog_v3`/
/// `decode_catalog_matching_v3`) via `check_value_kind_pages_v3`, not here.
fn parse_series_meta_tail_v3(
    meta_bytes: &[u8],
    pos: &mut usize,
    footer: &Footer,
    count: u32,
) -> Result<SeriesMetaTailV3, SegmentError> {
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut sample_count = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        let v = read_uvarint(block, &mut bpos)?;
        let v = u32::try_from(v).map_err(|_| SegmentError::FieldOverflow)?;
        if v == 0 {
            return Err(SegmentError::ZeroSampleCount);
        }
        sample_count.push(v);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut min_ts_ns = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        let delta = read_uvarint(block, &mut bpos)?;
        let sum = i128::from(footer.min_event_ts_ns) + i128::from(delta);
        min_ts_ns.push(i64::try_from(sum).map_err(|_| SegmentError::TimestampBoundsOverflow)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut max_ts_ns = Vec::with_capacity((count as usize).min(block.len()));
    for &min_ts in min_ts_ns.iter().take(count as usize) {
        let span = read_uvarint(block, &mut bpos)?;
        let sum = i128::from(min_ts) + i128::from(span);
        max_ts_ns.push(i64::try_from(sum).map_err(|_| SegmentError::TimestampBoundsOverflow)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let ts_section = find_section(footer, section_kind::TS_PAGES)
        .ok_or(SegmentError::MissingSection("TS_PAGES"))?;
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut ts_gap = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        ts_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut ts_len = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        ts_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let ts_page = reconstruct_ranges_v2(&ts_gap, &ts_len, ts_section.len)?;

    // VAL_PAGES is optional in v3: absent (zero scalar series) is treated
    // as a zero-length region, so a series legitimately carrying (0, 0)
    // here still reconstructs cleanly, while a series claiming real bytes
    // against an absent section fails the same bounds check v2 relies on.
    let val_section_len = find_section(footer, section_kind::VAL_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut val_gap = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        val_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut val_len = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        val_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let val_page = reconstruct_ranges_v2(&val_gap, &val_len, val_section_len)?;

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut value_kind = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        match take_u8(block, &mut bpos)? {
            0 => value_kind.push(ValueKind::Scalar),
            1 => value_kind.push(ValueKind::Histogram),
            other => return Err(SegmentError::InvalidValueKind(other)),
        }
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    // HIST_PAGES is optional in v3 symmetrically to VAL_PAGES above.
    let hist_section_len = find_section(footer, section_kind::HIST_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut hist_gap = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        hist_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut hist_len = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        hist_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let hist_page = reconstruct_ranges_v2(&hist_gap, &hist_len, hist_section_len)?;

    if *pos != meta_bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }

    for i in 0..count as usize {
        match value_kind[i] {
            ValueKind::Scalar => {
                if hist_page[i] != (0, 0) {
                    return Err(SegmentError::ScalarSeriesHasHistPage);
                }
            }
            ValueKind::Histogram => {
                if val_page[i] != (0, 0) {
                    return Err(SegmentError::HistogramSeriesHasValPage);
                }
                if hist_page[i].1 == 0 {
                    return Err(SegmentError::ZeroHistPageLen);
                }
            }
        }
    }

    Ok(SeriesMetaTailV3 {
        sample_count,
        min_ts_ns,
        max_ts_ns,
        ts_page,
        val_page,
        value_kind,
        hist_page,
    })
}

/// Footer-level cross-checks that need the fully parsed SERIES_META tail
/// (docs/rseg-v3-plan.md section 3.2): the count of `value_kind = Scalar`
/// series must equal the number of `val_page_len != 0` entries, and
/// symmetrically for `Histogram` against `hist_page_len != 0`; and a
/// VAL_PAGES/HIST_PAGES section present in the footer with zero series of
/// the matching kind is Corrupted (a mandatory-count-zero section entry).
/// Shared by [`decode_catalog_v3`] and [`decode_catalog_matching_v3`]:
/// both must apply this regardless of which series a matcher selects,
/// exactly as v2's `check_series_counts_v2` is shared by its two catalog
/// decoders.
fn check_value_kind_pages_v3(
    footer: &Footer,
    value_kind: &[ValueKind],
    val_page: &[(u64, u64)],
    hist_page: &[(u64, u64)],
) -> Result<(), SegmentError> {
    let scalar_count = value_kind
        .iter()
        .filter(|k| **k == ValueKind::Scalar)
        .count() as u64;
    let hist_count = value_kind
        .iter()
        .filter(|k| **k == ValueKind::Histogram)
        .count() as u64;

    if scalar_count == 0 && find_section(footer, section_kind::VAL_PAGES).is_some() {
        return Err(SegmentError::UnexpectedSectionPresent("VAL_PAGES"));
    }
    if hist_count == 0 && find_section(footer, section_kind::HIST_PAGES).is_some() {
        return Err(SegmentError::UnexpectedSectionPresent("HIST_PAGES"));
    }

    let val_page_nonempty = val_page.iter().filter(|(_, len)| *len != 0).count() as u64;
    if scalar_count != val_page_nonempty {
        return Err(SegmentError::ValueKindPageCountMismatch {
            kind: "VAL_SCALAR",
            value_kind_count: scalar_count,
            page_count: val_page_nonempty,
        });
    }
    let hist_page_nonempty = hist_page.iter().filter(|(_, len)| *len != 0).count() as u64;
    if hist_count != hist_page_nonempty {
        return Err(SegmentError::ValueKindPageCountMismatch {
            kind: "HIST_SPANS",
            value_kind_count: hist_count,
            page_count: hist_page_nonempty,
        });
    }
    Ok(())
}

/// Decodes LABEL_DICT + SERIES_IDS + SERIES_META (verifying section crcs)
/// into [`SeriesEntry`] values with materialized [`LabelSet`]s, including
/// the v3-only `value_kind`/`hist_page` columns (docs/rseg-v3-plan.md
/// section 3.4). The v3 counterpart of [`decode_catalog_v2`]: same output
/// shape, same eager (everyone materialized) semantics, plus the
/// value_kind-vs-page-count and unexpected-section checks section 3.2
/// requires.
pub fn decode_catalog_v3(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_ids_bytes: &[u8],
    series_meta_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntry>, SegmentError> {
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let series_ids_section = find_section(footer, section_kind::SERIES_IDS)
        .ok_or(SegmentError::MissingSection("SERIES_IDS"))?;
    let series_meta_section = find_section(footer, section_kind::SERIES_META)
        .ok_or(SegmentError::MissingSection("SERIES_META"))?;

    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let ids_bytes = decode_section_bytes(series_ids_section, series_ids_bytes, limits)?;
    let meta_bytes = decode_section_bytes(series_meta_section, series_meta_bytes, limits)?;

    let dict_index = index_label_dict(&dict_bytes)?;
    let series_ids = parse_series_ids_v2(&ids_bytes)?;
    let series_ids_count = series_ids.len() as u64;

    let mut pos = 0usize;
    let (meta_count, schemas) =
        parse_schema_list_v2(&meta_bytes, &mut pos, &dict_bytes, &dict_index)?;
    check_series_counts_v2(series_ids_count, u64::from(meta_count), footer.series_count)?;

    let schema_count = schemas.len() as u32;
    let schema_ref = parse_schema_ref_block_v2(&meta_bytes, &mut pos, meta_count, schema_count)?;
    let value_ord =
        parse_value_ord_block_all_v2(&meta_bytes, &mut pos, meta_count, &schema_ref, &schemas)?;
    let tail = parse_series_meta_tail_v3(&meta_bytes, &mut pos, footer, meta_count)?;
    check_value_kind_pages_v3(footer, &tail.value_kind, &tail.val_page, &tail.hist_page)?;

    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let mut entries = Vec::with_capacity(series_ids.len());
    let mut voff = 0usize;
    for i in 0..series_ids.len() {
        let schema = &schemas[schema_ref[i] as usize];
        let n = schema.name_ords.len();
        let vals = &value_ord[voff..voff + n];
        voff += n;
        let mut label_pairs = Vec::with_capacity(n);
        for (&name_ord, &val_ord) in schema.name_ords.iter().zip(vals) {
            let name = resolver.get(name_ord)?;
            let value = resolver.get(val_ord)?;
            label_pairs.push(Label {
                name: name.to_string(),
                value: value.to_string(),
            });
        }
        let label_set = LabelSet::new(label_pairs)?;
        entries.push(SeriesEntry {
            series_id: SeriesId(series_ids[i]),
            labels: label_set,
            sample_count: tail.sample_count[i],
            min_ts_ns: tail.min_ts_ns[i],
            max_ts_ns: tail.max_ts_ns[i],
            ts_page: tail.ts_page[i],
            val_page: tail.val_page[i],
            value_kind: tail.value_kind[i],
            hist_page: tail.hist_page[i],
        });
    }
    Ok(entries)
}

/// Decodes only the series whose labels satisfy every `(name, value)`
/// equality in `equals`, the v3 counterpart of [`decode_catalog_matching_v2`].
/// Column skipping and the schema-plausibility precomputation are
/// unchanged from v2 (docs/rseg-v2-plan.md section 3.4); the v3-only
/// checks (`check_value_kind_pages_v3`) still run over the whole tail
/// regardless of match, exactly as [`decode_catalog_matching_v2`] still
/// structurally validates every v2 row.
pub fn decode_catalog_matching_v3(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_ids_bytes: &[u8],
    series_meta_bytes: &[u8],
    equals: &[(&str, &str)],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntry>, SegmentError> {
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let series_ids_section = find_section(footer, section_kind::SERIES_IDS)
        .ok_or(SegmentError::MissingSection("SERIES_IDS"))?;
    let series_meta_section = find_section(footer, section_kind::SERIES_META)
        .ok_or(SegmentError::MissingSection("SERIES_META"))?;

    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let ids_bytes = decode_section_bytes(series_ids_section, series_ids_bytes, limits)?;
    let meta_bytes = decode_section_bytes(series_meta_section, series_meta_bytes, limits)?;

    let dict_index = index_label_dict(&dict_bytes)?;
    let series_ids = parse_series_ids_v2(&ids_bytes)?;
    let series_ids_count = series_ids.len() as u64;

    let mut pos = 0usize;
    let (meta_count, schemas) =
        parse_schema_list_v2(&meta_bytes, &mut pos, &dict_bytes, &dict_index)?;
    check_series_counts_v2(series_ids_count, u64::from(meta_count), footer.series_count)?;

    let find_ordinal = |needle: &str| -> Option<u64> {
        let needle = needle.as_bytes();
        dict_index
            .iter()
            .position(|&(start, len)| {
                dict_bytes
                    .get(start..start + len)
                    .is_some_and(|s| s == needle)
            })
            .map(|i| i as u64)
    };
    let mut matcher_ords: Vec<(u64, u64)> = Vec::with_capacity(equals.len());
    let mut resolvable = true;
    for (name, value) in equals {
        match (find_ordinal(name), find_ordinal(value)) {
            (Some(n), Some(v)) => matcher_ords.push((n, v)),
            _ => {
                resolvable = false;
                break;
            }
        }
    }

    let schema_count = schemas.len() as u32;
    let schema_ref = parse_schema_ref_block_v2(&meta_bytes, &mut pos, meta_count, schema_count)?;

    let schema_plausible: Vec<bool> = if resolvable {
        schemas
            .iter()
            .map(|schema| {
                matcher_ords
                    .iter()
                    .all(|(name_ord, _)| schema.name_ords.contains(name_ord))
            })
            .collect()
    } else {
        vec![false; schemas.len()]
    };

    let value_ord = parse_value_ord_block_selective_v2(
        &meta_bytes,
        &mut pos,
        meta_count,
        &schema_ref,
        &schemas,
        &schema_plausible,
    )?;
    let tail = parse_series_meta_tail_v3(&meta_bytes, &mut pos, footer, meta_count)?;
    check_value_kind_pages_v3(footer, &tail.value_kind, &tail.val_page, &tail.hist_page)?;

    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let mut entries = Vec::new();
    for i in 0..series_ids.len() {
        let Some(vals) = &value_ord[i] else {
            continue;
        };
        let schema = &schemas[schema_ref[i] as usize];
        let is_match = matcher_ords.iter().all(|(name_ord, wanted_val_ord)| {
            schema
                .name_ords
                .iter()
                .position(|n| n == name_ord)
                .is_some_and(|j| vals[j] == *wanted_val_ord)
        });
        if !is_match {
            continue;
        }
        let mut label_pairs = Vec::with_capacity(schema.name_ords.len());
        for (&name_ord, &val_ord) in schema.name_ords.iter().zip(vals) {
            let name = resolver.get(name_ord)?;
            let value = resolver.get(val_ord)?;
            label_pairs.push(Label {
                name: name.to_string(),
                value: value.to_string(),
            });
        }
        let label_set = LabelSet::new(label_pairs)?;
        entries.push(SeriesEntry {
            series_id: SeriesId(series_ids[i]),
            labels: label_set,
            sample_count: tail.sample_count[i],
            min_ts_ns: tail.min_ts_ns[i],
            max_ts_ns: tail.max_ts_ns[i],
            ts_page: tail.ts_page[i],
            val_page: tail.val_page[i],
            value_kind: tail.value_kind[i],
            hist_page: tail.hist_page[i],
        });
    }
    Ok(entries)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod dict_resolver_tests {
    use super::{DictResolver, dict_str, index_label_dict};
    use crate::error::SegmentError;

    /// Builds a LABEL_DICT payload (`count: u32`, then `len: varint` + bytes
    /// per entry) so tests exercise the same index `dict_str`/`DictResolver`
    /// consume. `entries` are raw bytes so an invalid-UTF-8 entry can be
    /// planted directly.
    fn build_dict(entries: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for e in entries {
            // single-byte varint length (all test entries are short)
            assert!(e.len() < 0x80);
            out.push(e.len() as u8);
            out.extend_from_slice(e);
        }
        out
    }

    #[test]
    fn resolver_matches_dict_str_for_valid_ordinals() {
        let dict = build_dict(&[b"__name__", b"region", b"us-east"]);
        let index = index_label_dict(&dict).expect("index");
        let mut resolver = DictResolver::new(&dict, &index);
        for ord in [0u64, 1, 2, 1, 0, 2] {
            // Repeated ordinals exercise the cache; every hit must equal the
            // uncached dict_str result.
            assert_eq!(
                resolver.get(ord).expect("resolve"),
                dict_str(&dict, &index, ord).expect("dict_str"),
            );
        }
    }

    #[test]
    fn resolver_reports_bad_ordinal_out_of_range() {
        let dict = build_dict(&[b"__name__"]);
        let index = index_label_dict(&dict).expect("index");
        let mut resolver = DictResolver::new(&dict, &index);
        assert!(matches!(resolver.get(5), Err(SegmentError::BadOrdinal(5))));
    }

    #[test]
    fn resolver_reports_bad_utf8_only_when_referenced() {
        // Ordinal 1 is invalid UTF-8; ordinal 0 is valid. A resolver that
        // only ever touches ordinal 0 must never surface the bad entry,
        // matching the deferred-validation contract dict_str provides.
        let dict = build_dict(&[b"ok", &[0xff, 0xfe]]);
        let index = index_label_dict(&dict).expect("index");
        let mut resolver = DictResolver::new(&dict, &index);
        assert_eq!(resolver.get(0).expect("valid"), "ok");
        assert!(matches!(resolver.get(1), Err(SegmentError::BadUtf8)));
    }

    #[test]
    fn resolver_caches_first_success_across_repeats() {
        // A second get of a valid ordinal returns the same bytes (cache hit
        // path), and a valid ordinal resolved after a bad one still works:
        // the bad-ordinal error is per-call, it does not poison the cache.
        let dict = build_dict(&[b"a", b"bb"]);
        let index = index_label_dict(&dict).expect("index");
        let mut resolver = DictResolver::new(&dict, &index);
        assert_eq!(resolver.get(1).expect("first"), "bb");
        assert!(matches!(resolver.get(9), Err(SegmentError::BadOrdinal(9))));
        assert_eq!(resolver.get(1).expect("cached"), "bb");
        assert_eq!(resolver.get(0).expect("other"), "a");
    }
}

//! RSEG v1 reader: footer-first parsing, LABEL_DICT/SERIES_TABLE decode,
//! series selection, byte-range planning, and page decode. Every offset,
//! length, count, and ordinal read from stored bytes is untrusted
//! (docs/segment-format.md); every fallible path returns a typed error.

use ravel_proto::segment::v1::{Footer, Section};
use ravel_types::{Label, LabelSet, Sample, SeriesId};

use crate::crc::page_crc;
use crate::error::SegmentError;
use crate::format::{
    MAGIC, RESERVED, ReaderLimits, SIGNAL_METRICS, VERSION, compression, page_comp, page_enc,
    section_kind,
};
use crate::varint::read_uvarint;

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
    if version != VERSION {
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
    }))
}

/// Validates footer-level section invariants (docs/segment-format.md): at
/// most one section per known kind, all four mandatory kinds present,
/// every section range within `[0, page_region_end)` with checked
/// arithmetic, and section `uncompressed_len` within `limits`.
pub fn validate_sections(
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

/// Convenience: parse and validate a footer from the complete object bytes.
pub fn open_from_full(bytes: &[u8], limits: ReaderLimits) -> Result<FooterLocation, SegmentError> {
    match parse_footer(bytes.len() as u64, bytes)? {
        FooterOutcome::Ready(loc) => {
            validate_sections(&loc.footer, loc.footer_offset, limits)?;
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
            validate_sections(&loc.footer, loc.footer_offset, limits)?;
            Ok(FooterOutcome::Ready(loc))
        }
        need @ FooterOutcome::NeedRange { .. } => Ok(need),
    }
}

/// A decoded SERIES_TABLE entry with its labels materialized from
/// LABEL_DICT. Page offsets/lengths are relative to their respective
/// section's start.
#[derive(Debug, Clone)]
pub struct SeriesEntry {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub sample_count: u32,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub ts_page: (u64, u64),
    pub val_page: (u64, u64),
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
/// TS and VAL pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedRange {
    pub series_id: SeriesId,
    pub ts_range: (u64, u64),
    pub val_range: (u64, u64),
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

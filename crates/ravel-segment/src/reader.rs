//! RSEG v5 reader: footer-first parsing, catalog decode, series selection,
//! byte-range planning, and page decode. ADR-0027 leaves v5 the only
//! supported version; `parse_footer` fails closed on any other. Every
//! offset, length, count, and ordinal read from stored bytes is untrusted
//! (docs/segment-format.md); every fallible path returns a typed error.

use ravel_proto::segment::v1::{Footer, Section};
use ravel_types::{Label, LabelSet, SeriesId};

use crate::crc::page_crc;
use crate::error::SegmentError;
use crate::format::{
    MAGIC, RESERVED, ReaderLimits, SIGNAL_METRICS, VERSION_V5, compression, page_comp, page_enc,
    section_kind,
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
    /// Trailer format version. ADR-0027 leaves 5 the only supported value;
    /// `parse_footer` rejects any other, so this is always 5 on a
    /// successfully parsed object. Retained in the trailer and here so a
    /// future version bump reuses the same dispatch point.
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
    // ADR-0027: v5 is the only supported version. Versions 1-4 fail closed
    // with the same typed error as any unknown future version; a stray
    // pre-v5 object is rejected, never half-parsed.
    if version != VERSION_V5 {
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

/// Validates footer-level section invariants (docs/segment-format.md).
/// ADR-0027 leaves v5 the only supported version, so this dispatches to the
/// v5 rule set or rejects with `UnsupportedVersion`: at most one section per
/// known kind, every mandatory kind present, exactly one of the whole
/// SERIES_META or the sparse SERIES_IDX+SERIES_META_CHUNKS pair, every
/// section range within `[0, page_region_end)` with checked arithmetic, and
/// section `uncompressed_len` within `limits`.
///
/// The count-equality check (SERIES_IDS `count`, SERIES_META `count`, and
/// `Footer.series_count` must all be equal) is deliberately NOT performed
/// here: `Section` (proto/ravel/segment.proto) carries no `count` field, so
/// that check needs each section's decoded payload bytes, which this
/// function -- called immediately after `parse_footer`, before any section
/// byte GET -- never has. It, and the run-count-sum and run-major
/// `value_kind`-vs-page-presence checks, are performed in `decode_catalog_v4`
/// / `decode_catalog_matching_v4` (the v5 whole-catalog decoders) instead,
/// which do have those bytes.
pub fn validate_sections(
    footer: &Footer,
    version: u16,
    page_region_end: u64,
    limits: ReaderLimits,
) -> Result<(), SegmentError> {
    match version {
        VERSION_V5 => validate_sections_v5(footer, page_region_end, limits),
        other => Err(SegmentError::UnsupportedVersion(other)),
    }
}

/// v5 mandatory-kind validation: LABEL_DICT and SERIES_IDS always; exactly
/// one of the whole SERIES_META (below the sparse threshold) or the
/// SERIES_IDX + SERIES_META_CHUNKS pair (at or above it); TS_PAGES/VAL_PAGES/
/// HIST_PAGES conditional on the series present.
fn validate_sections_v5(
    footer: &Footer,
    page_region_end: u64,
    limits: ReaderLimits,
) -> Result<(), SegmentError> {
    // 0 LABEL_DICT, 1 SERIES_IDS, 2 SERIES_META, 3 TS_PAGES, 4 VAL_PAGES,
    // 5 HIST_PAGES, 6 SERIES_IDX, 7 SERIES_META_CHUNKS.
    let mut seen = [false; 8];
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
            section_kind::SERIES_IDX => 6,
            section_kind::SERIES_META_CHUNKS => 7,
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
    const NAMES: [&str; 2] = ["LABEL_DICT", "SERIES_IDS"];
    for (i, name) in NAMES.iter().enumerate() {
        if !seen[i] {
            return Err(SegmentError::MissingSection(name));
        }
    }
    if !seen[3] {
        return Err(SegmentError::MissingSection("TS_PAGES"));
    }
    // Catalog body: exactly one of the whole-section (kind 6) or the chunked
    // (kind 8 + kind 9) form.
    let whole = seen[2];
    let chunked = seen[6] || seen[7];
    if whole && chunked {
        return Err(SegmentError::DuplicateSection(section_kind::SERIES_META));
    }
    if chunked && !(seen[6] && seen[7]) {
        return Err(SegmentError::SparseSectionsIncomplete);
    }
    if !whole && !chunked {
        return Err(SegmentError::MissingSection("SERIES_META"));
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

/// One compaction-input run within a v4 series
/// (docs/compaction-retention-plan.md section 4): dedup-priority
/// provenance (`created_unix_ns`, `writer_epoch`, `writer_seq`) plus this
/// run's own TS and VAL-or-HIST page ranges, relative to their section.
/// `val_page`/`hist_page` follow the same "(0, 0) means not applicable"
/// convention as [`SeriesEntry`]: a run's series has a uniform
/// `value_kind`, so exactly one of the pair is ever non-`(0, 0)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunEntry {
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub sample_count: u32,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub ts_page: (u64, u64),
    pub val_page: (u64, u64),
    pub hist_page: (u64, u64),
}

/// A decoded v4 SERIES_META entry (docs/compaction-retention-plan.md
/// section 4): `entry` is the folded per-series view callers keyed by
/// [`SeriesEntry`] already expect (`sample_count` summed over every run,
/// `min_ts_ns`/`max_ts_ns` spanning every run, `ts_page`/`val_page`/
/// `hist_page` always `(0, 0)` sentinels since a multi-run series has no
/// single page range at the series level); `runs` is the per-run view the
/// page fetcher needs to actually read TS/VAL/HIST bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesEntryV4 {
    pub entry: SeriesEntry,
    pub runs: Vec<RunEntry>,
}

pub(crate) fn find_section(footer: &Footer, kind: u32) -> Option<&Section> {
    footer.sections.iter().find(|s| s.kind == kind)
}

pub(crate) fn to_usize(v: u64) -> Result<usize, SegmentError> {
    usize::try_from(v).map_err(|_| SegmentError::SectionOutOfBounds)
}

/// Decompresses and crc-verifies a whole section's stored bytes, per
/// docs/segment-format.md ("Section crc32c covers the stored bytes";
/// "uncompressed_len must match the decompressed size exactly").
pub(crate) fn decode_section_bytes(
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

pub(crate) fn take_u32_le(bytes: &[u8], pos: &mut usize) -> Result<u32, SegmentError> {
    let s = take_bytes(bytes, pos, 4)?;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn take_array16(bytes: &[u8], pos: &mut usize) -> Result<[u8; 16], SegmentError> {
    let s = take_bytes(bytes, pos, 16)?;
    s.try_into().map_err(|_| SegmentError::Truncated)
}

pub(crate) fn index_label_dict(bytes: &[u8]) -> Result<Vec<(usize, usize)>, SegmentError> {
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

/// Uncached ordinal-to-`&str` resolution. Since ADR-0027 removed the v1
/// eager catalog decoder that used it in production, this survives only as
/// the reference oracle the `DictResolver` tests compare against, hence
/// `#[cfg(test)]`.
#[cfg(test)]
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
pub(crate) struct DictResolver<'a> {
    dict_bytes: &'a [u8],
    index: &'a [(usize, usize)],
    cache: Vec<Option<&'a str>>,
}

impl<'a> DictResolver<'a> {
    pub(crate) fn new(dict_bytes: &'a [u8], index: &'a [(usize, usize)]) -> Self {
        Self {
            dict_bytes,
            index,
            cache: vec![None; index.len()],
        }
    }

    pub(crate) fn get(&mut self, ord: u64) -> Result<&'a str, SegmentError> {
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
pub struct PlannedRunRange {
    pub series_id: SeriesId,
    pub run_index: usize,
    pub ts_range: (u64, u64),
    pub val_range: (u64, u64),
    pub hist_range: (u64, u64),
}

/// v4 counterpart of [`plan_ranges_v3`]: computes TS/VAL/HIST ranges for
/// every run of every selected series, looking up VAL_PAGES only if at
/// least one selected run is `ValueKind::Scalar` and HIST_PAGES only if at
/// least one is `ValueKind::Histogram` -- the same conditional lookup
/// `plan_ranges_v3` uses, since v4 keeps VAL_PAGES/HIST_PAGES each
/// optional (docs/compaction-retention-plan.md section 4: "strict
/// superset of v3").
pub fn plan_ranges_v4(
    footer: &Footer,
    selected: &[&SeriesEntryV4],
) -> Result<Vec<PlannedRunRange>, SegmentError> {
    let ts_section = find_section(footer, section_kind::TS_PAGES)
        .ok_or(SegmentError::MissingSection("TS_PAGES"))?;

    let mut out = Vec::new();
    for series in selected {
        for (run_index, run) in series.runs.iter().enumerate() {
            let (ts_off, ts_len) = run.ts_page;
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

            let val_range = match series.entry.value_kind {
                ValueKind::Scalar => {
                    let val_section = find_section(footer, section_kind::VAL_PAGES)
                        .ok_or(SegmentError::MissingSection("VAL_PAGES"))?;
                    let (val_off, val_len) = run.val_page;
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

            let hist_range = match series.entry.value_kind {
                ValueKind::Histogram => {
                    let hist_section = find_section(footer, section_kind::HIST_PAGES)
                        .ok_or(SegmentError::MissingSection("HIST_PAGES"))?;
                    let (hist_off, hist_len) = run.hist_page;
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

            out.push(PlannedRunRange {
                series_id: series.entry.series_id,
                run_index,
                ts_range: (ts_abs, ts_len),
                val_range,
                hist_range,
            });
        }
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

#[allow(clippy::too_many_arguments)]
fn decode_ts_page_into(
    series_id: &SeriesId,
    sample_count: u32,
    min_ts_ns: i64,
    max_ts_ns: i64,
    page: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    out: &mut Vec<i64>,
) -> Result<(), SegmentError> {
    let (enc, comp, payload) = split_page_header(series_id, page)?;
    if enc != page_enc::TS_DELTA_VARINT {
        return Err(SegmentError::InvalidEncoding(enc));
    }
    decompress_page_payload_into(comp, payload, limits, scratch)?;
    let count = to_usize(u64::from(sample_count))?;
    crate::ts_delta::decode_ts_deltas_into(scratch, count, min_ts_ns, max_ts_ns, out)
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
    series_id: &SeriesId,
    sample_count: u32,
    page: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    out: &mut Vec<f64>,
) -> Result<ValPageKind, SegmentError> {
    let (enc, comp, payload) = split_page_header(series_id, page)?;
    decompress_page_payload_into(comp, payload, limits, scratch)?;
    let count = to_usize(u64::from(sample_count))?;
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
    series_id: &SeriesId,
    sample_count: u32,
    page: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    out: &mut Vec<HistogramValue>,
) -> Result<(), SegmentError> {
    let (enc, comp, payload) = split_page_header(series_id, page)?;
    if enc != page_enc::HIST_SPANS {
        return Err(SegmentError::InvalidEncoding(enc));
    }
    decompress_page_payload_into(comp, payload, limits, scratch)?;
    let count = to_usize(u64::from(sample_count))?;
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

/// Decodes one v4 run's TS and VAL pages directly into separate
/// timestamp/value vecs (SoA), the per-run page decoder for a v5 (v4-grammar)
/// [`RunEntry`] (docs/segment-format.md). Validation contract: page crc
/// (bound to `series_id`), enc/comp validity, overflow- and bounds-checked
/// timestamp accumulation, on-disk order (including duplicate timestamps)
/// preserved.
#[allow(clippy::too_many_arguments)]
pub fn decode_run_pages_soa(
    series_id: &SeriesId,
    run: &RunEntry,
    ts_page_bytes: &[u8],
    val_page_bytes: &[u8],
    limits: ReaderLimits,
    scratch: &mut Vec<u8>,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> Result<ValPageKind, SegmentError> {
    decode_ts_page_into(
        series_id,
        run.sample_count,
        run.min_ts_ns,
        run.max_ts_ns,
        ts_page_bytes,
        limits,
        scratch,
        timestamps,
    )?;
    let val_kind = decode_val_page_into(
        series_id,
        run.sample_count,
        val_page_bytes,
        limits,
        scratch,
        values,
    )?;
    if timestamps.len() != values.len() {
        return Err(SegmentError::Truncated);
    }
    Ok(val_kind)
}

/// Decodes one v4 run's TS and HIST pages into histogram samples, the
/// per-run counterpart of [`decode_histogram_pages`] for a v4 [`RunEntry`]
/// (docs/compaction-retention-plan.md section 4).
pub fn decode_run_histogram_pages(
    series_id: &SeriesId,
    run: &RunEntry,
    ts_page_bytes: &[u8],
    hist_page_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<HistogramSample>, SegmentError> {
    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    decode_ts_page_into(
        series_id,
        run.sample_count,
        run.min_ts_ns,
        run.max_ts_ns,
        ts_page_bytes,
        limits,
        &mut scratch,
        &mut timestamps,
    )?;
    let mut values = Vec::new();
    decode_hist_page_into(
        series_id,
        run.sample_count,
        hist_page_bytes,
        limits,
        &mut scratch,
        &mut values,
    )?;
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
pub(crate) struct SchemaMetaV2 {
    pub(crate) name_ords: Vec<u64>,
}

/// Per-series output of SERIES_META's blocks 3-9 (docs/segment-format.md
/// v2 amendment): identical work for the eager and lazy decode paths (no
/// per-series skipping is possible here, unlike block 2 / value_ord, since
/// every column is exactly one varint per series regardless of schema).
pub(crate) fn take_block<'a>(bytes: &'a [u8], pos: &mut usize) -> Result<&'a [u8], SegmentError> {
    let block_len = read_uvarint(bytes, pos)?;
    let block_len = to_usize(block_len)?;
    take_bytes(bytes, pos, block_len)
}

/// Decodes SERIES_IDS (kind 5, v2 only, docs/segment-format.md): `count:
/// u32` then `count` 16-byte ids, strictly ascending by byte comparison.
/// Rejects non-ascending ids and any trailing bytes (equivalently, any
/// section length other than exactly `4 + 16*count`).
pub(crate) fn parse_series_ids_v2(bytes: &[u8]) -> Result<Vec<[u8; 16]>, SegmentError> {
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
pub(crate) fn parse_schema_list_v2(
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
pub(crate) fn check_series_counts_v2(
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
pub(crate) fn parse_schema_ref_block_v2(
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
pub(crate) fn parse_value_ord_block_all_v2(
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
pub(crate) fn reconstruct_ranges_v2(
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
struct SeriesMetaTailV4 {
    value_kind: Vec<ValueKind>,
    run_count: Vec<u32>,
    runs: Vec<RunEntry>,
}

/// SERIES_META blocks 3-16 (docs/compaction-retention-plan.md section 4):
/// `value_kind` and `run_count` (series-major, identical shape to v3's
/// value_kind block plus a new run-count column, each `run_count` entry
/// validated non-zero and summed against `run_total`), then ten run-major
/// columns folded into `run_total`-length [`RunEntry`] values. VAL_PAGES/
/// HIST_PAGES are each optional exactly as in v3, so their gap/len columns
/// reconstruct against the section's own `len` when present or `0` when
/// absent. Per-run `created_unix_ns`/`min_ts_ns`/`max_ts_ns` are
/// reconstructed via checked i128 arithmetic against
/// `footer.base_created_unix_ns`/`footer.min_event_ts_ns`
/// (`ProvenanceBoundsOverflow` on overflow -- kept distinct from v2/v3's
/// `TimestampBoundsOverflow` since these are new, run-major-only columns
/// with no v2/v3 error identity to reuse). The per-run value_kind-vs-page
/// cross-check (`ScalarSeriesHasHistPage`/`HistogramSeriesHasValPage`/
/// `ZeroHistPageLen`) and the trailing-bytes check run after all sixteen
/// blocks, mirroring v3's placement; the aggregate run-count-vs-page-count
/// check and the section-present-but-unneeded check need the footer's
/// section list alongside the fully parsed tail, so they run in the
/// caller via `check_value_kind_pages_v4`, exactly as v3 defers
/// `check_value_kind_pages_v3`.
/// Live-heap bytes one decoded run costs in [`parse_series_meta_tail_v4`]:
/// eleven `u64`/`i64`-valued run-major columns (`created_unix_ns`,
/// `writer_epoch`, `writer_seq`, `min_ts_ns`, `max_ts_ns`, `ts_gap`,
/// `ts_len`, `val_gap`, `val_len`, `hist_gap`, `hist_len`; 8 bytes each), one
/// `u32`-valued column (`sample_count`; 4 bytes), the three reconstructed
/// `(u64, u64)` range vectors (`ts_page`/`val_page`/`hist_page`; 16 bytes
/// each), and one folded [`RunEntry`] (`size_of::<RunEntry>()`, pinned at 96
/// bytes by `format_constants_are_pinned`-style layout: nine fixed-width
/// integer fields whose largest alignment is 8, so the 4-byte
/// `sample_count` field pads the struct to a 96-byte total). All of these
/// are resident simultaneously right before the final `runs` vector is
/// built (see `SeriesMetaTailV4`'s doc comment).
pub(crate) const RUN_LIVE_BYTES_DENSE: u64 = 11 * 8 + 4 + 3 * 16 + 96;

/// Same twelve-column shape as decoded by
/// [`crate::sparse::decode_chunk_frame`], except every run-major column
/// there (including `sample_count`) is read via `read_uvarint_block` into a
/// `Vec<u64>` and only narrowed to `u32` when folded into `RunEntry`, so all
/// twelve columns cost 8 bytes each rather than eleven at 8 and one at 4.
pub(crate) const RUN_LIVE_BYTES_CHUNK: u64 = 12 * 8 + 3 * 16 + 96;

/// Rejects a `run_total`/`frame_run_total` whose live-decoded working set --
/// the twelve run-major columns, three reconstructed range vectors, and the
/// folded `RunEntry` vector, all resident at once just before the fold into
/// the final structure (docs/segment-format.md SERIES_META run-major
/// columns) -- would exceed `limits.max_section_uncompressed_bytes`.
///
/// That cap already bounds *input* bytes, one section (or chunk frame) at a
/// time, in [`decode_section_bytes`]/`verify_and_decompress_chunk_frame`.
/// But a maliciously small compressed section can decompress to a byte count
/// near that cap consisting of nothing but 1-byte-varint runs, and the
/// live-byte expansion per run (roughly 20-25x, per [`RUN_LIVE_BYTES_DENSE`]/
/// [`RUN_LIVE_BYTES_CHUNK`] against the ~12-byte best-case input cost of one
/// run) means the input-byte cap alone lets `run_total` reach a live
/// footprint many times the configured budget before any per-column
/// allocation would fail on its own. Reusing the same cap as a live-byte
/// budget closes that gap without a second config knob, and this check runs
/// before the first run-major column `Vec` is allocated.
pub(crate) fn check_run_total_live_budget(
    run_total: u64,
    live_bytes_per_run: u64,
    limits: ReaderLimits,
) -> Result<(), SegmentError> {
    let live_bytes = run_total
        .checked_mul(live_bytes_per_run)
        .ok_or(SegmentError::FieldOverflow)?;
    if live_bytes > limits.max_section_uncompressed_bytes {
        return Err(SegmentError::RunTotalLiveBudgetExceeded {
            run_total,
            live_bytes,
            cap: limits.max_section_uncompressed_bytes,
        });
    }
    Ok(())
}

fn parse_series_meta_tail_v4(
    meta_bytes: &[u8],
    pos: &mut usize,
    footer: &Footer,
    count: u32,
    run_total: u32,
    limits: ReaderLimits,
) -> Result<SeriesMetaTailV4, SegmentError> {
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

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut run_count = Vec::with_capacity((count as usize).min(block.len()));
    let mut run_count_sum: u64 = 0;
    for _ in 0..count {
        let v = read_uvarint(block, &mut bpos)?;
        let v = u32::try_from(v).map_err(|_| SegmentError::FieldOverflow)?;
        if v == 0 {
            return Err(SegmentError::ZeroRunCount);
        }
        run_count_sum = run_count_sum
            .checked_add(u64::from(v))
            .ok_or(SegmentError::FieldOverflow)?;
        run_count.push(v);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    if run_count_sum != u64::from(run_total) {
        return Err(SegmentError::RunCountSumMismatch {
            run_count_sum,
            run_total: u64::from(run_total),
        });
    }
    let run_total_usize = to_usize(u64::from(run_total))?;
    check_run_total_live_budget(u64::from(run_total), RUN_LIVE_BYTES_DENSE, limits)?;

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut created_unix_ns = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        let delta = read_uvarint(block, &mut bpos)?;
        let sum = i128::from(footer.base_created_unix_ns) + i128::from(delta);
        created_unix_ns
            .push(i64::try_from(sum).map_err(|_| SegmentError::ProvenanceBoundsOverflow)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut writer_epoch = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        writer_epoch.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut writer_seq = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        writer_seq.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut sample_count = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
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
    let mut min_ts_ns = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        let delta = read_uvarint(block, &mut bpos)?;
        let sum = i128::from(footer.min_event_ts_ns) + i128::from(delta);
        min_ts_ns.push(i64::try_from(sum).map_err(|_| SegmentError::ProvenanceBoundsOverflow)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut max_ts_ns = Vec::with_capacity(run_total_usize.min(block.len()));
    for &min_ts in min_ts_ns.iter().take(run_total_usize) {
        let span = read_uvarint(block, &mut bpos)?;
        let sum = i128::from(min_ts) + i128::from(span);
        max_ts_ns.push(i64::try_from(sum).map_err(|_| SegmentError::ProvenanceBoundsOverflow)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    let ts_section = find_section(footer, section_kind::TS_PAGES)
        .ok_or(SegmentError::MissingSection("TS_PAGES"))?;
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut ts_gap = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        ts_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut ts_len = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        ts_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let ts_page = reconstruct_ranges_v2(&ts_gap, &ts_len, ts_section.len)?;

    let val_section_len = find_section(footer, section_kind::VAL_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut val_gap = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        val_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut val_len = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        val_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let val_page = reconstruct_ranges_v2(&val_gap, &val_len, val_section_len)?;

    let hist_section_len = find_section(footer, section_kind::HIST_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut hist_gap = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        hist_gap.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let block = take_block(meta_bytes, pos)?;
    let mut bpos = 0usize;
    let mut hist_len = Vec::with_capacity(run_total_usize.min(block.len()));
    for _ in 0..run_total {
        hist_len.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    let hist_page = reconstruct_ranges_v2(&hist_gap, &hist_len, hist_section_len)?;

    if *pos != meta_bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }

    // Per-run value_kind-vs-page cross-check, the run-major counterpart of
    // v3's per-series check: every run's page presence must match its
    // series' (uniform) value_kind. `run_count_sum == run_total` (checked
    // above) guarantees `run_idx` lands exactly on `run_total_usize`.
    let mut run_idx = 0usize;
    for i in 0..count as usize {
        for _ in 0..run_count[i] {
            match value_kind[i] {
                ValueKind::Scalar => {
                    if hist_page[run_idx].1 != 0 {
                        return Err(SegmentError::ScalarSeriesHasHistPage);
                    }
                }
                ValueKind::Histogram => {
                    if val_page[run_idx].1 != 0 {
                        return Err(SegmentError::HistogramSeriesHasValPage);
                    }
                    if hist_page[run_idx].1 == 0 {
                        return Err(SegmentError::ZeroHistPageLen);
                    }
                }
            }
            run_idx += 1;
        }
    }

    let runs = (0..run_total_usize)
        .map(|i| RunEntry {
            created_unix_ns: created_unix_ns[i],
            writer_epoch: writer_epoch[i],
            writer_seq: writer_seq[i],
            sample_count: sample_count[i],
            min_ts_ns: min_ts_ns[i],
            max_ts_ns: max_ts_ns[i],
            ts_page: ts_page[i],
            val_page: val_page[i],
            hist_page: hist_page[i],
        })
        .collect();

    Ok(SeriesMetaTailV4 {
        value_kind,
        run_count,
        runs,
    })
}

/// Footer-level cross-checks that need the fully parsed SERIES_META tail
/// (docs/compaction-retention-plan.md section 4), the run-weighted
/// counterpart of `check_value_kind_pages_v3`: since a v4 series' pages
/// live per-run rather than per-series, the "value_kind count must equal
/// non-empty page count" rule is now over *runs* weighted by each series'
/// `run_count`, not over series directly. Shared by [`decode_catalog_v4`]
/// and [`decode_catalog_matching_v4`], exactly as v3's counterpart is
/// shared by its two catalog decoders.
fn check_value_kind_pages_v4(
    footer: &Footer,
    value_kind: &[ValueKind],
    run_count: &[u32],
    runs: &[RunEntry],
) -> Result<(), SegmentError> {
    let mut scalar_run_count: u64 = 0;
    let mut hist_run_count: u64 = 0;
    for (kind, &rc) in value_kind.iter().zip(run_count) {
        match kind {
            ValueKind::Scalar => scalar_run_count += u64::from(rc),
            ValueKind::Histogram => hist_run_count += u64::from(rc),
        }
    }

    if scalar_run_count == 0 && find_section(footer, section_kind::VAL_PAGES).is_some() {
        return Err(SegmentError::UnexpectedSectionPresent("VAL_PAGES"));
    }
    if hist_run_count == 0 && find_section(footer, section_kind::HIST_PAGES).is_some() {
        return Err(SegmentError::UnexpectedSectionPresent("HIST_PAGES"));
    }

    let val_page_nonempty = runs.iter().filter(|r| r.val_page.1 != 0).count() as u64;
    if scalar_run_count != val_page_nonempty {
        return Err(SegmentError::ValueKindPageCountMismatch {
            kind: "VAL_SCALAR",
            value_kind_count: scalar_run_count,
            page_count: val_page_nonempty,
        });
    }
    let hist_page_nonempty = runs.iter().filter(|r| r.hist_page.1 != 0).count() as u64;
    if hist_run_count != hist_page_nonempty {
        return Err(SegmentError::ValueKindPageCountMismatch {
            kind: "HIST_SPANS",
            value_kind_count: hist_run_count,
            page_count: hist_page_nonempty,
        });
    }
    Ok(())
}

/// Decodes LABEL_DICT + SERIES_IDS + SERIES_META (verifying section crcs)
/// into [`SeriesEntryV4`] values with materialized [`LabelSet`]s, including
/// the v4-only run-major columns (docs/compaction-retention-plan.md
/// section 4). The v4 counterpart of [`decode_catalog_v3`]: same eager
/// (everyone materialized) semantics and the same schema_ref/value_ord
/// reuse of v2's primitives, but each output pairs a folded [`SeriesEntry`]
/// (`sample_count` summed, `min_ts_ns`/`max_ts_ns` spanning every run) with
/// the per-run [`RunEntry`] view the page fetcher needs.
pub fn decode_catalog_v4(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_ids_bytes: &[u8],
    series_meta_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntryV4>, SegmentError> {
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
    let run_total = take_u32_le(&meta_bytes, &mut pos)?;

    let schema_count = schemas.len() as u32;
    let schema_ref = parse_schema_ref_block_v2(&meta_bytes, &mut pos, meta_count, schema_count)?;
    let value_ord =
        parse_value_ord_block_all_v2(&meta_bytes, &mut pos, meta_count, &schema_ref, &schemas)?;
    let tail =
        parse_series_meta_tail_v4(&meta_bytes, &mut pos, footer, meta_count, run_total, limits)?;
    check_value_kind_pages_v4(footer, &tail.value_kind, &tail.run_count, &tail.runs)?;

    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let mut entries = Vec::with_capacity(series_ids.len());
    let mut voff = 0usize;
    let mut roff = 0usize;
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

        let run_count = tail.run_count[i] as usize;
        let runs: Vec<RunEntry> = tail.runs[roff..roff + run_count].to_vec();
        roff += run_count;

        let mut sample_count: u32 = 0;
        let mut min_ts_ns = i64::MAX;
        let mut max_ts_ns = i64::MIN;
        for r in &runs {
            sample_count = sample_count
                .checked_add(r.sample_count)
                .ok_or(SegmentError::FieldOverflow)?;
            min_ts_ns = min_ts_ns.min(r.min_ts_ns);
            max_ts_ns = max_ts_ns.max(r.max_ts_ns);
        }

        entries.push(SeriesEntryV4 {
            entry: SeriesEntry {
                series_id: SeriesId(series_ids[i]),
                labels: label_set,
                sample_count,
                min_ts_ns,
                max_ts_ns,
                ts_page: (0, 0),
                val_page: (0, 0),
                value_kind: tail.value_kind[i],
                hist_page: (0, 0),
            },
            runs,
        });
    }
    Ok(entries)
}

/// Decodes only the series whose labels satisfy every `(name, value)`
/// equality in `equals`, the v4 counterpart of
/// [`decode_catalog_matching_v3`]. Column skipping and the
/// schema-plausibility precomputation are unchanged from v2/v3; the
/// v4-only check (`check_value_kind_pages_v4`) still runs over the whole
/// tail regardless of match, and `roff` (the flat run cursor) still
/// advances for every series regardless of match, since
/// `parse_series_meta_tail_v4` already parsed every series' runs
/// up front.
pub fn decode_catalog_matching_v4(
    footer: &Footer,
    label_dict_bytes: &[u8],
    series_ids_bytes: &[u8],
    series_meta_bytes: &[u8],
    equals: &[(&str, &str)],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntryV4>, SegmentError> {
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
    let run_total = take_u32_le(&meta_bytes, &mut pos)?;

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
    let tail =
        parse_series_meta_tail_v4(&meta_bytes, &mut pos, footer, meta_count, run_total, limits)?;
    check_value_kind_pages_v4(footer, &tail.value_kind, &tail.run_count, &tail.runs)?;

    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let mut entries = Vec::new();
    let mut roff = 0usize;
    for i in 0..series_ids.len() {
        let run_count = tail.run_count[i] as usize;
        let run_slice = &tail.runs[roff..roff + run_count];
        roff += run_count;

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

        let mut sample_count: u32 = 0;
        let mut min_ts_ns = i64::MAX;
        let mut max_ts_ns = i64::MIN;
        for r in run_slice {
            sample_count = sample_count
                .checked_add(r.sample_count)
                .ok_or(SegmentError::FieldOverflow)?;
            min_ts_ns = min_ts_ns.min(r.min_ts_ns);
            max_ts_ns = max_ts_ns.max(r.max_ts_ns);
        }

        entries.push(SeriesEntryV4 {
            entry: SeriesEntry {
                series_id: SeriesId(series_ids[i]),
                labels: label_set,
                sample_count,
                min_ts_ns,
                max_ts_ns,
                ts_page: (0, 0),
                val_page: (0, 0),
                value_kind: tail.value_kind[i],
                hist_page: (0, 0),
            },
            runs: run_slice.to_vec(),
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

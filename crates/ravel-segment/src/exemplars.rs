//! EXEMPLARS (section kind 10, ADR-0047), RSEG v6 only. A pure codec module:
//! callers already own series-index resolution and LABEL_DICT interning
//! (`writer.rs`'s `write_v4`), so this module only encodes/decodes the
//! record grammar (docs/segment-format.md "RSEG v6 amendment"):
//!
//! ```text
//! count: u32
//! per record:
//!   series_index:  varint     index into SERIES_IDS
//!   ts_delta:      ivarint    from footer.min_event_ts_ns
//!   value:         f64 LE     bit pattern; never compared with ==
//!   trace_id:      [u8;16]    all zero if absent
//!   span_id:       [u8;8]     all zero if absent
//!   attr_count:    varint
//!   attr pairs:    (name_ord varint, value_ord varint) into LABEL_DICT
//! ```
//!
//! Sorted by `(series_index, ts_ns)`, which lets a per-series lookup
//! ([`probe_exemplars_by_series`]) stop scanning as soon as it passes the
//! target: the record width is variable (attr count varies per record), so
//! there is no auxiliary offset index to make this an actual binary search
//! the way the sparse SERIES_IDX machinery does for series lookups; this is
//! the early-exit linear scan that sort order actually buys.

use ravel_proto::segment::v1::Footer;
use ravel_types::SeriesId;

use crate::error::{SegmentError, WriteError};
use crate::format::{ReaderLimits, section_kind};
use crate::reader::{DictResolver, decode_section_bytes, find_section, index_label_dict, take_u32_le};
use crate::varint::{read_uvarint, read_zigzag_varint, write_uvarint, write_zigzag_varint};

/// One exemplar as a writer caller supplies it: labels ride as owned
/// `(name, value)` string pairs, interned into LABEL_DICT alongside series
/// labels by `writer.rs`'s dictionary build.
#[derive(Debug, Clone)]
pub struct ExemplarInput {
    pub series_id: SeriesId,
    pub ts_ns: i64,
    /// Bit pattern stored verbatim; never compared with `==` by this crate.
    pub value: f64,
    /// All-zero means "no trace id", not a legitimate all-zero trace id
    /// (docs/segment-format.md, ADR-0047 decision 1): OTel trace ids are
    /// 128-bit random identifiers, and the specification reserves the
    /// all-zero id as invalid/absent, so this crate reuses that convention
    /// rather than inventing a presence flag.
    pub trace_id: [u8; 16],
    /// Same all-zero-means-absent convention as `trace_id`.
    pub span_id: [u8; 8],
    pub attrs: Vec<(String, String)>,
}

/// One decoded EXEMPLARS record, attrs resolved to owned strings (the
/// section's bytes may not outlive the decode call, unlike the zero-copy
/// catalog decoders).
#[derive(Debug, Clone)]
pub struct ExemplarRecord {
    pub series_index: u64,
    pub ts_ns: i64,
    /// Bit pattern as stored; compare with `.to_bits()`, never `==`.
    pub value: f64,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attrs: Vec<(String, String)>,
}

/// A writer-resolved exemplar: `series_index` already looked up against the
/// output's sorted `SERIES_IDS`, attrs already interned into ordinals in the
/// same dictionary pass that interns series labels. Built and consumed
/// entirely inside `writer.rs`.
pub(crate) struct ResolvedExemplar {
    pub(crate) series_index: u32,
    pub(crate) ts_ns: i64,
    pub(crate) value: f64,
    pub(crate) trace_id: [u8; 16],
    pub(crate) span_id: [u8; 8],
    pub(crate) attr_ords: Vec<(u32, u32)>,
}

/// Encodes the EXEMPLARS section body from writer-resolved records, already
/// sorted and deduplicated by `(series_index, ts_ns)` by the caller. Returns
/// `Ok(vec![])`... no: callers must skip calling this at all when `resolved`
/// is empty, since an empty EXEMPLARS section is never legal to emit --
/// absence, not an empty section, is how "no exemplars" is represented
/// (ADR-0047 decision 1).
pub(crate) fn encode_exemplars_section(
    resolved: &[ResolvedExemplar],
    min_event_ts_ns: i64,
) -> Result<Vec<u8>, WriteError> {
    let count = u32::try_from(resolved.len()).map_err(|_| WriteError::TooManyExemplars)?;
    let mut buf = Vec::with_capacity(4 + resolved.len() * 40);
    buf.extend_from_slice(&count.to_le_bytes());
    for r in resolved {
        write_uvarint(&mut buf, u64::from(r.series_index));
        let ts_delta = r
            .ts_ns
            .checked_sub(min_event_ts_ns)
            .ok_or(WriteError::TimestampDeltaOverflow)?;
        write_zigzag_varint(&mut buf, ts_delta);
        buf.extend_from_slice(&r.value.to_le_bytes());
        buf.extend_from_slice(&r.trace_id);
        buf.extend_from_slice(&r.span_id);
        let attr_count =
            u32::try_from(r.attr_ords.len()).map_err(|_| WriteError::TooManyExemplarAttrs)?;
        write_uvarint(&mut buf, u64::from(attr_count));
        for &(name_ord, value_ord) in &r.attr_ords {
            write_uvarint(&mut buf, u64::from(name_ord));
            write_uvarint(&mut buf, u64::from(value_ord));
        }
    }
    Ok(buf)
}

fn take_fixed<const N: usize>(bytes: &[u8], pos: &mut usize) -> Result<[u8; N], SegmentError> {
    let end = pos.checked_add(N).ok_or(SegmentError::Truncated)?;
    let slice = bytes.get(*pos..end).ok_or(SegmentError::Truncated)?;
    *pos = end;
    let mut out = [0u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}

/// One decoded record's fixed-width fields (series_index through attr_count
/// have already been consumed by the caller); shared by the whole-section
/// decode and the probe so both apply the same bounds/overflow checks to
/// the value/trace_id/span_id trio.
fn take_record_fixed_fields(
    bytes: &[u8],
    pos: &mut usize,
) -> Result<(f64, [u8; 16], [u8; 8]), SegmentError> {
    let value = f64::from_le_bytes(take_fixed::<8>(bytes, pos)?);
    let trace_id = take_fixed::<16>(bytes, pos)?;
    let span_id = take_fixed::<8>(bytes, pos)?;
    Ok((value, trace_id, span_id))
}

/// Reads one record's `(series_index, ts_ns)` sort key, validating
/// `series_index` is in range and the sequence is strictly ascending.
/// Shared by the whole-section decode and the probe: both must apply the
/// identical untrusted-input checks to every record they touch, including
/// ones a probe will discard without resolving attrs.
fn read_sort_key(
    bytes: &[u8],
    pos: &mut usize,
    series_count: u64,
    min_event_ts_ns: i64,
    prev: &mut Option<(u64, i64)>,
) -> Result<(u64, i64), SegmentError> {
    let series_index = read_uvarint(bytes, pos)?;
    if series_index >= series_count {
        return Err(SegmentError::ExemplarSeriesIndexOutOfRange(series_index));
    }
    let ts_delta = read_zigzag_varint(bytes, pos)?;
    let ts_ns = min_event_ts_ns
        .checked_add(ts_delta)
        .ok_or(SegmentError::TimestampOverflow)?;
    if let Some(p) = *prev {
        if (series_index, ts_ns) <= p {
            return Err(SegmentError::ExemplarRecordsUnsorted);
        }
    }
    *prev = Some((series_index, ts_ns));
    Ok((series_index, ts_ns))
}

fn read_attr_pairs(
    bytes: &[u8],
    pos: &mut usize,
    dict: &mut DictResolver,
) -> Result<Vec<(String, String)>, SegmentError> {
    let attr_count = read_uvarint(bytes, pos)?;
    let cap = crate::reader::to_usize(attr_count)?.min(bytes.len());
    let mut attrs = Vec::with_capacity(cap);
    for _ in 0..attr_count {
        let name_ord = read_uvarint(bytes, pos)?;
        let value_ord = read_uvarint(bytes, pos)?;
        let name = dict.get(name_ord)?.to_string();
        let value = dict.get(value_ord)?.to_string();
        attrs.push((name, value));
    }
    Ok(attrs)
}

/// Skips one record's attr pairs without resolving them (the probe's
/// not-a-match path): still walks every varint so `pos` ends up exactly
/// where a full decode would leave it.
fn skip_attr_pairs(bytes: &[u8], pos: &mut usize) -> Result<(), SegmentError> {
    let attr_count = read_uvarint(bytes, pos)?;
    for _ in 0..attr_count {
        let _name_ord = read_uvarint(bytes, pos)?;
        let _value_ord = read_uvarint(bytes, pos)?;
    }
    Ok(())
}

/// Whole-section decode over already-decompressed section bytes and an
/// already-built dictionary resolver: every record, fully validated and
/// resolved. `series_count` bounds `series_index`; `min_event_ts_ns` is
/// `footer.min_event_ts_ns`. Used by the inspector and by compaction, which
/// need every record to remap `series_index` into its output's own
/// `SERIES_IDS` ordering (ADR-0047 decision 3). `DictResolver` is
/// `pub(crate)`, so this stays `pub(crate)` too; [`decode_exemplars_section`]
/// is the public entry point, taking raw stored bytes the way
/// `decode_catalog_v4` does.
pub(crate) fn decode_exemplars_from_decoded(
    bytes: &[u8],
    min_event_ts_ns: i64,
    series_count: u64,
    dict: &mut DictResolver,
) -> Result<Vec<ExemplarRecord>, SegmentError> {
    let mut pos = 0usize;
    let count = take_u32_le(bytes, &mut pos)?;
    let mut out = Vec::with_capacity((count as usize).min(bytes.len()));
    let mut prev = None;
    for _ in 0..count {
        let (series_index, ts_ns) =
            read_sort_key(bytes, &mut pos, series_count, min_event_ts_ns, &mut prev)?;
        let (value, trace_id, span_id) = take_record_fixed_fields(bytes, &mut pos)?;
        let attrs = read_attr_pairs(bytes, &mut pos, dict)?;
        out.push(ExemplarRecord {
            series_index,
            ts_ns,
            value,
            trace_id,
            span_id,
            attrs,
        });
    }
    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(out)
}

/// Per-series probe over already-decompressed section bytes and an
/// already-built dictionary resolver: an early-exit scan over records in
/// `(series_index, ts_ns)` sort order, stopping as soon as a record's
/// `series_index` exceeds `target_series_index`. This is not a true binary
/// search -- the record width varies with `attr_count`, and the grammar
/// carries no auxiliary offset index the way the sparse SERIES_IDX does for
/// series lookups -- but it never decodes or bounds-checks bytes past the
/// first record that sorts after the target, so a corruption later in the
/// section is invisible to a probe that never reaches it (only
/// [`decode_exemplars_from_decoded`] would catch it). `DictResolver` is
/// `pub(crate)`, so this stays `pub(crate)` too; [`probe_exemplars_by_series`]
/// is the public entry point, taking raw stored bytes.
pub(crate) fn probe_exemplars_from_decoded(
    bytes: &[u8],
    min_event_ts_ns: i64,
    series_count: u64,
    target_series_index: u64,
    dict: &mut DictResolver,
) -> Result<Vec<ExemplarRecord>, SegmentError> {
    let mut pos = 0usize;
    let count = take_u32_le(bytes, &mut pos)?;
    let mut out = Vec::new();
    let mut prev = None;
    for _ in 0..count {
        let (series_index, ts_ns) =
            read_sort_key(bytes, &mut pos, series_count, min_event_ts_ns, &mut prev)?;
        if series_index > target_series_index {
            break;
        }
        let (value, trace_id, span_id) = take_record_fixed_fields(bytes, &mut pos)?;
        if series_index < target_series_index {
            skip_attr_pairs(bytes, &mut pos)?;
            continue;
        }
        let attrs = read_attr_pairs(bytes, &mut pos, dict)?;
        out.push(ExemplarRecord {
            series_index,
            ts_ns,
            value,
            trace_id,
            span_id,
            attrs,
        });
    }
    Ok(out)
}

/// Public whole-section decode, mirroring `decode_catalog_v4`'s contract:
/// callers pass the object's raw stored LABEL_DICT and EXEMPLARS section
/// bytes (as sliced from the object by `footer.sections`' offset/len), and
/// this crc-verifies and decompresses both, builds its own `DictResolver`
/// (never exposed across the crate boundary), and decodes every record. An
/// absent EXEMPLARS section is legal and returns `Ok(vec![])`, not
/// `MissingSection`, since "no exemplars" is the common case (ADR-0047
/// decision 1).
pub fn decode_exemplars_section(
    footer: &Footer,
    label_dict_bytes: &[u8],
    exemplars_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<ExemplarRecord>, SegmentError> {
    let Some(section) = find_section(footer, section_kind::EXEMPLARS) else {
        return Ok(Vec::new());
    };
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let dict_index = index_label_dict(&dict_bytes)?;
    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let body = decode_section_bytes(section, exemplars_bytes, limits)?;
    decode_exemplars_from_decoded(
        &body,
        footer.min_event_ts_ns,
        footer.series_count,
        &mut resolver,
    )
}

/// Public per-series probe, mirroring [`decode_exemplars_section`]'s
/// stored-bytes contract. An absent EXEMPLARS section is legal and returns
/// `Ok(vec![])`.
pub fn probe_exemplars_by_series(
    footer: &Footer,
    label_dict_bytes: &[u8],
    exemplars_bytes: &[u8],
    limits: ReaderLimits,
    target_series_index: u64,
) -> Result<Vec<ExemplarRecord>, SegmentError> {
    let Some(section) = find_section(footer, section_kind::EXEMPLARS) else {
        return Ok(Vec::new());
    };
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let dict_bytes = decode_section_bytes(label_dict_section, label_dict_bytes, limits)?;
    let dict_index = index_label_dict(&dict_bytes)?;
    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let body = decode_section_bytes(section, exemplars_bytes, limits)?;
    probe_exemplars_from_decoded(
        &body,
        footer.min_event_ts_ns,
        footer.series_count,
        target_series_index,
        &mut resolver,
    )
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::reader::index_label_dict;
    use crate::varint::write_uvarint as raw_write_uvarint;

    /// Builds a tiny LABEL_DICT (count:u32 + len-prefixed strings) and its
    /// index, for a `DictResolver` in tests.
    fn dict_bytes(strings: &[&str]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(strings.len() as u32).to_le_bytes());
        for s in strings {
            raw_write_uvarint(&mut buf, s.len() as u64);
            buf.extend_from_slice(s.as_bytes());
        }
        buf
    }

    fn resolved(series_index: u32, ts_ns: i64, value: f64, attr_ords: Vec<(u32, u32)>) -> ResolvedExemplar {
        ResolvedExemplar {
            series_index,
            ts_ns,
            value,
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            attr_ords,
        }
    }

    #[test]
    fn exemplar_section_roundtrips_and_probes_by_series() {
        // Dictionary: ordinal 0 "trace_id", 1 "span_id", 2 "svc", 3 "checkout".
        let dict = dict_bytes(&["trace_id", "span_id", "svc", "checkout"]);
        let index = index_label_dict(&dict).expect("index");

        let min_event_ts_ns = 1_000i64;
        let records = vec![
            resolved(0, 1_000, 1.5, vec![(2, 3)]),
            resolved(0, 1_200, f64::NAN, vec![]),
            resolved(2, 1_050, -0.0, vec![(2, 3), (0, 1)]),
            resolved(5, 2_000, f64::MAX, vec![]),
        ];
        let encoded = encode_exemplars_section(&records, min_event_ts_ns).expect("encode");

        // Whole-section decode round-trips every field exactly, including
        // NaN and -0.0 bit patterns (never compared with `==`).
        let mut resolver = DictResolver::new(&dict, &index);
        let decoded = decode_exemplars_from_decoded(&encoded, min_event_ts_ns, 6, &mut resolver)
            .expect("whole decode");
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded[0].series_index, 0);
        assert_eq!(decoded[0].ts_ns, 1_000);
        assert_eq!(decoded[0].value.to_bits(), 1.5f64.to_bits());
        assert_eq!(decoded[0].trace_id, [0xAB; 16]);
        assert_eq!(decoded[0].span_id, [0xCD; 8]);
        assert_eq!(
            decoded[0].attrs,
            vec![("svc".to_string(), "checkout".to_string())]
        );
        assert_eq!(decoded[1].value.to_bits(), f64::NAN.to_bits());
        assert_eq!(decoded[2].value.to_bits(), (-0.0f64).to_bits());
        assert_eq!(
            decoded[2].attrs,
            vec![
                ("svc".to_string(), "checkout".to_string()),
                ("trace_id".to_string(), "span_id".to_string()),
            ]
        );
        assert_eq!(decoded[3].value.to_bits(), f64::MAX.to_bits());

        // Probe for series_index 2 returns only that series' record, with
        // attrs resolved, via the early-exit scan.
        let mut resolver = DictResolver::new(&dict, &index);
        let probed = probe_exemplars_from_decoded(&encoded, min_event_ts_ns, 6, 2, &mut resolver)
            .expect("probe series 2");
        assert_eq!(probed.len(), 1);
        assert_eq!(probed[0].series_index, 2);
        assert_eq!(probed[0].value.to_bits(), (-0.0f64).to_bits());
        assert_eq!(
            probed[0].attrs,
            vec![
                ("svc".to_string(), "checkout".to_string()),
                ("trace_id".to_string(), "span_id".to_string()),
            ]
        );

        // Probe for a series with no exemplar returns empty, not an error.
        let mut resolver = DictResolver::new(&dict, &index);
        let probed_none = probe_exemplars_from_decoded(&encoded, min_event_ts_ns, 6, 1, &mut resolver)
            .expect("probe series 1");
        assert!(probed_none.is_empty());

        // The probe for series_index 0 never reaches record 2's corrupted
        // bytes, so it succeeds even when a later record is corrupt; only
        // the whole-section decode reaches far enough to catch it.
        let mut corrupt = encoded.clone();
        let last_byte = corrupt.len() - 1;
        corrupt[last_byte] ^= 0xFF;
        let mut resolver = DictResolver::new(&dict, &index);
        let probed_early = probe_exemplars_from_decoded(&corrupt, min_event_ts_ns, 6, 0, &mut resolver)
            .expect("probe for an early series must not touch corrupted later bytes");
        // Two records carry series_index 0 (ts 1_000 and ts 1_200); the
        // probe fully resolves both before it reaches record 2's
        // series_index > 0 and breaks, so it never touches the corrupted
        // tail in record 3.
        assert_eq!(probed_early.len(), 2);
        assert_eq!(probed_early[0].value.to_bits(), 1.5f64.to_bits());
        assert_eq!(probed_early[1].value.to_bits(), f64::NAN.to_bits());
        let mut resolver = DictResolver::new(&dict, &index);
        decode_exemplars_from_decoded(&corrupt, min_event_ts_ns, 6, &mut resolver)
            .expect_err("whole decode must reach and reject the corrupted tail");
    }

    #[test]
    fn no_exemplars_emits_no_section() {
        // Encoding an empty resolved list is never called by the writer
        // (write_v4 skips the section entirely when resolved is empty);
        // this pins that an empty input encodes to just the zero count,
        // which callers must never emit as a section (absence is the only
        // legal "no exemplars" representation, ADR-0047 decision 1).
        let encoded = encode_exemplars_section(&[], 0).expect("encode empty");
        assert_eq!(encoded, 0u32.to_le_bytes().to_vec());
    }

    #[test]
    fn rejects_out_of_range_series_index() {
        let dict = dict_bytes(&["a", "b"]);
        let index = index_label_dict(&dict).expect("index");
        let records = vec![resolved(3, 0, 1.0, vec![])];
        let encoded = encode_exemplars_section(&records, 0).expect("encode");
        let mut resolver = DictResolver::new(&dict, &index);
        assert!(matches!(
            decode_exemplars_from_decoded(&encoded, 0, 3, &mut resolver),
            Err(SegmentError::ExemplarSeriesIndexOutOfRange(3))
        ));
    }

    #[test]
    fn rejects_out_of_range_dict_ordinal() {
        let dict = dict_bytes(&["a", "b"]);
        let index = index_label_dict(&dict).expect("index");
        let records = vec![resolved(0, 0, 1.0, vec![(9, 0)])];
        let encoded = encode_exemplars_section(&records, 0).expect("encode");
        let mut resolver = DictResolver::new(&dict, &index);
        assert!(matches!(
            decode_exemplars_from_decoded(&encoded, 0, 1, &mut resolver),
            Err(SegmentError::BadOrdinal(9))
        ));
    }

    #[test]
    fn rejects_non_ascending_records() {
        let dict = dict_bytes(&["a"]);
        let index = index_label_dict(&dict).expect("index");
        // Hand-build two records out of (series_index, ts_ns) order: the
        // writer always sorts, so this can only arise from a corrupt or
        // hand-crafted object.
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        write_uvarint(&mut buf, 1);
        write_zigzag_varint(&mut buf, 100);
        buf.extend_from_slice(&1.0f64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 16]);
        buf.extend_from_slice(&[0u8; 8]);
        write_uvarint(&mut buf, 0);
        write_uvarint(&mut buf, 0); // second record: series_index 0 < 1
        write_zigzag_varint(&mut buf, 50);
        buf.extend_from_slice(&2.0f64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 16]);
        buf.extend_from_slice(&[0u8; 8]);
        write_uvarint(&mut buf, 0);

        let mut resolver = DictResolver::new(&dict, &index);
        assert!(matches!(
            decode_exemplars_from_decoded(&buf, 0, 2, &mut resolver),
            Err(SegmentError::ExemplarRecordsUnsorted)
        ));
    }

    #[test]
    fn rejects_truncated_record() {
        let dict = dict_bytes(&["a"]);
        let index = index_label_dict(&dict).expect("index");
        let records = vec![resolved(0, 0, 1.0, vec![])];
        let mut encoded = encode_exemplars_section(&records, 0).expect("encode");
        encoded.truncate(encoded.len() - 3);
        let mut resolver = DictResolver::new(&dict, &index);
        assert!(matches!(
            decode_exemplars_from_decoded(&encoded, 0, 1, &mut resolver),
            Err(SegmentError::Truncated)
        ));
    }

    #[test]
    fn rejects_count_larger_than_payload() {
        let dict = dict_bytes(&["a"]);
        let index = index_label_dict(&dict).expect("index");
        let mut buf = Vec::new();
        buf.extend_from_slice(&5u32.to_le_bytes()); // declares 5, payload has 0
        let mut resolver = DictResolver::new(&dict, &index);
        assert!(matches!(
            decode_exemplars_from_decoded(&buf, 0, 1, &mut resolver),
            Err(SegmentError::Truncated)
        ));
    }

    #[test]
    fn rejects_trailing_bytes() {
        let dict = dict_bytes(&["a"]);
        let index = index_label_dict(&dict).expect("index");
        let records = vec![resolved(0, 0, 1.0, vec![])];
        let mut encoded = encode_exemplars_section(&records, 0).expect("encode");
        encoded.push(0xFF);
        let mut resolver = DictResolver::new(&dict, &index);
        assert!(matches!(
            decode_exemplars_from_decoded(&encoded, 0, 1, &mut resolver),
            Err(SegmentError::TrailingBytes)
        ));
    }
}

/// Corrupt-input mutation harness, mirroring `varint.rs`/`ts_delta.rs`
/// (docs/segment-format.md, CLAUDE.md testing patterns): the whole-section
/// decode over arbitrary/mutated bytes must only ever return a typed error
/// or a valid decode, never panic.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod fuzz_mutation {
    use super::*;
    use crate::reader::index_label_dict;
    use proptest::prelude::*;

    fn mutate(bytes: &[u8], bit: usize, truncate_to: usize) -> Vec<u8> {
        let mut out = bytes.to_vec();
        if !out.is_empty() {
            let byte = (bit / 8) % out.len();
            out[byte] ^= 1u8 << (bit % 8);
        }
        let cut = if out.is_empty() {
            0
        } else {
            truncate_to % (out.len() + 1)
        };
        out.truncate(cut);
        out
    }

    proptest! {
        #[test]
        fn decode_exemplars_section_never_panics(
            series_index in 0u32..8,
            ts_ns in -10_000i64..10_000,
            value in any::<f64>(),
            trace_id in any::<[u8; 16]>(),
            span_id in any::<[u8; 8]>(),
            bit in any::<usize>(),
            truncate_to in any::<usize>(),
        ) {
            let dict = {
                let mut buf = Vec::new();
                buf.extend_from_slice(&2u32.to_le_bytes());
                for s in ["name", "value"] {
                    crate::varint::write_uvarint(&mut buf, s.len() as u64);
                    buf.extend_from_slice(s.as_bytes());
                }
                buf
            };
            let index = index_label_dict(&dict).expect("index");
            let records = vec![ResolvedExemplar {
                series_index,
                ts_ns,
                value,
                trace_id,
                span_id,
                attr_ords: vec![(0, 1)],
            }];
            let encoded = encode_exemplars_section(&records, 0).expect("encode");
            let corrupt = mutate(&encoded, bit, truncate_to);

            let mut resolver = DictResolver::new(&dict, &index);
            let _ = decode_exemplars_from_decoded(&corrupt, 0, 16, &mut resolver);
            let mut resolver = DictResolver::new(&dict, &index);
            let _ = probe_exemplars_from_decoded(&corrupt, 0, 16, 0, &mut resolver);
        }
    }
}

//! RSEG v1 writer: builds a complete segment object from per-series sample
//! batches plus segment identity, per docs/segment-format.md.

use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;
use prost::Message;
use ravel_proto::segment::v1::{Footer, Section};
use ravel_types::{LabelSet, METRIC_NAME_LABEL, SeriesId};

use crate::crc::{footer_crc, page_crc};
use crate::error::WriteError;
use crate::format::{
    MAGIC, RESERVED, SIGNAL_METRICS, VERSION, ZSTD_LEVEL, compression, page_comp, page_enc,
    section_kind,
};
use crate::gorilla::encode_gorilla;
use crate::ts_delta::encode_ts_deltas;
use crate::varint::write_uvarint;

/// One series' identity, labels (including `__name__`), and samples.
/// Samples need not be pre-sorted; the writer stable-sorts by `ts_ns`.
pub struct SeriesInput {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub samples: Vec<ravel_types::Sample>,
}

/// Segment-wide identity fields recorded in the footer (ADR-0010 §1/§3).
pub struct SegmentIdentity {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
    pub writer_id: String,
    pub writer_epoch: u64,
    pub writer_seq: u64,
}

/// Ingest-time bounds for the batch, distinct from event-time bounds.
pub struct IngestBounds {
    pub min_ingest_ts_ns: i64,
    pub max_ingest_ts_ns: i64,
}

/// Stats a caller needs after a successful write (for the commit record).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSummary {
    pub min_event_ts_ns: i64,
    pub max_event_ts_ns: i64,
    pub sample_count: u64,
    pub series_count: u64,
    pub blake3: [u8; 32],
}

/// The encoded object plus the summary derived while building it.
pub struct WrittenSegment {
    pub bytes: Bytes,
    pub summary: SegmentSummary,
}

/// Builds RSEG v1 metric segment objects. Stateless; `write` is the entire
/// API surface.
pub struct SegmentWriter;

impl SegmentWriter {
    /// Encodes `series` into one segment object.
    ///
    /// Series with zero samples are dropped (a page requires at least one
    /// value); a segment with no series at all is a valid, empty object.
    /// Duplicate `series_id`s across the input batch are rejected: the
    /// format's SERIES_TABLE requires exactly one entry per id.
    pub fn write(
        mut series: Vec<SeriesInput>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
    ) -> Result<WrittenSegment, WriteError> {
        for s in &mut series {
            // Stable sort: ties (equal ts_ns) keep insertion order.
            s.samples.sort_by_key(|sample| sample.ts_ns);
        }
        series.retain(|s| !s.samples.is_empty());

        {
            let mut ids: Vec<[u8; 16]> = series.iter().map(|s| s.series_id.0).collect();
            ids.sort();
            if ids.windows(2).any(|w| w[0] == w[1]) {
                return Err(WriteError::DuplicateSeriesId);
            }
        }
        series.sort_by_key(|s| s.series_id.0);

        let series_count = u64::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;
        let mut sample_count: u64 = 0;
        let mut min_event_ts_ns = i64::MAX;
        let mut max_event_ts_ns = i64::MIN;
        for s in &series {
            sample_count = sample_count
                .checked_add(s.samples.len() as u64)
                .ok_or(WriteError::TooManySamples)?;
            if let Some(first) = s.samples.first() {
                min_event_ts_ns = min_event_ts_ns.min(first.ts_ns);
            }
            if let Some(last) = s.samples.last() {
                max_event_ts_ns = max_event_ts_ns.max(last.ts_ns);
            }
        }
        if series.is_empty() {
            min_event_ts_ns = 0;
            max_event_ts_ns = 0;
        }

        let dict = build_dictionary(&series);
        // Validate the count fits the on-disk u32 field *before* using it to
        // build ordinals, so a hypothetical oversized dictionary errors
        // cleanly instead of silently wrapping via truncation.
        u32::try_from(dict.len()).map_err(|_| WriteError::TooManyDictStrings)?;
        let ordinal_of: HashMap<&str, u32> = dict
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i as u32))
            .collect();

        let mut ts_pages = Vec::new();
        let mut val_pages = Vec::new();
        let series_table_raw =
            encode_series_table(&series, &ordinal_of, &mut ts_pages, &mut val_pages)?;
        let label_dict_raw = encode_label_dict(&dict)?;

        let label_dict_compressed = zstd_compress(&label_dict_raw)?;
        let series_table_compressed = zstd_compress(&series_table_raw)?;

        let mut object = Vec::with_capacity(
            label_dict_compressed.len()
                + series_table_compressed.len()
                + ts_pages.len()
                + val_pages.len()
                + 512,
        );

        let label_dict_offset = object.len() as u64;
        object.extend_from_slice(&label_dict_compressed);
        let series_table_offset = object.len() as u64;
        object.extend_from_slice(&series_table_compressed);
        let ts_pages_offset = object.len() as u64;
        object.extend_from_slice(&ts_pages);
        let val_pages_offset = object.len() as u64;
        object.extend_from_slice(&val_pages);

        let sections = vec![
            Section {
                kind: section_kind::LABEL_DICT,
                offset: label_dict_offset,
                len: label_dict_compressed.len() as u64,
                crc32c: crc32c::crc32c(&label_dict_compressed),
                comp: compression::ZSTD,
                uncompressed_len: label_dict_raw.len() as u64,
            },
            Section {
                kind: section_kind::SERIES_TABLE,
                offset: series_table_offset,
                len: series_table_compressed.len() as u64,
                crc32c: crc32c::crc32c(&series_table_compressed),
                comp: compression::ZSTD,
                uncompressed_len: series_table_raw.len() as u64,
            },
            Section {
                kind: section_kind::TS_PAGES,
                offset: ts_pages_offset,
                len: ts_pages.len() as u64,
                crc32c: crc32c::crc32c(&ts_pages),
                comp: compression::NONE,
                uncompressed_len: ts_pages.len() as u64,
            },
            Section {
                kind: section_kind::VAL_PAGES,
                offset: val_pages_offset,
                len: val_pages.len() as u64,
                crc32c: crc32c::crc32c(&val_pages),
                comp: compression::NONE,
                uncompressed_len: val_pages.len() as u64,
            },
        ];

        let footer = Footer {
            tenant_hash: identity.tenant_hash.to_vec(),
            shard: identity.shard,
            writer_id: identity.writer_id,
            writer_epoch: identity.writer_epoch,
            writer_seq: identity.writer_seq,
            min_event_ts_ns,
            max_event_ts_ns,
            min_ingest_ts_ns: ingest_bounds.min_ingest_ts_ns,
            max_ingest_ts_ns: ingest_bounds.max_ingest_ts_ns,
            sample_count,
            series_count,
            sections,
        };

        let footer_bytes = footer.encode_to_vec();
        let footer_len =
            u32::try_from(footer_bytes.len()).map_err(|_| WriteError::FooterTooLarge)?;
        object.extend_from_slice(&footer_bytes);

        let crc = footer_crc(&footer_bytes, footer_len, VERSION, SIGNAL_METRICS, RESERVED);

        object.extend_from_slice(&footer_len.to_le_bytes());
        object.extend_from_slice(&crc.to_le_bytes());
        object.extend_from_slice(&VERSION.to_le_bytes());
        object.push(SIGNAL_METRICS);
        object.push(RESERVED);
        object.extend_from_slice(&MAGIC);

        let blake3 = *blake3::hash(&object).as_bytes();

        Ok(WrittenSegment {
            bytes: Bytes::from(object),
            summary: SegmentSummary {
                min_event_ts_ns,
                max_event_ts_ns,
                sample_count,
                series_count,
                blake3,
            },
        })
    }
}

/// Ordinal 0 is always `"__name__"` (docs/segment-format.md); the rest of
/// the distinct label names/values used across the batch follow in sorted
/// order (arbitrary but deterministic -- the reader locates strings purely
/// by ordinal).
fn build_dictionary(series: &[SeriesInput]) -> Vec<String> {
    let mut set: BTreeSet<&str> = BTreeSet::new();
    for s in series {
        for label in s.labels.iter() {
            set.insert(label.name.as_str());
            set.insert(label.value.as_str());
        }
    }
    set.remove(METRIC_NAME_LABEL);
    let mut dict = Vec::with_capacity(set.len() + 1);
    dict.push(METRIC_NAME_LABEL.to_string());
    dict.extend(set.into_iter().map(str::to_string));
    dict
}

fn encode_label_dict(dict: &[String]) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::new();
    let count = u32::try_from(dict.len()).map_err(|_| WriteError::TooManyDictStrings)?;
    buf.extend_from_slice(&count.to_le_bytes());
    for s in dict {
        let bytes = s.as_bytes();
        write_uvarint(&mut buf, bytes.len() as u64);
        buf.extend_from_slice(bytes);
    }
    Ok(buf)
}

fn encode_series_table(
    series: &[SeriesInput],
    ordinal_of: &HashMap<&str, u32>,
    ts_pages: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::new();
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;
    buf.extend_from_slice(&count.to_le_bytes());

    for s in series {
        buf.extend_from_slice(&s.series_id.0);

        let label_count = u16::try_from(s.labels.len()).map_err(|_| WriteError::TooManyLabels)?;
        buf.extend_from_slice(&label_count.to_le_bytes());
        for label in s.labels.iter() {
            let name_ord = *ordinal_of
                .get(label.name.as_str())
                .ok_or(WriteError::DictionaryInvariant)?;
            let value_ord = *ordinal_of
                .get(label.value.as_str())
                .ok_or(WriteError::DictionaryInvariant)?;
            write_uvarint(&mut buf, u64::from(name_ord));
            write_uvarint(&mut buf, u64::from(value_ord));
        }

        let sample_count =
            u32::try_from(s.samples.len()).map_err(|_| WriteError::TooManySamples)?;
        buf.extend_from_slice(&sample_count.to_le_bytes());

        // Non-empty by construction (empty series are dropped before this
        // function is called).
        let min_ts_ns = s.samples.first().map_or(0, |sm| sm.ts_ns);
        let max_ts_ns = s.samples.last().map_or(0, |sm| sm.ts_ns);
        buf.extend_from_slice(&min_ts_ns.to_le_bytes());
        buf.extend_from_slice(&max_ts_ns.to_le_bytes());

        let ts_page_offset = ts_pages.len() as u64;
        build_ts_page(ts_pages, &s.series_id, &s.samples)?;
        let ts_page_len = ts_pages.len() as u64 - ts_page_offset;
        write_uvarint(&mut buf, ts_page_offset);
        write_uvarint(&mut buf, ts_page_len);

        let val_page_offset = val_pages.len() as u64;
        build_val_page(val_pages, &s.series_id, &s.samples);
        let val_page_len = val_pages.len() as u64 - val_page_offset;
        write_uvarint(&mut buf, val_page_offset);
        write_uvarint(&mut buf, val_page_len);
    }
    Ok(buf)
}

/// Encodes the TS page for `samples` straight into `ts_pages` (offset
/// `ts_pages.len()` before the call): header (enc, comp, crc) followed by
/// the compressed payload, with no intermediate whole-page buffer.
fn build_ts_page(
    ts_pages: &mut Vec<u8>,
    series_id: &SeriesId,
    samples: &[ravel_types::Sample],
) -> Result<(), WriteError> {
    let timestamps: Vec<i64> = samples.iter().map(|s| s.ts_ns).collect();
    let raw = encode_ts_deltas(&timestamps).ok_or(WriteError::TimestampDeltaOverflow)?;
    let compressed = lz4_flex::compress_prepend_size(&raw);
    let enc = page_enc::TS_DELTA_VARINT;
    let comp = page_comp::LZ4;
    let crc = page_crc(&series_id.0, enc, comp, &compressed);

    ts_pages.reserve(6 + compressed.len());
    ts_pages.push(enc);
    ts_pages.push(comp);
    ts_pages.extend_from_slice(&crc.to_le_bytes());
    ts_pages.extend_from_slice(&compressed);
    Ok(())
}

/// Encodes the VAL page for `samples` straight into `val_pages`, mirroring
/// [`build_ts_page`]'s direct-write shape.
fn build_val_page(val_pages: &mut Vec<u8>, series_id: &SeriesId, samples: &[ravel_types::Sample]) {
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
    let count = values.len() as u64;
    let gorilla = encode_gorilla(&values);
    let (enc, payload) = if (gorilla.len() as u64) >= 8 * count {
        (page_enc::VAL_RAW_F64, encode_raw_f64(&values))
    } else {
        (page_enc::VAL_GORILLA, gorilla)
    };
    let comp = page_comp::NONE;
    let crc = page_crc(&series_id.0, enc, comp, &payload);

    val_pages.reserve(6 + payload.len());
    val_pages.push(enc);
    val_pages.push(comp);
    val_pages.extend_from_slice(&crc.to_le_bytes());
    val_pages.extend_from_slice(&payload);
}

fn encode_raw_f64(values: &[f64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 8);
    for v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf
}

fn zstd_compress(data: &[u8]) -> Result<Vec<u8>, WriteError> {
    zstd::bulk::compress(data, ZSTD_LEVEL).map_err(|e| WriteError::Zstd(e.to_string()))
}

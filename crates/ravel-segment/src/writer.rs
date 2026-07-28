//! RSEG v1 writer: builds a complete segment object from per-series sample
//! batches plus segment identity, per docs/segment-format.md.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use bytes::Bytes;
use prost::Message;
use ravel_proto::segment::v1::{Footer, Section};
use ravel_types::{LabelSet, METRIC_NAME_LABEL, SeriesId};

use crate::crc::{footer_crc, page_crc};
use crate::error::WriteError;
use crate::format::{
    MAGIC, RESERVED, SIGNAL_METRICS, VERSION, VERSION_V2, VERSION_V3, VERSION_V4, ZSTD_LEVEL,
    compression, page_comp, page_enc, section_kind,
};
use crate::gorilla::encode_gorilla_into;
use crate::histogram::{HistogramValue, encode_histogram_record_into};
use crate::ts_delta::encode_ts_deltas_into;
use crate::varint::write_uvarint;

/// One series' identity, labels (including `__name__`), and samples.
/// Samples need not be pre-sorted; the writer stable-sorts by `ts_ns`.
#[derive(Debug)]
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

/// One histogram sample: an event timestamp paired with a native-histogram
/// value (docs/rseg-v3-plan.md section 2).
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSample {
    pub ts_ns: i64,
    pub value: HistogramValue,
}

/// A series' sample payload for RSEG v3: exactly one of scalar or
/// histogram samples, fixed for the series' whole life in one segment
/// (`value_kind`, docs/rseg-v3-plan.md section 3.4).
#[derive(Debug, Clone, PartialEq)]
pub enum SeriesValues {
    Scalar(Vec<ravel_types::Sample>),
    Histogram(Vec<HistogramSample>),
}

impl SeriesValues {
    fn len(&self) -> usize {
        match self {
            SeriesValues::Scalar(v) => v.len(),
            SeriesValues::Histogram(v) => v.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stable sort by `ts_ns`: ties keep insertion order.
    fn sort_by_ts(&mut self) {
        match self {
            SeriesValues::Scalar(v) => v.sort_by_key(|s| s.ts_ns),
            SeriesValues::Histogram(v) => v.sort_by_key(|s| s.ts_ns),
        }
    }

    fn first_ts(&self) -> Option<i64> {
        match self {
            SeriesValues::Scalar(v) => v.first().map(|s| s.ts_ns),
            SeriesValues::Histogram(v) => v.first().map(|s| s.ts_ns),
        }
    }

    fn last_ts(&self) -> Option<i64> {
        match self {
            SeriesValues::Scalar(v) => v.last().map(|s| s.ts_ns),
            SeriesValues::Histogram(v) => v.last().map(|s| s.ts_ns),
        }
    }

    fn ts_values(&self) -> Vec<i64> {
        match self {
            SeriesValues::Scalar(v) => v.iter().map(|s| s.ts_ns).collect(),
            SeriesValues::Histogram(v) => v.iter().map(|s| s.ts_ns).collect(),
        }
    }
}

/// One series' identity, labels, and RSEG v3 sample payload. Deliberately
/// a separate type from [`SeriesInput`] (not a shared/extended struct): v1
/// and v2 callers, including `ravel-ingest`, must never need to know
/// `SeriesValues` exists.
#[derive(Debug)]
pub struct SeriesInputV3 {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub values: SeriesValues,
}

/// One run's provenance, event-time bounds, and pre-encoded page bytes for
/// RSEG v4 (docs/compaction-retention-plan.md section 4). `ts_page` and
/// `value_page` are fully framed (6-byte page header -- enc, comp,
/// crc32c -- then payload) exactly as read from an input object;
/// `write_v4` never decodes or re-encodes them, only copies them
/// verbatim into the output's page sections.
#[derive(Debug, Clone)]
pub struct RunInputV4 {
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub sample_count: u32,
    pub ts_page: Vec<u8>,
    pub value_page: RunValuePageV4,
}

/// A run's VAL or HIST page, fully framed. The variant fixes the owning
/// series' `value_kind` for its whole life (docs/rseg-v3-plan.md section
/// 3.4, generalized to run granularity in v4); mixing variants within one
/// series is rejected (`WriteError::MixedValueKindInSeries`).
#[derive(Debug, Clone)]
pub enum RunValuePageV4 {
    Scalar(Vec<u8>),
    Histogram(Vec<u8>),
}

/// One series' identity, labels, and ordered run list for RSEG v4.
/// Deliberately a separate type from [`SeriesInputV3`]: a v4 caller
/// (`ravel-maintain`) supplies pre-encoded run pages copied verbatim from
/// input objects, never raw samples.
#[derive(Debug)]
pub struct SeriesInputV4 {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub runs: Vec<RunInputV4>,
}

/// Compaction-specific Footer provenance a caller supplies (ADR-0018).
/// `base_created_unix_ns` is deliberately excluded: `write_v4` derives it
/// itself as the min `created_unix_ns` over all runs, the same way
/// `min_event_ts_ns` is derived from run bounds rather than taken from the
/// caller.
#[derive(Debug, Clone)]
pub struct CompactionMetaV4 {
    pub ingest_hour_bucket: u32,
    pub input_set_hash: [u8; 32],
    pub part_index: u32,
    pub level: u32,
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

        // Ids are unique after the duplicate check below, so an unstable
        // sort is equivalent; sorting first lets the check run on adjacent
        // entries without a second ids-only sort.
        series.sort_unstable_by_key(|s| s.series_id.0);
        if series
            .windows(2)
            .any(|w| w[0].series_id.0 == w[1].series_id.0)
        {
            return Err(WriteError::DuplicateSeriesId);
        }

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

        let dict = build_dictionary(&series)?;

        let total_samples =
            usize::try_from(sample_count).map_err(|_| WriteError::TooManySamples)?;
        // Rough per-page upper bounds: 6-byte header plus delta varints
        // (ts) or near-raw f64s (val). Over-reserving trades a little
        // memory for zero reallocation during the append-only build.
        let mut ts_pages = Vec::with_capacity(series.len() * 16 + total_samples * 4);
        let mut val_pages = Vec::with_capacity(series.len() * 16 + total_samples * 9);
        let series_table_raw = encode_series_table(
            &series,
            &dict.occurrence_ordinals,
            &mut ts_pages,
            &mut val_pages,
        )?;
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
            // v1/v2/v3 never populate the v4 compaction-provenance fields.
            ..Default::default()
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

    /// Encodes `series` into one RSEG v2 segment object (ADR-0014,
    /// docs/segment-format.md "RSEG v2 amendment"). Same writer edge rules
    /// as `write` (zero-sample series dropped, duplicate ids rejected, an
    /// empty segment records bounds = 0); v2 changes only the catalog
    /// layout: LABEL_DICT drops the distinct-string sort (first-occurrence
    /// order instead), SERIES_TABLE is replaced by SERIES_IDS + SERIES_META
    /// (a schema dictionary plus 9 columnar blocks), and VAL_PAGES gains an
    /// 8-byte alignment rule for VAL_RAW_F64 page payloads. Page format,
    /// page encodings, sample ordering, and the raw-fallback rule are
    /// unchanged from v1.
    ///
    /// The preamble below (sort samples, drop empty series, sort by id,
    /// reject duplicates, compute footer bounds) is a deliberate copy of
    /// `write`'s preamble, not a shared helper: RSEG v1 is a frozen
    /// contract proved byte-identical by the golden-bytes test, and this
    /// duplication keeps the v1 code path trivially provable as untouched
    /// by inspection. Do not factor this into a shared function -- fix v1
    /// bugs in `write` and v2 bugs here, independently.
    pub fn write_v2(
        mut series: Vec<SeriesInput>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
    ) -> Result<WrittenSegment, WriteError> {
        for s in &mut series {
            // Stable sort: ties (equal ts_ns) keep insertion order.
            s.samples.sort_by_key(|sample| sample.ts_ns);
        }
        series.retain(|s| !s.samples.is_empty());

        series.sort_unstable_by_key(|s| s.series_id.0);
        if series
            .windows(2)
            .any(|w| w[0].series_id.0 == w[1].series_id.0)
        {
            return Err(WriteError::DuplicateSeriesId);
        }

        for s in &series {
            u16::try_from(s.labels.len()).map_err(|_| WriteError::TooManyLabels)?;
        }

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

        let dict = build_dictionary_v2(&series)?;

        let total_samples =
            usize::try_from(sample_count).map_err(|_| WriteError::TooManySamples)?;
        let mut ts_pages = Vec::with_capacity(series.len() * 16 + total_samples * 4);
        let mut val_pages =
            Vec::with_capacity(series.len() * 16 + total_samples * 9 + series.len() * 4);

        let series_meta_raw = build_series_meta_v2(
            &series,
            &dict.occurrence_ordinals,
            min_event_ts_ns,
            &mut ts_pages,
            &mut val_pages,
        )?;
        let series_ids_raw = encode_series_ids_v2(&series)?;
        let label_dict_raw = encode_label_dict_v2(&dict)?;

        let label_dict_compressed = zstd_compress_v2(&label_dict_raw)?;
        let series_meta_compressed = zstd_compress_v2(&series_meta_raw)?;
        // SERIES_IDS is deliberately never zstd-compressed: BLAKE3 ids are
        // incompressible, so it is stored raw (docs/segment-format.md v2
        // amendment).

        let mut object = Vec::with_capacity(
            label_dict_compressed.len()
                + series_ids_raw.len()
                + series_meta_compressed.len()
                + ts_pages.len()
                + val_pages.len()
                + 512,
        );

        // Physical section order 1, 5, 6, 3, 4 (LABEL_DICT, SERIES_IDS,
        // SERIES_META, TS_PAGES, VAL_PAGES).
        let label_dict_offset = object.len() as u64;
        object.extend_from_slice(&label_dict_compressed);

        let series_ids_offset = object.len() as u64;
        object.extend_from_slice(&series_ids_raw);

        let series_meta_offset = object.len() as u64;
        object.extend_from_slice(&series_meta_compressed);

        let ts_pages_offset = object.len() as u64;
        object.extend_from_slice(&ts_pages);

        // 8-byte-align the VAL_PAGES section offset (docs/segment-format.md
        // "VAL_RAW_F64 page alignment, v2"); any inter-section pad bytes
        // must be 0x00.
        let val_pad = (8 - (object.len() % 8)) % 8;
        object.extend(std::iter::repeat_n(0u8, val_pad));
        let val_pages_offset = object.len() as u64;
        debug_assert_eq!(
            val_pages_offset % 8,
            0,
            "VAL_PAGES section must be 8-byte aligned"
        );
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
                kind: section_kind::SERIES_IDS,
                offset: series_ids_offset,
                len: series_ids_raw.len() as u64,
                crc32c: crc32c::crc32c(&series_ids_raw),
                comp: compression::NONE,
                uncompressed_len: series_ids_raw.len() as u64,
            },
            Section {
                kind: section_kind::SERIES_META,
                offset: series_meta_offset,
                len: series_meta_compressed.len() as u64,
                crc32c: crc32c::crc32c(&series_meta_compressed),
                comp: compression::ZSTD,
                uncompressed_len: series_meta_raw.len() as u64,
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
            // v1/v2/v3 never populate the v4 compaction-provenance fields.
            ..Default::default()
        };

        let footer_bytes = footer.encode_to_vec();
        let footer_len =
            u32::try_from(footer_bytes.len()).map_err(|_| WriteError::FooterTooLarge)?;
        object.extend_from_slice(&footer_bytes);

        let crc = footer_crc(
            &footer_bytes,
            footer_len,
            VERSION_V2,
            SIGNAL_METRICS,
            RESERVED,
        );

        object.extend_from_slice(&footer_len.to_le_bytes());
        object.extend_from_slice(&crc.to_le_bytes());
        object.extend_from_slice(&VERSION_V2.to_le_bytes());
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

    /// Builds an RSEG v3 object (docs/rseg-v3-plan.md, docs/segment-format.md
    /// "RSEG v3 amendment"): v2's sections plus HIST_PAGES and SERIES_META's
    /// three new column blocks. VAL_PAGES is omitted entirely when no series
    /// is scalar-kind; HIST_PAGES is omitted entirely when no series is
    /// histogram-kind (section 3.2's conditional-mandatory-kinds rule) --
    /// unlike v1/v2, where VAL_PAGES is always present even for a
    /// zero-series segment.
    pub fn write_v3(
        mut series: Vec<SeriesInputV3>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
    ) -> Result<WrittenSegment, WriteError> {
        for s in &mut series {
            s.values.sort_by_ts();
        }
        series.retain(|s| !s.values.is_empty());

        series.sort_unstable_by_key(|s| s.series_id.0);
        if series
            .windows(2)
            .any(|w| w[0].series_id.0 == w[1].series_id.0)
        {
            return Err(WriteError::DuplicateSeriesId);
        }

        for s in &series {
            u16::try_from(s.labels.len()).map_err(|_| WriteError::TooManyLabels)?;
        }

        let series_count = u64::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;
        let mut sample_count: u64 = 0;
        let mut min_event_ts_ns = i64::MAX;
        let mut max_event_ts_ns = i64::MIN;
        for s in &series {
            sample_count = sample_count
                .checked_add(s.values.len() as u64)
                .ok_or(WriteError::TooManySamples)?;
            if let Some(first) = s.values.first_ts() {
                min_event_ts_ns = min_event_ts_ns.min(first);
            }
            if let Some(last) = s.values.last_ts() {
                max_event_ts_ns = max_event_ts_ns.max(last);
            }
        }
        if series.is_empty() {
            min_event_ts_ns = 0;
            max_event_ts_ns = 0;
        }

        let dict = build_dictionary_v3(&series)?;

        let total_samples =
            usize::try_from(sample_count).map_err(|_| WriteError::TooManySamples)?;
        let mut ts_pages = Vec::with_capacity(series.len() * 16 + total_samples * 4);
        let mut val_pages = Vec::with_capacity(series.len() * 16 + total_samples * 9);
        let mut hist_pages = Vec::with_capacity(series.len() * 16 + total_samples * 32);

        let series_meta_raw = build_series_meta_v3(
            &series,
            &dict.occurrence_ordinals,
            min_event_ts_ns,
            &mut ts_pages,
            &mut val_pages,
            &mut hist_pages,
        )?;
        let series_ids_raw = encode_series_ids_v3(&series)?;
        let label_dict_raw = encode_label_dict_v3(&dict)?;

        let label_dict_compressed = zstd_compress_v3(&label_dict_raw)?;
        let series_meta_compressed = zstd_compress_v3(&series_meta_raw)?;

        let mut object = Vec::with_capacity(
            label_dict_compressed.len()
                + series_ids_raw.len()
                + series_meta_compressed.len()
                + ts_pages.len()
                + val_pages.len()
                + hist_pages.len()
                + 512,
        );

        // Physical section order 1, 5, 6, 3, 4, 7 (LABEL_DICT, SERIES_IDS,
        // SERIES_META, TS_PAGES, VAL_PAGES, HIST_PAGES) per section 3.2.
        let label_dict_offset = object.len() as u64;
        object.extend_from_slice(&label_dict_compressed);

        let series_ids_offset = object.len() as u64;
        object.extend_from_slice(&series_ids_raw);

        let series_meta_offset = object.len() as u64;
        object.extend_from_slice(&series_meta_compressed);

        let ts_pages_offset = object.len() as u64;
        object.extend_from_slice(&ts_pages);

        let mut sections = vec![
            Section {
                kind: section_kind::LABEL_DICT,
                offset: label_dict_offset,
                len: label_dict_compressed.len() as u64,
                crc32c: crc32c::crc32c(&label_dict_compressed),
                comp: compression::ZSTD,
                uncompressed_len: label_dict_raw.len() as u64,
            },
            Section {
                kind: section_kind::SERIES_IDS,
                offset: series_ids_offset,
                len: series_ids_raw.len() as u64,
                crc32c: crc32c::crc32c(&series_ids_raw),
                comp: compression::NONE,
                uncompressed_len: series_ids_raw.len() as u64,
            },
            Section {
                kind: section_kind::SERIES_META,
                offset: series_meta_offset,
                len: series_meta_compressed.len() as u64,
                crc32c: crc32c::crc32c(&series_meta_compressed),
                comp: compression::ZSTD,
                uncompressed_len: series_meta_raw.len() as u64,
            },
            Section {
                kind: section_kind::TS_PAGES,
                offset: ts_pages_offset,
                len: ts_pages.len() as u64,
                crc32c: crc32c::crc32c(&ts_pages),
                comp: compression::NONE,
                uncompressed_len: ts_pages.len() as u64,
            },
        ];

        // VAL_PAGES: present only when at least one series is scalar-kind
        // (section 3.2's conditional-mandatory-kinds rule).
        if !val_pages.is_empty() {
            // 8-byte-align the VAL_PAGES section offset, unchanged from v2
            // (section 3.2, "v3 writers 8-byte-align VAL_PAGES exactly as
            // v2").
            let val_pad = (8 - (object.len() % 8)) % 8;
            object.extend(std::iter::repeat_n(0u8, val_pad));
            let val_pages_offset = object.len() as u64;
            debug_assert_eq!(
                val_pages_offset % 8,
                0,
                "VAL_PAGES section must be 8-byte aligned"
            );
            object.extend_from_slice(&val_pages);
            sections.push(Section {
                kind: section_kind::VAL_PAGES,
                offset: val_pages_offset,
                len: val_pages.len() as u64,
                crc32c: crc32c::crc32c(&val_pages),
                comp: compression::NONE,
                uncompressed_len: val_pages.len() as u64,
            });
        }

        // HIST_PAGES: present only when at least one series is
        // histogram-kind. No alignment requirement (section 3.2).
        if !hist_pages.is_empty() {
            let hist_pages_offset = object.len() as u64;
            object.extend_from_slice(&hist_pages);
            sections.push(Section {
                kind: section_kind::HIST_PAGES,
                offset: hist_pages_offset,
                len: hist_pages.len() as u64,
                crc32c: crc32c::crc32c(&hist_pages),
                comp: compression::NONE,
                uncompressed_len: hist_pages.len() as u64,
            });
        }

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
            // v1/v2/v3 never populate the v4 compaction-provenance fields.
            ..Default::default()
        };

        let footer_bytes = footer.encode_to_vec();
        let footer_len =
            u32::try_from(footer_bytes.len()).map_err(|_| WriteError::FooterTooLarge)?;
        object.extend_from_slice(&footer_bytes);

        let crc = footer_crc(
            &footer_bytes,
            footer_len,
            VERSION_V3,
            SIGNAL_METRICS,
            RESERVED,
        );

        object.extend_from_slice(&footer_len.to_le_bytes());
        object.extend_from_slice(&crc.to_le_bytes());
        object.extend_from_slice(&VERSION_V3.to_le_bytes());
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

    /// Encodes RSEG v4: the L0->L1 compaction multi-run format
    /// (ADR-0018, docs/compaction-retention-plan.md section 4). A strict
    /// superset of the real, already-landed v3 (ADR-0017, native
    /// histograms): SERIES_META's per-series columns become run-major (a
    /// series holds one or more runs, each with its own dedup-priority
    /// provenance), and the Footer gains additive compaction-provenance
    /// fields. This writer never decodes or re-encodes a sample: every
    /// run's TS/VAL/HIST page bytes are pre-framed by the caller and
    /// copied verbatim, including v3's HIST_PAGES bytes for histogram
    /// series, which stay an opaque per-run blob to this writer.
    ///
    /// Runs with `sample_count == 0` are dropped; a series left with no
    /// runs afterward is dropped in turn (mirrors the empty-series rule
    /// of `write`/`write_v2`/`write_v3`, generalized to run granularity).
    pub fn write_v4(
        mut series: Vec<SeriesInputV4>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        meta: CompactionMetaV4,
    ) -> Result<WrittenSegment, WriteError> {
        for s in &mut series {
            s.runs.retain(|r| r.sample_count != 0);
        }
        series.retain(|s| !s.runs.is_empty());

        series.sort_unstable_by_key(|s| s.series_id.0);
        if series
            .windows(2)
            .any(|w| w[0].series_id.0 == w[1].series_id.0)
        {
            return Err(WriteError::DuplicateSeriesId);
        }

        for s in &series {
            u16::try_from(s.labels.len()).map_err(|_| WriteError::TooManyLabels)?;

            let mut has_scalar = false;
            let mut has_histogram = false;
            for r in &s.runs {
                match &r.value_page {
                    RunValuePageV4::Scalar(_) => has_scalar = true,
                    RunValuePageV4::Histogram(_) => has_histogram = true,
                }
            }
            if has_scalar && has_histogram {
                return Err(WriteError::MixedValueKindInSeries);
            }
        }

        for s in &mut series {
            s.runs
                .sort_by_key(|r| (r.created_unix_ns, r.writer_epoch, r.writer_seq));
        }

        let series_count = u64::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;

        let mut run_total: u32 = 0;
        let mut sample_count: u64 = 0;
        let mut min_event_ts_ns = i64::MAX;
        let mut max_event_ts_ns = i64::MIN;
        let mut base_created_unix_ns = i64::MAX;
        for s in &series {
            let series_run_count =
                u32::try_from(s.runs.len()).map_err(|_| WriteError::TooManyRuns)?;
            run_total = run_total
                .checked_add(series_run_count)
                .ok_or(WriteError::TooManyRuns)?;
            for r in &s.runs {
                sample_count = sample_count
                    .checked_add(u64::from(r.sample_count))
                    .ok_or(WriteError::TooManySamples)?;
                min_event_ts_ns = min_event_ts_ns.min(r.min_ts_ns);
                max_event_ts_ns = max_event_ts_ns.max(r.max_ts_ns);
                base_created_unix_ns = base_created_unix_ns.min(r.created_unix_ns);
            }
        }
        if series.is_empty() {
            min_event_ts_ns = 0;
            max_event_ts_ns = 0;
            base_created_unix_ns = 0;
        }

        let dict = build_dictionary_v4(&series)?;

        let total_samples =
            usize::try_from(sample_count).map_err(|_| WriteError::TooManySamples)?;
        let run_total_usize = run_total as usize;
        let mut ts_pages = Vec::with_capacity(run_total_usize * 16 + total_samples * 4);
        let mut val_pages = Vec::with_capacity(run_total_usize * 16 + total_samples * 9);
        let mut hist_pages = Vec::with_capacity(run_total_usize * 16 + total_samples * 32);

        let series_meta_raw = build_series_meta_v4(
            &series,
            &dict.occurrence_ordinals,
            min_event_ts_ns,
            base_created_unix_ns,
            run_total,
            &mut ts_pages,
            &mut val_pages,
            &mut hist_pages,
        )?;
        let series_ids_raw = encode_series_ids_v4(&series)?;
        let label_dict_raw = encode_label_dict_v4(&dict)?;

        let label_dict_compressed = zstd_compress_v4(&label_dict_raw)?;
        let series_meta_compressed = zstd_compress_v4(&series_meta_raw)?;

        let mut object = Vec::with_capacity(
            label_dict_compressed.len()
                + series_ids_raw.len()
                + series_meta_compressed.len()
                + ts_pages.len()
                + val_pages.len()
                + hist_pages.len()
                + 512,
        );

        // Physical section order 1, 5, 6, 3, 4, 7 (LABEL_DICT, SERIES_IDS,
        // SERIES_META, TS_PAGES, VAL_PAGES, HIST_PAGES), unchanged from v3
        // (section 4: "no new section kind").
        let label_dict_offset = object.len() as u64;
        object.extend_from_slice(&label_dict_compressed);

        let series_ids_offset = object.len() as u64;
        object.extend_from_slice(&series_ids_raw);

        let series_meta_offset = object.len() as u64;
        object.extend_from_slice(&series_meta_compressed);

        let ts_pages_offset = object.len() as u64;
        object.extend_from_slice(&ts_pages);

        let mut sections = vec![
            Section {
                kind: section_kind::LABEL_DICT,
                offset: label_dict_offset,
                len: label_dict_compressed.len() as u64,
                crc32c: crc32c::crc32c(&label_dict_compressed),
                comp: compression::ZSTD,
                uncompressed_len: label_dict_raw.len() as u64,
            },
            Section {
                kind: section_kind::SERIES_IDS,
                offset: series_ids_offset,
                len: series_ids_raw.len() as u64,
                crc32c: crc32c::crc32c(&series_ids_raw),
                comp: compression::NONE,
                uncompressed_len: series_ids_raw.len() as u64,
            },
            Section {
                kind: section_kind::SERIES_META,
                offset: series_meta_offset,
                len: series_meta_compressed.len() as u64,
                crc32c: crc32c::crc32c(&series_meta_compressed),
                comp: compression::ZSTD,
                uncompressed_len: series_meta_raw.len() as u64,
            },
            Section {
                kind: section_kind::TS_PAGES,
                offset: ts_pages_offset,
                len: ts_pages.len() as u64,
                crc32c: crc32c::crc32c(&ts_pages),
                comp: compression::NONE,
                uncompressed_len: ts_pages.len() as u64,
            },
        ];

        // VAL_PAGES: present only when at least one series is scalar-kind.
        if !val_pages.is_empty() {
            // 8-byte-align the VAL_PAGES section offset, unchanged from
            // v2/v3.
            let val_pad = (8 - (object.len() % 8)) % 8;
            object.extend(std::iter::repeat_n(0u8, val_pad));
            let val_pages_offset = object.len() as u64;
            debug_assert_eq!(
                val_pages_offset % 8,
                0,
                "VAL_PAGES section must be 8-byte aligned"
            );
            object.extend_from_slice(&val_pages);
            sections.push(Section {
                kind: section_kind::VAL_PAGES,
                offset: val_pages_offset,
                len: val_pages.len() as u64,
                crc32c: crc32c::crc32c(&val_pages),
                comp: compression::NONE,
                uncompressed_len: val_pages.len() as u64,
            });
        }

        // HIST_PAGES: present only when at least one series is
        // histogram-kind. No alignment requirement, unchanged from v3.
        if !hist_pages.is_empty() {
            let hist_pages_offset = object.len() as u64;
            object.extend_from_slice(&hist_pages);
            sections.push(Section {
                kind: section_kind::HIST_PAGES,
                offset: hist_pages_offset,
                len: hist_pages.len() as u64,
                crc32c: crc32c::crc32c(&hist_pages),
                comp: compression::NONE,
                uncompressed_len: hist_pages.len() as u64,
            });
        }

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
            base_created_unix_ns,
            ingest_hour_bucket: meta.ingest_hour_bucket,
            input_set_hash: meta.input_set_hash.to_vec(),
            part_index: meta.part_index,
            level: meta.level,
        };

        let footer_bytes = footer.encode_to_vec();
        let footer_len =
            u32::try_from(footer_bytes.len()).map_err(|_| WriteError::FooterTooLarge)?;
        object.extend_from_slice(&footer_bytes);

        let crc = footer_crc(
            &footer_bytes,
            footer_len,
            VERSION_V4,
            SIGNAL_METRICS,
            RESERVED,
        );

        object.extend_from_slice(&footer_len.to_le_bytes());
        object.extend_from_slice(&crc.to_le_bytes());
        object.extend_from_slice(&VERSION_V4.to_le_bytes());
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

/// FNV-1a. Label names and values are short strings from a single,
/// admission-limited write batch, hashed into a map that lives only for
/// the duration of one `write` call, so a fast non-keyed hash is safe
/// here; SipHash cost dominated dictionary construction at high series
/// counts.
#[derive(Default)]
struct FnvHasher(u64);

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.0 == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.0
        };
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        self.0 = hash;
    }
}

/// Capacity hint for the schema-keyed name memo (issue #95). The distinct
/// schema count is not known up front; it is bounded by the series count but
/// is usually far smaller (a handful of shared label-name lists). Cap the
/// reservation so the common few-schema case does not over-allocate while
/// still avoiding rehash growth for realistic schema counts; beyond the cap
/// the map grows on its own, which is cheap relative to the interner pass.
fn schema_memo_capacity(series: &[SeriesInput]) -> usize {
    series.len().min(1024)
}

/// Interns one string into the v1 dictionary interner, returning its
/// distinct index (the pre-rank ordinal). New strings are appended to
/// `distinct` in first-occurrence order; the final sorted ranks are assigned
/// later. Extracted so the schema-memo fast path and slow path share one
/// probe implementation.
#[inline]
fn intern_distinct<'a>(
    interner: &mut HashMap<&'a str, u32, BuildHasherDefault<FnvHasher>>,
    distinct: &mut Vec<&'a str>,
    text: &'a str,
) -> Result<u32, WriteError> {
    match interner.entry(text) {
        std::collections::hash_map::Entry::Occupied(e) => Ok(*e.get()),
        std::collections::hash_map::Entry::Vacant(e) => {
            let id = u32::try_from(distinct.len()).map_err(|_| WriteError::TooManyDictStrings)?;
            distinct.push(text);
            e.insert(id);
            Ok(id)
        }
    }
}

/// Dictionary build result: the ordinal-ordered strings and the
/// pre-resolved ordinal of every label-string occurrence in series
/// iteration order (name, then value, per label).
struct Dictionary<'a> {
    /// Distinct non-`__name__` strings in ordinal order (ordinals 1..).
    sorted_non_name: Vec<&'a str>,
    /// On-disk ordinal for every occurrence, aligned with iterating
    /// `series` and each series' labels in order.
    occurrence_ordinals: Vec<u32>,
    /// Number of dictionary entries including ordinal 0 (`__name__`).
    count: u32,
}

/// Ordinal 0 is always `"__name__"` (docs/segment-format.md); the rest of
/// the distinct label names/values used across the batch follow in sorted
/// order (arbitrary but deterministic -- the reader locates strings purely
/// by ordinal).
///
/// Single interner pass resolves every occurrence's ordinal up front, so
/// series-table encoding never has to hash strings again. Only the
/// distinct strings are sorted (with a big-endian 8-byte prefix key to
/// keep most comparisons integer-sized), instead of feeding every
/// occurrence through a BTreeSet.
///
/// Schema-keyed name interning (issue #95): a series' ordered label-name
/// list is memoized to the distinct-index list it resolved to. A series
/// whose name list was already seen does one memo lookup instead of one
/// interner probe per name. Because `write` sorts the batch by series id,
/// equal schemas are not adjacent, so the memo is a map keyed by the
/// name-list content, not a last-seen check. v1 output is order-independent
/// (the distinct set is sorted below regardless of insertion order), so the
/// memo cannot change the emitted bytes; it only removes redundant probes.
fn build_dictionary(series: &[SeriesInput]) -> Result<Dictionary<'_>, WriteError> {
    let total_strings: usize = series.iter().map(|s| s.labels.len() * 2).sum();
    // `interner` is pre-sized to the occurrence upper bound (distinct count
    // <= occurrences) and `occurrence_ordinals` to the exact occurrence count.
    // `distinct` is left to grow: its final length is the distinct-string
    // count, well below `total_strings`, so reserving the upper bound commits
    // far more pages than it fills and costs more than the doubling reallocs
    // it would avoid (measured on the bench host, issue #95). `schema_memo` is
    // pre-sized to a bounded estimate of the distinct schema count.
    let mut interner: HashMap<&str, u32, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(total_strings, BuildHasherDefault::default());
    let mut distinct: Vec<&str> = Vec::new();
    let mut occurrence_ordinals: Vec<u32> = Vec::with_capacity(total_strings);
    let mut schema_memo: HashMap<Vec<&str>, Vec<u32>, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(
            schema_memo_capacity(series),
            BuildHasherDefault::default(),
        );
    let mut name_key: Vec<&str> = Vec::new();

    for s in series {
        name_key.clear();
        name_key.extend(s.labels.iter().map(|l| l.name.as_str()));

        if let Some(name_ords) = schema_memo.get(name_key.as_slice()) {
            // Fast path: every name in this schema is already interned (the
            // schema appeared on an earlier series), so only the values need
            // probing. Emitting the memoized name indices while interning the
            // values in the same interleaved order leaves the distinct set,
            // and therefore the sorted output, byte-identical to the slow
            // path below.
            for (label, &name_id) in s.labels.iter().zip(name_ords.iter()) {
                occurrence_ordinals.push(name_id);
                let value_id = intern_distinct(&mut interner, &mut distinct, label.value.as_str())?;
                occurrence_ordinals.push(value_id);
            }
        } else {
            // Slow path: first sighting of this schema. Intern name and value
            // interleaved exactly as the original single-pass loop did, and
            // record the resolved name indices for later repeats.
            let mut name_ords: Vec<u32> = Vec::with_capacity(s.labels.len());
            for label in s.labels.iter() {
                let name_id = intern_distinct(&mut interner, &mut distinct, label.name.as_str())?;
                let value_id = intern_distinct(&mut interner, &mut distinct, label.value.as_str())?;
                occurrence_ordinals.push(name_id);
                occurrence_ordinals.push(value_id);
                name_ords.push(name_id);
            }
            schema_memo.insert(name_key.clone(), name_ords);
        }
    }

    let mut order: Vec<(u64, u32)> = distinct
        .iter()
        .enumerate()
        .map(|(i, s)| (prefix_key(s), i as u32))
        .collect();
    order.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| distinct[a.1 as usize].cmp(distinct[b.1 as usize]))
    });

    let mut rank = vec![0u32; distinct.len()];
    let mut next: u32 = 1;
    for &(_, id) in &order {
        if distinct[id as usize] == METRIC_NAME_LABEL {
            rank[id as usize] = 0;
        } else {
            rank[id as usize] = next;
            next = next.checked_add(1).ok_or(WriteError::TooManyDictStrings)?;
        }
    }
    // `next` is now the dictionary size including the implicit ordinal 0.
    let count = next;

    let occurrence_ordinals = occurrence_ordinals
        .into_iter()
        .map(|id| rank[id as usize])
        .collect();

    let mut sorted_non_name: Vec<&str> = Vec::with_capacity(order.len());
    for &(_, id) in &order {
        let text = distinct[id as usize];
        if text != METRIC_NAME_LABEL {
            sorted_non_name.push(text);
        }
    }

    Ok(Dictionary {
        sorted_non_name,
        occurrence_ordinals,
        count,
    })
}

fn prefix_key(s: &str) -> u64 {
    let bytes = s.as_bytes();
    let mut key = [0u8; 8];
    let n = bytes.len().min(8);
    key[..n].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(key)
}

fn encode_label_dict(dict: &Dictionary<'_>) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&dict.count.to_le_bytes());
    write_uvarint(&mut buf, METRIC_NAME_LABEL.len() as u64);
    buf.extend_from_slice(METRIC_NAME_LABEL.as_bytes());
    for s in &dict.sorted_non_name {
        let bytes = s.as_bytes();
        write_uvarint(&mut buf, bytes.len() as u64);
        buf.extend_from_slice(bytes);
    }
    Ok(buf)
}

fn encode_series_table(
    series: &[SeriesInput],
    occurrence_ordinals: &[u32],
    ts_pages: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::with_capacity(4 + series.len() * 46 + occurrence_ordinals.len() * 3);
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;
    buf.extend_from_slice(&count.to_le_bytes());

    // Scratch buffers reused across every series: page building previously
    // allocated four fresh Vecs per series, which dominated tiny-page
    // segments at high cardinality.
    let mut ts_scratch: Vec<i64> = Vec::new();
    let mut val_scratch: Vec<f64> = Vec::new();
    let mut payload_scratch: Vec<u8> = Vec::new();

    let mut next_occurrence = occurrence_ordinals.iter();
    for s in series {
        buf.extend_from_slice(&s.series_id.0);

        let label_count = u16::try_from(s.labels.len()).map_err(|_| WriteError::TooManyLabels)?;
        buf.extend_from_slice(&label_count.to_le_bytes());
        for _ in 0..s.labels.len() {
            let name_ord = *next_occurrence
                .next()
                .ok_or(WriteError::DictionaryInvariant)?;
            let value_ord = *next_occurrence
                .next()
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
        append_ts_page(
            &s.series_id,
            &s.samples,
            &mut ts_scratch,
            &mut payload_scratch,
            ts_pages,
        )?;
        let ts_page_len = ts_pages.len() as u64 - ts_page_offset;
        write_uvarint(&mut buf, ts_page_offset);
        write_uvarint(&mut buf, ts_page_len);

        let val_page_offset = val_pages.len() as u64;
        append_val_page(
            &s.series_id,
            &s.samples,
            &mut val_scratch,
            &mut payload_scratch,
            val_pages,
        );
        let val_page_len = val_pages.len() as u64 - val_page_offset;
        write_uvarint(&mut buf, val_page_offset);
        write_uvarint(&mut buf, val_page_len);
    }
    Ok(buf)
}

/// Writer policy, not format: timestamp payloads below this size skip the
/// lz4 attempt and are stored with `comp = 0`, which the page grammar
/// explicitly permits. lz4's per-call setup dominates such pages, and the
/// 4-byte size prefix plus framing means compression cannot win there.
const LZ4_MIN_TS_PAYLOAD_BYTES: usize = 64;

/// Encodes one series' TS page directly into `ts_pages` (6-byte header
/// plus payload), reusing the caller's scratch buffers. lz4 is applied
/// only when the raw payload reaches the size floor and compression
/// actually shrinks it; otherwise the page is stored uncompressed.
fn append_ts_page(
    series_id: &SeriesId,
    samples: &[ravel_types::Sample],
    ts_scratch: &mut Vec<i64>,
    payload_scratch: &mut Vec<u8>,
    ts_pages: &mut Vec<u8>,
) -> Result<(), WriteError> {
    ts_scratch.clear();
    ts_scratch.extend(samples.iter().map(|s| s.ts_ns));
    payload_scratch.clear();
    encode_ts_deltas_into(payload_scratch, ts_scratch).ok_or(WriteError::TimestampDeltaOverflow)?;

    let enc = page_enc::TS_DELTA_VARINT;
    let compressed = if payload_scratch.len() >= LZ4_MIN_TS_PAYLOAD_BYTES {
        let candidate = lz4_flex::compress_prepend_size(payload_scratch);
        (candidate.len() < payload_scratch.len()).then_some(candidate)
    } else {
        None
    };
    let (comp, payload): (u8, &[u8]) = match &compressed {
        Some(candidate) => (page_comp::LZ4, candidate),
        None => (page_comp::NONE, payload_scratch),
    };
    let crc = page_crc(&series_id.0, enc, comp, payload);
    ts_pages.push(enc);
    ts_pages.push(comp);
    ts_pages.extend_from_slice(&crc.to_le_bytes());
    ts_pages.extend_from_slice(payload);
    Ok(())
}

/// Encodes one series' VAL page directly into `val_pages`, reusing the
/// caller's scratch buffers. Encoding choice is unchanged: Gorilla unless
/// it fails to beat raw f64 (docs/segment-format.md raw-fallback rule).
fn append_val_page(
    series_id: &SeriesId,
    samples: &[ravel_types::Sample],
    val_scratch: &mut Vec<f64>,
    payload_scratch: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
) {
    val_scratch.clear();
    val_scratch.extend(samples.iter().map(|s| s.value));
    payload_scratch.clear();
    encode_gorilla_into(val_scratch, payload_scratch);

    let count = val_scratch.len() as u64;
    let enc = if (payload_scratch.len() as u64) >= 8 * count {
        payload_scratch.clear();
        for v in val_scratch.iter() {
            payload_scratch.extend_from_slice(&v.to_le_bytes());
        }
        page_enc::VAL_RAW_F64
    } else {
        page_enc::VAL_GORILLA
    };
    let comp = page_comp::NONE;
    let crc = page_crc(&series_id.0, enc, comp, payload_scratch);
    val_pages.push(enc);
    val_pages.push(comp);
    val_pages.extend_from_slice(&crc.to_le_bytes());
    val_pages.extend_from_slice(payload_scratch);
}

fn zstd_compress(data: &[u8]) -> Result<Vec<u8>, WriteError> {
    zstd::bulk::compress(data, ZSTD_LEVEL).map_err(|e| WriteError::Zstd(e.to_string()))
}

// --- RSEG v2 encode path (ADR-0014, docs/segment-format.md "RSEG v2
// amendment"). Parallel to the v1 functions above; none of the v1 functions
// are called from here and none of them are edited, so the v1 code path
// above stays trivially provable as byte-identical by inspection. ---

/// v2 dictionary build result: strings in first-occurrence (interner)
/// order -- no distinct-string sort, unlike v1's `Dictionary` -- plus the
/// same per-occurrence ordinal resolution and dictionary size.
struct DictionaryV2<'a> {
    /// Distinct non-`__name__` strings in first-occurrence order
    /// (ordinals 1..).
    order: Vec<&'a str>,
    /// On-disk ordinal for every occurrence, aligned with iterating
    /// `series` and each series' labels in order (same shape as v1's
    /// `Dictionary::occurrence_ordinals`).
    occurrence_ordinals: Vec<u32>,
    /// Number of dictionary entries including ordinal 0 (`__name__`).
    count: u32,
}

/// Ordinal 0 is always `"__name__"` (explicit special case, independent of
/// interning order, exactly like v1's `build_dictionary`). Every other
/// distinct string gets the next ordinal the first time it is seen, in
/// series-then-label iteration order: no sort, unlike v1. This is the
/// "no distinct-string sort" rule from the v2 LABEL_DICT ordering
/// amendment; it is also why `__name__` must be pre-seeded into the
/// interner before the loop starts, rather than special-cased afterward
/// as v1 does with its sort-then-rank pass.
fn build_dictionary_v2(series: &[SeriesInput]) -> Result<DictionaryV2<'_>, WriteError> {
    let total_strings: usize = series.iter().map(|s| s.labels.len() * 2).sum();
    // Pre-size the interner to the occurrence upper bound (`+1` for the
    // preseeded __name__) and the schema memo to a bounded distinct-schema
    // estimate, to avoid rehash growth (issue #95). `order` is left to grow:
    // it holds one entry per distinct string, well below `total_strings`, so
    // reserving the upper bound commits far more pages than it fills and costs
    // more than the doubling reallocs it would avoid (measured on the bench
    // host).
    let mut interner: HashMap<&str, u32, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(total_strings + 1, BuildHasherDefault::default());
    interner.insert(METRIC_NAME_LABEL, 0);

    // Schema-keyed name interning (issue #95): a series' ordered label-name
    // list is memoized to the ordinals it resolved to, so a repeat schema
    // does one map lookup instead of one interner probe per name. `write_v2`
    // sorts the batch by series id, so equal schemas are not adjacent; the
    // memo is a content-keyed map, never a last-seen check.
    //
    // First-occurrence ordinal order is the v2 LABEL_DICT order and MUST stay
    // byte-identical. The fast path is only taken when the schema was seen
    // before, which means every one of its names is already interned, so it
    // assigns no new name ordinals; it interns the values in the same
    // interleaved order the slow path uses. The sequence of newly assigned
    // ordinals is therefore identical to the original single-pass loop.
    let mut schema_memo: HashMap<Vec<&str>, Vec<u32>, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(
            schema_memo_capacity(series),
            BuildHasherDefault::default(),
        );
    let mut name_key: Vec<&str> = Vec::new();

    let mut order: Vec<&str> = Vec::new();
    let mut occurrence_ordinals: Vec<u32> = Vec::with_capacity(total_strings);
    let mut next_ordinal: u32 = 1;

    for s in series {
        name_key.clear();
        name_key.extend(s.labels.iter().map(|l| l.name.as_str()));

        if let Some(name_ords) = schema_memo.get(name_key.as_slice()) {
            for (label, &name_ord) in s.labels.iter().zip(name_ords.iter()) {
                occurrence_ordinals.push(name_ord);
                let value_id = intern_v2(
                    &mut interner,
                    &mut order,
                    &mut next_ordinal,
                    label.value.as_str(),
                )?;
                occurrence_ordinals.push(value_id);
            }
        } else {
            let mut name_ords: Vec<u32> = Vec::with_capacity(s.labels.len());
            for label in s.labels.iter() {
                let name_id = intern_v2(
                    &mut interner,
                    &mut order,
                    &mut next_ordinal,
                    label.name.as_str(),
                )?;
                let value_id = intern_v2(
                    &mut interner,
                    &mut order,
                    &mut next_ordinal,
                    label.value.as_str(),
                )?;
                occurrence_ordinals.push(name_id);
                occurrence_ordinals.push(value_id);
                name_ords.push(name_id);
            }
            schema_memo.insert(name_key.clone(), name_ords);
        }
    }

    Ok(DictionaryV2 {
        order,
        occurrence_ordinals,
        count: next_ordinal,
    })
}

/// Interns one string into the v2 dictionary interner, returning its on-disk
/// ordinal. New strings are appended to `order` (first-occurrence order,
/// which is the v2 LABEL_DICT order) and assigned the next ordinal.
/// `__name__` is preseeded to ordinal 0 by the caller, so it is never
/// appended here. Extracted so the schema-memo fast and slow paths share one
/// probe implementation.
#[inline]
fn intern_v2<'a>(
    interner: &mut HashMap<&'a str, u32, BuildHasherDefault<FnvHasher>>,
    order: &mut Vec<&'a str>,
    next_ordinal: &mut u32,
    text: &'a str,
) -> Result<u32, WriteError> {
    match interner.entry(text) {
        std::collections::hash_map::Entry::Occupied(e) => Ok(*e.get()),
        std::collections::hash_map::Entry::Vacant(e) => {
            let id = *next_ordinal;
            *next_ordinal = next_ordinal
                .checked_add(1)
                .ok_or(WriteError::TooManyDictStrings)?;
            order.push(text);
            e.insert(id);
            Ok(id)
        }
    }
}

/// Grammar identical to v1's `encode_label_dict` (count:u32, then
/// len:varint + UTF-8 bytes per string, `__name__` first); the only
/// difference is that the remaining strings come from `dict.order`
/// (first-occurrence order) instead of a sorted list.
fn encode_label_dict_v2(dict: &DictionaryV2<'_>) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&dict.count.to_le_bytes());
    write_uvarint(&mut buf, METRIC_NAME_LABEL.len() as u64);
    buf.extend_from_slice(METRIC_NAME_LABEL.as_bytes());
    for s in &dict.order {
        let bytes = s.as_bytes();
        write_uvarint(&mut buf, bytes.len() as u64);
        buf.extend_from_slice(bytes);
    }
    Ok(buf)
}

/// SERIES_IDS (kind 5): `count: u32` then `count` series ids, strictly
/// ascending by byte comparison. `series` is already sorted by
/// `series_id.0` bytes (`write_v2`'s preamble), so this is a direct
/// concatenation. Never zstd-compressed by the writer (BLAKE3 ids are
/// incompressible).
fn encode_series_ids_v2(series: &[SeriesInput]) -> Result<Vec<u8>, WriteError> {
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;
    let mut buf = Vec::with_capacity(4 + series.len() * 16);
    buf.extend_from_slice(&count.to_le_bytes());
    for s in series {
        buf.extend_from_slice(&s.series_id.0);
    }
    Ok(buf)
}

/// Encodes one series' TS page directly into `ts_pages`. Identical
/// behavior to v1's `append_ts_page` (TS_PAGES is unchanged in v2); kept
/// as a separate function per the format-change discipline for this
/// ticket rather than calling v1's version, so the v1 path never depends
/// on anything added for v2.
fn append_ts_page_v2(
    series_id: &SeriesId,
    samples: &[ravel_types::Sample],
    ts_scratch: &mut Vec<i64>,
    payload_scratch: &mut Vec<u8>,
    ts_pages: &mut Vec<u8>,
) -> Result<(), WriteError> {
    ts_scratch.clear();
    ts_scratch.extend(samples.iter().map(|s| s.ts_ns));
    payload_scratch.clear();
    encode_ts_deltas_into(payload_scratch, ts_scratch).ok_or(WriteError::TimestampDeltaOverflow)?;

    let enc = page_enc::TS_DELTA_VARINT;
    let compressed = if payload_scratch.len() >= LZ4_MIN_TS_PAYLOAD_BYTES {
        let candidate = lz4_flex::compress_prepend_size(payload_scratch);
        (candidate.len() < payload_scratch.len()).then_some(candidate)
    } else {
        None
    };
    let (comp, payload): (u8, &[u8]) = match &compressed {
        Some(candidate) => (page_comp::LZ4, candidate),
        None => (page_comp::NONE, payload_scratch),
    };
    let crc = page_crc(&series_id.0, enc, comp, payload);
    ts_pages.push(enc);
    ts_pages.push(comp);
    ts_pages.extend_from_slice(&crc.to_le_bytes());
    ts_pages.extend_from_slice(payload);
    Ok(())
}

/// Encodes one series' VAL page directly into `val_pages`, same encoding
/// choice as v1's `append_val_page` (Gorilla unless it fails to beat raw
/// f64), plus the v2 alignment rule: a VAL_RAW_F64 page's payload start
/// (page offset + 6, the page header size) must land 0 mod 8 relative to
/// the section start. Since the caller 8-byte-aligns the VAL_PAGES section
/// offset itself, aligning relative to `val_pages`'s current length is
/// equivalent to aligning relative to the object start. Returns the pad
/// length inserted before this page's header (0 unless this page is
/// VAL_RAW_F64 and needed padding), recorded by the caller in that
/// series' `val_page_gap` column.
fn append_val_page_v2(
    series_id: &SeriesId,
    samples: &[ravel_types::Sample],
    val_scratch: &mut Vec<f64>,
    payload_scratch: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
) -> u64 {
    val_scratch.clear();
    val_scratch.extend(samples.iter().map(|s| s.value));
    payload_scratch.clear();
    encode_gorilla_into(val_scratch, payload_scratch);

    let count = val_scratch.len() as u64;
    let enc = if (payload_scratch.len() as u64) >= 8 * count {
        payload_scratch.clear();
        for v in val_scratch.iter() {
            payload_scratch.extend_from_slice(&v.to_le_bytes());
        }
        page_enc::VAL_RAW_F64
    } else {
        page_enc::VAL_GORILLA
    };

    let mut gap = 0u64;
    if enc == page_enc::VAL_RAW_F64 {
        const PAGE_HEADER_LEN: u64 = 6; // enc(1) + comp(1) + crc32c(4)
        let unaligned_payload_start = val_pages.len() as u64 + PAGE_HEADER_LEN;
        let rem = unaligned_payload_start % 8;
        if rem != 0 {
            gap = 8 - rem;
            val_pages.extend(std::iter::repeat_n(0u8, gap as usize));
        }
    }

    let comp = page_comp::NONE;
    let crc = page_crc(&series_id.0, enc, comp, payload_scratch);
    val_pages.push(enc);
    val_pages.push(comp);
    val_pages.extend_from_slice(&crc.to_le_bytes());
    val_pages.extend_from_slice(payload_scratch);
    gap
}

/// SERIES_META (kind 6, uncompressed form): `count: u32`, `schema_count:
/// u32`, then the schema dictionary (each schema is a dedup key on the
/// sequence of LABEL_DICT name ordinals a series uses), then 9 columnar
/// blocks in the fixed order the format specifies, each prefixed by its
/// own `block_len: varint`.
///
/// Landmine (docs/segment-format.md v2 amendment, "Writer note"): a
/// schema's name_ord sequence is read directly off `s.labels.iter()`,
/// which `LabelSet::new` already sorted by name bytes before this
/// function ever sees it (crates/ravel-types). It is never re-derived by
/// sorting ordinal values -- v2's relaxed LABEL_DICT order means ordinal
/// order and name-byte order no longer coincide, and sorting by ordinal
/// would silently permute label pairs and corrupt canonical series
/// identity (ADR-0005).
fn build_series_meta_v2(
    series: &[SeriesInput],
    occurrence_ordinals: &[u32],
    footer_min_event_ts_ns: i64,
    ts_pages: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
) -> Result<Vec<u8>, WriteError> {
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;

    // Pre-size the schema dedup structures and every column buffer from input
    // statistics (series count, one value ordinal per label) so the per-series
    // loop below never triggers a rehash or a buffer realloc (issue #95).
    // Per-series columns hold one varint per series (most are a single byte);
    // the value-ordinal column holds one varint per label pair.
    let series_len = series.len();
    let value_ord_count = occurrence_ordinals.len() / 2;
    let mut schema_index: HashMap<Vec<u32>, u32> =
        HashMap::with_capacity(schema_memo_capacity(series));
    let mut schemas: Vec<Vec<u32>> = Vec::with_capacity(schema_memo_capacity(series));

    let mut col_schema_ref: Vec<u8> = Vec::with_capacity(series_len);
    let mut col_value_ord: Vec<u8> = Vec::with_capacity(value_ord_count * 2);
    let mut col_sample_count: Vec<u8> = Vec::with_capacity(series_len);
    let mut col_min_ts_delta: Vec<u8> = Vec::with_capacity(series_len * 2);
    let mut col_ts_span: Vec<u8> = Vec::with_capacity(series_len * 2);
    let mut col_ts_page_gap: Vec<u8> = Vec::with_capacity(series_len);
    let mut col_ts_page_len: Vec<u8> = Vec::with_capacity(series_len);
    let mut col_val_page_gap: Vec<u8> = Vec::with_capacity(series_len);
    let mut col_val_page_len: Vec<u8> = Vec::with_capacity(series_len);

    let mut ts_scratch: Vec<i64> = Vec::new();
    let mut val_scratch: Vec<f64> = Vec::new();
    let mut payload_scratch: Vec<u8> = Vec::new();

    let mut next_occurrence = occurrence_ordinals.iter();

    for s in series {
        let label_count = s.labels.len();
        let mut name_ords: Vec<u32> = Vec::with_capacity(label_count);
        let mut value_ords: Vec<u32> = Vec::with_capacity(label_count);
        let mut prev_name: Option<&str> = None;
        for label in s.labels.iter() {
            // The whole schema layer rests on `s.labels` already being
            // sorted by name bytes (crates/ravel-types `LabelSet::new`);
            // assert it locally rather than trust the invariant silently.
            debug_assert!(
                prev_name.is_none_or(|prev| prev <= label.name.as_str()),
                "LabelSet invariant violated: labels not sorted by name bytes"
            );
            prev_name = Some(label.name.as_str());

            let name_ord = *next_occurrence
                .next()
                .ok_or(WriteError::DictionaryInvariant)?;
            let value_ord = *next_occurrence
                .next()
                .ok_or(WriteError::DictionaryInvariant)?;
            name_ords.push(name_ord);
            value_ords.push(value_ord);
        }

        let schema_ref = match schema_index.get(&name_ords) {
            Some(&idx) => idx,
            None => {
                let idx = u32::try_from(schemas.len()).map_err(|_| WriteError::TooManySchemas)?;
                schema_index.insert(name_ords.clone(), idx);
                schemas.push(name_ords);
                idx
            }
        };
        write_uvarint(&mut col_schema_ref, u64::from(schema_ref));
        for value_ord in &value_ords {
            write_uvarint(&mut col_value_ord, u64::from(*value_ord));
        }

        let sample_count =
            u32::try_from(s.samples.len()).map_err(|_| WriteError::TooManySamples)?;
        write_uvarint(&mut col_sample_count, u64::from(sample_count));

        // Non-empty by construction (empty series are dropped in
        // `write_v2`'s preamble before this function is called).
        let min_ts_ns = s.samples.first().map_or(0, |sm| sm.ts_ns);
        let max_ts_ns = s.samples.last().map_or(0, |sm| sm.ts_ns);
        // i128 intermediates: both deltas are non-negative by construction
        // (footer_min_event_ts_ns is the batch-wide minimum; samples are
        // sorted so max_ts_ns >= min_ts_ns), and the i64-range difference
        // always fits u64, so this never truncates or panics.
        let min_ts_delta = (i128::from(min_ts_ns) - i128::from(footer_min_event_ts_ns)) as u64;
        let ts_span = (i128::from(max_ts_ns) - i128::from(min_ts_ns)) as u64;
        write_uvarint(&mut col_min_ts_delta, min_ts_delta);
        write_uvarint(&mut col_ts_span, ts_span);

        let ts_page_offset_before = ts_pages.len() as u64;
        append_ts_page_v2(
            &s.series_id,
            &s.samples,
            &mut ts_scratch,
            &mut payload_scratch,
            ts_pages,
        )?;
        let ts_page_len = ts_pages.len() as u64 - ts_page_offset_before;
        // TS pages are never aligned (docs/segment-format.md v2
        // amendment); the gap column stays 0 but is still emitted so the
        // grammar generalizes to future placement changes.
        write_uvarint(&mut col_ts_page_gap, 0);
        write_uvarint(&mut col_ts_page_len, ts_page_len);

        let val_offset_before = val_pages.len() as u64;
        let val_gap = append_val_page_v2(
            &s.series_id,
            &s.samples,
            &mut val_scratch,
            &mut payload_scratch,
            val_pages,
        );
        let val_total_added = val_pages.len() as u64 - val_offset_before;
        let val_page_len = val_total_added - val_gap;
        write_uvarint(&mut col_val_page_gap, val_gap);
        write_uvarint(&mut col_val_page_len, val_page_len);
    }

    let schema_count = u32::try_from(schemas.len()).map_err(|_| WriteError::TooManySchemas)?;

    let mut buf = Vec::with_capacity(
        8 + schemas.len() * 4
            + col_schema_ref.len()
            + col_value_ord.len()
            + col_sample_count.len()
            + col_min_ts_delta.len()
            + col_ts_span.len()
            + col_ts_page_gap.len()
            + col_ts_page_len.len()
            + col_val_page_gap.len()
            + col_val_page_len.len()
            + 64,
    );
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&schema_count.to_le_bytes());
    for schema in &schemas {
        write_uvarint(&mut buf, schema.len() as u64);
        for name_ord in schema {
            write_uvarint(&mut buf, u64::from(*name_ord));
        }
    }
    for col in [
        &col_schema_ref,
        &col_value_ord,
        &col_sample_count,
        &col_min_ts_delta,
        &col_ts_span,
        &col_ts_page_gap,
        &col_ts_page_len,
        &col_val_page_gap,
        &col_val_page_len,
    ] {
        write_uvarint(&mut buf, col.len() as u64);
        buf.extend_from_slice(col);
    }
    Ok(buf)
}

/// Same operation as v1's `zstd_compress` (LABEL_DICT and SERIES_META are
/// both whole-section zstd in v2, same as LABEL_DICT/SERIES_TABLE in v1);
/// kept separate per this ticket's constraint that v1 functions are never
/// called from the v2 path.
fn zstd_compress_v2(data: &[u8]) -> Result<Vec<u8>, WriteError> {
    zstd::bulk::compress(data, ZSTD_LEVEL).map_err(|e| WriteError::Zstd(e.to_string()))
}

// --- RSEG v3 encode path (ADR-0017, docs/rseg-v3-plan.md, docs/segment-
// format.md "RSEG v3 amendment"). Parallel to the v1/v2 functions above;
// none of the v1/v2 functions are called from here and none of them are
// edited, so the v1 and v2 paths above stay trivially provable as byte-
// identical by inspection. LABEL_DICT/SERIES_IDS are unchanged from v2
// (section 3.3), but still get their own copies here per that same
// discipline. ---

/// Identical grammar and first-occurrence-order rule as v2's
/// `DictionaryV2` (section 3.3: "unchanged from v2").
struct DictionaryV3<'a> {
    order: Vec<&'a str>,
    occurrence_ordinals: Vec<u32>,
    count: u32,
}

fn build_dictionary_v3(series: &[SeriesInputV3]) -> Result<DictionaryV3<'_>, WriteError> {
    let total_strings: usize = series.iter().map(|s| s.labels.len() * 2).sum();
    let mut interner: HashMap<&str, u32, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(total_strings + 1, BuildHasherDefault::default());
    interner.insert(METRIC_NAME_LABEL, 0);

    let mut order: Vec<&str> = Vec::new();
    let mut occurrence_ordinals: Vec<u32> = Vec::with_capacity(total_strings);
    let mut next_ordinal: u32 = 1;

    for s in series {
        for label in s.labels.iter() {
            for text in [label.name.as_str(), label.value.as_str()] {
                let id = match interner.entry(text) {
                    std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let id = next_ordinal;
                        next_ordinal = next_ordinal
                            .checked_add(1)
                            .ok_or(WriteError::TooManyDictStrings)?;
                        order.push(text);
                        e.insert(id);
                        id
                    }
                };
                occurrence_ordinals.push(id);
            }
        }
    }

    Ok(DictionaryV3 {
        order,
        occurrence_ordinals,
        count: next_ordinal,
    })
}

/// Grammar identical to v1/v2's label-dict encode (count:u32, then
/// len:varint + UTF-8 bytes per string, `__name__` first).
fn encode_label_dict_v3(dict: &DictionaryV3<'_>) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&dict.count.to_le_bytes());
    write_uvarint(&mut buf, METRIC_NAME_LABEL.len() as u64);
    buf.extend_from_slice(METRIC_NAME_LABEL.as_bytes());
    for s in &dict.order {
        let bytes = s.as_bytes();
        write_uvarint(&mut buf, bytes.len() as u64);
        buf.extend_from_slice(bytes);
    }
    Ok(buf)
}

/// SERIES_IDS (kind 5): identical grammar to v2. `series` is already
/// sorted by `series_id.0` bytes (`write_v3`'s preamble).
fn encode_series_ids_v3(series: &[SeriesInputV3]) -> Result<Vec<u8>, WriteError> {
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;
    let mut buf = Vec::with_capacity(4 + series.len() * 16);
    buf.extend_from_slice(&count.to_le_bytes());
    for s in series {
        buf.extend_from_slice(&s.series_id.0);
    }
    Ok(buf)
}

/// Encodes one series' TS page directly into `ts_pages`. Identical
/// behavior to v2's `append_ts_page_v2` (TS_PAGES is unchanged in v3,
/// shared by scalar and histogram series, section 3.2); takes the already
/// -sorted timestamp values directly since a v3 series' samples may be
/// either `Sample`s or `HistogramSample`s.
fn append_ts_page_v3(
    series_id: &SeriesId,
    ts_values: &[i64],
    payload_scratch: &mut Vec<u8>,
    ts_pages: &mut Vec<u8>,
) -> Result<(), WriteError> {
    payload_scratch.clear();
    encode_ts_deltas_into(payload_scratch, ts_values).ok_or(WriteError::TimestampDeltaOverflow)?;

    let enc = page_enc::TS_DELTA_VARINT;
    let compressed = if payload_scratch.len() >= LZ4_MIN_TS_PAYLOAD_BYTES {
        let candidate = lz4_flex::compress_prepend_size(payload_scratch);
        (candidate.len() < payload_scratch.len()).then_some(candidate)
    } else {
        None
    };
    let (comp, payload): (u8, &[u8]) = match &compressed {
        Some(candidate) => (page_comp::LZ4, candidate),
        None => (page_comp::NONE, payload_scratch),
    };
    let crc = page_crc(&series_id.0, enc, comp, payload);
    ts_pages.push(enc);
    ts_pages.push(comp);
    ts_pages.extend_from_slice(&crc.to_le_bytes());
    ts_pages.extend_from_slice(payload);
    Ok(())
}

/// Encodes one scalar series' VAL page directly into `val_pages`. Same
/// encoding choice and 8-byte alignment rule as v2's `append_val_page_v2`
/// (section 3.2: "VAL_PAGES unchanged from v2; scalar series only").
/// Returns the pad length inserted before this page's header, recorded by
/// the caller in that series' `val_page_gap` column.
fn append_val_page_v3(
    series_id: &SeriesId,
    values: &[f64],
    payload_scratch: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
) -> u64 {
    payload_scratch.clear();
    encode_gorilla_into(values, payload_scratch);

    let count = values.len() as u64;
    let enc = if (payload_scratch.len() as u64) >= 8 * count {
        payload_scratch.clear();
        for v in values {
            payload_scratch.extend_from_slice(&v.to_le_bytes());
        }
        page_enc::VAL_RAW_F64
    } else {
        page_enc::VAL_GORILLA
    };

    let mut gap = 0u64;
    if enc == page_enc::VAL_RAW_F64 {
        const PAGE_HEADER_LEN: u64 = 6; // enc(1) + comp(1) + crc32c(4)
        let unaligned_payload_start = val_pages.len() as u64 + PAGE_HEADER_LEN;
        let rem = unaligned_payload_start % 8;
        if rem != 0 {
            gap = 8 - rem;
            val_pages.extend(std::iter::repeat_n(0u8, gap as usize));
        }
    }

    let comp = page_comp::NONE;
    let crc = page_crc(&series_id.0, enc, comp, payload_scratch);
    val_pages.push(enc);
    val_pages.push(comp);
    val_pages.extend_from_slice(&crc.to_le_bytes());
    val_pages.extend_from_slice(payload_scratch);
    gap
}

/// Encodes one histogram series' HIST page directly into `hist_pages`:
/// the page container framing is identical to TS/VAL pages (section 3.5),
/// holding `values.len()` back-to-back HIST_SPANS records. `comp` is
/// always 0 (writer policy, section 3.5): span/count data does not
/// benefit from per-page lz4 the way Gorilla-vs-raw does.
fn append_hist_page_v3(
    series_id: &SeriesId,
    values: &[HistogramValue],
    payload_scratch: &mut Vec<u8>,
    hist_pages: &mut Vec<u8>,
) -> Result<(), WriteError> {
    payload_scratch.clear();
    for value in values {
        encode_histogram_record_into(payload_scratch, value)?;
    }

    let enc = page_enc::HIST_SPANS;
    let comp = page_comp::NONE;
    let crc = page_crc(&series_id.0, enc, comp, payload_scratch);
    hist_pages.push(enc);
    hist_pages.push(comp);
    hist_pages.extend_from_slice(&crc.to_le_bytes());
    hist_pages.extend_from_slice(payload_scratch);
    Ok(())
}

/// SERIES_META (kind 6, uncompressed form): v2's grammar (`count`,
/// `schema_count`, schemas, blocks 1-9) unchanged verbatim, plus three new
/// blocks (10 `value_kind`, 11 `hist_page_gap`, 12 `hist_page_len`,
/// section 3.4). Same schema-dictionary landmine note as
/// `build_series_meta_v2`: a schema's name_ord sequence is read directly
/// off `s.labels.iter()` (already sorted by name bytes by
/// `LabelSet::new`), never re-derived by sorting ordinal values.
fn build_series_meta_v3(
    series: &[SeriesInputV3],
    occurrence_ordinals: &[u32],
    footer_min_event_ts_ns: i64,
    ts_pages: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
    hist_pages: &mut Vec<u8>,
) -> Result<Vec<u8>, WriteError> {
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;

    let mut schema_index: HashMap<Vec<u32>, u32> = HashMap::new();
    let mut schemas: Vec<Vec<u32>> = Vec::new();

    let mut col_schema_ref: Vec<u8> = Vec::new();
    let mut col_value_ord: Vec<u8> = Vec::new();
    let mut col_sample_count: Vec<u8> = Vec::new();
    let mut col_min_ts_delta: Vec<u8> = Vec::new();
    let mut col_ts_span: Vec<u8> = Vec::new();
    let mut col_ts_page_gap: Vec<u8> = Vec::new();
    let mut col_ts_page_len: Vec<u8> = Vec::new();
    let mut col_val_page_gap: Vec<u8> = Vec::new();
    let mut col_val_page_len: Vec<u8> = Vec::new();
    let mut col_value_kind: Vec<u8> = Vec::new();
    let mut col_hist_page_gap: Vec<u8> = Vec::new();
    let mut col_hist_page_len: Vec<u8> = Vec::new();

    let mut payload_scratch: Vec<u8> = Vec::new();

    let mut next_occurrence = occurrence_ordinals.iter();

    for s in series {
        let label_count = s.labels.len();
        let mut name_ords: Vec<u32> = Vec::with_capacity(label_count);
        let mut value_ords: Vec<u32> = Vec::with_capacity(label_count);
        let mut prev_name: Option<&str> = None;
        for label in s.labels.iter() {
            debug_assert!(
                prev_name.is_none_or(|prev| prev <= label.name.as_str()),
                "LabelSet invariant violated: labels not sorted by name bytes"
            );
            prev_name = Some(label.name.as_str());

            let name_ord = *next_occurrence
                .next()
                .ok_or(WriteError::DictionaryInvariant)?;
            let value_ord = *next_occurrence
                .next()
                .ok_or(WriteError::DictionaryInvariant)?;
            name_ords.push(name_ord);
            value_ords.push(value_ord);
        }

        let schema_ref = match schema_index.get(&name_ords) {
            Some(&idx) => idx,
            None => {
                let idx = u32::try_from(schemas.len()).map_err(|_| WriteError::TooManySchemas)?;
                schema_index.insert(name_ords.clone(), idx);
                schemas.push(name_ords);
                idx
            }
        };
        write_uvarint(&mut col_schema_ref, u64::from(schema_ref));
        for value_ord in &value_ords {
            write_uvarint(&mut col_value_ord, u64::from(*value_ord));
        }

        let sample_count = u32::try_from(s.values.len()).map_err(|_| WriteError::TooManySamples)?;
        write_uvarint(&mut col_sample_count, u64::from(sample_count));

        // Non-empty by construction (empty series are dropped in
        // `write_v3`'s preamble before this function is called).
        let min_ts_ns = s.values.first_ts().unwrap_or(0);
        let max_ts_ns = s.values.last_ts().unwrap_or(0);
        let min_ts_delta = (i128::from(min_ts_ns) - i128::from(footer_min_event_ts_ns)) as u64;
        let ts_span = (i128::from(max_ts_ns) - i128::from(min_ts_ns)) as u64;
        write_uvarint(&mut col_min_ts_delta, min_ts_delta);
        write_uvarint(&mut col_ts_span, ts_span);

        let ts_values = s.values.ts_values();
        let ts_page_offset_before = ts_pages.len() as u64;
        append_ts_page_v3(&s.series_id, &ts_values, &mut payload_scratch, ts_pages)?;
        let ts_page_len = ts_pages.len() as u64 - ts_page_offset_before;
        // TS pages are never aligned (unchanged from v2); the gap column
        // stays 0 but is still emitted so the grammar generalizes.
        write_uvarint(&mut col_ts_page_gap, 0);
        write_uvarint(&mut col_ts_page_len, ts_page_len);

        match &s.values {
            SeriesValues::Scalar(samples) => {
                col_value_kind.push(0);

                let vals: Vec<f64> = samples.iter().map(|sm| sm.value).collect();
                let val_offset_before = val_pages.len() as u64;
                let val_gap =
                    append_val_page_v3(&s.series_id, &vals, &mut payload_scratch, val_pages);
                let val_total_added = val_pages.len() as u64 - val_offset_before;
                let val_page_len = val_total_added - val_gap;
                write_uvarint(&mut col_val_page_gap, val_gap);
                write_uvarint(&mut col_val_page_len, val_page_len);

                write_uvarint(&mut col_hist_page_gap, 0);
                write_uvarint(&mut col_hist_page_len, 0);
            }
            SeriesValues::Histogram(hist_samples) => {
                col_value_kind.push(1);

                write_uvarint(&mut col_val_page_gap, 0);
                write_uvarint(&mut col_val_page_len, 0);

                let values: Vec<HistogramValue> =
                    hist_samples.iter().map(|sm| sm.value.clone()).collect();
                let hist_offset_before = hist_pages.len() as u64;
                append_hist_page_v3(&s.series_id, &values, &mut payload_scratch, hist_pages)?;
                let hist_page_len = hist_pages.len() as u64 - hist_offset_before;
                // HIST pages are never aligned or placed with a preceding
                // gap by this writer (section 3.2: "no alignment
                // requirement"); the gap column stays 0 but is still
                // emitted so the grammar generalizes.
                write_uvarint(&mut col_hist_page_gap, 0);
                write_uvarint(&mut col_hist_page_len, hist_page_len);
            }
        }
    }

    let schema_count = u32::try_from(schemas.len()).map_err(|_| WriteError::TooManySchemas)?;

    let mut buf = Vec::with_capacity(
        8 + schemas.len() * 4
            + col_schema_ref.len()
            + col_value_ord.len()
            + col_sample_count.len()
            + col_min_ts_delta.len()
            + col_ts_span.len()
            + col_ts_page_gap.len()
            + col_ts_page_len.len()
            + col_val_page_gap.len()
            + col_val_page_len.len()
            + col_value_kind.len()
            + col_hist_page_gap.len()
            + col_hist_page_len.len()
            + 64,
    );
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&schema_count.to_le_bytes());
    for schema in &schemas {
        write_uvarint(&mut buf, schema.len() as u64);
        for name_ord in schema {
            write_uvarint(&mut buf, u64::from(*name_ord));
        }
    }
    for col in [
        &col_schema_ref,
        &col_value_ord,
        &col_sample_count,
        &col_min_ts_delta,
        &col_ts_span,
        &col_ts_page_gap,
        &col_ts_page_len,
        &col_val_page_gap,
        &col_val_page_len,
        &col_value_kind,
        &col_hist_page_gap,
        &col_hist_page_len,
    ] {
        write_uvarint(&mut buf, col.len() as u64);
        buf.extend_from_slice(col);
    }
    Ok(buf)
}

/// Same operation as v1/v2's zstd compress; kept separate per this
/// ticket's constraint that v1/v2 functions are never called from the v3
/// path.
fn zstd_compress_v3(data: &[u8]) -> Result<Vec<u8>, WriteError> {
    zstd::bulk::compress(data, ZSTD_LEVEL).map_err(|e| WriteError::Zstd(e.to_string()))
}

// --- RSEG v4 only (ADR-0018, docs/compaction-retention-plan.md section 4):
// multi-run verbatim-copy writer. Deliberately duplicates rather than
// shares helpers with v1/v2/v3 (same discipline as the `_v3` functions
// above), so each version's byte-for-byte behavior stays provable by
// inspection alone. This writer never encodes a sample: TS/VAL/HIST page
// bytes are already framed by the caller, so there is no "encoding" to
// duplicate for VAL/HIST -- only page-copy-with-alignment bookkeeping. ---

/// Page header length (enc(1) + comp(1) + crc32c(4)), same framing as
/// v1/v2/v3. A pre-encoded run page shorter than this cannot be valid.
const RUN_PAGE_HEADER_LEN_V4: usize = 6;

/// Identical grammar and first-occurrence-order rule as v3's
/// `DictionaryV3` (section 4: "SERIES_IDS / LABEL_DICT ... as v2/v3").
struct DictionaryV4<'a> {
    order: Vec<&'a str>,
    occurrence_ordinals: Vec<u32>,
    count: u32,
}

fn build_dictionary_v4(series: &[SeriesInputV4]) -> Result<DictionaryV4<'_>, WriteError> {
    let total_strings: usize = series.iter().map(|s| s.labels.len() * 2).sum();
    let mut interner: HashMap<&str, u32, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(total_strings + 1, BuildHasherDefault::default());
    interner.insert(METRIC_NAME_LABEL, 0);

    let mut order: Vec<&str> = Vec::new();
    let mut occurrence_ordinals: Vec<u32> = Vec::with_capacity(total_strings);
    let mut next_ordinal: u32 = 1;

    for s in series {
        for label in s.labels.iter() {
            for text in [label.name.as_str(), label.value.as_str()] {
                let id = match interner.entry(text) {
                    std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let id = next_ordinal;
                        next_ordinal = next_ordinal
                            .checked_add(1)
                            .ok_or(WriteError::TooManyDictStrings)?;
                        order.push(text);
                        e.insert(id);
                        id
                    }
                };
                occurrence_ordinals.push(id);
            }
        }
    }

    Ok(DictionaryV4 {
        order,
        occurrence_ordinals,
        count: next_ordinal,
    })
}

/// Grammar identical to v1/v2/v3's label-dict encode (count:u32, then
/// len:varint + UTF-8 bytes per string, `__name__` first).
fn encode_label_dict_v4(dict: &DictionaryV4<'_>) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&dict.count.to_le_bytes());
    write_uvarint(&mut buf, METRIC_NAME_LABEL.len() as u64);
    buf.extend_from_slice(METRIC_NAME_LABEL.as_bytes());
    for s in &dict.order {
        let bytes = s.as_bytes();
        write_uvarint(&mut buf, bytes.len() as u64);
        buf.extend_from_slice(bytes);
    }
    Ok(buf)
}

/// SERIES_IDS (kind 5): identical grammar to v2/v3. `series` is already
/// sorted by `series_id.0` bytes (`write_v4`'s preamble).
fn encode_series_ids_v4(series: &[SeriesInputV4]) -> Result<Vec<u8>, WriteError> {
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;
    let mut buf = Vec::with_capacity(4 + series.len() * 16);
    buf.extend_from_slice(&count.to_le_bytes());
    for s in series {
        buf.extend_from_slice(&s.series_id.0);
    }
    Ok(buf)
}

/// Copies one run's pre-framed TS page verbatim into `ts_pages`. TS pages
/// are never aligned (unchanged from v1/v2/v3); returns the page's byte
/// length for the caller's `ts_page_len` column.
fn append_ts_run_page_v4(page: &[u8], ts_pages: &mut Vec<u8>) -> Result<u64, WriteError> {
    if page.len() < RUN_PAGE_HEADER_LEN_V4 {
        return Err(WriteError::RunPageTooShort);
    }
    ts_pages.extend_from_slice(page);
    Ok(page.len() as u64)
}

/// Copies one run's pre-framed VAL page verbatim into `val_pages`,
/// applying the v2/v3 raw-f64 alignment rule (ADR-0014 section 3.5) by
/// inspecting the page's own `enc` byte (its first byte) rather than
/// re-deciding the encoding: this writer never decodes the payload.
/// Returns the pad length inserted before this page's header (the
/// caller's `val_page_gap` column); the page itself is copied unmodified,
/// so its crc32c (bound to series_id/enc/comp/payload, not position)
/// stays valid.
fn append_val_run_page_v4(page: &[u8], val_pages: &mut Vec<u8>) -> Result<u64, WriteError> {
    if page.len() < RUN_PAGE_HEADER_LEN_V4 {
        return Err(WriteError::RunPageTooShort);
    }
    let enc = page[0];

    let mut gap = 0u64;
    if enc == page_enc::VAL_RAW_F64 {
        let unaligned_payload_start = val_pages.len() as u64 + RUN_PAGE_HEADER_LEN_V4 as u64;
        let rem = unaligned_payload_start % 8;
        if rem != 0 {
            gap = 8 - rem;
            val_pages.extend(std::iter::repeat_n(0u8, gap as usize));
        }
    }

    val_pages.extend_from_slice(page);
    Ok(gap)
}

/// Copies one run's pre-framed HIST page verbatim into `hist_pages`. No
/// alignment requirement, unchanged from v3 (section 3.2); this writer
/// treats v3's HIST_PAGES bytes as an opaque per-run blob (never
/// re-encodes a histogram record).
fn append_hist_run_page_v4(page: &[u8], hist_pages: &mut Vec<u8>) -> Result<(), WriteError> {
    if page.len() < RUN_PAGE_HEADER_LEN_V4 {
        return Err(WriteError::RunPageTooShort);
    }
    hist_pages.extend_from_slice(page);
    Ok(())
}

/// SERIES_META (kind 6, uncompressed form) for RSEG v4
/// (docs/compaction-retention-plan.md section 4): v3's series-major
/// `schema_ref`/`value_ord`/`value_kind` blocks unchanged, plus a new
/// series-major `run_count` block and a `run_total` header field, then
/// eight run-major blocks (provenance, sample_count, bounds, and the four
/// page gap/len pairs) that generalize v3's one-run-per-series columns.
/// Same schema-dictionary landmine note as `build_series_meta_v3`: a
/// schema's name_ord sequence is read directly off `s.labels.iter()`
/// (already sorted by name bytes by `LabelSet::new`), never re-derived by
/// sorting ordinal values.
#[allow(clippy::too_many_arguments)]
fn build_series_meta_v4(
    series: &[SeriesInputV4],
    occurrence_ordinals: &[u32],
    footer_min_event_ts_ns: i64,
    footer_base_created_unix_ns: i64,
    run_total: u32,
    ts_pages: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
    hist_pages: &mut Vec<u8>,
) -> Result<Vec<u8>, WriteError> {
    let count = u32::try_from(series.len()).map_err(|_| WriteError::TooManySeries)?;

    let mut schema_index: HashMap<Vec<u32>, u32> = HashMap::new();
    let mut schemas: Vec<Vec<u32>> = Vec::new();

    let mut col_schema_ref: Vec<u8> = Vec::new();
    let mut col_value_ord: Vec<u8> = Vec::new();
    let mut col_value_kind: Vec<u8> = Vec::new();
    let mut col_run_count: Vec<u8> = Vec::new();
    let mut col_run_created_delta: Vec<u8> = Vec::new();
    let mut col_run_epoch: Vec<u8> = Vec::new();
    let mut col_run_seq: Vec<u8> = Vec::new();
    let mut col_run_sample_count: Vec<u8> = Vec::new();
    let mut col_run_min_ts_delta: Vec<u8> = Vec::new();
    let mut col_run_ts_span: Vec<u8> = Vec::new();
    let mut col_ts_page_gap: Vec<u8> = Vec::new();
    let mut col_ts_page_len: Vec<u8> = Vec::new();
    let mut col_val_page_gap: Vec<u8> = Vec::new();
    let mut col_val_page_len: Vec<u8> = Vec::new();
    let mut col_hist_page_gap: Vec<u8> = Vec::new();
    let mut col_hist_page_len: Vec<u8> = Vec::new();

    let mut next_occurrence = occurrence_ordinals.iter();

    for s in series {
        let label_count = s.labels.len();
        let mut name_ords: Vec<u32> = Vec::with_capacity(label_count);
        let mut value_ords: Vec<u32> = Vec::with_capacity(label_count);
        let mut prev_name: Option<&str> = None;
        for label in s.labels.iter() {
            debug_assert!(
                prev_name.is_none_or(|prev| prev <= label.name.as_str()),
                "LabelSet invariant violated: labels not sorted by name bytes"
            );
            prev_name = Some(label.name.as_str());

            let name_ord = *next_occurrence
                .next()
                .ok_or(WriteError::DictionaryInvariant)?;
            let value_ord = *next_occurrence
                .next()
                .ok_or(WriteError::DictionaryInvariant)?;
            name_ords.push(name_ord);
            value_ords.push(value_ord);
        }

        let schema_ref = match schema_index.get(&name_ords) {
            Some(&idx) => idx,
            None => {
                let idx = u32::try_from(schemas.len()).map_err(|_| WriteError::TooManySchemas)?;
                schema_index.insert(name_ords.clone(), idx);
                schemas.push(name_ords);
                idx
            }
        };
        write_uvarint(&mut col_schema_ref, u64::from(schema_ref));
        for value_ord in &value_ords {
            write_uvarint(&mut col_value_ord, u64::from(*value_ord));
        }

        // Uniform across the series' runs by construction: `write_v4`'s
        // preamble rejects a mixed-kind series before this function runs.
        let value_kind: u8 = match s.runs.first().map(|r| &r.value_page) {
            Some(RunValuePageV4::Histogram(_)) => 1,
            _ => 0,
        };
        col_value_kind.push(value_kind);

        let run_count = u32::try_from(s.runs.len()).map_err(|_| WriteError::TooManyRuns)?;
        write_uvarint(&mut col_run_count, u64::from(run_count));

        for r in &s.runs {
            write_uvarint(
                &mut col_run_created_delta,
                (i128::from(r.created_unix_ns) - i128::from(footer_base_created_unix_ns)) as u64,
            );
            write_uvarint(&mut col_run_epoch, r.writer_epoch);
            write_uvarint(&mut col_run_seq, r.writer_seq);
            write_uvarint(&mut col_run_sample_count, u64::from(r.sample_count));
            write_uvarint(
                &mut col_run_min_ts_delta,
                (i128::from(r.min_ts_ns) - i128::from(footer_min_event_ts_ns)) as u64,
            );
            write_uvarint(
                &mut col_run_ts_span,
                (i128::from(r.max_ts_ns) - i128::from(r.min_ts_ns)) as u64,
            );

            let ts_page_len = append_ts_run_page_v4(&r.ts_page, ts_pages)?;
            write_uvarint(&mut col_ts_page_gap, 0);
            write_uvarint(&mut col_ts_page_len, ts_page_len);

            match &r.value_page {
                RunValuePageV4::Scalar(page) => {
                    let val_gap = append_val_run_page_v4(page, val_pages)?;
                    write_uvarint(&mut col_val_page_gap, val_gap);
                    write_uvarint(&mut col_val_page_len, page.len() as u64);
                    write_uvarint(&mut col_hist_page_gap, 0);
                    write_uvarint(&mut col_hist_page_len, 0);
                }
                RunValuePageV4::Histogram(page) => {
                    append_hist_run_page_v4(page, hist_pages)?;
                    write_uvarint(&mut col_val_page_gap, 0);
                    write_uvarint(&mut col_val_page_len, 0);
                    write_uvarint(&mut col_hist_page_gap, 0);
                    write_uvarint(&mut col_hist_page_len, page.len() as u64);
                }
            }
        }
    }

    let schema_count = u32::try_from(schemas.len()).map_err(|_| WriteError::TooManySchemas)?;

    let mut buf = Vec::with_capacity(
        12 + schemas.len() * 4
            + col_schema_ref.len()
            + col_value_ord.len()
            + col_value_kind.len()
            + col_run_count.len()
            + col_run_created_delta.len()
            + col_run_epoch.len()
            + col_run_seq.len()
            + col_run_sample_count.len()
            + col_run_min_ts_delta.len()
            + col_run_ts_span.len()
            + col_ts_page_gap.len()
            + col_ts_page_len.len()
            + col_val_page_gap.len()
            + col_val_page_len.len()
            + col_hist_page_gap.len()
            + col_hist_page_len.len()
            + 64,
    );
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&schema_count.to_le_bytes());
    for schema in &schemas {
        write_uvarint(&mut buf, schema.len() as u64);
        for name_ord in schema {
            write_uvarint(&mut buf, u64::from(*name_ord));
        }
    }
    buf.extend_from_slice(&run_total.to_le_bytes());
    for col in [
        &col_schema_ref,
        &col_value_ord,
        &col_value_kind,
        &col_run_count,
        &col_run_created_delta,
        &col_run_epoch,
        &col_run_seq,
        &col_run_sample_count,
        &col_run_min_ts_delta,
        &col_run_ts_span,
        &col_ts_page_gap,
        &col_ts_page_len,
        &col_val_page_gap,
        &col_val_page_len,
        &col_hist_page_gap,
        &col_hist_page_len,
    ] {
        write_uvarint(&mut buf, col.len() as u64);
        buf.extend_from_slice(col);
    }
    Ok(buf)
}

/// Same operation as v1/v2/v3's zstd compress; kept separate per this
/// crate's discipline that no version's writer path calls another's
/// helpers.
fn zstd_compress_v4(data: &[u8]) -> Result<Vec<u8>, WriteError> {
    zstd::bulk::compress(data, ZSTD_LEVEL).map_err(|e| WriteError::Zstd(e.to_string()))
}

/// Structural tests for the v2 encode path. No reader exists yet (phase 3 /
/// issue #31 adds one), so these tests parse the emitted bytes directly
/// using the same primitives the eventual reader will use (`crate::varint`,
/// `crate::crc`, `crate::ts_delta`, `crate::gorilla`). This module lives in
/// `src/` rather than `tests/` specifically for that crate-internal access;
/// an external integration test cannot reach `crate::varint::read_uvarint`
/// (not re-exported) or the section-kind/version constants without
/// duplicating them.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod v2_tests {
    use std::collections::HashSet;

    use proptest::prelude::*;
    use ravel_types::{Label, Sample};

    use super::*;
    use crate::varint::read_uvarint;

    fn test_identity() -> SegmentIdentity {
        SegmentIdentity {
            tenant_hash: [0x5A; 16],
            shard: 4,
            writer_id: "v2-test-writer".to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn test_bounds() -> IngestBounds {
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 1_000,
        }
    }

    /// Parses the 16-byte trailer and decodes the footer protobuf. Panics
    /// (rather than returning a typed error) on any mismatch: every test
    /// here controls its own input, so a parse failure is this test
    /// module's own bug, never untrusted data.
    fn decode_footer(object: &[u8]) -> Footer {
        let n = object.len();
        assert!(n >= 16, "object smaller than the trailer");
        let trailer = &object[n - 16..];
        let footer_len = u32::from_le_bytes(trailer[0..4].try_into().expect("4 bytes")) as usize;
        let version = u16::from_le_bytes(trailer[8..10].try_into().expect("2 bytes"));
        assert_eq!(version, VERSION_V2, "test helper only decodes v2 objects");
        assert_eq!(&trailer[12..16], &MAGIC, "bad magic");
        let footer_start = n - 16 - footer_len;
        let footer_bytes = &object[footer_start..n - 16];
        <Footer as prost::Message>::decode(footer_bytes).expect("footer decodes")
    }

    fn section<'a>(object: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
        let s = footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("missing section kind {kind}"));
        let start = s.offset as usize;
        let end = start + s.len as usize;
        &object[start..end]
    }

    fn section_desc(footer: &Footer, kind: u32) -> &Section {
        footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("missing section kind {kind}"))
    }

    fn decompress_section(stored: &[u8], uncompressed_len: u64) -> Vec<u8> {
        zstd::bulk::decompress(stored, uncompressed_len as usize).expect("zstd decompresses")
    }

    /// LABEL_DICT decoded into ordinal-indexed strings (index 0 =
    /// `"__name__"`).
    fn parse_label_dict(raw: &[u8]) -> Vec<String> {
        let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes")) as usize;
        let mut pos = 4usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let len = read_uvarint(raw, &mut pos).expect("varint") as usize;
            let s = std::str::from_utf8(&raw[pos..pos + len])
                .expect("utf8")
                .to_string();
            pos += len;
            out.push(s);
        }
        assert_eq!(pos, raw.len(), "trailing bytes in LABEL_DICT");
        assert_eq!(out.len(), count);
        out
    }

    /// SERIES_META decoded into plain Vecs, per docs/segment-format.md's
    /// "SERIES_META (uncompressed form)" grammar.
    struct ParsedSeriesMeta {
        count: u32,
        schemas: Vec<Vec<u32>>,
        schema_ref: Vec<u32>,
        value_ord: Vec<Vec<u32>>,
        sample_count: Vec<u32>,
        min_ts_delta: Vec<u64>,
        ts_span: Vec<u64>,
        ts_page_gap: Vec<u64>,
        ts_page_len: Vec<u64>,
        val_page_gap: Vec<u64>,
        val_page_len: Vec<u64>,
    }

    fn read_block<'a>(raw: &'a [u8], pos: &mut usize) -> &'a [u8] {
        let block_len = read_uvarint(raw, pos).expect("block_len varint") as usize;
        let start = *pos;
        let end = start + block_len;
        let slice = &raw[start..end];
        *pos = end;
        slice
    }

    fn read_varints(block: &[u8], n: usize) -> Vec<u64> {
        let mut pos = 0usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(read_uvarint(block, &mut pos).expect("varint"));
        }
        assert_eq!(pos, block.len(), "trailing bytes in column block");
        out
    }

    fn parse_series_meta(raw: &[u8]) -> ParsedSeriesMeta {
        let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes"));
        let schema_count = u32::from_le_bytes(raw[4..8].try_into().expect("4 bytes"));
        let mut pos = 8usize;
        let mut schemas = Vec::with_capacity(schema_count as usize);
        for _ in 0..schema_count {
            let name_count = read_uvarint(raw, &mut pos).expect("varint") as usize;
            let mut names = Vec::with_capacity(name_count);
            for _ in 0..name_count {
                names.push(read_uvarint(raw, &mut pos).expect("varint") as u32);
            }
            schemas.push(names);
        }

        let schema_ref_block = read_block(raw, &mut pos);
        let value_ord_block = read_block(raw, &mut pos);
        let sample_count_block = read_block(raw, &mut pos);
        let min_ts_delta_block = read_block(raw, &mut pos);
        let ts_span_block = read_block(raw, &mut pos);
        let ts_page_gap_block = read_block(raw, &mut pos);
        let ts_page_len_block = read_block(raw, &mut pos);
        let val_page_gap_block = read_block(raw, &mut pos);
        let val_page_len_block = read_block(raw, &mut pos);
        assert_eq!(pos, raw.len(), "trailing bytes in SERIES_META");

        let schema_ref: Vec<u32> = read_varints(schema_ref_block, count as usize)
            .into_iter()
            .map(|v| v as u32)
            .collect();

        let mut value_ord = Vec::with_capacity(count as usize);
        {
            let mut pos = 0usize;
            for &sref in &schema_ref {
                let name_count = schemas[sref as usize].len();
                let mut vals = Vec::with_capacity(name_count);
                for _ in 0..name_count {
                    vals.push(read_uvarint(value_ord_block, &mut pos).expect("varint") as u32);
                }
                value_ord.push(vals);
            }
            assert_eq!(
                pos,
                value_ord_block.len(),
                "trailing bytes in value_ord block"
            );
        }

        let sample_count: Vec<u32> = read_varints(sample_count_block, count as usize)
            .into_iter()
            .map(|v| v as u32)
            .collect();
        let min_ts_delta = read_varints(min_ts_delta_block, count as usize);
        let ts_span = read_varints(ts_span_block, count as usize);
        let ts_page_gap = read_varints(ts_page_gap_block, count as usize);
        let ts_page_len = read_varints(ts_page_len_block, count as usize);
        let val_page_gap = read_varints(val_page_gap_block, count as usize);
        let val_page_len = read_varints(val_page_len_block, count as usize);

        ParsedSeriesMeta {
            count,
            schemas,
            schema_ref,
            value_ord,
            sample_count,
            min_ts_delta,
            ts_span,
            ts_page_gap,
            ts_page_len,
            val_page_gap,
            val_page_len,
        }
    }

    fn parse_series_ids(raw: &[u8]) -> Vec<[u8; 16]> {
        let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes")) as usize;
        assert_eq!(raw.len(), 4 + count * 16, "SERIES_IDS length mismatch");
        (0..count)
            .map(|i| {
                let start = 4 + i * 16;
                raw[start..start + 16].try_into().expect("16 bytes")
            })
            .collect()
    }

    /// Splits a page's 6-byte header from its stored payload and verifies
    /// the page crc (series-id-bound, per docs/segment-format.md).
    fn split_and_verify_page<'a>(series_id: &SeriesId, page: &'a [u8]) -> (u8, u8, &'a [u8]) {
        assert!(page.len() >= 6, "page shorter than its header");
        let enc = page[0];
        let comp = page[1];
        let crc = u32::from_le_bytes(page[2..6].try_into().expect("4 bytes"));
        let payload = &page[6..];
        assert_eq!(
            page_crc(&series_id.0, enc, comp, payload),
            crc,
            "page crc mismatch"
        );
        (enc, comp, payload)
    }

    fn decompress_page_payload(comp: u8, payload: &[u8]) -> Vec<u8> {
        match comp {
            page_comp::NONE => payload.to_vec(),
            page_comp::LZ4 => {
                lz4_flex::decompress_size_prepended(payload).expect("lz4 decompresses")
            }
            other => panic!("unexpected page comp byte {other}"),
        }
    }

    // --- the identity-order regression (the landmine) ---

    #[test]
    fn schema_name_ord_follows_name_byte_order_not_ordinal_order() {
        // Series A interns "zone" (and its value) first, claiming a low
        // ordinal for a name that sorts *last* among {app, region, zone}
        // by byte comparison. Series B then uses all three names
        // together. A writer that derived a schema's name_ord sequence by
        // sorting ordinal *values* (instead of reusing the label list's
        // own name-byte order, which `LabelSet::new` already established
        // in crates/ravel-types) would silently reorder series B's label
        // pairs here and corrupt canonical series identity (ADR-0005).
        // This is invisible to a v1-vs-v2 roundtrip test because a writer
        // and a hypothetical reader sharing the same wrong assumption
        // round-trip perfectly with each other.
        let series_a = SeriesInput {
            series_id: SeriesId([0x01; 16]),
            labels: LabelSet::new(vec![Label {
                name: "zone".to_string(),
                value: "z1".to_string(),
            }])
            .expect("valid labels"),
            samples: vec![Sample {
                ts_ns: 0,
                value: 1.0,
            }],
        };
        // Fed in non-lexicographic order; `LabelSet::new` sorts it to
        // [app, region, zone] regardless (the type system already
        // forecloses "insertion order" mattering within one series). The
        // bug this test targets is about *dictionary* ordinal-assignment
        // order diverging from name-byte order across series, not about
        // label insertion order within one series.
        let series_b = SeriesInput {
            series_id: SeriesId([0x02; 16]),
            labels: LabelSet::new(vec![
                Label {
                    name: "zone".to_string(),
                    value: "z2".to_string(),
                },
                Label {
                    name: "app".to_string(),
                    value: "a1".to_string(),
                },
                Label {
                    name: "region".to_string(),
                    value: "r1".to_string(),
                },
            ])
            .expect("valid labels"),
            samples: vec![Sample {
                ts_ns: 0,
                value: 2.0,
            }],
        };

        let written =
            SegmentWriter::write_v2(vec![series_a, series_b], test_identity(), test_bounds())
                .expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object);

        let dict_desc = section_desc(&footer, section_kind::LABEL_DICT);
        let dict_raw = decompress_section(
            section(object, &footer, section_kind::LABEL_DICT),
            dict_desc.uncompressed_len,
        );
        let dict = parse_label_dict(&dict_raw);
        let ord = |s: &str| -> u32 {
            u32::try_from(
                dict.iter()
                    .position(|x| x.as_str() == s)
                    .unwrap_or_else(|| panic!("{s} not interned")),
            )
            .expect("ordinal fits u32")
        };

        // Sanity check the scenario actually exercises the landmine:
        // zone's ordinal must be lower than app's and region's (it was
        // interned first, by series A), even though zone sorts last by
        // name bytes.
        let ordinal_zone = ord("zone");
        let ordinal_app = ord("app");
        let ordinal_region = ord("region");
        assert!(
            ordinal_zone < ordinal_app && ordinal_zone < ordinal_region,
            "test setup did not create the intended ordinal/name-byte divergence"
        );

        let meta_desc = section_desc(&footer, section_kind::SERIES_META);
        let meta_raw = decompress_section(
            section(object, &footer, section_kind::SERIES_META),
            meta_desc.uncompressed_len,
        );
        let meta = parse_series_meta(&meta_raw);
        let ids = parse_series_ids(section(object, &footer, section_kind::SERIES_IDS));

        let index_of = |id: [u8; 16]| ids.iter().position(|&x| x == id).expect("id present");
        let series_a_idx = index_of([0x01; 16]);
        let series_b_idx = index_of([0x02; 16]);

        // Series A's schema is just [zone].
        assert_eq!(
            meta.schemas[meta.schema_ref[series_a_idx] as usize],
            vec![ordinal_zone]
        );

        // Series B's schema MUST be in name-byte order (app, region,
        // zone), i.e. [ordinal_app, ordinal_region, ordinal_zone] -- NOT
        // ascending by ordinal value, which would incorrectly be
        // [ordinal_zone, ordinal_app, ordinal_region].
        let schema_b = &meta.schemas[meta.schema_ref[series_b_idx] as usize];
        assert_eq!(
            schema_b,
            &vec![ordinal_app, ordinal_region, ordinal_zone],
            "schema name_ord sequence must follow name-byte order (app, region, zone), \
             not ordinal-value order"
        );

        // And the value ordinals must stay glued to their own name in
        // that same order: [app's value "a1", region's value "r1",
        // zone's value "z2"].
        let value_ord_b = &meta.value_ord[series_b_idx];
        assert_eq!(
            value_ord_b,
            &vec![ord("a1"), ord("r1"), ord("z2")],
            "value_ord must stay positionally paired with its own schema name, in the \
             same name-byte order"
        );
    }

    /// Two series sharing an identical label-name list must resolve to the
    /// same name ordinals through the schema memo (issue #95), even when a
    /// series with a different name list sits between them so the shared
    /// schema is not adjacent. This is the map-not-last-seen requirement: a
    /// last-seen check would miss series C's repeat of series A's schema.
    #[test]
    fn shared_name_list_resolves_to_identical_ordinals_via_memo() {
        let mk = |id: u8, labels: Vec<Label>, val: f64| SeriesInput {
            series_id: SeriesId([id; 16]),
            labels: LabelSet::new(labels).expect("valid labels"),
            samples: vec![Sample {
                ts_ns: 0,
                value: val,
            }],
        };
        // A and C share names {__name__, region, zone}; B breaks adjacency
        // with a different name list. Values differ throughout so the shared
        // ordinals cannot come from the series being trivially identical.
        let series = vec![
            mk(
                0x01,
                vec![
                    Label {
                        name: METRIC_NAME_LABEL.to_string(),
                        value: "m_a".to_string(),
                    },
                    Label {
                        name: "region".to_string(),
                        value: "r_a".to_string(),
                    },
                    Label {
                        name: "zone".to_string(),
                        value: "z_a".to_string(),
                    },
                ],
                1.0,
            ),
            mk(
                0x02,
                vec![
                    Label {
                        name: METRIC_NAME_LABEL.to_string(),
                        value: "m_b".to_string(),
                    },
                    Label {
                        name: "app".to_string(),
                        value: "a_b".to_string(),
                    },
                ],
                2.0,
            ),
            mk(
                0x03,
                vec![
                    Label {
                        name: METRIC_NAME_LABEL.to_string(),
                        value: "m_c".to_string(),
                    },
                    Label {
                        name: "region".to_string(),
                        value: "r_c".to_string(),
                    },
                    Label {
                        name: "zone".to_string(),
                        value: "z_c".to_string(),
                    },
                ],
                3.0,
            ),
        ];

        // Fed in id order directly (this exercises the interner helper, not
        // the full write): A interns the schema, B is a different schema, C
        // must hit the memo for A's schema.
        let dict = build_dictionary_v2(&series).expect("dictionary builds");

        // occurrence_ordinals is [name, value] per label, per series in order.
        // A occupies 6 entries (3 labels), B 4 (2 labels), C 6 (3 labels).
        // Names sit at even offsets within each series' block; LabelSet sorts
        // both A and C to [__name__, region, zone].
        let a_names = [
            dict.occurrence_ordinals[0],
            dict.occurrence_ordinals[2],
            dict.occurrence_ordinals[4],
        ];
        let c_base = 6 + 4; // A's 6 entries + B's 4 entries
        let c_names = [
            dict.occurrence_ordinals[c_base],
            dict.occurrence_ordinals[c_base + 2],
            dict.occurrence_ordinals[c_base + 4],
        ];
        assert_eq!(
            a_names, c_names,
            "a shared name list must resolve to identical name ordinals"
        );

        // __name__ is ordinal 0; region and zone got distinct ordinals.
        assert_eq!(a_names[0], 0, "__name__ is always ordinal 0");
        assert_ne!(a_names[1], a_names[2]);

        // The values, by contrast, are all distinct strings and must NOT
        // collapse: A's and C's value ordinals differ.
        let a_region_value = dict.occurrence_ordinals[3];
        let c_region_value = dict.occurrence_ordinals[c_base + 3];
        assert_ne!(
            a_region_value, c_region_value,
            "distinct label values must still get distinct ordinals"
        );
    }

    /// An empty segment still emits a well-formed SERIES_IDS section (a
    /// bare `count: u32 = 0`, i.e. exactly 4 bytes) and a well-formed
    /// SERIES_META section (`count = 0`, `schema_count = 0`, all 9 blocks
    /// present with `block_len = 0`), not an absent or zero-length
    /// section. The proptest generators above sometimes produce an empty
    /// batch (series count range starts at 0), but neither property test's
    /// loop body ever executes on that path, so this dedicated case is the
    /// only thing that actually checks the empty-segment section shapes
    /// against the mandatory-kinds rule (docs/segment-format.md v2
    /// amendment: LABEL_DICT, SERIES_IDS, SERIES_META, TS_PAGES, VAL_PAGES
    /// are all mandatory, with no "0 series means the section can be
    /// missing" exception).
    #[test]
    fn empty_segment_has_well_formed_sections() {
        let written =
            SegmentWriter::write_v2(Vec::new(), test_identity(), test_bounds()).expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object);

        assert_eq!(footer.series_count, 0);
        assert_eq!(footer.sample_count, 0);
        assert_eq!(footer.min_event_ts_ns, 0);
        assert_eq!(footer.max_event_ts_ns, 0);

        let ids_desc = section_desc(&footer, section_kind::SERIES_IDS);
        assert_eq!(ids_desc.comp, compression::NONE);
        assert_eq!(ids_desc.len, 4, "empty SERIES_IDS is a bare count:u32 = 0");
        assert_eq!(ids_desc.uncompressed_len, 4);
        let ids = parse_series_ids(section(object, &footer, section_kind::SERIES_IDS));
        assert!(ids.is_empty());

        let meta_desc = section_desc(&footer, section_kind::SERIES_META);
        let meta_raw = decompress_section(
            section(object, &footer, section_kind::SERIES_META),
            meta_desc.uncompressed_len,
        );
        let meta = parse_series_meta(&meta_raw);
        assert_eq!(meta.count, 0);
        assert!(meta.schemas.is_empty());
        assert!(meta.schema_ref.is_empty());
        assert!(meta.sample_count.is_empty());

        let ts_bytes = section(object, &footer, section_kind::TS_PAGES);
        let val_bytes = section(object, &footer, section_kind::VAL_PAGES);
        assert!(ts_bytes.is_empty());
        assert!(val_bytes.is_empty());
    }

    // --- proptest generators ---

    fn label_name_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("app".to_string()),
            Just("region".to_string()),
            Just("zone".to_string()),
            Just("instance".to_string()),
            Just("method".to_string()),
        ]
    }

    fn labelset_strategy() -> impl Strategy<Value = LabelSet> {
        (
            "[a-z_]{1,10}",
            prop::collection::vec((label_name_strategy(), "[a-zA-Z0-9_]{0,8}"), 0..4),
        )
            .prop_map(|(metric_name, extra)| {
                let mut ls = vec![Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: metric_name,
                }];
                let mut seen = HashSet::new();
                seen.insert(METRIC_NAME_LABEL.to_string());
                for (name, value) in extra {
                    if seen.insert(name.clone()) {
                        ls.push(Label { name, value });
                    }
                }
                LabelSet::new(ls).expect("no duplicate names by construction")
            })
    }

    fn ts_strategy() -> impl Strategy<Value = i64> {
        prop_oneof![
            3 => 0i64..300,
            1 => -1_000_000_000_000i64..=1_000_000_000_000i64,
        ]
    }

    fn sample_value_strategy() -> impl Strategy<Value = f64> {
        prop_oneof![
            10 => any::<f64>(),
            1 => Just(f64::NAN),
            1 => Just(-f64::NAN),
            1 => Just(f64::INFINITY),
            1 => Just(f64::NEG_INFINITY),
            1 => Just(0.0f64),
            1 => Just(-0.0f64),
        ]
    }

    fn samples_strategy() -> impl Strategy<Value = Vec<(i64, f64)>> {
        prop_oneof![
            // A 1-sample series always raw-falls-back (Gorilla's first
            // value alone is exactly 8 bytes, never smaller than raw), so
            // this branch guarantees VAL_RAW_F64 pages -- and therefore
            // alignment padding -- show up often, instead of only by
            // luck.
            3 => prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..2),
            5 => prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..12),
        ]
    }

    fn series_batch_strategy() -> impl Strategy<Value = Vec<SeriesInput>> {
        prop::collection::vec((labelset_strategy(), samples_strategy()), 0..8).prop_map(|entries| {
            entries
                .into_iter()
                .enumerate()
                .map(|(idx, (labels, samples))| {
                    let mut id = [0u8; 16];
                    id[..8].copy_from_slice(&(idx as u64).to_be_bytes());
                    SeriesInput {
                        series_id: SeriesId(id),
                        labels,
                        samples: samples
                            .into_iter()
                            .map(|(ts_ns, value)| Sample { ts_ns, value })
                            .collect(),
                    }
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Alignment property (docs/segment-format.md "VAL_RAW_F64 page
        /// alignment, v2"): the VAL_PAGES section offset is 0 mod 8, and
        /// every VAL_RAW_F64 page's payload start is 0 mod 8 relative to
        /// the section start (hence to the object start too, since the
        /// section itself is aligned).
        #[test]
        fn val_raw_f64_pages_are_8_byte_aligned(series in series_batch_strategy()) {
            let written = SegmentWriter::write_v2(series, test_identity(), test_bounds())
                .expect("writes");
            let object = written.bytes.as_ref();
            let footer = decode_footer(object);

            let val_desc = section_desc(&footer, section_kind::VAL_PAGES);
            prop_assert_eq!(val_desc.offset % 8, 0, "VAL_PAGES section offset must be 8-aligned");

            let val_bytes = section(object, &footer, section_kind::VAL_PAGES);
            let meta_desc = section_desc(&footer, section_kind::SERIES_META);
            let meta_raw = decompress_section(
                section(object, &footer, section_kind::SERIES_META),
                meta_desc.uncompressed_len,
            );
            let meta = parse_series_meta(&meta_raw);
            let ids = parse_series_ids(section(object, &footer, section_kind::SERIES_IDS));

            let mut running = 0u64;
            for (i, &id) in ids.iter().enumerate() {
                let offset = running + meta.val_page_gap[i];
                let len = meta.val_page_len[i];
                let end = offset + len;
                prop_assert!(end <= val_bytes.len() as u64);
                let series_id = SeriesId(id);
                let page = &val_bytes[offset as usize..end as usize];
                let (enc, _comp, _payload) = split_and_verify_page(&series_id, page);
                if enc == page_enc::VAL_RAW_F64 {
                    let payload_start = offset + 6;
                    prop_assert_eq!(
                        payload_start % 8,
                        0,
                        "VAL_RAW_F64 payload must start 0 mod 8 relative to the section start"
                    );
                }
                running = end;
            }
            prop_assert_eq!(running, val_bytes.len() as u64);
        }

        /// Internal-consistency property: the writer's declared per-series
        /// metadata (sample counts, ts bounds, reconstructed TS/VAL page
        /// ranges from the gap/len columns) is self-consistent with what
        /// was actually written to TS_PAGES/VAL_PAGES, decoding through
        /// the same TS_DELTA_VARINT / Gorilla / raw-f64 codecs the
        /// eventual reader (phase 3) will use.
        #[test]
        fn series_meta_v2_matches_written_pages(series in series_batch_strategy()) {
            let written = SegmentWriter::write_v2(series, test_identity(), test_bounds())
                .expect("writes");
            let object = written.bytes.as_ref();
            let footer = decode_footer(object);

            let ids = parse_series_ids(section(object, &footer, section_kind::SERIES_IDS));
            prop_assert_eq!(ids.len() as u64, footer.series_count);
            for w in ids.windows(2) {
                prop_assert!(w[0] < w[1], "SERIES_IDS must be strictly ascending");
            }

            let meta_desc = section_desc(&footer, section_kind::SERIES_META);
            let meta_raw = decompress_section(
                section(object, &footer, section_kind::SERIES_META),
                meta_desc.uncompressed_len,
            );
            let meta = parse_series_meta(&meta_raw);
            prop_assert_eq!(u64::from(meta.count), footer.series_count);
            prop_assert_eq!(meta.count as usize, ids.len());

            let ts_bytes = section(object, &footer, section_kind::TS_PAGES);
            let val_bytes = section(object, &footer, section_kind::VAL_PAGES);

            let mut ts_running = 0u64;
            let mut val_running = 0u64;
            for (i, &id) in ids.iter().enumerate() {
                let series_id = SeriesId(id);

                let ts_offset = ts_running + meta.ts_page_gap[i];
                let ts_len = meta.ts_page_len[i];
                let ts_end = ts_offset + ts_len;
                prop_assert!(ts_end <= ts_bytes.len() as u64);
                let (ts_enc, ts_comp, ts_payload) = split_and_verify_page(
                    &series_id,
                    &ts_bytes[ts_offset as usize..ts_end as usize],
                );
                prop_assert_eq!(ts_enc, page_enc::TS_DELTA_VARINT);
                let ts_decompressed = decompress_page_payload(ts_comp, ts_payload);

                let min_ts_ns = footer.min_event_ts_ns + meta.min_ts_delta[i] as i64;
                let max_ts_ns = min_ts_ns + meta.ts_span[i] as i64;
                let mut ts_out = Vec::new();
                let ts_decode = crate::ts_delta::decode_ts_deltas_into(
                    &ts_decompressed,
                    meta.sample_count[i] as usize,
                    min_ts_ns,
                    max_ts_ns,
                    &mut ts_out,
                );
                prop_assert!(ts_decode.is_ok(), "TS page failed to decode: {:?}", ts_decode);
                prop_assert_eq!(ts_out.len(), meta.sample_count[i] as usize);
                if let (Some(first), Some(last)) = (ts_out.first(), ts_out.last()) {
                    prop_assert_eq!(*first, min_ts_ns);
                    prop_assert_eq!(*last, max_ts_ns);
                }
                ts_running = ts_end;

                let val_offset = val_running + meta.val_page_gap[i];
                let val_len = meta.val_page_len[i];
                let val_end = val_offset + val_len;
                prop_assert!(val_end <= val_bytes.len() as u64);
                let (val_enc, val_comp, val_payload) = split_and_verify_page(
                    &series_id,
                    &val_bytes[val_offset as usize..val_end as usize],
                );
                prop_assert_eq!(val_comp, page_comp::NONE, "VAL pages are never compressed");
                let mut vals_out = Vec::new();
                match val_enc {
                    page_enc::VAL_GORILLA => {
                        let r = crate::gorilla::decode_gorilla_into(
                            val_payload,
                            meta.sample_count[i] as usize,
                            &mut vals_out,
                        );
                        prop_assert!(r.is_ok(), "Gorilla page failed to decode: {:?}", r);
                    }
                    page_enc::VAL_RAW_F64 => {
                        prop_assert_eq!(val_payload.len(), meta.sample_count[i] as usize * 8);
                        for chunk in val_payload.chunks_exact(8) {
                            vals_out.push(f64::from_le_bytes(chunk.try_into().expect("8 bytes")));
                        }
                    }
                    other => prop_assert!(false, "unexpected VAL page enc byte {other}"),
                }
                prop_assert_eq!(vals_out.len(), meta.sample_count[i] as usize);
                val_running = val_end;
            }
            prop_assert_eq!(ts_running, ts_bytes.len() as u64);
            prop_assert_eq!(val_running, val_bytes.len() as u64);
        }
    }
}

/// Structural tests for the v3 encode path (docs/rseg-v3-plan.md C3). No
/// reader exists yet (C4 adds one), so these tests parse the emitted bytes
/// directly, the same approach `v2_tests` takes -- including a test-only
/// HIST_SPANS record decoder mirroring the grammar in
/// docs/rseg-v3-plan.md section 3.5. This is not "reader changes" (the
/// ticket's scope line): it is test-only code that never ships, built
/// purely to check the writer's own byte-exact output.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod v3_tests {
    use std::collections::HashSet;

    use proptest::prelude::*;
    use ravel_types::{Label, Sample};

    use super::*;
    use crate::histogram::{HistogramCounts, HistogramSpan, ResetHint};
    use crate::varint::{read_uvarint, read_zigzag_varint};

    fn test_identity() -> SegmentIdentity {
        SegmentIdentity {
            tenant_hash: [0x5A; 16],
            shard: 4,
            writer_id: "v3-test-writer".to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn test_bounds() -> IngestBounds {
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 1_000,
        }
    }

    fn decode_footer(object: &[u8]) -> Footer {
        let n = object.len();
        assert!(n >= 16, "object smaller than the trailer");
        let trailer = &object[n - 16..];
        let footer_len = u32::from_le_bytes(trailer[0..4].try_into().expect("4 bytes")) as usize;
        let version = u16::from_le_bytes(trailer[8..10].try_into().expect("2 bytes"));
        assert_eq!(version, VERSION_V3, "test helper only decodes v3 objects");
        assert_eq!(&trailer[12..16], &MAGIC, "bad magic");
        let footer_start = n - 16 - footer_len;
        let footer_bytes = &object[footer_start..n - 16];
        <Footer as prost::Message>::decode(footer_bytes).expect("footer decodes")
    }

    fn section<'a>(object: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
        let s = footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("missing section kind {kind}"));
        let start = s.offset as usize;
        let end = start + s.len as usize;
        &object[start..end]
    }

    fn section_desc(footer: &Footer, kind: u32) -> Option<&Section> {
        footer.sections.iter().find(|s| s.kind == kind)
    }

    fn decompress_section(stored: &[u8], uncompressed_len: u64) -> Vec<u8> {
        zstd::bulk::decompress(stored, uncompressed_len as usize).expect("zstd decompresses")
    }

    struct ParsedSeriesMetaV3 {
        count: u32,
        sample_count: Vec<u32>,
        ts_page_gap: Vec<u64>,
        ts_page_len: Vec<u64>,
        val_page_gap: Vec<u64>,
        val_page_len: Vec<u64>,
        value_kind: Vec<u8>,
        hist_page_gap: Vec<u64>,
        hist_page_len: Vec<u64>,
    }

    fn read_block<'a>(raw: &'a [u8], pos: &mut usize) -> &'a [u8] {
        let block_len = read_uvarint(raw, pos).expect("block_len varint") as usize;
        let start = *pos;
        let end = start + block_len;
        let slice = &raw[start..end];
        *pos = end;
        slice
    }

    fn read_varints(block: &[u8], n: usize) -> Vec<u64> {
        let mut pos = 0usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(read_uvarint(block, &mut pos).expect("varint"));
        }
        assert_eq!(pos, block.len(), "trailing bytes in column block");
        out
    }

    /// Parses only the columns this test module needs (schema blocks 1-2
    /// are skipped past, not decoded, since no test here needs schema
    /// content).
    fn parse_series_meta_v3(raw: &[u8]) -> ParsedSeriesMetaV3 {
        let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes"));
        let schema_count = u32::from_le_bytes(raw[4..8].try_into().expect("4 bytes"));
        let mut pos = 8usize;
        for _ in 0..schema_count {
            let name_count = read_uvarint(raw, &mut pos).expect("varint") as usize;
            for _ in 0..name_count {
                read_uvarint(raw, &mut pos).expect("varint");
            }
        }

        let _schema_ref_block = read_block(raw, &mut pos);
        let _value_ord_block = read_block(raw, &mut pos);
        let sample_count_block = read_block(raw, &mut pos);
        let _min_ts_delta_block = read_block(raw, &mut pos);
        let _ts_span_block = read_block(raw, &mut pos);
        let ts_page_gap_block = read_block(raw, &mut pos);
        let ts_page_len_block = read_block(raw, &mut pos);
        let val_page_gap_block = read_block(raw, &mut pos);
        let val_page_len_block = read_block(raw, &mut pos);
        let value_kind_block = read_block(raw, &mut pos);
        let hist_page_gap_block = read_block(raw, &mut pos);
        let hist_page_len_block = read_block(raw, &mut pos);
        assert_eq!(pos, raw.len(), "trailing bytes in SERIES_META");

        let sample_count: Vec<u32> = read_varints(sample_count_block, count as usize)
            .into_iter()
            .map(|v| v as u32)
            .collect();
        let ts_page_gap = read_varints(ts_page_gap_block, count as usize);
        let ts_page_len = read_varints(ts_page_len_block, count as usize);
        let val_page_gap = read_varints(val_page_gap_block, count as usize);
        let val_page_len = read_varints(val_page_len_block, count as usize);
        assert_eq!(
            value_kind_block.len(),
            count as usize,
            "value_kind is one raw byte per series, not a varint column"
        );
        let value_kind = value_kind_block.to_vec();
        let hist_page_gap = read_varints(hist_page_gap_block, count as usize);
        let hist_page_len = read_varints(hist_page_len_block, count as usize);

        ParsedSeriesMetaV3 {
            count,
            sample_count,
            ts_page_gap,
            ts_page_len,
            val_page_gap,
            val_page_len,
            value_kind,
            hist_page_gap,
            hist_page_len,
        }
    }

    fn parse_series_ids(raw: &[u8]) -> Vec<[u8; 16]> {
        let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes")) as usize;
        assert_eq!(raw.len(), 4 + count * 16, "SERIES_IDS length mismatch");
        (0..count)
            .map(|i| {
                let start = 4 + i * 16;
                raw[start..start + 16].try_into().expect("16 bytes")
            })
            .collect()
    }

    fn split_and_verify_page<'a>(series_id: &SeriesId, page: &'a [u8]) -> (u8, u8, &'a [u8]) {
        assert!(page.len() >= 6, "page shorter than its header");
        let enc = page[0];
        let comp = page[1];
        let crc = u32::from_le_bytes(page[2..6].try_into().expect("4 bytes"));
        let payload = &page[6..];
        assert_eq!(
            page_crc(&series_id.0, enc, comp, payload),
            crc,
            "page crc mismatch"
        );
        (enc, comp, payload)
    }

    // --- test-only HIST_SPANS record decoder (docs/rseg-v3-plan.md
    // section 3.5), mirroring `encode_histogram_record_into`'s grammar
    // exactly, field for field, so a mismatch here catches a real writer
    // bug rather than two independently-wrong implementations agreeing. ---

    fn decode_spans(raw: &[u8], pos: &mut usize) -> (Vec<HistogramSpan>, usize) {
        let span_count = read_uvarint(raw, pos).expect("span_count varint") as usize;
        let mut spans = Vec::with_capacity(span_count);
        let mut total_len = 0usize;
        for _ in 0..span_count {
            let offset = read_zigzag_varint(raw, pos).expect("offset varint") as i32;
            let length = read_uvarint(raw, pos).expect("length varint") as u32;
            assert!(length > 0, "decoded span length must be > 0");
            total_len += length as usize;
            spans.push(HistogramSpan { offset, length });
        }
        (spans, total_len)
    }

    fn decode_int_counts(raw: &[u8], pos: &mut usize, n: usize) -> Vec<u64> {
        (0..n)
            .map(|_| read_uvarint(raw, pos).expect("bucket count varint"))
            .collect()
    }

    fn decode_float_counts(raw: &[u8], pos: &mut usize, n: usize) -> Vec<f64> {
        (0..n)
            .map(|_| {
                let v = f64::from_le_bytes(raw[*pos..*pos + 8].try_into().expect("8 bytes"));
                *pos += 8;
                v
            })
            .collect()
    }

    fn decode_histogram_record(raw: &[u8], pos: &mut usize) -> HistogramValue {
        let flags = raw[*pos];
        *pos += 1;
        let count_kind = flags & 0b1;
        let has_sum = (flags >> 1) & 0b1 == 1;
        let reset_bits = (flags >> 2) & 0b11;
        assert_eq!(flags >> 4, 0, "reserved flag bits must be 0");
        let reset_hint = match reset_bits {
            0 => ResetHint::Unknown,
            1 => ResetHint::Yes,
            2 => ResetHint::No,
            _ => ResetHint::Gauge,
        };

        let scale =
            i32::try_from(read_zigzag_varint(raw, pos).expect("scale varint")).expect("scale i32");
        let zero_threshold = f64::from_le_bytes(raw[*pos..*pos + 8].try_into().expect("8 bytes"));
        *pos += 8;

        let (zero_count_u64, count_u64, zero_count_f64, count_f64);
        if count_kind == 0 {
            zero_count_u64 = read_uvarint(raw, pos).expect("zero_count varint");
            count_u64 = read_uvarint(raw, pos).expect("count varint");
            zero_count_f64 = 0.0;
            count_f64 = 0.0;
        } else {
            zero_count_u64 = 0;
            count_u64 = 0;
            zero_count_f64 = f64::from_le_bytes(raw[*pos..*pos + 8].try_into().expect("8 bytes"));
            *pos += 8;
            count_f64 = f64::from_le_bytes(raw[*pos..*pos + 8].try_into().expect("8 bytes"));
            *pos += 8;
        }

        let sum = if has_sum {
            let v = f64::from_le_bytes(raw[*pos..*pos + 8].try_into().expect("8 bytes"));
            *pos += 8;
            Some(v)
        } else {
            None
        };

        let custom_values = if scale == -53 {
            let n = read_uvarint(raw, pos).expect("custom_values_count varint") as usize;
            let mut bounds = Vec::with_capacity(n);
            for _ in 0..n {
                bounds.push(f64::from_le_bytes(
                    raw[*pos..*pos + 8].try_into().expect("8 bytes"),
                ));
                *pos += 8;
            }
            Some(bounds)
        } else {
            None
        };

        let (positive_spans, positive_len) = decode_spans(raw, pos);
        let (counts, negative_spans) = if count_kind == 0 {
            let positive = decode_int_counts(raw, pos, positive_len);
            let (neg_spans, neg_len) = decode_spans(raw, pos);
            let negative = decode_int_counts(raw, pos, neg_len);
            (
                HistogramCounts::Int {
                    zero_count: zero_count_u64,
                    count: count_u64,
                    positive,
                    negative,
                },
                neg_spans,
            )
        } else {
            let positive = decode_float_counts(raw, pos, positive_len);
            let (neg_spans, neg_len) = decode_spans(raw, pos);
            let negative = decode_float_counts(raw, pos, neg_len);
            (
                HistogramCounts::Float {
                    zero_count: zero_count_f64,
                    count: count_f64,
                    positive,
                    negative,
                },
                neg_spans,
            )
        };

        HistogramValue {
            scale,
            zero_threshold,
            sum,
            custom_values,
            positive_spans,
            negative_spans,
            counts,
            reset_hint,
        }
    }

    /// Bit-exact comparison (CLAUDE.md: float comparisons in storage/dedup
    /// paths use bit patterns, never `==`; NaN and -0.0 are significant).
    fn assert_histogram_value_bit_exact(expected: &HistogramValue, actual: &HistogramValue) {
        assert_eq!(expected.scale, actual.scale);
        assert_eq!(
            expected.zero_threshold.to_bits(),
            actual.zero_threshold.to_bits()
        );
        assert_eq!(expected.sum.map(f64::to_bits), actual.sum.map(f64::to_bits));
        assert_eq!(
            expected
                .custom_values
                .as_ref()
                .map(|v| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>()),
            actual
                .custom_values
                .as_ref()
                .map(|v| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>())
        );
        assert_eq!(expected.positive_spans, actual.positive_spans);
        assert_eq!(expected.negative_spans, actual.negative_spans);
        assert_eq!(expected.reset_hint as u8, actual.reset_hint as u8);
        match (&expected.counts, &actual.counts) {
            (
                HistogramCounts::Int {
                    zero_count: ez,
                    count: ec,
                    positive: ep,
                    negative: en,
                },
                HistogramCounts::Int {
                    zero_count: az,
                    count: ac,
                    positive: ap,
                    negative: an,
                },
            ) => {
                assert_eq!(ez, az);
                assert_eq!(ec, ac);
                assert_eq!(ep, ap);
                assert_eq!(en, an);
            }
            (
                HistogramCounts::Float {
                    zero_count: ez,
                    count: ec,
                    positive: ep,
                    negative: en,
                },
                HistogramCounts::Float {
                    zero_count: az,
                    count: ac,
                    positive: ap,
                    negative: an,
                },
            ) => {
                assert_eq!(ez.to_bits(), az.to_bits());
                assert_eq!(ec.to_bits(), ac.to_bits());
                assert_eq!(
                    ep.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                    ap.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
                );
                assert_eq!(
                    en.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                    an.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
                );
            }
            _ => panic!("count_kind mismatch between expected and decoded histogram value"),
        }
    }

    // --- conditional VAL_PAGES/HIST_PAGES section presence (section 3.2)
    // --- the core new v3 behavior this ticket adds. ---

    #[test]
    fn empty_segment_omits_val_and_hist_sections() {
        let written =
            SegmentWriter::write_v3(Vec::new(), test_identity(), test_bounds()).expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object);

        assert_eq!(footer.series_count, 0);
        assert_eq!(footer.sample_count, 0);
        assert!(section_desc(&footer, section_kind::VAL_PAGES).is_none());
        assert!(section_desc(&footer, section_kind::HIST_PAGES).is_none());
        assert!(section_desc(&footer, section_kind::LABEL_DICT).is_some());
        assert!(section_desc(&footer, section_kind::SERIES_IDS).is_some());
        assert!(section_desc(&footer, section_kind::SERIES_META).is_some());
        assert!(section_desc(&footer, section_kind::TS_PAGES).is_some());
    }

    #[test]
    fn scalar_only_segment_omits_hist_pages_section() {
        let series = vec![SeriesInputV3 {
            series_id: SeriesId([0x01; 16]),
            labels: LabelSet::new(vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: "scalar_metric".to_string(),
            }])
            .expect("valid labels"),
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: 10,
                value: 1.5,
            }]),
        }];
        let written =
            SegmentWriter::write_v3(series, test_identity(), test_bounds()).expect("writes");
        let footer = decode_footer(written.bytes.as_ref());
        assert!(section_desc(&footer, section_kind::VAL_PAGES).is_some());
        assert!(section_desc(&footer, section_kind::HIST_PAGES).is_none());
    }

    #[test]
    fn histogram_only_segment_omits_val_pages_section() {
        let series = vec![SeriesInputV3 {
            series_id: SeriesId([0x02; 16]),
            labels: LabelSet::new(vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: "hist_metric".to_string(),
            }])
            .expect("valid labels"),
            values: SeriesValues::Histogram(vec![HistogramSample {
                ts_ns: 10,
                value: HistogramValue {
                    scale: 3,
                    zero_threshold: 0.001,
                    sum: Some(12.5),
                    custom_values: None,
                    positive_spans: vec![HistogramSpan {
                        offset: 0,
                        length: 2,
                    }],
                    negative_spans: vec![],
                    counts: HistogramCounts::Int {
                        zero_count: 1,
                        count: 4,
                        positive: vec![2, 1],
                        negative: vec![],
                    },
                    reset_hint: ResetHint::Unknown,
                },
            }]),
        }];
        let written =
            SegmentWriter::write_v3(series, test_identity(), test_bounds()).expect("writes");
        let footer = decode_footer(written.bytes.as_ref());
        assert!(section_desc(&footer, section_kind::VAL_PAGES).is_none());
        assert!(section_desc(&footer, section_kind::HIST_PAGES).is_some());
    }

    // --- proptest generators ---

    fn label_name_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("app".to_string()),
            Just("region".to_string()),
            Just("zone".to_string()),
            Just("instance".to_string()),
            Just("method".to_string()),
        ]
    }

    fn labelset_strategy() -> impl Strategy<Value = LabelSet> {
        (
            "[a-z_]{1,10}",
            prop::collection::vec((label_name_strategy(), "[a-zA-Z0-9_]{0,8}"), 0..4),
        )
            .prop_map(|(metric_name, extra)| {
                let mut ls = vec![Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: metric_name,
                }];
                let mut seen = HashSet::new();
                seen.insert(METRIC_NAME_LABEL.to_string());
                for (name, value) in extra {
                    if seen.insert(name.clone()) {
                        ls.push(Label { name, value });
                    }
                }
                LabelSet::new(ls).expect("no duplicate names by construction")
            })
    }

    fn ts_strategy() -> impl Strategy<Value = i64> {
        prop_oneof![
            3 => 0i64..300,
            1 => -1_000_000_000_000i64..=1_000_000_000_000i64,
        ]
    }

    fn sample_value_strategy() -> impl Strategy<Value = f64> {
        prop_oneof![
            10 => any::<f64>(),
            1 => Just(f64::NAN),
            1 => Just(-f64::NAN),
            1 => Just(f64::INFINITY),
            1 => Just(f64::NEG_INFINITY),
            1 => Just(0.0f64),
            1 => Just(-0.0f64),
        ]
    }

    fn scalar_samples_strategy() -> impl Strategy<Value = Vec<Sample>> {
        prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..12).prop_map(|pairs| {
            pairs
                .into_iter()
                .map(|(ts_ns, value)| Sample { ts_ns, value })
                .collect()
        })
    }

    fn bounded_f64_strategy() -> impl Strategy<Value = f64> {
        -1_000_000.0f64..1_000_000.0f64
    }

    fn reset_hint_strategy() -> impl Strategy<Value = ResetHint> {
        prop_oneof![
            Just(ResetHint::Unknown),
            Just(ResetHint::Yes),
            Just(ResetHint::No),
            Just(ResetHint::Gauge),
        ]
    }

    fn span_strategy() -> impl Strategy<Value = HistogramSpan> {
        (-8i32..8, 1u32..6).prop_map(|(offset, length)| HistogramSpan { offset, length })
    }

    fn spans_strategy() -> impl Strategy<Value = Vec<HistogramSpan>> {
        prop::collection::vec(span_strategy(), 0..4)
    }

    fn scale_strategy() -> impl Strategy<Value = i32> {
        prop_oneof![
            5 => -4i32..8,
            1 => Just(-53i32),
        ]
    }

    /// Generates a structurally valid `HistogramValue`: span lengths and
    /// bucket-count vector lengths always agree, `custom_values` presence
    /// always matches `scale == -53` with strictly ascending boundaries --
    /// every constraint `encode_histogram_record_into` enforces. Bucket
    /// count *contents* are derived deterministically from position
    /// (not independently randomized) to keep the strategy simple; this
    /// suite tests structural round-tripping, not count-value coverage
    /// (that belongs to C5's fuzz/property hardening).
    fn histogram_value_strategy() -> impl Strategy<Value = HistogramValue> {
        (
            scale_strategy(),
            spans_strategy(),
            spans_strategy(),
            any::<bool>(),
            prop::option::of(bounded_f64_strategy()),
            reset_hint_strategy(),
            bounded_f64_strategy(),
            prop::collection::vec(bounded_f64_strategy(), 0..6),
        )
            .prop_map(
                |(
                    scale,
                    positive_spans,
                    negative_spans,
                    is_float,
                    sum,
                    reset_hint,
                    zero_threshold,
                    custom_seed,
                )| {
                    let positive_len: usize =
                        positive_spans.iter().map(|s| s.length as usize).sum();
                    let negative_len: usize =
                        negative_spans.iter().map(|s| s.length as usize).sum();

                    let custom_values = if scale == -53 {
                        let mut bounds = if custom_seed.is_empty() {
                            vec![0.0]
                        } else {
                            custom_seed
                        };
                        bounds.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
                        for i in 1..bounds.len() {
                            if bounds[i] <= bounds[i - 1] {
                                bounds[i] = bounds[i - 1] + 1.0;
                            }
                        }
                        Some(bounds)
                    } else {
                        None
                    };

                    let counts = if is_float {
                        HistogramCounts::Float {
                            zero_count: 3.0,
                            count: (positive_len + negative_len) as f64 + 3.0,
                            positive: (0..positive_len).map(|i| i as f64 + 1.0).collect(),
                            negative: (0..negative_len).map(|i| i as f64 + 1.0).collect(),
                        }
                    } else {
                        HistogramCounts::Int {
                            zero_count: 3,
                            count: (positive_len + negative_len) as u64 + 3,
                            positive: (0..positive_len).map(|i| i as u64 + 1).collect(),
                            negative: (0..negative_len).map(|i| i as u64 + 1).collect(),
                        }
                    };

                    HistogramValue {
                        scale,
                        zero_threshold,
                        sum,
                        custom_values,
                        positive_spans,
                        negative_spans,
                        counts,
                        reset_hint,
                    }
                },
            )
    }

    fn histogram_samples_strategy() -> impl Strategy<Value = Vec<HistogramSample>> {
        prop::collection::vec((ts_strategy(), histogram_value_strategy()), 1..6).prop_map(
            |entries| {
                entries
                    .into_iter()
                    .map(|(ts_ns, value)| HistogramSample { ts_ns, value })
                    .collect()
            },
        )
    }

    fn series_values_strategy() -> impl Strategy<Value = SeriesValues> {
        prop_oneof![
            3 => scalar_samples_strategy().prop_map(SeriesValues::Scalar),
            3 => histogram_samples_strategy().prop_map(SeriesValues::Histogram),
        ]
    }

    fn series_batch_strategy() -> impl Strategy<Value = Vec<SeriesInputV3>> {
        prop::collection::vec((labelset_strategy(), series_values_strategy()), 0..8).prop_map(
            |entries| {
                entries
                    .into_iter()
                    .enumerate()
                    .map(|(idx, (labels, values))| {
                        let mut id = [0u8; 16];
                        id[..8].copy_from_slice(&(idx as u64).to_be_bytes());
                        SeriesInputV3 {
                            series_id: SeriesId(id),
                            labels,
                            values,
                        }
                    })
                    .collect()
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Internal-consistency property: the writer's declared per-series
        /// metadata (value_kind, sample counts, ts bounds, reconstructed
        /// TS/VAL/HIST page ranges from the gap/len columns) is
        /// self-consistent with what was actually written, and every
        /// HIST_SPANS record decodes back to bit-exact the same value that
        /// was written.
        #[test]
        fn series_meta_v3_matches_written_pages(series in series_batch_strategy()) {
            let expected_values: Vec<SeriesValues> = {
                let mut sorted = series.iter()
                    .filter(|s| !s.values.is_empty())
                    .map(|s| {
                        let mut values = s.values.clone();
                        values.sort_by_ts();
                        (s.series_id.0, values)
                    })
                    .collect::<Vec<_>>();
                sorted.sort_by_key(|(id, _)| *id);
                sorted.into_iter().map(|(_, v)| v).collect()
            };

            let written = SegmentWriter::write_v3(series, test_identity(), test_bounds())
                .expect("writes");
            let object = written.bytes.as_ref();
            let footer = decode_footer(object);

            let ids = parse_series_ids(section(object, &footer, section_kind::SERIES_IDS));
            prop_assert_eq!(ids.len() as u64, footer.series_count);
            prop_assert_eq!(ids.len(), expected_values.len());
            for w in ids.windows(2) {
                prop_assert!(w[0] < w[1], "SERIES_IDS must be strictly ascending");
            }

            let meta_desc = section_desc(&footer, section_kind::SERIES_META)
                .expect("SERIES_META is always mandatory");
            let meta_raw = decompress_section(
                section(object, &footer, section_kind::SERIES_META),
                meta_desc.uncompressed_len,
            );
            let meta = parse_series_meta_v3(&meta_raw);
            prop_assert_eq!(u64::from(meta.count), footer.series_count);

            let ts_bytes = section(object, &footer, section_kind::TS_PAGES);
            let val_bytes = section_desc(&footer, section_kind::VAL_PAGES)
                .map(|_| section(object, &footer, section_kind::VAL_PAGES))
                .unwrap_or(&[]);
            let hist_bytes = section_desc(&footer, section_kind::HIST_PAGES)
                .map(|_| section(object, &footer, section_kind::HIST_PAGES))
                .unwrap_or(&[]);

            let mut ts_running = 0u64;
            let mut val_running = 0u64;
            let mut hist_running = 0u64;
            for (i, &id) in ids.iter().enumerate() {
                let series_id = SeriesId(id);

                let ts_offset = ts_running + meta.ts_page_gap[i];
                let ts_len = meta.ts_page_len[i];
                let ts_end = ts_offset + ts_len;
                prop_assert!(ts_end <= ts_bytes.len() as u64);
                let (ts_enc, _ts_comp, _ts_payload) = split_and_verify_page(
                    &series_id,
                    &ts_bytes[ts_offset as usize..ts_end as usize],
                );
                prop_assert_eq!(ts_enc, page_enc::TS_DELTA_VARINT);
                ts_running = ts_end;

                match &expected_values[i] {
                    SeriesValues::Scalar(_) => {
                        prop_assert_eq!(meta.value_kind[i], 0);
                        prop_assert_eq!(meta.hist_page_gap[i], 0);
                        prop_assert_eq!(meta.hist_page_len[i], 0);

                        let val_offset = val_running + meta.val_page_gap[i];
                        let val_len = meta.val_page_len[i];
                        let val_end = val_offset + val_len;
                        prop_assert!(val_end <= val_bytes.len() as u64);
                        let (val_enc, val_comp, val_payload) = split_and_verify_page(
                            &series_id,
                            &val_bytes[val_offset as usize..val_end as usize],
                        );
                        prop_assert_eq!(val_comp, page_comp::NONE);
                        prop_assert!(
                            val_enc == page_enc::VAL_GORILLA || val_enc == page_enc::VAL_RAW_F64,
                            "unexpected VAL page enc byte {val_enc}"
                        );
                        let _ = val_payload;
                        val_running = val_end;
                    }
                    SeriesValues::Histogram(samples) => {
                        prop_assert_eq!(meta.value_kind[i], 1);
                        prop_assert_eq!(meta.val_page_gap[i], 0);
                        prop_assert_eq!(meta.val_page_len[i], 0);

                        let hist_offset = hist_running + meta.hist_page_gap[i];
                        let hist_len = meta.hist_page_len[i];
                        let hist_end = hist_offset + hist_len;
                        prop_assert!(hist_len > 0, "a histogram series must have a non-empty HIST page");
                        prop_assert!(hist_end <= hist_bytes.len() as u64);
                        let (hist_enc, hist_comp, hist_payload) = split_and_verify_page(
                            &series_id,
                            &hist_bytes[hist_offset as usize..hist_end as usize],
                        );
                        prop_assert_eq!(hist_enc, page_enc::HIST_SPANS);
                        prop_assert_eq!(hist_comp, page_comp::NONE);

                        let mut pos = 0usize;
                        for sample in samples {
                            let decoded = decode_histogram_record(hist_payload, &mut pos);
                            assert_histogram_value_bit_exact(&sample.value, &decoded);
                        }
                        prop_assert_eq!(
                            pos, hist_payload.len(),
                            "HIST page must decode exactly sample_count records with no trailing bytes"
                        );
                        prop_assert_eq!(samples.len() as u32, meta.sample_count[i]);

                        hist_running = hist_end;
                    }
                }
            }
            prop_assert_eq!(ts_running, ts_bytes.len() as u64);
            prop_assert_eq!(val_running, val_bytes.len() as u64);
            prop_assert_eq!(hist_running, hist_bytes.len() as u64);
        }
    }
}

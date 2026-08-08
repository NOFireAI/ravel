//! RSEG v5 writer: builds a complete segment object from per-series sample
//! batches or pre-framed compaction runs, per docs/segment-format.md.
//! ADR-0027 leaves v5 the only writable version; a raw-sample batch is
//! framed into single-run series and emitted through the same v5 path as the
//! compacted tier.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use bytes::Bytes;
use prost::Message;
use ravel_proto::segment::v1::{Footer, Section};
use ravel_types::{LabelSet, METRIC_NAME_LABEL, SeriesId};

use crate::crc::{footer_crc, page_crc};
use crate::error::WriteError;
use crate::exemplars::{ExemplarInput, ResolvedExemplar, encode_exemplars_section};
use crate::format::{
    MAGIC, RESERVED, SIGNAL_METRICS, V5_SPARSE_THRESHOLD, VERSION_V4, VERSION_V6, ZSTD_LEVEL,
    compression, page_comp, page_enc, section_kind,
};
use crate::gorilla::encode_gorilla_into;
use crate::histogram::{HistogramValue, encode_histogram_record_into};
use crate::ts_delta::encode_ts_deltas_into;
use crate::varint::write_uvarint;

/// Nanoseconds per hour, for deriving an L0 flush's `ingest_hour_bucket`.
const NS_PER_HOUR: i64 = 3_600_000_000_000;

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

    /// Appends this series' timestamps to `out` without allocating: `out` is
    /// caller-owned scratch, cleared and reused across series within one
    /// flush (issue #813).
    fn extend_ts_values_into(&self, out: &mut Vec<i64>) {
        out.clear();
        match self {
            SeriesValues::Scalar(v) => out.extend(v.iter().map(|s| s.ts_ns)),
            SeriesValues::Histogram(v) => out.extend(v.iter().map(|s| s.ts_ns)),
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

/// Builds RSEG v5 metric segment objects. Stateless. ADR-0027 makes v5 the
/// only writable version: [`SegmentWriter::write_v5`] is the single encode
/// surface, and [`SegmentWriter::write`] / [`SegmentWriter::write_histograms`]
/// are thin raw-sample adapters that frame one run per series and delegate to
/// it, so an L0 flush emits the same grammar as the compacted tier.
pub struct SegmentWriter;

impl SegmentWriter {
    /// Encodes a batch of scalar series into one RSEG v5 segment object.
    ///
    /// Raw-sample adapter over [`SegmentWriter::write_histograms`] (and thus
    /// [`SegmentWriter::write_v5`]): each series becomes a single run.
    /// Series with zero samples are dropped; a segment with no series at all
    /// is a valid, empty object; duplicate `series_id`s are rejected.
    pub fn write(
        series: Vec<SeriesInput>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
    ) -> Result<WrittenSegment, WriteError> {
        let series = series
            .into_iter()
            .map(|s| SeriesInputV3 {
                series_id: s.series_id,
                labels: s.labels,
                values: SeriesValues::Scalar(s.samples),
            })
            .collect();
        Self::write_histograms(series, identity, ingest_bounds)
    }

    /// Encodes a batch of scalar and/or histogram series into one RSEG v5
    /// segment object (ADR-0027). The raw-sample adapter the ingest flush
    /// path uses: each series is framed into exactly one run and handed to
    /// [`SegmentWriter::write_v5`]. An L0 flush is not a compaction output, so
    /// run provenance is derived from the ingest bounds (`created_unix_ns` =
    /// `max_ingest_ts_ns`) and the Footer's compaction-provenance fields carry
    /// L0 sentinels (`level` = 0, `part_index` = 0, `input_set_hash` =
    /// all-zero). Below the sparse threshold the object is byte-identical to
    /// the v4-grammar object it would be, save the trailer version.
    ///
    /// Series with zero samples are dropped; duplicate `series_id`s and
    /// over-large label sets are rejected by [`SegmentWriter::write_v5`].
    pub fn write_histograms(
        series: Vec<SeriesInputV3>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
    ) -> Result<WrittenSegment, WriteError> {
        Self::write_histograms_with_exemplars(series, identity, ingest_bounds, Vec::new())
    }

    /// Same as [`SegmentWriter::write_histograms`], additionally emitting the
    /// EXEMPLARS section (kind 10, ADR-0047) when `exemplars` is non-empty.
    /// This is the flush-path entry point for an ingest shard that buffered
    /// exemplars alongside its samples (issue #474): exemplars are independent
    /// of run/sample framing, so the adapter passes them straight through to
    /// [`SegmentWriter::write_v5_with_exemplars`].
    ///
    /// Every [`ExemplarInput::series_id`] must name a series that survives
    /// into the object, or the write fails with
    /// `WriteError::ExemplarUnknownSeries`. Zero-sample series are dropped
    /// here (as in `write_histograms`), so a caller that buffers exemplars
    /// must offer only exemplars whose parent samples are in this same batch
    /// (docs/segment-format.md "Writer edge rules").
    ///
    /// An empty `exemplars` emits no EXEMPLARS section at all rather than a
    /// zero-count one: absence is the only legal representation of "no
    /// exemplars" (ADR-0047 decision 1).
    pub fn write_histograms_with_exemplars(
        mut series: Vec<SeriesInputV3>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        exemplars: Vec<ExemplarInput>,
    ) -> Result<WrittenSegment, WriteError> {
        for s in &mut series {
            s.values.sort_by_ts();
        }
        series.retain(|s| !s.values.is_empty());

        // An L0 flush's runs share one provenance stamp. `write_v5` re-sorts
        // by id and rejects duplicates, so no pre-sort is needed here.
        let created_unix_ns = ingest_bounds.max_ingest_ts_ns;
        let ingest_hour_bucket =
            u32::try_from(created_unix_ns.div_euclid(NS_PER_HOUR)).unwrap_or(0);

        let mut v4_series = Vec::with_capacity(series.len());
        let mut scratch = WriteScratch::default();
        for s in series {
            let sample_count =
                u32::try_from(s.values.len()).map_err(|_| WriteError::TooManySamples)?;
            let min_ts_ns = s.values.first_ts().unwrap_or(0);
            let max_ts_ns = s.values.last_ts().unwrap_or(0);
            s.values.extend_ts_values_into(&mut scratch.ts_values);
            let ts_page = frame_ts_page(&s.series_id, &scratch.ts_values, &mut scratch.payload)?;
            let value_page = match &s.values {
                SeriesValues::Scalar(samples) => {
                    scratch.scalar_values.clear();
                    scratch
                        .scalar_values
                        .extend(samples.iter().map(|sm| sm.value));
                    RunValuePageV4::Scalar(frame_val_page(
                        &s.series_id,
                        &scratch.scalar_values,
                        &mut scratch.payload,
                    ))
                }
                SeriesValues::Histogram(hist) => RunValuePageV4::Histogram(frame_hist_page(
                    &s.series_id,
                    hist,
                    &mut scratch.payload,
                )?),
            };
            v4_series.push(SeriesInputV4 {
                series_id: s.series_id,
                labels: s.labels,
                runs: vec![RunInputV4 {
                    created_unix_ns,
                    writer_epoch: identity.writer_epoch,
                    writer_seq: identity.writer_seq,
                    min_ts_ns,
                    max_ts_ns,
                    sample_count,
                    ts_page,
                    value_page,
                }],
            });
        }

        let meta = CompactionMetaV4 {
            ingest_hour_bucket,
            input_set_hash: [0u8; 32],
            part_index: 0,
            level: 0,
        };
        Self::write_v5_with_exemplars(v4_series, identity, ingest_bounds, meta, exemplars)
    }

    /// Builds the v4-grammar object (multi-run, run-major SERIES_META, the
    /// compaction-provenance Footer fields) and finalizes it with a v4
    /// trailer, private since ADR-0027: it is the encode core
    /// [`SegmentWriter::write_v5`] wraps, no longer a public version of its
    /// own. Every run's TS/VAL/HIST page bytes are pre-framed by the caller
    /// and copied verbatim, including histogram pages, which stay an opaque
    /// per-run blob to this writer.
    ///
    /// Runs with `sample_count == 0` are dropped; a series left with no
    /// runs afterward is dropped in turn (mirrors the empty-series rule of
    /// the raw-sample adapters, generalized to run granularity).
    ///
    /// Test-only (issue #813): production now reaches the v4 grammar through
    /// `assemble_v4_body` directly (see
    /// [`SegmentWriter::write_v5_with_exemplars`]), which finalizes as v4 for
    /// the sparse path's `base` input and as v6 directly otherwise. This
    /// wrapper survives only so the v4-grammar unit tests below keep
    /// exercising the assemble+finalize pair through one call, unchanged
    /// from before the split.
    #[cfg(test)]
    fn write_v4(
        series: Vec<SeriesInputV4>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        meta: CompactionMetaV4,
        exemplars: Vec<ExemplarInput>,
    ) -> Result<WrittenSegment, WriteError> {
        let body = assemble_v4_body(series, identity, ingest_bounds, meta, exemplars)?;
        Ok(finalize_v4_trailer(body, VERSION_V4))
    }
}

/// The v4-grammar object body -- every section byte plus the encoded Footer
/// -- built but not yet trailer-finalized. Splitting assembly from
/// finalization lets [`SegmentWriter::write_v5_with_exemplars`] pick the
/// output trailer version (6 below the sparse threshold, 4 as the sparse
/// path's rebuild input) without assembling the body twice or computing
/// blake3 more than once for the version it actually emits (issue #813).
struct AssembledV4Body {
    object: Vec<u8>,
    footer_bytes: Vec<u8>,
    footer_len: u32,
    min_event_ts_ns: i64,
    max_event_ts_ns: i64,
    sample_count: u64,
    series_count: u64,
}

/// Builds every section byte and the encoded Footer for the v4 grammar,
/// stopping short of the trailer so the caller can finalize with whichever
/// version applies. See [`SegmentWriter::write_v4`] for the grammar this
/// assembles (moved out of that method, unchanged, so `write_v4` and
/// [`SegmentWriter::write_v5_with_exemplars`] share one assembly path).
fn assemble_v4_body(
    mut series: Vec<SeriesInputV4>,
    identity: SegmentIdentity,
    ingest_bounds: IngestBounds,
    meta: CompactionMetaV4,
    exemplars: Vec<ExemplarInput>,
) -> Result<AssembledV4Body, WriteError> {
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
        let series_run_count = u32::try_from(s.runs.len()).map_err(|_| WriteError::TooManyRuns)?;
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

    let dict = build_dictionary_v4(&series, &exemplars)?;

    let mut series_index_by_id: HashMap<[u8; 16], u32> = HashMap::with_capacity(series.len());
    for (i, s) in series.iter().enumerate() {
        series_index_by_id.insert(s.series_id.0, i as u32);
    }
    let mut resolved_exemplars = Vec::with_capacity(exemplars.len());
    let mut exemplar_attr_cursor = 0usize;
    for e in &exemplars {
        let series_index = *series_index_by_id
            .get(&e.series_id.0)
            .ok_or(WriteError::ExemplarUnknownSeries)?;
        let mut attr_ords = Vec::with_capacity(e.attrs.len());
        for _ in &e.attrs {
            let name_ord = dict.exemplar_attr_ordinals[exemplar_attr_cursor];
            let value_ord = dict.exemplar_attr_ordinals[exemplar_attr_cursor + 1];
            exemplar_attr_cursor += 2;
            attr_ords.push((name_ord, value_ord));
        }
        resolved_exemplars.push(ResolvedExemplar {
            series_index,
            ts_ns: e.ts_ns,
            value: e.value,
            trace_id: e.trace_id,
            span_id: e.span_id,
            attr_ords,
        });
    }
    // Sort by (series_index, ts_ns) per the EXEMPLARS grammar. Equal keys
    // are kept, not collapsed: compaction is a verbatim copy that never
    // drops a record (crates/ravel-maintain/src/publish.rs), so two inputs
    // each carrying an exemplar for the same series at the same timestamp
    // must both reach the output. The reader accepts equal keys for this
    // reason (ADR-0047 amendment 2026-08-03). `sort_by_key` is stable, so
    // equal keys keep the caller's original order, which makes the encoded
    // bytes a function of the input order alone. Admission-time capping is
    // ADR-0047 decision 2 and happens earlier, on a different layer.
    resolved_exemplars.sort_by_key(|r| (r.series_index, r.ts_ns));

    let total_samples = usize::try_from(sample_count).map_err(|_| WriteError::TooManySamples)?;
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

    // EXEMPLARS (kind 10, ADR-0047), RSEG v6 only: present only when at
    // least one sample carried an exemplar (docs/segment-format.md).
    // Physical section order 1, 5, 6, 3, 4, 7, 10.
    if !resolved_exemplars.is_empty() {
        let exemplars_raw = encode_exemplars_section(&resolved_exemplars, min_event_ts_ns)?;
        let exemplars_offset = object.len() as u64;
        object.extend_from_slice(&exemplars_raw);
        sections.push(Section {
            kind: section_kind::EXEMPLARS,
            offset: exemplars_offset,
            len: exemplars_raw.len() as u64,
            crc32c: crc32c::crc32c(&exemplars_raw),
            comp: compression::NONE,
            uncompressed_len: exemplars_raw.len() as u64,
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
    let footer_len = u32::try_from(footer_bytes.len()).map_err(|_| WriteError::FooterTooLarge)?;
    object.extend_from_slice(&footer_bytes);

    Ok(AssembledV4Body {
        object,
        footer_bytes,
        footer_len,
        min_event_ts_ns,
        max_event_ts_ns,
        sample_count,
        series_count,
    })
}

/// Appends the trailer for `version` to an assembled body and computes the
/// whole-object blake3 exactly once: the single finalization step for both
/// `write_v4` (always version 4) and the direct-emit non-sparse path in
/// [`SegmentWriter::write_v5_with_exemplars`] (version 6, no retrailer).
fn finalize_v4_trailer(body: AssembledV4Body, version: u16) -> WrittenSegment {
    let AssembledV4Body {
        mut object,
        footer_bytes,
        footer_len,
        min_event_ts_ns,
        max_event_ts_ns,
        sample_count,
        series_count,
    } = body;

    let crc = footer_crc(&footer_bytes, footer_len, version, SIGNAL_METRICS, RESERVED);

    object.extend_from_slice(&footer_len.to_le_bytes());
    object.extend_from_slice(&crc.to_le_bytes());
    object.extend_from_slice(&version.to_le_bytes());
    object.push(SIGNAL_METRICS);
    object.push(RESERVED);
    object.extend_from_slice(&MAGIC);

    let blake3 = *blake3::hash(&object).as_bytes();

    WrittenSegment {
        bytes: Bytes::from(object),
        summary: SegmentSummary {
            min_event_ts_ns,
            max_event_ts_ns,
            sample_count,
            series_count,
            blake3,
        },
    }
}

impl SegmentWriter {
    /// Encodes RSEG v5 (docs/segment-format.md): the v4 grammar plus, when the
    /// output object carries at least [`V5_SPARSE_THRESHOLD`] series, the
    /// sparse SERIES_IDX (kind 8) and chunked SERIES_META (kind 9) sections.
    /// Takes pre-framed [`SeriesInputV4`] runs; the raw-sample adapters
    /// ([`SegmentWriter::write`], [`SegmentWriter::write_histograms`]) frame
    /// their input into single-run series and delegate here, so every writer
    /// -- L0 flush included -- emits v5 (ADR-0027 decision 2, superseding
    /// ADR-0026's "L0 never emits v5" clause: the sparse-emission threshold,
    /// not the writer tier, protects small objects).
    ///
    /// Shares the private v4 assembly core (`assemble_v4_body`) rather than a
    /// bespoke encode path, so the v4 grammar stays a single source of truth.
    /// Below the threshold, the core is finalized with the version-6 trailer
    /// directly -- no intermediate v4 object, no retrailer copy, exactly one
    /// whole-object blake3 pass (issue #813; previously a `write_v4` call
    /// finalized as version 4 and a second pass, `retrailer_v4_to_v6`, copied
    /// the whole object again to rewrite the trailer as version 6). At or
    /// above the threshold, the core is finalized as version 4 (the shape
    /// [`crate::sparse::build_sparse_object`] expects as input) and the
    /// sparse sections are layered on from there, unchanged from before.
    pub fn write_v5(
        series: Vec<SeriesInputV4>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        meta: CompactionMetaV4,
    ) -> Result<WrittenSegment, WriteError> {
        Self::write_v5_with_exemplars(series, identity, ingest_bounds, meta, Vec::new())
    }

    /// Same as [`SegmentWriter::write_v5`], additionally emitting the
    /// EXEMPLARS section (kind 10, ADR-0047) when `exemplars` is non-empty.
    /// Each [`ExemplarInput::series_id`] must match a series in `series`
    /// (`WriteError::ExemplarUnknownSeries` otherwise); the batch is
    /// otherwise independent of run/sample framing.
    pub fn write_v5_with_exemplars(
        series: Vec<SeriesInputV4>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        meta: CompactionMetaV4,
        exemplars: Vec<ExemplarInput>,
    ) -> Result<WrittenSegment, WriteError> {
        let body = assemble_v4_body(series, identity, ingest_bounds, meta, exemplars)?;
        if body.series_count < V5_SPARSE_THRESHOLD {
            return Ok(finalize_v4_trailer(body, VERSION_V6));
        }
        let base = finalize_v4_trailer(body, VERSION_V4);
        crate::sparse::build_sparse_object(&base)
    }
}

/// Per-writer encode scratch reused across series within one flush (issue
/// #813): each buffer is cleared, not reallocated, between series, so a
/// flush of N series pays for growth once (amortized) instead of N fresh
/// heap allocations for values extracted from samples and for page payload
/// bytes.
#[derive(Default)]
struct WriteScratch {
    ts_values: Vec<i64>,
    scalar_values: Vec<f64>,
    payload: Vec<u8>,
}

/// Frames one series' TS page (6-byte header + payload) into a fresh buffer
/// for the raw-sample v5 adapters. TS_DELTA_VARINT payload, lz4-compressed
/// only when it clears the size floor and shrinks. The returned page is
/// copied verbatim (any alignment gap re-applied) by `append_ts_run_page_v4`.
/// `payload` is caller-owned scratch (cleared here, reused across series).
fn frame_ts_page(
    series_id: &SeriesId,
    ts_values: &[i64],
    payload: &mut Vec<u8>,
) -> Result<Vec<u8>, WriteError> {
    payload.clear();
    encode_ts_deltas_into(payload, ts_values).ok_or(WriteError::TimestampDeltaOverflow)?;
    let enc = page_enc::TS_DELTA_VARINT;
    let compressed = if payload.len() >= LZ4_MIN_TS_PAYLOAD_BYTES {
        let candidate = lz4_flex::compress_prepend_size(payload);
        (candidate.len() < payload.len()).then_some(candidate)
    } else {
        None
    };
    let (comp, body): (u8, &[u8]) = match &compressed {
        Some(candidate) => (page_comp::LZ4, candidate),
        None => (page_comp::NONE, payload.as_slice()),
    };
    Ok(frame_page(&series_id.0, enc, comp, body))
}

/// Frames one scalar series' VAL page into a fresh buffer: Gorilla unless it
/// fails to beat raw f64 (the raw-fallback rule). No alignment gap is applied
/// here; `append_val_run_page_v4` inserts the VAL_RAW_F64 pad when it copies
/// the page into the section. `payload` is caller-owned scratch (cleared
/// here, reused across series).
fn frame_val_page(series_id: &SeriesId, values: &[f64], payload: &mut Vec<u8>) -> Vec<u8> {
    payload.clear();
    encode_gorilla_into(values, payload);
    let count = values.len() as u64;
    let enc = if (payload.len() as u64) >= 8 * count {
        payload.clear();
        for v in values {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        page_enc::VAL_RAW_F64
    } else {
        page_enc::VAL_GORILLA
    };
    frame_page(&series_id.0, enc, page_comp::NONE, payload)
}

/// Frames one histogram series' HIST page into a fresh buffer: back-to-back
/// HIST_SPANS records, never per-page compressed (writer policy). Borrows
/// each sample's [`HistogramValue`] rather than cloning it (issue #813).
/// `payload` is caller-owned scratch (cleared here, reused across series).
fn frame_hist_page(
    series_id: &SeriesId,
    samples: &[HistogramSample],
    payload: &mut Vec<u8>,
) -> Result<Vec<u8>, WriteError> {
    payload.clear();
    for sample in samples {
        encode_histogram_record_into(payload, &sample.value)?;
    }
    Ok(frame_page(
        &series_id.0,
        page_enc::HIST_SPANS,
        page_comp::NONE,
        payload,
    ))
}

/// Assembles a framed page: enc(1) + comp(1) + crc32c(4) header then payload.
fn frame_page(series_id: &[u8; 16], enc: u8, comp: u8, payload: &[u8]) -> Vec<u8> {
    let crc = page_crc(series_id, enc, comp, payload);
    let mut page = Vec::with_capacity(6 + payload.len());
    page.push(enc);
    page.push(comp);
    page.extend_from_slice(&crc.to_le_bytes());
    page.extend_from_slice(payload);
    page
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

fn prefix_key(s: &str) -> u64 {
    let bytes = s.as_bytes();
    let mut key = [0u8; 8];
    let n = bytes.len().min(8);
    key[..n].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(key)
}

/// Writer policy, not format: timestamp payloads below this size skip the
/// lz4 attempt and are stored with `comp = 0`, which the page grammar
/// explicitly permits. lz4's per-call setup dominates such pages, and the
/// 4-byte size prefix plus framing means compression cannot win there.
const LZ4_MIN_TS_PAYLOAD_BYTES: usize = 64;

/// Sorts the distinct pre-rank strings and assigns sorted ordinals, `__name__`
/// pinned to 0, using the same big-endian 8-byte prefix key scheme v1's
/// `build_dictionary` uses (via the shared, version-agnostic `prefix_key`
/// sort-key helper). Returns the sorted non-`__name__` strings (in ordinal
/// order), a pre-rank-index-to-sorted-ordinal table, and the dictionary size
/// (including ordinal 0). Shared by the v2 and v3 dictionary builds; v1 keeps
/// its own inline copy so its path stays trivially provable as untouched by
/// inspection.
fn sort_and_rank_dict<'a>(
    distinct: &[&'a str],
) -> Result<(Vec<&'a str>, Vec<u32>, u32), WriteError> {
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

    let mut sorted_non_name: Vec<&str> = Vec::with_capacity(order.len());
    for &(_, id) in &order {
        let text = distinct[id as usize];
        if text != METRIC_NAME_LABEL {
            sorted_non_name.push(text);
        }
    }

    Ok((sorted_non_name, rank, count))
}

/// Interns one string into a v2/v3 dictionary interner, returning its
/// pre-rank distinct index (not the final ordinal; the sort-then-rank pass
/// assigns those). New strings are appended to `distinct` in first-occurrence
/// order. Shared by the v2 and v3 dictionary builds; deliberately a copy of
/// v1's `intern_distinct`, per the frozen-contract discipline that no v2/v3
/// code calls a v1 function.
#[inline]
fn intern_dict<'a>(
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

/// Identical grammar and sorted-order rule as v3's `DictionaryV3` (section
/// 4: "SERIES_IDS / LABEL_DICT ... as v2/v3"; issue #146 restored the sort in
/// v2/v3, issue #155 carries it here). `__name__` at ordinal 0, every other
/// distinct string in sorted (byte) order.
struct DictionaryV4<'a> {
    /// Distinct non-`__name__` strings in sorted order (ordinals 1..).
    order: Vec<&'a str>,
    occurrence_ordinals: Vec<u32>,
    /// Flattened `(name_ord, value_ord)` pairs for every EXEMPLARS attr, in
    /// the same exemplar-then-attr iteration order `write_v4` resolves
    /// exemplars in, so both sides can walk it with one shared cursor
    /// (ADR-0047; ordinals already resolved into the same sorted dictionary
    /// as series labels).
    exemplar_attr_ordinals: Vec<u32>,
    count: u32,
}

/// Interns every occurrence to a pre-rank distinct index in
/// series-then-label, then exemplar-then-attr iteration order, then assigns
/// sorted ordinals via the shared `sort_and_rank_dict` pass (`__name__`
/// pinned to 0), the same scheme as `build_dictionary_v2` / `build_dictionary_v3`.
/// LABEL_DICT is "as v2/v3" (section 4), so this inherits the issue #146 sort
/// (issue #155): v4 is the L1 compaction output, whose objects are larger and
/// longer-lived than L0 segments, so the compression win the sort buys is
/// worth more here. The order rule stays relaxed (readers locate strings by
/// ordinal), so this is a writer-side change: no version bump, no ADR.
/// EXEMPLARS attrs (ADR-0047) intern into this same dictionary rather than a
/// separate one, so a name/value repeated between a series label and an
/// exemplar attr (or between two exemplars) costs one dictionary entry, not
/// two.
fn build_dictionary_v4<'a>(
    series: &'a [SeriesInputV4],
    exemplars: &'a [ExemplarInput],
) -> Result<DictionaryV4<'a>, WriteError> {
    let series_strings: usize = series.iter().map(|s| s.labels.len() * 2).sum();
    let exemplar_strings: usize = exemplars.iter().map(|e| e.attrs.len() * 2).sum();
    let mut interner: HashMap<&str, u32, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(
            series_strings + exemplar_strings,
            BuildHasherDefault::default(),
        );
    let mut distinct: Vec<&str> = Vec::new();
    let mut occurrence_ordinals: Vec<u32> = Vec::with_capacity(series_strings);
    let mut exemplar_ordinals: Vec<u32> = Vec::with_capacity(exemplar_strings);

    for s in series {
        for label in s.labels.iter() {
            for text in [label.name.as_str(), label.value.as_str()] {
                occurrence_ordinals.push(intern_dict(&mut interner, &mut distinct, text)?);
            }
        }
    }
    for e in exemplars {
        for (name, value) in &e.attrs {
            exemplar_ordinals.push(intern_dict(&mut interner, &mut distinct, name.as_str())?);
            exemplar_ordinals.push(intern_dict(&mut interner, &mut distinct, value.as_str())?);
        }
    }

    let (order, rank, count) = sort_and_rank_dict(&distinct)?;

    let occurrence_ordinals = occurrence_ordinals
        .into_iter()
        .map(|id| rank[id as usize])
        .collect();
    let exemplar_attr_ordinals = exemplar_ordinals
        .into_iter()
        .map(|id| rank[id as usize])
        .collect();

    Ok(DictionaryV4 {
        order,
        occurrence_ordinals,
        exemplar_attr_ordinals,
        count,
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod v4_tests {
    use proptest::prelude::*;
    use ravel_types::{Label, Sample};

    use super::*;
    use crate::ReaderLimits;
    use crate::histogram::{HistogramCounts, HistogramSpan, ResetHint};
    use crate::varint::read_uvarint;

    fn test_identity() -> SegmentIdentity {
        SegmentIdentity {
            tenant_hash: [0x5A; 16],
            shard: 4,
            writer_id: "v4-test-writer".to_string(),
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

    fn test_compaction_meta() -> CompactionMetaV4 {
        CompactionMetaV4 {
            ingest_hour_bucket: 42,
            input_set_hash: [0x11; 32],
            part_index: 2,
            level: 1,
        }
    }

    fn labels(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    fn labels_kv(metric: &str, k: &str, v: &str) -> LabelSet {
        LabelSet::new(vec![
            Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: metric.to_string(),
            },
            Label {
                name: k.to_string(),
                value: v.to_string(),
            },
        ])
        .expect("valid labels")
    }

    /// Parses the 16-byte trailer and decodes the footer protobuf, for an
    /// object written at `expected_version` (the v5 raw-sample writer's
    /// output is decoded here too, to pull real histogram page bytes out for
    /// the verbatim-copy test below). Panics rather than returning a
    /// typed error: every test here controls its own input, so a parse
    /// failure is this test module's own bug, never untrusted data.
    fn decode_footer(object: &[u8], expected_version: u16) -> Footer {
        let n = object.len();
        assert!(n >= 16, "object smaller than the trailer");
        let trailer = &object[n - 16..];
        let footer_len = u32::from_le_bytes(trailer[0..4].try_into().expect("4 bytes")) as usize;
        let version = u16::from_le_bytes(trailer[8..10].try_into().expect("2 bytes"));
        assert_eq!(version, expected_version, "unexpected trailer version");
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

    /// LABEL_DICT decoded into ordinal-indexed strings (index 0 =
    /// `__name__`), same grammar as the v2/v3 test helpers.
    fn parse_label_dict(raw: &[u8]) -> Vec<String> {
        let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes")) as usize;
        let mut pos = 4usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let len = read_uvarint(raw, &mut pos).expect("string len varint") as usize;
            let s = std::str::from_utf8(&raw[pos..pos + len])
                .expect("utf-8")
                .to_string();
            pos += len;
            out.push(s);
        }
        assert_eq!(pos, raw.len(), "trailing bytes in LABEL_DICT");
        out
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

    /// SERIES_META v4 columns, decoded per the "SERIES_META in v4" grammar
    /// in docs/segment-format.md: schema preamble unchanged, then
    /// `run_total`, then 16 column blocks (4 series-major, 12 run-major).
    struct ParsedSeriesMetaV4 {
        run_total: u32,
        run_count: Vec<u32>,
        run_created_delta: Vec<u64>,
        run_sample_count: Vec<u64>,
        ts_page_gap: Vec<u64>,
        ts_page_len: Vec<u64>,
        val_page_gap: Vec<u64>,
        val_page_len: Vec<u64>,
    }

    fn parse_series_meta_v4(raw: &[u8]) -> ParsedSeriesMetaV4 {
        let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes"));
        let schema_count = u32::from_le_bytes(raw[4..8].try_into().expect("4 bytes"));
        let mut pos = 8usize;
        for _ in 0..schema_count {
            let name_count = read_uvarint(raw, &mut pos).expect("varint") as usize;
            for _ in 0..name_count {
                read_uvarint(raw, &mut pos).expect("varint");
            }
        }
        let run_total = u32::from_le_bytes(raw[pos..pos + 4].try_into().expect("4 bytes"));
        pos += 4;

        let _schema_ref_block = read_block(raw, &mut pos);
        let _value_ord_block = read_block(raw, &mut pos);
        let value_kind_block = read_block(raw, &mut pos);
        let run_count_block = read_block(raw, &mut pos);
        let run_created_delta_block = read_block(raw, &mut pos);
        let _run_epoch_block = read_block(raw, &mut pos);
        let _run_seq_block = read_block(raw, &mut pos);
        let run_sample_count_block = read_block(raw, &mut pos);
        let _run_min_ts_delta_block = read_block(raw, &mut pos);
        let _run_ts_span_block = read_block(raw, &mut pos);
        let ts_page_gap_block = read_block(raw, &mut pos);
        let ts_page_len_block = read_block(raw, &mut pos);
        let val_page_gap_block = read_block(raw, &mut pos);
        let val_page_len_block = read_block(raw, &mut pos);
        let _hist_page_gap_block = read_block(raw, &mut pos);
        let _hist_page_len_block = read_block(raw, &mut pos);
        assert_eq!(pos, raw.len(), "trailing bytes in SERIES_META");

        assert_eq!(
            value_kind_block.len(),
            count as usize,
            "value_kind is one raw byte per series, not a varint column"
        );

        let run_count: Vec<u32> = read_varints(run_count_block, count as usize)
            .into_iter()
            .map(|v| v as u32)
            .collect();
        assert_eq!(
            run_count.iter().copied().sum::<u32>(),
            run_total,
            "run_count must sum to run_total"
        );

        let run_total_usize = run_total as usize;
        ParsedSeriesMetaV4 {
            run_total,
            run_count,
            run_created_delta: read_varints(run_created_delta_block, run_total_usize),
            run_sample_count: read_varints(run_sample_count_block, run_total_usize),
            ts_page_gap: read_varints(ts_page_gap_block, run_total_usize),
            ts_page_len: read_varints(ts_page_len_block, run_total_usize),
            val_page_gap: read_varints(val_page_gap_block, run_total_usize),
            val_page_len: read_varints(val_page_len_block, run_total_usize),
        }
    }

    // --- hand-assembled, fully-framed pages: exactly the shape write_v4
    // expects a caller (the compactor) to hand it, having already read
    // them off an input v1/v2/v3 object. Building them directly here
    // (rather than only ever round-tripping through write_v1/v2/v3) lets
    // each test control page content precisely, e.g. to force the
    // VAL_RAW_F64 alignment path deterministically. ---

    fn raw_page(series_id: &SeriesId, enc: u8, payload: &[u8]) -> Vec<u8> {
        let comp = page_comp::NONE;
        let crc = page_crc(&series_id.0, enc, comp, payload);
        let mut page = Vec::with_capacity(6 + payload.len());
        page.push(enc);
        page.push(comp);
        page.extend_from_slice(&crc.to_le_bytes());
        page.extend_from_slice(payload);
        page
    }

    fn ts_page(series_id: &SeriesId, ts_ns: &[i64]) -> Vec<u8> {
        let mut payload = Vec::new();
        encode_ts_deltas_into(&mut payload, ts_ns).expect("ts deltas encode");
        raw_page(series_id, page_enc::TS_DELTA_VARINT, &payload)
    }

    fn val_raw_f64_page(series_id: &SeriesId, values: &[f64]) -> Vec<u8> {
        let mut payload = Vec::new();
        for v in values {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        raw_page(series_id, page_enc::VAL_RAW_F64, &payload)
    }

    fn val_gorilla_page(series_id: &SeriesId, values: &[f64]) -> Vec<u8> {
        let mut payload = Vec::new();
        encode_gorilla_into(values, &mut payload);
        raw_page(series_id, page_enc::VAL_GORILLA, &payload)
    }

    fn hist_page(series_id: &SeriesId, values: &[HistogramValue]) -> Vec<u8> {
        let mut payload = Vec::new();
        for v in values {
            encode_histogram_record_into(&mut payload, v).expect("histogram encodes");
        }
        raw_page(series_id, page_enc::HIST_SPANS, &payload)
    }

    #[allow(clippy::too_many_arguments)]
    fn scalar_run(
        created_unix_ns: i64,
        writer_epoch: u64,
        writer_seq: u64,
        series_id: &SeriesId,
        ts_ns: &[i64],
        values: &[f64],
        raw_f64: bool,
    ) -> RunInputV4 {
        assert_eq!(ts_ns.len(), values.len());
        RunInputV4 {
            created_unix_ns,
            writer_epoch,
            writer_seq,
            min_ts_ns: *ts_ns.iter().min().expect("non-empty"),
            max_ts_ns: *ts_ns.iter().max().expect("non-empty"),
            sample_count: ts_ns.len() as u32,
            ts_page: ts_page(series_id, ts_ns),
            value_page: RunValuePageV4::Scalar(if raw_f64 {
                val_raw_f64_page(series_id, values)
            } else {
                val_gorilla_page(series_id, values)
            }),
        }
    }

    fn sample_histogram_value(seed: i32) -> HistogramValue {
        let zero_count = 1;
        let positive = vec![2, (seed.unsigned_abs()) as u64 + 1];
        let count = zero_count + positive.iter().sum::<u64>();
        HistogramValue {
            scale: 3,
            zero_threshold: 0.001,
            sum: Some(f64::from(seed) + 0.5),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count,
                count,
                positive,
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        }
    }

    /// Issue #155: v4's LABEL_DICT is "as v2/v3" (section 4), so it inherits
    /// the restored sort (issue #146) -- `__name__` at ordinal 0, every other
    /// distinct string in sorted (byte) order. Inputs are chosen so
    /// first-occurrence order differs from sorted order.
    #[test]
    fn v4_label_dict_is_sorted() {
        let mk = |id: u8, metric: &str, k: &str, v: &str| {
            let series_id = SeriesId([id; 16]);
            SeriesInputV4 {
                series_id,
                labels: labels_kv(metric, k, v),
                runs: vec![scalar_run(100, 1, 1, &series_id, &[10], &[1.0], false)],
            }
        };
        let series = vec![
            mk(0x01, "zeta", "zzz", "yyy"),
            mk(0x02, "alpha", "aaa", "bbb"),
            mk(0x03, "mu", "mmm", "nnn"),
        ];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object, VERSION_V4);
        let dict_desc =
            section_desc(&footer, section_kind::LABEL_DICT).expect("LABEL_DICT present");
        let dict_raw = decompress_section(
            section(object, &footer, section_kind::LABEL_DICT),
            dict_desc.uncompressed_len,
        );
        let dict = parse_label_dict(&dict_raw);

        assert_eq!(
            dict[0].as_str(),
            METRIC_NAME_LABEL,
            "__name__ pinned at ordinal 0"
        );
        let rest = &dict[1..];
        let mut expected = rest.to_vec();
        expected.sort();
        assert_eq!(
            rest,
            expected.as_slice(),
            "v4 LABEL_DICT past ordinal 0 must be byte-sorted (as v2/v3, issue #155)"
        );
        assert_ne!(
            rest[0].as_str(),
            "zeta",
            "test setup must make sorted order differ from first-occurrence order"
        );
    }

    // --- drop rules (docs/compaction-retention-plan.md section 4: a run
    // with zero samples is dropped; a series left with zero runs is
    // dropped entirely, same silent-drop principle v1 established for
    // zero-sample series). ---

    #[test]
    fn empty_segment_has_zero_series_and_omits_val_hist_sections() {
        let written = SegmentWriter::write_v4(
            Vec::new(),
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let footer = decode_footer(written.bytes.as_ref(), VERSION_V4);
        assert_eq!(footer.series_count, 0);
        assert_eq!(footer.sample_count, 0);
        assert_eq!(footer.base_created_unix_ns, 0);
        assert!(section_desc(&footer, section_kind::VAL_PAGES).is_none());
        assert!(section_desc(&footer, section_kind::HIST_PAGES).is_none());
    }

    /// The L0 flush adapter (`write_histograms_with_exemplars`, the ingest
    /// shard's entry point, issue #474) carries exemplars into the EXEMPLARS
    /// section with `series_index` resolved against the output's sorted
    /// SERIES_IDS, and leaves the records ascending by `(series_index, ts_ns)`
    /// with duplicates intact (ADR-0047 amendment 2026-08-03).
    #[test]
    fn l0_flush_adapter_writes_exemplars_in_ascending_key_order() {
        // Ids chosen so `high` sorts after `low`: the adapter must resolve
        // series_index against the sorted output, not the input order.
        let high = SeriesId([0xF0; 16]);
        let low = SeriesId([0x0F; 16]);
        let series = vec![
            SeriesInputV3 {
                series_id: high,
                labels: labels("high"),
                values: SeriesValues::Scalar(vec![
                    Sample {
                        ts_ns: 1_000,
                        value: 1.0,
                    },
                    Sample {
                        ts_ns: 2_000,
                        value: 2.0,
                    },
                ]),
            },
            SeriesInputV3 {
                series_id: low,
                labels: labels("low"),
                values: SeriesValues::Scalar(vec![Sample {
                    ts_ns: 1_500,
                    value: 3.0,
                }]),
            },
        ];
        // Offered out of key order, and two records share `(low, 1_500)`:
        // both must survive, in the caller's order (stable sort).
        let exemplars = vec![
            ExemplarInput {
                series_id: high,
                ts_ns: 2_000,
                value: -0.0,
                trace_id: [0x11; 16],
                span_id: [0x22; 8],
                attrs: vec![("svc".to_string(), "checkout".to_string())],
            },
            ExemplarInput {
                series_id: low,
                ts_ns: 1_500,
                value: f64::NAN,
                trace_id: [0x33; 16],
                span_id: [0x44; 8],
                attrs: Vec::new(),
            },
            ExemplarInput {
                series_id: low,
                ts_ns: 1_500,
                value: 7.5,
                trace_id: [0x55; 16],
                span_id: [0u8; 8],
                attrs: Vec::new(),
            },
        ];
        let written = SegmentWriter::write_histograms_with_exemplars(
            series,
            test_identity(),
            test_bounds(),
            exemplars,
        )
        .expect("writes");

        let obj = written.bytes.as_ref();
        let footer = decode_footer(obj, VERSION_V6);
        assert!(section_desc(&footer, section_kind::EXEMPLARS).is_some());
        let records = crate::exemplars::decode_exemplars_section(
            &footer,
            section(obj, &footer, section_kind::LABEL_DICT),
            section(obj, &footer, section_kind::EXEMPLARS),
            ReaderLimits::default(),
        )
        .expect("decodes");
        assert_eq!(records.len(), 3);
        // `low` (0x0F..) is series_index 0, `high` (0xF0..) is 1.
        let keys: Vec<(u64, i64)> = records.iter().map(|r| (r.series_index, r.ts_ns)).collect();
        assert_eq!(keys, vec![(0, 1_500), (0, 1_500), (1, 2_000)]);
        // Bit patterns, never `==`: NaN and -0.0 are significant.
        assert_eq!(records[0].value.to_bits(), f64::NAN.to_bits());
        assert_eq!(records[1].value.to_bits(), 7.5f64.to_bits());
        assert_eq!(records[2].value.to_bits(), (-0.0f64).to_bits());
        assert_eq!(records[2].trace_id, [0x11; 16]);
        assert_eq!(
            records[2].attrs,
            vec![("svc".to_string(), "checkout".to_string())]
        );
    }

    /// A flush with no exemplars emits no EXEMPLARS section at all: absence,
    /// not a zero-count section, is how "no exemplars" is represented
    /// (ADR-0047 decision 1, docs/segment-format.md).
    #[test]
    fn l0_flush_adapter_without_exemplars_emits_no_section() {
        let series = vec![SeriesInputV3 {
            series_id: SeriesId([0x07; 16]),
            labels: labels("m"),
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: 10,
                value: 1.0,
            }]),
        }];
        let written = SegmentWriter::write_histograms(series, test_identity(), test_bounds())
            .expect("writes");
        let footer = decode_footer(written.bytes.as_ref(), VERSION_V6);
        assert!(
            section_desc(&footer, section_kind::EXEMPLARS).is_none(),
            "a flush with no exemplars must emit no EXEMPLARS section"
        );
    }

    /// An exemplar naming a series the output does not carry is a writer
    /// error, never a silent drop (docs/segment-format.md "Writer edge
    /// rules"). Reachable from the flush path because a zero-sample series is
    /// dropped, so the ingest shard must filter such exemplars itself.
    #[test]
    fn l0_flush_adapter_rejects_an_exemplar_for_a_dropped_series() {
        let present = SeriesId([0x01; 16]);
        let absent = SeriesId([0x02; 16]);
        let series = vec![
            SeriesInputV3 {
                series_id: present,
                labels: labels("present"),
                values: SeriesValues::Scalar(vec![Sample {
                    ts_ns: 10,
                    value: 1.0,
                }]),
            },
            // Zero samples: dropped by the adapter.
            SeriesInputV3 {
                series_id: absent,
                labels: labels("absent"),
                values: SeriesValues::Scalar(Vec::new()),
            },
        ];
        let exemplars = vec![ExemplarInput {
            series_id: absent,
            ts_ns: 10,
            value: 1.0,
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            attrs: Vec::new(),
        }];
        // `WrittenSegment` is not `Debug`, so match rather than `expect_err`.
        match SegmentWriter::write_histograms_with_exemplars(
            series,
            test_identity(),
            test_bounds(),
            exemplars,
        ) {
            Err(WriteError::ExemplarUnknownSeries) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("an exemplar for a dropped series must not be silently dropped"),
        }
    }

    #[test]
    fn zero_sample_run_is_dropped_but_series_survives_with_remaining_runs() {
        let id = SeriesId([0x01; 16]);
        let mut zero_run = scalar_run(100, 1, 1, &id, &[10], &[1.0], false);
        zero_run.sample_count = 0;
        let live_run = scalar_run(200, 1, 1, &id, &[20], &[2.0], false);

        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![zero_run, live_run],
        }];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let footer = decode_footer(written.bytes.as_ref(), VERSION_V4);
        assert_eq!(footer.series_count, 1);

        let meta_desc = section_desc(&footer, section_kind::SERIES_META).expect("mandatory");
        let meta_raw = decompress_section(
            section(written.bytes.as_ref(), &footer, section_kind::SERIES_META),
            meta_desc.uncompressed_len,
        );
        let meta = parse_series_meta_v4(&meta_raw);
        assert_eq!(meta.run_total, 1, "the zero-sample run must be dropped");
        assert_eq!(meta.run_count, vec![1]);
    }

    #[test]
    fn series_with_all_runs_dropped_is_absent_entirely() {
        let id = SeriesId([0x02; 16]);
        let mut zero_run = scalar_run(100, 1, 1, &id, &[10], &[1.0], false);
        zero_run.sample_count = 0;

        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![zero_run],
        }];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let footer = decode_footer(written.bytes.as_ref(), VERSION_V4);
        assert_eq!(footer.series_count, 0);
        assert_eq!(footer.base_created_unix_ns, 0);
    }

    // --- core correctness: single run, multi-run overlap harmlessness,
    // priority-tuple sort order, verbatim page copy. ---

    #[test]
    fn single_run_series_copies_ts_and_val_pages_verbatim() {
        let id = SeriesId([0x03; 16]);
        let run = scalar_run(1_000, 2, 5, &id, &[10, 20, 30], &[1.0, 2.0, 3.0], false);
        let expected_ts_page = run.ts_page.clone();
        let expected_val_page = match &run.value_page {
            RunValuePageV4::Scalar(p) => p.clone(),
            RunValuePageV4::Histogram(_) => unreachable!(),
        };

        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![run],
        }];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object, VERSION_V4);

        assert_eq!(
            section(object, &footer, section_kind::TS_PAGES),
            &expected_ts_page[..]
        );
        assert_eq!(
            section(object, &footer, section_kind::VAL_PAGES),
            &expected_val_page[..]
        );
    }

    #[test]
    fn multi_run_series_preserves_every_run_verbatim_and_sorts_by_priority() {
        let id = SeriesId([0x04; 16]);
        // Constructed out of priority order and with overlapping
        // timestamps (both runs cover ts=100): compaction never
        // deduplicates at rest, so both runs' full pages must survive,
        // reordered ascending by (created_unix_ns, writer_epoch,
        // writer_seq), never merged into one page.
        let run_b = scalar_run(2_000, 1, 1, &id, &[100, 110], &[9.0, 9.5], false);
        let run_a = scalar_run(1_000, 1, 1, &id, &[100, 105], &[1.0, 1.5], false);
        let expected_ts_a = run_a.ts_page.clone();
        let expected_ts_b = run_b.ts_page.clone();
        let expected_val_a = match &run_a.value_page {
            RunValuePageV4::Scalar(p) => p.clone(),
            RunValuePageV4::Histogram(_) => unreachable!(),
        };
        let expected_val_b = match &run_b.value_page {
            RunValuePageV4::Scalar(p) => p.clone(),
            RunValuePageV4::Histogram(_) => unreachable!(),
        };

        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            // Input order deliberately reversed vs. expected output order.
            runs: vec![run_b, run_a],
        }];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object, VERSION_V4);

        let meta_desc = section_desc(&footer, section_kind::SERIES_META).expect("mandatory");
        let meta_raw = decompress_section(
            section(object, &footer, section_kind::SERIES_META),
            meta_desc.uncompressed_len,
        );
        let meta = parse_series_meta_v4(&meta_raw);
        assert_eq!(meta.run_total, 2);
        assert_eq!(meta.run_count, vec![2]);
        // run_a (created_unix_ns=1000) must sort before run_b (2000).
        assert!(meta.run_created_delta[0] < meta.run_created_delta[1]);

        let ts_bytes = section(object, &footer, section_kind::TS_PAGES);
        let val_bytes = section(object, &footer, section_kind::VAL_PAGES);
        assert_eq!(
            ts_bytes,
            [expected_ts_a.clone(), expected_ts_b.clone()].concat(),
            "both runs' TS pages must survive back to back, in sorted order, never merged"
        );
        assert_eq!(
            val_bytes,
            [expected_val_a, expected_val_b].concat(),
            "both runs' VAL pages must survive back to back, in sorted order, never merged"
        );
    }

    #[test]
    fn raw_f64_run_requires_alignment_gap_and_decodes_correctly() {
        let id = SeriesId([0x05; 16]);
        let run = scalar_run(1_000, 1, 1, &id, &[10, 20, 30], &[1.5, 2.5, 3.5], true);
        let expected_val_page = match &run.value_page {
            RunValuePageV4::Scalar(p) => p.clone(),
            RunValuePageV4::Histogram(_) => unreachable!(),
        };

        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![run],
        }];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object, VERSION_V4);
        let val_desc = section_desc(&footer, section_kind::VAL_PAGES).expect("present");

        let meta_desc = section_desc(&footer, section_kind::SERIES_META).expect("mandatory");
        let meta_raw = decompress_section(
            section(object, &footer, section_kind::SERIES_META),
            meta_desc.uncompressed_len,
        );
        let meta = parse_series_meta_v4(&meta_raw);

        // The lone run's page starts at gap bytes into the section; its
        // page header is 6 bytes, so its payload start (gap + 6) must be
        // 8-byte aligned relative to the section (and hence the object,
        // since the section itself is 8-byte aligned).
        let gap = meta.val_page_gap[0];
        let payload_start = val_desc.offset + gap + 6;
        assert_eq!(
            payload_start % 8,
            0,
            "VAL_RAW_F64 payload must be 8-byte aligned"
        );

        let val_bytes = section(object, &footer, section_kind::VAL_PAGES);
        let page_start = gap as usize;
        let page_end = page_start + meta.val_page_len[0] as usize;
        assert_eq!(
            &val_bytes[page_start..page_end],
            &expected_val_page[..],
            "page bytes themselves are unchanged by the alignment gap"
        );
        assert!(
            val_bytes[..page_start].iter().all(|&b| b == 0),
            "alignment padding must be zero bytes"
        );
    }

    // --- histogram runs: verbatim reuse of real HIST_PAGES bytes framed by
    // the raw-sample v5 writer, never re-encoded (the ticket's core
    // requirement). ---

    #[test]
    fn histogram_run_reuses_real_hist_page_bytes_verbatim() {
        let id = SeriesId([0x06; 16]);
        let hist_values = [sample_histogram_value(1), sample_histogram_value(2)];
        let ts_values = [10i64, 20i64];

        // Produce real HIST_PAGES / TS_PAGES bytes via the raw-sample v5
        // writer (which frames histogram pages exactly as the retired v3
        // writer did) -- a single histogram series, so its page is the
        // section's entire content.
        let v3_series = vec![SeriesInputV3 {
            series_id: id,
            labels: labels("m"),
            values: SeriesValues::Histogram(
                ts_values
                    .iter()
                    .zip(hist_values.iter())
                    .map(|(&ts_ns, value)| HistogramSample {
                        ts_ns,
                        value: value.clone(),
                    })
                    .collect(),
            ),
        }];
        let v3_written = SegmentWriter::write_histograms(v3_series, test_identity(), test_bounds())
            .expect("writes");
        let v3_object = v3_written.bytes.as_ref();
        let v3_footer = decode_footer(v3_object, VERSION_V6);
        let v3_ts_page = section(v3_object, &v3_footer, section_kind::TS_PAGES).to_vec();
        let v3_hist_page = section(v3_object, &v3_footer, section_kind::HIST_PAGES).to_vec();

        let run = RunInputV4 {
            created_unix_ns: 500,
            writer_epoch: 1,
            writer_seq: 1,
            min_ts_ns: 10,
            max_ts_ns: 20,
            sample_count: 2,
            ts_page: v3_ts_page.clone(),
            value_page: RunValuePageV4::Histogram(v3_hist_page.clone()),
        };
        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![run],
        }];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object, VERSION_V4);

        assert_eq!(
            section(object, &footer, section_kind::TS_PAGES),
            &v3_ts_page[..]
        );
        assert_eq!(
            section(object, &footer, section_kind::HIST_PAGES),
            &v3_hist_page[..],
            "v4 must copy v3's histogram page bytes verbatim, never re-encode them"
        );
        assert!(section_desc(&footer, section_kind::VAL_PAGES).is_none());
    }

    // --- multi-schema series ---

    #[test]
    fn multi_schema_series_get_distinct_schema_refs() {
        let id_a = SeriesId([0x07; 16]);
        let id_b = SeriesId([0x08; 16]);
        let labels_a = labels("metric_a");
        let labels_b = LabelSet::new(vec![
            Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: "metric_b".to_string(),
            },
            Label {
                name: "region".to_string(),
                value: "us".to_string(),
            },
        ])
        .expect("valid labels");

        let series = vec![
            SeriesInputV4 {
                series_id: id_a,
                labels: labels_a,
                runs: vec![scalar_run(1_000, 1, 1, &id_a, &[10], &[1.0], false)],
            },
            SeriesInputV4 {
                series_id: id_b,
                labels: labels_b,
                runs: vec![scalar_run(1_000, 1, 1, &id_b, &[10], &[2.0], false)],
            },
        ];
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        )
        .expect("writes");
        let object = written.bytes.as_ref();
        let footer = decode_footer(object, VERSION_V4);
        let meta_desc = section_desc(&footer, section_kind::SERIES_META).expect("mandatory");
        let meta_raw = decompress_section(
            section(object, &footer, section_kind::SERIES_META),
            meta_desc.uncompressed_len,
        );
        let schema_count = u32::from_le_bytes(meta_raw[4..8].try_into().expect("4 bytes"));
        assert_eq!(
            schema_count, 2,
            "distinct label schemas must get distinct schema_ref entries"
        );
    }

    // --- rejections ---

    #[test]
    fn mixed_value_kind_in_one_series_is_rejected() {
        let id = SeriesId([0x09; 16]);
        let scalar = scalar_run(1_000, 1, 1, &id, &[10], &[1.0], false);
        let histogram = RunInputV4 {
            created_unix_ns: 2_000,
            writer_epoch: 1,
            writer_seq: 1,
            min_ts_ns: 20,
            max_ts_ns: 20,
            sample_count: 1,
            ts_page: ts_page(&id, &[20]),
            value_page: RunValuePageV4::Histogram(hist_page(&id, &[sample_histogram_value(1)])),
        };

        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![scalar, histogram],
        }];
        let result = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        );
        assert!(matches!(result, Err(WriteError::MixedValueKindInSeries)));
    }

    #[test]
    fn run_page_shorter_than_header_is_rejected() {
        let id = SeriesId([0x0A; 16]);
        let mut run = scalar_run(1_000, 1, 1, &id, &[10], &[1.0], false);
        run.ts_page = vec![0u8; 3]; // shorter than the 6-byte page header

        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![run],
        }];
        let result = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        );
        assert!(matches!(result, Err(WriteError::RunPageTooShort)));
    }

    #[test]
    fn duplicate_series_id_is_rejected() {
        let id = SeriesId([0x0B; 16]);
        let series = vec![
            SeriesInputV4 {
                series_id: id,
                labels: labels("m"),
                runs: vec![scalar_run(1_000, 1, 1, &id, &[10], &[1.0], false)],
            },
            SeriesInputV4 {
                series_id: id,
                labels: labels("m"),
                runs: vec![scalar_run(2_000, 1, 1, &id, &[20], &[2.0], false)],
            },
        ];
        let result = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
            Vec::new(),
        );
        assert!(matches!(result, Err(WriteError::DuplicateSeriesId)));
    }

    // --- footer provenance fields ---

    #[test]
    fn footer_provenance_fields_and_base_created_unix_ns_round_trip() {
        let id = SeriesId([0x0C; 16]);
        let series = vec![SeriesInputV4 {
            series_id: id,
            labels: labels("m"),
            runs: vec![
                scalar_run(5_000, 1, 1, &id, &[10], &[1.0], false),
                scalar_run(3_000, 1, 1, &id, &[20], &[2.0], false),
            ],
        }];
        let meta = test_compaction_meta();
        let written = SegmentWriter::write_v4(
            series,
            test_identity(),
            test_bounds(),
            meta.clone(),
            Vec::new(),
        )
        .expect("writes");
        let footer = decode_footer(written.bytes.as_ref(), VERSION_V4);

        assert_eq!(
            footer.base_created_unix_ns, 3_000,
            "min created_unix_ns over all runs"
        );
        assert_eq!(footer.ingest_hour_bucket, meta.ingest_hour_bucket);
        assert_eq!(footer.input_set_hash, meta.input_set_hash.to_vec());
        assert_eq!(footer.part_index, meta.part_index);
        assert_eq!(footer.level, meta.level);
    }

    // --- proptest: internal consistency across randomized multi-run,
    // mixed scalar/histogram batches. ---

    fn ts_strategy() -> impl Strategy<Value = i64> {
        0i64..1_000
    }

    fn run_identity_strategy() -> impl Strategy<Value = (i64, u64, u64)> {
        (0i64..10_000, 0u64..5, 0u64..5)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// Internal-consistency property: for randomized multi-run,
        /// mixed scalar/histogram batches, SERIES_META v4's declared
        /// run-major bookkeeping (run_total, per-run sample counts, ts
        /// bounds, reconstructed TS/VAL/HIST page ranges) is self-
        /// consistent with what was actually written, runs land sorted
        /// ascending by (created_unix_ns, writer_epoch, writer_seq), and
        /// every run's page bytes survive byte-for-byte.
        #[test]
        fn series_meta_v4_matches_written_pages(
            run_specs in prop::collection::vec(
                (run_identity_strategy(), prop::collection::vec((ts_strategy(), -1_000.0f64..1_000.0), 1..5), any::<bool>()),
                1..4,
            )
        ) {
            let id = SeriesId([0x42; 16]);
            let mut runs: Vec<RunInputV4> = run_specs
                .into_iter()
                .map(|((created, epoch, seq), mut samples, raw_f64)| {
                    samples.sort_by_key(|a| a.0);
                    let ts_ns: Vec<i64> = samples.iter().map(|(t, _)| *t).collect();
                    let values: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
                    scalar_run(created, epoch, seq, &id, &ts_ns, &values, raw_f64)
                })
                .collect();

            let mut expected_sorted = runs.clone();
            expected_sorted.sort_by_key(|r| (r.created_unix_ns, r.writer_epoch, r.writer_seq));
            let expected_ts_pages: Vec<Vec<u8>> =
                expected_sorted.iter().map(|r| r.ts_page.clone()).collect();
            let expected_val_pages: Vec<Vec<u8>> = expected_sorted
                .iter()
                .map(|r| match &r.value_page {
                    RunValuePageV4::Scalar(p) => p.clone(),
                    RunValuePageV4::Histogram(_) => unreachable!(),
                })
                .collect();

            let series = vec![SeriesInputV4 {
                series_id: id,
                labels: labels("m"),
                runs: std::mem::take(&mut runs),
            }];
            let written = SegmentWriter::write_v4(
                series,
                test_identity(),
                test_bounds(),
                test_compaction_meta(),
                Vec::new(),
            )
            .expect("writes");
            let object = written.bytes.as_ref();
            let footer = decode_footer(object, VERSION_V4);

            let meta_desc = section_desc(&footer, section_kind::SERIES_META).expect("mandatory");
            let meta_raw = decompress_section(
                section(object, &footer, section_kind::SERIES_META),
                meta_desc.uncompressed_len,
            );
            let meta = parse_series_meta_v4(&meta_raw);
            prop_assert_eq!(meta.run_total as usize, expected_sorted.len());
            prop_assert_eq!(meta.run_count, vec![expected_sorted.len() as u32]);

            for w in meta.run_created_delta.windows(2) {
                prop_assert!(w[0] <= w[1]);
            }

            let ts_bytes = section(object, &footer, section_kind::TS_PAGES);
            let val_bytes = section_desc(&footer, section_kind::VAL_PAGES)
                .map(|_| section(object, &footer, section_kind::VAL_PAGES))
                .unwrap_or(&[]);

            let mut ts_running = 0u64;
            let mut val_running = 0u64;
            for (i, run) in expected_sorted.iter().enumerate() {
                let ts_offset = ts_running + meta.ts_page_gap[i];
                let ts_len = meta.ts_page_len[i];
                let ts_end = ts_offset + ts_len;
                prop_assert!(ts_end <= ts_bytes.len() as u64);
                prop_assert_eq!(
                    &ts_bytes[ts_offset as usize..ts_end as usize],
                    &expected_ts_pages[i][..]
                );
                let (ts_enc, _, _) =
                    split_and_verify_page(&id, &ts_bytes[ts_offset as usize..ts_end as usize]);
                prop_assert_eq!(ts_enc, page_enc::TS_DELTA_VARINT);
                ts_running = ts_end;

                let val_offset = val_running + meta.val_page_gap[i];
                let val_len = meta.val_page_len[i];
                let val_end = val_offset + val_len;
                prop_assert!(val_end <= val_bytes.len() as u64);
                prop_assert_eq!(
                    &val_bytes[val_offset as usize..val_end as usize],
                    &expected_val_pages[i][..]
                );
                let (val_enc, val_comp, _) =
                    split_and_verify_page(&id, &val_bytes[val_offset as usize..val_end as usize]);
                prop_assert_eq!(val_comp, page_comp::NONE);
                prop_assert!(
                    val_enc == page_enc::VAL_GORILLA || val_enc == page_enc::VAL_RAW_F64,
                    "unexpected VAL page enc byte {val_enc}"
                );
                if val_enc == page_enc::VAL_RAW_F64 {
                    prop_assert_eq!((val_desc_offset(&footer) + val_offset + 6) % 8, 0);
                }
                val_running = val_end;

                prop_assert_eq!(meta.run_sample_count[i], run.sample_count as u64);
            }
            prop_assert_eq!(ts_running, ts_bytes.len() as u64);
            prop_assert_eq!(val_running, val_bytes.len() as u64);
        }
    }

    fn val_desc_offset(footer: &Footer) -> u64 {
        section_desc(footer, section_kind::VAL_PAGES)
            .map(|s| s.offset)
            .unwrap_or(0)
    }

    /// #282: a below-threshold (dense) v5 object carries the whole-section
    /// SERIES_META, so `decode_catalog_v5` decodes LABEL_DICT + SERIES_IDS
    /// itself and then builds the catalog via `decode_catalog_v4_from_decoded`.
    /// Pin that LABEL_DICT is decompressed exactly once per catalog decode:
    /// before the fix, `decode_catalog_v5` decoded it, then handed the raw
    /// section bytes to `decode_catalog_v4`, which decoded it a second time,
    /// so this counter read 2.
    #[test]
    fn dense_v5_catalog_decode_decompresses_label_dict_once() {
        let mk = |id: u8, metric: &str, k: &str, v: &str| {
            let series_id = SeriesId([id; 16]);
            SeriesInputV4 {
                series_id,
                labels: labels_kv(metric, k, v),
                runs: vec![scalar_run(
                    100,
                    1,
                    1,
                    &series_id,
                    &[10, 11],
                    &[1.0, 2.0],
                    false,
                )],
            }
        };
        // A handful of series, well below V5_SPARSE_THRESHOLD (4096), so the
        // object keeps the whole SERIES_META section (kind 6).
        let series = vec![
            mk(0x01, "zeta", "zzz", "yyy"),
            mk(0x02, "alpha", "aaa", "bbb"),
            mk(0x03, "mu", "mmm", "nnn"),
        ];
        let written = SegmentWriter::write_v5(
            series,
            test_identity(),
            test_bounds(),
            test_compaction_meta(),
        )
        .expect("write v5");
        let object = written.bytes.as_ref();
        let loc = crate::open_from_full(object, ReaderLimits::default()).expect("open v5");
        assert_eq!(loc.version, 6);
        assert!(
            section_desc(&loc.footer, section_kind::SERIES_META).is_some(),
            "below-threshold v5 must carry whole SERIES_META"
        );

        crate::reader::decode_counter::reset();
        let entries =
            crate::decode_catalog_v5(&loc.footer, object, ReaderLimits::default()).expect("decode");
        assert_eq!(entries.len(), 3);
        assert_eq!(
            crate::reader::decode_counter::label_dict_decodes(),
            1,
            "LABEL_DICT must be decompressed exactly once per catalog decode"
        );
    }

    /// #283: the page decoders append, so decoding a second run into the same
    /// timestamp/value buffers (what the fetcher's L0 one-unit-per-series path
    /// does) concatenates both runs instead of the second clobbering the
    /// first. Hand-build two runs' pages and decode them into shared buffers;
    /// before the fix the first run's samples were silently dropped.
    #[test]
    fn decode_run_pages_soa_appends_second_run_onto_first() {
        use crate::reader::{RunEntry, decode_run_pages_soa};

        let series_id = SeriesId([0x5A; 16]);
        let run = |sample_count: u32, min_ts_ns: i64, max_ts_ns: i64| RunEntry {
            created_unix_ns: 0,
            writer_epoch: 0,
            writer_seq: 0,
            sample_count,
            min_ts_ns,
            max_ts_ns,
            ts_page: (0, 0),
            val_page: (0, 0),
            hist_page: (0, 0),
        };

        let run0_ts = [10i64, 11, 12];
        let run0_vals = [1.0f64, 2.0, 3.0];
        let run1_ts = [20i64, 21];
        let run1_vals = [4.0f64, 5.0];

        let ts0 = ts_page(&series_id, &run0_ts);
        let val0 = val_raw_f64_page(&series_id, &run0_vals);
        let ts1 = ts_page(&series_id, &run1_ts);
        let val1 = val_raw_f64_page(&series_id, &run1_vals);

        let mut scratch = Vec::new();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        decode_run_pages_soa(
            &series_id,
            &run(3, 10, 12),
            &ts0,
            &val0,
            ReaderLimits::default(),
            &mut scratch,
            &mut timestamps,
            &mut values,
        )
        .expect("run 0 decodes");
        decode_run_pages_soa(
            &series_id,
            &run(2, 20, 21),
            &ts1,
            &val1,
            ReaderLimits::default(),
            &mut scratch,
            &mut timestamps,
            &mut values,
        )
        .expect("run 1 decodes");

        assert_eq!(
            timestamps,
            vec![10, 11, 12, 20, 21],
            "both runs' timestamps must survive, in on-disk order"
        );
        // Bit-pattern compare per the storage-path float rule.
        let got: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let want: Vec<u64> = [1.0f64, 2.0, 3.0, 4.0, 5.0]
            .iter()
            .map(|v| v.to_bits())
            .collect();
        assert_eq!(
            got, want,
            "both runs' values must survive, in on-disk order"
        );
    }
}

/// Bit-parity acceptance test for issue #813. The direct-emit writer
/// (`assemble_v4_body` finalized as version 6 for the non-sparse path, no
/// retrailer, one blake3 pass; scratch-reused `frame_ts_page`/
/// `frame_val_page`/`frame_hist_page`; a borrowed rather than cloned
/// `HistogramValue` per sample) must produce byte-for-byte identical output,
/// and an identical `SegmentSummary`, to the writer it replaced.
///
/// The reference implementation below (`old_*`) is not a re-derivation: every
/// function is copied verbatim from the pre-#813 commit (`git show
/// HEAD:crates/ravel-segment/src/writer.rs`, taken before this issue's
/// changes were made), including the deleted `retrailer_v4_to_v6` and the
/// original per-series-fresh-`Vec` `frame_*_page`/`ts_values`. A divergence
/// here means the new assembly actually changed a byte, not that the
/// reference drifted alongside it.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod direct_v6_emit_bit_parity {
    use proptest::prelude::*;
    use ravel_types::{Label, Sample};

    use super::*;

    fn labels(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    fn series_id_for(idx: u16) -> SeriesId {
        let mut id = [0u8; 16];
        id[0] = (idx >> 8) as u8;
        id[1] = idx as u8;
        SeriesId(id)
    }

    fn sample_histogram_value(seed: i32) -> HistogramValue {
        let zero_count = 1;
        let positive = vec![2, (seed.unsigned_abs()) as u64 + 1];
        let count = zero_count + positive.iter().sum::<u64>();
        HistogramValue {
            scale: 3,
            zero_threshold: 0.001,
            sum: Some(f64::from(seed) + 0.5),
            custom_values: None,
            positive_spans: vec![crate::histogram::HistogramSpan {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            counts: crate::histogram::HistogramCounts::Int {
                zero_count,
                count,
                positive,
                negative: vec![],
            },
            reset_hint: crate::histogram::ResetHint::Unknown,
        }
    }

    // ---- pre-#813 reference algorithm, copied verbatim ----

    fn old_ts_values(values: &SeriesValues) -> Vec<i64> {
        match values {
            SeriesValues::Scalar(v) => v.iter().map(|s| s.ts_ns).collect(),
            SeriesValues::Histogram(v) => v.iter().map(|s| s.ts_ns).collect(),
        }
    }

    fn old_frame_ts_page(series_id: &SeriesId, ts_values: &[i64]) -> Result<Vec<u8>, WriteError> {
        let mut payload = Vec::new();
        encode_ts_deltas_into(&mut payload, ts_values).ok_or(WriteError::TimestampDeltaOverflow)?;
        let enc = page_enc::TS_DELTA_VARINT;
        let compressed = if payload.len() >= LZ4_MIN_TS_PAYLOAD_BYTES {
            let candidate = lz4_flex::compress_prepend_size(&payload);
            (candidate.len() < payload.len()).then_some(candidate)
        } else {
            None
        };
        let (comp, body): (u8, &[u8]) = match &compressed {
            Some(candidate) => (page_comp::LZ4, candidate),
            None => (page_comp::NONE, &payload),
        };
        Ok(frame_page(&series_id.0, enc, comp, body))
    }

    fn old_frame_val_page(series_id: &SeriesId, values: &[f64]) -> Vec<u8> {
        let mut payload = Vec::new();
        encode_gorilla_into(values, &mut payload);
        let count = values.len() as u64;
        let enc = if (payload.len() as u64) >= 8 * count {
            payload.clear();
            for v in values {
                payload.extend_from_slice(&v.to_le_bytes());
            }
            page_enc::VAL_RAW_F64
        } else {
            page_enc::VAL_GORILLA
        };
        frame_page(&series_id.0, enc, page_comp::NONE, &payload)
    }

    fn old_frame_hist_page(
        series_id: &SeriesId,
        values: &[HistogramValue],
    ) -> Result<Vec<u8>, WriteError> {
        let mut payload = Vec::new();
        for value in values {
            encode_histogram_record_into(&mut payload, value)?;
        }
        Ok(frame_page(
            &series_id.0,
            page_enc::HIST_SPANS,
            page_comp::NONE,
            &payload,
        ))
    }

    /// Verbatim pre-#813 `write_v4`: the full assemble-and-finalize-as-v4
    /// body, not a call into today's split `assemble_v4_body` +
    /// `finalize_v4_trailer`. This is what makes the sparse-path parity case
    /// below a real check rather than a tautology.
    fn old_write_v4(
        mut series: Vec<SeriesInputV4>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        meta: CompactionMetaV4,
        exemplars: Vec<ExemplarInput>,
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

        let dict = build_dictionary_v4(&series, &exemplars)?;

        let mut series_index_by_id: HashMap<[u8; 16], u32> = HashMap::with_capacity(series.len());
        for (i, s) in series.iter().enumerate() {
            series_index_by_id.insert(s.series_id.0, i as u32);
        }
        let mut resolved_exemplars = Vec::with_capacity(exemplars.len());
        let mut exemplar_attr_cursor = 0usize;
        for e in &exemplars {
            let series_index = *series_index_by_id
                .get(&e.series_id.0)
                .ok_or(WriteError::ExemplarUnknownSeries)?;
            let mut attr_ords = Vec::with_capacity(e.attrs.len());
            for _ in &e.attrs {
                let name_ord = dict.exemplar_attr_ordinals[exemplar_attr_cursor];
                let value_ord = dict.exemplar_attr_ordinals[exemplar_attr_cursor + 1];
                exemplar_attr_cursor += 2;
                attr_ords.push((name_ord, value_ord));
            }
            resolved_exemplars.push(ResolvedExemplar {
                series_index,
                ts_ns: e.ts_ns,
                value: e.value,
                trace_id: e.trace_id,
                span_id: e.span_id,
                attr_ords,
            });
        }
        resolved_exemplars.sort_by_key(|r| (r.series_index, r.ts_ns));

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

        if !val_pages.is_empty() {
            let val_pad = (8 - (object.len() % 8)) % 8;
            object.extend(std::iter::repeat_n(0u8, val_pad));
            let val_pages_offset = object.len() as u64;
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

        if !resolved_exemplars.is_empty() {
            let exemplars_raw = encode_exemplars_section(&resolved_exemplars, min_event_ts_ns)?;
            let exemplars_offset = object.len() as u64;
            object.extend_from_slice(&exemplars_raw);
            sections.push(Section {
                kind: section_kind::EXEMPLARS,
                offset: exemplars_offset,
                len: exemplars_raw.len() as u64,
                crc32c: crc32c::crc32c(&exemplars_raw),
                comp: compression::NONE,
                uncompressed_len: exemplars_raw.len() as u64,
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

    /// Verbatim pre-#813 `retrailer_v4_to_v6`, the function this issue
    /// deleted: a full-object copy plus a second `footer_crc` and a second
    /// whole-object blake3.
    fn old_retrailer_v4_to_v6(base: WrittenSegment) -> WrittenSegment {
        let obj = base.bytes.as_ref();
        let total = obj.len();
        let trailer_start = total - crate::format::TRAILER_LEN as usize;
        let footer_len = u32::from_le_bytes([
            obj[total - 16],
            obj[total - 15],
            obj[total - 14],
            obj[total - 13],
        ]);
        let footer_end = trailer_start;
        let footer_start = footer_end - footer_len as usize;
        let footer_bytes = &obj[footer_start..footer_end];
        let crc = footer_crc(
            footer_bytes,
            footer_len,
            VERSION_V6,
            SIGNAL_METRICS,
            RESERVED,
        );

        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&obj[..footer_end]);
        out.extend_from_slice(&footer_len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&VERSION_V6.to_le_bytes());
        out.push(SIGNAL_METRICS);
        out.push(RESERVED);
        out.extend_from_slice(&MAGIC);

        let blake3 = *blake3::hash(&out).as_bytes();
        let mut summary = base.summary;
        summary.blake3 = blake3;
        WrittenSegment {
            bytes: Bytes::from(out),
            summary,
        }
    }

    fn old_write_v5_with_exemplars(
        series: Vec<SeriesInputV4>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        meta: CompactionMetaV4,
        exemplars: Vec<ExemplarInput>,
    ) -> Result<WrittenSegment, WriteError> {
        let base = old_write_v4(series, identity, ingest_bounds, meta, exemplars)?;
        if base.summary.series_count < V5_SPARSE_THRESHOLD {
            return Ok(old_retrailer_v4_to_v6(base));
        }
        crate::sparse::build_sparse_object(&base)
    }

    fn old_write_histograms_with_exemplars(
        mut series: Vec<SeriesInputV3>,
        identity: SegmentIdentity,
        ingest_bounds: IngestBounds,
        exemplars: Vec<ExemplarInput>,
    ) -> Result<WrittenSegment, WriteError> {
        for s in &mut series {
            s.values.sort_by_ts();
        }
        series.retain(|s| !s.values.is_empty());

        let created_unix_ns = ingest_bounds.max_ingest_ts_ns;
        let ingest_hour_bucket =
            u32::try_from(created_unix_ns.div_euclid(NS_PER_HOUR)).unwrap_or(0);

        let mut v4_series = Vec::with_capacity(series.len());
        for s in series {
            let sample_count =
                u32::try_from(s.values.len()).map_err(|_| WriteError::TooManySamples)?;
            let min_ts_ns = s.values.first_ts().unwrap_or(0);
            let max_ts_ns = s.values.last_ts().unwrap_or(0);
            let ts_page = old_frame_ts_page(&s.series_id, &old_ts_values(&s.values))?;
            let value_page = match &s.values {
                SeriesValues::Scalar(samples) => {
                    let vals: Vec<f64> = samples.iter().map(|sm| sm.value).collect();
                    RunValuePageV4::Scalar(old_frame_val_page(&s.series_id, &vals))
                }
                SeriesValues::Histogram(hist) => {
                    let vals: Vec<HistogramValue> = hist.iter().map(|h| h.value.clone()).collect();
                    RunValuePageV4::Histogram(old_frame_hist_page(&s.series_id, &vals)?)
                }
            };
            v4_series.push(SeriesInputV4 {
                series_id: s.series_id,
                labels: s.labels,
                runs: vec![RunInputV4 {
                    created_unix_ns,
                    writer_epoch: identity.writer_epoch,
                    writer_seq: identity.writer_seq,
                    min_ts_ns,
                    max_ts_ns,
                    sample_count,
                    ts_page,
                    value_page,
                }],
            });
        }

        let meta = CompactionMetaV4 {
            ingest_hour_bucket,
            input_set_hash: [0u8; 32],
            part_index: 0,
            level: 0,
        };
        old_write_v5_with_exemplars(v4_series, identity, ingest_bounds, meta, exemplars)
    }

    // ---- input generation: plain-data specs, instantiated twice (once for
    // the production writer, once for the old reference) so no `Clone` bound
    // is needed on the writer's own input types ----

    #[derive(Debug, Clone)]
    enum ValueSpec {
        Scalar(Vec<(i64, f64)>),
        Histogram(Vec<(i64, i32)>),
    }

    #[derive(Debug, Clone)]
    struct SeriesSpec {
        idx: u16,
        values: ValueSpec,
    }

    #[derive(Debug, Clone)]
    struct ExemplarSpec {
        target_idx: u16,
        ts_ns: i64,
        value: f64,
        trace_byte: u8,
        span_byte: u8,
        attr: Option<(String, String)>,
    }

    fn instantiate(specs: &[SeriesSpec]) -> Vec<SeriesInputV3> {
        specs
            .iter()
            .map(|spec| {
                let values = match &spec.values {
                    ValueSpec::Scalar(v) => SeriesValues::Scalar(
                        v.iter()
                            .map(|&(ts_ns, value)| Sample { ts_ns, value })
                            .collect(),
                    ),
                    ValueSpec::Histogram(v) => SeriesValues::Histogram(
                        v.iter()
                            .map(|&(ts_ns, seed)| HistogramSample {
                                ts_ns,
                                value: sample_histogram_value(seed),
                            })
                            .collect(),
                    ),
                };
                SeriesInputV3 {
                    series_id: series_id_for(spec.idx),
                    labels: labels(&format!("m{}", spec.idx)),
                    values,
                }
            })
            .collect()
    }

    fn instantiate_exemplars(specs: &[ExemplarSpec]) -> Vec<ExemplarInput> {
        specs
            .iter()
            .map(|e| ExemplarInput {
                series_id: series_id_for(e.target_idx),
                ts_ns: e.ts_ns,
                value: e.value,
                trace_id: [e.trace_byte; 16],
                span_id: [e.span_byte; 8],
                attrs: e.attr.clone().into_iter().collect(),
            })
            .collect()
    }

    fn fixed_identity() -> SegmentIdentity {
        SegmentIdentity {
            tenant_hash: [0x99; 16],
            shard: 7,
            writer_id: "parity-test-writer".to_string(),
            writer_epoch: 3,
            writer_seq: 11,
        }
    }

    fn fixed_bounds() -> IngestBounds {
        IngestBounds {
            min_ingest_ts_ns: -5_000,
            max_ingest_ts_ns: 50_000,
        }
    }

    /// Normalizes a write result into something comparable with `assert_eq!`
    /// (`WrittenSegment` itself carries `Bytes`, which is `PartialEq`, but
    /// bundled with a summary that is also `PartialEq`; unpacking both here
    /// makes a mismatch's assertion failure point at bytes vs. summary
    /// instead of just "not equal").
    fn normalize(
        result: Result<WrittenSegment, WriteError>,
    ) -> Result<(Vec<u8>, SegmentSummary), WriteError> {
        result.map(|w| (w.bytes.to_vec(), w.summary))
    }

    fn assert_parity(specs: Vec<SeriesSpec>, exemplar_specs: Vec<ExemplarSpec>) {
        let new_result = SegmentWriter::write_histograms_with_exemplars(
            instantiate(&specs),
            fixed_identity(),
            fixed_bounds(),
            instantiate_exemplars(&exemplar_specs),
        );
        let old_result = old_write_histograms_with_exemplars(
            instantiate(&specs),
            fixed_identity(),
            fixed_bounds(),
            instantiate_exemplars(&exemplar_specs),
        );
        assert_eq!(
            normalize(new_result),
            normalize(old_result),
            "direct-emit writer diverged from the pre-#813 reference"
        );
    }

    #[test]
    fn scalar_only() {
        let specs = vec![
            SeriesSpec {
                idx: 0,
                values: ValueSpec::Scalar(vec![(100, 1.0), (200, 2.0), (300, 3.0)]),
            },
            SeriesSpec {
                idx: 1,
                values: ValueSpec::Scalar(vec![(150, -1.5), (250, 0.0), (350, f64::MIN)]),
            },
        ];
        assert_parity(specs, Vec::new());
    }

    #[test]
    fn histogram_only() {
        let specs = vec![
            SeriesSpec {
                idx: 0,
                values: ValueSpec::Histogram(vec![(100, 5), (200, -3)]),
            },
            SeriesSpec {
                idx: 1,
                values: ValueSpec::Histogram(vec![(120, 0), (220, 42), (320, -17)]),
            },
        ];
        assert_parity(specs, Vec::new());
    }

    #[test]
    fn exemplar_carrying() {
        let specs = vec![
            SeriesSpec {
                idx: 0,
                values: ValueSpec::Scalar(vec![(100, 1.0), (200, 2.0)]),
            },
            SeriesSpec {
                idx: 1,
                values: ValueSpec::Histogram(vec![(150, 9)]),
            },
        ];
        let exemplars = vec![
            ExemplarSpec {
                target_idx: 0,
                ts_ns: 200,
                value: 42.5,
                trace_byte: 0xAB,
                span_byte: 0xCD,
                attr: Some(("trace_state".to_string(), "sampled=1".to_string())),
            },
            ExemplarSpec {
                target_idx: 1,
                ts_ns: 150,
                value: f64::NAN,
                trace_byte: 0,
                span_byte: 0,
                attr: None,
            },
        ];
        assert_parity(specs, exemplars);
    }

    /// A single sample makes Gorilla's own framing overhead exceed 8 bytes,
    /// so both the old and new `frame_val_page` fall back to VAL_RAW_F64
    /// (docs/segment-format.md "raw-fallback rule"); this exercises that
    /// branch under both algorithms.
    #[test]
    fn single_sample_series_raw_f64_fallback() {
        let specs = vec![SeriesSpec {
            idx: 0,
            values: ValueSpec::Scalar(vec![(42, 9.87654)]),
        }];
        assert_parity(specs, Vec::new());
    }

    /// Series A's TS page payload is comfortably over `LZ4_MIN_TS_PAYLOAD_BYTES`
    /// (64), forcing an lz4 compression attempt and growing the shared
    /// scratch buffer; series B's is comfortably under it, so no lz4 attempt.
    /// Both old (fresh `Vec` per series) and new (scratch `Vec`, cleared and
    /// reused across series) must produce the same page bytes for B: a
    /// `payload.clear()` that failed to actually truncate the buffer's
    /// logical length would leak A's leftover bytes into B's page.
    #[test]
    fn lz4_floor_edge_ts_pages_across_scratch_reuse() {
        let big: Vec<(i64, f64)> = (0..96i64).map(|i| (1_000 + i * 37, i as f64)).collect();
        let small = vec![(5i64, 1.0), (9i64, 2.0)];
        let specs = vec![
            SeriesSpec {
                idx: 0,
                values: ValueSpec::Scalar(big),
            },
            SeriesSpec {
                idx: 1,
                values: ValueSpec::Scalar(small),
            },
        ];
        assert_parity(specs, Vec::new());
    }

    /// At/above `V5_SPARSE_THRESHOLD` (4096), `write_v5_with_exemplars`
    /// finalizes the assembled body as version 4 (unchanged by issue #813)
    /// and feeds it to `crate::sparse::build_sparse_object`. Comparing
    /// against `old_write_v4` (the verbatim pre-#813 body, not today's
    /// `assemble_v4_body`) proves that intermediate v4 object is still
    /// byte-identical, not just that the sparse builder is unchanged.
    #[test]
    fn sparse_shape_at_threshold() {
        let n = V5_SPARSE_THRESHOLD as usize;
        let specs: Vec<SeriesSpec> = (0..n)
            .map(|i| {
                let idx = i as u16;
                let values = if i % 7 == 0 {
                    ValueSpec::Histogram(vec![(1_000 + i as i64, (i % 23) as i32 - 11)])
                } else {
                    ValueSpec::Scalar(vec![
                        (1_000 + i as i64, i as f64 * 0.5),
                        (2_000 + i as i64, i as f64 * 0.5 + 1.0),
                    ])
                };
                SeriesSpec { idx, values }
            })
            .collect();
        assert_parity(specs, Vec::new());
    }

    fn value_spec_strategy() -> impl Strategy<Value = ValueSpec> {
        prop_oneof![
            prop::collection::vec((-1_000_000i64..1_000_000, -1_000.0f64..1_000.0), 0..8)
                .prop_map(ValueSpec::Scalar),
            prop::collection::vec((-1_000_000i64..1_000_000, -64i32..64), 0..6)
                .prop_map(ValueSpec::Histogram),
        ]
    }

    fn series_specs_strategy() -> impl Strategy<Value = Vec<SeriesSpec>> {
        prop::collection::vec(value_spec_strategy(), 0..24).prop_map(|values| {
            values
                .into_iter()
                .enumerate()
                .map(|(i, values)| SeriesSpec {
                    idx: i as u16,
                    values,
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// General-shape parity: randomized scalar/histogram batches of
        /// varying series count, sample count, and value content, optionally
        /// carrying exemplars, must write identically under both algorithms.
        #[test]
        fn direct_v6_emit_bit_parity(
            specs in series_specs_strategy(),
            exemplar_raw in prop::collection::vec(
                (any::<u16>(), any::<i64>(), -1_000.0f64..1_000.0, any::<u8>(), any::<u8>(), any::<bool>()),
                0..4,
            ),
        ) {
            let exemplar_specs: Vec<ExemplarSpec> = if specs.is_empty() {
                Vec::new()
            } else {
                exemplar_raw
                    .into_iter()
                    .map(|(idx, ts_ns, value, trace_byte, span_byte, has_attr)| ExemplarSpec {
                        target_idx: idx % specs.len() as u16,
                        ts_ns,
                        value,
                        trace_byte,
                        span_byte,
                        attr: has_attr.then(|| ("k".to_string(), "v".to_string())),
                    })
                    .collect()
            };

            let new_result = SegmentWriter::write_histograms_with_exemplars(
                instantiate(&specs),
                fixed_identity(),
                fixed_bounds(),
                instantiate_exemplars(&exemplar_specs),
            );
            let old_result = old_write_histograms_with_exemplars(
                instantiate(&specs),
                fixed_identity(),
                fixed_bounds(),
                instantiate_exemplars(&exemplar_specs),
            );
            prop_assert_eq!(normalize(new_result), normalize(old_result));
        }
    }
}

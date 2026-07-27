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
    MAGIC, RESERVED, SIGNAL_METRICS, VERSION, VERSION_V2, ZSTD_LEVEL, compression, page_comp,
    page_enc, section_kind,
};
use crate::gorilla::encode_gorilla_into;
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
fn build_dictionary(series: &[SeriesInput]) -> Result<Dictionary<'_>, WriteError> {
    let total_strings: usize = series.iter().map(|s| s.labels.len() * 2).sum();
    let mut interner: HashMap<&str, u32, BuildHasherDefault<FnvHasher>> =
        HashMap::with_capacity_and_hasher(total_strings, BuildHasherDefault::default());
    let mut distinct: Vec<&str> = Vec::new();
    let mut occurrence_ordinals: Vec<u32> = Vec::with_capacity(total_strings);

    for s in series {
        for label in s.labels.iter() {
            for text in [label.name.as_str(), label.value.as_str()] {
                let id = match interner.entry(text) {
                    std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let id = u32::try_from(distinct.len())
                            .map_err(|_| WriteError::TooManyDictStrings)?;
                        distinct.push(text);
                        e.insert(id);
                        id
                    }
                };
                occurrence_ordinals.push(id);
            }
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

    Ok(DictionaryV2 {
        order,
        occurrence_ordinals,
        count: next_ordinal,
    })
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

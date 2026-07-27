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
    MAGIC, RESERVED, SIGNAL_METRICS, VERSION, ZSTD_LEVEL, compression, page_comp, page_enc,
    section_kind,
};
use crate::gorilla::encode_gorilla_into;
use crate::ts_delta::encode_ts_deltas_into;
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

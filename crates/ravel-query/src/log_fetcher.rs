//! LogSegmentFetcher: a thin fetch abstraction over one RLOG log segment
//! (crate `ravel-logseg`, docs/log-segment-format.md; ADR-0033, epic #236).
//!
//! This is the log-signal sibling of [`crate::SegmentFetcher`], which serves
//! the RSEG metric path. The two never share code and never touch each other:
//! RSEG and RLOG share only conventions, not bytes. Where `SegmentFetcher`
//! re-implements the footer-first suffix-GET / range-chase / decode protocol
//! itself, an RLOG object is read through [`ravel_logseg::RlogReader`], which
//! already performs the whole open/prune/verify/decode pipeline internally.
//! This wrapper therefore does only what the reader cannot: it decides
//! per-object relevance from the catalog summary before fetching anything,
//! resolves stream-identifying attribute equalities against the object's
//! STREAM_DIR, and combines those with the caller's ts-range and word/phrase
//! predicates into one [`ravel_logseg::Predicate`] handed to
//! [`RlogReader::scan`]. Skip-index and bloom pruning stay entirely inside
//! `scan`; nothing here duplicates format-layer logic.
//!
//! v1 fetches the whole object with a single [`GetRange::Full`]. RLOG objects
//! are not yet large enough to justify the suffix-then-range-chase read
//! `SegmentFetcher` uses for RSEG; see the module note in the issue #238
//! report for when that may deserve revisiting.

use std::sync::Arc;

use ravel_catalog::SegmentRef;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{
    AttrValue, LogRecord, LogSegError, LogStreamId, Predicate, RlogConfig, RlogReader, ScanStats,
    read_section,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_types::logstream::canonical_attr_bytes;

/// Upper bound on STREAM_DIR entries accepted when decoding the directory out
/// of band (mirrors the reader's own internal cap). A directory claiming more
/// is treated as corrupt rather than allocated.
const MAX_STREAMS: u64 = 1 << 24;

/// An equality on a stream-identifying attribute: a resource or scope
/// attribute whose `(key, value)` participates in [`LogStreamId`] identity
/// (docs/log-segment-format.md "STREAM_DIR"). These are resolved against the
/// object's STREAM_DIR into a concrete set of matching stream ids, never
/// evaluated per record (per-record attributes are not part of stream
/// identity and are matched through [`Predicate`] instead).
#[derive(Clone, Debug, PartialEq)]
pub struct StreamAttrEquals {
    pub key: String,
    pub value: AttrValue,
}

impl StreamAttrEquals {
    pub fn new(key: impl Into<String>, value: AttrValue) -> Self {
        StreamAttrEquals {
            key: key.into(),
            value,
        }
    }
}

/// One log query against a single segment: an inclusive ts range, zero or more
/// stream-attribute equalities (ANDed, resolved against STREAM_DIR), and zero
/// or more content predicates (`HasWord`/`Equals`, ANDed, passed straight to
/// the reader). The ts range is always applied; the other two are optional.
#[derive(Clone, Debug, PartialEq)]
pub struct LogQuery {
    pub ts_min_ns: i64,
    pub ts_max_ns: i64,
    pub stream_attrs: Vec<StreamAttrEquals>,
    pub content: Vec<Predicate>,
}

impl LogQuery {
    /// A query over the inclusive ts range `[ts_min_ns, ts_max_ns]` with no
    /// stream-attribute or content predicates.
    pub fn new(ts_min_ns: i64, ts_max_ns: i64) -> Self {
        LogQuery {
            ts_min_ns,
            ts_max_ns,
            stream_attrs: Vec::new(),
            content: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_stream_attr(mut self, filter: StreamAttrEquals) -> Self {
        self.stream_attrs.push(filter);
        self
    }

    #[must_use]
    pub fn with_content(mut self, pred: Predicate) -> Self {
        self.content.push(pred);
        self
    }
}

/// The records matching one fetch, plus the reader's own scan pruning counters.
#[derive(Clone, Debug)]
pub struct LogFetchOutput {
    pub records: Vec<LogRecord>,
    pub stats: ScanStats,
}

/// Errors fetching and decoding one RLOG segment. Every variant is a hard
/// error: the caller never receives partial or silently-wrong data.
#[derive(Debug, thiserror::Error)]
pub enum LogFetchError {
    #[error("object store error reading log segment {key}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("corrupt log segment {key}: {source}")]
    Corrupt {
        key: String,
        #[source]
        source: LogSegError,
    },
}

/// Fetches and scans one RLOG log segment at a time. Constructed with the same
/// [`ObjectStoreBackend`] trait object [`crate::SegmentFetcher`] takes.
#[derive(Clone)]
pub struct LogSegmentFetcher {
    store: Arc<dyn ObjectStoreBackend>,
    cfg: RlogConfig,
}

impl LogSegmentFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        LogSegmentFetcher {
            store,
            cfg: RlogConfig::default(),
        }
    }

    /// Overrides the [`RlogConfig`] used for section-size caps when decoding.
    #[must_use]
    pub fn with_config(mut self, cfg: RlogConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Per-object relevance from the catalog summary alone, with no object
    /// read: true iff the segment's event-ts span (`SegmentRef`'s
    /// `min_event_ts_ns..=max_event_ts_ns`, the same bounds the footer carries)
    /// overlaps the inclusive query range. A `false` return lets [`fetch`] skip
    /// the object without a GET, which is the point of pruning by time before
    /// touching object storage.
    ///
    /// [`fetch`]: Self::fetch
    #[must_use]
    pub fn ts_range_relevant(seg_ref: &SegmentRef, ts_min_ns: i64, ts_max_ns: i64) -> bool {
        seg_ref.min_event_ts_ns <= ts_max_ns && ts_min_ns <= seg_ref.max_event_ts_ns
    }

    /// Resolves stream-attribute equalities against an already-fetched object's
    /// STREAM_DIR, returning the ids of streams whose canonical resource+scope
    /// blob satisfies every filter (ANDed). An empty `filters` returns every
    /// stream in the object.
    ///
    /// Matching is by canonical-byte containment: each filter's `(key, value)`
    /// is encoded with the frozen [`canonical_attr_bytes`] grammar and searched
    /// for as a contiguous sub-sequence of the stored blob. Because the writer
    /// emits each attribute entry's `len(key) key encode_value(value)` bytes
    /// contiguously (only the entry *order* is canonicalized), a stream that
    /// truly carries the attribute always matches: there are no false
    /// negatives. See the issue #238 report for the (theoretical) false-positive
    /// bound and the note that an exact structured match would need a public
    /// STREAM_DIR blob decoder in `ravel-logseg`, which this task does not add.
    pub fn matching_streams(
        &self,
        bytes: &[u8],
        filters: &[StreamAttrEquals],
    ) -> Result<Vec<LogStreamId>, LogSegError> {
        let dir = self.decode_stream_dir(bytes)?;
        let needles: Vec<Vec<u8>> = filters.iter().map(stream_attr_needle).collect();
        let mut out = Vec::new();
        for entry in dir.entries() {
            if needles.iter().all(|n| blob_contains(&entry.blob, n)) {
                out.push(entry.stream_id);
            }
        }
        Ok(out)
    }

    /// Fetches, prunes, and scans one segment for records matching `query`.
    ///
    /// The ts-range relevance pre-check runs first, from the catalog summary
    /// only: an object whose span cannot satisfy the range returns `Ok(None)`
    /// with no GET. Otherwise the whole object is fetched once
    /// ([`GetRange::Full`]), the STREAM_DIR is consulted to resolve any
    /// stream-attribute equalities into a [`Predicate::StreamIn`], and the
    /// combined predicate (ts range AND resolved streams AND content) is handed
    /// to [`RlogReader::scan`], whose skip-index and bloom pruning do the
    /// block-level work.
    pub async fn fetch(
        &self,
        seg_ref: &SegmentRef,
        query: &LogQuery,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
            return Ok(None);
        }

        let key = &seg_ref.data_object_key;
        let got = self
            .store
            .get(key, GetRange::Full)
            .await
            .map_err(|source| LogFetchError::Store {
                key: key.to_string(),
                source,
            })?;
        let bytes = got.data;

        // Resolve stream-attribute equalities against STREAM_DIR before the
        // scan, so they become an exact StreamIn arm the reader prunes on.
        let stream_ids = if query.stream_attrs.is_empty() {
            None
        } else {
            Some(
                self.matching_streams(&bytes, &query.stream_attrs)
                    .map_err(|source| corrupt(key, source))?,
            )
        };

        let mut arms = Vec::with_capacity(2 + query.content.len());
        arms.push(Predicate::TsRange {
            min_ns: query.ts_min_ns,
            max_ns: query.ts_max_ns,
        });
        if let Some(ids) = stream_ids {
            // An empty set is intentional: it means no stream in this object
            // satisfies the attribute filter, and the reader short-circuits an
            // empty StreamIn to zero records.
            arms.push(Predicate::StreamIn(ids));
        }
        arms.extend(query.content.iter().cloned());
        let pred = Predicate::And(arms);

        let reader = RlogReader::new(&bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        let (records, stats) = reader.scan(&pred).map_err(|source| corrupt(key, source))?;
        Ok(Some(LogFetchOutput { records, stats }))
    }

    /// Decodes the STREAM_DIR section of an object from its own public section
    /// descriptor, using the crate's public whole-section reader (issue #221).
    /// This does not go through [`RlogReader`], which decodes STREAM_DIR
    /// internally but exposes no accessor for it.
    fn decode_stream_dir(&self, bytes: &[u8]) -> Result<StreamDir, LogSegError> {
        let footer = footer::open(bytes)?;
        let desc = footer
            .section(kind::STREAM_DIR)
            .ok_or_else(|| LogSegError::Corrupted("missing STREAM_DIR section".into()))?;
        let raw = read_section(bytes, desc, &self.cfg)?;
        StreamDir::decode(&raw, MAX_STREAMS)
    }
}

/// The canonical-byte needle for one stream-attribute equality: the single
/// `(key, value)` entry as it appears inside a larger canonical attribute set,
/// i.e. `canonical_attr_bytes([(key, value)])` with its leading one-entry count
/// varint stripped. The count of a single-entry set is `1`, a one-byte varint,
/// so exactly one leading byte is removed.
fn stream_attr_needle(filter: &StreamAttrEquals) -> Vec<u8> {
    let full = canonical_attr_bytes(std::slice::from_ref(&(
        filter.key.clone(),
        filter.value.clone(),
    )));
    // `encode_attrs` writes the entry count first; for one entry it is the
    // single byte 0x01. Everything after is `len(key) key encode_value(value)`.
    full.get(1..).unwrap_or(&[]).to_vec()
}

/// True if `needle` occurs as a contiguous sub-sequence of `blob`. An empty
/// needle matches everything.
fn blob_contains(blob: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > blob.len() {
        return false;
    }
    blob.windows(needle.len()).any(|w| w == needle)
}

fn corrupt(key: &str, source: LogSegError) -> LogFetchError {
    LogFetchError::Corrupt {
        key: key.to_string(),
        source,
    }
}

//! `SpanSegmentFetcher`: a thin fetch abstraction over one RSPAN span segment
//! (crate `ravel-rspan`, docs/span-segment-format.md; ADR-0041, phase 5).
//!
//! This is the span-signal sibling of `ravel-query`'s `LogSegmentFetcher`. It
//! lives here, in `ravel-sql`, rather than in `ravel-query` for one reason:
//! ADR-0041 phase 5 (this task) is the `spans` SQL table alone, and no span
//! query surface exists in `ravel-query` yet (`LogSegmentFetcher`'s home). The
//! fetcher is small, span-specific, and used only by [`crate::spans_scan`], so
//! keeping it beside the scan is the least-coupling choice until a broader span
//! query path (trace-by-id endpoint, service graph) earns a home in
//! `ravel-query`. See the crate report.
//!
//! Like the log fetcher, this does only what the format reader cannot: it
//! decides per-object relevance from the catalog summary before fetching
//! anything. Everything else -- skip-index block pruning (by trace_id range and
//! time-interval overlap), decode, and exact per-row re-evaluation -- happens
//! inside [`ravel_rspan::RspanReader::scan`]; nothing here duplicates
//! format-layer logic.
//!
//! v1 fetches the whole object with a single [`GetRange::Full`], matching the
//! log fetcher: RSPAN objects are not yet large enough to justify a
//! suffix-then-range-chase read.

use std::sync::Arc;

use ravel_catalog::SegmentRef;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_rspan::{RspanConfig, RspanReader, ScanStats, SpanQuery, SpanRecord, SpanSegError};

/// The records matching one fetch, plus the reader's own scan pruning counters.
/// [`ScanStats`] is what proves the trace fast path is cheaper: a
/// [`SpanQuery::trace`] lookup scans strictly fewer blocks than the bare
/// [`SpanQuery::ts_range`] window would over the same object.
#[derive(Clone, Debug)]
pub struct SpanFetchOutput {
    pub records: Vec<SpanRecord>,
    pub stats: ScanStats,
}

/// Errors fetching and decoding one RSPAN segment. Every variant is a hard
/// error: the caller never receives partial or silently-wrong data. Mirrors
/// `ravel_query::LogFetchError` so [`crate::error::SqlError`] can redact it the
/// same way (the `Display` embeds the object key, logged server-side only).
#[derive(Debug, thiserror::Error)]
pub enum SpanFetchError {
    #[error("object store error reading span segment {key}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("corrupt span segment {key}: {source}")]
    Corrupt {
        key: String,
        #[source]
        source: SpanSegError,
    },
}

/// Fetches and scans one RSPAN span segment at a time. Constructed with the
/// same [`ObjectStoreBackend`] trait object the log/metric fetchers take.
#[derive(Clone)]
pub struct SpanSegmentFetcher {
    store: Arc<dyn ObjectStoreBackend>,
    cfg: RspanConfig,
}

impl SpanSegmentFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        SpanSegmentFetcher {
            store,
            cfg: RspanConfig::default(),
        }
    }

    /// Overrides the [`RspanConfig`] used for section-size caps when decoding.
    #[must_use]
    pub fn with_config(mut self, cfg: RspanConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Per-object relevance from the catalog summary alone, with no object
    /// read: true iff the segment's event-ts span
    /// (`min_event_ts_ns..=max_event_ts_ns`) overlaps the inclusive query
    /// window. A `false` return lets [`fetch`] skip the object without a GET.
    ///
    /// A span object's summary ts span is `[min_start_ts, max_end_ts]` (the
    /// footer's whole-object interval), so this is the same interval-overlap
    /// test the block-level skip index applies, just at object granularity.
    ///
    /// [`fetch`]: Self::fetch
    #[must_use]
    pub fn ts_range_relevant(seg_ref: &SegmentRef, ts_min_ns: i64, ts_max_ns: i64) -> bool {
        seg_ref.min_event_ts_ns <= ts_max_ns && ts_min_ns <= seg_ref.max_event_ts_ns
    }

    /// Fetches, prunes, and scans one segment for spans matching `query`.
    ///
    /// The ts-range relevance pre-check runs first, from the catalog summary
    /// only: an object whose span cannot overlap the window returns `Ok(None)`
    /// with no GET. Otherwise the whole object is fetched once
    /// ([`GetRange::Full`]) and handed to [`RspanReader::scan`], whose
    /// skip-index pruning (by the query's trace_id range and time interval) and
    /// exact per-row re-evaluation do the block-level work.
    ///
    /// Every returned record satisfies `query` exactly: the ts-interval overlap
    /// and (when set) the trace_id equality are re-checked per row by the
    /// reader. `stats` reports how much the skip index pruned.
    pub async fn fetch(
        &self,
        seg_ref: &SegmentRef,
        query: &SpanQuery,
    ) -> Result<Option<SpanFetchOutput>, SpanFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min, query.ts_max) {
            return Ok(None);
        }

        let key = &seg_ref.data_object_key;
        let got = self
            .store
            .get(key, GetRange::Full)
            .await
            .map_err(|source| SpanFetchError::Store {
                key: key.to_string(),
                source,
            })?;
        let bytes = got.data;

        let reader = RspanReader::new(&bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        let (records, stats) = reader.scan(query).map_err(|source| corrupt(key, source))?;
        Ok(Some(SpanFetchOutput { records, stats }))
    }
}

fn corrupt(key: &str, source: SpanSegError) -> SpanFetchError {
    SpanFetchError::Corrupt {
        key: key.to_string(),
        source,
    }
}

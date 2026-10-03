//! Which gate a job belongs to and which call site submitted it.
//!
//! Both are closed enums so the `gate` and `site` labels a server renders
//! from them stay bounded. A site type belongs to exactly one gate
//! ([`GateSite::GATE`]), and [`crate::CpuGate`] is generic over it, so a
//! write-path site submitted to the read gate is a compile error rather than
//! a series under the wrong `gate` label.

use std::fmt::Debug;

/// The two gate instances a server builds (ADR-1702 decision 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GateKind {
    /// Query, catalog and maintenance decode, and large PromQL evaluations.
    Read,
    /// Flush encode and ingest payload decode.
    Write,
}

impl GateKind {
    /// The `gate` label value.
    pub fn name(self) -> &'static str {
        match self {
            GateKind::Read => "read",
            GateKind::Write => "write",
        }
    }
}

/// A closed set of call sites that submit work to one gate.
pub trait GateSite: Copy + Debug + Send + Sync + 'static {
    /// The gate every site of this type runs on.
    const GATE: GateKind;
    /// Every site, in [`GateSite::index`] order.
    const ALL: &'static [Self];
    /// Position of this site in [`GateSite::ALL`].
    fn index(self) -> usize;
    /// The `site` label value.
    fn name(self) -> &'static str;
}

/// Call sites on the read gate, one per unit the ADR-1702 follow-up tasks
/// wrap at its async caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadSite {
    /// A catalog snapshot part (`snapshot_format::part`).
    CatalogPart,
    /// A catalog postings object (`snapshot_format::postings`).
    CatalogPostings,
    /// A catalog column-stats object (`snapshot_format::column_stats`).
    CatalogColumnStats,
    /// A metrics-meta body (`ravel-catalog/src/metrics_meta.rs`).
    MetricsMeta,
    /// One RSEG section (`SegmentFetcher::decode_selected`).
    SegmentSection,
    /// An RSEG sparse catalog (`decode_catalog_v5_chunked`).
    SegmentSparseCatalog,
    /// One RLOG block with all its pages.
    LogBlock,
    /// One RLOG postings block.
    LogPostings,
    /// One RLOG footer section.
    LogSection,
    /// One RSPAN block with all its pages.
    SpanBlock,
    /// One RSPAN footer section.
    SpanSection,
    /// A PromQL evaluation at or above the evaluation floor.
    PromqlEval,
    /// Compaction decode and re-encode.
    Compaction,
    /// Catalog fold reads.
    Fold,
    /// At-rest scrub reads.
    Scrub,
    /// Reachability reads.
    Reachability,
}

impl GateSite for ReadSite {
    const GATE: GateKind = GateKind::Read;
    const ALL: &'static [Self] = &[
        ReadSite::CatalogPart,
        ReadSite::CatalogPostings,
        ReadSite::CatalogColumnStats,
        ReadSite::MetricsMeta,
        ReadSite::SegmentSection,
        ReadSite::SegmentSparseCatalog,
        ReadSite::LogBlock,
        ReadSite::LogPostings,
        ReadSite::LogSection,
        ReadSite::SpanBlock,
        ReadSite::SpanSection,
        ReadSite::PromqlEval,
        ReadSite::Compaction,
        ReadSite::Fold,
        ReadSite::Scrub,
        ReadSite::Reachability,
    ];

    fn index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        match self {
            ReadSite::CatalogPart => "catalog_part",
            ReadSite::CatalogPostings => "catalog_postings",
            ReadSite::CatalogColumnStats => "catalog_column_stats",
            ReadSite::MetricsMeta => "metrics_meta",
            ReadSite::SegmentSection => "segment_section",
            ReadSite::SegmentSparseCatalog => "segment_sparse_catalog",
            ReadSite::LogBlock => "log_block",
            ReadSite::LogPostings => "log_postings",
            ReadSite::LogSection => "log_section",
            ReadSite::SpanBlock => "span_block",
            ReadSite::SpanSection => "span_section",
            ReadSite::PromqlEval => "promql_eval",
            ReadSite::Compaction => "compaction",
            ReadSite::Fold => "fold",
            ReadSite::Scrub => "scrub",
            ReadSite::Reachability => "reachability",
        }
    }
}

/// Call sites on the write gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteSite {
    /// Metrics shard flush encode.
    MetricsFlush,
    /// Log shard flush encode.
    LogFlush,
    /// Span shard flush encode.
    SpanFlush,
    /// OTAP payload decompression.
    OtapDecode,
    /// OTLP-HTTP gzip decompression.
    OtlpHttpGzip,
    /// Remote-write snappy decompression.
    RemoteWriteSnappy,
}

impl GateSite for WriteSite {
    const GATE: GateKind = GateKind::Write;
    const ALL: &'static [Self] = &[
        WriteSite::MetricsFlush,
        WriteSite::LogFlush,
        WriteSite::SpanFlush,
        WriteSite::OtapDecode,
        WriteSite::OtlpHttpGzip,
        WriteSite::RemoteWriteSnappy,
    ];

    fn index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        match self {
            WriteSite::MetricsFlush => "metrics_flush",
            WriteSite::LogFlush => "log_flush",
            WriteSite::SpanFlush => "span_flush",
            WriteSite::OtapDecode => "otap_decode",
            WriteSite::OtlpHttpGzip => "otlp_http_gzip",
            WriteSite::RemoteWriteSnappy => "remote_write_snappy",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn assert_closed_set<S: GateSite>() {
        for (position, site) in S::ALL.iter().enumerate() {
            assert_eq!(site.index(), position, "{site:?} is out of ALL order");
        }
        let names: HashSet<&str> = S::ALL.iter().map(|site| site.name()).collect();
        assert_eq!(names.len(), S::ALL.len(), "site names must be distinct");
    }

    #[test]
    fn site_indexes_follow_all_order_and_names_are_distinct() {
        assert_closed_set::<ReadSite>();
        assert_closed_set::<WriteSite>();
        assert_eq!(ReadSite::ALL.len(), 16);
        assert_eq!(WriteSite::ALL.len(), 6);
    }
}

//! JSON rendering of a query's per-phase accounting and I/O shape (issue
//! #1367), for the `/api/v1/sql` response's `stats` object.
//!
//! Field names and phase ordering match `ravel_query::http::json`'s PromQL
//! `stats.phases`/`stats.io` rendering exactly, so a client reading both
//! surfaces sees one convention. That module's own phase-cost builder is
//! private to it, so the shape is rebuilt here from the same public
//! `QueryPhase`/`PhaseAccountingSnapshot`/`QueryIoShape` types rather than
//! reused; see this crate's `Refs: #1367` commit for that trade-off.
//!
//! # Which bytes each byte figure counts
//!
//! Every byte field is a WIRE byte figure -- the byte length of each
//! successful GET response body, retries excluded (a retried GET's failed
//! attempts never reach this counter; `object_store` retries beneath
//! `InstrumentedStore`, so a GET that retried counts once here) and range
//! reads included (a range GET's counter is the bytes the range itself
//! returned) -- with two named exceptions: `cacheServedBytes` never crossed
//! the wire (served from the in-process cache) and `decompressedOutputBytes`
//! is a decode-time output size no request paid for. The three are never
//! comparable or summable with each other.

use ravel_query::io_shape::QueryIoShape;
use ravel_query::phase_accounting::{PhaseAccountingSnapshot, QueryPhase};
use ravel_types::accounting::AccountedOp;
use serde_json::{Value as Json, json};

/// One entry per [`QueryPhase`], in [`QueryPhase::ALL`] order, each phase
/// exactly once: a phase added to the enum cannot be silently dropped from
/// this response.
pub fn phase_costs_json(snapshot: &PhaseAccountingSnapshot) -> Vec<Json> {
    QueryPhase::ALL
        .iter()
        .map(|phase| {
            let s = snapshot.phase(*phase);
            json!({
                "phase": phase.name(),
                "s3GetRequests": s.s3_requests(AccountedOp::Get),
                "s3GetWireBytes": s.s3_bytes(AccountedOp::Get),
                "s3ListRequests": s.s3_requests(AccountedOp::List),
                "cacheHits": s.cache_hits,
                "cacheMisses": s.cache_misses,
                "cacheServedBytes": s.cache_bytes,
                "decompressedOutputBytes": s.decompressed_bytes,
                "reusedRegionBytes": s.bytes_reused,
                "segmentsOpened": s.segments_opened,
                "seriesMatched": s.series_matched,
            })
        })
        .collect()
}

/// This query's I/O dependency shape, rendered under `stats.io`. See
/// `ravel_query::io_shape`'s module docs for what each figure means.
pub fn io_shape_json(shape: &QueryIoShape) -> Json {
    json!({
        "dependencyDepth": shape.dependency_depth,
        "listPageDepth": shape.list_page_depth,
        "serviceBatches": shape.service_batches,
        "unfoldedSegmentsResolved": shape.unfolded_segments_resolved,
        "unfoldedRecordsServedFromCache": shape.unfolded_records_served_from_cache,
        "planClass": shape.plan_class.name(),
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_query::phase_accounting::PhaseAccounting;

    use super::*;

    /// Every phase renders exactly once, in `resolve`/`plan`/`probe`/`scan`
    /// order, with no phase missing and none duplicated: a phase emitted
    /// twice or missing here fails a client's per-phase sum the same way a
    /// wrong number does.
    #[test]
    fn phase_costs_json_names_every_phase_exactly_once() {
        let phases = PhaseAccounting::new();
        let snapshot = phases.snapshot();
        let entries = phase_costs_json(&snapshot);
        let names: Vec<&str> = entries
            .iter()
            .map(|e| e["phase"].as_str().expect("phase is a string"))
            .collect();
        assert_eq!(names, vec!["resolve", "plan", "probe", "scan"]);
    }

    /// A query's per-phase requests and bytes sum back to the pooled total
    /// `QueryAccounting` (and thus `stats.accounting`) reports, by exact
    /// value: this is the assertion the summing-demonstration test in
    /// `executor.rs` also pins.
    #[test]
    fn phase_costs_json_sums_to_the_pooled_total() {
        let phases = PhaseAccounting::new();
        for (phase, bytes) in [
            (phases.resolve(), 100u64),
            (phases.plan(), 10),
            (phases.probe(), 1),
            (phases.scan(), 1000),
        ] {
            phase.record_s3_request(AccountedOp::Get);
            phase.add_s3_bytes(AccountedOp::Get, bytes);
        }

        let snapshot = phases.snapshot();
        let entries = phase_costs_json(&snapshot);
        let total_bytes: u64 = entries
            .iter()
            .map(|e| e["s3GetWireBytes"].as_u64().expect("u64"))
            .sum();
        let total_requests: u64 = entries
            .iter()
            .map(|e| e["s3GetRequests"].as_u64().expect("u64"))
            .sum();
        assert_eq!(
            total_bytes, 1111,
            "100 + 10 + 1 + 1000 resolve/plan/probe/scan"
        );
        assert_eq!(total_requests, 4, "one GET recorded per phase");
        assert_eq!(total_bytes, snapshot.pooled().total_s3_bytes());
        assert_eq!(total_requests, snapshot.pooled().total_s3_requests());
    }
}

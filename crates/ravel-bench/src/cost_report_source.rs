//! Projecting the shipping [`BenchReport`] into the [`CostReport`] the
//! request-cost regression gate compares (ADR-0996 workstream F, task 996-7).
//!
//! The gate is a comparison TOOL over an explicit figure list. This module is
//! the adapter that lets it read the report the bench actually writes
//! (`bench_report --out`), so a caller does not hand-transcribe figures from one
//! JSON document into another.
//!
//! # The field map
//!
//! | figure | class | `BenchReport` source | unit |
//! |---|---|---|---|
//! | `write_class_requests` | `write_class_requests` | `s3_requests.put_class_attempts()` | billed-attempts |
//! | `write_class_calls` | `write_class_requests` | `s3_requests.put + s3_requests.list` | calls |
//! | `data_gets` | `data_gets` | `s3_requests.get_attempts` | billed-attempts |
//! | `data_get_calls` | `data_gets` | `s3_requests.get` | calls |
//! | `modeled_request_cost` | `modeled_request_cost` | `modeled_cost.modeled_request_cost_nanodollars` | nanodollars |
//! | `modeled_transfer_cost` | `modeled_request_cost` | `modeled_cost.modeled_transfer_cost_nanodollars` | nanodollars |
//! | `modeled_retrieval_cost` | `modeled_request_cost` | `modeled_cost.modeled_retrieval_cost_nanodollars` | nanodollars |
//! | `bytes_written` | `bytes` | `bytes.written` | wire |
//! | `bytes_read` | `bytes` | `bytes.read` | wire |
//! | `ingest_ack_latency_p50` / `_p95` | `latency_p50` / `latency_p95` | `ingest.strict_ack_latency_ms` | ms |
//! | `query_cold_latency_p50` / `_p95` | `latency_p50` / `latency_p95` | `query.cold_latency_ms` | ms |
//! | `query_warm_latency_p50` / `_p95` | `latency_p50` / `latency_p95` | `query.warm_latency_ms` | ms |
//!
//! Two properties the projection preserves, because the gate's whole discipline
//! rests on them:
//!
//! - **Present exactly once.** Every figure above is emitted once per report and
//!   carries a distinct name, so the duplicate detector stays meaningful.
//! - **Absent is not zero.** A `BenchReport` field that is `Option::None`
//!   (`RequestCounts::get_attempts` on a backend with no attempt source,
//!   `ModeledCost::modeled_request_cost_nanodollars` when attempts are absent)
//!   projects to a figure whose VALUE is `None`. It is never flattened to `0.0`,
//!   which would compare as a real measurement of nothing.
//!
//! The request-count figures split attempts from calls under distinct names and
//! distinct units precisely so the two can never be compared to each other: a
//! billed attempt and a completed call are different quantities, and the unit
//! guard refuses a pair whose units differ.
//!
//! # The gap
//!
//! `BenchReport` carries no figure at all for four classes -- `object_count`,
//! `range_amplification`, `ranged_opens`, `peak_memory` -- so their bands go
//! unenforced on this source. [`BenchReportProjection::gaps`] names them and
//! [`BenchReportProjection::gap_note`] renders the line the `cost_regression_check`
//! binary prints above its table, so the omission is visible in the gate's own
//! output rather than inferred from a short table. Closing the gap means adding
//! those counters to the report, which this task does NOT do: the gate consumes
//! figures, it does not produce them.
//!
//! `BenchReport` also records no logs-fetch policy, so the projection leaves
//! [`CostReport::effective_policy`] absent and the request-minimal plan-shape
//! absolute is not asserted against it.

use crate::cost_regression::{CostReport, Figure, FigureClass, ProfileStamp};
use crate::report::{BenchReport, LatencyReport};

/// A [`CostReport`] projected from a [`BenchReport`], together with the figure
/// classes that report shape cannot supply.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchReportProjection {
    /// The projected report, ready to compare.
    pub report: CostReport,
    /// Classes no figure in [`Self::report`] carries, derived from the
    /// projection itself rather than listed by hand, so the two cannot drift.
    pub gaps: Vec<FigureClass>,
}

impl BenchReportProjection {
    /// This projection's [`gap_note`].
    pub fn gap_note(&self) -> Option<String> {
        gap_note(&self.gaps)
    }
}

/// The line naming classes a report source cannot supply, or `None` when it
/// supplies every class. Printed above the comparison table: a band nobody
/// emitted a figure for is a band that passed vacuously, and a reader has to be
/// told which ones those are rather than inferring it from a short table.
pub fn gap_note(gaps: &[FigureClass]) -> Option<String> {
    if gaps.is_empty() {
        return None;
    }
    let names: Vec<String> = gaps.iter().map(|c| format!("`{c}`")).collect();
    Some(format!(
        "NOTE: the bench-report source carries no figure for {}, so those bands are not enforced \
         here. The gate consumes figures and adds no counters.",
        names.join(", "),
    ))
}

/// One figure of a latency distribution, named `<prefix>_p50` / `<prefix>_p95`.
fn latency_figures(prefix: &str, latency: &LatencyReport) -> [Figure; 2] {
    [
        figure(
            &format!("{prefix}_p50"),
            FigureClass::LatencyP50,
            Some(latency.p50),
            Some("ms"),
        ),
        figure(
            &format!("{prefix}_p95"),
            FigureClass::LatencyP95,
            Some(latency.p95),
            Some("ms"),
        ),
    ]
}

fn figure(name: &str, class: FigureClass, value: Option<f64>, unit: Option<&str>) -> Figure {
    Figure {
        name: name.to_string(),
        class,
        value,
        unit: unit.map(str::to_string),
    }
}

/// A count that may be absent. `None` stays `None`; it never becomes `0.0`.
fn optional(value: Option<u64>) -> Option<f64> {
    value.map(|v| v as f64)
}

/// Project `report` into the gate's figure list. See the module docs for the
/// field map and the gap.
pub fn project_bench_report(report: &BenchReport) -> BenchReportProjection {
    let requests = &report.s3_requests;
    let cost = &report.modeled_cost;

    let mut figures = vec![
        figure(
            "write_class_requests",
            FigureClass::WriteClassRequests,
            optional(requests.put_class_attempts()),
            Some("billed-attempts"),
        ),
        // The call counts are real on any backend, including one whose attempts
        // are absent, so the gate still has a request surface to judge there.
        figure(
            "write_class_calls",
            FigureClass::WriteClassRequests,
            Some(requests.put.saturating_add(requests.list) as f64),
            Some("calls"),
        ),
        figure(
            "data_gets",
            FigureClass::DataGets,
            optional(requests.get_attempts),
            Some("billed-attempts"),
        ),
        figure(
            "data_get_calls",
            FigureClass::DataGets,
            Some(requests.get as f64),
            Some("calls"),
        ),
        figure(
            "modeled_request_cost",
            FigureClass::ModeledRequestCost,
            optional(cost.modeled_request_cost_nanodollars),
            Some("nanodollars"),
        ),
        // The byte terms stay separate figures: ADR-0996 decision 3 forbids
        // folding them into the request term, and summing them here would do
        // exactly that one layer later.
        figure(
            "modeled_transfer_cost",
            FigureClass::ModeledRequestCost,
            optional(cost.modeled_transfer_cost_nanodollars),
            Some("nanodollars"),
        ),
        figure(
            "modeled_retrieval_cost",
            FigureClass::ModeledRequestCost,
            optional(cost.modeled_retrieval_cost_nanodollars),
            Some("nanodollars"),
        ),
        figure(
            "bytes_written",
            FigureClass::Bytes,
            Some(report.bytes.written as f64),
            Some("wire"),
        ),
        figure(
            "bytes_read",
            FigureClass::Bytes,
            Some(report.bytes.read as f64),
            Some("wire"),
        ),
    ];
    figures.extend(latency_figures(
        "ingest_ack_latency",
        &report.ingest.strict_ack_latency_ms,
    ));
    figures.extend(latency_figures(
        "query_cold_latency",
        &report.query.cold_latency_ms,
    ));
    figures.extend(latency_figures(
        "query_warm_latency",
        &report.query.warm_latency_ms,
    ));

    let gaps = FigureClass::ALL
        .into_iter()
        .filter(|class| !figures.iter().any(|f| f.class == *class))
        .collect();

    BenchReportProjection {
        report: CostReport {
            profile: ProfileStamp {
                requested: report.environment.store_cost_profile_requested.clone(),
                effective: report.environment.store_cost_profile_effective.clone(),
            },
            // `BenchReport` records no logs-fetch policy, so the request-minimal
            // plan-shape absolute is not asserted against this source.
            effective_policy: None,
            figures,
        },
        gaps,
    }
}

/// Parse a `BenchReport` JSON document and project it, mapping malformed input
/// to the gate's own typed refusal rather than a panic.
pub fn project_bench_report_json(
    s: &str,
) -> Result<BenchReportProjection, crate::cost_regression::CompareError> {
    let report: BenchReport = serde_json::from_str(s)
        .map_err(|e| crate::cost_regression::CompareError::Malformed(e.to_string()))?;
    Ok(project_bench_report(&report))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_types::cost_profile::StoreCostProfile;

    use super::*;
    use crate::cost_regression::{Bands, compare};
    use crate::report::{
        BytesSection, Environment, IngestSection, ModeledCost, QuerySection, RequestCounts,
        WorkloadShape,
    };

    fn latency(p50: f64, p95: f64) -> LatencyReport {
        LatencyReport {
            p50,
            p95,
            p99: p95,
            max: p95,
            count: 8,
        }
    }

    /// A report shaped like a `MemoryStore` run: real call counts, no attempt
    /// source, so every attempt figure and the modeled request cost are absent.
    fn unattempted_report() -> BenchReport {
        BenchReport {
            environment: Environment {
                store_backend: "memory".to_string(),
                region: "n/a-memory".to_string(),
                shard_count: 2,
                max_flush_delay_ms: 500,
                workload: WorkloadShape {
                    target_series: 20,
                    points_per_sec: 4_000,
                    duration_secs: 1,
                    batch_size: 50,
                    query: "bench_gauge".to_string(),
                    warm_query_count: 5,
                },
                git_commit: "0".repeat(40),
                toolchain: "rustc 0.0.0-test".to_string(),
                store_cost_profile_requested: StoreCostProfile::reference(),
                store_cost_profile_effective: Some(StoreCostProfile::reference()),
            },
            ingest: IngestSection {
                strict_ack_latency_ms: latency(10.0, 100.0),
                accepted_points: 4_000,
                accepted_points_per_sec: 4_000.0,
                write_amplification: 1.5,
            },
            query: QuerySection {
                cold_latency_ms: latency(20.0, 20.0),
                warm_latency_ms: latency(2.0, 4.0),
                matched_series: 20,
            },
            s3_requests: RequestCounts {
                backend_bills_requests: false,
                put: 11,
                get: 23,
                list: 7,
                put_attempts: None,
                get_attempts: None,
                list_attempts: None,
                put_retry_overhead: None,
                get_retry_overhead: None,
                list_retry_overhead: None,
            },
            bytes: BytesSection {
                written: 96_000,
                read: 48_000,
            },
            modeled_cost: ModeledCost::default(),
        }
    }

    /// The same run on a request-billing backend: attempts wired, so the
    /// attempt figures and the modeled request cost carry values.
    fn attempted_report() -> BenchReport {
        let mut report = unattempted_report();
        report.s3_requests = RequestCounts {
            backend_bills_requests: true,
            put: 11,
            get: 23,
            list: 7,
            put_attempts: Some(13),
            get_attempts: Some(29),
            list_attempts: Some(7),
            put_retry_overhead: Some(2),
            get_retry_overhead: Some(6),
            list_retry_overhead: Some(0),
        };
        report.modeled_cost = ModeledCost::model(
            &StoreCostProfile::reference(),
            report.s3_requests.put_class_attempts(),
            report.s3_requests.get_attempts,
            report.bytes.read,
            report.bytes.read,
        );
        report
    }

    fn value_of(projection: &BenchReportProjection, name: &str) -> Option<f64> {
        projection
            .report
            .figures
            .iter()
            .find(|f| f.name == name)
            .expect("figure projected")
            .value
    }

    #[test]
    fn every_projected_figure_appears_exactly_once() {
        let projection = project_bench_report(&attempted_report());
        let mut names: Vec<&str> = projection
            .report
            .figures
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            total,
            "a duplicated figure name would defeat the present-exactly-once check"
        );
        assert_eq!(total, 15, "the field map projects 15 figures");
    }

    #[test]
    fn an_absent_bench_report_field_projects_to_an_absent_value_never_zero() {
        let projection = project_bench_report(&unattempted_report());
        for absent in [
            "write_class_requests",
            "data_gets",
            "modeled_request_cost",
            "modeled_transfer_cost",
            "modeled_retrieval_cost",
        ] {
            assert_eq!(
                value_of(&projection, absent),
                None,
                "`{absent}` has no source value: it must project ABSENT, never 0.0, which would \
                 compare as a real measurement"
            );
        }
        // The call counts are real on this backend and must not go absent with
        // them: `get = 23` is a measurement, `get_attempts = None` is not.
        assert_eq!(value_of(&projection, "data_get_calls"), Some(23.0));
        assert_eq!(value_of(&projection, "write_class_calls"), Some(18.0));
    }

    #[test]
    fn a_present_bench_report_field_projects_its_exact_value() {
        let report = attempted_report();
        let projection = project_bench_report(&report);
        // PUT-class folds LIST in: 13 PUT + 7 LIST attempts.
        assert_eq!(value_of(&projection, "write_class_requests"), Some(20.0));
        assert_eq!(value_of(&projection, "data_gets"), Some(29.0));
        assert_eq!(value_of(&projection, "data_get_calls"), Some(23.0));
        // 20 PUT-class at 5_000 + 29 GET at 400 = 100_000 + 11_600.
        assert_eq!(
            value_of(&projection, "modeled_request_cost"),
            Some(111_600.0)
        );
        assert_eq!(value_of(&projection, "bytes_written"), Some(96_000.0));
        assert_eq!(value_of(&projection, "bytes_read"), Some(48_000.0));
        assert_eq!(value_of(&projection, "ingest_ack_latency_p50"), Some(10.0));
        assert_eq!(value_of(&projection, "ingest_ack_latency_p95"), Some(100.0));
        assert_eq!(value_of(&projection, "query_warm_latency_p50"), Some(2.0));
        assert_eq!(value_of(&projection, "query_warm_latency_p95"), Some(4.0));
        assert_eq!(value_of(&projection, "query_cold_latency_p95"), Some(20.0));
        // The profile stamp rides across verbatim: a modeled cost without it is
        // not a result.
        assert_eq!(
            projection.report.profile.effective,
            Some(StoreCostProfile::reference())
        );
    }

    #[test]
    fn the_four_unsupplied_classes_are_named_as_gaps() {
        let projection = project_bench_report(&attempted_report());
        assert_eq!(
            projection.gaps,
            vec![
                FigureClass::ObjectCount,
                FigureClass::RangeAmplification,
                FigureClass::PeakMemory,
                FigureClass::RangedOpens,
            ],
            "BenchReport carries no figure for these four"
        );
        let note = projection.gap_note().expect("a gap has a note");
        for class in &projection.gaps {
            assert!(
                note.contains(class.as_str()),
                "the note names `{class}`: {note}"
            );
        }
    }

    #[test]
    fn a_projected_report_compares_against_itself_without_regressing() {
        // The end of the adapter's job: the projection is comparable under the
        // shipping defaults, including the expected-class check, which the four
        // gaps would otherwise fail.
        let projection = project_bench_report(&attempted_report());
        let comparison = compare(&projection.report, &projection.report, &Bands::defaults())
            .expect("a projected report is comparable with itself");
        assert!(
            !comparison.regressed(),
            "a report against itself must not regress:\n{}",
            comparison.render_table()
        );
        assert!(
            comparison.missing_expected.is_empty(),
            "no EXPECTED class is missing from the projection: {:?}",
            comparison.missing_expected
        );
    }

    #[test]
    fn a_moved_bench_report_figure_reaches_the_comparison() {
        // The projection is not decoration: a figure moved in the source report
        // fails the gate, naming the projected figure.
        let baseline = project_bench_report(&attempted_report()).report;
        let mut raised = attempted_report();
        raised.bytes.read = 48_000 + 4_801; // past the +5% bytes band
        let candidate = project_bench_report(&raised).report;
        let comparison =
            compare(&baseline, &candidate, &Bands::defaults()).expect("comparable pair");
        let failing: Vec<&str> = comparison
            .rows
            .iter()
            .filter(|r| r.verdict.is_fail())
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(failing, vec!["bytes_read"]);
    }

    #[test]
    fn a_malformed_bench_report_json_is_a_typed_refusal() {
        let err = project_bench_report_json("{ not json").expect_err("malformed");
        assert!(matches!(
            err,
            crate::cost_regression::CompareError::Malformed(_)
        ));
    }
}

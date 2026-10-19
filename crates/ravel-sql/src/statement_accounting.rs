//! One statement's cost, counted per signal table and summed back into the
//! statement total (issue #2912, ADR-2904 decision 7).
//!
//! Every signal lane records into its own [`PhaseAccounting`], Parquet tables
//! share one more, and a statement-level handle takes the charges that belong
//! to no table (the memory pool's peak report). [`StatementAccounting`] sums
//! them: the signal scans' request and byte budgets, the live cost and io_shape
//! read the statement total through it, never one lane's handle. The Parquet
//! read budget reads the statement total too when the statement also names a
//! signal table, through a `ravel_parquet::BudgetView` over
//! [`StatementAccounting::pooled_snapshot`], while each admitted read is still
//! recorded on the Parquet handle. A statement naming only Parquet tables reads
//! the Parquet handle, which equals the total there: the statement handle
//! holds only the pool peak.

use ravel_query::{PhaseAccounting, PhaseAccountingSnapshot};
use ravel_types::accounting::QueryAccountingSnapshot;

use crate::executor::{SIGNAL_LANE_ORDER, TargetSignal};

/// A statement's accounting handles and the view that sums them.
#[derive(Debug, Clone)]
pub(crate) enum StatementAccounting {
    /// One handle per signal lane (indexed by [`SIGNAL_LANE_ORDER`]), one for
    /// Parquet tables, and one for charges that belong to no table.
    Split {
        lanes: Box<[PhaseAccounting; SIGNAL_LANE_ORDER.len()]>,
        parquet: PhaseAccounting,
        statement: PhaseAccounting,
    },
    /// Every lane, Parquet and the statement record into one handle: the
    /// Flight SQL and distributed entries, whose figures this view leaves as
    /// they were.
    One(PhaseAccounting),
}

impl Default for StatementAccounting {
    fn default() -> Self {
        StatementAccounting::new()
    }
}

impl StatementAccounting {
    /// Fresh, independent handles for every lane, Parquet and the statement.
    pub(crate) fn new() -> Self {
        StatementAccounting::Split {
            lanes: Box::new(std::array::from_fn(|_| PhaseAccounting::new())),
            parquet: PhaseAccounting::new(),
            statement: PhaseAccounting::new(),
        }
    }

    /// The handle `signal`'s reads record into; Parquet maps to the Parquet
    /// handle.
    pub(crate) fn lane(&self, signal: TargetSignal) -> &PhaseAccounting {
        match self {
            StatementAccounting::Split { lanes, parquet, .. } => {
                let [metrics, logs, spans, alerts, audit] = &**lanes;
                match signal {
                    TargetSignal::Metrics => metrics,
                    TargetSignal::Logs => logs,
                    TargetSignal::Spans => spans,
                    TargetSignal::Alerts => alerts,
                    TargetSignal::Audit => audit,
                    TargetSignal::Parquet => parquet,
                }
            }
            StatementAccounting::One(handle) => handle,
        }
    }

    /// The handle every Parquet table records into.
    pub(crate) fn parquet(&self) -> &PhaseAccounting {
        match self {
            StatementAccounting::Split { parquet, .. } => parquet,
            StatementAccounting::One(handle) => handle,
        }
    }

    /// The handle for charges that belong to no table.
    pub(crate) fn statement(&self) -> &PhaseAccounting {
        match self {
            StatementAccounting::Split { statement, .. } => statement,
            StatementAccounting::One(handle) => handle,
        }
    }

    /// The statement total per phase: the field-wise sum of every handle,
    /// the same figure the single handle reported before the split.
    pub(crate) fn snapshot(&self) -> PhaseAccountingSnapshot {
        match self {
            StatementAccounting::Split {
                lanes,
                parquet,
                statement,
            } => lanes
                .iter()
                .chain([parquet, statement])
                .map(PhaseAccounting::snapshot)
                .fold(PhaseAccountingSnapshot::default(), |sum, next| {
                    add_phases(&sum, &next)
                }),
            StatementAccounting::One(handle) => handle.snapshot(),
        }
    }

    /// The statement total across every phase, read once per handle.
    pub(crate) fn pooled_snapshot(&self) -> QueryAccountingSnapshot {
        match self {
            StatementAccounting::Split { .. } => self.snapshot().pooled(),
            StatementAccounting::One(handle) => handle.pooled_snapshot(),
        }
    }

    /// The statement's resolve-phase total. For [`Self::One`] this is the
    /// shared handle's whole total, as it was before the split.
    pub(crate) fn resolve_snapshot(&self) -> QueryAccountingSnapshot {
        match self {
            StatementAccounting::Split { .. } => self.snapshot().resolve,
            StatementAccounting::One(handle) => handle.resolve().snapshot(),
        }
    }
}

/// Field-wise saturating sum of two per-phase snapshots, phase by phase.
pub(crate) fn add_phases(
    a: &PhaseAccountingSnapshot,
    b: &PhaseAccountingSnapshot,
) -> PhaseAccountingSnapshot {
    PhaseAccountingSnapshot {
        resolve: a.resolve.saturating_add(&b.resolve),
        plan: a.plan.saturating_add(&b.plan),
        probe: a.probe.saturating_add(&b.probe),
        scan: a.scan.saturating_add(&b.scan),
        key_index: a.key_index.saturating_add(&b.key_index),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_query::QueryPhase;
    use ravel_types::accounting::AccountedOp;

    use super::*;

    fn charge(handle: &ravel_types::accounting::QueryAccounting, op: AccountedOp, bytes: u64) {
        handle.record_s3_request(op);
        handle.add_s3_bytes(op, bytes);
    }

    #[test]
    fn lanes_parquet_and_statement_sum_to_the_total() {
        let view = StatementAccounting::new();
        charge(
            view.lane(TargetSignal::Metrics).resolve(),
            AccountedOp::Get,
            10,
        );
        charge(view.lane(TargetSignal::Logs).scan(), AccountedOp::Get, 7);
        charge(view.parquet().resolve(), AccountedOp::List, 0);
        charge(view.parquet().scan(), AccountedOp::Get, 100);
        view.statement().scan().observe_intermediate_bytes(64);

        let total = view.snapshot();
        let entries = [TargetSignal::Metrics, TargetSignal::Logs]
            .iter()
            .map(|signal| view.lane(*signal).snapshot())
            .fold(PhaseAccountingSnapshot::default(), |sum, next| {
                add_phases(&sum, &next)
            });
        let parquet_share = view.parquet().snapshot();
        let with_statement = add_phases(
            &add_phases(&entries, &parquet_share),
            &view.statement().snapshot(),
        );
        assert_eq!(with_statement, total);
        // The Parquet share is the total less the entries, phase by phase.
        for phase in QueryPhase::ALL {
            assert_eq!(
                total.phase(phase).total_s3_requests() - entries.phase(phase).total_s3_requests(),
                parquet_share.phase(phase).total_s3_requests()
            );
            assert_eq!(
                total.phase(phase).total_s3_bytes() - entries.phase(phase).total_s3_bytes(),
                parquet_share.phase(phase).total_s3_bytes()
            );
        }
        assert_eq!(view.parquet().pooled_snapshot().total_s3_requests(), 2);
        assert_eq!(total.pooled().total_s3_requests(), 4);
        assert_eq!(view.pooled_snapshot().total_s3_bytes(), 117);
        assert_eq!(view.resolve_snapshot().total_s3_requests(), 2);
        assert_eq!(view.pooled_snapshot().peak_intermediate_bytes, 64);
    }

    /// Each signal lane records into its own handle, distinct from every
    /// other lane's and from the Parquet handle.
    #[test]
    fn every_signal_has_its_own_lane_handle() {
        let view = StatementAccounting::new();
        for (index, signal) in SIGNAL_LANE_ORDER.iter().enumerate() {
            charge(view.lane(*signal).scan(), AccountedOp::Get, 1 << index);
        }
        for (index, signal) in SIGNAL_LANE_ORDER.iter().enumerate() {
            assert_eq!(
                view.lane(*signal).pooled_snapshot().total_s3_bytes(),
                1 << index,
                "{signal:?}"
            );
        }
        assert_eq!(view.parquet().pooled_snapshot().total_s3_requests(), 0);
        assert_eq!(view.pooled_snapshot().total_s3_bytes(), 0b11111);
    }

    #[test]
    fn one_handle_reads_as_the_shared_handle() {
        let shared = ravel_types::accounting::QueryAccounting::new();
        let view = StatementAccounting::One(PhaseAccounting::pooled_over(&shared));
        charge(view.lane(TargetSignal::Logs).scan(), AccountedOp::Get, 5);
        charge(view.parquet().resolve(), AccountedOp::Get, 3);
        assert_eq!(view.pooled_snapshot(), shared.snapshot());
        assert_eq!(view.resolve_snapshot(), shared.snapshot());
    }
}

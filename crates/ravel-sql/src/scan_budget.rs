//! The statement's byte and request budgets as every signal scan checks them
//! (ADR-0061 decision 1, ADR-0073 decision 4, issue #1409).
//!
//! A [`ScanBudget`] carries the effective `max_bytes_scanned` and
//! `max_s3_requests` and the statement total they are compared against, the
//! sum over every table the statement reads ([`StatementAccounting`]). Each
//! scan calls [`ScanBudget::check`] once per completed fetch, before decoding
//! what it fetched, so a statement that has passed either budget is refused
//! with a typed error before its scans issue another fetch, rather than
//! truncated. The samples, logs, spans, alerts and audit scans all check
//! through this one type.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use ravel_query::{
    ByteLimit, EngineConfig, PhaseAccounting, QueryError, RequestLimit, request_budget_exceeded,
};

use crate::error::SqlError;
use crate::statement_accounting::StatementAccounting;

/// The budgets one signal table's scan checked, as [`crate::SignalStats`]
/// reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnforcedBudgets {
    /// The byte ceiling the scan compared the statement's scanned bytes with.
    pub max_bytes_scanned: ByteLimit,
    /// The request ceiling (`max_store_requests` on the wire) the scan
    /// compared the statement's store requests with.
    pub max_store_requests: RequestLimit,
    /// How many times the scan checked both: once per completed fetch.
    pub checks: u64,
}

/// What one lane's installed budget recorded: the limits it was built with and
/// how many checks its scans ran.
#[derive(Debug, Default)]
pub(crate) struct BudgetLedger {
    limits: Mutex<Option<(ByteLimit, RequestLimit)>>,
    checks: AtomicU64,
}

impl BudgetLedger {
    /// `None` until a budget was installed on this lane's provider.
    pub(crate) fn report(&self) -> Option<EnforcedBudgets> {
        let limits = *self.limits.lock().unwrap_or_else(PoisonError::into_inner);
        limits.map(|(max_bytes_scanned, max_store_requests)| EnforcedBudgets {
            max_bytes_scanned,
            max_store_requests,
            checks: self.checks.load(Ordering::Relaxed),
        })
    }

    fn install(&self, max_bytes_scanned: ByteLimit, max_s3_requests: RequestLimit) {
        *self.limits.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((max_bytes_scanned, max_s3_requests));
    }
}

/// The effective byte and request budgets and the statement total a scan
/// checks them against.
#[derive(Debug, Clone)]
pub(crate) struct ScanBudget {
    max_bytes_scanned: ByteLimit,
    max_s3_requests: RequestLimit,
    statement: StatementAccounting,
    ledger: Option<Arc<BudgetLedger>>,
}

impl ScanBudget {
    /// `engine`'s budgets checked against `statement`, recording into
    /// `ledger` when one is given.
    pub(crate) fn new(
        engine: &EngineConfig,
        statement: StatementAccounting,
        ledger: Option<Arc<BudgetLedger>>,
    ) -> Self {
        if let Some(ledger) = &ledger {
            ledger.install(engine.max_bytes_scanned, engine.max_s3_requests);
        }
        ScanBudget {
            max_bytes_scanned: engine.max_bytes_scanned,
            max_s3_requests: engine.max_s3_requests,
            statement,
            ledger,
        }
    }

    /// No ceiling, over `own`: what a provider built outside the executor
    /// checks until a budget is installed on it.
    pub(crate) fn unlimited(own: &PhaseAccounting) -> Self {
        ScanBudget {
            max_bytes_scanned: ByteLimit::Unlimited,
            max_s3_requests: RequestLimit::Unlimited,
            statement: StatementAccounting::One(own.clone()),
            ledger: None,
        }
    }

    /// The byte ceiling, for a distributed fan-out that folds its workers'
    /// bytes against it.
    pub(crate) fn max_bytes_scanned(&self) -> ByteLimit {
        self.max_bytes_scanned
    }

    /// Refuses once the statement total has passed either budget.
    ///
    /// Reads [`StatementAccounting::pooled_snapshot`], not
    /// `snapshot().pooled()`: a handle built with `PhaseAccounting::pooled_over`
    /// shares one counter across its five phases, and summing the phases would
    /// report five times the real total.
    ///
    /// A request refusal names no fold lag: this budget carries no resolve
    /// verdict, so the resolve-boundary check in `executor.rs` is where a SQL
    /// refusal names it (ADR-1306 decision 6).
    pub(crate) fn check(&self) -> Result<(), SqlError> {
        if let Some(ledger) = &self.ledger {
            ledger.checks.fetch_add(1, Ordering::Relaxed);
        }
        let total = self.statement.pooled_snapshot();
        let scanned = total.total_s3_bytes();
        if let ByteLimit::Bounded(max) = self.max_bytes_scanned
            && scanned > max
        {
            return Err(SqlError::TooManyBytesScanned { scanned, max });
        }
        if let Some(QueryError::RequestBudgetExceeded {
            requests,
            max,
            fold_lag,
        }) = request_budget_exceeded(total.total_s3_requests(), self.max_s3_requests)
        {
            return Err(SqlError::RequestBudgetExceeded {
                requests,
                max,
                fold_lag,
                table: None,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_types::accounting::AccountedOp;

    use super::*;
    use crate::executor::TargetSignal;

    fn engine(bytes: u64, requests: u64) -> EngineConfig {
        EngineConfig {
            max_bytes_scanned: ByteLimit::Bounded(bytes),
            max_s3_requests: RequestLimit::Bounded(requests),
            ..EngineConfig::default()
        }
    }

    /// The check reads the statement total: two lanes each under the byte
    /// ceiling are refused once their sum passes it, at the summed figure.
    ///
    /// FLIP: checking `statement.lane(..)` (one table's own handle) instead of
    /// `pooled_snapshot()` reports Ok at 60 of 100 and fails the `expect_err`.
    #[test]
    fn a_budget_checks_the_statement_total_not_one_lane() {
        let statement = StatementAccounting::new();
        let budget = ScanBudget::new(&engine(100, 10), statement.clone(), None);
        let logs = statement.lane(TargetSignal::Logs).scan();
        logs.record_s3_request(AccountedOp::Get);
        logs.add_s3_bytes(AccountedOp::Get, 60);
        budget.check().expect("60 of 100");
        let spans = statement.lane(TargetSignal::Spans).scan();
        spans.record_s3_request(AccountedOp::Get);
        spans.add_s3_bytes(AccountedOp::Get, 60);
        let err = budget.check().expect_err("120 of 100");
        assert!(
            matches!(
                err,
                SqlError::TooManyBytesScanned {
                    scanned: 120,
                    max: 100
                }
            ),
            "{err:?}"
        );
    }

    /// A pooled handle is read once, not once per phase.
    ///
    /// FLIP: `statement.snapshot().pooled()` reads the shared counter five
    /// times, 50 bytes become 250 and the first check refuses.
    #[test]
    fn a_pooled_handle_counts_once() {
        let shared = ravel_types::accounting::QueryAccounting::new();
        let statement = StatementAccounting::One(PhaseAccounting::pooled_over(&shared));
        let budget = ScanBudget::new(&engine(100, 3), statement, None);
        shared.record_s3_request(AccountedOp::Get);
        shared.add_s3_bytes(AccountedOp::Get, 50);
        budget.check().expect("50 bytes, 1 request");
        for _ in 0..3 {
            shared.record_s3_request(AccountedOp::Get);
        }
        let err = budget.check().expect_err("4 requests of 3");
        assert!(
            matches!(
                err,
                SqlError::RequestBudgetExceeded {
                    requests: 4,
                    max: 3,
                    table: None,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    /// The ledger reports the installed limits and every check, a refused one
    /// included.
    #[test]
    fn the_ledger_counts_every_check() {
        let ledger = Arc::new(BudgetLedger::default());
        assert_eq!(ledger.report(), None);
        let statement = StatementAccounting::new();
        let budget = ScanBudget::new(&engine(0, 0), statement.clone(), Some(Arc::clone(&ledger)));
        budget.check().expect("nothing read");
        statement
            .lane(TargetSignal::Alerts)
            .scan()
            .record_s3_request(AccountedOp::Get);
        budget.check().expect_err("1 request of 0");
        assert_eq!(
            ledger.report(),
            Some(EnforcedBudgets {
                max_bytes_scanned: ByteLimit::Bounded(0),
                max_store_requests: RequestLimit::Bounded(0),
                checks: 2,
            })
        );
    }
}

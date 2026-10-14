//! The time window a `FROM logs` statement reads (ADR-2887 D11), derived from
//! its own `@timestamp` bounds.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use ravel_types::TimeRange;

use super::ir::BExpr;

/// The `@timestamp` bounds read so far, as closed nanosecond bounds.
#[derive(Debug, Clone, Default)]
pub(crate) struct WindowBounds {
    lower: Option<i64>,
    upper: Option<i64>,
    closed: bool,
}

impl WindowBounds {
    /// Read the bounds `condition` puts on `ts`, the internal name of the
    /// source's timestamp column: each top-level `AND` conjunct that compares
    /// `ts` with a timestamp literal. Nothing is read once [`Self::close`] ran.
    pub(crate) fn observe(&mut self, condition: &BExpr, ts: &str) {
        if self.closed {
            return;
        }
        let mut conjuncts = Vec::new();
        split_and(condition, &mut conjuncts);
        for conjunct in conjuncts {
            let BExpr::Binary(left, op, right) = conjunct else {
                continue;
            };
            let (op, value) = match (left.as_ref(), right.as_ref()) {
                (BExpr::Column(name), other) if name == ts => (*op, timestamp(other)),
                (other, BExpr::Column(name)) if name == ts => match op.swap() {
                    Some(swapped) => (swapped, timestamp(other)),
                    None => continue,
                },
                _ => continue,
            };
            let Some(value) = value else { continue };
            match op {
                Operator::Gt => self.raise(value.saturating_add(1)),
                Operator::GtEq => self.raise(value),
                Operator::Lt => self.lower_upper(value.saturating_sub(1)),
                Operator::LtEq => self.lower_upper(value),
                Operator::Eq => {
                    self.raise(value);
                    self.lower_upper(value);
                }
                _ => {}
            }
        }
    }

    /// Stop reading bounds: a `LIMIT`, a `STATS`, or a stage that redefines
    /// `@timestamp` came first.
    pub(crate) fn close(&mut self) {
        self.closed = true;
    }

    /// The window: the bounds read, a lower bound alone ending at `now_ns`
    /// and no lower bound starting at the epoch, narrowed by the request's
    /// half-open `[start, end)`. An empty intersection is a one-point window
    /// at its start, which the filters then empty.
    pub(crate) fn resolve(&self, now_ns: i64, start: Option<i64>, end: Option<i64>) -> TimeRange {
        let mut start_ns = self.lower.unwrap_or(0);
        let mut end_ns = self.upper.unwrap_or(now_ns);
        if let Some(start) = start {
            start_ns = start_ns.max(start);
        }
        if let Some(end) = end {
            end_ns = end_ns.min(end.saturating_sub(1));
        }
        TimeRange {
            start_ns,
            end_ns: end_ns.max(start_ns),
        }
    }

    fn raise(&mut self, value: i64) {
        self.lower = Some(self.lower.map_or(value, |lower| lower.max(value)));
    }

    fn lower_upper(&mut self, value: i64) {
        self.upper = Some(self.upper.map_or(value, |upper| upper.min(value)));
    }
}

fn split_and<'a>(e: &'a BExpr, out: &mut Vec<&'a BExpr>) {
    match e {
        BExpr::Binary(left, Operator::And, right) => {
            split_and(left, out);
            split_and(right, out);
        }
        other => out.push(other),
    }
}

fn timestamp(e: &BExpr) -> Option<i64> {
    match e {
        BExpr::Literal(ScalarValue::TimestampNanosecond(Some(value), None)) => Some(*value),
        _ => None,
    }
}

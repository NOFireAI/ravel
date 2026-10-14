//! The bound pipeline: what the binder produces and the lowering turns into a
//! plan. Columns are named by the binder's internal names, functions by the
//! name the session registers them under, so the pipeline can be lowered
//! against any session and its policy read off it before any session exists.

use std::collections::BTreeSet;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;

/// A bound expression.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BExpr {
    Column(String),
    Literal(ScalarValue),
    /// A scalar function, by its registered name.
    Scalar(&'static str, Vec<BExpr>),
    Cast(Box<BExpr>, DataType),
    TryCast(Box<BExpr>, DataType),
    Binary(Box<BExpr>, Operator, Box<BExpr>),
    Not(Box<BExpr>),
    Negative(Box<BExpr>),
    /// The value, and whether the test is `IS NOT NULL`.
    IsNull(Box<BExpr>, bool),
    /// The value, the list, and whether it is `NOT IN`.
    InList(Box<BExpr>, Vec<BExpr>, bool),
    /// `CASE WHEN .. THEN .. [ELSE ..] END`.
    Case(Vec<(BExpr, BExpr)>, Option<Box<BExpr>>),
}

impl BExpr {
    pub(crate) fn binary(left: BExpr, op: Operator, right: BExpr) -> BExpr {
        BExpr::Binary(Box::new(left), op, Box::new(right))
    }

    pub(crate) fn cast(self, to: DataType) -> BExpr {
        BExpr::Cast(Box::new(self), to)
    }

    pub(crate) fn try_cast(self, to: DataType) -> BExpr {
        BExpr::TryCast(Box::new(self), to)
    }

    /// The expression nodes in this tree, this one included.
    pub(crate) fn nodes(&self) -> usize {
        let mut count = 0;
        self.walk(&mut |_| count += 1);
        count
    }

    /// Every column name this tree reads.
    pub(crate) fn columns<'e>(&'e self, out: &mut Vec<&'e str>) {
        self.walk(&mut |e| {
            if let BExpr::Column(name) = e {
                out.push(name);
            }
        });
    }

    fn walk<'e>(&'e self, visit: &mut impl FnMut(&'e BExpr)) {
        visit(self);
        match self {
            BExpr::Column(_) | BExpr::Literal(_) => {}
            BExpr::Scalar(_, args) => args.iter().for_each(|arg| arg.walk(visit)),
            BExpr::Cast(inner, _)
            | BExpr::TryCast(inner, _)
            | BExpr::Not(inner)
            | BExpr::Negative(inner)
            | BExpr::IsNull(inner, _) => inner.walk(visit),
            BExpr::Binary(left, _, right) => {
                left.walk(visit);
                right.walk(visit);
            }
            BExpr::InList(value, list, _) => {
                value.walk(visit);
                list.iter().for_each(|item| item.walk(visit));
            }
            BExpr::Case(arms, otherwise) => {
                for (when, then) in arms {
                    when.walk(visit);
                    then.walk(visit);
                }
                if let Some(otherwise) = otherwise {
                    otherwise.walk(visit);
                }
            }
        }
    }

    fn scalars(&self, out: &mut BTreeSet<&'static str>) {
        match self {
            BExpr::Column(_) | BExpr::Literal(_) => {}
            BExpr::Scalar(name, args) => {
                out.insert(name);
                args.iter().for_each(|arg| arg.scalars(out));
            }
            BExpr::Cast(inner, _)
            | BExpr::TryCast(inner, _)
            | BExpr::Not(inner)
            | BExpr::Negative(inner)
            | BExpr::IsNull(inner, _) => inner.scalars(out),
            BExpr::Binary(left, _, right) => {
                left.scalars(out);
                right.scalars(out);
            }
            BExpr::InList(value, list, _) => {
                value.scalars(out);
                list.iter().for_each(|item| item.scalars(out));
            }
            BExpr::Case(arms, otherwise) => {
                for (when, then) in arms {
                    when.scalars(out);
                    then.scalars(out);
                }
                if let Some(otherwise) = otherwise {
                    otherwise.scalars(out);
                }
            }
        }
    }
}

/// One aggregate call: the function's registered name, its arguments, and the
/// per-aggregate `WHERE`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BAggregate {
    pub(crate) function: &'static str,
    pub(crate) args: Vec<BExpr>,
    pub(crate) filter: Option<BExpr>,
}

/// One sort key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BSortKey {
    pub(crate) expr: BExpr,
    pub(crate) asc: bool,
    pub(crate) nulls_first: bool,
}

/// One plan node over the stage before it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Stage {
    /// The output columns, each an expression and its internal name.
    Project(Vec<(BExpr, String)>),
    Filter(BExpr),
    /// Grouping keys then aggregates, each under its internal name.
    Aggregate {
        groups: Vec<(BExpr, String)>,
        aggregates: Vec<(BAggregate, String)>,
    },
    Sort(Vec<BSortKey>),
    Limit(u64),
}

impl Stage {
    /// The expression nodes this stage holds.
    pub(crate) fn nodes(&self) -> usize {
        match self {
            Stage::Project(columns) => columns.iter().map(|(e, _)| e.nodes()).sum(),
            Stage::Filter(condition) => condition.nodes(),
            Stage::Aggregate { groups, aggregates } => {
                groups.iter().map(|(e, _)| e.nodes()).sum::<usize>()
                    + aggregates
                        .iter()
                        .map(|(call, _)| {
                            call.args.iter().map(BExpr::nodes).sum::<usize>()
                                + call.filter.as_ref().map_or(0, BExpr::nodes)
                        })
                        .sum::<usize>()
            }
            Stage::Sort(keys) => keys.iter().map(|key| key.expr.nodes()).sum(),
            Stage::Limit(_) => 0,
        }
    }
}

/// Where the rows come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// The tenant's `logs` table.
    Logs,
    /// One row with no columns, for `ROW`.
    Row,
}

/// A bound statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Pipeline {
    pub(crate) source: Source,
    pub(crate) stages: Vec<Stage>,
    /// The result columns: internal name, then the ES|QL name.
    pub(crate) output: Vec<(String, String)>,
}

impl Pipeline {
    /// Every expression node in the pipeline's stages.
    #[cfg(test)]
    pub(crate) fn nodes(&self) -> usize {
        self.stages.iter().map(Stage::nodes).sum()
    }

    /// Every scalar and every aggregate function the pipeline calls, by
    /// registered name.
    pub(crate) fn functions(&self) -> (BTreeSet<&'static str>, BTreeSet<&'static str>) {
        let mut scalars = BTreeSet::new();
        let mut aggregates = BTreeSet::new();
        for stage in &self.stages {
            match stage {
                Stage::Project(columns) => {
                    columns.iter().for_each(|(e, _)| e.scalars(&mut scalars))
                }
                Stage::Filter(condition) => condition.scalars(&mut scalars),
                Stage::Aggregate {
                    groups,
                    aggregates: calls,
                } => {
                    groups.iter().for_each(|(e, _)| e.scalars(&mut scalars));
                    for (call, _) in calls {
                        aggregates.insert(call.function);
                        call.args.iter().for_each(|e| e.scalars(&mut scalars));
                        if let Some(filter) = &call.filter {
                            filter.scalars(&mut scalars);
                        }
                    }
                }
                Stage::Sort(keys) => keys.iter().for_each(|key| key.expr.scalars(&mut scalars)),
                Stage::Limit(_) => {}
            }
        }
        (scalars, aggregates)
    }
}

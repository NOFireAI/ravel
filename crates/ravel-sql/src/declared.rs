//! Per-tenant typed attribute columns for the `logs` table (ADR-0090).
//!
//! The `logs` SQL table exposes every attribute through one merged
//! `attrs: Map(Utf8, Utf8)` column ([`crate::logs_schema`]), so every numeric
//! or boolean predicate over an attribute is a `CAST(attrs['k'] AS ...)` over a
//! stringified value and no typed comparison is possible at all. ADR-0090 lets
//! an operator *declare* a per-tenant set of attribute keys as native typed
//! columns, appended after `attrs`, so the same value reads back as a real
//! `Int64`/`Boolean`/`Binary` Arrow column, or, for a `Str` column,
//! `Dictionary(Int32, Utf8)` (ADR-0099 decision 5).
//!
//! This module owns the query-side contract only: the [`DeclaredColumnSource`]
//! trait a plan resolves its declared columns from (ADR-0090 decision 2), the
//! [`DeclaredColumn`]/[`DeclaredType`] value types, and a
//! [`StaticDeclaredColumns`] test implementation so this crate's own tests do
//! not depend on `ravel-server`. The real cache-aside, `TenantConfig`-backed
//! implementation lives in `services/ravel-server` (a following wave, #302);
//! this crate is built and tested entirely against [`StaticDeclaredColumns`].
//!
//! # Four declarable types, not five (ADR-0090 decision 1)
//!
//! [`DeclaredType`] has exactly the four non-float, non-nested variants of
//! [`ravel_logseg::record::FieldType`]: `Str`, `I64`, `Bool`, `Bytes`. `f64` is
//! deferred: a declared float aggregate is order-sensitive
//! (ADR-0013/ADR-0022), which this ADR does not solve ahead of #277. Date and
//! timestamp declared columns are likewise deferred (they need a lifting rule
//! from `i64` storage plus a declared unit). The four types here all aggregate
//! exactly under any partitioning, so `SUM`/`COUNT`/`MIN`/`MAX`/`AVG` over a
//! declared `i64` column need none of that machinery.

use std::sync::Arc;

use async_trait::async_trait;
use ravel_types::TenantHash;

/// One of the four logical types a declared attribute column can take
/// (ADR-0090 decision 1), matching [`ravel_logseg::record::FieldType`]'s four
/// non-float, non-nested variants. `f64`, date, and timestamp are deferred; see
/// the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclaredType {
    /// A `Str`-typed attribute, projected as Arrow `Dictionary(Int32, Utf8)`
    /// (ADR-0099 decision 5): a dict-encoded page becomes the Arrow dictionary
    /// and its ids with no per-row allocation, and a plain page a degenerate
    /// identity dictionary, so both paths build the one schema DataFusion checks.
    ///
    /// A `GROUP BY` over a column of this type must not reach DataFusion's
    /// `RowConverter` path: `Dictionary(Int32, Utf8)` is grouped through
    /// `GroupValuesRows`, whose `emit` decodes every group key back into one
    /// `Utf8` array and panics with `offset overflow` once the decoded bytes
    /// cross `i32::MAX` (issue #737).
    Str,
    /// An `I64`-typed attribute, projected as Arrow `Int64`.
    I64,
    /// A `Bool`-typed attribute, projected as Arrow `Boolean`.
    Bool,
    /// A `Bytes`-typed attribute (including a canonicalized `List`/`Map` value),
    /// projected as Arrow `Binary`.
    Bytes,
}

/// One declared typed attribute column: the attribute key, verbatim, and the
/// logical type it is projected as. The SQL column name *is* `key`, never
/// mangled: a key containing `.` or uppercase characters requires
/// double-quoting in SQL per DataFusion's identifier rules (ADR-0090
/// decision 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredColumn {
    /// The attribute key promoted into a typed column.
    pub key: String,
    /// The logical type the column carries.
    pub ty: DeclaredType,
}

impl DeclaredColumn {
    /// Build a declared column for `key` with type `ty`.
    pub fn new(key: impl Into<String>, ty: DeclaredType) -> Self {
        DeclaredColumn {
            key: key.into(),
            ty,
        }
    }
}

/// Query-time source of a tenant's declared typed attribute columns (ADR-0090
/// decision 2).
///
/// Resolved **once per plan**, at the entry point that carries both the tenant
/// and the query's injected `now_ns` (`SqlExecutor::run` for HTTP, Flight's
/// `get_flight_info`), and the resolved `Vec<DeclaredColumn>` is threaded down
/// as a plain parameter; the lower planning functions never call this
/// themselves. The order of the returned list is preserved verbatim as the
/// schema-append order, so it MUST be stable for a given tenant (never
/// re-sorted between a cache refresh and the next plan).
///
/// The real implementation (`ravel-server`, #302) is cache-aside over
/// `TenantConfig`; its result is not a pure function of `(tenant, now_ns)`, so
/// a plan-then-stream transport (Flight SQL) pins the resolved list into its
/// ticket rather than re-resolving at `DoGet`.
#[async_trait]
pub trait DeclaredColumnSource: Send + Sync {
    /// The declared typed attribute columns for `tenant`, resolved as of
    /// `now_ns`. An empty vec means the `logs` table has its zero-declaration
    /// base schema for this tenant.
    async fn declared_columns(&self, tenant: TenantHash, now_ns: i64) -> Vec<DeclaredColumn>;
}

/// A fixed declared-column set, ignoring `tenant` and `now_ns` (ADR-0090
/// decision 2's test implementation).
///
/// This is a pure constant source: `declared_columns` returns the same list on
/// every call, so re-resolving it (e.g. at a Flight `DoGet`) is byte-identical
/// to the first resolution. `ravel-sql`'s own tests use it so they never depend
/// on `ravel-server`'s real cache-aside overlay.
#[derive(Clone, Debug, Default)]
pub struct StaticDeclaredColumns {
    columns: Vec<DeclaredColumn>,
}

impl StaticDeclaredColumns {
    /// A source declaring exactly `columns`.
    pub fn new(columns: Vec<DeclaredColumn>) -> Self {
        StaticDeclaredColumns { columns }
    }

    /// A source declaring no columns: the `logs` table's zero-declaration base
    /// schema for every tenant. This is the default `SqlExecutor` carries until
    /// a caller installs a real source.
    pub fn empty() -> Self {
        StaticDeclaredColumns {
            columns: Vec::new(),
        }
    }
}

#[async_trait]
impl DeclaredColumnSource for StaticDeclaredColumns {
    async fn declared_columns(&self, _tenant: TenantHash, _now_ns: i64) -> Vec<DeclaredColumn> {
        self.columns.clone()
    }
}

/// The default declared-column source a freshly built `SqlExecutor` carries: an
/// empty [`StaticDeclaredColumns`], so `SqlExecutor::new` stays
/// source-compatible and the `logs` table keeps its zero-declaration base
/// schema until a caller installs a real source with
/// [`crate::SqlExecutor::with_declared_column_source`].
pub(crate) fn default_declared_source() -> Arc<dyn DeclaredColumnSource> {
    Arc::new(StaticDeclaredColumns::empty())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_source_ignores_tenant_and_now() {
        let src = StaticDeclaredColumns::new(vec![
            DeclaredColumn::new("duration_ms", DeclaredType::I64),
            DeclaredColumn::new("ok", DeclaredType::Bool),
        ]);
        let a = src.declared_columns(TenantHash([1u8; 16]), 10).await;
        let b = src.declared_columns(TenantHash([2u8; 16]), 999).await;
        assert_eq!(a, b, "a static source is a pure constant");
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].key, "duration_ms");
        assert_eq!(a[0].ty, DeclaredType::I64);
    }

    #[tokio::test]
    async fn empty_source_declares_nothing() {
        let src = StaticDeclaredColumns::empty();
        assert!(
            src.declared_columns(TenantHash([0u8; 16]), 0)
                .await
                .is_empty()
        );
    }
}

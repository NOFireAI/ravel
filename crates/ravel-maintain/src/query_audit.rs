//! Query-audit records (ADR-0042 decision 4, issue #391).
//!
//! `ravel-server` writes one immutable [`Signal::Audit`] record every time it
//! executes a tenant's SQL request through `POST /api/v1/sql`, so that
//! transport's query activity is durably logged and cannot be forged or
//! suppressed by the tenant: the record is written by the server itself, from
//! the interception point in the SQL handler, never derived from a
//! client-supplied request body.
//!
//! This does not yet cover every way a tenant can run SQL: the Flight SQL
//! transport (`services/ravel-server/src/flight.rs`) executes tenant queries
//! against the same `SqlExecutor` with no audit hook, so a tenant using that
//! transport today leaves no query-audit trail (tracked as a fast-follow).
//! The "cannot be forged or suppressed" property holds only for the
//! transport this module instruments.
//!
//! # Record shape
//!
//! A query-audit record rides RLOG v1 exactly like a legal-hold record
//! ([`crate::legal_hold`]), on its own [`QUERY_AUDIT_SHARD`] rather than
//! [`crate::legal_hold::AUDIT_HOLD_SHARD`]: `Signal::Audit` is not in
//! `services/ravel-server/src/maintain.rs`'s `MAINTAINED_SIGNALS`, so nothing
//! compacts or retention-sweeps it today, and one query-audit record per SQL
//! request is unbounded, permanent growth - collocating it with the
//! legal-hold control plane would mean every future legal-hold refresh (a
//! full shard listing) reads and discards an ever-growing pile of query
//! records. A distinct shard costs nothing today (no `Signal::Audit`
//! resolution exists yet for either shard - the generic `audit` SQL table,
//! crates/ravel-sql/src/audit_schema.rs, is not yet registered in any
//! session) and is unfixable later, once records are immutable and keyed.
//!
//! Every record carries:
//!
//! - `kind` = `query` (distinguishes it from a `legal_hold` record; the audit
//!   table's predicates select on this attr);
//! - `query.language` = the query language, `sql` today;
//! - `query.tenant` = the tenant's hex hash (the record is attributed to the
//!   resolved tenant, never to a client-supplied identity);
//! - `query.status` = `ok` or `error`, the request's outcome;
//! - `query.window_start_ns` / `query.window_end_ns` = the request's resolved
//!   time range (ADR-0042 decision 4 names time range as part of the record,
//!   alongside tenant, query text, and result status);
//! - `query.text` = the query text, verbatim. It is not truncated here; the
//!   SQL handler has already bounded the whole request body before this record
//!   is written.
//!
//! The record's `ts_ns` is the request timestamp (the handler's injected
//! clock), and its severity reflects the status: `INFO` for `ok`, `ERROR` for
//! `error`.

use ravel_logseg::{AttrValue, LogStreamId, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

use crate::audit_write::{AuditWrite, write_audit_object};
use crate::error::Result;

/// The [`Signal::Audit`] shard query-audit records are written to. Deliberately
/// distinct from [`crate::legal_hold::AUDIT_HOLD_SHARD`] - see the module doc.
pub const QUERY_AUDIT_SHARD: u32 = 1;

/// `attrs` key marking an audit record's kind. A query-audit record carries
/// [`KIND_QUERY`]; a legal-hold record carries `legal_hold`.
const ATTR_KIND: &str = "kind";
/// `attrs[ATTR_KIND]` value identifying a query-audit record.
const KIND_QUERY: &str = "query";
/// `attrs` key holding the query language (`sql`).
const ATTR_LANGUAGE: &str = "query.language";
/// `attrs` key holding the resolved tenant's hex hash.
const ATTR_TENANT: &str = "query.tenant";
/// `attrs` key holding the request outcome, [`QueryStatus::as_str`].
const ATTR_STATUS: &str = "query.status";
/// `attrs` key holding the resolved query window's start, in nanoseconds.
const ATTR_WINDOW_START: &str = "query.window_start_ns";
/// `attrs` key holding the resolved query window's end, in nanoseconds.
const ATTR_WINDOW_END: &str = "query.window_end_ns";
/// `attrs` key holding the verbatim query text.
const ATTR_TEXT: &str = "query.text";

/// Resource-attr `record_type` value for the shared query-audit log stream.
const STREAM_RECORD_TYPE: &str = "query_audit";
/// Scope name of the shared query-audit log stream.
const STREAM_SCOPE_NAME: &str = "ravel.query_audit";
const STREAM_SCOPE_VERSION: &str = "1";

/// The outcome of a SQL request, recorded under `query.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    /// The query executed and produced a result.
    Ok,
    /// The query failed (any `SqlError`); the record still names the attempt.
    Error,
}

impl QueryStatus {
    /// The `query.status` attr value.
    pub fn as_str(self) -> &'static str {
        match self {
            QueryStatus::Ok => "ok",
            QueryStatus::Error => "error",
        }
    }

    /// The record severity for this status: `INFO` for a successful query, and
    /// `ERROR` for a failed one, so the audit table's `severity_text` column
    /// reflects the outcome without decoding the `query.status` attr.
    fn severity(self) -> (u8, &'static str) {
        match self {
            // OTLP severity numbers: INFO=9, ERROR=17.
            QueryStatus::Ok => (9, "INFO"),
            QueryStatus::Error => (17, "ERROR"),
        }
    }
}

/// Write one immutable query-audit record for `tenant` at `now_ns`, recording
/// that a `language` query with `query_text` finished with `status`.
///
/// The record is written by the server, never derived from a client body, so a
/// tenant cannot forge or suppress it. A fresh `Uuid` is minted per call for
/// the object identity; the record is otherwise a pure function of its inputs.
/// The write is idempotent on the data object (content-addressed) exactly like
/// every other L0 audit write.
#[allow(clippy::too_many_arguments)]
pub async fn write_query_audit(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    now_ns: i64,
    query_text: &str,
    language: &str,
    status: QueryStatus,
    window_start_ns: i64,
    window_end_ns: i64,
) -> Result<()> {
    let (stream_id, stream_attrs) = query_stream();
    let (severity_num, severity_text) = status.severity();
    let attrs = vec![
        (ATTR_KIND.to_string(), AttrValue::Str(KIND_QUERY.into())),
        (ATTR_LANGUAGE.to_string(), AttrValue::Str(language.into())),
        (ATTR_TENANT.to_string(), AttrValue::Str(tenant.to_hex())),
        (
            ATTR_STATUS.to_string(),
            AttrValue::Str(status.as_str().into()),
        ),
        (
            ATTR_WINDOW_START.to_string(),
            AttrValue::Str(window_start_ns.to_string()),
        ),
        (
            ATTR_WINDOW_END.to_string(),
            AttrValue::Str(window_end_ns.to_string()),
        ),
        (ATTR_TEXT.to_string(), AttrValue::Str(query_text.into())),
    ];
    let body = format!("{language} query {}", status.as_str());
    write_audit_object(
        store,
        tenant,
        AuditWrite {
            shard: QUERY_AUDIT_SHARD,
            record_id: Uuid::new_v4(),
            now_ns,
            stream_id,
            stream_attrs,
            severity_num,
            severity_text: severity_text.to_string(),
            body,
            attrs,
        },
    )
    .await
}

/// The shared query-audit log stream's id and canonical resource+scope blob.
/// The id is the true hash of the blob (no placeholder), so the object records
/// real stream identity.
fn query_stream() -> (LogStreamId, Vec<u8>) {
    let resource = vec![(
        "ravel.record_type".to_string(),
        AttrValue::Str(STREAM_RECORD_TYPE.into()),
    )];
    let id = log_stream_id(&resource, STREAM_SCOPE_NAME, STREAM_SCOPE_VERSION, &[]);
    let blob = stream_attrs_bytes(&resource, STREAM_SCOPE_NAME, STREAM_SCOPE_VERSION, &[]);
    (id, blob)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use ravel_commit::keys;
    use ravel_commit::record;
    use ravel_logseg::{LogRecord, Predicate, RlogConfig, RlogReader};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
    use ravel_types::Signal;

    fn str_attr<'a>(row: &'a LogRecord, key: &str) -> Option<&'a str> {
        row.attrs.iter().find(|(k, _)| k == key).and_then(|(_, v)| {
            if let AttrValue::Str(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
    }

    /// Read back every RLOG record written to the tenant's query-audit shard.
    async fn read_audit_records(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
    ) -> Vec<LogRecord> {
        let prefix = keys::commit_shard_prefix(tenant, Signal::Audit, QUERY_AUDIT_SHARD).unwrap();
        let metas = list_all(store, &prefix).await.unwrap();
        let cfg = RlogConfig::default();
        let mut out = Vec::new();
        for meta in metas {
            let got = store.get(&meta.key, GetRange::Full).await.unwrap();
            let commit = record::decode(&got.data).unwrap();
            let data_key = keys::reconstruct_data_key(&commit).unwrap();
            let object = store.get(&data_key, GetRange::Full).await.unwrap();
            let reader = RlogReader::new(object.data.as_ref(), &cfg).unwrap();
            let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).unwrap();
            out.extend(rows);
        }
        out
    }

    #[tokio::test]
    async fn query_audit_record_round_trips_with_documented_attrs() {
        let store = MemoryStore::new();
        let tenant = TenantHash([7u8; 16]);
        let now_ns = 3 * 3_600_000_000_000;

        write_query_audit(
            &store,
            &tenant,
            now_ns,
            "SELECT value FROM samples LIMIT 1",
            "sql",
            QueryStatus::Ok,
            now_ns - 3_600_000_000_000,
            now_ns,
        )
        .await
        .expect("write ok record");

        let rows = read_audit_records(&store, &tenant).await;
        assert_eq!(rows.len(), 1, "exactly one audit record");
        let row = &rows[0];
        assert_eq!(row.ts_ns, now_ns);
        assert_eq!(row.severity_text, "INFO");
        assert_eq!(str_attr(row, "kind"), Some("query"));
        assert_eq!(str_attr(row, "query.language"), Some("sql"));
        assert_eq!(
            str_attr(row, "query.tenant"),
            Some(tenant.to_hex().as_str())
        );
        assert_eq!(str_attr(row, "query.status"), Some("ok"));
        assert_eq!(
            str_attr(row, "query.window_start_ns"),
            Some((now_ns - 3_600_000_000_000).to_string().as_str())
        );
        assert_eq!(
            str_attr(row, "query.window_end_ns"),
            Some(now_ns.to_string().as_str())
        );
        assert_eq!(
            str_attr(row, "query.text"),
            Some("SELECT value FROM samples LIMIT 1")
        );
    }

    #[tokio::test]
    async fn a_failed_query_records_status_error_and_error_severity() {
        let store = MemoryStore::new();
        let tenant = TenantHash([9u8; 16]);
        let now_ns = 5 * 3_600_000_000_000;

        write_query_audit(
            &store,
            &tenant,
            now_ns,
            "SELECT nope",
            "sql",
            QueryStatus::Error,
            now_ns - 3_600_000_000_000,
            now_ns,
        )
        .await
        .expect("write error record");

        let rows = read_audit_records(&store, &tenant).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].severity_text, "ERROR");
        assert_eq!(str_attr(&rows[0], "query.status"), Some("error"));
        assert_eq!(str_attr(&rows[0], "query.text"), Some("SELECT nope"));
    }

    #[tokio::test]
    async fn a_query_audit_record_never_registers_as_a_legal_hold() {
        use crate::legal_hold::LegalHoldCheck;

        let store = MemoryStore::new();
        let tenant = TenantHash([11u8; 16]);
        let now_ns = 7 * 3_600_000_000_000;

        write_query_audit(
            &store,
            &tenant,
            now_ns,
            "SELECT 1",
            "sql",
            QueryStatus::Ok,
            now_ns,
            now_ns,
        )
        .await
        .expect("write ok record");

        // Query-audit records live on a distinct shard from legal-hold's, so a
        // hold refresh (which only ever lists AUDIT_HOLD_SHARD) never even
        // observes them; this pins that isolation, not just the kind tag.
        let check = LegalHoldCheck::refresh(&store, &tenant)
            .await
            .expect("refresh");
        assert!(
            check.is_empty(),
            "a query-audit record must never be mistaken for an active hold"
        );
    }
}

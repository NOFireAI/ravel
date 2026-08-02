//! Query-audit records (ADR-0042 decision 4, issue #391).
//!
//! `ravel-server` writes one immutable [`Signal::Audit`] record every time it
//! executes a tenant's SQL request, so a tenant's own query activity is
//! durably logged and cannot be forged or suppressed by the tenant: the record
//! is written by the server itself, from the interception point in the SQL
//! handler, never derived from a client-supplied request body.
//!
//! # Record shape
//!
//! A query-audit record rides RLOG v1 exactly like a legal-hold record
//! ([`crate::legal_hold`]) and lands on the same control-plane audit shard
//! ([`crate::legal_hold::AUDIT_HOLD_SHARD`]), so the generic `audit` SQL table
//! (crates/ravel-sql/src/audit_schema.rs) surfaces it with no schema change -
//! only new attrs. Every record carries:
//!
//! - `kind` = `query` (distinguishes it from a `legal_hold` record on the same
//!   shard; the audit table's predicates select on this attr);
//! - `query.language` = the query language, `sql` today;
//! - `query.tenant` = the tenant's hex hash (the record is attributed to the
//!   resolved tenant, never to a client-supplied identity);
//! - `query.status` = `ok` or `error`, the request's outcome;
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
use crate::legal_hold::AUDIT_HOLD_SHARD;

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
pub async fn write_query_audit(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    now_ns: i64,
    query_text: &str,
    language: &str,
    status: QueryStatus,
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
        (ATTR_TEXT.to_string(), AttrValue::Str(query_text.into())),
    ];
    let body = format!("{language} query {}", status.as_str());
    write_audit_object(
        store,
        tenant,
        AuditWrite {
            shard: AUDIT_HOLD_SHARD,
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

    /// Read back every RLOG record written to the tenant's audit shard.
    async fn read_audit_records(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
    ) -> Vec<LogRecord> {
        let prefix = keys::commit_shard_prefix(tenant, Signal::Audit, AUDIT_HOLD_SHARD).unwrap();
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
        )
        .await
        .expect("write error record");

        let rows = read_audit_records(&store, &tenant).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].severity_text, "ERROR");
        assert_eq!(str_attr(&rows[0], "query.status"), Some("error"));
        assert_eq!(str_attr(&rows[0], "query.text"), Some("SELECT nope"));
    }
}

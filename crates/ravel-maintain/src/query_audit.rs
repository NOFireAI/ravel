//! Query-audit records (ADR-0042 decision 4).
//!
//! `ravel-server` writes one immutable [`Signal::Audit`] record every time it
//! executes a tenant's SQL request, so query activity is durably logged and
//! cannot be forged or suppressed by the tenant: the record is written by the
//! server itself, from the interception point in the SQL execution path, never
//! derived from a client-supplied request body or ticket.
//!
//! Both SQL transports write this record. `POST /api/v1/sql`
//! (`services/ravel-server/src/sql.rs`) writes one after every request, and the
//! Flight SQL transport (`crates/ravel-sql/src/flight`) writes one after every
//! executed statement, from `DoGet` where the statement runs through the same
//! `SqlExecutor`. The "cannot be forged or suppressed" property
//! now holds for every way a tenant runs SQL, not just one transport.
//!
//! The two paths share this writer and differ only in what a time window means
//! to each. The HTTP body carries an explicit event-time range, recorded
//! verbatim. A Flight statement's window is consumed at `GetFlightInfo` to
//! resolve and pin the snapshot and is not carried on the `DoGet` redemption
//! path, so the Flight path has no resolved window to record at the point it
//! audits and passes `window_start_ns`/`window_end_ns` as `0` (unknown) rather
//! than a fabricated range. Every other attribute is identical across the two.
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
//! records. The split is invisible to readers: `Signal::Audit`'s
//! `fixed_read_shards` is 2 (crates/ravel-types/src/lib.rs), so the
//! `Catalog::resolve` behind an `audit` query floors its scan set at both
//! shards, and the generic `audit` SQL table
//! (crates/ravel-sql/src/audit_schema.rs) is registered in the session
//! `crates/ravel-sql/src/session.rs` builds for that query. Which shard a
//! record rides is a write-side layout choice no reader sees, and it is
//! unfixable later, once records are immutable and keyed.
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
//! - `query.text` = the query text, in the posture the deployment configured
//!   (see [`AuditTextPolicy`]): the structure-preserving keyed tokenization of
//!   ADR-0062 decision 2e by default, or verbatim under an explicit opt-in. It
//!   is not truncated here; the SQL handler has already bounded the whole
//!   request body before this record is written.
//!
//! The record's `ts_ns` is the request timestamp (the handler's injected
//! clock), and its severity reflects the status: `INFO` for `ok`, `ERROR` for
//! `error`.

use std::sync::Arc;

use ravel_logseg::{AttrValue, LogStreamId, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::audit_pipeline::{AuditEvent, QueryAuditSink};
use crate::audit_write::{AuditWrite, write_audit_object};
use crate::error::Result;

/// The [`Signal::Audit`] shard query-audit records are written to. Deliberately
/// distinct from [`crate::legal_hold::AUDIT_HOLD_SHARD`] - see the module doc.
pub const QUERY_AUDIT_SHARD: u32 = 1;
// Moving this writer to a shard outside the reader's floor (ADR-1101
// decision 2) fails the build here rather than silently dropping every
// query-audit record from audit queries.
const _: () = assert!(QUERY_AUDIT_SHARD < Signal::Audit.fixed_read_shards());

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

/// Turns one surface's raw query text into the text its audit record is
/// allowed to carry, under the `redacted` posture of ADR-0062 decision 2e.
///
/// `language` is the record's `query.language` value, so one implementation
/// can dispatch to the right parser: `sql` text is a SQL statement, and every
/// other surface's text is PromQL (one expression for `promql` and
/// `analytics`, a joined selector list for `labels`, `label_values`, `series`,
/// and `exemplars`).
pub trait QueryTextRedactor: Send + Sync {
    /// The text to record for a `language` query whose raw text is
    /// `query_text`.
    ///
    /// Total by contract. An implementation that cannot parse `query_text`
    /// must still return text carrying no caller value (a token over the whole
    /// text is the fail-safe), never `query_text` itself: a redactor that
    /// echoed unparseable input would make the posture silently plaintext for
    /// exactly the inputs a caller shapes most freely.
    fn redact(&self, language: &str, query_text: &str) -> String;
}

/// How a query-audit record's `query.text` is recorded (ADR-0062 decision 2e).
///
/// `Redacted` carries a redactor rather than a token key because the two
/// languages' redactors live in `ravel-promql` and `ravel-sql`, and
/// `ravel-sql` depends on this crate: calling one from here would close a
/// dependency cycle. The deployment builds one and injects it, and
/// [`RedactingAuditSink`] applies it.
#[derive(Clone, Default)]
pub enum AuditTextPolicy {
    /// Record `query.text` verbatim.
    ///
    /// The `--audit-text plaintext` opt-in, and the default for an embedding
    /// that configures nothing: a process holds no token key it was not given.
    /// `ravel-server` refuses to start under `redacted` with no key rather than
    /// falling back to this.
    #[default]
    Plaintext,
    /// Record the redactor's structure-preserving keyed tokenization of
    /// `query.text`.
    Redacted(Arc<dyn QueryTextRedactor>),
}

impl std::fmt::Debug for AuditTextPolicy {
    /// Names the posture and nothing else: the redactor holds the deployment's
    /// token key, which must not reach a log line or an error body.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditTextPolicy::Plaintext => f.write_str("Plaintext"),
            AuditTextPolicy::Redacted(_) => f.write_str("Redacted(<redactor>)"),
        }
    }
}

impl AuditTextPolicy {
    /// `sink`, wrapped so it can only ever receive redacted text.
    ///
    /// Returns `sink` unchanged under [`AuditTextPolicy::Plaintext`], so a
    /// deployment that opted into verbatim text pays for no wrapper.
    pub fn wrap(&self, sink: Arc<dyn QueryAuditSink>) -> Arc<dyn QueryAuditSink> {
        match self {
            AuditTextPolicy::Plaintext => sink,
            AuditTextPolicy::Redacted(redactor) => {
                Arc::new(RedactingAuditSink::new(sink, Arc::clone(redactor)))
            }
        }
    }
}

/// A [`QueryAuditSink`] that rewrites an event's `query.text` into its
/// redacted form before the sink it wraps ever sees the event.
///
/// One instance wraps the process's one pipeline, so the pipeline, the RLOG
/// object it writes, and that object's commit record only ever hold redacted
/// text. Wrapping the sink rather than each surface's event construction is
/// deliberate: a surface has to reach a sink to make its record durable (the
/// Flight stream's cancellation path submits from `Drop`, not from a handler),
/// so there is no way to add an audited query surface that skips redaction.
pub struct RedactingAuditSink {
    inner: Arc<dyn QueryAuditSink>,
    redactor: Arc<dyn QueryTextRedactor>,
}

impl RedactingAuditSink {
    /// Wrap `inner` so every event it receives has been through `redactor`.
    pub fn new(inner: Arc<dyn QueryAuditSink>, redactor: Arc<dyn QueryTextRedactor>) -> Self {
        RedactingAuditSink { inner, redactor }
    }
}

#[async_trait::async_trait]
impl QueryAuditSink for RedactingAuditSink {
    async fn submit(&self, mut event: AuditEvent) -> Result<()> {
        redact_event_text(&mut event, self.redactor.as_ref());
        self.inner.submit(event).await
    }
}

/// Replace `event`'s `query.text` attr with `redactor`'s form of it, picking
/// the language from its `query.language` attr.
///
/// An event carrying no `query.text` is left untouched: a legal-hold or
/// reshard record has no query text to redact, and neither reaches this sink
/// today.
fn redact_event_text(event: &mut AuditEvent, redactor: &dyn QueryTextRedactor) {
    let language = event
        .attrs
        .iter()
        .find(|(key, _)| key == ATTR_LANGUAGE)
        .and_then(|(_, value)| match value {
            AttrValue::Str(language) => Some(language.clone()),
            _ => None,
        })
        .unwrap_or_default();
    for (key, value) in event.attrs.iter_mut() {
        if key == ATTR_TEXT {
            if let AttrValue::Str(text) = value {
                *text = redactor.redact(&language, text);
            }
        }
    }
}

/// Build one query-audit [`AuditEvent`] for `tenant` at `now_ns`, recording
/// that a `language` query with `query_text` finished with `status`, over the
/// resolved `[window_start_ns, window_end_ns]` range.
///
/// This is the record content every query surface submits through the
/// [`QueryAuditSink`](crate::audit_pipeline::QueryAuditSink) seam (ADR-0062
/// §2a). It is the single source of the query-audit
/// record shape: the attrs, the shared log stream, and the status-derived
/// severity live here so every surface -- PromQL, SQL HTTP, Flight SQL,
/// analytics, exemplars -- emits one identical schema differing only in the
/// `language` value and the recorded text/status/window. [`write_query_audit`]
/// is the degenerate direct-write path built on this same event, kept for the
/// callers that write a single record per call without a pipeline.
///
/// The event carries the record content and the tenant it belongs to; the
/// pipeline owns the shard and mints the per-batch object identity
/// (`record_id`). `tenant` appears twice on purpose: as the event's `tenant`
/// field, which routes the record to that tenant's own audit prefix when the
/// pipeline flushes, and as the `query.tenant` attr, so a reader sees the
/// tenant Ravel resolved rather than any identity the client claimed.
///
/// `query_text` is recorded as given. The `--audit-text` posture is applied on
/// the way to the pipeline by [`RedactingAuditSink`], which every surface's
/// events pass through, so a caller here never has to decide whether the text
/// it holds may be stored.
pub fn query_audit_event(
    tenant: &TenantHash,
    now_ns: i64,
    query_text: &str,
    language: &str,
    status: QueryStatus,
    window_start_ns: i64,
    window_end_ns: i64,
) -> AuditEvent {
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
    AuditEvent {
        tenant: *tenant,
        now_ns,
        stream_id,
        stream_attrs,
        severity_num,
        severity_text: severity_text.to_string(),
        body,
        attrs,
    }
}

/// Write one immutable query-audit record for `tenant` at `now_ns`, recording
/// that a `language` query with `query_text` finished with `status`.
///
/// The record is written by the server, never derived from a client body, so a
/// tenant cannot forge or suppress it. A fresh `Uuid` is minted per call for
/// the object identity; the record is otherwise a pure function of its inputs.
/// The write is idempotent on the data object (content-addressed) exactly like
/// every other L0 audit write. The record content is built by
/// [`query_audit_event`], the same shape every pipeline-backed surface submits.
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
    let event = query_audit_event(
        tenant,
        now_ns,
        query_text,
        language,
        status,
        window_start_ns,
        window_end_ns,
    );
    write_audit_object(
        store,
        tenant,
        AuditWrite {
            shard: QUERY_AUDIT_SHARD,
            record_id: Uuid::new_v4(),
            now_ns: event.now_ns,
            stream_id: event.stream_id,
            stream_attrs: event.stream_attrs,
            severity_num: event.severity_num,
            severity_text: event.severity_text,
            body: event.body,
            attrs: event.attrs,
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

    /// A redactor that reports what it was asked to redact and returns a
    /// fixed marker, so a test can prove the rewrite happened and that the
    /// language reached the redactor.
    #[derive(Default)]
    struct RecordingRedactor {
        seen: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl QueryTextRedactor for RecordingRedactor {
        fn redact(&self, language: &str, query_text: &str) -> String {
            self.seen
                .lock()
                .unwrap()
                .push((language.to_string(), query_text.to_string()));
            format!("redacted<{language}>")
        }
    }

    /// A sink that keeps every event it received, so a test can assert on what
    /// the pipeline would have written.
    #[derive(Default)]
    struct RecordingSink {
        events: std::sync::Mutex<Vec<AuditEvent>>,
    }

    #[async_trait::async_trait]
    impl QueryAuditSink for RecordingSink {
        async fn submit(&self, event: AuditEvent) -> Result<()> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn the_redacting_sink_rewrites_query_text_before_the_inner_sink_sees_it() {
        let inner = Arc::new(RecordingSink::default());
        let redactor = Arc::new(RecordingRedactor::default());
        let policy = AuditTextPolicy::Redacted(redactor.clone());
        let sink = policy.wrap(inner.clone());

        let tenant = TenantHash([13u8; 16]);
        let event = query_audit_event(
            &tenant,
            42,
            "SELECT value FROM samples WHERE user = 'alice'",
            "sql",
            QueryStatus::Ok,
            0,
            42,
        );
        sink.submit(event).await.expect("submit");

        let seen = redactor.seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![(
                "sql".to_string(),
                "SELECT value FROM samples WHERE user = 'alice'".to_string()
            )],
            "the redactor is called exactly once, with the record's language"
        );

        let events = inner.events.lock().unwrap();
        assert_eq!(events.len(), 1, "exactly one event reaches the inner sink");
        let text = events[0]
            .attrs
            .iter()
            .filter(|(key, _)| key == ATTR_TEXT)
            .map(|(_, value)| match value {
                AttrValue::Str(text) => text.clone(),
                _ => panic!("query.text is a string attr"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            text,
            vec!["redacted<sql>".to_string()],
            "exactly one query.text attr, holding the redacted form"
        );
    }

    #[tokio::test]
    async fn the_plaintext_policy_leaves_the_sink_and_the_text_unchanged() {
        let inner = Arc::new(RecordingSink::default());
        let sink = AuditTextPolicy::Plaintext.wrap(inner.clone());

        let tenant = TenantHash([17u8; 16]);
        sink.submit(query_audit_event(
            &tenant,
            42,
            "up{job=\"api\"}",
            "promql",
            QueryStatus::Ok,
            0,
            42,
        ))
        .await
        .expect("submit");

        let events = inner.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]
                .attrs
                .iter()
                .find(|(key, _)| key == ATTR_TEXT)
                .map(|(_, value)| value.clone()),
            Some(AttrValue::Str("up{job=\"api\"}".to_string())),
            "plaintext records the text verbatim"
        );
    }

    #[tokio::test]
    async fn the_redacting_sink_leaves_a_record_with_no_query_text_alone() {
        let inner = Arc::new(RecordingSink::default());
        let redactor = Arc::new(RecordingRedactor::default());
        let sink = AuditTextPolicy::Redacted(redactor.clone()).wrap(inner.clone());

        // A legal-hold-shaped event: no `query.text`, no `query.language`.
        let (stream_id, stream_attrs) = query_stream();
        sink.submit(AuditEvent {
            tenant: TenantHash([19u8; 16]),
            now_ns: 42,
            stream_id,
            stream_attrs,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "legal hold set".to_string(),
            attrs: vec![(
                ATTR_KIND.to_string(),
                AttrValue::Str("legal_hold".to_string()),
            )],
        })
        .await
        .expect("submit");

        assert_eq!(
            redactor.seen.lock().unwrap().len(),
            0,
            "nothing to redact on a record carrying no query text"
        );
        let events = inner.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].attrs,
            vec![(
                ATTR_KIND.to_string(),
                AttrValue::Str("legal_hold".to_string())
            )],
            "the attrs pass through unchanged"
        );
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

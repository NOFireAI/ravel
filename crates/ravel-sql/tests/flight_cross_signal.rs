//! Flight SQL over statements that name two or more signal tables (ADR-2904
//! decision 10): `GetFlightInfo` resolves one snapshot per signal table and
//! mints a version 10 ticket carrying one segment group per signal, and
//! `DoGet` rebuilds exactly those snapshots without resolving again.
//!
//! These tests drive the real resolve -> mint -> redeem pipeline over real
//! RSEG and RLOG objects. The refusal tests forge tickets the honest mint
//! never produces by re-signing a modified decode under the service's own
//! ticket key, so only the modified field is under test, and assert the
//! refusal happened before any store request.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::sync::Arc;

use arrow_flight::Ticket;
use arrow_flight::sql::{ProstMessageExt, TicketStatementQuery};
use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::pretty::pretty_format_batches;
use prost::Message;
use ravel_commit::{erasure, keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_sql::{
    FlightTicket, MSG_UNSATISFIABLE, MinCommitToken, SqlError, TargetSignal, TicketKey,
};
use ravel_types::{CommitToken, Signal, TenantHash, TenantId};
use tonic::Code;
use util::flight_harness::{Harness, MIN_COMMIT_TOKEN_KEY, statement_ticket};
use util::{CountingStore, SegSpec, SeriesSpec, tenant_id};
use uuid::Uuid;

const JOIN: &str = "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts ORDER BY s.ts";

/// Samples at 100 (cpu), 150 (mem) and 200 (cpu), 300 (cpu).
fn sample_specs() -> Vec<SegSpec> {
    vec![
        SegSpec::new(
            10,
            1,
            1,
            vec![
                SeriesSpec::new("cpu", vec![(100, 1.0), (200, 2.0)]),
                SeriesSpec::new("mem", vec![(150, 7.5)]),
            ],
        ),
        SegSpec::new(20, 1, 2, vec![SeriesSpec::new("cpu", vec![(300, 3.0)])]),
    ]
}

/// A harness over `store` whose tenant `acme` holds [`sample_specs`] and one
/// logs object at 100, 150, 200 and 400.
async fn harness_over(store: Arc<dyn ObjectStoreBackend>) -> (Harness, TenantId) {
    let tenant = tenant_id("acme");
    let harness = Harness::build(store, &[(&tenant, &sample_specs())]).await;
    util::publish_logs(harness.store.as_ref(), &tenant, 0, &[100, 150, 200, 400]).await;
    (harness, tenant)
}

/// The first column of every batch, cast to `i64`, in stream order.
fn first_column(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let col = cast(batch.column(0), &DataType::Int64).expect("cast to i64");
        let col = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 column");
        out.extend((0..col.len()).map(|i| col.value(i)));
    }
    out
}

fn wrap(handle: Vec<u8>) -> Ticket {
    Ticket::new(
        TicketStatementQuery {
            statement_handle: handle.into(),
        }
        .as_any()
        .encode_to_vec(),
    )
}

fn decode(ticket: &Ticket, key: &TicketKey) -> FlightTicket {
    FlightTicket::decode(&statement_ticket(ticket).statement_handle, key).expect("ticket decodes")
}

/// Writes a real `.dreq` for `signal` erasing `key=value`, picked up by the
/// next resolve of that signal.
async fn put_dreq(
    store: &dyn ObjectStoreBackend,
    tenant: TenantHash,
    sig: Signal,
    key: &str,
    value: &str,
) {
    let request_id = Uuid::new_v4();
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant.0.to_vec(),
        signal: signal::to_proto(sig) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 1,
        predicate: vec![ErasurePredicateMatcher {
            key: key.to_string(),
            value: value.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    let object_key = keys::erasure_request_key(&tenant, sig, request_id).expect("dreq key");
    store
        .put(
            &object_key,
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

/// A commit to each signal lands between `GetFlightInfo` and `DoGet`, and
/// each would add a joined row: samples 400 joins the existing log at 400,
/// and a log at 300 joins the existing sample at 300. The pinned `DoGet`
/// sees neither, so each signal's pin is shown separately, and a fresh
/// `GetFlightInfo` sees both.
#[tokio::test]
async fn do_get_reads_exactly_the_pinned_snapshots_of_both_signals() {
    let (harness, tenant) = harness_over(Arc::new(MemoryStore::new())).await;

    let ticket = harness
        .get_flight_info("acme", JOIN)
        .await
        .expect("flight info");
    let handle = statement_ticket(&ticket).statement_handle;
    assert_eq!(handle[4], 10, "a two-signal statement mints version 10");
    let decoded = decode(&ticket, harness.service.ticket_key());
    let signals: Vec<TargetSignal> = decoded.signal_groups.iter().map(|g| g.signal).collect();
    assert_eq!(signals, vec![TargetSignal::Metrics, TargetSignal::Logs]);
    assert_eq!(decoded.signal_groups[0].segments.len(), 2);
    assert_eq!(decoded.signal_groups[1].segments.len(), 1);
    assert!(decoded.segments.is_empty(), "v10 carries no flat pins");

    let extra = SegSpec::new(30, 1, 3, vec![SeriesSpec::new("cpu", vec![(400, 4.0)])]);
    util::publish_segment(harness.store.as_ref(), &tenant, 99, &extra).await;
    util::publish_logs(harness.store.as_ref(), &tenant, 1, &[300]).await;

    let pinned = harness.do_get("acme", &ticket).await.expect("do get");
    let pinned_ts = first_column(&pinned);
    assert!(
        !pinned_ts.contains(&400),
        "samples figure: the samples commit after GetFlightInfo is not visible, got {pinned_ts:?}"
    );
    assert!(
        !pinned_ts.contains(&300),
        "logs figure: the logs commit after GetFlightInfo is not visible, got {pinned_ts:?}"
    );
    assert_eq!(pinned_ts, vec![100, 150, 200]);

    let fresh_ticket = harness
        .get_flight_info("acme", JOIN)
        .await
        .expect("fresh flight info");
    let fresh = harness
        .do_get("acme", &fresh_ticket)
        .await
        .expect("fresh do get");
    assert_eq!(first_column(&fresh), vec![100, 150, 200, 300, 400]);
}

/// A one-signal ticket whose statement is swapped for a two-signal one is
/// refused at `DoGet` with INVALID_ARGUMENT, before any store request.
#[tokio::test]
async fn a_one_signal_ticket_over_a_two_signal_statement_is_refused_before_any_store_request() {
    let counting = CountingStore::new(Arc::new(MemoryStore::new()));
    let (harness, _) = harness_over(Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>).await;

    let ticket = harness
        .get_flight_info("acme", "SELECT ts FROM samples ORDER BY ts")
        .await
        .expect("flight info");
    assert_eq!(
        statement_ticket(&ticket).statement_handle[4],
        8,
        "a one-signal statement keeps minting version 8"
    );
    let key = harness.service.ticket_key();
    let mut forged = decode(&ticket, key);
    assert!(forged.signal_groups.is_empty());
    forged.statement = JOIN.to_string();
    let handle = forged.encode(key).expect("re-encode");
    assert_eq!(handle[4], 8, "a ticket with no groups keeps version 8");

    let before = counting.total_ops();
    let err = harness
        .do_get("acme", &wrap(handle))
        .await
        .expect_err("a one-signal pin cannot answer two signals");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    assert!(err.message().contains("pins one signal table"), "{err:?}");
    assert_eq!(
        counting.total_ops(),
        before,
        "refused before any store request"
    );
}

/// A version 10 ticket whose groups name a different set of signal tables
/// than its statement reads is refused at `DoGet` with INVALID_ARGUMENT,
/// before any store request.
#[tokio::test]
async fn v10_groups_that_differ_from_the_statement_are_refused_before_any_store_request() {
    let counting = CountingStore::new(Arc::new(MemoryStore::new()));
    let (harness, _) = harness_over(Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>).await;

    let ticket = harness
        .get_flight_info("acme", JOIN)
        .await
        .expect("flight info");
    let key = harness.service.ticket_key();
    let mut forged = decode(&ticket, key);
    forged.signal_groups[1].signal = TargetSignal::Spans;
    let handle = forged.encode(key).expect("re-encode");
    assert_eq!(handle[4], 10);

    let before = counting.total_ops();
    let err = harness
        .do_get("acme", &wrap(handle))
        .await
        .expect_err("groups samples,spans cannot answer samples JOIN logs");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    assert!(
        err.message().contains("(samples, spans)") && err.message().contains("(samples, logs)"),
        "{err:?}"
    );
    assert_eq!(
        counting.total_ops(),
        before,
        "refused before any store request"
    );
}

/// Each group carries the erasure its own resolve saw pending: a `.dreq`
/// erasing `mem` removes the joined row at 150, and the same honestly
/// resolved ticket with the samples group's erasure cleared, re-signed under
/// the real key, brings it back.
#[tokio::test]
async fn each_signal_group_carries_its_own_pending_erasure() {
    let store = Arc::new(MemoryStore::new());
    let (harness, tenant) = harness_over(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>).await;
    put_dreq(
        store.as_ref(),
        tenant.hash(),
        Signal::Metrics,
        "__name__",
        "mem",
    )
    .await;

    let ticket = harness
        .get_flight_info("acme", JOIN)
        .await
        .expect("flight info");
    let key = harness.service.ticket_key();
    let mut cleared = decode(&ticket, key);
    assert_eq!(cleared.signal_groups[0].pending_erasure.len(), 1);
    assert!(cleared.signal_groups[1].pending_erasure.is_empty());

    let honest = harness.do_get("acme", &ticket).await.expect("do get");
    assert_eq!(first_column(&honest), vec![100, 200]);

    cleared.signal_groups[0].pending_erasure.clear();
    let leaked = harness
        .do_get("acme", &wrap(cleared.encode(key).expect("re-encode")))
        .await
        .expect("do get with cleared erasure");
    assert_eq!(first_column(&leaked), vec![100, 150, 200]);
}

/// A two-signal statement returns the same rows over Flight SQL as over
/// `POST /api/v1/sql` on the same committed set.
#[tokio::test]
async fn a_two_signal_statement_agrees_with_http() {
    let (harness, tenant) = harness_over(Arc::new(MemoryStore::new())).await;
    let sql = "SELECT s.ts, s.value, l.severity_text, l.body \
               FROM samples s JOIN logs l ON s.ts = l.ts ORDER BY s.ts, s.value";

    let http = harness
        .executor
        .execute(tenant.hash(), &util::request(sql))
        .await
        .expect("http execute");
    let ticket = harness
        .get_flight_info("acme", sql)
        .await
        .expect("flight info");
    let flight = harness.do_get("acme", &ticket).await.expect("do get");

    let http_text = pretty_format_batches(http.output.batches())
        .expect("format http")
        .to_string();
    let flight_text = pretty_format_batches(&flight)
        .expect("format flight")
        .to_string();
    assert_eq!(flight_text, http_text);
    assert_eq!(first_column(&flight), vec![100, 150, 200]);
}

/// The HTTP statement path's error for `sql` carrying `tokens`.
async fn http_error(harness: &Harness, tenant: &TenantId, sql: &str, tokens: &[&str]) -> SqlError {
    let mut request = util::request(sql);
    request.min_tokens = tokens
        .iter()
        .map(|raw| MinCommitToken::parse(raw).expect("a token the HTTP path parses"))
        .collect();
    harness
        .executor
        .execute(tenant.hash(), &request)
        .await
        .expect_err("the HTTP path refuses it too")
}

/// The commit token [`util::publish_logs`] returns for index 1, known before
/// that commit is published so a test can carry it while it is unsatisfiable.
fn second_logs_commit_token() -> CommitToken {
    CommitToken {
        shard: 0,
        writer_id: Uuid::from_u128(0x7001),
        epoch: 1,
        seq: 2,
        ingest_hour_bucket: 0,
    }
}

/// An unqualified token on a statement naming two signal tables is refused at
/// `GetFlightInfo` with the HTTP path's error, `CommitTokenNeedsQualifier`,
/// whose text tells the client to qualify it as `<table>:<token>`; the same
/// statement with no token runs, and a one-table statement takes the token
/// unqualified or qualified with its table.
///
/// FLIP: admitting an unqualified token on the one lane `first_lane` names,
/// whatever the statement's table count, mints a ticket for the join.
#[tokio::test]
async fn a_plain_commit_token_on_a_two_signal_statement_is_refused_at_get_flight_info() {
    let (harness, tenant) = harness_over(Arc::new(MemoryStore::new())).await;
    let extra = SegSpec::new(30, 1, 3, vec![SeriesSpec::new("cpu", vec![(400, 4.0)])]);
    let (token, _) = util::publish_segment(harness.store.as_ref(), &tenant, 99, &extra).await;
    let plain = token.encode();

    let without_token = harness
        .get_flight_info("acme", JOIN)
        .await
        .expect("no token: flight info");
    assert_eq!(
        first_column(
            &harness
                .do_get("acme", &without_token)
                .await
                .expect("do get")
        ),
        vec![100, 150, 200, 400]
    );

    let err = harness
        .get_flight_info_with_tokens("acme", JOIN, &[&plain])
        .await
        .expect_err("a plain token cannot say which signal it names");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    let expected = SqlError::CommitTokenNeedsQualifier {
        tables: "samples, logs".to_owned(),
    };
    assert_eq!(err.message(), expected.client_message());
    assert_eq!(
        err.message(),
        "an unqualified min_commit_token is ambiguous on a statement that reads more than \
         one of the samples, logs, spans, alerts and audit tables; qualify \
         min_commit_token with one of samples, logs, as <table>:<token>"
    );
    assert_eq!(
        err.message(),
        http_error(&harness, &tenant, JOIN, &[&plain])
            .await
            .client_message(),
        "the HTTP path's text"
    );

    for value in [plain.clone(), format!("samples:{plain}")] {
        let one_table = harness
            .get_flight_info_with_tokens("acme", "SELECT ts FROM samples ORDER BY ts", &[&value])
            .await
            .expect("a one-table statement admits the token");
        assert_eq!(
            first_column(&harness.do_get("acme", &one_table).await.expect("do get")),
            vec![100, 150, 200, 300, 400],
            "{value}"
        );
    }
}

/// A `logs:`-qualified token on a two-signal Flight statement bounds the logs
/// resolve and only that one: while its commit is unpublished `GetFlightInfo`
/// is UNAVAILABLE with the fixed unsatisfiable-token message, once published
/// the statement runs and reads the new log line, and the same token
/// qualified `samples:` is still unsatisfiable because no samples commit
/// carries it. The ticket keeps the token as an untagged list.
///
/// FLIP: refusing any qualified token at `GetFlightInfo`, as the Flight path
/// did before ADR-2904 decision 3 reached it, fails the first assertion with
/// INVALID_ARGUMENT; applying a qualified token to every signal's resolve
/// fails the published `logs:` case with UNAVAILABLE; dropping qualified
/// tokens mints a ticket while the commit is unpublished.
#[tokio::test]
async fn a_logs_qualified_token_bounds_only_the_logs_resolve_of_a_two_signal_statement() {
    let (harness, tenant) = harness_over(Arc::new(MemoryStore::new())).await;
    let pending = second_logs_commit_token();
    let logs_qualified = format!("logs:{}", pending.encode());
    let samples_qualified = format!("samples:{}", pending.encode());

    let err = harness
        .get_flight_info_with_tokens("acme", JOIN, &[&logs_qualified])
        .await
        .expect_err("the logs commit is not published yet");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert_eq!(err.message(), MSG_UNSATISFIABLE);

    let (published, _) = util::publish_logs(harness.store.as_ref(), &tenant, 1, &[300]).await;
    assert_eq!(
        published, pending,
        "the token names the commit just published"
    );

    let ticket = harness
        .get_flight_info_with_tokens("acme", JOIN, &[&logs_qualified])
        .await
        .expect("logs:T bounds the logs resolve");
    assert_eq!(
        decode(&ticket, harness.service.ticket_key()).min_commit_tokens,
        vec![pending.clone()],
        "the ticket carries the token untagged"
    );
    assert_eq!(
        first_column(&harness.do_get("acme", &ticket).await.expect("do get")),
        vec![100, 150, 200, 300]
    );

    let err = harness
        .get_flight_info_with_tokens("acme", JOIN, &[&samples_qualified])
        .await
        .expect_err("no samples commit carries T");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert_eq!(err.message(), MSG_UNSATISFIABLE);
}

/// A qualifier naming a table the statement does not read is refused at
/// `GetFlightInfo` with the HTTP path's `CommitTokenQualifierNotRead`.
///
/// FLIP: routing an unknown qualifier to the statement's first lane instead
/// of refusing it mints a ticket.
#[tokio::test]
async fn a_qualifier_the_flight_statement_does_not_read_is_refused() {
    let (harness, tenant) = harness_over(Arc::new(MemoryStore::new())).await;
    let spans_qualified = format!("spans:{}", second_logs_commit_token().encode());

    let err = harness
        .get_flight_info_with_tokens("acme", JOIN, &[&spans_qualified])
        .await
        .expect_err("the join reads no spans");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    let expected = SqlError::CommitTokenQualifierNotRead {
        qualifier: "spans".to_owned(),
        tables: "samples, logs".to_owned(),
    };
    assert_eq!(err.message(), expected.client_message());
    assert_eq!(
        err.message(),
        http_error(&harness, &tenant, JOIN, &[&spans_qualified])
            .await
            .client_message(),
        "the HTTP path's text"
    );
}

/// A malformed qualified value is refused at `GetFlightInfo` with
/// `MinCommitToken::parse`'s error, before any resolve.
///
/// FLIP: a parse of its own that splits at the last colon accepts
/// `a:b:<token>` with the qualifier `a:b`, and one that skips an empty
/// qualifier accepts `:<token>`.
#[tokio::test]
async fn a_malformed_qualified_flight_token_is_refused_with_the_parse_error() {
    let (harness, _tenant) = harness_over(Arc::new(MemoryStore::new())).await;
    let encoded = second_logs_commit_token().encode();
    for raw in [format!(":{encoded}"), format!("a:b:{encoded}")] {
        let parse_err = MinCommitToken::parse(&raw).expect_err("malformed");
        let err = harness
            .get_flight_info_with_tokens("acme", JOIN, &[&raw])
            .await
            .expect_err("refused");
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
        assert_eq!(
            err.message(),
            format!("invalid {MIN_COMMIT_TOKEN_KEY}: {raw:?}: {parse_err}")
        );
    }
}

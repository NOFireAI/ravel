//! Flight SQL flip-proof tests for the ADR-0064 decision 3 ticket carry (issue
//! #829). A prior review found that `FlightTicket::snapshot()` hardcoded
//! `pending_erasure: Vec::new()`, so both `DoGet` data paths -- the whole-set
//! statement path and the internal slice-fragment path -- executed with zero
//! exclusion regardless of what `GetFlightInfo`'s resolve actually saw
//! pending. These tests drive the real resolve -> mint -> redeem pipeline
//! (a real `.dreq` object in the store, the real `Catalog::resolve`, the real
//! `get_flight_info_statement`/`do_get_statement` RPCs) and prove the carry
//! both ways: the honest ticket excludes, and a ticket built the way the
//! pre-fix code would have built it (empty `pending_erasure`, re-signed under
//! the same real key so only the missing field is under test) does not.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::Ticket;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{ProstMessageExt, TicketStatementQuery};
use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::distrib::partition::DistribThresholds;
use ravel_query::{
    LogSegmentFetcher, QueryAdmissionController, QueryConcurrencyLimit, SegmentFetcher,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{
    DistributedFlightConfig, FlightClock, FlightSqlConfig, FlightTicket, RavelFlightSqlService,
    SegmentPin, SqlConfig, SqlExecutor,
};
use ravel_types::accounting::NoopQueryCostRecorder;
use ravel_types::{Sample, SeriesId, Signal, TenantHash, TenantId};
use tonic::Request;
use uuid::Uuid;

use util::flight_harness::{
    Harness, TOKEN_KEY, TestAuth, TestClock, collect, insert, merged, specs, statement_ticket,
};
use util::tenant_id;

const QUERY: &str = "SELECT ts, value FROM samples ORDER BY series_id, ts";

/// Writes a real `.dreq` erasure request object under `tenant_hash`'s metrics
/// `del/` prefix, exactly the erasure submit path does. The next
/// `Catalog::resolve` -- which `GetFlightInfo` calls -- picks it up.
async fn put_dreq(store: &MemoryStore, tenant_hash: TenantHash, key: &str, value: &str) {
    let request_id = Uuid::new_v4();
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
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
    let object_key =
        keys::erasure_request_key(&tenant_hash, Signal::Metrics, request_id).expect("dreq key");
    store
        .put(
            &object_key,
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

/// Publishes one real RSEG segment on `shard`, so a snapshot spanning two
/// calls with different `shard`s genuinely partitions into two shard-major
/// slices under `plan_distributed_slices` (unlike `util::publish_segment`,
/// which hardcodes `shard: 0` for every segment it writes).
async fn publish_segment_on_shard(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    shard: u32,
    created_unix_ns: i64,
    series: &[(&str, &[(i64, f64)])],
) {
    let tenant_hash = tenant.hash();
    let inputs: Vec<SeriesInput> = series
        .iter()
        .map(|(metric, samples)| SeriesInput {
            series_id: SeriesId(util::series_id_for(tenant, metric)),
            labels: util::labels_for(metric),
            samples: samples
                .iter()
                .map(|(ts_ns, value)| Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        })
        .collect();
    let writer_id = Uuid::from_u128(9_000 + index as u128);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Builds a Flight SQL service and its executor over a real two-shard
/// catalog for `tenant`, so `plan_distributed_slices` has more than one
/// shard-major group to cut.
async fn build_shard_service(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
) -> (RavelFlightSqlService, Arc<SqlExecutor>) {
    let catalog_config = CatalogConfig {
        shard_count: 2,
        ..CatalogConfig::default()
    };
    let catalog = Arc::new(Catalog::new(Arc::clone(&store), catalog_config).expect("catalog"));
    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let executor = Arc::new(SqlExecutor::new(
        Arc::clone(&catalog),
        fetcher,
        LogSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    ));
    let auth = TestAuth::new(&[(tenant.as_str(), tenant)]);
    let clock = TestClock::at(util::NOW_NS);
    let service = RavelFlightSqlService::new(
        Arc::clone(&executor),
        auth,
        Arc::clone(&clock) as Arc<dyn FlightClock>,
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            ..FlightSqlConfig::default()
        },
        Arc::new(NoopQueryCostRecorder),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
    );
    (service, executor)
}

/// `DoGet` for a raw ticket as `token`, over `service` directly (not through
/// [`Harness`], which always publishes to a single shard).
async fn do_get_raw(
    service: &RavelFlightSqlService,
    token: &str,
    ticket: &Ticket,
) -> Vec<datafusion::arrow::array::RecordBatch> {
    let tsq = statement_ticket(ticket);
    let mut request = Request::new(ticket.clone());
    insert(request.metadata_mut(), TOKEN_KEY, token);
    let stream = service
        .do_get_statement(tsq, request)
        .await
        .expect("do_get slice")
        .into_inner();
    collect(stream).await.expect("collect slice")
}

/// Re-encode `ticket` under the harness's real in-process key, the way an
/// attacker cannot (they lack the key) but the *server itself* would if a
/// mint site forgot to carry `pending_erasure` forward -- exactly the bug
/// this suite guards. Isolates the missing-field failure mode from any MAC
/// or transport concern: the resulting ticket is honestly signed, only its
/// `pending_erasure` differs from what the real resolve saw.
fn re_encode_with_erasure_cleared(ticket: &Ticket, key: &ravel_sql::TicketKey) -> Ticket {
    let mut decoded = FlightTicket::decode(&statement_ticket(ticket).statement_handle, key)
        .expect("ticket decodes");
    // The exact line this suite proves matters: `flight/service.rs`'s
    // `let pending_erasure = ravel_query::erasure::snapshot_pending_erasure_predicates(&snapshot);`
    // flipped back to the pre-fix `pending_erasure: Vec::new()` this ticket
    // would have carried.
    decoded.pending_erasure = Vec::new();
    let handle = decoded.encode(key).expect("re-encode");
    Ticket::new(
        TicketStatementQuery {
            statement_handle: handle.into(),
        }
        .as_any()
        .encode_to_vec(),
    )
}

/// Statement path (`GetFlightInfo` -> `DoGet` over the whole pinned set,
/// `slice_count == 1`): a real pending `.dreq` for `mem` excludes the `mem`
/// sample and keeps every `cpu` sample once `GetFlightInfo` has minted the
/// ticket and `DoGet` has redeemed it.
///
/// Flip-proof: [`re_encode_with_erasure_cleared`] rebuilds the same honestly
/// resolved ticket with `pending_erasure` forced back to empty (the shape
/// `flight/service.rs`'s hardcoded `pending_erasure: Vec::new()` produced
/// before the fix) and re-signs it under the real key. Redeeming that ticket
/// must bring the erased `mem` row back, proving the exclusion in the
/// honest-ticket assertion above is not vacuous.
#[tokio::test]
async fn statement_do_get_excludes_what_the_resolve_saw_pending() {
    let tenant = tenant_id("acme");
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let harness = Harness::build(backend, &[(&tenant, &specs())]).await;

    put_dreq(&store, tenant.hash(), "__name__", "mem").await;

    let ticket = harness
        .get_flight_info("acme", QUERY)
        .await
        .expect("flight info");

    let excluded = harness.do_get("acme", &ticket).await.expect("do get");
    let excluded_rows = merged(&excluded);
    assert_eq!(
        excluded_rows.num_rows(),
        3,
        "mem's one sample is excluded; cpu's three samples survive"
    );

    let flipped = re_encode_with_erasure_cleared(&ticket, harness.service.ticket_key());
    let leaked = harness
        .do_get("acme", &flipped)
        .await
        .expect("do get with cleared erasure");
    assert_eq!(
        merged(&leaked).num_rows(),
        4,
        "with pending_erasure dropped, the erased mem row reappears (the pre-fix bug)"
    );
}

/// Distributed slice-fragment path (`do_get_statement`'s `slice_count > 1`
/// branch, `worker_fragment_stream`): a slice ticket built the way the real
/// coordinator would -- `pending_erasure` copied from the coordinator's own
/// resolved snapshot -- excludes the same way the statement path does.
///
/// Flip-proof: the same [`re_encode_with_erasure_cleared`] on a slice ticket
/// (`slice_count == 2`) reproduces the leak on this second `DoGet` branch,
/// proving `distributed.rs`'s `pending_erasure: template.pending_erasure.clone()`
/// is what the exclusion actually depends on here too, not something the
/// statement-path fix alone happens to cover.
#[tokio::test]
async fn distributed_slice_do_get_excludes_what_the_resolve_saw_pending() {
    let tenant = tenant_id("dist-erasure-tenant");
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;

    // Two shards, so the snapshot genuinely partitions into two slices under
    // `plan_distributed_slices` (a single-shard snapshot collapses to one
    // slice and the planner returns `None`, as `util::publish_segment`'s
    // hardcoded `shard: 0` would). `mem` lives on shard 0 alongside `cpu`, so
    // the slice covering shard 0 is the one exclusion must reach.
    publish_segment_on_shard(
        backend.as_ref(),
        &tenant,
        0,
        0,
        10,
        &[("cpu", &[(100, 1.0), (200, 2.0)]), ("mem", &[(150, 7.5)])],
    )
    .await;
    publish_segment_on_shard(
        backend.as_ref(),
        &tenant,
        1,
        1,
        20,
        &[("cpu", &[(300, 3.0)])],
    )
    .await;

    put_dreq(&store, tenant.hash(), "__name__", "mem").await;

    let (service, executor) = build_shard_service(Arc::clone(&backend), &tenant).await;

    let (snapshot, estimate) = executor
        .resolve_snapshot(
            tenant.hash(),
            &util::request(QUERY),
            &ravel_types::accounting::QueryAccounting::new(),
        )
        .await
        .expect("resolve");
    assert!(
        !snapshot.pending_erasure.is_empty(),
        "the real resolve must have picked up the dreq"
    );

    let template = FlightTicket {
        tenant: tenant.hash(),
        statement: String::new(),
        segments: snapshot
            .segments
            .iter()
            .map(SegmentPin::from_segment_ref)
            .collect(),
        min_commit_tokens: Vec::new(),
        now_ns: util::NOW_NS,
        deadline_ns: util::NOW_NS + 1_000_000_000,
        slice_index: 0,
        slice_count: 1,
        pending_erasure: ravel_query::erasure::snapshot_pending_erasure_predicates(&snapshot),
    };
    let config = DistributedFlightConfig {
        workers: Arc::new(ravel_sql::StaticWorkerEndpoints::new([
            "grpc://worker-a".to_string(),
            "grpc://worker-b".to_string(),
        ])),
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 2,
            max_parallel_slices: 8,
        },
        shared_ticket_key: None,
    };
    let plan = ravel_sql::plan_distributed_slices(&snapshot, &estimate, &config, &template)
        .expect("the two-shard snapshot distributes");
    assert_eq!(plan.len(), 2, "one slice per shard-major group");

    // Find the slice ticket that pins the segment carrying the `mem` sample
    // (shard 0's segment spans ts 100..=200), so redeeming it is a
    // non-vacuous exclusion check.
    let mem_slice = plan
        .iter()
        .find(|s| {
            s.ticket
                .segments
                .iter()
                .any(|seg| seg.min_event_ts_ns <= 150 && seg.max_event_ts_ns >= 150)
        })
        .expect("one slice pins the segment carrying the mem sample");

    let handle = mem_slice
        .ticket
        .encode(service.ticket_key())
        .expect("encode slice ticket");
    let raw = Ticket::new(
        TicketStatementQuery {
            statement_handle: handle.into(),
        }
        .as_any()
        .encode_to_vec(),
    );
    let excluded_rows = merged(&do_get_raw(&service, tenant.as_str(), &raw).await);
    for i in 0..excluded_rows.num_rows() {
        // Internal schema's series_id column is index 2 (see
        // `ravel_sql::internal_schema`); this asserts no row at ts=150
        // (the mem sample) appears rather than decoding the series id,
        // keeping the assertion to the ts column shared with the public
        // schema.
        let ts = excluded_rows
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
            .expect("ts col")
            .value(i);
        assert_ne!(
            ts, 150,
            "the erased mem sample (ts=150) must not appear in the slice"
        );
    }

    // Flip: the same slice ticket with `pending_erasure` forced back to
    // empty, re-signed under the real key, must bring ts=150 back.
    let flipped = re_encode_with_erasure_cleared(&raw, service.ticket_key());
    let leaked_rows = merged(&do_get_raw(&service, tenant.as_str(), &flipped).await);
    let has_150 = (0..leaked_rows.num_rows()).any(|i| {
        leaked_rows
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
            .expect("ts col")
            .value(i)
            == 150
    });
    assert!(
        has_150,
        "with pending_erasure dropped, the erased mem sample (ts=150) reappears in the slice"
    );
}

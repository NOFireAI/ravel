//! SQL-lane distributed read fan-out acceptance tests (ADR-0071).
//!
//! These prove the deliverables end to end. Nothing in the shipping binary
//! reaches the distributed scan or the slice-fragment `do_get` path until the
//! coordinator wiring lands; the callers here are the acceptance
//! tests themselves. Two of them stand in for that future coordinator: the
//! `InProcessWorker` calls the worker fragment directly, and the `WireWorker`
//! redeems slice tickets through the real `RavelFlightSqlService::do_get`, so
//! the coordinator and worker halves are proven to compose over the actual
//! Flight wire, not just in the abstract.
//!
//! - `single_client_endpoint_even_when_distributed`: an external Flight SQL
//!   client ALWAYS receives exactly ONE endpoint from `GetFlightInfo`, even
//!   over a snapshot that genuinely partitions into more than one slice. Slice
//!   fan-out is the internal coordinator-to-worker contract, never a
//!   client-facing partition set (ADR-0071).
//! - `multi_endpoint_tickets_partition_pinned_snapshot`: the internal
//!   coordinator plan (`plan_distributed_slices`) emits one slice per
//!   shard-major group, every pinned segment lands in exactly one slice ticket
//!   with no duplicates, and an old-version (v3) envelope is rejected typed.
//! - `distributed_scan_equals_local_scan` / `distributed_scan_over_wire_equals_local`:
//!   the distributed scan (over the in-process worker, then over the real
//!   Flight wire) is bit-for-bit identical (f64 as raw bits) to the local
//!   single-endpoint scan of the same pinned snapshot, including a
//!   `(series_id, ts)` that appears in two shards' slices (the cross-slice
//!   dedup path).
//! - `distributed_aggregate_equals_local`: `sum`/`count` over the distributed
//!   provider equal the local aggregate, and equal the deduped values (not the
//!   inflated un-deduped ones), proving the coordinator's cross-slice dedup
//!   runs before aggregation.
//! - `fragment_do_get_serves_internal_schema`: a slice ticket redeemed through
//!   the real service `do_get` serves the internal, provenance-carrying schema
//!   (8 columns), the flip-side of the whole-set ticket that serves the public
//!   4-column statement result.
//! - `distributed_scan_folds_bytes_into_coordinator_accounting`: the
//!   coordinator folds every worker-returned batch's bytes into its own
//!   `QueryAccounting`, and a tight `max_bytes_scanned` surfaces as a typed
//!   scanned-too-many-bytes error through `collect`.
//! - `distributed_plan_shape_is_dedup_over_merge_over_scan`: the coordinator
//!   plan is `RsegDedupExec -> SortPreservingMergeExec -> DistributedScanExec`,
//!   single-partitioned at the top with no repartition, so aggregation stays
//!   single-partition and the `session.rs` determinism ban is untouched.
//! - `limit_hint_returns_exactly_limit_rows_over_multiple_slices`: a limited
//!   distributed scan returns exactly the limit's rows, equal to the first
//!   `limit` rows of the local scan.
//!
//! prove-the-test (distributed_scan_equals_local_scan): removing the
//! `RsegDedupExec` from `distributed_samples_plan` (feeding the merge output
//! straight through) makes this test fail. Verified by flipping that one line:
//! `reduce_batches` then panics on its public-schema assertion, because the
//! un-deduped merge emits the internal, provenance-carrying schema (and, had
//! it reached the row loop, the `(cpu, ts=100)` sample shared by both shards'
//! slices would have surfaced twice, tripping the duplicate-(series, ts)
//! assertion). Restoring the dedup makes it pass. Both failure modes exist only
//! because the two shards genuinely share that `(series_id, ts)`, which is what
//! makes the cross-slice equality non-vacuous.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

mod util;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, Int64Array, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{collect, displayable, execute_stream};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::distrib::partition::{DistribThresholds, partition_snapshot};
use ravel_query::{
    ByteLimit, EngineConfig, LogSegmentFetcher, QueryAdmissionController, QueryConcurrencyLimit,
    SegmentFetcher,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{
    DistributedFlightConfig, FlightAuth, FlightClock, FlightSqlConfig, FlightTicket,
    FlightTicketError, RavelFlightSqlService, RavelTableProvider, SegmentPin, SqlConfig,
    SqlExecutor, StaticWorkerEndpoints, WorkerSlice, WorkerSliceClient, distributed_samples_plan,
    internal_schema, plan_distributed_slices,
};
use ravel_types::accounting::{NoopQueryCostRecorder, QueryAccounting};
use ravel_types::{CommitToken, Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};
use uuid::Uuid;

use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_types::Signal;

use util::flight_harness::{TOKEN_KEY, TestAuth, TestClock, insert, window_metadata};
use util::{NOW_NS, request};

/// The raw tenant hash the exec-level tests fetch under. Independent of the
/// `TenantId` used to compute series ids (see `series_id_for`); the `MemoryStore`
/// fetcher reads by object key, so the two need not agree, matching the
/// tests/pipeline.rs convention.
const TENANT: TenantHash = TenantHash([7u8; 16]);

// ---------------------------------------------------------------------------
// Hand-built multi-shard fixture (exec-level tests: equality, plan, limit)
// ---------------------------------------------------------------------------

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_id_for(metric: &str) -> [u8; 16] {
    let tenant = TenantId::new("t".to_string());
    SeriesId::compute(&tenant, metric, &labels_for(metric))
        .expect("series id")
        .0
}

/// One series in a segment: metric name plus (ts, value) samples.
struct SeriesSpec {
    metric: &'static str,
    samples: Vec<(i64, f64)>,
}

/// Write one real RSEG segment onto `store` and return its `SegmentRef` with
/// the given `shard`, so a hand-built snapshot spans more than one shard.
async fn write_segment(
    store: &dyn ObjectStoreBackend,
    key: &str,
    shard: u32,
    writer_id: Uuid,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    series: &[SeriesSpec],
) -> SegmentRef {
    let inputs: Vec<SeriesInput> = series
        .iter()
        .filter(|s| !s.samples.is_empty())
        .map(|s| SeriesInput {
            series_id: SeriesId(series_id_for(s.metric)),
            labels: labels_for(s.metric),
            samples: s
                .samples
                .iter()
                .map(|(ts_ns, value)| Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        })
        .collect();

    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    store
        .put(key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment");

    SegmentRef {
        data_object_key: key.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 0,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch,
        writer_seq,
        created_unix_ns,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
    }
}

/// A two-shard snapshot where `(cpu, ts=100)` appears in BOTH shards with
/// distinct provenance, so the winner is only decidable at the coordinator's
/// cross-slice dedup. Shard 1's segment has the greater `created_unix_ns`, so
/// its value (111.0) wins that key.
async fn two_shard_snapshot() -> (Arc<MemoryStore>, Snapshot) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    // Shard 0.
    segments.push(
        write_segment(
            store.as_ref(),
            "t/metrics/s0-seg.rseg",
            0,
            Uuid::from_u128(1),
            10,
            1,
            1,
            &[
                SeriesSpec {
                    metric: "cpu",
                    samples: vec![(100, 1.0), (200, 2.0)],
                },
                SeriesSpec {
                    metric: "mem",
                    samples: vec![(150, 7.5)],
                },
            ],
        )
        .await,
    );
    // Shard 1: shares (cpu, ts=100), higher created_unix_ns wins it.
    segments.push(
        write_segment(
            store.as_ref(),
            "t/metrics/s1-seg.rseg",
            1,
            Uuid::from_u128(2),
            20,
            1,
            1,
            &[SeriesSpec {
                metric: "cpu",
                samples: vec![(100, 111.0), (300, 3.0)],
            }],
        )
        .await,
    );
    // Deterministic dedup segment order (created_unix_ns, epoch, seq, shard).
    segments.sort_by_key(|s| (s.created_unix_ns, s.writer_epoch, s.writer_seq, s.shard));
    (
        store,
        Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
    )
}

/// Build one worker endpoint per shard-major slice of `snapshot`, minting a
/// slice ticket for each. Mirrors what `get_flight_info_statement` produces,
/// but without the service, so the exec-level tests stay focused on the
/// coordinator plan.
fn endpoints_for(snapshot: &Snapshot) -> Vec<WorkerSlice> {
    let slices = partition_snapshot(snapshot, 8);
    let count = slices.len() as u32;
    slices
        .iter()
        .enumerate()
        .map(|(k, slice)| WorkerSlice {
            location: format!("grpc://worker-{k}"),
            ticket: FlightTicket {
                tenant: TENANT,
                statement: String::new(),
                segments: slice
                    .segments
                    .iter()
                    .map(SegmentPin::from_segment_ref)
                    .collect(),
                min_commit_tokens: Vec::new(),
                now_ns: NOW_NS,
                deadline_ns: NOW_NS + 1_000_000_000,
                slice_index: k as u32,
                slice_count: count,
                pending_erasure: Vec::new(),
                declared_columns: Vec::new(),
            },
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The in-process Flight worker
// ---------------------------------------------------------------------------

/// The single caller of the distributed scan today (see the module docs). Given
/// a slice ticket, it rebuilds the slice's snapshot, runs the worker fragment
/// (`RsegScanExec -> SortPreservingMergeExec`, internal schema, no dedup), and
/// streams the result back. It records the `limit` hints it is handed so a test
/// can prove the hint propagates across the distributed partitions.
struct InProcessWorker {
    fetcher: SegmentFetcher,
    config: SqlConfig,
    limits_seen: Mutex<Vec<Option<usize>>>,
}

impl InProcessWorker {
    fn new(fetcher: SegmentFetcher) -> Self {
        InProcessWorker {
            fetcher,
            config: SqlConfig::default(),
            limits_seen: Mutex::new(Vec::new()),
        }
    }
}

impl std::fmt::Debug for InProcessWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("InProcessWorker")
    }
}

impl WorkerSliceClient for InProcessWorker {
    fn fetch_slice(
        &self,
        _location: &str,
        ticket: &FlightTicket,
        limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.limits_seen.lock().expect("lock").push(limit);
        // Rebuild the slice's snapshot from the ticket and run the worker
        // fragment over it. Over-fetching is safe: the coordinator re-applies
        // the exact limit above the dedup, so the worker returns the whole
        // slice regardless of the hint.
        let snapshot = ticket.snapshot();
        let segments = snapshot.segments.clone();
        let provider = RavelTableProvider::new(
            snapshot,
            TENANT,
            self.fetcher.clone(),
            self.config.clone(),
            QueryAccounting::new(),
        );
        let plan = provider.worker_fragment(segments.len().max(1), &segments)?;
        execute_stream(plan, Arc::new(TaskContext::default()))
    }
}

// ---------------------------------------------------------------------------
// The over-the-wire Flight worker
// ---------------------------------------------------------------------------

/// A tenant resolver that maps every credential to [`TENANT`]. The wire tests
/// mint slice tickets under `TENANT` (the identity the fixture segments embed
/// and their object keys resolve under), so the service's metadata-resolved
/// tenant must match the ticket's for `do_get_statement` to redeem it.
struct FixedAuth;

impl FlightAuth for FixedAuth {
    fn tenant(&self, _metadata: &MetadataMap) -> Result<TenantHash, Status> {
        Ok(TENANT)
    }

    fn min_commit_tokens(&self, _metadata: &MetadataMap) -> Result<Vec<CommitToken>, Status> {
        Ok(Vec::new())
    }
}

/// Build a Flight SQL service whose fetcher reads `store` by object key. The
/// catalog is present but never consulted on the slice-fragment path: a slice
/// ticket carries its own pinned snapshot, so `do_get_statement` never
/// re-resolves. That is exactly why the wire tests can mint tickets whose
/// segments live in `store` under keys no catalog listing would return.
fn wire_service(store: Arc<dyn ObjectStoreBackend>) -> Arc<RavelFlightSqlService> {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let executor = Arc::new(SqlExecutor::new(
        catalog,
        fetcher,
        LogSegmentFetcher::new(Arc::clone(&store)),
        ravel_sql::SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    ));
    let clock = TestClock::at(NOW_NS);
    Arc::new(RavelFlightSqlService::new(
        executor,
        Arc::new(FixedAuth),
        clock as Arc<dyn FlightClock>,
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            ..FlightSqlConfig::default()
        },
        Arc::new(NoopQueryCostRecorder),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
    ))
}

/// A distributed-scan client that redeems each slice ticket through the real
/// service `do_get`, over the actual Flight wire (encode -> `do_get_statement`
/// slice-fragment branch -> `FlightData` -> decode). It is the composition
/// proof: the coordinator's `DistributedScanExec` and the worker's
/// `do_get_statement` slice path agree on the internal schema and on the bytes.
struct WireWorker {
    service: Arc<RavelFlightSqlService>,
    token: String,
}

impl std::fmt::Debug for WireWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("WireWorker")
    }
}

impl WorkerSliceClient for WireWorker {
    fn fetch_slice(
        &self,
        _location: &str,
        ticket: &FlightTicket,
        _limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Encode the slice ticket exactly as the coordinator would, then redeem
        // it against the real service. `slice_count > 1` routes it to the
        // scan-only fragment branch, which streams the internal, provenance-
        // carrying schema back for the coordinator to merge and dedup.
        let handle = ticket
            .encode(self.service.ticket_key())
            .map_err(|err| DataFusionError::Internal(err.to_string()))?;
        let service = Arc::clone(&self.service);
        let token = self.token.clone();
        let setup = async move {
            let tsq = TicketStatementQuery {
                statement_handle: handle.into(),
            };
            let raw = Ticket::new(tsq.as_any().encode_to_vec());
            let mut req = Request::new(raw);
            util::flight_harness::insert(req.metadata_mut(), TOKEN_KEY, &token);
            let response = service
                .do_get_statement(tsq, req)
                .await
                .map_err(|status| FlightError::Tonic(Box::new(status)))?;
            let flight_data = response
                .into_inner()
                .map_err(|status| FlightError::Tonic(Box::new(status)));
            Ok::<_, FlightError>(FlightRecordBatchStream::new_from_flight_data(flight_data))
        };
        let batches = futures::stream::once(setup)
            .try_flatten()
            .map_err(|err| DataFusionError::Internal(err.to_string()));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            internal_schema(),
            batches,
        )))
    }
}

// ---------------------------------------------------------------------------
// Comparison helpers
// ---------------------------------------------------------------------------

/// Per-series ts -> winning value bits. Asserts the public schema, per-series
/// ascending ts, and no duplicate (series, ts) in the output -- the last of
/// which is the cross-slice-dedup guard the prove-the-test note relies on.
type Reduced = HashMap<[u8; 16], BTreeMap<i64, u64>>;

fn reduce_batches(batches: &[RecordBatch]) -> Reduced {
    let mut out: Reduced = HashMap::new();
    for batch in batches {
        assert_eq!(batch.schema(), ravel_sql::public_schema(), "public schema");
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value col");
        let sid = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("series_id col");
        for i in 0..batch.num_rows() {
            let series = <[u8; 16]>::try_from(sid.value(i)).expect("16 bytes");
            let prior = out
                .entry(series)
                .or_default()
                .insert(ts.value(i), value.value(i).to_bits());
            assert!(prior.is_none(), "duplicate (series, ts) in dedup output");
        }
    }
    out
}

fn assert_reduced_eq(got: &Reduced, want: &Reduced) {
    assert_eq!(got.len(), want.len(), "series count");
    let got_total: usize = got.values().map(|m| m.len()).sum();
    let want_total: usize = want.values().map(|m| m.len()).sum();
    assert_eq!(got_total, want_total, "total sample count");
    for (sid, want_map) in want {
        let got_map = got
            .get(sid)
            .unwrap_or_else(|| panic!("missing series {sid:?}"));
        assert_eq!(got_map, want_map, "series {sid:?} sample bits differ");
    }
}

/// Flatten batches into `(series_id, ts, value bits)` rows in emission order.
/// The dedup emits in `(series_id, ts)` order on a single partition, so this is
/// the sorted output order the exact limit truncates.
fn flatten_rows(batches: &[RecordBatch]) -> Vec<([u8; 16], i64, u64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value col");
        let sid = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("series_id col");
        for i in 0..batch.num_rows() {
            rows.push((
                <[u8; 16]>::try_from(sid.value(i)).expect("16 bytes"),
                ts.value(i),
                value.value(i).to_bits(),
            ));
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Deliverable 3 + the differential invariant
// ---------------------------------------------------------------------------

#[tokio::test]
async fn distributed_scan_equals_local_scan() {
    let (store, snapshot) = two_shard_snapshot().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(Arc::clone(&backend));

    // Local single-endpoint scan of the whole pinned snapshot.
    let local = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher.clone(),
        SqlConfig::default(),
        QueryAccounting::new(),
    );
    let local_plan = local.plan(4).expect("local plan");
    let local_batches = collect(local_plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect local");
    let local_reduced = reduce_batches(&local_batches);

    // Distributed scan over the in-process worker, same pinned snapshot.
    let client: Arc<dyn WorkerSliceClient> = Arc::new(InProcessWorker::new(fetcher.clone()));
    let distributed = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher,
        SqlConfig::default(),
        QueryAccounting::new(),
    )
    .with_distributed_scan(endpoints_for(&snapshot), Arc::clone(&client));
    let ctx = SessionContext::new();
    let dist_plan = distributed
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("distributed scan");
    let dist_batches = collect(dist_plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect distributed");
    let dist_reduced = reduce_batches(&dist_batches);

    // Byte-identical (f64 as raw bits), column by column via the reduction.
    assert_reduced_eq(&dist_reduced, &local_reduced);

    // The cross-slice dedup actually decided (cpu, ts=100): shard 1's 111.0
    // (greater created_unix_ns) wins over shard 0's 1.0. If the coordinator did
    // not re-dedup across slices, reduce_batches would have panicked on the
    // duplicate above; this pins the winning value too.
    let cpu = series_id_for("cpu");
    assert_eq!(
        dist_reduced.get(&cpu).and_then(|m| m.get(&100)).copied(),
        Some(111.0_f64.to_bits()),
        "cross-slice winner for (cpu, ts=100) must be shard 1's 111.0"
    );
}

// ---------------------------------------------------------------------------
// Deliverable 3: plan shape
// ---------------------------------------------------------------------------

#[tokio::test]
async fn distributed_plan_shape_is_dedup_over_merge_over_scan() {
    let (store, snapshot) = two_shard_snapshot().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(backend);
    let client: Arc<dyn WorkerSliceClient> = Arc::new(InProcessWorker::new(fetcher));

    let plan = distributed_samples_plan(
        endpoints_for(&snapshot),
        client,
        EngineConfig::default().max_samples,
        None,
        QueryAccounting::new(),
        ByteLimit::Unlimited,
    )
    .expect("distributed plan");

    let text = displayable(plan.as_ref()).indent(true).to_string();
    // RsegDedupExec (top) over SortPreservingMergeExec over DistributedScanExec.
    let dedup_at = text.find("RsegDedupExec").expect("dedup present");
    let merge_at = text.find("SortPreservingMergeExec").expect("merge present");
    let scan_at = text
        .find("DistributedScanExec")
        .expect("distributed scan present");
    assert!(
        dedup_at < merge_at && merge_at < scan_at,
        "plan must be RsegDedupExec -> SortPreservingMergeExec -> DistributedScanExec; got:\n{text}"
    );

    // No parallel-aggregation hazard: nothing reshuffles above the dedup, and
    // the dedup emits a single partition, so any aggregate the session plans on
    // top runs single-partitioned (the session.rs determinism ban is
    // untouched).
    assert!(
        !text.contains("RepartitionExec"),
        "distributed plan must not repartition; got:\n{text}"
    );
    assert!(
        !text.contains("CoalescePartitionsExec"),
        "merge must not be an unordered coalesce; got:\n{text}"
    );
    assert_eq!(
        plan.properties().output_partitioning().partition_count(),
        1,
        "the coordinator plan emits a single partition for single-partition aggregation"
    );
}

// ---------------------------------------------------------------------------
// Deliverable 4: limit hint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn limit_hint_returns_exactly_limit_rows_over_multiple_slices() {
    let (store, snapshot) = two_shard_snapshot().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(Arc::clone(&backend));

    // The full local result, to define the correct first-`limit` rows.
    let local = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher.clone(),
        SqlConfig::default(),
        QueryAccounting::new(),
    );
    let local_batches = collect(
        local.plan(4).expect("local plan"),
        Arc::new(TaskContext::default()),
    )
    .await
    .expect("collect local");
    let local_rows = flatten_rows(&local_batches);
    // The fixture has four distinct (series, ts) rows: the limit must truncate.
    const LIMIT: usize = 2;
    assert!(
        local_rows.len() > LIMIT,
        "fixture must have more than LIMIT rows for the test to be non-vacuous"
    );

    let worker = Arc::new(InProcessWorker::new(fetcher.clone()));
    let client: Arc<dyn WorkerSliceClient> = worker.clone();
    let distributed = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher,
        SqlConfig::default(),
        QueryAccounting::new(),
    )
    .with_distributed_scan(endpoints_for(&snapshot), Arc::clone(&client));

    let ctx = SessionContext::new();
    let dist_plan = distributed
        .scan(&ctx.state(), None, &[], Some(LIMIT))
        .await
        .expect("distributed limited scan");
    let dist_batches = collect(dist_plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect distributed");
    let dist_rows = flatten_rows(&dist_batches);

    assert_eq!(dist_rows.len(), LIMIT, "exactly the limit's rows");
    assert_eq!(
        dist_rows,
        local_rows[..LIMIT].to_vec(),
        "the limited distributed scan returns the first LIMIT rows of the local scan (never under-fetch)"
    );

    // The fetch-stop hint reached every distributed partition (there is more
    // than one slice), proving it propagates across partitions even though the
    // exact limit is what enforces correctness above the dedup.
    let limits = worker.limits_seen.lock().expect("lock");
    assert!(limits.len() >= 2, "the hint reached more than one slice");
    assert!(
        limits.iter().all(|l| *l == Some(LIMIT)),
        "every distributed partition received the limit hint: {limits:?}"
    );
}

// ---------------------------------------------------------------------------
// Deliverable 1 + 2: multi-endpoint tickets + partition property + version
// ---------------------------------------------------------------------------

/// Publish one real RSEG segment for `metric`/`samples` on `shard`, resolvable
/// by a `shard_count`-aware catalog.
async fn publish_on_shard(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    shard: u32,
    created_unix_ns: i64,
    metric: &str,
    samples: &[(i64, f64)],
) {
    let tenant_hash = tenant.hash();
    let inputs = vec![SeriesInput {
        series_id: SeriesId(
            SeriesId::compute(tenant, metric, &labels_for(metric))
                .expect("series id")
                .0,
        ),
        labels: labels_for(metric),
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }];
    let writer_id = Uuid::from_u128(5_000 + index as u128);
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

/// Build a Flight service over a two-shard catalog. Returns the service, its
/// executor (for resolving the whole pinned set in assertions), and the tenant.
///
/// The service is deliberately configured with no distribution knob at all:
/// ADR-0071 makes the client-facing `GetFlightInfo` always emit exactly one
/// endpoint, so there is nothing to toggle. The internal slice plan is a
/// separate, coordinator-side call (`plan_distributed_slices`) the tests drive
/// directly.
async fn service_over_two_shards() -> (RavelFlightSqlService, Arc<SqlExecutor>, TenantId) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("dist-tenant".to_string());
    publish_on_shard(
        store.as_ref(),
        &tenant,
        0,
        0,
        10,
        "cpu",
        &[(100, 1.0), (200, 2.0)],
    )
    .await;
    publish_on_shard(
        store.as_ref(),
        &tenant,
        1,
        1,
        20,
        "cpu",
        &[(100, 111.0), (300, 3.0)],
    )
    .await;

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
        ravel_sql::SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    ));
    let auth = TestAuth::new(&[(tenant.as_str(), &tenant)]);
    let clock = TestClock::at(NOW_NS);
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
    (service, executor, tenant)
}

async fn flight_info_endpoints(
    service: &RavelFlightSqlService,
    tenant: &TenantId,
    sql: &str,
) -> arrow_flight::FlightInfo {
    let mut req = tonic::Request::new(FlightDescriptor::new_cmd(sql.as_bytes().to_vec()));
    insert(req.metadata_mut(), TOKEN_KEY, tenant.as_str());
    window_metadata(req.metadata_mut());
    service
        .get_flight_info_statement(
            CommandStatementQuery {
                query: sql.to_string(),
                transaction_id: None,
            },
            req,
        )
        .await
        .expect("get_flight_info")
        .into_inner()
}

/// The coordinator-side distributed config for the two-shard fixture: the gate
/// trips at `min_segments = 2` and slices shard-major over two workers.
fn two_worker_config() -> DistributedFlightConfig {
    DistributedFlightConfig {
        workers: Arc::new(StaticWorkerEndpoints::new([
            "grpc://worker-a".to_string(),
            "grpc://worker-b".to_string(),
        ])),
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 2,
            max_parallel_slices: 8,
        },
        shared_ticket_key: None,
    }
}

/// A whole-set ticket template for `snapshot` under `tenant`, the shape the
/// coordinator feeds `plan_distributed_slices` to stamp per-slice tickets from.
fn slice_template(tenant: TenantHash, statement: &str, snapshot: &Snapshot) -> FlightTicket {
    FlightTicket {
        tenant,
        statement: statement.to_string(),
        segments: snapshot
            .segments
            .iter()
            .map(SegmentPin::from_segment_ref)
            .collect(),
        min_commit_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline_ns: NOW_NS + 1_000_000_000,
        slice_index: 0,
        slice_count: 1,
        pending_erasure: ravel_query::erasure::snapshot_pending_erasure_predicates(snapshot),
        declared_columns: Vec::new(),
    }
}

#[tokio::test]
async fn multi_endpoint_tickets_partition_pinned_snapshot() {
    const SQL: &str = "SELECT ts, value FROM samples";

    let (service, executor, tenant) = service_over_two_shards().await;

    // The whole pinned segment set, resolved once as the source of truth.
    let (snapshot, estimate) = executor
        .resolve_snapshot(tenant.hash(), &request(SQL), &QueryAccounting::new())
        .await
        .expect("resolve");
    let whole: BTreeSet<String> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    assert_eq!(whole.len(), 2, "fixture spans two segments (two shards)");

    // The INTERNAL coordinator slice plan. This is never a
    // client-facing endpoint set (see `single_client_endpoint_even_when_distributed`);
    // it is the coordinator-to-worker fan-out the future service `do_get` will
    // drive. Each slice ticket carries the coordinator's resolved tenant.
    let config = two_worker_config();
    let template = slice_template(tenant.hash(), SQL, &snapshot);
    let slices = plan_distributed_slices(&snapshot, &estimate, &config, &template)
        .expect("the two-shard snapshot distributes");
    assert_eq!(slices.len(), 2, "one slice per shard-major group");

    // Every pinned segment lands in exactly one slice ticket, no dupes.
    let mut seen: Vec<String> = Vec::new();
    let mut indices: Vec<u32> = Vec::new();
    for slice in &slices {
        assert!(
            !slice.location.is_empty(),
            "a distributed slice carries a worker location"
        );
        assert_eq!(
            slice.ticket.slice_count, 2,
            "slice_count reflects the fan-out"
        );
        assert_eq!(
            slice.ticket.tenant,
            tenant.hash(),
            "a slice ticket carries the coordinator's resolved tenant"
        );
        assert!(
            slice.ticket.statement.is_empty(),
            "a slice ticket carries no SQL text; the fragment path never plans"
        );
        indices.push(slice.ticket.slice_index);
        for pin in &slice.ticket.segments {
            seen.push(pin.data_object_key.clone());
        }
    }
    // No duplicates: a segment appears in exactly one slice.
    let unique: BTreeSet<String> = seen.iter().cloned().collect();
    assert_eq!(unique.len(), seen.len(), "no segment appears in two slices");
    // Total coverage: the union equals the whole pinned set.
    assert_eq!(
        unique, whole,
        "every pinned segment lands in exactly one slice"
    );
    // Slice indices are the distinct 0..N set.
    indices.sort_unstable();
    assert_eq!(indices, vec![0, 1], "slice indices are 0..N, distinct");

    // An old-version (v3) envelope is rejected typed. It is MAC-valid under the
    // service's own key, so the version byte -- not the MAC -- is what rejects
    // it: exactly the rolling-deploy safety a v4 coordinator relies on.
    let key = service.ticket_key();
    let mut body = Vec::new();
    body.extend_from_slice(b"RFT1");
    body.push(3); // the predecessor envelope version
    body.extend_from_slice(&[0u8; 16]); // tenant
    body.extend_from_slice(&0i64.to_le_bytes()); // now_ns
    body.extend_from_slice(&0i64.to_le_bytes()); // deadline_ns
    body.extend_from_slice(&0u32.to_le_bytes()); // slice_index
    body.extend_from_slice(&0u32.to_le_bytes()); // slice_count
    body.extend_from_slice(&0u32.to_le_bytes()); // token_count
    body.extend_from_slice(&0u32.to_le_bytes()); // seg_count
    body.extend_from_slice(&0u32.to_le_bytes()); // erasure_count
    body.extend_from_slice(&0u32.to_le_bytes()); // declared_count, padding to v6's MIN_ENCODED_LEN
    body.extend_from_slice(&0u32.to_le_bytes()); // stmt_len
    let tag = blake3::keyed_hash(key, &body);
    body.extend_from_slice(tag.as_bytes());
    assert_eq!(
        FlightTicket::decode(&body, key),
        Err(FlightTicketError::UnsupportedVersion(3)),
        "an old-version envelope is rejected on the version byte"
    );
}

#[tokio::test]
async fn a_cheap_query_stays_single_endpoint() {
    // No distributed config: the cost gate never engages, so the same query
    // returns exactly one local endpoint over the whole pinned set.
    let (service, _executor, tenant) = service_over_two_shards().await;
    let info = flight_info_endpoints(&service, &tenant, "SELECT ts, value FROM samples").await;
    assert_eq!(info.endpoint.len(), 1, "local path emits one endpoint");
    let raw = info.endpoint[0].ticket.as_ref().expect("ticket");
    let tsq = util::flight_harness::statement_ticket(raw);
    let ticket = FlightTicket::decode(&tsq.statement_handle, service.ticket_key()).expect("decode");
    assert_eq!(ticket.slice_count, 1, "a whole-snapshot ticket is 1 of 1");
    assert_eq!(ticket.slice_index, 0);
    assert_eq!(
        ticket.segments.len(),
        2,
        "the single ticket pins every segment"
    );
}

// ---------------------------------------------------------------------------
// ADR-0071 client contract: one endpoint, always
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_client_endpoint_even_when_distributed() {
    const SQL: &str = "SELECT ts, value FROM samples";
    let (service, executor, tenant) = service_over_two_shards().await;

    // An external Flight SQL client ALWAYS receives exactly one endpoint. Arrow
    // Flight's N endpoints are partitions of ONE result set a client unions;
    // a cross-shard scan fan-out is not that, so it never surfaces as endpoints.
    let info = flight_info_endpoints(&service, &tenant, SQL).await;
    assert_eq!(
        info.endpoint.len(),
        1,
        "external clients always receive exactly one endpoint (ADR-0071)"
    );
    let raw = info.endpoint[0].ticket.as_ref().expect("ticket");
    let tsq = util::flight_harness::statement_ticket(raw);
    let ticket = FlightTicket::decode(&tsq.statement_handle, service.ticket_key()).expect("decode");
    assert_eq!(
        ticket.slice_count, 1,
        "the client endpoint is the whole set, 1 of 1"
    );

    // Companion: the same pinned snapshot genuinely partitions into more than
    // one slice. So the single client endpoint is the deliberate contract, not
    // an artifact of a snapshot that happens not to distribute. Those slices are
    // the INTERNAL coordinator-to-worker plan, never endpoints.
    let (snapshot, estimate) = executor
        .resolve_snapshot(tenant.hash(), &request(SQL), &QueryAccounting::new())
        .await
        .expect("resolve");
    let config = two_worker_config();
    let template = slice_template(tenant.hash(), SQL, &snapshot);
    let slices = plan_distributed_slices(&snapshot, &estimate, &config, &template)
        .expect("the snapshot distributes");
    assert!(
        slices.len() > 1,
        "the snapshot genuinely partitions into multiple slices, yet the client saw one endpoint"
    );
}

// ---------------------------------------------------------------------------
// The two halves compose over the real Flight wire
// ---------------------------------------------------------------------------

#[tokio::test]
async fn distributed_scan_over_wire_equals_local() {
    let (store, snapshot) = two_shard_snapshot().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(Arc::clone(&backend));

    // Local single-endpoint scan of the whole pinned snapshot.
    let local = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher.clone(),
        SqlConfig::default(),
        QueryAccounting::new(),
    );
    let local_batches = collect(
        local.plan(4).expect("local plan"),
        Arc::new(TaskContext::default()),
    )
    .await
    .expect("collect local");
    let local_reduced = reduce_batches(&local_batches);

    // Distributed scan whose slices are redeemed through the REAL service
    // `do_get`, over the actual Flight wire: this is the composition proof that
    // the coordinator's `DistributedScanExec` and the worker's slice-fragment
    // `do_get_statement` agree on the internal schema and the bytes.
    let service = wire_service(Arc::clone(&backend));
    let client: Arc<dyn WorkerSliceClient> = Arc::new(WireWorker {
        service: Arc::clone(&service),
        token: "wire-token".to_string(),
    });
    let distributed = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher,
        SqlConfig::default(),
        QueryAccounting::new(),
    )
    .with_distributed_scan(endpoints_for(&snapshot), Arc::clone(&client));
    let ctx = SessionContext::new();
    let dist_plan = distributed
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("distributed scan");
    let dist_batches = collect(dist_plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect distributed");
    let dist_reduced = reduce_batches(&dist_batches);

    assert_reduced_eq(&dist_reduced, &local_reduced);

    // The cross-slice dedup decided (cpu, ts=100) over the wire too: shard 1's
    // 111.0 (greater created_unix_ns) wins over shard 0's 1.0.
    let cpu = series_id_for("cpu");
    assert_eq!(
        dist_reduced.get(&cpu).and_then(|m| m.get(&100)).copied(),
        Some(111.0_f64.to_bits()),
        "cross-slice winner over the wire must be shard 1's 111.0"
    );
}

#[tokio::test]
async fn fragment_do_get_serves_internal_schema() {
    let (store, snapshot) = two_shard_snapshot().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let service = wire_service(Arc::clone(&backend));

    // A slice ticket (`slice_count > 1`) is the internal coordinator-to-worker
    // contract. Redeem the first slice directly through the service `do_get`.
    let endpoints = endpoints_for(&snapshot);
    assert!(
        endpoints.len() >= 2,
        "fixture partitions into more than one slice"
    );
    let ticket = endpoints[0].ticket.clone();
    assert_eq!(ticket.slice_count, 2, "a slice ticket, not the whole set");

    let handle = ticket.encode(service.ticket_key()).expect("encode");
    let tsq = TicketStatementQuery {
        statement_handle: handle.into(),
    };
    let raw = Ticket::new(tsq.as_any().encode_to_vec());
    let mut req = Request::new(raw);
    util::flight_harness::insert(req.metadata_mut(), TOKEN_KEY, "wire-token");
    let stream = service
        .do_get_statement(tsq, req)
        .await
        .expect("do_get slice")
        .into_inner();
    let batches = util::flight_harness::collect(stream)
        .await
        .expect("decode slice fragment");

    // Flip-line proof: the slice branch serves `worker_fragment_stream`, whose
    // plan is `RsegScanExec -> SortPreservingMergeExec` over `internal_schema()`
    // (8 columns: the four public plus four provenance). Had `do_get_statement`
    // instead planned the full statement (the `statement_stream` path taken when
    // `slice_count == 1`), it would serve `public_schema()` (4 columns). The
    // schema is the observable difference between the two branches.
    assert!(!batches.is_empty(), "the slice yields rows");
    assert_eq!(
        batches[0].schema(),
        internal_schema(),
        "a slice fragment serves the internal, provenance-carrying schema"
    );
    assert_eq!(batches[0].num_columns(), 8, "internal schema is 8 columns");
}

// ---------------------------------------------------------------------------
// Aggregation runs above the coordinator's cross-slice dedup
// ---------------------------------------------------------------------------

/// Run `SELECT sum(value), count(*) FROM samples` on `ctx` and return the sum's
/// raw f64 bits and the row count.
async fn sum_count(ctx: &SessionContext) -> (u64, i64) {
    let batches = ctx
        .sql("SELECT sum(value) AS s, count(*) AS c FROM samples")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let batch = &batches[0];
    let s = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("sum f64")
        .value(0);
    let c = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count i64")
        .value(0);
    (s.to_bits(), c)
}

#[tokio::test]
async fn distributed_aggregate_equals_local() {
    let (store, snapshot) = two_shard_snapshot().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(Arc::clone(&backend));

    let local_ctx = SessionContext::new();
    local_ctx
        .register_table(
            "samples",
            Arc::new(RavelTableProvider::new(
                snapshot.clone(),
                TENANT,
                fetcher.clone(),
                SqlConfig::default(),
                QueryAccounting::new(),
            )),
        )
        .expect("register local");
    let (local_sum, local_count) = sum_count(&local_ctx).await;

    let client: Arc<dyn WorkerSliceClient> = Arc::new(InProcessWorker::new(fetcher.clone()));
    let dist_ctx = SessionContext::new();
    dist_ctx
        .register_table(
            "samples",
            Arc::new(
                RavelTableProvider::new(
                    snapshot.clone(),
                    TENANT,
                    fetcher,
                    SqlConfig::default(),
                    QueryAccounting::new(),
                )
                .with_distributed_scan(endpoints_for(&snapshot), Arc::clone(&client)),
            ),
        )
        .expect("register distributed");
    let (dist_sum, dist_count) = sum_count(&dist_ctx).await;

    // The distributed aggregate equals the local one, bit for bit.
    assert_eq!(dist_sum, local_sum, "distributed sum equals local");
    assert_eq!(dist_count, local_count, "distributed count equals local");

    // Non-vacuity: the coordinator dedups the cross-slice (cpu, ts=100) BEFORE
    // aggregating, so the sum is 111.0 + 2.0 + 3.0 + 7.5 = 123.5 over 4 rows.
    // Had it aggregated the un-deduped merge, shard 0's losing 1.0 would count
    // too: 124.5 over 5 rows. The exact deduped value is what proves dedup runs
    // below the aggregate, not that the two providers merely agree.
    assert_eq!(
        dist_sum,
        123.5_f64.to_bits(),
        "deduped sum, not the inflated 124.5"
    );
    assert_eq!(dist_count, 4, "deduped row count, not the inflated 5");
}

// ---------------------------------------------------------------------------
// The coordinator folds worker bytes into its own accounting and enforces it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn distributed_scan_folds_bytes_into_coordinator_accounting() {
    let (store, snapshot) = two_shard_snapshot().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(Arc::clone(&backend));
    let client: Arc<dyn WorkerSliceClient> = Arc::new(InProcessWorker::new(fetcher.clone()));

    // (a) Every worker-returned batch's bytes fold into the COORDINATOR's
    // accounting handle, the one the provider was built with.
    let accounting = QueryAccounting::new();
    let provider = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher.clone(),
        SqlConfig::default(),
        accounting.clone(),
    )
    .with_distributed_scan(endpoints_for(&snapshot), Arc::clone(&client));
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan");
    let _ = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    assert!(
        accounting.snapshot().total_s3_bytes() > 0,
        "the distributed scan folds worker-returned bytes into the coordinator accounting"
    );

    // (b) A tight `max_bytes_scanned` ceiling surfaces as a typed
    // scanned-too-many-bytes error through `collect`.
    let tight = SqlConfig::from(EngineConfig {
        max_bytes_scanned: ByteLimit::Bounded(1),
        ..EngineConfig::default()
    });
    let bounded = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        fetcher,
        tight,
        QueryAccounting::new(),
    )
    .with_distributed_scan(endpoints_for(&snapshot), Arc::clone(&client));
    let ctx2 = SessionContext::new();
    let plan2 = bounded
        .scan(&ctx2.state(), None, &[], None)
        .await
        .expect("scan");
    let err = collect(plan2, Arc::new(TaskContext::default()))
        .await
        .expect_err("the byte ceiling trips");
    assert!(
        err.to_string().contains("too many bytes"),
        "the byte ceiling surfaces as a scanned-too-many-bytes error: {err}"
    );
}

// ===========================================================================
// RLOG family (logs / alerts / audit) SQL-lane distributed fan-out (#326, T6a)
// ===========================================================================
//
// These reproduce, at the SQL layer, the queryfrag-layer merge-correctness
// invariant #284 established (`ravel_query::distrib::merge_log_records`):
// logs/alerts/audit merge under a total order with NO dedup. Two shapes, per
// signal, both from T2:
//
// - *reshard-straddle*: one stream's records live in TWO shards (two slices),
//   with `ts` interleaving across the shards. The distributed output must be
//   the total-order merge of both slices, which differs from any slice-order
//   concatenation. The specific line a naive concat breaks: the coordinator
//   `SortPreservingMergeExec` in `distributed_slice_plan` (equivalently the
//   worker `SortExec` in `sort_slice_fragment`). Replace either with an
//   unordered coalesce and the ordered assertion below fails, because the
//   fixture's `ts` values interleave [10,20,30,40] across the two shards, an
//   order neither slice-concat produces ([10,30,20,40] or [20,40,10,30]).
//
// - *duplicate-preservation*: two byte-identical records, one per shard (a
//   retry after a lost ack). Both must survive: logs/alerts/audit have no
//   query-time dedup (docs/consistency-model.md, "logs and spans"). The
//   specific line a reintroduced dedup breaks: the absence of any dedup node
//   above the merge. Add a `.dedup`/distinct and the row count drops from 2 to
//   1 and the assertion below fails.
//
// prove-the-test discipline (the second #284 trap): the "local" reference on
// every differential is the plain single-process provider scan (`plan(..)`)
// collected and sorted IN-TEST by a hand-written comparator -- it never calls
// the distributed merge exec under test on the reference side, so a regression
// in that exec cannot hide by appearing on both sides.

use bytes::Bytes;
use datafusion::arrow::array::StringArray;
use datafusion::physical_plan::ExecutionPlan;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_sql::{AlertsTableProvider, AuditTableProvider, LogsTableProvider};
use ravel_types::logstream::log_stream_id;

/// A generic RLOG record on the stream identified by `resource`, carrying
/// per-record dynamic `attrs`. Structurally shared by all three signals (an
/// alert/audit record rides RLOG verbatim, ADR-0040).
fn log_record(
    resource: &[(String, AttrValue)],
    attrs: &[(String, AttrValue)],
    ts: i64,
    body: &str,
) -> LogRecord {
    LogRecord {
        stream_id: log_stream_id(resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs.to_vec(),
    }
}

/// An alert record: the four promoted scalar fields carried as attributes
/// (ADR-0040 decision 2), `alert_id` the differentiator this test reads back.
fn alert_record(resource: &[(String, AttrValue)], ts: i64, alert_id: &str) -> LogRecord {
    log_record(
        resource,
        &[
            ("alert_id".to_string(), AttrValue::Str(alert_id.into())),
            ("rule_id".to_string(), AttrValue::Str("rule-1".into())),
            ("state".to_string(), AttrValue::Str("firing".into())),
            ("generation".to_string(), AttrValue::I64(1)),
        ],
        ts,
        "",
    )
}

/// Write one RLOG object from `records` on `shard`, put it at `key`, and return a
/// matching L0 `SegmentRef` carrying the object's true ts span and `shard` (so
/// `partition_snapshot` groups it shard-major into its own slice). The identity's
/// `tenant_hash` must match [`TENANT`], which the RLOG read path enforces.
async fn write_rlog_segment(
    store: &dyn ObjectStoreBackend,
    key: &str,
    shard: u32,
    writer_seq: u64,
    created_unix_ns: i64,
    records: &[LogRecord],
) -> SegmentRef {
    let identity = ObjectIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        w.push(r.clone()).expect("push record");
    }
    let bytes = w.finish().expect("finish object");
    let size = bytes.len() as u64;
    store
        .put(key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(shard as u128 + 1),
        writer_epoch: 1,
        writer_seq,
        created_unix_ns,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    }
}

fn rlog_snapshot(segments: Vec<SegmentRef>) -> Snapshot {
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// One worker endpoint per shard-major slice, minting a slice ticket for each.
/// Reuses the signal-agnostic slice-ticket plumbing (`partition_snapshot`,
/// `FlightTicket`, `SegmentPin`) unchanged -- it carries no signal discriminator.
fn rlog_endpoints_for(snapshot: &Snapshot) -> Vec<WorkerSlice> {
    let slices = partition_snapshot(snapshot, 8);
    let count = slices.len() as u32;
    slices
        .iter()
        .enumerate()
        .map(|(k, slice)| WorkerSlice {
            location: format!("grpc://rlog-worker-{k}"),
            ticket: FlightTicket {
                tenant: TENANT,
                statement: String::new(),
                segments: slice
                    .segments
                    .iter()
                    .map(SegmentPin::from_segment_ref)
                    .collect(),
                min_commit_tokens: Vec::new(),
                now_ns: NOW_NS,
                deadline_ns: NOW_NS + 1_000_000_000,
                slice_index: k as u32,
                slice_count: count,
                pending_erasure: Vec::new(),
                declared_columns: Vec::new(),
            },
        })
        .collect()
}

/// The in-process RLOG worker: rebuilds the slice's snapshot from the ticket and
/// runs that signal's worker fragment (`<signal>ScanExec -> SortExec`, public
/// schema, one sorted partition, NO dedup), streaming the result back. The
/// signal is selected by the `build` fn pointer, so one worker type serves all
/// three signals (and, later, spans).
struct RlogWorker {
    fetcher: LogSegmentFetcher,
    build: fn(Snapshot, LogSegmentFetcher, usize) -> DFResult<Arc<dyn ExecutionPlan>>,
}

impl std::fmt::Debug for RlogWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("RlogWorker")
    }
}

impl WorkerSliceClient for RlogWorker {
    fn fetch_slice(
        &self,
        _location: &str,
        ticket: &FlightTicket,
        _limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let snapshot = ticket.snapshot();
        let target_partitions = snapshot.segments.len().max(1);
        let plan = (self.build)(snapshot, self.fetcher.clone(), target_partitions)?;
        execute_stream(plan, Arc::new(TaskContext::default()))
    }
}

fn logs_fragment(
    snapshot: Snapshot,
    fetcher: LogSegmentFetcher,
    target_partitions: usize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    LogsTableProvider::new(snapshot, TENANT, fetcher, QueryAccounting::new())
        .worker_fragment(target_partitions)
}

fn alerts_fragment(
    snapshot: Snapshot,
    fetcher: LogSegmentFetcher,
    target_partitions: usize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    AlertsTableProvider::new(snapshot, TENANT, fetcher, QueryAccounting::new())
        .worker_fragment(target_partitions)
}

fn audit_fragment(
    snapshot: Snapshot,
    fetcher: LogSegmentFetcher,
    target_partitions: usize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    AuditTableProvider::new(snapshot, TENANT, fetcher, QueryAccounting::new())
        .worker_fragment(target_partitions)
}

/// Extract `(ts, discriminator)` rows in emission order. `ts_col` is the
/// signal's timestamp column (`ts` for logs, `ts_ns` for alerts/audit);
/// `other_col` a nullable/non-null Utf8 differentiator. A NULL differentiator
/// renders as the empty string.
fn ts_and(batches: &[RecordBatch], ts_col: &str, other_col: &str) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ts = b
            .column_by_name(ts_col)
            .unwrap_or_else(|| panic!("column {ts_col}"))
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts column type");
        let other = b
            .column_by_name(other_col)
            .unwrap_or_else(|| panic!("column {other_col}"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("discriminator column type");
        for i in 0..b.num_rows() {
            let s = if other.is_null(i) {
                String::new()
            } else {
                other.value(i).to_string()
            };
            out.push((ts.value(i), s));
        }
    }
    out
}

async fn collect_plan(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect plan")
}

async fn collect_distributed(provider: Arc<dyn TableProvider>) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("distributed scan");
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect distributed")
}

// --- logs -----------------------------------------------------------------

#[tokio::test]
async fn distributed_logs_reshard_straddle_equals_sorted_local() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    // One stream (`service.name = api`) straddles two shards, ts interleaving.
    let res = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let s0 = write_rlog_segment(
        backend.as_ref(),
        "logs/reshard-0.rlog",
        0,
        1,
        10,
        &[
            log_record(&res, &[], 10, "b10"),
            log_record(&res, &[], 30, "b30"),
        ],
    )
    .await;
    let s1 = write_rlog_segment(
        backend.as_ref(),
        "logs/reshard-1.rlog",
        1,
        1,
        20,
        &[
            log_record(&res, &[], 20, "b20"),
            log_record(&res, &[], 40, "b40"),
        ],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&backend));

    assert!(
        rlog_endpoints_for(&snapshot).len() >= 2,
        "fixture must genuinely partition into more than one slice"
    );

    // Independent local reference: plain scan, sorted in-test (never the merge
    // exec under test).
    let local = collect_plan(
        LogsTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "ts", "body");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(RlogWorker {
        fetcher: fetcher.clone(),
        build: logs_fragment,
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        LogsTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let got = ts_and(&collect_distributed(dist).await, "ts", "body");

    assert_eq!(
        got, want,
        "the distributed output is the total-order merge of both slices; a slice-order concat would differ"
    );
    assert_eq!(
        got.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        vec![10, 20, 30, 40],
        "the straddling stream's ts are globally sorted across slices, not slice-concatenated"
    );
}

#[tokio::test]
async fn distributed_logs_preserves_duplicate_rows() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let res = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    // Two byte-identical records, one per shard: a retry after a lost ack.
    let s0 = write_rlog_segment(
        backend.as_ref(),
        "logs/dup-0.rlog",
        0,
        1,
        10,
        &[log_record(&res, &[], 50, "dup")],
    )
    .await;
    let s1 = write_rlog_segment(
        backend.as_ref(),
        "logs/dup-1.rlog",
        1,
        1,
        20,
        &[log_record(&res, &[], 50, "dup")],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&backend));

    let local = collect_plan(
        LogsTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "ts", "body");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(RlogWorker {
        fetcher: fetcher.clone(),
        build: logs_fragment,
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        LogsTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let mut got = ts_and(&collect_distributed(dist).await, "ts", "body");
    got.sort();

    assert_eq!(got, want, "distributed multiset equals local multiset");
    assert_eq!(
        got,
        vec![(50, "dup".to_string()), (50, "dup".to_string())],
        "both byte-identical rows survive; a reintroduced dedup would collapse them to one"
    );
}

#[tokio::test]
async fn distributed_logs_plan_merges_without_dedup() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let res = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let s0 = write_rlog_segment(
        backend.as_ref(),
        "logs/shape-0.rlog",
        0,
        1,
        10,
        &[log_record(&res, &[], 10, "b10")],
    )
    .await;
    let s1 = write_rlog_segment(
        backend.as_ref(),
        "logs/shape-1.rlog",
        1,
        1,
        20,
        &[log_record(&res, &[], 20, "b20")],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&backend));

    let client: Arc<dyn WorkerSliceClient> = Arc::new(RlogWorker {
        fetcher: fetcher.clone(),
        build: logs_fragment,
    });
    let dist = LogsTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
        .with_distributed_scan(rlog_endpoints_for(&snapshot), client);
    let ctx = SessionContext::new();
    let plan = dist
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("distributed scan");
    let text = displayable(plan.as_ref()).indent(true).to_string();

    assert!(
        text.contains("SortPreservingMergeExec"),
        "the RLOG merge is a SortPreservingMergeExec:\n{text}"
    );
    assert!(
        text.contains("DistributedSliceScanExec"),
        "over the fan-out slice scan:\n{text}"
    );
    // The load-bearing negative: logs/alerts/audit have NO query-time dedup, so
    // the plan must never grow an RsegDedupExec (or any dedup node) above the
    // merge. This is trap #1 from the task, guarded directly.
    assert!(
        !text.to_lowercase().contains("dedup"),
        "logs/alerts/audit have no query-time dedup; the merge must not introduce one:\n{text}"
    );
}

// --- alerts ----------------------------------------------------------------

#[tokio::test]
async fn distributed_alerts_reshard_straddle_equals_sorted_local() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let res = vec![("service.name".to_string(), AttrValue::Str("alerts".into()))];
    let s0 = write_rlog_segment(
        backend.as_ref(),
        "alerts/reshard-0.rlog",
        0,
        1,
        10,
        &[alert_record(&res, 10, "a10"), alert_record(&res, 30, "a30")],
    )
    .await;
    let s1 = write_rlog_segment(
        backend.as_ref(),
        "alerts/reshard-1.rlog",
        1,
        1,
        20,
        &[alert_record(&res, 20, "a20"), alert_record(&res, 40, "a40")],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&backend));

    let local = collect_plan(
        AlertsTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "ts_ns", "alert_id");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(RlogWorker {
        fetcher: fetcher.clone(),
        build: alerts_fragment,
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        AlertsTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let got = ts_and(&collect_distributed(dist).await, "ts_ns", "alert_id");

    assert_eq!(got, want, "alerts distributed merge equals sorted local");
    assert_eq!(
        got.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        vec![10, 20, 30, 40],
        "the straddling stream's ts are globally sorted across slices"
    );
}

#[tokio::test]
async fn distributed_alerts_preserves_duplicate_rows() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let res = vec![("service.name".to_string(), AttrValue::Str("alerts".into()))];
    let s0 = write_rlog_segment(
        backend.as_ref(),
        "alerts/dup-0.rlog",
        0,
        1,
        10,
        &[alert_record(&res, 50, "adup")],
    )
    .await;
    let s1 = write_rlog_segment(
        backend.as_ref(),
        "alerts/dup-1.rlog",
        1,
        1,
        20,
        &[alert_record(&res, 50, "adup")],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&backend));

    let local = collect_plan(
        AlertsTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "ts_ns", "alert_id");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(RlogWorker {
        fetcher: fetcher.clone(),
        build: alerts_fragment,
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        AlertsTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let mut got = ts_and(&collect_distributed(dist).await, "ts_ns", "alert_id");
    got.sort();

    assert_eq!(
        got, want,
        "alerts distributed multiset equals local multiset"
    );
    assert_eq!(
        got,
        vec![(50, "adup".to_string()), (50, "adup".to_string())],
        "both duplicate alert rows survive; a reintroduced dedup would drop one"
    );
}

// --- audit -----------------------------------------------------------------

#[tokio::test]
async fn distributed_audit_reshard_straddle_equals_sorted_local() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let res = vec![("service.name".to_string(), AttrValue::Str("audit".into()))];
    let s0 = write_rlog_segment(
        backend.as_ref(),
        "audit/reshard-0.rlog",
        0,
        1,
        10,
        &[
            log_record(&res, &[], 10, "e10"),
            log_record(&res, &[], 30, "e30"),
        ],
    )
    .await;
    let s1 = write_rlog_segment(
        backend.as_ref(),
        "audit/reshard-1.rlog",
        1,
        1,
        20,
        &[
            log_record(&res, &[], 20, "e20"),
            log_record(&res, &[], 40, "e40"),
        ],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&backend));

    let local = collect_plan(
        AuditTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "ts_ns", "body");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(RlogWorker {
        fetcher: fetcher.clone(),
        build: audit_fragment,
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        AuditTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let got = ts_and(&collect_distributed(dist).await, "ts_ns", "body");

    assert_eq!(got, want, "audit distributed merge equals sorted local");
    assert_eq!(
        got.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        vec![10, 20, 30, 40],
        "the straddling stream's ts are globally sorted across slices"
    );
}

#[tokio::test]
async fn distributed_audit_preserves_duplicate_rows() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let res = vec![("service.name".to_string(), AttrValue::Str("audit".into()))];
    let s0 = write_rlog_segment(
        backend.as_ref(),
        "audit/dup-0.rlog",
        0,
        1,
        10,
        &[log_record(&res, &[], 50, "edup")],
    )
    .await;
    let s1 = write_rlog_segment(
        backend.as_ref(),
        "audit/dup-1.rlog",
        1,
        1,
        20,
        &[log_record(&res, &[], 50, "edup")],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&backend));

    let local = collect_plan(
        AuditTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "ts_ns", "body");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(RlogWorker {
        fetcher: fetcher.clone(),
        build: audit_fragment,
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        AuditTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let mut got = ts_and(&collect_distributed(dist).await, "ts_ns", "body");
    got.sort();

    assert_eq!(
        got, want,
        "audit distributed multiset equals local multiset"
    );
    assert_eq!(
        got,
        vec![(50, "edup".to_string()), (50, "edup".to_string())],
        "both duplicate audit rows survive; a reintroduced dedup would drop one"
    );
}

// ===========================================================================
// spans SQL-lane distributed fan-out (#327, T6b)
// ===========================================================================
//
// The span sibling of the RLOG family above. Spans also have NO query-time
// dedup (docs/consistency-model.md, "logs and spans"; ADR-0051 section 5), so
// the coordinator merges under the span total order (`SPAN_ORDER_COLS`) with
// nothing above it. The two shapes mirror the RLOG ones exactly:
//
// - *reshard-straddle*: one trace's spans live in TWO shards (two slices), with
//   `start_ts` interleaving across the shards. The span total-order key is
//   primary on `(trace_id, span_id, start_ts, ...)`, so the fixture gives every
//   span the SAME trace_id and makes span_id order agree with start_ts order;
//   the total-order merge then emits start_ts [10, 20, 30, 40], an order NO
//   slice-order concatenation produces ([10,30,20,40] or [20,40,10,30]). The
//   specific line a naive concat breaks: the coordinator `SortPreservingMergeExec`
//   in `distributed_slice_plan` (equivalently the worker `SortExec` in
//   `sort_slice_fragment`). Replace either with an unordered coalesce and the
//   ordered assertion below fails.
//
// - *duplicate-preservation*: two byte-identical spans, one per shard (a retry
//   after a lost ack). Both must survive. The specific line a reintroduced dedup
//   breaks: the absence of any dedup node above the merge. Add a dedup/distinct
//   and the row count drops from 2 to 1 and the assertion below fails.
//
// prove-the-test discipline: the "local" reference on every differential is the
// plain single-process `SpansTableProvider::plan(..)` scan, collected and sorted
// IN-TEST -- it never calls the distributed merge exec under test, so a
// regression in that exec cannot hide by appearing on both sides.

use ravel_rspan::{
    ObjectIdentity as RspanObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode,
};
use ravel_sql::{SpanSegmentFetcher, SpansTableProvider};

/// A span on trace `trace` with the given `span_id`, `start_ts`, and `name`
/// (the differential's read-back discriminator). `end_ts = start_ts + 1`, root
/// span, `Ok` status, one attribute -- the fields the span total-order key does
/// not otherwise pin are held constant so a test controls the order through
/// `span_id`/`start_ts` alone.
fn span(trace: [u8; 16], span_id: [u8; 8], start: i64, name: &str) -> SpanRecord {
    SpanRecord {
        trace_id: trace,
        span_id,
        parent_span_id: None,
        name: name.to_string(),
        start_ts_ns: start,
        end_ts_ns: start + 1,
        status_code: StatusCode::Ok,
        status_message: None,
        attrs: vec![("service.name".to_string(), "svc".to_string())],
    }
}

/// Write one RSPAN object from `records` on `shard`, put it at `key`, and return
/// a matching L0 `SegmentRef` carrying `shard` (so `partition_snapshot` groups
/// it shard-major into its own slice). The identity's `tenant_hash` must match
/// [`TENANT`], which the span read path enforces in `fetch_accounted`.
async fn write_rspan_segment(
    store: &dyn ObjectStoreBackend,
    key: &str,
    shard: u32,
    writer_seq: u64,
    created_unix_ns: i64,
    records: &[SpanRecord],
) -> SegmentRef {
    let identity = RspanObjectIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq,
    };
    let mut w = RspanWriter::new(RspanConfig::default(), identity);
    for r in records {
        w.push(r.clone());
    }
    let bytes = w.finish().expect("finish object");
    let size = bytes.len() as u64;
    store
        .put(key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records
        .iter()
        .map(|r| r.start_ts_ns)
        .min()
        .expect("nonempty");
    let max = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(shard as u128 + 1),
        writer_epoch: 1,
        writer_seq,
        created_unix_ns,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_rspan::footer::VERSION),
    }
}

/// The in-process spans worker: rebuilds the slice's snapshot from the ticket and
/// runs `SpansTableProvider::worker_fragment` (`SpansScanExec -> SortExec`, public
/// spans schema, one sorted partition, NO dedup), streaming the result back. The
/// span analog of `RlogWorker`, using the `SpanSegmentFetcher` the spans path
/// reads through.
struct SpanWorker {
    fetcher: SpanSegmentFetcher,
}

impl std::fmt::Debug for SpanWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("SpanWorker")
    }
}

impl WorkerSliceClient for SpanWorker {
    fn fetch_slice(
        &self,
        _location: &str,
        ticket: &FlightTicket,
        _limit: Option<usize>,
    ) -> DFResult<SendableRecordBatchStream> {
        let snapshot = ticket.snapshot();
        let target_partitions = snapshot.segments.len().max(1);
        let plan = SpansTableProvider::new(
            snapshot,
            TENANT,
            self.fetcher.clone(),
            QueryAccounting::new(),
        )
        .worker_fragment(target_partitions)?;
        execute_stream(plan, Arc::new(TaskContext::default()))
    }
}

#[tokio::test]
async fn distributed_spans_reshard_straddle_equals_sorted_local() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    // One trace straddles two shards; span_id order agrees with start_ts order,
    // and the two shards interleave ts [10, 20, 30, 40].
    let trace = [1u8; 16];
    let s0 = write_rspan_segment(
        backend.as_ref(),
        "spans/reshard-0.rspan",
        0,
        1,
        10,
        &[
            span(trace, [1u8; 8], 10, "s10"),
            span(trace, [3u8; 8], 30, "s30"),
        ],
    )
    .await;
    let s1 = write_rspan_segment(
        backend.as_ref(),
        "spans/reshard-1.rspan",
        1,
        1,
        20,
        &[
            span(trace, [2u8; 8], 20, "s20"),
            span(trace, [4u8; 8], 40, "s40"),
        ],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = SpanSegmentFetcher::new(Arc::clone(&backend));

    assert!(
        rlog_endpoints_for(&snapshot).len() >= 2,
        "fixture must genuinely partition into more than one slice"
    );

    // Independent local reference: plain scan, sorted in-test (never the merge
    // exec under test).
    let local = collect_plan(
        SpansTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "start_ts", "name");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(SpanWorker {
        fetcher: fetcher.clone(),
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        SpansTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let got = ts_and(&collect_distributed(dist).await, "start_ts", "name");

    assert_eq!(
        got, want,
        "the distributed output is the total-order merge of both slices; a slice-order concat would differ"
    );
    assert_eq!(
        got.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        vec![10, 20, 30, 40],
        "the straddling trace's start_ts are globally sorted across slices, not slice-concatenated"
    );
}

#[tokio::test]
async fn distributed_spans_preserves_duplicate_rows() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    // Two byte-identical spans, one per shard: a retry after a lost ack.
    let dup = span([9u8; 16], [9u8; 8], 50, "dup");
    let s0 = write_rspan_segment(
        backend.as_ref(),
        "spans/dup-0.rspan",
        0,
        1,
        10,
        std::slice::from_ref(&dup),
    )
    .await;
    let s1 = write_rspan_segment(
        backend.as_ref(),
        "spans/dup-1.rspan",
        1,
        1,
        20,
        std::slice::from_ref(&dup),
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = SpanSegmentFetcher::new(Arc::clone(&backend));

    let local = collect_plan(
        SpansTableProvider::new(
            snapshot.clone(),
            TENANT,
            fetcher.clone(),
            QueryAccounting::new(),
        )
        .plan(4)
        .expect("local plan"),
    )
    .await;
    let mut want = ts_and(&local, "start_ts", "name");
    want.sort();

    let client: Arc<dyn WorkerSliceClient> = Arc::new(SpanWorker {
        fetcher: fetcher.clone(),
    });
    let dist: Arc<dyn TableProvider> = Arc::new(
        SpansTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
            .with_distributed_scan(rlog_endpoints_for(&snapshot), client),
    );
    let mut got = ts_and(&collect_distributed(dist).await, "start_ts", "name");
    got.sort();

    assert_eq!(
        got, want,
        "spans distributed multiset equals local multiset"
    );
    assert_eq!(
        got,
        vec![(50, "dup".to_string()), (50, "dup".to_string())],
        "both byte-identical spans survive; a reintroduced dedup would collapse them to one"
    );
}

#[tokio::test]
async fn distributed_spans_plan_merges_without_dedup() {
    let store = Arc::new(MemoryStore::new());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let s0 = write_rspan_segment(
        backend.as_ref(),
        "spans/shape-0.rspan",
        0,
        1,
        10,
        &[span([1u8; 16], [1u8; 8], 10, "s10")],
    )
    .await;
    let s1 = write_rspan_segment(
        backend.as_ref(),
        "spans/shape-1.rspan",
        1,
        1,
        20,
        &[span([2u8; 16], [2u8; 8], 20, "s20")],
    )
    .await;
    let snapshot = rlog_snapshot(vec![s0, s1]);
    let fetcher = SpanSegmentFetcher::new(Arc::clone(&backend));

    let client: Arc<dyn WorkerSliceClient> = Arc::new(SpanWorker {
        fetcher: fetcher.clone(),
    });
    let dist = SpansTableProvider::new(snapshot.clone(), TENANT, fetcher, QueryAccounting::new())
        .with_distributed_scan(rlog_endpoints_for(&snapshot), client);
    let ctx = SessionContext::new();
    let plan = dist
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("distributed scan");
    let text = displayable(plan.as_ref()).indent(true).to_string();

    assert!(
        text.contains("SortPreservingMergeExec"),
        "the spans merge is a SortPreservingMergeExec:\n{text}"
    );
    assert!(
        text.contains("DistributedSliceScanExec"),
        "over the fan-out slice scan:\n{text}"
    );
    // The load-bearing negative: spans have NO query-time dedup, so the plan must
    // never grow an RsegDedupExec (or any dedup node) above the merge. This is
    // trap #1 from the task, guarded directly.
    assert!(
        !text.to_lowercase().contains("dedup"),
        "spans have no query-time dedup; the merge must not introduce one:\n{text}"
    );
}

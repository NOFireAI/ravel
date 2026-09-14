//! SQL-lane distributed read fan-out (ADR-0071).
//!
//! # What this crate owns, and what it does not
//!
//! ADR-0071 distributes *where bytes are fetched and decoded*, never *what a
//! query computes*. On the SQL lane the coordinator resolves ONE pinned
//! snapshot exactly as today, partitions it into shard-major slices (the same
//! [`partition_snapshot`](ravel_query::distrib::partition::partition_snapshot)
//! the PromQL lane uses -- this crate never re-implements slicing), and hands
//! each slice to a worker over Arrow Flight. Each worker executes the existing
//! fetch/decode path over only its slice's segments and streams back the
//! *internal*, provenance-carrying, `(series_id, ts)`-sorted rows -- the exact
//! shape [`crate::scan::RsegScanExec`] produces, pre-merged per slice but NOT
//! deduplicated. The coordinator's [`DistributedScanExec`] exposes each worker
//! stream as one sorted partition feeding the SAME
//! `SortPreservingMergeExec -> RsegDedupExec` pair the local plan uses, so the
//! k-way merge and the authoritative cross-slice dedup are unchanged and the
//! distributed result is byte-identical to local execution.
//!
//! Dedup is authoritative at the coordinator, not the worker: the same
//! `(series_id, ts)` sample can be written through segments in different ingest
//! shards, so its greatest-wins winner is only decidable once every slice's
//! candidates meet. That is why workers return the provenance columns and do
//! not deduplicate: the coordinator's `RsegDedupExec` is the one dedup, sitting
//! as an idempotent belt above the merge.
//!
//! Aggregation does not move (ADR-0071 rejected alternative 3, and the
//! `session.rs` single-partition determinism ban): `RsegDedupExec` emits a
//! single partition, so any aggregate the session plans above it runs
//! single-partitioned exactly as on the local path. Distribution changes the
//! scan's partitions, never the aggregation's.
//!
//! # Reachability
//!
//! No client-facing RPC ever reaches this, and none ever will. An external
//! Flight SQL client always receives exactly ONE
//! endpoint from `get_flight_info_statement`, distribution installed or not:
//! Arrow Flight's two-RPC contract makes the N endpoints of one FlightInfo the
//! *partitions of a single result set* a client unions, which a cross-shard
//! scan fan-out is not (unioning N per-slice partial scans would double-count
//! undeduped rows and mis-count aggregates). Slice tickets are instead an
//! INTERNAL coordinator-to-worker contract: [`plan_distributed_slices`] mints
//! them, [`DistributedScanExec`] redeems them through a [`WorkerSliceClient`],
//! and a worker serves each one over its `do_get` slice-fragment path
//! (internal schema, scan-only, no statement planning or dedup).
//!
//! The coordinator that mints slices and installs the fan-out is the running
//! server: `ravel-server` builds a [`DistributedFlightConfig`] over its live
//! query-worker roster (`services/ravel-server/src/sql_distrib.rs`) and installs
//! it on the Flight SQL service under `--distributed-query` in a query-serving
//! mode, so every statement over the cost gate builds a
//! [`DistributedScanExec`]. The acceptance tests
//! (`tests/flight_distributed.rs`) drive the same minting-plus-fan-out path
//! against an in-process worker, the real service `do_get`, and a refused port.
//!
//! # No dependency on the fleet registry
//!
//! Worker locations arrive through the [`WorkerEndpoints`] trait defined here;
//! the coordinator fetches each slice through the [`WorkerSliceClient`] trait
//! defined here. ravel-sql takes no dependency on `ravel-fleet` or any worker
//! registry: the deployment implements both traits over the real
//! heartbeat/rendezvous membership and a real Arrow Flight client.
//!
//! # Failure behavior
//!
//! A registered worker can be dead: the fleet's heartbeat keeps a corpse in the
//! live set for `3 * H` after it stops beating, so one unreachable location
//! would otherwise fail every statement over the cost gate for that whole
//! window. [`DistributedScanExec::execute`] therefore runs the same three-step
//! sequence the PromQL lane's `RoutingSliceFetcher::dispatch` runs
//! (`services/ravel-server/src/distrib.rs`), per slice:
//!
//! 1. the location the slice was assigned;
//! 2. EXACTLY ONE re-dispatch, to the first other location in the endpoint list;
//! 3. a coordinator-local read of the same slice ticket
//!    ([`CoordinatorSliceReader`]), which runs the identical worker fragment
//!    against the identical object store, so a successful local read is
//!    byte-identical to the remote result it replaces;
//! 4. otherwise a typed [`SqlError::Execution`] naming the last cause. Never a
//!    partial merge.
//!
//! A step is taken when the previous one fails either at construction
//! ([`WorkerSliceClient::fetch_slice`] returns `Err`: an unparseable location, a
//! ticket that will not encode) or on the FIRST poll of its stream (a refused
//! or reset connection, which is where a dead port usually surfaces, since the
//! channel is dialed lazily). Both shapes are caught because each attempt is
//! probed for its first batch before the partition emits anything. A failure
//! after the first batch is NOT re-dispatchable and surfaces typed: rows have
//! already reached the coordinator's `SortPreservingMergeExec`, and restarting
//! the slice underneath it would feed the merge a second, out-of-order run of
//! the same `(series_id, ts)` range.
//!
//! [`SliceFallbackCounters`] counts the steps, split so a re-dispatch to another
//! worker and a coordinator-local read are never pooled into one figure.
//!
//! What this lane still does NOT have, and the PromQL lane does: rendezvous
//! placement (slice `k` goes to `endpoints()[k % len]`, so roster order alone
//! decides placement) and a quarantine map (a dead location is re-tried by the
//! next statement rather than skipped). Both need shared state with the PromQL
//! router and are tracked separately.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{ProstMessageExt, TicketStatementQuery};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, execute_stream,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use prost::Message as _;
use ravel_catalog::Snapshot;
use ravel_query::distrib::partition::{partition_snapshot, should_distribute};
use ravel_query::{ByteLimit, PhaseAccounting, SegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, CostEstimate, QueryAccounting};
use tonic::Request;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

use crate::config::SqlConfig;
use crate::dedup::RsegDedupExec;
use crate::error::SqlError;
use crate::flight_ticket::{FlightTicket, SegmentPin, TicketKey};
use crate::provider::RavelTableProvider;
use crate::schema::internal_schema;

/// Advertised worker Flight locations for a distributed query.
///
/// The deployment implements this over its real worker membership (ADR-0071:
/// a heartbeat live-set with rendezvous hashing); ravel-sql only
/// states what it needs. An empty list means no workers are available, and the
/// query runs fully local: [`plan_distributed_slices`] returns `None`, no
/// [`DistributedScanExec`] is built, and the coordinator scans the whole pinned
/// set itself through the ordinary local pipeline.
///
/// The list is a *placement* roster, not a liveness guarantee. A heartbeat
/// live-set keeps a dead worker registered for its full staleness window, and
/// the deployment's own exclusions (a version-skewed worker, the coordinator
/// itself) are applied by the implementation, not here. A location that is
/// listed but unreachable costs one failed attempt per slice assigned to it and
/// is then replaced by the next step of [`DistributedScanExec`]'s fallback
/// sequence; it never fails the statement on its own.
pub trait WorkerEndpoints: Send + Sync + 'static {
    /// The worker locations (Arrow Flight URIs), in a stable order. The
    /// coordinator assigns slice `k` to `endpoints()[k % len]`; placement is
    /// round-robin over roster order, with no rendezvous rank (see the module
    /// doc's failure-behavior section).
    fn endpoints(&self) -> Vec<String>;
}

/// The location string handed to a [`CoordinatorSliceReader`] (or any other
/// coordinator-local [`WorkerSliceClient`]) for the local step of the fallback
/// sequence. A local read dials nothing, so the value is only ever a label: it
/// is what a log line or a stats entry names when a slice ran on the
/// coordinator instead of a worker.
pub const COORDINATOR_LOCAL_LOCATION: &str = "coordinator-local";

/// Per-query counters for [`DistributedScanExec`]'s fallback sequence, split by
/// step (module doc, "Failure behavior").
///
/// Cloning shares the counts: the plan holds one handle and the caller that
/// built the plan keeps another, so a query's figures are readable once its
/// stream has drained. The split is the point -- a single "a fallback happened"
/// figure cannot tell a cluster that re-dispatched successfully from one whose
/// every worker is gone and whose reads are all landing on the coordinator.
///
/// Each counter records an ATTEMPT, matching the PromQL lane's
/// `record_slice_redispatched`/`record_slice_fallback`: [`Self::redispatched`]
/// moves only when a second remote fetch is actually sent, so a fan-out with a
/// single distinct location records a local read and no phantom re-dispatch.
#[derive(Clone, Debug, Default)]
pub struct SliceFallbackCounters {
    inner: Arc<SliceFallbackCounts>,
}

#[derive(Debug, Default)]
struct SliceFallbackCounts {
    redispatched: AtomicU64,
    local_reads: AtomicU64,
    failed: AtomicU64,
}

impl SliceFallbackCounters {
    /// A fresh, zeroed handle.
    pub fn new() -> Self {
        SliceFallbackCounters::default()
    }

    /// Slices whose assigned worker failed and for which a second remote fetch
    /// was actually sent to another location.
    pub fn redispatched(&self) -> u64 {
        self.inner.redispatched.load(Ordering::Relaxed)
    }

    /// Slices read on the coordinator itself after every remote step failed.
    pub fn local_reads(&self) -> u64 {
        self.inner.local_reads.load(Ordering::Relaxed)
    }

    /// Slices that exhausted the whole sequence and failed typed.
    pub fn failed(&self) -> u64 {
        self.inner.failed.load(Ordering::Relaxed)
    }

    fn record_redispatch(&self) {
        self.inner.redispatched.fetch_add(1, Ordering::Relaxed);
    }

    fn record_local_read(&self) {
        self.inner.local_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.inner.failed.fetch_add(1, Ordering::Relaxed);
    }
}

/// What [`DistributedScanExec`] falls back to when a slice's assigned worker
/// fails: the coordinator's own slice reader (the sequence's third step) and
/// the counters the sequence reports through.
///
/// [`Self::remote_only`] leaves the local step out, which shortens the sequence
/// to "assigned worker, one other worker, typed error". That is the right shape
/// for a caller that has no local store access for the slice's segments; it is
/// never a silent degradation, because the counters still distinguish the steps
/// that did run.
#[derive(Clone, Debug, Default)]
pub struct SliceFallback {
    local: Option<Arc<dyn WorkerSliceClient>>,
    counters: SliceFallbackCounters,
}

impl SliceFallback {
    /// The full sequence, with `local` as the coordinator-local step.
    pub fn with_local(local: Arc<dyn WorkerSliceClient>) -> Self {
        SliceFallback {
            local: Some(local),
            counters: SliceFallbackCounters::new(),
        }
    }

    /// The sequence without a coordinator-local step.
    pub fn remote_only() -> Self {
        SliceFallback::default()
    }

    /// This fallback's counter handle; clone it before building the plan to
    /// read the figures afterwards.
    pub fn counters(&self) -> SliceFallbackCounters {
        self.counters.clone()
    }
}

/// The coordinator's own reader for a slice ticket: the last step of
/// [`DistributedScanExec`]'s fallback sequence, and the only one that leaves no
/// process.
///
/// It runs exactly what a worker would run for the same ticket --
/// [`RavelTableProvider::worker_fragment`] over the ticket's pinned segments
/// (`RsegScanExec -> SortPreservingMergeExec`, internal schema, no dedup), the
/// same plan `SqlExecutor::worker_fragment_stream` serves a remote slice fetch
/// from -- against the same object store. A successful local read is therefore
/// byte-identical to the remote result it replaces; only the hop is gone.
///
/// The tenant is the coordinator's own resolved tenant, held here rather than
/// read from the ticket: a local read must not be steerable by ticket content.
/// The byte and request budgets come from the coordinator's [`SqlConfig`] and
/// are enforced by the scan itself, which also folds its real store spend into
/// the phase accounting handle given here -- the same handle the coordinator's
/// local path uses, so a fallback read is accounted as the store work it is
/// rather than as the wire bytes it is not.
pub struct CoordinatorSliceReader {
    tenant_hash: TenantHash,
    fetcher: SegmentFetcher,
    config: SqlConfig,
    phase_accounting: PhaseAccounting,
}

impl CoordinatorSliceReader {
    /// Build the coordinator's local slice reader from the same tenant,
    /// fetcher, config, and accounting handle its local scan path uses.
    pub fn new(
        tenant_hash: TenantHash,
        fetcher: SegmentFetcher,
        config: SqlConfig,
        phase_accounting: PhaseAccounting,
    ) -> Self {
        CoordinatorSliceReader {
            tenant_hash,
            fetcher,
            config,
            phase_accounting,
        }
    }
}

impl fmt::Debug for CoordinatorSliceReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CoordinatorSliceReader")
            .finish_non_exhaustive()
    }
}

impl WorkerSliceClient for CoordinatorSliceReader {
    /// `location` is ignored: this reader is the coordinator, and the only
    /// value it is ever passed is [`COORDINATOR_LOCAL_LOCATION`]. The `limit`
    /// hint is ignored for the same reason the in-process worker ignores it:
    /// the exact limit is re-applied above the dedup, so returning the whole
    /// slice is always correct.
    fn fetch_slice(
        &self,
        _location: &str,
        ticket: &FlightTicket,
        _limit: Option<usize>,
        context: &Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let snapshot = ticket.snapshot();
        let segments = snapshot.segments.clone();
        let provider = RavelTableProvider::new(
            snapshot,
            self.tenant_hash,
            self.fetcher.clone(),
            self.config.clone(),
            self.phase_accounting.clone(),
        );
        let plan = provider.worker_fragment(segments.len().max(1), &segments)?;
        // The partition's own context, not a default one. `RsegScanExec`
        // registers its `MemoryReservation` against `context.memory_pool()`,
        // and a bare `TaskContext` carries an unbounded pool, so a default here
        // would make this the one read path whose decoded batches are neither
        // counted against nor limited by the tenant's query budget.
        execute_stream(plan, Arc::clone(context))
    }
}

/// A fixed worker-location list, for tests and for a deployment with a static
/// roster. Deliberately trivial: real membership lives behind the same trait
/// in the server crate.
#[derive(Debug, Clone, Default)]
pub struct StaticWorkerEndpoints(Vec<String>);

impl StaticWorkerEndpoints {
    /// Build from an iterator of location strings.
    pub fn new(locations: impl IntoIterator<Item = String>) -> Self {
        StaticWorkerEndpoints(locations.into_iter().collect())
    }
}

impl WorkerEndpoints for StaticWorkerEndpoints {
    fn endpoints(&self) -> Vec<String> {
        self.0.clone()
    }
}

/// The service-side distributed context: the cost gate/fan-out width and the
/// worker roster. Held in an `Option` on the Flight service; `None` (the
/// default) means every query returns a single local endpoint, exactly as
/// before this module existed.
pub struct DistributedFlightConfig {
    /// Worker locations, resolved per query.
    pub workers: Arc<dyn WorkerEndpoints>,
    /// The cost gate and maximum fan-out width. Reuses the PromQL lane's
    /// [`DistribThresholds`](ravel_query::distrib::partition::DistribThresholds)
    /// so both lanes gate distribution on the same estimate semantics.
    pub thresholds: ravel_query::distrib::partition::DistribThresholds,
    /// The cluster-shared ticket MAC key, or `None` to keep the service's own
    /// key.
    ///
    /// A coordinator mints slice tickets a *different* worker process verifies
    /// (ADR-0071), so both must key the MAC identically. A
    /// multi-process deployment sets this to
    /// [`derive_ticket_key`](crate::flight_ticket::derive_ticket_key) over the
    /// shared cluster secret; installing the config then overrides the service's
    /// per-process random key with it. `None` (the in-process test default)
    /// leaves the service's own key untouched, so a single-process fixture whose
    /// coordinator and worker are the same instance stays byte-identical.
    pub shared_ticket_key: Option<TicketKey>,
}

/// One worker endpoint for [`DistributedScanExec`]: where to fetch the slice
/// from, and the Flight ticket that pins it.
#[derive(Debug, Clone)]
pub struct WorkerSlice {
    /// The worker's Arrow Flight location (URI).
    pub location: String,
    /// The slice ticket, pinning this endpoint's subset of the resolved
    /// snapshot (ADR-0071; [`FlightTicket::slice_index`]/`slice_count`).
    pub ticket: FlightTicket,
}

/// The coordinator's seam to a worker's Flight `DoGet`: given a location and a
/// slice ticket, open the worker's internal-schema, `(series_id, ts)`-sorted
/// stream for that slice.
///
/// The returned stream MUST carry [`internal_schema`] (the four provenance
/// columns the coordinator's dedup needs) and be sorted by `(series_id, ts)`,
/// which is exactly what a worker's `RsegScanExec -> SortPreservingMergeExec`
/// fragment produces. `limit`, when present, is a best-effort fetch-stop hint:
/// a worker MAY stop early, but over-fetching is always safe because the
/// coordinator re-applies the exact limit above the dedup.
pub trait WorkerSliceClient: Send + Sync + fmt::Debug {
    /// Open the worker `DoGet` stream for `ticket` at `location`.
    /// `context` is the partition's own `TaskContext`, threaded from
    /// [`DistributedScanExec::execute`]. A remote client ignores it: the
    /// worker accounts for its own decode. The coordinator-local reader must
    /// NOT, because it decodes on this process and its batches have to land in
    /// the same memory pool the rest of the query uses. Passing
    /// `TaskContext::default()` there gives an unbounded pool and takes the
    /// fallback read outside the tenant's budget, in exactly the degraded
    /// window this trait's fallback exists to serve.
    fn fetch_slice(
        &self,
        location: &str,
        ticket: &FlightTicket,
        limit: Option<usize>,
        context: &Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream>;
}

/// The production [`WorkerSliceClient`]: dials a worker's Arrow Flight location
/// over tonic and redeems a slice ticket through its `DoGet`, decoding the
/// internal-schema record batches the worker's scan-only fragment path streams
/// back (ADR-0071).
///
/// This is the real-wire twin of the acceptance tests' in-process worker
/// (`tests/flight_distributed.rs`): the encode/redeem/decode steps are
/// identical, only the transport differs -- here a real
/// [`FlightServiceClient`] over a [`Channel`] rather than an in-process
/// `do_get_statement` call. Every failure -- an unparseable location, a ticket
/// that will not encode, a transport or `DoGet` `Status`, a decode error --
/// surfaces as the trait's typed [`DataFusionError`]; nothing panics.
///
/// The coordinator forwards the inbound request's gRPC metadata (the tenant
/// credential) to each worker unchanged, so a worker's own `FlightAuth`
/// resolves the same tenant the slice ticket pins. `ticket_key` is the
/// coordinator's per-service ticket MAC key; a worker only accepts a slice
/// ticket signed by a key it shares (single process today; cross-process key
/// distribution is the ADR-0071 follow-up).
#[derive(Clone)]
pub struct FlightWorkerSliceClient {
    /// The coordinator's ticket MAC key, used to sign each slice ticket the
    /// worker will verify.
    ticket_key: TicketKey,
    /// The inbound request's gRPC metadata, forwarded verbatim to every worker
    /// so the worker resolves the same tenant credential.
    credentials: MetadataMap,
    /// Bounded connect timeout for dialing a worker location. The channel is
    /// built lazily; this caps how long the first `DoGet` waits for the TCP and
    /// HTTP/2 handshake before failing with a typed error.
    connect_timeout: Duration,
}

impl fmt::Debug for FlightWorkerSliceClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Deliberately opaque: `credentials` carries the tenant bearer token,
        // and `MetadataMap`'s own `Debug` would print it. Never widen this to
        // derive `Debug`.
        f.debug_struct("FlightWorkerSliceClient")
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

impl FlightWorkerSliceClient {
    /// Build a client that signs slice tickets with `ticket_key`, forwards
    /// `credentials` to every worker, and bounds each dial by `connect_timeout`.
    pub fn new(ticket_key: TicketKey, credentials: MetadataMap, connect_timeout: Duration) -> Self {
        FlightWorkerSliceClient {
            ticket_key,
            credentials,
            connect_timeout,
        }
    }
}

impl WorkerSliceClient for FlightWorkerSliceClient {
    fn fetch_slice(
        &self,
        location: &str,
        ticket: &FlightTicket,
        _limit: Option<usize>,
        // The worker decodes on its own process and accounts there.
        _context: &Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Encode the slice ticket exactly as the in-process path does. A
        // `slice_count > 1` ticket routes to the worker's scan-only fragment
        // branch, which streams the internal, provenance-carrying schema for the
        // coordinator to merge and dedup.
        let handle = ticket
            .encode(&self.ticket_key)
            .map_err(|err| DataFusionError::Internal(err.to_string()))?;
        // Build the channel eagerly so an unparseable location fails here, as a
        // typed error, rather than inside the stream. `connect_lazy` defers the
        // actual TCP/HTTP2 handshake to the first `DoGet`, bounded by
        // `connect_timeout`.
        let channel = Channel::from_shared(location.to_string())
            .map_err(|err| {
                DataFusionError::Internal(format!("invalid worker location {location:?}: {err}"))
            })?
            .connect_timeout(self.connect_timeout)
            .connect_lazy();
        let credentials = self.credentials.clone();
        let setup = async move {
            let tsq = TicketStatementQuery {
                statement_handle: handle.into(),
            };
            let raw = Ticket::new(tsq.as_any().encode_to_vec());
            let mut req = Request::new(raw);
            // Forward the inbound tenant credential unchanged so the worker's
            // own `FlightAuth` resolves the same tenant the ticket pins.
            *req.metadata_mut() = credentials;
            let response = FlightServiceClient::new(channel)
                .do_get(req)
                .await
                .map_err(|status| FlightError::Tonic(Box::new(status)))?;
            let flight_data = response
                .into_inner()
                .map_err(|status| FlightError::Tonic(Box::new(status)));
            Ok::<_, FlightError>(FlightRecordBatchStream::new_from_flight_data(flight_data))
        };
        // Validate every worker batch against the internal scan schema. The
        // adapter below merely *declares* `internal_schema()`; it does not check
        // that the frames the worker actually streams carry it. A version-skewed,
        // buggy, or hostile worker that returns a different layout must fail
        // typed here, not feed mismatched columns into the coordinator's
        // `(series_id, ts)` merge/dedup downstream.
        let expected = internal_schema();
        let validate = expected.clone();
        let batches = futures::stream::once(setup)
            .try_flatten()
            .map_err(|err| DataFusionError::Internal(err.to_string()))
            .map(move |item| {
                let batch = item?;
                validate_worker_schema(batch.schema().as_ref(), validate.as_ref())?;
                Ok(batch)
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(expected, batches)))
    }
}

/// Verify a worker's slice stream carries the internal scan schema the
/// coordinator's merge/dedup pipeline requires (ADR-0071).
///
/// The coordinator declares [`internal_schema`] as the distributed scan's
/// output, and its `SortPreservingMergeExec`/`RsegDedupExec` reference the
/// `series_id`/`ts` columns by name and type. A worker that streams a different
/// layout must fail with a typed error here, not silently feed mismatched
/// columns into the merge (a panic or a wrong result downstream). Column count,
/// names, data types, and nullability must match exactly; incidental
/// schema-level or field-level metadata that Flight encode/decode may carry is
/// not compared.
fn validate_worker_schema(actual: &Schema, expected: &Schema) -> DFResult<()> {
    let same = actual.fields().len() == expected.fields().len()
        && actual.fields().iter().zip(expected.fields()).all(|(a, e)| {
            a.name() == e.name()
                && a.data_type() == e.data_type()
                && a.is_nullable() == e.is_nullable()
        });
    if same {
        Ok(())
    } else {
        Err(DataFusionError::Internal(format!(
            "worker slice stream schema does not match the expected internal scan schema: \
             expected {expected:?}, got {actual:?}"
        )))
    }
}

/// The leaf of the coordinator's distributed samples pipeline: one DataFusion
/// partition per worker endpoint, each partition the worker's
/// internal-schema, `(series_id, ts)`-sorted slice stream.
///
/// Mirrors [`crate::scan::RsegScanExec`]'s declared properties -- the same
/// internal schema, a `(series_id, ts)` ordering, and one partition per unit of
/// work -- so it drops straight into the existing
/// `SortPreservingMergeExec -> RsegDedupExec` pair (see
/// [`distributed_samples_plan`]).
pub struct DistributedScanExec {
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    /// Best-effort fetch-stop hint threaded to each worker (ADR-0071). The
    /// exact limit is re-applied above the dedup by [`distributed_samples_plan`]
    /// / DataFusion, so this may over-fetch but never causes under-fetch.
    limit: Option<usize>,
    /// The coordinator's byte-accounting handle (ADR-0044, ADR-0061 decision
    /// 1). Each batch a worker streams back is folded into it by
    /// `RecordBatch::get_array_memory_size`, so a distributed query's fetched
    /// bytes join the coordinator's `/metrics` fold exactly as a local scan's
    /// do. `crate::provider` threads the query's own handle in here.
    accounting: QueryAccounting,
    /// The per-tenant bytes-scanned budget (ADR-0061 decision 1). Checked after
    /// each folded batch; a trip fails the scan with
    /// [`SqlError::TooManyBytesScanned`], the same typed error the local
    /// `RsegScanExec` path raises, so the two lanes surface the trip the same
    /// way. [`ByteLimit::Unlimited`] (the default) never trips and still folds
    /// the bytes.
    max_bytes_scanned: ByteLimit,
    /// The per-slice failure sequence: the coordinator's own reader for the
    /// local step, and the counters every step reports through (module doc,
    /// "Failure behavior").
    fallback: SliceFallback,
    /// The internal scan schema and `(series_id, ts)` ordering; the schema is
    /// reached through `ExecutionPlan::schema()` off these properties.
    properties: Arc<PlanProperties>,
}

impl DistributedScanExec {
    /// Build a scan over one worker endpoint per slice. `endpoints` must be
    /// non-empty: a distributed plan with no worker is a caller bug, not a
    /// silently-empty result.
    pub fn new(
        endpoints: Vec<WorkerSlice>,
        client: Arc<dyn WorkerSliceClient>,
        limit: Option<usize>,
        accounting: QueryAccounting,
        max_bytes_scanned: ByteLimit,
        fallback: SliceFallback,
    ) -> DFResult<Self> {
        if endpoints.is_empty() {
            return Err(DataFusionError::Internal(
                "DistributedScanExec needs at least one worker endpoint".into(),
            ));
        }
        let properties = Arc::new(Self::compute_properties(
            &internal_schema(),
            endpoints.len(),
        )?);
        Ok(DistributedScanExec {
            endpoints,
            client,
            limit,
            accounting,
            max_bytes_scanned,
            fallback,
            properties,
        })
    }

    /// The `(series_id, ts)` ordering each partition promises -- the ordering
    /// the coordinator's `SortPreservingMergeExec` merges on and
    /// `RsegDedupExec` requires. Declaring it is what keeps the optimizer from
    /// striking the merge (the exact hazard `crate::dedup` documents).
    fn compute_properties(schema: &SchemaRef, n: usize) -> DFResult<PlanProperties> {
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_exprs = ["series_id", "ts"]
            .into_iter()
            .map(|name| Ok(PhysicalSortExpr::new(col(name, schema)?, asc)))
            .collect::<DFResult<Vec<_>>>()?;
        let ordering = LexOrdering::new(sort_exprs)
            .ok_or_else(|| DataFusionError::Internal("empty distributed scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for DistributedScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedScanExec {{ endpoints: {} }}",
            self.endpoints.len()
        )
    }
}

impl DisplayAs for DistributedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedScanExec: endpoints={}, limit={:?}",
            self.endpoints.len(),
            self.limit
        )
    }
}

impl ExecutionPlan for DistributedScanExec {
    fn name(&self) -> &str {
        "DistributedScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let endpoint = self
            .endpoints
            .get(partition)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "DistributedScanExec: partition {partition} out of range"
                ))
            })?
            .clone();
        // The single re-dispatch target: the first location in the endpoint list
        // that is not the failed one. `None` (every slice on one location) drops
        // straight to the coordinator-local step, which is what the PromQL lane
        // does when its second-ranked owner is the coordinator itself.
        let next = self
            .endpoints
            .iter()
            .map(|e| e.location.as_str())
            .find(|location| *location != endpoint.location)
            .map(str::to_string);
        let client = Arc::clone(&self.client);
        let local = self.fallback.local.clone();
        let counters = self.fallback.counters.clone();
        let limit = self.limit;
        let accounting = self.accounting.clone();
        let max_bytes_scanned = self.max_bytes_scanned;
        let schema = self.schema();

        // The whole sequence runs inside the partition's stream, so it is paid
        // lazily, on the first poll, exactly where a single fetch used to be.
        let setup = async move {
            let WorkerSlice {
                location: primary,
                ticket,
            } = endpoint;

            // Step 1: the assigned worker.
            let mut cause = match probe_slice(client.as_ref(), &primary, &ticket, limit, &context)
                .await
            {
                Ok(stream) => return Ok(fold_wire_bytes(stream, accounting, max_bytes_scanned)),
                Err(err) => err,
            };

            // Step 2: EXACTLY ONE re-dispatch, to another worker. The counter
            // moves here, where a second remote fetch is actually sent, not on
            // entry to the sequence.
            if let Some(next) = next {
                counters.record_redispatch();
                tracing::warn!(
                    slice_index = ticket.slice_index,
                    slice_count = ticket.slice_count,
                    error = %cause,
                    "distributed slice fetch failed; re-dispatching to another worker"
                );
                match probe_slice(client.as_ref(), &next, &ticket, limit, &context).await {
                    Ok(stream) => {
                        return Ok(fold_wire_bytes(stream, accounting, max_bytes_scanned));
                    }
                    Err(err) => cause = err,
                }
            }

            // Step 3: the coordinator reads the slice itself. Not folded through
            // `fold_wire_bytes`: there are no wire bytes, and the local scan
            // folds its real store spend into the same query accounting (and
            // enforces the same byte ceiling) on its own, so folding the batch
            // memory sizes on top would double-count the same read.
            if let Some(local) = local {
                counters.record_local_read();
                tracing::warn!(
                    slice_index = ticket.slice_index,
                    slice_count = ticket.slice_count,
                    error = %cause,
                    "distributed slice fetch failed on every worker; reading it on the coordinator"
                );
                match probe_slice(
                    local.as_ref(),
                    COORDINATOR_LOCAL_LOCATION,
                    &ticket,
                    limit,
                    &context,
                )
                .await
                {
                    Ok(stream) => return Ok(stream),
                    Err(err) => cause = err,
                }
            }

            // Step 4: every step failed. Typed, naming the last cause, and never
            // a partial result: nothing has been emitted at this point, because
            // each attempt is probed before the partition emits anything.
            counters.record_failure();
            Err(DataFusionError::from(SqlError::Execution(format!(
                "distributed slice {} of {} could not be read on any worker \
                 or on the coordinator: {cause}",
                ticket.slice_index, ticket.slice_count
            ))))
        };

        let batches = futures::stream::once(setup).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

/// One slice's batches, in the internal scan schema: what each step of the
/// fallback sequence yields and what the partition ultimately emits.
type SliceStream = BoxStream<'static, DFResult<RecordBatch>>;

/// Open `location`'s slice stream AND pull its first batch, so an attempt is
/// only accepted once it has actually produced something.
///
/// This is what makes the fallback cover both Flight failure shapes (module
/// doc): `fetch_slice` returns `Err` for a construct-time failure, and the
/// first poll is where a lazily-dialed channel surfaces a refused or reset
/// connection. The pulled batch is put back in front of the rest of the stream,
/// so nothing is lost and no row is emitted twice.
async fn probe_slice(
    client: &dyn WorkerSliceClient,
    location: &str,
    ticket: &FlightTicket,
    limit: Option<usize>,
    context: &Arc<TaskContext>,
) -> DFResult<SliceStream> {
    let mut stream = client.fetch_slice(location, ticket, limit, context)?;
    let first = match stream.next().await {
        Some(Ok(batch)) => Some(batch),
        Some(Err(err)) => return Err(err),
        // An empty slice is a successful attempt: a shard can hold no rows in
        // the query's window, and re-dispatching it would only fetch the same
        // emptiness from somewhere else.
        None => None,
    };
    Ok(Box::pin(futures::stream::iter(first.map(Ok)).chain(stream)))
}

/// Fold each worker batch into the coordinator's byte accounting and enforce
/// the per-tenant budget here, at the coordinator (ADR-0061 decision 1,
/// ADR-0044).
///
/// A worker's Flight stream carries only rows, not a structured cost sidecar,
/// so `get_array_memory_size` is the bytes-scanned proxy this lane folds -- the
/// same per-batch measure the local scan's memory reservation grows by. The
/// check mirrors the local `RsegScanExec` path (crate::scan): once the running
/// total passes a bounded cap, fail with the same typed
/// `SqlError::TooManyBytesScanned` rather than truncating. `Unlimited` never
/// trips, so a caller that does not opt in behaves exactly as before this check
/// existed.
fn fold_wire_bytes(
    stream: SliceStream,
    accounting: QueryAccounting,
    max_bytes_scanned: ByteLimit,
) -> SliceStream {
    Box::pin(stream.map(move |item| {
        let batch = item?;
        let bytes = batch.get_array_memory_size() as u64;
        accounting.add_s3_bytes(AccountedOp::Get, bytes);
        let scanned = accounting.snapshot().total_s3_bytes();
        if max_bytes_scanned.is_exceeded_by(scanned) {
            let max = match max_bytes_scanned {
                ByteLimit::Bounded(max) => max,
                ByteLimit::Unlimited => scanned,
            };
            return Err(DataFusionError::from(SqlError::TooManyBytesScanned {
                scanned,
                max,
            }));
        }
        Ok(batch)
    }))
}

/// Assemble the coordinator's distributed samples pipeline:
///
/// ```text
/// DistributedScanExec (one partition per worker slice)
///   -> SortPreservingMergeExec (series_id, ts)
///   -> RsegDedupExec            (single partition, authoritative dedup)
///   [-> GlobalLimitExec(limit)  when a fetch limit is present]
/// ```
///
/// The merge and dedup nodes are the *same* ones [`crate::provider`] builds for
/// the local path, so a distributed query merges and deduplicates identically.
/// When `limit` is `Some`, an exact [`GlobalLimitExec`] is placed above the
/// dedup: the per-worker fetch is only a hint (it may over-fetch), and this
/// exact limit is what guarantees the plan returns exactly `limit` rows without
/// ever under-fetching. Aggregation, when the session plans it, sits above this
/// whole stack on the dedup's single partition, so the SQL determinism ban is
/// untouched.
///
/// `fallback` carries the per-slice failure sequence (module doc, "Failure
/// behavior"): the coordinator's own slice reader, and the counters the
/// sequence reports through. Keep a clone of
/// [`SliceFallback::counters`] to read the query's figures afterwards.
pub fn distributed_samples_plan(
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    max_samples: usize,
    limit: Option<usize>,
    accounting: QueryAccounting,
    max_bytes_scanned: ByteLimit,
    fallback: SliceFallback,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DistributedScanExec::new(
        endpoints,
        client,
        limit,
        accounting,
        max_bytes_scanned,
        fallback,
    )?);
    let scan_schema = scan.schema();

    let asc = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let merge_exprs = ["series_id", "ts"]
        .into_iter()
        .map(|name| Ok(PhysicalSortExpr::new(col(name, &scan_schema)?, asc)))
        .collect::<DFResult<Vec<_>>>()?;
    let ordering = LexOrdering::new(merge_exprs)
        .ok_or_else(|| DataFusionError::Internal("empty merge ordering".into()))?;
    let merge: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(ordering, scan));

    let dedup: Arc<dyn ExecutionPlan> = Arc::new(RsegDedupExec::new(merge, max_samples)?);

    match limit {
        Some(fetch) => Ok(Arc::new(GlobalLimitExec::new(dedup, 0, Some(fetch)))),
        None => Ok(dedup),
    }
}

/// Partition one resolved, pinned snapshot into per-worker slice tickets, or
/// return `None` when the query must run whole-set (local) instead.
///
/// This is the *minting* half of ADR-0071's SQL lane and the sole producer of
/// multi-slice tickets. It is deliberately NOT reachable from
/// `get_flight_info_statement`: an external Flight SQL client always receives a
/// single endpoint (the two-RPC contract makes N endpoints partitions of ONE
/// result set, which a scan fan-out is not). The coordinator service is
/// the intended caller -- it mints slice tickets here, hands them to
/// [`DistributedScanExec`] through a [`WorkerSliceClient`], and redeems each one
/// against a worker's `do_get` slice-fragment path.
///
/// Every returned ticket pins the SAME snapshot the coordinator resolved: the
/// `k`-th ticket carries slice `k`'s segments, `slice_index = k`, and a common
/// `slice_count`, while `tenant`, `min_commit_tokens`, `now_ns`, and
/// `deadline_ns` are copied verbatim from `template`. `statement` is minted
/// empty: the fragment path never plans or validates SQL, so a slice ticket
/// carries no statement text (see the field comment below). That is what makes
/// the union of slices exactly the whole-set scan, and what lets a worker
/// reject a slice ticket that outlived the snapshot's protection horizon with
/// the same deadline check the whole-set path uses.
///
/// Returns `None` (run local) when distribution is not worthwhile: the cost
/// gate says no ([`should_distribute`]), no workers are advertised, or the
/// snapshot partitions into a single slice (nothing to fan out).
pub fn plan_distributed_slices(
    snapshot: &Snapshot,
    estimate: &CostEstimate,
    config: &DistributedFlightConfig,
    template: &FlightTicket,
) -> Option<Vec<WorkerSlice>> {
    if !should_distribute(&config.thresholds, estimate) {
        return None;
    }
    let workers = config.workers.endpoints();
    if workers.is_empty() {
        return None;
    }
    let slices = partition_snapshot(snapshot, config.thresholds.max_parallel_slices);
    if slices.len() <= 1 {
        return None;
    }
    let count = slices.len() as u32;
    Some(
        slices
            .into_iter()
            .enumerate()
            .map(|(k, slice)| {
                let segments = slice
                    .segments
                    .iter()
                    .map(SegmentPin::from_segment_ref)
                    .collect();
                let location = workers[k % workers.len()].clone();
                let ticket = FlightTicket {
                    tenant: template.tenant,
                    // A slice ticket carries no SQL text: the fragment path
                    // never plans or validates a statement (the worker serves
                    // the raw internal-schema scan), and clearing it here is
                    // what makes that skip sound. The template's statement
                    // stays on the coordinator's own whole-set ticket only.
                    statement: String::new(),
                    segments,
                    min_commit_tokens: template.min_commit_tokens.clone(),
                    now_ns: template.now_ns,
                    deadline_ns: template.deadline_ns,
                    slice_index: k as u32,
                    slice_count: count,
                    // Copied verbatim from the coordinator's own resolve, same
                    // as every other template-derived field above: a worker
                    // never independently resolves a snapshot, so this is the
                    // only way its slice excludes what the coordinator saw
                    // pending (ADR-0064 decision 3).
                    pending_erasure: template.pending_erasure.clone(),
                    // A slice fetch serves the raw internal-schema samples scan
                    // (metrics only, no statement planning), so declared typed
                    // attribute columns never apply; copied from the template
                    // (empty for a metrics query) for uniformity, ADR-0090.
                    declared_columns: template.declared_columns.clone(),
                };
                WorkerSlice { location, ticket }
            })
            .collect(),
    )
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod schema_validation_tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};

    /// The exact internal scan schema passes.
    #[test]
    fn matching_schema_is_accepted() {
        let expected = internal_schema();
        validate_worker_schema(expected.as_ref(), expected.as_ref())
            .expect("the internal schema matches itself");
    }

    /// A worker that drops a provenance column (wrong field count) is rejected
    /// with a typed error, not trusted into the `(series_id, ts)` merge.
    #[test]
    fn missing_column_is_rejected() {
        let expected = internal_schema();
        let mut fields: Vec<Field> = expected
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.pop();
        let actual = Schema::new(fields);
        let err = validate_worker_schema(&actual, expected.as_ref())
            .expect_err("a short schema must be rejected");
        assert!(matches!(err, DataFusionError::Internal(_)));
    }

    /// A worker that keeps the column count but changes a column's type (here
    /// `value` Float64 -> Int64) is rejected.
    #[test]
    fn wrong_column_type_is_rejected() {
        let expected = internal_schema();
        let fields: Vec<Field> = expected
            .fields()
            .iter()
            .map(|f| {
                if f.name() == "value" {
                    Field::new(f.name(), DataType::Int64, f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let actual = Schema::new(fields);
        validate_worker_schema(&actual, expected.as_ref())
            .expect_err("a type-mismatched schema must be rejected");
    }

    /// Incidental schema-level metadata differences do not trip the check: only
    /// column shape matters.
    #[test]
    fn metadata_only_difference_is_accepted() {
        let expected = internal_schema();
        let fields: Vec<Field> = expected
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let actual = Schema::new(fields).with_metadata(
            [("origin".to_string(), "worker".to_string())]
                .into_iter()
                .collect(),
        );
        validate_worker_schema(&actual, expected.as_ref())
            .expect("metadata-only differences are ignored");
    }
}

//! Flight SQL vs JSON result-egress bench (GitHub issue #154, Phase C).
//!
//! Answers one question the arrow-datafusion plan's Phase C bench criteria
//! asks: for the same query over the same dataset, what does result egress
//! cost over Flight SQL (Arrow IPC streamed through `DoGet`) versus the
//! existing `POST /api/v1/sql` JSON encoding? The deliverable is
//! bytes-on-wire and wall time, normalized per million samples returned.
//!
//! Both paths run in-process against the same published segments and the same
//! `SqlExecutor`, so the numbers isolate the *encoding* choice, not transport
//! or storage:
//!
//!   * Flight SQL: the real [`RavelFlightSqlService`] answers `GetFlightInfo`
//!     (pinning a snapshot into a ticket) and then `DoGet`. Bytes-on-wire is
//!     the sum of `FlightData::encoded_len()` over the streamed messages --
//!     the exact protobuf payload the gRPC layer puts on the socket, Arrow
//!     IPC body plus Flight framing. Wall time covers the whole
//!     GetFlightInfo + DoGet + drain round, which re-runs the query pipeline
//!     each iteration.
//!   * JSON: `SqlExecutor::execute` produces the same result, then
//!     `QueryOutput::to_json` plus `serde_json::to_vec` produce the response
//!     body `POST /api/v1/sql` returns to a JSON client. Bytes-on-wire is
//!     that body's length; wall time covers execute + encode.
//!
//! Both wall times include one query execution, so the delta between them is
//! the encoding cost and the totals are the full server-side egress cost.
//!
//! Report-only: never changes library behavior. Gated on the `flight-egress`
//! feature so the default build never compiles ravel-sql/datafusion/
//! arrow-flight or this bin.
//!
//! Run: `cargo run -p ravel-bench --release --features flight-egress \
//!   --bin flight_sql_egress`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{Any, CommandStatementQuery, TicketStatementQuery};
use arrow_flight::{FlightData, FlightDescriptor, Ticket};
use clap::Parser;
use futures::StreamExt;
use prost::Message as _;
use serde::Serialize;

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{
    LogSegmentFetcher, QueryAdmissionController, QueryConcurrencyLimit, SegmentFetcher,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{
    FlightAuth, FlightClock, FlightSqlConfig, RavelFlightSqlService, SqlConfig, SqlExecutor,
    SqlRequest,
};
use ravel_types::accounting::NoopQueryCostRecorder;
use ravel_types::{CommitToken, Signal, TenantHash, TenantId, TimeRange};
use tonic::Request;
use tonic::metadata::{MetadataMap, MetadataValue};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Frozen clock. Small on purpose: `Catalog::resolve` issues one LIST per
/// (shard, ingest-hour) across the window, so a wall-clock value would fan out
/// to hundreds of thousands of LISTs. The published data lives in ingest-hour
/// bucket 0 with event timestamps a few milliseconds after the epoch.
const NOW_NS: i64 = 4 * NS_PER_HOUR;
/// The query both paths egress. Selecting ts + value is the canonical
/// telemetry read; ordering by series_id then ts fixes the row order so the
/// two encodings describe identical result rows.
const QUERY: &str = "SELECT ts, value FROM samples ORDER BY series_id, ts";
const TENANT: &str = "bench-tenant";
const TOKEN: &str = "bench-token";
const START_KEY: &str = "x-ravel-start";
const END_KEY: &str = "x-ravel-end";

#[derive(Parser, Debug)]
#[command(about = "Flight SQL (Arrow IPC via DoGet) vs POST /api/v1/sql JSON result egress")]
struct Args {
    /// Distinct series in the dataset.
    #[arg(long, default_value_t = 2_000)]
    series: usize,
    /// Samples per series. Total returned rows are series * samples.
    #[arg(long, default_value_t = 500)]
    samples_per_series: usize,
    /// Timed iterations per path; the fastest is reported (bytes are
    /// deterministic, so only wall time varies across iterations).
    #[arg(long, default_value_t = 5)]
    iters: usize,
}

/// A tenant resolver that maps the single bench token to the bench tenant and
/// carries no min-commit tokens (the dataset is already visible under the
/// frozen clock).
struct BenchAuth {
    tenant: TenantHash,
}

impl FlightAuth for BenchAuth {
    fn tenant(&self, _metadata: &MetadataMap) -> Result<TenantHash, tonic::Status> {
        Ok(self.tenant)
    }

    fn min_commit_tokens(
        &self,
        _metadata: &MetadataMap,
    ) -> Result<Vec<CommitToken>, tonic::Status> {
        Ok(Vec::new())
    }
}

/// A clock frozen at [`NOW_NS`], matching the request `now_ns` the JSON path
/// uses, so both paths resolve the same snapshot.
struct BenchClock;

impl FlightClock for BenchClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

#[derive(Debug, Serialize)]
struct PathResult {
    bytes_on_wire: u64,
    wall_ms: f64,
    bytes_per_million_samples: f64,
    ms_per_million_samples: f64,
}

impl PathResult {
    fn new(bytes: u64, wall: Duration, samples: u64) -> Self {
        let per = samples as f64 / 1_000_000.0;
        PathResult {
            bytes_on_wire: bytes,
            wall_ms: wall.as_secs_f64() * 1e3,
            bytes_per_million_samples: bytes as f64 / per,
            ms_per_million_samples: wall.as_secs_f64() * 1e3 / per,
        }
    }
}

#[derive(Debug, Serialize)]
struct Report {
    query: String,
    series: usize,
    samples_per_series: usize,
    total_samples: u64,
    iters: usize,
    flight_sql: PathResult,
    json: PathResult,
    /// JSON bytes divided by Flight SQL bytes; how much larger the JSON body
    /// is on the wire for the same rows.
    json_over_flight_bytes: f64,
    /// JSON wall time divided by Flight SQL wall time.
    json_over_flight_wall: f64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let report = run(&args).await;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

async fn run(args: &Args) -> Report {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT.to_string());

    let total_samples = publish_dataset(store.as_ref(), &tenant, args).await;

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = Arc::new(SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    ));
    let service = RavelFlightSqlService::new(
        Arc::clone(&executor),
        Arc::new(BenchAuth {
            tenant: tenant.hash(),
        }),
        Arc::new(BenchClock),
        FlightSqlConfig::default(),
        Arc::clone(&store),
        // This binary measures egress bytes, not process metrics. There is no
        // `/metrics` route here to fold a cost into, and folding one would put
        // the benchmark's own synthetic queries into the same counters a real
        // deployment reports.
        Arc::new(NoopQueryCostRecorder),
        // This benchmark measures egress bytes with a single warm run plus
        // timed iterations run sequentially (see below), not fleet
        // concurrency, so it opts out of the ceiling the same way it opts
        // out of cost recording above.
        Arc::new(QueryAdmissionController::new(
            QueryConcurrencyLimit::Unlimited,
        )),
    );

    // One warm run per path establishes the deterministic byte counts and
    // primes any lazy initialization before the timed iterations.
    let flight_bytes = flight_egress_bytes(&service).await;
    let json_bytes = json_egress_bytes(&executor, tenant.hash()).await;
    assert!(
        flight_bytes > 0 && json_bytes > 0,
        "both paths produced rows"
    );

    let mut flight_wall = Duration::MAX;
    let mut json_wall = Duration::MAX;
    for _ in 0..args.iters.max(1) {
        let start = Instant::now();
        let bytes = flight_egress_bytes(&service).await;
        flight_wall = flight_wall.min(start.elapsed());
        assert_eq!(bytes, flight_bytes, "flight egress is deterministic");

        let start = Instant::now();
        let bytes = json_egress_bytes(&executor, tenant.hash()).await;
        json_wall = json_wall.min(start.elapsed());
        assert_eq!(bytes, json_bytes, "json egress is deterministic");
    }

    let flight = PathResult::new(flight_bytes, flight_wall, total_samples);
    let json = PathResult::new(json_bytes, json_wall, total_samples);
    Report {
        query: QUERY.to_string(),
        series: args.series,
        samples_per_series: args.samples_per_series,
        total_samples,
        iters: args.iters.max(1),
        json_over_flight_bytes: json.bytes_on_wire as f64 / flight.bytes_on_wire as f64,
        json_over_flight_wall: json.wall_ms / flight.wall_ms,
        flight_sql: flight,
        json,
    }
}

/// Write every generated series into one RSEG object and publish its commit
/// record, returning the total sample count the query will return.
async fn publish_dataset(store: &dyn ObjectStoreBackend, tenant: &TenantId, args: &Args) -> u64 {
    let workload = WorkloadConfig {
        tenant: TENANT.to_string(),
        series_count: args.series,
        samples_per_series: args.samples_per_series,
        // Keep every event timestamp a few milliseconds after the epoch so it
        // lands inside the [0, NOW_NS] window and ingest-hour bucket 0.
        start_ts_ns: 1_000,
        interval_ns: 1_000,
        cardinality: CardinalityProfile::many_small(args.series),
        ..WorkloadConfig::default()
    };
    let raw = generate_raw(&workload).expect("generate dataset");
    let total_samples: u64 = raw.iter().map(|(_, _, samples)| samples.len() as u64).sum();

    let series: Vec<SeriesInput> = raw
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect();

    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(154);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");

    total_samples
}

/// Drive `GetFlightInfo` then `DoGet` and sum the encoded length of every
/// `FlightData` message the stream carries: the exact bytes the gRPC layer
/// serializes for a `DoGet` response.
async fn flight_egress_bytes(service: &RavelFlightSqlService) -> u64 {
    let command = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };
    let info = service
        .get_flight_info_statement(command.clone(), windowed(FlightDescriptor::new_cmd(vec![])))
        .await
        .expect("flight info")
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .expect("one endpoint")
        .ticket
        .clone()
        .expect("a ticket");

    let statement = statement_ticket(&ticket);
    let mut stream = service
        .do_get_statement(statement, windowed(ticket))
        .await
        .expect("do get")
        .into_inner();

    let mut bytes = 0u64;
    while let Some(item) = stream.next().await {
        let data: FlightData = item.expect("flight data");
        bytes += data.encoded_len() as u64;
    }
    bytes
}

/// Execute the query and encode the JSON response body `POST /api/v1/sql`
/// returns, reporting its length.
async fn json_egress_bytes(executor: &SqlExecutor, tenant: TenantHash) -> u64 {
    let req = SqlRequest {
        sql: QUERY.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
    };
    let outcome = executor.execute(tenant, &req).await.expect("execute query");
    let json = outcome.output.to_json().expect("encode json");
    serde_json::to_vec(&json)
        .expect("serialize json body")
        .len() as u64
}

/// Re-read the `TicketStatementQuery` out of the opaque ticket bytes, which is
/// what arrow-flight's `do_get` dispatcher does before calling
/// `do_get_statement`.
fn statement_ticket(ticket: &Ticket) -> TicketStatementQuery {
    let any = Any::decode(&*ticket.ticket).expect("ticket is an Any");
    any.unpack::<TicketStatementQuery>()
        .expect("ticket decodes")
        .expect("ticket is a TicketStatementQuery")
}

/// Attach the bearer token and the event-time window the dataset lives in, the
/// way a Flight SQL client names the window `POST /api/v1/sql` takes in its
/// JSON body.
fn windowed<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {TOKEN}")).expect("ascii token"),
    );
    metadata.insert(
        START_KEY,
        MetadataValue::try_from("0").expect("ascii start"),
    );
    metadata.insert(
        END_KEY,
        MetadataValue::try_from(format!("{}", NOW_NS as f64 / 1e9)).expect("ascii end"),
    );
    request
}

fn print_human_table(report: &Report) {
    println!();
    println!(
        "Flight SQL egress vs JSON: {} series x {} samples = {} rows, best of {} iters",
        report.series, report.samples_per_series, report.total_samples, report.iters
    );
    println!(
        "{:<12} | {:>14} | {:>12} | {:>16} | {:>14}",
        "encoding", "bytes-on-wire", "wall (ms)", "bytes/Msample", "ms/Msample"
    );
    println!(
        "{:-<12}-+-{:-<14}-+-{:-<12}-+-{:-<16}-+-{:-<14}",
        "", "", "", "", ""
    );
    let row = |name: &str, r: &PathResult| {
        println!(
            "{:<12} | {:>14} | {:>12.3} | {:>16.0} | {:>14.3}",
            name, r.bytes_on_wire, r.wall_ms, r.bytes_per_million_samples, r.ms_per_million_samples
        );
    };
    row("flight-sql", &report.flight_sql);
    row("json", &report.json);
    println!();
    println!(
        "JSON is {:.2}x the bytes and {:.2}x the wall time of Flight SQL for these rows.",
        report.json_over_flight_bytes, report.json_over_flight_wall
    );
}

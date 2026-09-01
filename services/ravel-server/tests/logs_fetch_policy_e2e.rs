//! ADR-0996 task 996-5 reachability: `--logs-fetch-policy` selects the logs
//! read SHAPE of a running server, resolved through the server's own config
//! path.
//!
//! The whole point of this file is that it does not call
//! `ravel_query::resolve_logs_fetch`. It parses a real argv with clap, runs it
//! through `Cli::query_budgets` -> `QueryBudgets::apply_to_engine` ->
//! `ravel_server::query::build_sql_state` (the three hops `ravel_server::start`
//! uses, in that order), and then executes a real statement over real RLOG
//! objects on a `MemoryStore`. What is asserted is the accounting handle's
//! opens-by-shape counters, which only `PartitionCtx::record_open_shape` writes
//! -- the route that ran, not the value that was configured (the ADR-0904
//! 904-4 rule).
//!
//! Before this task's wiring the policy was inert: `apply_to_engine` copied the
//! raw ADR-0904 byte flags onto the `EngineConfig` and nothing anywhere called
//! the resolution, so a server started with `--logs-fetch-policy
//! request-minimal` still routed a projected scan down the ranged path. The
//! `request_minimal` assertions below are what that failure fails.
//!
//! # Why the fixture's objects are about a megabyte
//!
//! The route is decided by `LogSegmentFetcher::ranged_projection_pays`: an
//! object routes ranged when the bytes its projection SKIPS
//! (`object_size * (1 - projected_fraction)`) exceed the fetcher's effective
//! whole-object crossover. Under `byte-minimal` at default flags that crossover
//! is the 512 KiB `--logs-block-range-threshold`, and the projection here is
//! `ts` alone, which `ResolvedColumns::fraction_of` scores as 2 object columns
//! (`ts` plus the always-decoded `stream_ref`) out of the 10 fixed ones. So an
//! object must clear roughly 640 KiB before the ranged route is reachable at
//! all; [`byte_minimal_fixture_clears_the_ranged_route_precondition`] pins that
//! as a fixture property, so a fixture that shrinks below it fails as a fixture
//! bug rather than silently pinning nothing. The bodies are pseudo-random text
//! because the writer's compression would otherwise shrink the objects back
//! under the threshold.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_server::Cli;
use ravel_server::query::{build_catalog, build_sql_state};
use ravel_sql::SqlRequest;
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Frozen clock reading. Small on purpose: `Catalog::resolve` lists one prefix
/// per (shard, ingest-hour) across the window.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// RLOG objects in the fixture, and the SQL scan partition count the runs are
/// driven at (`--fetch-concurrency`). Equal, so the whole-segment fast path's
/// `relevant_segments >= target_partitions` conjunct holds: that fast path is
/// where both routes and the `record_open_shape` recording site live. This is
/// also the exact count both counters are pinned to.
const SEGMENTS: usize = 2;

/// Records per object, and body bytes per record: 320 x 4 KiB is about 1.3 MB
/// of incompressible body text per object, comfortably past the ~640 KiB the
/// ranged route needs (see the header).
const RECORDS_PER_SEGMENT: usize = 320;
const BODY_BYTES: usize = 4096;

/// The share of an object's columns the `SELECT ts` projection reads, as
/// `crates/ravel-sql/src/logs_scan.rs`'s `ResolvedColumns::fraction_of` scores
/// it: `ts` and the always-decoded `stream_ref`, of the 10 fixed object columns
/// (this tenant declares no typed attribute columns). Restated here so the
/// precondition test can check the fixture without executing a plan.
const NARROW_FRACTION: f64 = 2.0 / 10.0;

/// The compiled-in ADR-0107 crossover, which is also the effective one under
/// `byte-minimal` at default flags: `build_sql_state` sets the block-range
/// fetcher's whole-object crossover explicitly from the resolved threshold.
const BYTE_MINIMAL_CROSSOVER: u64 = ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD;

/// Pseudo-random printable filler, so the writer's compression cannot shrink an
/// object back under the crossover this fixture has to clear.
fn filler(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = seed;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.push(ALPHABET[(z & 63) as usize] as char);
    }
    out
}

fn log_record(seg: usize, index: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("checkout".to_string()),
    )];
    let ts = (seg * 1_000_000 + index) as i64 + 1;
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: filler((seg as u64) << 32 | index as u64, BODY_BYTES),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Publish one RLOG object plus its `Signal::Logs` commit record, exactly as
/// `ravel-ingest`'s log shard actor does. Returns the object's size, which the
/// precondition test scores the ranged route against.
async fn publish_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId, seg: usize) -> u64 {
    let tenant_hash = tenant.hash();
    let records: Vec<LogRecord> = (0..RECORDS_PER_SEGMENT)
        .map(|index| log_record(seg, index))
        .collect();
    let writer_id = Uuid::from_u128(4_000 + seg as u128);
    let writer_seq = seg as u64 + 1;
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq,
        },
    );
    for rec in &records {
        writer.push(rec.clone()).expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let object_size = bytes.len() as u64;
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let min_event_ts_ns = records.iter().map(|r| r.ts_ns).min().expect("records");
    let max_event_ts_ns = records.iter().map(|r| r.ts_ns).max().expect("records");

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size,
        content_hash,
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10 + seg as i64,
        ingest_hour_bucket: 0,
    })
    .expect("valid log commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
    object_size
}

/// What one run of the statement routed and returned.
struct Routed {
    whole_object_opens: u64,
    ranged_opens: u64,
    rows: usize,
    /// Every `ts` the run emitted, for the cross-policy row comparison: the
    /// policy selects a read path, never a result.
    timestamps: Vec<i64>,
    /// The object sizes the fixture published, so a precondition can be scored
    /// against the objects the run actually read.
    object_sizes: Vec<u64>,
    /// The `EngineConfig` the server's own resolution produced for this argv.
    engine: ravel_query::EngineConfig,
}

/// Publish the fixture, resolve `argv` the way `ravel_server::start` does, and
/// run the narrow-projection statement through the resulting SQL state.
async fn run(argv: &[&str]) -> Routed {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme");
    let mut object_sizes = Vec::with_capacity(SEGMENTS);
    for seg in 0..SEGMENTS {
        object_sizes.push(publish_segment(store.as_ref(), &tenant, seg).await);
    }

    // The three hops `start` uses, in order. `apply_to_engine` is where
    // `--logs-fetch-policy` is resolved into the byte quantities below.
    let cli = Cli::try_parse_from(argv).expect("flags parse");
    let budgets = cli.query_budgets().expect("budgets resolve");
    let engine = budgets
        .apply_to_engine(ravel_query::EngineConfig::default())
        .expect("engine config resolves");

    let catalog = build_catalog(
        Arc::clone(&store),
        1,
        cli.disable_cache,
        cli.cache_max_bytes,
        cli.cache_dir.clone(),
    )
    .expect("catalog");
    let state = build_sql_state(
        catalog,
        Arc::clone(&store),
        Arc::new(StaticBearerTokenResolver::new(HashMap::from([(
            "acme-token".to_string(),
            tenant.clone(),
        )]))),
        None,
        engine,
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        budgets.sql_parallel_final_aggregation,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
    )
    .expect("sql state");

    let outcome = state
        .executor
        .execute(
            tenant.hash(),
            &SqlRequest {
                // `ts` alone: the narrow projection the route hinges on. No
                // predicate, so the whole-segment fast path is not rejected for
                // a block predicate.
                sql: "SELECT ts FROM logs".to_string(),
                window: TimeRange {
                    start_ns: 0,
                    end_ns: NOW_NS,
                },
                min_tokens: Vec::new(),
                now_ns: NOW_NS,
                deadline: Duration::from_secs(120),
            },
        )
        .await
        .expect("statement executes");

    let json = outcome.output.to_json().expect("rows render");
    let timestamps: Vec<i64> = json["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|row| row[0].as_i64().expect("ts is an integer"))
        .collect();

    Routed {
        whole_object_opens: outcome.accounting.logs_whole_object_opens,
        ranged_opens: outcome.accounting.logs_ranged_opens,
        rows: outcome.output.num_rows(),
        timestamps,
        object_sizes,
        engine,
    }
}

/// Every run passes `--fetch-concurrency 2`, one partition per fixture segment
/// (see [`SEGMENTS`]), so the policy flag is the only variable between runs.
const BASE_ARGV: [&str; 3] = ["ravel-server", "--fetch-concurrency", "2"];

/// Fixture precondition: the objects are large enough that the ranged route is
/// REACHABLE at all under `byte-minimal`. Without this a shrunken fixture would
/// make every policy route whole-object and the differential below would pin
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn byte_minimal_fixture_clears_the_ranged_route_precondition() {
    let routed = run(&[
        BASE_ARGV.as_slice(),
        &["--logs-fetch-policy", "byte-minimal"],
    ]
    .concat())
    .await;
    for size in &routed.object_sizes {
        let skipped = *size as f64 * (1.0 - NARROW_FRACTION);
        assert!(
            skipped > BYTE_MINIMAL_CROSSOVER as f64,
            "fixture object of {size} bytes skips only {skipped} bytes under the narrow \
             projection, which does not clear the {BYTE_MINIMAL_CROSSOVER}-byte crossover: \
             the ranged route would be unreachable and this file would pin nothing"
        );
    }
}

/// The acceptance test (ADR-0996 task 996-5): a server configured with
/// `--logs-fetch-policy request-minimal` genuinely routes whole-object, and so
/// does the shipped default (`cost-based` at the reference profile), while
/// `byte-minimal` keeps today's ranged routing byte for byte. The counters
/// invert exactly, in both directions, against the fixture's segment count.
///
/// Prove-the-test: revert `QueryBudgets::apply_to_engine` to copying the raw
/// flags (`logs_block_range_threshold: self.logs_block_range_threshold
/// .unwrap_or(DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD)` and `logs_request_cost_bytes:
/// self.logs_request_cost_bytes.unwrap_or(
/// ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES)`), which is the pre-wiring body,
/// and the request-minimal run reports `(ranged, whole) = (2, 0)` against the
/// expected `(0, 2)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_policy_selects_the_logs_read_shape_end_to_end() {
    let request_minimal = run(&[
        BASE_ARGV.as_slice(),
        &["--logs-fetch-policy", "request-minimal"],
    ]
    .concat())
    .await;
    assert_eq!(
        (
            request_minimal.ranged_opens,
            request_minimal.whole_object_opens
        ),
        (0, SEGMENTS as u64),
        "--logs-fetch-policy request-minimal must route every projected object whole-object"
    );

    // The shipped default: no policy flag at all, cost-based at the reference
    // profile (transfer and retrieval free), which resolves to the same shape.
    let default_policy = run(&BASE_ARGV).await;
    assert_eq!(
        (
            default_policy.ranged_opens,
            default_policy.whole_object_opens
        ),
        (0, SEGMENTS as u64),
        "the shipped default (cost-based at the reference profile) must route whole-object too"
    );

    // byte-minimal keeps today's routing, and the resolved quantities are the
    // pre-wiring values exactly.
    let byte_minimal = run(&[
        BASE_ARGV.as_slice(),
        &["--logs-fetch-policy", "byte-minimal"],
    ]
    .concat())
    .await;
    assert_eq!(
        (byte_minimal.ranged_opens, byte_minimal.whole_object_opens),
        (SEGMENTS as u64, 0),
        "--logs-fetch-policy byte-minimal must keep the ADR-0107 ranged route"
    );
    assert_eq!(
        byte_minimal.engine.logs_block_range_threshold, BYTE_MINIMAL_CROSSOVER,
        "byte-minimal must resolve the routing threshold to the exact pre-wiring 512 KiB"
    );
    assert_eq!(
        byte_minimal.engine.logs_request_cost_bytes,
        ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES,
        "byte-minimal must resolve the request cost to the exact pre-wiring constant"
    );

    // The row-identity invariant (ADR-0996 decision 2): for any policy the
    // statement returns exactly the same rows; only counters may differ. A
    // counters-only differential would pass on a route that dropped rows.
    let expected_rows = SEGMENTS * RECORDS_PER_SEGMENT;
    assert_eq!(request_minimal.rows, expected_rows);
    assert_eq!(byte_minimal.rows, expected_rows);
    assert_eq!(default_policy.rows, expected_rows);
    let sorted = |mut ts: Vec<i64>| {
        ts.sort_unstable();
        ts
    };
    let whole = sorted(request_minimal.timestamps);
    assert_eq!(
        whole,
        sorted(byte_minimal.timestamps),
        "the two read shapes must return identical rows"
    );
    assert_eq!(whole, sorted(default_policy.timestamps));
}

/// The explicit-flag seam, end to end: an operator who already passes
/// `--logs-request-cost-bytes` keeps their routing when the policy default
/// changes under them, because the explicit byte flag wins over the policy's
/// derivation (ADR-0996 decision 2). At the ADR-0904 default value that is the
/// ranged route this fixture's objects take today.
///
/// Prove-the-test: pass the resolution `Some(...unwrap_or(default))` as its
/// explicit input, erasing the unset case, and the unset run below routes
/// ranged `(2, 0)` instead of the expected whole-object `(0, 2)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_request_cost_flag_keeps_its_deployment_on_the_ranged_route() {
    let explicit = run(&[
        BASE_ARGV.as_slice(),
        &["--logs-request-cost-bytes", "1887437"],
    ]
    .concat())
    .await;
    assert_eq!(
        (explicit.ranged_opens, explicit.whole_object_opens),
        (SEGMENTS as u64, 0),
        "an explicit --logs-request-cost-bytes must win over the default cost-based policy, \
         leaving an existing deployment's routing byte-identical"
    );
    assert_eq!(
        explicit.engine.logs_request_cost_bytes,
        ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES
    );
    assert_eq!(
        explicit.engine.logs_block_range_threshold, BYTE_MINIMAL_CROSSOVER,
        "a finite explicit rate leaves the routing threshold in force"
    );

    // The same server with the flag UNSET derives the rate from the policy and
    // routes whole-object: the two cases are distinguishable end to end, which
    // is what the Option-typed flag buys.
    let derived = run(&BASE_ARGV).await;
    assert_eq!(
        (derived.ranged_opens, derived.whole_object_opens),
        (0, SEGMENTS as u64)
    );
}

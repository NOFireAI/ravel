//! A spans `trace_id` lookup over `POST /api/v1/sql` probes each object's
//! KEY_IDX section (ADR-2707 decision 5, issue #2761) and reads every object
//! the catalog lists, whatever its shard: spans shard routing is deferred to
//! issue #3034 (ADR-2707, amendment for issue #2761).
//!
//! Each fixture tenant carries a shard-count change, and every object is
//! folded into a catalog part before the lookup. The expected reads of each
//! object are computed from its bytes with `ravel_rspan`'s ranged reader,
//! independently of the fetcher under test, and compared with what the store
//! actually served.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use clap::Parser;
use ravel_catalog::{AbsentPolicy, append_generation, validate_or_adopt};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::shard_for_span;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_rspan::footer::{DEFAULT_MAX_SECTION_UNCOMP, kind};
use ravel_rspan::{RspanRangeReader, SpanSegError, TraceKeyProbe};
use ravel_server::Cli;
use ravel_server::query::{build_catalog, build_sql_state};
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
/// Late enough that a fold seals hours 0 and 1, whose seal margin is the
/// one-hour flush lifetime plus twenty minutes.
const NOW_NS: i64 = 4 * NS_PER_HOUR;
/// Two shards from hour 0, three from hour 1.
const RESHARD_2_TO_3: &[(u32, u32)] = &[(2, 0), (3, 1)];
const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";
/// Single-span traces per object. Enough blocks that the SKIP_IDX section
/// and the footer past KEY_IDX exceed the fetcher's 64 KiB tail probe, so
/// every KEY_IDX read is a GET of its own.
const DECOYS: u64 = 2_400;
const BLOCK_RECORDS: usize = 4;

struct FixedClock(i64);
impl ravel_ingest::Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// A `MemoryStore` that records every GET's key and range.
struct RecordingStore {
    inner: MemoryStore,
    gets: Mutex<Vec<(String, GetRange)>>,
}

impl RecordingStore {
    /// The ranges GET'd on `key` since the last call, in issue order.
    fn take(&self, key: &str) -> Vec<GetRange> {
        let mut gets = self.gets.lock().expect("get log");
        let (mine, rest): (Vec<_>, Vec<_>) = gets.drain(..).partition(|(k, _)| k == key);
        *gets = rest;
        mine.into_iter().map(|(_, range)| range).collect()
    }

    fn clear(&self) {
        self.gets.lock().expect("get log").clear();
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets
            .lock()
            .expect("get log")
            .push((key.to_string(), range));
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A trace id satisfying `routes`, a condition on the write side's own shard
/// rule. Its first byte is low, so it sorts ahead of every decoy and its
/// block sits far from the object's tail.
fn trace_where(routes: impl Fn(&[u8; 16]) -> bool) -> [u8; 16] {
    (0u8..=u8::MAX)
        .map(|n| {
            let mut t = [0x40; 16];
            t[0] = 0x01;
            t[1] = n;
            t
        })
        .find(|t| routes(t))
        .expect("some trace id routes as asked")
}

/// A trace id that routes to shard `want_2` under 2 shards and `want_3` under
/// 3.
fn trace_routed(want_2: u32, want_3: u32) -> [u8; 16] {
    trace_where(|t| shard_for_span(t, 2) == want_2 && shard_for_span(t, 3) == want_3)
}

/// `trace` with byte 7 moved by `delta`: a different KEY_IDX key that sorts
/// right beside it, so an object holding both neighbours keeps a SKIP_IDX
/// block whose trace range covers `trace` without holding it.
fn neighbour(trace: &[u8; 16], delta: i8) -> [u8; 16] {
    let mut t = *trace;
    t[7] = t[7].wrapping_add_signed(delta);
    t
}

/// A uniformly spread trace id with its first byte at or above 0x80.
fn decoy(seed: u64) -> [u8; 16] {
    let mut t = [0u8; 16];
    t.copy_from_slice(&blake3::hash(&seed.to_le_bytes()).as_bytes()[..16]);
    t[0] |= 0x80;
    t
}

/// One object to publish: its shard, its ingest hour, and the named spans it
/// carries beside the decoys.
struct ObjectSpec {
    shard: u32,
    hour: u32,
    spans: Vec<([u8; 16], &'static str)>,
}

struct Published {
    key: String,
    bytes: Bytes,
}

/// Writes and commits one RSPAN object; `seq` keeps writer ids and decoys
/// distinct across objects.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    spec: &ObjectSpec,
) -> Published {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(2_761_000 + u128::from(seq));
    let mut writer = ravel_rspan::RspanWriter::new(
        ravel_rspan::RspanConfig {
            block_target_records: BLOCK_RECORDS,
            ..ravel_rspan::RspanConfig::default()
        },
        ravel_rspan::ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: spec.shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let base = i64::from(spec.hour) * NS_PER_HOUR + 60 * NS_PER_SEC;
    let named = spec
        .spans
        .iter()
        .map(|(trace, name)| (*trace, name.to_string()));
    let decoys = (0..DECOYS).map(|i| (decoy(seq * 1_000_000 + i), "decoy".to_string()));
    let mut count = 0u64;
    let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
    for (i, (trace_id, name)) in named.chain(decoys).enumerate() {
        let start = base + i as i64;
        min_ts = min_ts.min(start);
        max_ts = max_ts.max(start + 1_000);
        count += 1;
        writer.push(ravel_rspan::SpanRecord {
            trace_id,
            span_id: (i as u64 + 1).to_le_bytes(),
            parent_span_id: None,
            name,
            start_ts_ns: start,
            end_ts_ns: start + 1_000,
            status_code: ravel_rspan::StatusCode::Ok,
            status_message: None,
            attrs: vec![("service.name".to_string(), "checkout".to_string())],
        });
    }
    let bytes = writer.finish().expect("finish rspan object");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: spec.shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: count,
        series_count: count,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        created_unix_ns: min_ts,
        ingest_hour_bucket: spec.hour,
    })
    .expect("valid span commit record");
    let key = keys::reconstruct_data_key(&rec).expect("data key");
    let bytes = Bytes::from(bytes);
    store
        .put(&key, bytes.clone(), PutOptions::default())
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
    Published { key, bytes }
}

/// The reads a trace lookup must issue on one object it keeps, per phase.
#[derive(Debug, Default)]
struct ExpectedReads {
    probe: Vec<GetRange>,
    key_index: Vec<GetRange>,
    scan: Vec<GetRange>,
    present: bool,
    /// The lowest section offset: every block lies before it.
    blocks_end: u64,
}

impl ExpectedReads {
    fn all(&self) -> Vec<GetRange> {
        let mut all = self.probe.clone();
        all.extend(self.key_index.iter().copied());
        all.extend(self.scan.iter().copied());
        all
    }
}

fn len(range: &GetRange) -> u64 {
    match *range {
        GetRange::Range(start, end) => end - start,
        other => panic!("expected a bounded range, got {other:?}"),
    }
}

/// The fetcher's ranged trace path over `bytes`, recomputed from the format
/// crate: the tail probe, SKIP_IDX unless the probe holds it, the KEY_IDX
/// reads `RspanRangeReader::probe_trace` asks for, and the trace's block run
/// when KEY_IDX says it is present. Also asserts the fixture geometry the
/// exact counts rest on: SKIP_IDX keeps a block for `trace`, so KEY_IDX is
/// probed, and no KEY_IDX or block read falls inside the tail probe, so each
/// is a GET.
fn expected_reads(bytes: &Bytes, trace: &[u8; 16]) -> ExpectedReads {
    let size = bytes.len() as u64;
    let probe_len = ravel_query::span_fetcher::DEFAULT_TRACE_PROBE_BYTES.min(size);
    let probe_start = size - probe_len;
    let footer = ravel_rspan::open(bytes).expect("footer");
    let skip = *footer.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let key_idx = *footer.section(kind::KEY_IDX).expect("KEY_IDX");
    assert!(
        key_idx.offset + key_idx.len <= probe_start,
        "fixture geometry: KEY_IDX [{}, {}) lies before the tail probe at {probe_start}",
        key_idx.offset,
        key_idx.offset + key_idx.len
    );
    let mut reads = ExpectedReads {
        probe: vec![GetRange::Range(probe_start, size)],
        blocks_end: footer
            .sections
            .iter()
            .map(|s| s.offset)
            .min()
            .expect("sections"),
        ..ExpectedReads::default()
    };
    if skip.offset < probe_start {
        reads
            .probe
            .push(GetRange::Range(skip.offset, skip.offset + skip.len));
    }
    let skip_raw =
        ravel_rspan::read_section(bytes, &skip, DEFAULT_MAX_SECTION_UNCOMP).expect("SKIP_IDX");
    let reader = RspanRangeReader::from_sections(&footer, &skip_raw).expect("range reader");
    assert!(
        !reader
            .skip_index()
            .candidate_blocks(Some(trace), i64::MIN, i64::MAX, None, None)
            .is_empty(),
        "fixture geometry: SKIP_IDX keeps a block for the trace"
    );
    let answer = reader
        .probe_trace(trace, |r: std::ops::Range<u64>| {
            reads.key_index.push(GetRange::Range(r.start, r.end));
            Ok::<_, SpanSegError>(bytes.slice(r.start as usize..r.end as usize))
        })
        .expect("probe");
    match answer {
        TraceKeyProbe::Absent => {}
        TraceKeyProbe::Present(_) => {
            reads.present = true;
            let run = reader
                .trace_block_span(trace)
                .expect("block span")
                .expect("a present trace has a block run");
            assert!(
                run.end() <= probe_start,
                "fixture geometry: run before probe"
            );
            reads.scan.push(GetRange::Range(run.start(), run.end()));
        }
        TraceKeyProbe::Declined => panic!("KEY_IDX declined the fixture trace"),
    }
    reads
}

/// Seeds the spans shard generations `generations`, each a `(shard_count,
/// activation_hour)`, publishes `specs`, folds them all at `now_ns`, and
/// serves the SQL router over the recording store with its clock at `now_ns`.
async fn serve(
    generations: &[(u32, u32)],
    now_ns: i64,
    specs: &[ObjectSpec],
) -> (Arc<RecordingStore>, Router, Vec<Published>) {
    let recorder = Arc::new(RecordingStore {
        inner: MemoryStore::new(),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let tenant = TenantId::new(TENANT);
    let (&(first_count, _), later) = generations.split_first().expect("a generation");
    validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        Signal::Spans,
        first_count,
        0,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("generation 0");
    for &(count, activation_hour) in later {
        append_generation(
            store.as_ref(),
            &tenant.hash(),
            Signal::Spans,
            count,
            activation_hour,
            0,
        )
        .await
        .expect("append generation");
    }

    let mut published = Vec::new();
    for (seq, spec) in specs.iter().enumerate() {
        published.push(publish_object(store.as_ref(), &tenant, seq as u64, spec).await);
    }

    let cli = Cli::try_parse_from(["ravel-server"]).expect("default flags parse");
    let catalog = || {
        build_catalog(
            Arc::clone(&store),
            first_count,
            true,
            ravel_server::config::DEFAULT_CACHE_MAX_BYTES,
            None,
            cli.catalog_resolve_concurrency,
            None,
            cli.resolve_flush_cadence()
                .expect("flush cadence resolves")
                .max_flush_delay,
        )
        .expect("catalog")
    };
    let report = catalog()
        .fold(
            &tenant.hash(),
            Signal::Spans,
            Uuid::from_u128(1),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.entry_count,
        specs.len() as u64,
        "every object folds into a part: {report:?}"
    );

    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant);
    let mut sql_state = build_sql_state(
        catalog(),
        Arc::clone(&store),
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        None,
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    )
    .expect("sql state");
    sql_state.clock = Arc::new(FixedClock(now_ns));
    recorder.clear();
    (recorder, ravel_server::sql::router(sql_state), published)
}

async fn lookup(app: &Router, now_ns: i64, trace: &[u8; 16]) -> Value {
    let sql = format!(
        "SELECT name FROM spans WHERE trace_id = '{}' ORDER BY name",
        hex(trace)
    );
    let payload = serde_json::json!({
        "query": sql,
        "start": 0.0,
        "end": now_ns as f64 / NS_PER_SEC as f64,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).expect("body is JSON");
    assert_eq!(status, StatusCode::OK, "lookup failed: {value}");
    value
}

/// Field `field` of the `stats.phases` entry for phase `name`.
fn phase(value: &Value, name: &str, field: &str) -> u64 {
    value["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|phase| phase["phase"] == name)
        .unwrap_or_else(|| panic!("no {name} phase in {value}"))[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{name}.{field} is a u64 in {value}"))
}

/// Field `field` of `stats.pruning`, which must be present.
fn pruning(value: &Value, field: &str) -> u64 {
    value["stats"]["pruning"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.pruning.{field} is a u64 in {value}"))
}

/// Whether every read in `got` lies at or past `blocks_end`, so none of them
/// touched a block.
fn reads_no_block(got: &[GetRange], blocks_end: u64) -> bool {
    got.iter().all(|range| match *range {
        GetRange::Range(start, _) => start >= blocks_end,
        _ => false,
    })
}

/// The trace's two neighbours: an object holding them keeps a SKIP_IDX block
/// whose trace range covers the trace, so the lookup probes its KEY_IDX.
fn bracket(trace: &[u8; 16]) -> Vec<([u8; 16], &'static str)> {
    vec![
        (neighbour(trace, -1), "left"),
        (neighbour(trace, 1), "right"),
    ]
}

/// Two shards and two hours of folded objects across a 2-to-3 reshard, the
/// trace in exactly one. Every object is read as far as KEY_IDX: the three
/// that do not hold the trace keep a SKIP_IDX block for it and KEY_IDX says
/// the trace is absent, so they read no block, and only the holder reads the
/// trace's run.
///
/// FLIP: `blocks.is_none() && !candidate_idx.is_empty()` to `false` in
/// `ranged_trace_scan` (no probe) issues no KEY_IDX read and reads a block
/// run from every object.
#[tokio::test]
async fn trace_lookup_on_a_folded_tenant_reads_index_buckets_and_the_trace_blocks_only() {
    let trace = trace_routed(1, 0);
    let specs = [
        ObjectSpec {
            shard: 0,
            hour: 0,
            spans: bracket(&trace),
        },
        ObjectSpec {
            shard: 1,
            hour: 0,
            spans: vec![(trace, "t-0"), (trace, "t-1"), (trace, "t-2")],
        },
        ObjectSpec {
            shard: 0,
            hour: 1,
            spans: bracket(&trace),
        },
        ObjectSpec {
            shard: 1,
            hour: 1,
            spans: bracket(&trace),
        },
    ];
    let (recorder, app, objects) = serve(RESHARD_2_TO_3, NOW_NS, &specs).await;
    let want: Vec<ExpectedReads> = objects
        .iter()
        .map(|o| expected_reads(&o.bytes, &trace))
        .collect();
    assert_eq!(
        want.iter().map(|r| r.present).collect::<Vec<_>>(),
        vec![false, true, false, false],
        "fixture: KEY_IDX holds the trace in the hour-0 shard-1 object only"
    );

    let value = lookup(&app, NOW_NS, &trace).await;
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["t-0"], ["t-1"], ["t-2"]]),
        "{value}"
    );

    for (object, want) in objects.iter().zip(&want) {
        let got = recorder.take(&object.key);
        assert_eq!(
            got,
            want.all(),
            "{}: the tail probe, SKIP_IDX, the KEY_IDX reads, and the trace's run only",
            object.key
        );
        if !want.present {
            assert!(
                !want.key_index.is_empty() && got.iter().any(|r| want.key_index.contains(r)),
                "{}: KEY_IDX was read",
                object.key
            );
            assert!(
                reads_no_block(&got, want.blocks_end),
                "{}: no block was read: {got:?}",
                object.key
            );
        }
    }

    assert_eq!(pruning(&value, "segments"), 4, "{value}");
    assert_eq!(pruning(&value, "segmentsPrunedByKeyIndex"), 3, "{value}");
    // A writer-sorted object's SKIP_IDX already keeps exactly the trace's
    // blocks, so KEY_IDX has none left to prune.
    assert_eq!(pruning(&value, "blocksPrunedByKeyIndex"), 0, "{value}");

    let key_reads: Vec<GetRange> = want.iter().flat_map(|r| r.key_index.clone()).collect();
    assert!(key_reads.len() >= 4, "each object reads KEY_IDX");
    assert_eq!(
        phase(&value, "keyIndex", "s3GetRequests"),
        key_reads.len() as u64,
        "{value}"
    );
    assert_eq!(
        phase(&value, "keyIndex", "s3GetWireBytes"),
        key_reads.iter().map(len).sum::<u64>(),
        "{value}"
    );
    let scan: Vec<GetRange> = want.iter().flat_map(|r| r.scan.clone()).collect();
    assert_eq!(scan.len(), 1, "one block run, in the trace's object");
    assert_eq!(phase(&value, "scan", "s3GetRequests"), 1, "{value}");
    assert_eq!(
        phase(&value, "scan", "s3GetWireBytes"),
        len(&scan[0]),
        "{value}"
    );
    let probe: Vec<GetRange> = want.iter().flat_map(|r| r.probe.clone()).collect();
    assert_eq!(
        phase(&value, "probe", "s3GetRequests"),
        probe.len() as u64,
        "{value}"
    );
    assert_eq!(
        phase(&value, "probe", "s3GetWireBytes"),
        probe.iter().map(len).sum::<u64>(),
        "{value}"
    );
}

/// A trace written under both shard counts, on shard 1 in hour 0 (2 shards)
/// and shard 2 in hour 1 (3 shards), is returned whole, and the two objects
/// that do not hold it are dropped by KEY_IDX after the probe.
///
/// FLIP: a lookup that drops each object whose shard the trace does not route
/// to under one fixed count, 2 or 3, loses one hour's spans.
#[tokio::test]
async fn a_trace_straddling_a_shard_count_change_is_fully_returned() {
    let trace = trace_routed(1, 2);
    let specs = [
        ObjectSpec {
            shard: 0,
            hour: 0,
            spans: bracket(&trace),
        },
        ObjectSpec {
            shard: 1,
            hour: 0,
            spans: vec![(trace, "gen0-a"), (trace, "gen0-b")],
        },
        ObjectSpec {
            shard: 0,
            hour: 1,
            spans: bracket(&trace),
        },
        ObjectSpec {
            shard: 2,
            hour: 1,
            spans: vec![(trace, "gen1-a")],
        },
    ];
    let (recorder, app, objects) = serve(RESHARD_2_TO_3, NOW_NS, &specs).await;

    let value = lookup(&app, NOW_NS, &trace).await;
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["gen0-a"], ["gen0-b"], ["gen1-a"]]),
        "{value}"
    );
    assert_eq!(pruning(&value, "segmentsPrunedByKeyIndex"), 2, "{value}");
    for object in &objects {
        assert_eq!(
            recorder.take(&object.key),
            expected_reads(&object.bytes, &trace).all(),
            "{}: every listed object is probed",
            object.key
        );
    }
    assert_eq!(phase(&value, "scan", "s3GetRequests"), 2, "{value}");
}

/// An object written in place on a shard below the hour's scan count, under a
/// shard count that hour's scan set no longer admits, is listed and read.
/// Generations: 4 shards from hour 0, 3 from hour 100. Hour 103 is past the
/// retiring count's slack, so it scans 3 shards, and an object on shard 2
/// holding a trace that the 4-shard rule puts on shard 2 and the 3-shard rule
/// does not is what ingest's in-place span paths can write there.
///
/// FLIP: a lookup that drops an object when no count in its hour's scan set
/// routes the trace to its shard returns no rows.
#[tokio::test]
async fn a_trace_written_in_place_under_a_retired_shard_count_is_returned() {
    const HOUR: u32 = 103;
    let generations: &[(u32, u32)] = &[(4, 0), (3, 100)];
    let history: Vec<ravel_catalog::ShardGeneration> = generations
        .iter()
        .enumerate()
        .map(
            |(generation, &(shard_count, activation_hour))| ravel_catalog::ShardGeneration {
                generation: generation as u32,
                shard_count,
                activation_hour,
                appended_unix_ns: 0,
            },
        )
        .collect();
    assert_eq!(
        ravel_catalog::scan_count(&history, HOUR, ravel_catalog::DEFAULT_SCAN_SLACK_HOURS),
        3,
        "fixture: hour {HOUR} scans 3 shards"
    );
    let trace = trace_where(|t| shard_for_span(t, 4) == 2 && shard_for_span(t, 3) != 2);
    let now_ns = i64::from(HOUR + 3) * NS_PER_HOUR;
    let specs = [ObjectSpec {
        shard: 2,
        hour: HOUR,
        spans: vec![(trace, "in-place-a"), (trace, "in-place-b")],
    }];
    let (recorder, app, objects) = serve(generations, now_ns, &specs).await;

    let value = lookup(&app, now_ns, &trace).await;
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["in-place-a"], ["in-place-b"]]),
        "{value}"
    );
    assert_eq!(pruning(&value, "segments"), 1, "{value}");
    assert_eq!(pruning(&value, "segmentsPrunedByKeyIndex"), 0, "{value}");
    assert_eq!(
        recorder.take(&objects[0].key),
        expected_reads(&objects[0].bytes, &trace).all(),
        "the object is probed and its trace run read"
    );
}

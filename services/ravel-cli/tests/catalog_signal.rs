//! `ravel-cli catalog fold/inspect/verify --signal` (issue #718): the three
//! commands act on the signal named by the flag, not on metrics only.
//!
//! Driven in-process against one shared `MemoryStore`, the same pattern (and
//! for the same reason) as `tests/catalog.rs`: a subprocess-per-invocation of
//! the binary gets its own empty in-memory store, so a
//! publish -> fold -> resolve scenario cannot be built that way without a
//! persistent S3/MinIO backend, unavailable here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{
    LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Default seal margins sum to 1h20m, but hour-bucket quantization means
/// anything under ~2h20m can land on the wrong side of
/// `sealed_watermark_hour`'s boundary depending on which minute of the hour
/// the test runs in. 3h clears that with room to spare (same constant as
/// `tests/catalog.rs`).
const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;

/// How many RLOG objects (and therefore commit records) the logs tenant gets.
/// Small, and split across two shards so the fold lists more than one commit
/// bucket.
const PUBLISHED_OBJECTS: u64 = 6;

const SHARD_COUNT: u32 = 2;

/// Passthrough steps per counting sequence. A `Sequence`'s progress is clamped
/// to its step count, so this must exceed the largest count any case here can
/// reach (the metrics-flipped red case reads every one of
/// [`PUBLISHED_OBJECTS`] commit records) for the assertions to be exact rather
/// than saturated.
const COUNT_STEPS: usize = 256;

fn now_ns() -> i64 {
    ravel_cli::now_ns().expect("system clock readable")
}

fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// Publish one real RLOG object plus its L0 commit record for the logs signal.
async fn publish_rlog(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    seq: u64,
    observed_ts_ns: i64,
) {
    let tenant_hash = TenantId::new(tenant).hash();
    let writer_id = Uuid::new_v4();
    let epoch = 1u64;

    let stream_attrs = stream_attrs_bytes(
        &[("service.name".into(), AttrValue::Str("svc".into()))],
        "scope",
        "1.0",
        &[],
    );
    let log = LogRecord {
        stream_id: LogStreamId([u8::try_from(seq % 256).expect("fits u8"); 16]),
        stream_attrs,
        ts_ns: observed_ts_ns - 1_000,
        observed_ts_ns,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("log-{shard}-{seq}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    };
    let identity = ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.into_bytes(),
        writer_epoch: epoch,
        writer_seq: seq,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    writer.push(log).expect("push log record");
    let object = Bytes::from(writer.finish().expect("finish rlog"));
    let content_hash = *blake3::hash(&object).as_bytes();

    let ingest_hour_bucket = u32::try_from(observed_ts_ns / NS_PER_HOUR).expect("fits u32");
    let commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: object.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: observed_ts_ns - 1_000,
        max_event_ts_ns: observed_ts_ns,
        min_ingest_ts_ns: observed_ts_ns,
        max_ingest_ts_ns: observed_ts_ns,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: observed_ts_ns,
        ingest_hour_bucket,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, object)
        .await
        .expect("put rlog data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// Seed a logs-only tenant whose every commit record sits in a sealed hour.
async fn seed_logs_tenant(store: &MemoryStore, tenant: &str) {
    let observed = now_ns() - SEALED_AGE_NS;
    for seq in 0..PUBLISHED_OBJECTS {
        let shard = u32::try_from(seq % u64::from(SHARD_COUNT)).expect("fits u32");
        publish_rlog(store, tenant, shard, seq + 1, observed).await;
    }
}

/// A `FaultStore` whose plan injects nothing and only counts: sequence 0
/// counts `get` calls under the tenant's logs catalog prefix (HEAD and
/// snapshot parts), sequence 1 counts `get` calls under its logs commit-record
/// prefix. The two patterns are disjoint, so each call lands in exactly one.
/// Counting through `FaultStore` rather than a bespoke wrapper keeps the same
/// store type every other failure-path test in this crate uses.
fn counting_store(
    inner: Arc<MemoryStore>,
    tenant_hash: &TenantHash,
) -> Arc<FaultStore<Arc<MemoryStore>>> {
    let steps = vec![SequenceStep::Passthrough; COUNT_STEPS];
    let plan = FaultPlan::empty()
        .with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(format!("t/{}/catalog/l/", tenant_hash.to_hex()))
                .with_steps(steps.clone()),
        )
        .with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(format!("t/{}/l/c/", tenant_hash.to_hex()))
                .with_steps(steps),
        );
    Arc::new(FaultStore::new(inner, plan))
}

const CATALOG_GETS: usize = 0;
const COMMIT_GETS: usize = 1;

/// The point of `--signal logs`: after a logs fold, a logs resolve over the
/// whole published window is served entirely from the snapshot. It GETs the
/// logs HEAD and each of its parts and reads no commit record at all, instead
/// of listing and GETting every one of them (issue #718: 8,425 GETs and 13-14s
/// cold on a 100M-row logs tenant, because the CLI only ever folded metrics).
///
/// Red demonstration: flip `FOLD_SIGNAL` below to `SignalArg::Metrics` (the
/// pre-#718 hardcoded value). Measured: `entry_count 0`, no logs HEAD, an
/// empty metrics HEAD written instead, and the logs resolve falls back to
/// listing with 6 commit-record GETs and 1 catalog GET instead of 0 and 2.
#[tokio::test]
async fn fold_signal_logs_folds_the_logs_snapshot_and_a_logs_resolve_reads_no_commit_record() {
    const FOLD_SIGNAL: SignalArg = SignalArg::Logs;

    let inner = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-logs";
    let tenant_hash = TenantId::new(tenant).hash();
    seed_logs_tenant(&inner, tenant).await;

    let (report, _printed) = catalog::fold(
        inner.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        SHARD_COUNT,
        FOLD_SIGNAL,
        None,
        now_ns(),
    )
    .await
    .expect("cli fold succeeds");

    assert_eq!(
        report.entry_count, PUBLISHED_OBJECTS,
        "a logs fold must cover every published logs object exactly; folding \
         metrics on this tenant covers none"
    );

    // The snapshot landed under the logs key, and the metrics key was never
    // written: the two are separate objects.
    let logs_head = head_key(&tenant_hash, Signal::Logs);
    let head_bytes = inner
        .get(&logs_head, GetRange::Full)
        .await
        .expect("the logs HEAD is the key the fold wrote")
        .data;
    assert!(
        inner
            .get(&head_key(&tenant_hash, Signal::Metrics), GetRange::Full)
            .await
            .is_err(),
        "a logs fold must not write the tenant's metrics HEAD"
    );

    let head = ravel_catalog::decode_head(&head_bytes).expect("decode logs HEAD");
    assert_eq!(
        head.parts.len(),
        1,
        "this tenant is far below snapshot_part_max_entries, so one part; the \
         expected GET count below is HEAD + this many parts"
    );

    // Now resolve the full published window for the logs signal through the
    // counting store.
    let counting = counting_store(inner.clone(), &tenant_hash);
    let catalog = ravel_catalog::Catalog::new(
        counting.clone() as Arc<dyn ObjectStoreBackend>,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD_COUNT,
            ..ravel_catalog::CatalogConfig::default()
        },
    )
    .expect("build catalog");

    let now = now_ns();
    let range = TimeRange {
        start_ns: now - SEALED_AGE_NS - NS_PER_HOUR,
        end_ns: now,
    };
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Logs, range, &[], now)
        .await
        .expect("resolve the logs signal");
    assert_eq!(
        snapshot.segments.len() as u64,
        PUBLISHED_OBJECTS,
        "the resolve must still see every published object"
    );

    assert_eq!(
        counting.sequence_progress(COMMIT_GETS),
        0,
        "a snapshot-served logs resolve GETs no commit record; folding metrics \
         instead leaves it GETting all {PUBLISHED_OBJECTS} of them"
    );
    // Exactly HEAD plus one GET per snapshot part: no re-read, no postings
    // object (a logs fold writes none, ADR-0033), nothing else under the
    // tenant's logs catalog prefix. Folding metrics instead makes this 1: the
    // absent-logs-HEAD probe, after which the resolve falls back to listing.
    assert_eq!(
        counting.sequence_progress(CATALOG_GETS),
        1 + head.parts.len() as u64,
        "the whole resolve cost is the logs HEAD plus its {} snapshot part(s)",
        head.parts.len()
    );
}

/// `catalog inspect --signal logs` decodes the logs HEAD and names the signal
/// with the word, not only the numeric proto value.
#[tokio::test]
async fn inspect_signal_logs_prints_the_signal_word() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-inspect-logs";
    seed_logs_tenant(&store, tenant).await;

    catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        SHARD_COUNT,
        SignalArg::Logs,
        None,
        now_ns(),
    )
    .await
    .expect("cli fold succeeds");

    let mut out = String::new();
    catalog::render_inspect(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        SignalArg::Logs,
        &mut out,
    )
    .await
    .expect("inspect renders the logs HEAD");

    assert!(
        out.contains("signal: logs"),
        "inspect must name the signal with the word, got:\n{out}"
    );
    assert!(
        out.contains("parts: 1"),
        "inspect must have decoded the logs HEAD the fold wrote, got:\n{out}"
    );
}

/// `catalog verify --signal logs` compares the logs snapshot against the logs
/// sealed commit history. Before #718 it always read the metrics HEAD, so on
/// this tenant it reported "nothing folded yet" no matter what the logs
/// snapshot looked like.
#[tokio::test]
async fn verify_signal_logs_checks_the_logs_snapshot() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-verify-logs";
    seed_logs_tenant(&store, tenant).await;

    catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        SHARD_COUNT,
        SignalArg::Logs,
        None,
        now_ns(),
    )
    .await
    .expect("cli fold succeeds");

    catalog::verify(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        SignalArg::Logs,
    )
    .await
    .expect("a freshly folded logs snapshot matches its sealed commit history");

    // A logs commit record published after the fold is sealed but unfolded, so
    // the logs snapshot now under-counts: verify must fail on the logs signal.
    publish_rlog(
        &store,
        tenant,
        0,
        PUBLISHED_OBJECTS + 1,
        now_ns() - SEALED_AGE_NS,
    )
    .await;
    let err = catalog::verify(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        SignalArg::Logs,
    )
    .await
    .expect_err("verify must fail when a sealed logs record is missing from the logs snapshot");
    assert!(
        err.to_string().contains("missing"),
        "unexpected error: {err}"
    );

    // The same tenant has no metrics snapshot at all, and verifying metrics
    // reports exactly that rather than the logs divergence above.
    catalog::verify(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        SignalArg::Metrics,
    )
    .await
    .expect("no metrics HEAD on a logs-only tenant is 'nothing to verify', not a divergence");
}

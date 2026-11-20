//! A logs erasure whose subject is named only in a resource or scope attribute
//! (a stream attribute) must stay erased once its request object is swept.
//!
//! Query-time exclusion matches the merged resource + scope + record view
//! (`ravel_query::erasure::retain_unerased_log_records`), so such a subject is
//! hidden while its `.dreq` is pending. The rewrite is what makes the erasure
//! permanent: once completion is written and the `.dreq` is swept, the
//! rewrite output is the only thing standing between the subject and a query.
//!
//! Drives the real library entry points in-process against one `MemoryStore`:
//! the OTLP ingest router, `erase submit`, the maintain rewrite and completion
//! check, and `export --signal logs`, which reads through the query path's
//! visibility rules. The `.dreq` is removed with a plain delete, which is the
//! store effect of the maintain tick's request sweep.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use ravel_cli::erase;
use ravel_cli::export;
use ravel_cli::load::{self, Mapping};
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_ingest::{Clock, IngestConfig, LogIngestRouter, PendingAckBudget, WriteMode};
use ravel_logseg::stream_attrs_bytes;
use ravel_maintain::erasure_rewrite::ErasureRewriteOutcome;
use ravel_maintain::scan::MaintainMemo;
use ravel_maintain::sweep::NoLeases;
use ravel_maintain::{
    Bucket, CompactorConfig, bucket_erasure_completion, erasure_rewrite_bucket,
    pending_erasure_requests,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TENANT: &str = "acme";
const BASE_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
const ONE_SEC_NS: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const TS_BODY_MAPPING: &str = "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n";

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

fn str_attrs(pairs: &[(&str, &str)]) -> Vec<(String, AttrValue)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), AttrValue::Str((*v).to_string())))
        .collect()
}

/// One OTLP-shaped record: `resource` and `scope_attrs` form its stream
/// identity, `record_attrs` are its own attributes.
fn record(
    resource: &[(&str, &str)],
    scope_attrs: &[(&str, &str)],
    record_attrs: &[(&str, &str)],
    ts_ns: i64,
    body: &str,
) -> NormalizedLogRecord {
    let res = str_attrs(resource);
    let scope = str_attrs(scope_attrs);
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope-lib", "1.0", &scope),
        stream_attrs: stream_attrs_bytes(&res, "scope-lib", "1.0", &scope).into(),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: str_attrs(record_attrs),
    }
}

fn mapping(text: &str) -> Mapping {
    load::parse_mapping(text).expect("valid mapping")
}

fn exported_bodies(path: &Path) -> Vec<String> {
    let file = std::fs::File::open(path).expect("open exported parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("build reader");
    let mut bodies = Vec::new();
    for batch in reader {
        let batch = batch.expect("read batch");
        let body = batch
            .column_by_name("body")
            .expect("body")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body is Utf8")
            .clone();
        bodies.extend((0..body.len()).map(|i| body.value(i).to_string()));
    }
    bodies.sort();
    bodies
}

async fn export_bodies(
    store: &Arc<dyn ObjectStoreBackend>,
    out: &Path,
    start_ns: i64,
    end_ns: i64,
    now_ns: i64,
) -> (Vec<String>, usize) {
    let report = export::export_logs(
        Arc::clone(store),
        StoreSelection::explicit(StoreKind::Memory),
        TENANT,
        start_ns,
        end_ns,
        &mapping(TS_BODY_MAPPING),
        out,
        1,
        None,
        now_ns,
        &export::BloomColumns::Default,
    )
    .await
    .expect("export succeeds");
    (exported_bodies(out), report.erasure_predicates)
}

async fn erase_user(store: &Arc<dyn ObjectStoreBackend>, subject: &str, id: u128, now_ns: i64) {
    erase::submit(
        Arc::clone(store),
        TENANT,
        SignalArg::Logs,
        vec![("user_id".to_string(), subject.to_string())],
        0,
        0,
        "stream-attribute erasure rewrite test".to_string(),
        Uuid::from_u128(id),
        now_ns,
    )
    .await
    .expect("the erasure request is recorded");
}

/// The keys under the tenant's logs `del/` prefix that end in `suffix`.
async fn del_keys(store: &Arc<dyn ObjectStoreBackend>, suffix: &str) -> Vec<String> {
    let prefix = keys::del_prefix(&TenantId::new(TENANT).hash(), Signal::Logs);
    list_all(store.as_ref(), &prefix)
        .await
        .expect("list del/")
        .into_iter()
        .map(|m| m.key)
        .filter(|k| k.ends_with(suffix))
        .collect()
}

/// Erasing `user_id = alice`, where alice is named only in a resource or a
/// scope attribute, keeps her records out of every read after the request
/// completes and its `.dreq` is swept, exactly as erasing `user_id = dave`,
/// named in a record attribute, keeps his out.
///
/// The bob records are survivors: no request names him, so they must come
/// through the rewrite and every read unchanged.
#[tokio::test]
async fn a_stream_attribute_subject_stays_erased_after_its_request_is_swept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("export.parquet");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let ingest_ns = BASE_NS + 10 * ONE_SEC_NS;
    let t = |n: i64| BASE_NS + n * ONE_SEC_NS;

    let router = LogIngestRouter::new(
        IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        Arc::new(FixedClock(ingest_ns)),
        Arc::new(PendingAckBudget::new(0)),
    );
    for batch in [
        vec![
            record(
                &[("service.name", "api"), ("user_id", "alice")],
                &[],
                &[],
                t(0),
                "alice-resource",
            ),
            record(
                &[("service.name", "api")],
                &[("user_id", "alice")],
                &[],
                t(1),
                "alice-scope",
            ),
            record(
                &[("service.name", "api")],
                &[],
                &[("user_id", "dave")],
                t(2),
                "dave-record",
            ),
        ],
        vec![
            record(
                &[("service.name", "api"), ("user_id", "bob")],
                &[],
                &[],
                t(3),
                "bob-resource",
            ),
            record(
                &[("service.name", "api")],
                &[],
                &[("user_id", "bob")],
                t(4),
                "bob-record",
            ),
            record(
                &[("service.name", "web"), ("user_id", "alice")],
                &[],
                &[],
                t(5),
                "alice-resource-web",
            ),
        ],
    ] {
        router
            .write(
                TenantId::new(TENANT),
                batch,
                WriteMode::Strict,
                std::time::Duration::from_secs(30),
            )
            .await
            .expect("strict write commits");
    }
    router.shutdown().await;

    let all = vec![
        "alice-resource",
        "alice-resource-web",
        "alice-scope",
        "bob-record",
        "bob-resource",
        "dave-record",
    ];
    let survivors = vec!["bob-record".to_string(), "bob-resource".to_string()];
    let window = (t(0), t(6));
    let (before, predicates) = export_bodies(&store, &out, window.0, window.1, ingest_ns).await;
    assert_eq!(predicates, 0);
    assert_eq!(before, all, "every record is readable before any erasure");

    erase_user(&store, "alice", 0x4501_a11c, ingest_ns).await;
    erase_user(&store, "dave", 0x4501_da7e, ingest_ns).await;
    let (pending_view, predicates) =
        export_bodies(&store, &out, window.0, window.1, ingest_ns).await;
    assert_eq!(predicates, 2);
    assert_eq!(
        pending_view, survivors,
        "while both requests are pending, the read excludes alice (stream attributes) and dave (record attribute)"
    );

    // The maintain tick's rewrite and completion check, on the sealed bucket.
    let config = CompactorConfig::default();
    let hour = u32::try_from(ingest_ns / NS_PER_HOUR).expect("hour fits u32");
    let bucket = Bucket::new(TenantId::new(TENANT).hash(), Signal::Logs, 0, hour);
    let sealed =
        ravel_maintain::FixedClock::new((i64::from(hour) + 1) * NS_PER_HOUR + 3 * NS_PER_HOUR);
    let pending =
        pending_erasure_requests(store.as_ref(), &TenantId::new(TENANT).hash(), Signal::Logs)
            .await
            .expect("pending requests");
    assert_eq!(pending.len(), 2);
    let mut memo = MaintainMemo::with_default_interval();
    let outcome = erasure_rewrite_bucket(
        store.as_ref(),
        &sealed,
        &config,
        &NoLeases,
        &bucket,
        &pending,
        &mut memo,
    )
    .await
    .expect("rewrite runs");
    assert!(
        matches!(
            outcome,
            ErasureRewriteOutcome::Rewritten {
                abandoned: None,
                ..
            }
        ),
        "the sealed bucket is rewritten, got {outcome:?}"
    );
    let completion = bucket_erasure_completion(
        store.as_ref(),
        &sealed,
        &config,
        &NoLeases,
        &bucket,
        &pending,
    )
    .await
    .expect("completion check runs");
    assert!(
        completion.blocked.is_empty(),
        "completion finds nothing pre-rewrite still served, so both requests may complete: {:?}",
        completion.blocked
    );

    // The request sweep's store effect: the `.dreq` objects are deleted, the
    // permanent `.done` markers (if any) stay.
    let dreqs = del_keys(&store, ".dreq").await;
    assert_eq!(dreqs.len(), 2);
    for key in &dreqs {
        store.delete(key).await.expect("delete .dreq");
    }

    let (after, predicates) = export_bodies(&store, &out, window.0, window.1, ingest_ns).await;
    assert_eq!(predicates, 0, "no request is pending once both are swept");
    assert_eq!(
        after, survivors,
        "after the requests are swept, only the rewrite keeps the erased subjects out: \
         dave (record attribute) and alice (resource or scope attribute) must both stay erased"
    );
}

/// The changelog's remedy for a bucket an earlier rewrite left alice in: a
/// new request for the same subject rewrites the already-rewritten bucket
/// again and drops her. The first request here matches nothing, standing in
/// for an earlier rewrite that dropped nothing; the bucket's live record is
/// then a rewrite, and the second request must not be skipped as applied.
#[tokio::test]
async fn a_new_request_rewrites_a_bucket_an_earlier_rewrite_left_the_subject_in() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("export.parquet");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let ingest_ns = BASE_NS + 10 * ONE_SEC_NS;
    let t = |n: i64| BASE_NS + n * ONE_SEC_NS;

    let router = LogIngestRouter::new(
        IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        Arc::new(FixedClock(ingest_ns)),
        Arc::new(PendingAckBudget::new(0)),
    );
    for batch in [
        vec![record(
            &[("service.name", "api"), ("user_id", "alice")],
            &[],
            &[],
            t(0),
            "alice-resource",
        )],
        vec![record(
            &[("service.name", "api"), ("user_id", "bob")],
            &[],
            &[],
            t(1),
            "bob-resource",
        )],
    ] {
        router
            .write(
                TenantId::new(TENANT),
                batch,
                WriteMode::Strict,
                std::time::Duration::from_secs(30),
            )
            .await
            .expect("strict write commits");
    }
    router.shutdown().await;

    let config = CompactorConfig::default();
    let hour = u32::try_from(ingest_ns / NS_PER_HOUR).expect("hour fits u32");
    let bucket = Bucket::new(TenantId::new(TENANT).hash(), Signal::Logs, 0, hour);
    let sealed =
        ravel_maintain::FixedClock::new((i64::from(hour) + 1) * NS_PER_HOUR + 3 * NS_PER_HOUR);
    let mut memo = MaintainMemo::with_default_interval();

    for (subject, id) in [("nobody", 0x4503_0001), ("alice", 0x4503_0002)] {
        erase_user(&store, subject, id, ingest_ns).await;
        let pending =
            pending_erasure_requests(store.as_ref(), &TenantId::new(TENANT).hash(), Signal::Logs)
                .await
                .expect("pending requests");
        let outcome = erasure_rewrite_bucket(
            store.as_ref(),
            &sealed,
            &config,
            &NoLeases,
            &bucket,
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite runs");
        assert!(
            matches!(
                outcome,
                ErasureRewriteOutcome::Rewritten {
                    abandoned: None,
                    ..
                }
            ),
            "the request for {subject} rewrites the bucket, got {outcome:?}"
        );
    }

    for key in del_keys(&store, ".dreq").await {
        store.delete(&key).await.expect("delete .dreq");
    }
    let (after, predicates) = export_bodies(&store, &out, t(0), t(2), ingest_ns).await;
    assert_eq!(predicates, 0);
    assert_eq!(
        after,
        vec!["bob-resource".to_string()],
        "the second request's rewrite dropped alice from the already-rewritten bucket"
    );
}

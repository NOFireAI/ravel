//! The `sys/compat` activation policy on the log and span flush paths
//! (ADR-4233 decisions 2 to 4), driven through each router's write API with
//! the policy's family-version seam: no policy publishes byte for byte what
//! the parent of the policy work published, the bucket is fenced before the
//! first PUT of a version above the baseline, and a view that ages past the
//! horizon after grant stops the flush before its next attempt.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use prost::Message;
use ravel_catalog::compat::{
    COMPAT_VIEW_HORIZON_NS, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, create_compat,
};
use ravel_catalog::{AbsentPolicy, CompatClock, CompatKeys, CompatRefusal, CompatView};
use ravel_commit::rng::SeededRng;
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_proto::sys::v1 as sysproto;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

use crate::clock::Clock;
use crate::compat::{CompatPublishRefusal, IngestCompatPolicy};
use crate::config::IngestConfig;
use crate::format_floor::FormatFamily;
use crate::indexed_fields::IndexedFieldsOverlay;
use crate::log_error::LogWriteError;
use crate::log_router::{LogIngestRouter, NoIndexedFields};
use crate::pending_ack::PendingAckBudget;
use crate::router::WriteMode;
use crate::span_error::SpanWriteError;
use crate::span_router::SpanIngestRouter;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

const ACK_DEADLINE: Duration = Duration::from_secs(60);

/// What `acme`'s one-record log write and one-span span write published on
/// the parent of the activation policy work (b6646b020), under
/// [`seeded_log_router`] and [`seeded_span_router`].
const PARENT_LOGS_DIGEST: &str = "98ecb0f9875f6c059ae729a5ecc6b0a33ca3c18497eb6de738d366a136d7c8d7";
const PARENT_SPANS_DIGEST: &str =
    "254f5ddc969569986bc590ff5928c7871de5656ec720067e98b17e5a8cb4d629";

/// Pins every clock-derived identity field. Flushes run on arrival, never
/// on the age tick.
struct FixedClock(i64);

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

struct CompatTestClock(AtomicI64);

impl CompatClock for CompatTestClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    }
}

fn log_record() -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs).into(),
        ts_ns: 1_000,
        observed_ts_ns: 1_000,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "line".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn span() -> NormalizedSpan {
    NormalizedSpan {
        trace_id: [1; 16],
        span_id: [1; 8],
        parent_span_id: None,
        name: "handle".to_string(),
        start_ts_ns: 1_000,
        end_ts_ns: 1_100,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

/// Every key under `tenant`'s `signal` prefix, sorted.
async fn keys(store: &dyn ObjectStoreBackend, tenant: &TenantId, signal: Signal) -> Vec<String> {
    let prefix = format!("t/{}/{}/", tenant.hash().to_hex(), signal.key_prefix());
    let mut keys: Vec<String> = list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    keys.sort();
    keys
}

fn objects(keys: &[String]) -> usize {
    keys.iter()
        .filter(|k| k.contains("/l0/") || k.contains("/c/"))
        .count()
}

/// blake3 over every key and object under `tenant`'s `signal` prefix, in
/// key order.
async fn digest(store: &dyn ObjectStoreBackend, tenant: &TenantId, signal: Signal) -> String {
    let keys = keys(store, tenant, signal).await;
    assert!(keys.iter().any(|key| key.contains("/l0/")), "{keys:?}");
    assert!(keys.iter().any(|key| key.contains("/c/")), "{keys:?}");
    let mut hasher = blake3::Hasher::new();
    for key in &keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    hasher.finalize().to_hex().to_string()
}

fn seeded_log_router(store: Arc<dyn ObjectStoreBackend>) -> LogIngestRouter {
    LogIngestRouter::with_rng(
        config(),
        store,
        Arc::new(FixedClock(BASE_NS)),
        Arc::new(IndexedFieldsOverlay::new(Arc::new(NoIndexedFields))),
        Arc::new(SeededRng::new(4244)),
        Arc::new(PendingAckBudget::new(0)),
    )
}

fn seeded_span_router(store: Arc<dyn ObjectStoreBackend>) -> SpanIngestRouter {
    SpanIngestRouter::with_rng(
        config(),
        store,
        Arc::new(FixedClock(BASE_NS)),
        Arc::new(SeededRng::new(4244)),
        Arc::new(PendingAckBudget::new(0)),
    )
}

async fn log_digest(policy: Option<Option<Arc<IngestCompatPolicy>>>) -> String {
    let acme = TenantId::new("acme");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut router = seeded_log_router(Arc::clone(&store));
    if let Some(policy) = policy {
        router = router.with_compat_policy(policy).expect("no publish bound");
    }
    router
        .write(
            acme.clone(),
            vec![log_record()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect("written");
    digest(store.as_ref(), &acme, Signal::Logs).await
}

async fn span_digest(policy: Option<Option<Arc<IngestCompatPolicy>>>) -> String {
    let acme = TenantId::new("acme");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut router = seeded_span_router(Arc::clone(&store));
    if let Some(policy) = policy {
        router = router.with_compat_policy(policy).expect("no publish bound");
    }
    router
        .write(
            acme.clone(),
            vec![span()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect("written");
    digest(store.as_ref(), &acme, Signal::Spans).await
}

/// ADR-4233 decision 8: with no policy installed, never set or set to
/// `None`, a log and a span write store the same keys and bytes as before
/// the policy existed.
#[tokio::test]
async fn no_policy_publishes_the_parents_log_and_span_bytes() {
    assert_eq!(log_digest(None).await, PARENT_LOGS_DIGEST);
    assert_eq!(log_digest(Some(None)).await, PARENT_LOGS_DIGEST);
    assert_eq!(span_digest(None).await, PARENT_SPANS_DIGEST);
    assert_eq!(span_digest(Some(None)).await, PARENT_SPANS_DIGEST);
}

/// A store holding `tenant`'s `signal` provisioning record and a
/// `sys/compat` at epoch 1 naming each family's `CURRENT`; a policy over a
/// view that read it, with `family`'s baseline lowered one below `CURRENT`
/// so `CURRENT` is above the baseline and needs the bucket fence.
async fn above_baseline(
    tenant: &TenantId,
    signal: Signal,
    family: FormatFamily,
) -> (
    Arc<FaultStore<MemoryStore>>,
    Arc<CompatTestClock>,
    Arc<IngestCompatPolicy>,
) {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        signal,
        1,
        BASE_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    let keys = CompatKeys::from_file_keys(&[[4; 32]]).expect("one key");
    let families = [
        (FAMILY_RSEG, FormatFamily::Rseg),
        (FAMILY_RLOG, FormatFamily::Rlog),
        (FAMILY_RSPAN, FormatFamily::Rspan),
    ]
    .into_iter()
    .map(|(name, family)| sysproto::CompatFamily {
        family: name.to_string(),
        active_write_version: family.writable_version(),
        ..Default::default()
    })
    .collect();
    create_compat(
        store.as_ref(),
        &keys,
        sysproto::CompatRecord {
            compat_epoch: 1,
            families,
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
    let clock = Arc::new(CompatTestClock(AtomicI64::new(BASE_NS)));
    let view = CompatView::new(Arc::clone(&store), keys, clock.clone());
    view.refresh().await.expect("read sys/compat");
    let current = u16::try_from(family.writable_version()).expect("fits");
    let policy = Arc::new(
        IngestCompatPolicy::new(view, clock.clone()).with_family_versions(
            family,
            vec![current],
            current - 1,
        ),
    );
    (fault_store, clock, policy)
}

/// The `compat_epoch` of `tenant`'s `signal` provisioning record.
async fn bucket_compat_epoch(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> u64 {
    let key = ravel_catalog::provisioning_key(&tenant.hash(), signal);
    let bytes = store
        .get(&key, GetRange::Full)
        .await
        .expect("provisioning record")
        .data;
    sysproto::ProvisioningRecord::decode(bytes.as_ref())
        .expect("decode provisioning record")
        .compat_epoch
}

/// Waits for one PUT to park at `gate`, failing rather than hanging when the
/// flush never sends it.
async fn held(gate: &GateHandle) {
    tokio::time::timeout(Duration::from_secs(30), gate.wait_until_held(1))
        .await
        .expect("a PUT parked at the gate");
}

fn policy_refusal(
    refused: &CompatPublishRefusal,
    family: FormatFamily,
    want: impl Fn(&CompatRefusal) -> bool,
) -> bool {
    matches!(
        refused,
        CompatPublishRefusal::Policy { family: f, refusal } if *f == family && want(refusal)
    )
}

/// ADR-4233 decision 4 on the log path: the bucket carries the view's epoch
/// by the time the first data PUT is sent. A fence raised after the PUT, or
/// never, leaves it at 0 there.
#[tokio::test]
async fn a_log_flush_above_the_baseline_fences_the_bucket_before_its_first_put() {
    let acme = TenantId::new("acme");
    let (fault_store, _, policy) = above_baseline(&acme, Signal::Logs, FormatFamily::Rlog).await;
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let router = Arc::new(
        seeded_log_router(Arc::clone(&store))
            .with_compat_policy(Some(policy))
            .expect("no publish bound"),
    );
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    let write = tokio::spawn({
        let (router, acme) = (Arc::clone(&router), acme.clone());
        async move {
            router
                .write(
                    acme,
                    vec![log_record()],
                    WriteMode::StrictArrival,
                    ACK_DEADLINE,
                )
                .await
        }
    });
    held(&gate).await;
    assert_eq!(
        bucket_compat_epoch(store.as_ref(), &acme, Signal::Logs).await,
        1,
        "the bucket is fenced to the view's epoch before the data PUT"
    );
    for id in gate.held() {
        assert!(gate.release(id));
    }
    write.await.expect("write task").expect("written");
    assert_eq!(objects(&keys(store.as_ref(), &acme, Signal::Logs).await), 2);
    assert_eq!(router.metrics_snapshot().compat_publish_refused.total(), 0);
}

#[tokio::test]
async fn a_span_flush_above_the_baseline_fences_the_bucket_before_its_first_put() {
    let acme = TenantId::new("acme");
    let (fault_store, _, policy) = above_baseline(&acme, Signal::Spans, FormatFamily::Rspan).await;
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let router = Arc::new(
        seeded_span_router(Arc::clone(&store))
            .with_compat_policy(Some(policy))
            .expect("no publish bound"),
    );
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    let write = tokio::spawn({
        let (router, acme) = (Arc::clone(&router), acme.clone());
        async move {
            router
                .write(acme, vec![span()], WriteMode::StrictArrival, ACK_DEADLINE)
                .await
        }
    });
    held(&gate).await;
    assert_eq!(
        bucket_compat_epoch(store.as_ref(), &acme, Signal::Spans).await,
        1,
        "the bucket is fenced to the view's epoch before the data PUT"
    );
    for id in gate.held() {
        assert!(gate.release(id));
    }
    write.await.expect("write task").expect("written");
    assert_eq!(
        objects(&keys(store.as_ref(), &acme, Signal::Spans).await),
        2
    );
    assert_eq!(router.metrics_snapshot().compat_publish_refused.total(), 0);
}

/// ADR-4233 decision 3 on the log path: the view ages to the horizon while
/// the granted flush's fence PUT is in flight, and the flush is abandoned
/// before its data PUT. A check made only at grant lets the PUT through.
#[tokio::test]
async fn a_log_view_stale_after_grant_abandons_the_flush_without_a_put() {
    let acme = TenantId::new("acme");
    let (fault_store, compat_clock, policy) =
        above_baseline(&acme, Signal::Logs, FormatFamily::Rlog).await;
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let router = Arc::new(
        seeded_log_router(Arc::clone(&store))
            .with_compat_policy(Some(policy))
            .expect("no publish bound"),
    );
    let gate = fault_store.hold(Op::Put, Some("/prov".to_string()), Occurrence::Nth(1));

    let write = tokio::spawn({
        let (router, acme) = (Arc::clone(&router), acme.clone());
        async move {
            router
                .write(
                    acme,
                    vec![log_record()],
                    WriteMode::StrictArrival,
                    ACK_DEADLINE,
                )
                .await
        }
    });
    held(&gate).await;
    compat_clock
        .0
        .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);
    for id in gate.held() {
        assert!(gate.release(id));
    }

    let err = write
        .await
        .expect("write task")
        .expect_err("the view went stale before the data PUT");
    assert!(
        matches!(&err, LogWriteError::CompatRefused(r)
            if policy_refusal(r, FormatFamily::Rlog, |r| matches!(r, CompatRefusal::ViewStale { .. }))),
        "{err:?}"
    );
    assert_eq!(objects(&keys(store.as_ref(), &acme, Signal::Logs).await), 0);
    let refused = router.metrics_snapshot().compat_publish_refused;
    assert_eq!(refused.view_stale, 1);
    assert_eq!(refused.total(), 1);
}

#[tokio::test]
async fn a_span_view_stale_after_grant_abandons_the_flush_without_a_put() {
    let acme = TenantId::new("acme");
    let (fault_store, compat_clock, policy) =
        above_baseline(&acme, Signal::Spans, FormatFamily::Rspan).await;
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let router = Arc::new(
        seeded_span_router(Arc::clone(&store))
            .with_compat_policy(Some(policy))
            .expect("no publish bound"),
    );
    let gate = fault_store.hold(Op::Put, Some("/prov".to_string()), Occurrence::Nth(1));

    let write = tokio::spawn({
        let (router, acme) = (Arc::clone(&router), acme.clone());
        async move {
            router
                .write(acme, vec![span()], WriteMode::StrictArrival, ACK_DEADLINE)
                .await
        }
    });
    held(&gate).await;
    compat_clock
        .0
        .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);
    for id in gate.held() {
        assert!(gate.release(id));
    }

    let err = write
        .await
        .expect("write task")
        .expect_err("the view went stale before the data PUT");
    assert!(
        matches!(&err, SpanWriteError::CompatRefused(r)
            if policy_refusal(r, FormatFamily::Rspan, |r| matches!(r, CompatRefusal::ViewStale { .. }))),
        "{err:?}"
    );
    assert_eq!(
        objects(&keys(store.as_ref(), &acme, Signal::Spans).await),
        0
    );
    let refused = router.metrics_snapshot().compat_publish_refused;
    assert_eq!(refused.view_stale, 1);
    assert_eq!(refused.total(), 1);
}

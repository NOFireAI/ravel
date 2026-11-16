//! The activation policy on the log and span write paths (ADR-4233
//! decisions 2 and 3), through the public router API: a view naming a version
//! this build cannot write refuses at admission, a view that went stale
//! before the flush's permit grant refuses the flush with no PUT, and a fresh
//! view at the baseline publishes at the writer's `CURRENT` version. Every
//! refusal is counted on the router's own metrics snapshot under its reason.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::{TestClock, tenant};
use ravel_catalog::compat::{
    COMPAT_VIEW_HORIZON_NS, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, create_compat,
};
use ravel_catalog::{
    AbsentPolicy, CompatClock, CompatKeys, CompatRefusal, CompatView, validate_or_adopt,
};
use ravel_commit::record;
use ravel_ingest::{
    CompatPublishRefusal, CompatRefusalSnapshot, FormatFamily, IngestCompatPolicy, IngestConfig,
    LogIngestRouter, LogWriteError, PendingAckBudget, SpanIngestRouter, SpanWriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_proto::sys::v1 as sysproto;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

const ACK_DEADLINE: Duration = Duration::from_secs(60);

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

fn family_versions() -> [(&'static str, u32); 3] {
    [
        (FAMILY_RSEG, FormatFamily::Rseg.writable_version()),
        (FAMILY_RLOG, FormatFamily::Rlog.writable_version()),
        (FAMILY_RSPAN, FormatFamily::Rspan.writable_version()),
    ]
}

/// A store with `tenant`'s provisioning record for `signal` and a
/// `sys/compat` naming each family's writer version, except `family` at
/// `active`; a view that read it at `BASE_NS`; and that view's clock.
async fn setup(
    tenant: &TenantId,
    signal: Signal,
    family: &str,
    active: u32,
) -> (
    Arc<MemoryStore>,
    Arc<CompatTestClock>,
    Arc<IngestCompatPolicy>,
) {
    let store = Arc::new(MemoryStore::new());
    validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        signal,
        1,
        BASE_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    let keys = CompatKeys::from_file_keys(&[[3; 32]]).expect("one key");
    let families = family_versions()
        .into_iter()
        .map(|(name, version)| sysproto::CompatFamily {
            family: name.to_string(),
            active_write_version: if name == family { active } else { version },
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
    let view = CompatView::new(store.clone(), keys, clock.clone());
    view.refresh().await.expect("read sys/compat");
    let policy = Arc::new(IngestCompatPolicy::new(view, clock.clone()));
    (store, clock, policy)
}

fn log_router(
    store: &Arc<MemoryStore>,
    policy: Arc<IngestCompatPolicy>,
) -> (LogIngestRouter, Arc<TestClock>) {
    let clock = TestClock::new(BASE_NS);
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    let router = LogIngestRouter::new(
        config(),
        store,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_compat_policy(Some(policy))
    .expect("no publish bound");
    (router, clock)
}

fn span_router(
    store: &Arc<MemoryStore>,
    policy: Arc<IngestCompatPolicy>,
) -> (SpanIngestRouter, Arc<TestClock>) {
    let clock = TestClock::new(BASE_NS);
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    let router = SpanIngestRouter::new(
        config(),
        store,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_compat_policy(Some(policy))
    .expect("no publish bound");
    (router, clock)
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

/// Every data object and commit record under `tenant`'s `signal` prefix.
async fn objects(store: &MemoryStore, tenant: &TenantId, signal: Signal) -> Vec<String> {
    let prefix = format!("t/{}/{}/", tenant.hash().to_hex(), signal.key_prefix());
    list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.key)
        .filter(|key| key.contains("/l0/") || key.contains("/c/"))
        .collect()
}

/// The `segment_format_version` of every commit record under `tenant`'s
/// `signal` prefix.
async fn committed_versions(store: &MemoryStore, tenant: &TenantId, signal: Signal) -> Vec<u32> {
    let mut versions = Vec::new();
    for key in objects(store, tenant, signal).await {
        if key.contains("/c/") {
            let bytes = store
                .get(&key, ravel_object_store::GetRange::Full)
                .await
                .expect("commit record")
                .data;
            versions.push(
                record::decode(&bytes)
                    .expect("decode commit record")
                    .segment_format_version,
            );
        }
    }
    versions
}

fn only(snapshot: CompatRefusalSnapshot) -> CompatRefusalSnapshot {
    assert_eq!(snapshot.total(), 1, "{snapshot:?}");
    snapshot
}

fn not_writable(refused: &CompatPublishRefusal, family: FormatFamily, active: u32) -> bool {
    matches!(
        refused,
        CompatPublishRefusal::Policy {
            family: f,
            refusal: CompatRefusal::ActiveVersionNotWritable { active: a, .. },
        } if *f == family && *a == active
    )
}

fn stale(refused: &CompatPublishRefusal, family: FormatFamily) -> bool {
    matches!(
        refused,
        CompatPublishRefusal::Policy {
            family: f,
            refusal: CompatRefusal::ViewStale { .. },
        } if *f == family
    )
}

#[tokio::test]
async fn a_log_write_under_an_rlog_version_this_build_cannot_write_is_refused_at_admission() {
    let acme = tenant("acme");
    let active = FormatFamily::Rlog.writable_version() + 1;
    let (store, _, policy) = setup(&acme, Signal::Logs, FAMILY_RLOG, active).await;
    let (router, _) = log_router(&store, policy);

    for mode in [WriteMode::StrictArrival, WriteMode::Buffered] {
        let err = router
            .write(acme.clone(), vec![log_record()], mode, ACK_DEADLINE)
            .await
            .expect_err("the active RLOG version is not writable");
        assert!(
            matches!(&err, LogWriteError::CompatRefused(r) if not_writable(r, FormatFamily::Rlog, active)),
            "{err:?}"
        );
        assert!(err.is_retryable());
    }
    let refused = router.metrics_snapshot().compat_publish_refused;
    assert_eq!(refused.active_version_not_writable, 2);
    assert_eq!(refused.total(), 2);
    router.flush_all().await;
    assert_eq!(
        objects(&store, &acme, Signal::Logs).await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_span_write_under_an_rspan_version_this_build_cannot_write_is_refused_at_admission() {
    let acme = tenant("acme");
    let active = FormatFamily::Rspan.writable_version() + 1;
    let (store, _, policy) = setup(&acme, Signal::Spans, FAMILY_RSPAN, active).await;
    let (router, _) = span_router(&store, policy);

    for mode in [WriteMode::StrictArrival, WriteMode::Buffered] {
        let err = router
            .write(acme.clone(), vec![span()], mode, ACK_DEADLINE)
            .await
            .expect_err("the active RSPAN version is not writable");
        assert!(
            matches!(&err, SpanWriteError::CompatRefused(r) if not_writable(r, FormatFamily::Rspan, active)),
            "{err:?}"
        );
        assert!(err.is_retryable());
    }
    let refused = router.metrics_snapshot().compat_publish_refused;
    assert_eq!(refused.active_version_not_writable, 2);
    assert_eq!(refused.total(), 2);
    router.flush_all().await;
    assert_eq!(
        objects(&store, &acme, Signal::Spans).await,
        Vec::<String>::new()
    );
}

/// Admission lets a stale view through (the flush re-asks at permit grant),
/// and the grant refuses it: no object is stored and the refusal is counted
/// under `view_stale`.
#[tokio::test]
async fn a_log_flush_granted_on_a_stale_view_is_refused_without_a_put() {
    let acme = tenant("acme");
    let (store, compat_clock, policy) = setup(
        &acme,
        Signal::Logs,
        FAMILY_RLOG,
        FormatFamily::Rlog.writable_version(),
    )
    .await;
    let (router, _) = log_router(&store, policy);
    compat_clock
        .0
        .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);

    let err = router
        .write(
            acme.clone(),
            vec![log_record()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect_err("the view is past the horizon at grant");
    assert!(
        matches!(&err, LogWriteError::CompatRefused(r) if stale(r, FormatFamily::Rlog)),
        "{err:?}"
    );
    assert!(err.is_retryable());
    assert_eq!(
        only(router.metrics_snapshot().compat_publish_refused).view_stale,
        1
    );
    assert_eq!(
        objects(&store, &acme, Signal::Logs).await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_span_flush_granted_on_a_stale_view_is_refused_without_a_put() {
    let acme = tenant("acme");
    let (store, compat_clock, policy) = setup(
        &acme,
        Signal::Spans,
        FAMILY_RSPAN,
        FormatFamily::Rspan.writable_version(),
    )
    .await;
    let (router, _) = span_router(&store, policy);
    compat_clock
        .0
        .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);

    let err = router
        .write(
            acme.clone(),
            vec![span()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect_err("the view is past the horizon at grant");
    assert!(
        matches!(&err, SpanWriteError::CompatRefused(r) if stale(r, FormatFamily::Rspan)),
        "{err:?}"
    );
    assert!(err.is_retryable());
    assert_eq!(
        only(router.metrics_snapshot().compat_publish_refused).view_stale,
        1
    );
    assert_eq!(
        objects(&store, &acme, Signal::Spans).await,
        Vec::<String>::new()
    );
}

/// A fresh view at the baseline publishes at the writer's `CURRENT` version
/// and refuses nothing.
#[tokio::test]
async fn log_and_span_flushes_under_a_fresh_baseline_view_publish_at_current() {
    let acme = tenant("acme");
    let rlog = FormatFamily::Rlog.writable_version();
    let (store, _, policy) = setup(&acme, Signal::Logs, FAMILY_RLOG, rlog).await;
    let (router, _) = log_router(&store, policy);
    router
        .write(
            acme.clone(),
            vec![log_record()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect("written");
    assert_eq!(
        committed_versions(&store, &acme, Signal::Logs).await,
        vec![rlog]
    );
    assert_eq!(router.metrics_snapshot().compat_publish_refused.total(), 0);

    let rspan = FormatFamily::Rspan.writable_version();
    let (store, _, policy) = setup(&acme, Signal::Spans, FAMILY_RSPAN, rspan).await;
    let (router, _) = span_router(&store, policy);
    router
        .write(
            acme.clone(),
            vec![span()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect("written");
    assert_eq!(
        committed_versions(&store, &acme, Signal::Spans).await,
        vec![rspan]
    );
    assert_eq!(router.metrics_snapshot().compat_publish_refused.total(), 0);
}

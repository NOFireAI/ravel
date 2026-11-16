//! A real server on a keyed bucket whose `sys/compat` it cannot read: every
//! GET of the record fails with a 5xx (ADR-4233 decision 3, "refresh and
//! staleness"). A view that never read the record and saw a failure other
//! than `AccessDenied` is not blind, so the bucket's activation cannot be
//! ignored: the alert evaluator takes no lease and writes no alert history,
//! a first touch of the metrics signal is refused with a retryable 503 and
//! leaves no provisioning record, and `/metrics` counts both refusals under
//! `ravel_compat_publish_refused_total{reason="view_stale"}`.
//!
//! The record names `rlog` and `rseg` at the build's current versions, so a
//! server that ignored the failed read would write both at their baselines;
//! that is the behaviour this binary pins as refused.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use ravel_alerting::lease::alert_lease_key;
use ravel_catalog::compat::{COMPAT_KEY, CompatKeys, FAMILY_RLOG, FAMILY_RSEG, create_compat};
use ravel_catalog::provisioning_key;
use ravel_commit::keys;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_remote_write::proto::prometheus::{
    Label as ProtoLabel, Sample as ProtoSample, TimeSeries as ProtoTimeSeries,
    WriteRequest as ProtoWriteRequest,
};
use ravel_server::alerting::ALERT_SHARD;
use ravel_types::{Signal, TenantId};

#[path = "support/alert_history.rs"]
mod alert_history;

use alert_history::{
    TENANT, TOKEN, sample, scrape, scrape_until_positive, seed_alert_object_at_version,
    start_server_on,
};

const DEPLOYMENT_KEY: [u8; 32] = [0x6d; 32];
const RLOG_STALE: &str =
    r#"ravel_compat_publish_refused_total{mode="all",family="rlog",reason="view_stale"}"#;
const RSEG_STALE: &str =
    r#"ravel_compat_publish_refused_total{mode="all",family="rseg",reason="view_stale"}"#;

fn rw1_body() -> Vec<u8> {
    let ts_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("now fits i64");
    let request = ProtoWriteRequest {
        timeseries: vec![ProtoTimeSeries {
            labels: vec![ProtoLabel {
                name: "__name__".to_string(),
                value: "blind_probe".to_string(),
            }],
            samples: vec![ProtoSample {
                value: 1.0,
                timestamp: ts_ms,
            }],
            exemplars: vec![],
            histograms: vec![],
        }],
        metadata: vec![],
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("compress")
}

async fn alert_commit_count(store: &dyn ObjectStoreBackend) -> usize {
    let tenant = TenantId::new(TENANT).hash();
    let prefix = keys::commit_shard_prefix(&tenant, Signal::Alerts, ALERT_SHARD).expect("prefix");
    ravel_object_store::list_all(store, &prefix)
        .await
        .expect("list")
        .len()
}

/// Watch it fail: give `CompatView::check_fresh`'s blind exemption back to
/// a view whose latest refresh failed with a 5xx (drop `!latest_failed_not_denied`
/// from `blind`), and the remote write is accepted with a 204, a provisioning
/// record is written, and the evaluator takes the lease; or drop
/// `born_fenced_epoch`'s `check_fresh` call, and the write is accepted under
/// the same view.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_sys_compat_refuses_alerts_and_a_first_touch() {
    let inner = MemoryStore::new();
    let current_rseg = u32::from(ravel_segment::SegmentVersion::CURRENT.number());
    let current_rlog = u32::from(ravel_logseg::WriteVersion::CURRENT.number());
    create_compat(
        &inner,
        &CompatKeys::from_file_keys(&[DEPLOYMENT_KEY]).expect("key"),
        sysproto::CompatRecord {
            compat_epoch: 2,
            families: vec![
                sysproto::CompatFamily {
                    family: FAMILY_RLOG.to_string(),
                    active_write_version: current_rlog,
                    activated_epoch: 2,
                    ..Default::default()
                },
                sysproto::CompatFamily {
                    family: FAMILY_RSEG.to_string(),
                    active_write_version: current_rseg,
                    activated_epoch: 2,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
    seed_alert_object_at_version(&inner, ravel_logseg::WriteVersion::CURRENT.number()).await;
    let faults = Arc::new(FaultStore::new(
        inner,
        FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("503 Service Unavailable".to_string()),
            )
            .with_key_contains(COMPAT_KEY),
        ),
    ));
    assert_eq!(alert_commit_count(faults.as_ref()).await, 1);

    let running = start_server_on(faults.clone(), Some(DEPLOYMENT_KEY)).await;
    let view = running.compat_view().expect("a keyed server holds a view");
    assert_eq!(view.last_read_unix_ns(), None, "no read succeeded");
    assert!(!view.record_seen());
    assert!(
        faults.fault_count(Op::Get, FaultKind::Transient) >= 1,
        "the startup read hit the fault"
    );

    // The first touch of (acme, metrics) is refused, retryably, twice: a
    // refusal caches nothing and starts no backoff.
    let client = reqwest::Client::new();
    for attempt in 1..=2 {
        let response = client
            .post(format!("http://{}/api/v1/write", running.http_addr))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header(
                "content-type",
                "application/x-protobuf;proto=prometheus.WriteRequest",
            )
            .body(rw1_body())
            .send()
            .await
            .expect("write request completes");
        let code = response.status();
        let text = response.text().await.expect("body");
        assert_eq!(code, 503, "attempt {attempt}: {text}");
        assert!(text.contains("retry later"), "attempt {attempt}: {text}");
    }
    let tenant = TenantId::new(TENANT).hash();
    assert!(
        matches!(
            faults
                .head(&provisioning_key(&tenant, Signal::Metrics))
                .await,
            Err(StoreError::NotFound)
        ),
        "a refused first touch writes no provisioning record"
    );

    let body = scrape_until_positive(&running, RLOG_STALE).await;
    assert!(
        sample(&body, RLOG_STALE).is_some_and(|refused| refused >= 1.0),
        "the evaluator's refusal is counted:\n{body}"
    );
    assert_eq!(sample(&body, RSEG_STALE), Some(2.0), "{body}");
    assert!(
        sample(&body, r#"ravel_compat_refresh_failures_total{mode="all"}"#)
            .is_some_and(|failures| failures >= 1.0),
        "{body}"
    );
    assert_eq!(
        sample(&body, r#"ravel_compat_view_age_seconds{mode="all"}"#),
        Some(f64::INFINITY),
        "{body}"
    );
    assert_eq!(
        sample(&body, r#"ravel_alert_records_written_total{mode="all"}"#),
        Some(0.0),
        "{body}"
    );
    assert_eq!(
        alert_commit_count(faults.as_ref()).await,
        1,
        "no alert history written"
    );
    assert!(
        matches!(
            faults.head(&alert_lease_key(&tenant)).await,
            Err(StoreError::NotFound)
        ),
        "an evaluator under a non-blind never-read view takes no lease"
    );

    // Still refused on a later scrape: nothing in the server retired the
    // refusal.
    let later = scrape(&running).await;
    assert_eq!(sample(&later, RSEG_STALE), Some(2.0), "{later}");

    running.shutdown().await.expect("graceful shutdown");
}

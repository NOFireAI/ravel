//! A real server on a keyed bucket whose `sys/compat` names an `rlog` version
//! this build does not write (ADR-4233 decision 3, "a build that cannot
//! honour the active version"): it still starts, its alert evaluator writes
//! no alert history and takes no lease, and `/metrics` counts the refusal under
//! `ravel_compat_publish_refused_total{family="rlog",
//! reason="active_version_not_writable"}`.
//!
//! The history holds one firing transition, so a server that ignored the
//! policy would resolve it on its first tick and write a record. The writable
//! case is `compat_policy_alerts_e2e.rs`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_alerting::lease::alert_lease_key;
use ravel_catalog::compat::{CompatKeys, FAMILY_RLOG, create_compat};
use ravel_commit::keys;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_server::alerting::ALERT_SHARD;
use ravel_types::{Signal, TenantId};

#[path = "support/alert_history.rs"]
mod alert_history;

use alert_history::{
    sample, scrape_until_positive, seed_alert_object_at_version, start_server_with_key,
};

const DEPLOYMENT_KEY: [u8; 32] = [0x6c; 32];
const REFUSED: &str = r#"ravel_compat_publish_refused_total{mode="all",family="rlog",reason="active_version_not_writable"}"#;
const LEASE_UNAVAILABLE: &str =
    r#"ravel_alert_ticks_total{mode="all",outcome="lease_unavailable"}"#;

async fn commit_count(store: &dyn ObjectStoreBackend) -> usize {
    let tenant = TenantId::new("acme").hash();
    let prefix = keys::commit_shard_prefix(&tenant, Signal::Alerts, ALERT_SHARD).expect("prefix");
    ravel_object_store::list_all(store, &prefix)
        .await
        .expect("list")
        .len()
}

/// Watch it fail: make `AlertEvaluator::rlog_policy` return
/// `WriteVersion::CURRENT` whatever the view says. The first tick then
/// resolves the seeded alert and writes a record, the commit count assertion
/// fails, and the refusal counter stays 0 so the poll runs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwritable_rlog_version_stops_alert_writes_and_is_counted() {
    let store = Arc::new(MemoryStore::new());
    let unwritable = u32::from(ravel_logseg::footer::SUPPORTED_VERSIONS.newest()) + 1;
    create_compat(
        store.as_ref(),
        &CompatKeys::from_file_keys(&[DEPLOYMENT_KEY]).expect("key"),
        sysproto::CompatRecord {
            compat_epoch: 1,
            families: vec![sysproto::CompatFamily {
                family: FAMILY_RLOG.to_string(),
                active_write_version: unwritable,
                activated_epoch: 1,
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
    seed_alert_object_at_version(store.as_ref(), ravel_logseg::WriteVersion::CURRENT.number())
        .await;
    assert_eq!(commit_count(store.as_ref()).await, 1);

    let running = start_server_with_key(Arc::clone(&store), Some(DEPLOYMENT_KEY)).await;
    let body = scrape_until_positive(&running, LEASE_UNAVAILABLE).await;
    assert!(
        sample(&body, REFUSED).is_some_and(|refused| refused >= 1.0),
        "the refusal is counted on /metrics:\n{body}"
    );
    assert_eq!(
        sample(
            &body,
            r#"ravel_compat_active_write_version{mode="all",family="rlog"}"#
        ),
        Some(f64::from(unwritable)),
        "{body}"
    );
    assert_eq!(
        sample(&body, r#"ravel_alert_records_written_total{mode="all"}"#),
        Some(0.0),
        "{body}"
    );
    assert_eq!(
        commit_count(store.as_ref()).await,
        1,
        "no alert history written"
    );
    assert!(
        matches!(
            store
                .head(&alert_lease_key(&TenantId::new("acme").hash()))
                .await,
            Err(StoreError::NotFound)
        ),
        "an evaluator that cannot write the active version takes no lease"
    );

    running.shutdown().await.expect("graceful shutdown");
}

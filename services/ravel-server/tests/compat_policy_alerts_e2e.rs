//! A real server on a keyed bucket installs the `sys/compat` write policy
//! (ADR-4233 decision 3): `/metrics` renders the compat families from the
//! record it read at startup, its alert evaluator writes alert history under
//! the policy, and it holds the alert lease in the parked form because `rlog`
//! is recorded as activated (decision 4).
//!
//! This build's RLOG window is one version wide, so the version the policy
//! names equals the built-in one and the trailer assertions here pin the
//! policy wiring, not the version choice. A record carrying a second, distinct
//! version is the drill case, which lands with #4249.
//!
//! The history holds one firing transition for the configured rule and no
//! `up` series, so the first evaluated tick resolves it and writes a record.
//! The unwritable-version case is `compat_policy_refused_e2e.rs`, in its own
//! binary because the alert and compat counters are process-global.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_alerting::lease::{AlertLease, alert_lease_key};
use ravel_catalog::compat::{CompatKeys, FAMILY_RLOG, create_compat};
use ravel_commit::{keys, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::sys::v1 as sysproto;
use ravel_server::alerting::ALERT_SHARD;
use ravel_types::{Signal, TenantId};

#[path = "support/alert_history.rs"]
mod alert_history;

use alert_history::{
    sample, scrape_until_positive, seed_alert_object_at_version, start_server_with_key,
};

const DEPLOYMENT_KEY: [u8; 32] = [0x6b; 32];
const WRITTEN: &str = r#"ravel_alert_records_written_total{mode="all"}"#;

/// `(writer_id, trailer version, commit segment_format_version)` of every
/// alert history object.
async fn alert_objects(store: &dyn ObjectStoreBackend) -> Vec<(String, u16, u32)> {
    let tenant = TenantId::new("acme").hash();
    let prefix = keys::commit_shard_prefix(&tenant, Signal::Alerts, ALERT_SHARD).expect("prefix");
    let mut out = Vec::new();
    for meta in ravel_object_store::list_all(store, &prefix)
        .await
        .expect("list")
    {
        let commit = record::decode(
            &store
                .get(&meta.key, GetRange::Full)
                .await
                .expect("get commit")
                .data,
        )
        .expect("decode commit");
        let data_key = keys::verify_object_key(&commit).expect("object key");
        let bytes = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get data")
            .data;
        let at = bytes.len() - ravel_logseg::footer::TRAILER_LEN + 8;
        out.push((
            commit.writer_id.clone(),
            u16::from_le_bytes([bytes[at], bytes[at + 1]]),
            commit.segment_format_version,
        ));
    }
    out
}

/// Watch it fail: drop `.with_compat_policy(compat_view.clone())` from
/// `start`, and no compat family reaches `/metrics` and the lease is written
/// unparked. What an evaluator does when the policy names a version it cannot
/// write is pinned by `compat_policy_refused_e2e.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyed_server_renders_the_compat_view_and_writes_alerts_at_the_policy_version() {
    let store = Arc::new(MemoryStore::new());
    let policy_version = ravel_logseg::WriteVersion::CURRENT.number();
    create_compat(
        store.as_ref(),
        &CompatKeys::from_file_keys(&[DEPLOYMENT_KEY]).expect("key"),
        sysproto::CompatRecord {
            compat_epoch: 2,
            families: vec![sysproto::CompatFamily {
                family: FAMILY_RLOG.to_string(),
                active_write_version: u32::from(policy_version),
                activated_epoch: 2,
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
    seed_alert_object_at_version(store.as_ref(), policy_version).await;
    let before = alert_objects(store.as_ref()).await;
    assert_eq!(before.len(), 1, "one seeded firing transition");

    let running = start_server_with_key(Arc::clone(&store), Some(DEPLOYMENT_KEY)).await;
    assert_eq!(
        running.compat_view().map(|view| view.seen_epoch()),
        Some(2),
        "the server read sys/compat before it started serving"
    );
    let body = scrape_until_positive(&running, WRITTEN).await;
    assert!(
        sample(&body, WRITTEN).is_some_and(|written| written >= 1.0),
        "an alert tick wrote a transition:\n{body}"
    );

    // OBSERVABILITY: every compat family on the real /metrics surface.
    assert_eq!(
        sample(&body, r#"ravel_compat_epoch{mode="all"}"#),
        Some(2.0),
        "{body}"
    );
    assert_eq!(
        sample(
            &body,
            r#"ravel_compat_active_write_version{mode="all",family="rlog"}"#
        ),
        Some(f64::from(policy_version)),
        "{body}"
    );
    let age =
        sample(&body, r#"ravel_compat_view_age_seconds{mode="all"}"#).expect("view age rendered");
    assert!(
        (0.0..600.0).contains(&age),
        "a view read at startup is younger than the horizon: {age}"
    );
    for reason in [
        "view_stale",
        "active_version_not_writable",
        "compat_record_missing",
    ] {
        assert_eq!(
            sample(
                &body,
                &format!(
                    r#"ravel_compat_publish_refused_total{{mode="all",family="rlog",reason="{reason}"}}"#
                ),
            ),
            Some(0.0),
            "{reason}: {body}"
        );
    }
    assert_eq!(
        sample(&body, r#"ravel_compat_refresh_failures_total{mode="all"}"#),
        Some(0.0),
        "{body}"
    );

    // The transition the tick wrote carries the policy's version in its
    // trailer and in its commit record.
    let after = alert_objects(store.as_ref()).await;
    let written: Vec<_> = after
        .iter()
        .filter(|object| !before.contains(object))
        .collect();
    assert!(!written.is_empty(), "the tick wrote an alert object");
    for (_, trailer, segment_format_version) in &written {
        assert_eq!(*trailer, policy_version);
        assert_eq!(*segment_format_version, u32::from(policy_version));
    }

    // `rlog` is recorded as activated, so the evaluator holds its lease parked.
    let lease = store
        .get(
            &alert_lease_key(&TenantId::new("acme").hash()),
            GetRange::Full,
        )
        .await
        .expect("lease written");
    let lease = AlertLease::from_slice(&lease.data).expect("lease parses");
    assert!(lease.is_parked(), "{lease:?}");
    assert_eq!(lease.compat_epoch, 2);
    assert!(lease.compat_expiry_ns > 0, "{lease:?}");

    running.shutdown().await.expect("graceful shutdown");
}

//! A real server with alert rules configured refuses its alert tick when the
//! tenant's alert history holds an object written by a newer build, one whose
//! RLOG trailer version is above the newest this build reads (ADR-4233
//! decision 4, hazard H3).
//!
//! The object is a real alert transition object, encoded by the same
//! `ravel_alerting::encode_record_object` the evaluator writes with, published
//! through a real commit record whose content hash matches the rewritten
//! bytes. The server is started over that store and scraped over HTTP until
//! its alert loop has refused a tick. The older version case is
//! `alerting_pre_bump_history.rs`, in its own binary because alert metrics are
//! process-global.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_object_store::memory::MemoryStore;

#[path = "support/alert_history.rs"]
mod alert_history;

use alert_history::{
    EVALUATED, HISTORY_UNAVAILABLE, NEWER_VERSION, REFUSED, SKIPPED, sample, scrape_until_positive,
    seed_alert_object_at_version, start_server,
};

/// Watch it fail: make the `UnsupportedVersion` arm in
/// `AlertEvaluator::fold_commit_entries` skip a newer version as it skips an
/// older one. Ticks then evaluate, the refusal counter stays 0 and the poll
/// runs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newer_version_alert_object_refuses_the_tick() {
    let store = Arc::new(MemoryStore::new());
    seed_alert_object_at_version(store.as_ref(), NEWER_VERSION).await;
    let running = start_server(Arc::clone(&store)).await;

    let body = scrape_until_positive(&running, REFUSED).await;

    let refused = sample(&body, REFUSED).expect("refusal counter rendered");
    assert!(
        refused >= 1.0,
        "the newer-version object refused the tick and was counted:\n{body}"
    );
    assert!(
        sample(&body, HISTORY_UNAVAILABLE).is_some_and(|unavailable| unavailable >= 1.0),
        "the refused tick reported history_unavailable:\n{body}"
    );
    assert_eq!(
        sample(&body, EVALUATED),
        Some(0.0),
        "no tick evaluated rules over the incomplete fold:\n{body}"
    );
    assert_eq!(
        sample(&body, SKIPPED),
        Some(0.0),
        "the newer-version object was not skipped:\n{body}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

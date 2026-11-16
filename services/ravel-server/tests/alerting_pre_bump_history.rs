//! A real server with alert rules configured keeps evaluating them when the
//! tenant's alert history holds an object written before an RLOG format bump
//! (ADR-2708 decision D6, issue #2713).
//!
//! The object is a real alert transition object, encoded by the same
//! `ravel_alerting::encode_record_object` the evaluator writes with, published
//! through a real commit record, and then rewritten to a trailer version older
//! than this build reads. The server is started over that store and scraped
//! over HTTP until its alert loop has completed an evaluated tick. The newer
//! version case is `alerting_newer_version_history.rs`, in its own binary
//! because alert metrics are process-global.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_object_store::memory::MemoryStore;

#[path = "support/alert_history.rs"]
mod alert_history;

use alert_history::{
    EVALUATED, HISTORY_UNAVAILABLE, PRE_BUMP_VERSION, REFUSED, SKIPPED, sample,
    scrape_until_positive, seed_alert_object_at_version, start_server,
};

/// Watch it fail: delete the `UnsupportedVersion` arm in
/// `AlertEvaluator::fold_commit_entries`, or split it on
/// `version != SUPPORTED_VERSIONS.newest()` so an older version refuses too.
/// Every tick then ends in `history_unavailable`, the evaluated count stays 0
/// and the poll runs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pre_bump_alert_object_does_not_stop_rule_evaluation() {
    let store = Arc::new(MemoryStore::new());
    seed_alert_object_at_version(store.as_ref(), PRE_BUMP_VERSION).await;
    let running = start_server(Arc::clone(&store)).await;

    let body = scrape_until_positive(&running, EVALUATED).await;

    let evaluated = sample(&body, EVALUATED).expect("evaluated tick series rendered");
    assert!(
        evaluated >= 1.0,
        "a tick evaluated the rules despite the pre-bump object:\n{body}"
    );
    let skipped = sample(&body, SKIPPED).expect("skip counter rendered");
    assert!(
        skipped >= 1.0,
        "the pre-bump object was skipped and counted:\n{body}"
    );
    assert_eq!(
        sample(&body, HISTORY_UNAVAILABLE),
        Some(0.0),
        "no tick aborted on the pre-bump object:\n{body}"
    );
    assert_eq!(
        sample(&body, REFUSED),
        Some(0.0),
        "the pre-bump object refused no tick:\n{body}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

//! `ravel-cli catalog fold --retention-default` (issue #4071): the
//! deployment-default retention window reaches the fold as
//! `default_retention_ns`, so a tenant with no `retention_ns` of its own gets
//! the retention-frontier reconcile and a tombstone far outside the fixed
//! reconcile window is observed.
//!
//! Driven in-process against a `MemoryStore` at a fixed injected `now`, for
//! the reason `tests/catalog_fold_max_flush_lifetime.rs` gives.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::RetentionTombstone;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_MINUTE: i64 = 60_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const SHARD_COUNT: u32 = 1;
const RETIRED_HOUR: u32 = 5;
const RECENT_HOUR: u32 = 200;

/// A clock reading that seals `hour` under the default seal margin
/// (max_flush_lifetime 1h + clock skew 5m + fold safety 15m = 80m past the
/// hour's end), with ten minutes to spare.
fn now_sealing(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + 90 * NS_PER_MINUTE
}

async fn publish_segment(store: &MemoryStore, tenant: &str, seq: u64, hour: u32) {
    let created_ns = i64::from(hour) * NS_PER_HOUR + NS_PER_MINUTE;
    let payload = format!("seg-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let commit = record::build(NewCommitRecord {
        tenant_hash: TenantId::new(tenant).hash(),
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_ns - 1_000,
        max_event_ts_ns: created_ns,
        min_ingest_ts_ns: created_ns - 1_000,
        max_ingest_ts_ns: created_ns,
        segment_format_version: 1,
        created_unix_ns: created_ns,
        ingest_hour_bucket: hour,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish");
}

async fn publish_tombstone(store: &MemoryStore, tenant: &str, hour: u32) {
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: TenantId::new(tenant).hash().0.to_vec(),
        signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
        shard: 0,
        ingest_hour_bucket: hour,
        retired_at_ns: 0,
        retention_window_ns: 0,
        record_count_observed: 0,
    };
    let key = keys::retention_tombstone_key_for(&tombstone).expect("tombstone key");
    store
        .put(
            &key,
            record::encode_tombstone(&tombstone),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put tombstone");
}

async fn fold(
    store: &Arc<MemoryStore>,
    tenant: &str,
    retention_default_ns: Option<i64>,
    now: i64,
) -> ravel_catalog::FoldReport {
    catalog::fold_with_retention_default(
        Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        None,
        false,
        false,
        ravel_catalog::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
        false,
        None,
        retention_default_ns,
        now,
        true,
    )
    .await
    .expect("fold succeeds")
    .0
}

/// One entry at hour 5 and one at hour 200 are folded; hour 5 is then retired
/// by a tombstone about 195 hours behind the watermark, far outside the fixed
/// 26-hour reconcile window behind hour 200. The second fold runs with
/// `retention_default_ns` as given, and its report is returned.
async fn second_fold_after_out_of_window_tombstone(
    tenant: &str,
    retention_default_ns: Option<i64>,
) -> ravel_catalog::FoldReport {
    let store = Arc::new(MemoryStore::new());
    publish_segment(&store, tenant, 1, RETIRED_HOUR).await;
    publish_segment(&store, tenant, 2, RECENT_HOUR).await;
    let first = fold(&store, tenant, None, now_sealing(RECENT_HOUR)).await;
    assert_eq!(first.entry_count, 2, "the first fold names both hours");
    assert_eq!(first.watermark_hour, Some(RECENT_HOUR));

    publish_tombstone(&store, tenant, RETIRED_HOUR).await;
    fold(
        &store,
        tenant,
        retention_default_ns,
        now_sealing(RECENT_HOUR + 2),
    )
    .await
}

/// With `--retention-default 200h` and no tenant `retention_ns`, the frontier
/// sits near hour 29 (now minus 200h plus the 25h5m protection horizon), so
/// the frontier reconcile lists hour 5, applies its tombstone, and hour 5's
/// entry leaves HEAD. The same fold without the flag has no retention window,
/// lists no frontier hour, and HEAD keeps naming hour 5. Passing `None` in
/// place of `default_retention_ns` at the `fold_with_seal_through` call in
/// `catalog::fold_core` (the flag ignored) fails the first half's assertions.
#[tokio::test]
async fn a_cli_fold_with_retention_default_reconciles_a_frontier_tombstone() {
    let retention_default_ns = ravel_cli::parse_retention_default_ns("200h").expect("200h parses");
    assert_eq!(retention_default_ns, 200 * NS_PER_HOUR);

    let with_flag = second_fold_after_out_of_window_tombstone(
        "cli-fold-retention-default-on",
        Some(retention_default_ns),
    )
    .await;
    assert_eq!(
        with_flag.frontier_hours_reconciled, 1,
        "the frontier reconcile lists hour 5 alone"
    );
    assert_eq!(with_flag.frontier_hours_deferred, 0);
    assert_eq!(
        with_flag.entry_count, 1,
        "the tombstoned hour 5 leaves HEAD and hour 200 stays"
    );

    let without_flag =
        second_fold_after_out_of_window_tombstone("cli-fold-retention-default-off", None).await;
    assert_eq!(
        without_flag.frontier_hours_reconciled, 0,
        "no retention window, so no frontier reconcile"
    );
    assert_eq!(
        without_flag.entry_count, 2,
        "without the window the fold never observes hour 5's tombstone"
    );
}

/// The run-time entry point refuses a zero window, which the flag's parser
/// already refuses, naming the flag.
#[tokio::test]
async fn fold_with_retention_default_refuses_a_zero_window() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-retention-default-zero";
    publish_segment(&store, tenant, 1, RECENT_HOUR).await;
    let err = catalog::fold_with_retention_default(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        None,
        false,
        false,
        ravel_catalog::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
        false,
        None,
        Some(0),
        now_sealing(RECENT_HOUR),
        true,
    )
    .await
    .expect_err("a zero window is refused");
    assert!(
        err.to_string().contains("--retention-default"),
        "the refusal names the flag: {err}"
    );
}

/// The flag's parser refuses 0 and a value that does not parse, naming the
/// flag in each message, and the binary's help lists the flag.
#[test]
fn retention_default_parse_refusals_name_the_flag() {
    for value in ["0", "0s", "thirty days", "300years"] {
        let err = ravel_cli::parse_retention_default_ns(value).expect_err("refused");
        assert!(
            err.contains("--retention-default"),
            "{value}: the refusal names the flag: {err}"
        );
    }
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["catalog", "fold", "--help"])
        .output()
        .expect("run ravel-cli");
    assert!(out.status.success());
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("--retention-default <DURATION>"), "{help}");

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args([
            "catalog",
            "fold",
            "--tenant",
            "acme",
            "--retention-default",
            "0s",
        ])
        .output()
        .expect("run ravel-cli");
    assert!(!out.status.success(), "the binary refuses a zero window");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--retention-default '0s' must be greater than zero"),
        "{stderr}"
    );
}

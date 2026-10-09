//! `ravel-cli catalog fold --writers-stopped` (ADR-2677 decision 1, issue
//! #2679): the operator asserts no writer will publish into the current hour
//! or any earlier one, and the fold seals through the hour it runs in without
//! moving its clock.
//!
//! Driven in-process against one shared `MemoryStore` at a fixed injected
//! `now`, for the reason `tests/catalog_fold_max_flush_lifetime.rs` gives; the
//! help text, which clap produces, is read from the real binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::Command;
use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_MINUTE: i64 = 60_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const SHARD_COUNT: u32 = 2;
const PREVIOUS_HOUR_RECORDS: u64 = 4;
const CURRENT_HOUR_RECORDS: u64 = 2;

/// 30 minutes into the current unix hour `H`: the default margin seals
/// `H - 2` and `--max-flush-lifetime 0s` seals `H - 1`, so only the seal
/// through `H` reaches the current hour's records.
fn fixed_now() -> i64 {
    let real = ravel_cli::now_ns().expect("system clock readable");
    real.div_euclid(NS_PER_HOUR) * NS_PER_HOUR + 30 * NS_PER_MINUTE
}

fn hour_of(now_ns: i64) -> u32 {
    u32::try_from(now_ns.div_euclid(NS_PER_HOUR)).expect("fits u32")
}

async fn publish_segment(store: &MemoryStore, tenant: &str, shard: u32, seq: u64, created_ns: i64) {
    let tenant_hash = TenantId::new(tenant).hash();
    let payload = format!("seg-{shard}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
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
        ingest_hour_bucket: hour_of(created_ns),
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

/// Records in the hour before now and in the current, still-open hour.
async fn seed_tenant(store: &MemoryStore, tenant: &str, now_ns: i64) {
    let previous_hour_ts = now_ns - NS_PER_HOUR - 20 * NS_PER_MINUTE;
    let current_hour_ts = now_ns - 25 * NS_PER_MINUTE;
    assert_eq!(hour_of(previous_hour_ts) + 1, hour_of(now_ns));
    assert_eq!(hour_of(current_hour_ts), hour_of(now_ns));
    for seq in 0..PREVIOUS_HOUR_RECORDS {
        let shard = u32::try_from(seq % u64::from(SHARD_COUNT)).expect("fits u32");
        publish_segment(store, tenant, shard, seq + 1, previous_hour_ts).await;
    }
    for seq in 0..CURRENT_HOUR_RECORDS {
        let shard = u32::try_from(seq % u64::from(SHARD_COUNT)).expect("fits u32");
        publish_segment(
            store,
            tenant,
            shard,
            PREVIOUS_HOUR_RECORDS + seq + 1,
            current_hour_ts,
        )
        .await;
    }
}

async fn head_created_unix_ns(store: &MemoryStore, tenant: &str) -> i64 {
    let key = format!(
        "t/{}/catalog/{}/HEAD",
        TenantId::new(tenant).hash().to_hex(),
        Signal::Metrics.key_prefix()
    );
    let got = store.get(&key, GetRange::Full).await.expect("HEAD present");
    ravel_catalog::decode_head(&got.data)
        .expect("HEAD decodes")
        .created_unix_ns
}

async fn fold(
    store: &Arc<MemoryStore>,
    tenant: &str,
    max_flush_lifetime_ns: Option<i64>,
    writers_stopped: bool,
    now: i64,
    json: bool,
) -> (ravel_catalog::FoldReport, String) {
    catalog::fold_with_writers_stopped(
        Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        max_flush_lifetime_ns,
        writers_stopped,
        now,
        json,
    )
    .await
    .expect("fold succeeds")
}

/// `--writers-stopped` seals the open hour `H` the fold runs in: every record
/// of both hours is folded, the report names `H` as the seal-through hour, and
/// HEAD is stamped with the real `now`, not a clock moved forward far enough
/// for the margin to seal `H`. A default fold afterwards is a no-op at `H`.
#[tokio::test]
async fn writers_stopped_seals_the_hour_the_fold_runs_in_without_moving_the_clock() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-writers-stopped";
    let now = fixed_now();
    let hour = hour_of(now);
    seed_tenant(&store, tenant, now).await;

    let (report, printed) = fold(&store, tenant, None, true, now, false).await;
    assert!(!report.no_op);
    assert_eq!(report.watermark_hour, Some(hour));
    assert_eq!(report.seal_through_hour, Some(hour));
    assert_eq!(
        report.entry_count,
        PREVIOUS_HOUR_RECORDS + CURRENT_HOUR_RECORDS,
        "the current hour's records are sealed too"
    );
    assert!(
        printed.contains(&format!("seal_through_hour: Some({hour})\n")),
        "the text report names the seal-through hour, got:\n{printed}"
    );
    assert!(
        printed.contains("seal_margin: 1h 20m\n"),
        "the margin itself is unchanged, got:\n{printed}"
    );
    assert_eq!(head_created_unix_ns(&store, tenant).await, now);

    let (after, after_printed) = fold(&store, tenant, None, false, now, true).await;
    assert!(after.no_op, "the default margin has nothing left to seal");
    assert_eq!(after.watermark_hour, Some(hour));
    assert_eq!(after.seal_through_hour, None);
    let json: serde_json::Value = serde_json::from_str(&after_printed).expect("one JSON doc");
    assert_eq!(json["seal_through_hour"], serde_json::Value::Null);
}

/// The flag combines with `--max-flush-lifetime`: the margin is the overridden
/// one (`20m`, sealing `H - 1`) and the seal-through hour still lifts the
/// watermark to `H`. `--json` carries the seal-through hour.
#[tokio::test]
async fn writers_stopped_combines_with_max_flush_lifetime_and_reports_in_json() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-writers-stopped-mfl";
    let now = fixed_now();
    let hour = hour_of(now);
    seed_tenant(&store, tenant, now).await;

    let (report, printed) = fold(&store, tenant, Some(0), true, now, true).await;
    assert_eq!(report.watermark_hour, Some(hour));
    assert_eq!(report.seal_through_hour, Some(hour));
    assert_eq!(
        report.entry_count,
        PREVIOUS_HOUR_RECORDS + CURRENT_HOUR_RECORDS
    );
    let json: serde_json::Value = serde_json::from_str(&printed).expect("one JSON doc");
    assert_eq!(json["seal_through_hour"], serde_json::json!(hour));
    assert_eq!(json["watermark_hour"], serde_json::json!(hour));
    assert_eq!(json["seal_margin"], "20m");
    assert_eq!(head_created_unix_ns(&store, tenant).await, now);
}

fn help_text(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(args)
        .arg("--help")
        .output()
        .expect("ravel-cli runs");
    assert!(output.status.success(), "--help exits 0");
    String::from_utf8(output.stdout).expect("stdout is utf-8")
}

/// Both flags carry the UNSAFE wording and the assertion the operator (or the
/// loader) makes by passing them.
#[test]
fn the_seal_flags_help_states_the_assertion_and_the_unsafe_case() {
    let fold = help_text(&["catalog", "fold"]);
    assert!(fold.contains("--writers-stopped"), "{fold}");
    assert!(
        fold.contains("no writer will publish into the current hour or any earlier one"),
        "{fold}"
    );
    assert!(fold.contains("UNSAFE under a live writer"), "{fold}");

    let load = help_text(&["load"]);
    assert!(load.contains("--fold-after-load"), "{load}");
    assert!(
        load.contains("no other writer will publish into any hour up to and including"),
        "{load}"
    );
    assert!(load.contains("UNSAFE under a live writer"), "{load}");
}

/// `--fold-after-load` folds only logs; a metrics or spans load is refused
/// before any row is read, naming the manual fold instead. The paths do not
/// exist, so a refusal that came after the Parquet or mapping read would
/// report that read's error instead.
#[test]
fn fold_after_load_refuses_metrics_and_spans_loads() {
    for signal in ["metrics", "spans"] {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
        for (key, _) in std::env::vars() {
            if key.starts_with("RAVEL_") {
                cmd.env_remove(key);
            }
        }
        let output = cmd
            .args(["--store", "memory", "--tenant-hash-unkeyed", "load"])
            .args(["--parquet", "/nonexistent/in.parquet"])
            .args(["--tenant", "t", "--mapping", "/nonexistent/m.toml"])
            .args(["--signal", signal, "--fold-after-load"])
            .output()
            .expect("ravel-cli runs");
        assert!(!output.status.success(), "a {signal} load must be refused");
        let stderr = String::from_utf8(output.stderr).expect("stderr is utf-8");
        assert!(
            stderr.contains(&format!(
                "--fold-after-load supports only --signal logs; this {signal} load was refused \
                 before any row was read or written"
            )),
            "{stderr}"
        );
        assert!(
            stderr.contains(&format!(
                "ravel-cli catalog fold --signal {signal} --writers-stopped"
            )),
            "{stderr}"
        );
    }
}

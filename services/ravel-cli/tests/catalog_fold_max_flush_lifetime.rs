//! `ravel-cli catalog fold --max-flush-lifetime <DURATION>` (issue #786): the
//! same seal-margin override `maintain compact-bucket` and `compact-tenant`
//! already carry, so an operator whose writer process has exited can fold a
//! freshly loaded tenant instead of waiting out the default 1h20m margin.
//!
//! Driven in-process against one shared `MemoryStore` at a fixed injected
//! `now`, the same pattern (and for the same reason) as `tests/catalog.rs` and
//! `tests/catalog_signal.rs`: a subprocess-per-invocation of the binary gets
//! its own empty in-memory store, so a fold -> fold -> fold sequence over one
//! tenant cannot be built that way without a persistent S3/MinIO backend,
//! unavailable here. The one case that does need the real binary (the flag's
//! rejection text, produced by clap, not by a library call) runs as a
//! subprocess at the bottom of this file.

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
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_MINUTE: i64 = 60_000_000_000;

/// Every fixture here builds its own `MemoryStore`, which is the explicit
/// `--store memory` case (issue #1024): the fold reports `store: memory` and
/// an empty result stays a success, unlike a walk on the defaulted store.
const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const SHARD_COUNT: u32 = 2;

/// Records published into the hour before the current one, split evenly across
/// [`SHARD_COUNT`] shards: two commit buckets, four entries.
const PREVIOUS_HOUR_RECORDS: u64 = 4;
/// Records published into the current (still open) hour, one per shard. No
/// margin, however small, seals the hour a record was just written into, so
/// these are the entries an override must still leave out.
const CURRENT_HOUR_RECORDS: u64 = 2;

/// The whole test runs at one injected instant: 30 minutes into the current
/// unix hour `H`. Quantizing to the hour is what makes the expected watermarks
/// below exact rather than dependent on the minute the test happens to run in.
/// With the default margin (1h + 5m + 15m) the sealed watermark at this instant
/// is `H - 2`; with `--max-flush-lifetime 0s` (5m + 15m) it is `H - 1`. Both
/// hold for any offset from 20 to 59 minutes into the hour; 30 sits in the
/// middle of that band.
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

/// A tenant whose whole history is the current hour and the one before it:
/// exactly the shape a bulk load that has just finished leaves behind.
async fn seed_tenant(store: &MemoryStore, tenant: &str, now_ns: i64) {
    let previous_hour_ts = now_ns - NS_PER_HOUR - 20 * NS_PER_MINUTE;
    let current_hour_ts = now_ns - 25 * NS_PER_MINUTE;
    assert_eq!(
        hour_of(previous_hour_ts) + 1,
        hour_of(now_ns),
        "the fixture's older records must land in the hour immediately before now"
    );
    assert_eq!(
        hour_of(current_hour_ts),
        hour_of(now_ns),
        "the fixture's newer records must land in the current, still-open hour"
    );

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

/// The point of #786: on a tenant whose writers have exited, the default fold
/// seals nothing (its watermark sits an hour behind every bucket the load
/// wrote), and `--max-flush-lifetime 0s` seals the finished hour instead, while
/// the clock-skew allowance and the fold safety margin still hold back the hour
/// that is still open.
///
/// Red demonstration: flip `OVERRIDE` below to `None` (the pre-#786 behavior,
/// where the fold had no such flag and always ran on the default margin).
#[tokio::test]
async fn max_flush_lifetime_zero_seals_the_finished_hour_a_default_fold_leaves_alone() {
    const OVERRIDE: Option<i64> = Some(0);

    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-mfl";
    let now = fixed_now();
    let hour = hour_of(now);
    seed_tenant(&store, tenant, now).await;

    // 1. The default fold. Its watermark is `hour - 2`, below every bucket this
    //    tenant has, so it lists nothing and publishes an empty HEAD.
    let (default_report, default_printed) = catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        None,
        now,
    )
    .await
    .expect("default fold succeeds");

    assert_eq!(
        default_report.watermark_hour,
        Some(hour - 2),
        "the default margin is 1h + 5m + 15m, which at 30 minutes past the hour \
         seals only up to hour - 2"
    );
    assert_eq!(default_report.previous_watermark_hour, None);
    assert_eq!(
        default_report.buckets_folded, 0,
        "every bucket this tenant wrote sits above the default watermark"
    );
    assert_eq!(
        default_report.entry_count, 0,
        "a default fold on a just-finished load covers none of its objects"
    );
    assert!(
        default_printed.contains("seal_margin: 1h 20m\n"),
        "the default fold reports the default margin, got:\n{default_printed}"
    );
    // Issue #1024: the effective store heads every walk-shaped command's
    // report, so a fold that seals nothing can never hide which store it read.
    assert!(
        default_printed.starts_with("store: memory\n"),
        "the fold report must open with the effective store, got:\n{default_printed}"
    );

    // 2. The same fold with the override. `0s` removes only the flush-lifetime
    //    term: 5m + 15m still stand, so the current hour stays unsealed.
    let (override_report, override_printed) = catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        OVERRIDE,
        now,
    )
    .await
    .expect("override fold succeeds");

    assert_eq!(
        override_report.watermark_hour,
        Some(hour - 1),
        "with the flush-lifetime term removed the margin is 5m + 15m, which \
         seals the hour that just ended and no further"
    );
    assert_eq!(
        override_report.previous_watermark_hour,
        Some(hour - 2),
        "the override advances the watermark from where the default fold left it"
    );
    assert_eq!(
        override_report.buckets_folded, 2,
        "one commit bucket per shard for hour - 1; the current hour's two \
         buckets are not listed"
    );
    assert_eq!(
        override_report.entry_count, PREVIOUS_HOUR_RECORDS,
        "exactly the finished hour's records are folded, not the current \
         hour's {CURRENT_HOUR_RECORDS}"
    );
    assert!(
        override_printed.contains("seal_margin: 20m\n"),
        "the fold reports the margin the override left it with, got:\n{override_printed}"
    );
}

/// A default fold run after an override fold must not undo it. The override's
/// watermark is ahead of what the default margin would compute, and the fold's
/// watermark is monotonic: the later call reports a no-op at the watermark
/// already reached, not a rewind to `hour - 2`.
#[tokio::test]
async fn a_default_fold_after_an_override_fold_is_a_no_op_at_the_reached_watermark() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-mfl-noop";
    let now = fixed_now();
    let hour = hour_of(now);
    seed_tenant(&store, tenant, now).await;

    let (override_report, _) = catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        Some(0),
        now,
    )
    .await
    .expect("override fold succeeds");
    assert_eq!(override_report.watermark_hour, Some(hour - 1));
    assert_eq!(override_report.entry_count, PREVIOUS_HOUR_RECORDS);

    let (second_report, second_printed) = catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        None,
        now,
    )
    .await
    .expect("second, default fold succeeds");

    assert!(second_report.no_op, "there is nothing new to seal");
    assert_eq!(
        second_report.previous_watermark_hour,
        Some(hour - 1),
        "the watermark the override reached"
    );
    assert_eq!(
        second_report.watermark_hour,
        Some(hour - 1),
        "a default fold must not move the watermark back to hour - 2"
    );
    assert_eq!(second_report.buckets_folded, 0);
    assert_eq!(
        second_report.entry_count, 0,
        "a no-op fold writes no part and reports no entries; the published \
         HEAD still names the override's"
    );
    assert!(
        second_printed.contains("seal_margin: 1h 20m\n"),
        "the second fold reports its own (default) margin, got:\n{second_printed}"
    );
}

fn flag_rejection_stderr(args: &[String]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory"])
        .args(args)
        .output()
        .expect("ravel-cli runs");
    assert!(
        !output.status.success(),
        "a malformed --max-flush-lifetime must be rejected, not accepted"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf-8");
    stderr
        .lines()
        .find(|line| line.contains("invalid value"))
        .unwrap_or_else(|| panic!("no clap value error in stderr:\n{stderr}"))
        .to_string()
}

/// `catalog fold` rejects a malformed duration, and a negative one, with the
/// text `maintain compact-bucket` produces: both flags run through the one
/// `ravel_cli::parse_max_flush_lifetime_ns`, so an operator moving between the
/// two commands never has to learn a second grammar.
///
/// Red demonstration: give `catalog fold`'s `max_flush_lifetime` field no
/// `value_parser` (parse it as a bare `String`) and the malformed value is
/// accepted outright, so `flag_rejection_stderr`'s exit-status assertion fails.
#[test]
fn the_fold_flag_rejects_the_same_values_as_the_compactor_flag_with_the_same_text() {
    for (value, expected) in [
        ("banana", "expected number at 0"),
        ("-1s", "expected number at 0"),
    ] {
        // `--flag=value`, not `--flag value`: clap reads a leading `-` in the
        // separated form as the start of the next argument, so the negative
        // case would never reach the value parser at all.
        let flag = format!("--max-flush-lifetime={value}");
        let fold = flag_rejection_stderr(&[
            "catalog".to_string(),
            "fold".to_string(),
            "--tenant".to_string(),
            "t".to_string(),
            flag.clone(),
        ]);
        let compact = flag_rejection_stderr(&[
            "maintain".to_string(),
            "compact-bucket".to_string(),
            "--tenant".to_string(),
            "t".to_string(),
            "--signal".to_string(),
            "metrics".to_string(),
            "--shard".to_string(),
            "0".to_string(),
            "--hour".to_string(),
            "0".to_string(),
            flag,
        ]);
        assert_eq!(
            fold, compact,
            "the two commands must reject '{value}' with identical text"
        );
        assert_eq!(
            fold,
            format!(
                "error: invalid value '{value}' for '--max-flush-lifetime <DURATION>': \
                 invalid --max-flush-lifetime '{value}': {expected}"
            ),
            "unexpected rejection text for '{value}'"
        );
    }
}

/// A lifetime near `i64::MAX` would overflow the seal margin (lifetime +
/// clock-skew + fold-safety additions); the fold must refuse with a clear
/// error before touching the catalog, never wrap into a bogus watermark.
#[tokio::test]
async fn a_lifetime_that_overflows_the_seal_margin_is_refused() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-mfl-overflow";
    let now = fixed_now();
    seed_tenant(&store, tenant, now).await;

    let err = catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SHARD_COUNT,
        SignalArg::Metrics,
        Some(i64::MAX),
        now,
    )
    .await
    .expect_err("an overflowing seal margin must be refused");
    let text = err.to_string();
    assert!(
        text.contains("--max-flush-lifetime is too large"),
        "the error names the flag and the overflow, got: {text}"
    );
    let keys: Vec<String> = store
        .list("t/", None)
        .await
        .expect("list")
        .objects
        .into_iter()
        .map(|o| o.key)
        .collect();
    assert!(
        keys.iter().all(|k| !k.contains("/catalog/")),
        "refusal happens before any catalog write; found {keys:?}"
    );
}

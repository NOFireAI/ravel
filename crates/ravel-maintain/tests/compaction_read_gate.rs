//! ADR-1702 task 10: compaction's decode and re-encode run on the read gate,
//! and the gated compaction writes byte-for-byte what the inline one writes.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use common::*;
use ravel_commit::keys;
use ravel_cpu_gate::{CpuGateConfig, InstantClock, ReadGate, ReadSite};
use ravel_maintain::read_gate::MaintainReadGate;
use ravel_maintain::{CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::list_all;
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// Three L0 inputs over three series: `m{k=a}` is in the first two, so its
/// runs are decoded, merged and re-encoded; `m{k=b}` and `m{k=c}` are in one
/// input each and take the single-run path. The default part target holds all
/// three series in one part.
fn specs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(42),
            5,
            1,
            vec![
                raw_series("m", &[("k", "b")], &[(1_000, 1.0), (5_000, -0.0)]),
                raw_series("m", &[("k", "a")], &[(2_000, f64::NAN)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(7),
            5,
            2,
            vec![raw_series("m", &[("k", "a")], &[(3_000, 2.5)])],
        ),
        InputSpec::new(
            Uuid::from_u128(7),
            5,
            1,
            vec![raw_series("m", &[("k", "c")], &[(4_000, 9.0)])],
        ),
    ]
}

/// One job per input catalog decode (3), per output series' materialization
/// (3: `m{k=a}`, `m{k=b}`, `m{k=c}`), and per output part's encode (1).
const EXPECTED_COMPACTION_JOBS: u64 = 3 + 3 + 1;

/// Compacts the fixture bucket under `config` and returns the compaction
/// record key and bytes, then every part's key and bytes.
async fn compact_once(config: &CompactorConfig) -> Vec<(String, Vec<u8>)> {
    let store = MemoryStore::new();
    for s in &specs() {
        seed_input(&store, s).await;
    }
    let bucket = bucket();
    compact_bucket(&store, &FixedClock::new(sealed_now_ns()), config, &bucket)
        .await
        .expect("compact");
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let record_key = list_all(&store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .find(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .expect("record key");
    let record_bytes = get_full(&store, &record_key).await.to_vec();
    let record = fetch_compaction_record(&store, &bucket).await;
    let mut out = vec![(record_key, record_bytes)];
    for p in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, p).unwrap();
        let bytes = get_full(&store, &key).await.to_vec();
        out.push((key, bytes));
    }
    out
}

/// With the inline floor at 0, one compaction moves the read gate's
/// `compaction` site by exactly [`EXPECTED_COMPACTION_JOBS`] and runs nothing
/// inline, no other site moves, and the record and part bytes equal the
/// inline baseline's.
///
/// Fails with any one of the three wraps removed (the count reads 4 without
/// the catalog decode's, 4 without the materialization's, 6 without the part
/// encode's), and with the gate dropped from the config (it reads 0).
#[tokio::test]
async fn compaction_decode_and_encode_run_through_the_read_gate() {
    let gate = Arc::new(ReadGate::new(
        CpuGateConfig {
            inline_floor_bytes: 0,
            ..CpuGateConfig::with_permits(2)
        },
        Arc::new(InstantClock::new()),
    ));
    let inline = compact_once(&CompactorConfig::default()).await;
    let gated = compact_once(&CompactorConfig {
        read_gate: MaintainReadGate::new(Arc::clone(&gate)),
        ..CompactorConfig::default()
    })
    .await;

    assert_eq!(inline.len(), 2, "one record and one part");
    assert_eq!(gated, inline, "gated compaction output is byte-identical");
    for site in gate.snapshot().sites {
        let expected = if site.site == ReadSite::Compaction {
            (EXPECTED_COMPACTION_JOBS, 0)
        } else {
            (0, 0)
        };
        assert_eq!((site.jobs, site.inline), expected, "{:?}", site.site);
    }
}

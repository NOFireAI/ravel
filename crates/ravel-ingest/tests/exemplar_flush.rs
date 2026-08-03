//! Exemplars from the wire through the shard buffer into the flushed RSEG v6
//! object's EXEMPLARS section (kind 10, ADR-0047 decisions 1 and 2, issue
//! #474). Reads the section back with `ravel-segment`'s own decoder, so these
//! tests assert on stored bytes rather than on the ingest crate's own view of
//! what it buffered.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, build_labels, tenant};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{
    IngestConfig, IngestExemplar, IngestPoint, IngestRouter, IngestValue, WriteMode,
};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_segment::{ExemplarRecord, ReaderLimits};
use ravel_types::{Exemplar, Label, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// EXEMPLARS section kind (docs/segment-format.md); `ravel_segment` does not
/// re-export its section-kind registry, so the number is named here as the
/// format contract, as `ravel-maintain`'s reader does.
const EXEMPLARS: u32 = 10;

fn flush_on_first_point() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

fn series_id_of(tenant_id: &TenantId, metric: &str) -> SeriesId {
    let labels = build_labels(&[(METRIC_NAME_LABEL, metric)]);
    SeriesId::compute(tenant_id, metric, &labels).expect("series id")
}

fn scalar_point(tenant_id: &TenantId, metric: &str, ts_ns: i64, value: f64) -> IngestPoint {
    let labels = build_labels(&[(METRIC_NAME_LABEL, metric)]);
    IngestPoint {
        series_id: series_id_of(tenant_id, metric),
        labels,
        value: IngestValue::Scalar(Sample { ts_ns, value }),
    }
}

/// One exemplar for `metric`, with a trace id derived from `tag` so each is
/// distinguishable in the decoded section.
fn exemplar(tenant_id: &TenantId, metric: &str, ts_ns: i64, value: f64, tag: u8) -> IngestExemplar {
    IngestExemplar {
        series_id: series_id_of(tenant_id, metric),
        exemplar: Exemplar {
            ts_ns,
            value_bits: value.to_bits(),
            trace_id: [tag; 16],
            span_id: [tag; 8],
            filtered_attributes: vec![Label {
                name: "peer".to_string(),
                value: format!("svc-{tag}"),
            }],
        },
    }
}

/// Flush one batch and return the flushed object's bytes.
async fn flush_and_fetch(
    store: &Arc<dyn ObjectStoreBackend>,
    router: &IngestRouter,
    tenant_id: &TenantId,
    points: Vec<IngestPoint>,
    exemplars: Vec<IngestExemplar>,
) -> bytes::Bytes {
    let receipt = router
        .write_values_with_exemplars(
            tenant_id.clone(),
            points,
            exemplars,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("batch is accepted");
    assert_eq!(receipt.tokens.len(), 1, "one shard flushed");
    let commit_key =
        keys::commit_key_for_token(&tenant_id.hash(), Signal::Metrics, &receipt.tokens[0])
            .expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded = record::decode(&commit_bytes).expect("decode commit record");
    store
        .get(&decoded.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data
}

/// Every EXEMPLARS record of an object, or an empty vec when the section is
/// absent. Also returns whether the section is present at all, which "no
/// exemplars means no section" needs to distinguish from an empty one.
fn read_exemplars(obj: &[u8]) -> (bool, Vec<ExemplarRecord>) {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(obj, limits).expect("opens segment");
    let footer = &loc.footer;
    let present = footer.sections.iter().any(|s| s.kind == EXEMPLARS);
    let section = |kind: u32| -> &[u8] {
        let s = footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("missing section kind {kind}"));
        &obj[s.offset as usize..(s.offset + s.len) as usize]
    };
    if !present {
        return (false, Vec::new());
    }
    let records =
        ravel_segment::decode_exemplars_section(footer, section(1), section(EXEMPLARS), limits)
            .expect("decodes EXEMPLARS");
    (true, records)
}

fn router_with(store: &Arc<dyn ObjectStoreBackend>) -> IngestRouter {
    IngestRouter::new(
        flush_on_first_point(),
        Arc::clone(store),
        Signal::Metrics,
        TestClock::new(BASE_NS),
    )
}

/// Exemplars buffered alongside their samples reach the flushed object's
/// EXEMPLARS section, and the records come out ascending by `(series_index,
/// ts_ns)` -- the sort invariant the reader enforces and a per-series probe
/// relies on (docs/segment-format.md EXEMPLARS).
#[tokio::test]
async fn flushed_exemplars_are_written_ascending() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let router = router_with(&store);
    let t = tenant("acme");

    // Two series, exemplars offered in an order that is neither series- nor
    // timestamp-sorted, and far enough apart in event time to be in distinct
    // cap windows (default 10s) so the cap admits every one.
    let points = vec![
        scalar_point(&t, "alpha", 1_000, 1.0),
        scalar_point(&t, "alpha", 20_000_000_000, 2.0),
        scalar_point(&t, "beta", 1_000, 3.0),
    ];
    let exemplars = vec![
        exemplar(&t, "beta", 1_000, 3.0, 3),
        exemplar(&t, "alpha", 20_000_000_000, 2.0, 2),
        exemplar(&t, "alpha", 1_000, 1.0, 1),
    ];
    let obj = flush_and_fetch(&store, &router, &t, points, exemplars).await;
    let (present, records) = read_exemplars(&obj);
    assert!(present, "the flush must emit an EXEMPLARS section");
    assert_eq!(records.len(), 3, "every admitted exemplar is written");

    let keys: Vec<(u64, i64)> = records.iter().map(|r| (r.series_index, r.ts_ns)).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(
        keys, sorted,
        "records must be ascending by (series_index, ts_ns)"
    );

    // Content survives verbatim: trace/span ids and the filtered attribute.
    let mut tags: Vec<u8> = records.iter().map(|r| r.trace_id[0]).collect();
    tags.sort_unstable();
    assert_eq!(tags, vec![1, 2, 3]);
    for r in &records {
        let tag = r.trace_id[0];
        assert_eq!(r.span_id, [tag; 8]);
        assert_eq!(r.attrs, vec![("peer".to_string(), format!("svc-{tag}"))]);
    }

    router.shutdown().await;
}

/// A flush with no exemplars emits no EXEMPLARS section at all. Absence, not
/// a zero-count section, is the only legal representation of "no exemplars"
/// (ADR-0047 decision 1).
#[tokio::test]
async fn flush_without_exemplars_emits_no_section() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let router = router_with(&store);
    let t = tenant("acme");

    let obj = flush_and_fetch(
        &store,
        &router,
        &t,
        vec![scalar_point(&t, "alpha", 1_000, 1.0)],
        Vec::new(),
    )
    .await;
    let (present, records) = read_exemplars(&obj);
    assert!(
        !present,
        "no exemplars must mean no section, not an empty one"
    );
    assert!(records.is_empty());

    router.shutdown().await;
}

/// An exemplar whose parent sample is not in the flush is not written: the
/// object carries no measurement for it. It must be dropped (and counted), not
/// fail the flush, and not fabricate a series.
#[tokio::test]
async fn exemplar_without_a_parent_sample_is_not_written() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let router = router_with(&store);
    let t = tenant("acme");

    // "orphan" has an exemplar but no sample anywhere in this flush.
    let obj = flush_and_fetch(
        &store,
        &router,
        &t,
        vec![scalar_point(&t, "alpha", 1_000, 1.0)],
        vec![
            exemplar(&t, "alpha", 1_000, 1.0, 1),
            exemplar(&t, "orphan", 1_000, 9.0, 9),
        ],
    )
    .await;

    let (present, records) = read_exemplars(&obj);
    assert!(present);
    assert_eq!(records.len(), 1, "only the exemplar with a parent sample");
    assert_eq!(records[0].trace_id, [1u8; 16]);
    assert_eq!(records[0].series_index, 0, "the one series in the object");

    let snap = router.metrics().snapshot();
    assert_eq!(snap.exemplars_written_total, 1);
    assert_eq!(snap.exemplars_dropped_total, 1);

    router.shutdown().await;
}

/// The flush-scoped cap keeps one exemplar per series per window, and it is
/// the newest one in that window: `ExemplarCap::admit` is first-wins and never
/// retracts, so the flush site owes it a newest-first ordering (ADR-0047
/// decision 2).
#[tokio::test]
async fn cap_keeps_the_newest_exemplar_in_a_window() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let router = router_with(&store);
    let t = tenant("acme");

    // Three exemplars for one series inside one 10s window, offered
    // oldest-first; the newest (tag 3, ts 3_000) must be the survivor.
    let obj = flush_and_fetch(
        &store,
        &router,
        &t,
        vec![
            scalar_point(&t, "alpha", 1_000, 1.0),
            scalar_point(&t, "alpha", 2_000, 2.0),
            scalar_point(&t, "alpha", 3_000, 3.0),
        ],
        vec![
            exemplar(&t, "alpha", 1_000, 1.0, 1),
            exemplar(&t, "alpha", 2_000, 2.0, 2),
            exemplar(&t, "alpha", 3_000, 3.0, 3),
        ],
    )
    .await;

    let (_, records) = read_exemplars(&obj);
    assert_eq!(records.len(), 1, "one exemplar per series per window");
    assert_eq!(records[0].ts_ns, 3_000);
    assert_eq!(records[0].trace_id, [3u8; 16]);

    let snap = router.metrics().snapshot();
    assert_eq!(snap.exemplars_written_total, 1);
    assert_eq!(snap.exemplars_dropped_total, 2);

    router.shutdown().await;
}

/// Exemplar values are stored and read back by bit pattern: a NaN payload and
/// a negative zero survive the flush exactly (repository invariant, and
/// docs/segment-format.md EXEMPLARS states it for this section).
#[tokio::test]
async fn exemplar_values_round_trip_by_bit_pattern() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let router = router_with(&store);
    let t = tenant("acme");

    let nan = f64::from_bits(0x7FF8_0000_0000_00AB);
    let obj = flush_and_fetch(
        &store,
        &router,
        &t,
        vec![
            scalar_point(&t, "alpha", 1_000, 1.0),
            scalar_point(&t, "beta", 1_000, 1.0),
        ],
        vec![
            exemplar(&t, "alpha", 1_000, nan, 1),
            exemplar(&t, "beta", 1_000, -0.0, 2),
        ],
    )
    .await;

    let (_, records) = read_exemplars(&obj);
    assert_eq!(records.len(), 2);
    let by_tag: std::collections::BTreeMap<u8, u64> = records
        .iter()
        .map(|r| (r.trace_id[0], r.value.to_bits()))
        .collect();
    assert_eq!(by_tag[&1], nan.to_bits(), "NaN payload preserved");
    assert_eq!(by_tag[&2], (-0.0f64).to_bits(), "-0.0 preserved");

    router.shutdown().await;
}

/// A strict-mode write whose exemplars route to a shard that receives no
/// points must still succeed, and must not bury that shard as dead.
///
/// The router messages a shard that owns only an exemplar's series, because
/// that shard's buffer may already hold the parent samples from an earlier
/// request in this flush window. When it does not, the flush writes nothing and
/// returns early. Minting a strict ack for such a shard would leave a waiter its
/// flush path has no commit token to answer with, and a dropped oneshot reads to
/// the router as a dead actor: `mark_shard_dead` plus `ShardUnavailable`, so a
/// healthy shard is counted dead forever and the caller sees an error for a
/// write that committed elsewhere. Strict mode acknowledges the points a request
/// sent, and this request sent none to that shard.
///
/// This needs more than one shard, so it builds its own router rather than
/// using `router_with`, whose `shard_count` is 1 and under which the case
/// cannot arise at all.
#[tokio::test]
async fn an_exemplar_only_shard_does_not_fail_a_strict_write() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let router = IngestRouter::new(
        IngestConfig {
            shard_count: 4,
            ..flush_on_first_point()
        },
        Arc::clone(&store),
        Signal::Metrics,
        TestClock::new(BASE_NS),
    );
    let t = tenant("acme");

    let shards = router.shard_count();
    let alpha_shard = ravel_types::shard_for(&series_id_of(&t, "alpha"), shards);
    let other = (0..500)
        .map(|i| format!("beta{i}"))
        .find(|m| ravel_types::shard_for(&series_id_of(&t, m), shards) != alpha_shard)
        .expect("a metric on a different shard");

    let receipt = router
        .write_values_with_exemplars(
            t.clone(),
            vec![scalar_point(&t, "alpha", 1_000, 1.0)],
            vec![
                exemplar(&t, "alpha", 1_000, 1.0, 1),
                exemplar(&t, &other, 1_000, 9.0, 9),
            ],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the write must succeed, not report the shard unavailable");

    assert_eq!(receipt.tokens.len(), 1, "only the shard that took points");
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        0,
        "no healthy shard may be counted dead"
    );

    router.shutdown().await;
}

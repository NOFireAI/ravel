//! The per-bucket compaction driver: seal + trigger checks, then the
//! plan-build-publish pipeline.
//! Stateless and idempotent: a crashed run re-run from scratch reuses
//! content-addressed part keys and converges at the record's
//! `CreateIfAbsent`.

use ravel_object_store::ObjectStoreBackend;
use ravel_types::Signal;

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::codec::{RsegCodec, SegmentCodec};
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::publish::{PublishOutcome, conserve_exact};
use crate::read;
use crate::read::list_bucket;
use crate::rewrite::rewrite_and_publish;
use crate::rlog::RlogCodec;
use crate::rspan_codec::SpanCodec;

/// The result of a `compact_bucket` call. Every variant except
/// [`CompactionOutcome::Compacted`] means the bucket was left untouched, with
/// the reason; the scan driver treats all of them except `NotSealed` as
/// "this hour is done" for cursor advancement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionOutcome {
    /// Not yet sealed: the writer interlock does not yet guarantee a complete
    /// input set. Later hours are also unsealed.
    NotSealed,
    /// A retention tombstone is present: the bucket contributes nothing and is
    /// never compacted (ADR-0019).
    Tombstoned,
    /// A compaction record already exists; nothing to do.
    AlreadyCompacted,
    /// Fewer than `min_compaction_inputs` L0 records; not worth compacting.
    BelowMinInputs { count: usize },
    /// Built and published (or converged / abandoned): `parts` parts written,
    /// `publish` records how the record PUT resolved.
    Compacted {
        parts: usize,
        publish: PublishOutcome,
    },
}

/// Compact one sealed bucket end to end. Safe to call
/// concurrently with other compactors over the same bucket: the record's
/// `CreateIfAbsent` picks a single winner and losers converge.
pub async fn compact_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> Result<CompactionOutcome> {
    let start_ns = clock.now_ns();
    if !bucket.is_sealed(start_ns, config) {
        return Ok(CompactionOutcome::NotSealed);
    }

    let listing = list_bucket(store, bucket).await?;
    if listing.tombstone_key.is_some() {
        return Ok(CompactionOutcome::Tombstoned);
    }
    if !listing.compaction_record_keys.is_empty() {
        return Ok(CompactionOutcome::AlreadyCompacted);
    }
    if listing.commit_keys.len() < config.min_compaction_inputs {
        return Ok(CompactionOutcome::BelowMinInputs {
            count: listing.commit_keys.len(),
        });
    }

    // Everything up to here is signal-generic (seal, tombstone, already-done,
    // and input-count gates on the bucket listing). The plan-build step is the
    // only signal-specific part: dispatch it to the codec for this bucket's
    // signal and run the identical shared pipeline (canonical ordering,
    // input_set_hash, publish) around it (ADR-0032).
    match bucket.signal {
        Signal::Metrics => {
            run_pipeline::<RsegCodec>(store, clock, config, bucket, &listing.commit_keys, start_ns)
                .await
        }
        Signal::Logs => {
            run_pipeline::<RlogCodec>(store, clock, config, bucket, &listing.commit_keys, start_ns)
                .await
        }
        Signal::Spans => {
            run_pipeline::<SpanCodec>(store, clock, config, bucket, &listing.commit_keys, start_ns)
                .await
        }
        // Query-audit records ride RLOG (see `query_audit`), so they compact
        // through the same RLOG codec as logs -- the machinery is reused, only
        // the signal and shard are new. The legal-hold shard
        // (`legal_hold::AUDIT_HOLD_SHARD` = 0) is deliberately excluded: the
        // legal-hold fold (`legal_hold::load_hold_records`) reads L0 commit
        // records only and ignores compaction records, so compacting its records
        // into L1 and letting the superseded sweep delete the L0 originals would
        // silently drop every hold from the fold. Only the query-audit shard
        // gains compaction here.
        Signal::Audit if bucket.shard == crate::query_audit::QUERY_AUDIT_SHARD => {
            run_pipeline::<RlogCodec>(store, clock, config, bucket, &listing.commit_keys, start_ns)
                .await
        }
        Signal::Audit => Err(MaintainError::Invariant(format!(
            "audit compaction is only implemented for the query-audit shard \
             (QUERY_AUDIT_SHARD = {}), never the legal-hold shard; got shard {}",
            crate::query_audit::QUERY_AUDIT_SHARD,
            bucket.shard
        ))),
        other => Err(MaintainError::Invariant(format!(
            "compaction is not implemented for signal {other:?}"
        ))),
    }
}

/// The signal-generic plan-build-publish pipeline, parameterized over the
/// per-signal [`SegmentCodec`]. Compaction is the exact-conservation case of
/// the shared rewrite primitive ([`crate::rewrite::rewrite_and_publish`],
/// ADR-0066 decision 5): it loads and canonically orders the inputs, derives
/// the `input_set_hash`, decodes each input's catalog metadata through the
/// codec, streams the merge into size-capped parts through the codec, and
/// publishes the record with [`conserve_exact`]. Only the two `C::` calls know
/// the on-object format; everything else is identical for every signal.
async fn run_pipeline<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    commit_keys: &[String],
    start_ns: i64,
) -> Result<CompactionOutcome> {
    let outcome = rewrite_and_publish::<C>(
        store,
        clock,
        config,
        bucket,
        commit_keys,
        conserve_exact(),
        start_ns,
    )
    .await?;

    Ok(CompactionOutcome::Compacted {
        parts: outcome.parts,
        publish: outcome.publish,
    })
}

// Re-export the input-listing type so callers (and tests) can inspect a
// bucket without reaching into the module.
pub use read::BucketListing;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    //! Exemplar carry-through for L0-to-L1 metric compaction (ADR-0047
    //! decision 3). Seeds real L0 RSEG v6 objects through the
    //! ingest flush writer, runs the whole `compact_bucket` pipeline over a
    //! `MemoryStore`, and reads the L1 parts' EXEMPLARS sections back with
    //! `ravel-segment`'s own decoder.
    //!
    //! The invariant under test is ADR-0018's overlap harmlessness applied to
    //! exemplars: an L1 object's exemplars are the exact multiset of its
    //! inputs', with only `series_index` remapped. Every assertion here is on
    //! counts, not sets, so any deduplication fails the test.

    use std::collections::BTreeMap;

    use bytes::Bytes;
    use ravel_commit::keys;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CompactionRecord;
    use ravel_segment::{
        ExemplarInput, IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInputV3,
        SeriesValues, VERSION_V7,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::{CompactorConfig, FixedClock};

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;
    /// LABEL_DICT and EXEMPLARS section kinds (docs/segment-format.md); named
    /// here as the format contract, as `read.rs` does.
    const LABEL_DICT: u32 = 1;
    const EXEMPLARS: u32 = 10;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Metrics, SHARD, HOUR)
    }

    /// Past the seal margin for [`HOUR`] under default config.
    fn sealed_now_ns() -> i64 {
        (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    fn labels(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    fn series_id(metric: &str) -> SeriesId {
        SeriesId::compute(&TenantId::new(TENANT), metric, &labels(metric)).expect("series id")
    }

    fn series(metric: &str, samples: &[(i64, f64)]) -> SeriesInputV3 {
        SeriesInputV3 {
            series_id: series_id(metric),
            labels: labels(metric),
            values: SeriesValues::Scalar(
                samples
                    .iter()
                    .map(|(ts_ns, value)| Sample {
                        ts_ns: *ts_ns,
                        value: *value,
                    })
                    .collect(),
            ),
        }
    }

    /// One exemplar for `metric`, tagged so it is distinguishable after the
    /// round trip: `tag` fills the trace and span ids and rides in an
    /// attribute value.
    fn exemplar(metric: &str, ts_ns: i64, value: f64, tag: u8) -> ExemplarInput {
        ExemplarInput {
            series_id: series_id(metric),
            ts_ns,
            value,
            trace_id: [tag; 16],
            span_id: [tag; 8],
            attrs: vec![("peer".to_string(), format!("svc-{tag}"))],
        }
    }

    /// Seed one L0 input (data object + commit record) through the production
    /// ingest flush writer, so the seeded object's EXEMPLARS section is
    /// byte-for-byte what a real flush would have written.
    async fn seed(
        store: &dyn ObjectStoreBackend,
        seq: u64,
        series: Vec<SeriesInputV3>,
        exemplars: Vec<ExemplarInput>,
    ) -> Bytes {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let identity = SegmentIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.to_string(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
        };
        let written =
            SegmentWriter::write_histograms_with_exemplars(series, identity, bounds, exemplars)
                .expect("write L0");
        let content_hash = written.summary.blake3;
        let data_key = keys::data_key(
            &th,
            Signal::Metrics,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");

        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Metrics,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: u32::from(VERSION_V7),
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
        written.bytes
    }

    /// The single compaction record in the bucket plus every L1 part's bytes,
    /// in record (ascending series-range) order.
    async fn read_output(store: &dyn ObjectStoreBackend) -> (CompactionRecord, Vec<Bytes>) {
        use prost::Message;
        let b = bucket();
        let prefix =
            keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
                .expect("prefix");
        let metas = list_all(store, &prefix).await.expect("list");
        let mut rec_keys: Vec<String> = metas
            .into_iter()
            .map(|m| m.key)
            .filter(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .collect();
        rec_keys.sort();
        assert_eq!(rec_keys.len(), 1, "expected exactly one compaction record");
        let got = store
            .get(&rec_keys[0], GetRange::Full)
            .await
            .expect("get record");
        let record = CompactionRecord::decode(got.data.as_ref()).expect("decode record");
        let mut parts = Vec::new();
        for p in &record.parts {
            let key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
            parts.push(
                store
                    .get(&key, GetRange::Full)
                    .await
                    .expect("get part")
                    .data,
            );
        }
        (record, parts)
    }

    /// One exemplar in the canonical, order-independent form the multiset
    /// assertions compare: the series it names (not its index, which is
    /// object-relative), the timestamp, the value's bit pattern (never `==`:
    /// NaN and -0.0 are significant), both ids, and its attributes.
    type Canon = ([u8; 16], i64, u64, [u8; 16], [u8; 8], Vec<(String, String)>);

    /// Whether an object carries an EXEMPLARS section at all, plus every
    /// record in it canonicalized against the object's own SERIES_IDS
    /// ordering, and the raw `(series_index, ts_ns)` sort keys in stored
    /// order.
    fn read_exemplars(obj: &[u8]) -> (bool, Vec<Canon>, Vec<(u64, i64)>) {
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(obj, limits).expect("open object");
        let footer = &loc.footer;
        if !footer.sections.iter().any(|s| s.kind == EXEMPLARS) {
            return (false, Vec::new(), Vec::new());
        }
        let section = |kind: u32| -> &[u8] {
            let s = footer
                .sections
                .iter()
                .find(|s| s.kind == kind)
                .unwrap_or_else(|| panic!("missing section kind {kind}"));
            &obj[s.offset as usize..(s.offset + s.len) as usize]
        };
        let records = ravel_segment::decode_exemplars_section(
            footer,
            section(LABEL_DICT),
            section(EXEMPLARS),
            limits,
        )
        .expect("decode EXEMPLARS");
        // SERIES_IDS order, which is what `series_index` indexes into.
        let entries = ravel_segment::decode_catalog_v5(footer, obj, limits).expect("catalog");
        let ids: Vec<[u8; 16]> = entries.iter().map(|e| e.entry.series_id.0).collect();

        let keys = records
            .iter()
            .map(|r| (r.series_index, r.ts_ns))
            .collect::<Vec<_>>();
        let canon = records
            .iter()
            .map(|r| {
                let id = ids[usize::try_from(r.series_index).expect("index fits")];
                (
                    id,
                    r.ts_ns,
                    r.value.to_bits(),
                    r.trace_id,
                    r.span_id,
                    r.attrs.clone(),
                )
            })
            .collect();
        (true, canon, keys)
    }

    /// The exemplar multiset of a whole set of objects (inputs or L1 parts),
    /// sorted so two multisets compare directly. A `Vec`, never a set: a
    /// deduplicating compactor must fail these assertions.
    fn canon_multiset(objects: &[Bytes]) -> Vec<Canon> {
        let mut all: Vec<Canon> = Vec::new();
        for obj in objects {
            let (_, canon, _) = read_exemplars(obj);
            all.extend(canon);
        }
        all.sort();
        all
    }

    /// The acceptance test: the exemplars in the L1 output are
    /// exactly the union (as a multiset) of the inputs', with series indices
    /// remapped into each part's own SERIES_IDS ordering. Series ids are
    /// chosen so the two inputs' orderings differ from the output's, so a
    /// missing remap would surface as a wrong series id rather than passing by
    /// accident.
    #[tokio::test]
    async fn exemplars_survive_compaction_verbatim() {
        let store = MemoryStore::new();
        // Input 1 carries alpha and gamma; input 2 carries beta and gamma. The
        // merged output carries all three, so every input's local series
        // indices differ from the output's.
        let a = seed(
            &store,
            1,
            vec![
                series("alpha", &[(10, 1.0)]),
                series("gamma", &[(10, 3.0), (30, 3.3)]),
            ],
            vec![exemplar("alpha", 10, 1.0, 1), exemplar("gamma", 30, 3.3, 2)],
        )
        .await;
        let b = seed(
            &store,
            2,
            vec![series("beta", &[(20, 2.0)]), series("gamma", &[(40, 3.4)])],
            vec![exemplar("beta", 20, 2.0, 3), exemplar("gamma", 40, 3.4, 4)],
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

        let (record, parts) = read_output(&store).await;
        assert_eq!(record.level, 1);
        assert_eq!(parts.len(), 1, "small corpus fits one part");

        let expected = canon_multiset(&[a, b]);
        assert_eq!(expected.len(), 4, "the inputs carry four exemplars");
        assert_eq!(
            canon_multiset(&parts),
            expected,
            "L1 exemplars must be the exact multiset of the inputs'"
        );

        // The remap is real: gamma's two exemplars name the same series id,
        // which in the output is a different index than in either input.
        let (_, canon, keys) = read_exemplars(&parts[0]);
        let gamma = series_id("gamma").0;
        assert_eq!(
            canon.iter().filter(|c| c.0 == gamma).count(),
            2,
            "both gamma exemplars survive, from two different inputs"
        );
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "records ascending by (series_index, ts_ns)");
    }

    /// Two inputs each carrying an exemplar for the same `(series, ts)` with
    /// different trace ids: both reach the output. This is what a retried
    /// write produces (the ingest cap is flush-scoped), and the ADR-0047
    /// 2026-08-03 amendment made equal sort keys legal precisely so the
    /// compactor can encode both. Deduplicating here would make an L1 object
    /// something other than the multiset of its inputs.
    #[tokio::test]
    async fn two_exemplars_sharing_a_series_and_timestamp_both_survive() {
        let store = MemoryStore::new();
        seed(
            &store,
            1,
            vec![series("alpha", &[(10, 1.0)])],
            vec![exemplar("alpha", 10, 1.0, 0xAA)],
        )
        .await;
        seed(
            &store,
            2,
            vec![series("alpha", &[(10, 1.0)])],
            vec![exemplar("alpha", 10, 1.0, 0xBB)],
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;

        let (present, canon, keys) = read_exemplars(&parts[0]);
        assert!(present);
        assert_eq!(canon.len(), 2, "both records survive, not one");
        assert_eq!(
            keys,
            vec![(0, 10), (0, 10)],
            "equal sort keys are legal and are not collapsed"
        );
        let mut traces: Vec<[u8; 16]> = canon.iter().map(|c| c.3).collect();
        traces.sort();
        assert_eq!(traces, vec![[0xAA; 16], [0xBB; 16]]);
    }

    /// Inputs with no exemplars produce parts with no EXEMPLARS section:
    /// absence, not a zero-count section, is how "no exemplars" is represented
    /// (ADR-0047 decision 1).
    #[tokio::test]
    async fn inputs_without_exemplars_produce_a_part_without_the_section() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])], Vec::new()).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])], Vec::new()).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;
        let (present, canon, _) = read_exemplars(&parts[0]);
        assert!(!present, "an L1 part with no exemplars emits no section");
        assert!(canon.is_empty());
    }

    /// A part cap small enough to split forces exemplars to follow their own
    /// series into whichever part carries it: no part may hold an exemplar for
    /// a series it does not carry (that is a writer error, not a silent drop),
    /// and across parts the multiset is still conserved.
    #[tokio::test]
    async fn part_splitting_keeps_each_exemplar_with_its_series() {
        let store = MemoryStore::new();
        let mut seeded = Vec::new();
        for seq in 1..=2u64 {
            let mut batch = Vec::new();
            let mut exemplars = Vec::new();
            for n in 0..8u8 {
                let metric = format!("m{n}");
                batch.push(series(&metric, &[(10 * i64::from(n) + 1, f64::from(n))]));
                exemplars.push(exemplar(
                    &metric,
                    10 * i64::from(n) + 1,
                    f64::from(n),
                    n * 16 + seq as u8,
                ));
            }
            seeded.push(seed(&store, seq, batch, exemplars).await);
        }

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig {
            max_l1_part_bytes: 128,
            ..CompactorConfig::default()
        };
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;
        assert!(parts.len() >= 2, "a tiny cap must split into parts");

        assert_eq!(
            canon_multiset(&parts),
            canon_multiset(&seeded),
            "the exemplar multiset is conserved across the split"
        );

        // Each part's records name only series that part carries, and stay
        // ascending after the per-part remap.
        for part in &parts {
            let limits = ReaderLimits::default();
            let loc = ravel_segment::open_from_full(part, limits).expect("open part");
            let entries =
                ravel_segment::decode_catalog_v5(&loc.footer, part, limits).expect("catalog");
            let ids: BTreeMap<[u8; 16], ()> =
                entries.iter().map(|e| (e.entry.series_id.0, ())).collect();
            let (_, canon, keys) = read_exemplars(part);
            for c in &canon {
                assert!(
                    ids.contains_key(&c.0),
                    "a part must not carry an exemplar for a series it lacks"
                );
            }
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "records ascending by (series_index, ts_ns)");
        }
    }

    /// Exemplars on the sparse-catalog decode path.
    ///
    /// `read.rs` routes an input with `series_count >= V5_SPARSE_THRESHOLD`
    /// (4096) to the whole-object `decode_catalog_v5`, and every other test
    /// here (and `ravel-ingest`'s `exemplar_flush.rs`) stays far below that, so
    /// the chunked decoder is the only one exercised for exemplars. Exemplar
    /// `series_index` resolution depends on the decoder returning entries in
    /// SERIES_IDS order; if a future change to the sparse decoder broke that,
    /// every exemplar here would resolve to the wrong series while the smaller
    /// tests still passed. This drives two inputs that each cross the threshold
    /// (4096 distinct series, one exemplar apiece) and asserts the L1 output's
    /// exemplars are the exact multiset of the inputs', which pins the resolved
    /// series ids on both the sparse input decode and the merge remap.
    #[tokio::test]
    async fn sparse_catalog_path_exemplars_resolve_to_same_series() {
        // At the threshold exactly, so each input's own catalog decodes through
        // the sparse (whole-object) path, not the chunked one.
        const N: usize = ravel_segment::V5_SPARSE_THRESHOLD as usize;

        let start = std::time::Instant::now();
        let store = MemoryStore::new();

        // Two inputs carrying disjoint series ranges, so the merged output's
        // SERIES_IDS ordering differs from either input's local ordering and the
        // per-part remap is non-trivial (not the identity).
        let mut seeded = Vec::new();
        for seq in 1..=2u64 {
            let base = (seq as usize - 1) * N;
            let mut batch = Vec::with_capacity(N);
            let mut exemplars = Vec::with_capacity(N);
            for i in 0..N {
                let n = base + i;
                let metric = format!("m{n:05}");
                let ts = n as i64 + 1;
                batch.push(series(&metric, &[(ts, n as f64)]));
                exemplars.push(exemplar(&metric, ts, n as f64, (n % 251) as u8));
            }
            seeded.push(seed(&store, seq, batch, exemplars).await);
        }

        // Each seeded input really is on the sparse branch.
        for obj in &seeded {
            let loc = ravel_segment::open_from_full(obj, ReaderLimits::default()).expect("open");
            assert!(
                loc.footer.series_count >= ravel_segment::V5_SPARSE_THRESHOLD,
                "input must cross the sparse threshold to exercise the sparse decode"
            );
        }

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;

        let expected = canon_multiset(&seeded);
        assert_eq!(
            expected.len(),
            2 * N,
            "inputs carry one exemplar per series"
        );
        assert_eq!(
            canon_multiset(&parts),
            expected,
            "L1 exemplars from a sparse-catalog input must resolve to the same \
             series ids as the inputs'"
        );

        eprintln!(
            "sparse_catalog_path_exemplars_resolve_to_same_series: {} series, {} exemplars, {:?}",
            2 * N,
            2 * N,
            start.elapsed()
        );
    }
}

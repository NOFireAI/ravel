//! The shared segment-rewrite primitive (ADR-0066 decision 5).
//!
//! A rewrite reads a named set of live objects, decodes them through this
//! crate's existing per-signal reader machinery, re-encodes them through the
//! current writer, and publishes the result through the same
//! `CreateIfAbsent`-then-supersede path compaction uses -- but with the
//! record-count conservation gate taken as a parameter instead of hardcoded to
//! exact match. That one generalization is what lets a single primitive serve
//! two epics:
//!
//! - EM (this crate's caller below): format migration. An old-format
//!   object is decoded and re-encoded into the current format, conserving the
//!   record count exactly ([`crate::publish::conserve_exact`]). Nothing is
//!   dropped; the bytes change format, not content.
//! - EJ (a later task, not built here): selective erasure. The same read
//!   -> re-encode -> publish shape, but the writer drops the erased subject's
//!   records and the predicate is "input equals output plus the erased count"
//!   (ADR-0064 decision 3 point 4). EJ supplies that predicate; it does not
//!   re-implement the primitive.
//!
//! ## The N-1 reader
//!
//! The primitive does not contain a decoder; it calls
//! [`SegmentCodec::load_input_catalog`] and [`SegmentCodec::build_parts`],
//! exactly as [`crate::compact::compact_bucket`] does. For RLOG and RSPAN
//! those genuinely decode and re-encode on every merge, so once
//! ravel-logseg/ravel-rspan gain an N-1 reader (ADR-0066 decision 1, at first
//! public release), the codec's read path accepts the older version and this
//! primitive migrates real old-version objects with no change here.
//!
//! RSEG is now the same shape: since ADR-0066 decision 5, its `build_parts`
//! (`build.rs`) still copies a *current*-version input's pages verbatim, but
//! decodes and re-encodes an input recorded *below* the current output version
//! at the current version before the shared merge/plan step, so an RSEG
//! rewrite is a real format migration, not just a same-version round trip.
//! [`RsegCodec::validate_rewrite_inputs`] no longer refuses an older
//! input; it now fails closed only on an input recorded *newer* than the
//! current output version (ADR-0066 decision 2), which a forward migration
//! cannot write. Whether an older object's bytes are actually decodable is the
//! ravel-segment reader's `SUPPORTED_VERSIONS` window to enforce at open time;
//! today that window is a single version (ADR-0027), so the decode-and-re-encode
//! path is exercised by tests and dry-runs rather than by carrying real dual
//! versions in anger (ADR-0066 Consequences), and it converges an
//! older-recorded input to the current version the day the window opens.
//!
//! ## Durability is unchanged
//!
//! The primitive generalizes the read side and the conservation check, never
//! the publish/durability side. [`crate::build::build_parts`] still PUTs every
//! part `CreateIfAbsent` under a content-addressed key, and
//! [`crate::publish::publish_record_with_conservation`] still runs the
//! abandonment deadline, the single-winner record PUT, and the racing-loser
//! convergence/repair unchanged. A crashed rewrite re-run from scratch rebuilds
//! the identical content-addressed parts and converges at the record's
//! `CreateIfAbsent`, the same statelessness compaction has.

use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::Signal;

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::codec::{RsegCodec, SegmentCodec};
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::publish::{
    ConservationPredicate, PublishOutcome, conserve_exact, publish_record_with_conservation,
};
use crate::read::{input_set_hash, list_bucket, load_inputs};
use crate::rlog::RlogCodec;
use crate::rspan_codec::SpanCodec;

/// The result of a [`rewrite_and_publish`] call: how many parts were built and
/// how the record PUT resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteOutcome {
    /// Parts built and PUT for the new object set.
    pub parts: usize,
    /// How publishing the record resolved (won, converged, or abandoned).
    pub publish: PublishOutcome,
}

/// Read the objects named by `commit_keys`, decode each through codec `C`,
/// re-encode through `C`'s current writer into size-capped parts, and publish
/// the superseding record with `conservation` as the record-count gate.
///
/// This is the shared primitive. `C` selects the signal's read/re-encode path
/// (`RsegCodec`, `RlogCodec`, `SpanCodec`); `conservation` selects the epic's
/// conservation arithmetic ([`conserve_exact`] for compaction and format
/// migration, a drop-aware predicate for erasure). Every other step -- input
/// ordering, the `input_set_hash`, part building, and the whole publish
/// protocol -- is identical to compaction and shared verbatim.
///
/// `start_ns` is the run's start for the abandonment deadline, as in
/// [`crate::publish::publish_record`].
pub async fn rewrite_and_publish<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    commit_keys: &[String],
    conservation: impl ConservationPredicate,
    start_ns: i64,
) -> Result<RewriteOutcome> {
    let inputs = load_inputs(store, bucket, commit_keys, config.input_read_concurrency).await?;
    C::validate_rewrite_inputs(&inputs)?;
    let hash = input_set_hash(&inputs);

    // Catalogs aligned one-to-one with `inputs` (canonical order): the merge
    // relies on that alignment for deterministic tie-breaking, same as
    // compaction's pipeline. `buffered` (not `buffer_unordered`) keeps that
    // alignment while `input_read_concurrency` catalog loads are in flight:
    // it yields results in input order regardless of completion order.
    //
    // The futures are built in a plain loop rather than a `.map()` closure:
    // a closure returning a future that borrows its `&InputRecord` argument is
    // inferred as higher-ranked over that lifetime, and its auto-traits then
    // fail to leak through the stream adapter with "implementation of `FnOnce`
    // is not general enough" wherever the maintain loop is `tokio::spawn`ed.
    let mut pending = Vec::with_capacity(inputs.len());
    for input in &inputs {
        pending.push(C::load_input_catalog(store, config, input));
    }
    let catalogs: Vec<C::Catalog> = stream_iter(pending)
        .buffered(config.input_read_concurrency.max(1))
        .try_collect()
        .await?;

    let parts = C::build_parts(store, config, bucket, &inputs, catalogs, &hash).await?;

    let publish = publish_record_with_conservation(
        store,
        config,
        clock,
        bucket,
        &inputs,
        &hash,
        &parts,
        start_ns,
        conservation,
    )
    .await?;

    // Issue #977: surface the compaction's peak memory split by phase. Every
    // term is populated by now (catalog load before build_parts, cursor/writer/
    // retained during build_parts, publish record during publish), and `parts`
    // is still alive so the retained high-water reflects the whole set. Each
    // field names its byte kind; they are NOT summable across kinds. Only fires
    // when a tracker is installed (never in production today, see
    // `MergeMemoryTracker`); installing one is the one-line service change that
    // makes this visible to an operator.
    if let Some(t) = config.merge_memory_tracker.as_ref() {
        let peaks = t.phase_peaks();
        tracing::info!(
            signal = ?bucket.signal,
            shard = bucket.shard,
            ingest_hour_bucket = bucket.ingest_hour_bucket,
            parts = parts.len(),
            catalog_directory_decoded_bytes = peaks.catalog_directory_decoded_bytes,
            cursor_bytes = peaks.cursor_bytes,
            writer_heap_bytes = peaks.writer_heap_bytes,
            retained_part_encoded_bytes = peaks.retained_part_encoded_bytes,
            publish_record_encoded_bytes = peaks.publish_record_encoded_bytes,
            "compaction peak memory by phase (byte kinds differ per term; do not sum)"
        );
    }

    Ok(RewriteOutcome {
        parts: parts.len(),
        publish,
    })
}

/// The result of a [`migrate_bucket_format`] call. Every variant except
/// [`MigrateOutcome::Rewritten`] left the bucket untouched, with the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrateOutcome {
    /// Not yet sealed: the input set is not guaranteed complete.
    NotSealed,
    /// A retention tombstone is present; the bucket is never rewritten.
    Tombstoned,
    /// A compaction (or rewrite) record already exists. The live record set is
    /// L1 parts, not raw L0 objects; migrating those is the rewrite-on-
    /// touch, which reads L1 parts as inputs. This caller handles the pre-
    /// compaction L0 case only.
    AlreadyCompacted,
    /// No L0 input below `target_version`: the bucket is already at or above
    /// the floor, nothing to migrate.
    UpToDate,
    /// At least one below-target L0 input; the whole live L0 set was rewritten
    /// into current-format L1 parts and published, conserving the record count
    /// exactly.
    Rewritten {
        parts: usize,
        publish: PublishOutcome,
    },
}

/// EM's compaction-variant caller of the shared primitive: migrate a sealed
/// bucket's live L0 objects to the current format when any of them was written
/// below `target_version`.
///
/// This is the "N-1 decode, re-encode, publish, conservation" shape named in
/// The rewrite primitive, wired end to end: it gates the bucket exactly as
/// [`crate::compact::compact_bucket`] does (seal, tombstone, already-compacted),
/// reads the L0 commit records, and -- if any records the current writer would
/// supersede was written below the target format version -- rewrites the whole
/// live L0 set through [`rewrite_and_publish`] with [`conserve_exact`],
/// producing current-format L1 parts and a superseding record. The old objects
/// become sweepable exactly like any superseded compaction input.
///
/// `target_version` is the format version the migration is raising toward
/// (`ravel_segment::VERSION_V7 as u32` in production). Passing a version above
/// the current writer's output makes every current-version object eligible,
/// which is how the tests exercise the migration path before an actual N-1
/// version exists to read.
///
/// The `maintain migrate` driver (resumable cursor, budget, verify-and-
/// raise-floor) and rewrite-on-touch build on this entry point; the
/// server/CLI wiring is their scope, not this task's.
pub async fn migrate_bucket_format(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    target_version: u32,
) -> Result<MigrateOutcome> {
    let start_ns = clock.now_ns();
    if !bucket.is_sealed(start_ns, config) {
        return Ok(MigrateOutcome::NotSealed);
    }

    let listing = list_bucket(store, bucket).await?;
    if listing.tombstone_key.is_some() {
        return Ok(MigrateOutcome::Tombstoned);
    }
    if !listing.compaction_record_keys.is_empty() {
        return Ok(MigrateOutcome::AlreadyCompacted);
    }
    if listing.commit_keys.is_empty() {
        return Ok(MigrateOutcome::UpToDate);
    }

    // Decide eligibility from the commit records' recorded format version,
    // never a data-object GET (ADR-0066: the catalog knows every live object's
    // version without reading it). `load_inputs` verifies each record's key and
    // bucket, so the eligibility read rides on the same decode the rewrite
    // needs anyway.
    let inputs = load_inputs(
        store,
        bucket,
        &listing.commit_keys,
        config.input_read_concurrency,
    )
    .await?;
    let needs_migration = inputs
        .iter()
        .any(|i| i.record.segment_format_version < target_version);
    if !needs_migration {
        return Ok(MigrateOutcome::UpToDate);
    }

    let outcome = dispatch_rewrite(
        bucket.signal,
        store,
        clock,
        config,
        bucket,
        &listing.commit_keys,
        start_ns,
    )
    .await?;

    Ok(MigrateOutcome::Rewritten {
        parts: outcome.parts,
        publish: outcome.publish,
    })
}

/// Dispatch [`rewrite_and_publish`] on the bucket's signal to the matching
/// codec, with the exact-conservation predicate. Mirrors
/// [`crate::compact::compact_bucket`]'s signal dispatch so the rewrite path
/// covers the same three signals with the same codecs.
async fn dispatch_rewrite(
    signal: Signal,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    commit_keys: &[String],
    start_ns: i64,
) -> Result<RewriteOutcome> {
    match signal {
        Signal::Metrics => {
            rewrite_and_publish::<RsegCodec>(
                store,
                clock,
                config,
                bucket,
                commit_keys,
                conserve_exact(),
                start_ns,
            )
            .await
        }
        Signal::Logs => {
            rewrite_and_publish::<RlogCodec>(
                store,
                clock,
                config,
                bucket,
                commit_keys,
                conserve_exact(),
                start_ns,
            )
            .await
        }
        Signal::Spans => {
            rewrite_and_publish::<SpanCodec>(
                store,
                clock,
                config,
                bucket,
                commit_keys,
                conserve_exact(),
                start_ns,
            )
            .await
        }
        other => Err(MaintainError::Invariant(format!(
            "rewrite is not implemented for signal {other:?}"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    //! Exercises the shared primitive end to end over real RSEG v7 objects on a
    //! `MemoryStore`: a format-migration round trip that conserves record
    //! content, the conservation-abort path (a rejecting predicate publishes
    //! nothing), the crash-and-rerun convergence at the content-addressed key,
    //! and proof the conservation predicate is genuinely pluggable rather than
    //! silently exact.

    use bytes::Bytes;
    use ravel_commit::keys;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CompactionRecord;
    use ravel_segment::{
        IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues,
        VERSION_V7,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::publish::conserve_exact;
    use crate::read::{input_set_hash, load_inputs};
    use crate::{CompactorConfig, FixedClock};

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;
    /// A version above the current writer's output, so every real v7 object
    /// counts as "old" and the migration path runs. Stands in for the N-1
    /// version an actual reader window would supply.
    const FUTURE_VERSION: u32 = VERSION_V7 as u32 + 1;

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

    /// Seed one L0 input (data object + commit record) through the production
    /// flush writer, so the seeded object is byte-for-byte a real v7 L0 flush.
    /// Returns the data object's bytes.
    async fn seed(store: &dyn ObjectStoreBackend, seq: u64, series: Vec<SeriesInputV3>) -> Bytes {
        seed_with_version(store, seq, series, VERSION_V7 as u32).await
    }

    /// Like [`seed`], but records `segment_format_version` in the commit
    /// record as given rather than the writer's true output version. The
    /// object bytes are always real v7 -- only the metadata a caller reads
    /// without decoding (the guard under test) can disagree with them.
    async fn seed_with_version(
        store: &dyn ObjectStoreBackend,
        seq: u64,
        series: Vec<SeriesInputV3>,
        segment_format_version: u32,
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
            SegmentWriter::write_histograms_with_exemplars(series, identity, bounds, Vec::new())
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
            segment_format_version,
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

    /// The single compaction record in the bucket, if one was published.
    async fn read_record(store: &dyn ObjectStoreBackend) -> Option<CompactionRecord> {
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
        let key = rec_keys.first()?;
        let got = store.get(key, GetRange::Full).await.expect("get record");
        Some(CompactionRecord::decode(got.data.as_ref()).expect("decode record"))
    }

    /// Every series id carried by an object's catalog, in stored order.
    fn series_ids_of(obj: &[u8]) -> Vec<[u8; 16]> {
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(obj, limits).expect("open object");
        let entries = ravel_segment::decode_catalog_v5(&loc.footer, obj, limits).expect("catalog");
        entries.iter().map(|e| e.entry.series_id.0).collect()
    }

    /// Every scalar sample an RSEG object carries, as a canonical multiset of
    /// `(series_id, ts_ns, value_bits)` -- value compared by bit pattern, never
    /// `==`, per the storage-path float rule. Decodes the whole object (catalog
    /// plus every run's TS/VAL pages) so a round-trip check can assert real data
    /// survived, not just that the sample counts matched.
    fn decode_scalar_multiset(obj: &[u8]) -> Vec<([u8; 16], i64, u64)> {
        use ravel_segment::{ValueKind, decode_run_pages_soa, plan_ranges_v4};
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(obj, limits).expect("open object");
        let entries = ravel_segment::decode_catalog_v5(&loc.footer, obj, limits).expect("catalog");
        let refs: Vec<&ravel_segment::SeriesEntryV4> = entries.iter().collect();
        let planned = plan_ranges_v4(&loc.footer, &refs).expect("plan ranges");
        let mut planned = planned.into_iter();
        let mut out = Vec::new();
        for entry in &entries {
            let series_id = entry.entry.series_id;
            for run in &entry.runs {
                let range = planned.next().expect("a range per run");
                assert!(
                    matches!(entry.entry.value_kind, ValueKind::Scalar),
                    "helper only handles scalar series"
                );
                let ts =
                    &obj[range.ts_range.0 as usize..(range.ts_range.0 + range.ts_range.1) as usize];
                let val = &obj
                    [range.val_range.0 as usize..(range.val_range.0 + range.val_range.1) as usize];
                let mut scratch = Vec::new();
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                decode_run_pages_soa(
                    &series_id,
                    run,
                    ts,
                    val,
                    limits,
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .expect("decode run");
                for (ts_ns, value) in timestamps.into_iter().zip(values) {
                    out.push((series_id.0, ts_ns, value.to_bits()));
                }
            }
        }
        out.sort();
        out
    }

    /// The migration round trip: two real v7 L0 objects decode, re-encode, and
    /// publish into a current-version L1 part carrying the same series content,
    /// with the record-count conserved exactly.
    #[tokio::test]
    async fn migrate_round_trips_and_conserves_content() {
        let store = MemoryStore::new();
        let a = seed(
            &store,
            1,
            vec![series("alpha", &[(10, 1.0)]), series("gamma", &[(30, 3.0)])],
        )
        .await;
        let b = seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            FUTURE_VERSION,
        )
        .await
        .expect("migrate");

        let (parts, publish) = match outcome {
            MigrateOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(publish, PublishOutcome::Published);
        assert_eq!(parts, 1, "small corpus fits one part");

        let record = read_record(&store).await.expect("a record was published");
        assert_eq!(record.level, 1);
        assert_eq!(record.parts.len(), 1);
        let part = &record.parts[0];
        assert_eq!(
            part.segment_format_version, VERSION_V7 as u32,
            "the output is stamped the current writer version"
        );

        // Record count conserved: the L1 part carries every input sample.
        let input_samples: u64 = [&a, &b]
            .iter()
            .map(|obj| {
                let loc = ravel_segment::open_from_full(obj, ReaderLimits::default())
                    .expect("open input");
                loc.footer.sample_count
            })
            .sum();
        assert_eq!(part.sample_count, input_samples, "sample count conserved");

        // Series content conserved: the output's series ids are the union of
        // the inputs'.
        let part_key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
        let part_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        let mut got = series_ids_of(&part_bytes);
        got.sort();
        let mut want = vec![
            series_id("alpha").0,
            series_id("beta").0,
            series_id("gamma").0,
        ];
        want.sort();
        assert_eq!(got, want, "L1 carries exactly the inputs' series");
    }

    /// Slice B headline (ADR-0066 decision 5): the synthetic-N-1 mixed-fleet
    /// convergence case. An RSEG object recorded *below* the current output
    /// version is genuinely decoded and re-encoded to the current version by
    /// the rewrite primitive -- not refused (the pre-slice-B guard), and not
    /// verbatim-copied under a mislabeled trailer.
    ///
    /// No real N-1 RSEG version has ever shipped, so the "old" object is built
    /// the way slice A's window tests use a synthetic version number
    /// (`n_and_prev_window_is_exactly_two_wide_with_a_floor`): real, fully
    /// decodable v7 bytes under a commit record that records an older version.
    /// The reader accepts the bytes (they are real v7); the compactor's
    /// `build_parts` sees an input recorded below the output version and routes
    /// its runs through the decode-and-re-encode path
    /// ([`crate::build::build_parts`]'s `migrate_keys` branch) rather than the
    /// verbatim page copy; every sample round-trips and the record count is
    /// conserved exactly by the shared publish gate.
    ///
    /// Flip proof (non-vacuous): against `RsegCodec::validate_rewrite_inputs`'s
    /// pre-slice-B refusal (reject any input recorded `!= OUTPUT_FORMAT_VERSION`)
    /// this test fails at `expect("migrate")` -- the migration returns the
    /// guard's `Invariant` error instead of `Rewritten`. Verified by running it
    /// against the old guard before the build/codec change landed.
    #[tokio::test]
    async fn migrates_an_input_recorded_below_the_output_version() {
        let store = MemoryStore::new();
        let old = VERSION_V7 as u32 - 1;
        // Two inputs recorded below the output version, over real v7 bytes; one
        // carries two series (one of them with two samples) so the round-trip
        // check spans multiple series and a multi-sample run.
        let a = seed_with_version(
            &store,
            1,
            vec![
                series("alpha", &[(10, 1.0), (11, 1.5)]),
                series("gamma", &[(30, 3.0)]),
            ],
            old,
        )
        .await;
        let b = seed_with_version(&store, 2, vec![series("beta", &[(20, 2.0)])], old).await;

        let clock = FixedClock::new(sealed_now_ns());
        // Target the current output version: eligibility is `recorded < target`,
        // and the rewrite re-encodes to the current version (== target), so the
        // output lands at the target -- the convergence RSEG could not reach
        // before slice B, because its verbatim path could never lift an
        // older-recorded input to the current version.
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            VERSION_V7 as u32,
        )
        .await
        .expect("migrate");

        let (parts, publish) = match outcome {
            MigrateOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(publish, PublishOutcome::Published);
        assert_eq!(parts, 1, "small corpus fits one part");

        let record = read_record(&store).await.expect("a record was published");
        assert_eq!(record.parts.len(), 1);
        let part = &record.parts[0];
        assert_eq!(
            part.segment_format_version, VERSION_V7 as u32,
            "the re-encoded output is stamped the current writer version"
        );

        // Record count conserved: the L1 part carries every input sample. The
        // input total is read from the objects' own footers (the real v7 bytes),
        // independent of the commit records' (deliberately older) version tag.
        let input_samples: u64 = [&a, &b]
            .iter()
            .map(|obj| {
                ravel_segment::open_from_full(obj, ReaderLimits::default())
                    .expect("open input")
                    .footer
                    .sample_count
            })
            .sum();
        assert_eq!(part.sample_count, input_samples, "sample count conserved");

        // Real data round-trips: the L1 part's decoded (series, ts, value-bits)
        // multiset equals the union of the inputs', proving the decode-and-
        // re-encode preserved every sample's value, not merely its count.
        let part_key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
        let part_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        let mut got = decode_scalar_multiset(&part_bytes);
        let mut want: Vec<([u8; 16], i64, u64)> = decode_scalar_multiset(&a)
            .into_iter()
            .chain(decode_scalar_multiset(&b))
            .collect();
        got.sort();
        want.sort();
        assert_eq!(
            got, want,
            "every input sample round-trips through the rewrite"
        );
    }

    /// Fail-closed-on-newer (ADR-0066 decision 2): an input recorded *above* the
    /// current output version -- newer than this build can read or write -- is
    /// refused before any decode or PUT, never mislabeled downward. This is the
    /// half of the old below-output refusal that survives slice B: the guard
    /// stopped rejecting older (migratable) inputs and now rejects only
    /// newer-than-writable ones.
    #[tokio::test]
    async fn rseg_rewrite_rejects_input_recorded_above_output_version() {
        let store = MemoryStore::new();
        seed_with_version(
            &store,
            1,
            vec![series("alpha", &[(10, 1.0)])],
            VERSION_V7 as u32 + 1,
        )
        .await;
        let listing = list_bucket(&store, &bucket()).await.expect("list");
        let clock = FixedClock::new(sealed_now_ns());

        let err = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            conserve_exact(),
            sealed_now_ns(),
        )
        .await
        .expect_err("an input recorded above the output version must be rejected");
        assert!(
            matches!(err, MaintainError::Invariant(_)),
            "expected the validate_rewrite_inputs fail-closed-on-newer guard, got {err:?}"
        );
        assert!(
            read_record(&store).await.is_none(),
            "a rejected input set publishes nothing and PUTs nothing"
        );
    }

    /// A conservation predicate that rejects the built parts' true count aborts
    /// the publish: the run returns the typed [`MaintainError::ConservationViolation`]
    /// and no compaction record is written, so the L0 inputs stay live -- the
    /// same abort posture compaction has (services/ravel-server maintain records
    /// it as a conservation abort).
    #[tokio::test]
    async fn conservation_failure_aborts_publish() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;

        let listing = list_bucket(&store, &bucket()).await.expect("list");
        let clock = FixedClock::new(sealed_now_ns());

        // Demand exactly one dropped record where the rewrite drops none: the
        // exact output count is rejected, so the publish aborts.
        let err = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            |input: u64, output: u64| input == output + 1,
            sealed_now_ns(),
        )
        .await
        .expect_err("a rejected count must abort");
        assert!(
            matches!(err, MaintainError::ConservationViolation { .. }),
            "expected ConservationViolation, got {err:?}"
        );
        assert!(
            read_record(&store).await.is_none(),
            "an aborted publish writes no compaction record; the L0 inputs stay live"
        );
    }

    /// A crash-and-rerun converges at the same content-addressed key: the
    /// second run rebuilds identical parts, its record PUT sees the winner, and
    /// it reports convergence rather than a second record. Mirrors compaction's
    /// stateless-and-idempotent property.
    #[tokio::test]
    async fn crash_rerun_converges_at_content_addressed_key() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;
        let listing = list_bucket(&store, &bucket()).await.expect("list");
        let clock = FixedClock::new(sealed_now_ns());

        let first = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            conserve_exact(),
            sealed_now_ns(),
        )
        .await
        .expect("first rewrite");
        assert_eq!(first.publish, PublishOutcome::Published);
        let record_after_first = read_record(&store).await.expect("record after first");

        // Re-run from scratch, as a crashed run would.
        let second = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            conserve_exact(),
            sealed_now_ns(),
        )
        .await
        .expect("second rewrite");
        assert_eq!(
            second.publish,
            PublishOutcome::Converged { parts_repaired: 0 },
            "the rerun converges on the first run's record, repairing nothing"
        );

        let record_after_second = read_record(&store).await.expect("record after second");
        assert_eq!(
            record_after_first.parts, record_after_second.parts,
            "the same content-addressed parts; no second object set"
        );
        // Exactly one record object in the bucket.
        let prefix =
            keys::commit_shard_hour_prefix(&bucket().tenant_hash, Signal::Metrics, SHARD, HOUR)
                .expect("prefix");
        let record_keys: Vec<String> = list_all(&store, &prefix)
            .await
            .expect("list")
            .into_iter()
            .map(|m| m.key)
            .filter(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .collect();
        assert_eq!(record_keys.len(), 1, "exactly one compaction record");
    }

    /// The migration is a no-op when every input is already at or above the
    /// floor: passing the current writer version as the target leaves the
    /// bucket untouched.
    #[tokio::test]
    async fn migrate_up_to_date_is_a_noop() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            VERSION_V7 as u32,
        )
        .await
        .expect("migrate");
        assert_eq!(outcome, MigrateOutcome::UpToDate);
        assert!(
            read_record(&store).await.is_none(),
            "an up-to-date bucket publishes nothing"
        );
    }

    /// The predicate is genuinely pluggable: a drop-aware predicate that
    /// accepts an output count one short of the inputs (the erasure shape)
    /// publishes, where the exact-match default would abort. Driven at the
    /// publish layer so a deliberately-reduced count can be supplied directly.
    #[tokio::test]
    async fn pluggable_predicate_accepts_a_reduced_count() {
        use crate::build::{BuiltPart, OUTPUT_FORMAT_VERSION};
        use crate::publish::publish_record_with_conservation;
        use ravel_proto::commit::v1::CompactionPart;

        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0), (11, 1.1)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;
        let b = bucket();
        let listing = list_bucket(&store, &b).await.expect("list");
        let inputs = load_inputs(&store, &b, &listing.commit_keys, 1)
            .await
            .expect("inputs");
        let hash = input_set_hash(&inputs);
        let input_total: u64 = inputs.iter().map(|i| i.record.sample_count).sum();

        // A synthetic part carrying one fewer record than the inputs: the
        // erasure case, where exactly one record was dropped. The bytes are
        // irrelevant to the publish gate, which reads `part.sample_count`.
        let part = BuiltPart {
            key: "l1/synthetic".to_string(),
            bytes: Bytes::new(),
            part: CompactionPart {
                part_index: 0,
                content_hash: vec![0u8; 32],
                sample_count: input_total - 1,
                segment_format_version: OUTPUT_FORMAT_VERSION,
                ..CompactionPart::default()
            },
        };
        let clock = FixedClock::new(sealed_now_ns());

        // The exact-match default rejects the short count.
        let exact = publish_record_with_conservation(
            &store,
            &CompactorConfig::default(),
            &clock,
            &b,
            &inputs,
            &hash,
            std::slice::from_ref(&part),
            sealed_now_ns(),
            conserve_exact(),
        )
        .await;
        assert!(
            matches!(exact, Err(MaintainError::ConservationViolation { .. })),
            "exact conservation must reject a dropped record, got {exact:?}"
        );

        // The drop-aware predicate accepts it and the record publishes: the
        // primitive honored the supplied predicate rather than the default.
        let dropped = 1u64;
        let outcome = publish_record_with_conservation(
            &store,
            &CompactorConfig::default(),
            &clock,
            &b,
            &inputs,
            &hash,
            std::slice::from_ref(&part),
            sealed_now_ns(),
            move |input: u64, output: u64| input == output + dropped,
        )
        .await
        .expect("drop-aware predicate must accept the reduced count");
        assert_eq!(outcome, PublishOutcome::Published);
    }
}

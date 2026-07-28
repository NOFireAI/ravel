//! Publish and idempotency for a compaction run (plan section 3.4).
//!
//! The record `CreateIfAbsent` is the single serialization point: parts are
//! content-addressed (their key embeds the object blake3) and harmless to
//! leave behind, so the only ordering that matters for correctness is
//! "record visible only after all its parts are durable". This module PUTs
//! every part first, checks the abandonment deadline immediately before the
//! record PUT (the writer-interlock mirror, plan section 3.4 step 4), then
//! PUTs the compaction record. On `AlreadyExists` it converges on the
//! winner: same `input_set_hash` means a racing or prior run already won,
//! so it HEAD-verifies (and re-PUTs any missing) part the winner references
//! and reports success; a different `input_set_hash` at the same key is an
//! invariant breach that alarms and stops without deleting anything.

use std::collections::HashMap;

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionPart, CompactionRecord};
use ravel_types::{Signal, TenantHash};

use crate::clock::Clock;
use crate::config::MaintainConfig;
use crate::error::MaintainError;
use crate::input::{self, CompactionInput};
use crate::merge::BuiltPart;

/// Everything one compaction run needs to publish: the bucket identity, the
/// canonically sorted inputs it consumed (verbatim into the record's input
/// list, plan section 3.1), the `input_set_hash` those inputs produced, the
/// built parts from [`crate::merge::merge_and_build`], and the injected
/// clock time the run began (the abandonment deadline is measured from it).
pub struct PublishInput<'a> {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
    /// Inputs in the same canonical `(writer_id, epoch, seq)` order used to
    /// compute `input_set_hash` (see [`input::sort_inputs_canonically`]).
    pub inputs: &'a [CompactionInput],
    pub input_set_hash: [u8; 32],
    pub parts: &'a [BuiltPart],
    pub started_ns: i64,
}

/// Outcome of a publish attempt (both variants are success).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// This run wrote the compaction record itself.
    Published,
    /// A prior or racing run's record with the same `input_set_hash` already
    /// existed; this run HEAD-verified its parts (re-PUTting any it could
    /// supply) and reports success without writing a record.
    Converged,
}

/// Publish a compaction run per plan section 3.4.
///
/// Steps, in order: PUT every part `CreateIfAbsent` (`AlreadyExists` is
/// success, the key embeds the content hash); check the abandonment
/// deadline immediately before the record PUT and refuse to publish past it
/// (parts are content-addressed and harmless to leave behind); PUT the
/// compaction record `CreateIfAbsent`. On `AlreadyExists`, converge on the
/// winner (see [`converge`]).
pub async fn publish(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &MaintainConfig,
    input: &PublishInput<'_>,
) -> Result<PublishOutcome, MaintainError> {
    let input_set_hash16 = input::hash16(&input.input_set_hash);

    // Step 1: PUT each part, CreateIfAbsent. AlreadyExists = success.
    for part in input.parts {
        let key = part_key(input, &input_set_hash16, part)?;
        put_content_addressed(store, &key, part.written.bytes.clone()).await?;
    }

    // Step 4: abandonment mirror of the writer interlock, checked
    // immediately before the record PUT. A run that could not get here
    // within max_compaction_lifetime must never publish its record; its
    // already-written parts are content-addressed and swept as unreferenced
    // (plan section 5).
    let now_ns = clock.now_ns();
    if config.is_abandoned(input.started_ns, now_ns) {
        return Err(MaintainError::Abandoned {
            started_ns: input.started_ns,
            now_ns,
        });
    }

    // Step 2: PUT the compaction record, CreateIfAbsent.
    let record = build_record(input, now_ns);
    let record_key = ravel_commit::keys::compaction_record_key(
        &input.tenant_hash,
        input.signal,
        input.shard,
        input.ingest_hour_bucket,
        &input_set_hash16,
    )?;
    let payload = record.encode_to_vec();
    match store
        .put(&record_key, payload.into(), PutOptions::create_if_absent())
        .await
    {
        Ok(_) => Ok(PublishOutcome::Published),
        // Step 3: someone else's record is already there. Converge.
        Err(StoreError::AlreadyExists) => converge(store, &record_key, input).await,
        Err(err) => Err(err.into()),
    }
}

/// Converge on the record already at `record_key` (plan section 3.4 step 3).
///
/// Same `input_set_hash`: a racing or prior run won; HEAD every part it
/// references and re-PUT any missing one this run can supply (content
/// addressed, so a re-PUT is byte-identical or absent, never a divergent
/// overwrite). Different `input_set_hash`: a sealed bucket somehow yielded
/// two input sets, the writer-interlock invariant is breached; alarm,
/// delete nothing, stop.
async fn converge(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    input: &PublishInput<'_>,
) -> Result<PublishOutcome, MaintainError> {
    let existing = store.get(record_key, GetRange::Full).await?;
    let stored = CompactionRecord::decode(existing.data.as_ref())?;

    if stored.input_set_hash.as_slice() != input.input_set_hash.as_slice() {
        return Err(MaintainError::InputSetHashMismatch {
            ours: hex::encode(input.input_set_hash),
            stored: hex::encode(&stored.input_set_hash),
        });
    }

    // Same input set: HEAD-and-repair the winner's parts. Index our own
    // built parts by content hash so a missing winner part we happen to
    // have identical bytes for can be re-PUT under the winner's own key.
    let ours_by_hash: HashMap<&[u8], &BuiltPart> = input
        .parts
        .iter()
        .map(|p| (p.written.summary.blake3.as_slice(), p))
        .collect();

    for part in &stored.parts {
        let key = ravel_commit::keys::reconstruct_l1_part_key(&stored, part)?;
        match store.head(&key).await {
            Ok(_) => {}
            Err(StoreError::NotFound) => {
                if let Some(built) = ours_by_hash.get(part.content_hash.as_slice()) {
                    put_content_addressed(store, &key, built.written.bytes.clone()).await?;
                }
                // Else our build diverged from the winner's and we cannot
                // supply this part's bytes. It is content-addressed and
                // unreferenced-by-us; a re-run or the sweeper resolves it.
                // (The determinism the merge guarantees makes this
                // unreachable when both runs saw the same inputs.)
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(PublishOutcome::Converged)
}

/// PUT a content-addressed object `CreateIfAbsent`. `AlreadyExists` is
/// success: the key embeds the object's blake3, so the stored bytes are
/// identical to `bytes` by construction.
async fn put_content_addressed(
    store: &dyn ObjectStoreBackend,
    key: &str,
    bytes: bytes::Bytes,
) -> Result<(), MaintainError> {
    match store.put(key, bytes, PutOptions::create_if_absent()).await {
        Ok(_) | Err(StoreError::AlreadyExists) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn part_key(
    input: &PublishInput<'_>,
    input_set_hash16: &str,
    part: &BuiltPart,
) -> Result<String, MaintainError> {
    let hash16 = input::hash16(&part.written.summary.blake3);
    Ok(ravel_commit::keys::l1_part_key(
        &input.tenant_hash,
        input.signal,
        input.shard,
        input.ingest_hour_bucket,
        input_set_hash16,
        part.part_index,
        &hash16,
    )?)
}

fn build_record(input: &PublishInput<'_>, created_unix_ns: i64) -> CompactionRecord {
    let inputs = input
        .inputs
        .iter()
        .map(|i| CompactionInputIdentity {
            writer_id: i.record.writer_id.clone(),
            writer_epoch: i.record.writer_epoch,
            writer_seq: i.record.writer_seq,
        })
        .collect();
    let parts = input
        .parts
        .iter()
        .map(|p| CompactionPart {
            part_index: p.part_index,
            first_series_id: p.first_series_id.0.to_vec(),
            last_series_id: p.last_series_id.0.to_vec(),
            content_hash: p.written.summary.blake3.to_vec(),
            object_size: p.written.bytes.len() as u64,
            sample_count: p.written.summary.sample_count,
            series_count: p.written.summary.series_count,
            run_count: p.run_count,
            min_event_ts_ns: p.written.summary.min_event_ts_ns,
            max_event_ts_ns: p.written.summary.max_event_ts_ns,
            // RSEG v4: merge.rs writes write_v4 parts. The proto comment and
            // ticket text still say "v3"/"write_v3"; that mismatch is
            // resolved in merge.rs's own doc comment (v4 is the current
            // segment format, the ticket text is stale). Kept at 4 here to
            // match what the part bytes actually are.
            segment_format_version: 4,
        })
        .collect();

    CompactionRecord {
        format_version: 1,
        tenant_hash: input.tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(input.signal) as i32,
        shard: input.shard,
        ingest_hour_bucket: input.ingest_hour_bucket,
        level: 1,
        inputs,
        input_set_hash: input.input_set_hash.to_vec(),
        parts,
        created_unix_ns,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use crate::input::{input_set_hash, sort_inputs_canonically};
    use crate::merge::{BuildIdentity, merge_and_build};
    use crate::scan::scan_next;
    use crate::segread::read_input;
    use ravel_commit::record::{NewCommitRecord, build};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{PutMode, PutOptions};
    use ravel_segment::{IngestBounds, SegmentIdentity, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
    use uuid::Uuid;

    const HOUR: u32 = 0;
    const SHARD: u32 = 1;
    // Comfortably past the seal threshold for hour 0.
    const FAR_FUTURE_NS: i64 = 3_600_000_000_000 * 10;

    fn tenant_hash() -> TenantHash {
        TenantHash([0x55; 16])
    }

    /// Write one RSEG v1 L0 segment plus its commit record for `metric`
    /// under `writer_id`, returning the `CompactionInput` a scan would
    /// produce for it. Distinct `metric` values give distinct series ids;
    /// `writer_id` pins run provenance for determinism tests.
    async fn seed(
        store: &MemoryStore,
        th: &TenantHash,
        writer_id: Uuid,
        metric: &str,
    ) -> CompactionInput {
        let labels = LabelSet::new(vec![Label {
            name: "job".into(),
            value: "test".into(),
        }])
        .expect("labels");
        let series_id = SeriesId::compute(&TenantId::new("t"), metric, &labels).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: 1_000,
                value: 42.0,
            }],
        }];
        let identity = SegmentIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 1_000,
            max_ingest_ts_ns: 1_000,
        };
        let written =
            ravel_segment::SegmentWriter::write(series, identity, bounds).expect("write v1");
        let record = build(NewCommitRecord {
            tenant_hash: *th,
            signal: Signal::Metrics,
            shard: SHARD,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: 1_000,
            max_ingest_ts_ns: 1_000,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: HOUR,
        })
        .expect("valid record");

        let overwrite = PutOptions {
            mode: PutMode::Overwrite,
            checksum: None,
        };
        store
            .put(&record.object_key, written.bytes, overwrite.clone())
            .await
            .expect("put segment");
        let key = ravel_commit::keys::commit_key_for_record(&record).expect("commit key");
        store
            .put(&key, ravel_commit::record::encode(&record), overwrite)
            .await
            .expect("put commit record");
        CompactionInput { key, record }
    }

    /// Sort, hash, decode, and merge a set of raw inputs into built parts,
    /// exactly as the scan/build path feeds the publisher.
    async fn prepare(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        raw: Vec<CompactionInput>,
        max_l1_part_bytes: u64,
    ) -> (Vec<CompactionInput>, [u8; 32], Vec<BuiltPart>) {
        let sorted = sort_inputs_canonically(raw).expect("sort");
        let hash = input_set_hash(&sorted).expect("hash");
        let mut decoded = Vec::new();
        for input in &sorted {
            decoded.push(read_input(store, input).await.expect("read input"));
        }
        let identity = BuildIdentity {
            tenant_hash: th.0,
            shard: SHARD,
        };
        let parts =
            merge_and_build(&decoded, &identity, HOUR, hash, max_l1_part_bytes).expect("merge");
        (sorted, hash, parts)
    }

    fn plan<'a>(
        th: &TenantHash,
        inputs: &'a [CompactionInput],
        input_set_hash: [u8; 32],
        parts: &'a [BuiltPart],
        started_ns: i64,
    ) -> PublishInput<'a> {
        PublishInput {
            tenant_hash: *th,
            signal: Signal::Metrics,
            shard: SHARD,
            ingest_hour_bucket: HOUR,
            inputs,
            input_set_hash,
            parts,
            started_ns,
        }
    }

    async fn record_key(th: &TenantHash, input_set_hash: [u8; 32]) -> String {
        ravel_commit::keys::compaction_record_key(
            th,
            Signal::Metrics,
            SHARD,
            HOUR,
            &input::hash16(&input_set_hash),
        )
        .expect("record key")
    }

    async fn get_record(store: &dyn ObjectStoreBackend, key: &str) -> CompactionRecord {
        let got = store.get(key, GetRange::Full).await.expect("get record");
        CompactionRecord::decode(got.data.as_ref()).expect("decode record")
    }

    #[tokio::test]
    async fn publishes_record_and_parts_end_to_end() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let raw = vec![
            seed(&store, &th, Uuid::new_v4(), "metric_a").await,
            seed(&store, &th, Uuid::new_v4(), "metric_b").await,
        ];
        let (sorted, hash, parts) = prepare(&store, &th, raw, 256 << 20).await;
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();

        let outcome = publish(
            &store,
            &clock,
            &config,
            &plan(&th, &sorted, hash, &parts, 0),
        )
        .await
        .expect("publish");
        assert_eq!(outcome, PublishOutcome::Published);

        let rkey = record_key(&th, hash).await;
        let record = get_record(&store, &rkey).await;
        assert_eq!(record.format_version, 1);
        assert_eq!(record.level, 1);
        assert_eq!(record.inputs.len(), 2);
        assert_eq!(record.input_set_hash, hash.to_vec());
        assert_eq!(record.parts.len(), parts.len());
        for part in &record.parts {
            assert_eq!(part.segment_format_version, 4);
        }

        // Every part object the record names is durable.
        for part in &parts {
            let key = part_key(
                &plan(&th, &sorted, hash, &parts, 0),
                &input::hash16(&hash),
                part,
            )
            .expect("part key");
            store.head(&key).await.expect("part durable");
        }
    }

    #[tokio::test]
    async fn end_to_end_through_paginated_scan() {
        // with_page_size(2) forces multi-page LIST inside scan_next; the
        // whole scan -> build -> publish path must still see all inputs.
        let store = MemoryStore::with_page_size(2);
        let th = tenant_hash();
        for i in 0..5u32 {
            seed(&store, &th, Uuid::new_v4(), &format!("metric_{i}")).await;
        }
        let clock = FakeClock::new(FAR_FUTURE_NS);
        let config = MaintainConfig::default();

        let bucket = scan_next(&store, &clock, &config, &th, Signal::Metrics, SHARD)
            .await
            .expect("scan")
            .expect("bucket needs compaction");
        assert_eq!(bucket.inputs.len(), 5);

        let (sorted, hash, parts) = prepare(&store, &th, bucket.inputs, 256 << 20).await;
        let outcome = publish(
            &store,
            &clock,
            &config,
            &plan(&th, &sorted, hash, &parts, FAR_FUTURE_NS),
        )
        .await
        .expect("publish");
        assert_eq!(outcome, PublishOutcome::Published);

        let record = get_record(&store, &record_key(&th, hash).await).await;
        assert_eq!(record.inputs.len(), 5);
    }

    #[tokio::test]
    async fn republish_same_inputs_converges() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let raw = vec![
            seed(&store, &th, Uuid::new_v4(), "metric_a").await,
            seed(&store, &th, Uuid::new_v4(), "metric_b").await,
        ];
        let (sorted, hash, parts) = prepare(&store, &th, raw, 256 << 20).await;
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();
        let req = plan(&th, &sorted, hash, &parts, 0);

        assert_eq!(
            publish(&store, &clock, &config, &req).await.expect("first"),
            PublishOutcome::Published
        );
        // A second independent run over the same inputs finds the record and
        // converges without writing a second one.
        assert_eq!(
            publish(&store, &clock, &config, &req)
                .await
                .expect("second"),
            PublishOutcome::Converged
        );
    }

    #[tokio::test]
    async fn converge_repairs_a_missing_winner_part() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let raw = vec![
            seed(&store, &th, Uuid::new_v4(), "metric_a").await,
            seed(&store, &th, Uuid::new_v4(), "metric_b").await,
        ];
        // Tiny cap: two parts, so a single deleted part is a partial set.
        let (sorted, hash, parts) = prepare(&store, &th, raw, 1).await;
        assert_eq!(parts.len(), 2);
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();
        let req = plan(&th, &sorted, hash, &parts, 0);

        publish(&store, &clock, &config, &req).await.expect("first");

        // A lost part PUT (crash row 2/3 remnant): delete one part, then a
        // re-run must HEAD-and-repair it during convergence.
        let victim = part_key(&req, &input::hash16(&hash), &parts[1]).expect("key");
        store.delete(&victim).await.expect("delete part");
        store.head(&victim).await.expect_err("part gone");

        assert_eq!(
            publish(&store, &clock, &config, &req)
                .await
                .expect("converge"),
            PublishOutcome::Converged
        );
        store.head(&victim).await.expect("part re-put by repair");
    }

    #[tokio::test]
    async fn different_input_set_hash_at_key_alarms() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let raw = vec![
            seed(&store, &th, Uuid::new_v4(), "metric_a").await,
            seed(&store, &th, Uuid::new_v4(), "metric_b").await,
        ];
        let (sorted, hash, parts) = prepare(&store, &th, raw, 256 << 20).await;
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();

        // Plant a record at our exact record key whose body carries a
        // different input_set_hash: a sealed bucket that somehow yielded two
        // input sets (the writer-interlock breach of row 11). Publish must
        // GET it on AlreadyExists and alarm, deleting nothing.
        let rkey = record_key(&th, hash).await;
        let mut foreign = build_record(&plan(&th, &sorted, hash, &parts, 0), 0);
        foreign.input_set_hash = vec![0xAB; 32];
        store
            .put(
                &rkey,
                foreign.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("plant foreign record");

        let err = publish(
            &store,
            &clock,
            &config,
            &plan(&th, &sorted, hash, &parts, 0),
        )
        .await
        .expect_err("must alarm");
        assert!(
            matches!(err, MaintainError::InputSetHashMismatch { .. }),
            "expected InputSetHashMismatch, got {err}"
        );
    }

    #[tokio::test]
    async fn deterministic_bytes_and_keys_across_independent_runs() {
        // Fixed writer ids so the two runs see byte-identical inputs; the
        // merge is deterministic and the keys are content-addressed, so the
        // stored record bytes and every object key must match.
        let th = tenant_hash();
        let w_a = Uuid::parse_str("00000000-0000-0000-0000-0000000000a1").expect("uuid");
        let w_b = Uuid::parse_str("00000000-0000-0000-0000-0000000000b2").expect("uuid");
        let config = MaintainConfig::default();

        async fn run(
            th: &TenantHash,
            w_a: Uuid,
            w_b: Uuid,
            cfg: &MaintainConfig,
        ) -> (String, Vec<u8>) {
            let store = MemoryStore::new();
            let raw = vec![
                seed(&store, th, w_a, "metric_a").await,
                seed(&store, th, w_b, "metric_b").await,
            ];
            let (sorted, hash, parts) = prepare(&store, th, raw, 256 << 20).await;
            let clock = FakeClock::new(777);
            publish(&store, &clock, cfg, &plan(th, &sorted, hash, &parts, 0))
                .await
                .expect("publish");
            let rkey = record_key(th, hash).await;
            let bytes = store.get(&rkey, GetRange::Full).await.expect("get").data;
            (rkey, bytes.to_vec())
        }

        let (key1, bytes1) = run(&th, w_a, w_b, &config).await;
        let (key2, bytes2) = run(&th, w_a, w_b, &config).await;
        assert_eq!(key1, key2, "record keys must match");
        assert_eq!(bytes1, bytes2, "record bytes must match");
    }

    // --- FaultStore crash walk (plan section 3.6 rows 1-4, 10-11, 13) ---

    /// Seed two disjoint-series L0 inputs and build their parts at `cap`,
    /// returning the underlying store (ready to wrap in a `FaultStore`).
    async fn two_input_run(
        cap: u64,
    ) -> (
        MemoryStore,
        TenantHash,
        Vec<CompactionInput>,
        [u8; 32],
        Vec<BuiltPart>,
    ) {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let raw = vec![
            seed(&store, &th, Uuid::new_v4(), "metric_a").await,
            seed(&store, &th, Uuid::new_v4(), "metric_b").await,
        ];
        let (sorted, hash, parts) = prepare(&store, &th, raw, cap).await;
        (store, th, sorted, hash, parts)
    }

    /// Row 1: a fault during the LIST/footer read phase writes nothing; the
    /// scan is read-only and a re-run recovers.
    #[tokio::test]
    async fn row1_fault_during_list_is_read_only() {
        let (mem, th, _sorted, hash, _parts) = two_input_run(256 << 20).await;
        let clock = FakeClock::new(FAR_FUTURE_NS);
        let config = MaintainConfig::default();
        let fault = FaultStore::new(
            mem,
            FaultPlan::empty().with_rule(Rule::new(Op::List, ScriptedFault::Timeout)),
        );

        let err = scan_next(&fault, &clock, &config, &th, Signal::Metrics, SHARD)
            .await
            .expect_err("scan faults on LIST");
        assert!(
            matches!(err, MaintainError::Store(StoreError::Timeout)),
            "got {err}"
        );
        assert!(fault.fault_count(Op::List, FaultKind::Timeout) >= 1);
        // Nothing written: no compaction record exists.
        let rkey = record_key(&th, hash).await;
        assert!(matches!(
            fault.inner().head(&rkey).await,
            Err(StoreError::NotFound)
        ));
    }

    /// Row 2: a fault after some (not all) part PUTs leaves record-less parts
    /// under l1/; a re-run reuses the identical content-addressed keys.
    #[tokio::test]
    async fn row2_fault_after_some_part_puts() {
        let (mem, th, sorted, hash, parts) = two_input_run(1).await;
        assert_eq!(parts.len(), 2, "tiny cap must split into two parts");
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();
        let fault = FaultStore::new(
            mem,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::PartialWriteThenError)
                    .with_key_contains(".rseg")
                    .with_occurrence(Occurrence::Nth(2)),
            ),
        );
        let req = plan(&th, &sorted, hash, &parts, 0);

        let err = publish(&fault, &clock, &config, &req)
            .await
            .expect_err("second part PUT faults");
        assert!(
            matches!(err, MaintainError::Store(StoreError::Transient(_))),
            "got {err}"
        );
        assert!(fault.fault_count(Op::Put, FaultKind::PartialWriteThenError) >= 1);
        // No record references the partial part set.
        let rkey = record_key(&th, hash).await;
        assert!(matches!(
            fault.inner().head(&rkey).await,
            Err(StoreError::NotFound)
        ));

        // Re-run (Nth(2) consumed): both parts land, record publishes.
        assert_eq!(
            publish(&fault, &clock, &config, &req)
                .await
                .expect("re-run"),
            PublishOutcome::Published
        );
        fault.inner().head(&rkey).await.expect("record now durable");
    }

    /// Row 3: a fault on the record PUT (all parts already durable) leaves
    /// the same record-less state as row 2; a re-run publishes the record.
    #[tokio::test]
    async fn row3_fault_after_all_parts_before_record() {
        let (mem, th, sorted, hash, parts) = two_input_run(256 << 20).await;
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();
        let fault = FaultStore::new(
            mem,
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Put,
                    ScriptedFault::Transient("record put dropped".into()),
                )
                .with_key_contains(".cmt")
                .with_occurrence(Occurrence::Nth(1)),
            ),
        );
        let req = plan(&th, &sorted, hash, &parts, 0);

        let err = publish(&fault, &clock, &config, &req)
            .await
            .expect_err("record PUT faults");
        assert!(
            matches!(err, MaintainError::Store(StoreError::Transient(_))),
            "got {err}"
        );
        assert!(fault.fault_count(Op::Put, FaultKind::Transient) >= 1);

        let rkey = record_key(&th, hash).await;
        assert!(matches!(
            fault.inner().head(&rkey).await,
            Err(StoreError::NotFound)
        ));
        // Parts were all durable before the record PUT.
        for part in &parts {
            let key = part_key(&req, &input::hash16(&hash), part).expect("part key");
            fault.inner().head(&key).await.expect("part durable");
        }

        assert_eq!(
            publish(&fault, &clock, &config, &req)
                .await
                .expect("re-run"),
            PublishOutcome::Published
        );
        fault.inner().head(&rkey).await.expect("record durable");
    }

    /// Row 4: a dropped multipart upload leaves no visible object; the
    /// re-run reuses the identical key and completes.
    #[tokio::test]
    async fn row4_partial_multipart_upload_stays_invisible() {
        let (mem, th, sorted, hash, parts) = two_input_run(256 << 20).await;
        assert_eq!(parts.len(), 1);
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();
        let fault = FaultStore::new(
            mem,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::PartialWriteThenError)
                    .with_key_contains(".rseg")
                    .with_occurrence(Occurrence::Nth(1)),
            ),
        );
        let req = plan(&th, &sorted, hash, &parts, 0);

        let err = publish(&fault, &clock, &config, &req)
            .await
            .expect_err("part upload drops");
        assert!(matches!(
            err,
            MaintainError::Store(StoreError::Transient(_))
        ));
        assert!(fault.fault_count(Op::Put, FaultKind::PartialWriteThenError) >= 1);

        let pkey = part_key(&req, &input::hash16(&hash), &parts[0]).expect("part key");
        assert!(
            matches!(fault.inner().head(&pkey).await, Err(StoreError::NotFound)),
            "incomplete upload must leave no visible object"
        );
        let rkey = record_key(&th, hash).await;
        assert!(matches!(
            fault.inner().head(&rkey).await,
            Err(StoreError::NotFound)
        ));

        assert_eq!(
            publish(&fault, &clock, &config, &req)
                .await
                .expect("re-run reuses the key"),
            PublishOutcome::Published
        );
        fault.inner().head(&pkey).await.expect("part now durable");
    }

    /// Row 10: two compactors race the same inputs; the record CreateIfAbsent
    /// picks one winner and the loser verifies the winner's parts and
    /// converges. The loser's rejected conditional write is modeled with
    /// FailedConditionalWrite on the record key.
    #[tokio::test]
    async fn row10_racing_compactor_loser_converges() {
        let (mem, th, sorted, hash, parts) = two_input_run(256 << 20).await;
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();
        let req = plan(&th, &sorted, hash, &parts, 0);

        assert_eq!(
            publish(&mem, &clock, &config, &req)
                .await
                .expect("winner publishes"),
            PublishOutcome::Published
        );

        let fault = FaultStore::new(
            mem,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains(".cmt"),
            ),
        );
        assert_eq!(
            publish(&fault, &clock, &config, &req)
                .await
                .expect("loser converges"),
            PublishOutcome::Converged
        );
        assert!(fault.fault_count(Op::Put, FaultKind::FailedConditionalWrite) >= 1);
    }

    /// Row 11: AlreadyExists with a different input_set_hash at the same key
    /// is an invariant breach; publish alarms and deletes nothing.
    #[tokio::test]
    async fn row11_alreadyexists_different_hash_alarms() {
        let (mem, th, sorted, hash, parts) = two_input_run(256 << 20).await;
        let clock = FakeClock::new(0);
        let config = MaintainConfig::default();
        let req = plan(&th, &sorted, hash, &parts, 0);

        // Plant a foreign record before wrapping (so the plant is un-faulted).
        let rkey = record_key(&th, hash).await;
        let mut foreign = build_record(&req, 0);
        foreign.input_set_hash = vec![0x01; 32];
        mem.put(
            &rkey,
            foreign.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::Overwrite,
                checksum: None,
            },
        )
        .await
        .expect("plant foreign record");

        let fault = FaultStore::new(
            mem,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains(".cmt"),
            ),
        );
        let err = publish(&fault, &clock, &config, &req)
            .await
            .expect_err("must alarm");
        assert!(
            matches!(err, MaintainError::InputSetHashMismatch { .. }),
            "got {err}"
        );
        assert!(fault.fault_count(Op::Put, FaultKind::FailedConditionalWrite) >= 1);
    }

    /// Row 13: the abandonment interlock. A run past max_compaction_lifetime
    /// must not publish its record even while the sweeper concurrently
    /// deletes one of its parts. The late run may harmlessly re-PUT the part
    /// (it is content-addressed and unreferenced), but the record PUT is
    /// never reached, so its scripted fault cannot fire.
    #[tokio::test]
    async fn row13_abandoned_run_never_publishes() {
        let (mem, th, sorted, hash, parts) = two_input_run(256 << 20).await;
        let config = MaintainConfig::default();
        // started at 0, now well past the 1 h abandonment deadline.
        let clock = FakeClock::new(config.max_compaction_lifetime_ns + 1);
        let req = plan(&th, &sorted, hash, &parts, 0);

        // Sweeper deletes a part concurrently with the late retry.
        let pkey = part_key(&req, &input::hash16(&hash), &parts[0]).expect("part key");
        mem.delete(&pkey).await.expect("sweeper delete");

        let fault = FaultStore::new(
            mem,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains(".cmt"),
            ),
        );
        let err = publish(&fault, &clock, &config, &req)
            .await
            .expect_err("run is abandoned");
        assert!(matches!(err, MaintainError::Abandoned { .. }), "got {err}");
        // The record PUT was never attempted: its fault could not fire.
        assert_eq!(
            fault.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            0
        );
        // The part was re-referenced (harmless, content-addressed)...
        fault
            .inner()
            .head(&pkey)
            .await
            .expect("part harmlessly re-put");
        // ...but no record was published.
        let rkey = record_key(&th, hash).await;
        assert!(matches!(
            fault.inner().head(&rkey).await,
            Err(StoreError::NotFound)
        ));
    }
}

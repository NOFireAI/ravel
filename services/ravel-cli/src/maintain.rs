//! `ravel-cli maintain` subcommands (docs/compaction-retention-plan.md P8,
//! issue #115): one-shot drivers for the compaction, sweep, retention, and
//! version-audit paths, plus decode/print for `CompactionRecord` and
//! `RetentionTombstone`. Built strictly against `ravel-maintain`'s and
//! `ravel-commit`'s public APIs; no maintenance decision logic lives here.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clap::ValueEnum;
use prost::Message;
use ravel_commit::keys;
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, NoLeases, PublishOutcome, SweepReport,
    compact_bucket, sweep_shard,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::{CompactionRecord, RetentionTombstone};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

/// CLI signal selector for the `--signal` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SignalArg {
    Metrics,
    Logs,
    Spans,
}

impl SignalArg {
    fn to_signal(self) -> Signal {
        match self {
            SignalArg::Metrics => Signal::Metrics,
            SignalArg::Logs => Signal::Logs,
            SignalArg::Spans => Signal::Spans,
        }
    }
}

/// A wall clock snapshot for a one-shot CLI invocation: the whole command runs
/// at a single `now`, which is exactly what the compactor's seal check and the
/// sweep/retention horizon gates need (a moving clock matters only for a
/// long-lived loop, which the service task provides, not this CLI).
fn wall_clock() -> anyhow::Result<FixedClock> {
    Ok(FixedClock::new(crate::now_ns()?))
}

/// `maintain compact-bucket`: run one compaction pass over a single bucket.
pub async fn compact(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    shard: u32,
    hour: u32,
    dry_run: bool,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let bucket = Bucket::new(tenant_hash, signal.to_signal(), shard, hour);
    let config = CompactorConfig {
        dry_run,
        ..CompactorConfig::default()
    };
    let clock = wall_clock()?;

    let outcome = compact_bucket(store.as_ref(), &clock, &config, &bucket)
        .await
        .map_err(|err| anyhow::anyhow!("compaction failed: {err}"))?;

    println!("dry_run: {dry_run}");
    match outcome {
        CompactionOutcome::NotSealed => println!("outcome: NotSealed (bucket not yet sealed)"),
        CompactionOutcome::Tombstoned => println!("outcome: Tombstoned (retired; not compacted)"),
        CompactionOutcome::AlreadyCompacted => {
            println!("outcome: AlreadyCompacted (a compaction record already exists)")
        }
        CompactionOutcome::BelowMinInputs { count } => {
            println!("outcome: BelowMinInputs (only {count} L0 record(s); nothing to do)")
        }
        CompactionOutcome::Compacted { parts, publish } => {
            let verb = if dry_run { "would write" } else { "wrote" };
            println!("outcome: Compacted");
            println!("parts ({verb}): {parts}");
            match publish {
                PublishOutcome::Published => println!("publish: Published"),
                PublishOutcome::Converged { parts_repaired } => {
                    println!("publish: Converged (parts_repaired={parts_repaired})")
                }
                PublishOutcome::Abandoned => {
                    println!("publish: Abandoned (past lifetime deadline)")
                }
            }
        }
    }
    Ok(())
}

/// `maintain sweep`: run one sweep pass (all three GC rules) over a shard.
pub async fn sweep(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    shard: u32,
    dry_run: bool,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let config = CompactorConfig {
        dry_run,
        ..CompactorConfig::default()
    };
    let clock = wall_clock()?;

    let SweepReport {
        orphans_deleted,
        superseded_records_deleted,
        superseded_data_deleted,
        unreferenced_parts_deleted,
    } = sweep_shard(
        store.as_ref(),
        &clock,
        &config,
        &NoLeases,
        &tenant_hash,
        signal.to_signal(),
        shard,
    )
    .await
    .map_err(|err| anyhow::anyhow!("sweep failed: {err}"))?;

    let verb = if dry_run { "would delete" } else { "deleted" };
    println!("dry_run: {dry_run}");
    println!("orphans ({verb}): {orphans_deleted}");
    println!("superseded_records ({verb}): {superseded_records_deleted}");
    println!("superseded_data ({verb}): {superseded_data_deleted}");
    println!("unreferenced_parts ({verb}): {unreferenced_parts_deleted}");
    Ok(())
}

/// `maintain status`: report a bucket's current maintenance state without
/// mutating anything (so it needs no `--dry-run`).
pub async fn status(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    shard: u32,
    hour: u32,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let sig = signal.to_signal();
    let bucket = Bucket::new(tenant_hash, sig, shard, hour);
    let config = CompactorConfig::default();
    let now = crate::now_ns()?;

    let listing = ravel_maintain::read::list_bucket(store.as_ref(), &bucket)
        .await
        .map_err(|err| anyhow::anyhow!("failed to list bucket: {err}"))?;

    // Superseded-input count: the L0 inputs every compaction record names,
    // which the superseded sweep would delete once the horizon elapses. Also
    // build the referenced-L1 set to count unreferenced parts.
    let mut superseded_inputs = 0usize;
    let mut referenced_parts: HashSet<String> = HashSet::new();
    for key in &listing.compaction_record_keys {
        let got = store
            .get(key, GetRange::Full)
            .await
            .map_err(|err| anyhow::anyhow!("failed to fetch compaction record {key}: {err}"))?;
        let record = CompactionRecord::decode(got.data.as_ref())
            .map_err(|err| anyhow::anyhow!("compaction record {key} is corrupt: {err}"))?;
        superseded_inputs += record.inputs.len();
        for part in &record.parts {
            referenced_parts.insert(
                keys::reconstruct_l1_part_key(&record, part)
                    .map_err(|err| anyhow::anyhow!("failed to reconstruct L1 part key: {err}"))?,
            );
        }
    }

    // Unreferenced parts: L1 objects physically present in this bucket that no
    // compaction record references (what the unreferenced-part sweep targets).
    let l1_prefix = format!(
        "t/{}/{}/{}/{:04}/{}/",
        tenant_hash.to_hex(),
        sig.key_prefix(),
        keys::L1_DIR,
        shard,
        keys::ingest_hour_string(hour),
    );
    let l1_objects = list_all(store.as_ref(), &l1_prefix)
        .await
        .map_err(|err| anyhow::anyhow!("failed to list {l1_prefix}: {err}"))?;
    let unreferenced_parts = l1_objects
        .iter()
        .filter(|m| !referenced_parts.contains(&m.key))
        .count();

    println!("tenant: {tenant}");
    println!("signal: {:?}", sig);
    println!("shard: {shard}");
    println!("ingest_hour_bucket: {hour}");
    println!("sealed: {}", bucket.is_sealed(now, &config));
    println!("tombstoned: {}", listing.tombstone_key.is_some());
    println!(
        "compacted: {} ({} compaction record(s))",
        !listing.compaction_record_keys.is_empty(),
        listing.compaction_record_keys.len()
    );
    println!("l0_commit_records: {}", listing.commit_keys.len());
    println!("superseded_input_count: {superseded_inputs}");
    println!("l1_parts_present: {}", l1_objects.len());
    println!("unreferenced_part_count: {unreferenced_parts}");
    Ok(())
}

/// `maintain audit-versions`: a safety audit of the on-object format versions
/// live for a tenant, across all three signals (issue #115 rescoped text,
/// extended to spans by #355). For RSEG (metrics) it confirms the ADR-0027
/// single-version policy holds: any live object at a version other than the
/// one supported version is an anomaly (there is no migration path, only this
/// report). For RLOG (logs) and RSPAN (spans) it reports the live population
/// by trailer version, since neither format has a dual-reader path today: a
/// live object at an unsupported version is unreadable, so this is a safety
/// audit, not a migration tool.
///
/// The supported version is read from each reader crate's own constant
/// (`ravel_segment::VERSION_V5`, `ravel_logseg::footer::VERSION`,
/// `ravel_rspan::footer::VERSION`) so a future version bump does not silently
/// make this audit stale. Version numbers are
/// read from each surviving commit record's `segment_format_version` (an L0
/// object's liveness == a surviving commit record) and each compaction
/// record's parts (live L1 objects). Exits nonzero if any anomaly is found.
pub async fn audit_versions(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    shards: u32,
) -> anyhow::Result<()> {
    use std::collections::BTreeMap;

    let tenant_hash = TenantId::new(tenant).hash();
    let mut anomalies = 0usize;
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
        let supported: u32 = match signal {
            Signal::Metrics => u32::from(ravel_segment::VERSION_V5),
            Signal::Logs => u32::from(ravel_logseg::footer::VERSION),
            Signal::Spans => u32::from(ravel_rspan::footer::VERSION),
            other => {
                return Err(anyhow::anyhow!(
                    "audit-versions does not support signal {other:?}"
                ));
            }
        };
        // version -> (l0_count, l1_count)
        let mut hist: BTreeMap<u32, (usize, usize)> = BTreeMap::new();
        for shard in 0..shards {
            let prefix = keys::commit_shard_prefix(&tenant_hash, signal, shard)
                .map_err(|err| anyhow::anyhow!("failed to build shard prefix: {err}"))?;
            let metas = list_all(store.as_ref(), &prefix)
                .await
                .map_err(|err| anyhow::anyhow!("failed to list {prefix}: {err}"))?;
            for meta in metas {
                match keys::partition_bucket_entry(&meta.key) {
                    Ok(keys::BucketEntry::CommitRecord(_)) => {
                        let got = store.get(&meta.key, GetRange::Full).await.map_err(|err| {
                            anyhow::anyhow!("failed to fetch {}: {err}", meta.key)
                        })?;
                        let record = ravel_commit::record::decode(&got.data).map_err(|err| {
                            anyhow::anyhow!("commit record {} is corrupt: {err}", meta.key)
                        })?;
                        hist.entry(record.segment_format_version).or_default().0 += 1;
                    }
                    Ok(keys::BucketEntry::CompactionRecord(_)) => {
                        let got = store.get(&meta.key, GetRange::Full).await.map_err(|err| {
                            anyhow::anyhow!("failed to fetch {}: {err}", meta.key)
                        })?;
                        let record =
                            CompactionRecord::decode(got.data.as_ref()).map_err(|err| {
                                anyhow::anyhow!("compaction record {} is corrupt: {err}", meta.key)
                            })?;
                        for part in &record.parts {
                            hist.entry(part.segment_format_version).or_default().1 += 1;
                        }
                    }
                    Ok(keys::BucketEntry::Tombstone(_)) => {}
                    Err(err) => {
                        return Err(anyhow::anyhow!("unknown key shape {}: {err}", meta.key));
                    }
                }
            }
        }

        println!("== signal {:?} (supported version: {supported}) ==", signal);
        if hist.is_empty() {
            println!("  no live objects");
        }
        for (version, (l0, l1)) in &hist {
            let flag = if *version == supported {
                ""
            } else {
                "  <-- ANOMALY (unsupported version)"
            };
            println!("  version {version}: l0_records={l0} l1_parts={l1}{flag}");
            if *version != supported {
                anomalies += l0 + l1;
            }
        }
    }

    if anomalies > 0 {
        anyhow::bail!(
            "audit-versions found {anomalies} live object(s) at an unsupported version; there is \
             no dual-reader or migration path (ADR-0027 for RSEG, ADR-0032 for RLOG, ADR-0041 for \
             RSPAN)"
        );
    }
    println!("audit-versions: all live objects are at a supported version");
    Ok(())
}

/// The outcome of independently re-hashing one content-addressed object at
/// rest and comparing that hash against the `hash16` embedded in its key.
enum ObjectCheck {
    /// The object's content still hashes to the `hash16` its key embeds.
    Verified,
    /// The object exists but its content no longer matches its key's `hash16`:
    /// corruption that happened after the write's own CRC pre-flight passed.
    Mismatch { expected: String, actual: String },
    /// The object is absent from the store (a plain `NotFound`, not a
    /// transient error). Whether this is an anomaly is the caller's call: a
    /// surviving record whose object vanished is corruption, but a compaction
    /// input the sweeper legitimately reclaimed is expected.
    Missing,
}

/// Extract the `hash16` component a data/part key embeds. Both the L0
/// (`writer.epoch.seq.hash16.rseg`) and L1
/// (`input_set_hash16.part.hash16.rseg`) filenames carry the object's own
/// content hash prefix as the dot-segment immediately before the `.rseg`
/// suffix, so one extraction serves both. Keys handed here are reconstructed
/// via `ravel-commit`'s own builders, so they are always well-formed.
fn embedded_hash16(key: &str) -> anyhow::Result<&str> {
    let filename = key.rsplit('/').next().unwrap_or(key);
    let stem = filename
        .strip_suffix(".rseg")
        .ok_or_else(|| anyhow::anyhow!("object key {key} has no .rseg suffix"))?;
    stem.rsplit('.')
        .next()
        .filter(|s| s.len() == 16)
        .ok_or_else(|| anyhow::anyhow!("object key {key} has no hash16 component"))
}

/// GET the object at `key`, hash its bytes with blake3, and compare the hex of
/// the first 8 bytes against the `hash16` embedded in the key. This is the
/// same content-addressing invariant `S3Store` checks pre-flight at write
/// time, re-verified here independently and at rest, so corruption that
/// happened after a successful write is caught.
async fn check_object(store: &dyn ObjectStoreBackend, key: &str) -> anyhow::Result<ObjectCheck> {
    let got = match store.get(key, GetRange::Full).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(ObjectCheck::Missing),
        Err(err) => return Err(anyhow::anyhow!("failed to fetch {key}: {err}")),
    };
    let expected = embedded_hash16(key)?.to_string();
    let actual = hex::encode(&blake3::hash(got.data.as_ref()).as_bytes()[..8]);
    if actual == expected {
        Ok(ObjectCheck::Verified)
    } else {
        Ok(ObjectCheck::Mismatch { expected, actual })
    }
}

/// `maintain verify-custody`: independently re-verify the content-addressed
/// chain for a tenant, at rest and after the fact (ADR-0042 decision 5). It
/// extends `audit_versions`'s tenant/shard-scoped per-object walk (same
/// liveness definition: an L0 object is live iff a surviving commit record
/// references it, an L1 object iff a surviving compaction record references
/// it) with two content-hash checks. This command only reads; there is no
/// `--dry-run` because it never writes or deletes.
///
/// It distinguishes exactly three outcomes:
///
/// - **content-hash mismatch** (ANOMALY): an object that exists but whose
///   bytes no longer hash to the `hash16` its key embeds. Post-write
///   corruption; the write-time CRC cannot catch it.
/// - **missing-and-unexpected** (ANOMALY): a live data object (referenced by a
///   surviving commit or compaction record) that is absent from the store.
///   A surviving record must have its object.
/// - **missing-but-expected** (NOT an anomaly): a compaction record's recorded
///   input identity that no longer resolves to a present object. The sweeper
///   legitimately reclaims superseded inputs once past their protection
///   horizon, so this is steady-state behavior, reported as a count only.
///
/// Exits nonzero if any anomaly is found.
pub async fn verify_custody(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    shards: u32,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let mut anomalies = 0usize;

    // Aggregate counters for the closing summary.
    let mut data_objects_verified = 0usize; // live L0/L1 object, hash matches key
    let mut content_mismatches = 0usize; // exists, wrong hash: ANOMALY
    let mut missing_live_objects = 0usize; // surviving record, object gone: ANOMALY
    let mut inputs_verified = 0usize; // compaction input resolved and hash matches
    let mut inputs_swept = 0usize; // compaction input no longer present: expected

    for signal in [Signal::Metrics, Signal::Logs] {
        println!("== signal {:?} ==", signal);
        for shard in 0..shards {
            // A compaction record's input identities carry only
            // (writer_id, epoch, seq), never a content hash, so they cannot be
            // reconstructed into a content-addressed L0 key directly. List the
            // shard's physical L0 objects once and index them by identity, so
            // each input can be resolved to the key it actually lives at (if
            // any) and its content re-verified.
            let l0_prefix = format!(
                "t/{}/{}/l0/{:04}/",
                tenant_hash.to_hex(),
                signal.key_prefix(),
                shard,
            );
            let l0_objects = list_all(store.as_ref(), &l0_prefix)
                .await
                .map_err(|err| anyhow::anyhow!("failed to list {l0_prefix}: {err}"))?;
            let mut l0_by_identity: HashMap<(Uuid, u64, u64), String> = HashMap::new();
            for meta in &l0_objects {
                if let Ok(parsed) = keys::parse_data_key(&meta.key) {
                    l0_by_identity.insert(
                        (parsed.writer_id, parsed.epoch, parsed.seq),
                        meta.key.clone(),
                    );
                }
            }

            let prefix = keys::commit_shard_prefix(&tenant_hash, signal, shard)
                .map_err(|err| anyhow::anyhow!("failed to build shard prefix: {err}"))?;
            let metas = list_all(store.as_ref(), &prefix)
                .await
                .map_err(|err| anyhow::anyhow!("failed to list {prefix}: {err}"))?;
            for meta in metas {
                match keys::partition_bucket_entry(&meta.key) {
                    Ok(keys::BucketEntry::CommitRecord(_)) => {
                        let got = store.get(&meta.key, GetRange::Full).await.map_err(|err| {
                            anyhow::anyhow!("failed to fetch {}: {err}", meta.key)
                        })?;
                        let record = ravel_commit::record::decode(&got.data).map_err(|err| {
                            anyhow::anyhow!("commit record {} is corrupt: {err}", meta.key)
                        })?;
                        let data_key = keys::reconstruct_data_key(&record).map_err(|err| {
                            anyhow::anyhow!(
                                "failed to reconstruct data key for {}: {err}",
                                meta.key
                            )
                        })?;
                        match check_object(store.as_ref(), &data_key).await? {
                            ObjectCheck::Verified => data_objects_verified += 1,
                            ObjectCheck::Mismatch { expected, actual } => {
                                content_mismatches += 1;
                                anomalies += 1;
                                println!(
                                    "  CONTENT MISMATCH (l0) {data_key}: key hash16={expected} \
                                     content hash16={actual}  <-- ANOMALY"
                                );
                            }
                            ObjectCheck::Missing => {
                                missing_live_objects += 1;
                                anomalies += 1;
                                println!(
                                    "  MISSING LIVE OBJECT (l0) {data_key}: referenced by \
                                     surviving commit record {}  <-- ANOMALY",
                                    meta.key
                                );
                            }
                        }
                    }
                    Ok(keys::BucketEntry::CompactionRecord(_)) => {
                        let got = store.get(&meta.key, GetRange::Full).await.map_err(|err| {
                            anyhow::anyhow!("failed to fetch {}: {err}", meta.key)
                        })?;
                        let record =
                            CompactionRecord::decode(got.data.as_ref()).map_err(|err| {
                                anyhow::anyhow!("compaction record {} is corrupt: {err}", meta.key)
                            })?;

                        // Live L1 objects: every part a surviving compaction
                        // record references must exist and still match.
                        for part in &record.parts {
                            let part_key =
                                keys::reconstruct_l1_part_key(&record, part).map_err(|err| {
                                    anyhow::anyhow!("failed to reconstruct L1 part key: {err}")
                                })?;
                            match check_object(store.as_ref(), &part_key).await? {
                                ObjectCheck::Verified => data_objects_verified += 1,
                                ObjectCheck::Mismatch { expected, actual } => {
                                    content_mismatches += 1;
                                    anomalies += 1;
                                    println!(
                                        "  CONTENT MISMATCH (l1) {part_key}: key hash16={expected} \
                                         content hash16={actual}  <-- ANOMALY"
                                    );
                                }
                                ObjectCheck::Missing => {
                                    missing_live_objects += 1;
                                    anomalies += 1;
                                    println!(
                                        "  MISSING LIVE OBJECT (l1) {part_key}: referenced by \
                                         surviving compaction record {}  <-- ANOMALY",
                                        meta.key
                                    );
                                }
                            }
                        }

                        // Recorded inputs: resolve each identity to the L0 key
                        // it lives at. A resolved-and-present input must still
                        // match its hash; an input the sweeper reclaimed is
                        // expected, counted but never an anomaly.
                        for input in &record.inputs {
                            let writer_id = Uuid::parse_str(&input.writer_id).map_err(|_| {
                                anyhow::anyhow!(
                                    "compaction record {} has an invalid input writer_id {:?}",
                                    meta.key,
                                    input.writer_id
                                )
                            })?;
                            let identity = (writer_id, input.writer_epoch, input.writer_seq);
                            let Some(input_key) = l0_by_identity.get(&identity) else {
                                // Unresolved: no surviving L0 object for this
                                // identity. Expected once the sweeper reclaims
                                // a superseded input past its horizon.
                                inputs_swept += 1;
                                continue;
                            };
                            match check_object(store.as_ref(), input_key).await? {
                                ObjectCheck::Verified => inputs_verified += 1,
                                ObjectCheck::Mismatch { expected, actual } => {
                                    content_mismatches += 1;
                                    anomalies += 1;
                                    println!(
                                        "  CONTENT MISMATCH (input) {input_key}: key \
                                         hash16={expected} content hash16={actual}  <-- ANOMALY"
                                    );
                                }
                                // Listed a moment ago but gone on GET: a
                                // concurrent sweep raced us. Expected, not an
                                // anomaly.
                                ObjectCheck::Missing => inputs_swept += 1,
                            }
                        }
                    }
                    Ok(keys::BucketEntry::Tombstone(_)) => {}
                    Err(err) => {
                        return Err(anyhow::anyhow!("unknown key shape {}: {err}", meta.key));
                    }
                }
            }
        }
    }

    println!("verify-custody summary:");
    println!("  live data objects verified (content hash matches key): {data_objects_verified}");
    println!("  compaction inputs resolved and verified: {inputs_verified}");
    println!(
        "  compaction inputs no longer present (expected; legitimately swept): {inputs_swept}"
    );
    println!("  content-hash mismatches (ANOMALY): {content_mismatches}");
    println!("  live objects missing from store (ANOMALY): {missing_live_objects}");
    println!("  total anomalies: {anomalies}");

    if anomalies > 0 {
        anyhow::bail!(
            "verify-custody found {anomalies} custody anomaly(ies): {content_mismatches} \
             content-hash mismatch(es) and {missing_live_objects} live object(s) missing from \
             the store (a mismatch is post-write corruption; a missing live object is a surviving \
             record whose data vanished)"
        );
    }
    println!("verify-custody: content-addressed chain intact for every live object");
    Ok(())
}

/// Decode and print a `CompactionRecord` (proto), mirroring `commit decode`'s
/// field-by-field style. Reports the compaction identity plus every input
/// identity and every part's summary and level/part_index/version.
pub fn decode_compaction_record(bytes: &[u8]) -> anyhow::Result<()> {
    let record = CompactionRecord::decode(bytes)
        .map_err(|err| anyhow::anyhow!("failed to decode compaction record: {err}"))?;
    println!("format_version: {}", record.format_version);
    println!("tenant_hash: {}", hex::encode(&record.tenant_hash));
    println!("signal: {}", record.signal);
    println!("shard: {}", record.shard);
    println!("ingest_hour_bucket: {}", record.ingest_hour_bucket);
    println!("level: {}", record.level);
    println!("input_set_hash: {}", hex::encode(&record.input_set_hash));
    println!("created_unix_ns: {}", record.created_unix_ns);
    println!("inputs: {}", record.inputs.len());
    for input in &record.inputs {
        println!(
            "  writer_id={} writer_epoch={} writer_seq={}",
            input.writer_id, input.writer_epoch, input.writer_seq
        );
    }
    println!("parts: {}", record.parts.len());
    for part in &record.parts {
        println!(
            "  part_index={} first_series_id={} last_series_id={} content_hash={} \
             object_size={} sample_count={} series_count={} run_count={} \
             min_event_ts_ns={} max_event_ts_ns={} segment_format_version={}",
            part.part_index,
            hex::encode(&part.first_series_id),
            hex::encode(&part.last_series_id),
            hex::encode(&part.content_hash),
            part.object_size,
            part.sample_count,
            part.series_count,
            part.run_count,
            part.min_event_ts_ns,
            part.max_event_ts_ns,
            part.segment_format_version,
        );
    }
    Ok(())
}

/// Decode and print a `RetentionTombstone` (proto).
pub fn decode_retention_tombstone(bytes: &[u8]) -> anyhow::Result<()> {
    let tombstone = RetentionTombstone::decode(bytes)
        .map_err(|err| anyhow::anyhow!("failed to decode retention tombstone: {err}"))?;
    println!("format_version: {}", tombstone.format_version);
    println!("tenant_hash: {}", hex::encode(&tombstone.tenant_hash));
    println!("signal: {}", tombstone.signal);
    println!("shard: {}", tombstone.shard);
    println!("ingest_hour_bucket: {}", tombstone.ingest_hour_bucket);
    println!("retired_at_ns: {}", tombstone.retired_at_ns);
    println!("retention_window_ns: {}", tombstone.retention_window_ns);
    println!("record_count_observed: {}", tombstone.record_count_observed);
    Ok(())
}

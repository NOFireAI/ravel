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
    Bucket, CompactionOutcome, CompactorConfig, FamilyMigrateReport, FixedClock, LegalHoldCheck,
    MigrateBudget, PublishOutcome, SweepReport, Verification, compact_bucket, migrate_family,
    sweep_shard,
};
use ravel_object_store::conformance::NoncurrentVersionSource;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::{CompactionRecord, RetentionTombstone};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

/// CLI signal selector for the `--signal` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SignalArg {
    Metrics,
    Logs,
    Spans,
}

impl SignalArg {
    pub(crate) fn to_signal(self) -> Signal {
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
///
/// Refreshes the tenant's [`LegalHoldCheck`] before the pass, matching the
/// server driver's semantics (ADR-0048 decision 1): the refresh happens once,
/// per invocation, with no flag to skip it. A refresh failure skips the whole
/// pass (`Err`, not a fallback to `NoLeases`), since running the sweep
/// unprotected would convert a transient store fault into an unprotected
/// delete pass.
pub async fn sweep(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    shard: u32,
    dry_run: bool,
    override_orphan_breaker: bool,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let config = CompactorConfig {
        dry_run,
        force_orphan_gc: override_orphan_breaker,
        ..CompactorConfig::default()
    };
    let clock = wall_clock()?;

    let hold = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "legal hold refresh failed for tenant {tenant}: {err}; sweep skipped this \
                 invocation rather than falling back to unprotected (rerun once the hold shard \
                 is reachable)"
            )
        })?;

    let SweepReport {
        orphans_deleted,
        superseded_records_deleted,
        superseded_data_deleted,
        unreferenced_parts_deleted,
        orphan_breaker_tripped,
        orphans_withheld,
        orphan_breaker_overridden,
        full_pass,
    } = sweep_shard(
        store.as_ref(),
        &clock,
        &config,
        &hold,
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
    println!("full_pass: {full_pass}");
    if orphan_breaker_tripped {
        println!(
            "orphan breaker: TRIPPED, {orphans_withheld} candidates withheld, deleted nothing \
             (halt is sticky; see docs/consistency-model.md \"Deletion and GC\")"
        );
    } else if orphan_breaker_overridden {
        println!("orphan breaker: overridden by force_orphan_gc, deleted despite tripping");
    }
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

/// The generation-aware shard scan range for one (tenant, signal): the union
/// of every generation's shard range, i.e. the largest `shard_count` across the
/// tenant's shard-generation history (ADR-0052 section 4). This mirrors the
/// server maintain loop (`ravel_server::maintain::run_tick`): after an increase
/// it covers the new, wider shards; after a decrease it keeps covering the old,
/// wider shards until retention ages their hours out. An empty high shard lists
/// cheaply and is tolerated by the per-shard loops. Read fresh and uncached.
///
/// Fail-closed on a read error: a CLI maintenance pass over a possibly-truncated
/// shard range would silently skip live shards, so this refuses rather than
/// scanning `0..configured_shards` blindly. An absent record (`Ok(None)`) is
/// the single implicit generation at the configured `--shards`, unchanged from
/// pre-ADR-0052 behavior.
async fn generation_scan_shards(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    configured_shards: u32,
) -> anyhow::Result<u32> {
    match ravel_catalog::read_generations_from_store(store, tenant, signal).await {
        Ok(Some(generations)) => Ok(generations
            .iter()
            .map(|g| g.shard_count)
            .max()
            .unwrap_or(configured_shards)),
        Ok(None) => Ok(configured_shards),
        Err(err) => Err(anyhow::anyhow!(
            "shard-generation history read failed for signal {signal:?}: {err}; refusing to \
             scan a possibly-truncated shard range (ADR-0052 section 4)"
        )),
    }
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
/// (`ravel_segment::VERSION_V6`, `ravel_logseg::footer::VERSION`,
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
            Signal::Metrics => u32::from(ravel_segment::VERSION_V6),
            Signal::Logs => u32::from(ravel_logseg::footer::VERSION),
            Signal::Spans => u32::from(ravel_rspan::footer::VERSION),
            other => {
                return Err(anyhow::anyhow!(
                    "audit-versions does not support signal {other:?}"
                ));
            }
        };
        // Generation-aware scan range (ADR-0052 section 4), not the static
        // `--shards`: a reshard-increase widens the live shard set, and this
        // audit must visit every shard any generation ever wrote or it would
        // silently skip live objects in the new range.
        let scan_shards =
            generation_scan_shards(store.as_ref(), &tenant_hash, signal, shards).await?;
        // version -> (l0_count, l1_count)
        let mut hist: BTreeMap<u32, (usize, usize)> = BTreeMap::new();
        for shard in 0..scan_shards {
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
                    Ok(keys::BucketEntry::RewriteRecord(_)) => {
                        // A selective-erasure rewrite record (ADR-0064) names
                        // L1 output parts exactly as a compaction record does;
                        // count their segment format versions the same way.
                        let got = store.get(&meta.key, GetRange::Full).await.map_err(|err| {
                            anyhow::anyhow!("failed to fetch {}: {err}", meta.key)
                        })?;
                        let record = ravel_commit::erasure::decode_rewrite(got.data.as_ref())
                            .map_err(|err| {
                                anyhow::anyhow!("rewrite record {} is corrupt: {err}", meta.key)
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

/// The canonical format-family identifier for a signal's on-object format
/// (ADR-0066 decision 3): the lowercase family string the format floor is keyed
/// by. One family per signal today (metrics=RSEG, logs=RLOG, spans=RSPAN).
fn signal_family(signal: Signal) -> &'static str {
    match signal {
        Signal::Metrics => "rseg",
        Signal::Logs => "rlog",
        Signal::Spans => "rspan",
        _ => "unknown",
    }
}

/// The current supported on-object format version for a signal, read from each
/// reader crate's own constant so a future version bump does not silently make
/// the default migration target stale. Mirrors `audit_versions`' supported
/// versions.
fn signal_current_version(signal: Signal) -> anyhow::Result<u32> {
    Ok(match signal {
        Signal::Metrics => u32::from(ravel_segment::VERSION_V6),
        Signal::Logs => u32::from(ravel_logseg::footer::VERSION),
        Signal::Spans => u32::from(ravel_rspan::footer::VERSION),
        other => {
            return Err(anyhow::anyhow!("migrate does not support signal {other:?}"));
        }
    })
}

/// `maintain migrate`: raise a `(tenant, signal, format family)`'s recorded
/// format floor to `target_version`, migrating every live record still below it
/// first (epic EM, EM-T5; issues #770, #463). The same operation the server
/// maintain loop can call via [`migrate_family`]; this is its one-shot CLI
/// driver.
///
/// Resumable and bounded: one invocation migrates at most `--budget-records` L0
/// records (0 = unlimited) before persisting its durable cursor and returning,
/// so a large migration runs across repeated invocations. Once a walk drains
/// within budget, migrate re-audits fresh and raises the floor only if nothing
/// below the target survives; if a straggler is found the floor is left
/// untouched and this exits nonzero, reporting what it found. The re-audit
/// already excludes a bucket's pre-rewrite commit records once that bucket
/// carries a compaction/rewrite record (dead, sweepable leftovers of a
/// rewrite this same invocation may have just performed, not stragglers;
/// issue #826), so a clean migration converges and raises the floor in one
/// invocation -- no interleaved `sweep` required. A reported straggler is
/// therefore genuine below-target live data still to migrate.
///
/// `target_version` defaults to the signal's current supported version
/// ([`signal_current_version`]); `family` defaults to the signal's canonical
/// family ([`signal_family`]).
#[allow(clippy::too_many_arguments)]
pub async fn migrate(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    shards: u32,
    target_version: Option<u32>,
    family: Option<String>,
    budget_records: u64,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let sig = signal.to_signal();
    let target = match target_version {
        Some(v) => v,
        None => signal_current_version(sig)?,
    };
    let family = family.unwrap_or_else(|| signal_family(sig).to_string());
    let clock = wall_clock()?;
    let config = CompactorConfig::default();
    let budget = MigrateBudget {
        max_records: budget_records,
    };

    let report: FamilyMigrateReport = migrate_family(
        store.as_ref(),
        &clock,
        &config,
        tenant_hash,
        sig,
        &family,
        target,
        shards,
        budget,
        "ravel-cli maintain migrate",
    )
    .await
    .map_err(|err| anyhow::anyhow!("migrate failed: {err}"))?;

    println!("tenant: {tenant}");
    println!("signal: {sig:?}");
    println!("family: {family}");
    println!("target_version: {target}");
    println!("budget_records: {budget_records} (0 = unlimited)");
    println!("buckets_examined: {}", report.buckets_examined);
    println!("buckets_migrated: {}", report.buckets_migrated);
    println!("records_migrated: {}", report.records_migrated);
    if let Some((shard, hour)) = report.cursor_advanced_to {
        println!("cursor_advanced_to: shard={shard} hour={hour}");
    }
    println!("walk_complete: {}", report.walk_complete);

    if !report.walk_complete {
        println!(
            "budget exhausted; the walk did not finish. Re-run migrate to resume from the \
             persisted cursor (the floor is not raised until the walk drains and the re-audit is \
             clean)."
        );
        return Ok(());
    }

    match report.verification {
        Some(Verification::FloorRaised { floor_version }) => {
            println!("verification: clean (no records below target)");
            println!("floor_raised_to: {floor_version}");
            Ok(())
        }
        Some(Verification::Stragglers { l0, l1 }) => {
            println!(
                "verification: FOUND STRAGGLERS l0_commit_records={l0} l1_compaction_parts={l1}"
            );
            anyhow::bail!(
                "migrate refused to raise the {family} floor for tenant {tenant} signal {sig:?}: \
                 the fresh re-audit found {l0} commit record(s) and {l1} compaction part(s) still \
                 below target version {target} (data landed below the target between the walk \
                 finishing and the floor raise, or is not yet migratable -- e.g. still \
                 unsealed). These are genuinely live: a bucket's own pre-rewrite commit records \
                 are already excluded from this count once that bucket has been migrated, so a \
                 `sweep` will not make this converge. The floor was NOT raised; re-run migrate \
                 once the stragglers are at or above the target."
            )
        }
        None => {
            // walk_complete is true, so verification is always set; defend
            // against a future refactor rather than panicking.
            anyhow::bail!("migrate completed the walk but produced no verification result")
        }
    }
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

/// Check one live object's write time against the tenant's recorded key-epoch
/// history (ADR-0062 decision 1b). When `epochs` is `None` the tenant has no
/// `t/<hash>/enc` record, so every object was written under the deployment
/// default key and there is no epoch contradiction to check. When it is
/// `Some`, the object's write time must locate it in exactly one epoch
/// ([`ravel_catalog::epoch_for_write`] cannot return a gap by construction: it
/// finds the latest epoch whose `activated_ns <= write_ns`, so any write past
/// epoch 0's activation always locates); a write time that predates the
/// tenant's first recorded epoch is a distinct custody anomaly, counted and
/// reported like a content-hash mismatch.
///
/// REQUIRES that whatever writes the first key-epoch record for a tenant
/// (server startup wiring, EL-7) bootstraps epoch 0 as `key_arn: ""`
/// (deployment default) with an `activated_ns` at or before the tenant's
/// earliest live object, not just at "whenever the operator first configures
/// a per-tenant key." Otherwise every object written before that
/// configuration moment -- the ordinary case for a tenant that ingested under
/// the default for months before opting into a per-tenant key -- reads as a
/// false-positive anomaly here, since it predates the (wrongly late) epoch 0.
fn check_object_epoch(
    epochs: Option<&[ravel_catalog::KeyEpoch]>,
    write_ns: i64,
    level: &str,
    object_key: &str,
    located: &mut usize,
    inconsistencies: &mut usize,
    anomalies: &mut usize,
) {
    let Some(epochs) = epochs else {
        return;
    };
    match ravel_catalog::epoch_for_write(epochs, write_ns) {
        Some(_) => *located += 1,
        None => {
            *inconsistencies += 1;
            *anomalies += 1;
            let first = epochs.first().map(|e| e.activated_ns).unwrap_or(0);
            println!(
                "  EPOCH INCONSISTENCY ({level}) {object_key}: write time {write_ns} ns predates \
                 the tenant's first recorded key epoch (activated_ns {first})  <-- ANOMALY"
            );
        }
    }
}

/// `maintain verify-custody`: independently re-verify the content-addressed
/// chain for a tenant, at rest and after the fact (ADR-0042 decision 5). It
/// extends `audit_versions`'s tenant/shard-scoped per-object walk (same
/// liveness definition: an L0 object is live iff a surviving commit record
/// references it, an L1 object iff a surviving compaction record references
/// it) with two content-hash checks, plus a key-epoch consistency check
/// (ADR-0062 decision 1b). This command only reads; there is no `--dry-run`
/// because it never writes or deletes.
///
/// It distinguishes these outcomes:
///
/// - **content-hash mismatch** (ANOMALY): an object that exists but whose
///   bytes no longer hash to the `hash16` its key embeds. Post-write
///   corruption; the write-time CRC cannot catch it.
/// - **missing-and-unexpected** (ANOMALY): a live data object (referenced by a
///   surviving commit or compaction record) that is absent from the store.
///   A surviving record must have its object.
/// - **key-epoch inconsistency** (ANOMALY): when the tenant has a
///   `t/<hash>/enc` key-epoch record, a live object whose write time predates
///   the tenant's first recorded epoch. Every object's write time must locate
///   it in exactly one epoch, so "which key encrypts what" stays answerable
///   from the bucket alone. A tenant with no epoch record used the deployment
///   default key, so there is no contradiction to check. Requires the epoch-0
///   bootstrap discipline documented on [`check_object_epoch`] -- otherwise
///   this reports a false positive for every pre-existing object once a
///   tenant's first per-tenant key is configured.
/// - **missing-but-expected** (NOT an anomaly): a compaction record's recorded
///   input identity that no longer resolves to a present object. The sweeper
///   legitimately reclaims superseded inputs once past their protection
///   horizon, so this is steady-state behavior, reported as a count only.
/// - **recoverable-prior-version** (ANOMALY, versioning-aware mode only): on a
///   versioned bucket, a Ravel delete leaves a recoverable prior version that
///   the content-addressed walk above cannot see (ADR-0064 §7, S4-12). Its own
///   distinct anomaly class, separate from the four above.
///
/// `versioning_aware` enables the recoverable-prior-version check. It needs a
/// versioned listing the `ObjectStoreBackend` contract does not expose, so
/// against a real backend it reports an honest gap (not an anomaly); the check
/// itself lives in [`check_noncurrent_versions`], which a versioning-aware
/// fixture drives directly.
///
/// Exits nonzero if any anomaly is found.
pub async fn verify_custody(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    shards: u32,
    versioning_aware: bool,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let mut anomalies = 0usize;

    // The tenant's key-epoch history (ADR-0062 decision 1b), read once and
    // fresh. `None` means no per-tenant key was ever configured: every object
    // is under the deployment default, so the epoch check is skipped. A corrupt
    // or misfiled record fails closed here rather than being read as "no key".
    let key_epochs = ravel_catalog::read_epochs_from_store(store.as_ref(), &tenant_hash)
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "key-epoch record read failed for tenant {tenant}: {err}; refusing to verify \
                 custody without a trustworthy epoch history (ADR-0062 decision 1b)"
            )
        })?;
    match &key_epochs {
        Some(epochs) => println!(
            "key-epoch history: {} epoch(s) recorded; checking every live object's write time \
             against it",
            epochs.len()
        ),
        None => println!(
            "key-epoch history: none recorded; every object is under the deployment default key \
             (no epoch check)"
        ),
    }

    // Aggregate counters for the closing summary.
    let mut data_objects_verified = 0usize; // live L0/L1 object, hash matches key
    let mut content_mismatches = 0usize; // exists, wrong hash: ANOMALY
    let mut missing_live_objects = 0usize; // surviving record, object gone: ANOMALY
    let mut inputs_verified = 0usize; // compaction input resolved and hash matches
    let mut inputs_swept = 0usize; // compaction input no longer present: expected
    let mut epoch_objects_located = 0usize; // live object located in a recorded epoch
    let mut epoch_inconsistencies = 0usize; // live object outside every recorded epoch: ANOMALY
    let mut recoverable_prior_versions = 0usize; // noncurrent version under a swept key: ANOMALY

    for signal in [Signal::Metrics, Signal::Logs] {
        println!("== signal {:?} ==", signal);
        // Generation-aware scan range (ADR-0052 section 4), not the static
        // `--shards`: custody must be verified across every shard any
        // generation ever wrote, so a reshard-increase's new shards are not
        // silently skipped.
        let scan_shards =
            generation_scan_shards(store.as_ref(), &tenant_hash, signal, shards).await?;
        for shard in 0..scan_shards {
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
                        // Key-epoch consistency: the commit record's
                        // created_unix_ns is used as this object's write time.
                        // It is an approximation, not the exact PUT time --
                        // durability order writes the data object first and
                        // the commit record after, so created_unix_ns is
                        // always >= the object's real write time. This can
                        // only make the check MORE permissive at an epoch
                        // boundary (an object written just before an epoch
                        // activation but committed just after is attributed
                        // to the later epoch), never produce a false anomaly.
                        check_object_epoch(
                            key_epochs.as_deref(),
                            record.created_unix_ns,
                            "l0",
                            &data_key,
                            &mut epoch_objects_located,
                            &mut epoch_inconsistencies,
                            &mut anomalies,
                        );
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
                            // Key-epoch consistency: an L1 part's write time
                            // is approximated by its compaction record's
                            // created_unix_ns (>= the part's real write time,
                            // same durability-order caveat as the L0 case
                            // above), shared by every part the record
                            // produced.
                            check_object_epoch(
                                key_epochs.as_deref(),
                                record.created_unix_ns,
                                "l1",
                                &part_key,
                                &mut epoch_objects_located,
                                &mut epoch_inconsistencies,
                                &mut anomalies,
                            );
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
                    Ok(keys::BucketEntry::RewriteRecord(_)) => {
                        // A selective-erasure rewrite record (ADR-0064 decision
                        // 3) names live L1 output parts exactly as a compaction
                        // record does; verify each exists and still matches its
                        // content hash. Input-identity resolution and the
                        // versioning-aware custody surface for erased data are
                        // EJ-T6's to extend; this only covers the surviving
                        // rewritten parts so verify-custody does not silently
                        // ignore a bucket that has been erased once.
                        let got = store.get(&meta.key, GetRange::Full).await.map_err(|err| {
                            anyhow::anyhow!("failed to fetch {}: {err}", meta.key)
                        })?;
                        let record = ravel_commit::erasure::decode_rewrite(got.data.as_ref())
                            .map_err(|err| {
                                anyhow::anyhow!("rewrite record {} is corrupt: {err}", meta.key)
                            })?;
                        for part in &record.parts {
                            let part_key = keys::reconstruct_rewrite_part_key(&record, part)
                                .map_err(|err| {
                                    anyhow::anyhow!("failed to reconstruct rewrite part key: {err}")
                                })?;
                            match check_object(store.as_ref(), &part_key).await? {
                                ObjectCheck::Verified => data_objects_verified += 1,
                                ObjectCheck::Mismatch { expected, actual } => {
                                    content_mismatches += 1;
                                    anomalies += 1;
                                    println!(
                                        "  CONTENT MISMATCH (l1 rewrite) {part_key}: key \
                                         hash16={expected} content hash16={actual}  <-- ANOMALY"
                                    );
                                }
                                ObjectCheck::Missing => {
                                    missing_live_objects += 1;
                                    anomalies += 1;
                                    println!(
                                        "  MISSING LIVE OBJECT (l1 rewrite) {part_key}: referenced \
                                         by surviving rewrite record {}  <-- ANOMALY",
                                        meta.key
                                    );
                                }
                            }
                            check_object_epoch(
                                key_epochs.as_deref(),
                                record.created_unix_ns,
                                "l1",
                                &part_key,
                                &mut epoch_objects_located,
                                &mut epoch_inconsistencies,
                                &mut anomalies,
                            );
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

    // Versioning-aware pass (ADR-0064 §7, S4-12): on a versioned bucket, list
    // noncurrent versions under the tenant's keys and report each as the
    // distinct recoverable-prior-version anomaly class. Against a real backend
    // the source cannot enumerate versions, so this reports an honest gap
    // rather than a false clean bill; a versioning-aware fixture surfaces the
    // prior versions it holds.
    if versioning_aware {
        let tenant_prefix = format!("t/{}/", tenant_hash.to_hex());
        println!("== versioning-aware: noncurrent versions under {tenant_prefix} ==");
        let report = check_noncurrent_versions(store.as_ref(), &tenant_prefix).await?;
        if report.supported {
            recoverable_prior_versions = report.recoverable_versions;
            anomalies += recoverable_prior_versions;
        } else {
            println!(
                "  backend cannot enumerate noncurrent versions through the ObjectStoreBackend \
                 contract (object_store 0.14 has no versioned listing); reporting an honest gap, \
                 not an anomaly. On a versioned bucket, confirm noncurrent-version expiration is \
                 configured (ADR-0064 §7) out of band"
            );
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
    if key_epochs.is_some() {
        println!("  live objects located in a recorded key epoch: {epoch_objects_located}");
        println!("  key-epoch inconsistencies (ANOMALY): {epoch_inconsistencies}");
    }
    if versioning_aware {
        println!(
            "  recoverable prior versions (ANOMALY, recoverable-prior-version): \
             {recoverable_prior_versions}"
        );
    }
    println!("  total anomalies: {anomalies}");

    if anomalies > 0 {
        anyhow::bail!(
            "verify-custody found {anomalies} custody anomaly(ies): {content_mismatches} \
             content-hash mismatch(es), {missing_live_objects} live object(s) missing from the \
             store, {epoch_inconsistencies} key-epoch inconsistency(ies), and \
             {recoverable_prior_versions} recoverable prior version(s) (a mismatch is \
             post-write corruption; a missing live object is a surviving record whose data \
             vanished; an epoch inconsistency is a live object whose write time falls outside \
             every recorded key epoch; a recoverable prior version is data deleted on a versioned \
             bucket that a prior-version restore could resurrect, ADR-0064 §7)"
        );
    }
    println!("verify-custody: content-addressed chain intact for every live object");
    Ok(())
}

/// Outcome of the versioning-aware [`check_noncurrent_versions`] pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoncurrentVersionReport {
    /// Whether the source could enumerate versioned listings at all. `false`
    /// is the honest gap through the `ObjectStoreBackend` contract, not an
    /// anomaly.
    pub supported: bool,
    /// Number of noncurrent (prior) versions found: each one a distinct
    /// recoverable-prior-version anomaly.
    pub recoverable_versions: usize,
}

/// Versioning-aware custody check (ADR-0064 §7, S4-12): list noncurrent
/// versions under `tenant_prefix` and report each as the distinct "deleted but
/// recoverable as prior version" anomaly class, separate from the
/// content-mismatch, missing-object, and epoch-inconsistency classes.
///
/// When the source cannot enumerate versions (`supported: false`, every
/// production backend through the trait contract), this reports zero anomalies:
/// it cannot see prior versions, so it neither confirms nor denies them, an
/// honest gap rather than a false clean bill. A noncurrent version only ever
/// exists on a versioned bucket, so a nonzero count here is proof both that the
/// bucket is versioned and that a recoverable prior version is present.
pub async fn check_noncurrent_versions<S: NoncurrentVersionSource + ?Sized>(
    source: &S,
    tenant_prefix: &str,
) -> anyhow::Result<NoncurrentVersionReport> {
    let listing = source
        .list_noncurrent_versions(tenant_prefix)
        .await
        .map_err(|err| {
            anyhow::anyhow!("failed to list noncurrent versions under {tenant_prefix}: {err}")
        })?;
    if !listing.supported {
        return Ok(NoncurrentVersionReport {
            supported: false,
            recoverable_versions: 0,
        });
    }
    for version in &listing.versions {
        println!(
            "  DELETED BUT RECOVERABLE AS PRIOR VERSION {} (version_id={})  \
             <-- ANOMALY (recoverable-prior-version)",
            version.key, version.version_id
        );
    }
    Ok(NoncurrentVersionReport {
        supported: true,
        recoverable_versions: listing.versions.len(),
    })
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;

    /// Publish one commit record (and its data object) at `shard` for Metrics
    /// with the given `segment_format_version`, so the version audit has a live
    /// object to classify.
    async fn publish_commit_at(
        store: &MemoryStore,
        tenant_hash: &TenantHash,
        shard: u32,
        version: u32,
    ) {
        const NS_PER_HOUR: i64 = 3_600_000_000_000;
        let ingest_hour_bucket = 100u32;
        // The commit builder cross-checks ingest_hour_bucket against
        // created_unix_ns's hour, so keep them consistent.
        let created_unix_ns = i64::from(ingest_hour_bucket) * NS_PER_HOUR;
        let writer_id = Uuid::new_v4();
        let payload = format!("seg-{shard}-{writer_id}").into_bytes();
        let content_hash = *blake3::hash(&payload).as_bytes();
        let commit = record::build(NewCommitRecord {
            tenant_hash: *tenant_hash,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created_unix_ns,
            max_event_ts_ns: created_unix_ns,
            min_ingest_ts_ns: created_unix_ns,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: version,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&commit).expect("data key");
        publish::put_data_object(store, &data_key, bytes::Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(store, &commit, &RetryPolicy::default())
            .await
            .expect("publish commit record");
    }

    /// Finding 5: the CLI `maintain audit-versions` loop must visit the
    /// generation-aware shard range (ADR-0052 section 4), not the static
    /// `--shards`. With an increase reshard (gen0 count 2, gen1 count 4) and a
    /// live object in shard 3 -- outside `--shards=2` -- the audit must still
    /// list shard 3 and surface the unsupported-version anomaly. Under the old
    /// static `0..shards` loop shard 3 was never listed and the audit passed.
    #[tokio::test]
    async fn audit_versions_visits_post_reshard_shard_range() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "cli-audit-reshard";
        let tenant_hash = TenantId::new(tenant).hash();

        ravel_catalog::validate_or_adopt(
            store.as_ref(),
            &tenant_hash,
            Signal::Metrics,
            2,
            0,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("create generation 0");
        ravel_catalog::append_generation(store.as_ref(), &tenant_hash, Signal::Metrics, 4, 1, 0)
            .await
            .expect("append generation 1");

        // An unsupported-version live object in shard 3 (reachable only under
        // the widened count 4).
        publish_commit_at(&store, &tenant_hash, 3, 99).await;

        let err = audit_versions(store.clone(), tenant, 2)
            .await
            .expect_err("audit must visit the post-reshard shard 3 and find the anomaly");
        assert!(
            err.to_string().contains("unsupported version"),
            "expected an unsupported-version anomaly, got: {err}"
        );
    }

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const ARN: &str = "arn:aws:kms:us-east-1:111122223333:key/aaaaaaaa";

    /// verify-custody's key-epoch check passes clean when every live object's
    /// write time falls inside a recorded epoch. The object is written at hour
    /// 100 (publish_commit_at's fixed created_unix_ns) and the tenant's epoch 0
    /// activated at hour 50, so the write time locates in exactly one epoch.
    #[tokio::test]
    async fn verify_custody_epoch_check_passes_when_object_inside_epoch() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "cli-custody-epoch-ok";
        let tenant_hash = TenantId::new(tenant).hash();

        publish_commit_at(&store, &tenant_hash, 0, 6).await;
        ravel_catalog::record_key_epoch(store.as_ref(), &tenant_hash, ARN, 50 * NS_PER_HOUR, 0)
            .await
            .expect("record epoch 0 activated before the object's write time");

        verify_custody(store.clone(), tenant, 1, false)
            .await
            .expect("every object's write time is inside a recorded epoch");
    }

    /// verify-custody flags an object written before the tenant's first
    /// recorded epoch as a distinct key-epoch anomaly, counted separately from
    /// content-hash mismatches (of which there are none: the object's content
    /// still matches its key). Object at hour 100, epoch 0 activated at hour
    /// 200: the write time predates every recorded epoch.
    #[tokio::test]
    async fn verify_custody_epoch_check_flags_object_before_first_epoch() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "cli-custody-epoch-anomaly";
        let tenant_hash = TenantId::new(tenant).hash();

        publish_commit_at(&store, &tenant_hash, 0, 6).await;
        ravel_catalog::record_key_epoch(store.as_ref(), &tenant_hash, ARN, 200 * NS_PER_HOUR, 0)
            .await
            .expect("record epoch 0 activated after the object's write time");

        let err = verify_custody(store.clone(), tenant, 1, false)
            .await
            .expect_err("an object predating the first epoch must be a custody anomaly");
        let msg = err.to_string();
        assert!(
            msg.contains("1 key-epoch inconsistency"),
            "the epoch anomaly must be counted: {msg}"
        );
        assert!(
            msg.contains("0 content-hash mismatch"),
            "the epoch anomaly must be counted separately from content mismatches: {msg}"
        );
    }

    /// With no `t/<hash>/enc` record, verify-custody runs its content checks and
    /// skips the epoch check entirely (the tenant used the deployment default
    /// key), so a clean object passes.
    #[tokio::test]
    async fn verify_custody_no_epoch_record_skips_epoch_check() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "cli-custody-no-epoch";
        let tenant_hash = TenantId::new(tenant).hash();

        publish_commit_at(&store, &tenant_hash, 0, 6).await;

        verify_custody(store.clone(), tenant, 1, false)
            .await
            .expect("no epoch record means no epoch check; the object passes clean");
    }

    /// The versioning-aware recoverable-prior-version anomaly (ADR-0064 §7,
    /// S4-12) fires only on a versioned bucket that actually holds a noncurrent
    /// version. A noncurrent version exists only on a versioned bucket, so a
    /// nonzero count is proof of both conditions. When the source cannot
    /// enumerate versions (the production trait-contract default) or holds none,
    /// no anomaly fires.
    #[tokio::test]
    async fn versioning_aware_recoverable_prior_version_fires_only_when_present() {
        use ravel_object_store::StoreError;
        use ravel_object_store::conformance::{
            NoncurrentVersion, NoncurrentVersionListing, NoncurrentVersionSource,
        };

        struct Fixture(NoncurrentVersionListing);

        #[async_trait::async_trait]
        impl NoncurrentVersionSource for Fixture {
            async fn list_noncurrent_versions(
                &self,
                _prefix: &str,
            ) -> Result<NoncurrentVersionListing, StoreError> {
                Ok(self.0.clone())
            }
        }

        // A versioned bucket with an actual noncurrent version present: the
        // anomaly fires (count 1).
        let present = Fixture(NoncurrentVersionListing {
            supported: true,
            versions: vec![NoncurrentVersion {
                key: "t/ab/m/l0/0000/obj.rseg".to_string(),
                version_id: "v-prior-1".to_string(),
            }],
        });
        let report = check_noncurrent_versions(&present, "t/ab/")
            .await
            .expect("check runs");
        assert!(report.supported);
        assert_eq!(
            report.recoverable_versions, 1,
            "a present noncurrent version must fire the anomaly"
        );

        // A versioned bucket with no noncurrent versions: no anomaly.
        let clean = Fixture(NoncurrentVersionListing {
            supported: true,
            versions: vec![],
        });
        let report = check_noncurrent_versions(&clean, "t/ab/")
            .await
            .expect("check runs");
        assert!(report.supported);
        assert_eq!(
            report.recoverable_versions, 0,
            "no noncurrent version means no anomaly"
        );

        // A backend that cannot enumerate versions (production default): an
        // honest gap, not an anomaly.
        let store = MemoryStore::new();
        let report = check_noncurrent_versions(&store as &dyn ObjectStoreBackend, "t/ab/")
            .await
            .expect("check runs");
        assert!(
            !report.supported,
            "the trait-contract default cannot enumerate versions"
        );
        assert_eq!(report.recoverable_versions, 0);
    }
}

//! `ravel-cli commit reconstruct` (ADR-0058 decision 2): rebuild
//! the L0 commit records for a single shard that have been lost out of band
//! (an accidental delete, a bad S3 lifecycle rule, a fat-fingered prefix
//! delete), from the record-less data objects' own footers.
//!
//! A commit record is the sole metadata root a reader trusts
//! (docs/object-store-contract.md); once a shard's records are gone the data
//! objects they named become invisible and the orphan sweeper physically
//! deletes them at the grace horizon. This tool reconstructs a faithful
//! record for each record-less data object so the reader (and the sweeper's
//! "referenced" check) can see it again.
//!
//! Scope is one `(tenant, signal, shard)`, never a bucket-wide sweep, to bound
//! blast radius and cost (ADR-0058 decision 2). Only metrics (RSEG) and logs
//! (RLOG) are supported: their footers carry the ingest-time fields a commit
//! record needs. RSPAN footers do not, so spans are refused rather than
//! reconstructed from incomplete data.
//!
//! Two footer-derived fields are honest reconstructions, not byte-for-byte
//! copies of the original record (ADR-0058 Context):
//!
//! - `created_unix_ns` cannot be recovered exactly (it is not in any footer);
//!   the data object's own `ObjectMeta.last_modified` (its real S3 write
//!   timestamp) is used, off from the true flush-open time only by the
//!   data-PUT-to-commit-PUT latency (sub-second in practice), and every
//!   horizon it feeds is measured in hours.
//! - RLOG's footer carries no `ingest_hour_bucket` (RSEG's field 14 has no
//!   RLOG analogue), so it is derived as
//!   `floor(min_observed_ts_ns / NS_PER_HOUR)` from the earliest ingested
//!   sample. RSEG reads the field directly.
//!
//! Every write is `PutMode::CreateIfAbsent`: the tool never deletes and never
//! overwrites an existing commit record. If a record is already present at a
//! candidate's derived key (a concurrent partial reconstruction, or the record
//! was not actually lost), that candidate is reported as a conflict and
//! skipped, never clobbered.

use std::collections::HashSet;
use std::sync::Arc;

use ravel_commit::keys::{self, BucketEntry};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::footer::LogFooter;
use ravel_object_store::{
    GetRange, ObjectMeta, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum, list_all,
};
use ravel_proto::segment::v1::Footer as RsegFooter;
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

use crate::maintain::SignalArg;

/// Nanoseconds per ingest-hour bucket (mirrors `ravel_commit::record`'s own
/// private `NS_PER_HOUR`, kept local since that one is not exported).
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// What reconstruction did for one candidate data object.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// A commit record was rebuilt and written `CreateIfAbsent`.
    Reconstructed {
        data_key: String,
        commit_key: String,
    },
    /// A commit record already existed at the derived key; the candidate was
    /// skipped and the existing record left untouched (never clobbered).
    AlreadyPresent {
        data_key: String,
        commit_key: String,
    },
    /// Reconstruction failed for this candidate (footer parse, build, or write
    /// error). Reported with the reason; the command exits nonzero.
    Failed { data_key: String, reason: String },
}

/// `commit reconstruct --tenant <t> --signal <s> --shard <n>`: rebuild the
/// record-less L0 data objects' commit records for one shard (ADR-0058
/// decision 2).
pub async fn reconstruct(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    shard: u32,
) -> anyhow::Result<()> {
    let signal = signal.to_signal();
    if !matches!(signal, Signal::Metrics | Signal::Logs) {
        anyhow::bail!(
            "commit reconstruct supports only metrics (RSEG) and logs (RLOG); spans (RSPAN) \
             footers do not carry the ingest-time fields a commit record needs (ADR-0058 \
             decision 2)"
        );
    }
    let tenant_hash = TenantId::new(tenant).hash();
    let store = store.as_ref();

    // Phase (a): list the shard's L0 data objects and the identities that
    // already have a commit record; the set difference (by identity) is the
    // candidate orphan set. Same two-list shape as
    // `ravel_maintain::sweep::sweep_orphans` (ADR-0048 decision 5).
    let l0_prefix = l0_data_prefix(&tenant_hash, signal, shard)?;
    let objects = list_all(store, &l0_prefix)
        .await
        .map_err(|err| anyhow::anyhow!("failed to list {l0_prefix}: {err}"))?;
    let referenced = referenced_identities(store, &tenant_hash, signal, shard).await?;

    let mut candidates: Vec<ObjectMeta> = Vec::new();
    for meta in objects {
        let parsed = keys::parse_data_key(&meta.key)
            .map_err(|err| anyhow::anyhow!("malformed L0 data key {}: {err}", meta.key))?;
        let identity = (parsed.writer_id, parsed.epoch, parsed.seq);
        if referenced.contains(&identity) {
            continue;
        }
        candidates.push(meta);
    }

    // Phase (b): one fresh, re-verify LIST of the commit prefix before acting
    // (a commit record may have landed for a candidate since the first
    // listing). This mirrors `sweep_orphans`'s re-verify; the
    // `CreateIfAbsent` write below is the ultimate guard, but re-verifying
    // avoids needless conflict reports for records that reappeared.
    if !candidates.is_empty() {
        let fresh = referenced_identities(store, &tenant_hash, signal, shard).await?;
        candidates.retain(|meta| match keys::parse_data_key(&meta.key) {
            Ok(parsed) => !fresh.contains(&(parsed.writer_id, parsed.epoch, parsed.seq)),
            // A key that parsed in phase (a) cannot stop parsing here; keep it
            // as a candidate so the per-object step reports the anomaly rather
            // than silently dropping it.
            Err(_) => true,
        });
    }

    let mut outcomes = Vec::with_capacity(candidates.len());
    for meta in &candidates {
        outcomes.push(reconstruct_one(store, signal, meta).await);
    }

    // Phase: per-object report and exit code.
    let mut reconstructed = 0usize;
    let mut already_present = 0usize;
    let mut failed = 0usize;

    println!("tenant: {tenant}");
    println!("signal: {signal:?}");
    println!("shard: {shard}");
    println!("candidates: {}", outcomes.len());
    for outcome in &outcomes {
        match outcome {
            Outcome::Reconstructed {
                data_key,
                commit_key,
            } => {
                reconstructed += 1;
                println!("  RECONSTRUCTED {data_key} -> {commit_key}");
            }
            Outcome::AlreadyPresent {
                data_key,
                commit_key,
            } => {
                already_present += 1;
                println!(
                    "  ALREADY PRESENT (conflict, skipped, not clobbered) {data_key} -> \
                     {commit_key}"
                );
            }
            Outcome::Failed { data_key, reason } => {
                failed += 1;
                println!("  FAILED {data_key}: {reason}");
            }
        }
    }
    println!(
        "reconstruct summary: reconstructed={reconstructed} already_present_skipped={already_present} \
         failed={failed}"
    );

    if failed > 0 {
        anyhow::bail!("commit reconstruct: {failed} candidate(s) failed; see the report above");
    }
    Ok(())
}

/// Reconstruct and write one candidate's commit record, folding any error into
/// an [`Outcome::Failed`] so one bad object never aborts the whole shard.
async fn reconstruct_one(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    meta: &ObjectMeta,
) -> Outcome {
    match try_reconstruct_one(store, signal, meta).await {
        Ok(outcome) => outcome,
        Err(err) => Outcome::Failed {
            data_key: meta.key.clone(),
            reason: err.to_string(),
        },
    }
}

async fn try_reconstruct_one(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    meta: &ObjectMeta,
) -> anyhow::Result<Outcome> {
    // One full-object GET recovers both the footer (parsed below) and the exact
    // bytes to hash: `content_hash` is defined over the stored bytes, so
    // rehashing them is exact (ADR-0058). `object_size` comes from the LIST's
    // own `ObjectMeta`, so no extra HEAD is needed.
    let got = store
        .get(&meta.key, GetRange::Full)
        .await
        .map_err(|err| anyhow::anyhow!("failed to GET {}: {err}", meta.key))?;
    let bytes = got.data;
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

    // `created_unix_ns` from the data object's own S3 write timestamp, NOT
    // wall-clock now: a genuine historical timestamp (ADR-0058 decision 2).
    let created_unix_ns = meta
        .last_modified_unix_ms
        .checked_mul(1_000_000)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "object {} last_modified_unix_ms {} overflows nanoseconds",
                meta.key,
                meta.last_modified_unix_ms
            )
        })?;

    let input = match signal {
        Signal::Metrics => {
            let location = ravel_segment::open_from_full(
                bytes.as_ref(),
                ravel_segment::ReaderLimits::default(),
            )
            .map_err(|err| {
                anyhow::anyhow!("failed to parse RSEG footer for {}: {err}", meta.key)
            })?;
            rseg_input(
                &location.footer,
                location.version,
                meta,
                content_hash,
                created_unix_ns,
            )?
        }
        Signal::Logs => {
            let footer = ravel_logseg::footer::open(bytes.as_ref()).map_err(|err| {
                anyhow::anyhow!("failed to parse RLOG footer for {}: {err}", meta.key)
            })?;
            rlog_input(&footer, meta, content_hash, created_unix_ns)?
        }
        other => anyhow::bail!("unsupported signal {other:?}"),
    };

    let candidate = record::build(input)
        .map_err(|err| anyhow::anyhow!("failed to build commit record for {}: {err}", meta.key))?;
    let commit_key = keys::commit_key_for_record(&candidate)
        .map_err(|err| anyhow::anyhow!("failed to derive commit key for {}: {err}", meta.key))?;
    let payload = record::encode(&candidate);
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&payload));
    let opts = PutOptions::create_if_absent().with_checksum(checksum);

    let put_result = store.put(&commit_key, payload, opts).await.map(|_| ());
    classify_put_result(put_result, &meta.key, commit_key)
}

/// Turn the `CreateIfAbsent` put result for a reconstructed commit record into
/// an [`Outcome`] or a typed error.
///
/// The `AccessDenied` arm exists so ADR-0055's deliberate operational
/// limitation is loud at the point an operator hits it, not silent behind a
/// generic write failure: the Admin role is granted `kms:Decrypt` only, so a
/// `commit reconstruct` write against a `--tenant-kms-config` tenant routes
/// through the per-tenant key and is denied unless `kms:GenerateDataKey*` is
/// also granted to that role. The original store `msg` is preserved so an
/// operator debugging a genuinely different `AccessDenied` cause still sees the
/// real S3/KMS error text. Every other error variant keeps the generic message.
fn classify_put_result(
    put_result: Result<(), StoreError>,
    data_key: &str,
    commit_key: String,
) -> anyhow::Result<Outcome> {
    match put_result {
        Ok(()) => Ok(Outcome::Reconstructed {
            data_key: data_key.to_string(),
            commit_key,
        }),
        // CreateIfAbsent refused: a record already exists at this key. Report
        // the conflict and skip; never overwrite (ADR-0058 decision 2 step 4).
        Err(StoreError::AlreadyExists) => Ok(Outcome::AlreadyPresent {
            data_key: data_key.to_string(),
            commit_key,
        }),
        // A KMS-permission denial on the PutObject surfaces here as a distinct
        // variant (crates/ravel-object-store, S3 403 -> AccessDenied). This is
        // the visible failure point of ADR-0055's Decrypt-only Admin posture,
        // so name it explicitly while preserving the underlying message.
        Err(StoreError::AccessDenied(msg)) => Err(anyhow::anyhow!(
            "failed to write commit record {commit_key}: access denied: {msg}. This may be \
             ADR-0055's known limitation: the Admin role is granted kms:Decrypt only, so a \
             `commit reconstruct` write against a --tenant-kms-config tenant can be denied here \
             because it routes through the per-tenant KMS key and needs kms:GenerateDataKey* \
             granted to the Admin role. See the \"Per-tenant SSE-KMS routing\" section of \
             docs/guides/operations.md (the Admin GetObject-only KMS grant) for the manual \
             workaround."
        )),
        Err(err) => Err(anyhow::anyhow!(
            "failed to write commit record {commit_key}: {err}"
        )),
    }
}

/// Map an RSEG footer (metrics) to the commit-record inputs. `ingest_hour_bucket`
/// is read directly from the footer (field 14); every other field is a direct
/// copy of a footer field (ADR-0058 Context table).
fn rseg_input(
    footer: &RsegFooter,
    version: u16,
    meta: &ObjectMeta,
    content_hash: [u8; 32],
    created_unix_ns: i64,
) -> anyhow::Result<NewCommitRecord> {
    let tenant_hash = tenant_hash_from_slice(&footer.tenant_hash, &meta.key)?;
    let writer_id = Uuid::parse_str(&footer.writer_id).map_err(|_| {
        anyhow::anyhow!(
            "RSEG footer writer_id {:?} is not a uuid for {}",
            footer.writer_id,
            meta.key
        )
    })?;
    Ok(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: footer.shard,
        writer_id,
        writer_epoch: footer.writer_epoch,
        writer_seq: footer.writer_seq,
        object_size: meta.size,
        content_hash,
        sample_count: footer.sample_count,
        series_count: footer.series_count,
        min_event_ts_ns: footer.min_event_ts_ns,
        max_event_ts_ns: footer.max_event_ts_ns,
        min_ingest_ts_ns: footer.min_ingest_ts_ns,
        max_ingest_ts_ns: footer.max_ingest_ts_ns,
        segment_format_version: u32::from(version),
        created_unix_ns,
        ingest_hour_bucket: footer.ingest_hour_bucket,
    })
}

/// Map an RLOG footer (logs) to the commit-record inputs. RLOG's footer names
/// the ingest bounds `min/max_observed_ts_ns`, the sample count `record_count`,
/// and the series count `stream_count`; it carries no `ingest_hour_bucket`, so
/// that is derived from `min_observed_ts_ns` (ADR-0058 Context table).
fn rlog_input(
    footer: &LogFooter,
    meta: &ObjectMeta,
    content_hash: [u8; 32],
    created_unix_ns: i64,
) -> anyhow::Result<NewCommitRecord> {
    let ingest_hour_bucket =
        derive_rlog_ingest_hour_bucket(footer.min_observed_ts_ns).map_err(|err| {
            anyhow::anyhow!("cannot derive ingest_hour_bucket for {}: {err}", meta.key)
        })?;
    Ok(NewCommitRecord {
        tenant_hash: TenantHash(footer.tenant_hash),
        signal: Signal::Logs,
        shard: footer.shard,
        writer_id: Uuid::from_bytes(footer.writer_id),
        writer_epoch: footer.writer_epoch,
        writer_seq: footer.writer_seq,
        object_size: meta.size,
        content_hash,
        sample_count: footer.record_count,
        series_count: footer.stream_count,
        min_event_ts_ns: footer.min_ts_ns,
        max_event_ts_ns: footer.max_ts_ns,
        min_ingest_ts_ns: footer.min_observed_ts_ns,
        max_ingest_ts_ns: footer.max_observed_ts_ns,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns,
        ingest_hour_bucket,
    })
}

/// Derive an RLOG object's `ingest_hour_bucket` from its earliest observed
/// (ingest) sample: `floor(min_observed_ts_ns / NS_PER_HOUR)`. This matches
/// the invariant `CommitRecord::validate` enforces
/// (`ingest_hour_bucket <= floor(created_unix_ns / NS_PER_HOUR)`, with no lower
/// bound), because a late-arriving sample files under an older hour bucket than
/// the flush that wrote it (ADR-0058). RSEG carries this field directly and is
/// the cross-check oracle in the tests below.
fn derive_rlog_ingest_hour_bucket(min_observed_ts_ns: i64) -> anyhow::Result<u32> {
    let bucket = min_observed_ts_ns.div_euclid(NS_PER_HOUR);
    u32::try_from(bucket).map_err(|_| {
        anyhow::anyhow!(
            "min_observed_ts_ns {min_observed_ts_ns} yields an out-of-range hour bucket {bucket}"
        )
    })
}

fn tenant_hash_from_slice(bytes: &[u8], key: &str) -> anyhow::Result<TenantHash> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("footer tenant_hash is not 16 bytes for {key}"))?;
    Ok(TenantHash(arr))
}

/// `t/<tenant_hash_hex>/<signal>/l0/<shard>/` -- every L0 data object for one
/// `(tenant, signal, shard)`, across all ingest hours (L0 data keys are not
/// hour-bucketed). Built from the same pieces `keys::data_key` uses; no public
/// prefix builder exists in ravel-commit for this shape (mirrors
/// `ravel_maintain::sweep`'s own local builder).
fn l0_data_prefix(tenant_hash: &TenantHash, signal: Signal, shard: u32) -> anyhow::Result<String> {
    if shard > 9999 {
        anyhow::bail!("shard {shard} exceeds the 4-digit key width (max 9999)");
    }
    Ok(format!(
        "t/{}/{}/l0/{:04}/",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        shard
    ))
}

/// The set of L0 commit-record identities `(writer_id, epoch, seq)` present in
/// a shard, across every hour, read from commit-record keys only (no GET). An
/// `l0/` data object whose identity is in this set already has a record and is
/// not a reconstruction candidate. Mirrors
/// `ravel_maintain::sweep::referenced_l0_identities`.
async fn referenced_identities(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
) -> anyhow::Result<HashSet<(Uuid, u64, u64)>> {
    let prefix = keys::commit_shard_prefix(tenant_hash, signal, shard)
        .map_err(|err| anyhow::anyhow!("failed to build commit shard prefix: {err}"))?;
    let metas = list_all(store, &prefix)
        .await
        .map_err(|err| anyhow::anyhow!("failed to list {prefix}: {err}"))?;
    let mut out = HashSet::new();
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(pk)) => {
                out.insert((pk.writer_id, pk.epoch, pk.seq));
            }
            // Only L0 commit identities are collected; a compaction, rewrite
            // (ADR-0064), or tombstone record names none.
            Ok(
                BucketEntry::CompactionRecord(_)
                | BucketEntry::RewriteRecord(_)
                | BucketEntry::Tombstone(_),
            ) => {}
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "unknown commit-prefix key shape {}: {err}",
                    meta.key
                ));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_logseg::footer::{LogFooter, SectionDesc, kind};
    use ravel_object_store::{Etag, Version};

    use super::*;

    /// Build a minimal `ObjectMeta` with a chosen `last_modified`, for the
    /// mapping-function unit tests (they never read the store).
    fn meta_with_last_modified(key: &str, size: u64, last_modified_unix_ms: i64) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            size,
            etag: Etag("test-etag".to_string()),
            version: Version(String::new()),
            last_modified_unix_ms,
        }
    }

    fn sample_rlog_footer(min_observed_ts_ns: i64, max_observed_ts_ns: i64) -> LogFooter {
        LogFooter {
            tenant_hash: [0x11; 16],
            shard: 3,
            writer_id: [0x22; 16],
            writer_epoch: 7,
            writer_seq: 9,
            min_ts_ns: min_observed_ts_ns - 500,
            max_ts_ns: max_observed_ts_ns - 100,
            min_observed_ts_ns,
            max_observed_ts_ns,
            record_count: 42,
            block_count: 2,
            stream_count: 5,
            sections: vec![SectionDesc {
                kind: kind::STREAM_DIR,
                offset: 0,
                len: 1,
                crc32c: 0,
                comp: 0,
                uncomp_len: 1,
            }],
            level: 0,
            input_set_hash: Vec::new(),
            part_index: 0,
        }
    }

    fn sample_rseg_footer(ingest_hour_bucket: u32, min_ingest_ts_ns: i64) -> RsegFooter {
        RsegFooter {
            tenant_hash: vec![0x11; 16],
            shard: 3,
            writer_id: Uuid::from_u128(0x1234_5678).to_string(),
            writer_epoch: 7,
            writer_seq: 9,
            min_event_ts_ns: min_ingest_ts_ns - 500,
            max_event_ts_ns: min_ingest_ts_ns + 500,
            min_ingest_ts_ns,
            max_ingest_ts_ns: min_ingest_ts_ns + 1_000,
            sample_count: 42,
            series_count: 5,
            sections: Vec::new(),
            base_created_unix_ns: min_ingest_ts_ns,
            ingest_hour_bucket,
            input_set_hash: Vec::new(),
            part_index: 0,
            level: 0,
        }
    }

    /// `content_hash` reconstruction is exact for uncorrupted bytes: the record
    /// carries blake3 of exactly the bytes fed in, no approximation (ADR-0058).
    /// The mapping copies whatever `content_hash` it is handed, so the exactness
    /// property lives in that the caller hashes the object's own bytes; this
    /// asserts the value is threaded through unchanged.
    #[test]
    fn content_hash_is_the_exact_blake3_passed_in() {
        let bytes = b"some rlog object bytes";
        let content_hash = *blake3::hash(bytes).as_bytes();
        let footer = sample_rlog_footer(495_734 * NS_PER_HOUR, 495_734 * NS_PER_HOUR + 1_000);
        let meta = meta_with_last_modified("t/x/l/l0/0003/o.rseg", 22, 495_735 * 3_600_000);
        let input = rlog_input(&footer, &meta, content_hash, 495_735 * NS_PER_HOUR).expect("map");
        assert_eq!(input.content_hash, content_hash);
        // And it is exactly blake3 of the bytes, not some other digest.
        assert_eq!(input.content_hash, *blake3::hash(bytes).as_bytes());
    }

    /// `created_unix_ns` comes from `ObjectMeta.last_modified`, never wall-clock
    /// now (ADR-0058 decision 2 step 3).
    #[test]
    fn created_unix_ns_uses_last_modified_not_now() {
        // A last_modified far in the past: 2026-07-26T00 in ms.
        let last_modified_ms = 495_720 * 3_600_000;
        let expected_created_ns = last_modified_ms * 1_000_000;
        let content_hash = [0u8; 32];

        let footer = sample_rlog_footer(495_700 * NS_PER_HOUR, 495_700 * NS_PER_HOUR + 5_000);
        let meta = meta_with_last_modified("t/x/l/l0/0003/o.rseg", 10, last_modified_ms);
        let input = rlog_input(&footer, &meta, content_hash, expected_created_ns).expect("map");
        assert_eq!(input.created_unix_ns, expected_created_ns);

        // Sanity: the historical timestamp is far from any plausible "now"
        // (this test's own clock), proving the value is the object's write time.
        let now = crate::now_ns().expect("clock");
        assert!(
            input.created_unix_ns < now,
            "reconstructed created_unix_ns must be the object's historical write time, not now"
        );
    }

    /// The RLOG `ingest_hour_bucket` derivation
    /// (`floor(min_observed_ts_ns / NS_PER_HOUR)`) cross-checked against a real
    /// RSEG footer's directly-stored `ingest_hour_bucket` (field 14): build a
    /// synthetic RSEG and RLOG footer with the same underlying timestamp and
    /// confirm the derived RLOG value equals the directly-read RSEG value
    /// (ADR-0058 decision 2 step 3).
    #[test]
    fn rlog_ingest_hour_bucket_derivation_matches_rseg_stored_field() {
        for hour in [0u32, 1, 23, 24, 495_734, 1_000_000] {
            // A shared timestamp somewhere inside `hour` (30 min past the top).
            let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60_000_000_000;

            // RSEG stores the bucket directly (field 14).
            let rseg = sample_rseg_footer(hour, ts);
            // RLOG carries only min_observed_ts_ns; the bucket is derived.
            let rlog = sample_rlog_footer(ts, ts + 1_000);

            let derived = derive_rlog_ingest_hour_bucket(rlog.min_observed_ts_ns).expect("derive");
            assert_eq!(
                derived, rseg.ingest_hour_bucket,
                "derived RLOG bucket must equal RSEG's directly-stored field for the same ts"
            );

            // And the whole RSEG mapping reads that same field straight through.
            let meta =
                meta_with_last_modified("t/x/m/l0/0003/o.rseg", 1, i64::from(hour + 1) * 3_600_000);
            let rseg_input = rseg_input(
                &rseg,
                6,
                &meta,
                [0u8; 32],
                i64::from(hour + 1) * NS_PER_HOUR,
            )
            .expect("rseg map");
            assert_eq!(rseg_input.ingest_hour_bucket, hour);
        }
    }

    /// The RLOG mapping copies the observed (ingest) bounds and the log-specific
    /// counts into the commit-record shape the reader expects.
    #[test]
    fn rlog_mapping_copies_observed_bounds_and_counts() {
        let min_obs = 495_734 * NS_PER_HOUR;
        let max_obs = min_obs + 90_000_000_000; // 90s later, next-ish
        let footer = sample_rlog_footer(min_obs, max_obs);
        let meta = meta_with_last_modified("t/x/l/l0/0003/o.rseg", 128, 495_735 * 3_600_000);
        let input = rlog_input(&footer, &meta, [0u8; 32], 495_735 * NS_PER_HOUR).expect("map");

        assert_eq!(input.signal, Signal::Logs);
        assert_eq!(input.shard, 3);
        assert_eq!(input.writer_epoch, 7);
        assert_eq!(input.writer_seq, 9);
        assert_eq!(input.object_size, 128);
        assert_eq!(input.sample_count, 42); // record_count
        assert_eq!(input.series_count, 5); // stream_count
        assert_eq!(input.min_ingest_ts_ns, min_obs);
        assert_eq!(input.max_ingest_ts_ns, max_obs);
        assert_eq!(
            input.segment_format_version,
            u32::from(ravel_logseg::footer::VERSION)
        );
        // The record built from this input passes structural validation.
        record::build(input).expect("valid record");
    }

    /// A KMS-permission `AccessDenied` on the commit-record PUT is surfaced with
    /// ADR-0055's known-limitation pointer AND the original store message, so an
    /// operator hitting the Decrypt-only Admin limitation is sent straight to
    /// docs/guides/operations.md instead of a generic write failure. This is the
    /// exact arm a pre-fix generic catch-all swallowed: with only the catch-all,
    /// the text was `failed to write commit record <key>: <msg>` and carried no
    /// ADR-0055 pointer, so the `contains("ADR-0055")` assertion below failed.
    #[test]
    fn access_denied_put_names_adr_0055_and_preserves_message() {
        let original = "User: arn:aws:sts::111122223333:assumed-role/ravel-admin/session is not \
                        authorized to perform: kms:GenerateDataKey on resource: \
                        arn:aws:kms:us-east-1:111122223333:key/abcd because no identity-based \
                        policy allows the kms:GenerateDataKey action";
        let err = classify_put_result(
            Err(StoreError::AccessDenied(original.to_string())),
            "t/x/m/l0/0003/o.rseg",
            "c/x/m/0003/commit-key".to_string(),
        )
        .expect_err("AccessDenied must map to an error, not an Ok outcome");
        let text = format!("{err:#}");

        // The real S3/KMS text is preserved so a genuinely different
        // AccessDenied cause is still debuggable.
        assert!(
            text.contains(original),
            "must preserve the original AccessDenied message: {text}"
        );
        // The ADR-0055 / operations.md pointer is present.
        assert!(text.contains("ADR-0055"), "must name ADR-0055: {text}");
        assert!(
            text.contains("kms:GenerateDataKey*"),
            "must name the grant the write needs: {text}"
        );
        assert!(
            text.contains("kms:Decrypt only"),
            "must name the Decrypt-only Admin posture: {text}"
        );
        assert!(
            text.contains("Per-tenant SSE-KMS routing"),
            "must point at the operations.md section heading by name: {text}"
        );
        assert!(
            text.contains("docs/guides/operations.md"),
            "must point at the operations guide: {text}"
        );
    }

    /// Every non-AccessDenied write error keeps the generic message and does NOT
    /// carry the ADR-0055 pointer, so the two arms stay distinguishable and an
    /// unrelated failure is never mislabeled as the KMS limitation.
    #[test]
    fn generic_put_error_omits_adr_0055_pointer() {
        let err = classify_put_result(
            Err(StoreError::Permanent(
                "bucket policy forbids overwrite".to_string(),
            )),
            "t/x/m/l0/0003/o.rseg",
            "c/x/m/0003/commit-key".to_string(),
        )
        .expect_err("a permanent write error must map to an error");
        let text = format!("{err:#}");

        assert!(
            text.contains("failed to write commit record"),
            "keeps the generic prefix: {text}"
        );
        assert!(
            text.contains("bucket policy forbids overwrite"),
            "keeps the underlying error text: {text}"
        );
        assert!(
            !text.contains("ADR-0055"),
            "a generic error must NOT carry the ADR-0055 pointer: {text}"
        );
        assert!(
            !text.contains("Per-tenant SSE-KMS routing"),
            "a generic error must NOT point at the KMS section: {text}"
        );
    }

    /// The CreateIfAbsent refusal still maps to `AlreadyPresent` (conflict,
    /// skipped, never clobbered), unchanged by the new AccessDenied arm.
    #[test]
    fn already_exists_still_maps_to_already_present() {
        let outcome = classify_put_result(
            Err(StoreError::AlreadyExists),
            "t/x/m/l0/0003/o.rseg",
            "c/x/m/0003/commit-key".to_string(),
        )
        .expect("AlreadyExists is a skipped conflict, not an error");
        assert_eq!(
            outcome,
            Outcome::AlreadyPresent {
                data_key: "t/x/m/l0/0003/o.rseg".to_string(),
                commit_key: "c/x/m/0003/commit-key".to_string(),
            }
        );
    }
}

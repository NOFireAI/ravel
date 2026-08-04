//! Durable, startup-checked `shard_count` (ADR-0050 section 5, EC5).
//!
//! Each (tenant, signal) carries an immutable provisioning record at
//! `t/<tenant_hash>/<sig>/prov` recording the `shard_count` its data was
//! written under. `shard_count` lives only in process config otherwise, and
//! resolution iterates `0..shard_count` (catalog.rs), so a process configured
//! with a lower value than the data was written under silently omits every
//! series in the missing shards. This record turns that silent truncation into
//! a loud refusal: every ingest, catalog, and maintain touch validates the
//! configured value against the record before acting.
//!
//! This module is the one shared implementation the three consumers named in
//! ADR-0050 section 5 call: ingest-router first write, catalog first resolve,
//! and the maintain per-tenant loop. [`validate_or_adopt`] handles all three
//! ADR scenarios (no record + no data; no record + pre-ADR data; record
//! present) so no consumer reimplements the decision.
//!
//! Scope note: `shard_count` is immutable per (tenant, signal) in this
//! decision. Resharding (a shard-epoch map) is deferred to its own ADR (epic
//! EK); nothing here changes a recorded value, only refuses when config and
//! record disagree.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash};

/// Format floor written into every provisioning record this build emits, and
/// the highest record version it understands. A record declaring a higher
/// version is refused rather than misread under this layout (ADR-0050 section
/// 5), matching the `sys/tenancy` marker's version guard.
pub const PROVISIONING_FORMAT_VERSION: u32 = 1;

/// Object key for a (tenant, signal) provisioning record: `t/<hex>/<sig>/prov`.
/// Under the tenant's own prefix, alongside its `l0/` and `c/` shard data, not
/// a bucket-root `sys/` object (ADR-0050 section 5).
pub fn provisioning_key(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/prov", tenant_hash.to_hex(), signal.key_prefix())
}

/// Prefix under which one (tenant, signal)'s L0 (uncompacted) shard directories
/// live. Delimiter-listing it yields one common prefix per present shard.
fn l0_prefix(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/l0/", tenant_hash.to_hex(), signal.key_prefix())
}

/// Prefix under which one (tenant, signal)'s commit/compaction records live,
/// one shard directory each. Listed alongside `l0/` so a shard that has only
/// commit records (its L0 data already compacted and swept) is still observed.
fn commit_prefix(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/c/", tenant_hash.to_hex(), signal.key_prefix())
}

/// Map a domain [`Signal`] to the persisted `sys.v1.Signal` enum. The record
/// stores the signal so a misfiled record (right key, wrong body) is caught on
/// decode, the same defense commit records carry (ADR-0010 §10).
fn to_sys_signal(signal: Signal) -> sysproto::Signal {
    match signal {
        Signal::Metrics => sysproto::Signal::Metrics,
        Signal::Logs => sysproto::Signal::Logs,
        Signal::Spans => sysproto::Signal::Spans,
        Signal::Profiles => sysproto::Signal::Profiles,
        Signal::Alerts => sysproto::Signal::Alerts,
        Signal::Audit => sysproto::Signal::Audit,
    }
}

/// A typed provisioning failure. Every variant is fatal to the touch that
/// raised it: a static tenant's mismatch refuses startup, a dynamic tenant's
/// fails that one request (ADR-0050 section 5). None warn and continue.
#[derive(Debug, thiserror::Error)]
pub enum ProvisioningError {
    #[error("object store error on provisioning record {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("provisioning record {key:?} could not be decoded: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "provisioning record {key:?} declares format_version {got}, but this build only \
         understands version {PROVISIONING_FORMAT_VERSION}: refusing rather than misread a future \
         record format"
    )]
    UnsupportedVersion { key: String, got: u32 },
    #[error(
        "provisioning record {key:?} is misfiled: it records {field} {actual}, but the key it was \
         read under expects {expected}"
    )]
    CorruptRecord {
        key: String,
        field: &'static str,
        expected: String,
        actual: String,
    },
    /// The record exists and its `shard_count` disagrees with the configured
    /// value. This is the S1-E6 refusal (ADR-0050 section 5): a `FieldMismatch`
    /// naming the record, tenant, signal, expected, and actual.
    #[error(
        "provisioning record {key:?} for tenant {tenant_hash} signal {signal} records \
         shard_count {actual}, but this process is configured for {expected}: refusing to resolve \
         over a subset of shards (ADR-0050 section 5, S1-E6)"
    )]
    ShardCountMismatch {
        key: String,
        tenant_hash: String,
        signal: &'static str,
        expected: u32,
        actual: u32,
    },
    /// Pre-ADR data has a shard index at or above the configured count, so the
    /// configured value is provably hiding data. Adoption writes nothing and
    /// refuses (ADR-0050 section 5): adopting a value just proven wrong would
    /// bless the truncation this record exists to prevent.
    #[error(
        "cannot adopt tenant {tenant_hash} signal {signal}: existing data has shard index \
         {observed_shard}, at or above the configured shard_count {configured}, so the configured \
         value would hide shards {configured}..={observed_shard}. Refusing to write a provisioning \
         record (ADR-0050 section 5)"
    )]
    AdoptionWouldHideData {
        tenant_hash: String,
        signal: &'static str,
        configured: u32,
        observed_shard: u32,
    },
}

impl ProvisioningError {
    fn store(key: &str, source: StoreError) -> Self {
        ProvisioningError::Store {
            key: key.to_string(),
            source,
        }
    }
}

/// What [`validate_or_adopt`] did. Every variant is a success; a disagreement
/// is a [`ProvisioningError`], never a variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisioningCheck {
    /// A record was present and its `shard_count` matched the configured value.
    Matched,
    /// No record and no shard data: a fresh (tenant, signal). Nothing was
    /// written (the caller passed [`AbsentPolicy::AdoptIfData`]); the record is
    /// created on the tenant's first actual write. This is the fresh-tenant,
    /// fresh-deployment case that must never refuse (ADR-0050 section 5).
    FreshNoData,
    /// No record existed; the record was written from config. Covers both a
    /// genuine first write ([`AbsentPolicy::CreateFromConfig`] on empty data)
    /// and adoption of pre-ADR data whose shard indices are all in range.
    Written,
}

/// What [`validate_or_adopt`] is allowed to write when no record exists. Every
/// policy validates a *present* record identically and refuses on a
/// `shard_count` mismatch; they differ only in what happens when the record is
/// absent (ADR-0050 section 5 lists ingest and maintenance as adopters, and the
/// read path as write-free).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsentPolicy {
    /// Write the record from config on a fresh (tenant, signal), and adopt
    /// pre-ADR data whose shard indices are all in range. Used on a tenant's
    /// first actual ingest write, so the durable `shard_count` is pinned as
    /// data lands.
    CreateFromConfig,
    /// Do not create a record for a fresh (no-data) (tenant, signal), but do
    /// adopt pre-ADR data whose shard indices are all in range. Used by the
    /// startup static-tenant check, the maintain per-tenant loop, and
    /// `provision adopt`: a signal with no data yet has nothing to provision
    /// (the record is created on its first write), while a signal with pre-ADR
    /// data is adopted exactly once here.
    AdoptIfData,
    /// Never write anything: validate a present record, and pass through when
    /// absent regardless of whether shard data exists. Used on the catalog
    /// resolve (read) path, which must not mutate storage — a query-only node
    /// may run with write-restricted credentials, and adoption belongs to the
    /// ingest/maintenance/CLI paths, not to a read.
    CheckOnly,
}

/// Read a (tenant, signal)'s provisioning record, if present. `NotFound` is
/// `Ok(None)`, not an error: absence is a valid state with adoption semantics.
async fn read_record(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<sysproto::ProvisioningRecord>, ProvisioningError> {
    match store.get(key, GetRange::Full).await {
        Ok(outcome) => {
            let record =
                sysproto::ProvisioningRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    ProvisioningError::Decode {
                        key: key.to_string(),
                        source,
                    }
                })?;
            Ok(Some(record))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(ProvisioningError::store(key, err)),
    }
}

/// Validate a decoded record against the (tenant, signal) it was read under and
/// the configured `shard_count`. A version, tenant_hash, or signal disagreement
/// is a corrupt/misfiled record; a `shard_count` disagreement is the S1-E6
/// mismatch.
fn validate_record(
    record: &sysproto::ProvisioningRecord,
    key: &str,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
) -> Result<(), ProvisioningError> {
    if record.format_version > PROVISIONING_FORMAT_VERSION {
        return Err(ProvisioningError::UnsupportedVersion {
            key: key.to_string(),
            got: record.format_version,
        });
    }
    if record.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant_hash.to_hex(),
            actual: hex::encode(&record.tenant_hash),
        });
    }
    if record.signal != to_sys_signal(signal) as i32 {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "signal",
            expected: format!("{:?}", to_sys_signal(signal)),
            actual: format!("{}", record.signal),
        });
    }
    if record.shard_count != shard_count {
        return Err(ProvisioningError::ShardCountMismatch {
            key: key.to_string(),
            tenant_hash: tenant_hash.to_hex(),
            signal: signal.key_prefix(),
            expected: shard_count,
            actual: record.shard_count,
        });
    }
    Ok(())
}

/// Parse the shard index out of a delimiter-listing common prefix such as
/// `t/<hex>/<sig>/l0/0007/`. Returns `None` for a segment that is not a plain
/// shard number; such a segment is not a numbered shard this check can hide, so
/// skipping it is safe (it never lowers the observed maximum).
fn shard_index_from_common_prefix(common_prefix: &str, listed_prefix: &str) -> Option<u32> {
    let rest = common_prefix.strip_prefix(listed_prefix)?;
    let segment = rest.strip_suffix('/').unwrap_or(rest);
    // Shard directories are fixed-width zero-padded (`format_shard`), so a
    // plain decimal parse recovers the index. Reject anything non-numeric.
    if segment.is_empty() || !segment.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    segment.parse::<u32>().ok()
}

/// The highest shard index with data present under `l0/` or `c/` for this
/// (tenant, signal), or `None` when neither prefix holds any shard directory.
/// Delimiter listing costs one request per prefix and returns the shard
/// directories as common prefixes.
async fn max_observed_shard(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Option<u32>, ProvisioningError> {
    let mut max: Option<u32> = None;
    for prefix in [
        l0_prefix(tenant_hash, signal),
        commit_prefix(tenant_hash, signal),
    ] {
        let list = store
            .list_delimited(&prefix)
            .await
            .map_err(|err| ProvisioningError::store(&prefix, err))?;
        for common in &list.common_prefixes {
            if let Some(idx) = shard_index_from_common_prefix(common, &prefix) {
                max = Some(max.map_or(idx, |m| m.max(idx)));
            }
        }
    }
    Ok(max)
}

/// Build the record to persist for this (tenant, signal, shard_count).
fn build_record(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
    now_ns: i64,
) -> sysproto::ProvisioningRecord {
    sysproto::ProvisioningRecord {
        format_version: PROVISIONING_FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: to_sys_signal(signal) as i32,
        shard_count,
        created_unix_ns: now_ns,
    }
}

/// Write the record with `CreateIfAbsent`. A racing loser (`AlreadyExists`)
/// re-reads the winner's record and validates the configured value against it,
/// so a race can never let an incompatible `shard_count` through (ADR-0050
/// section 5, mirroring the `sys/tenancy` write_marker race handling).
async fn write_record_race_safe(
    store: &dyn ObjectStoreBackend,
    key: &str,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
    now_ns: i64,
) -> Result<ProvisioningCheck, ProvisioningError> {
    let record = build_record(tenant_hash, signal, shard_count, now_ns);
    match store
        .put(
            key,
            record.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(_) => Ok(ProvisioningCheck::Written),
        Err(StoreError::AlreadyExists) => {
            // A concurrent writer won. Validate our config against what landed
            // rather than assuming our own value was recorded.
            let winner = read_record(store, key).await?.ok_or_else(|| {
                // The winner's write and our failed CreateIfAbsent both saw the
                // object, so a subsequent absence is a store anomaly, not a
                // normal state. Surface it as a store error on the key.
                ProvisioningError::store(key, StoreError::NotFound)
            })?;
            validate_record(&winner, key, tenant_hash, signal, shard_count)?;
            Ok(ProvisioningCheck::Matched)
        }
        Err(err) => Err(ProvisioningError::store(key, err)),
    }
}

/// Validate the configured `shard_count` for one (tenant, signal) against its
/// durable provisioning record, adopting or creating the record as ADR-0050
/// section 5 prescribes. The single shared entry point for all consumers.
///
/// Three scenarios, kept distinct:
/// 1. No record, no shard data: fresh. With [`AbsentPolicy::CreateFromConfig`],
///    write the record now; with [`AbsentPolicy::AdoptIfData`] or
///    [`AbsentPolicy::CheckOnly`], return [`ProvisioningCheck::FreshNoData`]
///    without writing (the record is created on the first actual write). Either
///    way this never refuses: a brand-new tenant with no prior writes and no
///    record must start cleanly.
/// 2. No record, pre-ADR shard data present: adopt (except under
///    [`AbsentPolicy::CheckOnly`], which never writes and returns
///    [`ProvisioningCheck::FreshNoData`]). If every observed shard index is
///    `< shard_count`, write the record from config. If any is `>= shard_count`,
///    the configured value hides data:
///    [`ProvisioningError::AdoptionWouldHideData`], writing nothing.
/// 3. Record present: compare `shard_count`. Equal is
///    [`ProvisioningCheck::Matched`]; unequal is
///    [`ProvisioningError::ShardCountMismatch`]. Identical under every policy.
pub async fn validate_or_adopt(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
    now_ns: i64,
    absent_policy: AbsentPolicy,
) -> Result<ProvisioningCheck, ProvisioningError> {
    let key = provisioning_key(tenant_hash, signal);

    if let Some(record) = read_record(store, &key).await? {
        validate_record(&record, &key, tenant_hash, signal, shard_count)?;
        return Ok(ProvisioningCheck::Matched);
    }

    // No record. The read path never writes and never needs to list: pass
    // through without adopting (adoption belongs to ingest/maintain/CLI).
    if matches!(absent_policy, AbsentPolicy::CheckOnly) {
        return Ok(ProvisioningCheck::FreshNoData);
    }

    // Decide fresh (scenario 1) vs pre-ADR data (scenario 2) from the shard
    // directories actually present. One listing answers both "is there data?"
    // and "what is the highest shard index?".
    match max_observed_shard(store, tenant_hash, signal).await? {
        None => match absent_policy {
            // No record and no data is the fresh case: adopt-if-data and the
            // read-only check both pass through without writing. CheckOnly
            // returns earlier (line above) so it does not reach here today, but
            // handling it explicitly keeps this arm correct if that early return
            // is ever moved, rather than leaving a panic a refactor could trip.
            AbsentPolicy::AdoptIfData | AbsentPolicy::CheckOnly => {
                Ok(ProvisioningCheck::FreshNoData)
            }
            AbsentPolicy::CreateFromConfig => {
                write_record_race_safe(store, &key, tenant_hash, signal, shard_count, now_ns).await
            }
        },
        Some(observed) if observed >= shard_count => {
            Err(ProvisioningError::AdoptionWouldHideData {
                tenant_hash: tenant_hash.to_hex(),
                signal: signal.key_prefix(),
                configured: shard_count,
                observed_shard: observed,
            })
        }
        Some(_) => {
            // Pre-ADR data, all shard indices in range: safe to adopt.
            write_record_race_safe(store, &key, tenant_hash, signal, shard_count, now_ns).await
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutMode, PutOptions};
    use std::sync::Arc;

    fn tenant() -> TenantHash {
        TenantHash([0xABu8; 16])
    }

    fn mem() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// Seed a shard directory so a (tenant, signal) looks like pre-ADR data
    /// with the given shard index present under `l0/`.
    async fn seed_l0_shard(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        signal: Signal,
        shard: u32,
    ) {
        let key = format!(
            "t/{}/{}/l0/{:04}/writer.0.{:020}.deadbeefdeadbeef.rseg",
            th.to_hex(),
            signal.key_prefix(),
            shard,
            1u64
        );
        store
            .put(&key, vec![1, 2, 3].into(), PutOptions::default())
            .await
            .expect("seed l0");
    }

    async fn seed_record(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        signal: Signal,
        shard_count: u32,
    ) {
        let key = provisioning_key(th, signal);
        let record = build_record(th, signal, shard_count, 1_000);
        store
            .put(
                &key,
                record.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed record");
    }

    /// Pin the sys.v1.Signal mapping value-for-value against ravel.commit.v1
    /// (via ravel_commit::signal), so the two never drift: a provisioning
    /// record and a commit record under the same `<sig>` prefix must decode to
    /// the same signal.
    #[test]
    fn sys_signal_mapping_matches_commit_signal() {
        for signal in [
            Signal::Metrics,
            Signal::Logs,
            Signal::Spans,
            Signal::Profiles,
            Signal::Alerts,
            Signal::Audit,
        ] {
            assert_eq!(
                to_sys_signal(signal) as i32,
                ravel_commit::signal::to_proto(signal) as i32,
                "sys and commit Signal enums disagree for {signal:?}"
            );
        }
    }

    #[tokio::test]
    async fn fresh_tenant_no_record_no_data_passes_through() {
        let store = mem();
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("a fresh tenant with no record and no data must not refuse");
        assert_eq!(out, ProvisioningCheck::FreshNoData);
        // PassThrough must not have written a record.
        let got = store
            .get(
                &provisioning_key(&tenant(), Signal::Metrics),
                GetRange::Full,
            )
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }

    #[tokio::test]
    async fn first_write_creates_record_from_config() {
        let store = mem();
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("first write creates the record");
        assert_eq!(out, ProvisioningCheck::Written);
        let record = read_record(
            store.as_ref(),
            &provisioning_key(&tenant(), Signal::Metrics),
        )
        .await
        .expect("read")
        .expect("record present");
        assert_eq!(record.shard_count, 4);
        assert_eq!(record.signal, sysproto::Signal::Metrics as i32);
    }

    #[tokio::test]
    async fn record_present_and_matching_is_ok() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("matching record");
        assert_eq!(out, ProvisioningCheck::Matched);
    }

    #[tokio::test]
    async fn record_present_and_disagreeing_is_shard_count_mismatch() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            2,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect_err("a lower configured shard_count must refuse");
        match err {
            ProvisioningError::ShardCountMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 4);
            }
            other => panic!("wrong error: {other}"),
        }
    }

    #[tokio::test]
    async fn adoption_writes_record_when_all_shards_in_range() {
        let store = mem();
        // Data across shards 0..=2, configured for 4: all in range.
        for shard in 0..=2 {
            seed_l0_shard(store.as_ref(), &tenant(), Signal::Logs, shard).await;
        }
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Logs,
            4,
            2_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("adoption succeeds when all observed shards are in range");
        assert_eq!(out, ProvisioningCheck::Written);
        let record = read_record(store.as_ref(), &provisioning_key(&tenant(), Signal::Logs))
            .await
            .expect("read")
            .expect("record present");
        assert_eq!(record.shard_count, 4);
    }

    #[tokio::test]
    async fn adoption_refuses_and_writes_nothing_when_shard_out_of_range() {
        let store = mem();
        // Data at shard 5, configured for 4: shard 4 and 5 would be hidden.
        seed_l0_shard(store.as_ref(), &tenant(), Signal::Logs, 5).await;
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Logs,
            4,
            2_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect_err("an out-of-range shard must refuse adoption");
        match err {
            ProvisioningError::AdoptionWouldHideData {
                configured,
                observed_shard,
                ..
            } => {
                assert_eq!(configured, 4);
                assert_eq!(observed_shard, 5);
            }
            other => panic!("wrong error: {other}"),
        }
        // Nothing was written.
        let got = store
            .get(&provisioning_key(&tenant(), Signal::Logs), GetRange::Full)
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }

    /// A commit-only shard (data compacted and swept, only records under `c/`)
    /// is still observed, so its index still bounds the safe shard_count.
    #[tokio::test]
    async fn adoption_observes_commit_only_shards() {
        let store = mem();
        let th = tenant();
        let key = format!(
            "t/{}/{}/c/{:04}/1970-01-01-00/writer.0.{:020}.cmt",
            th.to_hex(),
            Signal::Spans.key_prefix(),
            6,
            1u64
        );
        store
            .put(&key, vec![9].into(), PutOptions::default())
            .await
            .expect("seed commit record");
        let err = validate_or_adopt(
            store.as_ref(),
            &th,
            Signal::Spans,
            4,
            2_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect_err("a commit-only shard 6 out of range for shard_count 4 must refuse");
        assert!(matches!(
            err,
            ProvisioningError::AdoptionWouldHideData {
                observed_shard: 6,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn corrupt_record_decodes_to_typed_error_not_panic() {
        let store = mem();
        let key = provisioning_key(&tenant(), Signal::Metrics);
        // Bytes that are not a valid ProvisioningRecord: a truncated/garbage
        // protobuf. Decode must be a typed error, never a panic.
        store
            .put(
                &key,
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("put garbage");
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect_err("garbage bytes must be a typed decode error");
        assert!(
            matches!(err, ProvisioningError::Decode { .. }),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn unsupported_future_version_is_refused() {
        let store = mem();
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let mut record = build_record(&tenant(), Signal::Metrics, 4, 1_000);
        record.format_version = PROVISIONING_FORMAT_VERSION + 1;
        store
            .put(&key, record.encode_to_vec().into(), PutOptions::default())
            .await
            .expect("put future record");
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect_err("a future format_version must be refused");
        assert!(matches!(err, ProvisioningError::UnsupportedVersion { .. }));
    }

    /// CreateIfAbsent race: the writer's own initial GET misses (an eventual
    /// blip, injected on the first Get of the prov key), but a concurrent
    /// winner's record is really present, so the CreateIfAbsent put returns
    /// AlreadyExists and the loser re-reads and validates against the winner
    /// rather than erroring. A matching winner is accepted.
    #[tokio::test]
    async fn create_if_absent_race_loser_revalidates_against_winner() {
        let inner = MemoryStore::new();
        let th = tenant();
        // Winner already wrote shard_count=4.
        seed_record(&inner, &th, Signal::Metrics, 4).await;
        // Loser's first GET of the prov key misses (eventual-consistency blip).
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                .with_key_contains("/prov")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);

        // Configured to agree with the winner: the re-read validates and passes.
        let out = validate_or_adopt(
            &store,
            &th,
            Signal::Metrics,
            4,
            3_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("race loser re-reads and validates against the winner");
        assert_eq!(out, ProvisioningCheck::Matched);
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::NotFoundBlip),
            1,
            "the blip must have fired for this to exercise the race path"
        );
    }

    /// Same race, but the loser's configured shard_count disagrees with the
    /// winner's record: the re-read surfaces the mismatch rather than silently
    /// accepting either value.
    #[tokio::test]
    async fn create_if_absent_race_loser_surfaces_mismatch() {
        let inner = MemoryStore::new();
        let th = tenant();
        seed_record(&inner, &th, Signal::Metrics, 4).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                .with_key_contains("/prov")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);

        let err = validate_or_adopt(
            &store,
            &th,
            Signal::Metrics,
            2,
            3_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect_err("a race loser configured for a different shard_count must refuse");
        assert!(matches!(err, ProvisioningError::ShardCountMismatch { .. }));
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::NotFoundBlip),
            1
        );
    }
}

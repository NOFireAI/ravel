//! Per-tenant KMS key-epoch record (ADR-0062 decision 1b).
//!
//! Each tenant carries an append-only key-epoch record at `t/<tenant_hash>/enc`
//! recording, in order, every KMS key the tenant's objects have been written
//! under: a list of epochs, each `{epoch, key_arn (empty = deployment default),
//! activated_ns}`. The record is tenant-scoped, not per-signal, because a
//! tenant's KMS key applies across every signal, so it sits directly under the
//! tenant prefix beside the per-signal `<sig>/prov` records.
//!
//! Its purpose is auditability: because data objects are immutable, every
//! object's write time locates it in exactly one epoch, so "which key encrypts
//! what" is answerable from the bucket alone. `verify-custody` uses this to add
//! an epoch-consistency check ([`epoch_for_write`]): a live object whose write
//! time predates the tenant's first recorded epoch is a custody anomaly.
//!
//! This module builds and verifies the durable record independently of the
//! live `KmsRoutingStore` routing decorator; wiring the record into the
//! decorator at server startup is a separate wiring step, not this module's. That
//! bootstrap MUST record epoch 0 with `key_arn: ""` (deployment default) and
//! an `activated_ns` at or before the tenant's earliest live object -- not
//! only at the moment an operator first configures a per-tenant key -- or
//! `verify-custody`'s epoch check reports a false-positive anomaly for every
//! object the tenant wrote before that moment.
//!
//! The mutation shape mirrors [`crate::provisioning::append_generation`]
//! exactly: read the existing record and its `CasVersion`, decode and validate
//! its history, append exactly one new entry whose `activated_ns` is strictly
//! past the last, and write back under `PutMode::CasVersion`. A lost CAS race
//! is a typed [`KeyEpochError::CasConflict`] the caller re-reads and retries on,
//! never a silent overwrite. The one difference from `append_generation` is the
//! not-found case: a tenant's first configured key has no record to append to,
//! so [`record_key_epoch`] bootstraps epoch 0 with `CreateIfAbsent`, following
//! the same race-safe precedent [`crate::provisioning::validate_or_adopt`] uses
//! to first create a `ProvisioningRecord`.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::TenantHash;

/// Format floor written into every key-epoch record this build emits, and the
/// highest record version it understands. A record declaring a higher version
/// is refused rather than misread under this layout (ADR-0062 decision 1b),
/// matching the `prov`/`sys/gc` records' version guards.
pub const KEY_EPOCH_FORMAT_VERSION: u32 = 1;

/// Object key for a tenant's key-epoch record: `t/<hex>/enc`. Tenant-scoped,
/// not per-signal, since a tenant's KMS key applies across every signal
/// (ADR-0062 decision 1b). Under the tenant's own prefix, alongside its
/// per-signal `<sig>/prov` records, never a bucket-root `sys/` object.
pub fn enc_key(tenant_hash: &TenantHash) -> String {
    format!("t/{}/enc", tenant_hash.to_hex())
}

/// A typed key-epoch failure. Every variant is fatal to the touch that raised
/// it: the fail-closed-on-any-anomaly discipline `ProvisioningError` follows.
/// None warn and continue.
#[derive(Debug, thiserror::Error)]
pub enum KeyEpochError {
    #[error("object store error on key-epoch record {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("key-epoch record {key:?} could not be decoded: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "key-epoch record {key:?} declares format_version {got}, but this build only understands \
         version {KEY_EPOCH_FORMAT_VERSION}: refusing rather than misread a future record format"
    )]
    UnsupportedVersion { key: String, got: u32 },
    #[error(
        "key-epoch record {key:?} is misfiled: it records {field} {actual}, but the key it was \
         read under expects {expected}"
    )]
    CorruptRecord {
        key: String,
        field: &'static str,
        expected: String,
        actual: String,
    },
    /// The record's append-only epoch history violates a structural invariant.
    /// Fail closed, same discipline as every other corrupt-input path: a
    /// malformed history could misattribute an object's custody. `defect` names
    /// the exact rule broken.
    #[error(
        "key-epoch record {key:?} has a corrupt epoch history: {defect} (ADR-0062 decision 1b)"
    )]
    CorruptEpochs { key: String, defect: EpochDefect },
    /// The appended epoch's `activated_ns` is not strictly after the current
    /// last epoch's. Rejected so the history stays a strictly increasing, gap-
    /// free timeline: an object's write time must locate it in exactly one
    /// epoch, which requires monotonic activation boundaries.
    #[error(
        "key-epoch activated_ns {activated_ns} is not strictly after the last epoch's \
         {last_activated_ns}: refusing to append a non-monotonic epoch (ADR-0062 decision 1b)"
    )]
    ActivationNotAfterLast {
        activated_ns: i64,
        last_activated_ns: i64,
    },
    /// The appended epoch's `key_arn` equals the current last epoch's, so the
    /// append would record a "rotation" to the same key: a no-op. Adjacent
    /// epochs must name different keys (mirroring `append_generation`'s
    /// same-count refusal).
    #[error(
        "key-epoch rotation to key_arn {key_arn:?} is a no-op: it equals the current epoch's key; \
         adjacent epochs must differ; refusing to append"
    )]
    SameKey { key_arn: String },
    /// A concurrent write moved the record's version between this append's read
    /// and its `CasVersion` write (or created it between this bootstrap's
    /// not-found read and its `CreateIfAbsent` write). The loser re-reads rather
    /// than silently overwriting the winner (ADR-0062 decision 1b, the
    /// `append_generation` CAS-conflict pattern).
    #[error(
        "a concurrent write changed key-epoch record {key:?} since this one read it (CAS \
         precondition failed): re-read and retry rather than overwrite the other write"
    )]
    CasConflict { key: String },
}

/// The exact structural rule a corrupt epoch history broke (ADR-0062 decision
/// 1b). Each is a fail-closed decode error, never a silent normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochDefect {
    /// A present record has no epochs at all (a written record always has at
    /// least epoch 0).
    Empty,
    /// Epoch numbers are not a dense 0-based sequence (0, 1, 2, ...).
    NotDense,
    /// `activated_ns` is not strictly increasing across the list.
    ActivationNotIncreasing,
}

impl std::fmt::Display for EpochDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EpochDefect::Empty => "a present record has no epochs (expected at least epoch 0)",
            EpochDefect::NotDense => "epoch numbers are not dense 0-based (0, 1, 2, ...)",
            EpochDefect::ActivationNotIncreasing => {
                "activated_ns is not strictly increasing across epochs"
            }
        };
        f.write_str(s)
    }
}

/// One entry of a key-epoch record's append-only history, decoded into a plain
/// struct so consumers ([`epoch_for_write`], `verify-custody`) never touch the
/// proto type (ADR-0062 decision 1b).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyEpoch {
    /// Dense, 0-based epoch number.
    pub epoch: u32,
    /// The KMS key ARN active from `activated_ns` onward. Empty string means
    /// the deployment default key (ADR-0062 decision 1c).
    pub key_arn: String,
    /// Unix ns at and after which objects are written under this epoch's key.
    pub activated_ns: i64,
}

/// The outcome of a successful [`record_key_epoch`] append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyEpochOutcome {
    /// The epoch number the call created (0 on the bootstrap create, one past
    /// the prior last on an append).
    pub epoch: u32,
    /// The full history after the write, epoch 0 first.
    pub epochs: Vec<KeyEpoch>,
}

impl KeyEpochError {
    fn store(key: &str, source: StoreError) -> Self {
        KeyEpochError::Store {
            key: key.to_string(),
            source,
        }
    }
}

/// Decode and validate a record's epoch history into the normalized form the
/// epoch-locating rule consumes (ADR-0062 decision 1b). Validated fail-closed:
/// non-empty, dense 0-based epochs, strictly increasing `activated_ns`. Any
/// violation is a typed [`KeyEpochError::CorruptEpochs`], never a panic or a
/// silent fix.
pub fn read_epochs(
    record: &sysproto::KeyEpochRecord,
    key: &str,
) -> Result<Vec<KeyEpoch>, KeyEpochError> {
    let corrupt = |defect: EpochDefect| KeyEpochError::CorruptEpochs {
        key: key.to_string(),
        defect,
    };
    if record.epochs.is_empty() {
        return Err(corrupt(EpochDefect::Empty));
    }
    let mut out = Vec::with_capacity(record.epochs.len());
    for (idx, e) in record.epochs.iter().enumerate() {
        if e.epoch as usize != idx {
            return Err(corrupt(EpochDefect::NotDense));
        }
        if idx > 0 && e.activated_ns <= record.epochs[idx - 1].activated_ns {
            return Err(corrupt(EpochDefect::ActivationNotIncreasing));
        }
        out.push(KeyEpoch {
            epoch: e.epoch,
            key_arn: e.key_arn.clone(),
            activated_ns: e.activated_ns,
        });
    }
    Ok(out)
}

/// [`read_epochs`], plus the format-version and tenant-misfile guard a caller
/// that decodes a record it read directly must apply before trusting it: a
/// record from a future format, or one misfiled/corrupted under the wrong
/// tenant's `t/<hash>/enc` key, must be refused rather than read as this
/// tenant's key history.
pub fn read_epochs_checked(
    record: &sysproto::KeyEpochRecord,
    key: &str,
    tenant_hash: &TenantHash,
) -> Result<Vec<KeyEpoch>, KeyEpochError> {
    if record.format_version > KEY_EPOCH_FORMAT_VERSION {
        return Err(KeyEpochError::UnsupportedVersion {
            key: key.to_string(),
            got: record.format_version,
        });
    }
    if record.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(KeyEpochError::CorruptRecord {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant_hash.to_hex(),
            actual: hex::encode(&record.tenant_hash),
        });
    }
    read_epochs(record, key)
}

/// Read a tenant's key-epoch record, if present. `NotFound` is `Ok(None)`, not
/// an error: absence means the tenant has only ever used the deployment default
/// key (no per-tenant key was ever configured).
async fn read_record(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<sysproto::KeyEpochRecord>, KeyEpochError> {
    match store.get(key, GetRange::Full).await {
        Ok(outcome) => {
            let record =
                sysproto::KeyEpochRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    KeyEpochError::Decode {
                        key: key.to_string(),
                        source,
                    }
                })?;
            Ok(Some(record))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(KeyEpochError::store(key, err)),
    }
}

/// Read and normalize a tenant's full key-epoch history straight from the
/// store, fresh and uncached. `Ok(None)` means no per-tenant key was ever
/// configured (the tenant has only ever used the deployment default), which
/// the caller treats as "nothing to check", not an error. A present record is
/// fully version/misfile/history-checked via [`read_epochs_checked`], so a
/// corrupt or misfiled record fails closed rather than being read as "no key".
pub async fn read_epochs_from_store(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
) -> Result<Option<Vec<KeyEpoch>>, KeyEpochError> {
    let key = enc_key(tenant_hash);
    match read_record(store, &key).await? {
        Some(record) => Ok(Some(read_epochs_checked(&record, &key, tenant_hash)?)),
        None => Ok(None),
    }
}

/// Locate the epoch covering write time `write_ns`: the latest epoch whose
/// `activated_ns` is `<= write_ns`. Returns `None` when `write_ns` predates the
/// first epoch's activation (the object was written before the tenant's first
/// recorded key epoch), which `verify-custody` reports as a custody anomaly.
///
/// `epochs` must be the normalized, validated history [`read_epochs`] returns
/// (non-empty, dense, activation-increasing). Because activation boundaries are
/// strictly increasing and every epoch is active until the next one activates,
/// a `write_ns` at or after the first epoch always lands in exactly one epoch
/// (no gaps exist by construction); `None` is returned only for the
/// predates-first-epoch case. A pure function with no I/O.
pub fn epoch_for_write(epochs: &[KeyEpoch], write_ns: i64) -> Option<&KeyEpoch> {
    epochs
        .iter()
        .filter(|e| e.activated_ns <= write_ns)
        .max_by_key(|e| e.epoch)
}

/// Build the epoch-0 record persisted when a tenant's key is first configured.
fn build_record(
    tenant_hash: &TenantHash,
    key_arn: &str,
    activated_ns: i64,
    now_ns: i64,
) -> sysproto::KeyEpochRecord {
    sysproto::KeyEpochRecord {
        format_version: KEY_EPOCH_FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        created_unix_ns: now_ns,
        epochs: vec![sysproto::KeyEpoch {
            epoch: 0,
            key_arn: key_arn.to_string(),
            activated_ns,
        }],
    }
}

/// Record a new or changed KMS key for a tenant by appending one epoch to its
/// `t/<hash>/enc` record (ADR-0062 decision 1b). The single legal mutation of
/// the record: it appends exactly one epoch whose `activated_ns` is strictly
/// past the last, and never touches an existing byte of history.
///
/// When no record exists yet, this bootstraps epoch 0 with `CreateIfAbsent`,
/// following the race-safe precedent [`crate::provisioning::validate_or_adopt`]
/// uses to first create a `ProvisioningRecord`. When a record exists, it is
/// version/misfile/history-checked and normalized via [`read_epochs_checked`]
/// before one epoch is appended under `CasVersion`, exactly as
/// [`crate::provisioning::append_generation`] does for shard generations.
///
/// Enforced fail-closed before any write on the append path:
/// - the existing history must decode and validate ([`read_epochs`]);
/// - `activated_ns` must be strictly after the current last epoch's
///   ([`KeyEpochError::ActivationNotAfterLast`]);
/// - `key_arn` must differ from the current last epoch's
///   ([`KeyEpochError::SameKey`]).
///
/// A concurrent write that moved the version (or created the record between the
/// not-found read and the `CreateIfAbsent`) is a typed
/// [`KeyEpochError::CasConflict`]: the loser re-reads, never silently
/// overwrites the winner.
///
/// `key_arn` is the tenant's KMS key ARN; an empty string records the
/// deployment default key (ADR-0062 decision 1c). `now_ns` is stamped as the
/// record's informational `created_unix_ns` on the bootstrap create.
pub async fn record_key_epoch(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    key_arn: &str,
    activated_ns: i64,
    now_ns: i64,
) -> Result<KeyEpochOutcome, KeyEpochError> {
    let key = enc_key(tenant_hash);

    let (record, version) = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let record =
                sysproto::KeyEpochRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    KeyEpochError::Decode {
                        key: key.clone(),
                        source,
                    }
                })?;
            (record, outcome.version)
        }
        Err(StoreError::NotFound) => {
            // Bootstrap: no record exists, so there is nothing to append to.
            // Create it with epoch 0 under CreateIfAbsent. A racing loser
            // (AlreadyExists) surfaces as a CAS conflict so the caller re-reads
            // and retries down the append path, never overwriting the winner.
            let record = build_record(tenant_hash, key_arn, activated_ns, now_ns);
            return match store
                .put(
                    &key,
                    record.encode_to_vec().into(),
                    PutOptions::create_if_absent(),
                )
                .await
            {
                Ok(_) => Ok(KeyEpochOutcome {
                    epoch: 0,
                    epochs: vec![KeyEpoch {
                        epoch: 0,
                        key_arn: key_arn.to_string(),
                        activated_ns,
                    }],
                }),
                Err(StoreError::AlreadyExists) => Err(KeyEpochError::CasConflict { key }),
                Err(err) => Err(KeyEpochError::store(&key, err)),
            };
        }
        Err(err) => return Err(KeyEpochError::store(&key, err)),
    };

    // Version, misfile, and history-corruption guard, then normalize the
    // existing history (fail closed on any of these) before extending it. A
    // record misfiled under another tenant must never be extended as this
    // tenant's history: record_key_epoch writes the record back, so trusting a
    // misfiled read would durably corrupt it.
    let mut history = read_epochs_checked(&record, &key, tenant_hash)?;
    // A validated history is non-empty (read_epochs rejects Empty), so `last`
    // is always present; the fallback keeps this total without an unwrap.
    let last = match history.last() {
        Some(last) => last.clone(),
        None => {
            return Err(KeyEpochError::CorruptEpochs {
                key,
                defect: EpochDefect::Empty,
            });
        }
    };

    if activated_ns <= last.activated_ns {
        return Err(KeyEpochError::ActivationNotAfterLast {
            activated_ns,
            last_activated_ns: last.activated_ns,
        });
    }
    if key_arn == last.key_arn {
        return Err(KeyEpochError::SameKey {
            key_arn: key_arn.to_string(),
        });
    }

    let new_epoch = last.epoch + 1;
    history.push(KeyEpoch {
        epoch: new_epoch,
        key_arn: key_arn.to_string(),
        activated_ns,
    });

    let mut new_record = record;
    new_record.epochs = history
        .iter()
        .map(|e| sysproto::KeyEpoch {
            epoch: e.epoch,
            key_arn: e.key_arn.clone(),
            activated_ns: e.activated_ns,
        })
        .collect();

    match store
        .put(
            &key,
            new_record.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::CasVersion(version),
                checksum: None,
            },
        )
        .await
    {
        Ok(_) => Ok(KeyEpochOutcome {
            epoch: new_epoch,
            epochs: history,
        }),
        Err(StoreError::PreconditionFailed) => Err(KeyEpochError::CasConflict { key }),
        Err(err) => Err(KeyEpochError::store(&key, err)),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use std::sync::Arc;

    fn tenant() -> TenantHash {
        TenantHash([0xABu8; 16])
    }

    fn mem() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    const ARN_A: &str = "arn:aws:kms:us-east-1:111122223333:key/aaaaaaaa";
    const ARN_B: &str = "arn:aws:kms:us-east-1:111122223333:key/bbbbbbbb";

    /// Overwrite-seed a record with the given epochs directly, for tests that
    /// need an existing record without going through the CreateIfAbsent path.
    async fn seed_record(store: &dyn ObjectStoreBackend, th: &TenantHash, epochs: Vec<KeyEpoch>) {
        let record = sysproto::KeyEpochRecord {
            format_version: KEY_EPOCH_FORMAT_VERSION,
            tenant_hash: th.0.to_vec(),
            created_unix_ns: 1_000,
            epochs: epochs
                .iter()
                .map(|e| sysproto::KeyEpoch {
                    epoch: e.epoch,
                    key_arn: e.key_arn.clone(),
                    activated_ns: e.activated_ns,
                })
                .collect(),
        };
        store
            .put(
                &enc_key(th),
                record.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed key-epoch record");
    }

    #[test]
    fn enc_key_is_tenant_scoped() {
        assert_eq!(
            enc_key(&tenant()),
            format!("t/{}/enc", tenant().to_hex()),
            "the key is tenant-scoped, not per-signal"
        );
    }

    /// Appending a first epoch to a tenant with no record creates it; a second
    /// epoch with a later activated_ns succeeds and the read returns both in
    /// order.
    #[tokio::test]
    async fn first_append_creates_then_second_appends_in_order() {
        let store = mem();
        let first = record_key_epoch(store.as_ref(), &tenant(), ARN_A, 1_000, 500)
            .await
            .expect("first epoch creates the record");
        assert_eq!(first.epoch, 0);
        assert_eq!(first.epochs.len(), 1);
        assert_eq!(first.epochs[0].key_arn, ARN_A);

        let second = record_key_epoch(store.as_ref(), &tenant(), ARN_B, 2_000, 1_500)
            .await
            .expect("a later activated_ns appends epoch 1");
        assert_eq!(second.epoch, 1);
        assert_eq!(second.epochs.len(), 2);

        let read = read_epochs_from_store(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("record present");
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].epoch, 0);
        assert_eq!(read[0].key_arn, ARN_A);
        assert_eq!(read[0].activated_ns, 1_000);
        assert_eq!(read[1].epoch, 1);
        assert_eq!(read[1].key_arn, ARN_B);
        assert_eq!(read[1].activated_ns, 2_000);
    }

    /// A read on a tenant that never had a record returns Ok(None) cleanly:
    /// "no per-tenant key configured", not an error.
    #[tokio::test]
    async fn read_absent_record_is_none_not_error() {
        let store = mem();
        let got = read_epochs_from_store(store.as_ref(), &tenant())
            .await
            .expect("an absent record is not an error");
        assert!(got.is_none(), "no per-tenant key ever configured");
    }

    /// Appending an epoch whose activated_ns is not strictly after the last
    /// epoch's is a typed error, and no write happens: a direct read-back shows
    /// the record unchanged (still one epoch).
    #[tokio::test]
    async fn append_non_monotonic_activation_is_error_and_writes_nothing() {
        let store = mem();
        record_key_epoch(store.as_ref(), &tenant(), ARN_A, 2_000, 500)
            .await
            .expect("epoch 0 created");

        // Equal activated_ns: not strictly after.
        let err = record_key_epoch(store.as_ref(), &tenant(), ARN_B, 2_000, 2_500)
            .await
            .expect_err("a non-increasing activated_ns must be refused");
        assert!(
            matches!(err, KeyEpochError::ActivationNotAfterLast { .. }),
            "got: {err}"
        );

        // Earlier activated_ns: also refused.
        let err = record_key_epoch(store.as_ref(), &tenant(), ARN_B, 1_000, 2_500)
            .await
            .expect_err("an earlier activated_ns must be refused");
        assert!(matches!(err, KeyEpochError::ActivationNotAfterLast { .. }));

        // The record is unchanged: still exactly the one epoch 0.
        let read = read_epochs_from_store(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(read.len(), 1, "no epoch was appended");
        assert_eq!(read[0].epoch, 0);
        assert_eq!(read[0].key_arn, ARN_A);
    }

    /// A rotation to the same key ARN is a no-op and refused (adjacent epochs
    /// must name different keys).
    #[tokio::test]
    async fn append_same_key_is_refused() {
        let store = mem();
        record_key_epoch(store.as_ref(), &tenant(), ARN_A, 1_000, 500)
            .await
            .expect("epoch 0 created");
        let err = record_key_epoch(store.as_ref(), &tenant(), ARN_A, 2_000, 1_500)
            .await
            .expect_err("a same-key rotation must be refused");
        assert!(matches!(err, KeyEpochError::SameKey { .. }), "got: {err}");
    }

    /// A concurrent append racing the same tenant: the first CAS wins, the
    /// second reads the same version and its stale CAS write is rejected as a
    /// typed `CasConflict`, never silently overwriting the winner. Proven with
    /// `FaultStore` turning the loser's CAS `put` into a `PreconditionFailed`,
    /// exactly what a concurrent append that moved the version would cause.
    #[tokio::test]
    async fn cas_conflict_on_concurrent_append() {
        let inner = MemoryStore::new();
        seed_record(
            &inner,
            &tenant(),
            vec![KeyEpoch {
                epoch: 0,
                key_arn: ARN_A.to_string(),
                activated_ns: 1_000,
            }],
        )
        .await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/enc"),
        );
        let store = FaultStore::new(inner, plan);

        let err = record_key_epoch(&store, &tenant(), ARN_B, 2_000, 1_500)
            .await
            .expect_err("a CAS precondition failure must surface as a typed conflict");
        assert!(
            matches!(err, KeyEpochError::CasConflict { .. }),
            "got: {err}"
        );
        assert_eq!(
            store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            1,
            "the injected precondition failure must have fired"
        );
    }

    /// A concurrent bootstrap race: two writers both see no record, one wins
    /// the CreateIfAbsent, the loser's CreateIfAbsent returns AlreadyExists and
    /// surfaces as a typed conflict, never a silent overwrite.
    #[tokio::test]
    async fn cas_conflict_on_concurrent_bootstrap() {
        let inner = MemoryStore::new();
        // Under CreateIfAbsent, a FailedConditionalWrite surfaces as
        // StoreError::AlreadyExists, exactly what a concurrent bootstrap winner
        // would cause on the loser's create.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/enc"),
        );
        let store = FaultStore::new(inner, plan);
        let err = record_key_epoch(&store, &tenant(), ARN_A, 1_000, 500)
            .await
            .expect_err("a losing CreateIfAbsent race must surface as a typed conflict");
        assert!(
            matches!(err, KeyEpochError::CasConflict { .. }),
            "got: {err}"
        );
        assert_eq!(
            store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            1
        );
    }

    #[test]
    fn epoch_for_write_locates_the_covering_epoch() {
        let epochs = vec![
            KeyEpoch {
                epoch: 0,
                key_arn: ARN_A.to_string(),
                activated_ns: 100,
            },
            KeyEpoch {
                epoch: 1,
                key_arn: ARN_B.to_string(),
                activated_ns: 200,
            },
        ];
        assert!(
            epoch_for_write(&epochs, 99).is_none(),
            "before the first epoch: predates every recorded epoch"
        );
        assert_eq!(epoch_for_write(&epochs, 100).expect("in epoch 0").epoch, 0);
        assert_eq!(epoch_for_write(&epochs, 199).expect("in epoch 0").epoch, 0);
        assert_eq!(epoch_for_write(&epochs, 200).expect("in epoch 1").epoch, 1);
        assert_eq!(
            epoch_for_write(&epochs, 10_000).expect("in epoch 1").epoch,
            1
        );
    }

    /// Corrupt histories are typed `CorruptEpochs` errors naming the exact
    /// defect, never a panic.
    #[test]
    fn corrupt_epoch_histories_are_typed_errors() {
        let key = enc_key(&tenant());
        let base = |epochs: Vec<sysproto::KeyEpoch>| sysproto::KeyEpochRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            created_unix_ns: 0,
            epochs,
        };
        let e = |epoch, activated_ns| sysproto::KeyEpoch {
            epoch,
            key_arn: ARN_A.to_string(),
            activated_ns,
        };
        let expect_defect = |record: &sysproto::KeyEpochRecord, defect: EpochDefect| {
            match read_epochs(record, &key) {
                Err(KeyEpochError::CorruptEpochs { defect: got, .. }) => {
                    assert_eq!(got, defect, "wrong defect");
                }
                other => panic!("expected CorruptEpochs({defect:?}), got {other:?}"),
            }
        };
        expect_defect(&base(vec![]), EpochDefect::Empty);
        expect_defect(&base(vec![e(0, 100), e(2, 200)]), EpochDefect::NotDense);
        expect_defect(
            &base(vec![e(0, 100), e(1, 100)]),
            EpochDefect::ActivationNotIncreasing,
        );
    }

    /// A record misfiled under another tenant's `enc` key is refused rather
    /// than read as this tenant's key history.
    #[tokio::test]
    async fn read_rejects_misfiled_tenant() {
        let store = mem();
        let other = TenantHash([0xEE; 16]);
        // Seed under `tenant()`'s key but with `other`'s tenant_hash in the body.
        seed_record(
            store.as_ref(),
            &other,
            vec![KeyEpoch {
                epoch: 0,
                key_arn: ARN_A.to_string(),
                activated_ns: 1_000,
            }],
        )
        .await;
        // Move it to tenant()'s key: a misfile.
        let body = {
            let record = sysproto::KeyEpochRecord {
                format_version: 1,
                tenant_hash: other.0.to_vec(),
                created_unix_ns: 0,
                epochs: vec![sysproto::KeyEpoch {
                    epoch: 0,
                    key_arn: ARN_A.to_string(),
                    activated_ns: 1_000,
                }],
            };
            record.encode_to_vec()
        };
        store
            .put(&enc_key(&tenant()), body.into(), PutOptions::default())
            .await
            .expect("put misfiled record");
        let err = read_epochs_from_store(store.as_ref(), &tenant())
            .await
            .expect_err("a record for a different tenant must be refused");
        assert!(matches!(
            err,
            KeyEpochError::CorruptRecord {
                field: "tenant_hash",
                ..
            }
        ));
    }

    /// A future format_version is refused rather than misread under this
    /// layout.
    #[tokio::test]
    async fn read_rejects_future_format_version() {
        let store = mem();
        let record = sysproto::KeyEpochRecord {
            format_version: KEY_EPOCH_FORMAT_VERSION + 1,
            tenant_hash: tenant().0.to_vec(),
            created_unix_ns: 0,
            epochs: vec![sysproto::KeyEpoch {
                epoch: 0,
                key_arn: ARN_A.to_string(),
                activated_ns: 1_000,
            }],
        };
        store
            .put(
                &enc_key(&tenant()),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("put future record");
        let err = read_epochs_from_store(store.as_ref(), &tenant())
            .await
            .expect_err("a future format_version must be refused");
        assert!(matches!(err, KeyEpochError::UnsupportedVersion { .. }));
    }
}

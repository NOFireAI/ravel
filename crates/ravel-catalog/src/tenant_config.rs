//! Per-tenant durable config record (ADR-0066 decision 6).
//!
//! Each tenant carries a config record at `t/<tenant_hash>/config` recording the
//! per-tenant overrides that used to live only in process flags: a lifecycle
//! state (`active` / `suspended` / `offboarding`), admission-limit overrides,
//! a retention override, and an indexed-field override. Defaults still come from
//! flags/limits-file at startup; a field present here overrides the default for
//! this tenant, an absent one leaves the default in place.
//!
//! The record is tenant-scoped, not per-signal, because lifecycle, limits, and
//! retention apply across every signal, so it sits directly under the tenant
//! prefix beside the tenant-scoped `enc` record and the per-signal `<sig>/prov`
//! records, never a bucket-root `sys/` object.
//!
//! Mutation shape: whole-record CAS-replace, mirroring
//! [`ravel_maintain`-style `set_gc_config`](../../ravel-maintain/src/gc_config.rs)
//! (read the current version, swap under `PutMode::CasVersion`), NOT the
//! append-only repeated-field shape of `provisioning`'s `generations` /
//! `format_floors` or `key_epoch`'s `epochs`. Those histories are append-only
//! because each entry is an immutable historical fact whose order is load-bearing
//! and which must never be lost. A config override is the opposite: mutable
//! current state whose latest value is the only one that matters, and which must
//! support lowering a limit or clearing an override entirely — a change an
//! append-only list cannot express. So the current value is stored in place and
//! swapped under `CasVersion`; a concurrent write is a caught
//! [`TenantConfigError::CasConflict`] the loser re-reads, never a silent
//! overwrite. On a tenant with no record yet, [`set_tenant_config`] bootstraps it
//! with `CreateIfAbsent`, the race-safe first-write precedent
//! [`crate::provisioning::validate_or_adopt`] and `set_gc_config` both use.
//!
//! This module builds and verifies the durable record only. The bounded-staleness
//! refresh loop that reads it on a horizon and re-invokes the admission
//! controller's `set_tenant_limits` (the hot-path-untouched application of these
//! overrides) is a separate hot-path step, not this module's.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::TenantHash;

/// Format floor written into every config record this build emits, and the
/// highest record version it understands. A record declaring a higher version is
/// refused rather than misread under this layout, matching the `prov` / `enc`
/// records' version guards (both other `t/<hash>/` records in this crate).
pub const TENANT_CONFIG_FORMAT_VERSION: u32 = 1;

/// Object key for a tenant's config record: `t/<hex>/config`. Tenant-scoped, not
/// per-signal (ADR-0066 decision 6). Under the tenant's own prefix, alongside its
/// per-signal `<sig>/prov` and tenant-scoped `enc` records, never a bucket-root
/// `sys/` object.
pub fn config_key(tenant_hash: &TenantHash) -> String {
    format!("t/{}/config", tenant_hash.to_hex())
}

/// A tenant's lifecycle state (ADR-0066 decision 6), decoded into a domain enum
/// so consumers never touch the proto `i32`. The three real states of the
/// durable record; a record carrying the proto `UNSPECIFIED` sentinel is corrupt
/// and refused on decode ([`TenantConfigError::InvalidLifecycleState`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantLifecycleState {
    /// Unrestricted: ingest, query, and maintenance all run.
    Active,
    /// Refuse ingest and query; keep maintenance running.
    Suspended,
    /// Refuse ingest and query; keep maintenance and retention running until the
    /// tenant's data is gone (the offboarding hook consumes).
    Offboarding,
}

impl TenantLifecycleState {
    /// Map to the persisted proto enum value.
    fn to_proto(self) -> sysproto::TenantLifecycleState {
        match self {
            TenantLifecycleState::Active => sysproto::TenantLifecycleState::Active,
            TenantLifecycleState::Suspended => sysproto::TenantLifecycleState::Suspended,
            TenantLifecycleState::Offboarding => sysproto::TenantLifecycleState::Offboarding,
        }
    }

    /// Map from the persisted proto `i32`. `UNSPECIFIED` (0) and any unknown
    /// value are refused: a written record always names a real state, so an
    /// absent or unknown one is corruption, not a default to guess at.
    fn from_proto_i32(value: i32) -> Option<Self> {
        match sysproto::TenantLifecycleState::try_from(value).ok()? {
            sysproto::TenantLifecycleState::Unspecified => None,
            sysproto::TenantLifecycleState::Active => Some(TenantLifecycleState::Active),
            sysproto::TenantLifecycleState::Suspended => Some(TenantLifecycleState::Suspended),
            sysproto::TenantLifecycleState::Offboarding => Some(TenantLifecycleState::Offboarding),
        }
    }
}

/// A tenant's durable config, decoded into a plain struct so consumers (the
/// refresh loop, the admission controller) never touch the proto type. An
/// absent optional override means "use the deployment default"; a present one
/// overrides it for this tenant (a present `0` is a real override, an explicit
/// zero cap, never "unset").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantConfig {
    /// The lifecycle state gating ingest/query/maintenance.
    pub lifecycle_state: TenantLifecycleState,
    /// Active-series cap override (ADR-0051 admission dimension).
    pub max_active_series: Option<u64>,
    /// Active-streams cap override.
    pub max_active_streams: Option<u64>,
    /// Ingest byte-rate cap override.
    pub max_ingest_byte_rate: Option<u64>,
    /// Series/stream creation-rate cap override.
    pub max_series_creation_rate: Option<u64>,
    /// Retention override, in ns.
    pub retention_ns: Option<i64>,
    /// Indexed-field override: `None` = no override (deployment default);
    /// `Some(vec![])` = override to no indexed fields; `Some(non-empty)` = the
    /// explicit indexed-field set.
    pub indexed_fields: Option<Vec<String>>,
}

impl TenantConfig {
    /// A config that overrides nothing but the lifecycle state: every limit,
    /// retention, and indexed-field override left at the deployment default. The
    /// minimal record for a tenant whose only durable fact is its lifecycle.
    pub fn new(lifecycle_state: TenantLifecycleState) -> Self {
        TenantConfig {
            lifecycle_state,
            max_active_series: None,
            max_active_streams: None,
            max_ingest_byte_rate: None,
            max_series_creation_rate: None,
            retention_ns: None,
            indexed_fields: None,
        }
    }
}

/// A typed config-record failure. Every variant is fatal to the touch that
/// raised it, the fail-closed-on-any-anomaly discipline `ProvisioningError` and
/// `KeyEpochError` follow. None warn and continue.
#[derive(Debug, thiserror::Error)]
pub enum TenantConfigError {
    #[error("object store error on config record {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("config record {key:?} could not be decoded: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "config record {key:?} declares format_version {got}, but this build only understands \
         version {TENANT_CONFIG_FORMAT_VERSION}: refusing rather than misread a future record format"
    )]
    UnsupportedVersion { key: String, got: u32 },
    #[error(
        "config record {key:?} is misfiled: it records {field} {actual}, but the key it was read \
         under expects {expected}"
    )]
    CorruptRecord {
        key: String,
        field: &'static str,
        expected: String,
        actual: String,
    },
    /// The record's `lifecycle_state` is the `UNSPECIFIED` sentinel or an unknown
    /// value. A written record always names a real state; refusing fail-closed
    /// rather than guessing a default keeps a corrupt record from silently
    /// admitting or refusing a tenant.
    #[error(
        "config record {key:?} has an invalid lifecycle_state {got}: a written record must name \
         active, suspended, or offboarding (ADR-0066 decision 6)"
    )]
    InvalidLifecycleState { key: String, got: i32 },
    /// A concurrent write moved the record's version between this write's read and
    /// its `CasVersion` swap (or created it between a not-found read and the
    /// `CreateIfAbsent`). The loser re-reads rather than silently overwriting the
    /// winner (the `set_gc_config` / `append_generation` CAS-conflict pattern).
    #[error(
        "a concurrent write changed config record {key:?} since this one read it (CAS precondition \
         failed): re-read and retry rather than overwrite the other write"
    )]
    CasConflict { key: String },
}

impl TenantConfigError {
    fn store(key: &str, source: StoreError) -> Self {
        TenantConfigError::Store {
            key: key.to_string(),
            source,
        }
    }
}

/// Decode a proto record into the domain [`TenantConfig`], validating the
/// version, the (tenant) misfile guard, and the lifecycle state fail-closed. A
/// record from a future format, one misfiled under the wrong tenant's `config`
/// key, or one carrying an invalid lifecycle state is refused rather than read as
/// this tenant's config.
fn decode_record(
    record: &sysproto::TenantConfigRecord,
    key: &str,
    tenant_hash: &TenantHash,
) -> Result<TenantConfig, TenantConfigError> {
    if record.format_version > TENANT_CONFIG_FORMAT_VERSION {
        return Err(TenantConfigError::UnsupportedVersion {
            key: key.to_string(),
            got: record.format_version,
        });
    }
    if record.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(TenantConfigError::CorruptRecord {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant_hash.to_hex(),
            actual: hex::encode(&record.tenant_hash),
        });
    }
    let lifecycle_state = TenantLifecycleState::from_proto_i32(record.lifecycle_state).ok_or(
        TenantConfigError::InvalidLifecycleState {
            key: key.to_string(),
            got: record.lifecycle_state,
        },
    )?;
    Ok(TenantConfig {
        lifecycle_state,
        max_active_series: record.max_active_series,
        max_active_streams: record.max_active_streams,
        max_ingest_byte_rate: record.max_ingest_byte_rate,
        max_series_creation_rate: record.max_series_creation_rate,
        retention_ns: record.retention_ns,
        indexed_fields: record.indexed_fields.as_ref().map(|c| c.fields.clone()),
    })
}

/// Build the proto record to persist for a tenant's config.
fn build_record(
    tenant_hash: &TenantHash,
    config: &TenantConfig,
    created_unix_ns: i64,
    updated_unix_ns: i64,
) -> sysproto::TenantConfigRecord {
    sysproto::TenantConfigRecord {
        format_version: TENANT_CONFIG_FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        lifecycle_state: config.lifecycle_state.to_proto() as i32,
        max_active_series: config.max_active_series,
        max_active_streams: config.max_active_streams,
        max_ingest_byte_rate: config.max_ingest_byte_rate,
        max_series_creation_rate: config.max_series_creation_rate,
        retention_ns: config.retention_ns,
        indexed_fields: config
            .indexed_fields
            .as_ref()
            .map(|fields| sysproto::IndexedFieldConfig {
                fields: fields.clone(),
            }),
        created_unix_ns,
        updated_unix_ns,
    }
}

/// Read a tenant's config record, returning the decoded config and the store
/// version needed for a later `CasVersion` swap. `Ok(None)` when no record
/// exists: absence means "no per-tenant overrides; the tenant runs on the
/// deployment defaults", a valid state, not an error. A present record is fully
/// version/misfile/lifecycle-checked, so a corrupt or misfiled record fails
/// closed rather than being read as this tenant's config.
pub async fn read_config(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
) -> Result<Option<(TenantConfig, Version)>, TenantConfigError> {
    let key = config_key(tenant_hash);
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let record =
                sysproto::TenantConfigRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    TenantConfigError::Decode {
                        key: key.clone(),
                        source,
                    }
                })?;
            let config = decode_record(&record, &key, tenant_hash)?;
            Ok(Some((config, outcome.version)))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(TenantConfigError::store(&key, err)),
    }
}

/// Read a tenant's config, dropping the store version. The convenience form for
/// a caller that only needs the values (the refresh loop's read), not a version
/// to swap against. `Ok(None)` is the no-overrides case, identical to
/// [`read_config`].
pub async fn read_config_values(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
) -> Result<Option<TenantConfig>, TenantConfigError> {
    Ok(read_config(store, tenant_hash).await?.map(|(c, _v)| c))
}

/// The result of a [`set_tenant_config`] write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOutcome {
    /// No record existed and one was created with `CreateIfAbsent`.
    Created,
    /// A record existed and was swapped in place under `CasVersion`.
    Updated,
}

/// Write a tenant's config, whole-record CAS-replace (ADR-0066 decision 6,
/// mirroring `set_gc_config`). The single legal mutation of the record: it reads
/// the current record and its version, then swaps in the new values under
/// `PutMode::CasVersion`, so a concurrent write is a
/// [`TenantConfigError::CasConflict`], never a silent overwrite. On a tenant with
/// no record yet it creates one with `CreateIfAbsent` (a concurrent creator
/// winning that race is also a `CasConflict`, since this call's read observed no
/// object).
///
/// `now_ns` is stamped as `updated_unix_ns`, and as `created_unix_ns` too when
/// the record is first created; on an update the existing `created_unix_ns` is
/// carried through verbatim.
pub async fn set_tenant_config(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    config: &TenantConfig,
    now_ns: i64,
) -> Result<SetOutcome, TenantConfigError> {
    let key = config_key(tenant_hash);

    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            // A record exists. Decode it only to carry its created_unix_ns
            // through (and to fail closed on a misfiled/future record rather than
            // overwrite one that is not this tenant's), then swap under
            // CasVersion.
            let existing =
                sysproto::TenantConfigRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    TenantConfigError::Decode {
                        key: key.clone(),
                        source,
                    }
                })?;
            // Guard version and misfile before writing back: set_tenant_config
            // persists the record, so trusting a misfiled or future-format read
            // would durably corrupt it.
            decode_record(&existing, &key, tenant_hash)?;
            let created_unix_ns = existing.created_unix_ns;
            let record = build_record(tenant_hash, config, created_unix_ns, now_ns);
            match store
                .put(
                    &key,
                    record.encode_to_vec().into(),
                    PutOptions {
                        mode: PutMode::CasVersion(outcome.version),
                        checksum: None,
                    },
                )
                .await
            {
                Ok(_) => Ok(SetOutcome::Updated),
                Err(StoreError::PreconditionFailed) => Err(TenantConfigError::CasConflict { key }),
                Err(err) => Err(TenantConfigError::store(&key, err)),
            }
        }
        Err(StoreError::NotFound) => {
            let record = build_record(tenant_hash, config, now_ns, now_ns);
            match store
                .put(
                    &key,
                    record.encode_to_vec().into(),
                    PutOptions::create_if_absent(),
                )
                .await
            {
                Ok(_) => Ok(SetOutcome::Created),
                Err(StoreError::AlreadyExists) => Err(TenantConfigError::CasConflict { key }),
                Err(err) => Err(TenantConfigError::store(&key, err)),
            }
        }
        Err(err) => Err(TenantConfigError::store(&key, err)),
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
        TenantHash([0x11u8; 16])
    }

    fn mem() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// A config exercising every field: a non-default lifecycle, every optional
    /// override present, and a non-empty indexed-field list.
    fn full_config() -> TenantConfig {
        TenantConfig {
            lifecycle_state: TenantLifecycleState::Suspended,
            max_active_series: Some(1_000_000),
            max_active_streams: Some(2_000),
            max_ingest_byte_rate: Some(5_000_000),
            max_series_creation_rate: Some(100),
            retention_ns: Some(72 * 3_600_000_000_000),
            indexed_fields: Some(vec!["service.name".into(), "http.route".into()]),
        }
    }

    #[test]
    fn config_key_is_tenant_scoped() {
        assert_eq!(
            config_key(&tenant()),
            format!("t/{}/config", tenant().to_hex()),
            "the key is tenant-scoped, not per-signal or bucket-root"
        );
    }

    /// A full config round-trips through the store byte-for-byte on the domain
    /// values: every override, the lifecycle state, and the indexed-field list
    /// read back exactly as written.
    #[tokio::test]
    async fn full_config_round_trips() {
        let store = mem();
        let cfg = full_config();
        let outcome = set_tenant_config(store.as_ref(), &tenant(), &cfg, 1_000)
            .await
            .expect("first write creates the record");
        assert_eq!(outcome, SetOutcome::Created);

        let (read, _version) = read_config(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("record present");
        assert_eq!(read, cfg, "every field round-trips unchanged");
    }

    /// The minimal config (only a lifecycle state, every override absent)
    /// round-trips with all overrides `None`, distinct from a present-but-empty
    /// override.
    #[tokio::test]
    async fn minimal_config_round_trips_with_absent_overrides() {
        let store = mem();
        let cfg = TenantConfig::new(TenantLifecycleState::Active);
        set_tenant_config(store.as_ref(), &tenant(), &cfg, 1_000)
            .await
            .expect("create");
        let (read, _v) = read_config(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(read, cfg);
        assert!(read.max_active_series.is_none());
        assert!(read.indexed_fields.is_none(), "absent, not empty");
    }

    /// A present-but-empty indexed-field override ("override to no indexed
    /// fields") is distinct from an absent one ("use the deployment default") and
    /// round-trips as `Some(vec![])`, not `None`.
    #[tokio::test]
    async fn empty_indexed_fields_override_is_distinct_from_absent() {
        let store = mem();
        let cfg = TenantConfig {
            indexed_fields: Some(vec![]),
            ..TenantConfig::new(TenantLifecycleState::Active)
        };
        set_tenant_config(store.as_ref(), &tenant(), &cfg, 1)
            .await
            .expect("create");
        let (read, _v) = read_config(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            read.indexed_fields,
            Some(vec![]),
            "present-but-empty override must not decode as absent"
        );
    }

    /// An absent record is `Ok(None)`, not an error: the tenant runs on
    /// deployment defaults.
    #[tokio::test]
    async fn read_absent_record_is_none_not_error() {
        let store = mem();
        let got = read_config_values(store.as_ref(), &tenant())
            .await
            .expect("an absent record is not an error");
        assert!(got.is_none(), "no per-tenant overrides configured");
    }

    /// A second write updates in place under CasVersion, carrying the original
    /// created_unix_ns through and stamping a new updated_unix_ns.
    #[tokio::test]
    async fn second_write_updates_in_place_and_preserves_created_ts() {
        let store = mem();
        set_tenant_config(
            store.as_ref(),
            &tenant(),
            &TenantConfig::new(TenantLifecycleState::Active),
            1_000,
        )
        .await
        .expect("create");

        let updated = TenantConfig::new(TenantLifecycleState::Offboarding);
        let outcome = set_tenant_config(store.as_ref(), &tenant(), &updated, 2_000)
            .await
            .expect("update");
        assert_eq!(outcome, SetOutcome::Updated);

        let (read, _v) = read_config(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(read.lifecycle_state, TenantLifecycleState::Offboarding);

        // created_unix_ns is preserved (the first write's), updated_unix_ns is
        // the second write's. Inspect the raw proto to check the timestamps.
        let raw = store
            .get(&config_key(&tenant()), GetRange::Full)
            .await
            .expect("get");
        let proto =
            sysproto::TenantConfigRecord::decode(raw.data.as_ref()).expect("decode raw record");
        assert_eq!(proto.created_unix_ns, 1_000, "created_unix_ns preserved");
        assert_eq!(proto.updated_unix_ns, 2_000, "updated_unix_ns refreshed");
    }

    /// A concurrent update racing the same tenant: the first CAS wins, the
    /// second reads the same version and its stale CAS write is rejected as a
    /// typed `CasConflict`, never silently overwriting the winner. Proven with
    /// `FaultStore` turning the loser's CAS `put` into a `PreconditionFailed`,
    /// exactly what a concurrent update that moved the version would cause.
    #[tokio::test]
    async fn cas_conflict_on_concurrent_update() {
        let inner = MemoryStore::new();
        // Seed an existing record so set_tenant_config takes the CasVersion path.
        let seed = build_record(
            &tenant(),
            &TenantConfig::new(TenantLifecycleState::Active),
            1,
            1,
        );
        inner
            .put(
                &config_key(&tenant()),
                seed.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed record");

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/config"),
        );
        let store = FaultStore::new(inner, plan);

        let err = set_tenant_config(
            &store,
            &tenant(),
            &TenantConfig::new(TenantLifecycleState::Suspended),
            2,
        )
        .await
        .expect_err("a CAS precondition failure must surface as a typed conflict");
        assert!(
            matches!(err, TenantConfigError::CasConflict { .. }),
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

    /// A concurrent create race: two writers both see no record, one wins the
    /// CreateIfAbsent, the loser's CreateIfAbsent returns AlreadyExists and
    /// surfaces as a typed conflict, never a silent overwrite.
    #[tokio::test]
    async fn cas_conflict_on_concurrent_create() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/config"),
        );
        let store = FaultStore::new(inner, plan);
        let err = set_tenant_config(
            &store,
            &tenant(),
            &TenantConfig::new(TenantLifecycleState::Active),
            1,
        )
        .await
        .expect_err("a losing CreateIfAbsent race must surface as a typed conflict");
        assert!(
            matches!(err, TenantConfigError::CasConflict { .. }),
            "got: {err}"
        );
    }

    /// A record misfiled under another tenant's `config` key is refused rather
    /// than read as this tenant's config.
    #[tokio::test]
    async fn read_rejects_misfiled_tenant() {
        let store = mem();
        let other = TenantHash([0xEE; 16]);
        let body = build_record(
            &other,
            &TenantConfig::new(TenantLifecycleState::Active),
            0,
            0,
        )
        .encode_to_vec();
        // Write `other`'s record under `tenant()`'s key: a misfile.
        store
            .put(&config_key(&tenant()), body.into(), PutOptions::default())
            .await
            .expect("put misfiled record");
        let err = read_config(store.as_ref(), &tenant())
            .await
            .expect_err("a record for a different tenant must be refused");
        assert!(matches!(
            err,
            TenantConfigError::CorruptRecord {
                field: "tenant_hash",
                ..
            }
        ));
    }

    /// A future format_version is refused rather than misread under this layout.
    #[tokio::test]
    async fn read_rejects_future_format_version() {
        let store = mem();
        let mut record = build_record(
            &tenant(),
            &TenantConfig::new(TenantLifecycleState::Active),
            0,
            0,
        );
        record.format_version = TENANT_CONFIG_FORMAT_VERSION + 1;
        store
            .put(
                &config_key(&tenant()),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("put future record");
        let err = read_config(store.as_ref(), &tenant())
            .await
            .expect_err("a future format_version must be refused");
        assert!(matches!(err, TenantConfigError::UnsupportedVersion { .. }));
    }

    /// An UNSPECIFIED (0) lifecycle state is a corrupt record refused on decode,
    /// never defaulted to active.
    #[tokio::test]
    async fn read_rejects_unspecified_lifecycle_state() {
        let store = mem();
        let mut record = build_record(
            &tenant(),
            &TenantConfig::new(TenantLifecycleState::Active),
            0,
            0,
        );
        record.lifecycle_state = sysproto::TenantLifecycleState::Unspecified as i32;
        store
            .put(
                &config_key(&tenant()),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("put record with unspecified lifecycle");
        let err = read_config(store.as_ref(), &tenant())
            .await
            .expect_err("an unspecified lifecycle state must be refused");
        assert!(matches!(
            err,
            TenantConfigError::InvalidLifecycleState { got: 0, .. }
        ));
    }

    /// A corrupt (undecodable) record body is a typed `Decode` error, never a
    /// panic.
    #[tokio::test]
    async fn corrupt_record_is_typed_decode_error() {
        let store = mem();
        store
            .put(
                &config_key(&tenant()),
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage");
        let err = read_config(store.as_ref(), &tenant())
            .await
            .expect_err("garbage must be a typed decode error, not a panic");
        assert!(
            matches!(err, TenantConfigError::Decode { .. }),
            "got: {err}"
        );
    }
}

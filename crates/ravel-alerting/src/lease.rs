//! The per-tenant alert lease object, `t/<tenant_hash>/a/alert-lease`, and its
//! parked form (ADR-4233 decision 4, "Before the baseline, alerts").
//!
//! The lease is small JSON coordination state, not a frozen persistent format.
//! A lease is *parked* when its `expiry_ns` is `i64::MAX`: a pre-baseline
//! evaluator reads that as held by someone else forever and stops writing,
//! while a baseline-or-later evaluator reads `compat_expiry_ns` as the real
//! expiry. `compat activate` parks every tenant's lease with
//! [`park_lease`] when `rlog` is activated above the baseline; every parked
//! lease carries the `compat_epoch` it was parked at.
//!
//! The two compat fields default to 0 when absent, so a lease written by a
//! build that predates them still parses, and a build that predates them
//! ignores them when it reads a lease this build wrote.

use bytes::Bytes;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError};
use ravel_types::TenantHash;
use serde::{Deserialize, Serialize};

/// The `expiry_ns` of a parked lease.
pub const PARKED_EXPIRY_NS: i64 = i64::MAX;

/// The holder prefix [`park_lease`] writes: `compat-fence/<epoch>`.
pub const COMPAT_FENCE_HOLDER_PREFIX: &str = "compat-fence/";

/// How many lost CAS races [`park_lease`] retries before it reports
/// [`LeaseError::Contended`].
pub const PARK_LEASE_MAX_ATTEMPTS: u32 = 8;

/// The lease object key for a tenant's alert evaluation. Under the tenant's
/// alert keyspace but outside the `c/<shard>/` commit prefix the history fold
/// lists.
pub fn alert_lease_key(tenant: &TenantHash) -> String {
    format!("t/{}/a/alert-lease", tenant.to_hex())
}

/// The holder [`park_lease`] writes for `compat_epoch`.
pub fn compat_fence_holder(compat_epoch: u64) -> String {
    format!("{COMPAT_FENCE_HOLDER_PREFIX}{compat_epoch}")
}

/// The alert lease object body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertLease {
    /// The holding evaluator's `writer_id`, or `compat-fence/<epoch>` for a
    /// lease [`park_lease`] wrote.
    pub holder: String,
    /// Wall-clock nanosecond at or after which any evaluator may take the
    /// lease over. [`PARKED_EXPIRY_NS`] on a parked lease.
    pub expiry_ns: i64,
    /// The real expiry of a parked lease; 0 on a lease nobody holds yet and on
    /// an unparked one.
    #[serde(default)]
    pub compat_expiry_ns: i64,
    /// The `sys/compat` epoch the writer had seen when it wrote the lease; 0
    /// from a build that predates the field.
    #[serde(default)]
    pub compat_epoch: u64,
}

impl AlertLease {
    /// An unparked lease held by `holder` until `expiry_ns`.
    pub fn unparked(holder: String, expiry_ns: i64, compat_epoch: u64) -> AlertLease {
        AlertLease {
            holder,
            expiry_ns,
            compat_expiry_ns: 0,
            compat_epoch,
        }
    }

    /// A parked lease held by `holder` until `compat_expiry_ns`.
    pub fn parked(holder: String, compat_expiry_ns: i64, compat_epoch: u64) -> AlertLease {
        AlertLease {
            holder,
            expiry_ns: PARKED_EXPIRY_NS,
            compat_expiry_ns,
            compat_epoch,
        }
    }

    /// The lease [`park_lease`] writes: held by `compat-fence/<epoch>`,
    /// expired for any baseline-or-later evaluator.
    pub fn fence(compat_epoch: u64) -> AlertLease {
        AlertLease::parked(compat_fence_holder(compat_epoch), 0, compat_epoch)
    }

    /// Whether the lease is in the parked form.
    pub fn is_parked(&self) -> bool {
        self.expiry_ns == PARKED_EXPIRY_NS
    }

    /// The expiry a baseline-or-later evaluator honours: `compat_expiry_ns`
    /// on a parked lease, `expiry_ns` otherwise.
    pub fn effective_expiry_ns(&self) -> i64 {
        if self.is_parked() {
            self.compat_expiry_ns
        } else {
            self.expiry_ns
        }
    }

    /// The JSON body.
    pub fn to_bytes(&self) -> Result<Bytes, LeaseError> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|source| LeaseError::Encode { source })
    }

    /// Parse a lease body.
    pub fn from_slice(bytes: &[u8]) -> Result<AlertLease, LeaseError> {
        serde_json::from_slice(bytes).map_err(|source| LeaseError::Decode { source })
    }
}

/// A failure reading or writing an alert lease.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("alert lease object store: {0}")]
    Store(#[from] StoreError),
    #[error("alert lease decode: {source}")]
    Decode {
        #[source]
        source: serde_json::Error,
    },
    #[error("alert lease encode: {source}")]
    Encode {
        #[source]
        source: serde_json::Error,
    },
    /// Every attempt lost a CAS race to another writer of the same lease.
    #[error("alert lease {key}: {attempts} attempts each lost a CAS race")]
    Contended { key: String, attempts: u32 },
}

/// What [`park_lease`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParkOutcome {
    /// No lease existed; a parked one was created.
    Created,
    /// An existing lease was replaced by a parked one, whoever held it.
    Replaced,
    /// The lease was already parked at this epoch or a later one and was left
    /// alone, so a capable evaluator that took it over keeps it.
    AlreadyParked { compat_epoch: u64 },
}

/// Park `tenant`'s alert lease at `compat_epoch` (ADR-4233 decision 4): an
/// absent lease is created with `CreateIfAbsent`, an existing one is replaced
/// with a CAS on the version read, whoever holds it, in both cases with holder
/// `compat-fence/<epoch>`, `expiry_ns = i64::MAX`, `compat_expiry_ns = 0` and
/// `compat_epoch`. A lease already parked at `compat_epoch` or later is left
/// alone, so the walk is resumable. A body that does not parse is replaced: a
/// parked lease is the safe direction. A lost race is retried up to
/// [`PARK_LEASE_MAX_ATTEMPTS`] times.
pub async fn park_lease(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    compat_epoch: u64,
) -> Result<ParkOutcome, LeaseError> {
    let key = alert_lease_key(tenant);
    let body = AlertLease::fence(compat_epoch).to_bytes()?;
    for _ in 0..PARK_LEASE_MAX_ATTEMPTS {
        let current = match store.get(&key, GetRange::Full).await {
            Ok(current) => current,
            Err(StoreError::NotFound) => {
                match store
                    .put(&key, body.clone(), PutOptions::create_if_absent())
                    .await
                {
                    Ok(_) => return Ok(ParkOutcome::Created),
                    Err(StoreError::AlreadyExists) => continue,
                    Err(err) => return Err(err.into()),
                }
            }
            Err(err) => return Err(err.into()),
        };
        if let Ok(existing) = AlertLease::from_slice(&current.data)
            && existing.is_parked()
            && existing.compat_epoch >= compat_epoch
        {
            return Ok(ParkOutcome::AlreadyParked {
                compat_epoch: existing.compat_epoch,
            });
        }
        let opts = PutOptions {
            mode: PutMode::CasVersion(current.version),
            checksum: None,
        };
        match store.put(&key, body.clone(), opts).await {
            Ok(_) => return Ok(ParkOutcome::Replaced),
            Err(StoreError::PreconditionFailed) => continue,
            Err(err) => return Err(err.into()),
        }
    }
    Err(LeaseError::Contended {
        key,
        attempts: PARK_LEASE_MAX_ATTEMPTS,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    fn tenant() -> TenantHash {
        TenantHash([0x4a; 16])
    }

    async fn read(store: &dyn ObjectStoreBackend) -> AlertLease {
        let got = store
            .get(&alert_lease_key(&tenant()), GetRange::Full)
            .await
            .expect("lease present");
        AlertLease::from_slice(&got.data).expect("lease parses")
    }

    #[test]
    fn a_lease_without_the_compat_fields_parses_with_them_at_zero() {
        // The body v0.33.0's `acquire_lease` writes.
        let lease = AlertLease::from_slice(
            br#"{"holder":"6f1c2a7e-0000-4000-8000-000000000001","expiry_ns":1800000000000000000}"#,
        )
        .expect("a v0.33-shaped lease parses");
        assert_eq!(
            lease,
            AlertLease {
                holder: "6f1c2a7e-0000-4000-8000-000000000001".to_string(),
                expiry_ns: 1_800_000_000_000_000_000,
                compat_expiry_ns: 0,
                compat_epoch: 0,
            }
        );
        assert!(!lease.is_parked());
        assert_eq!(lease.effective_expiry_ns(), 1_800_000_000_000_000_000);
    }

    #[test]
    fn a_parked_lease_reads_as_expired_only_through_compat_expiry() {
        let parked = AlertLease::parked("me".to_string(), 500, 3);
        assert!(parked.is_parked());
        assert_eq!(parked.expiry_ns, i64::MAX);
        assert_eq!(parked.effective_expiry_ns(), 500);

        // A build that predates the compat fields reads only `holder` and
        // `expiry_ns`, and sees a lease held until the end of time.
        #[derive(Deserialize)]
        struct V033Lease {
            holder: String,
            expiry_ns: i64,
        }
        let old: V033Lease =
            serde_json::from_slice(&parked.to_bytes().expect("encode")).expect("v0.33 reads it");
        assert_eq!(old.holder, "me");
        assert_eq!(old.expiry_ns, i64::MAX);
    }

    #[tokio::test]
    async fn park_lease_creates_an_absent_lease() {
        let store = MemoryStore::new();
        let out = park_lease(&store, &tenant(), 4).await.expect("park");
        assert_eq!(out, ParkOutcome::Created);
        assert_eq!(read(&store).await, AlertLease::fence(4));
        assert_eq!(
            read(&store).await,
            AlertLease {
                holder: "compat-fence/4".to_string(),
                expiry_ns: i64::MAX,
                compat_expiry_ns: 0,
                compat_epoch: 4,
            }
        );
    }

    #[tokio::test]
    async fn park_lease_replaces_a_live_lease_whoever_holds_it() {
        let store = MemoryStore::new();
        let live = AlertLease::unparked("evaluator-a".to_string(), i64::MAX - 1, 0);
        store
            .put(
                &alert_lease_key(&tenant()),
                live.to_bytes().expect("encode"),
                PutOptions::default(),
            )
            .await
            .expect("seed live lease");
        let out = park_lease(&store, &tenant(), 2).await.expect("park");
        assert_eq!(out, ParkOutcome::Replaced);
        assert_eq!(read(&store).await, AlertLease::fence(2));
    }

    #[tokio::test]
    async fn park_lease_leaves_a_lease_parked_at_the_same_or_later_epoch() {
        let store = MemoryStore::new();
        // A capable evaluator took the parked lease over and holds it parked.
        let held = AlertLease::parked("evaluator-b".to_string(), 9_000, 2);
        store
            .put(
                &alert_lease_key(&tenant()),
                held.to_bytes().expect("encode"),
                PutOptions::default(),
            )
            .await
            .expect("seed parked lease");
        let out = park_lease(&store, &tenant(), 2).await.expect("park");
        assert_eq!(out, ParkOutcome::AlreadyParked { compat_epoch: 2 });
        assert_eq!(read(&store).await, held);

        // A later activation re-parks it.
        let out = park_lease(&store, &tenant(), 3).await.expect("park");
        assert_eq!(out, ParkOutcome::Replaced);
        assert_eq!(read(&store).await, AlertLease::fence(3));
    }

    #[tokio::test]
    async fn park_lease_replaces_an_unparseable_body() {
        let store = MemoryStore::new();
        store
            .put(
                &alert_lease_key(&tenant()),
                Bytes::from_static(b"not json"),
                PutOptions::default(),
            )
            .await
            .expect("seed");
        let out = park_lease(&store, &tenant(), 1).await.expect("park");
        assert_eq!(out, ParkOutcome::Replaced);
        assert_eq!(read(&store).await, AlertLease::fence(1));
    }
}

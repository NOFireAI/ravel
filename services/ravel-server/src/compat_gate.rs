//! Retired-reader startup gate (ADR-4233 decision 7).
//!
//! A release whose reader for a format family no longer admits a version
//! reads `sys/compat` once at startup, before any listener binds or any
//! write, and refuses to start in every mode unless that family carries a
//! certificate at or above the oldest version the release reads, the
//! certificate is not contradicted, and the late-write watermark has been
//! advanced past the census start `S`.
//!
//! `S` is not stored in the certificate. `certify` sets the watermark to `S`
//! in the same CAS that writes the certificate, and `S` precedes
//! `certified_unix_ns`, so the gate takes a watermark above
//! `certified_unix_ns` as proof that a late-write pass started after the
//! certificate was written and finished: a stricter test than "past `S`" that
//! needs no new field.
//!
//! The gate is a no-op on a build that retires no reader: [`RETIRED_READERS`]
//! is empty today and [`enforce`] sends no request for an empty set. A store
//! with neither `sys/compat` nor `sys/tenancy` and no `t/` data is fresh and
//! starts; one with `sys/tenancy` and no `sys/compat`, or with `t/` data and
//! neither marker (a pre-ADR-0050 bucket), is refused, which is how an
//! unsupported upgrade path (skipping the release that carried both readers)
//! is refused before it modifies anything.
//!
//! [`enforce`] itself is read-only. On a fresh store with a deployment key,
//! startup follows it with [`bootstrap_fresh_store`], the one write this
//! module makes: a create-only `sys/compat`, before `sys/tenancy` or anything
//! else is written. An unkeyed fresh store gets no `sys/compat`, so the
//! next start of the same release is refused with `sys/tenancy` and no
//! `sys/compat`.

use std::collections::BTreeMap;

use ravel_catalog::compat::{
    COMPAT_KEY, CompatError, CompatKeys, create_compat, finalization_pending, read_compat,
};
use ravel_maintain::DEFAULT_PUBLISH_LIFETIME_NS;
use ravel_maintain::compat_init::{CompatInitError, InitConfig, baseline_record};
use ravel_object_store::{ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;

use crate::tenancy::{TENANCY_MARKER_KEY, TENANT_PREFIX, bucket_has_tenant_data};

/// A family whose reader this build retired: it reads no version below
/// `oldest_readable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredReader {
    pub family: &'static str,
    pub oldest_readable: u32,
}

/// The readers this build retired. Empty: every reader since the baseline
/// still admits every version it ever wrote.
pub const RETIRED_READERS: &[RetiredReader] = &[];

/// A watermark older than this only warns (ADR-4233 decision 7), so a long
/// maintenance outage does not stop processes from restarting.
pub const WATERMARK_WARN_AGE_NS: i64 = 7 * 24 * 3_600 * 1_000_000_000;

/// The baseline release a fresh-store bootstrap records: v0.34, the default
/// of `ravel-cli compat init`.
pub const BOOTSTRAP_BASELINE_RELEASE: (u32, u32) = (0, 34);

/// Why the gate let the process start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    /// This build retires no reader; nothing was read.
    NoRetiredReader,
    /// Neither `sys/compat` nor `sys/tenancy` exists and the store holds no
    /// `t/` data: a fresh store. With a deployment key, startup then runs
    /// [`bootstrap_fresh_store`] before any other write, so the next start
    /// finds a certified `sys/compat`; without one it writes nothing here.
    /// Only a build with a non-empty [`RETIRED_READERS`] reaches this
    /// outcome.
    Fresh,
    /// Every retired family carries a current certificate.
    Certified { families: Vec<CertifiedFamily> },
}

/// One retired family's certificate state at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertifiedFamily {
    pub family: &'static str,
    pub certificate_version: u32,
    pub watermark_unix_ns: i64,
    /// The watermark is older than [`WATERMARK_WARN_AGE_NS`]: logged as a
    /// warning, not refused.
    pub watermark_stale: bool,
}

/// A refusal to start. Every variant names the family it concerns, when one
/// applies, and the step that clears it.
#[derive(Debug, thiserror::Error)]
pub enum CompatGateError {
    #[error(
        "{TENANCY_MARKER_KEY} exists but {COMPAT_KEY} does not: this release retired a reader, \
         so it starts only on a store adopted into the compatibility contract; run \
         `ravel-cli compat init` from the release that reads both versions, then migrate and \
         certify (ADR-4233 decision 7)"
    )]
    TenancyWithoutCompat,
    #[error(
        "the store holds {TENANT_PREFIX} data but neither {TENANCY_MARKER_KEY} nor {COMPAT_KEY}: \
         this release retired a reader, so it starts only on a store adopted into the \
         compatibility contract; run `ravel-cli compat init` from the release that reads both \
         versions, then migrate and certify (ADR-4233 decision 7)"
    )]
    TenantDataWithoutMarkers,
    #[error(
        "{COMPAT_KEY} exists but no deployment key (--tenant-hash-key-file) is configured, so \
         its certificates cannot be authenticated; this release retired a reader and refuses \
         to start without them"
    )]
    Unkeyed,
    #[error("could not check {key} for the retired-reader gate: {source}")]
    Store {
        key: &'static str,
        #[source]
        source: StoreError,
    },
    #[error("could not read {COMPAT_KEY} for the retired-reader gate: {0}")]
    CompatUnreadable(#[source] CompatError),
    #[error(
        "family {family}: {COMPAT_KEY} has no entry, so no certificate; run `ravel-cli compat \
         certify` from the release that reads both versions"
    )]
    FamilyAbsent { family: &'static str },
    #[error(
        "family {family}: no certificate; this release reads only version {oldest_readable} \
         and above, so run `ravel-cli compat certify` from the release that reads both versions"
    )]
    CertificateAbsent {
        family: &'static str,
        oldest_readable: u32,
    },
    #[error(
        "family {family}: certificate at version {certified} is below {oldest_readable}, the \
         oldest version this release reads; migrate and re-certify from the release that \
         reads both versions"
    )]
    CertificateBelow {
        family: &'static str,
        certified: u32,
        oldest_readable: u32,
    },
    #[error(
        "family {family}: certificate is contradicted (a late write below version {certified} \
         was found); roll back to the release that reads both versions, migrate, and \
         re-certify"
    )]
    Contradicted {
        family: &'static str,
        certified: u32,
    },
    #[error(
        "family {family}: the late-write watermark ({watermark_unix_ns}) was never advanced \
         past the census (certified at {certified_unix_ns}); run `ravel-cli compat verify` \
         from the release that reads both versions"
    )]
    WatermarkNotPastCensus {
        family: &'static str,
        watermark_unix_ns: i64,
        certified_unix_ns: i64,
    },
    #[error("could not build {COMPAT_KEY} for a fresh store: {0}")]
    BootstrapRecord(#[source] CompatInitError),
    #[error("could not create {COMPAT_KEY} on a fresh store: {0}")]
    BootstrapCreate(#[source] CompatError),
    #[error(
        "{COMPAT_KEY} existed when this process tried to create it on a fresh store, and was \
         gone when it re-read it; refusing to start"
    )]
    BootstrapVanished,
}

/// Run the gate for `retired` against `store`. Read-only. `deployment_key` is
/// the configured `--tenant-hash-key-file` key, which authenticates
/// `sys/compat`.
pub async fn enforce(
    store: &dyn ObjectStoreBackend,
    deployment_key: Option<&[u8; 32]>,
    retired: &[RetiredReader],
    now_unix_ns: i64,
) -> Result<GateOutcome, CompatGateError> {
    if retired.is_empty() {
        return Ok(GateOutcome::NoRetiredReader);
    }
    if !exists(store, COMPAT_KEY).await? {
        return without_compat(store).await;
    }
    let Some(key) = deployment_key else {
        return Err(CompatGateError::Unkeyed);
    };
    let keys = CompatKeys::from_file_keys(&[*key]).map_err(CompatGateError::CompatUnreadable)?;
    let Some((record, _)) = read_compat(store, &keys)
        .await
        .map_err(CompatGateError::CompatUnreadable)?
    else {
        // Deleted between the HEAD and the GET: the record is gone, and
        // fresh or refused is decided exactly as above.
        return without_compat(store).await;
    };
    check_record(&record, retired, now_unix_ns)
}

/// The per-family rules of the gate, applied to a `sys/compat` record.
fn check_record(
    record: &sysproto::CompatRecord,
    retired: &[RetiredReader],
    now_unix_ns: i64,
) -> Result<GateOutcome, CompatGateError> {
    let mut families = Vec::with_capacity(retired.len());
    for reader in retired {
        let family = reader.family;
        let Some(entry) = record.families.iter().find(|entry| entry.family == family) else {
            return Err(CompatGateError::FamilyAbsent { family });
        };
        let Some(certificate) = entry.certificate.as_ref() else {
            return Err(CompatGateError::CertificateAbsent {
                family,
                oldest_readable: reader.oldest_readable,
            });
        };
        if certificate.version < reader.oldest_readable {
            return Err(CompatGateError::CertificateBelow {
                family,
                certified: certificate.version,
                oldest_readable: reader.oldest_readable,
            });
        }
        if entry.contradicted {
            return Err(CompatGateError::Contradicted {
                family,
                certified: certificate.version,
            });
        }
        let watermark_unix_ns = entry.late_writes_verified_through_unix_ns;
        if finalization_pending(entry) {
            return Err(CompatGateError::WatermarkNotPastCensus {
                family,
                watermark_unix_ns,
                certified_unix_ns: certificate.certified_unix_ns,
            });
        }
        let watermark_stale = now_unix_ns.saturating_sub(watermark_unix_ns) > WATERMARK_WARN_AGE_NS;
        if watermark_stale {
            tracing::warn!(
                family,
                watermark_unix_ns,
                now_unix_ns,
                "the late-write watermark is older than 7 days: the daily late-write pass in \
                 `--mode maintain` has not completed recently; starting anyway (ADR-4233 \
                 decision 7)"
            );
        }
        families.push(CertifiedFamily {
            family,
            certificate_version: certificate.version,
            watermark_unix_ns,
            watermark_stale,
        });
    }
    Ok(GateOutcome::Certified { families })
}

/// What [`bootstrap_fresh_store`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bootstrap {
    /// This build retires no reader; nothing was read or written.
    NoRetiredReader,
    /// No deployment key is configured, so nothing can MAC `sys/compat`;
    /// nothing was written, exactly as before the bootstrap existed.
    Unkeyed,
    /// This process created `sys/compat`.
    Created,
    /// `sys/compat` already existed when the create ran (a concurrent fresh
    /// start, or an earlier attempt of this one whose acknowledgement was
    /// lost), and the record it found passes the gate.
    Existing,
}

/// Bring a store the gate found fresh into the compatibility contract, so
/// the next start of this release is not refused for lacking `sys/compat`.
///
/// Runs only for a non-empty `retired` set and only with a deployment key;
/// otherwise it sends no request. It must run before anything else writes to
/// the store, `sys/tenancy` included. The write is create-only: a lost race
/// leaves the existing record untouched, re-reads it, and starts only if the
/// gate accepts it. `census_unix_ns` is the time the gate took before the
/// `t/` LIST that found the store empty.
pub async fn bootstrap_fresh_store(
    store: &dyn ObjectStoreBackend,
    deployment_key: Option<&[u8; 32]>,
    retired: &[RetiredReader],
    census_unix_ns: i64,
) -> Result<Bootstrap, CompatGateError> {
    if retired.is_empty() {
        return Ok(Bootstrap::NoRetiredReader);
    }
    let Some(key) = deployment_key else {
        return Ok(Bootstrap::Unkeyed);
    };
    let record = bootstrap_record(retired, census_unix_ns)?;
    check_record(&record, retired, census_unix_ns)?;
    let keys = CompatKeys::from_file_keys(&[*key]).map_err(CompatGateError::BootstrapCreate)?;
    match create_compat(store, &keys, record).await {
        Ok(_) => Ok(Bootstrap::Created),
        Err(CompatError::AlreadyExists { .. }) => {
            let Some((existing, _)) = read_compat(store, &keys)
                .await
                .map_err(CompatGateError::CompatUnreadable)?
            else {
                return Err(CompatGateError::BootstrapVanished);
            };
            check_record(&existing, retired, census_unix_ns)?;
            Ok(Bootstrap::Existing)
        }
        Err(err) => Err(CompatGateError::BootstrapCreate(err)),
    }
}

/// The record a fresh-store bootstrap creates: the record `compat init`
/// writes on an empty store (every known family at its baseline write
/// version, `compat_epoch` 0, the baseline release, `P` at its default),
/// plus, for each retired family, what `compat certify` and a late-write pass
/// leave on an empty store. That is a certificate at the family's active
/// version over an empty census (no members, no buckets, the digest of the
/// empty tenant set) dated `census_unix_ns`, and a watermark one nanosecond
/// past it: the least the gate accepts, so the daily late-write pass in
/// `--mode maintain` checks every write after the empty LIST.
pub fn bootstrap_record(
    retired: &[RetiredReader],
    census_unix_ns: i64,
) -> Result<sysproto::CompatRecord, CompatGateError> {
    let config = InitConfig {
        baseline_release_major: BOOTSTRAP_BASELINE_RELEASE.0,
        baseline_release_minor: BOOTSTRAP_BASELINE_RELEASE.1,
        max_publish_lifetime_ns: DEFAULT_PUBLISH_LIFETIME_NS,
    };
    let mut record =
        baseline_record(&config, &BTreeMap::new()).map_err(CompatGateError::BootstrapRecord)?;
    let empty_tenant_set = blake3::Hasher::new().finalize().as_bytes().to_vec();
    for reader in retired {
        let family = reader.family;
        let Some(entry) = record
            .families
            .iter_mut()
            .find(|entry| entry.family == family)
        else {
            return Err(CompatGateError::FamilyAbsent { family });
        };
        entry.certificate = Some(sysproto::Certificate {
            version: entry.active_write_version,
            certified_unix_ns: census_unix_ns,
            member_count: 0,
            newest_created_unix_ns: 0,
            buckets_scanned: 0,
            tenant_set_digest: empty_tenant_set.clone(),
        });
        entry.late_writes_verified_through_unix_ns = census_unix_ns.saturating_add(1);
    }
    Ok(record)
}

/// The outcome for a store without `sys/compat`: fresh only when it also has
/// no `sys/tenancy` and no `t/` data.
async fn without_compat(store: &dyn ObjectStoreBackend) -> Result<GateOutcome, CompatGateError> {
    if exists(store, TENANCY_MARKER_KEY).await? {
        return Err(CompatGateError::TenancyWithoutCompat);
    }
    let has_tenant_data =
        bucket_has_tenant_data(store)
            .await
            .map_err(|source| CompatGateError::Store {
                key: TENANT_PREFIX,
                source,
            })?;
    if has_tenant_data {
        return Err(CompatGateError::TenantDataWithoutMarkers);
    }
    Ok(GateOutcome::Fresh)
}

async fn exists(
    store: &dyn ObjectStoreBackend,
    key: &'static str,
) -> Result<bool, CompatGateError> {
    match store.head(key).await {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound) => Ok(false),
        Err(source) => Err(CompatGateError::Store { key, source }),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ravel_catalog::compat::{COMPAT_FORMAT_VERSION, FAMILY_RLOG, FAMILY_RSEG, create_compat};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::sys::v1 as sysproto;

    const KEY: [u8; 32] = [0x2A; 32];
    const CERTIFIED_NS: i64 = 1_000_000_000_000;
    const NOW_NS: i64 = CERTIFIED_NS + 3_600_000_000_000;
    const RSEG_8: RetiredReader = RetiredReader {
        family: FAMILY_RSEG,
        oldest_readable: 8,
    };

    fn certificate(version: u32) -> sysproto::Certificate {
        sysproto::Certificate {
            version,
            certified_unix_ns: CERTIFIED_NS,
            member_count: 3,
            ..Default::default()
        }
    }

    fn entry(
        certificate: Option<sysproto::Certificate>,
        watermark_unix_ns: i64,
        contradicted: bool,
    ) -> sysproto::CompatFamily {
        sysproto::CompatFamily {
            family: FAMILY_RSEG.to_string(),
            active_write_version: 8,
            certificate,
            late_writes_verified_through_unix_ns: watermark_unix_ns,
            contradicted,
            ..Default::default()
        }
    }

    async fn store_with(families: Vec<sysproto::CompatFamily>) -> MemoryStore {
        let store = MemoryStore::new();
        create_compat(
            &store,
            &CompatKeys::from_file_keys(&[KEY]).expect("key"),
            sysproto::CompatRecord {
                format_version: COMPAT_FORMAT_VERSION,
                families,
                ..Default::default()
            },
        )
        .await
        .expect("create sys/compat");
        put_tenancy(&store).await;
        store
    }

    async fn put_tenancy(store: &MemoryStore) {
        store
            .put(
                TENANCY_MARKER_KEY,
                Bytes::from_static(b"marker"),
                PutOptions::default(),
            )
            .await
            .expect("put sys/tenancy");
    }

    async fn gate(store: &MemoryStore) -> Result<GateOutcome, CompatGateError> {
        enforce(store, Some(&KEY), &[RSEG_8], NOW_NS).await
    }

    /// The ADR-4233 decision 7 table row: no, stale, contradicted, or
    /// unverified certificate refuses; a `Current` one starts.
    #[tokio::test]
    async fn retired_reader_refuses_without_current_certificate() {
        let past = CERTIFIED_NS + 1;

        let no_certificate = store_with(vec![entry(None, past, false)]).await;
        assert!(
            matches!(
                gate(&no_certificate).await,
                Err(CompatGateError::CertificateAbsent {
                    family: FAMILY_RSEG,
                    oldest_readable: 8
                })
            ),
            "no certificate"
        );

        let stale = store_with(vec![entry(Some(certificate(7)), past, false)]).await;
        assert!(
            matches!(
                gate(&stale).await,
                Err(CompatGateError::CertificateBelow {
                    certified: 7,
                    oldest_readable: 8,
                    ..
                })
            ),
            "a certificate below the oldest readable version"
        );

        let contradicted = store_with(vec![entry(Some(certificate(8)), past, true)]).await;
        assert!(
            matches!(
                gate(&contradicted).await,
                Err(CompatGateError::Contradicted { certified: 8, .. })
            ),
            "a contradicted certificate"
        );

        // certify leaves the watermark at S, before certified_unix_ns.
        let unverified =
            store_with(vec![entry(Some(certificate(8)), CERTIFIED_NS - 1, false)]).await;
        assert!(
            matches!(
                gate(&unverified).await,
                Err(CompatGateError::WatermarkNotPastCensus {
                    watermark_unix_ns,
                    certified_unix_ns: CERTIFIED_NS,
                    ..
                }) if watermark_unix_ns == CERTIFIED_NS - 1
            ),
            "a watermark never advanced past the census"
        );
        let at_census = store_with(vec![entry(Some(certificate(8)), CERTIFIED_NS, false)]).await;
        assert!(
            matches!(
                gate(&at_census).await,
                Err(CompatGateError::WatermarkNotPastCensus { .. })
            ),
            "a watermark equal to certified_unix_ns is not past it"
        );

        let missing_family = store_with(vec![sysproto::CompatFamily {
            family: FAMILY_RLOG.to_string(),
            certificate: Some(certificate(8)),
            late_writes_verified_through_unix_ns: past,
            ..Default::default()
        }])
        .await;
        assert!(
            matches!(
                gate(&missing_family).await,
                Err(CompatGateError::FamilyAbsent {
                    family: FAMILY_RSEG
                })
            ),
            "another family's certificate does not certify this one"
        );

        // A certificate above the oldest readable version also covers it.
        for version in [8, 9] {
            let current = store_with(vec![entry(Some(certificate(version)), past, false)]).await;
            assert_eq!(
                gate(&current).await.expect("a current certificate starts"),
                GateOutcome::Certified {
                    families: vec![CertifiedFamily {
                        family: FAMILY_RSEG,
                        certificate_version: version,
                        watermark_unix_ns: past,
                        watermark_stale: false,
                    }]
                }
            );
        }
    }

    /// `finalization_pending`, which `compat status`, `compat verify` and the
    /// maintain job report, is true exactly when the gate refuses a current
    /// certificate for its watermark: at, one below and one past
    /// `certified_unix_ns`, contradicted or not.
    #[tokio::test]
    async fn finalization_pending_is_the_gate_watermark_refusal() {
        for watermark in [CERTIFIED_NS - 1, CERTIFIED_NS, CERTIFIED_NS + 1] {
            for contradicted in [false, true] {
                let family = entry(Some(certificate(8)), watermark, contradicted);
                let pending = finalization_pending(&family);
                let refused = matches!(
                    gate(&store_with(vec![family]).await).await,
                    Err(CompatGateError::WatermarkNotPastCensus { .. })
                );
                assert_eq!(
                    pending, refused,
                    "watermark {watermark} contradicted {contradicted}"
                );
                assert_eq!(
                    pending,
                    !contradicted && watermark <= CERTIFIED_NS,
                    "watermark {watermark} contradicted {contradicted}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_week_old_watermark_warns_and_starts() {
        let watermark = CERTIFIED_NS + 1;
        let store = store_with(vec![entry(Some(certificate(8)), watermark, false)]).await;
        let outcome = enforce(
            &store,
            Some(&KEY),
            &[RSEG_8],
            watermark + WATERMARK_WARN_AGE_NS + 1,
        )
        .await
        .expect("a stale watermark only warns");
        let GateOutcome::Certified { families } = outcome else {
            panic!("expected certified");
        };
        assert!(families[0].watermark_stale);
        let at_bound = enforce(
            &store,
            Some(&KEY),
            &[RSEG_8],
            watermark + WATERMARK_WARN_AGE_NS,
        )
        .await
        .expect("starts");
        let GateOutcome::Certified { families } = at_bound else {
            panic!("expected certified");
        };
        assert!(!families[0].watermark_stale);
    }

    #[tokio::test]
    async fn an_empty_bucket_starts() {
        let store = MemoryStore::new();
        assert_eq!(gate(&store).await.expect("fresh"), GateOutcome::Fresh);
        // A fresh store needs no key: nothing exists to authenticate.
        assert_eq!(
            enforce(&store, None, &[RSEG_8], NOW_NS)
                .await
                .expect("fresh"),
            GateOutcome::Fresh
        );
    }

    #[tokio::test]
    async fn tenancy_without_compat_is_refused_naming_compat_init() {
        let store = MemoryStore::new();
        put_tenancy(&store).await;
        let err = gate(&store).await.expect_err("refused");
        assert!(
            matches!(err, CompatGateError::TenancyWithoutCompat),
            "{err:?}"
        );
        assert!(err.to_string().contains("`ravel-cli compat init`"), "{err}");
    }

    async fn every_object(store: &MemoryStore) -> Vec<(String, String, String)> {
        let mut objects = Vec::new();
        let mut page = None;
        loop {
            let listed = store.list("", page).await.expect("list");
            objects.extend(listed.objects.into_iter().map(|meta| {
                (
                    meta.key,
                    format!("{:?}", meta.etag),
                    format!("{:?}", meta.version),
                )
            }));
            match listed.next {
                Some(next) => page = Some(next),
                None => return objects,
            }
        }
    }

    /// A pre-ADR-0050 bucket (tenant data, no `sys/tenancy`, no `sys/compat`)
    /// is not fresh: a retired-reader release refuses it before writing
    /// anything, so its data is never read by a build that cannot read it.
    #[tokio::test]
    async fn a_bucket_with_tenant_data_and_no_markers_is_refused_by_a_retired_reader_release() {
        let store = MemoryStore::new();
        store
            .put(
                "t/acme/m/commits/00000001",
                Bytes::from_static(b"commit"),
                PutOptions::default(),
            )
            .await
            .expect("put tenant data");
        let before = every_object(&store).await;
        let err = gate(&store).await.expect_err("refused");
        assert!(
            matches!(err, CompatGateError::TenantDataWithoutMarkers),
            "{err:?}"
        );
        assert!(err.to_string().contains("`ravel-cli compat init`"), "{err}");
        assert_eq!(every_object(&store).await, before);
        assert_eq!(before.len(), 1);
    }

    /// A failed tenant-data LIST refuses rather than reading as an empty
    /// bucket.
    #[tokio::test]
    async fn a_failed_tenant_data_list_refuses() {
        let plan = FaultPlan {
            rules: vec![Rule::new(Op::List, ScriptedFault::Timeout)],
            ..FaultPlan::empty()
        };
        let store = FaultStore::new(MemoryStore::new(), plan);
        assert!(matches!(
            enforce(&store, Some(&KEY), &[RSEG_8], NOW_NS).await,
            Err(CompatGateError::Store {
                key: TENANT_PREFIX,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn an_unkeyed_process_cannot_authenticate_a_certificate() {
        let store = store_with(vec![entry(Some(certificate(8)), CERTIFIED_NS + 1, false)]).await;
        assert!(matches!(
            enforce(&store, None, &[RSEG_8], NOW_NS).await,
            Err(CompatGateError::Unkeyed)
        ));
        assert!(matches!(
            enforce(&store, Some(&[0x01; 32]), &[RSEG_8], NOW_NS).await,
            Err(CompatGateError::CompatUnreadable(
                CompatError::BadMac { .. }
            ))
        ));
    }

    /// A failed existence check refuses rather than reading as absent, which
    /// would start the release on an adopted store as if it were fresh.
    #[tokio::test]
    async fn a_failed_head_refuses() {
        let plan = FaultPlan {
            rules: vec![Rule::new(Op::Head, ScriptedFault::Timeout)],
            ..FaultPlan::empty()
        };
        let store = FaultStore::new(MemoryStore::new(), plan);
        assert!(matches!(
            enforce(&store, Some(&KEY), &[RSEG_8], NOW_NS).await,
            Err(CompatGateError::Store {
                key: COMPAT_KEY,
                ..
            })
        ));
    }

    /// Today's build retires no reader: the gate sends no request, so a store
    /// that fails every read, and one that would be refused, both start.
    #[tokio::test]
    async fn the_gate_is_a_no_op_without_a_retired_reader() {
        assert!(RETIRED_READERS.is_empty());
        let plan = FaultPlan {
            rules: vec![
                Rule::new(Op::Head, ScriptedFault::Timeout),
                Rule::new(Op::Get, ScriptedFault::Timeout),
            ],
            ..FaultPlan::empty()
        };
        let store = FaultStore::new(MemoryStore::new(), plan);
        put_tenancy(store.inner()).await;
        assert_eq!(
            enforce(&store, None, RETIRED_READERS, NOW_NS)
                .await
                .expect("no-op"),
            GateOutcome::NoRetiredReader
        );
        assert!(store.counters_snapshot().is_empty());
    }
}

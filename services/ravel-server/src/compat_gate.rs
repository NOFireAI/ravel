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
//! with neither `sys/compat` nor `sys/tenancy` is fresh and starts; one with
//! `sys/tenancy` and no `sys/compat` is refused, which is how an unsupported
//! upgrade path (skipping the release that carried both readers) is refused
//! before it modifies anything.

use ravel_catalog::compat::{
    COMPAT_KEY, CompatError, CompatKeys, finalization_pending, read_compat,
};
use ravel_object_store::{ObjectStoreBackend, StoreError};

use crate::tenancy::TENANCY_MARKER_KEY;

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

/// Why the gate let the process start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    /// This build retires no reader; nothing was read.
    NoRetiredReader,
    /// Neither `sys/compat` nor `sys/tenancy` exists: a fresh store, which
    /// this build bootstraps at its own versions.
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
        if exists(store, TENANCY_MARKER_KEY).await? {
            return Err(CompatGateError::TenancyWithoutCompat);
        }
        return Ok(GateOutcome::Fresh);
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
        // sys/tenancy decides fresh or refused exactly as above.
        if exists(store, TENANCY_MARKER_KEY).await? {
            return Err(CompatGateError::TenancyWithoutCompat);
        }
        return Ok(GateOutcome::Fresh);
    };

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

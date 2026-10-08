//! Deleting superseded manifest versions (ADR-2040, Lifecycle).
//!
//! Ravel stores no Parquet data of its own: the files a table names live in
//! the tenant's bucket, under an operator's grant, and a sweep does not
//! delete them. The only objects a sweep touches are manifest versions under
//! `t/<tenant_hash>/pq/t/`, which is also the only prefix it lists.
//!
//! The grace is measured from when a version stopped being its table's newest:
//! a manifest version other than the newest is deleted once the version that
//! superseded it (the next one present) is older than the grace plus
//! [`SKEW_MS`]. What that protects is a writer's resolve-to-put window. A
//! query reads a manifest when it resolves, except a Flight SQL `DoGet`, which
//! reads the version `GetFlightInfo` pinned up to the ticket's deadline later;
//! nothing ties that deadline to this grace, so a version swept in between
//! fails `DoGet` as an invalidated snapshot rather than reading another one.
//! For the writer: [`crate::writer`]
//! finishes a put within half of the `min_grace_ms` its caller passes, so
//! provided that value is no larger than the grace this sweep runs under,
//! every version committed after the writer's resolve is too young for a
//! sweep to free the key the put targets. [`plan`] only ever selects a version that has a
//! successor in the same listing, so it does not select a table's newest
//! version, dropped or live, which the next writer numbers from. Versions
//! above [`MAX_MANIFEST_VERSION`] are left out of that pairing, and so are
//! `.pqm` keys whose slot names no version, which the listing skips
//! and counts as [`crate::resolve::versions`] does: none is a table's newest,
//! so none counts as a successor, and none is deleted here. A `.pqm` key the
//! Query grant admits under a segment that is not a valid table name
//! ([`ListedManifestKey::InvalidTable`]) belongs to no table; the listing
//! skips and counts it as [`crate::resolve::tenant_listing`] does, and the
//! sweep never deletes it. On S3 one such key holding a control character, an
//! empty segment or a `.` or `..` segment fails the listing, and so the sweep,
//! with [`SweepError::Store`].
//!
//! A create-only object credential can put a forged version above a table's
//! newest, and once that aged past the grace the sweep would delete the real
//! versions under it. So a table's versions past the grace are deleted only
//! once the [`NewestGate`] admits its newest version (ADR-2430). In a keyed
//! deployment [`plan`] reads that version and requires a valid MAC under the
//! deployment's [`ManifestMacKey`]; a version 1 manifest has none, so until a
//! table's next DDL writes a version 2 one its predecessors are kept. An
//! unkeyed deployment can authenticate nothing and instead requires the
//! newest version to be older than [`UNKEYED_NEWEST_GRACE_MULTIPLE`] times
//! the grace. Either way the kept tables are reported in [`SweepPlan::held`].
//!
//! [`plan`] refuses a grace below the deployment's minimum (its
//! `--gc-max-query-duration`). Ages come from the store's
//! `last_modified_unix_ms`, which may have 1-second granularity and is
//! stamped by the store's clock while `now_ms` comes from the sweeper's, so
//! the margin [`SKEW_MS`] is added to the grace. The comparison is strict: a
//! version whose successor is exactly `grace_ms + SKEW_MS` old is kept.

use std::collections::{BTreeMap, BTreeSet};

use ravel_object_store::{GetRange, ObjectMeta, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, ListedManifestKey, MAX_MANIFEST_VERSION, parse_listed_manifest_key,
    tenant_manifest_prefix,
};
use crate::manifest::{MacStatus, ManifestMacKey, decode_authenticated};
use crate::resolve;

#[derive(Debug, thiserror::Error)]
pub enum SweepError {
    #[error("grace {grace_ms} ms is below the minimum {min_grace_ms} ms")]
    GraceBelowMinimum { grace_ms: u64, min_grace_ms: u64 },
    #[error("object store error on {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("unexpected object {key:?} under {prefix:?}: {reason}")]
    ForeignKey {
        key: String,
        prefix: String,
        reason: String,
    },
    #[error(transparent)]
    Key(#[from] KeyError),
}

/// Manifest keys a sweep will delete, sorted, and the tables whose superseded
/// versions the [`NewestGate`] kept, in table order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepPlan {
    pub manifest_deletes: Vec<String>,
    pub held: Vec<HeldTable>,
}

/// What [`execute`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub manifests_deleted: Vec<String>,
}

/// Clock-skew margin added to the grace: a superseded version is deleted only
/// once `now_ms - successor.last_modified_unix_ms > grace_ms + SKEW_MS`. The
/// same five minutes the catalog allows for clock skew.
pub const SKEW_MS: u64 = 300_000;

fn past_grace(now_ms: i64, last_modified_ms: i64, grace_ms: u64) -> bool {
    i128::from(now_ms) - i128::from(last_modified_ms) > i128::from(grace_ms) + i128::from(SKEW_MS)
}

fn foreign(key: &str, prefix: &str, reason: impl ToString) -> SweepError {
    SweepError::ForeignKey {
        key: key.to_string(),
        prefix: prefix.to_string(),
        reason: reason.to_string(),
    }
}

/// One table's manifest versions with their listing metadata.
type Versions = Vec<(u64, ObjectMeta)>;

/// Every manifest version of `tenant`, grouped by table, ascending. The only
/// LIST a sweep issues, and it is scoped to the manifest prefix.
async fn manifests_by_table(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<BTreeMap<String, Versions>, SweepError> {
    let prefix = tenant_manifest_prefix(tenant);
    let listed = list_all(store, &prefix)
        .await
        .map_err(|source| SweepError::Store {
            key: prefix.clone(),
            source,
        })?;
    // Per table: its versions, and the version characters of its keys that
    // name no version.
    let mut tables: BTreeMap<String, (Versions, Vec<String>)> = BTreeMap::new();
    let mut invalid_table_keys = Vec::new();
    for meta in listed {
        match parse_listed_manifest_key(&meta.key).map_err(|e| foreign(&meta.key, &prefix, e))? {
            ListedManifestKey::Version(parsed) => {
                if parsed.tenant_hash != *tenant {
                    return Err(foreign(&meta.key, &prefix, "belongs to another tenant"));
                }
                tables
                    .entry(parsed.table)
                    .or_default()
                    .0
                    .push((parsed.version, meta));
            }
            ListedManifestKey::InvalidVersion {
                tenant_hash,
                table,
                slot,
                ..
            } => {
                if tenant_hash != *tenant {
                    return Err(foreign(&meta.key, &prefix, "belongs to another tenant"));
                }
                tables.entry(table).or_default().1.push(slot);
            }
            ListedManifestKey::InvalidTable { tenant_hash, .. } => {
                if tenant_hash != *tenant {
                    return Err(foreign(&meta.key, &prefix, "belongs to another tenant"));
                }
                invalid_table_keys.push(meta.key);
            }
        }
    }
    invalid_table_keys.sort_unstable();
    invalid_table_keys.dedup();
    resolve::note_invalid_tables(tenant, &invalid_table_keys);
    Ok(tables
        .into_iter()
        .map(|(table, (mut versions, mut slots))| {
            versions.sort_by_key(|(v, _)| *v);
            versions.dedup_by_key(|(v, _)| *v);
            slots.sort_unstable();
            slots.dedup();
            let numbers: Vec<u64> = versions.iter().map(|(v, _)| *v).collect();
            resolve::note_unresolvable(tenant, &table, &numbers, &slots);
            (table, versions)
        })
        .collect())
}

/// What licenses deleting a table's predecessors: its newest version must
/// first be shown to be the DDL writer's (ADR-2430 decisions 4 and 7).
#[derive(Debug, Clone)]
pub enum NewestGate {
    /// Keyed deployment: the newest version must carry a valid MAC under this
    /// key. A version 1 newest, or a version 2 one with an absent or invalid
    /// MAC, holds every predecessor.
    Mac(ManifestMacKey),
    /// Unkeyed deployment, where nothing can be authenticated: the newest
    /// version must be older than `multiple` times the grace, plus
    /// [`SKEW_MS`], before any predecessor is deleted.
    UnkeyedGrace { multiple: u64 },
}

/// The grace multiple an unkeyed deployment's newest version must age past
/// before its predecessors go: a week at a one-hour grace, time for an
/// operator to find a forged version in the DDL audit log and remove it.
pub const UNKEYED_NEWEST_GRACE_MULTIPLE: u64 = 168;

impl NewestGate {
    /// The gate for a deployment holding `deployment_key`, or for an unkeyed
    /// one when it holds none.
    pub fn for_deployment(deployment_key: Option<&[u8; 32]>) -> Self {
        match deployment_key {
            Some(key) => NewestGate::Mac(ManifestMacKey::from_deployment_key(key)),
            None => NewestGate::UnkeyedGrace {
                multiple: UNKEYED_NEWEST_GRACE_MULTIPLE,
            },
        }
    }
}

/// Why a table's predecessors past the grace were all kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HoldReason {
    /// The newest version is version 1, which carries no MAC.
    Unversioned,
    /// The newest version is version 2 with no MAC.
    MacAbsent,
    /// The newest version's MAC is not the deployment key's.
    MacInvalid,
    /// The newest version could not be read or decoded.
    Unreadable(String),
    /// Unkeyed deployment: the newest version is younger than the longer
    /// grace.
    UnkeyedGrace,
}

/// One table whose superseded versions the gate kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldTable {
    pub table: String,
    /// The newest version, which the gate judged.
    pub newest: u64,
    /// How many versions past the grace were kept.
    pub held: usize,
    pub reason: HoldReason,
}

/// Why `gate` keeps the predecessors of `table`, whose newest bounded
/// version is `newest`, or `None` when it lets them go. Under
/// [`NewestGate::Mac`] this is the one GET a table with deletable
/// predecessors costs.
async fn hold_reason(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    newest: &(u64, ObjectMeta),
    now_ms: i64,
    grace_ms: u64,
    gate: &NewestGate,
) -> Result<Option<HoldReason>, SweepError> {
    let key = match gate {
        NewestGate::UnkeyedGrace { multiple } => {
            let long_grace = grace_ms.saturating_mul(*multiple);
            return Ok(
                (!past_grace(now_ms, newest.1.last_modified_unix_ms, long_grace))
                    .then_some(HoldReason::UnkeyedGrace),
            );
        }
        NewestGate::Mac(key) => key,
    };
    let bytes = match store.get(&newest.1.key, GetRange::Full).await {
        Ok(outcome) => outcome.data,
        // Swept or repaired since the listing: nothing to authenticate.
        Err(StoreError::NotFound) => {
            return Ok(Some(HoldReason::Unreadable("deleted since listed".into())));
        }
        Err(source) => {
            return Err(SweepError::Store {
                key: newest.1.key.clone(),
                source,
            });
        }
    };
    let reason = match decode_authenticated(&newest.1.key, &bytes, key) {
        Ok((_, MacStatus::Valid)) => None,
        Ok((_, MacStatus::Unversioned)) => Some(HoldReason::Unversioned),
        Ok((_, MacStatus::Absent)) => Some(HoldReason::MacAbsent),
        Ok((_, MacStatus::Invalid)) => Some(HoldReason::MacInvalid),
        // Anyone holding the Query credential can put a body that does not
        // decode, so it holds the table rather than failing the whole sweep.
        Err(e) => Some(HoldReason::Unreadable(e.to_string())),
    };
    if let Some(reason) = &reason {
        tracing::warn!(
            tenant = %tenant.to_hex(),
            table,
            newest = newest.0,
            ?reason,
            "parquet sweep: the newest manifest version is not authenticated, so its \
             predecessors are held"
        );
    }
    Ok(reason)
}

/// Decide what a sweep of `tenant` at `now_ms` deletes. Deletes nothing.
///
/// A table's versions past the grace are deleted only when `gate` admits its
/// newest version; otherwise they are all kept and the table is named in
/// [`SweepPlan::held`]. Under [`NewestGate::Mac`] the newest version of each
/// table with a version past the grace is read, one GET each.
pub async fn plan(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    now_ms: i64,
    grace_ms: u64,
    min_grace_ms: u64,
    gate: &NewestGate,
) -> Result<SweepPlan, SweepError> {
    if grace_ms < min_grace_ms {
        return Err(SweepError::GraceBelowMinimum {
            grace_ms,
            min_grace_ms,
        });
    }
    let tables = manifests_by_table(store, tenant).await?;
    let mut manifest_deletes = BTreeSet::new();
    let mut held = Vec::new();
    for (table, versions) in &tables {
        // A version above the bound is never a table's newest
        // (`resolve::newest`), so it supersedes nothing and is left for
        // `crate::repair`.
        let bounded = &versions[..versions.partition_point(|(v, _)| *v <= MAX_MANIFEST_VERSION)];
        let past: Vec<&str> = bounded
            .windows(2)
            .filter(|pair| past_grace(now_ms, pair[1].1.last_modified_unix_ms, grace_ms))
            .map(|pair| pair[0].1.key.as_str())
            .collect();
        let Some(newest) = bounded.last() else {
            continue;
        };
        if past.is_empty() {
            continue;
        }
        match hold_reason(store, tenant, table, newest, now_ms, grace_ms, gate).await? {
            None => manifest_deletes.extend(past.into_iter().map(str::to_string)),
            Some(reason) => held.push(HeldTable {
                table: table.clone(),
                newest: newest.0,
                held: past.len(),
                reason,
            }),
        }
    }
    Ok(SweepPlan {
        manifest_deletes: manifest_deletes.into_iter().collect(),
        held,
    })
}

/// Carry out `plan`. A delete that fails stops the sweep; the versions already
/// deleted are in the error-free part of a later plan, which recomputes from
/// what is left.
pub async fn execute(
    store: &dyn ObjectStoreBackend,
    plan: &SweepPlan,
) -> Result<SweepReport, SweepError> {
    let mut report = SweepReport::default();
    for key in &plan.manifest_deletes {
        store
            .delete(key)
            .await
            .map_err(|source| SweepError::Store {
                key: key.clone(),
                source,
            })?;
        report.manifests_deleted.push(key.clone());
    }
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use prost::Message;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::clock::FixedClock;
    use crate::keys::{grants_key, manifest_key};
    use crate::manifest::{MAC_FORMAT_VERSION, Manifest, encode_manifest, encode_manifest_with};
    use crate::resolve;
    use crate::test_util::{
        CountingStore, S3KeyStore, TENANT_A, TENANT_B, file_for, live_manifest, test_mac_key,
    };
    use crate::writer::{Intent, Outcome, apply, apply_stamped};

    const GRACE: u64 = 660_000;

    /// The keyed gate, under the key every keyed write in these tests MACs
    /// with.
    fn gate() -> NewestGate {
        NewestGate::Mac(test_mac_key())
    }

    /// `m` as a version 2 manifest of `tenant`, MACed under `key` (or carrying
    /// no MAC when `key` is `None`).
    fn encode_v2(tenant: &TenantHash, m: &Manifest, key: Option<&ManifestMacKey>) -> Vec<u8> {
        encode_manifest_with(tenant, m, MAC_FORMAT_VERSION, key).expect("encode")
    }

    /// A version 2 manifest MACed under the test key: what the writer puts
    /// once it stamps version 2.
    fn authentic(tenant: &TenantHash, m: &Manifest) -> Vec<u8> {
        encode_v2(tenant, m, Some(&test_mac_key()))
    }

    async fn put_bytes(store: &dyn ObjectStoreBackend, key: &str, bytes: Vec<u8>) {
        store
            .put(key, Bytes::from(bytes), PutOptions::create_if_absent())
            .await
            .expect("put");
    }

    fn create(seeds: &[u8]) -> Intent {
        Intent::CreateOrReplace {
            location: "s3://customer/data/".into(),
            grant: "s3://customer/data".into(),
            files: seeds.iter().map(|&s| file_for(s)).collect(),
            options: BTreeMap::new(),
            created_by: "t".into(),
            statement: "s".into(),
        }
    }

    fn drop_table() -> Intent {
        Intent::Drop {
            if_exists: false,
            created_by: "t".into(),
            statement: "DROP".into(),
        }
    }

    /// Apply `intent` through the writer's version 2 path, MACed under the
    /// test key.
    async fn commit(store: &dyn ObjectStoreBackend, table: &str, intent: Intent) -> u64 {
        let outcome = apply_stamped(
            store,
            &TENANT_A,
            table,
            intent,
            &FixedClock::new(0),
            GRACE,
            Some(&test_mac_key()),
            MAC_FORMAT_VERSION,
        )
        .await
        .expect("apply");
        match outcome {
            Outcome::Committed { version } => version,
            Outcome::NoOp => panic!("no-op"),
        }
    }

    fn mkey(table: &str, version: u64) -> String {
        manifest_key(&TENANT_A, table, version).expect("key")
    }

    #[tokio::test]
    async fn a_grace_below_the_minimum_is_refused() {
        let store = MemoryStore::new();
        let got = plan(&store, &TENANT_A, 0, GRACE - 1, GRACE, &gate()).await;
        assert!(matches!(
            got,
            Err(SweepError::GraceBelowMinimum { grace_ms, min_grace_ms })
                if grace_ms == GRACE - 1 && min_grace_ms == GRACE
        ));
        assert_eq!(
            plan(&store, &TENANT_A, 0, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            SweepPlan::default()
        );
    }

    #[tokio::test]
    async fn a_sweep_deletes_the_superseded_versions_and_keeps_every_newest() {
        let store = MemoryStore::with_page_size(2);
        // t = 0: "hits" v1.
        store.set_clock_ms(0);
        assert_eq!(commit(&store, "hits", create(&[1, 2])).await, 1);
        // t = 1000: "hits" v2 supersedes v1; "gone" is created and dropped.
        store.set_clock_ms(1_000);
        assert_eq!(commit(&store, "hits", create(&[3])).await, 2);
        assert_eq!(commit(&store, "gone", create(&[9])).await, 1);
        assert_eq!(commit(&store, "gone", drop_table()).await, 2);
        // Another tenant's superseded versions, which a tenant A sweep never
        // sees.
        apply(
            &store,
            &TENANT_B,
            "hits",
            create(&[1]),
            &FixedClock::new(0),
            GRACE,
            None,
        )
        .await
        .expect("tenant b");

        // At the grace plus the skew margin exactly: nothing goes.
        assert_eq!(
            plan(
                &store,
                &TENANT_A,
                (1_000 + GRACE + SKEW_MS) as i64,
                GRACE,
                GRACE,
                &gate()
            )
            .await
            .expect("plan"),
            SweepPlan::default()
        );

        // One millisecond later v1 of both tables is superseded past the
        // grace and the margin. The dropped table keeps its newest (dropped)
        // version.
        let now = (1_001 + GRACE + SKEW_MS) as i64;
        let mut manifest_deletes = vec![mkey("hits", 1), mkey("gone", 1)];
        manifest_deletes.sort();
        let got = plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
            .await
            .expect("plan");
        assert_eq!(
            got,
            SweepPlan {
                held: Vec::new(),
                manifest_deletes: manifest_deletes.clone()
            }
        );

        assert_eq!(
            execute(&store, &got).await.expect("execute"),
            SweepReport {
                manifests_deleted: manifest_deletes
            }
        );
        let mut remaining: Vec<String> = list_all(&store, "t/")
            .await
            .expect("list")
            .into_iter()
            .map(|m| m.key)
            .collect();
        remaining.sort();
        let mut expected = vec![
            mkey("hits", 2),
            mkey("gone", 2),
            manifest_key(&TENANT_B, "hits", 1).expect("key"),
        ];
        expected.sort();
        assert_eq!(remaining, expected);
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![2]
        );
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            SweepPlan::default()
        );
    }

    #[tokio::test]
    async fn a_version_above_the_bound_never_supersedes_the_newest() {
        // Without the bound, the forged versions are v1's successors, and v1
        // (the table's only legitimate version) is deleted once they age.
        let store = MemoryStore::new();
        store.set_clock_ms(0);
        assert_eq!(commit(&store, "hits", create(&[1])).await, 1);
        store.set_clock_ms(1_000);
        for v in [MAX_MANIFEST_VERSION + 1, u64::MAX] {
            let bytes =
                encode_manifest(&TENANT_A, &live_manifest("hits", v, &[9])).expect("encode");
            store
                .put(
                    &mkey("hits", v),
                    Bytes::from(bytes),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("put");
        }
        let now = (1_001 + GRACE + SKEW_MS) as i64;
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            SweepPlan::default()
        );

        // A legitimate successor still supersedes v1; the forged versions
        // are neither successors nor deleted.
        assert_eq!(commit(&store, "hits", create(&[2])).await, 2);
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            SweepPlan {
                held: Vec::new(),
                manifest_deletes: vec![mkey("hits", 1)]
            }
        );

        // A version exactly at the bound is inside it: it supersedes v2, and
        // as the newest it is kept.
        let bytes = authentic(
            &TENANT_A,
            &live_manifest("hits", MAX_MANIFEST_VERSION, &[3]),
        );
        store
            .put(
                &mkey("hits", MAX_MANIFEST_VERSION),
                Bytes::from(bytes),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put");
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            SweepPlan {
                held: Vec::new(),
                manifest_deletes: vec![mkey("hits", 1), mkey("hits", 2)]
            }
        );
    }

    #[tokio::test]
    async fn a_key_naming_no_version_is_skipped_by_the_sweep_listing_and_counted() {
        use crate::resolve::tests::{INVALID_SLOTS, capture_logs, put_invalid, warnings_naming};

        for (i, slot) in INVALID_SLOTS.iter().enumerate() {
            let tenant = TenantHash([0x47 + i as u8; 16]);
            let (logs, _guard) = capture_logs();
            let store = MemoryStore::with_page_size(2);
            store.set_clock_ms(0);
            for v in [1, 2] {
                let bytes = authentic(&tenant, &live_manifest("hits", v, &[1]));
                store
                    .put(
                        &manifest_key(&tenant, "hits", v).expect("key"),
                        Bytes::from(bytes),
                        PutOptions::create_if_absent(),
                    )
                    .await
                    .expect("put");
            }
            put_invalid(&store, &tenant, slot).await;
            let now = (1 + GRACE + SKEW_MS) as i64;
            for _ in 0..2 {
                assert_eq!(
                    plan(&store, &tenant, now, GRACE, GRACE, &gate())
                        .await
                        .expect("plan"),
                    SweepPlan {
                        held: Vec::new(),
                        manifest_deletes: vec![manifest_key(&tenant, "hits", 1).expect("key")]
                    },
                    "{slot}"
                );
            }
            assert_eq!(resolve::above_bound_resolves(&tenant, "hits"), 2, "{slot}");
            assert_eq!(warnings_naming(&logs, slot), 1, "{slot}");
        }
    }

    #[tokio::test]
    async fn a_nested_key_under_a_table_v_prefix_is_skipped_by_the_sweep() {
        use crate::resolve::tests::{capture_logs, put_invalid, warnings_naming};

        // The nested shape (`hits/v/q/v/<20>.pqm`) sits under the table's own
        // `v/` prefix, so the sweep listing skips and counts it instead of
        // failing, and still supersedes v1 with v2.
        const NESTED: &str = "q/v/00000000000000000001";
        const TENANT: TenantHash = TenantHash([0x5b; 16]);
        let (logs, _guard) = capture_logs();
        let store = MemoryStore::with_page_size(2);
        store.set_clock_ms(0);
        for v in [1, 2] {
            let bytes = authentic(&TENANT, &live_manifest("hits", v, &[1]));
            store
                .put(
                    &manifest_key(&TENANT, "hits", v).expect("key"),
                    Bytes::from(bytes),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("put");
        }
        put_invalid(&store, &TENANT, NESTED).await;
        let now = (1 + GRACE + SKEW_MS) as i64;
        assert_eq!(
            plan(&store, &TENANT, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            SweepPlan {
                held: Vec::new(),
                manifest_deletes: vec![manifest_key(&TENANT, "hits", 1).expect("key")]
            }
        );
        assert_eq!(resolve::above_bound_resolves(&TENANT, "hits"), 1);
        assert_eq!(warnings_naming(&logs, NESTED), 1);
    }

    #[tokio::test]
    async fn a_key_under_an_invalid_table_segment_is_skipped_by_the_sweep() {
        use crate::resolve::tests::{STRAY_SHAPES, capture_logs, put_stray, stray_warnings_naming};

        for (i, rest) in STRAY_SHAPES.iter().enumerate() {
            let tenant = TenantHash([0x63 + i as u8; 16]);
            let (logs, _guard) = capture_logs();
            let store = two_versions_behind_s3_keys(&tenant).await;
            let stray = put_stray(store.inner.inner(), &tenant, rest).await;
            let now = (1 + GRACE + SKEW_MS) as i64;
            for _ in 0..2 {
                let planned = plan(&store, &tenant, now, GRACE, GRACE, &gate())
                    .await
                    .expect("plan");
                assert_eq!(
                    planned,
                    SweepPlan {
                        held: Vec::new(),
                        manifest_deletes: vec![manifest_key(&tenant, "hits", 1).expect("key")]
                    },
                    "{rest}"
                );
            }
            assert_eq!(resolve::invalid_table_listings(&tenant), 2, "{rest}");
            assert_eq!(stray_warnings_naming(&logs, &stray), 1, "{rest}");
            let planned = plan(&store, &tenant, now, GRACE, GRACE, &gate())
                .await
                .expect("plan");
            execute(&store, &planned).await.expect("execute");
            assert_eq!(
                store.inner.metrics().snapshot().op(StoreOp::Delete).calls,
                1
            );
            store
                .head(&stray)
                .await
                .expect("the stray key is untouched");
        }
    }

    /// Versions 1 and 2 of `hits`, written at store time 0, behind the S3
    /// adapter's key handling.
    async fn two_versions_behind_s3_keys(
        tenant: &TenantHash,
    ) -> S3KeyStore<InstrumentedStore<MemoryStore>> {
        let store = S3KeyStore {
            inner: InstrumentedStore::new(MemoryStore::with_page_size(2)),
        };
        store.inner.inner().set_clock_ms(0);
        for v in [1, 2] {
            let bytes = authentic(tenant, &live_manifest("hits", v, &[1]));
            store
                .put(
                    &manifest_key(tenant, "hits", v).expect("key"),
                    Bytes::from(bytes),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("put");
        }
        store
    }

    /// On S3 a stray key `Path::parse` refuses fails the sweep's listing with
    /// the store error, and nothing is deleted.
    #[tokio::test]
    async fn a_stray_key_the_s3_adapter_cannot_list_fails_the_sweep() {
        use crate::resolve::tests::{UNLISTABLE_SHAPES, is_unparsed_listing, put_stray};

        for (i, rest) in UNLISTABLE_SHAPES.iter().enumerate() {
            let tenant = TenantHash([0x7a + i as u8; 16]);
            let store = two_versions_behind_s3_keys(&tenant).await;
            put_stray(store.inner.inner(), &tenant, rest).await;
            let now = (1 + GRACE + SKEW_MS) as i64;
            let got = plan(&store, &tenant, now, GRACE, GRACE, &gate()).await;
            assert!(
                matches!(&got, Err(SweepError::Store { key, source })
                    if *key == tenant_manifest_prefix(&tenant) && is_unparsed_listing(source)),
                "{rest:?}: {got:?}"
            );
            assert_eq!(resolve::invalid_table_listings(&tenant), 0, "{rest:?}");
            assert_eq!(
                store.inner.metrics().snapshot().op(StoreOp::Delete).calls,
                0
            );
        }
    }

    #[tokio::test]
    async fn a_key_under_an_invalid_table_segment_without_the_suffix_is_refused() {
        for rest in [
            "Hits/v/00000000000000000001.parquet",
            "a/b/v/00000000000000000001.pqm.tmp",
            "Hits/v/00000000000000000001",
            "Hits/v/0000000000000001.txt",
        ] {
            let store = MemoryStore::new();
            let junk = format!("{}{rest}", tenant_manifest_prefix(&TENANT_A));
            store
                .put(&junk, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
            assert!(
                matches!(
                    plan(&store, &TENANT_A, 0, GRACE, GRACE, &gate()).await,
                    Err(SweepError::ForeignKey { ref key, .. }) if *key == junk
                ),
                "{rest}"
            );
        }
    }

    #[tokio::test]
    async fn nothing_outside_the_manifest_prefix_is_listed_or_deleted() {
        let store = CountingStore::new(MemoryStore::new());
        store.inner.set_clock_ms(0);
        commit(&store, "hits", create(&[1])).await;
        commit(&store, "hits", create(&[2])).await;
        // A grants record and an object at the tenant root: neither is a
        // manifest, and a sweep must not even list them.
        let outside = [
            grants_key(&TENANT_A),
            format!("t/{}/pq/other", TENANT_A.to_hex()),
            "sys/pq-probe/abc".to_string(),
        ];
        for key in &outside {
            store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
        }
        let manifest_prefix = tenant_manifest_prefix(&TENANT_A);
        let before = store.list_count();
        let planned = plan(
            &store,
            &TENANT_A,
            (1 + GRACE + SKEW_MS) as i64,
            GRACE,
            GRACE,
            &gate(),
        )
        .await
        .expect("plan");
        assert_eq!(planned.manifest_deletes, vec![mkey("hits", 1)]);
        let listed = store.listed_prefixes();
        assert!(listed.len() > before);
        assert!(
            listed[before..].iter().all(|p| *p == manifest_prefix),
            "{listed:?}"
        );
        execute(&store, &planned).await.expect("execute");
        for key in &outside {
            store.head(key).await.expect("untouched");
        }
    }

    #[tokio::test]
    async fn a_delete_failure_stops_the_sweep_and_names_the_key() {
        let faults = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Delete, ScriptedFault::Permanent("down".into()))
                    .with_key_contains("/pq/t/"),
            ),
        );
        let store = faults.inner();
        store.set_clock_ms(0);
        commit(store, "hits", create(&[1])).await;
        commit(store, "hits", create(&[2])).await;
        let now = (1 + GRACE + SKEW_MS) as i64;
        let planned = plan(&faults, &TENANT_A, now, GRACE, GRACE, &gate())
            .await
            .expect("plan");
        assert_eq!(
            planned,
            SweepPlan {
                held: Vec::new(),
                manifest_deletes: vec![mkey("hits", 1)]
            }
        );
        let got = execute(&faults, &planned).await;
        assert!(
            matches!(&got, Err(SweepError::Store { key, .. }) if *key == mkey("hits", 1)),
            "{got:?}"
        );
        assert_eq!(
            faults.fault_count(Op::Delete, ravel_object_store::fault::FaultKind::Permanent),
            1
        );
        store.head(&mkey("hits", 1)).await.expect("still there");
        assert_eq!(
            plan(store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            planned
        );
    }

    #[tokio::test]
    async fn a_foreign_key_under_the_manifest_prefix_is_refused() {
        let store = MemoryStore::new();
        let junk = format!("{}readme.txt", tenant_manifest_prefix(&TENANT_A));
        store
            .put(&junk, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
        assert!(matches!(
            plan(&store, &TENANT_A, 0, GRACE, GRACE, &gate()).await,
            Err(SweepError::ForeignKey { key, .. }) if key == junk
        ));
    }

    fn get_calls(store: &InstrumentedStore<MemoryStore>) -> u64 {
        store.metrics().snapshot().op(StoreOp::Get).calls
    }

    fn held(newest: u64, count: usize, reason: HoldReason) -> SweepPlan {
        SweepPlan {
            manifest_deletes: Vec::new(),
            held: vec![HeldTable {
                table: "hits".into(),
                newest,
                held: count,
                reason,
            }],
        }
    }

    /// `hits` v1 and v2 written by the version 2 writer at store time 0, and
    /// whatever `forged` holds put directly at v3 at store time 1000, as a
    /// create-only credential can.
    async fn history_under(store: &InstrumentedStore<MemoryStore>, forged: Option<Vec<u8>>) -> i64 {
        store.inner().set_clock_ms(0);
        assert_eq!(commit(store, "hits", create(&[1])).await, 1);
        assert_eq!(commit(store, "hits", create(&[2])).await, 2);
        if let Some(bytes) = forged {
            store.inner().set_clock_ms(1_000);
            put_bytes(store, &mkey("hits", 3), bytes).await;
        }
        (1_001 + GRACE + SKEW_MS) as i64
    }

    #[tokio::test]
    async fn a_valid_mac_on_the_newest_version_deletes_its_aged_predecessors_for_one_get() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let now = history_under(
            &store,
            Some(authentic(&TENANT_A, &live_manifest("hits", 3, &[3]))),
        )
        .await;
        // A second table with nothing past the grace costs no GET.
        store.inner().set_clock_ms(now as u64);
        commit(&store, "quiet", create(&[1])).await;
        commit(&store, "quiet", create(&[2])).await;
        let before = get_calls(&store);
        let planned = plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
            .await
            .expect("plan");
        assert_eq!(
            planned,
            SweepPlan {
                manifest_deletes: vec![mkey("hits", 1), mkey("hits", 2)],
                held: Vec::new(),
            }
        );
        assert_eq!(get_calls(&store) - before, 1);
        execute(&store, &planned).await.expect("execute");
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![3]
        );
    }

    #[tokio::test]
    async fn a_forged_newest_version_holds_every_predecessor() {
        let forger = ManifestMacKey::from_deployment_key(&[0x66; 32]);
        let v3 = live_manifest("hits", 3, &[7]);
        // v2's genuine MAC, pasted into a forged v3 body.
        let replayed = {
            let real = authentic(&TENANT_A, &live_manifest("hits", 2, &[2]));
            let real =
                ravel_proto::parquet_table::v1::ParquetTableManifest::decode(real.as_slice())
                    .expect("decode");
            let mut body = ravel_proto::parquet_table::v1::ParquetTableManifest::decode(
                encode_v2(&TENANT_A, &v3, None).as_slice(),
            )
            .expect("decode");
            body.mac = real.mac;
            body.encode_to_vec()
        };
        let flipped = {
            let mut body = ravel_proto::parquet_table::v1::ParquetTableManifest::decode(
                authentic(&TENANT_A, &v3).as_slice(),
            )
            .expect("decode");
            body.mac[31] ^= 0x01;
            body.encode_to_vec()
        };
        let cases: Vec<(&str, Vec<u8>, HoldReason)> = vec![
            (
                "no mac",
                encode_v2(&TENANT_A, &v3, None),
                HoldReason::MacAbsent,
            ),
            (
                "the forger's own key",
                encode_v2(&TENANT_A, &v3, Some(&forger)),
                HoldReason::MacInvalid,
            ),
            ("a replayed mac", replayed, HoldReason::MacInvalid),
            ("one tag bit flipped", flipped, HoldReason::MacInvalid),
            (
                "version 1",
                encode_manifest(&TENANT_A, &v3).expect("encode"),
                HoldReason::Unversioned,
            ),
        ];
        for (name, bytes, reason) in cases {
            let store = InstrumentedStore::new(MemoryStore::new());
            let now = history_under(&store, Some(bytes)).await;
            let before = get_calls(&store);
            let planned = plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan");
            assert_eq!(planned, held(3, 2, reason), "{name}");
            assert_eq!(get_calls(&store) - before, 1, "{name}");
            assert_eq!(
                execute(&store, &planned).await.expect("execute"),
                SweepReport::default(),
                "{name}"
            );
            assert_eq!(
                store.metrics().snapshot().op(StoreOp::Delete).calls,
                0,
                "{name}"
            );
            assert_eq!(
                resolve::versions(&store, &TENANT_A, "hits")
                    .await
                    .expect("versions"),
                vec![1, 2, 3],
                "{name}"
            );

            // Once the forged version is removed (`parquet repair
            // --delete-version 3`), v2 is the authenticated newest again and
            // v1 goes.
            store.delete(&mkey("hits", 3)).await.expect("repair");
            assert_eq!(
                plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                    .await
                    .expect("plan"),
                SweepPlan {
                    manifest_deletes: vec![mkey("hits", 1)],
                    held: Vec::new(),
                },
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn a_newest_version_that_does_not_decode_holds_rather_than_failing_the_sweep() {
        for bytes in [b"not a manifest".to_vec(), Vec::new()] {
            let store = InstrumentedStore::new(MemoryStore::new());
            let now = history_under(&store, Some(bytes.clone())).await;
            let planned = plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan");
            assert!(planned.manifest_deletes.is_empty(), "{bytes:?}");
            assert!(
                matches!(
                    planned.held.as_slice(),
                    [HeldTable {
                        newest: 3,
                        held: 2,
                        reason: HoldReason::Unreadable(_),
                        ..
                    }]
                ),
                "{planned:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_failed_read_of_the_newest_version_fails_the_sweep_and_deletes_nothing() {
        let faults = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::Permanent("down".into()))
                    .with_key_contains(&mkey("hits", 2)),
            ),
        );
        let store = faults.inner();
        store.set_clock_ms(0);
        commit(store, "hits", create(&[1])).await;
        commit(store, "hits", create(&[2])).await;
        let now = (1 + GRACE + SKEW_MS) as i64;
        let got = plan(&faults, &TENANT_A, now, GRACE, GRACE, &gate()).await;
        assert!(
            matches!(&got, Err(SweepError::Store { key, .. }) if *key == mkey("hits", 2)),
            "{got:?}"
        );
        assert_eq!(
            faults.fault_count(Op::Get, ravel_object_store::fault::FaultKind::Permanent),
            1
        );
    }

    /// The migration window (ADR-2430 decision 5): this build's writer stamps
    /// version 1, which carries no MAC, so a keyed sweep keeps every
    /// predecessor until the table's next DDL writes a version 2 manifest.
    #[tokio::test]
    async fn a_version_one_newest_holds_predecessors_until_a_version_two_write() {
        let store = InstrumentedStore::new(MemoryStore::new());
        store.inner().set_clock_ms(0);
        for seed in [1, 2] {
            apply(
                &store,
                &TENANT_A,
                "hits",
                create(&[seed]),
                &FixedClock::new(0),
                GRACE,
                Some(&test_mac_key()),
            )
            .await
            .expect("apply");
        }
        let now = (1 + GRACE + SKEW_MS) as i64;
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            held(2, 1, HoldReason::Unversioned)
        );

        // The next DDL, through the version 2 writer, authenticates the
        // table, and both version 1 predecessors go once it ages.
        assert_eq!(commit(&store, "hits", create(&[3])).await, 3);
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE, &gate())
                .await
                .expect("plan"),
            SweepPlan {
                manifest_deletes: vec![mkey("hits", 1), mkey("hits", 2)],
                held: Vec::new(),
            }
        );
    }

    /// ADR-2430 decision 7: with no deployment key nothing is authenticated,
    /// so the newest version must outlive the longer grace, and the sweep reads
    /// no manifest.
    #[tokio::test]
    async fn an_unkeyed_sweep_holds_predecessors_until_the_newest_outlives_the_longer_grace() {
        const MULTIPLE: u64 = 3;
        let unkeyed = NewestGate::UnkeyedGrace { multiple: MULTIPLE };
        let store = InstrumentedStore::new(MemoryStore::new());
        store.inner().set_clock_ms(0);
        for seed in [1, 2] {
            apply(
                &store,
                &TENANT_A,
                "hits",
                create(&[seed]),
                &FixedClock::new(0),
                GRACE,
                None,
            )
            .await
            .expect("apply");
        }
        store.inner().set_clock_ms(1_000);
        put_bytes(
            &store,
            &mkey("hits", 3),
            encode_manifest(&TENANT_A, &live_manifest("hits", 3, &[3])).expect("encode"),
        )
        .await;
        let before = get_calls(&store);
        // Past the ordinary grace, and at the longer one exactly: both kept.
        for now in [1_001 + GRACE + SKEW_MS, 1_000 + MULTIPLE * GRACE + SKEW_MS] {
            assert_eq!(
                plan(&store, &TENANT_A, now as i64, GRACE, GRACE, &unkeyed)
                    .await
                    .expect("plan"),
                held(3, 2, HoldReason::UnkeyedGrace),
                "{now}"
            );
        }
        // One millisecond past the longer grace both go.
        let now = (1_001 + MULTIPLE * GRACE + SKEW_MS) as i64;
        let planned = plan(&store, &TENANT_A, now, GRACE, GRACE, &unkeyed)
            .await
            .expect("plan");
        assert_eq!(
            planned,
            SweepPlan {
                manifest_deletes: vec![mkey("hits", 1), mkey("hits", 2)],
                held: Vec::new(),
            }
        );
        assert_eq!(get_calls(&store), before);
        execute(&store, &planned).await.expect("execute");
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![3]
        );
    }

    #[test]
    fn the_gate_follows_the_deployment_key() {
        assert!(matches!(
            NewestGate::for_deployment(None),
            NewestGate::UnkeyedGrace {
                multiple: UNKEYED_NEWEST_GRACE_MULTIPLE
            }
        ));
        let NewestGate::Mac(key) =
            NewestGate::for_deployment(Some(&crate::test_util::TEST_DEPLOYMENT_KEY))
        else {
            panic!("a deployment key selects the MAC gate");
        };
        let m = live_manifest("hits", 1, &[1]);
        let bytes = authentic(&TENANT_A, &m);
        assert_eq!(
            decode_authenticated(&mkey("hits", 1), &bytes, &key)
                .expect("decode")
                .1,
            MacStatus::Valid
        );
    }
}

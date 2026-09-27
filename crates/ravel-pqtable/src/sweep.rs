//! Deleting superseded manifest versions and unreferenced data objects
//! (ADR-2040, Lifecycle).
//!
//! The grace period is what keeps a query that resolved an older version
//! readable, so it is measured from when an object stopped being needed:
//!
//! - a manifest version other than its table's newest is deleted once the
//!   version that superseded it (the next one present) is older than the
//!   grace; the newest version of a table, dropped or live, is never deleted,
//!   since the next writer numbers from it;
//! - a data object is deleted once no remaining manifest version references
//!   it (the newest live version, and every older version still inside its
//!   grace) and the object itself is older than the grace, so an upload
//!   whose CREATE has not committed yet is left alone for that long.
//!
//! [`plan`] refuses a grace below the deployment's minimum (its
//! `--gc-max-query-duration`). Ages come from the store's
//! `last_modified_unix_ms`, which may have 1-second granularity; the
//! comparison is strict, so an object exactly at the grace is kept.

use std::collections::{BTreeMap, BTreeSet};

use ravel_object_store::{ObjectMeta, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, parse_dataset_object_key, parse_manifest_key, tenant_data_prefix,
    tenant_manifest_prefix,
};
use crate::resolve::{self, ResolveError};

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
    /// A manifest version the plan keeps was gone when it was read: another
    /// sweep is running, or something deleted it outside the sweep.
    #[error("manifest {key:?} vanished during the sweep")]
    Vanished { key: String },
    #[error(transparent)]
    Resolve(#[from] ResolveError),
    #[error(transparent)]
    Key(#[from] KeyError),
}

/// Keys a sweep will delete, each list sorted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepPlan {
    pub manifest_deletes: Vec<String>,
    pub data_deletes: Vec<String>,
}

/// What [`execute`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub manifests_deleted: Vec<String>,
    pub data_deleted: Vec<String>,
    /// Planned data deletes a manifest committed since the plan now
    /// references; left in place.
    pub data_kept: Vec<String>,
}

fn past_grace(now_ms: i64, last_modified_ms: i64, grace_ms: u64) -> bool {
    i128::from(now_ms) - i128::from(last_modified_ms) > i128::from(grace_ms)
}

fn foreign(key: &str, prefix: &str, reason: impl ToString) -> SweepError {
    SweepError::ForeignKey {
        key: key.to_string(),
        prefix: prefix.to_string(),
        reason: reason.to_string(),
    }
}

async fn list(store: &dyn ObjectStoreBackend, prefix: &str) -> Result<Vec<ObjectMeta>, SweepError> {
    list_all(store, prefix)
        .await
        .map_err(|source| SweepError::Store {
            key: prefix.to_string(),
            source,
        })
}

/// Every manifest version of `tenant`, grouped by table, ascending.
async fn manifests_by_table(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<BTreeMap<String, Vec<(u64, ObjectMeta)>>, SweepError> {
    let prefix = tenant_manifest_prefix(tenant);
    let mut tables: BTreeMap<String, Vec<(u64, ObjectMeta)>> = BTreeMap::new();
    for meta in list(store, &prefix).await? {
        let parsed = parse_manifest_key(&meta.key).map_err(|e| foreign(&meta.key, &prefix, e))?;
        if parsed.tenant_hash != *tenant {
            return Err(foreign(&meta.key, &prefix, "belongs to another tenant"));
        }
        tables
            .entry(parsed.table)
            .or_default()
            .push((parsed.version, meta));
    }
    for versions in tables.values_mut() {
        versions.sort_by_key(|(v, _)| *v);
        versions.dedup_by_key(|(v, _)| *v);
    }
    Ok(tables)
}

/// Data object keys referenced by every manifest version not in `deleting`.
async fn referenced_keys(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    tables: &BTreeMap<String, Vec<(u64, ObjectMeta)>>,
    deleting: &BTreeSet<String>,
) -> Result<BTreeSet<String>, SweepError> {
    let mut referenced = BTreeSet::new();
    for (table, versions) in tables {
        for (version, meta) in versions {
            if deleting.contains(&meta.key) {
                continue;
            }
            let manifest = resolve::read_version(store, tenant, table, *version)
                .await?
                .ok_or_else(|| SweepError::Vanished {
                    key: meta.key.clone(),
                })?;
            referenced.extend(manifest.files.into_iter().map(|f| f.key));
        }
    }
    Ok(referenced)
}

/// Decide what a sweep of `tenant` at `now_ms` deletes. Reads every manifest
/// version it keeps; deletes nothing.
pub async fn plan(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    now_ms: i64,
    grace_ms: u64,
    min_grace_ms: u64,
) -> Result<SweepPlan, SweepError> {
    if grace_ms < min_grace_ms {
        return Err(SweepError::GraceBelowMinimum {
            grace_ms,
            min_grace_ms,
        });
    }
    let tables = manifests_by_table(store, tenant).await?;
    let mut manifest_deletes = BTreeSet::new();
    for versions in tables.values() {
        for pair in versions.windows(2) {
            let (older, successor) = (&pair[0].1, &pair[1].1);
            if past_grace(now_ms, successor.last_modified_unix_ms, grace_ms) {
                manifest_deletes.insert(older.key.clone());
            }
        }
    }
    let referenced = referenced_keys(store, tenant, &tables, &manifest_deletes).await?;

    let prefix = tenant_data_prefix(tenant);
    let mut data_deletes = Vec::new();
    for meta in list(store, &prefix).await? {
        let parsed =
            parse_dataset_object_key(&meta.key).map_err(|e| foreign(&meta.key, &prefix, e))?;
        if parsed.tenant_hash != *tenant {
            return Err(foreign(&meta.key, &prefix, "belongs to another tenant"));
        }
        if !referenced.contains(&meta.key)
            && past_grace(now_ms, meta.last_modified_unix_ms, grace_ms)
        {
            data_deletes.push(meta.key);
        }
    }
    data_deletes.sort();
    data_deletes.dedup();
    Ok(SweepPlan {
        manifest_deletes: manifest_deletes.into_iter().collect(),
        data_deletes,
    })
}

/// Carry out `plan`. Manifest versions go first, so a sweep interrupted
/// part-way leaves data objects a later sweep still finds, never a kept
/// manifest pointing at deleted data. Before deleting data it re-reads the
/// manifests and keeps any planned key a version committed since the plan
/// references; a CREATE that commits between that re-read and the delete is
/// not covered, which is why data younger than the grace is never planned.
pub async fn execute(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
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
    let tables = manifests_by_table(store, tenant).await?;
    let referenced = referenced_keys(store, tenant, &tables, &BTreeSet::new()).await?;
    for key in &plan.data_deletes {
        if referenced.contains(key) {
            report.data_kept.push(key.clone());
            continue;
        }
        store
            .delete(key)
            .await
            .map_err(|source| SweepError::Store {
                key: key.clone(),
                source,
            })?;
        report.data_deleted.push(key.clone());
    }
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::keys::manifest_key;
    use crate::test_util::{TENANT_A, TENANT_B, file_for};
    use crate::upload::upload_file;
    use crate::writer::{Intent, Outcome, WriteError, apply};

    const GRACE: u64 = 660_000;

    /// Put the data object `file_for(tenant, dataset, seed)` names.
    async fn put_data(store: &MemoryStore, tenant: &TenantHash, dataset: &str, seed: u8) -> String {
        let f = file_for(tenant, dataset, seed);
        store
            .put(
                &f.key,
                Bytes::from(vec![seed]),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put");
        f.key
    }

    fn create(dataset: &str, seeds: &[u8]) -> Intent {
        Intent::CreateOrReplace {
            dataset: dataset.into(),
            files: seeds
                .iter()
                .map(|&s| file_for(&TENANT_A, dataset, s))
                .collect(),
            options: BTreeMap::new(),
            created_by: "t".into(),
            statement: "s".into(),
        }
    }

    async fn commit(store: &MemoryStore, table: &str, intent: Intent) -> u64 {
        match apply(store, &TENANT_A, table, intent, 0)
            .await
            .expect("apply")
        {
            Outcome::Committed { version } => version,
            Outcome::NoOp => panic!("no-op"),
        }
    }

    fn mkey(table: &str, version: u64) -> String {
        manifest_key(&TENANT_A, table, version).expect("key")
    }

    fn dkey(dataset: &str, seed: u8) -> String {
        file_for(&TENANT_A, dataset, seed).key
    }

    #[tokio::test]
    async fn a_grace_below_the_minimum_is_refused() {
        let store = MemoryStore::new();
        let got = plan(&store, &TENANT_A, 0, GRACE - 1, GRACE).await;
        assert!(matches!(
            got,
            Err(SweepError::GraceBelowMinimum { grace_ms, min_grace_ms })
                if grace_ms == GRACE - 1 && min_grace_ms == GRACE
        ));
        assert_eq!(
            plan(&store, &TENANT_A, 0, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan::default()
        );
    }

    #[tokio::test]
    async fn sweep_keeps_referenced_and_young_objects_and_deletes_the_rest() {
        let store = MemoryStore::with_page_size(2);
        // t = 0: data 1..=4 uploaded, table "hits" v1 references 1 and 2.
        store.set_clock_ms(0);
        for seed in 1..=4 {
            put_data(&store, &TENANT_A, "hits", seed).await;
        }
        assert_eq!(commit(&store, "hits", create("hits", &[1, 2])).await, 1);
        // t = 1000: v2 replaces with 3. Data 1 and 2 are now referenced only
        // by v1, which stays until v2 is past the grace.
        store.set_clock_ms(1_000);
        assert_eq!(commit(&store, "hits", create("hits", &[3])).await, 2);
        // "gone" is created and dropped at t = 1000; its data 9 was uploaded
        // then too.
        put_data(&store, &TENANT_A, "gone", 9).await;
        assert_eq!(commit(&store, "gone", create("gone", &[9])).await, 1);
        assert_eq!(
            commit(&store, "gone", Intent::Drop { if_exists: false }).await,
            2
        );
        // t = 1000 + GRACE: a fresh unreferenced upload (5) and another
        // tenant's unreferenced old object, which a tenant A sweep never sees.
        store.set_clock_ms(1_000 + GRACE);
        put_data(&store, &TENANT_A, "hits", 5).await;
        store.set_clock_ms(0);
        put_data(&store, &TENANT_B, "hits", 7).await;

        // Just before v2 passes the grace: only data 4 (never referenced,
        // old) goes.
        let early = plan(&store, &TENANT_A, (1_000 + GRACE) as i64, GRACE, GRACE)
            .await
            .expect("plan");
        assert_eq!(
            early,
            SweepPlan {
                manifest_deletes: vec![],
                data_deletes: vec![dkey("hits", 4)],
            }
        );

        // One millisecond later v1 of both tables is superseded past the
        // grace; the data only they referenced goes with them. Data 3 (v2)
        // and 5 (young) stay; the dropped table keeps its newest version.
        let now = (1_001 + GRACE) as i64;
        let got = plan(&store, &TENANT_A, now, GRACE, GRACE)
            .await
            .expect("plan");
        let mut data_deletes = vec![
            dkey("hits", 1),
            dkey("hits", 2),
            dkey("hits", 4),
            dkey("gone", 9),
        ];
        data_deletes.sort();
        let mut manifest_deletes = vec![mkey("hits", 1), mkey("gone", 1)];
        manifest_deletes.sort();
        assert_eq!(
            got,
            SweepPlan {
                manifest_deletes: manifest_deletes.clone(),
                data_deletes: data_deletes.clone(),
            }
        );

        let report = execute(&store, &TENANT_A, &got).await.expect("execute");
        assert_eq!(
            report,
            SweepReport {
                manifests_deleted: manifest_deletes,
                data_deleted: data_deletes,
                data_kept: vec![],
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
            dkey("hits", 3),
            dkey("hits", 5),
            file_for(&TENANT_B, "hits", 7).key,
        ];
        expected.sort();
        assert_eq!(remaining, expected);
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan::default()
        );
    }

    #[tokio::test]
    async fn execute_keeps_data_a_manifest_committed_after_the_plan_references() {
        let store = MemoryStore::new();
        put_data(&store, &TENANT_A, "hits", 1).await;
        let planned = plan(&store, &TENANT_A, 1 + GRACE as i64, GRACE, GRACE)
            .await
            .expect("plan");
        assert_eq!(planned.data_deletes, vec![dkey("hits", 1)]);
        commit(&store, "hits", create("hits", &[1])).await;
        let report = execute(&store, &TENANT_A, &planned).await.expect("execute");
        assert_eq!(report.data_kept, vec![dkey("hits", 1)]);
        assert!(report.data_deleted.is_empty());
        store.head(&dkey("hits", 1)).await.expect("still there");
    }

    #[tokio::test]
    async fn a_crash_between_upload_and_manifest_leaves_no_manifest_and_sweep_reaps_the_data() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("f.parquet");
        std::fs::write(&path, b"PAR1 not really PAR1").expect("write");
        let faults = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::PartialWriteThenError)
                    .with_key_contains("/pq/t/"),
            ),
        );
        faults.inner().set_clock_ms(0);
        let uploaded = upload_file(&faults, &TENANT_A, "hits", &path)
            .await
            .expect("upload");
        let intent = Intent::Create {
            if_not_exists: false,
            dataset: "hits".into(),
            files: vec![uploaded.clone().into_file(1, 10)],
            options: BTreeMap::new(),
            created_by: "t".into(),
            statement: "s".into(),
        };
        let got = apply(&faults, &TENANT_A, "hits", intent, 0).await;
        assert!(matches!(got, Err(WriteError::Store { .. })), "{got:?}");
        assert_eq!(
            faults.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::PartialWriteThenError
            ),
            1
        );
        assert_eq!(
            resolve::versions(&faults, &TENANT_A, "hits")
                .await
                .expect("versions"),
            Vec::<u64>::new()
        );
        let young = plan(&faults, &TENANT_A, GRACE as i64, GRACE, GRACE)
            .await
            .expect("plan");
        assert_eq!(young, SweepPlan::default());
        let old = plan(&faults, &TENANT_A, 1 + GRACE as i64, GRACE, GRACE)
            .await
            .expect("plan");
        assert_eq!(
            old,
            SweepPlan {
                manifest_deletes: vec![],
                data_deletes: vec![uploaded.key],
            }
        );
    }

    #[tokio::test]
    async fn a_sweep_interrupted_at_the_data_deletes_leaves_them_for_the_next_one() {
        let faults = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Delete, ScriptedFault::Permanent("down".into()))
                    .with_key_contains("/pq/d/"),
            ),
        );
        let store = faults.inner();
        put_data(store, &TENANT_A, "hits", 1).await;
        put_data(store, &TENANT_A, "hits", 2).await;
        commit(store, "hits", create("hits", &[1])).await;
        commit(store, "hits", create("hits", &[2])).await;
        let now = 1 + GRACE as i64;
        let planned = plan(&faults, &TENANT_A, now, GRACE, GRACE)
            .await
            .expect("plan");
        let expected = SweepPlan {
            manifest_deletes: vec![mkey("hits", 1)],
            data_deletes: vec![dkey("hits", 1)],
        };
        assert_eq!(planned, expected);
        let got = execute(&faults, &TENANT_A, &planned).await;
        assert!(matches!(got, Err(SweepError::Store { .. })), "{got:?}");
        assert_eq!(
            faults.fault_count(Op::Delete, ravel_object_store::fault::FaultKind::Permanent),
            1
        );
        assert!(matches!(
            store.head(&mkey("hits", 1)).await,
            Err(StoreError::NotFound)
        ));
        store.head(&dkey("hits", 1)).await.expect("data kept");
        assert_eq!(
            plan(store, &TENANT_A, now, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan {
                manifest_deletes: vec![],
                data_deletes: vec![dkey("hits", 1)],
            }
        );
    }

    #[tokio::test]
    async fn a_foreign_key_under_the_data_prefix_is_refused() {
        let store = MemoryStore::new();
        let junk = format!("{}hits/readme.txt", tenant_data_prefix(&TENANT_A));
        store
            .put(&junk, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
        assert!(matches!(
            plan(&store, &TENANT_A, 0, GRACE, GRACE).await,
            Err(SweepError::ForeignKey { key, .. }) if key == junk
        ));
    }
}

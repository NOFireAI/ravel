//! Deleting superseded manifest versions (ADR-2040, Lifecycle).
//!
//! Ravel stores no Parquet data of its own: the files a table names live in
//! the tenant's bucket, under an operator's grant, and Ravel never deletes
//! them. The only objects a sweep touches are manifest versions under
//! `t/<tenant_hash>/pq/t/`, which is also the only prefix it lists.
//!
//! The grace period is what keeps a query that resolved an older version
//! readable, so it is measured from when a version stopped being needed: a
//! manifest version other than its table's newest is deleted once the version
//! that superseded it (the next one present) is older than the grace. The
//! newest version of a table, dropped or live, is never deleted, since the
//! next writer numbers from it.
//!
//! [`plan`] refuses a grace below the deployment's minimum (its
//! `--gc-max-query-duration`). Ages come from the store's
//! `last_modified_unix_ms`, which may have 1-second granularity; the
//! comparison is strict, so a version exactly at the grace is kept.

use std::collections::{BTreeMap, BTreeSet};

use ravel_object_store::{ObjectMeta, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{KeyError, parse_manifest_key, tenant_manifest_prefix};

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

/// Manifest keys a sweep will delete, sorted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepPlan {
    pub manifest_deletes: Vec<String>,
}

/// What [`execute`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub manifests_deleted: Vec<String>,
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

/// Every manifest version of `tenant`, grouped by table, ascending. The only
/// LIST a sweep issues, and it is scoped to the manifest prefix.
async fn manifests_by_table(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<BTreeMap<String, Vec<(u64, ObjectMeta)>>, SweepError> {
    let prefix = tenant_manifest_prefix(tenant);
    let listed = list_all(store, &prefix)
        .await
        .map_err(|source| SweepError::Store {
            key: prefix.clone(),
            source,
        })?;
    let mut tables: BTreeMap<String, Vec<(u64, ObjectMeta)>> = BTreeMap::new();
    for meta in listed {
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

/// Decide what a sweep of `tenant` at `now_ms` deletes. Reads no manifest
/// bodies and deletes nothing.
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
    Ok(SweepPlan {
        manifest_deletes: manifest_deletes.into_iter().collect(),
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
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::clock::FixedClock;
    use crate::keys::{grants_key, manifest_key};
    use crate::resolve;
    use crate::test_util::{CountingStore, TENANT_A, TENANT_B, file_for};
    use crate::writer::{Intent, Outcome, apply};

    const GRACE: u64 = 660_000;

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

    async fn commit(store: &dyn ObjectStoreBackend, table: &str, intent: Intent) -> u64 {
        let outcome = apply(store, &TENANT_A, table, intent, &FixedClock::new(0), GRACE)
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
        )
        .await
        .expect("tenant b");

        // Just before the superseding versions pass the grace: nothing goes.
        assert_eq!(
            plan(&store, &TENANT_A, (1_000 + GRACE) as i64, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan::default()
        );

        // One millisecond later v1 of both tables is superseded past the
        // grace. The dropped table keeps its newest (dropped) version.
        let now = (1_001 + GRACE) as i64;
        let mut manifest_deletes = vec![mkey("hits", 1), mkey("gone", 1)];
        manifest_deletes.sort();
        let got = plan(&store, &TENANT_A, now, GRACE, GRACE)
            .await
            .expect("plan");
        assert_eq!(
            got,
            SweepPlan {
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
            plan(&store, &TENANT_A, now, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan::default()
        );
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
        let planned = plan(&store, &TENANT_A, 1 + GRACE as i64, GRACE, GRACE)
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
        let now = 1 + GRACE as i64;
        let planned = plan(&faults, &TENANT_A, now, GRACE, GRACE)
            .await
            .expect("plan");
        assert_eq!(
            planned,
            SweepPlan {
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
            plan(store, &TENANT_A, now, GRACE, GRACE)
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
            plan(&store, &TENANT_A, 0, GRACE, GRACE).await,
            Err(SweepError::ForeignKey { key, .. }) if key == junk
        ));
    }
}

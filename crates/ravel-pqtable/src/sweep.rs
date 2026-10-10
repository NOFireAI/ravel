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
//! sweep never deletes it. A key the store lists but reports
//! unaddressable is in no table's versions and is never deleted; the listing
//! is counted as [`crate::resolve::unaddressable_listings`] describes.
//!
//! [`plan`] refuses a grace below the deployment's minimum (its
//! `--gc-max-query-duration`). Ages come from the store's
//! `last_modified_unix_ms`, which may have 1-second granularity and is
//! stamped by the store's clock while `now_ms` comes from the sweeper's, so
//! the margin [`SKEW_MS`] is added to the grace. The comparison is strict: a
//! version whose successor is exactly `grace_ms + SKEW_MS` old is kept.

use std::collections::{BTreeMap, BTreeSet};

use ravel_object_store::{ObjectMeta, ObjectStoreBackend, StoreError, list_all_reporting};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, ListedManifestKey, MAX_MANIFEST_VERSION, parse_listed_manifest_key,
    tenant_manifest_prefix,
};
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
    let listing = list_all_reporting(store, &prefix)
        .await
        .map_err(|source| SweepError::Store {
            key: prefix.clone(),
            source,
        })?;
    resolve::note_unaddressable(tenant, &listing.unaddressable);
    let listed = listing.objects;
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
        // A version above the bound is never a table's newest
        // (`resolve::newest`), so it supersedes nothing and is left for
        // `crate::repair`.
        let bounded = &versions[..versions.partition_point(|(v, _)| *v <= MAX_MANIFEST_VERSION)];
        for pair in bounded.windows(2) {
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
    use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::clock::FixedClock;
    use crate::keys::{grants_key, manifest_key};
    use crate::manifest::encode_manifest;
    use crate::resolve;
    use crate::test_util::{CountingStore, TENANT_A, TENANT_B, file_for, live_manifest};
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

        // At the grace plus the skew margin exactly: nothing goes.
        assert_eq!(
            plan(
                &store,
                &TENANT_A,
                (1_000 + GRACE + SKEW_MS) as i64,
                GRACE,
                GRACE
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
        let mut remaining: Vec<String> = ravel_object_store::list_all(&store, "t/")
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
            plan(&store, &TENANT_A, now, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan::default()
        );

        // A legitimate successor still supersedes v1; the forged versions
        // are neither successors nor deleted.
        assert_eq!(commit(&store, "hits", create(&[2])).await, 2);
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan {
                manifest_deletes: vec![mkey("hits", 1)]
            }
        );

        // A version exactly at the bound is inside it: it supersedes v2, and
        // as the newest it is kept.
        let bytes = encode_manifest(
            &TENANT_A,
            &live_manifest("hits", MAX_MANIFEST_VERSION, &[3]),
        )
        .expect("encode");
        store
            .put(
                &mkey("hits", MAX_MANIFEST_VERSION),
                Bytes::from(bytes),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put");
        assert_eq!(
            plan(&store, &TENANT_A, now, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan {
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
                let bytes =
                    encode_manifest(&tenant, &live_manifest("hits", v, &[1])).expect("encode");
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
                    plan(&store, &tenant, now, GRACE, GRACE)
                        .await
                        .expect("plan"),
                    SweepPlan {
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
            let bytes = encode_manifest(&TENANT, &live_manifest("hits", v, &[1])).expect("encode");
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
            plan(&store, &TENANT, now, GRACE, GRACE)
                .await
                .expect("plan"),
            SweepPlan {
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
            let store = two_versions(&tenant).await;
            let stray = put_stray(store.inner(), &tenant, rest).await;
            let now = (1 + GRACE + SKEW_MS) as i64;
            for _ in 0..2 {
                let planned = plan(&store, &tenant, now, GRACE, GRACE)
                    .await
                    .expect("plan");
                assert_eq!(
                    planned,
                    SweepPlan {
                        manifest_deletes: vec![manifest_key(&tenant, "hits", 1).expect("key")]
                    },
                    "{rest}"
                );
            }
            assert_eq!(resolve::invalid_table_listings(&tenant), 2, "{rest}");
            assert_eq!(stray_warnings_naming(&logs, &stray), 1, "{rest}");
            let planned = plan(&store, &tenant, now, GRACE, GRACE)
                .await
                .expect("plan");
            execute(&store, &planned).await.expect("execute");
            assert_eq!(store.metrics().snapshot().op(StoreOp::Delete).calls, 1);
            store
                .head(&stray)
                .await
                .expect("the stray key is untouched");
        }
    }

    /// Versions 1 and 2 of `hits`, written at store time 0.
    async fn two_versions(tenant: &TenantHash) -> InstrumentedStore<MemoryStore> {
        let store = InstrumentedStore::new(MemoryStore::with_page_size(2));
        store.inner().set_clock_ms(0);
        for v in [1, 2] {
            let bytes = encode_manifest(tenant, &live_manifest("hits", v, &[1])).expect("encode");
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

    /// A stray key the S3 adapter cannot address is skipped by the sweep's
    /// listing and counted in [`resolve::unaddressable_listings`]; the sweep
    /// deletes the superseded version and leaves the stray key in the store.
    #[tokio::test]
    async fn an_unaddressable_stray_key_is_skipped_and_counted_by_the_sweep() {
        use crate::resolve::tests::{UNLISTABLE_SHAPES, put_stray};

        for (i, rest) in UNLISTABLE_SHAPES.iter().enumerate() {
            let tenant = TenantHash([0x7a + i as u8; 16]);
            let store = two_versions(&tenant).await;
            let stray = put_stray(store.inner(), &tenant, rest).await;
            let now = (1 + GRACE + SKEW_MS) as i64;
            let planned = plan(&store, &tenant, now, GRACE, GRACE)
                .await
                .expect("plan");
            assert_eq!(
                planned,
                SweepPlan {
                    manifest_deletes: vec![manifest_key(&tenant, "hits", 1).expect("key")]
                },
                "{rest:?}"
            );
            assert_eq!(resolve::invalid_table_listings(&tenant), 0, "{rest:?}");
            assert_eq!(resolve::unaddressable_listings(&tenant), 1, "{rest:?}");
            execute(&store, &planned).await.expect("execute");
            assert_eq!(
                store.metrics().snapshot().op(StoreOp::Delete).calls,
                1,
                "{rest:?}"
            );
            store
                .inner()
                .head(&stray)
                .await
                .expect_err("unaddressable, so the memory store refuses it too");
            assert!(
                ravel_object_store::list_all_reporting(
                    store.inner(),
                    &tenant_manifest_prefix(&tenant)
                )
                .await
                .expect("list")
                .unaddressable
                .sample
                .iter()
                .any(|k| k.key == stray),
                "{rest:?}: the stray key is still in the store"
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
                    plan(&store, &TENANT_A, 0, GRACE, GRACE).await,
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

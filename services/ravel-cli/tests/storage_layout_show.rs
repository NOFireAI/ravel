//! `ravel-cli clustering-key show` and `ravel-cli bloom-scope show` (issue
//! #2145): the output for each state of config record fields 13 and 14,
//! including a raw format-version-3 record no writer in this build can stamp.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use prost::Message;
use ravel_catalog::{
    DeclaredColumnType, DeclaredTypedColumn, StorageLayoutConfigError, TenantConfig,
    TenantLifecycleState, config_key, set_tenant_config,
};
use ravel_cli::storage_layout::{bloom_scope_show_to, clustering_key_show_to};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::TenantId;

const TENANT: &str = "acme";

/// The line a set key ends with when the record has no typed_attr_columns
/// override.
const NO_OVERRIDE_NOTE: &str = "note: tenant acme has no typed attribute column override, so \
                                this command checked the key's shape only; this build's log \
                                ingest flush resolves a key against that override alone, so it \
                                leaves this key unresolved and writes the tenant's log objects \
                                without it\n";

fn column(key: &str, ty: sysproto::TypedAttrColumnType) -> sysproto::TypedAttrColumn {
    sysproto::TypedAttrColumn {
        key: key.to_string(),
        r#type: ty as i32,
    }
}

/// A raw version-3 record declaring `svc:str` and `code:i64`, carrying the
/// given clustering key and bloom scope.
fn v3_record(
    clustering_key: Option<sysproto::ClusteringKeyConfig>,
    bloom_scope: sysproto::BloomScope,
) -> sysproto::TenantConfigRecord {
    sysproto::TenantConfigRecord {
        format_version: 3,
        tenant_hash: TenantId::new(TENANT).hash().0.to_vec(),
        lifecycle_state: sysproto::TenantLifecycleState::Active as i32,
        typed_attr_columns: Some(sysproto::TypedAttrColumnConfig {
            columns: vec![
                column("svc", sysproto::TypedAttrColumnType::Str),
                column("code", sysproto::TypedAttrColumnType::I64),
            ],
        }),
        clustering_key,
        bloom_scope: bloom_scope as i32,
        created_unix_ns: 1,
        updated_unix_ns: 1,
        ..Default::default()
    }
}

async fn store_with(record: &sysproto::TenantConfigRecord) -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    store
        .put(
            &config_key(&TenantId::new(TENANT).hash()),
            record.encode_to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("put the raw config record");
    store
}

async fn clustering_key(store: &Arc<dyn ObjectStoreBackend>) -> String {
    let mut out = Vec::new();
    clustering_key_show_to(Arc::clone(store), TENANT, &mut out)
        .await
        .expect("clustering-key show");
    String::from_utf8(out).expect("utf-8 output")
}

async fn bloom_scope(store: &Arc<dyn ObjectStoreBackend>) -> String {
    let mut out = Vec::new();
    bloom_scope_show_to(Arc::clone(store), TENANT, &mut out)
        .await
        .expect("bloom-scope show");
    String::from_utf8(out).expect("utf-8 output")
}

#[tokio::test]
async fn clustering_key_show_reports_absent_before_the_writer_flip() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    assert_eq!(
        clustering_key(&store).await,
        "no config record for tenant acme: no clustering key was ever set (clustering \
         generation 0)\n"
    );

    // This build's writer stamps version 2, which cannot carry field 13.
    let config = TenantConfig {
        typed_attr_columns: Some(vec![DeclaredTypedColumn {
            key: "svc".to_string(),
            ty: DeclaredColumnType::Str,
        }]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    set_tenant_config(store.as_ref(), &TenantId::new(TENANT).hash(), &config, 1)
        .await
        .expect("write a version-2 record");
    assert_eq!(
        clustering_key(&store).await,
        "tenant acme never set a clustering key (clustering generation 0)\n"
    );
    assert_eq!(bloom_scope(&store).await, "tenant acme bloom scope: all\n");

    // A cleared key is not "never set": it carries the generation of the clear.
    let cleared = store_with(&v3_record(
        Some(sysproto::ClusteringKeyConfig {
            columns: Vec::new(),
            bucket_width: sysproto::ClusteringBucketWidth::Unspecified as i32,
            generation: 7,
        }),
        sysproto::BloomScope::All,
    ))
    .await;
    assert_eq!(
        clustering_key(&cleared).await,
        "tenant acme cleared its clustering key at generation 7\n"
    );
}

#[tokio::test]
async fn clustering_key_show_reports_a_v3_record() {
    // Key order differs from declaration order, so a printer that walks the
    // declaration instead of the key fails.
    let store = store_with(&v3_record(
        Some(sysproto::ClusteringKeyConfig {
            columns: vec!["code".to_string(), "svc".to_string()],
            bucket_width: sysproto::ClusteringBucketWidth::SixHours as i32,
            generation: 5,
        }),
        sysproto::BloomScope::Text,
    ))
    .await;
    assert_eq!(
        clustering_key(&store).await,
        "tenant acme clustering key at generation 5, bucket width 6h, 2 column(s) in key \
         order:\n  code:i64\n  svc:str\n"
    );
    assert_eq!(bloom_scope(&store).await, "tenant acme bloom scope: text\n");

    // Without a typed_attr_columns override the types are the deployment
    // default's, which the command cannot read, and it says so.
    let mut record = v3_record(
        Some(sysproto::ClusteringKeyConfig {
            columns: vec!["code".to_string(), "svc".to_string()],
            bucket_width: sysproto::ClusteringBucketWidth::OneDay as i32,
            generation: 9,
        }),
        sysproto::BloomScope::Text,
    );
    record.typed_attr_columns = None;
    let store = store_with(&record).await;
    assert_eq!(
        clustering_key(&store).await,
        format!(
            "tenant acme clustering key at generation 9, bucket width 1d, 2 column(s) in key \
             order:\n  code:deployment-default\n  svc:deployment-default\n{NO_OVERRIDE_NOTE}"
        )
    );

    // The third bucket width, with the override, so the note is absent.
    let store = store_with(&v3_record(
        Some(sysproto::ClusteringKeyConfig {
            columns: vec!["svc".to_string()],
            bucket_width: sysproto::ClusteringBucketWidth::OneHour as i32,
            generation: 3,
        }),
        sysproto::BloomScope::All,
    ))
    .await;
    assert_eq!(
        clustering_key(&store).await,
        "tenant acme clustering key at generation 3, bucket width 1h, 1 column(s) in key \
         order:\n  svc:str\n"
    );

    // A key the accessor refuses is an error, never printed.
    for invalid in [
        sysproto::ClusteringKeyConfig {
            columns: vec!["code".to_string()],
            bucket_width: sysproto::ClusteringBucketWidth::OneHour as i32,
            generation: 0,
        },
        sysproto::ClusteringKeyConfig {
            columns: vec!["code".to_string()],
            bucket_width: sysproto::ClusteringBucketWidth::Unspecified as i32,
            generation: 2,
        },
        sysproto::ClusteringKeyConfig {
            columns: vec!["undeclared".to_string()],
            bucket_width: sysproto::ClusteringBucketWidth::OneHour as i32,
            generation: 2,
        },
    ] {
        let store = store_with(&v3_record(Some(invalid.clone()), sysproto::BloomScope::All)).await;
        let mut out = Vec::new();
        let result = clustering_key_show_to(store, TENANT, &mut out).await;
        assert!(result.is_err(), "{invalid:?} must be refused");
        assert!(out.is_empty(), "{invalid:?} must print nothing");
    }
}

/// Without a typed_attr_columns override the key is checked for shape only,
/// and a shape the accessor refuses is still an error naming that shape rule,
/// with nothing printed.
#[tokio::test]
async fn clustering_key_show_refuses_a_bad_shape_without_an_override() {
    let key = |columns: &[&str], bucket_width: sysproto::ClusteringBucketWidth, generation| {
        sysproto::ClusteringKeyConfig {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            bucket_width: bucket_width as i32,
            generation,
        }
    };
    use sysproto::ClusteringBucketWidth::{OneHour, Unspecified};
    for (invalid, expected) in [
        (
            key(&["code"], OneHour, 0),
            StorageLayoutConfigError::ZeroClusteringGeneration,
        ),
        (
            key(&["code"], Unspecified, 2),
            StorageLayoutConfigError::UnspecifiedBucketWidth,
        ),
        (
            key(&["a", "b", "c", "d", "e"], OneHour, 2),
            StorageLayoutConfigError::TooManyClusteringKeyColumns { count: 5, max: 4 },
        ),
        (
            key(&["code", "code"], OneHour, 2),
            StorageLayoutConfigError::DuplicateClusteringKeyColumn {
                column: "code".to_string(),
            },
        ),
    ] {
        let mut record = v3_record(Some(invalid.clone()), sysproto::BloomScope::All);
        record.typed_attr_columns = None;
        let store = store_with(&record).await;
        let mut out = Vec::new();
        let err = clustering_key_show_to(store, TENANT, &mut out)
            .await
            .expect_err("a bad shape without an override must be refused");
        assert_eq!(
            err.downcast_ref::<StorageLayoutConfigError>(),
            Some(&expected),
            "{invalid:?}: {err:#}"
        );
        assert!(out.is_empty(), "{invalid:?} must print nothing");
    }
}

/// A store with one config record whose every GET of that record fails with
/// a permanent error.
fn store_failing_the_config_get() -> Arc<FaultStore<MemoryStore>> {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Get,
            ScriptedFault::Permanent("simulated config read failure".to_string()),
        )
        .with_key_contains(config_key(&TenantId::new(TENANT).hash())),
    );
    Arc::new(FaultStore::new(MemoryStore::new(), plan))
}

/// A failed read of the config record is an error with nothing printed, not
/// the no-record output.
#[tokio::test]
async fn clustering_key_show_fails_closed_on_a_config_read_error() {
    let store = store_failing_the_config_get();
    let mut out = Vec::new();
    let result = clustering_key_show_to(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        TENANT,
        &mut out,
    )
    .await;
    assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
    let err = result.expect_err("a failed config read must be an error");
    assert!(
        format!("{err:#}").contains("simulated config read failure"),
        "the error carries the store failure: {err:#}"
    );
    assert!(out.is_empty(), "printed: {}", String::from_utf8_lossy(&out));
}

/// [`clustering_key_show_fails_closed_on_a_config_read_error`] for
/// `bloom-scope show`.
#[tokio::test]
async fn bloom_scope_show_fails_closed_on_a_config_read_error() {
    let store = store_failing_the_config_get();
    let mut out = Vec::new();
    let result = bloom_scope_show_to(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        TENANT,
        &mut out,
    )
    .await;
    assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
    let err = result.expect_err("a failed config read must be an error");
    assert!(
        format!("{err:#}").contains("simulated config read failure"),
        "the error carries the store failure: {err:#}"
    );
    assert!(out.is_empty(), "printed: {}", String::from_utf8_lossy(&out));
}

#[tokio::test]
async fn bloom_scope_show_reports_the_record() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    assert_eq!(
        bloom_scope(&store).await,
        "no config record for tenant acme: bloom scope all (the default)\n"
    );

    for (scope, spelling) in [
        (sysproto::BloomScope::All, "all"),
        (sysproto::BloomScope::Undeclared, "undeclared"),
        (sysproto::BloomScope::Text, "text"),
    ] {
        let store = store_with(&v3_record(None, scope)).await;
        assert_eq!(
            bloom_scope(&store).await,
            format!("tenant acme bloom scope: {spelling}\n")
        );
    }

    // An unknown stored value is refused, not read as the default.
    let mut record = v3_record(None, sysproto::BloomScope::All);
    record.bloom_scope = 99;
    let store = store_with(&record).await;
    let mut out = Vec::new();
    assert!(bloom_scope_show_to(store, TENANT, &mut out).await.is_err());
    assert!(out.is_empty());
}

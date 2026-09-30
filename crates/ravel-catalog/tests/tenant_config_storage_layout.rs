//! The storage-layout setters on the tenant config record (ADR-2135 decision 7,
//! issue #2146): the clustering key and bloom scope are written through
//! `set_tenant_config` as a version-3 record only behind the operator opt-in,
//! and the write gate validates whatever a config carries, on its own and
//! against the record it replaces. Every test goes through the public write and read path on a
//! `MemoryStore`, and asserts the record on the wire where the claim is about
//! the record.

#![allow(clippy::expect_used)]

use prost::Message as _;
use ravel_catalog::BloomScope;
use ravel_catalog::{
    ClusteringBucketWidth, ClusteringKey, ClusteringKeyState, DeclaredColumnType,
    DeclaredTypedColumn, StorageLayoutConfigError, StorageLayoutWrite, TenantConfig,
    TenantConfigError, TenantLifecycleState, config_key, read_config_values,
    resolve_declared_columns, set_tenant_config, validate_typed_attr_columns,
};
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{TenantHash, TenantId};

const OPTED_IN: StorageLayoutWrite = StorageLayoutWrite::ReadersRolledOut;
const SIX_HOURS: i32 = sysproto::ClusteringBucketWidth::SixHours as i32;
const ONE_DAY: i32 = sysproto::ClusteringBucketWidth::OneDay as i32;

fn tenant() -> TenantHash {
    TenantId::new("storage-layout").hash()
}

fn col(key: &str, ty: DeclaredColumnType) -> DeclaredTypedColumn {
    DeclaredTypedColumn {
        key: key.into(),
        ty,
    }
}

fn names(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|c| c.to_string()).collect()
}

/// `a` (Str), `b` (I64) and `c` (Str): the record-own declaration every test
/// keys on unless it says otherwise.
fn declared() -> Vec<DeclaredTypedColumn> {
    vec![
        col("a", DeclaredColumnType::Str),
        col("b", DeclaredColumnType::I64),
        col("c", DeclaredColumnType::Str),
    ]
}

fn base_config() -> TenantConfig {
    TenantConfig {
        typed_attr_columns: Some(declared()),
        ..TenantConfig::new(TenantLifecycleState::Active)
    }
}

async fn write(store: &dyn ObjectStoreBackend, cfg: &TenantConfig, now_ns: i64) {
    set_tenant_config(store, &tenant(), cfg, now_ns)
        .await
        .expect("write the tenant config");
}

async fn read(store: &dyn ObjectStoreBackend) -> TenantConfig {
    read_config_values(store, &tenant())
        .await
        .expect("read the tenant config")
        .expect("the record is present")
}

async fn raw_bytes(store: &dyn ObjectStoreBackend) -> Vec<u8> {
    store
        .get(&config_key(&tenant()), GetRange::Full)
        .await
        .expect("get the record")
        .data
        .to_vec()
}

/// The record exactly as stored, decoded from the bytes on the wire.
async fn wire(store: &dyn ObjectStoreBackend) -> sysproto::TenantConfigRecord {
    sysproto::TenantConfigRecord::decode(raw_bytes(store).await.as_slice())
        .expect("decode the stored record")
}

async fn put_raw(store: &dyn ObjectStoreBackend, record: &sysproto::TenantConfigRecord) {
    store
        .put(
            &config_key(&tenant()),
            record.encode_to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("seed a raw record");
}

/// A raw record for `tenant()` declaring `columns` and nothing else.
fn raw_record(
    format_version: u32,
    columns: &[DeclaredTypedColumn],
) -> sysproto::TenantConfigRecord {
    sysproto::TenantConfigRecord {
        format_version,
        tenant_hash: tenant().0.to_vec(),
        lifecycle_state: sysproto::TenantLifecycleState::Active as i32,
        typed_attr_columns: Some(sysproto::TypedAttrColumnConfig {
            columns: columns
                .iter()
                .map(|c| sysproto::TypedAttrColumn {
                    key: c.key.clone(),
                    r#type: match c.ty {
                        DeclaredColumnType::Str => sysproto::TypedAttrColumnType::Str,
                        DeclaredColumnType::I64 => sysproto::TypedAttrColumnType::I64,
                        DeclaredColumnType::Bool => sysproto::TypedAttrColumnType::Bool,
                        DeclaredColumnType::Bytes => sysproto::TypedAttrColumnType::Bytes,
                    } as i32,
                })
                .collect(),
        }),
        created_unix_ns: 1,
        updated_unix_ns: 1,
        ..Default::default()
    }
}

fn layout_error(err: TenantConfigError) -> StorageLayoutConfigError {
    match err {
        TenantConfigError::InvalidStorageLayoutConfig { source, .. } => source,
        other => panic!("expected a storage-layout refusal, got {other}"),
    }
}

/// A set key is written as a version-3 record carrying exactly its columns,
/// in order, and bucket width, at generation 1 for a key never set before, and
/// the reader returns the same key. A second set stores generation 2.
#[tokio::test]
async fn set_key_writes_a_version_3_record_the_reader_returns() {
    let store = MemoryStore::new();
    write(&store, &base_config(), 1).await;
    assert_eq!(wire(&store).await.format_version, 2);

    let mut cfg = read(&store).await;
    cfg.set_clustering_key(
        names(&["b", "a"]),
        ClusteringBucketWidth::SixHours,
        OPTED_IN,
    )
    .expect("set the key");
    write(&store, &cfg, 2).await;

    let record = wire(&store).await;
    assert_eq!(record.format_version, 3);
    assert_eq!(
        record.clustering_key,
        Some(sysproto::ClusteringKeyConfig {
            columns: names(&["b", "a"]),
            bucket_width: SIX_HOURS,
            generation: 1,
        })
    );
    let back = read(&store).await;
    assert_eq!(
        back.clustering_key(),
        Ok(ClusteringKeyState::Set(ClusteringKey {
            columns: names(&["b", "a"]),
            bucket_width: ClusteringBucketWidth::SixHours,
            generation: 1,
        }))
    );
    assert_eq!(back.clustering_generation(), 1);

    let mut cfg = back;
    cfg.set_clustering_key(names(&["c"]), ClusteringBucketWidth::OneDay, OPTED_IN)
        .expect("set a second key");
    write(&store, &cfg, 3).await;
    assert_eq!(
        wire(&store).await.clustering_key,
        Some(sysproto::ClusteringKeyConfig {
            columns: names(&["c"]),
            bucket_width: ONE_DAY,
            generation: 2,
        })
    );
    assert_eq!(
        read(&store).await.clustering_key(),
        Ok(ClusteringKeyState::Set(ClusteringKey {
            columns: names(&["c"]),
            bucket_width: ClusteringBucketWidth::OneDay,
            generation: 2,
        }))
    );
}

/// A never-set key is an absent field 13 at generation 0. A clear keeps field
/// 13 present with no columns at the next generation, and a set after the clear
/// takes the one after that.
#[tokio::test]
async fn clear_key_keeps_the_generation_and_drops_the_columns() {
    let store = MemoryStore::new();
    write(&store, &base_config(), 1).await;
    assert_eq!(wire(&store).await.clustering_key, None);
    let never = read(&store).await;
    assert_eq!(never.clustering_key(), Ok(ClusteringKeyState::NeverSet));
    assert_eq!(never.clustering_generation(), 0);

    let mut cfg = never;
    cfg.set_clustering_key(names(&["a"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set");
    write(&store, &cfg, 2).await;
    let mut cfg = read(&store).await;
    cfg.clear_clustering_key(OPTED_IN).expect("clear");
    write(&store, &cfg, 3).await;

    let record = wire(&store).await;
    assert_eq!(record.format_version, 3);
    assert_eq!(
        record.clustering_key,
        Some(sysproto::ClusteringKeyConfig {
            columns: Vec::new(),
            bucket_width: sysproto::ClusteringBucketWidth::Unspecified as i32,
            generation: 2,
        })
    );
    let cleared = read(&store).await;
    assert_eq!(
        cleared.clustering_key(),
        Ok(ClusteringKeyState::Cleared { generation: 2 })
    );
    assert_eq!(cleared.clustering_generation(), 2);

    let mut cfg = cleared;
    cfg.set_clustering_key(names(&["c"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set after the clear");
    write(&store, &cfg, 4).await;
    assert_eq!(read(&store).await.clustering_generation(), 3);
}

/// Without the opt-in, all three setters refuse with the writer error and leave
/// the config as it was. A config that carries a field 13 not produced under
/// the opt-in, decoded here from a version-2 record, is refused by
/// `set_tenant_config` before it issues a single store request.
#[tokio::test]
async fn set_without_opt_in_is_refused_and_writes_nothing() {
    let refusal = |field| StorageLayoutConfigError::WriterCannotEmit {
        field,
        writer_version: 2,
        required: 3,
    };
    let store = InstrumentedStore::new(MemoryStore::new());
    write(&store, &base_config(), 1).await;
    let seeded = raw_bytes(&store).await;

    let mut cfg = read(&store).await;
    let before = cfg.clone();
    assert_eq!(
        cfg.set_clustering_key(
            names(&["a"]),
            ClusteringBucketWidth::OneHour,
            StorageLayoutWrite::Disabled
        ),
        Err(refusal("clustering_key"))
    );
    assert_eq!(
        cfg.set_bloom_scope(BloomScope::Text, StorageLayoutWrite::Disabled),
        Err(refusal("bloom_scope"))
    );
    assert_eq!(cfg, before);
    let mut with_key = cfg.clone();
    with_key
        .set_clustering_key(names(&["a"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set");
    let keyed = with_key.clone();
    assert_eq!(
        with_key.clear_clustering_key(StorageLayoutWrite::Disabled),
        Err(refusal("clustering_key"))
    );
    assert_eq!(with_key, keyed);
    assert_eq!(raw_bytes(&store).await, seeded);

    let mut v2_with_key = raw_record(2, &declared());
    v2_with_key.clustering_key = Some(sysproto::ClusteringKeyConfig {
        columns: names(&["a"]),
        bucket_width: SIX_HOURS,
        generation: 1,
    });
    put_raw(&store, &v2_with_key).await;
    let decoded = read(&store).await;
    let stored = raw_bytes(&store).await;

    let requests = store.metrics().snapshot();
    let err = set_tenant_config(&store, &tenant(), &decoded, 5)
        .await
        .expect_err("a field 13 without the opt-in is refused");
    assert_eq!(layout_error(err), refusal("clustering_key"));
    assert_eq!(
        store.metrics().snapshot(),
        requests,
        "the refusal issues no store request"
    );
    assert_eq!(raw_bytes(&store).await, stored);
}

/// A key column must be declared in the config record's own
/// `typed_attr_columns`: the setter names the missing column, and a record that
/// declares no columns declares no key column.
#[tokio::test]
async fn key_column_must_be_declared_on_the_record() {
    let store = MemoryStore::new();
    write(&store, &base_config(), 1).await;
    let mut cfg = read(&store).await;
    let before = cfg.clone();
    let err = cfg
        .set_clustering_key(names(&["a", "z"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect_err("z is not declared");
    assert_eq!(
        err,
        StorageLayoutConfigError::UndeclaredClusteringKeyColumn { column: "z".into() }
    );
    assert_eq!(
        err.to_string(),
        "clustering key column \"z\" is not a declared typed attribute column in this tenant's \
         config record: declare it in the record's typed_attr_columns first"
    );
    assert_eq!(cfg, before);

    let mut undeclared = TenantConfig::new(TenantLifecycleState::Active);
    assert_eq!(
        undeclared.set_clustering_key(names(&["a"]), ClusteringBucketWidth::OneHour, OPTED_IN),
        Err(StorageLayoutConfigError::UndeclaredClusteringKeyColumn { column: "a".into() })
    );
    assert_eq!(undeclared.stored_clustering_key, None);
}

/// With a key set on `a` and `b`, a write that retypes `b` or drops `a` is
/// refused with the column named and nothing written; adding a column and
/// retyping `c`, which the key does not name, both write. After a clear, `a`
/// can be dropped.
#[tokio::test]
async fn retyping_or_dropping_a_key_column_is_refused() {
    let store = MemoryStore::new();
    let mut cfg = base_config();
    cfg.set_clustering_key(names(&["a", "b"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set");
    write(&store, &cfg, 1).await;
    let seeded = raw_bytes(&store).await;
    let keyed = read(&store).await;

    let retyped = TenantConfig {
        typed_attr_columns: Some(vec![
            col("a", DeclaredColumnType::Str),
            col("b", DeclaredColumnType::Str),
            col("c", DeclaredColumnType::Str),
        ]),
        ..keyed.clone()
    };
    let err = layout_error(
        set_tenant_config(&store, &tenant(), &retyped, 2)
            .await
            .expect_err("retyping a key column"),
    );
    assert_eq!(
        err,
        StorageLayoutConfigError::ClusteringKeyColumnRetyped {
            column: "b".into(),
            from: DeclaredColumnType::I64,
            to: DeclaredColumnType::Str,
        }
    );
    assert_eq!(
        err.to_string(),
        "typed attribute column \"b\" is named by the current clustering key and cannot be \
         retyped from I64 to Str: clear the clustering key first"
    );

    let dropped = TenantConfig {
        typed_attr_columns: Some(vec![
            col("b", DeclaredColumnType::I64),
            col("c", DeclaredColumnType::Str),
        ]),
        ..keyed.clone()
    };
    let err = layout_error(
        set_tenant_config(&store, &tenant(), &dropped, 3)
            .await
            .expect_err("dropping a key column"),
    );
    assert_eq!(
        err,
        StorageLayoutConfigError::ClusteringKeyColumnRemoved { column: "a".into() }
    );
    assert_eq!(
        err.to_string(),
        "typed attribute column \"a\" is named by the current clustering key and cannot be \
         removed: clear the clustering key first"
    );
    assert_eq!(raw_bytes(&store).await, seeded, "neither refusal writes");

    let widened = TenantConfig {
        typed_attr_columns: Some(vec![
            col("a", DeclaredColumnType::Str),
            col("b", DeclaredColumnType::I64),
            col("c", DeclaredColumnType::Bool),
            col("d", DeclaredColumnType::Bytes),
        ]),
        ..keyed.clone()
    };
    write(&store, &widened, 4).await;
    let back = read(&store).await;
    assert_eq!(back.typed_attr_columns, widened.typed_attr_columns);
    assert_eq!(
        back.clustering_key(),
        Ok(ClusteringKeyState::Set(ClusteringKey {
            columns: names(&["a", "b"]),
            bucket_width: ClusteringBucketWidth::OneHour,
            generation: 1,
        }))
    );

    let mut cleared = back;
    cleared.clear_clustering_key(OPTED_IN).expect("clear");
    cleared.typed_attr_columns = Some(vec![col("b", DeclaredColumnType::I64)]);
    write(&store, &cleared, 5).await;
    assert_eq!(
        read(&store).await.typed_attr_columns,
        Some(vec![col("b", DeclaredColumnType::I64)])
    );
}

/// A config with no clustering key and the default bloom scope writes the same
/// version-2 bytes as before the storage-layout fields existed, on create and
/// on update, and setting the scope back to `ALL` keeps it that way.
#[tokio::test]
async fn a_config_without_layout_fields_still_writes_version_2() {
    let cfg = TenantConfig {
        max_active_series: Some(10),
        retention_ns: Some(3_600_000_000_000),
        indexed_fields: Some(vec!["service.name".into()]),
        ..base_config()
    };
    let expected = |created, updated| {
        sysproto::TenantConfigRecord {
            max_active_series: Some(10),
            retention_ns: Some(3_600_000_000_000),
            indexed_fields: Some(sysproto::IndexedFieldConfig {
                fields: vec!["service.name".into()],
            }),
            created_unix_ns: created,
            updated_unix_ns: updated,
            ..raw_record(2, &declared())
        }
        .encode_to_vec()
    };

    let store = MemoryStore::new();
    write(&store, &cfg, 7).await;
    assert_eq!(raw_bytes(&store).await, expected(7, 7));

    let mut all = read(&store).await;
    all.set_bloom_scope(BloomScope::All, OPTED_IN)
        .expect("opted in");
    write(&store, &all, 8).await;
    assert_eq!(raw_bytes(&store).await, expected(7, 8));
}

/// A value the setters would refuse cannot reach the wire another way. A
/// version-3 record carrying an invalid key decodes, but writing it back fails
/// the setter's shape rule; a key taken from another config fails the
/// declared-column rule, a key at the stored generation that differs from the
/// stored key is refused, and so is a stale config below the stored generation.
#[tokio::test]
async fn hand_built_layout_cannot_bypass_the_setter() {
    let store = MemoryStore::new();
    let mut five = raw_record(3, &declared());
    five.clustering_key = Some(sysproto::ClusteringKeyConfig {
        columns: names(&["a", "b", "c", "d", "e"]),
        bucket_width: SIX_HOURS,
        generation: 1,
    });
    put_raw(&store, &five).await;
    let decoded = read(&store).await;
    let err = layout_error(
        set_tenant_config(&store, &tenant(), &decoded, 2)
            .await
            .expect_err("five columns"),
    );
    assert_eq!(
        err,
        StorageLayoutConfigError::TooManyClusteringKeyColumns { count: 5, max: 4 }
    );
    assert_eq!(
        raw_bytes(&store).await,
        five.encode_to_vec(),
        "the refusal writes nothing"
    );

    let other = MemoryStore::new();
    let mut donor = base_config();
    donor
        .set_clustering_key(names(&["a"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set");
    let transplanted = TenantConfig {
        stored_clustering_key: donor.stored_clustering_key.clone(),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    let err = layout_error(
        set_tenant_config(&other, &tenant(), &transplanted, 1)
            .await
            .expect_err("a key naming a column this config does not declare"),
    );
    assert_eq!(
        err,
        StorageLayoutConfigError::UndeclaredClusteringKeyColumn { column: "a".into() }
    );

    let keyed = MemoryStore::new();
    let mut on_c = base_config();
    on_c.set_clustering_key(names(&["c"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set");
    write(&keyed, &on_c, 1).await;
    let err = layout_error(
        set_tenant_config(&keyed, &tenant(), &donor, 2)
            .await
            .expect_err("a second key at generation 1"),
    );
    assert_eq!(
        err,
        StorageLayoutConfigError::ClusteringKeyChangedWithoutGeneration { generation: 1 }
    );

    let stale = read(&keyed).await;
    let mut newer = stale.clone();
    newer
        .set_clustering_key(names(&["b"]), ClusteringBucketWidth::OneDay, OPTED_IN)
        .expect("set");
    write(&keyed, &newer, 3).await;
    let err = layout_error(
        set_tenant_config(&keyed, &tenant(), &stale, 4)
            .await
            .expect_err("a config read before the key moved to generation 2"),
    );
    assert_eq!(
        err,
        StorageLayoutConfigError::ClusteringGenerationRegressed {
            stored: 2,
            proposed: 1,
        }
    );
    assert_eq!(
        wire(&keyed).await.clustering_key,
        Some(sysproto::ClusteringKeyConfig {
            columns: names(&["b"]),
            bucket_width: ONE_DAY,
            generation: 2,
        })
    );
}

/// The gate does not require the stored generation plus one: two setter calls
/// on one config read at generation 1 are written at generation 3, with the
/// second call's key.
#[tokio::test]
async fn chained_setters_write_the_stored_generation_plus_two() {
    let store = MemoryStore::new();
    let mut first = base_config();
    first
        .set_clustering_key(names(&["a"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set");
    write(&store, &first, 1).await;

    let mut chained = read(&store).await;
    chained
        .set_clustering_key(names(&["b"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("second set");
    chained
        .set_clustering_key(names(&["c", "a"]), ClusteringBucketWidth::OneDay, OPTED_IN)
        .expect("third set");
    write(&store, &chained, 2).await;
    assert_eq!(
        wire(&store).await.clustering_key,
        Some(sysproto::ClusteringKeyConfig {
            columns: names(&["c", "a"]),
            bucket_width: ONE_DAY,
            generation: 3,
        })
    );
}

/// At the stored generation, a key with the stored columns and a different
/// bucket width is a changed key and is refused, and the stored record is left
/// as it was. The stored key rewritten unchanged at that generation is written.
#[tokio::test]
async fn a_bucket_width_change_needs_a_new_generation() {
    let store = MemoryStore::new();
    let mut hourly = base_config();
    hourly
        .set_clustering_key(names(&["a", "b"]), ClusteringBucketWidth::OneHour, OPTED_IN)
        .expect("set");
    write(&store, &hourly, 1).await;
    let before = raw_bytes(&store).await;

    let donor = MemoryStore::new();
    let mut six_hours = raw_record(3, &declared());
    six_hours.clustering_key = Some(sysproto::ClusteringKeyConfig {
        columns: names(&["a", "b"]),
        bucket_width: SIX_HOURS,
        generation: 1,
    });
    put_raw(&donor, &six_hours).await;
    let rewidened = read(&donor).await;
    assert_eq!(rewidened.clustering_generation(), 1);

    let err = layout_error(
        set_tenant_config(&store, &tenant(), &rewidened, 2)
            .await
            .expect_err("a new bucket width at generation 1"),
    );
    assert_eq!(
        err,
        StorageLayoutConfigError::ClusteringKeyChangedWithoutGeneration { generation: 1 }
    );
    assert_eq!(
        raw_bytes(&store).await,
        before,
        "the refusal writes nothing"
    );

    let mut unchanged = read(&store).await;
    unchanged.retention_ns = Some(7);
    write(&store, &unchanged, 3).await;
    let stored = wire(&store).await;
    assert_eq!(stored.retention_ns, Some(7));
    assert_eq!(
        stored.clustering_key,
        Some(sysproto::ClusteringKeyConfig {
            columns: names(&["a", "b"]),
            bucket_width: sysproto::ClusteringBucketWidth::OneHour as i32,
            generation: 1,
        })
    );
}

/// A version-3 record carrying a bloom scope value outside the enum decodes,
/// and writing that config back is refused with the value it carries and
/// writes nothing. Both a value above the highest known scope and a negative
/// one are checked.
#[tokio::test]
async fn an_unknown_bloom_scope_is_not_written_back() {
    for got in [42, -1] {
        let store = MemoryStore::new();
        let mut record = raw_record(3, &declared());
        record.bloom_scope = got;
        put_raw(&store, &record).await;
        let decoded = read(&store).await;
        assert_eq!(
            decoded.bloom_scope(),
            Err(StorageLayoutConfigError::UnknownBloomScope { got })
        );

        let err = layout_error(
            set_tenant_config(&store, &tenant(), &decoded, 2)
                .await
                .expect_err("an unknown bloom scope"),
        );
        assert_eq!(err, StorageLayoutConfigError::UnknownBloomScope { got });
        assert_eq!(
            raw_bytes(&store).await,
            record.encode_to_vec(),
            "the refusal writes nothing"
        );
    }
}

/// A decodable `typed_attr_columns` list that fails validation resolves to the
/// base columns, as the server's declared-column overlay treats it; a valid
/// list on the same path resolves to itself.
#[tokio::test]
async fn resolve_declared_columns_falls_back_on_an_invalid_list() {
    let base = vec![col("base", DeclaredColumnType::Str)];
    let store = MemoryStore::new();
    let invalid = vec![
        col("a", DeclaredColumnType::Str),
        col("a", DeclaredColumnType::I64),
    ];
    assert!(validate_typed_attr_columns(&invalid).is_err());
    put_raw(&store, &raw_record(2, &invalid)).await;
    let decoded = read(&store).await;
    assert_eq!(decoded.typed_attr_columns, Some(invalid));
    assert_eq!(resolve_declared_columns(Some(&decoded), &base), &base[..]);

    put_raw(&store, &raw_record(2, &declared())).await;
    let valid = read(&store).await;
    assert_eq!(
        resolve_declared_columns(Some(&valid), &base),
        &declared()[..]
    );
}

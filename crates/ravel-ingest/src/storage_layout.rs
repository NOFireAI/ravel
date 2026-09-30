//! A tenant's RLOG storage layout, its clustering key and bloom scope
//! (ADR-2135 decisions 1 and 5), as the log flush applies it.
//!
//! [`crate::indexed_fields::IndexedFieldsOverlay`] resolves a
//! [`StorageLayout`] from the same `TenantConfig` read that supplies the
//! tenant's indexed fields and declared typed columns, and caches it beside
//! them. `LogFlushCtx::run_flush` turns the cached layout into the
//! [`RlogWriter`](ravel_logseg::RlogWriter) builder arguments with
//! [`writer_layout`].

use ravel_catalog::{
    BloomScope, ClusteringBucketWidth, ClusteringKey, ClusteringKeyState, DeclaredColumnType,
    DeclaredTypedColumn, StorageLayoutConfigError, TenantConfig,
};
use ravel_logseg::BloomScope as RlogBloomScope;
use ravel_logseg::footer::{
    MAX_SORT_KEY_COLUMNS, SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType,
};

/// A tenant's clustering key and bloom scope as last resolved from its config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageLayout {
    /// Both accessors accepted the stored values, and a set key passes the
    /// writer's sort descriptor rules.
    Resolved {
        clustering: ClusteringKeyState,
        bloom_scope: BloomScope,
    },
    /// The stored layout cannot be written as stored. The flush writes the
    /// default layout and counts it.
    Unresolved(UnresolvedLayout),
}

/// Why a stored layout did not resolve.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum UnresolvedLayout {
    /// The config accessor refused the stored key or scope, for instance a key
    /// column missing from the declared typed columns.
    #[error(transparent)]
    Config(#[from] StorageLayoutConfigError),
    /// The key passed the config accessor but the writer would refuse it as a
    /// sort descriptor, for instance an empty column name.
    #[error("the clustering key is not a valid sort descriptor: {0}")]
    Descriptor(String),
}

impl Default for StorageLayout {
    /// The layout of a tenant with no config record, or a record without
    /// fields 13 and 14.
    fn default() -> Self {
        StorageLayout::Resolved {
            clustering: ClusteringKeyState::NeverSet,
            bloom_scope: BloomScope::All,
        }
    }
}

impl StorageLayout {
    /// Resolve `config`'s stored key and scope. The key is validated against
    /// `typed_columns`, the declared typed columns the flush stamps statistics
    /// from, so every column of a resolved key has a type in that list, and
    /// then against the writer's sort descriptor rules, so a resolved key never
    /// makes the writer refuse the object.
    pub(crate) fn resolve(config: &TenantConfig, typed_columns: &[DeclaredTypedColumn]) -> Self {
        let clustering = match config.clustering_key(typed_columns) {
            Ok(state) => state,
            Err(e) => return StorageLayout::Unresolved(e.into()),
        };
        if let ClusteringKeyState::Set(key) = &clustering
            && let Err(why) = sort_descriptor(key, typed_columns)
        {
            return StorageLayout::Unresolved(UnresolvedLayout::Descriptor(why));
        }
        match config.bloom_scope() {
            Ok(bloom_scope) => StorageLayout::Resolved {
                clustering,
                bloom_scope,
            },
            Err(e) => StorageLayout::Unresolved(e.into()),
        }
    }
}

/// The arguments one flush passes to `with_sort_descriptor` and
/// `with_bloom_scope`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriterLayout {
    pub(crate) descriptor: Option<SortDescriptor>,
    pub(crate) generation: u64,
    pub(crate) bloom_scope: RlogBloomScope,
}

impl WriterLayout {
    /// No descriptor, generation 0 and full bloom coverage: the builder calls
    /// that leave an object's bytes as a writer with no builder calls writes
    /// them.
    pub(crate) fn unkeyed() -> Self {
        WriterLayout {
            descriptor: None,
            generation: 0,
            bloom_scope: RlogBloomScope::All,
        }
    }
}

/// Map a cached layout to the writer's builder arguments. `typed_columns` is
/// the declared typed column list the flush stamps statistics from: it gives
/// each key column its type and gives the `undeclared` scope its names.
/// Returns `None` when the layout is unresolved or a set key fails
/// [`sort_descriptor`]; the caller then writes [`WriterLayout::unkeyed`].
pub(crate) fn writer_layout(
    layout: &StorageLayout,
    typed_columns: &[DeclaredTypedColumn],
) -> Option<WriterLayout> {
    let StorageLayout::Resolved {
        clustering,
        bloom_scope,
    } = layout
    else {
        return None;
    };
    let (descriptor, generation) = match clustering {
        ClusteringKeyState::NeverSet => (None, 0),
        ClusteringKeyState::Cleared { generation } => (None, *generation),
        ClusteringKeyState::Set(key) => (
            Some(sort_descriptor(key, typed_columns).ok()?),
            key.generation,
        ),
    };
    let bloom_scope = match bloom_scope {
        BloomScope::All => RlogBloomScope::All,
        BloomScope::Text => RlogBloomScope::Text,
        BloomScope::Undeclared => RlogBloomScope::Undeclared {
            declared: typed_columns.iter().map(|col| col.key.clone()).collect(),
        },
    };
    Some(WriterLayout {
        descriptor,
        generation,
        bloom_scope,
    })
}

/// `key` as the sort descriptor the writer records, each column typed from
/// `typed_columns`. Refuses a key column with no type there, and every
/// descriptor the writer's own check refuses at `finish`: a zero generation,
/// a column count outside 1..=[`MAX_SORT_KEY_COLUMNS`], an empty column name,
/// or a column named twice.
fn sort_descriptor(
    key: &ClusteringKey,
    typed_columns: &[DeclaredTypedColumn],
) -> Result<SortDescriptor, String> {
    if key.generation == 0 {
        return Err("a descriptor needs a nonzero clustering generation".into());
    }
    if key.columns.is_empty() || key.columns.len() > MAX_SORT_KEY_COLUMNS {
        return Err(format!(
            "{} key columns, not 1..={MAX_SORT_KEY_COLUMNS}",
            key.columns.len()
        ));
    }
    let mut key_columns: Vec<SortKeyColumn> = Vec::with_capacity(key.columns.len());
    for name in &key.columns {
        if name.is_empty() {
            return Err("key column name empty".into());
        }
        if key_columns.iter().any(|col| &col.name == name) {
            return Err(format!("key column {name:?} named twice"));
        }
        let Some(declared) = typed_columns.iter().find(|col| &col.key == name) else {
            return Err(format!("key column {name:?} has no declared type"));
        };
        key_columns.push(SortKeyColumn {
            name: name.clone(),
            ty: sort_key_type(declared.ty),
        });
    }
    Ok(SortDescriptor {
        bucket_width: sort_bucket_width(key.bucket_width),
        key_columns,
    })
}

fn sort_bucket_width(width: ClusteringBucketWidth) -> SortBucketWidth {
    match width {
        ClusteringBucketWidth::OneHour => SortBucketWidth::OneHour,
        ClusteringBucketWidth::SixHours => SortBucketWidth::SixHours,
        ClusteringBucketWidth::OneDay => SortBucketWidth::OneDay,
    }
}

fn sort_key_type(ty: DeclaredColumnType) -> SortKeyType {
    match ty {
        DeclaredColumnType::Str => SortKeyType::Str,
        DeclaredColumnType::I64 => SortKeyType::I64,
        DeclaredColumnType::Bool => SortKeyType::Bool,
        DeclaredColumnType::Bytes => SortKeyType::Bytes,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_logseg::{LogRecord, ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_types::logstream::{AttrValue, log_stream_id};

    use super::*;

    fn col(key: &str, ty: DeclaredColumnType) -> DeclaredTypedColumn {
        DeclaredTypedColumn {
            key: key.to_string(),
            ty,
        }
    }

    fn set(
        columns: &[&str],
        bucket_width: ClusteringBucketWidth,
        generation: u64,
    ) -> StorageLayout {
        StorageLayout::Resolved {
            clustering: ClusteringKeyState::Set(ClusteringKey {
                columns: columns.iter().map(|c| c.to_string()).collect(),
                bucket_width,
                generation,
            }),
            bloom_scope: BloomScope::All,
        }
    }

    #[test]
    fn bucket_widths_map_one_to_one() {
        let typed = [col("k", DeclaredColumnType::Str)];
        for (width, want) in [
            (ClusteringBucketWidth::OneHour, SortBucketWidth::OneHour),
            (ClusteringBucketWidth::SixHours, SortBucketWidth::SixHours),
            (ClusteringBucketWidth::OneDay, SortBucketWidth::OneDay),
        ] {
            let got = writer_layout(&set(&["k"], width, 4), &typed).expect("resolved");
            assert_eq!(got.descriptor.expect("descriptor").bucket_width, want);
            assert_eq!(got.generation, 4);
        }
    }

    #[test]
    fn key_types_come_from_the_typed_column_list() {
        let typed = [
            col("b", DeclaredColumnType::Bool),
            col("s", DeclaredColumnType::Str),
            col("y", DeclaredColumnType::Bytes),
            col("i", DeclaredColumnType::I64),
        ];
        let got = writer_layout(
            &set(&["i", "b", "y", "s"], ClusteringBucketWidth::OneHour, 1),
            &typed,
        )
        .expect("resolved");
        let key: Vec<(String, SortKeyType)> = got
            .descriptor
            .expect("descriptor")
            .key_columns
            .into_iter()
            .map(|c| (c.name, c.ty))
            .collect();
        assert_eq!(
            key,
            vec![
                ("i".to_string(), SortKeyType::I64),
                ("b".to_string(), SortKeyType::Bool),
                ("y".to_string(), SortKeyType::Bytes),
                ("s".to_string(), SortKeyType::Str),
            ]
        );
    }

    #[test]
    fn a_key_column_without_a_type_does_not_resolve() {
        let typed = [col("a", DeclaredColumnType::Str)];
        let layout = set(&["a", "missing"], ClusteringBucketWidth::OneHour, 2);
        assert_eq!(writer_layout(&layout, &typed), None);
    }

    #[test]
    fn never_set_and_cleared_write_no_descriptor() {
        let never = StorageLayout::default();
        assert_eq!(writer_layout(&never, &[]), Some(WriterLayout::unkeyed()));
        let cleared = StorageLayout::Resolved {
            clustering: ClusteringKeyState::Cleared { generation: 3 },
            bloom_scope: BloomScope::All,
        };
        let got = writer_layout(&cleared, &[]).expect("resolved");
        assert_eq!(got.descriptor, None);
        assert_eq!(got.generation, 3);
    }

    #[test]
    fn bloom_scopes_map_with_the_declared_names() {
        let typed = [
            col("region", DeclaredColumnType::Str),
            col("code", DeclaredColumnType::I64),
        ];
        let layout = |bloom_scope| StorageLayout::Resolved {
            clustering: ClusteringKeyState::NeverSet,
            bloom_scope,
        };
        let scope = |s| {
            writer_layout(&layout(s), &typed)
                .expect("resolved")
                .bloom_scope
        };
        assert_eq!(scope(BloomScope::All), RlogBloomScope::All);
        assert_eq!(scope(BloomScope::Text), RlogBloomScope::Text);
        assert_eq!(
            scope(BloomScope::Undeclared),
            RlogBloomScope::Undeclared {
                declared: vec!["region".to_string(), "code".to_string()],
            }
        );
    }

    #[test]
    fn an_unresolved_layout_maps_to_none() {
        let layout = StorageLayout::Unresolved(
            StorageLayoutConfigError::UnknownBloomScope { got: 9 }.into(),
        );
        assert_eq!(writer_layout(&layout, &[]), None);
    }

    /// Whether a writer given `descriptor` and `generation` finishes an object
    /// holding one record. The record carries no key column, which a keyed
    /// writer stores as absent key values.
    fn writer_accepts(descriptor: SortDescriptor, generation: u64) -> bool {
        let res = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
        let record = LogRecord {
            stream_id: log_stream_id(&res, "scope", "", &[]),
            stream_attrs: ravel_logseg::stream_attrs_bytes(&res, "scope", "", &[]),
            ts_ns: 1,
            observed_ts_ns: 1,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "b".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        };
        let identity = ObjectIdentity {
            tenant_hash: [1; 16],
            shard: 0,
            writer_id: [2; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity)
            .with_sort_descriptor(Some(descriptor), generation);
        writer.push(record).expect("push");
        writer.finish().is_ok()
    }

    fn str_key(columns: &[&str]) -> SortDescriptor {
        SortDescriptor {
            bucket_width: SortBucketWidth::OneHour,
            key_columns: columns
                .iter()
                .map(|c| SortKeyColumn {
                    name: c.to_string(),
                    ty: SortKeyType::Str,
                })
                .collect(),
        }
    }

    /// A key the config accessor lets through but the writer would refuse at
    /// `finish` maps to `None`, so the flush writes the unkeyed default rather
    /// than handing the writer a descriptor that abandons the object. Each case
    /// is also shown to be one the writer really refuses, and the accepted
    /// control one it really accepts.
    #[test]
    fn keys_the_writer_refuses_do_not_resolve() {
        let typed: Vec<DeclaredTypedColumn> = ["", "a", "b", "c", "d", "e"]
            .iter()
            .map(|k| col(k, DeclaredColumnType::Str))
            .collect();
        let cases: [(&[&str], u64); 5] = [
            (&[""], 1),
            (&["a", "a"], 1),
            (&["a", "b", "c", "d", "e"], 1),
            (&[], 1),
            (&["a"], 0),
        ];
        for (columns, generation) in cases {
            let layout = set(columns, ClusteringBucketWidth::OneHour, generation);
            assert_eq!(
                writer_layout(&layout, &typed),
                None,
                "{columns:?} at generation {generation}"
            );
            assert!(
                !writer_accepts(str_key(columns), generation),
                "the writer refuses {columns:?} at generation {generation}"
            );
        }
        let four = set(&["a", "b", "c", "d"], ClusteringBucketWidth::OneHour, 1);
        let got = writer_layout(&four, &typed).expect("four columns resolve");
        assert_eq!(got.descriptor, Some(str_key(&["a", "b", "c", "d"])));
        assert!(writer_accepts(str_key(&["a", "b", "c", "d"]), 1));
    }
}

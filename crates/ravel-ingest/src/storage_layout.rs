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
    BloomScope, ClusteringBucketWidth, ClusteringKeyState, DeclaredColumnType, DeclaredTypedColumn,
    StorageLayoutConfigError, TenantConfig,
};
use ravel_logseg::BloomScope as RlogBloomScope;
use ravel_logseg::footer::{SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType};

/// A tenant's clustering key and bloom scope as last resolved from its config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageLayout {
    /// Both accessors accepted the stored values.
    Resolved {
        clustering: ClusteringKeyState,
        bloom_scope: BloomScope,
    },
    /// The stored clustering key or bloom scope was refused by its accessor,
    /// for instance a key column missing from the declared typed columns. The
    /// flush writes the default layout and counts it.
    Unresolved(StorageLayoutConfigError),
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
    /// from, so every column of a resolved key has a type in that list.
    pub(crate) fn resolve(config: &TenantConfig, typed_columns: &[DeclaredTypedColumn]) -> Self {
        let clustering = match config.clustering_key(typed_columns) {
            Ok(state) => state,
            Err(e) => return StorageLayout::Unresolved(e),
        };
        match config.bloom_scope() {
            Ok(bloom_scope) => StorageLayout::Resolved {
                clustering,
                bloom_scope,
            },
            Err(e) => StorageLayout::Unresolved(e),
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
/// Returns `None` when the layout is unresolved or a key column has no type in
/// `typed_columns`; the caller then writes [`WriterLayout::unkeyed`].
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
        ClusteringKeyState::Set(key) => {
            let key_columns = key
                .columns
                .iter()
                .map(|name| {
                    typed_columns
                        .iter()
                        .find(|col| &col.key == name)
                        .map(|col| SortKeyColumn {
                            name: name.clone(),
                            ty: sort_key_type(col.ty),
                        })
                })
                .collect::<Option<Vec<_>>>()?;
            let descriptor = SortDescriptor {
                bucket_width: sort_bucket_width(key.bucket_width),
                key_columns,
            };
            (Some(descriptor), key.generation)
        }
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
    use ravel_catalog::ClusteringKey;

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
        let layout =
            StorageLayout::Unresolved(StorageLayoutConfigError::UnknownBloomScope { got: 9 });
        assert_eq!(writer_layout(&layout, &[]), None);
    }
}

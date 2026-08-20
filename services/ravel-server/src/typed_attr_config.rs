//! Per-tenant declared typed attribute columns for the `logs` SQL table
//! (ADR-0090 decision 1), as the deployment's CLI flags express them.
//!
//! The `logs` table exposes every attribute through one merged
//! `attrs: Map(Utf8, Utf8)` column, so a numeric or boolean predicate over an
//! attribute is a `CAST(attrs['k'] AS ...)` over a stringified value. ADR-0090
//! lets an operator declare a per-tenant set of attribute keys as native typed
//! columns; this module holds the CLI-derived half of that declaration, the
//! base a durable `TenantConfig.typed_attr_columns` override replaces per
//! tenant.
//!
//! The shape follows [`crate::postings_config`] exactly (which itself follows
//! `ravel_maintain::RetentionConfig`): a process-wide default plus per-tenant
//! overrides, tenant ids hashed at load so the validated config never stores a
//! raw id, and a total (not additive) override. The one difference from the
//! indexed-field flags is the unset default: there is no shipped default
//! declaration, because a declared column changes the SQL schema a tenant's
//! queries see, and shipping one by default would add columns to every
//! deployment's `logs` table without an operator asking for it. An unset
//! `--typed-attr-column` therefore means zero declared columns, the
//! zero-declaration base schema `ravel_sql::logs_schema` already returns.
//!
//! Validation is not reimplemented here: [`TypedAttrColumnConfig::from_policy`]
//! calls `ravel_catalog::validate_typed_attr_columns`, the SAME operator-input
//! gate `ravel_catalog::set_tenant_config` runs before a durable write, so a
//! CLI-declared list and a durably-written one are rejected on identical rules
//! (empty key, duplicate key, the same key declared with two types, and a
//! collision with one of the nine fixed logs SQL column names).
//!
//! Where this list reaches a query: [`crate::start`] builds one
//! [`TypedAttrColumnConfig`] as the CLI-derived base, then wraps it in
//! [`crate::declared_columns::TenantConfigDeclaredColumns`] (the cache-aside
//! `DeclaredColumnSource`, ADR-0090 decision 2) and installs it on the shared
//! `SqlExecutor`. Changing the CLI-derived base still requires a restart, the
//! same contract every other CLI-only per-tenant flag has; the durable
//! per-tenant override written by `ravel-cli typed-attr-column set` is the
//! no-restart path.

use std::collections::HashMap;

use ravel_catalog::{DeclaredColumnType, DeclaredTypedColumn, validate_typed_attr_columns};
use ravel_types::{TenantHash, TenantId};

/// The four type spellings `--typed-attr-column KEY:TYPE` accepts, matching
/// [`DeclaredColumnType`]'s four variants (ADR-0090 decision 1: `f64`, date,
/// and timestamp are deferred and have no spelling here). Named as a constant
/// so the flag's rejection message lists exactly what the parser accepts.
pub const DECLARED_TYPE_SPELLINGS: [&str; 4] = ["str", "i64", "bool", "bytes"];

/// Parse a declared-column type spelling, case-insensitively (`i64`, `I64`, and
/// `I64 ` after trimming all resolve the same). Returns `None` for an unknown
/// spelling, which the flag parser turns into a startup failure naming
/// [`DECLARED_TYPE_SPELLINGS`] rather than silently dropping the declaration.
pub fn parse_declared_column_type(spelling: &str) -> Option<DeclaredColumnType> {
    match spelling.trim().to_ascii_lowercase().as_str() {
        "str" => Some(DeclaredColumnType::Str),
        "i64" => Some(DeclaredColumnType::I64),
        "bool" => Some(DeclaredColumnType::Bool),
        "bytes" => Some(DeclaredColumnType::Bytes),
        _ => None,
    }
}

/// The type spelling for a [`DeclaredColumnType`], the inverse of
/// [`parse_declared_column_type`]. Used by startup logging and by
/// `ravel-cli typed-attr-column show`, so an operator reads back the same
/// spelling they wrote.
pub fn declared_column_type_spelling(ty: DeclaredColumnType) -> &'static str {
    match ty {
        DeclaredColumnType::Str => "str",
        DeclaredColumnType::I64 => "i64",
        DeclaredColumnType::Bool => "bool",
        DeclaredColumnType::Bytes => "bytes",
    }
}

/// A raw per-tenant declared-column policy as a deployment expresses it, before
/// validation and hashing. Tenant ids are plain strings here;
/// [`TypedAttrColumnConfig::from_policy`] hashes them at load, mirroring
/// [`crate::postings_config::IndexedFieldPolicy`].
#[derive(Debug, Clone, Default)]
pub struct TypedAttrColumnPolicy {
    /// The process-wide default declaration, in flag order. Empty means no
    /// declared columns by default (there is no shipped default list).
    pub default: Vec<DeclaredTypedColumn>,
    /// Per-tenant overrides: `(tenant_id, declaration)`, each in flag order.
    /// An override replaces the default for that tenant outright.
    pub tenants: Vec<(String, Vec<DeclaredTypedColumn>)>,
}

/// A malformed declaration, rejected at load rather than silently producing a
/// `logs` schema an operator did not declare.
///
/// The inner [`ravel_catalog::TypedAttrColumnError`] carries which rule failed;
/// this wrapper adds which declaration it failed in (`default`, or a tenant
/// id), the same shape [`crate::postings_config::IndexedFieldConfigError`] uses.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TypedAttrColumnConfigError {
    #[error("declared typed attribute columns for {scope} are invalid: {source}")]
    Invalid {
        /// `default` for the process-wide declaration, else the tenant id.
        scope: String,
        #[source]
        source: ravel_catalog::TypedAttrColumnError,
    },
}

/// The validated per-tenant declared-column configuration. Tenant ids are
/// hashed at construction, so this struct never holds a raw tenant id (the same
/// discipline [`crate::postings_config::IndexedFieldConfig`] keeps).
///
/// Declaration order is preserved verbatim and never re-sorted: ADR-0090
/// decision 3 appends declared columns to the `logs` schema in declaration
/// order, so re-sorting would reorder a tenant's SQL schema.
#[derive(Debug, Clone, Default)]
pub struct TypedAttrColumnConfig {
    default_columns: Vec<DeclaredTypedColumn>,
    tenants: HashMap<TenantHash, Vec<DeclaredTypedColumn>>,
}

impl TypedAttrColumnConfig {
    /// Validate a [`TypedAttrColumnPolicy`] and hash every tenant id. Every
    /// list, default and per-tenant, goes through
    /// `ravel_catalog::validate_typed_attr_columns`, so a bad declaration fails
    /// startup rather than silently declaring the wrong schema.
    ///
    /// A tenant named twice by repeated `--typed-attr-column-tenant` flags is
    /// not an error: the flags accumulate into that tenant's one ordered list
    /// (the caller's parser builds the accumulation), and the validation then
    /// catches a key repeated across those flags exactly as it catches one
    /// repeated within a single flag.
    pub fn from_policy(policy: TypedAttrColumnPolicy) -> Result<Self, TypedAttrColumnConfigError> {
        validate_scope("default", &policy.default)?;
        let mut tenants = HashMap::with_capacity(policy.tenants.len());
        for (id, columns) in policy.tenants {
            validate_scope(&id, &columns)?;
            tenants.insert(TenantId::new(&id).hash(), columns);
        }
        Ok(TypedAttrColumnConfig {
            default_columns: policy.default,
            tenants,
        })
    }

    /// The declaration that applies to one tenant: its override if set, else
    /// the process-wide default. This is the CLI-only resolution; the durable
    /// `TenantConfig.typed_attr_columns` override, when present, replaces it
    /// (see [`crate::declared_columns`]).
    pub fn columns_for(&self, tenant: &TenantHash) -> &[DeclaredTypedColumn] {
        self.tenants
            .get(tenant)
            .map(Vec::as_slice)
            .unwrap_or(&self.default_columns)
    }

    /// The process-wide default declaration, for introspection and startup
    /// logging.
    pub fn default_columns(&self) -> &[DeclaredTypedColumn] {
        &self.default_columns
    }

    /// Whether this configuration declares nothing at all: no default and no
    /// per-tenant override. The shipped state of a deployment that passes
    /// neither flag, and the state in which every tenant's `logs` table keeps
    /// its zero-declaration base schema unless a durable override says
    /// otherwise.
    pub fn declares_nothing(&self) -> bool {
        self.default_columns.is_empty() && self.tenants.is_empty()
    }

    /// A one-line `key:type,key:type` rendering of one tenant's resolved
    /// declaration, for startup logging.
    pub fn render(columns: &[DeclaredTypedColumn]) -> String {
        columns
            .iter()
            .map(|c| format!("{}:{}", c.key, declared_column_type_spelling(c.ty)))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Run the shared operator-input validation over one scope's declaration,
/// tagging the failure with the scope it came from.
fn validate_scope(
    scope: &str,
    columns: &[DeclaredTypedColumn],
) -> Result<(), TypedAttrColumnConfigError> {
    validate_typed_attr_columns(columns).map_err(|source| TypedAttrColumnConfigError::Invalid {
        scope: scope.to_string(),
        source,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_catalog::TypedAttrColumnError;

    fn col(key: &str, ty: DeclaredColumnType) -> DeclaredTypedColumn {
        DeclaredTypedColumn {
            key: key.to_string(),
            ty,
        }
    }

    #[test]
    fn type_spellings_round_trip_case_insensitively() {
        for spelling in DECLARED_TYPE_SPELLINGS {
            let ty = parse_declared_column_type(spelling).expect("known spelling");
            assert_eq!(declared_column_type_spelling(ty), spelling);
            let upper = spelling.to_ascii_uppercase();
            assert_eq!(
                parse_declared_column_type(&upper),
                Some(ty),
                "type spellings are case-insensitive"
            );
        }
        assert_eq!(parse_declared_column_type("f64"), None, "f64 is deferred");
        assert_eq!(parse_declared_column_type("int"), None);
    }

    #[test]
    fn an_unset_default_declares_nothing() {
        let config =
            TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy::default()).expect("valid");
        assert!(config.declares_nothing());
        assert!(
            config.columns_for(&TenantId::new("acme").hash()).is_empty(),
            "no shipped default declaration: an unset flag is zero declared columns"
        );
    }

    #[test]
    fn a_per_tenant_override_shadows_the_default_for_that_tenant_only() {
        let config = TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy {
            default: vec![col("dur_ms", DeclaredColumnType::I64)],
            tenants: vec![(
                "acme".to_string(),
                vec![col("ok", DeclaredColumnType::Bool)],
            )],
        })
        .expect("valid");
        assert_eq!(
            config.columns_for(&TenantId::new("acme").hash()),
            [col("ok", DeclaredColumnType::Bool)],
            "the override is total, not additive"
        );
        assert_eq!(
            config.columns_for(&TenantId::new("globex").hash()),
            [col("dur_ms", DeclaredColumnType::I64)]
        );
    }

    #[test]
    fn declaration_order_is_preserved() {
        let config = TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy {
            default: vec![
                col("z", DeclaredColumnType::Str),
                col("a", DeclaredColumnType::I64),
            ],
            tenants: Vec::new(),
        })
        .expect("valid");
        let keys: Vec<&str> = config
            .default_columns()
            .iter()
            .map(|c| c.key.as_str())
            .collect();
        assert_eq!(
            keys,
            ["z", "a"],
            "declaration order is the schema-append order and is never re-sorted"
        );
    }

    /// The four validation rules are `ravel_catalog`'s, reached through
    /// `from_policy`: an empty key, a duplicate key, a conflicting type, and a
    /// fixed-column collision, each tagged with the scope it failed in.
    #[test]
    fn the_catalog_validation_rules_apply_to_both_scopes() {
        let empty = TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy {
            default: vec![col("", DeclaredColumnType::Str)],
            tenants: Vec::new(),
        });
        assert!(matches!(
            empty,
            Err(TypedAttrColumnConfigError::Invalid { ref scope, source: TypedAttrColumnError::EmptyKey })
                if scope == "default"
        ));

        let dup = TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy {
            default: Vec::new(),
            tenants: vec![(
                "acme".to_string(),
                vec![
                    col("dur", DeclaredColumnType::I64),
                    col("dur", DeclaredColumnType::I64),
                ],
            )],
        });
        assert!(matches!(
            dup,
            Err(TypedAttrColumnConfigError::Invalid { ref scope, source: TypedAttrColumnError::DuplicateKey { ref key } })
                if scope == "acme" && key == "dur"
        ));

        let conflicting = TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy {
            default: vec![
                col("dur", DeclaredColumnType::I64),
                col("dur", DeclaredColumnType::Str),
            ],
            tenants: Vec::new(),
        });
        assert!(matches!(
            conflicting,
            Err(TypedAttrColumnConfigError::Invalid {
                source: TypedAttrColumnError::ConflictingType { .. },
                ..
            })
        ));

        let collision = TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy {
            default: vec![col("body", DeclaredColumnType::Str)],
            tenants: Vec::new(),
        });
        assert!(matches!(
            collision,
            Err(TypedAttrColumnConfigError::Invalid { source: TypedAttrColumnError::FixedColumnCollision { ref key }, .. })
                if key == "body"
        ));
    }

    #[test]
    fn render_reads_back_the_flag_spelling() {
        assert_eq!(
            TypedAttrColumnConfig::render(&[
                col("dur", DeclaredColumnType::I64),
                col("payload", DeclaredColumnType::Bytes),
            ]),
            "dur:i64,payload:bytes"
        );
    }
}

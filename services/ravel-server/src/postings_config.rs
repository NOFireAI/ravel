//! Per-tenant POSTINGS indexed-field configuration (ADR-0049 decision 3).
//!
//! POSTINGS indexing is opt-in per field, never automatic: every indexed field
//! makes each write maintain a postings list, so indexing a field no query
//! filters on spends ingest and storage cost for no query benefit (ADR-0049
//! decision 3). This module holds the operator-editable list of attribute names
//! the log writer indexes, defaulting to a small sensible set and overridable
//! per tenant.
//!
//! The shape follows the retention configuration exactly
//! (`ravel_maintain::RetentionConfig`, driven by `--retention-default` /
//! `--retention-tenant`): a default plus per-tenant overrides, tenant ids
//! hashed at load so the validated config never stores a raw id. The one
//! difference is that an unset default falls back to a shipped list
//! ([`DEFAULT_INDEXED_FIELDS`]) rather than to "off": absence of a POSTINGS
//! section is always legal (decision 5), but the feature ships useful by
//! default, and an operator turns it off for a tenant with an explicit empty
//! override (`--indexed-field-tenant acme=`).
//!
//! Where this list reaches the writer: [`crate::start`] builds one
//! [`IndexedFieldConfig`] as the CLI-derived base/default, then wraps it in
//! `ravel_ingest::IndexedFieldsOverlay` (ADR-0079) before handing it to the
//! log ingest router. The overlay resolves each flush's field list from a
//! per-tenant cache-aside read of the durable `TenantConfig.indexed_fields`
//! override, falling back to this base's own `fields_for(tenant_hash)` when
//! no durable override exists, and passes the result to
//! `RlogWriter::with_indexed_fields`. Changing the CLI-derived base/default
//! still requires editing `--indexed-field-default` / `--indexed-field-tenant`
//! and restarting, the same restart-to-change contract every other
//! CLI-only per-tenant flag has (`--retention-tenant`, `--tenant-token`);
//! the durable per-tenant override, once a write path for it exists, is the
//! no-restart path ADR-0066 decision 6 originally promised.

use std::collections::{HashMap, HashSet};

use ravel_types::{TenantHash, TenantId};

/// The shipped default indexed-field list, applied to every tenant with no
/// override when the operator configured no default of their own.
///
/// `service.name` and `k8s.namespace.name` are the two ADR-0049 decision 3
/// names directly: the service and the Kubernetes namespace are the
/// two coordinates almost every log query filters on first. The third,
/// `http.status_code`, is the highest-value non-identity equality predicate in
/// log triage ("show me the 5xx"), it is named in ADR-0049 decision 3's own
/// example list, and unlike the two resource coordinates it is commonly a
/// per-record attribute, so it exercises the index in the ordinary case rather
/// than only once a merged-view resource column exists for it. All three are
/// bounded-cardinality by nature, which is what keeps the default list within
/// the per-field distinct-value cap (decision 4).
pub const DEFAULT_INDEXED_FIELDS: [&str; 3] =
    ["service.name", "k8s.namespace.name", "http.status_code"];

/// A raw per-tenant indexed-field policy as a deployment expresses it, before
/// validation and hashing. Tenant ids are plain strings here;
/// [`IndexedFieldConfig::from_policy`] hashes them at load, mirroring
/// `ravel_maintain::RetentionPolicy`.
#[derive(Debug, Clone, Default)]
pub struct IndexedFieldPolicy {
    /// The default list, or `None` to fall back to [`DEFAULT_INDEXED_FIELDS`].
    /// `Some(vec![])` is a deliberate empty default (index nothing by default);
    /// only an unset default (`None`) takes the shipped list.
    pub default: Option<Vec<String>>,
    /// Per-tenant overrides: `(tenant_id, field_list)`. An empty list is a
    /// valid explicit opt-out for that tenant.
    pub tenants: Vec<(String, Vec<String>)>,
}

/// A malformed indexed-field list, rejected at load rather than silently
/// producing a writer that indexes the wrong set (or panics on an empty name).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IndexedFieldConfigError {
    #[error("indexed-field list for {tenant} contains an empty field name")]
    EmptyField { tenant: String },
    #[error("indexed-field list for {tenant} names '{field}' more than once")]
    DuplicateField { tenant: String, field: String },
}

/// The validated per-tenant indexed-field configuration. Tenant ids are hashed
/// at construction, so this struct never holds a raw tenant id (the same
/// discipline `RetentionConfig` keeps).
#[derive(Debug, Clone)]
pub struct IndexedFieldConfig {
    default_fields: Vec<String>,
    tenants: HashMap<TenantHash, Vec<String>>,
}

impl Default for IndexedFieldConfig {
    /// The shipped configuration: [`DEFAULT_INDEXED_FIELDS`] for every tenant,
    /// no overrides. Built without going through [`Self::from_policy`] so it
    /// needs no `expect` on a list known to validate.
    fn default() -> Self {
        IndexedFieldConfig {
            default_fields: DEFAULT_INDEXED_FIELDS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            tenants: HashMap::new(),
        }
    }
}

impl IndexedFieldConfig {
    /// Validate an [`IndexedFieldPolicy`] and hash every tenant id. An unset
    /// default takes the shipped list; every list is checked for empty and
    /// duplicate names so a bad list fails startup rather than silently
    /// indexing the wrong set.
    pub fn from_policy(policy: IndexedFieldPolicy) -> Result<Self, IndexedFieldConfigError> {
        let default_fields = match policy.default {
            Some(list) => validate_list("default", list)?,
            None => DEFAULT_INDEXED_FIELDS
                .iter()
                .map(|s| s.to_string())
                .collect(),
        };
        let mut tenants = HashMap::with_capacity(policy.tenants.len());
        for (id, list) in policy.tenants {
            let list = validate_list(&id, list)?;
            tenants.insert(TenantId::new(&id).hash(), list);
        }
        Ok(IndexedFieldConfig {
            default_fields,
            tenants,
        })
    }

    /// The indexed-field list that applies to one tenant: its override if set
    /// (which may be empty, an explicit opt-out), else the default list. Called
    /// once per flush and handed straight to `RlogWriter::with_indexed_fields`.
    pub fn fields_for(&self, tenant: &TenantHash) -> &[String] {
        self.tenants
            .get(tenant)
            .map(Vec::as_slice)
            .unwrap_or(&self.default_fields)
    }

    /// The default list, for introspection and startup logging.
    pub fn default_fields(&self) -> &[String] {
        &self.default_fields
    }
}

/// Bridge to the ingest layer's resolver trait. `ravel-ingest`
/// owns the trait so it need not depend on this crate's config types; the log
/// ingest router calls `fields_for` per flush. The inherent `fields_for` above
/// wins method resolution here, so this is not a self-recursive call; it only
/// copies the resolved slice into the owned `Vec` the trait returns.
impl ravel_ingest::LogIndexedFields for IndexedFieldConfig {
    fn fields_for(&self, tenant: &TenantHash) -> Vec<String> {
        IndexedFieldConfig::fields_for(self, tenant).to_vec()
    }
}

/// Reject an empty or duplicate field name in one list. Order is preserved: the
/// writer treats the list as a set, but keeping insertion order makes the
/// startup log line read back the way the operator wrote it.
fn validate_list(tenant: &str, list: Vec<String>) -> Result<Vec<String>, IndexedFieldConfigError> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(list.len());
    for field in &list {
        if field.is_empty() {
            return Err(IndexedFieldConfigError::EmptyField {
                tenant: tenant.to_string(),
            });
        }
        if !seen.insert(field.as_str()) {
            return Err(IndexedFieldConfigError::DuplicateField {
                tenant: tenant.to_string(),
                field: field.clone(),
            });
        }
    }
    Ok(list)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_logseg::writer::WriteStats;
    use ravel_logseg::{
        AttrValue, FieldSel, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader,
        RlogWriter, ScanStats, stream_attrs_bytes,
    };
    use ravel_types::logstream::log_stream_id;

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [9u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    /// One record on a single fixed stream, carrying the given per-record
    /// attributes. The stream's resource carries `service.name = "svc"`, so the
    /// stream identity is well formed; the per-record attributes are what the
    /// writer splits into dynamic columns and therefore what postings can key
    /// on in this checkout.
    fn record(ts: i64, attrs: &[(&str, AttrValue)]) -> LogRecord {
        let resource = [(
            "service.name".to_string(),
            AttrValue::Str("svc".to_string()),
        )];
        LogRecord {
            stream_id: log_stream_id(&resource, "", "", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "", "", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "body".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        }
    }

    /// Small blocks so a handful of records span several blocks and a prune has
    /// something to drop.
    fn cfg() -> RlogConfig {
        RlogConfig {
            block_target_records: 2,
            ..RlogConfig::default()
        }
    }

    fn scan_for(obj: &[u8], field: &str, value: &str) -> ScanStats {
        let reader = RlogReader::new(obj, &cfg()).expect("open");
        let prune = [Predicate::Equals {
            field: FieldSel::Attr(field.to_string()),
            value: AttrValue::Str(value.to_string()),
        }];
        let (_rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        stats
    }

    // ---- configuration resolution ------------------------------------------

    #[test]
    fn unset_default_falls_back_to_the_shipped_list() {
        let config = IndexedFieldConfig::from_policy(IndexedFieldPolicy::default()).expect("valid");
        let tenant = TenantId::new("acme").hash();
        assert_eq!(config.fields_for(&tenant), DEFAULT_INDEXED_FIELDS);
        assert_eq!(
            IndexedFieldConfig::default().default_fields(),
            DEFAULT_INDEXED_FIELDS
        );
    }

    #[test]
    fn a_configured_default_replaces_the_shipped_list() {
        let config = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: Some(vec!["a".to_string(), "b".to_string()]),
            tenants: Vec::new(),
        })
        .expect("valid");
        assert_eq!(config.fields_for(&TenantId::new("acme").hash()), ["a", "b"]);
    }

    #[test]
    fn a_per_tenant_override_shadows_the_default_for_that_tenant_only() {
        let config = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: Some(vec!["default.field".to_string()]),
            tenants: vec![("acme".to_string(), vec!["acme.field".to_string()])],
        })
        .expect("valid");
        assert_eq!(
            config.fields_for(&TenantId::new("acme").hash()),
            ["acme.field"]
        );
        assert_eq!(
            config.fields_for(&TenantId::new("globex").hash()),
            ["default.field"]
        );
    }

    #[test]
    fn an_empty_override_is_an_explicit_opt_out() {
        let config = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: None,
            tenants: vec![("acme".to_string(), Vec::new())],
        })
        .expect("valid");
        assert!(config.fields_for(&TenantId::new("acme").hash()).is_empty());
        // A different tenant still gets the shipped default.
        assert_eq!(
            config.fields_for(&TenantId::new("globex").hash()),
            DEFAULT_INDEXED_FIELDS
        );
    }

    #[test]
    fn an_empty_or_duplicate_field_name_is_rejected() {
        let empty = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: Some(vec![String::new()]),
            tenants: Vec::new(),
        });
        assert!(matches!(
            empty,
            Err(IndexedFieldConfigError::EmptyField { .. })
        ));
        let dup = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: None,
            tenants: vec![(
                "acme".to_string(),
                vec!["service.name".to_string(), "service.name".to_string()],
            )],
        });
        assert!(matches!(
            dup,
            Err(IndexedFieldConfigError::DuplicateField { field, .. }) if field == "service.name"
        ));
    }

    // ---- the resolved list drives the writer -------------------------------

    /// The acceptance test (this exact name): a tenant whose
    /// resolved list is absent (empty) indexes nothing, so the object carries no
    /// POSTINGS section and every write-side POSTINGS counter round-trips as
    /// zero. Absence is always legal (ADR-0049 decision 5).
    #[test]
    fn absent_field_list_indexes_nothing_and_flags_round_trip() {
        let config = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: None,
            tenants: vec![("acme".to_string(), Vec::new())],
        })
        .expect("valid");
        let fields = config.fields_for(&TenantId::new("acme").hash());
        assert!(fields.is_empty(), "the acme override resolves to nothing");

        let mut writer = RlogWriter::new(cfg(), identity()).with_indexed_fields(fields.to_vec());
        for i in 0..6i64 {
            writer
                .push(record(
                    i,
                    &[(
                        "http.status_code",
                        AttrValue::Str(format!("{}", 200 + i % 2)),
                    )],
                ))
                .expect("push");
        }
        let (obj, stats) = writer.finish_with_stats().expect("finish");

        // Every POSTINGS counter is zero: no section, no fields, no distinct
        // values, no capped fields, no bytes.
        assert_eq!(
            stats,
            WriteStats::default(),
            "an absent field list must leave every POSTINGS counter at its default"
        );
        // And the object still scans: probing an unindexed field prunes nothing
        // (the postings channel has no entry), so every block survives.
        let scanned = scan_for(&obj, "http.status_code", "200");
        assert_eq!(
            scanned.blocks_after_postings, scanned.blocks_after_skip,
            "with no postings, the prune arm drops no block"
        );
        assert!(!scanned.postings_degraded);
    }

    /// A tenant with a configured list indexes exactly those fields and no
    /// others: the configured field prunes, a field present in the object but
    /// absent from the list does not.
    #[test]
    fn a_configured_list_indexes_exactly_those_fields_and_no_others() {
        let config = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: None,
            tenants: vec![("acme".to_string(), vec!["http.status_code".to_string()])],
        })
        .expect("valid");
        let fields = config.fields_for(&TenantId::new("acme").hash());
        assert_eq!(fields, ["http.status_code"]);

        // Twelve records over six two-record blocks (block target 2, so block
        // j holds records 2j and 2j+1). `http.status_code` cycles three
        // values every record (s{i%3}); block j therefore holds
        // s{(2j)%3} and s{(2j+1)%3}, so "s0" lands in blocks 0, 1, 3, 4 (a
        // proper subset of the six) and is absent from blocks 2 and 5.
        // `http.method` is NOT in the configured list, and it is deliberately
        // NOT alternated per record (m{i%2} would put both values in every
        // block, since each block holds one even and one odd `i` -- that
        // shape cannot discriminate "not pruned" from "pruned but every block
        // matched anyway"). Instead it takes "m0" for i in 0..6 and "m1" for
        // i in 6..12, a change that lands exactly on a block boundary, so
        // "m0" occupies blocks 0, 1, 2 only -- also a proper subset, not
        // every block.
        let mut writer = RlogWriter::new(cfg(), identity()).with_indexed_fields(fields.to_vec());
        for i in 0..12i64 {
            let method = if i < 6 { "m0" } else { "m1" };
            writer
                .push(record(
                    i,
                    &[
                        ("http.status_code", AttrValue::Str(format!("s{}", i % 3))),
                        ("http.method", AttrValue::Str(method.to_string())),
                    ],
                ))
                .expect("push");
        }
        let (obj, stats) = writer.finish_with_stats().expect("finish");
        assert_eq!(
            stats.postings_indexed_fields, 1,
            "exactly one field is indexed"
        );

        // The configured field prunes: probing one of its three values drops
        // the blocks that hold only the other two. Pin down the exact block
        // layout above so a future change to the fixture cannot silently
        // regress this test back to vacuity.
        let indexed = scan_for(&obj, "http.status_code", "s0");
        assert_eq!(indexed.blocks_total, 6, "twelve records at two per block");
        assert_eq!(
            indexed.blocks_after_postings, 4,
            "\"s0\" occupies exactly blocks 0, 1, 3, 4"
        );
        assert!(
            indexed.blocks_after_postings < indexed.blocks_after_skip,
            "the configured field must prune: {} !< {}",
            indexed.blocks_after_postings,
            indexed.blocks_after_skip
        );

        // The unconfigured field does not prune: it has no posting list, so its
        // prune arm resolves to nothing and every candidate block survives.
        // "m0" occupies only blocks 0, 1, 2 of 6 (see the layout comment
        // above), so this equality is not trivially true: it would fail if a
        // regression started pruning on an unconfigured field, since that
        // would drop the equality to 3 blocks instead of 6.
        let unindexed = scan_for(&obj, "http.method", "m0");
        assert_eq!(unindexed.blocks_total, 6);
        assert_eq!(
            unindexed.blocks_after_postings, unindexed.blocks_after_skip,
            "an unconfigured field must not prune"
        );
        assert!(!unindexed.postings_degraded);
    }

    /// A field configured but absent from the object indexes nothing and does
    /// not error: no record carries the key, so it has no column, so it emits no
    /// posting list, and the write still succeeds.
    #[test]
    fn a_configured_field_absent_from_the_object_indexes_nothing_without_error() {
        let config = IndexedFieldConfig::from_policy(IndexedFieldPolicy {
            default: None,
            tenants: vec![("acme".to_string(), vec!["http.status_code".to_string()])],
        })
        .expect("valid");
        let fields = config.fields_for(&TenantId::new("acme").hash());

        // Records carry only `http.method`, never the configured
        // `http.status_code`.
        let mut writer = RlogWriter::new(cfg(), identity()).with_indexed_fields(fields.to_vec());
        for i in 0..6i64 {
            writer
                .push(record(
                    i,
                    &[("http.method", AttrValue::Str(format!("m{}", i % 2)))],
                ))
                .expect("push");
        }
        // No error, and the configured-but-absent field contributes no postings.
        let (_obj, stats) = writer
            .finish_with_stats()
            .expect("a missing field must not error");
        assert_eq!(stats.postings_indexed_fields, 0);
        assert_eq!(stats.postings_distinct_total, 0);
        assert_eq!(
            stats.postings_bytes, 0,
            "no column means no POSTINGS section"
        );
    }
}

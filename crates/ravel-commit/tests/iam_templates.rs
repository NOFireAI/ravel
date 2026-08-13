//! Structural validation of the shipped IAM policy templates
//! (`deploy/iam/*.json`) against real object-key shapes.
//!
//! Every `s3:prefix` (ListBucket condition) and resource-ARN key pattern in
//! the templates is checked with the same `StringLike` wildcard semantics
//! IAM uses, against representative keys built with `ravel-commit`'s own
//! key constructors -- never against string literals typed by hand. A
//! pattern that stops matching any real key shape (a key-layout change) or
//! a discovery-listing regression (ADR-0072 decision 5) fails this test
//! instead of reaching production.
//!
//! A handful of policy prefixes name keyspaces `ravel-commit` has no
//! constructor for (`idem/`, `prov`, `catalog/`, `admission/`, `sys/*`):
//! those objects are built by other crates. This test explicitly skips
//! them (see `OUT_OF_SCOPE_PATTERNS`) rather than fabricate a literal key
//! that would defeat the point of testing against real constructors.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_commit::keys::{
    commit_key, compaction_record_key, data_key, del_prefix, erasure_completion_key,
    erasure_request_key, l1_part_key, maint_cursor_key, retention_tombstone_key,
    rewrite_record_key,
};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

/// Legal-hold shard for `Signal::Audit` (ADR-0055 section 3 as amended by
/// EL-6, issue #763). Hardcoded rather than imported from `ravel-maintain`:
/// that crate is out of this test's scope, and the shard number is a
/// frozen part of the object-key contract, not an implementation detail.
const AUDIT_HOLD_SHARD: u32 = 0;
/// Query-audit shard for `Signal::Audit` (same amendment).
const QUERY_AUDIT_SHARD: u32 = 1;

const ALL_SIGNALS: [Signal; 6] = [
    Signal::Metrics,
    Signal::Logs,
    Signal::Spans,
    Signal::Profiles,
    Signal::Alerts,
    Signal::Audit,
];

fn test_tenant() -> TenantHash {
    TenantHash([0xab; 16])
}

fn hash16() -> &'static str {
    "0123456789abcdef"
}

/// Every real object key `ravel-commit`'s key constructors can produce,
/// across every signal (including `Signal::Audit` at both its shards).
/// This is the ground truth the policy patterns are checked against.
fn representative_keys() -> Vec<String> {
    let tenant = test_tenant();
    let writer_id = Uuid::from_u128(1);
    let request_id = Uuid::from_u128(2);
    let content_hash = [0u8; 32];
    let ingest_hour_bucket = 0;

    let mut keys = Vec::new();
    for &signal in &ALL_SIGNALS {
        let shards: &[u32] = if signal == Signal::Audit {
            &[AUDIT_HOLD_SHARD, QUERY_AUDIT_SHARD]
        } else {
            &[0]
        };
        for &shard in shards {
            keys.push(
                data_key(&tenant, signal, shard, writer_id, 1, 1, &content_hash).expect("data_key"),
            );
            keys.push(
                commit_key(&tenant, signal, shard, ingest_hour_bucket, writer_id, 1, 1)
                    .expect("commit_key"),
            );
            keys.push(
                l1_part_key(
                    &tenant,
                    signal,
                    shard,
                    ingest_hour_bucket,
                    hash16(),
                    0,
                    hash16(),
                )
                .expect("l1_part_key"),
            );
            keys.push(
                compaction_record_key(&tenant, signal, shard, ingest_hour_bucket, hash16())
                    .expect("compaction_record_key"),
            );
            keys.push(
                retention_tombstone_key(&tenant, signal, shard, ingest_hour_bucket)
                    .expect("retention_tombstone_key"),
            );
            keys.push(
                rewrite_record_key(&tenant, signal, shard, ingest_hour_bucket, hash16())
                    .expect("rewrite_record_key"),
            );
            keys.push(maint_cursor_key(&tenant, signal, shard).expect("maint_cursor_key"));
        }
        // del/ erasure-request and erasure-completion keys are real
        // ravel-commit shapes (ADR-0064), but no shipped IAM policy grants
        // any action on del/ today -- see the crate-level doc comment.
        keys.push(del_prefix(&tenant, signal));
        keys.push(erasure_request_key(&tenant, signal, request_id).expect("erasure_request_key"));
        keys.push(
            erasure_completion_key(&tenant, signal, request_id).expect("erasure_completion_key"),
        );
    }
    keys
}

/// Policy prefixes naming keyspaces `ravel-commit` has no key constructor
/// for. Matched as plain substrings against the raw pattern text.
const OUT_OF_SCOPE_PATTERNS: &[&str] = &["idem/", "/prov", "catalog/", "admission/", "sys/"];

fn is_out_of_scope(pattern: &str) -> bool {
    OUT_OF_SCOPE_PATTERNS
        .iter()
        .any(|marker| pattern.contains(marker))
}

/// Translate an AWS `StringLike` glob (`*` = any sequence, no other
/// metacharacters in these templates) into an anchored regex.
fn glob_to_regex(pattern: &str) -> regex::Regex {
    let mut regex_src = String::from("^");
    for part in pattern.split('*') {
        if !regex_src.ends_with('^') {
            regex_src.push_str(".*");
        }
        regex_src.push_str(&regex::escape(part));
    }
    regex_src.push('$');
    regex::Regex::new(&regex_src).expect("valid glob-derived regex")
}

fn glob_matches(pattern: &str, candidate: &str) -> bool {
    glob_to_regex(pattern).is_match(candidate)
}

struct Policy {
    role: &'static str,
    statements: serde_json::Value,
}

fn load_policy(role: &'static str) -> Policy {
    let path = format!(
        "{}/../../deploy/iam/{role}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let json: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    let statements = json["Statement"].clone();
    assert!(statements.is_array(), "{path}: Statement is not an array");
    Policy { role, statements }
}

/// `s3:prefix` patterns from every `s3:ListBucket` statement's
/// `Condition.StringLike` block.
fn list_prefix_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let action = &stmt["Action"];
        let is_list = action == "s3:ListBucket"
            || action
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v == "s3:ListBucket"));
        if !is_list {
            continue;
        }
        if let Some(patterns) = stmt["Condition"]["StringLike"]["s3:prefix"].as_array() {
            for p in patterns {
                out.push(p.as_str().expect("s3:prefix entry is a string").to_string());
            }
        }
    }
    out
}

/// Resource-ARN key patterns (bucket prefix stripped) from every
/// non-ListBucket statement (`GetObject`, `PutObject`, `DeleteObject`, ...).
fn resource_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let action = &stmt["Action"];
        let is_list_only = action == "s3:ListBucket";
        if is_list_only {
            continue;
        }
        let resources = match &stmt["Resource"] {
            serde_json::Value::Array(a) => a.clone(),
            v @ serde_json::Value::String(_) => vec![v.clone()],
            _ => continue,
        };
        for r in resources {
            let r = r.as_str().expect("Resource entry is a string");
            if let Some(key_pattern) = r.strip_prefix("arn:aws:s3:::my-ravel-bucket/") {
                out.push(key_pattern.to_string());
            }
        }
    }
    out
}

/// Resource-ARN key patterns (bucket prefix stripped) from every statement
/// whose `Action` grants `s3:PutObject` --- the writes that go through
/// `KmsRoutingStore` and can select a per-tenant key. Delete/Get/Deny
/// statements are excluded: they never route (reads and deletes delegate to
/// the default store unconditionally, see `kms_routing.rs`).
fn put_resource_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let grants_put = match &stmt["Action"] {
            serde_json::Value::String(s) => s == "s3:PutObject",
            serde_json::Value::Array(a) => a.iter().any(|v| v == "s3:PutObject"),
            _ => false,
        };
        if !grants_put {
            continue;
        }
        let resources = match &stmt["Resource"] {
            serde_json::Value::Array(a) => a.clone(),
            v @ serde_json::Value::String(_) => vec![v.clone()],
            _ => continue,
        };
        for r in resources {
            let r = r.as_str().expect("Resource entry is a string");
            if let Some(key_pattern) = r.strip_prefix("arn:aws:s3:::my-ravel-bucket/") {
                out.push(key_pattern.to_string());
            }
        }
    }
    out
}

const ROLES_WITH_DISCOVERY: [&str; 3] = ["gateway", "query", "maintain"];
const ALL_ROLES: [&str; 4] = ["gateway", "query", "maintain", "admin"];

/// Roles that PUT routed `t/<hash>/...` objects yet are deliberately kept
/// Decrypt-only, exempt from the routed-write KMS grant rule below. Today only
/// `admin`: ADR-0055 keeps it `GetObject`-only for routine data and forbids it
/// `kms:GenerateDataKey*` (see `admin_has_no_kms_generate_data_key`) so a
/// leaked Admin credential cannot mint ciphertext under tenant keys it has no
/// write role for. Admin's narrow routed PUTs (`t/*/*/c/*` via
/// `ravel-cli commit reconstruct`, the `t/*/u/*` audit prefix) therefore fail
/// closed under `--tenant-kms-config` -- a known operational gap documented in
/// docs/guides/operations.md, not a bug this test should paper over by
/// demanding the grant. Listing the role here (rather than skipping the whole
/// admin policy) keeps the exemption explicit and load-bearing: the test still
/// asserts an exempt role actually writes routed objects, so the exemption
/// cannot rot into masking a future regression.
const ROUTED_WRITE_EXEMPT_ROLES: [&str; 1] = ["admin"];

/// Roles whose IAM policy writes tenant data through `KmsRoutingStore`
/// (issues #929, #972): Gateway's ingest PUTs, Maintain's compaction/rewrite
/// PUTs, and Query's catalog-fold (`t/<hash>/catalog/.../snap|HEAD|idx`) and
/// query-audit (`t/<hash>/u/...`) PUTs all land under `t/<hash>/...` and, once
/// `--tenant-kms-config` routes that tenant to its own key, require
/// `kms:GenerateDataKey*`/`kms:Encrypt` on that key or the PUT fails closed.
/// This is a hand-maintained list; `roles_writing_routed_objects_have_kms_grant`
/// is the anti-drift guard that derives the same requirement from each policy's
/// own PUT patterns and `KmsRoutingStore`'s real routing predicate.
const WRITE_ROLES: [&str; 3] = ["gateway", "maintain", "query"];

/// `kms:*` action strings appearing anywhere in `policy`'s statements
/// (`Action` as a bare string or an array), regardless of statement Sid.
fn kms_actions(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let actions: Vec<String> = match &stmt["Action"] {
            serde_json::Value::String(s) => vec![s.clone()],
            serde_json::Value::Array(a) => a
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            _ => continue,
        };
        out.extend(actions.into_iter().filter(|a| a.starts_with("kms:")));
    }
    out
}

/// Gateway/Maintain/Query write tenant data objects through `KmsRoutingStore`
/// (ADR-0062 decision 1a): a configured tenant's PUT is delegated to a
/// per-tenant `S3Store` built with that tenant's SSE-KMS key, which needs
/// `kms:GenerateDataKey*` (and `kms:Encrypt`) on the caller's IAM policy or
/// the PUT fails closed with `AccessDenied` -- issues #929, #972. Flip any
/// role's `*TenantKms` statement (or narrow its Action list to drop
/// `kms:GenerateDataKey*`) in `deploy/iam/{gateway,maintain,query}.json` and
/// this test fails.
#[test]
fn write_roles_have_kms_generate_data_key() {
    for role in WRITE_ROLES {
        let policy = load_policy(role);
        let actions = kms_actions(&policy);
        assert!(
            actions.iter().any(|a| a.starts_with("kms:GenerateDataKey")),
            "{role}: policy is missing kms:GenerateDataKey* -- its ingest/compaction/\
             catalog-fold PUTs under t/<hash>/... will fail closed against a \
             --tenant-kms-config tenant (issues #929, #972). Found kms actions: {actions:?}"
        );
    }
}

/// Anti-drift guard (issue #972): for EVERY role, derive whether it PUTs any
/// object that routes through `KmsRoutingStore`'s per-tenant key straight from
/// the policy's own `s3:PutObject` resource patterns and the crate's real
/// routing predicate (`ravel_object_store::routes_through_tenant_key`), then
/// require that role to carry both `kms:Encrypt` and `kms:GenerateDataKey*`.
///
/// Unlike `write_roles_have_kms_generate_data_key`, this test hardcodes no role
/// list and no key strings: it reads whatever the policy grants PutObject on
/// and asks the routing code itself whether that shape routes. So it fails in
/// two independent drift directions --- a policy gaining a new routed PUT class
/// without the KMS grant, or `routes_through_tenant_key` widening to cover a
/// keyspace some role already PUTs --- either of which would otherwise ship a
/// write that fails closed under `--tenant-kms-config`.
///
/// Demonstrated non-vacuous: before #972, `deploy/iam/query.json` PUT
/// `t/*/catalog/*/{snap/*,HEAD,idx/*}` and `t/*/u/*` (all routed) while its
/// `QueryTenantKms` statement granted only `kms:Decrypt`; this assertion failed
/// naming `query` until `kms:Encrypt`/`kms:GenerateDataKey*` were added.
#[test]
fn roles_writing_routed_objects_have_kms_grant() {
    for role in ALL_ROLES {
        let policy = load_policy(role);
        let routed: Vec<String> = put_resource_key_patterns(&policy)
            .into_iter()
            .filter(|p| ravel_object_store::routes_through_tenant_key(p))
            .collect();
        if ROUTED_WRITE_EXEMPT_ROLES.contains(&role) {
            // Keep the exemption honest: an exempt role that no longer writes
            // any routed object should be dropped from the list, not left to
            // silently mask a later regression. `admin_has_no_kms_generate_data_key`
            // separately pins that this exempt role stays Decrypt-only.
            assert!(
                !routed.is_empty(),
                "{role}: listed in ROUTED_WRITE_EXEMPT_ROLES but PUTs no routed \
                 object -- remove it from the exemption list"
            );
            continue;
        }
        if routed.is_empty() {
            continue;
        }
        let actions = kms_actions(&policy);
        assert!(
            actions.iter().any(|a| a.starts_with("kms:GenerateDataKey")),
            "{role}: PUTs routed object class(es) {routed:?} but policy lacks \
             kms:GenerateDataKey* -- those writes fail closed under \
             --tenant-kms-config (issue #972). Found kms actions: {actions:?}"
        );
        assert!(
            actions.iter().any(|a| a == "kms:Encrypt"),
            "{role}: PUTs routed object class(es) {routed:?} but policy lacks \
             kms:Encrypt -- those writes fail closed under --tenant-kms-config \
             (issue #972). Found kms actions: {actions:?}"
        );
    }
}

/// Admin is GetObject-only per ADR-0055 (its only tenant-scoped writes are
/// narrow, create-only control paths, not routine data-object PUTs), so it
/// must never carry `kms:GenerateDataKey*`: granting it would widen the
/// compromise blast radius the ADR-0072 key-policy posture exists to limit.
/// Flip `deploy/iam/admin.json`'s `AdminTenantKms` statement to include
/// `kms:GenerateDataKey*` and this test fails.
#[test]
fn admin_has_no_kms_generate_data_key() {
    let policy = load_policy("admin");
    let actions = kms_actions(&policy);
    assert!(
        !actions.iter().any(|a| a.starts_with("kms:GenerateDataKey")),
        "admin: policy must not carry kms:GenerateDataKey* (Decrypt-only per ADR-0055). \
         Found kms actions: {actions:?}"
    );
}

/// Every role reads SSE-KMS objects at some point (fold, query resolve,
/// compaction inputs, `ravel-cli` inspection) and decryption on GET is
/// server-side but still requires `kms:Decrypt` on the caller's IAM policy
/// (ADR-0062 section 1a's "reads never select a key" is about client-side
/// key *selection*, not about needing no grant at all). Remove any role's
/// `kms:Decrypt` action in `deploy/iam/*.json` and this test fails, naming
/// that role.
#[test]
fn every_role_has_kms_decrypt() {
    for role in ALL_ROLES {
        let policy = load_policy(role);
        let actions = kms_actions(&policy);
        assert!(
            actions.iter().any(|a| a == "kms:Decrypt"),
            "{role}: policy is missing kms:Decrypt -- its reads of SSE-KMS objects \
             under a --tenant-kms-config tenant will fail closed (issue #929). \
             Found kms actions: {actions:?}"
        );
    }
}

#[test]
fn discovery_prefix_admitted_for_every_discovering_role() {
    for role in ROLES_WITH_DISCOVERY {
        let policy = load_policy(role);
        let patterns = list_prefix_patterns(&policy);
        assert!(
            patterns.iter().any(|p| glob_matches(p, "t/")),
            "{role}: ListBucket s3:prefix condition {patterns:?} does not admit \
             the bare delimited discovery prefix \"t/\" (ravel-maintain's \
             discover_tenants calls list_delimited(\"t/\"))"
        );
    }
}

#[test]
fn every_in_scope_policy_pattern_matches_a_real_key_shape() {
    let keys = representative_keys();
    for role in ALL_ROLES {
        let policy = load_policy(role);
        let mut patterns = list_prefix_patterns(&policy);
        patterns.extend(resource_key_patterns(&policy));
        for pattern in patterns {
            if is_out_of_scope(&pattern) {
                continue;
            }
            if pattern == "t/" {
                // The discovery-only literal: not a per-key pattern, checked
                // by discovery_prefix_admitted_for_every_discovering_role.
                continue;
            }
            let matched = keys.iter().any(|k| glob_matches(&pattern, k));
            assert!(
                matched,
                "{}: pattern {pattern:?} matches no key shape produced by \
                 ravel-commit's key constructors",
                policy.role
            );
        }
    }
}

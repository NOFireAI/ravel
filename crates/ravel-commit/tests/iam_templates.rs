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

/// Legal-hold shard for `Signal::Audit` (ADR-0055 section 3).
/// Hardcoded rather than imported from `ravel-maintain`:
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

/// The object-ARN prefix (and bare bucket ARN) the shipped templates use. A
/// delete-capable statement whose `Resource` does not start with
/// `BUCKET_KEY_PREFIX` reaches outside the configured bucket (or is the bare
/// `"*"`, or names a different bucket); it is surfaced as a test failure, never
/// stripped to nothing.
const BUCKET_ARN: &str = "arn:aws:s3:::my-ravel-bucket";
const BUCKET_KEY_PREFIX: &str = "arn:aws:s3:::my-ravel-bucket/";

/// Translate an IAM policy glob into an anchored regex. IAM resolves two
/// wildcard characters inside both `Action` and `Resource` strings: `*` matches
/// any sequence and `?` matches exactly one character; every other character is
/// literal. This once handled only `*`; the shipped templates carry no `?`, so
/// widening it changes no existing match, but a `?` smuggled into an action or
/// resource pattern is now resolved as the wildcard IAM treats it as instead of
/// being escaped to a literal `\?` that quietly matches nothing.
fn glob_to_regex(pattern: &str) -> regex::Regex {
    let mut regex_src = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex_src.push_str(".*"),
            '?' => regex_src.push('.'),
            other => regex_src.push_str(&regex::escape(&other.to_string())),
        }
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

/// The complete set of statement keys any guard in this file reads. Sid,
/// Effect, Action, and Resource are read directly (see `statement_actions`,
/// `resource_key_patterns`, `delete_key_patterns`, `kms_statement_resources`,
/// ...); Condition is read for the `s3:prefix` ListBucket block
/// (`list_prefix_patterns`). Nothing else is examined by any guard.
///
/// This list is the guard's contract: a statement carrying any key outside it
/// is one no guard reasons about, so it must fail closed at `load_policy`
/// rather than be silently skipped (issue #1346).
const HANDLED_STATEMENT_KEYS: &[&str] = &["Sid", "Effect", "Action", "Resource", "Condition"];

/// The complete set of `Condition` operators any guard in this file reads.
/// Only `list_prefix_patterns` reads a Condition at all, and only its
/// `StringLike` block. A statement whose Condition names any other operator --
/// a different comparison such as `StringNotLike`, or a set-qualified form such
/// as `ForAnyValue:StringLike` -- carries a constraint no guard reasons about,
/// so it must fail closed at `validate_statement` rather than pass with its
/// Condition unexamined (issue #1346).
const HANDLED_CONDITION_OPERATORS: &[&str] = &["StringLike"];

/// The complete set of `Condition` keys any guard in this file reads, under a
/// handled operator. Only `s3:prefix` is inspected (by `list_prefix_patterns`);
/// any other condition key (`s3:delimiter`, `aws:SourceIp`, ...) is a constraint
/// no guard reads and must fail closed rather than sit unexamined.
const HANDLED_CONDITION_KEYS: &[&str] = &["s3:prefix"];

/// Keys that describe a statement shape these guards deliberately cannot
/// reason about: `NotAction`/`NotResource` invert the set the Action/Resource
/// guards inspect (so a statement carrying them is permissive in exactly the
/// direction the guard reads, while the guard sees an empty positive set and
/// passes it), and `Principal`/`NotPrincipal` scope a statement to identities
/// this file models nothing about. Each is rejected with a message saying so,
/// rather than lumped in with an unrecognized-typo key.
const NEGATED_OR_PRINCIPAL_KEYS: &[&str] =
    &["NotAction", "NotResource", "NotPrincipal", "Principal"];

/// Fail closed on any statement shape the guards in this file do not fully
/// understand. This is the single choke point every shipped-template guard
/// passes through (`load_policy` calls it for each statement), so a new
/// unhandled shape is rejected once here instead of slipping past a guard that
/// only reads the fields it happens to know.
///
/// Rejects, naming the `Sid` and the offending key or field:
/// - a statement that is not a JSON object;
/// - `NotAction`/`NotResource`/`NotPrincipal`/`Principal` (negated or
///   principal-scoped: the guard cannot reason about them);
/// - any other key outside `HANDLED_STATEMENT_KEYS` (e.g. a `Resources` typo);
/// - an `Effect` that is neither `Allow` nor `Deny`;
/// - an `Action` or `Resource` that is neither a string nor a non-empty array
///   of strings (an empty array is vacuously "all strings", so it slipped past
///   as a valid set the guards then derived nothing from);
/// - a missing `Resource` key (the exact shape #1346 records being skipped:
///   the resource guards read `stmt["Resource"]`, find `Null`, and drop the
///   statement as having nothing to check);
/// - a `Condition` whose sub-shape is anything other than the one block a guard
///   reads: it must be a JSON object of handled operators
///   (`HANDLED_CONDITION_OPERATORS`, today `StringLike`), each mapping handled
///   condition keys (`HANDLED_CONDITION_KEYS`, today `s3:prefix`) to a string or
///   non-empty array of strings. A different operator, a set-qualified operator,
///   or an unhandled key is a constraint `list_prefix_patterns` never reads, so
///   it fails closed here rather than contributing nothing silently.
fn validate_statement(role: &str, index: usize, stmt: &serde_json::Value) -> Result<(), String> {
    let obj = stmt
        .as_object()
        .ok_or_else(|| format!("{role}: statement #{index} is not a JSON object: {stmt:?}"))?;
    let sid = obj
        .get("Sid")
        .and_then(|v| v.as_str())
        .unwrap_or("<no Sid>");

    for key in obj.keys() {
        if NEGATED_OR_PRINCIPAL_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "{role}/{sid}: statement uses {key:?}; the guards in this file \
                 cannot reason about negated or principal-scoped statements \
                 (they read only the positive Action/Resource sets), so a policy \
                 carrying it must be rejected rather than silently passed"
            ));
        }
        if !HANDLED_STATEMENT_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "{role}/{sid}: statement uses key {key:?}, which no guard in this \
                 file handles (handled keys: {HANDLED_STATEMENT_KEYS:?}); it must \
                 fail closed rather than sit unexamined"
            ));
        }
    }

    match obj.get("Effect").and_then(|v| v.as_str()) {
        Some(e) if e.eq_ignore_ascii_case("Allow") || e.eq_ignore_ascii_case("Deny") => {}
        other => {
            return Err(format!(
                "{role}/{sid}: Effect is neither \"Allow\" nor \"Deny\": {other:?}"
            ));
        }
    }

    if !is_string_or_string_array(obj.get("Action")) {
        return Err(format!(
            "{role}/{sid}: Action is neither a string nor an array of strings: {:?}",
            obj.get("Action")
        ));
    }

    match obj.get("Resource") {
        None => {
            return Err(format!(
                "{role}/{sid}: statement has no Resource key -- a statement with no \
                 Resource is the exact shape the resource guards skip (they read \
                 stmt[\"Resource\"], find Null, and treat it as nothing to check)"
            ));
        }
        resource if !is_string_or_string_array(resource) => {
            return Err(format!(
                "{role}/{sid}: Resource is neither a string nor a non-empty array \
                 of strings: {resource:?}"
            ));
        }
        _ => {}
    }

    if let Some(condition) = obj.get("Condition") {
        validate_condition(role, sid, condition)?;
    }

    Ok(())
}

/// Validate a statement's `Condition` sub-shape against the exact operators and
/// keys the guards read (`HANDLED_CONDITION_OPERATORS` / `HANDLED_CONDITION_KEYS`).
/// The Condition must be a JSON object; every operator in it must be handled;
/// every key under a handled operator must be handled and map to a string or
/// non-empty array of strings (the shape `list_prefix_patterns` reads). Anything
/// else -- an unhandled operator such as `StringNotLike` or a set-qualified
/// `ForAnyValue:StringLike`, an unhandled key such as `s3:delimiter`, or a
/// non-object Condition -- is a constraint no guard reasons about and is rejected
/// by name (issue #1346). Without this, a ListBucket statement carrying such a
/// Condition passes validation and `list_prefix_patterns` then finds no
/// `["StringLike"]["s3:prefix"]` array and silently contributes nothing.
fn validate_condition(role: &str, sid: &str, condition: &serde_json::Value) -> Result<(), String> {
    let cond_obj = condition
        .as_object()
        .ok_or_else(|| format!("{role}/{sid}: Condition is not a JSON object: {condition:?}"))?;
    for (operator, keys) in cond_obj {
        if !HANDLED_CONDITION_OPERATORS.contains(&operator.as_str()) {
            return Err(format!(
                "{role}/{sid}: Condition uses operator {operator:?}, which no guard \
                 in this file reads (handled operators: {HANDLED_CONDITION_OPERATORS:?}); \
                 a different or set-qualified operator such as StringNotLike or \
                 ForAnyValue:StringLike must fail closed rather than sit unexamined"
            ));
        }
        let key_obj = keys.as_object().ok_or_else(|| {
            format!("{role}/{sid}: Condition operator {operator:?} is not a JSON object: {keys:?}")
        })?;
        for (cond_key, value) in key_obj {
            if !HANDLED_CONDITION_KEYS.contains(&cond_key.as_str()) {
                return Err(format!(
                    "{role}/{sid}: Condition operator {operator:?} names key {cond_key:?}, \
                     which no guard in this file reads (handled keys: {HANDLED_CONDITION_KEYS:?}); \
                     it must fail closed rather than sit unexamined"
                ));
            }
            if !is_string_or_string_array(Some(value)) {
                return Err(format!(
                    "{role}/{sid}: Condition {operator:?}.{cond_key:?} is neither a string \
                     nor a non-empty array of strings: {value:?}"
                ));
            }
        }
    }
    Ok(())
}

/// True only for a JSON string or a non-empty array whose every element is a
/// string. An empty array is rejected: `iter().all(..)` is vacuously true on it,
/// so `"Action": []` / `"Resource": []` used to pass and every downstream guard
/// then derived an empty set and skipped the statement (issue #1346).
fn is_string_or_string_array(value: Option<&serde_json::Value>) -> bool {
    match value {
        Some(serde_json::Value::String(_)) => true,
        Some(serde_json::Value::Array(a)) => {
            !a.is_empty() && a.iter().all(serde_json::Value::is_string)
        }
        _ => false,
    }
}

/// Run `validate_statement` over every statement in a policy's `Statement`
/// array, returning the first rejection. This is the real per-statement
/// validation loop `load_policy` runs on every shipped template; extracting it
/// lets the regression tests call THIS function (and drive it through
/// `build_policy`, load_policy's own body) rather than a re-typed copy of the
/// loop that would keep passing if the call in `load_policy` were deleted.
fn validate_policy_statements(role: &str, statements: &serde_json::Value) -> Result<(), String> {
    let array = statements
        .as_array()
        .ok_or_else(|| format!("{role}: Statement is not an array"))?;
    for (index, stmt) in array.iter().enumerate() {
        validate_statement(role, index, stmt)?;
    }
    Ok(())
}

/// The parse-and-validate body shared by `load_policy` and the regression tests
/// that need to drive the real validation call site with a synthetic policy
/// (`load_policy` itself only reads the fixed `deploy/iam/*.json` paths). Panics,
/// naming `source`, if `Statement` is not an array or any statement is rejected.
fn build_policy(role: &'static str, source: &str, json: &serde_json::Value) -> Policy {
    let statements = json["Statement"].clone();
    assert!(statements.is_array(), "{source}: Statement is not an array");
    if let Err(msg) = validate_policy_statements(role, &statements) {
        panic!("{source}: {msg}");
    }
    Policy { role, statements }
}

fn load_policy(role: &'static str) -> Policy {
    let path = format!(
        "{}/../../deploy/iam/{role}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let json: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    build_policy(role, &path, &json)
}

/// IAM action names are case-insensitive: a statement granting
/// `KMS:GenerateDataKey*` grants exactly what `kms:GenerateDataKey*` grants.
/// Every action comparison in this file goes through this helper (or
/// `action_has_prefix`), in both directions. A case-sensitive positive check
/// only fails loudly, but a case-sensitive negative check reports a role
/// unprivileged while it holds the grant.
fn action_eq(action: &str, expected: &str) -> bool {
    action.eq_ignore_ascii_case(expected)
}

/// Case-insensitive prefix match for action names, so `kms:GenerateDataKey`
/// selects `KMS:GenerateDataKey*` too. Indexed with `get` rather than a slice
/// so an action name whose bytes do not split on a char boundary at
/// `prefix.len()` returns false instead of panicking.
fn action_has_prefix(action: &str, prefix: &str) -> bool {
    action
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// Every action name in a statement's `Action` (a bare string or an array),
/// with the policy's own capitalization preserved so a failure message quotes
/// what the template actually says.
fn statement_actions(stmt: &serde_json::Value) -> Vec<String> {
    match &stmt["Action"] {
        serde_json::Value::String(s) => vec![s.clone()],
        serde_json::Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

/// `s3:prefix` patterns from every `s3:ListBucket` statement's
/// `Condition.StringLike` block.
fn list_prefix_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let is_list = statement_actions(stmt)
            .iter()
            .any(|a| action_eq(a, "s3:ListBucket"));
        if !is_list {
            continue;
        }
        // IAM allows a single `s3:prefix` value as a bare string or an array;
        // read both so a bare-string prefix is not silently skipped (its shape
        // is checked by validate_condition, which accepts both). Any other JSON
        // shape here was already rejected at load_policy.
        match &stmt["Condition"]["StringLike"]["s3:prefix"] {
            serde_json::Value::String(s) => out.push(s.clone()),
            serde_json::Value::Array(patterns) => {
                for p in patterns {
                    out.push(p.as_str().expect("s3:prefix entry is a string").to_string());
                }
            }
            _ => {}
        }
    }
    out
}

/// Resource-ARN key patterns (bucket prefix stripped) from every
/// non-ListBucket statement (`GetObject`, `PutObject`, `DeleteObject`, ...).
fn resource_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let is_list_only = stmt["Action"]
            .as_str()
            .is_some_and(|a| action_eq(a, "s3:ListBucket"));
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
        let grants_put = statement_actions(stmt)
            .iter()
            .any(|a| action_eq(a, "s3:PutObject"));
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

/// The literal S3 actions that delete a stored object, ASCII-folded. IAM action
/// names are case-insensitive, so every comparison folds both sides.
const DELETE_ACTION_NAMES: [&str; 2] = ["s3:deleteobject", "s3:deleteobjectversion"];

/// True when an IAM `Action` grants (in an Allow) or withdraws (in a Deny) the
/// capability to delete a stored object: one of the literal delete actions, or
/// ANY IAM wildcard pattern (`*`/`?`) that matches one of them. This is the
/// predicate the delete guard must use instead of an exact `s3:DeleteObject`
/// match. `"*"`, `"s3:*"`, `"s3:Delete*"`, `"s3:DeleteObject*"`, and a
/// mis-cased `"S3:DELETEOBJECT"` all grant the delete a bare-literal check
/// misses, and missing one lets a delete-everywhere statement sit unexamined
/// while the guard reports Admin scoped to the scratch prefix (issue #1372, the
/// seventh instance of the skip-what-you-don't-recognize class #1346 tracks).
fn action_is_delete_capable(action: &str) -> bool {
    let folded = action.to_ascii_lowercase();
    if DELETE_ACTION_NAMES.contains(&folded.as_str()) {
        return true;
    }
    // Only a wildcard pattern can match beyond an exact name; a plain unrelated
    // action (`s3:DeleteObjectTagging`, `s3:DeleteBucket`) is correctly not
    // delete-capable. Fold before globbing so `S3:Delete*` is resolved too.
    if folded.contains(IAM_WILDCARDS) {
        return DELETE_ACTION_NAMES
            .iter()
            .any(|name| glob_matches(&folded, name));
    }
    false
}

/// Strip the configured bucket's object-ARN prefix from a delete statement's
/// `Resource`, or fail the test naming the role, `Sid`, and resource. A
/// delete-capable statement whose resource is `"*"`, names a different bucket,
/// or is otherwise not bucket-relative grants (or denies) delete outside the
/// configured bucket, and dropping it silently is the same mistake #1346
/// records for `NotResource`: the entry you do not understand is exactly the
/// one that must not vanish.
fn require_bucket_relative_delete_resource(role: &str, sid: &str, resource: &str) -> String {
    resource
        .strip_prefix(BUCKET_KEY_PREFIX)
        .map(str::to_string)
        .unwrap_or_else(|| {
            panic!(
                "{role}/{sid}: delete-capable statement names resource {resource:?}, \
                 which is not bucket-relative to {BUCKET_ARN:?} -- a delete grant or \
                 deny on \"*\" or on another bucket reaches outside the configured \
                 bucket and must not be silently dropped"
            )
        })
}

/// Resource key patterns (bucket prefix stripped) from every statement whose
/// `Effect` is `effect` and whose `Action` is delete-capable (see
/// `action_is_delete_capable`).
///
/// One function for both effects, because the two sides differ only in which
/// `Effect` they select. The Resource-shape panic, the `Sid` fallback, and the
/// bucket-relative requirement have to be identical on both, and a rule
/// tightened on one copy and not the other is the failure this file keeps
/// repeating.
///
/// `"Allow"` returns the delete capability a role actually holds. The
/// `DenyDeleteProtected` block names delete actions too, but it withdraws
/// capability rather than granting it (and an explicit IAM `Deny` always wins),
/// so it is selected separately by `"Deny"`, which returns the keys that block
/// protects (ADR-0055 §3). Either way a delete-capable statement whose Resource
/// is not bucket-relative fails the test rather than being dropped: a Deny
/// naming a delete on `"*"` would deny the qualification probe's own delete and
/// one on another bucket protects nothing here.
fn delete_key_patterns(policy: &Policy, effect: &str) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let has_effect = stmt["Effect"]
            .as_str()
            .is_some_and(|e| e.eq_ignore_ascii_case(effect));
        if !has_effect {
            continue;
        }
        let touches_delete = statement_actions(stmt)
            .iter()
            .any(|a| action_is_delete_capable(a));
        if !touches_delete {
            continue;
        }
        let sid = stmt["Sid"].as_str().unwrap_or("<no Sid>");
        let resources = match &stmt["Resource"] {
            serde_json::Value::Array(a) => a.clone(),
            v @ serde_json::Value::String(_) => vec![v.clone()],
            other => panic!(
                "{}/{sid}: delete-capable {effect} has a Resource that is neither a \
                 string nor an array: {other:?}",
                policy.role
            ),
        };
        for r in resources {
            let r = r.as_str().expect("Resource entry is a string");
            out.push(require_bucket_relative_delete_resource(policy.role, sid, r));
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

/// Roles whose IAM policy writes tenant data through `KmsRoutingStore`:
/// Gateway's ingest PUTs, Maintain's compaction/rewrite
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
///
/// Selection is case-insensitive, so `KMS:GenerateDataKey*` is returned; the
/// strings themselves keep the template's capitalization, so a caller's
/// failure message shows what the policy said rather than a normalized form
/// the operator would then grep for in vain. Callers compare with `action_eq`
/// or `action_has_prefix` rather than against these strings directly.
fn kms_actions(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        out.extend(
            statement_actions(stmt)
                .into_iter()
                .filter(|a| action_has_prefix(a, "kms:")),
        );
    }
    out
}

/// Sid and parsed `Resource` list for every statement carrying a `kms:`
/// action. Both resource guards below go through this one selection rule, so
/// a change to it cannot reach one guard and miss the other.
///
/// Panics on a `Resource` that is neither a string nor an array, as the
/// guards did inline: a malformed template must fail the test rather than be
/// skipped as "no resources to check".
fn kms_statement_resources(policy: &Policy) -> Vec<(String, Vec<String>)> {
    let role = policy.role;
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        if !statement_actions(stmt)
            .iter()
            .any(|a| action_has_prefix(a, "kms:"))
        {
            continue;
        }
        let sid = stmt["Sid"].as_str().unwrap_or("<no Sid>").to_string();
        let resources: Vec<String> = match &stmt["Resource"] {
            serde_json::Value::String(s) => vec![s.clone()],
            serde_json::Value::Array(a) => a
                .iter()
                .map(|v| v.as_str().expect("Resource entry is a string").to_string())
                .collect(),
            other => {
                panic!("{role}/{sid}: Resource is neither a string nor an array: {other:?}")
            }
        };
        out.push((sid, resources));
    }
    out
}

/// Gateway/Maintain/Query write tenant data objects through `KmsRoutingStore`
/// (ADR-0062 decision 1a): a configured tenant's PUT is delegated to a
/// per-tenant `S3Store` built with that tenant's SSE-KMS key, which needs
/// `kms:GenerateDataKey*` (and `kms:Encrypt`) on the caller's IAM policy or
/// the PUT fails closed with `AccessDenied`. Flip any
/// role's `*TenantKms` statement (or narrow its Action list to drop
/// `kms:GenerateDataKey*`) in `deploy/iam/{gateway,maintain,query}.json` and
/// this test fails.
#[test]
fn write_roles_have_kms_generate_data_key() {
    for role in WRITE_ROLES {
        let policy = load_policy(role);
        let actions = kms_actions(&policy);
        assert!(
            actions
                .iter()
                .any(|a| action_has_prefix(a, "kms:GenerateDataKey")),
            "{role}: policy is missing kms:GenerateDataKey* -- its ingest/compaction/\
             catalog-fold PUTs under t/<hash>/... will fail closed against a \
             --tenant-kms-config tenant. Found kms actions: {actions:?}"
        );
    }
}

/// Anti-drift guard: for EVERY role, derive whether it PUTs any
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
/// Non-vacuous by construction: a role whose policy PUTs routed object classes
/// such as `t/*/catalog/*/{snap/*,HEAD,idx/*}` or `t/*/u/*` while its
/// tenant-KMS statement grants only `kms:Decrypt` makes this assertion fail,
/// naming that role until `kms:Encrypt`/`kms:GenerateDataKey*` are added.
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
            actions
                .iter()
                .any(|a| action_has_prefix(a, "kms:GenerateDataKey")),
            "{role}: PUTs routed object class(es) {routed:?} but policy lacks \
             kms:GenerateDataKey* -- those writes fail closed under \
             --tenant-kms-config. Found kms actions: {actions:?}"
        );
        assert!(
            actions.iter().any(|a| action_eq(a, "kms:Encrypt")),
            "{role}: PUTs routed object class(es) {routed:?} but policy lacks \
             kms:Encrypt -- those writes fail closed under --tenant-kms-config. \
             Found kms actions: {actions:?}"
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
        !actions
            .iter()
            .any(|a| action_has_prefix(a, "kms:GenerateDataKey")),
        "admin: policy must not carry kms:GenerateDataKey* (Decrypt-only per ADR-0055). \
         Found kms actions: {actions:?}"
    );
}

/// The delete-grant guard's body, factored out so the regression fixtures run
/// the real assertion on synthetic policies rather than a re-typed copy of it
/// (the same pattern the KMS `assert_kms_resource_*` helpers use). The delete
/// set is derived first, so a delete-capable Allow that is out-of-bucket or
/// wildcard-actioned fails inside `delete_key_patterns` before any
/// protected-block check, and a synthetic fixture need not carry a Deny block
/// to make the guard fire for the right reason.
fn assert_admin_delete_grant_is_scratch_only(policy: &Policy) {
    let deletes = delete_key_patterns(policy, "Allow");
    assert_eq!(
        deletes,
        vec!["sys/qualify/*".to_string()],
        "{}: the only delete-capable Allow must be the qualification scratch \
         prefix sys/qualify/* (so `store qualify` can exercise the delete probe); \
         found {deletes:?}",
        policy.role
    );

    // Whatever the grant list is, it must reach no real tenant object under
    // t/**. representative_keys() are all t/<hash>/... shapes, so a delete
    // pattern matching any of them would grant delete on tenant data.
    let tenant_keys = representative_keys();
    for pattern in &deletes {
        for key in &tenant_keys {
            assert!(
                !glob_matches(pattern, key),
                "{}: delete grant {pattern:?} reaches tenant key {key:?}",
                policy.role
            );
        }
    }

    // ...and it must not cover any key the same policy denies delete on. Each
    // DenyDeleteProtected pattern is instantiated into a concrete key (every
    // `*` becomes a literal segment) and the grant glob must not match it.
    let protected = delete_key_patterns(policy, "Deny");
    assert!(
        !protected.is_empty(),
        "{}: DenyDeleteProtected names no delete-protected keys -- the \
         disjointness check below would pass having examined nothing",
        policy.role
    );
    for grant in &deletes {
        for prot in &protected {
            let sample = prot.replace('*', "x");
            assert!(
                !glob_matches(grant, &sample),
                "{}: delete grant {grant:?} covers protected key {sample:?} \
                 (from DenyDeleteProtected pattern {prot:?})",
                policy.role
            );
        }
    }
}

/// Admin's only `s3:DeleteObject` grant is the transient conformance scratch
/// prefix `sys/qualify/*`, so `ravel-cli store qualify` can run ADR-0050's
/// delete-visibility probe (which deletes a key under `sys/qualify/<run-id>/`)
/// on a fresh bucket without failing closed. Admin holds no delete on tenant
/// data (`t/**`) or on any key the same policy's `DenyDeleteProtected` block
/// covers, so ADR-0055's property "Admin never deletes tenant data or a
/// protected key" still holds. Widen the grant beyond that one prefix, or drop
/// it so qualification fails the delete probe, and this test fails.
#[test]
fn admin_delete_grant_is_qualify_scratch_only() {
    let policy = load_policy("admin");
    assert_admin_delete_grant_is_scratch_only(&policy);
}

/// The pre-fix Allow side of `delete_key_patterns`, kept verbatim so the fixture below
/// pins the two holes existed independent of the fixed code: delete capability
/// recognized only by an exact `s3:DeleteObject` match, and any resource not
/// under the bucket prefix silently dropped (`if let Some(..)` with no `else`).
fn pre_fix_allow_delete_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy.statements.as_array().unwrap() {
        let is_allow = stmt["Effect"]
            .as_str()
            .is_some_and(|e| e.eq_ignore_ascii_case("Allow"));
        if !is_allow {
            continue;
        }
        let grants_delete = statement_actions(stmt)
            .iter()
            .any(|a| action_eq(a, "s3:DeleteObject"));
        if !grants_delete {
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

/// Regression fixture for the delete-capability holes (issue #1372, the seventh
/// in the skip-what-you-don't-recognize sequence #1346 tracks). The pre-fix
/// guard recognized a delete grant only by an exact `s3:DeleteObject` match and
/// silently dropped any resource not under the bucket prefix. So a statement
/// granting `s3:*`/`s3:Delete*`/`*` on any resource, or `s3:DeleteObject` on
/// `"*"` or a different bucket, was invisible to it: the delete-pattern helper
/// returned an empty set and the assertion "Admin's only delete Allow is
/// sys/qualify/*" passed while a delete-everywhere grant sat unexamined.
///
/// Two independent holes, both closed: the Action is matched as an IAM wildcard
/// pattern (ASCII-folded, `*`/`?` resolved), and a delete-capable statement
/// whose Resource is not bucket-relative is a test failure naming the Sid, never
/// a dropped entry.
///
/// Synthetic statements, not `deploy/iam/*.json`: a fixture over the shipped
/// (correct) admin policy passes whichever way the matcher behaves and proves
/// nothing about the matcher. Each case asserts in both directions --- that the
/// pre-fix helper accepted the input (returned no delete entry, hiding the
/// grant, with a message flagging the fixture invalid if it ever stops) and
/// that the post-fix guard rejects it.
#[test]
fn wildcard_or_out_of_bucket_delete_grant_is_not_a_bypass() {
    // (Sid, Action, Resource, does the pre-fix EXACT action match recognize it)
    let cases = [
        // Wildcard action the exact match misses, on the bare "*" resource: the
        // action hole and the resource hole at once.
        (
            "StarActionStarResource",
            serde_json::json!("s3:*"),
            "*",
            false,
        ),
        // Wildcard action, bucket-relative resource OUTSIDE sys/qualify: the
        // action hole alone; post-fix this surfaces as a non-scratch pattern.
        (
            "DeleteStarOutsidePrefix",
            serde_json::json!("s3:Delete*"),
            "arn:aws:s3:::my-ravel-bucket/t/tenant/data",
            false,
        ),
        // The all-actions wildcard on a DIFFERENT bucket's ARN: the action hole
        // plus a resource that names another bucket entirely.
        (
            "StarActionOtherBucket",
            serde_json::json!("*"),
            "arn:aws:s3:::other-bucket/t/*",
            false,
        ),
        // Mis-cased literal delete on "*": here the pre-fix EXACT match already
        // recognized the action (it folds case), so this isolates the resource
        // hole --- the statement was seen as a delete yet its "*" resource was
        // silently dropped, leaving an empty delete set.
        (
            "WrongCaseDeleteObjectStarResource",
            serde_json::json!("S3:DELETEOBJECT"),
            "*",
            true,
        ),
    ];

    for (sid, action, resource, pre_fix_exact_recognizes) in cases {
        let policy = Policy {
            role: "fixture",
            statements: serde_json::json!([{
                "Sid": sid,
                "Effect": "Allow",
                "Action": action,
                "Resource": resource,
            }]),
        };
        let stmt = &policy.statements.as_array().unwrap()[0];
        let actions = statement_actions(stmt);

        // Observation 1 (load-bearing): the pre-fix helper returned no delete
        // entry, so the permissive grant was invisible and the scratch-only
        // assertion passed over it. If this stops being empty, the fixture no
        // longer proves the hole existed and must be rewritten, not deleted.
        let pre_fix = pre_fix_allow_delete_key_patterns(&policy);
        assert!(
            pre_fix.is_empty(),
            "fixture {sid} invalid: pre_fix_allow_delete_key_patterns was \
             expected to return no delete entry (hiding the grant); returned \
             {pre_fix:?}"
        );

        // ...pin WHY it was empty so the two holes stay distinguishable: either
        // the pre-fix exact match did not see a wildcard action, or it saw the
        // action but the silent drop discarded a non-bucket-relative resource.
        let pre_fix_action_hit = actions.iter().any(|a| action_eq(a, "s3:DeleteObject"));
        assert_eq!(
            pre_fix_action_hit, pre_fix_exact_recognizes,
            "fixture {sid} invalid: expected pre-fix exact action recognition = \
             {pre_fix_exact_recognizes} for {actions:?}"
        );
        assert!(
            actions.iter().any(|a| action_is_delete_capable(a)),
            "fixture {sid} invalid: post-fix action_is_delete_capable must \
             recognize {actions:?} as a delete grant"
        );

        // Observation 2: the real guard fires on the synthetic policy --- either
        // panicking inside delete_key_patterns on an out-of-bucket
        // resource, or failing the scratch-only assert_eq on a non-scratch key.
        // catch_unwind so the panic is reported as a result here.
        let guard = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_admin_delete_grant_is_scratch_only(&policy)
        }));
        assert!(
            guard.is_err(),
            "the delete guard must reject fixture {sid}: a delete-capable \
             statement on {resource:?} must surface as a permissive grant, not \
             be silently dropped"
        );
    }
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
            actions.iter().any(|a| action_eq(a, "kms:Decrypt")),
            "{role}: policy is missing kms:Decrypt -- its reads of SSE-KMS objects \
             under a --tenant-kms-config tenant will fail closed. \
             Found kms actions: {actions:?}"
        );
    }
}

/// The two wildcard characters IAM resolves inside a `Resource` ARN: `*`
/// matches any sequence, `?` matches exactly one character. Either one, in
/// any segment of a KMS ARN, grants more keys than the ARN appears to name:
/// `key/????????-????-????-????-????????????` covers every key whose id has
/// the shape of a UUID, and a `?` or `*` in the region or account position
/// widens the grant the same way.
const IAM_WILDCARDS: [char; 2] = ['*', '?'];

/// True when `resource` names more than the single key it appears to name.
fn kms_resource_is_wildcarded(resource: &str) -> bool {
    resource.contains(IAM_WILDCARDS)
}

/// The ARN shape a KMS `Resource` must have to pin one key: no IAM wildcard
/// in any segment, and a key-id segment that cannot span a `/` (so an
/// `alias/...` or a nested path cannot pose as a key id).
fn kms_key_arn_pattern() -> regex::Regex {
    regex::Regex::new(r"^arn:aws:kms:[^:*?/]+:[^:*?/]+:key/[^:*?/]+$").expect("valid regex")
}

/// The account-wide-grant half of the KMS resource guard, as one function so
/// the regression fixtures below run the real assertion rather than a copy of
/// its text.
fn assert_kms_resource_is_not_account_wide(role: &str, sid: &str, resource: &str) {
    assert!(
        !kms_resource_is_wildcarded(resource),
        "{role}: statement {sid:?} grants a kms: action on {resource:?}, which \
         contains an IAM wildcard (`*` or `?`) and so covers keys beyond the one \
         it appears to name"
    );
}

/// The names-a-specific-key half of the KMS resource guard, likewise shared
/// with the fixtures.
fn assert_kms_resource_names_a_key_id(role: &str, sid: &str, resource: &str) {
    assert!(
        kms_key_arn_pattern().is_match(resource),
        "{role}: statement {sid:?} resource {resource:?} does not name a \
         specific key id (expected arn:aws:kms:<region>:<account>:key/<id>, \
         with no `*` or `?` in any segment)"
    );
}

/// Every statement whose `Action` grants any `kms:` action must not name a
/// `Resource` that covers every key in the account/region: not the bare
/// wildcard `"*"`, not an ARN ending `:key/*`, and not an ARN carrying an IAM
/// wildcard character anywhere else either. Pinned as a negation (not an
/// exact tenant-key ARN) so it survives an operator's post-substitution
/// value. Widen any of the four templates' KMS `Resource` back to
/// `arn:aws:kms:us-east-1:111122223333:key/*` and this test fails, naming
/// the role and the statement `Sid`.
#[test]
fn no_kms_statement_grants_every_key_in_the_region() {
    for role in ALL_ROLES {
        let policy = load_policy(role);
        let statements = kms_statement_resources(&policy);
        assert!(
            !statements.is_empty(),
            "{role}: no statement carrying a kms: action was found -- this guard \
             would pass having examined nothing"
        );
        for (sid, resources) in statements {
            for resource in &resources {
                assert_kms_resource_is_not_account_wide(role, &sid, resource);
            }
        }
    }
}

/// Every statement whose `Action` grants any `kms:` action must name a
/// `Resource` that pins a specific key id, so an operator cannot re-widen
/// the grant to every key by substituting `arn:aws:kms:<region>:<account>:key/*`
/// (or any other wildcard variant, in the key id or in the region or account
/// segment) in place of the placeholder.
#[test]
fn every_kms_statement_names_a_key_id() {
    for role in ALL_ROLES {
        let policy = load_policy(role);
        let statements = kms_statement_resources(&policy);
        assert!(
            !statements.is_empty(),
            "{role}: no statement carrying a kms: action was found -- this guard \
             would pass having examined nothing"
        );
        for (sid, resources) in statements {
            for resource in &resources {
                assert_kms_resource_names_a_key_id(role, &sid, resource);
            }
        }
    }
}

/// Regression fixture for the case-sensitivity fix above: `KMS:Decrypt` (or any
/// other capitalization) on `"Resource": "*"` is exactly as fully-permissive as
/// `kms:Decrypt`, and the case-sensitive `starts_with("kms:")` selection both
/// guards above used to run would silently skip it -- reporting the templates
/// safe while a fully-permissive statement sat unexamined. This is not a real
/// shipped template; it is a synthetic statement built to prove the matcher
/// itself, independent of what `deploy/iam/*.json` currently contains.
#[test]
fn mixed_case_kms_action_is_not_a_bypass() {
    let stmt = serde_json::json!({
        "Sid": "MixedCaseFullyPermissive",
        "Effect": "Allow",
        "Action": "KMS:Decrypt",
        "Resource": "*"
    });
    let actions = statement_actions(&stmt);

    // Observation 1 (load-bearing): the pre-fix selection (case-sensitive)
    // misses the mixed-case action entirely, exactly the hole this test guards
    // against. Kept as the literal pre-fix expression, not a call into the
    // fixed code, so it pins that the hole existed.
    let selected_before_fix = actions.iter().any(|a| a.starts_with("kms:"));
    assert!(
        !selected_before_fix,
        "fixture invalid: the pre-fix case-sensitive matcher was expected to \
         miss \"KMS:Decrypt\" -- if it didn't, this fixture no longer proves \
         the hole existed"
    );

    // Observation 2: the post-fix selection catches it, through the same
    // helper the guards use.
    let selected_after_fix = actions.iter().any(|a| action_has_prefix(a, "kms:"));
    assert!(
        selected_after_fix,
        "fixture invalid: the post-fix matcher should select \"KMS:Decrypt\""
    );

    // Observation 3: the guard's own function -- not a re-typed copy of its
    // expression -- must fire on this fixture's "Resource": "*" now that the
    // statement is selected. Run it through catch_unwind so this test reports
    // the panic as an assertion result instead of aborting the binary.
    let resource = stmt["Resource"].as_str().expect("Resource is a string");
    let sid = stmt["Sid"].as_str().expect("Sid is a string");
    let guard_result = std::panic::catch_unwind(|| {
        assert_kms_resource_is_not_account_wide("fixture", sid, resource)
    });
    assert!(
        guard_result.is_err(),
        "the guard must reject \"KMS:Decrypt\" on Resource \"*\" once the \
         statement is selected -- a mixed-case action must not bypass the \
         no-account-wide-key assertion"
    );
}

/// Regression fixture for the case-sensitivity hole in a NEGATIVE assertion,
/// which fails silent where the positive ones fail loud. IAM action names are
/// case-insensitive, so a policy granting `KMS:GenerateDataKey*` holds exactly
/// the privilege `admin_has_no_kms_generate_data_key` asserts the role does
/// not hold. Both halves of that assertion used to compare case-sensitively:
/// `kms_actions` selected on `starts_with("kms:")`, and the caller compared
/// with `starts_with("kms:GenerateDataKey")`. Either one alone was enough to
/// report a role Decrypt-only while it could mint ciphertext under every
/// tenant key the statement names.
///
/// Synthetic, not read from `deploy/iam/`: the shipped admin template is
/// lowercase and correct, so a fixture over it passes whichever way the
/// matcher behaves and proves nothing about the matcher.
#[test]
fn mixed_case_generate_data_key_is_not_missed_by_the_negative_assertion() {
    let policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "MixedCaseGenerateDataKey",
            "Effect": "Allow",
            "Action": ["KMS:GenerateDataKey*", "KMS:Decrypt"],
            "Resource":
                "arn:aws:kms:us-east-1:111122223333:key/abcd1234-5678-90ab-cdef-1234567890ab"
        }]),
    };
    let declared: Vec<String> = policy
        .statements
        .as_array()
        .expect("fixture statements are an array")
        .iter()
        .flat_map(statement_actions)
        .collect();

    // Observation 1 (load-bearing): the pre-fix selection saw no kms action at
    // all, so the negative assertion held over an empty action list while the
    // grant sat in the policy. Written as the literal pre-fix expressions, so
    // this pins the hole rather than restating the fix.
    let selected_before_fix: Vec<&String> =
        declared.iter().filter(|a| a.starts_with("kms:")).collect();
    assert!(
        selected_before_fix.is_empty(),
        "fixture invalid: the pre-fix selection was expected to miss \
         \"KMS:GenerateDataKey*\" -- if it saw it, this fixture no longer \
         proves the hole existed"
    );
    assert!(
        !selected_before_fix
            .iter()
            .any(|a| a.starts_with("kms:GenerateDataKey")),
        "fixture invalid: the pre-fix negative assertion was expected to hold \
         (reporting the role unprivileged) on a policy that grants the key"
    );
    // The second half of the hole, independent of the first: even handed the
    // action directly, the pre-fix comparison did not recognize it.
    assert!(
        !declared
            .iter()
            .any(|a| a.starts_with("kms:GenerateDataKey")),
        "fixture invalid: the pre-fix comparison was expected to miss \
         \"KMS:GenerateDataKey*\" even when given the action"
    );

    // Observation 2: the post-fix selection and comparison both see it, so the
    // negative assertion in admin_has_no_kms_generate_data_key now fires.
    let actions = kms_actions(&policy);
    assert!(
        actions
            .iter()
            .any(|a| action_has_prefix(a, "kms:GenerateDataKey")),
        "the post-fix matcher must see KMS:GenerateDataKey* as the \
         kms:GenerateDataKey* grant it is. Found kms actions: {actions:?}"
    );
    assert!(
        actions.iter().any(|a| action_eq(a, "kms:Decrypt")),
        "the post-fix matcher must see KMS:Decrypt as kms:Decrypt. \
         Found kms actions: {actions:?}"
    );

    // Observation 3: the selected strings keep the template's own
    // capitalization, so a failure message quotes what the policy says rather
    // than a normalized form the operator cannot find in the file.
    assert!(
        actions.iter().any(|a| a == "KMS:GenerateDataKey*"),
        "kms_actions must preserve the policy's own spelling for failure \
         messages. Found kms actions: {actions:?}"
    );
}

/// The same case-sensitivity shape on the `s3:PutObject` selection that feeds
/// `roles_writing_routed_objects_have_kms_grant`. That guard derives the set
/// of routed PUT patterns from the policy and skips the role outright when the
/// set is empty, so a case-sensitive match on the action name silently skips
/// the KMS-grant check for a role that does route writes -- the same
/// fails-silent shape as the negative assertion above, reached through an
/// empty derived set instead of an empty action list.
///
/// Synthetic for the same reason: the shipped templates spell the action
/// lowercase.
#[test]
fn mixed_case_put_object_still_selects_routed_write_patterns() {
    let policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "MixedCaseRoutedPut",
            "Effect": "Allow",
            "Action": ["S3:PutObject"],
            "Resource": ["arn:aws:s3:::my-ravel-bucket/t/*/*/l0/*"]
        }]),
    };

    // Observation 1 (load-bearing): the pre-fix case-sensitive equality
    // selected nothing, so the derived routed-PUT set was empty and the guard
    // continued past this role without checking any KMS grant.
    let selected_before_fix = policy
        .statements
        .as_array()
        .expect("fixture statements are an array")
        .iter()
        .any(|stmt| statement_actions(stmt).iter().any(|a| a == "s3:PutObject"));
    assert!(
        !selected_before_fix,
        "fixture invalid: the pre-fix case-sensitive match was expected to \
         miss \"S3:PutObject\" -- if it saw it, this fixture no longer proves \
         the hole existed"
    );

    // Observation 2: the post-fix selection returns the pattern, and the real
    // routing predicate confirms it is a routed write, so the guard now
    // reaches its KMS-grant assertions for this role instead of skipping it.
    let patterns = put_resource_key_patterns(&policy);
    assert_eq!(
        patterns,
        vec!["t/*/*/l0/*".to_string()],
        "the post-fix selection must return the statement's PUT pattern"
    );
    assert!(
        patterns
            .iter()
            .any(|p| ravel_object_store::routes_through_tenant_key(p)),
        "fixture invalid: {patterns:?} must route through the tenant key, or \
         the guard would skip this role for a reason unrelated to the matcher"
    );
}

/// Regression fixture for the single-character-wildcard hole: IAM resolves `?`
/// inside a `Resource` ARN as exactly one character, so
/// `key/????????-????-????-????-????????????` grants every key whose id has the
/// shape of a UUID, and a `?` in the region or account segment widens the grant
/// the same way. Both pre-fix predicates accepted those ARNs: the direct check
/// tested only for the two literals `"*"` and a `:key/*` suffix, and the ARN
/// regex excluded `*` from the key-id segment alone. The guards therefore ran
/// and reported the templates scoped while an effectively account-wide grant sat
/// unexamined.
///
/// Synthetic statements, not `deploy/iam/*.json`: a fixture read from the config
/// directory passes for reasons unrelated to the matcher, and goes green the day
/// someone edits the config. Each case asserts in both directions -- that the
/// pre-fix predicates did NOT reject the resource (so the test cannot quietly
/// become a tautology if the predicates are rewritten) and that the post-fix
/// ones do.
#[test]
fn single_character_wildcard_in_kms_resource_is_not_a_bypass() {
    // The pre-fix ARN shape check, kept verbatim so observation 1 below pins
    // the hole rather than restating the fix.
    let pre_fix_key_arn_pattern =
        regex::Regex::new(r"^arn:aws:kms:[^:]+:[^:]+:key/[^*]+$").expect("valid regex");

    let cases = [
        (
            "QuestionMarkKeyId",
            "arn:aws:kms:us-east-1:111122223333:key/????????-????-????-????-????????????",
        ),
        (
            "QuestionMarkRegionAndAccount",
            "arn:aws:kms:us-east-?:11112222333?:key/abcd1234-5678-90ab-cdef-1234567890ab",
        ),
    ];

    for (sid, resource) in cases {
        let stmt = serde_json::json!({
            "Sid": sid,
            "Effect": "Allow",
            "Action": ["kms:Decrypt", "kms:Encrypt"],
            "Resource": [resource],
        });
        let resources: Vec<String> = stmt["Resource"]
            .as_array()
            .expect("Resource is an array")
            .iter()
            .map(|v| v.as_str().expect("Resource entry is a string").to_string())
            .collect();
        assert_eq!(
            resources.len(),
            1,
            "fixture invalid: expected exactly one resource for {sid}"
        );
        let resource = resources[0].as_str();

        // Observation 1 (load-bearing): neither pre-fix predicate rejected this
        // resource. If either one does, the fixture no longer proves the hole
        // existed and must be rewritten rather than deleted.
        let pre_fix_direct_rejects = resource == "*" || resource.ends_with(":key/*");
        assert!(
            !pre_fix_direct_rejects,
            "fixture invalid: the pre-fix direct check was expected to accept \
             {resource:?} -- if it rejects it, this fixture no longer proves the \
             `?` hole existed"
        );
        assert!(
            pre_fix_key_arn_pattern.is_match(resource),
            "fixture invalid: the pre-fix ARN regex was expected to accept \
             {resource:?} -- if it rejects it, this fixture no longer proves the \
             `?` hole existed"
        );

        // Observation 2: both post-fix predicates reject it.
        assert!(
            kms_resource_is_wildcarded(resource),
            "the post-fix direct check must treat {resource:?} as wildcarded"
        );
        assert!(
            !kms_key_arn_pattern().is_match(resource),
            "the post-fix ARN regex must reject {resource:?}"
        );

        // Observation 3: the real guards' own assertions fire on this statement.
        // Run through catch_unwind so the panic is reported as a result here
        // instead of aborting the binary.
        let account_wide_guard = std::panic::catch_unwind(|| {
            assert_kms_resource_is_not_account_wide("fixture", sid, resource)
        });
        assert!(
            account_wide_guard.is_err(),
            "no_kms_statement_grants_every_key_in_the_region's assertion must \
             fire on {resource:?}"
        );
        let key_id_guard = std::panic::catch_unwind(|| {
            assert_kms_resource_names_a_key_id("fixture", sid, resource)
        });
        assert!(
            key_id_guard.is_err(),
            "every_kms_statement_names_a_key_id's assertion must fire on \
             {resource:?}"
        );
    }
}

/// Regression fixtures for the fail-closed choke point (issue #1346, the fourth
/// instance of the skip-what-you-do-not-understand class this guard keeps
/// repeating: could not read `Resource`; matched the action prefix
/// case-sensitively; accepted `?` in ARN segments; and now ignores
/// `NotResource`/`NotAction`). Before this fix, `load_policy` performed no
/// per-statement validation, so a statement whose permission lived in a field
/// no guard reads -- `NotResource`, `NotAction`, an unrecognized key such as a
/// `Resources` typo, or a statement with no `Resource` at all -- was loaded and
/// then silently skipped by whichever guard went looking for a field it did not
/// find. The guard ran, reported the templates safe, and the statement sat
/// unexamined.
///
/// Synthetic statements, not `deploy/iam/*.json`: the shipped templates carry
/// only handled, well-formed statements, so a fixture over them proves nothing
/// about the choke point and goes green the day someone edits the config. Each
/// negative case asserts in both directions -- that the pre-fix guards found
/// nothing to object to (Observation 1, the hole) and that `validate_statement`
/// now rejects it naming both the `Sid` and the offending key (Observation 2).
#[test]
fn statement_using_an_unhandled_key_fails_closed() {
    // (Sid, statement, substring the rejection must name, which field hid the
    // permission pre-fix: "resource" => resource_key_patterns skipped it,
    // "action" => statement_actions saw no action).
    let negative_cases = [
        (
            "NegatedResource",
            serde_json::json!({
                "Sid": "NegatedResource",
                "Effect": "Allow",
                "Action": "s3:DeleteObject",
                "NotResource": "arn:aws:s3:::my-ravel-bucket/t/*/*/prov"
            }),
            "NotResource",
            "resource",
        ),
        (
            "NegatedAction",
            serde_json::json!({
                "Sid": "NegatedAction",
                "Effect": "Allow",
                "NotAction": "s3:GetObject",
                "Resource": "arn:aws:s3:::my-ravel-bucket/t/*"
            }),
            "NotAction",
            "action",
        ),
        (
            "TypoResources",
            serde_json::json!({
                "Sid": "TypoResources",
                "Effect": "Allow",
                "Action": "s3:PutObject",
                "Resources": "arn:aws:s3:::my-ravel-bucket/t/*/*/l0/*"
            }),
            "Resources",
            "resource",
        ),
        (
            "MissingResource",
            serde_json::json!({
                "Sid": "MissingResource",
                "Effect": "Allow",
                "Action": "s3:GetObject"
            }),
            "no Resource",
            "resource",
        ),
    ];

    for (sid, stmt, must_name, hidden_side) in &negative_cases {
        let policy = Policy {
            role: "fixture",
            statements: serde_json::json!([stmt.clone()]),
        };

        // Observation 1 (load-bearing): the pre-fix guard that would have read
        // the permission found nothing. A resource-hidden statement produces no
        // resource pattern to check; an action-hidden statement produces no
        // action. If the relevant set stops being empty, the fixture no longer
        // proves the statement was skipped and must be rewritten, not deleted.
        match *hidden_side {
            "resource" => assert!(
                resource_key_patterns(&policy).is_empty(),
                "fixture {sid} invalid: resource_key_patterns was expected to \
                 skip the statement (returning nothing); it did not"
            ),
            "action" => assert!(
                statement_actions(stmt).is_empty(),
                "fixture {sid} invalid: statement_actions was expected to see no \
                 action (returning nothing); it did not"
            ),
            other => panic!("fixture {sid}: unknown hidden_side {other:?}"),
        }

        // Observation 2: the choke point rejects it, naming the Sid and the key.
        let err = validate_statement("fixture", 0, stmt)
            .expect_err(&format!("validate_statement must reject fixture {sid}"));
        assert!(
            err.contains(must_name),
            "fixture {sid}: rejection must name {must_name:?}; got {err:?}"
        );
        assert!(
            err.contains(sid),
            "fixture {sid}: rejection must name the Sid; got {err:?}"
        );

        // ...and so does the extracted loop `load_policy` actually runs
        // (`validate_policy_statements`), called directly here rather than
        // re-typed into a closure. `load_policy_rejects_an_unhandled_statement`
        // separately pins that `load_policy`/`build_policy` still call it.
        assert!(
            validate_policy_statements(policy.role, &policy.statements).is_err(),
            "fixture {sid}: validate_policy_statements (load_policy's real loop) \
             must reject it"
        );
    }

    // Positive control: a well-formed Allow whose keys are all handled passes.
    let ok = serde_json::json!({
        "Sid": "WellFormedAllow",
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:PutObject"],
        "Resource": ["arn:aws:s3:::my-ravel-bucket/t/*"]
    });
    assert!(
        validate_statement("fixture", 0, &ok).is_ok(),
        "a well-formed Allow with only handled keys must pass"
    );
}

/// Companion to the choke point's key check: a statement whose keys are all
/// handled but whose values are malformed must also fail closed rather than be
/// skipped. Pre-fix, an `Effect` that is neither Allow nor Deny was skipped by
/// every effect-filtered guard (they compare case-insensitively against exactly
/// those two), and an `Action` or `Resource` that is neither a string nor an
/// array of strings was read as an empty set and dropped. Synthetic for the
/// same reason as above.
#[test]
fn malformed_effect_action_or_resource_fails_closed() {
    let cases = [
        (
            "BadEffect",
            serde_json::json!({
                "Sid": "BadEffect",
                "Effect": "Permit",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::my-ravel-bucket/t/*"
            }),
            "Effect",
        ),
        (
            "ActionIsNumber",
            serde_json::json!({
                "Sid": "ActionIsNumber",
                "Effect": "Allow",
                "Action": 7,
                "Resource": "arn:aws:s3:::my-ravel-bucket/t/*"
            }),
            "Action",
        ),
        (
            "ActionArrayHasNonString",
            serde_json::json!({
                "Sid": "ActionArrayHasNonString",
                "Effect": "Allow",
                "Action": ["s3:GetObject", 7],
                "Resource": "arn:aws:s3:::my-ravel-bucket/t/*"
            }),
            "Action",
        ),
        (
            "ResourceIsNumber",
            serde_json::json!({
                "Sid": "ResourceIsNumber",
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": 7
            }),
            "Resource",
        ),
    ];

    for (sid, stmt, must_name) in &cases {
        let err = validate_statement("fixture", 0, stmt)
            .expect_err(&format!("validate_statement must reject fixture {sid}"));
        assert!(
            err.contains(must_name),
            "fixture {sid}: rejection must name {must_name:?}; got {err:?}"
        );
        assert!(
            err.contains(sid),
            "fixture {sid}: rejection must name the Sid; got {err:?}"
        );
    }
}

/// F2 regression: an empty `Action`/`Resource` array must fail closed. Pre-fix,
/// `is_string_or_string_array` returned true for `[]` (`iter().all(..)` is
/// vacuously true on an empty array), so `"Action": []` / `"Resource": []`
/// passed validation and every downstream guard derived an empty set and
/// skipped the statement -- the skip class #1346 tracks, one level inside a
/// handled key. Synthetic, not `deploy/iam/*.json`: the shipped templates carry
/// no empty arrays.
#[test]
fn empty_action_or_resource_array_fails_closed() {
    let cases = [
        (
            "EmptyAction",
            serde_json::json!({
                "Sid": "EmptyAction",
                "Effect": "Allow",
                "Action": [],
                "Resource": "arn:aws:s3:::my-ravel-bucket/t/*"
            }),
            "Action",
        ),
        (
            "EmptyResource",
            serde_json::json!({
                "Sid": "EmptyResource",
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": []
            }),
            "Resource",
        ),
    ];

    for (sid, stmt, must_name) in &cases {
        // The fixed predicate rejects the empty array. (An `iter().all` over the
        // empty array is vacuously true, which is exactly the pre-fix bug.)
        assert!(
            !is_string_or_string_array(stmt.get(must_name)),
            "fixture {sid}: an empty {must_name} array must not count as a string array"
        );
        let err = validate_statement("fixture", 0, stmt)
            .expect_err(&format!("validate_statement must reject fixture {sid}"));
        assert!(
            err.contains(must_name),
            "fixture {sid}: rejection must name {must_name:?}; got {err:?}"
        );
        assert!(
            err.contains(sid),
            "fixture {sid}: rejection must name the Sid; got {err:?}"
        );
    }
}

/// F1 regression: a `Condition` whose sub-shape is anything other than the one
/// `StringLike`/`s3:prefix` block a guard reads must fail closed. Pre-fix,
/// `validate_statement` accepted any `Condition` value, so a statement carrying
/// `StringNotLike`, a set-qualified `ForAnyValue:StringLike`, an unhandled key
/// such as `s3:delimiter`, or a non-object Condition passed validation and
/// `list_prefix_patterns` then found no `["StringLike"]["s3:prefix"]` array and
/// silently contributed nothing -- the skip class moved one level down into a
/// handled key. Synthetic, not `deploy/iam/*.json`: every shipped Condition is
/// exactly `StringLike`/`s3:prefix`.
#[test]
fn unhandled_condition_shape_fails_closed() {
    let cases = [
        (
            "StringNotLikeOperator",
            serde_json::json!({
                "Sid": "StringNotLikeOperator",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::my-ravel-bucket",
                "Condition": {"StringNotLike": {"s3:prefix": ["t/*"]}}
            }),
            "StringNotLike",
        ),
        (
            "SetQualifiedOperator",
            serde_json::json!({
                "Sid": "SetQualifiedOperator",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::my-ravel-bucket",
                "Condition": {"ForAnyValue:StringLike": {"s3:prefix": ["t/*"]}}
            }),
            "ForAnyValue:StringLike",
        ),
        (
            "UnhandledConditionKey",
            serde_json::json!({
                "Sid": "UnhandledConditionKey",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::my-ravel-bucket",
                "Condition": {"StringLike": {"s3:delimiter": ["/"]}}
            }),
            "s3:delimiter",
        ),
        (
            "ConditionNotObject",
            serde_json::json!({
                "Sid": "ConditionNotObject",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::my-ravel-bucket",
                "Condition": "StringLike"
            }),
            "Condition",
        ),
    ];

    for (sid, stmt, must_name) in &cases {
        // Load-bearing: the guard that would read this Condition finds nothing,
        // so the constraint sits unexamined. Prove the pre-fix skip on the two
        // operator cases (both are s3:ListBucket, so list_prefix_patterns is the
        // guard that skips them).
        if *sid == "StringNotLikeOperator" || *sid == "SetQualifiedOperator" {
            let policy = Policy {
                role: "fixture",
                statements: serde_json::json!([stmt.clone()]),
            };
            assert!(
                list_prefix_patterns(&policy).is_empty(),
                "fixture {sid}: list_prefix_patterns was expected to skip the \
                 statement (finding no StringLike/s3:prefix); it did not"
            );
        }

        let err = validate_statement("fixture", 0, stmt)
            .expect_err(&format!("validate_statement must reject fixture {sid}"));
        assert!(
            err.contains(must_name),
            "fixture {sid}: rejection must name {must_name:?}; got {err:?}"
        );
        assert!(
            err.contains(sid),
            "fixture {sid}: rejection must name the Sid; got {err:?}"
        );
    }

    // Positive control: the shipped StringLike/s3:prefix shape passes.
    let ok = serde_json::json!({
        "Sid": "GoodCondition",
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::my-ravel-bucket",
        "Condition": {"StringLike": {"s3:prefix": ["t/*", "sys/*"]}}
    });
    assert!(
        validate_statement("fixture", 0, &ok).is_ok(),
        "the shipped StringLike/s3:prefix Condition shape must pass"
    );
}

/// F3 wiring guard: `load_policy` (through its shared body `build_policy`) must
/// actually call `validate_policy_statements`. Driving a synthetic invalid
/// policy through `build_policy` -- the real validation call site -- is what
/// makes this test fail if that call is deleted; a test that only calls
/// `validate_policy_statements` directly would keep passing. Proven by removing
/// the call from `build_policy`: this test then reports no panic and fails.
#[test]
fn load_policy_rejects_an_unhandled_statement() {
    let json = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "NegatedResource",
            "Effect": "Allow",
            "Action": "s3:DeleteObject",
            "NotResource": "arn:aws:s3:::my-ravel-bucket/t/*"
        }]
    });
    let built = std::panic::catch_unwind(|| build_policy("fixture", "synthetic", &json));
    assert!(
        built.is_err(),
        "build_policy (load_policy's real body) must reject a statement whose \
         permission lives in NotResource, a field no guard reads"
    );
}

/// Sweep proof: `list_prefix_patterns` reads a bare-string `s3:prefix`, not only
/// an array. IAM allows either shape; before this fix the `.as_array()` read
/// skipped a bare string, so a ListBucket discovery prefix expressed as a string
/// contributed nothing.
#[test]
fn list_prefix_patterns_reads_a_bare_string_prefix() {
    let policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "BareStringPrefix",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::my-ravel-bucket",
            "Condition": {"StringLike": {"s3:prefix": "t/"}}
        }]),
    };
    assert_eq!(
        list_prefix_patterns(&policy),
        vec!["t/".to_string()],
        "a bare-string s3:prefix must be read, not skipped"
    );
    // ...and the shipped array shape still validates and is read.
    assert!(
        validate_statement("fixture", 0, &policy.statements.as_array().unwrap()[0]).is_ok(),
        "a bare-string s3:prefix is a valid IAM shape and must pass validation"
    );
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

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

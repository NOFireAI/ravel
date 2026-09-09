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

// ---------------------------------------------------------------------------
// The action vocabulary. One predicate (`action_grants`) and one operation list
// per axis; every axis in this file routes through them.
// ---------------------------------------------------------------------------

/// The S3 operations these guards reason about that act on an OBJECT, so a
/// statement granting one must name bucket-relative object ARNs.
const S3_OBJECT_OPERATIONS: [&str; 4] = [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
];

/// The S3 operations these guards reason about that act on the BUCKET, so a
/// statement granting one must name the bare bucket ARN. `s3:ListBucket` is the
/// only one: it is what `list_prefix_patterns` reads an `s3:prefix` Condition
/// for, and what the Condition-presence rule keys on.
const S3_BUCKET_OPERATIONS: [&str; 1] = ["s3:ListBucket"];

/// The S3 operations that destroy a stored object. A subset of
/// `S3_OBJECT_OPERATIONS` (pinned by `operation_vocabulary_is_consistent`), kept
/// separate because the delete axis selects on exactly these.
const S3_DELETE_OPERATIONS: [&str; 2] = ["s3:DeleteObject", "s3:DeleteObjectVersion"];

/// The KMS operations that mint a data key, i.e. that let the holder produce new
/// ciphertext under a tenant key. `write_roles_have_kms_generate_data_key` and
/// `roles_writing_routed_objects_have_kms_grant` require one; ADR-0055 forbids
/// admin any of them (`admin_has_no_kms_generate_data_key`). All four spellings
/// are listed so the negative assertion cannot be evaded by granting a variant.
const KMS_DATA_KEY_OPERATIONS: [&str; 4] = [
    "kms:GenerateDataKey",
    "kms:GenerateDataKeyWithoutPlaintext",
    "kms:GenerateDataKeyPair",
    "kms:GenerateDataKeyPairWithoutPlaintext",
];

/// The remaining KMS operations these guards reason about.
const KMS_OTHER_OPERATIONS: [&str; 2] = ["kms:Decrypt", "kms:Encrypt"];

/// The single action predicate every axis in this file goes through: does the
/// policy string `action` (possibly an IAM wildcard pattern, in whatever case
/// the template spells it) grant `operation` (a literal IAM operation name)?
///
/// IAM action names are case-insensitive and resolve two wildcards, `*` (any
/// sequence) and `?` (exactly one character), so `"*"`, `"s3:*"`, `"s3:Delete*"`
/// and `"S3:DELETEOBJECT"` all grant `s3:DeleteObject`. Before this predicate
/// existed each axis carried its own matcher and only the delete axis resolved
/// wildcards, so `"Action": "s3:*"` granted PutObject and ListBucket while being
/// selected by neither -- and the Condition-presence rule, which keys on the
/// list grant, never fired either (issue #1346, H3). There is now one place that
/// decides, so no axis can be wildcard-aware while another is not.
///
/// The asymmetry is deliberate and asserted: the wildcard lives on the policy
/// side. An `operation` carrying `*` or `?` is a caller bug, not a pattern to
/// resolve, because a wildcarded operation would make the predicate answer a
/// question no axis asked.
fn action_grants(action: &str, operation: &str) -> bool {
    assert!(
        !operation.contains(IAM_WILDCARDS),
        "action_grants takes a literal IAM operation name, not a pattern: {operation:?}"
    );
    let action = action.to_ascii_lowercase();
    let operation = operation.to_ascii_lowercase();
    if action == operation {
        return true;
    }
    // Only a wildcard pattern grants beyond its own name: an unrelated literal
    // action (`s3:DeleteObjectTagging`, `s3:DeleteBucket`) grants nothing else.
    action.contains(IAM_WILDCARDS) && glob_matches(&action, &operation)
}

/// True when `action` grants at least one of `operations`.
fn action_grants_any(action: &str, operations: &[&str]) -> bool {
    operations.iter().any(|op| action_grants(action, op))
}

/// True when at least one of a statement's `actions` grants at least one of
/// `operations`. Every axis's statement selection is this call.
fn any_action_grants_any(actions: &[String], operations: &[&str]) -> bool {
    actions.iter().any(|a| action_grants_any(a, operations))
}

/// True when `action` grants any KMS operation these guards reason about, so the
/// statement carrying it is a KMS statement whose `Resource` the KMS resource
/// guards own. This replaces the old case-folded `kms:` prefix test, which a
/// wildcard action (`"*"`, `"kms:*"`) slipped past.
fn action_selects_kms(action: &str) -> bool {
    action_grants_any(action, &KMS_OTHER_OPERATIONS)
        || action_grants_any(action, &KMS_DATA_KEY_OPERATIONS)
}

// ---------------------------------------------------------------------------
// The resource vocabulary. `classify_resource` is the only place a `Resource`
// string is interpreted; the choke point and every helper share it, so a shape
// cannot be understood in one and dropped in the other.
// ---------------------------------------------------------------------------

/// The ARN service prefix of a KMS key resource.
const KMS_ARN_PREFIX: &str = "arn:aws:kms:";

/// What a `Resource` string names, as far as the guards in this file are
/// concerned. `Unclassified` is not a fourth kind of resource: it is the shape
/// `validate_statement` rejects, so no helper downstream ever has to decide what
/// to do with one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceShape<'a> {
    /// An object ARN under the configured bucket; carries the bucket-relative
    /// key pattern (always non-empty).
    ObjectKey(&'a str),
    /// Exactly the bare bucket ARN: what an `s3:ListBucket` grant names.
    Bucket,
    /// A KMS key ARN.
    KmsKey,
    /// Anything else, including every shape issue #1346 H1 lists: an S3
    /// access-point ARN (`arn:aws:s3:<region>:<account>:accesspoint/...`), an
    /// Object Lambda ARN (`arn:aws:s3-object-lambda:...`), a multi-region access
    /// point (`arn:aws:s3::<account>:accesspoint/...`), the everything-grant
    /// `arn:*`, the bare `"*"`, another bucket's ARN, and a malformed non-ARN
    /// such as `my-ravel-bucket/*`.
    Unclassified,
}

/// Classify one `Resource` string. Total: every string lands in exactly one
/// shape, and everything the guards cannot reason about lands in `Unclassified`
/// rather than being returned as "nothing to check".
///
/// The bucket-relative arm requires a NON-EMPTY key pattern:
/// `arn:aws:s3:::my-ravel-bucket/` names the object with the empty key, which no
/// key constructor produces and no guard has a pattern for, so it is
/// unclassified rather than an `ObjectKey("")` that matches nothing.
fn classify_resource(resource: &str) -> ResourceShape<'_> {
    match resource.strip_prefix(BUCKET_KEY_PREFIX) {
        Some(key) if !key.is_empty() => return ResourceShape::ObjectKey(key),
        _ => {}
    }
    if resource == BUCKET_ARN {
        return ResourceShape::Bucket;
    }
    if resource.starts_with(KMS_ARN_PREFIX) {
        return ResourceShape::KmsKey;
    }
    ResourceShape::Unclassified
}

#[derive(Debug)]
struct Policy {
    role: &'static str,
    statements: serde_json::Value,
}

/// The complete set of statement keys any guard in this file reads. Sid,
/// Effect, Action, and Resource are read directly (see `statement_actions`,
/// `statement_resources`, `key_patterns_for`, `kms_statement_resources`, ...);
/// Condition is read for the `s3:prefix` ListBucket block
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
/// # The closure argument
///
/// Every earlier round of this guard fixed one hole and left another, because
/// each helper decided for itself which shapes it understood and answered "no
/// resources to check" for the rest. The fix is structural: ALL shape decisions
/// happen here, and the helpers are total functions over the shapes that got
/// through. Concretely, a statement reaching any guard has:
///
/// - a present string `Sid`, so every message can name it;
/// - only keys in `HANDLED_STATEMENT_KEYS`, so no permission lives in a field no
///   guard reads (`NotAction`/`NotResource`/`Principal` are rejected by name);
/// - an `Effect` of `Allow` or `Deny`, so the effect-filtered axes partition it;
/// - an `Action` that is a string or non-empty array of strings, EVERY element of
///   which grants at least one operation in the vocabulary
///   (`S3_OBJECT_OPERATIONS`, `S3_BUCKET_OPERATIONS`, `KMS_OTHER_OPERATIONS`,
///   `KMS_DATA_KEY_OPERATIONS`), decided by the one predicate `action_grants`;
/// - a `Resource` that is a string or non-empty array of strings, EVERY element
///   of which `classify_resource` places in `ObjectKey`, `Bucket`, or `KmsKey`
///   -- never `Unclassified`;
/// - for each granted operation class, at least one resource of the matching
///   shape, and no resource of a shape whose class the statement does not grant.
///   So an object grant carries only bucket-relative object ARNs, a list grant
///   carries the bare bucket ARN, a KMS grant carries only `arn:aws:kms:` ARNs,
///   and the normal mixed idiom (ListBucket + GetObject over the bucket ARN plus
///   an object prefix) is accepted because both classes are granted;
/// - a `Condition` exactly when the Action grants a list operation, whose
///   sub-shape is the one `StringLike`/`s3:prefix` block
///   `list_prefix_patterns` reads.
///
/// A helper downstream can therefore no longer meet a shape it does not
/// understand. `object_key_patterns` matches all four `ResourceShape` variants:
/// it strips `ObjectKey`, skips `Bucket` and `KmsKey` because this function
/// proved the same statement grants the list or KMS operation whose own guard
/// reads that exact string, and panics on `Unclassified` because this function
/// rejects it. Each remaining `continue` in a helper is axis selection ("this
/// statement grants no operation on my axis"), never a shape skip: the coverage
/// rule above means every statement is selected by at least one axis, and that
/// axis sees every resource it carries.
///
/// Effect is a third axis, orthogonal to Action and Resource, and this guard
/// does NOT partition on it: a `Deny` statement is validated for shape exactly
/// like an `Allow`, so a well-formed `Deny` reaches every helper its Action and
/// Resource select. IAM resolves an explicit `Deny` as an overriding
/// prohibition, so any helper whose result a caller reads as a held permission
/// ("this role MAY do X", "X is scoped") or a prohibition ("this role MAY NOT
/// do X") MUST filter by Effect itself; the guard cannot do it for them without
/// rejecting legitimate `Deny` blocks. The contract is per helper, stated in
/// its own doc: a helper that reads as a grant takes an effect argument and its
/// permission-reading callers pass `Some("Allow")` (`key_patterns_for`,
/// `list_prefix_patterns`, `put_resource_key_patterns`, `delete_key_patterns`),
/// or the helper filters to `Allow` internally when every caller reads it as a
/// grant (`kms_actions`, `kms_statement_resources`). A pure shape check that
/// reads the result as neither a grant nor a prohibition (the pattern-vs-key
/// coverage guard) passes `None` and sees both effects on purpose. The failure
/// this closes: a helper selected by Action and Resource but blind to Effect,
/// whose Deny-derived output is then read as an Allow-derived permission,
/// asserts the inverse of the fact (issue #1346, F1/F2). This is the axis the
/// next reviewer checks the next finding against.
///
/// Rejects, naming the `Sid` (and the statement index) and the offending key or
/// field:
/// - a statement that is not a JSON object;
/// - a missing or non-string `Sid` (`"Sid": 123`): Sid is in the handled set,
///   but its type went unchecked pre-fix and the `<no Sid>` fallback then named
///   nothing (issue #1346, F3);
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
/// - an `Action` element that grants no operation in the vocabulary, so no axis
///   would select the statement (H3);
/// - a `Resource` element `classify_resource` cannot place, quoting the value:
///   an S3 access-point or Object Lambda or multi-region-access-point ARN, the
///   everything-grant `arn:*`, the bare `"*"`, another bucket's ARN, or a
///   malformed non-ARN such as `my-ravel-bucket/*` (H1);
/// - a `Resource` whose shape belongs to an operation class the statement does
///   not grant (an `s3:ListBucket` on an object ARN, an `s3:GetObject` on the
///   bare bucket ARN, a KMS ARN on a statement granting no KMS operation), and
///   conversely a granted class with no resource of its shape (an
///   `s3:ListBucket` that names no bucket ARN -- the H2 hole, where round
///   three's list-only exemption left a list statement's Resource read by
///   nothing at all);
/// - a Condition whose presence does not track the ListBucket action: a
///   ListBucket statement with no Condition (an unconstrained bucket-wide list),
///   or a Condition on any non-ListBucket statement (read by no guard) (F2);
/// - a `Condition` whose sub-shape is anything other than the one block a guard
///   reads: it must be a non-empty JSON object of handled operators
///   (`HANDLED_CONDITION_OPERATORS`, today `StringLike`), each a non-empty map of
///   handled condition keys (`HANDLED_CONDITION_KEYS`, today `s3:prefix`) to a
///   string or non-empty array of strings. A different operator, a set-qualified
///   operator, an unhandled key, or an empty `{}`/`{"StringLike":{}}` (which
///   constrains nothing) is a shape `list_prefix_patterns` cannot read, so it
///   fails closed here rather than contributing nothing silently.
fn validate_statement(role: &str, index: usize, stmt: &serde_json::Value) -> Result<(), String> {
    let obj = stmt
        .as_object()
        .ok_or_else(|| format!("{role}: statement #{index} is not a JSON object: {stmt:?}"))?;

    // Sid must be a present string. Every rejection below quotes it, and every
    // guard's own failure message names it; a missing Sid, or a non-string
    // `"Sid": 123`, was accepted pre-fix through the `<no Sid>` fallback (Sid is
    // in the handled set but its type was never checked), leaving a statement
    // whose rejections could name nothing (issue #1346, F3).
    let sid = match obj.get("Sid") {
        Some(serde_json::Value::String(s)) => s.as_str(),
        Some(other) => {
            return Err(format!(
                "{role}: statement #{index} has a non-string Sid: {other:?} -- Sid is a \
                 handled key and must be a string so every rejection can name it"
            ));
        }
        None => {
            return Err(format!(
                "{role}: statement #{index} has no Sid -- Sid is required so every \
                 rejection names the statement it rejects"
            ));
        }
    };

    for key in obj.keys() {
        if NEGATED_OR_PRINCIPAL_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "{role}/{sid} (statement #{index}): statement uses {key:?}; the guards \
                 in this file cannot reason about negated or principal-scoped \
                 statements (they read only the positive Action/Resource sets), so a \
                 policy carrying it must be rejected rather than silently passed"
            ));
        }
        if !HANDLED_STATEMENT_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "{role}/{sid} (statement #{index}): statement uses key {key:?}, which no \
                 guard in this file handles (handled keys: {HANDLED_STATEMENT_KEYS:?}); it \
                 must fail closed rather than sit unexamined"
            ));
        }
    }

    match obj.get("Effect").and_then(|v| v.as_str()) {
        Some(e) if e.eq_ignore_ascii_case("Allow") || e.eq_ignore_ascii_case("Deny") => {}
        other => {
            return Err(format!(
                "{role}/{sid} (statement #{index}): Effect is neither \"Allow\" nor \
                 \"Deny\": {other:?}"
            ));
        }
    }

    if !is_string_or_string_array(obj.get("Action")) {
        return Err(format!(
            "{role}/{sid} (statement #{index}): Action is neither a string nor a \
             non-empty array of strings: {:?}",
            obj.get("Action")
        ));
    }

    match obj.get("Resource") {
        None => {
            return Err(format!(
                "{role}/{sid} (statement #{index}): statement has no Resource key -- a \
                 statement with no Resource is the exact shape the resource guards skip \
                 (they read stmt[\"Resource\"], find Null, and treat it as nothing to \
                 check)"
            ));
        }
        resource if !is_string_or_string_array(resource) => {
            return Err(format!(
                "{role}/{sid} (statement #{index}): Resource is neither a string nor a \
                 non-empty array of strings: {resource:?}"
            ));
        }
        _ => {}
    }

    // Both shape checks above have passed, so `statement_actions` and
    // `statement_resources` now return exactly what the policy declares.
    let actions = statement_actions(stmt);
    let resources = statement_resources(stmt);
    validate_actions_and_resources(role, sid, index, &actions, &resources)?;

    // Condition presence must track the list action. `list_prefix_patterns`
    // is the only Condition reader and only reads one when the Action grants
    // s3:ListBucket, so:
    //  - a list statement MUST carry a Condition (an unconstrained bucket-wide
    //    list is rejected, exactly like a missing Resource), and
    //  - a non-list statement must carry NONE (a Condition there is read by no
    //    guard: a StringLike/s3:prefix on the protected-delete Deny would pass
    //    validation while, in AWS, a DeleteObject request carries no s3:prefix
    //    context key, so the Deny never fires and protects nothing).
    // (issue #1346, F2)
    //
    // The list grant is decided by `action_grants`, the same predicate every
    // other axis uses, so `"Action": "s3:*"` -- which grants ListBucket -- is
    // required to carry a Condition here too. Under round three's exact-name
    // detection it was not, and `list_prefix_patterns` then read nothing from it
    // (issue #1346, H3).
    let grants_list = any_action_grants_any(&actions, &S3_BUCKET_OPERATIONS);
    match obj.get("Condition") {
        Some(condition) => {
            if !grants_list {
                return Err(format!(
                    "{role}/{sid} (statement #{index}): statement carries a Condition but \
                     its Action does not include s3:ListBucket -- only the ListBucket \
                     s3:prefix Condition is read by any guard, so a Condition on any other \
                     statement sits unexamined and must fail closed"
                ));
            }
            validate_condition(role, sid, index, condition)?;
        }
        None => {
            if grants_list {
                return Err(format!(
                    "{role}/{sid} (statement #{index}): s3:ListBucket statement carries no \
                     Condition -- an unconstrained bucket-wide list must be rejected, the \
                     same as a missing Resource; it must carry a non-empty \
                     StringLike/s3:prefix Condition"
                ));
            }
        }
    }

    Ok(())
}

/// The Action/Resource half of the choke point: every action must grant an
/// operation in the vocabulary, and every resource must have a shape that some
/// granted operation class asks for, with no granted class left without one.
///
/// This is what makes the resource helpers total. Round three put the same
/// question inside `bucket_relative_s3_pattern`, which meant each helper decided
/// on its own which resources it understood, and everything else came back as
/// "nothing to check": an access-point ARN, an Object Lambda ARN, `arn:*` and a
/// malformed `my-ravel-bucket/*` were all real S3 object grants reaching outside
/// the bucket and all silently dropped (H1), and the list-only exemption added to
/// keep the bare bucket ARN from tripping the strip left list statements'
/// Resource examined by nothing at all (H2). Asking it here instead answers it
/// once, for every axis, before any helper runs.
///
/// The three classes are independent, so a statement granting several carries the
/// resources of each. That is what makes the normal IAM idiom -- one statement
/// granting `s3:ListBucket` and `s3:GetObject` over the bucket ARN plus an object
/// prefix -- valid rather than a rejection with a misleading reason.
fn validate_actions_and_resources(
    role: &str,
    sid: &str,
    index: usize,
    actions: &[String],
    resources: &[&str],
) -> Result<(), String> {
    for action in actions {
        let known = action_grants_any(action, &S3_OBJECT_OPERATIONS)
            || action_grants_any(action, &S3_BUCKET_OPERATIONS)
            || action_selects_kms(action);
        if !known {
            return Err(format!(
                "{role}/{sid} (statement #{index}): Action {action:?} grants no operation \
                 any guard in this file reasons about (S3 object {S3_OBJECT_OPERATIONS:?}, \
                 S3 bucket {S3_BUCKET_OPERATIONS:?}, KMS {KMS_OTHER_OPERATIONS:?} + \
                 {KMS_DATA_KEY_OPERATIONS:?}) -- no axis would select this statement, so \
                 its Resource would sit unexamined"
            ));
        }
    }

    let grants_object = any_action_grants_any(actions, &S3_OBJECT_OPERATIONS);
    let grants_list = any_action_grants_any(actions, &S3_BUCKET_OPERATIONS);
    let grants_kms = actions.iter().any(|a| action_selects_kms(a));

    let mut saw_object = false;
    let mut saw_bucket = false;
    let mut saw_kms = false;
    for resource in resources {
        match classify_resource(resource) {
            ResourceShape::ObjectKey(_) => {
                if !grants_object {
                    return Err(format!(
                        "{role}/{sid} (statement #{index}): Resource {resource:?} names an \
                         object under {BUCKET_ARN:?}, but the statement's Action \
                         {actions:?} grants no S3 object operation \
                         ({S3_OBJECT_OPERATIONS:?}) -- an object ARN on a statement that \
                         cannot act on an object is a shape no axis reads"
                    ));
                }
                saw_object = true;
            }
            ResourceShape::Bucket => {
                if !grants_list {
                    return Err(format!(
                        "{role}/{sid} (statement #{index}): Resource {resource:?} is the \
                         bare bucket ARN, but the statement's Action {actions:?} grants no \
                         S3 bucket operation ({S3_BUCKET_OPERATIONS:?}) -- an S3 object \
                         operation must name a bucket-relative object ARN, not the bucket"
                    ));
                }
                saw_bucket = true;
            }
            ResourceShape::KmsKey => {
                if !grants_kms {
                    return Err(format!(
                        "{role}/{sid} (statement #{index}): Resource {resource:?} is a KMS \
                         key ARN, but the statement's Action {actions:?} grants no KMS \
                         operation -- the KMS resource guards select on the action, so this \
                         ARN would be checked by nothing"
                    ));
                }
                saw_kms = true;
            }
            ResourceShape::Unclassified => {
                return Err(format!(
                    "{role}/{sid} (statement #{index}): Resource {resource:?} is neither \
                     bucket-relative to {BUCKET_ARN:?}, nor exactly that bucket ARN, nor \
                     an {KMS_ARN_PREFIX:?} key ARN -- an access-point or Object Lambda or \
                     multi-region-access-point ARN, \"*\", \"arn:*\", another bucket, or a \
                     malformed non-ARN grants access no guard in this file can check, so \
                     it must fail closed rather than be dropped as nothing to check \
                     (issue #1346, H1)"
                ));
            }
        }
    }

    if grants_object && !saw_object {
        return Err(format!(
            "{role}/{sid} (statement #{index}): Action {actions:?} grants an S3 object \
             operation but Resource {resources:?} names no object under {BUCKET_ARN:?} -- \
             the object axis would derive an empty pattern set and skip the statement"
        ));
    }
    if grants_list && !saw_bucket {
        return Err(format!(
            "{role}/{sid} (statement #{index}): Action {actions:?} grants an S3 bucket \
             operation but Resource {resources:?} is not the bare bucket ARN \
             {BUCKET_ARN:?} -- a list grant on any other resource enumerates a bucket this \
             file models nothing about, and round three's list-only exemption left exactly \
             this shape read by no guard (issue #1346, H2)"
        ));
    }
    if grants_kms && !saw_kms {
        return Err(format!(
            "{role}/{sid} (statement #{index}): Action {actions:?} grants a KMS operation \
             but Resource {resources:?} names no {KMS_ARN_PREFIX:?} key ARN -- \
             kms_statement_resources would return an empty resource list and both KMS \
             resource guards would examine nothing"
        ));
    }
    Ok(())
}

/// Validate a statement's `Condition` sub-shape against the exact operators and
/// keys the guards read (`HANDLED_CONDITION_OPERATORS` / `HANDLED_CONDITION_KEYS`).
/// The Condition must be a NON-EMPTY JSON object; every operator in it must be
/// handled and map to a NON-EMPTY object; every key under a handled operator must
/// be handled and map to a string or non-empty array of strings (the shape
/// `list_prefix_patterns` reads). Anything else -- an unhandled operator such as
/// `StringNotLike` or a set-qualified `ForAnyValue:StringLike`, an unhandled key
/// such as `s3:delimiter`, a non-object Condition, or an empty `{}` /
/// `{"StringLike": {}}` (a `for` over an empty map iterates zero times, so it used
/// to return Ok and constrain nothing) -- is a shape no guard reasons about and is
/// rejected by name (issue #1346, F2). Without this, a ListBucket statement
/// carrying such a Condition passes validation and `list_prefix_patterns` then
/// finds no `["StringLike"]["s3:prefix"]` array and silently contributes nothing.
///
/// A non-empty `StringLike` map whose every key is handled forces `s3:prefix` to
/// be present (it is the only handled key), so no separate presence check is
/// needed.
fn validate_condition(
    role: &str,
    sid: &str,
    index: usize,
    condition: &serde_json::Value,
) -> Result<(), String> {
    let cond_obj = condition.as_object().ok_or_else(|| {
        format!("{role}/{sid} (statement #{index}): Condition is not a JSON object: {condition:?}")
    })?;
    if cond_obj.is_empty() {
        return Err(format!(
            "{role}/{sid} (statement #{index}): Condition is an empty object -- an empty \
             Condition constrains nothing, so a ListBucket statement carrying it is an \
             unconstrained bucket-wide list; it must be a non-empty StringLike/s3:prefix \
             block (issue #1346, F2)"
        ));
    }
    for (operator, keys) in cond_obj {
        if !HANDLED_CONDITION_OPERATORS.contains(&operator.as_str()) {
            return Err(format!(
                "{role}/{sid} (statement #{index}): Condition uses operator {operator:?}, \
                 which no guard in this file reads (handled operators: \
                 {HANDLED_CONDITION_OPERATORS:?}); a different or set-qualified operator \
                 such as StringNotLike or ForAnyValue:StringLike must fail closed rather \
                 than sit unexamined"
            ));
        }
        let key_obj = keys.as_object().ok_or_else(|| {
            format!(
                "{role}/{sid} (statement #{index}): Condition operator {operator:?} is not a \
                 JSON object: {keys:?}"
            )
        })?;
        if key_obj.is_empty() {
            return Err(format!(
                "{role}/{sid} (statement #{index}): Condition operator {operator:?} is an \
                 empty object -- it must map s3:prefix to a non-empty value; an empty \
                 operator map constrains nothing (issue #1346, F2)"
            ));
        }
        for (cond_key, value) in key_obj {
            if !HANDLED_CONDITION_KEYS.contains(&cond_key.as_str()) {
                return Err(format!(
                    "{role}/{sid} (statement #{index}): Condition operator {operator:?} names \
                     key {cond_key:?}, which no guard in this file reads (handled keys: \
                     {HANDLED_CONDITION_KEYS:?}); it must fail closed rather than sit \
                     unexamined"
                ));
            }
            if !is_string_or_string_array(Some(value)) {
                return Err(format!(
                    "{role}/{sid} (statement #{index}): Condition {operator:?}.{cond_key:?} is \
                     neither a string nor a non-empty array of strings: {value:?}"
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

/// The on-disk path of a shipped template.
fn policy_json_path(role: &str) -> String {
    format!(
        "{}/../../deploy/iam/{role}.json",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn load_policy(role: &'static str) -> Policy {
    load_policy_from(role, &policy_json_path(role))
}

/// The real file-reading entry point: read the policy at `path`, parse it, and
/// run the full per-statement validation through `build_policy`. `load_policy`
/// is exactly this against the fixed `deploy/iam/{role}.json` paths; a
/// regression test drives it with a synthetic invalid file so the validation is
/// exercised through the entry point production uses, not only through
/// `build_policy` (issue #1346, F4).
fn load_policy_from(role: &'static str, path: &str) -> Policy {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let json: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    build_policy(role, path, &json)
}

/// Every action name in a statement's `Action` (a bare string or an array),
/// with the policy's own capitalization preserved so a failure message quotes
/// what the template actually says.
///
/// The `_ => Vec::new()` arm and the `filter_map` are unreachable for any
/// statement that passed the choke point: `validate_statement` runs
/// `is_string_or_string_array` on `Action` BEFORE calling this, so a non-string,
/// a non-array, an empty array, and an array holding a non-string are all
/// already rejected. The empty return survives only for the pre-fix fixtures,
/// which call this on a raw `NotAction` statement precisely to pin that the
/// action used to be invisible.
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

/// Every resource string in a statement's `Resource` (a bare string or an
/// array). Unreachable-arm justification is the same as `statement_actions`:
/// `validate_statement` checks `Resource` with `is_string_or_string_array`
/// before anything reads it.
fn statement_resources(stmt: &serde_json::Value) -> Vec<&str> {
    match &stmt["Resource"] {
        serde_json::Value::String(s) => vec![s.as_str()],
        serde_json::Value::Array(a) => a.iter().filter_map(|v| v.as_str()).collect(),
        _ => Vec::new(),
    }
}

/// A statement's `Sid`. Total for anything that passed the choke point, which
/// requires a present string Sid so every message can name the statement it is
/// about; the old `unwrap_or("<no Sid>")` fallback named nothing.
fn statement_sid(stmt: &serde_json::Value) -> &str {
    stmt["Sid"].as_str().expect(
        "the choke point (validate_statement) requires a present string Sid, so any \
         statement reaching a guard has one",
    )
}

/// `s3:prefix` patterns from the `Condition.StringLike` block of every statement
/// whose `Effect` matches `effect` (`None` matches any) and whose `Action` grants
/// a list operation.
///
/// The `effect` argument is the same one `key_patterns_for` takes, and it exists
/// for the same reason: a prefix is read as a permission or a prohibition
/// depending on which caller reads it, and an explicit IAM `Deny` wins over an
/// `Allow`. `discovery_prefix_admitted_for_every_discovering_role` reads the
/// result as the prefixes a role is ALLOWED to list, so it must pass
/// `Some("Allow")`: pooling a `Deny` block's `s3:prefix` into that vector would
/// assert the inverse of the fact, admitting a discovery prefix that an explicit
/// Deny withdraws (issue #1346, F1). A caller that only checks each prefix names a
/// real key shape (`every_in_scope_policy_pattern_matches_a_real_key_shape`) is
/// effect-agnostic and passes `None`.
///
/// Selection goes through `action_grants`, so `"Action": "s3:*"` is read here
/// too. Under round three's exact-name detection it was not, which is half of
/// H3: an `s3:*` statement granted an unconstrained ListBucket while this
/// function read nothing from it.
fn list_prefix_patterns(policy: &Policy, effect: Option<&str>) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        if let Some(effect) = effect {
            let has_effect = stmt["Effect"]
                .as_str()
                .is_some_and(|e| e.eq_ignore_ascii_case(effect));
            if !has_effect {
                continue;
            }
        }
        if !any_action_grants_any(&statement_actions(stmt), &S3_BUCKET_OPERATIONS) {
            continue;
        }
        // IAM allows a single `s3:prefix` value as a bare string or an array;
        // read both so a bare-string prefix is not silently skipped. Any other
        // shape is unreachable: the choke point requires a list statement to
        // carry a non-empty StringLike/s3:prefix Condition whose value is a
        // string or non-empty array of strings, so the panic arm fires only for
        // a statement that never passed validation.
        match &stmt["Condition"]["StringLike"]["s3:prefix"] {
            serde_json::Value::String(s) => out.push(s.clone()),
            serde_json::Value::Array(patterns) => {
                for p in patterns {
                    out.push(p.as_str().expect("s3:prefix entry is a string").to_string());
                }
            }
            other => panic!(
                "{}/{}: list statement's Condition.StringLike.s3:prefix is {other:?} -- the \
                 choke point requires a string or non-empty array of strings here, so \
                 reaching this means a guard ran on an unvalidated statement",
                policy.role,
                statement_sid(stmt)
            ),
        }
    }
    out
}

/// A policy's statement array. `Statement`-is-an-array is asserted by
/// `build_policy` and `validate_policy_statements` before a `Policy` exists.
fn policy_statements(policy: &Policy) -> &Vec<serde_json::Value> {
    policy
        .statements
        .as_array()
        .expect("a Policy's Statement is an array (checked by build_policy)")
}

/// The bucket-relative key patterns among one statement's `resources`.
///
/// Total over `ResourceShape`, which is the point of the redesign: every arm is
/// named and justified by a choke-point rule, so there is no catch-all that can
/// quietly absorb a shape nobody thought about.
///
/// - `ObjectKey` is the pattern to check, returned.
/// - `Bucket` contributes nothing to strip. Skipping it is safe because
///   `validate_actions_and_resources` proved the same statement grants a list
///   operation, and a list grant is checked on both counts: its Resource must be
///   exactly this ARN, and its Condition must be the `s3:prefix` block
///   `list_prefix_patterns` reads. This is the arm that makes the normal mixed
///   ListBucket+GetObject idiom work without a misleading rejection, and round
///   three's alternative -- exempting whole list statements at the caller --
///   is what left list Resources read by nothing (H2).
/// - `KmsKey` likewise: the choke point proved the statement grants a KMS
///   operation, so `kms_statement_resources` selects it and both KMS resource
///   guards run over this exact string.
/// - `Unclassified` panics. The choke point rejects it, so reaching here means a
///   guard ran on a statement that never passed validation (a synthetic fixture
///   built straight from `Policy`), and the loud failure is the belt-and-braces
///   half of the H1 fix.
///
/// What this does NOT close: a bucket-relative FULL-bucket grant
/// `arn:aws:s3:::my-ravel-bucket/*` still passes. It strips to `"*"`, is not
/// out-of-scope, and matches every representative key, so
/// `every_in_scope_policy_pattern_matches_a_real_key_shape` -- which asserts a
/// pattern matches AT LEAST ONE real key -- accepts it by construction: a
/// coverage check cannot reject an over-broad pattern. Rejecting an in-bucket
/// grant that is too wide is a separate guard, out of scope here.
fn object_key_patterns(role: &str, sid: &str, resources: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    for resource in resources {
        match classify_resource(resource) {
            ResourceShape::ObjectKey(key_pattern) => out.push(key_pattern.to_string()),
            ResourceShape::Bucket | ResourceShape::KmsKey => {}
            ResourceShape::Unclassified => panic!(
                "{role}/{sid}: statement names resource {resource:?}, which is neither \
                 bucket-relative to {BUCKET_ARN:?}, nor exactly that bucket ARN, nor an \
                 {KMS_ARN_PREFIX:?} key ARN -- validate_statement rejects this shape, so \
                 reaching here means a guard ran on an unvalidated statement (issue \
                 #1346, H1)"
            ),
        }
    }
    out
}

/// Bucket-relative key patterns from every statement whose `Effect` matches
/// `effect` (`None` matches any) and whose `Action` grants at least one of
/// `operations`.
///
/// The single resource-collection loop every S3 axis uses. Round three had four
/// near-copies of it (read, put, delete-Allow, delete-Deny) that differed in
/// which actions they selected, whether the selection resolved wildcards, and
/// what they did with a resource they could not strip; the read copy and the
/// delete copy then disagreed about the bare bucket ARN, which is how the
/// list-only exemption got added to one of them. There is now one loop, one
/// action predicate, and one resource classifier.
///
/// The `continue`s are axis selection, not shape skips: "this statement grants
/// no operation on my axis" and "this statement is the other Effect". The choke
/// point's coverage rule guarantees every statement is selected by at least one
/// axis, and whichever axis selects it sees every resource it carries.
fn key_patterns_for(policy: &Policy, operations: &[&str], effect: Option<&str>) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        if let Some(effect) = effect {
            let has_effect = stmt["Effect"]
                .as_str()
                .is_some_and(|e| e.eq_ignore_ascii_case(effect));
            if !has_effect {
                continue;
            }
        }
        if !any_action_grants_any(&statement_actions(stmt), operations) {
            continue;
        }
        out.extend(object_key_patterns(
            policy.role,
            statement_sid(stmt),
            &statement_resources(stmt),
        ));
    }
    out
}

/// Bucket-relative key patterns from every statement granting an S3 object
/// operation (`GetObject`, `PutObject`, `DeleteObject`, ...), either Effect.
///
/// Selection is by grant, not by "not ListBucket": a list-only statement grants
/// no object operation and is not selected, and a mixed ListBucket+GetObject
/// statement IS selected and contributes its object patterns.
fn resource_key_patterns(policy: &Policy) -> Vec<String> {
    key_patterns_for(policy, &S3_OBJECT_OPERATIONS, None)
}

/// Bucket-relative key patterns from every statement whose `Action` grants
/// `s3:PutObject` --- the writes that go through `KmsRoutingStore` and can
/// select a per-tenant key. Delete/Get/Deny statements are excluded: they never
/// route (reads and deletes delegate to the default store unconditionally, see
/// `kms_routing.rs`).
///
/// Delete and Get are excluded by action selection; `Deny` is excluded by the
/// `Some("Allow")` effect argument. Its consumer,
/// `roles_writing_routed_objects_have_kms_grant`, reads the result as the routed
/// writes a role performs and then demands a KMS grant for them, so a `Deny`
/// PutObject read as a routed write would demand a KMS grant for a write the role
/// cannot perform --- the inverse of the fact, since an explicit Deny withdraws
/// the write (issue #1346, F1/F2). Passing `None` here left `Deny` matching, so
/// the sentence above was true of Delete/Get and false of Deny; `Some("Allow")`
/// makes it true.
///
/// `"Action": "s3:*"` grants PutObject and so is selected here, which round
/// three's exact-name match missed (H3).
fn put_resource_key_patterns(policy: &Policy) -> Vec<String> {
    key_patterns_for(policy, &["s3:PutObject"], Some("Allow"))
}

/// Bucket-relative key patterns from every statement whose `Effect` is `effect`
/// and whose `Action` grants an S3 delete operation.
///
/// `"Allow"` returns the delete capability a role actually holds. The
/// `DenyDeleteProtected` block names delete actions too, but it withdraws
/// capability rather than granting it (and an explicit IAM `Deny` always wins),
/// so it is selected separately by `"Deny"`, which returns the keys that block
/// protects (ADR-0055 §3).
fn delete_key_patterns(policy: &Policy, effect: &str) -> Vec<String> {
    key_patterns_for(policy, &S3_DELETE_OPERATIONS, Some(effect))
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

/// `kms:*` action strings granted by an `Allow` statement anywhere in `policy`
/// (`Action` as a bare string or an array), regardless of statement Sid.
///
/// Allow-only, for the F1 reason applied to actions rather than resources: every
/// caller reads the result as a grant the role HOLDS
/// (`write_roles_have_kms_generate_data_key`,
/// `roles_writing_routed_objects_have_kms_grant`, `every_role_has_kms_decrypt`) or
/// as the absence of one (`admin_has_no_kms_generate_data_key`), and an explicit
/// IAM `Deny` grants nothing. Pooling a `Deny kms:GenerateDataKey` into this
/// vector would report a role able to mint ciphertext when the Deny withdraws
/// exactly that, and would make the negative admin assertion fail on a policy that
/// safely denies the operation --- the inverse of the fact in both directions
/// (issue #1346, F1 sweep). A `Deny` KMS statement passes the choke point, so this
/// case is reachable, not hypothetical.
///
/// Selection goes through `action_selects_kms`, so `KMS:GenerateDataKey*` is
/// returned and so is a wildcard action (`"*"`, `"kms:*"`) that grants a KMS
/// operation without naming the service literally --- which the old case-folded
/// `kms:` prefix test missed. The strings themselves keep the template's
/// capitalization, so a caller's failure message shows what the policy said
/// rather than a normalized form the operator would then grep for in vain.
/// Callers ask `action_grants` what a returned string grants; none compares it
/// literally.
fn kms_actions(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        let is_allow = stmt["Effect"]
            .as_str()
            .is_some_and(|e| e.eq_ignore_ascii_case("Allow"));
        if !is_allow {
            continue;
        }
        out.extend(
            statement_actions(stmt)
                .into_iter()
                .filter(|a| action_selects_kms(a)),
        );
    }
    out
}

/// Sid and KMS key ARNs for every `Allow` statement granting a KMS operation.
/// Both KMS resource guards below go through this one selection rule, so a change
/// to it cannot reach one guard and miss the other.
///
/// Allow-only, for the F1 reason: both guards read the result as a GRANT that must
/// be narrowly scoped (`no_kms_statement_grants_every_key_in_the_region`,
/// `every_kms_statement_names_a_key_id`). An explicit `Deny` grants nothing, so a
/// `Deny` naming `key/*` is a broad prohibition (safe), yet these guards would
/// flag it as an account-wide grant --- the inverse of the fact. A `Deny` KMS
/// statement passes the choke point, so this is reachable; scoping is asked only
/// of the grants (issue #1346, F1 sweep).
///
/// The returned resources are filtered to `ResourceShape::KmsKey`, so a mixed
/// statement's S3 object ARNs are not handed to the KMS ARN-shape assertions
/// (which would reject them for the wrong reason). Nothing is lost: the choke
/// point proved a KMS-granting statement carries at least one KMS ARN, and any
/// object ARN it also carries is read by the object axis.
fn kms_statement_resources(policy: &Policy) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        let is_allow = stmt["Effect"]
            .as_str()
            .is_some_and(|e| e.eq_ignore_ascii_case("Allow"));
        if !is_allow {
            continue;
        }
        if !statement_actions(stmt)
            .iter()
            .any(|a| action_selects_kms(a))
        {
            continue;
        }
        let resources: Vec<String> = statement_resources(stmt)
            .into_iter()
            .filter(|r| matches!(classify_resource(r), ResourceShape::KmsKey))
            .map(str::to_string)
            .collect();
        out.push((statement_sid(stmt).to_string(), resources));
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
                .any(|a| action_grants_any(a, &KMS_DATA_KEY_OPERATIONS)),
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
                .any(|a| action_grants_any(a, &KMS_DATA_KEY_OPERATIONS)),
            "{role}: PUTs routed object class(es) {routed:?} but policy lacks \
             kms:GenerateDataKey* -- those writes fail closed under \
             --tenant-kms-config. Found kms actions: {actions:?}"
        );
        assert!(
            actions.iter().any(|a| action_grants(a, "kms:Encrypt")),
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
            .any(|a| action_grants_any(a, &KMS_DATA_KEY_OPERATIONS)),
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
///
/// Every pre-fix body in this file spells its own comparisons out (here
/// `eq_ignore_ascii_case`, below `strip_prefix` against a literal ARN) rather
/// than calling a live helper: a pre-fix copy that reuses today's predicates
/// stops proving the hole the moment those predicates change, which is how a
/// fixture becomes a tautology.
fn pre_fix_allow_delete_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        let is_allow = stmt["Effect"]
            .as_str()
            .is_some_and(|e| e.eq_ignore_ascii_case("Allow"));
        if !is_allow {
            continue;
        }
        let grants_delete = statement_actions(stmt)
            .iter()
            .any(|a| a.eq_ignore_ascii_case("s3:DeleteObject"));
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
        let stmt = &policy_statements(&policy)[0];
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
        let pre_fix_action_hit = actions
            .iter()
            .any(|a| a.eq_ignore_ascii_case("s3:DeleteObject"));
        assert_eq!(
            pre_fix_action_hit, pre_fix_exact_recognizes,
            "fixture {sid} invalid: expected pre-fix exact action recognition = \
             {pre_fix_exact_recognizes} for {actions:?}"
        );
        assert!(
            any_action_grants_any(&actions, &S3_DELETE_OPERATIONS),
            "fixture {sid} invalid: the post-fix action predicate must recognize \
             {actions:?} as a delete grant"
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
            actions.iter().any(|a| action_grants(a, "kms:Decrypt")),
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
            assert!(
                !resources.is_empty(),
                "{role}/{sid}: statement grants a KMS operation but names no \
                 {KMS_ARN_PREFIX:?} key ARN -- this guard would examine nothing \
                 (validate_statement rejects the shape, so a shipped template reaching \
                 here means the choke point was bypassed)"
            );
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
            assert!(
                !resources.is_empty(),
                "{role}/{sid}: statement grants a KMS operation but names no \
                 {KMS_ARN_PREFIX:?} key ARN -- this guard would examine nothing \
                 (validate_statement rejects the shape, so a shipped template reaching \
                 here means the choke point was bypassed)"
            );
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
    let selected_after_fix = actions.iter().any(|a| action_selects_kms(a));
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
            .any(|a| action_grants_any(a, &KMS_DATA_KEY_OPERATIONS)),
        "the post-fix matcher must see KMS:GenerateDataKey* as the \
         kms:GenerateDataKey* grant it is. Found kms actions: {actions:?}"
    );
    assert!(
        actions.iter().any(|a| action_grants(a, "kms:Decrypt")),
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
                pre_fix_list_prefix_patterns(&policy).is_empty(),
                "fixture {sid}: the pre-fix list_prefix_patterns was expected to \
                 skip the statement (finding no StringLike/s3:prefix); it did not"
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
        list_prefix_patterns(&policy, None),
        vec!["t/".to_string()],
        "a bare-string s3:prefix must be read, not skipped"
    );
    // ...and the shipped array shape still validates and is read.
    assert!(
        validate_statement("fixture", 0, &policy_statements(&policy)[0]).is_ok(),
        "a bare-string s3:prefix is a valid IAM shape and must pass validation"
    );
}

#[test]
fn discovery_prefix_admitted_for_every_discovering_role() {
    for role in ROLES_WITH_DISCOVERY {
        let policy = load_policy(role);
        // Allow-only: an explicit Deny ListBucket withdraws listing, so its
        // s3:prefix is not a prefix the role is ALLOWED to discover. Reading it
        // here would assert the inverse of the fact (issue #1346, F1).
        let patterns = list_prefix_patterns(&policy, Some("Allow"));
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
        // None: this is a shape check (does every pattern name a real key?), not
        // a permission read, so a Deny list prefix must still name a real key.
        let mut patterns = list_prefix_patterns(&policy, None);
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

/// The pre-fix `list_prefix_patterns` body, verbatim: an exact-name list
/// detection and a `_ => {}` arm that contributed nothing for any Condition
/// sub-shape it did not recognize. Kept so the Condition fixtures can pin that
/// the constraint used to sit unread; the live helper now panics on that arm,
/// because the choke point rejects every shape that could reach it.
fn pre_fix_list_prefix_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        let is_list = statement_actions(stmt)
            .iter()
            .any(|a| a.eq_ignore_ascii_case("s3:ListBucket"));
        if !is_list {
            continue;
        }
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

/// The pre-fix `resource_key_patterns` body, verbatim: `is_list_only` read from
/// `stmt["Action"].as_str()` only, and a resource that did not strip the bucket
/// prefix silently dropped (`if let Some(..)` with no `else`). Kept so the F1
/// fixture pins the two holes existed rather than restating the fix.
fn pre_fix_resource_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        let is_list_only = stmt["Action"]
            .as_str()
            .is_some_and(|a| a.eq_ignore_ascii_case("s3:ListBucket"));
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

/// The pre-fix `put_resource_key_patterns` body, verbatim (same silent drop).
fn pre_fix_put_resource_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        let grants_put = statement_actions(stmt)
            .iter()
            .any(|a| a.eq_ignore_ascii_case("s3:PutObject"));
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

/// F1 regression: a Resource that grants S3 object access but is not
/// bucket-relative (`"*"`, `s3:*` on `"*"`, another bucket's ARN) must surface as
/// a test failure naming the role and Sid, in BOTH the read and write resource
/// helpers, not be silently dropped. Synthetic statements, not `deploy/iam/*.json`:
/// a fixture over the (correct) shipped templates passes whichever way the strip
/// behaves and proves nothing. Each case asserts in both directions -- that the
/// pre-fix helper dropped it (returning an empty set, hiding the grant) and that
/// the post-fix helper panics on it.
#[test]
fn out_of_bucket_resource_grant_is_not_a_bypass() {
    // Read side (resource_key_patterns): (Sid, Action, Resource).
    let read_cases = [
        ("GetOnStar", serde_json::json!("s3:GetObject"), "*"),
        (
            "GetOnOtherBucket",
            serde_json::json!("s3:GetObject"),
            "arn:aws:s3:::other-bucket/t/*",
        ),
        ("StarActionStarResource", serde_json::json!("s3:*"), "*"),
    ];
    for (sid, action, resource) in &read_cases {
        let policy = Policy {
            role: "fixture",
            statements: serde_json::json!([{
                "Sid": sid,
                "Effect": "Allow",
                "Action": action,
                "Resource": resource,
            }]),
        };
        // Observation 1 (load-bearing): the pre-fix helper dropped the
        // out-of-bucket resource, so the derived set was empty and every resource
        // guard skipped the statement. If it stops being empty the fixture no
        // longer proves the hole and must be rewritten, not deleted.
        let pre = pre_fix_resource_key_patterns(&policy);
        assert!(
            pre.is_empty(),
            "fixture {sid} invalid: pre-fix resource_key_patterns was expected to \
             drop the out-of-bucket resource (returning nothing); returned {pre:?}"
        );
        // Observation 2: the post-fix helper panics naming the role and Sid.
        let guard = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            resource_key_patterns(&policy)
        }));
        assert!(
            guard.is_err(),
            "resource_key_patterns must reject fixture {sid}: an S3 grant on \
             {resource:?} must surface, not be silently dropped"
        );
    }

    // Write side (put_resource_key_patterns): PutObject on "*". Dropping it left
    // roles_writing_routed_objects_have_kms_grant an empty routed set, so it
    // skipped the role's KMS-grant check entirely.
    let put_policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "PutOnStar",
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "*",
        }]),
    };
    let pre_put = pre_fix_put_resource_key_patterns(&put_policy);
    assert!(
        pre_put.is_empty(),
        "fixture PutOnStar invalid: pre-fix put_resource_key_patterns was expected \
         to drop the \"*\" resource; returned {pre_put:?}"
    );
    let put_guard = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        put_resource_key_patterns(&put_policy)
    }));
    assert!(
        put_guard.is_err(),
        "put_resource_key_patterns must reject PutObject on \"*\": dropping it \
         makes roles_writing_routed_objects_have_kms_grant skip the role on an \
         empty routed set"
    );

    // Trap (must not trip the new guard): an array-form ListBucket statement's
    // bare bucket ARN must be SKIPPED, not panicked. The pre-fix `.as_str()`
    // is_list_only returned None for the array form, so the bare bucket ARN would
    // reach the now-fatal strip; the post-fix statement_actions-based detection
    // recognizes it as list-only and skips it.
    let list_policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "ArrayFormList",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": "arn:aws:s3:::my-ravel-bucket",
        }]),
    };
    let pre_fix_is_list_only = policy_statements(&list_policy)[0]["Action"]
        .as_str()
        .is_some_and(|a| a.eq_ignore_ascii_case("s3:ListBucket"));
    assert!(
        !pre_fix_is_list_only,
        "fixture ArrayFormList invalid: the pre-fix as_str() detection was \
         expected to miss the array-form ListBucket (proving the trap is real)"
    );
    let list_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        resource_key_patterns(&list_policy)
    }));
    assert_eq!(
        list_result.ok(),
        Some(Vec::<String>::new()),
        "resource_key_patterns must skip an array-form ListBucket statement, not \
         panic on its bare bucket ARN"
    );

    // A kms: ARN on a non-list statement is skipped (None), not panicked: the KMS
    // resource guards own it. This is why the read-path strip cannot blindly
    // panic on every non-bucket-relative resource -- every role carries a
    // *TenantKms statement whose Resource is a kms: ARN.
    let kms_policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "KmsResource",
            "Effect": "Allow",
            "Action": "kms:Decrypt",
            "Resource": "arn:aws:kms:us-east-1:111122223333:key/abcd1234-5678-90ab-cdef-1234567890ab",
        }]),
    };
    let kms_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        resource_key_patterns(&kms_policy)
    }));
    assert_eq!(
        kms_result.ok(),
        Some(Vec::<String>::new()),
        "resource_key_patterns must skip a kms: ARN (checked by the KMS guards), \
         not panic"
    );
}

/// The pre-fix `validate_condition` body, verbatim (round two's version, no
/// empty-object checks): a `for` over an empty map iterates zero times, so `{}`
/// and `{"StringLike": {}}` both returned Ok. Kept so the F2 fixture pins the
/// hole existed rather than restating the fix.
fn pre_fix_validate_condition(condition: &serde_json::Value) -> Result<(), String> {
    let cond_obj = condition
        .as_object()
        .ok_or_else(|| "Condition is not an object".to_string())?;
    for (operator, keys) in cond_obj {
        if !HANDLED_CONDITION_OPERATORS.contains(&operator.as_str()) {
            return Err(format!("operator {operator:?}"));
        }
        let key_obj = keys
            .as_object()
            .ok_or_else(|| "operator is not an object".to_string())?;
        for (cond_key, value) in key_obj {
            if !HANDLED_CONDITION_KEYS.contains(&cond_key.as_str()) {
                return Err(format!("key {cond_key:?}"));
            }
            if !is_string_or_string_array(Some(value)) {
                return Err(format!("value {value:?}"));
            }
        }
    }
    Ok(())
}

/// F2 regression (empty maps): `"Condition": {}` and `"Condition": {"StringLike":
/// {}}` must fail closed. Pre-fix, a `for` over an empty map iterated zero times,
/// so both returned Ok -- the vacuous-set bug round two fixed for arrays,
/// re-created in the code that fixed it. Synthetic, not `deploy/iam/*.json`: the
/// shipped Conditions are all non-empty StringLike/s3:prefix blocks.
#[test]
fn empty_condition_or_stringlike_map_fails_closed() {
    let cases = [
        ("EmptyCondition", serde_json::json!({})),
        ("EmptyStringLike", serde_json::json!({"StringLike": {}})),
    ];
    for (sid, condition) in &cases {
        // Observation 1 (load-bearing): the pre-fix validator accepted the empty
        // map (zero loop iterations), so the constraint sat unexamined.
        assert!(
            pre_fix_validate_condition(condition).is_ok(),
            "fixture {sid} invalid: pre-fix validate_condition was expected to \
             accept {condition:?} (vacuous empty-map loop); it did not"
        );
        // Observation 2: the full statement validator rejects a ListBucket
        // statement carrying it, naming the empty shape and the Sid.
        let stmt = serde_json::json!({
            "Sid": sid,
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::my-ravel-bucket",
            "Condition": condition,
        });
        let err = validate_statement("fixture", 0, &stmt)
            .expect_err(&format!("validate_statement must reject fixture {sid}"));
        assert!(
            err.contains("empty"),
            "fixture {sid}: rejection must name the empty shape; got {err:?}"
        );
        assert!(
            err.contains(sid),
            "fixture {sid}: rejection must name the Sid; got {err:?}"
        );
    }
}

/// F2 regression (Condition presence): a ListBucket statement must carry a
/// Condition, and a non-ListBucket statement must not. Pre-fix, `validate_statement`
/// only validated a Condition when present and never checked the Action, so a
/// ListBucket with no Condition (an unconstrained bucket-wide list) and a
/// StringLike/s3:prefix on the protected-delete Deny (read by no guard, and never
/// fired in AWS since a DeleteObject request carries no s3:prefix) both passed.
/// Synthetic for the same reason as above.
#[test]
fn condition_presence_must_track_list_bucket_action() {
    // Hole: a ListBucket statement with NO Condition. list_prefix_patterns
    // contributes nothing for it, and pre-fix validation accepted it.
    let no_condition = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "ListNoCondition",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::my-ravel-bucket",
        }]),
    };
    assert!(
        pre_fix_list_prefix_patterns(&no_condition).is_empty(),
        "fixture invalid: a ListBucket statement with no Condition must contribute \
         no s3:prefix pattern (the unconstrained-list shape)"
    );
    let err = validate_statement("fixture", 0, &policy_statements(&no_condition)[0])
        .expect_err("validate_statement must reject a ListBucket statement with no Condition");
    assert!(
        err.contains("no Condition"),
        "rejection must name the missing Condition; got {err:?}"
    );
    assert!(
        err.contains("ListNoCondition"),
        "rejection must name the Sid; got {err:?}"
    );

    // Hole: a StringLike/s3:prefix Condition on a NON-ListBucket statement (a
    // protected-delete Deny). list_prefix_patterns never reads it (not
    // ListBucket), so the Condition sits unexamined while pre-fix validation
    // blessed it.
    let deny_with_prefix = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "DenyWithPrefix",
            "Effect": "Deny",
            "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
            "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/prov",
            "Condition": {"StringLike": {"s3:prefix": ["t/*"]}},
        }]),
    };
    assert!(
        list_prefix_patterns(&deny_with_prefix, None).is_empty(),
        "fixture invalid: list_prefix_patterns must skip a non-ListBucket \
         statement, so its Condition sits unread"
    );
    let err = validate_statement("fixture", 0, &policy_statements(&deny_with_prefix)[0])
        .expect_err("validate_statement must reject a Condition on a non-ListBucket statement");
    assert!(
        err.contains("does not include s3:ListBucket"),
        "rejection must explain the non-ListBucket Condition; got {err:?}"
    );
    assert!(
        err.contains("DenyWithPrefix"),
        "rejection must name the Sid; got {err:?}"
    );
}

/// F1 regression (Effect axis, list discovery): a `Deny` ListBucket carrying an
/// s3:prefix must not be read as a prefix the role is ALLOWED to discover. This
/// round's Condition-presence rule forces a deny-listing block to carry an
/// s3:prefix, and `list_prefix_patterns`' only permission-reading caller,
/// `discovery_prefix_admitted_for_every_discovering_role`, pools every
/// statement's prefixes into one vector it reads as allowed listing. Reading a
/// Deny's prefix there asserts the inverse of the fact, since an explicit IAM
/// Deny withdraws listing. The fix gives `list_prefix_patterns` the same effect
/// argument `key_patterns_for` takes and calls it with `Some("Allow")` from the
/// discovery guard. Synthetic, not `deploy/iam/*.json`: no shipped template
/// carries a Deny ListBucket, so a fixture over them proves nothing.
#[test]
fn deny_list_prefix_does_not_admit_a_discovery_prefix() {
    // Arm A: Allow lists only "sys/*"; no statement mentions "t/".
    let arm_a = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "AllowListSys",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::my-ravel-bucket",
            "Condition": {"StringLike": {"s3:prefix": ["sys/*"]}},
        }]),
    };
    // Arm B: same Allow, plus a Deny ListBucket carrying the discovery prefix.
    let arm_b = Policy {
        role: "fixture",
        statements: serde_json::json!([
            {
                "Sid": "AllowListSys",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::my-ravel-bucket",
                "Condition": {"StringLike": {"s3:prefix": ["sys/*"]}},
            },
            {
                "Sid": "DenyListOutsideTenant",
                "Effect": "Deny",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::my-ravel-bucket",
                "Condition": {"StringLike": {"s3:prefix": ["t/"]}},
            },
        ]),
    };

    // Observation 1 (load-bearing): the effect-blind read (`None`, the mode the
    // discovery guard used before the fix) pools the Deny's "t/" into the set in
    // arm B. If this stops admitting "t/" the effect-blindness is gone and the
    // fixture no longer pins the hole.
    assert!(
        list_prefix_patterns(&arm_b, None)
            .iter()
            .any(|p| glob_matches(p, "t/")),
        "fixture invalid: an effect-blind read must pool the Deny ListBucket's \
         s3:prefix \"t/\" into the discovery set (the hole this fixes)"
    );

    // Observation 2: the Allow-only read the discovery guard now performs admits
    // "t/" in NEITHER arm -- arm A never allows it, and arm B only DENIES it.
    for (label, policy) in [("arm A", &arm_a), ("arm B", &arm_b)] {
        let admitted = list_prefix_patterns(policy, Some("Allow"));
        assert!(
            !admitted.iter().any(|p| glob_matches(p, "t/")),
            "{label}: Allow-only list_prefix_patterns must not admit \"t/\" as a \
             discovery prefix (arm A never allows it; arm B only denies it); got \
             {admitted:?}"
        );
    }

    // Both statements are well-formed IAM the choke point accepts, so the Deny
    // really does reach the helper: the fix is the effect filter, not a rejection.
    for (idx, stmt) in policy_statements(&arm_b).iter().enumerate() {
        assert!(
            validate_statement("fixture", idx, stmt).is_ok(),
            "fixture invalid: statement #{idx} must pass the choke point so the \
             Deny reaches list_prefix_patterns"
        );
    }
}

/// F1/F2 regression (Effect axis, routed write): a `Deny` PutObject must not be
/// read as a routed write. `put_resource_key_patterns`' caller
/// `roles_writing_routed_objects_have_kms_grant` reads its result as object
/// classes the role WRITES and then demands a KMS grant for them; a Deny put is
/// a prohibition, so reading it there would demand a KMS grant to satisfy a write
/// the policy forbids. The fix passes `Some("Allow")` to the effect-aware base
/// helper (F2), the consistent choice given F1. Synthetic: no shipped template
/// carries a Deny PutObject.
#[test]
fn deny_put_object_is_not_a_routed_write() {
    let policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "DenyPutRouted",
            "Effect": "Deny",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/catalog/*/HEAD",
        }]),
    };
    // Observation 1 (load-bearing): the effect-blind base helper reads the Deny
    // put as a routed write. If it stops returning the pattern the fixture no
    // longer pins the hole.
    let blind = key_patterns_for(&policy, &["s3:PutObject"], None);
    assert!(
        blind.iter().any(|p| p == "t/*/catalog/*/HEAD"),
        "fixture invalid: an effect-blind read must return the Deny PutObject \
         resource as a routed write (the hole this fixes); got {blind:?}"
    );
    // Observation 2: the Allow-only put helper the caller uses returns nothing.
    let routed = put_resource_key_patterns(&policy);
    assert!(
        routed.is_empty(),
        "put_resource_key_patterns must not read a Deny PutObject as a routed \
         write; got {routed:?}"
    );
    // The Deny reaches the helper: it passes the choke point.
    assert!(
        validate_statement("fixture", 0, &policy_statements(&policy)[0]).is_ok(),
        "fixture invalid: the Deny PutObject must pass the choke point so it \
         reaches put_resource_key_patterns"
    );
}

/// F1 regression (Effect axis, KMS actions): a `Deny kms:GenerateDataKey` must
/// not be read as a held grant. Every `kms_actions` caller reads its result as a
/// KMS operation the role HOLDS (or, for admin, the absence of one); an explicit
/// Deny grants nothing, so pooling it would report a role able to mint ciphertext
/// the Deny withdraws, and would flip the negative admin assertion. The fix
/// filters `kms_actions` to `Allow` internally, since every caller reads a grant.
/// Synthetic: no shipped template carries a Deny KMS statement.
#[test]
fn deny_kms_action_is_not_read_as_a_grant() {
    let policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "DenyGenerateDataKey",
            "Effect": "Deny",
            "Action": "kms:GenerateDataKey",
            "Resource": "arn:aws:kms:us-east-1:111122223333:key/abcd1234-5678-90ab-cdef-1234567890ab",
        }]),
    };
    // Observation 1 (load-bearing): the pre-fix effect-blind scan (the live body
    // minus its `Allow` filter) pooled the Deny action into the grant set.
    let mut blind: Vec<String> = Vec::new();
    for stmt in policy_statements(&policy) {
        blind.extend(
            statement_actions(stmt)
                .into_iter()
                .filter(|a| action_selects_kms(a)),
        );
    }
    assert!(
        blind
            .iter()
            .any(|a| action_grants_any(a, &KMS_DATA_KEY_OPERATIONS)),
        "fixture invalid: an effect-blind scan must pool the Deny \
         kms:GenerateDataKey into the grant set (the hole this fixes); got {blind:?}"
    );
    // Observation 2: the Allow-only kms_actions returns nothing.
    let actions = kms_actions(&policy);
    assert!(
        actions.is_empty(),
        "kms_actions must not read a Deny KMS statement as a held grant; got \
         {actions:?}"
    );
    // The Deny reaches the helper: it passes the choke point.
    assert!(
        validate_statement("fixture", 0, &policy_statements(&policy)[0]).is_ok(),
        "fixture invalid: the Deny KMS statement must pass the choke point so it \
         reaches kms_actions"
    );
}

/// F1 regression (Effect axis, KMS resources): a `Deny` naming `key/*` is a broad
/// prohibition (safe), but both KMS resource guards read `kms_statement_resources`
/// as a grant that must be narrowly scoped, so reading a Deny there would flag a
/// safe account-wide prohibition as an over-broad grant -- the inverse of the
/// fact. The fix filters `kms_statement_resources` to `Allow`; scoping is asked
/// only of the grants. Synthetic: no shipped template carries a Deny KMS
/// statement.
#[test]
fn deny_kms_statement_is_not_scope_checked() {
    let policy = Policy {
        role: "fixture",
        statements: serde_json::json!([{
            "Sid": "DenyBroadKms",
            "Effect": "Deny",
            "Action": "kms:Decrypt",
            "Resource": "arn:aws:kms:us-east-1:111122223333:key/*",
        }]),
    };
    // Observation 1 (load-bearing): the pre-fix effect-blind scan (the live body
    // minus its `Allow` filter) returned the Deny's broad key/* ARN.
    let mut blind: Vec<(String, Vec<String>)> = Vec::new();
    for stmt in policy_statements(&policy) {
        if !statement_actions(stmt)
            .iter()
            .any(|a| action_selects_kms(a))
        {
            continue;
        }
        let resources: Vec<String> = statement_resources(stmt)
            .into_iter()
            .filter(|r| matches!(classify_resource(r), ResourceShape::KmsKey))
            .map(str::to_string)
            .collect();
        blind.push((statement_sid(stmt).to_string(), resources));
    }
    assert!(
        blind
            .iter()
            .any(|(_, rs)| rs.iter().any(|r| r.ends_with("key/*"))),
        "fixture invalid: an effect-blind scan must return the Deny's broad key/* \
         ARN (the hole this fixes); got {blind:?}"
    );
    // Observation 2: the Allow-only helper returns nothing.
    let statements = kms_statement_resources(&policy);
    assert!(
        statements.is_empty(),
        "kms_statement_resources must not hand a Deny KMS statement to the \
         scope-check guards; got {statements:?}"
    );
    // The Deny reaches the helper: it passes the choke point.
    assert!(
        validate_statement("fixture", 0, &policy_statements(&policy)[0]).is_ok(),
        "fixture invalid: the Deny KMS statement must pass the choke point so it \
         reaches kms_statement_resources"
    );
}

/// F3 regression (Sid): a missing or non-string Sid must fail closed. Pre-fix,
/// `obj.get("Sid").and_then(as_str).unwrap_or("<no Sid>")` swallowed a
/// `"Sid": 123` and an absent Sid, and the otherwise-valid statement passed while
/// every rejection could name only `<no Sid>`. Synthetic, not `deploy/iam/*.json`:
/// every shipped statement carries a string Sid.
#[test]
fn sid_must_be_a_present_string() {
    // Non-string Sid.
    let non_string = serde_json::json!({
        "Sid": 123,
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::my-ravel-bucket/t/*",
    });
    // Observation 1 (load-bearing): the pre-fix extraction saw no string Sid, so
    // it fell back to "<no Sid>" and validated the rest as if well-formed.
    assert!(
        non_string["Sid"].as_str().is_none(),
        "fixture invalid: the pre-fix as_str() extraction was expected to see no \
         string Sid for 123"
    );
    let err = validate_statement("fixture", 0, &non_string)
        .expect_err("validate_statement must reject a non-string Sid");
    assert!(
        err.contains("non-string Sid"),
        "rejection must name the non-string Sid; got {err:?}"
    );

    // Missing Sid entirely.
    let missing = serde_json::json!({
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::my-ravel-bucket/t/*",
    });
    let err = validate_statement("fixture", 0, &missing)
        .expect_err("validate_statement must reject a missing Sid");
    assert!(
        err.contains("no Sid"),
        "rejection must name the missing Sid; got {err:?}"
    );
}

/// F3 regression (index): every rejection names the statement index, so an
/// operator can locate the offending statement in a Sid-less or duplicate-Sid
/// array. Pre-fix, messages carried only `role/Sid`.
#[test]
fn rejection_message_names_the_statement_index() {
    let statements = serde_json::json!([
        {"Sid": "Good", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::my-ravel-bucket/t/*"},
        {"Sid": "Bad", "Effect": "Permit", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::my-ravel-bucket/t/*"},
    ]);
    let err = validate_policy_statements("fixture", &statements)
        .expect_err("the invalid second statement must be rejected");
    assert!(
        err.contains("#1"),
        "rejection must name the statement index #1; got {err:?}"
    );
    assert!(
        err.contains("Bad"),
        "rejection must name the Sid; got {err:?}"
    );
}

/// F4 wiring guard: `load_policy_from` -- `load_policy`'s real file-reading body --
/// must reject an invalid policy. Driving a synthetic invalid file through it is
/// what fails if the validation call is deleted from `build_policy`, or if
/// `load_policy_from` stops calling `build_policy`; a test that only calls
/// `validate_policy_statements` directly would keep passing. This closes the F4
/// gap: the shipped entry point, not only the extracted body, is exercised.
#[test]
fn load_policy_from_rejects_a_synthetic_invalid_file() {
    let path = std::env::temp_dir().join(format!(
        "ravel-iam-fixture-{}-invalid.json",
        std::process::id()
    ));
    let json = r#"{"Version":"2012-10-17","Statement":[{"Sid":"NegatedResource","Effect":"Allow","Action":"s3:DeleteObject","NotResource":"arn:aws:s3:::my-ravel-bucket/t/*"}]}"#;
    std::fs::write(&path, json).expect("write synthetic policy fixture");
    let path_str = path.to_str().expect("temp path is valid UTF-8").to_string();
    let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        load_policy_from("fixture", &path_str)
    }));
    std::fs::remove_file(&path).ok();
    assert!(
        built.is_err(),
        "load_policy_from (load_policy's real file-reading body) must reject a \
         statement whose permission lives in NotResource, a field no guard reads"
    );
}

// ---------------------------------------------------------------------------
// Round four (issue #1346, holes H1/H2/H3): the redesign's own fixtures.
//
// Round three's `bucket_relative_s3_pattern` and the list-only exemption its
// callers carried, kept verbatim, so each fixture below pins the hole it closes
// instead of restating today's rule.
// ---------------------------------------------------------------------------

/// Round three's resource classifier, verbatim: it panicked for exactly two
/// shapes (`"*"` and an `arn:aws:s3:::` prefix) and returned `None` for
/// everything else. Its doc claimed `None` meant "names a different service such
/// as a KMS ARN", but nothing checked that, so every S3 grant shape outside those
/// two -- access point, Object Lambda, multi-region access point, `arn:*`, a
/// malformed non-ARN -- came back as "nothing to check" (H1).
fn round_three_bucket_relative_s3_pattern(resource: &str) -> Option<String> {
    if let Some(key_pattern) = resource.strip_prefix("arn:aws:s3:::my-ravel-bucket/") {
        return Some(key_pattern.to_string());
    }
    if resource == "*" || resource.starts_with("arn:aws:s3:::") {
        panic!("round three panicked for {resource:?}");
    }
    None
}

/// Round three's `resource_key_patterns`, verbatim: the list-only exemption that
/// skipped a list statement before its Resource was looked at by anything (H2),
/// plus the silent-drop classifier above.
fn round_three_resource_key_patterns(policy: &Policy) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in policy_statements(policy) {
        let actions = statement_actions(stmt);
        let is_list_only = !actions.is_empty()
            && actions
                .iter()
                .all(|a| a.eq_ignore_ascii_case("s3:ListBucket"));
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
            if let Some(key_pattern) = round_three_bucket_relative_s3_pattern(r) {
                out.push(key_pattern);
            }
        }
    }
    out
}

/// The message a `catch_unwind` payload carries, so a fixture can assert the
/// rejection names the offending statement rather than only that something
/// panicked.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    if let Some(s) = payload.downcast_ref::<&str>() {
        return (*s).to_string();
    }
    "<non-string panic payload>".to_string()
}

/// A single-statement synthetic policy.
fn fixture_policy(stmt: serde_json::Value) -> Policy {
    Policy {
        role: "fixture",
        statements: serde_json::json!([stmt]),
    }
}

/// H1 regression: every S3 object grant whose `Resource` is not bucket-relative
/// must be rejected by name. Round three's classifier panicked only for `"*"` and
/// for an `arn:aws:s3:::` prefix and returned `None` for everything else, so each
/// of these five shapes -- all real S3 object grants reaching outside the
/// configured bucket -- was silently dropped and every resource guard derived an
/// empty set.
///
/// All five shapes are covered, not one: the access-point and multi-region
/// access-point forms carry a region and/or account and so miss the `:::` form,
/// Object Lambda is a different service name entirely, `arn:*` is the
/// everything-grant, and `my-ravel-bucket/*` is not an ARN at all.
///
/// Synthetic statements, not `deploy/iam/*.json`: the shipped templates name only
/// bucket-relative object ARNs, so a fixture over them passes whichever way the
/// classifier behaves. Each case asserts in both directions.
#[test]
fn non_bucket_relative_s3_object_resource_fails_closed() {
    let cases = [
        (
            "AccessPointArn",
            "arn:aws:s3:us-east-1:111122223333:accesspoint/ap/object/*",
        ),
        (
            "ObjectLambdaArn",
            "arn:aws:s3-object-lambda:us-east-1:111122223333:accesspoint/olap/object/*",
        ),
        (
            "MultiRegionAccessPointArn",
            "arn:aws:s3::111122223333:accesspoint/mrap/object/*",
        ),
        ("EverythingArn", "arn:*"),
        ("MalformedNonArn", "my-ravel-bucket/*"),
    ];

    for (sid, resource) in cases {
        let stmt = serde_json::json!({
            "Sid": sid,
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": resource,
        });
        let policy = fixture_policy(stmt.clone());

        // Observation 1 (load-bearing): the resource is not bucket-relative, yet
        // round three's classifier fell through to None instead of panicking, so
        // its callers dropped it. Written as round three's own two literal
        // conditions, so this pins the hole rather than restating the fix.
        assert!(
            resource.strip_prefix(BUCKET_KEY_PREFIX).is_none(),
            "fixture {sid} invalid: {resource:?} must NOT be bucket-relative, or it \
             is not an out-of-bucket grant at all"
        );
        let round_three_panicked = resource == "*" || resource.starts_with("arn:aws:s3:::");
        assert!(
            !round_three_panicked,
            "fixture {sid} invalid: round three's two panic conditions were expected \
             to miss {resource:?} -- if one catches it, this fixture no longer proves \
             the catch-all None hole existed"
        );
        assert_eq!(
            round_three_bucket_relative_s3_pattern(resource),
            None,
            "fixture {sid} invalid: round three's classifier was expected to return \
             None (silently dropping the grant) for {resource:?}"
        );
        assert!(
            round_three_resource_key_patterns(&policy).is_empty(),
            "fixture {sid} invalid: round three's resource helper was expected to \
             derive an empty pattern set for {resource:?}, hiding the grant"
        );

        // Observation 2: the choke point rejects it, naming the role, the
        // statement index, the Sid and the offending value.
        let err = validate_statement("gateway", 3, &stmt)
            .expect_err(&format!("validate_statement must reject fixture {sid}"));
        for expected in [resource, sid, "#3", "gateway"] {
            assert!(
                err.contains(expected),
                "fixture {sid}: rejection must name {expected:?}; got {err:?}"
            );
        }

        // Observation 3 (belt and braces): the helper itself panics if a guard is
        // ever run on a statement that never passed the choke point.
        let guard = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            resource_key_patterns(&policy)
        }));
        let message = panic_message(guard.expect_err(&format!(
            "resource_key_patterns must panic on fixture {sid}, not drop the resource"
        )));
        assert!(
            message.contains(resource) && message.contains(sid),
            "fixture {sid}: the helper's panic must name the Sid and the resource; \
             got {message:?}"
        );
    }
}

/// H2 regression: a list statement's `Resource` must be examined. Round three
/// made a non-bucket-relative Resource fatal and then, to stop that tripping the
/// bare bucket ARN that `s3:ListBucket` legitimately names, exempted whole
/// list-only statements from the resource helpers. The result was a list
/// statement whose Resource was read by NO guard in the file: `s3:ListBucket` on
/// `"*"` -- which enumerates every bucket in the account -- passed validation,
/// was skipped by the resource helpers, and `list_prefix_patterns` read only its
/// Condition. This hole did not exist before round three.
///
/// The fix is the choke point's resource-class rule: a list grant must name the
/// bare bucket ARN and nothing else, so the exemption is no longer needed and the
/// helper never has to guess.
#[test]
fn list_statement_resource_is_examined() {
    let cases = [
        // The account-wide list: no class at all, rejected as unclassified.
        ("ListOnStar", "*", "nor exactly that bucket ARN"),
        // A list grant naming an object ARN: legal JSON, and in AWS a ListBucket
        // on an object ARN matches nothing, so it is a template bug either way.
        // The resource classifies, and the mismatch with the granted class is
        // what rejects it.
        (
            "ListOnObjectArn",
            "arn:aws:s3:::my-ravel-bucket/t/*",
            "grants no S3 object operation",
        ),
    ];

    for (sid, resource, must_explain) in cases {
        let stmt = serde_json::json!({
            "Sid": sid,
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": resource,
            "Condition": {"StringLike": {"s3:prefix": ["t/*"]}},
        });
        let policy = fixture_policy(stmt.clone());

        // Observation 1 (load-bearing): round three's list-only exemption skipped
        // the statement before its Resource was classified at all, so the
        // resource helper derived nothing.
        assert!(
            round_three_resource_key_patterns(&policy).is_empty(),
            "fixture {sid} invalid: round three's list-only exemption was expected \
             to skip the statement entirely; it did not"
        );

        // ...and the only guard that DID look at this statement looked solely at
        // its Condition, which is the shape of the hole: the prefix was read, the
        // Resource was not.
        assert_eq!(
            pre_fix_list_prefix_patterns(&policy),
            vec!["t/*".to_string()],
            "fixture {sid} invalid: list_prefix_patterns was expected to read the \
             Condition (and nothing else) for this statement"
        );

        // Observation 2: the choke point now rejects the Resource by name.
        let err = validate_statement("admin", 0, &stmt)
            .expect_err(&format!("validate_statement must reject fixture {sid}"));
        for expected in [resource, sid, must_explain] {
            assert!(
                err.contains(expected),
                "fixture {sid}: rejection must name {expected:?}; got {err:?}"
            );
        }
    }

    // The same hole with the list grant hidden beside an object grant, which is
    // what makes the class-mismatch arms above insufficient on their own: the
    // object ARN is legitimate for the GetObject half, so only the coverage rule
    // ("grants a bucket operation, names no bucket ARN") catches the ListBucket
    // half reaching every key in the bucket with no Resource a guard reads.
    let hidden = serde_json::json!({
        "Sid": "ListHiddenBesideRead",
        "Effect": "Allow",
        "Action": ["s3:ListBucket", "s3:GetObject"],
        "Resource": "arn:aws:s3:::my-ravel-bucket/t/*",
        "Condition": {"StringLike": {"s3:prefix": ["t/*"]}},
    });
    let hidden_policy = fixture_policy(hidden.clone());

    // Observation 1 (load-bearing): round three read this statement twice and
    // never once asked what its list half was scoped to. The resource helper saw
    // it (is_list_only was false, so the exemption did not apply) and derived only
    // the object pattern; the list guard saw it and read only the Condition.
    assert_eq!(
        round_three_resource_key_patterns(&hidden_policy),
        vec!["t/*".to_string()],
        "fixture invalid: round three's resource helper was expected to derive only \
         the object pattern here, saying nothing about the list grant"
    );
    assert_eq!(
        pre_fix_list_prefix_patterns(&hidden_policy),
        vec!["t/*".to_string()],
        "fixture invalid: round three's list guard was expected to read only the \
         Condition here"
    );

    // Observation 2: the coverage rule rejects it, tagged H2.
    let err = validate_statement("query", 2, &hidden)
        .expect_err("validate_statement must reject a list grant that names no bucket ARN");
    for expected in ["ListHiddenBesideRead", "#2", "issue #1346, H2"] {
        assert!(
            err.contains(expected),
            "the rejection must name {expected:?}; got {err:?}"
        );
    }

    // Trap (must not trip): the shipped list shape -- exactly the bare bucket ARN
    // with an s3:prefix Condition -- still passes, and still contributes its
    // prefixes.
    let shipped = serde_json::json!({
        "Sid": "ShippedListShape",
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": BUCKET_ARN,
        "Condition": {"StringLike": {"s3:prefix": ["t/", "sys/*"]}},
    });
    assert!(
        validate_statement("fixture", 0, &shipped).is_ok(),
        "the shipped list statement shape (bare bucket ARN + s3:prefix Condition) \
         must still pass"
    );
    assert_eq!(
        list_prefix_patterns(&fixture_policy(shipped), None),
        vec!["t/".to_string(), "sys/*".to_string()],
        "the shipped list shape must still contribute its s3:prefix patterns"
    );
}

/// H3 regression, the predicate: `"s3:*"` and `"*"` grant PutObject, ListBucket
/// and the delete actions, and `"*"` grants the KMS operations too. Round three
/// resolved IAM wildcards on the delete axis only; put, list and KMS selection
/// all compared exact names, so an `s3:*` statement was selected by none of them
/// and the Condition-presence rule (which keys on the list grant) never fired.
///
/// This test is over the predicate rather than a policy because the predicate is
/// now the only place any axis decides: if it answers all of these, no axis can
/// be wildcard-blind.
#[test]
fn wildcard_action_is_selected_by_every_axis() {
    // (action, does it grant an S3 object op, a bucket op, a KMS op)
    let cases = [
        ("s3:*", true, true, false),
        ("S3:*", true, true, false),
        ("*", true, true, true),
        ("s3:?utObject", true, false, false),
        ("kms:*", false, false, true),
        // Control: a literal action grants only itself.
        ("s3:GetObject", true, false, false),
        // Control: an unrelated literal grants nothing in the vocabulary.
        ("s3:DeleteObjectTagging", false, false, false),
    ];

    for (action, grants_object, grants_list, grants_kms) in cases {
        assert_eq!(
            action_grants_any(action, &S3_OBJECT_OPERATIONS),
            grants_object,
            "action_grants: {action:?} vs {S3_OBJECT_OPERATIONS:?}"
        );
        assert_eq!(
            action_grants_any(action, &S3_BUCKET_OPERATIONS),
            grants_list,
            "action_grants: {action:?} vs {S3_BUCKET_OPERATIONS:?}"
        );
        assert_eq!(
            action_selects_kms(action),
            grants_kms,
            "action_selects_kms: {action:?}"
        );
    }

    // Observation 1 (load-bearing): round three's per-axis matchers -- exact
    // case-folded equality for put and list, a case-folded `kms:` prefix for KMS
    // -- each missed the wildcard action that grants their operation. Written as
    // those literal expressions, so this pins the hole rather than restating the
    // fix.
    assert!(
        !"s3:*".eq_ignore_ascii_case("s3:PutObject"),
        "fixture invalid: round three's put selection was expected to miss \"s3:*\""
    );
    assert!(
        !"s3:*".eq_ignore_ascii_case("s3:ListBucket"),
        "fixture invalid: round three's list selection was expected to miss \"s3:*\""
    );
    assert!(
        !"*".starts_with("kms:"),
        "fixture invalid: round three's KMS selection was expected to miss \"*\""
    );

    // ...while the delete axis, the one round three made wildcard-aware, saw it.
    // That asymmetry is what the single predicate removes.
    assert!(
        action_grants_any("s3:*", &S3_DELETE_OPERATIONS),
        "the delete axis was already wildcard-aware and must stay so"
    );
}

/// H3 regression, the shipped-template mutation the reviewer used: changing
/// `GatewayWrite`'s Action from `s3:PutObject` to `s3:*` passed all 26 tests
/// under round three. `s3:*` grants PutObject (so the routed-write KMS check
/// should select the statement) and ListBucket (so the Condition-presence rule
/// should demand an `s3:prefix` Condition and the resource rule should demand the
/// bucket ARN), and round three's exact-name matchers saw neither.
///
/// The mutation is applied to an in-memory copy of `deploy/iam/gateway.json`; the
/// file on disk is not touched, and the unmutated policy is asserted to still
/// load.
#[test]
fn shipped_gateway_write_mutated_to_wildcard_action_fails_closed() {
    let path = policy_json_path("gateway");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut json: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"));

    // The unmutated template still loads: this test's failure is about the
    // mutation, not about the shipped file.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| build_policy(
            "gateway", &path, &json
        )))
        .is_ok(),
        "the shipped gateway template must load unchanged"
    );

    let statements = json["Statement"]
        .as_array_mut()
        .expect("gateway.json Statement is an array");
    let target = statements
        .iter_mut()
        .find(|stmt| stmt["Sid"] == serde_json::json!("GatewayWrite"))
        .expect("gateway.json carries a GatewayWrite statement");
    let original = target["Action"].clone();
    assert_eq!(
        original,
        serde_json::json!("s3:PutObject"),
        "fixture invalid: GatewayWrite's Action is no longer the s3:PutObject this \
         mutation replaces"
    );
    target["Action"] = serde_json::json!("s3:*");

    let mutated = Policy {
        role: "gateway",
        statements: json["Statement"].clone(),
    };

    // Observation 1 (load-bearing): under round three's exact-name matchers the
    // mutated policy hid the grant in two independent places.
    assert!(
        pre_fix_put_resource_key_patterns(&mutated).is_empty(),
        "fixture invalid: round three's PutObject selection was expected to derive \
         an EMPTY routed-write set from the mutated policy (so \
         roles_writing_routed_objects_have_kms_grant skipped the role)"
    );
    let round_three_list_prefixes = pre_fix_list_prefix_patterns(&mutated);
    assert!(
        !round_three_list_prefixes.contains(&"*".to_string()),
        "fixture invalid: round three's list detection was expected to read nothing \
         from the s3:* statement; got {round_three_list_prefixes:?}"
    );
    assert!(
        round_three_bucket_relative_s3_pattern("arn:aws:s3:::my-ravel-bucket/t/*/*/l0/*").is_some(),
        "fixture invalid: the mutated statement's resources still strip cleanly, so \
         nothing but the Action matcher could have caught this"
    );

    // Observation 2: the choke point rejects the mutated template, naming the
    // statement, and `load_policy`'s real body is what does it.
    let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        build_policy("gateway", &path, &json)
    }));
    let message = panic_message(built.expect_err(
        "build_policy must reject GatewayWrite mutated to \"s3:*\": the action grants \
         ListBucket and PutObject, and its resources are object ARNs with no Condition",
    ));
    assert!(
        message.contains("GatewayWrite") && message.contains("s3:*"),
        "the rejection must name the mutated statement and its Action; got {message:?}"
    );

    // Observation 3: the post-fix put axis DOES select the mutated statement, so
    // the routed-write KMS check would run on it rather than skip the role.
    let routed: Vec<String> = put_resource_key_patterns(&mutated)
        .into_iter()
        .filter(|p| ravel_object_store::routes_through_tenant_key(p))
        .collect();
    assert!(
        !routed.is_empty(),
        "the post-fix put axis must select the s3:* statement's routed PUT patterns"
    );
}

/// H3 companion: the normal IAM idiom round three's list-only exemption would
/// have mishandled -- one statement granting both `s3:ListBucket` and
/// `s3:GetObject`, naming both the bare bucket ARN and an object prefix -- must be
/// ACCEPTED, and both axes must read it. Rejecting it (or rejecting it with a
/// misleading "not bucket-relative" reason) would be a new failure of the same
/// class, in the opposite direction.
#[test]
fn mixed_list_and_object_statement_is_accepted() {
    let stmt = serde_json::json!({
        "Sid": "MixedListAndRead",
        "Effect": "Allow",
        "Action": ["s3:ListBucket", "s3:GetObject"],
        "Resource": [BUCKET_ARN, "arn:aws:s3:::my-ravel-bucket/t/*"],
        "Condition": {"StringLike": {"s3:prefix": ["t/*"]}},
    });
    assert!(
        validate_statement("fixture", 0, &stmt).is_ok(),
        "a statement granting both ListBucket and GetObject over the bucket ARN plus \
         an object prefix is a normal IAM idiom and must be accepted: {:?}",
        validate_statement("fixture", 0, &stmt)
    );

    let policy = fixture_policy(stmt);

    // The object axis reads the object prefix and skips the bucket ARN rather
    // than panicking on it (round three's classifier panicked for the bare bucket
    // ARN, which is why the exemption was added at the caller).
    assert!(
        std::panic::catch_unwind(|| round_three_bucket_relative_s3_pattern(BUCKET_ARN)).is_err(),
        "fixture invalid: round three's classifier was expected to panic on the bare \
         bucket ARN (the reason the list-only exemption existed)"
    );
    assert_eq!(
        resource_key_patterns(&policy),
        vec!["t/*".to_string()],
        "the object axis must read the object prefix and skip the bucket ARN"
    );

    // ...and the list axis reads the Condition, so neither half sits unexamined.
    assert_eq!(
        list_prefix_patterns(&policy, None),
        vec!["t/*".to_string()],
        "the list axis must read the mixed statement's s3:prefix Condition"
    );
}

/// The vocabulary's own invariants, so a later edit cannot silently narrow it:
/// every operation is a literal name (`action_grants` asserts this, and an
/// operation carrying a wildcard would make it answer a different question), the
/// delete axis selects a subset of the object axis (a delete that is not an
/// object operation would be selected by the delete guard while the choke point
/// demanded a resource shape for a class it does not grant), and the three
/// classes are disjoint (a shape rule keyed on class would otherwise be
/// ambiguous).
#[test]
fn operation_vocabulary_is_consistent() {
    let all: Vec<&str> = S3_OBJECT_OPERATIONS
        .iter()
        .chain(S3_BUCKET_OPERATIONS.iter())
        .chain(KMS_OTHER_OPERATIONS.iter())
        .chain(KMS_DATA_KEY_OPERATIONS.iter())
        .copied()
        .collect();
    for op in &all {
        assert!(
            !op.contains(IAM_WILDCARDS),
            "operation {op:?} carries an IAM wildcard; operations must be literal names"
        );
    }
    for op in S3_DELETE_OPERATIONS {
        assert!(
            S3_OBJECT_OPERATIONS.contains(&op),
            "delete operation {op:?} is not in S3_OBJECT_OPERATIONS, so a statement \
             granting it would be selected by the delete axis while the choke point \
             required no object resource for it"
        );
    }
    for op in &all {
        let object = S3_OBJECT_OPERATIONS.contains(op);
        let bucket = S3_BUCKET_OPERATIONS.contains(op);
        let kms = action_selects_kms(op);
        assert_eq!(
            u8::from(object) + u8::from(bucket) + u8::from(kms),
            1,
            "operation {op:?} belongs to more than one resource class (object \
             {object}, bucket {bucket}, kms {kms}), so the choke point's shape rules \
             would be ambiguous"
        );
    }
}

/// Every shipped template under `deploy/iam` passes the choke point, and the set
/// this file guards is exactly the set on disk. `ALL_ROLES` is hand-written, so
/// without this a new template would ship unguarded.
#[test]
fn every_shipped_template_passes_the_choke_point() {
    let dir = format!("{}/../../deploy/iam", env!("CARGO_MANIFEST_DIR"));
    let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read dir {dir}: {e}"))
        .map(|entry| entry.expect("dir entry").file_name())
        .filter_map(|name| {
            name.to_str()
                .and_then(|n| n.strip_suffix(".json"))
                .map(str::to_string)
        })
        .collect();
    on_disk.sort();
    let mut guarded: Vec<String> = ALL_ROLES.iter().map(|r| (*r).to_string()).collect();
    guarded.sort();
    assert_eq!(
        on_disk, guarded,
        "the templates under deploy/iam and the roles ALL_ROLES guards must be the \
         same set"
    );
    assert_eq!(
        on_disk.len(),
        4,
        "expected 4 shipped templates: {on_disk:?}"
    );

    for role in ALL_ROLES {
        // load_policy panics on any rejection, naming role, index, Sid and field.
        let policy = load_policy(role);
        assert!(
            !policy_statements(&policy).is_empty(),
            "{role}: policy has no statements"
        );
    }
}

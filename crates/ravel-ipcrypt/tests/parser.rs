//! Suite D (PARSER): one dedicated test per `PolicyError` refusal variant,
//! plus the sanitize-collision case using a dots-to-underscores sanitizer
//! (the same shape a metrics label sanitizer takes).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_ipcrypt::policy::{PolicyError, Variant, parse_policy_file};

fn dots_to_underscores(key: &str) -> String {
    key.replace('.', "_")
}

#[test]
fn malformed_toml_is_a_parse_error() {
    let err = parse_policy_file("this is not [ valid toml", dots_to_underscores).unwrap_err();
    assert!(matches!(err, PolicyError::Parse(_)));
}

#[test]
fn valid_rule_parses() {
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic" },
        ]
    "#;
    let policy = parse_policy_file(text, dots_to_underscores).expect("valid policy parses");
    assert_eq!(policy.defaults.rules.len(), 1);
    assert_eq!(policy.defaults.rules[0].key, "client.ip");
    assert_eq!(policy.defaults.rules[0].variant, Variant::Deterministic);
    // epoch defaults to 1, retired_epochs defaults to empty (module doc).
    assert_eq!(policy.defaults.rules[0].epoch, 1);
    assert!(policy.defaults.rules[0].retired_epochs.is_empty());
}

#[test]
fn tenant_table_overlay_is_a_separate_table() {
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic" },
        ]

        [tenants.acme]
        rules = [
            { key = "client.ip", variant = "prefix", epoch = 2 },
        ]
    "#;
    let policy = parse_policy_file(text, dots_to_underscores).expect("valid policy parses");
    assert_eq!(policy.defaults.rules[0].variant, Variant::Deterministic);
    let tenant = policy.tenants.get("acme").expect("tenant table present");
    assert_eq!(tenant.rules[0].variant, Variant::Prefix);
    assert_eq!(tenant.rules[0].epoch, 2);
}

#[test]
fn duplicate_key_in_one_table_is_refused() {
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic" },
            { key = "client.ip", variant = "prefix" },
        ]
    "#;
    let err = parse_policy_file(text, dots_to_underscores).unwrap_err();
    assert_eq!(
        err,
        PolicyError::DuplicateKey {
            table: "[defaults]".to_string(),
            key: "client.ip".to_string(),
        }
    );
}

#[test]
fn duplicate_key_across_different_tables_is_not_refused() {
    // Deliverable 2 / module doc: each table (`[defaults]` on its own, each
    // `[tenants.<id>]` on its own) is checked independently; two different
    // tenant tables reusing the same key name never conflict with each
    // other.
    let text = r#"
        [tenants.acme]
        rules = [
            { key = "client.ip", variant = "deterministic" },
        ]

        [tenants.globex]
        rules = [
            { key = "client.ip", variant = "prefix" },
        ]
    "#;
    let policy = parse_policy_file(text, dots_to_underscores).expect("no cross-table conflict");
    assert_eq!(policy.tenants.len(), 2);
}

#[test]
fn empty_key_is_refused() {
    let text = r#"
        [defaults]
        rules = [
            { key = "", variant = "deterministic" },
        ]
    "#;
    let err = parse_policy_file(text, dots_to_underscores).unwrap_err();
    assert_eq!(
        err,
        PolicyError::EmptyKey {
            table: "[defaults]".to_string(),
        }
    );
}

#[test]
fn zero_epoch_is_refused() {
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic", epoch = 0 },
        ]
    "#;
    let err = parse_policy_file(text, dots_to_underscores).unwrap_err();
    assert_eq!(
        err,
        PolicyError::ZeroEpoch {
            table: "[defaults]".to_string(),
            key: "client.ip".to_string(),
        }
    );
}

#[test]
fn zero_retired_epoch_is_refused() {
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic", epoch = 2, retired_epochs = [0] },
        ]
    "#;
    let err = parse_policy_file(text, dots_to_underscores).unwrap_err();
    assert_eq!(
        err,
        PolicyError::ZeroRetiredEpoch {
            table: "[defaults]".to_string(),
            key: "client.ip".to_string(),
        }
    );
}

#[test]
fn retired_epoch_equal_to_current_epoch_is_refused() {
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic", epoch = 2, retired_epochs = [2] },
        ]
    "#;
    let err = parse_policy_file(text, dots_to_underscores).unwrap_err();
    assert_eq!(
        err,
        PolicyError::RetiredEqualsCurrent {
            table: "[defaults]".to_string(),
            key: "client.ip".to_string(),
            epoch: 2,
        }
    );
}

#[test]
fn positive_retired_epochs_distinct_from_current_are_accepted() {
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic", epoch = 3, retired_epochs = [1, 2] },
        ]
    "#;
    let policy = parse_policy_file(text, dots_to_underscores).expect("valid retired epochs");
    assert_eq!(policy.defaults.rules[0].retired_epochs, vec![1, 2]);
}

#[test]
fn sanitize_collision_is_refused() {
    // Two distinct raw keys that the sanitizer (dots -> underscores) maps to
    // the same metric label name.
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic" },
            { key = "client_ip", variant = "deterministic" },
        ]
    "#;
    let err = parse_policy_file(text, dots_to_underscores).unwrap_err();
    assert_eq!(
        err,
        PolicyError::SanitizeCollision {
            table: "[defaults]".to_string(),
            a: "client.ip".to_string(),
            b: "client_ip".to_string(),
            label: "client_ip".to_string(),
        }
    );
}

#[test]
fn sanitize_collision_is_scoped_per_table() {
    // The same two colliding raw keys split across [defaults] and a tenant
    // table never collide with each other: the collision check runs once
    // per table only (module doc).
    let text = r#"
        [defaults]
        rules = [
            { key = "client.ip", variant = "deterministic" },
        ]

        [tenants.acme]
        rules = [
            { key = "client_ip", variant = "deterministic" },
        ]
    "#;
    let policy = parse_policy_file(text, dots_to_underscores).expect("no cross-table collision");
    assert_eq!(policy.defaults.rules[0].key, "client.ip");
    assert_eq!(policy.tenants["acme"].rules[0].key, "client_ip");
}

//! Enumeration guard for ADR-0066 decision 4 (the format-migration classes).
//!
//! Every message in `proto/ravel/sys.proto` that carries a `format_version`
//! field must be classified in the table below as either rewritten-under-CAS
//! (a lagging writer could strip an additive field, so every additive change
//! MUST bump `format_version`, sequenced readers-before-writers) or
//! never-rewritten (written once and immutable, or overwritten wholesale from a
//! sole writer's live state, so no peer ever reads-and-rewrites its bytes and a
//! strip cannot happen). A new versioned message that lands without a table
//! entry fails this test, so a message cannot be added to the frozen sys schema
//! without a deliberate classification.
//!
//! Why this lives in `crates/ravel-catalog/tests/` rather than a unit module:
//! the invariant spans every message in the shared sys proto, whose readers and
//! writers are owned by four crates (ravel-catalog, ravel-ingest, ravel-maintain,
//! ravel-server), so it belongs to no single module. An integration test parses
//! the checked-in proto TEXT (via `CARGO_MANIFEST_DIR`), which is exactly the
//! artifact a new message is added to, rather than the generated Rust types --
//! so a message that never got a Rust reader is still caught. It is scoped to
//! ravel-catalog because that crate owns three of the versioned records and its
//! test gate (`cargo test -p ravel-catalog`) always runs it.

use std::collections::{BTreeMap, BTreeSet};

/// How a versioned sys/* record is mutated, and the read-version set its readers
/// accept today. The vocabulary of ADR-0066 decision 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// Read-modify-write under CAS (or a sole-record CAS-replace that rebuilds
    /// the body from a decoded view): a lagging writer that re-encodes through an
    /// older field set silently strips any additive field a newer writer added.
    /// Every additive change to such a record MUST bump `format_version`,
    /// sequenced readers-before-writers, and its rewrite paths refuse a record
    /// newer than the writer can reproduce (ADR-0066 decision 5). The slice is
    /// the supported read-version set its readers accept today.
    CasMutable(&'static [u32]),
    /// Never rewritten by a peer: either written once with `CreateIfAbsent` and
    /// immutable, or overwritten wholesale by its single owning writer from that
    /// writer's own live state (`PutMode::Overwrite`, no prior bytes read). A
    /// strip cannot happen because nothing reads-and-rewrites the object. The
    /// slice is the supported read-version set its readers accept today.
    Immutable(&'static [u32]),
}

/// The checked-in classification of every `format_version`-carrying message in
/// `proto/ravel/sys.proto`. Adding a versioned message without adding it here
/// fails `classification_is_complete`.
fn classification_table() -> BTreeMap<&'static str, Class> {
    use Class::{CasMutable, Immutable};
    BTreeMap::from([
        // Written once with CreateIfAbsent, then immutable.
        ("TenancyMarker", Immutable(&[1])),
        ("TenantRecoveryManifest", Immutable(&[1])),
        // Sole-writer PutMode::Overwrite snapshots: each write is a fresh dump of
        // the owning process's live state, never a read-modify-write of prior
        // bytes, so a lagging binary cannot strip a sibling's additive field.
        ("AdmissionUsageSnapshot", Immutable(&[1])),
        ("WorkerHeartbeat", Immutable(&[1])),
        // Read-modify-write under CAS. ProvisioningRecord / TenantConfigRecord /
        // MetricMetadataRecord accept {1, 2} after ADR-0066 R1; AuthTokenMap
        // accepts {1, 2} (managed_by, ADR-0072 #897) and KeyEpochRecord {1}, both
        // as a floor-and-ceiling set since ADR-0066 R2. GcConfig and
        // CompactionClaim still carry ceiling-only gates in their own crates
        // (ravel-maintain, ravel-fleet), outside this change's scope: reported,
        // not fixed. Every slice belonging to a ravel-catalog reader is checked
        // against that reader's own constants by
        // `catalog_read_sets_match_their_readers_constants`.
        ("ProvisioningRecord", CasMutable(&[1, 2])),
        ("TenantConfigRecord", CasMutable(&[1, 2])),
        ("MetricMetadataRecord", CasMutable(&[1, 2])),
        ("AuthTokenMap", CasMutable(&[1, 2])),
        ("GcConfig", CasMutable(&[1])),
        ("CompactionClaim", CasMutable(&[1])),
        ("KeyEpochRecord", CasMutable(&[1])),
    ])
}

/// The checked-in proto text this guard classifies.
fn sys_proto_text() -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto/ravel/sys.proto");
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read sys.proto at {path}: {e}"))
}

/// The set of top-level `message` names in `proto` that declare a `format_version`
/// field. Parses the proto text (not generated types) so a message that was never
/// given a Rust reader is still enumerated. Messages here are flat (no nested
/// messages), so a brace-depth scan that resets the current message at depth 0 is
/// sufficient; a `format_version` field is recognized by the field declaration
/// `uint32 format_version = <n>;`, distinct from the many doc comments that
/// mention the word.
fn messages_with_format_version(proto: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut current: Option<String> = None;
    let mut depth: i32 = 0;

    for raw in proto.lines() {
        // Drop any trailing `// ...` comment so a commented mention of
        // `format_version` or a stray brace inside a comment is ignored.
        let line = match raw.find("//") {
            Some(idx) => &raw[..idx],
            None => raw,
        };
        let trimmed = line.trim();

        if depth == 0
            && let Some(rest) = trimmed.strip_prefix("message ")
        {
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_end_matches('{')
                .to_string();
            if !name.is_empty() {
                current = Some(name);
            }
        }

        if let Some(name) = &current
            && trimmed.starts_with("uint32 format_version")
        {
            out.insert(name.clone());
        }

        depth += trimmed.matches('{').count() as i32;
        depth -= trimmed.matches('}').count() as i32;
        if depth <= 0 {
            depth = 0;
            current = None;
        }
    }

    out
}

/// Every `format_version`-carrying message in the proto is classified, and the
/// table lists no message the proto does not have. This is the enumeration guard:
/// a new versioned message added to the frozen sys schema without a table entry
/// fails here.
#[test]
fn classification_is_complete() {
    let proto = sys_proto_text();
    let in_proto = messages_with_format_version(&proto);
    let classified: BTreeSet<String> = classification_table()
        .keys()
        .map(|s| s.to_string())
        .collect();

    let unclassified: Vec<&String> = in_proto.difference(&classified).collect();
    assert!(
        unclassified.is_empty(),
        "sys.proto messages carrying format_version with no ADR-0066 classification: {unclassified:?}. \
         Classify each in classification_table() as CasMutable (a lagging writer could strip an \
         additive field, so bump format_version on every additive change) or Immutable (never \
         rewritten by a peer)."
    );

    let stale: Vec<&String> = classified.difference(&in_proto).collect();
    assert!(
        stale.is_empty(),
        "classification_table() names messages not in sys.proto (renamed or removed?): {stale:?}"
    );

    // Sanity: the proto and the table enumerate the same number of messages, so a
    // parser regression that silently found none (or found a subset) is itself
    // caught. The count comes from the table rather than a literal so that adding
    // a message and classifying it in the same commit does not have to edit a
    // number in a third place -- but the floor below still catches a parser that
    // returns nothing, which the equality alone would not if the table were also
    // empty.
    assert!(
        !classified.is_empty(),
        "the classification table is empty: it must enumerate every versioned sys.proto message"
    );
    assert_eq!(
        in_proto.len(),
        classification_table().len(),
        "the proto and the classification table must enumerate the same messages; found {}: {in_proto:?}",
        in_proto.len()
    );
}

/// Every read-version slice in the table that belongs to a reader THIS crate owns
/// is the closed set that reader's own `MIN_READ_VERSION..=MAX_READ_VERSION`
/// constants define. Widening a gate without updating the table therefore fails
/// here, which is the point: a table that merely restated the constants would
/// track any change silently and guard nothing.
///
/// The six messages absent from this list are read outside ravel-catalog
/// (ravel-fleet, ravel-maintain, ravel-server, ravel-ingest). Asserting their
/// slices against their constants would need a dev-dependency on each of those
/// crates, so their entries stay literal and are maintained by hand.
#[test]
fn catalog_read_sets_match_their_readers_constants() {
    use ravel_catalog as cat;

    let expected: BTreeMap<&'static str, Vec<u32>> = BTreeMap::from([
        (
            "ProvisioningRecord",
            (cat::PROVISIONING_MIN_READ_VERSION..=cat::PROVISIONING_MAX_READ_VERSION).collect(),
        ),
        (
            "TenantConfigRecord",
            (cat::TENANT_CONFIG_MIN_READ_VERSION..=cat::TENANT_CONFIG_MAX_READ_VERSION).collect(),
        ),
        (
            "MetricMetadataRecord",
            (cat::METRICS_META_MIN_READ_VERSION..=cat::METRICS_META_MAX_READ_VERSION).collect(),
        ),
        (
            "AuthTokenMap",
            (cat::AUTH_TOKEN_MAP_MIN_READ_VERSION..=cat::AUTH_TOKEN_MAP_MAX_READ_VERSION).collect(),
        ),
        (
            "KeyEpochRecord",
            (cat::KEY_EPOCH_MIN_READ_VERSION..=cat::KEY_EPOCH_MAX_READ_VERSION).collect(),
        ),
    ]);

    let table = classification_table();
    for (name, versions) in expected {
        let class = table
            .get(name)
            .unwrap_or_else(|| panic!("{name} must be classified"));
        let listed = match class {
            Class::CasMutable(v) | Class::Immutable(v) => *v,
        };
        assert_eq!(
            listed,
            versions.as_slice(),
            "{name}: the table's read set disagrees with its reader's MIN/MAX constants. \
             A widened gate must be recorded here too (ADR-0066 decision 4)."
        );
    }
}

/// Every CasMutable supported set is non-empty and includes version 1 (the
/// original floor); no Immutable record advertises a wider read set than {1}
/// today. Pins the table's own shape so a typo (an empty set, a missing 1)
/// cannot pass.
#[test]
fn classification_table_is_well_formed() {
    for (name, class) in classification_table() {
        match class {
            Class::CasMutable(versions) => {
                assert!(!versions.is_empty(), "{name}: CasMutable set is empty");
                assert!(
                    versions.contains(&1),
                    "{name}: supported set must include 1"
                );
            }
            Class::Immutable(versions) => {
                assert_eq!(
                    versions,
                    &[1],
                    "{name}: an immutable record reads only {{1}}"
                );
            }
        }
    }
}

/// The guard bites: a fake versioned message appended to a COPY of the proto is
/// enumerated and, because it is absent from the table, would fail
/// `classification_is_complete`. Proves the enumeration actually detects a new
/// message rather than trivially passing.
#[test]
fn a_new_versioned_message_is_detected_as_unclassified() {
    let mut proto = sys_proto_text();
    proto.push_str(
        "\nmessage FutureThing {\n  uint32 format_version = 1;\n  bytes payload = 2;\n}\n",
    );

    let in_proto = messages_with_format_version(&proto);
    assert!(
        in_proto.contains("FutureThing"),
        "the parser must enumerate a newly added versioned message"
    );

    let classified: BTreeSet<String> = classification_table()
        .keys()
        .map(|s| s.to_string())
        .collect();
    assert!(
        !classified.contains("FutureThing"),
        "the fake message must be absent from the table, so the completeness check would fail"
    );
    // And the completeness relation the real test asserts would now be violated.
    assert!(
        in_proto.difference(&classified).next().is_some(),
        "an unclassified message must make the completeness difference non-empty"
    );
}

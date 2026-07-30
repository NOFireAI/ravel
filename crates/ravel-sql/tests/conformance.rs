//! SQL conformance suite and table generator (issue #256, ADR-0035).
//!
//! This suite discharges three obligations of ADR-0035's SQL half:
//!
//! 1. It verifies every construct the registry
//!    (`ravel_sql::conformance::registry`) enumerates against its declared
//!    classification: a supported construct must execute through the real
//!    pipeline, and a rejected construct must return its declared typed error
//!    -- never a panic, never a wrong-but-successful result.
//! 2. It fails if any construct's live behavior contradicts its declaration
//!    (an effectively `Unclassified`/broken row), so a regression surfaces
//!    here rather than in a client's query.
//! 3. It generates `docs/sql-conformance.md` from the verified surface and
//!    asserts the committed file is in sync, so the published table and score
//!    cannot silently drift from what the suite proves (ADR-0035: "the score
//!    is computed, not hand-maintained"). Regenerate with:
//!
//!        REGEN_SQL_CONFORMANCE=1 cargo test -p ravel-sql --test conformance

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::path::PathBuf;

use ravel_sql::conformance::{
    Classification, Construct, Verdict, Verified, registry, render_document, score,
};
use ravel_sql::{SqlError, ValidationError, validate};
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// A small metrics dataset the supported constructs execute against: two
/// series, overlapping timestamps, a negative value so `WHERE value > 0`
/// actually filters.
fn dataset() -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        1,
        vec![
            SeriesSpec::new("a", vec![(1, 1.0), (2, 2.0)]),
            SeriesSpec::new("b", vec![(1, -1.0), (3, 3.0)]),
        ],
    )]
}

/// The stable name of a [`ValidationError`] variant, for matching a live
/// rejection against the registry's declared typed error.
fn validation_variant(err: &ValidationError) -> &'static str {
    match err {
        ValidationError::Empty => "ValidationError::Empty",
        ValidationError::MultipleStatements { .. } => "ValidationError::MultipleStatements",
        ValidationError::Parse(_) => "ValidationError::Parse",
        ValidationError::NotReadOnly { .. } => "ValidationError::NotReadOnly",
        ValidationError::WriteInQuery { .. } => "ValidationError::WriteInQuery",
        ValidationError::ExcludedAggregate { .. } => "ValidationError::ExcludedAggregate",
    }
}

/// Check one construct's live behavior against its declared classification.
async fn verify(construct: &Construct, fixture: &Fixture) -> Verdict {
    let tenant = tenant_id("conformance");
    match construct.classification {
        Classification::SupportedAndCovered { .. } => {
            match fixture
                .executor
                .execute(tenant.hash(), &request(&construct.example))
                .await
            {
                Ok(_) => Verdict::Confirmed,
                Err(e) => Verdict::Broken {
                    observed: format!("query failed: {e}"),
                },
            }
        }
        Classification::IntentionallyRejected { typed_error } => {
            if typed_error == "SqlError::CrossSignalQuery" {
                match fixture
                    .executor
                    .execute(tenant.hash(), &request(&construct.example))
                    .await
                {
                    Err(SqlError::CrossSignalQuery) => Verdict::Confirmed,
                    Err(e) => Verdict::Broken {
                        observed: format!("wrong error: {e}"),
                    },
                    Ok(_) => Verdict::Broken {
                        observed: "cross-signal query was accepted".to_string(),
                    },
                }
            } else {
                // Every other rejected construct is refused by the read-only
                // single-statement gate before any planning, so the typed
                // error is a `ValidationError`.
                match validate(&construct.example) {
                    Ok(()) => Verdict::Broken {
                        observed: "statement was accepted by the gate".to_string(),
                    },
                    Err(e) => {
                        let observed = validation_variant(&e);
                        if observed == typed_error {
                            Verdict::Confirmed
                        } else {
                            Verdict::Broken {
                                observed: format!("rejected as {observed}, expected {typed_error}"),
                            }
                        }
                    }
                }
            }
        }
        Classification::Unclassified => Verdict::Broken {
            observed: "declared Unclassified".to_string(),
        },
    }
}

/// Verify every enumerated construct and return the annotated surface.
async fn verify_all() -> Vec<Verified> {
    let tenant = tenant_id("conformance");
    let specs = dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let mut verified = Vec::new();
    for construct in registry() {
        let verdict = verify(&construct, &fixture).await;
        verified.push(Verified { construct, verdict });
    }
    verified
}

/// The absolute path of the published conformance document.
fn doc_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/sql-conformance.md")
}

// ---------------------------------------------------------------------------
// Acceptance tests
// ---------------------------------------------------------------------------

/// Every construct declared supported executes through the real pipeline. This
/// is the test the registry names as evidence for its state-1 rows.
#[tokio::test]
async fn supported_constructs_execute() {
    let tenant = tenant_id("conformance");
    let specs = dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    for construct in registry() {
        if let Classification::SupportedAndCovered { .. } = construct.classification {
            let outcome = fixture
                .executor
                .execute(tenant.hash(), &request(&construct.example))
                .await;
            assert!(
                outcome.is_ok(),
                "supported construct `{}` must execute: {:?}",
                construct.name,
                outcome.err()
            );
        }
    }
}

/// Every construct declared intentionally rejected returns its declared typed
/// error: each excluded aggregate, each write statement, and the cross-signal
/// query. This is the acceptance test proving the state-2 rows fail cleanly
/// (ADR-0035 Consequences: "the real value of this work is proving state-2 rows
/// fail cleanly").
#[tokio::test]
async fn every_intentionally_rejected_construct_returns_a_typed_error() {
    let tenant = tenant_id("conformance");
    let fixture = Fixture::memory(&[(&tenant, &[])]).await;

    let mut rejected = 0usize;
    for construct in registry() {
        if let Classification::IntentionallyRejected { typed_error } = construct.classification {
            let verdict = verify(&construct, &fixture).await;
            assert_eq!(
                verdict,
                Verdict::Confirmed,
                "construct `{}` (expected {typed_error}) was not cleanly rejected: {verdict:?}",
                construct.name
            );
            rejected += 1;
        }
    }
    assert!(
        rejected > 0,
        "the registry must enumerate rejected constructs"
    );
}

/// The registry declares exactly one state per distinct construct, and after
/// live verification no construct is effectively `Unclassified` -- every
/// enumerated construct resolves to a deliberate, verified state 1 or state 2.
#[tokio::test]
async fn registry_has_exactly_one_state_per_construct() {
    let registry = registry();

    // Exactly one classification per distinct (category, name) key.
    let mut keys = std::collections::BTreeSet::new();
    for c in &registry {
        assert!(
            keys.insert((c.category.label(), c.name.clone())),
            "duplicate construct `{}` in {}",
            c.name,
            c.category.label()
        );
    }

    // After verification, nothing is Unclassified/broken.
    let verified = verify_all().await;
    let broken: Vec<&Verified> = verified
        .iter()
        .filter(|v| matches!(v.effective(), Classification::Unclassified))
        .collect();
    assert!(
        broken.is_empty(),
        "constructs contradicted their declaration: {:?}",
        broken
            .iter()
            .map(|v| (&v.construct.name, &v.verdict))
            .collect::<Vec<_>>()
    );

    let s = score(&verified);
    assert_eq!(
        s.total(),
        verified.len(),
        "the score must account for every enumerated construct"
    );
    assert_eq!(
        s.unclassified, 0,
        "no construct may be left unclassified once verified"
    );
}

/// Generate `docs/sql-conformance.md` from the verified surface and assert the
/// committed file is in sync. Set `REGEN_SQL_CONFORMANCE=1` to rewrite it.
#[tokio::test]
async fn generate_and_check_conformance_doc() {
    let verified = verify_all().await;
    let rendered = render_document(&verified);
    let path = doc_path();

    if std::env::var_os("REGEN_SQL_CONFORMANCE").is_some() {
        std::fs::write(&path, &rendered).expect("write conformance doc");
        return;
    }

    let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}. Regenerate with \
             REGEN_SQL_CONFORMANCE=1 cargo test -p ravel-sql --test conformance",
            path.display()
        )
    });
    assert_eq!(
        committed, rendered,
        "docs/sql-conformance.md is out of date. Regenerate with \
         REGEN_SQL_CONFORMANCE=1 cargo test -p ravel-sql --test conformance"
    );
}

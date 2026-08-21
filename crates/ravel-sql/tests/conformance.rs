//! SQL conformance suite and table generator (ADR-0035).
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
use std::sync::Arc;

use datafusion::arrow::array::Array;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_sql::conformance::{
    Category, Classification, Construct, Verdict, Verified, registry, render_document, score,
};
use ravel_sql::{QueryOutput, SqlError, ValidationError, validate};
use ravel_types::{Signal, TenantId, logstream};
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};
use uuid::Uuid;

/// A small metrics dataset the supported constructs execute against: two
/// series, overlapping timestamps, a negative value so `WHERE value > 0`
/// actually filters.
///
/// The values are load-bearing, not just present: [`expectation`] states the
/// exact aggregate each admitted aggregate must return over them and the exact
/// row count each clause must produce, so a construct that executes but answers
/// wrongly (or answers with nothing) is a state-3 row.
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

/// The four sample values [`dataset`] publishes, in no particular order.
const SAMPLE_VALUES: [f64; 4] = [1.0, 2.0, -1.0, 3.0];

/// How many log records [`publish_logs`] writes.
const LOG_RECORD_COUNT: usize = 3;

/// What a supported construct's result must look like. Every supported registry
/// row needs one: "the query returned `Ok`" is not evidence, because an empty
/// result set is a perfectly successful answer to a query that found nothing.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Expect {
    /// One row, one column, carrying exactly this `f64` (compared by bit
    /// pattern, per the repo's float rule).
    Scalar(f64),
    /// Exactly this many rows, each of which must carry a value in every
    /// column (no all-null padding row standing in for data).
    Rows(usize),
    /// One row, one column, carrying exactly this string. Row-count-only
    /// checks can't tell a construct that transformed its input correctly
    /// from one that passed it through unchanged or applied a different
    /// (still successful, still one-row) transform.
    Str(&'static str),
}

/// The declared result for one supported construct, or `None` when the
/// construct is a rejected row (whose expectation is its typed error).
///
/// Total over the supported surface by construction: [`verify`] reports a
/// supported construct with no expectation as broken, so a new supported row
/// cannot be added without stating what it must return.
fn expectation(construct: &Construct) -> Option<Expect> {
    let sum: f64 = SAMPLE_VALUES.iter().sum();
    let count = SAMPLE_VALUES.len();
    match (construct.category, construct.name.as_str()) {
        (Category::Aggregate, "count") => Some(Expect::Scalar(count as f64)),
        (Category::Aggregate, "sum") => Some(Expect::Scalar(sum)),
        (Category::Aggregate, "min") => Some(Expect::Scalar(-1.0)),
        (Category::Aggregate, "max") => Some(Expect::Scalar(3.0)),
        (Category::Aggregate, "avg" | "mean") => Some(Expect::Scalar(sum / count as f64)),
        // The four samples, projected and ordered.
        (Category::Clause, "Projection" | "ORDER BY") => Some(Expect::Rows(count)),
        // `WHERE value > 0` drops the one negative sample.
        (Category::Clause, "Filter (WHERE)") => Some(Expect::Rows(count - 1)),
        // One row per series.
        (Category::Clause, "GROUP BY") => Some(Expect::Rows(2)),
        (Category::Clause, "LIMIT") => Some(Expect::Rows(1)),
        (Category::TableDispatch, "samples -> Signal::Metrics") => Some(Expect::Rows(count)),
        (Category::TableDispatch, "logs -> Signal::Logs") => Some(Expect::Rows(LOG_RECORD_COUNT)),
        // ADR-0090 decision 8: analytical clause/operator shapes over the
        // metrics dataset. Every expected value is re-derived directly from the
        // four fixture samples (1, 2, -1, 3), not from any implementation path.
        // Two series ("a", "b"), so `count(DISTINCT series_id)` is 2 -- a
        // plain `count(series_id)` would be 4, so DISTINCT is load-bearing.
        (Category::Clause, "count(DISTINCT)") => Some(Expect::Scalar(2.0)),
        // Four values ordered, skip the first: three rows.
        (Category::Clause, "OFFSET") => Some(Expect::Rows(3)),
        // Both series carry exactly 2 samples, so `HAVING count(value) > 2`
        // excludes both: 0 rows, versus 2 without the filter.
        (Category::Clause, "HAVING") => Some(Expect::Rows(0)),
        // One group per series.
        (Category::Clause, "GROUP BY ordinal") => Some(Expect::Rows(2)),
        // One CASE result per sample.
        (Category::Clause, "CASE") => Some(Expect::Rows(count)),
        // Two of the four values are in (1, 2).
        (Category::Clause, "IN list") => Some(Expect::Rows(2)),
        // A one-row scalar rewrite ('ab' -> 'ba' via the backreference).
        (Category::Clause, "REGEXP_REPLACE backreference") => Some(Expect::Str("ba")),
        // One extracted minute per sample.
        (Category::Clause, "date_part(minute)") => Some(Expect::Rows(count)),
        // One truncated timestamp per sample.
        (Category::Clause, "DATE_TRUNC") => Some(Expect::Rows(count)),
        // ADR-0090: typed queries over the declared `i64` column `dur`, whose
        // fixture values are 10, 20, 30 (see `publish_logs`). Two rows have
        // `dur >= 20`; the sum is 60.
        (Category::Clause, "declared i64 typed comparison") => Some(Expect::Rows(2)),
        (Category::Clause, "declared i64 typed aggregate") => Some(Expect::Scalar(60.0)),

        // ADR-0097 decision 8: one representative scalar per admitted family,
        // each result re-derived from its literal inputs.
        (Category::Scalar, "upper") => Some(Expect::Str("AB")),
        // Character-wise reverse: 'résumé' -> 'émusér'. A byte-wise reverse
        // would corrupt the two-byte 'é', so this is load-bearing for unicode.
        (Category::Scalar, "reverse") => Some(Expect::Str("émusér")),
        // to_char over a tz-free Date32, so the format is deterministic.
        (Category::Scalar, "to_char") => Some(Expect::Str("2024-07-15")),
        (Category::Scalar, "abs") => Some(Expect::Scalar(3.5)),
        // Default (non-global) replace hits the first digit run only.
        (Category::Scalar, "regexp_replace") => Some(Expect::Str("fooXbar")),
        // hex of the single byte 0x61 ('a').
        (Category::Scalar, "encode") => Some(Expect::Str("61")),
        // Ravel UDFs over the fixture. The sample with value 3.0 belongs to
        // series "b", whose only label is `__name__ = "b"`.
        (Category::Scalar, "label") => Some(Expect::Str("b")),
        // Series "b" carries 2 of the 4 samples, so the anchored matcher keeps
        // exactly 2 rows.
        (Category::Scalar, "label_match") => Some(Expect::Scalar(2.0)),
        // Of the three log bodies ("conformance record 0/1/2"), only the second
        // tokenizes to contain the word "1".
        (Category::Scalar, "has_word") => Some(Expect::Scalar(1.0)),

        // ADR-0097 decision 8: the eight admitted window functions, each result
        // re-derived from the four fixture samples (values -1, 1, 2, 3; all
        // distinct, so no ties). Each example aggregates the window column to a
        // single deterministic scalar.
        // 4 rows -> row numbers 1..=4 -> max 4.
        (Category::Window, "row_number") => Some(Expect::Scalar(4.0)),
        // Distinct values -> ranks 1,2,3,4 -> max 4.
        (Category::Window, "rank") => Some(Expect::Scalar(4.0)),
        // Distinct values -> dense ranks 1,2,3,4 -> max 4.
        (Category::Window, "dense_rank") => Some(Expect::Scalar(4.0)),
        // ntile(2) over 4 rows -> buckets 1,1,2,2 -> max bucket 2.
        (Category::Window, "ntile") => Some(Expect::Scalar(2.0)),
        // lag is null on the first row -> 3 non-null of 4.
        (Category::Window, "lag") => Some(Expect::Scalar(3.0)),
        // lead is null on the last row -> 3 non-null of 4.
        (Category::Window, "lead") => Some(Expect::Scalar(3.0)),
        // cume_dist reaches exactly 1.0 on the last row.
        (Category::Window, "cume_dist") => Some(Expect::Scalar(1.0)),
        // percent_rank = (rank-1)/(n-1); the last row is (4-1)/(4-1) = 1.0.
        (Category::Window, "percent_rank") => Some(Expect::Scalar(1.0)),

        _ => None,
    }
}

/// Checks a query's actual output against its declared expectation. `Ok(())`
/// means the construct answered with the values the registry claims for it.
fn check_output(expect: Expect, output: &QueryOutput) -> Result<(), String> {
    match expect {
        Expect::Scalar(want) => {
            if output.num_rows() != 1 {
                return Err(format!(
                    "expected 1 row carrying {want}, got {} rows",
                    output.num_rows()
                ));
            }
            let got = single_f64(output)?;
            // Bit-pattern comparison, per CLAUDE.md's float rule: NaN and
            // -0.0 are significant in an aggregate result.
            if got.to_bits() != want.to_bits() {
                return Err(format!("expected {want}, got {got}"));
            }
            Ok(())
        }
        Expect::Rows(want) => {
            if output.num_rows() != want {
                return Err(format!("expected {want} rows, got {}", output.num_rows()));
            }
            for batch in output.batches() {
                for column in batch.columns() {
                    if !column.is_empty() && column.null_count() == column.len() {
                        return Err(
                            "a result column is entirely null, so the rows carry no values"
                                .to_string(),
                        );
                    }
                }
            }
            Ok(())
        }
        Expect::Str(want) => {
            if output.num_rows() != 1 {
                return Err(format!(
                    "expected 1 row carrying {want:?}, got {} rows",
                    output.num_rows()
                ));
            }
            let got = single_str(output)?;
            if got != want {
                return Err(format!("expected {want:?}, got {got:?}"));
            }
            Ok(())
        }
    }
}

/// The single string cell of a one-row, one-column result.
fn single_str(output: &QueryOutput) -> Result<String, String> {
    use datafusion::arrow::array::StringArray;

    let batch = output
        .batches()
        .iter()
        .find(|b| b.num_rows() == 1)
        .ok_or_else(|| "no single-row batch in the result".to_string())?;
    if batch.num_columns() != 1 {
        return Err(format!("expected 1 column, got {}", batch.num_columns()));
    }
    let column = batch.column(0);
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("result column has type {}, not Utf8", column.data_type()))?;
    Ok(strings.value(0).to_string())
}

/// The single numeric cell of a one-row, one-column result, as an `f64`.
/// `count` comes back as an integer column, so both cases are read.
fn single_f64(output: &QueryOutput) -> Result<f64, String> {
    use datafusion::arrow::array::{Float64Array, Int64Array, UInt64Array};

    let batch = output
        .batches()
        .iter()
        .find(|b| b.num_rows() == 1)
        .ok_or_else(|| "no single-row batch in the result".to_string())?;
    if batch.num_columns() != 1 {
        return Err(format!("expected 1 column, got {}", batch.num_columns()));
    }
    let column = batch.column(0);
    if let Some(floats) = column.as_any().downcast_ref::<Float64Array>() {
        return Ok(floats.value(0));
    }
    if let Some(ints) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(ints.value(0) as f64);
    }
    if let Some(ints) = column.as_any().downcast_ref::<UInt64Array>() {
        return Ok(ints.value(0) as f64);
    }
    Err(format!(
        "result column has type {}, which is not a number",
        column.data_type()
    ))
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
        ValidationError::ExcludedScalar { .. } => "ValidationError::ExcludedScalar",
        ValidationError::ExcludedWindow { .. } => "ValidationError::ExcludedWindow",
    }
}

/// Publishes one RLOG object of [`LOG_RECORD_COUNT`] records plus its
/// `Signal::Logs` commit record, so the `logs` dispatch row has real rows to
/// return instead of passing on an empty-but-successful answer.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("conformance".to_string()),
    )];
    let stream_id = logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let writer_id = Uuid::from_u128(2_000);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for i in 0..LOG_RECORD_COUNT {
        let ts_ns = 1_000 + i as i64;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("conformance record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                // A declared `i64` attribute for the ADR-0090 typed-column
                // conformance rows: dur = 10, 20, 30 across the three records.
                attrs: vec![("dur".to_string(), AttrValue::I64((i as i64 + 1) * 10))],
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");

    // The RLOG fetch path never re-hashes the object (unlike RSEG's reader), so
    // a fixed content hash is enough here: it only has to be the same bytes the
    // data key is derived from.
    let content_hash = [7u8; 32];
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: LOG_RECORD_COUNT as u64,
        series_count: 1,
        min_event_ts_ns: 1_000,
        max_event_ts_ns: 1_000 + LOG_RECORD_COUNT as i64 - 1,
        min_ingest_ts_ns: 1_000,
        max_ingest_ts_ns: 1_000 + LOG_RECORD_COUNT as i64 - 1,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// A fixture carrying both the metrics dataset and the logs object, which is
/// what every construct in the registry is verified against.
async fn conformance_fixture() -> Fixture {
    let tenant = tenant_id("conformance");
    let specs = dataset();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let fixture = Fixture::build(
        Arc::clone(&store),
        &[(&tenant, &specs)],
        ravel_sql::SqlConfig::default(),
        1 << 30,
    )
    .await;
    publish_logs(store.as_ref(), &tenant).await;
    // Install a declared `i64` attribute column `dur` (ADR-0090) so the
    // typed-column conformance rows resolve a widened `logs` schema and project
    // `dur` as a native `Int64` column. `StaticDeclaredColumns` keeps the
    // fixture self-contained (no dependency on ravel-server's real overlay).
    let Fixture {
        store,
        catalog,
        fetcher,
        executor,
        data_keys,
    } = fixture;
    let executor = executor.with_declared_column_source(Arc::new(
        ravel_sql::StaticDeclaredColumns::new(vec![ravel_sql::DeclaredColumn::new(
            "dur",
            ravel_sql::DeclaredType::I64,
        )]),
    ));
    Fixture {
        store,
        catalog,
        fetcher,
        executor,
        data_keys,
    }
}

/// Runs one supported construct's example and checks the result against a
/// declared expectation, rather than against `Ok(_)` alone.
async fn verify_supported(example: &str, expect: Expect, fixture: &Fixture) -> Verdict {
    let tenant = tenant_id("conformance");
    match fixture
        .executor
        .execute(tenant.hash(), &request(example))
        .await
    {
        Ok(outcome) => match check_output(expect, &outcome.output) {
            Ok(()) => Verdict::Confirmed,
            Err(observed) => Verdict::Broken { observed },
        },
        Err(e) => Verdict::Broken {
            observed: format!("query failed: {e}"),
        },
    }
}

/// Check one construct's live behavior against its declared classification.
async fn verify(construct: &Construct, fixture: &Fixture) -> Verdict {
    let tenant = tenant_id("conformance");
    match construct.classification {
        Classification::SupportedAndCovered { .. } => match expectation(construct) {
            Some(expect) => verify_supported(&construct.example, expect, fixture).await,
            // A supported row with no declared result is unverifiable: `Ok(_)`
            // alone would let an empty answer publish as covered.
            None => Verdict::Broken {
                observed: "no result expectation is declared for this construct".to_string(),
            },
        },
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
    let fixture = conformance_fixture().await;

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

/// Every construct declared supported executes through the real pipeline *and*
/// returns the result its expectation declares. This is the test the registry
/// names as evidence for its state-1 rows, and the value check is what keeps an
/// empty-but-successful answer from counting as coverage.
///
/// The name is load-bearing: the registry cites
/// `tests/conformance.rs::supported_constructs_execute` as the evidence for
/// every state-1 row, so this function keeps that name.
#[tokio::test]
async fn supported_constructs_execute() {
    let tenant = tenant_id("conformance");
    let fixture = conformance_fixture().await;

    let mut checked = 0usize;
    for construct in registry() {
        if let Classification::SupportedAndCovered { .. } = construct.classification {
            let expect = expectation(&construct).unwrap_or_else(|| {
                panic!(
                    "supported construct `{}` declares no result expectation",
                    construct.name
                )
            });
            let outcome = fixture
                .executor
                .execute(tenant.hash(), &request(&construct.example))
                .await;
            let output = outcome
                .unwrap_or_else(|e| {
                    panic!("supported construct `{}` must execute: {e}", construct.name)
                })
                .output;
            if let Err(why) = check_output(expect, &output) {
                panic!(
                    "supported construct `{}` ran `{}` but {why}",
                    construct.name, construct.example
                );
            }
            checked += 1;
        }
    }
    assert!(
        checked > 0,
        "the registry must enumerate supported constructs"
    );
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

/// A construct that executes but answers with nothing is
/// broken, not covered.
///
/// The synthetic regression is a real query that really succeeds and really
/// returns zero rows (`WHERE value > 1000` over a dataset whose largest value
/// is 3). Under the old `Ok(_)`-only check this was a confirmed state-1 row; the
/// value check makes it a state-3 row, and the same check catches a wrong
/// aggregate value.
#[tokio::test]
async fn an_empty_but_successful_result_is_not_coverage() {
    let fixture = conformance_fixture().await;

    let verdict = verify_supported(
        "SELECT ts, value FROM samples WHERE value > 1000 ORDER BY ts",
        Expect::Rows(SAMPLE_VALUES.len()),
        &fixture,
    )
    .await;
    match verdict {
        Verdict::Broken { observed } => assert_eq!(observed, "expected 4 rows, got 0"),
        Verdict::Confirmed => panic!("an empty result must not confirm a supported construct"),
    }

    // The control: the same shape of query over the real data confirms.
    assert_eq!(
        verify_supported(
            "SELECT ts, value FROM samples WHERE value > 0 ORDER BY ts",
            Expect::Rows(SAMPLE_VALUES.len() - 1),
            &fixture,
        )
        .await,
        Verdict::Confirmed
    );

    // A successful query answering with the wrong value is caught the same
    // way: `sum` over this dataset is 5, not 4.
    match verify_supported(
        "SELECT sum(value) FROM samples",
        Expect::Scalar(4.0),
        &fixture,
    )
    .await
    {
        Verdict::Broken { observed } => assert_eq!(observed, "expected 4, got 5"),
        Verdict::Confirmed => panic!("a wrong aggregate value must not confirm"),
    }
}

/// Every supported row states what it must return, so no construct can be
/// added to the registry and verified on `Ok(_)` alone.
#[test]
fn every_supported_construct_declares_a_result_expectation() {
    let mut supported = 0usize;
    for construct in registry() {
        if let Classification::SupportedAndCovered { .. } = construct.classification {
            assert!(
                expectation(&construct).is_some(),
                "supported construct `{}` ({}) declares no result expectation",
                construct.name,
                construct.category.label()
            );
            supported += 1;
        }
        if let Classification::IntentionallyRejected { .. } = construct.classification {
            assert!(
                expectation(&construct).is_none(),
                "rejected construct `{}` declares a result expectation",
                construct.name
            );
        }
    }
    assert!(
        supported > 0,
        "the registry must enumerate supported constructs"
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

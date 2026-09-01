//! Security tests: the two SQL surface invariants.
//!
//! Invariant 1 (read-only single statement) is asserted at the level the
//! plan requires: *rejected before planning*. The proof is a side-effect
//! counter rather than the error message alone -- every rejection case runs
//! against a store that counts operations, and the assertion is that the
//! store saw **zero** operations. A gate that rejected after resolving would
//! still return an error, but the counter would be non-zero, and a gate that
//! rejected after planning would have listed commit records. Asserting "no
//! I/O happened" is the only assertion that distinguishes those.
//!
//! Invariant 2 (per-query single-tenant session) is asserted two ways:
//! tenant A's query cannot read tenant B's rows, and tenant A's query cannot
//! move tenant B's memory accountant.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::collections::HashSet;
use std::sync::Arc;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{ErrorClass, SqlConfig, SqlError, ValidationError};
use util::{CountingStore, Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// Every statement kind checked, plus a multi-statement body.
const REJECTED: &[(&str, &str)] = &[
    (
        "create external table",
        "CREATE EXTERNAL TABLE evil (a INT) STORED AS PARQUET LOCATION 's3://evil/x'",
    ),
    ("copy to", "COPY (SELECT * FROM samples) TO 's3://evil/out'"),
    ("insert", "INSERT INTO samples VALUES (1, 2.0)"),
    ("set", "SET datafusion.execution.batch_size = 1"),
    ("multi statement", "SELECT 1; SELECT 2"),
];

fn one_segment() -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new("m", vec![(100, 1.0), (200, 2.0)])],
    )]
}

/// A pass-through store that counts every operation, which is what makes
/// "no side effect" checkable.
async fn counting_fixture() -> (Arc<CountingStore>, Fixture) {
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let counting = CountingStore::new(inner);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let tenant = tenant_id("acme");
    let specs = one_segment();
    let fixture = Fixture::build(store, &[(&tenant, &specs)], SqlConfig::default(), 1 << 30).await;
    (counting, fixture)
}

#[tokio::test]
async fn rejected_statement_kinds_return_bad_request_before_any_planning() {
    let (counting, fixture) = counting_fixture().await;
    let tenant = tenant_id("acme");

    for (name, sql) in REJECTED {
        let before = counting.total_ops();
        let err = fixture
            .executor
            .execute(tenant.hash(), &request(sql))
            .await
            .expect_err(name);

        assert_eq!(
            err.class(),
            ErrorClass::BadRequest,
            "{name} must be a 400-class rejection, got {err}"
        );
        assert!(
            matches!(err, SqlError::Validation(_)),
            "{name} must be rejected by the validation gate, got {err}"
        );

        // The load-bearing assertion: no catalog LIST, no GET, no PUT. The
        // rejection happened before the resolve, which is before planning.
        assert_eq!(
            counting.total_ops(),
            before,
            "{name} must not touch the object store at all"
        );
    }
}

/// The rejection reason is specific, not a generic parse failure: a client
/// (and an operator reading logs) can tell "you used DDL" from "your SQL is
/// malformed".
#[tokio::test]
async fn rejection_reasons_name_the_statement_kind() {
    let (_counting, fixture) = counting_fixture().await;
    let tenant = tenant_id("acme");

    let cases: &[(&str, ValidationError)] = &[
        (
            "CREATE EXTERNAL TABLE evil (a INT) STORED AS PARQUET LOCATION 's3://evil/x'",
            ValidationError::NotReadOnly {
                kind: "CREATE EXTERNAL TABLE",
            },
        ),
        (
            "COPY (SELECT * FROM samples) TO 's3://evil/out'",
            ValidationError::NotReadOnly { kind: "COPY" },
        ),
        (
            "INSERT INTO samples VALUES (1, 2.0)",
            ValidationError::NotReadOnly { kind: "INSERT" },
        ),
        (
            "SET datafusion.execution.batch_size = 1",
            ValidationError::NotReadOnly { kind: "SET" },
        ),
        (
            "SELECT 1; SELECT 2",
            ValidationError::MultipleStatements { count: 2 },
        ),
    ];

    for (sql, want) in cases {
        let err = fixture
            .executor
            .execute(tenant.hash(), &request(sql))
            .await
            .expect_err(sql);
        match err {
            SqlError::Validation(got) => assert_eq!(&got, want, "sql: {sql}"),
            other => panic!("expected a validation error for {sql}, got {other}"),
        }
    }
}

/// A rejected `COPY ... TO` must leave nothing behind. Listing the whole
/// store afterwards and comparing against the pre-query key set proves it,
/// independently of the operation counter above.
#[tokio::test]
async fn a_rejected_copy_writes_no_object() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = tenant_id("acme");
    let specs = one_segment();
    let fixture = Fixture::build(
        Arc::clone(&store),
        &[(&tenant, &specs)],
        SqlConfig::default(),
        1 << 30,
    )
    .await;

    let before = all_keys(store.as_ref()).await;
    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("COPY (SELECT * FROM samples) TO 'exfil.parquet'"),
        )
        .await
        .expect_err("COPY must be rejected");
    assert!(matches!(err, SqlError::Validation(_)));

    assert_eq!(
        all_keys(store.as_ref()).await,
        before,
        "a rejected COPY must not create any object"
    );
}

/// Invariant 2, data half: two tenants publish under the same metric name
/// with different values. Each tenant's query sees only its own rows.
#[tokio::test]
async fn a_tenant_query_cannot_observe_another_tenants_rows() {
    let a = tenant_id("tenant-a");
    let b = tenant_id("tenant-b");
    let a_specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new("shared", vec![(100, 1.0), (200, 2.0)])],
    )];
    let b_specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "shared",
            vec![(100, 900.0), (200, 901.0), (300, 902.0)],
        )],
    )];
    let fixture = Fixture::memory(&[(&a, &a_specs), (&b, &b_specs)]).await;

    let sql = "SELECT count(value) AS n, sum(value) AS total FROM samples";

    let out_a = fixture
        .executor
        .execute(a.hash(), &request(sql))
        .await
        .expect("tenant a query");
    let out_b = fixture
        .executor
        .execute(b.hash(), &request(sql))
        .await
        .expect("tenant b query");

    let (count_a, sum_a) = count_and_sum(&out_a.output);
    let (count_b, sum_b) = count_and_sum(&out_b.output);

    assert_eq!(count_a, 2, "tenant a must see only its own two samples");
    assert_eq!(sum_a.to_bits(), 3.0f64.to_bits());
    assert_eq!(count_b, 3, "tenant b must see only its own three samples");
    assert_eq!(sum_b.to_bits(), 2703.0f64.to_bits());
}

/// Invariant 2, data half, adversarial query shapes: the single
/// flat-aggregate positive test above proves the happy path, but tenant
/// isolation is the invariant whose failure is an S0 cross-tenant
/// disclosure, so it must be pinned against the query shapes an attacker
/// would actually probe, not just `count/sum`. Each shape below is issued
/// for tenant A and for tenant B against the same two-tenant fixture. The
/// tenants publish under the same metric name with disjoint value sets, so
/// "every returned value belongs to the requester and none to the other
/// tenant" is a bit-exact isolation check. A shape that resolves must return
/// only the requester's own rows; a shape that fails to plan is equally
/// acceptable (it reaches no data). Either way, no shape may surface another
/// tenant's value.
///
/// This locks in the *structural* guarantee: `tenant_hash` is a caller
/// parameter no SQL text can influence, and exactly one tenant-bound
/// `samples` table exists per session. A future change that registered a
/// second catalog/schema or a cross-tenant-capable provider would break
/// these cases rather than passing silently.
#[tokio::test]
async fn adversarial_query_shapes_stay_tenant_isolated() {
    let a = tenant_id("tenant-a");
    let b = tenant_id("tenant-b");
    let a_specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new("shared", vec![(100, 1.0), (200, 2.0)])],
    )];
    let b_specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "shared",
            vec![(100, 900.0), (200, 901.0), (300, 902.0)],
        )],
    )];
    let fixture = Fixture::memory(&[(&a, &a_specs), (&b, &b_specs)]).await;

    let a_bits: HashSet<u64> = [1.0f64, 2.0].iter().map(|v| v.to_bits()).collect();
    let b_bits: HashSet<u64> = [900.0f64, 901.0, 902.0]
        .iter()
        .map(|v| v.to_bits())
        .collect();

    // Every shape projects a single `value` column so the result is compared
    // uniformly, whatever the enclosing query structure.
    let shapes: &[(&str, &str)] = &[
        // A fully qualified catalog.schema.table reference. DataFusion's
        // default catalog/schema is `datafusion.public`, so this names the
        // one registered tenant-bound table by its long name; it must not
        // become a handle to any other tenant's data.
        (
            "qualified datafusion.public.samples",
            "SELECT value FROM datafusion.public.samples",
        ),
        // A reference into a catalog that does not exist. There is no second
        // catalog to resolve into, so this must fail to plan, never fall back
        // to another tenant.
        ("qualified other.samples", "SELECT value FROM other.samples"),
        // UNION (distinct) and UNION ALL: both arms resolve to the same
        // single tenant-bound provider; there is no second data source.
        (
            "union",
            "SELECT value FROM samples UNION SELECT value FROM samples",
        ),
        (
            "union all",
            "SELECT value FROM samples UNION ALL SELECT value FROM samples",
        ),
        // A correlated subquery: the inner query references the outer row and
        // reads `samples`, but `samples` is still the single tenant-bound
        // table on both sides.
        (
            "correlated subquery",
            "SELECT value FROM samples s \
             WHERE EXISTS (SELECT 1 FROM samples t WHERE t.ts = s.ts)",
        ),
        // A CTE reading `samples`: the WITH binding names the same provider.
        (
            "cte",
            "WITH c AS (SELECT value FROM samples) SELECT value FROM c",
        ),
    ];

    for (shape, sql) in shapes {
        for (me, mine, theirs) in [(&a, &a_bits, &b_bits), (&b, &b_bits, &a_bits)] {
            let who = me.as_str();
            match fixture.executor.execute(me.hash(), &request(sql)).await {
                Ok(out) => {
                    let got = value_bits(&out.output);
                    // A resolving shape must actually read the requester's own
                    // rows, not silently return an empty set that would hide a
                    // wrong resolution.
                    assert!(
                        !got.is_empty(),
                        "shape `{shape}` for {who}: resolved but returned no rows"
                    );
                    for bits in &got {
                        assert!(
                            mine.contains(bits),
                            "shape `{shape}` for {who}: value {} is not the requester's own row",
                            f64::from_bits(*bits)
                        );
                        assert!(
                            !theirs.contains(bits),
                            "shape `{shape}` for {who}: reached another tenant's value {}",
                            f64::from_bits(*bits)
                        );
                    }
                }
                Err(err) => {
                    // Failing to plan is an acceptable outcome: it reaches no
                    // data. It must be a client-safe class, not an internal
                    // leak, and it must not carry another tenant's detail to
                    // the client.
                    assert!(
                        matches!(err, SqlError::Plan(_))
                            || matches!(err.class(), ErrorClass::BadRequest),
                        "shape `{shape}` for {who}: unexpected error class: {err}"
                    );
                }
            }
        }
    }
}

/// Invariant 2, budget half: tenant A running a query must not move tenant
/// B's accountant. Both accountants are read before and after.
#[tokio::test]
async fn a_tenant_query_cannot_move_another_tenants_memory_budget() {
    let a = tenant_id("tenant-a");
    let b = tenant_id("tenant-b");
    let a_specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "m",
            (0..500).map(|i| (i, i as f64)).collect(),
        )],
    )];
    let b_specs = one_segment();
    let fixture = Fixture::memory(&[(&a, &a_specs), (&b, &b_specs)]).await;

    let budget_a = fixture.executor.tenant_budget(a.hash());
    let budget_b = fixture.executor.tenant_budget(b.hash());
    assert_eq!(budget_a.reserved(), 0);
    assert_eq!(budget_b.reserved(), 0);
    assert!(
        !Arc::ptr_eq(&budget_a, &budget_b),
        "each tenant must have its own accountant, never a shared one"
    );

    fixture
        .executor
        .execute(a.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("tenant a query");

    assert_eq!(
        budget_b.reserved(),
        0,
        "tenant b's accountant must be untouched by tenant a's query"
    );
    // And tenant A's own accountant returned to zero: every reservation the
    // query took was released when its streams dropped.
    assert_eq!(
        budget_a.reserved(),
        0,
        "tenant a's accountant must return to zero after the query completes"
    );
}

/// `information_schema` is disabled, so the metadata surface a tenant could
/// use to enumerate anything is simply absent (defense in depth behind the
/// parse gate; the query is a legal `Statement::Query`, so validation lets
/// it through and the session config is what rejects it).
#[tokio::test]
async fn information_schema_is_not_reachable() {
    let tenant = tenant_id("acme");
    let specs = one_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT * FROM information_schema.tables"),
        )
        .await
        .expect_err("information_schema must not resolve");
    assert!(
        matches!(err, SqlError::Plan(_)),
        "expected a planning failure, got {err}"
    );
    // And the failure detail does not reach the client.
    assert_eq!(err.client_message(), ravel_sql::MSG_PLAN);
}

/// `range`/`generate_series` are DataFusion table functions registered by
/// `with_default_features()` regardless of what this crate wants exposed
/// (checkpoint review finding): they generate rows in memory, so
/// `EmptyObjectStoreRegistry` does not block them the way it blocks
/// `CREATE EXTERNAL TABLE`/`COPY`. `samples` must be the only table a query
/// can read from.
#[tokio::test]
async fn table_functions_are_not_reachable() {
    let tenant = tenant_id("acme");
    let specs = one_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    for sql in [
        "SELECT * FROM range(0, 10)",
        "SELECT * FROM generate_series(0, 10)",
    ] {
        let err = fixture
            .executor
            .execute(tenant.hash(), &request(sql))
            .await
            .expect_err("table function must not resolve");
        assert!(
            matches!(err, SqlError::Plan(_)),
            "{sql}: expected a planning failure, got {err}"
        );
        assert_eq!(err.client_message(), ravel_sql::MSG_PLAN);
    }
}

/// ADR-0097 decision 9 end-to-end reachability: a scalar excluded only by the
/// registry gate (`uuid`, nondeterministic) and a window function excluded only
/// by the gate (`first_value` with `OVER`) must both be refused through the
/// path a real caller takes -- `SqlExecutor::execute` -- with a *typed* error
/// naming why, not an opaque plan failure. This mirrors
/// `table_functions_are_not_reachable` above (same fixture shape), but asserts
/// the specific `ValidationError` variants that make the refusal legible: an
/// excluded scalar is not an aggregate, and a windowed excluded name is not one
/// either, so neither reuses `ExcludedAggregate`.
///
/// `validate` (step 1 of `execute`) runs before any catalog resolve, so the
/// `FROM logs` reference never needs a logs snapshot: the request is refused on
/// its text alone, which is exactly the message layer this test pins. The
/// registry gate in `build_session` is the backstop that actually fails closed;
/// this walk only produces the better error, and the gate's independent refusal
/// is proven by `session::tests::admitted_and_excluded_cover_all_registries_for_every_table`.
#[tokio::test]
async fn excluded_scalar_and_window_functions_are_not_reachable() {
    let tenant = tenant_id("acme");
    let specs = one_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    // An excluded nondeterministic scalar: typed ExcludedScalar, not Plan.
    let err = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT uuid() FROM logs"))
        .await
        .expect_err("uuid() must be refused");
    assert!(
        matches!(
            err,
            SqlError::Validation(ValidationError::ExcludedScalar { ref name }) if name == "uuid"
        ),
        "SELECT uuid() must yield a typed ExcludedScalar error, got {err}"
    );
    assert_eq!(err.class(), ErrorClass::BadRequest);

    // An excluded window function used with OVER: typed ExcludedWindow, whose
    // message names the admitted window surface rather than calling it an
    // aggregate.
    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT first_value(body) OVER (ORDER BY ts) FROM logs"),
        )
        .await
        .expect_err("first_value() OVER must be refused");
    assert!(
        matches!(
            err,
            SqlError::Validation(ValidationError::ExcludedWindow { ref name }) if name == "first_value"
        ),
        "SELECT first_value(...) OVER must yield a typed ExcludedWindow error, got {err}"
    );
    assert_eq!(err.class(), ErrorClass::BadRequest);
}

/// ADR-0097 Consequences: "Reachability, not registry state, is the acceptance
/// evidence." The focused test above pins two representative names end-to-end;
/// this one drives *every* name in [`ravel_sql::EXCLUDED_SCALARS`] and
/// [`ravel_sql::EXCLUDED_WINDOWS`] through `SqlExecutor::execute` -- the path a
/// real caller takes -- and asserts each is refused with its typed error,
/// never a successful result and never an opaque plan/execution error.
///
/// The names are read straight from the two constants, so this test cannot
/// drift from them: a name added to or removed from either list changes what
/// this test drives with no second edit here. That closes the gap the
/// conformance rows leave open, where excluded scalar/window names are attested
/// only through the validator, not through end-to-end execution.
///
/// During ADR-0097's design a probe drove these queries with the registry gate
/// patched off and confirmed the validator's finder still refused every one
/// before planning, with zero executing. This is that probe turned into a
/// standing test; its non-vacuousness rests on the finder producing a *typed*
/// error rather than the gate's opaque plan failure, so patching the finder off
/// (its `is_excluded_scalar`/`is_excluded_window` sources) makes every case
/// here execute or fail differently and the test go red.
///
/// Every call here uses a bare identifier. A quoted-identifier spelling
/// (`"uuid"()`) still reaches the registry gate and is still refused, but as
/// the same opaque plan failure the finder produces when patched off above,
/// not a typed error -- this test does not exercise or close that gap.
#[tokio::test]
async fn every_excluded_scalar_and_window_is_unreachable_through_execute() {
    let tenant = tenant_id("acme");
    let specs = one_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    assert!(
        !ravel_sql::EXCLUDED_SCALARS.is_empty() && !ravel_sql::EXCLUDED_WINDOWS.is_empty(),
        "the excluded sets must be non-empty or this test asserts nothing"
    );

    for name in ravel_sql::EXCLUDED_SCALARS {
        let sql = format!("SELECT {} FROM samples", scalar_call(name));
        let err = match fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await
        {
            Ok(_) => panic!("excluded scalar `{name}` executed to completion: `{sql}`"),
            Err(err) => err,
        };
        assert!(
            matches!(
                &err,
                SqlError::Validation(ValidationError::ExcludedScalar { name: got }) if got == name
            ),
            "excluded scalar `{name}` must yield a typed ExcludedScalar naming it, \
             got {err} (sql: {sql})"
        );
        assert_eq!(
            err.class(),
            ErrorClass::BadRequest,
            "excluded scalar `{name}` must be a 400-class refusal (sql: {sql})"
        );
    }

    for name in ravel_sql::EXCLUDED_WINDOWS {
        let sql = format!("SELECT {} FROM samples", window_call(name));
        let err = match fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await
        {
            Ok(_) => panic!("excluded window `{name}` executed to completion: `{sql}`"),
            Err(err) => err,
        };
        assert!(
            matches!(
                &err,
                SqlError::Validation(ValidationError::ExcludedWindow { name: got }) if got == name
            ),
            "excluded window `{name}` used with OVER must yield a typed ExcludedWindow \
             naming it, got {err} (sql: {sql})"
        );
        assert_eq!(
            err.class(),
            ErrorClass::BadRequest,
            "excluded window `{name}` must be a 400-class refusal (sql: {sql})"
        );
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// A syntactically valid call for an excluded scalar. Most are ordinary
/// parenthesized calls; the SQL-standard datetime niladics (`current_timestamp`
/// and friends) are spelled as bare keywords, the form the parser accepts and
/// the shape a client is most likely to write. Argument count is irrelevant to
/// the refusal: the validator matches on the function name before planning, so
/// the call only has to parse.
fn scalar_call(name: &str) -> String {
    match name {
        "current_timestamp" | "current_date" | "current_time" => name.to_string(),
        _ => format!("{name}()"),
    }
}

/// A syntactically valid windowed call for an excluded window function. The
/// simplest legal frame (`OVER (ORDER BY ts)`) is enough: the point is that the
/// name carrying an `OVER` clause is refused, not that a particular frame shape
/// is. A single column argument parses for all excluded window names, including
/// `nth_value`, because the validator refuses on the name before any argument
/// arity check.
fn window_call(name: &str) -> String {
    format!("{name}(value) OVER (ORDER BY ts)")
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let mut keys: Vec<String> = ravel_object_store::list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect();
    keys.sort();
    keys
}

/// Every cell of a single-column `value` projection, as raw bit patterns so
/// NaN and signed zero compare exactly. Used by the adversarial-shape
/// isolation test, where each shape projects only `value`.
fn value_bits(output: &ravel_sql::QueryOutput) -> Vec<u64> {
    use datafusion::arrow::array::Float64Array;

    let mut bits = Vec::new();
    for batch in output.batches() {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value column is Float64");
        for i in 0..batch.num_rows() {
            bits.push(col.value(i).to_bits());
        }
    }
    bits
}

fn count_and_sum(output: &ravel_sql::QueryOutput) -> (i64, f64) {
    use datafusion::arrow::array::{Float64Array, Int64Array};

    let batch = output
        .batches()
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);
    let sum = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("sum column")
        .value(0);
    (count, sum)
}

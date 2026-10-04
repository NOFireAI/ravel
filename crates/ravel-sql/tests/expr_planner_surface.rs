//! Probe (temporary).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_sql::SqlError;

use crate::util::{self, Fixture, SegSpec, SeriesSpec, tenant_id};

fn dataset() -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new("a", vec![(1, 1.0), (2, 2.0)])],
    )]
}

#[tokio::test]
async fn probe_outcomes() {
    let tenant = tenant_id("expr-planner-surface-2476");
    let specs = dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
    for sql in [
        "SELECT POSITION('a' IN 'ab')",
        "SELECT strpos('ab', 'a')",
        "SELECT SUBSTRING('abc' FROM 1 FOR 2)",
        "SELECT SUBSTRING('abc', 1, 2)",
        "SELECT substr('abc', 1, 2)",
        "SELECT EXTRACT(minute FROM ts) FROM samples",
        "SELECT OVERLAY('abc' PLACING 'x' FROM 2)",
        "SELECT overlay('abc', 'x', 2)",
        "SELECT STRUCT(1, 2)",
        "SELECT STRUCT(1 AS a)",
        "SELECT struct(1, 2)",
        "SELECT named_struct('a', 1)",
        "SELECT {'a': 1}",
        "SELECT MAP {'a': 1}",
        "SELECT make_map('a', 1)",
        "SELECT map('a', 1)",
        "SELECT [1, 2]",
        "SELECT make_array(1, 2)",
        "SELECT labels['__name__'] FROM samples",
        "SELECT labels.foo FROM samples",
        "SELECT named_struct('a', 1)['a']",
        "SELECT 'abc'[1]",
        "SELECT 1 @> 2",
        "SELECT 'a' || 'b'",
        "SELECT count(*) FROM samples",
        "SELECT count() FROM samples",
        "SELECT count(*) OVER () FROM samples",
        "SELECT ts FROM samples WHERE ts = '00112233445566778899aabbccddeeff'",
    ] {
        let r = fixture
            .executor
            .execute(tenant.hash(), &util::request(sql))
            .await;
        match r {
            Ok(o) => println!("PROBE OK rows={} :: {sql}", o.output.num_rows()),
            Err(e) => {
                let class = e.class();
                let kind = match &e {
                    SqlError::Plan(_) => "Plan",
                    SqlError::Execution(_) => "Execution",
                    SqlError::Validation(_) => "Validation",
                    _ => "Other",
                };
                println!("PROBE ERR {kind} {class:?} :: {sql} :: {e}");
            }
        }
    }
}

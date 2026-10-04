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
        "SELECT \"substr\"('abc', 1, 2)",
        "SELECT \"substring\"('abc', 1, 2)",
        "SELECT left('abc', 2)",
        "SELECT attrs['k'] FROM logs",
        "SELECT s.a FROM (SELECT named_struct('a', 1) AS s)",
        "SELECT ts FROM spans WHERE trace_id = '00112233445566778899aabbccddeeff'",
        "SELECT ts FROM spans WHERE trace_id <> '00112233445566778899aabbccddeeff'",
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

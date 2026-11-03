//! Numeric precision on the MCP surface (issue #1383, ADR-1374 D4 and D8).
//!
//! D4 carries every integer cell as a JSON string, because a nanosecond epoch
//! and a `u64` both exceed the 2^53 a JSON number holds exactly; carries
//! finite floats as JSON numbers with the sign of zero kept; and carries the
//! three non-finite floats as the strings `"NaN"`, `"+Inf"` and `"-Inf"`, with
//! every NaN payload collapsing to `"NaN"` (`crates/ravel-mcp/src/envelope.rs`,
//! `float_to_json`: the envelope reports no NaN bits). These cases run each
//! of those through `ravel_query_sql` and `ravel_query_promql` over a real
//! server, where the integers travel through the SQL output encoder and the
//! adapter's `json_to_cell`, and the floats through the stored segment.

#![cfg(feature = "mcp")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod mcp_adversarial_common;

use mcp_adversarial_common::*;
use ravel_object_store::ObjectStoreBackend;
use serde_json::{Value, json};

/// 2^53 + 1, the first integer an `f64` cannot hold.
const ABOVE_2_53: i64 = 9_007_199_254_740_993;
/// A quiet NaN whose payload is not the canonical one, and not the
/// Prometheus staleness marker either, so the engine treats it as a value.
const NAN_BITS: u64 = 0x7ff8_0000_0000_dead;

/// The ingest hour of a sample at `ts_ns`, so the segment holding it is
/// listed where a query over that instant resolves.
fn hour_of(ts_ns: i64) -> u32 {
    u32::try_from(ts_ns / NS_PER_HOUR).expect("the hour fits a u32")
}

/// Metric `m`: 1.5 at 2^53 + 1 ns, -0.0 one second later and the
/// non-canonical NaN a second after that, one segment in their ingest hour.
async fn seed(store: &dyn ObjectStoreBackend) {
    let nan = f64::from_bits(NAN_BITS);
    assert!(nan.is_nan());
    assert_ne!(nan.to_bits(), f64::NAN.to_bits());
    publish_series(
        store,
        TENANT_A,
        hour_of(ABOVE_2_53),
        1,
        "m",
        &[TestSeries {
            labels: Vec::new(),
            samples: vec![
                (ABOVE_2_53, 1.5),
                (ABOVE_2_53 + NS_PER_SEC, -0.0),
                (ABOVE_2_53 + 2 * NS_PER_SEC, nan),
            ],
        }],
    )
    .await;
}

fn window() -> Value {
    json!({
        "start_ns": "0",
        "end_ns": (ABOVE_2_53 + NS_PER_HOUR).to_string(),
    })
}

/// A JSON number cell's `f64`, sign of zero included.
fn float_bits(cell: &Value) -> u64 {
    cell.as_f64()
        .unwrap_or_else(|| panic!("a JSON number: {cell}"))
        .to_bits()
}

/// `ravel_query_sql` answers 2^53 + 1, `u64::MAX` and `i64::MIN` as their
/// exact decimal strings, a stored -0.0 as the JSON number `-0.0` whose sign
/// bit survives parsing, a stored NaN of any payload as the string `"NaN"`,
/// and each sample's nanosecond timestamp, itself past 2^53, exactly.
///
/// FLIP: `json_to_cell` in `services/ravel-server/src/mcp/envelope.rs` trying
/// `as_f64` before `as_i64`/`as_u64` makes `u64::MAX` the float
/// 18446744073709551615 rounds to, a JSON number, and the first cell
/// assertion fails.
/// FLIP: `float_to_json` in `crates/ravel-mcp/src/envelope.rs` writing
/// `json!(value + 0.0)` for a finite float answers `0.0` for the -0.0 row.
#[tokio::test]
async fn query_sql_keeps_large_integers_negative_zero_and_nan() {
    let store = memory_store();
    seed(store.as_ref()).await;
    let running = start(server_config(), store).await;
    let client = connect(&running, TOKEN_A).await;

    let (is_error, literals) = call(
        &client,
        "ravel_query_sql",
        json!({
            "query": format!(
                "SELECT arrow_cast({}, 'UInt64') AS u64_max, \
                 CAST({ABOVE_2_53} AS BIGINT) AS above_2_53, \
                 CAST(-9223372036854775807 AS BIGINT) - 1 AS i64_min \
                 FROM samples",
                u64::MAX
            ),
            "time_range": window(),
        }),
    )
    .await;
    assert_eq!(is_error, Some(false), "{literals}");
    // One row of the three literals per stored sample.
    let row = json!([
        u64::MAX.to_string(),
        ABOVE_2_53.to_string(),
        i64::MIN.to_string()
    ]);
    assert_eq!(
        literals["data"]["rows"],
        json!([row, row, row]),
        "{literals}"
    );

    let (is_error, samples) = call(
        &client,
        "ravel_query_sql",
        json!({"query": "SELECT ts, value FROM samples ORDER BY ts", "time_range": window()}),
    )
    .await;
    assert_eq!(is_error, Some(false), "{samples}");
    assert_eq!(
        column(&samples, "ts"),
        [
            json!(ABOVE_2_53.to_string()),
            json!((ABOVE_2_53 + NS_PER_SEC).to_string()),
            json!((ABOVE_2_53 + 2 * NS_PER_SEC).to_string()),
        ],
        "{samples}"
    );
    let values = column(&samples, "value");
    assert_eq!(float_bits(&values[0]), 1.5f64.to_bits());
    assert_eq!(float_bits(&values[1]), (-0.0f64).to_bits(), "{samples}");
    assert_eq!(values[2], json!("NaN"), "{samples}");
    // The wire text itself says -0.0, not 0 or 0.0.
    let text = samples["data"]["rows"].to_string();
    assert!(text.contains(",-0.0]"), "{text}");

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}

/// `ravel_query_promql` answers the raw samples of a range selector at their
/// own nanosecond timestamps, the first of them 2^53 + 1, as exact strings;
/// -0.0 as the JSON number `-0.0`; and the non-canonical NaN as `"NaN"`. An
/// instant evaluation answers the -0.0 too, stamped with its evaluation time.
///
/// FLIP: `series_data` in `crates/ravel-mcp/src/tools/query_promql.rs`
/// pushing `json!(ts_ns as f64)` instead of the decimal string answers
/// 9007199254740992.0 for the first sample.
/// FLIP: `float_to_json` returning `Value::Null` for a NaN answers `null`.
/// FLIP: `float_to_json` writing `json!(value + 0.0)` answers `0.0` for the
/// -0.0 sample.
#[tokio::test]
async fn query_promql_keeps_nanosecond_timestamps_negative_zero_and_nan() {
    let store = memory_store();
    seed(store.as_ref()).await;
    let running = start(server_config(), store).await;
    let client = connect(&running, TOKEN_A).await;
    // A whole millisecond after the last sample.
    let at_ms = (ABOVE_2_53 + 2 * NS_PER_SEC) / 1_000_000 + 1;
    let at_ns = at_ms * 1_000_000;

    let (is_error, raw) = call(
        &client,
        "ravel_query_promql",
        json!({"query": "m[1m]", "evaluation_time": at_ns.to_string()}),
    )
    .await;
    assert_eq!(is_error, Some(false), "{raw}");
    let samples = &rows(&raw)[0][1];
    assert_eq!(
        samples["ts_ns"],
        json!([
            ABOVE_2_53.to_string(),
            (ABOVE_2_53 + NS_PER_SEC).to_string(),
            (ABOVE_2_53 + 2 * NS_PER_SEC).to_string(),
        ]),
        "{raw}"
    );
    let values = samples["value"].as_array().expect("values");
    assert_eq!(float_bits(&values[0]), 1.5f64.to_bits());
    assert_eq!(float_bits(&values[1]), (-0.0f64).to_bits(), "{raw}");
    assert_eq!(values[2], json!("NaN"), "{raw}");

    // The -0.0 is the latest sample at one second past it.
    let instant_ns = ((ABOVE_2_53 + NS_PER_SEC) / 1_000_000 + 1) * 1_000_000;
    let (is_error, instant) = call(
        &client,
        "ravel_query_promql",
        json!({"query": "m", "evaluation_time": instant_ns.to_string()}),
    )
    .await;
    assert_eq!(is_error, Some(false), "{instant}");
    assert_eq!(
        rows(&instant)[0][1].to_string(),
        format!(r#"{{"ts_ns":["{instant_ns}"],"value":[-0.0]}}"#),
        "{instant}"
    );

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}

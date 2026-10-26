//! The status mapper as the MetricsBench query runner will call it: each
//! engine answers over HTTP through `PrometheusClient`, the oracle's outcome
//! becomes an [`OracleAnswer`], and the candidate's outcome is classified.
//! Until the portable query runner lands, this test is the mapper's caller.

#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::time::Duration;

use ravel_bench::metrics_correctness::{
    ClassifyError, Denominators, Engine, EntryExpectation, OracleAnswer, RefusalReason, classify,
};
use ravel_bench::report_schema::ResultStatus;
use ravel_promql_difftest::corpus::ComparisonMode;
use ravel_promql_difftest::prometheus_client::{PrometheusClient, QueryOutcome};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const DEADLINE: Duration = Duration::from_millis(500);

/// Answers every request with `status` and `body` until the test ends, or
/// never answers when `body` is `None`.
async fn serve(status: &'static str, body: Option<&'static str>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let Some(body) = body else {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    return;
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(response.as_bytes()).await;
            });
        }
    });
    addr
}

async fn query(addr: SocketAddr) -> QueryOutcome {
    PrometheusClient::with_deadline(format!("http://{addr}"), DEADLINE)
        .instant_query_with_deadline("up", 1_700_000_000_000, DEADLINE)
        .await
}

const ORACLE: &str = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"a"},"value":[1700000000,"1"]}]}}"#;
const ORACLE_EMPTY: &str = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;

fn entry() -> EntryExpectation {
    EntryExpectation {
        intended_empty: false,
        tolerance_ulps: None,
        mode: ComparisonMode::Unordered,
        constructs: vec!["vector_selector".to_string()],
    }
}

async fn oracle_answer() -> OracleAnswer {
    let outcome = query(serve("200 OK", Some(ORACLE)).await).await;
    OracleAnswer::from_outcome(&outcome).expect("oracle answered")
}

#[tokio::test]
async fn each_engine_outcome_over_http_gets_one_status() {
    let oracle = oracle_answer().await;
    let cases: [(Engine, &str, Option<&'static str>, ResultStatus); 7] = [
        (Engine::Ravel, "200 OK", Some(ORACLE), ResultStatus::Ok),
        (
            Engine::Ravel,
            "200 OK",
            Some(
                r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"a"},"value":[1700000000,"2"]}]}}"#,
            ),
            ResultStatus::Incorrect,
        ),
        // https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/
        (
            Engine::VictoriaMetrics,
            "200 OK",
            Some(
                r#"{"status":"success","isPartial":true,"data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"a"},"value":[1700000000,"1"]}]}}"#,
            ),
            ResultStatus::Partial,
        ),
        (Engine::Mimir, "200 OK", None, ResultStatus::Timeout),
        (
            Engine::Ravel,
            "503 Service Unavailable",
            Some(
                r#"{"status":"error","errorType":"unavailable","error":"upstream storage temporarily unavailable"}"#,
            ),
            ResultStatus::Error,
        ),
        (
            Engine::Ravel,
            "422 Unprocessable Entity",
            Some(
                r#"{"status":"error","errorType":"execution","error":"unsupported PromQL construct: x"}"#,
            ),
            ResultStatus::UnsupportedConstruct,
        ),
        (
            Engine::Ravel,
            "422 Unprocessable Entity",
            Some(
                r#"{"status":"error","errorType":"execution","error":"query issued 9 S3 requests, exceeding the budget of 8"}"#,
            ),
            ResultStatus::Refused,
        ),
    ];

    let mut classified = Vec::new();
    for (engine, status, body, want) in cases {
        let outcome = query(serve(status, body).await).await;
        let c = classify(engine, &outcome, Some(&oracle), &entry()).expect("classified");
        assert_eq!(c.status, want, "{engine:?} {status} {body:?}");
        classified.push(c);
    }
    assert_eq!(
        classified[6].refusal_reason,
        Some(RefusalReason::Budget),
        "{:?}",
        classified[6]
    );
    assert_eq!(
        Denominators::of(&classified),
        Denominators {
            applicable: 7,
            correctness: 3,
            competitive: 1,
            coverage_success: 1,
        }
    );
}

#[tokio::test]
async fn ravel_partial_coverage_refusal_over_http_is_refused_partial_coverage() {
    let oracle = oracle_answer().await;
    let outcome = query(
        serve(
            "503 Service Unavailable",
            Some(
                r#"{"status":"error","errorType":"unavailable","error":"partial results: coverage is incomplete (one or more federated clusters were skipped); set allow_partial=true to receive partial results"}"#,
            ),
        )
        .await,
    )
    .await;
    let c = classify(Engine::Ravel, &outcome, Some(&oracle), &entry()).expect("classified");
    assert_eq!(c.status, ResultStatus::Refused);
    assert_eq!(c.refusal_reason, Some(RefusalReason::PartialCoverage));
}

#[tokio::test]
async fn a_failed_or_empty_oracle_never_classifies_the_candidate() {
    let failed = query(serve("503 Service Unavailable", Some(r#"{"status":"error","errorType":"timeout","error":"query timed out in expression evaluation"}"#)).await).await;
    assert!(matches!(
        OracleAnswer::from_outcome(&failed),
        Err(ClassifyError::OracleUnavailable(_))
    ));

    let empty = query(serve("200 OK", Some(ORACLE_EMPTY)).await).await;
    let empty = OracleAnswer::from_outcome(&empty).expect("empty oracle answered");
    let candidate = query(serve("200 OK", Some(ORACLE_EMPTY)).await).await;
    assert!(matches!(
        classify(Engine::Ravel, &candidate, Some(&empty), &entry()),
        Err(ClassifyError::InvalidFixture(_))
    ));
}

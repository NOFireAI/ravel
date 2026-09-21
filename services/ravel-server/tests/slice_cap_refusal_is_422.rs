//! Issue #1687 part B, the status contract: a slice decode cap breach is a
//! refusal the caller can read, not a redacted outage they will retry.
//!
//! The coordinator's bounded slice decoder
//! (`ravel_query::distrib::SliceStreamDecoder`) refuses a slice that outruns
//! either of its two caps. Where that refusal lands matters more than that it
//! happens: every OTHER distributed slice failure is deliberately redacted to
//! `QueryError::Distrib` / `QueryError::Federation`, which render as a
//! retryable 503 with a fixed message, because they can leak a worker endpoint
//! or a store fault. A cap breach carries no server state -- both counts are
//! the coordinator's own -- and retrying it against the same remote cannot
//! succeed, so redacting it to a 503 would both hide the reason and invite the
//! one response that cannot help.
//!
//! The unit tests in `ravel-query` prove the decoder produces these two typed
//! errors and that the fan-out's error funnel keeps them in the budget class.
//! This test pins the other end: what a client actually receives. It asserts
//! against `ravel_query::http::QueryErrorResponse`, the single mapping every
//! HTTP query surface in the workspace renders through, and contrasts each cap
//! refusal with the redacted distributed failures it must not be confused with.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use axum::http::StatusCode;
use ravel_query::QueryError;
use ravel_query::http::QueryErrorResponse;

fn render(err: QueryError) -> QueryErrorResponse {
    QueryErrorResponse::from_query_error(err)
}

/// The frame cap's refusal: 422, the `execution` tag every budget refusal
/// carries, and both counts echoed so an operator can see what was asked for
/// and what the ceiling is.
#[test]
fn frame_cap_refusal_renders_as_422_with_both_counts() {
    let rendered = render(QueryError::TooManySliceFrames {
        frames: 1_048_577,
        max: 1_048_576,
    });
    assert_eq!(rendered.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(rendered.error_type, "execution");
    assert!(
        rendered.message.contains("1048577") && rendered.message.contains("1048576"),
        "both counts are echoed: {}",
        rendered.message
    );
}

/// The byte cap's refusal takes the same status and class, and its rendered
/// body names both figures AND which quantity they are.
///
/// It is deliberately not `TooManyBytesScanned`: that message says "query
/// scanned N bytes", meaning store bytes the query read, a figure an operator
/// can reconcile against `bytesReported` and the query's accounting. These are
/// one slice's protobuf frame bytes, which appear in neither. A reader given
/// the wrong noun has no way to tell the two apart, so the body is asserted to
/// carry the distinguishing words, not only the numbers.
#[test]
fn byte_cap_refusal_renders_as_422_naming_the_wire_bytes_and_the_cap() {
    let rendered = render(QueryError::TooManySliceBytes {
        bytes: 67_117_056,
        max: 67_108_864,
    });
    assert_eq!(rendered.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(rendered.error_type, "execution");
    assert!(
        rendered.message.contains("67117056") && rendered.message.contains("67108864"),
        "the bytes consumed and the cap are both echoed: {}",
        rendered.message
    );
    assert!(
        rendered.message.contains("wire bytes") && rendered.message.contains("per-slice"),
        "and the body says which quantity they are: {}",
        rendered.message
    );
}

/// The two cap refusals are distinguishable in the rendered body, not only in
/// the typed error: an operator reading a 422 can tell a frame-count breach
/// from a wire-byte breach without server access.
#[test]
fn the_two_cap_refusals_are_distinguishable_in_the_rendered_body() {
    let frames = render(QueryError::TooManySliceFrames {
        frames: 1_048_577,
        max: 1_048_576,
    });
    let bytes = render(QueryError::TooManySliceBytes {
        bytes: 67_117_056,
        max: 67_108_864,
    });
    assert_eq!(frames.status, bytes.status);
    assert_eq!(frames.error_type, bytes.error_type);
    assert_ne!(frames.message, bytes.message);
    assert!(
        frames.message.contains("response frames") && !frames.message.contains("wire bytes"),
        "the frame cap names frames: {}",
        frames.message
    );
    assert!(
        bytes.message.contains("wire bytes") && !bytes.message.contains("response frames,"),
        "the byte cap names wire bytes: {}",
        bytes.message
    );
}

/// The contrast that gives the two tests above their meaning: an ordinary
/// distributed or federated slice failure is a redacted 503. If a cap breach
/// were funnelled into either variant it would land here instead, losing both
/// its counts and its class.
#[test]
fn an_ordinary_slice_failure_is_still_a_redacted_503() {
    for err in [
        QueryError::Distrib {
            reason: "worker 10.0.0.7:7000 closed the stream".to_string(),
        },
        QueryError::Federation {
            cluster: "eu-west".to_string(),
            reason: "remote returned a malformed response".to_string(),
        },
    ] {
        let rendered = render(err);
        assert_eq!(
            rendered.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "a non-cap slice failure stays a retryable outage"
        );
        assert!(
            !rendered.message.contains("10.0.0.7") && !rendered.message.contains("eu-west"),
            "and stays redacted: {}",
            rendered.message
        );
    }
}

/// The service layer classifies a cap breach as `BudgetExceeded`, the ADR-1374
/// class a non-HTTP transport (the MCP adapter) maps from. A transport that
/// never sees a status code must still be able to tell a refusal from an
/// outage.
#[test]
fn cap_refusals_classify_as_budget_exceeded_for_non_http_transports() {
    use ravel_server::service::error::{ServiceError, ServiceErrorKind};

    for err in [
        QueryError::TooManySliceFrames { frames: 9, max: 8 },
        QueryError::TooManySliceBytes { bytes: 9, max: 8 },
    ] {
        assert_eq!(
            ServiceError::from_query(err).kind,
            ServiceErrorKind::BudgetExceeded
        );
    }
    assert_eq!(
        ServiceError::from_query(QueryError::Distrib {
            reason: "stream closed".to_string(),
        })
        .kind,
        ServiceErrorKind::Unavailable,
        "an ordinary slice failure is still an availability problem"
    );
}

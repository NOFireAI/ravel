//! `ravel_get_trace`: every span of one trace id.
//!
//! The body validates the arguments and hands the lookup to
//! [`QueryBackend::trace_lookup`], which runs the same statement and window
//! rule as `GET /api/traces/{trace_id}`. Without a `time_range` the lookup
//! reads every span the tenant retains; with one, the window is kept exactly.

use serde::Deserialize;
use serde_json::Value;

use crate::catalog::GetTraceInput;
use crate::envelope::{Envelope, Failure, FailureClass};
use crate::service::{QueryBackend, TraceLookupRequest};

use super::ToolContext;
use super::capabilities::refused;

/// Run the tool. An argument the body cannot use is an `invalid_argument`
/// envelope and reaches no store.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let request = match request(args) {
        Ok(request) => request,
        Err(failure) => return refused(failure, ctx),
    };
    match backend.trace_lookup(ctx.tenant_hash, &request).await {
        Ok(envelope) => envelope,
        Err(failure) => refused(failure, ctx),
    }
}

fn request(args: Value) -> Result<TraceLookupRequest, Failure> {
    let input = GetTraceInput::deserialize(args)
        .map_err(|err| invalid(format!("ravel_get_trace arguments: {err}")))?;
    if input.include_logs == Some(true) {
        return Err(Failure {
            class: FailureClass::Unsupported,
            message: "include_logs is not served by this build".to_string(),
            counter: None,
        });
    }
    if input.evidence_ref.is_some() {
        return Err(Failure {
            class: FailureClass::Unsupported,
            message: "evidence_ref is not served by ravel_get_trace in this build".to_string(),
            counter: None,
        });
    }
    let trace_id = parse_trace_id(&input.trace_id)?;
    let window = input
        .time_range
        .map(|range| {
            let start_ns = parse_ns("time_range.start_ns", &range.start_ns)?;
            let end_ns = parse_ns("time_range.end_ns", &range.end_ns)?;
            if start_ns > end_ns {
                return Err(invalid(
                    "time_range.start_ns must not be after time_range.end_ns".to_string(),
                ));
            }
            Ok(ravel_types::TimeRange { start_ns, end_ns })
        })
        .transpose()?;
    Ok(TraceLookupRequest { trace_id, window })
}

fn parse_trace_id(raw: &str) -> Result<[u8; 16], Failure> {
    let invalid_id = || invalid(format!("trace_id {raw:?} is not 32 hexadecimal characters"));
    let bytes = raw.as_bytes();
    if bytes.len() != 32 {
        return Err(invalid_id());
    }
    let mut id = [0u8; 16];
    for (out, pair) in id.iter_mut().zip(bytes.chunks_exact(2)) {
        let hi = char::from(pair[0]).to_digit(16).ok_or_else(invalid_id)?;
        let lo = char::from(pair[1]).to_digit(16).ok_or_else(invalid_id)?;
        *out = (hi << 4 | lo) as u8;
    }
    Ok(id)
}

fn parse_ns(name: &str, raw: &str) -> Result<i64, Failure> {
    raw.parse::<i64>()
        .map_err(|_| invalid(format!("{name} {raw:?} is not an integer nanosecond epoch")))
}

fn invalid(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_omitted_time_range_is_the_no_window_lookup() {
        let request = request(json!({"trace_id": "00112233445566778899AABBCCDDEEFF"}))
            .expect("valid arguments");
        assert_eq!(request.window, None);
        assert_eq!(request.trace_id[0], 0x00);
        assert_eq!(request.trace_id[10], 0xaa);
        assert_eq!(request.trace_id[15], 0xff);
    }

    #[test]
    fn a_given_time_range_is_kept_exactly() {
        let request = request(json!({
            "trace_id": "00112233445566778899aabbccddeeff",
            "time_range": {"start_ns": "7", "end_ns": "9"},
        }))
        .expect("valid arguments");
        assert_eq!(
            request.window,
            Some(ravel_types::TimeRange {
                start_ns: 7,
                end_ns: 9
            })
        );
    }

    #[test]
    fn unusable_arguments_are_refused_with_their_class() {
        for (args, class) in [
            (json!({}), FailureClass::InvalidArgument),
            (json!({"trace_id": "abc"}), FailureClass::InvalidArgument),
            (
                json!({"trace_id": "00112233445566778899aabbccddeefg"}),
                FailureClass::InvalidArgument,
            ),
            (
                json!({"trace_id": "+0112233445566778899aabbccddeeff"}),
                FailureClass::InvalidArgument,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "time_range": {"start_ns": "9", "end_ns": "7"},
                }),
                FailureClass::InvalidArgument,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "time_range": {"start_ns": "soon", "end_ns": "7"},
                }),
                FailureClass::InvalidArgument,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "include_logs": true,
                }),
                FailureClass::Unsupported,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "evidence_ref": "ev1",
                }),
                FailureClass::Unsupported,
            ),
        ] {
            let failure = request(args.clone()).expect_err("refused");
            assert_eq!(failure.class, class, "{args}");
        }
    }
}

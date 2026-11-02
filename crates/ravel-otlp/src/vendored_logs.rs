//! Bytes-retyped OTLP logs protos (ADR-3492 D3b, `proto/README.md`).
//!
//! The generated types mirror `opentelemetry_proto::tonic`'s module layout
//! field for field, except the six fields named in `proto/README.md` decode
//! as [`bytes::Bytes`] instead of `String`: a sub-slice of the request body,
//! refcounted rather than copied. UTF-8 validation on those fields moves to
//! [`validate_utf8`], with the same refusal upstream's `String` decode gives
//! today for invalid UTF-8.
//!
//! Reachability: by design, nothing outside this crate's tests and bench
//! reads these types yet; step 2 of #3654 wires both ingest handlers to them
//! if this step's bench shows the decode pays.

use bytes::Bytes;
use thiserror::Error;

pub mod opentelemetry {
    pub mod proto {
        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.collector.logs.v1.rs"
                    ));
                }
            }
        }
        pub mod logs {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.logs.v1.rs"));
            }
        }
        pub mod common {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.common.v1.rs"
                ));
            }
        }
        pub mod resource {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.resource.v1.rs"
                ));
            }
        }
    }
}

pub use opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
pub use opentelemetry::proto::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
pub use opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
pub use opentelemetry::proto::resource::v1::Resource;

/// A retyped field held bytes that are not valid UTF-8. Upstream's `String`
/// decode refuses the same bytes at decode time; this is the same refusal,
/// surfaced at validation instead.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("field {field} is not valid UTF-8")]
pub struct Utf8Error {
    /// The retyped field that held invalid UTF-8, named the same way as in
    /// `proto/README.md` (`Message.field`).
    pub field: &'static str,
}

fn check(field: &'static str, value: &Bytes) -> Result<(), Utf8Error> {
    std::str::from_utf8(value)
        .map(|_| ())
        .map_err(|_| Utf8Error { field })
}

/// Validates UTF-8 on every retyped field of a decoded
/// [`ExportLogsServiceRequest`], so the accepted set equals upstream's
/// `String`-typed decode.
pub fn validate_utf8(req: &ExportLogsServiceRequest) -> Result<(), Utf8Error> {
    for resource_logs in &req.resource_logs {
        if let Some(resource) = &resource_logs.resource {
            for kv in &resource.attributes {
                check_key_value(kv)?;
            }
        }
        for scope_logs in &resource_logs.scope_logs {
            if let Some(scope) = &scope_logs.scope {
                check("InstrumentationScope.name", &scope.name)?;
                check("InstrumentationScope.version", &scope.version)?;
                for kv in &scope.attributes {
                    check_key_value(kv)?;
                }
            }
            for record in &scope_logs.log_records {
                check("LogRecord.severity_text", &record.severity_text)?;
                check("LogRecord.event_name", &record.event_name)?;
                if let Some(body) = &record.body {
                    check_any_value(body)?;
                }
                for kv in &record.attributes {
                    check_key_value(kv)?;
                }
            }
        }
    }
    Ok(())
}

fn check_key_value(kv: &KeyValue) -> Result<(), Utf8Error> {
    check("KeyValue.key", &kv.key)?;
    if let Some(value) = &kv.value {
        check_any_value(value)?;
    }
    Ok(())
}

fn check_any_value(value: &AnyValue) -> Result<(), Utf8Error> {
    match &value.value {
        Some(any_value::Value::StringValue(s)) => check("AnyValue.string_value", s),
        Some(any_value::Value::ArrayValue(array)) => {
            for v in &array.values {
                check_any_value(v)?;
            }
            Ok(())
        }
        Some(any_value::Value::KvlistValue(kvlist)) => {
            for kv in &kvlist.values {
                check_key_value(kv)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

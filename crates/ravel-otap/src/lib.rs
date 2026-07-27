//! OTAP (OpenTelemetry Arrow Protocol) ingest.
//!
//! Implementer contract: docs/otap-ingest.md and ADR-0011. Vendored protocol
//! definitions live in proto/otel-arrow (pinned otel-arrow v0.50.0); the
//! decoder is written against docs/otap-spec.md and data_model.md there.

pub mod proto {
    pub mod experimental {
        pub mod arrow {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.experimental.arrow.v1.rs"
                ));
            }
        }
    }
}

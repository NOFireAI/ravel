# Vendored OpenTelemetry Arrow (OTAP) protocol definitions

Source: https://github.com/open-telemetry/otel-arrow at tag v0.50.0
(commit 5096a35), Apache-2.0 (LICENSE alongside).

Contents:
- opentelemetry/proto/experimental/arrow/v1/arrow_service.proto: the OTAP
  gRPC service and message definitions, compiled by crates/ravel-otap's
  build.rs via protox.
- docs/otap-spec.md, docs/data_model.md: the protocol and Arrow schema
  reference the decoder is written against.

Vendored so builds and fleet executors need no network access to GitHub.
Update by bumping the tag here and re-copying; treat protocol changes as
a compatibility event (differential tests against the pinned OTel
Collector exporter gate any bump).

# Vendored OTLP logs protos (ADR-3492 D3b)

These are not the frozen `proto/` at the repository root. They exist only to
let `build.rs` generate the bytes-retyped logs types this crate's decode
bench measures; the wire format and the set of accepted requests are
unchanged (same field numbers, same wire types).

## Source

The four files vendored here are the subset of the upstream
[open-telemetry/opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto)
protos that `ExportLogsServiceRequest` needs:

- `opentelemetry/proto/collector/logs/v1/logs_service.proto`
- `opentelemetry/proto/logs/v1/logs.proto`
- `opentelemetry/proto/common/v1/common.proto`
- `opentelemetry/proto/resource/v1/resource.proto`

Copied from the `opentelemetry-proto` crate version 0.32.0's own vendored
copy (`src/proto/opentelemetry-proto/opentelemetry/proto/...` in that
crate's source, located with `cargo metadata --format-version 1`), which
that crate's `CHANGELOG.md` records as generated from upstream
`opentelemetry-proto` release **v1.10.0**. `opentelemetry-proto` 0.32.0 is
the version already pinned in this workspace's `[workspace.dependencies]`
and used by `ravel-otlp`'s own normalize path.

## Layout

- `upstream/` -- the four files verbatim, unmodified copies of the crate's
  vendored source above.
- `retyped/` -- the same four files with exactly six fields changed from
  `string` to `bytes` (wire type 2 either way, so the wire format is
  unchanged): `KeyValue.key`, `AnyValue.string_value`,
  `LogRecord.severity_text`, `LogRecord.event_name`,
  `InstrumentationScope.name`, `InstrumentationScope.version`. Every
  package name, field number, and message name is unchanged. No other line
  differs from `upstream/`.

`build.rs` compiles only `retyped/`. `tests/vendored_logs_retype.rs` pins
that `retyped/` differs from `upstream/` in exactly those six places and
nowhere else.

## Why bytes

A `bytes`-typed field decodes through prost as a `bytes::Bytes` slice of
the request body (refcounted, no per-field allocation or copy) instead of
a freshly allocated, copied `String`. UTF-8 validation moves to the
consumer (`vendored_logs::validate_utf8` in this crate), with the same
refusal prost's `String` decode gives today for invalid UTF-8.

## Reachability

None by design: nothing in `services/` or this crate's normalize path uses
these types yet. Step 2 of #3654 wires both ingest handlers to them if
this step's bench shows the decode is cheaper.

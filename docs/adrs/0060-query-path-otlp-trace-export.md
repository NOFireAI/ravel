# ADR-0060: Query-path OTLP trace export

Status: Accepted

## Context

ADR-0044 decision 5 put `tracing` spans on the query path: `catalog_resolve`,
`segment_open`, `catalog_decode`, `page_fetch`, `decode`, `evaluate`, and the
three request-level spans `sql_query`, `analytics_query`, and
`flight_sql_statement`. `docs/guides/tracing.md` documents how to read them,
but only by widening `RUST_LOG` and reading the process's own stdout log
stream. `services/ravel-server/src/main.rs` and
`services/ravel-operator/src/main.rs` each install a bare
`tracing_subscriber::fmt()` subscriber with no further `Layer`; there is no
`Layer` composition anywhere in production code today (`registry().with(...)`
appears only in test code, `crates/ravel-query/tests/e2e.rs` and
`crates/ravel-query/src/fetcher.rs`'s `#[cfg(test)]` module). A span is
therefore local to whichever process emitted it and is gone the moment that
process's log line scrolls past, which is exactly the gap
`docs/guides/tracing.md`'s "Known gaps" section names: "There is no span
export to an OTLP trace backend."

The workspace already speaks OTLP, but only as a receiver. `Cargo.toml`
depends directly on `opentelemetry-proto 0.32` (`gen-tonic`, `metrics`,
`logs`, `trace` features) for `crates/ravel-otlp` and `crates/ravel-otap` to
decode telemetry a client sends Ravel. `opentelemetry 0.32.0` and
`opentelemetry_sdk 0.32.1` are present only transitively, pulled in by
`opentelemetry-proto`. No `opentelemetry-otlp` or `tracing-opentelemetry`
dependency exists. Turning Ravel into an OTLP trace *client* -- exporting its
own spans to a collector -- is new surface, not a wiring gap in existing
surface.

## Decision

### 1. A small shared crate, not per-binary duplication

`ravel-tracing-export` is a new crate holding the subscriber construction
that both `ravel-server` and `ravel-operator` need. `ravel-operator` does not
depend on `ravel-server` or `ravel-types` today, so there is no existing
shared crate to extend without adding a dependency edge that does not
otherwise exist. The crate exposes one entry point,
`ravel_tracing_export::init(filter: EnvFilter, otlp: Option<OtlpExportConfig>)`,
called once at process start in place of each binary's current
`tracing_subscriber::fmt()...init()`. With `otlp: None` it builds exactly
today's bare `fmt` subscriber -- same output, same behavior, zero new
objects constructed, zero new dependency code executed. With `otlp: Some(_)`
it builds a `Registry` with two layers: the existing `fmt` layer, and a
`tracing-opentelemetry` `OpenTelemetryLayer` wired to a
`BatchSpanProcessor` over an OTLP/gRPC exporter (`opentelemetry-otlp`,
`grpc-tonic` feature, matching the crate's own existing `tonic` dependency
rather than adding a second HTTP client stack for this one path).

### 2. One filter, both layers

The OTLP layer is filtered by the same `EnvFilter` the `fmt` layer already
uses, not a second independent level knob. `docs/guides/tracing.md` already
teaches operators to set `RUST_LOG=info,ravel_catalog=debug,ravel_query=debug`
to see the phase spans; whatever that filter admits to the log stream is
exactly what OTLP export sends, no separate configuration surface to learn.

### 3. Off by default, one flag, per binary

Each binary gets its own `--otlp-trace-endpoint <URL>` flag (`Option<String>`,
absent by default), following the value-valued opt-in pattern
`--limits-file` already uses in `services/ravel-server/src/config.rs`, not a
bare boolean: the endpoint itself is the configuration. `ravel-server` and
`ravel-operator` are separately deployed processes and each already carries
its own CLI surface, so each gets its own flag rather than a shared config
file -- consistent with how `--metrics-tenant-labels` (ADR-0051 section 6)
is per-binary today. When the flag is absent, `init()` is called with
`otlp: None` and the binary's behavior, dependency footprint at runtime, and
log output are byte-identical to before this ADR.

### 4. No new span fields, no new disclosure

The layer exports exactly the fields already recorded on each span --
`tenant_hash`, `s3_requests`, `s3_bytes`, `segments_pruned`,
`decompressed_bytes`, and the rest of ADR-0044 section 4's allowlist. OTLP
export adds a transport, not new content; nothing crosses this boundary that
was not already permitted onto the `debug`-level log stream that
`docs/guides/tracing.md` already documents as readable by anyone who can set
`RUST_LOG` on the process.

### 5. Resource attributes

Each exported span carries a `service.name` resource attribute (`ravel-server`
or `ravel-operator`) and a `ravel.mode` attribute mirroring the `mode` label
`/metrics` already renders (`all`, `gateway`, `query`, `maintain`) where the
binary has a `--mode` flag, so spans from a fleet of processes are
distinguishable in a collector the same way `/metrics` scrapes already are.

### 6. Export is best-effort and never blocks a query

The `BatchSpanProcessor` buffers and exports off the query's own task; a
slow, unreachable, or misconfigured collector drops spans (the OTel SDK's
own default behavior) and never adds latency or a failure mode to a query,
an ingest write, or a `/metrics` scrape. This is a debugging aid, not a
durability guarantee -- it does not touch this repository's "no durability
may depend on local disk" invariant, because no query result or commit
record depends on whether a span made it to a collector.

### 7. Flush on clean shutdown

Both binaries already have a graceful-shutdown path (`SIGTERM` handling
around the `axum`/`tonic` servers). `init()` returns a guard whose `Drop`
(or an explicit call on the shutdown path, whichever the implementing task
finds fits each binary's existing shutdown sequence more cleanly) calls the
tracer provider's `shutdown()`, which flushes buffered spans before the
process exits. A `SIGKILL` still loses whatever was buffered and not yet
exported, the same loss profile the log stream already has for buffered
stdout.

## Rejected alternatives

1. **Wire OTLP export directly into each binary's `main.rs`, no shared
   crate.** Rejected: the construction (exporter, batch processor, resource
   attributes, shutdown flush) is real code, not three lines, and two
   binaries need it identically. Duplicating it invites the two copies
   drifting exactly the way `docs/guides/tracing.md` already found one
   drift bug in this area (`log_fetcher.rs`'s `page_fetch`/`decode` spans
   carry a different field set than the metric path's, undocumented) --
   flagged separately, not part of this ADR, since it predates this
   decision and is a doc-currency gap, not an export design question.
2. **A separate log-level or sampling knob for OTLP export, independent of
   `RUST_LOG`.** Rejected for v1: it is a second thing to learn on top of
   the filter `docs/guides/tracing.md` already teaches, for no benefit this
   repository has a concrete need for yet. If a real deployment needs
   different verbosity for its collector than for its own stdout, that is
   its own follow-up with a real requirement behind it, not a knob added on
   spec.
3. **A boolean `--otlp-trace-export` flag plus a separate `--otlp-endpoint`
   value flag.** Rejected: two flags that must agree is a state the config
   validator has to reject when they disagree (`cli.validate()`, the
   `ADR-0050 section 1` pattern this repo already uses for other
   value-valued opt-in flags), for no benefit over one optional value flag
   where absence already means disabled.
4. **OTLP/HTTP instead of OTLP/gRPC.** Rejected: the workspace already
   depends on `tonic` directly (Flight SQL); `opentelemetry-otlp`'s
   `grpc-tonic` feature reuses it. OTLP/HTTP would pull in a second HTTP
   client stack (`reqwest` or `hyper` directly through the exporter crate)
   for a redundant transport.
5. **Synchronous, blocking span export (an exporter that awaits the
   collector before the span's owning task continues).** Rejected outright:
   it would couple query latency to collector reachability, which no
   observability feature in this repository is permitted to do to the
   query or ingest path it is observing.

## Consequences

- Two new direct dependencies: `opentelemetry-otlp` and
  `tracing-opentelemetry`, both pinned to the `0.32` line already fixed by
  the existing `opentelemetry-proto` dependency. The implementing task
  confirms exact version compatibility against the workspace's pinned
  `tonic` version and adjusts feature flags if the default `grpc-tonic`
  feature set pulls in an incompatible `tonic` requirement; any adjustment
  is reported, not silently absorbed.
- A new crate, `ravel-tracing-export`, owning subscriber construction for
  both service binaries. Both `main.rs` files change to call its `init()`
  instead of building `tracing_subscriber::fmt()` inline.
- Two new CLI flags, one per binary: `--otlp-trace-endpoint`.
- No frozen format touched. No persistent object, commit record, manifest,
  or index object changes. No change to any query result, ingest
  acknowledgement, or `/metrics` sample.
- Default behavior (flag absent) is provably unchanged: `init()` with
  `otlp: None` builds exactly the subscriber both binaries build today, so
  every existing test and every operator relying on today's log output sees
  no difference.
- Known pre-existing doc gap surfaced during this ADR's research, not fixed
  here: `crates/ravel-query/src/log_fetcher.rs` emits `page_fetch` and
  `decode` spans with a `signal = "logs"` field instead of the metric path's
  `page_kind`/`series_count` fields, and `docs/guides/tracing.md`'s span
  table does not mention this divergence. Reported separately; not part of
  this ADR's scope.

# Query-path tracing

Ravel instruments the read path with `tracing` spans so a slow query can be
attributed to a phase. Each crate opens a span around the work it owns, and
every span carries the same bounded fields the `/metrics` label allowlist
permits (a tenant hash and per-span byte and request counts), never a query
text, a metric name, a label value, or an object key. This is the design of
[ADR-0044](../adrs/0044-query-cost-accounting.md) section 5, shipped by issues
#642 and #643.

## This guide and the observability guide

This guide covers the query-path spans: which spans exist, what each records,
how to turn them on, and how to read them to place a slow query's time in a
phase. Spans answer "where did the time go" for one request.

The [observability guide](observability.md) is the catalog of `GET /metrics`.
Metrics answer "how much" in aggregate across the process: request counts,
byte counts, cache outcomes, error kinds, and the per-query cost estimate
against the actual. It does not carry per-request timing. Read it to
understand a sample on the route; read this guide to attribute one query.

## The spans

There are two tiers. Request-level spans wrap a whole query and are created at
`info` level, so they appear under the default log filter. Phase spans wrap
one stage of the read path and are created at `debug` level, so they are off
until you widen the filter (see [Turning them on](#turning-them-on)).

### Request-level spans (info)

Each transport opens one span for the whole request and records the query's
final store-request and byte counts once it finishes.

| Span | Where | Fields |
|---|---|---|
| `sql_query` | `services/ravel-server/src/sql.rs` | `tenant_hash`, `workload_class`, `s3_requests`, `s3_bytes` |
| `analytics_query` | `services/ravel-server/src/analytics.rs` | `tenant_hash`, `workload_class`, `s3_requests`, `s3_bytes` |
| `flight_sql_statement` | `crates/ravel-sql/src/flight/service.rs` | `tenant_hash`, `workload_class`, `s3_requests`, `s3_bytes` |

`workload_class` is the literal `interactive` on all three: every query over
these transports is client-driven. `s3_requests` and `s3_bytes` start empty
and are recorded from the query's accounting handle when it returns, so they
are the whole query's authoritative totals, the same numbers the response body
and `/metrics` are fed from.

### Phase spans (debug)

Six span names cover the read-path phases. `page_fetch`, `decode`, and
`evaluate` each have two callsites (a scalar and a histogram variant, an
instant and a range variant); the span name is the same at both.

| Span | Where | Fields |
|---|---|---|
| `catalog_resolve` | `crates/ravel-catalog/src/catalog.rs` | `tenant_hash`, `s3_requests`, `s3_bytes`, `segments_pruned` |
| `segment_open` | `crates/ravel-query/src/fetcher.rs` | `tenant_hash`, `object_size`, `s3_requests`, `s3_bytes` |
| `catalog_decode` | `crates/ravel-query/src/fetcher.rs` | `matcher_count`, `total_size`, `series_matched` |
| `page_fetch` | `crates/ravel-query/src/fetcher.rs` | `page_kind`, `series_count`, `s3_requests`, `s3_bytes` |
| `decode` | `crates/ravel-query/src/fetcher.rs` | `page_kind`, `series_count`, `decompressed_bytes` |
| `evaluate` | `crates/ravel-query/src/engine.rs` | `eval_kind` |

Field notes, all verified against the fields the code records:

- `catalog_resolve` records `s3_requests`, `s3_bytes`, and `segments_pruned`
  as the delta this resolve alone added to the query's accounting, not the
  whole query's total. A query fetches segments after resolving on the same
  handle, so the resolve span's counts are just the LIST/GET fan-out cost of
  finding the segments.
- `segment_open` records `object_size` (the segment's size) at open, and
  `s3_requests`/`s3_bytes` as that one segment's own GET cost. Concurrent
  segment opens do not fold into each other's counts.
- `catalog_decode` records `matcher_count` and `total_size` at open and
  `series_matched` once the decode finds its matching series.
- `page_fetch` and `decode` carry `page_kind` (`scalar` or `histogram`) and
  `series_count`. `page_fetch` records the GET cost of pulling pages;
  `decode` records `decompressed_bytes`, the uncompressed size it produced.
- `evaluate` carries `eval_kind` (`instant` or `range`) and no counts; it is
  pure in-memory evaluation over already-fetched data, so its cost is time,
  not bytes.

## Turning them on

The request-level spans are `info`, so they are visible under the default
filter. Both `services/ravel-server/src/main.rs` and
`services/ravel-operator/src/main.rs` fall back to `EnvFilter::new("info")`
when `RUST_LOG` is unset, and the server installs a `tracing_subscriber::fmt`
subscriber.

The phase spans are `debug`. They live in two crates: `catalog_resolve` in
`ravel-catalog`, and `segment_open`, `catalog_decode`, `page_fetch`, `decode`,
and `evaluate` in `ravel-query`. To see all six while keeping the request-level
spans visible, set:

```sh
RUST_LOG=info,ravel_catalog=debug,ravel_query=debug
```

The leading `info` matters. `EnvFilter` only applies its fallback level when
`RUST_LOG` is unset; once you set it, targets you do not name drop to the
implicit `error` default, which would hide the `info`-level request spans in
`ravel_server` and `ravel_sql`. The `info,` prefix keeps them on while the two
`=debug` directives add the phase spans. No `ravel_sql=debug` is needed:
`flight_sql_statement` is an `info` span, and no query-path phase span lives in
`ravel-sql`.

## Attributing a slow query to a phase

The concrete question (issue #638) is: a query is slow, which phase owns the
time? Read the phase spans nested under the request span for that query. Two
signals combine.

- The `s3_requests` and `s3_bytes` fields tell you where the store cost went.
  If `catalog_resolve` dominates, the LIST/GET fan-out to find segments is the
  cost; a large `segments_pruned` next to a small byte count means the resolve
  did its job and the cost is elsewhere. If `segment_open` and `page_fetch`
  dominate, the query is I/O-bound on segment reads. If `decode`'s
  `decompressed_bytes` is large but its store cost is zero, the data was
  already cached and the cost is CPU decompression.
- `evaluate` carries no counts. Time spent there with small fetch counts means
  the query is evaluation-bound, not store-bound.

### The acceptance test as a runnable illustration

The end-to-end test that proves all six phase spans fire on a real query is a
runnable illustration of the span set and its fields:

```sh
cargo test -p ravel-query --test e2e instant_query_emits_all_six_phase_spans -- --nocapture
```

`instant_query_emits_all_six_phase_spans` runs an instant query through the
full HTTP handler and asserts that `catalog_resolve`, `segment_open`,
`catalog_decode`, `page_fetch`, `decode`, and `evaluate` each fire at least
once. It captures spans with a custom `tracing` layer rather than printing
them, so it verifies the phase set exists; it does not dump durations to the
console.

The per-span byte accounting that makes phase attribution meaningful is proven
by a second test in the same file:

```sh
cargo test -p ravel-query --test e2e segment_open_span_bytes_are_per_segment_not_the_shared_query_total -- --nocapture
```

It opens three segments concurrently and asserts each `segment_open` span
records only its own segment's GET bytes, and that the per-segment bytes sum to
no more than the query's authoritative total. This is why a `segment_open`
span's `s3_bytes` can be trusted to attribute one segment's I/O rather than the
whole query's.

## OTLP trace export

By default the spans this guide documents stay on the process's own log stream,
readable only by whoever can watch that process's stdout. Export is an opt-in
way to also ship those same spans to an OTLP collector, so spans from a fleet of
processes land in one place and outlive any single process's log buffer. It is
an addition, not a replacement: the local log stream behaves exactly as before,
and export sends the same spans in parallel.

### Turning it on

Both `ravel-server` and `ravel-operator` take a `--otlp-trace-endpoint <URL>`
flag, absent by default. Point it at a collector's OTLP/gRPC endpoint (for
example `http://otel-collector:4317`) to enable export for that process. The two
binaries are configured independently, each with its own flag rather than a
shared config file, because they are separately deployed processes that each
already carry their own CLI surface
([ADR-0060](../adrs/0060-query-path-otlp-trace-export.md) decision 3). Set the
flag on each process you want exporting.

There is no second verbosity knob. The OTLP layer is gated by the same
`EnvFilter` as the log stream ([ADR-0060](../adrs/0060-query-path-otlp-trace-export.md)
decision 2), so the `RUST_LOG` filter [Turning them on](#turning-them-on)
already teaches is exactly what export ships: whatever that filter admits to the
log stream is what reaches the collector. Widen `RUST_LOG` to add phase spans to
the exported stream the same way you would to see them locally.

### What gets exported

Exactly the spans and fields the [span tables above](#the-spans) already
document, and nothing more. Export adds a transport, not new content: no query
text, no metric or label values, no object keys
([ADR-0060](../adrs/0060-query-path-otlp-trace-export.md) decision 4). Nothing
crosses to the collector that was not already on the `debug`-level log stream.

Each exported span carries two resource attributes:

- `service.name`: `ravel-server` or `ravel-operator`, the binary that emitted
  the span.
- `ravel.mode`: for `ravel-server`, the same value its `/metrics` `mode` label
  renders (`all`, `gateway`, `query`, or `maintain`), derived from the process's
  `--mode`. `ravel-operator` has no mode selection and always reports the fixed
  literal `operator`.

Together they distinguish spans from a fleet in the collector the same way
`/metrics` scrapes are distinguished today.

### Best-effort, never blocking

Export is best-effort. A down, slow, or unreachable collector drops spans and
never blocks a query, an ingest write, or a `/metrics` scrape, and never
surfaces an error to the caller
([ADR-0060](../adrs/0060-query-path-otlp-trace-export.md) decision 6); the ADR
covers the batch-processor mechanism behind that guarantee.

One limitation to know (issue #711, open): a well-formed but unreachable or
wrong-collector endpoint currently exports nothing and prints no warning. Only a
malformed URL, which fails the exporter build at startup, produces an "OTLP
trace export disabled" warning; a syntactically valid URL is dialed lazily in
the background, so a wrong host or a down collector is silent. Until #711 adds a
reachability signal, confirm export is working by checking the collector, not
the process log.

## Known gaps

- The `fmt` subscriber the server installs does not emit per-span
  enter/close lines with wall-clock durations by default; span fields surface
  as context on events emitted within a span. Reading raw phase durations off
  a running process requires a subscriber configured to emit span-close
  events. The programmatic capture in the acceptance test is the supported way
  to observe the span set and its recorded count fields today.

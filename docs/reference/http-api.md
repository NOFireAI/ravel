# Ravel HTTP API reference

Every HTTP route `ravel-server` registers, derived from the router rather than
from any guide. A reader who knows which call they want comes here for its exact
shape: the method, what it accepts, what it returns, the status codes, whether a
tenant credential is required, which process modes serve it, and its cargo
feature gate where it has one.

This is a reference, not a tutorial. The mental model behind the vocabulary used
below (tenant, signal, segment, commit token, fold, snapshot) is in
[concepts](../concepts.md), and the normative guarantees for acknowledgement and
visibility are in [the consistency model](../consistency-model.md). Server flags
that change any behavior described here are in
[the server flag reference](ravel-server-flags.md).

## Listeners and authentication

`ravel-server` serves HTTP on one listener (`--listen-http`, default
`127.0.0.1:4318`). OTLP ingest for metrics, logs, and traces is also served
over gRPC on a second listener (`--listen-grpc`); this page documents the HTTP
surface only.

A tenant-scoped route resolves the request to a tenant before it does any work.
The default resolver is a static bearer-token map (`--tenant-token`); a
deployment may instead resolve the tenant from an OIDC token or a
proxy-forwarded mTLS identity. Whichever resolver is configured, a request that
carries no resolvable credential to a tenant-scoped route is rejected with 401
before any object-store access, so a 401 guarantees nothing was written or read.

The health and `/metrics` routes carry no tenant identity and require no
credential. They are served in every mode, including maintain mode, whose router
is otherwise empty.

Where a dedicated mutual-TLS listener is configured (`--mtls-listener`), it
serves the same ingest and query routes with the mTLS resolver, and
deliberately serves neither the health routes nor `/metrics`.

## Ingest routes

Served in `all` and `gateway` modes. Every ingest route is strict-acknowledgement
by default: a 2xx means the data object and its commit record are durably
stored. On a strict acknowledgement the OTLP responses carry an
`x-ravel-commit-token` header, a comma-separated token per shard the request
flushed through, which a later query replays as `min_commit_token` for
read-your-write. Buffered acknowledgement is opt-in per request, with the
`x-ravel-ingest-mode: buffered` header, on the OTLP routes only.

The request body limit is 16 MiB on the wire and 64 MiB after decompression, on
every ingest route. OTLP HTTP accepts an absent, identity, or single `gzip`
(`x-gzip`) `Content-Encoding`; a chained or unknown encoding is 415. Remote
Write requires Snappy compression rather than accepting it optionally, and is
strict-acknowledgement only: a buffered-mode header on a Remote Write request is
ignored.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| POST | `/v1/metrics` | OTLP `ExportMetricsServiceRequest` protobuf, gzip optional | OTLP `ExportMetricsServiceResponse` protobuf; `x-ravel-commit-token` on strict ack | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |
| POST | `/v1/logs` | OTLP `ExportLogsServiceRequest` protobuf, gzip optional | OTLP `ExportLogsServiceResponse` protobuf; `x-ravel-commit-token` on strict ack | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |
| POST | `/v1/traces` | OTLP `ExportTraceServiceRequest` protobuf, gzip optional | OTLP `ExportTraceServiceResponse` protobuf; `x-ravel-commit-token` on strict ack | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |
| POST | `/api/v1/write` | Prometheus Remote Write 1.0 or 2.0 protobuf, Snappy-compressed | Empty body; the `x-prometheus-remote-write-*-written` count headers | 200, 400, 401, 413, 415, 429, 500, 503 | Yes | `all`, `gateway` | none |

A 429 carries a `Retry-After` header and sheds the request without buffering it.
Remote Write returns 415 when the content-type or version header names neither
1.0 nor 2.0. An active-series cap breach on Remote Write reduces the written
count inside a 200 rather than returning 429; 429 there is reserved for rate
limits.

## Query routes

Served in `all` and `query` modes. Every query route resolves one immutable
snapshot and answers from it, so commits, compactions, and deletions that land
mid-query cannot change the answer. A caller that holds commit tokens passes them
back as repeated `min_commit_token` values for read-your-write.

The Prometheus-shaped routes take form-encoded parameters (on GET, the query
string; on POST, the body). `/api/v1/sql` and `/api/v1/analytics` take a JSON
body instead, because SQL text and analytic parameters are awkward to
percent-encode.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET, POST | `/api/v1/query` | `query`, `time`, `timeout`, `min_commit_token` | Prometheus instant-query JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET, POST | `/api/v1/query_range` | `query`, `start`, `end`, `step`, `timeout`, `min_commit_token` | Prometheus range-query JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET | `/api/v1/labels` | `start`, `end`, `match[]` | Prometheus label-names JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET | `/api/v1/label/{name}/values` | path `name`, plus `start`, `end`, `match[]` | Prometheus label-values JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET, POST | `/api/v1/series` | `match[]`, `start`, `end` | Prometheus series JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| GET | `/api/v1/status/buildinfo` | none | Prometheus build-info JSON; Ravel's own version, empty `goVersion` | 200 | No | `all`, `query` | none |
| GET | `/api/v1/metadata` | `metric`, `limit` | Prometheus metric-metadata JSON; an empty object when no tenant resolves | 200 | Optional | `all`, `query` | none |
| GET, POST | `/api/v1/query_exemplars` | `query`, `start`, `end` | Prometheus exemplars JSON envelope | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| POST | `/api/v1/analytics` | JSON: `query`, `start`, `end`, `step`, `op`, optional `timeout`, `min_commit_token`, `allow_partial` | JSON analytics envelope, one entry per series | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | none |
| POST | `/api/v1/sql` | JSON: `query`, `start`, `end`, optional `timeout`, `min_commit_token` | Arrow IPC stream or JSON, per `Accept` | 200, 400, 401, 422, 500, 503, 504 | Yes | `all`, `query` | `sql` |
| POST | `/mcp` | One MCP JSON-RPC message (revision `2026-07-28` or `2025-11-25`) | JSON or `text/event-stream`, per revision; a tool result carries the result envelope | 200, 400, 401, 403, 405, 413, 500 | Yes | `all`, `query` | `mcp` |

`/api/v1/status/buildinfo` and `/api/v1/metadata` exist for Prometheus-shaped
clients (Grafana's datasource test probes both). `/api/v1/metadata` never
returns 401: a request with no resolvable tenant receives an empty object rather
than an error, and reads no object storage on that path.

`/api/v1/analytics` accepts exactly two operations, selected by the `op` type
field: change point detection (`change_point`) and summary statistics
(`summary`). An unknown `op`, a missing field, or a malformed body is 400; an
analytics computation error or the per-call series cap is 422; partial federated
coverage without `allow_partial: true` is 503. `/api/v1/analytics` and
`/api/v1/query_exemplars` take a permit from the same query concurrency
ceiling as the other query routes, and a request refused by it gets the same
503 body those routes return.

`/api/v1/sql` is behind the `sql` cargo feature. The published server image
builds that feature, so the route is available there. Its response envelope is

```json
{"status": "success",
 "data": {"columns": [{"name": "ts", "type": "..."}], "rows": [[...]]},
 "stats": {}}
```

with one array per row under `data.rows`. A non-finite float comes back as a
string: `NaN`, `+Inf`, and `-Inf`. Sending `Accept:
application/vnd.apache.arrow.stream` yields an Arrow IPC stream instead, which is
bit-exact for every float. The SQL surface registers exactly five tables, one
per signal: `samples` (metrics), `logs`, `spans` (traces), `alerts` (alert
state transitions), and `audit` (audit records, including the query-audit
trail).

`/mcp` is behind the `mcp` cargo feature and `--mcp`: a build carrying the
feature serves no MCP route until an operator passes the flag. It is the only
MCP path, and it serves POST only. Every request is authenticated with the
listener's own tenant resolver before the JSON-RPC message is parsed, so a
request with no resolvable credential is 401 whatever it asked for, including
a legacy `initialize`. An `Origin` outside `--mcp-allowed-origins` is 403, a
body past `--mcp-max-body-bytes` is 413, and on revision `2026-07-28` a
`Mcp-Method` or `Mcp-Name` header that disagrees with the body is 400.

For the query routes, the status codes come from one shared error mapping:

- 400 `bad_data`: a malformed query, a bad time range, or a non-positive step.
- 422 `execution`: a resource-budget refusal (too many segments, series,
  samples, or scanned bytes; an over-wide window), or an unsupported construct.
- 500 `internal`: a permanent data-integrity fault in already-stored objects (a
  corrupt segment, an unreconstructable commit record, a non-monotonic run). It
  is not retryable, and its message is fixed so no object key or tenant hash
  leaks.
- 503 `unavailable`: a transient storage fault, an invalidated snapshot, or an
  unsatisfiable `min_commit_token`. Retryable.
- 504 `timeout`: the query passed its deadline.
- 401 `unauthorized`: no resolvable credential.

## Maintenance route

Served in `all` and `query` modes, alongside the query surface, because it shares
the same catalog and folder identity as the scheduled fold.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| POST | `/api/v1/admin/fold` | JSON: `signal`, optional `tenant` | JSON naming the fold outcome | 200, 400, 401, 403, 503 | Yes | `all`, `query` | none |

`/api/v1/admin/fold` triggers a catalog fold for one tenant and one signal, the
on-demand form of the background fold. It is authorized by the same tenant
credential the query routes require; when the body names a tenant, that name must
hash to the authenticated tenant or the request is 403. A fold reveals no data
and destroys none: it rewrites a query-cost index the tenant already owns.

The call returns 200 with one of four named outcomes:

- `published`: this call wrote a new snapshot and advanced HEAD.
- `nothing_eligible`: no commit was eligible. An ingest hour seals only after the
  maximum flush lifetime plus a clock-skew allowance plus a fold safety margin
  has elapsed, so a fold run right after a load seals nothing. That is the rule
  working, which is why it is a distinct status.
- `lost_cas`: a concurrent fold won the HEAD compare-and-swap. The catalog is
  fine and the winner's snapshot is published; this call did not publish one.
- `throttled`: the rate gate declined because HEAD was published more recently
  than the fold interval. No listing ran, so this call makes no eligibility
  claim.

A 503 on this route means the outcome is unknown and the call should be retried,
never that nothing was written.

## Health and metrics routes

Unauthenticated, and served in every mode, including maintain mode.

| Method | Path | Request | Response | Status codes | Credential | Modes | Feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GET | `/healthz` | none | `ok` | 200 | No | all modes | none |
| GET | `/readyz` | none | empty body | 200, 503 | No | all modes | none |
| GET | `/-/healthy` | none | `ok` | 200 | No | all modes | none |
| GET | `/-/ready` | none | empty body | 200, 503 | No | all modes | none |
| GET | `/metrics` | none | Prometheus text exposition | 200 | No | all modes | none |

`/healthz` (and its Prometheus spelling `/-/healthy`) is liveness: reaching the
handler proves the server task can route, so it is 200 whenever it answers, and a
store outage never makes it fail.

`/readyz` (and `/-/ready`) is readiness, the AND of four conditions: startup has
completed (config parsed, the object-store capability gate passed, listeners
bound), the process is not draining, the store is reachable, and no ingest shard
actor has been condemned after exhausting its respawn budget. It issues no
object-store request itself and takes no lock: each condition is an atomic load,
including the ingest one, which reads the condemned-shard counter. A background
store probe with hysteresis supplies the store atomic: four consecutive failed
probes flip readiness to 503, and a single success recovers it.

Only the store condition recovers on its own. The startup latch and the drain
latch are one-way, and a condemned ingest shard cannot recover in-process, so a
503 from that cause persists until the process is rolled. Readiness never
restarts the process: a 503 sheds traffic (Kubernetes removes the pod from its
Service endpoints), which is why liveness is the separate `/healthz` route.

`/metrics` is the Prometheus scrape endpoint. It is unauthenticated, so
per-tenant labels on the admission and query families are opt-in
(`--metrics-tenant-labels`); by default every tenant folds into a single
`tenant_hash="other"` series.

## Not on this HTTP surface

- Flight SQL is behind the `flight-sql` cargo feature and is a gRPC service on
  the gRPC listener, not an HTTP route. The published image builds that feature.
- OTAP (OpenTelemetry Arrow Protocol) metrics ingest is a gRPC service that
  needs both the `otap` cargo feature and the `--otap` runtime flag. The
  published image builds the feature; the flag is still required.
- OTLP ingest for metrics, logs, and traces is additionally available over
  gRPC on the gRPC listener; this page documents the HTTP form.

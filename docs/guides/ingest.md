# Ingest

## Endpoints

`ravel-server` accepts OTLP metrics and logs on two transports, both bound
by `--listen-http` (default `127.0.0.1:4318`) and `--listen-grpc` (default
`127.0.0.1:4317`):

- `POST /v1/metrics` over HTTP, body is a binary-encoded
  `ExportMetricsServiceRequest` (`Content-Type: application/x-protobuf`).
- `opentelemetry.proto.collector.metrics.v1.MetricsService/Export` over
  gRPC.
- `POST /v1/logs` over HTTP, body is a binary-encoded
  `ExportLogsServiceRequest` (`Content-Type: application/x-protobuf`).
  Responds with a binary `ExportLogsServiceResponse`.
- `opentelemetry.proto.collector.logs.v1.LogsService/Export` over gRPC.

All four are only present when `ravel-server` runs in `--mode all` (the
default) or `--mode gateway`; `--mode query` starts none of them.

Authentication, the strict/buffered mode header, the commit-token
header, and the status-code mapping are identical on all four: a log export
is a metrics export with a different payload and a different keyspace
underneath. Traces and profiles are not accepted on any transport yet.

## Authentication

Every request must resolve to a tenant or it is rejected with
`401 Unauthorized`. The default (and only always-on) resolver is a static
bearer-token map, built from one or more `--tenant-token TOKEN=TENANT`
flags:

```sh
ravel-server --tenant-token devtoken=acme --tenant-token other=other-co ...
```

A request must send `Authorization: Bearer devtoken` to be resolved as
tenant `acme`. There is no default tenant and no anonymous access; an
unauthenticated deployment only happens if you never pass `--tenant-token`,
which is a conscious choice, not an oversight.

`--dev-insecure-tenant-header` adds a second resolver, tried only if the
bearer lookup fails: it reads the tenant name directly from an
`x-ravel-tenant` request header, no token needed. `ravel-server` refuses to
start with this flag set unless `--listen-http` binds a loopback address
(`127.0.0.1` or `::1`). It exists for local development against a
loopback-only server, not for any deployment reachable over a network.

## Strict vs. buffered acknowledgement

Every write has a mode, strict by default:

- **Strict**: the HTTP or gRPC call does not return until every shard your
  points landed in has flushed: its segment object and commit record are
  both durably written to the object store. The response carries a commit
  token for each shard that flushed
  (`x-ravel-commit-token` over HTTP, comma-separated if more than one
  shard). Once you have that ack, the data survives the crash of any Ravel
  process, because it survives anything the object store survives
  ([docs/consistency-model.md](../consistency-model.md)).
- **Buffered**: the call returns as soon as the request is validated and
  enqueued into its shard's in-memory buffer, before any flush. This is
  lower latency but not durable: a crash between the ack and the next flush
  loses that buffered window, bounded by `max_flush_delay` (500ms default).
  No commit token is issued, because there is nothing yet to point one at.

Send `x-ravel-ingest-mode: buffered` on an HTTP export to use buffered mode
for that request; omit it, or send any other value, for strict. As shipped
today, this header is honored for any tenant with no additional gate: the
ingest design doc describes gating buffered mode per tenant config, but
that gate is not implemented yet.

## Partial success and rejections

A single `ExportMetricsServiceRequest` can contain a mix of good and bad
data points. Ravel never silently drops a point: every rejected point (or
group of points) is counted and returned in the OTLP
`ExportMetricsPartialSuccess` message, with `rejected_data_points` and a
combined `error_message`. The points that were admitted are still
ingested and acknowledged normally.

Every rejection reason ([crates/ravel-otlp/src/limits.rs](../../crates/ravel-otlp/src/limits.rs)):

| Rejection | Meaning |
|---|---|
| `TooManyDataPoints` | Whole request exceeds `max_data_points_per_request`; nothing in the request is admitted. |
| `TooManyResourceAttributes` | A `Resource` has more attributes than `max_resource_attributes`; every point under it is rejected. |
| `MetricNameTooLong` | Metric name (before sanitization) exceeds `max_metric_name_len`; every point on that metric is rejected. |
| `EmptyMetricName` | Metric name sanitizes to empty; every point on that metric is rejected. |
| `TooManyAttributes` | One data point has more attributes than `max_attributes_per_point`. |
| `LabelNameTooLong` | A label name (after sanitization) exceeds `max_label_name_len`. Also applies to the synthesized `job` label after its `namespace/name` join. |
| `LabelValueTooLong` | A label value exceeds `max_label_value_len`. |
| `DuplicateLabelName` | Two attributes sanitize to the same label name (or a data-point attribute collides with a synthesized `job`/`instance` label). |
| `ComplexAttributeValue` | An attribute value is an array, kvlist, or bytes value, which has no label representation. For a resource attribute, this rejects every point under that resource. |
| `MissingValue` | The data point has neither an int nor a double value set. |
| `UnsupportedMetricType` | The metric is a Histogram, ExponentialHistogram, or Summary. Only Gauge and cumulative Sum are supported in Phase 1; the whole metric's points are rejected, never dropped as if they didn't exist. |
| `UnsupportedTemporality` | A Sum metric has delta (or unspecified) temporality. Only cumulative sums are accepted. |
| `ZeroTimestamp` | The data point's event timestamp is zero. |
| `FutureSkew` | Event timestamp is ahead of ingest time by more than `max_future_skew_ns`. |
| `TooOld` | Event timestamp is behind ingest time by more than `max_ingest_lag_ns`. |
| `OversizedSeriesComponent` | A series identity component (tenant, metric name, or label set) is too large to encode. |

Two behaviors worth knowing about, both intentional:

- Attribute values that are strings pass through verbatim; bools, ints, and
  doubles are canonicalized to their string form (`true`, `3`, `3.5`).
- Label and metric name sanitization replaces each disallowed character
  with `_` in place; it does not shift or prefix. A metric named `1foo` and
  one named `_foo` both sanitize to `_foo` and become the same series. This
  is a documented consequence of the sanitization rule, not a bug.

## Job and instance labels

If the resource has `service.name`, its points get a `job` label:
`service.namespace/service.name` if a namespace is present, else just
`service.name`. `service.instance.id`, if present, becomes `instance`. A
configurable allowlist of other resource attributes is flattened into
labels with dots replaced by underscores; the default allowlist is
`k8s.namespace.name`, `k8s.pod.name`, `k8s.container.name`, `host.name`,
`deployment.environment.name`, `cloud.provider`, `cloud.region`. Any
resource attribute not on this list, and not one of the three above, is
dropped, not stored as a label.

## Commit tokens and read-your-write

![ingest commit sequence](../diagrams/ingest-commit-sequence.svg)

A strict-mode ack returns one commit token per shard that flushed. Each
token is self-locating: it's base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`. Pass any of
those tokens back as `min_commit_token` on a query
([docs/guides/query.md](query.md#min_commit_token)) to make the catalog GET
that exact commit record directly, instead of relying on a listing that
might not yet include it. If the token can't be resolved, the query fails
with a 5xx `unavailable` error rather than silently serving a snapshot that
predates your write.

## Admission limits

Defaults ([crates/ravel-otlp/src/limits.rs](../../crates/ravel-otlp/src/limits.rs)), not currently configurable via
`ravel-server` flags:

| Limit | Default |
|---|---|
| `max_data_points_per_request` | 100,000 |
| `max_attributes_per_point` | 64 |
| `max_label_name_len` | 256 bytes |
| `max_label_value_len` | 4,096 bytes |
| `max_metric_name_len` | 512 bytes |
| `max_resource_attributes` | 128 |
| `max_future_skew_ns` | 10 minutes |
| `max_ingest_lag_ns` | 2 hours |

## Event-time skew bounds

Ravel never trusts a data point's event timestamp for discovery: commit
records are bucketed by ingest hour, not event hour, so a query only has to
look at buckets near "now" to find recent writes. The skew bounds keep
event time close enough to ingest time that this holds:

- More than 10 minutes in the future (`FutureSkew`): rejected, because a
  point far in the future would sit in an ingest-hour bucket a query
  wouldn't think to check yet, and would be indistinguishable from clock
  skew or a hostile sender trying to make data invisible to normal query
  ranges.
- More than 2 hours in the past (`TooOld`): rejected, because a point that
  old could otherwise land in an ingest-hour bucket a query has already
  finished reading, and the reader would never revisit it. This is also why
  [scripts/demo.sh](../../scripts/demo.sh) regenerates its OTLP fixture with
  fresh timestamps on every run.

Both bounds are inclusive: a skew or lag exactly equal to the limit is
accepted, one nanosecond past it is rejected.

## Logs

Everything above about authentication, strict vs. buffered acknowledgement,
and commit tokens applies unchanged to `POST /v1/logs` and the gRPC
`LogsService`. What follows is what differs.

```sh
curl -X POST http://127.0.0.1:4318/v1/logs \
  -H 'authorization: Bearer devtoken' \
  -H 'content-type: application/x-protobuf' \
  --data-binary @logs.pb
```

A strict-mode log export returns `200` with a binary
`ExportLogsServiceResponse` body and one `x-ravel-commit-token` per shard
that flushed, exactly like a metrics export. An unresolvable tenant is
`401`, an undecodable protobuf body is `400`, and a write the log pipeline
could not accept is `503`.

Log records are durable in RLOG objects under the tenant's `l` keyspace once
the strict ack returns. **There is no query surface over logs yet**: the
catalog fold task still runs for metrics only, so a log commit token cannot
be passed as `min_commit_token` to any endpoint, and no PromQL or SQL query
reads log data. Today the way to read a log object back is
`ravel-cli rlog inspect`
([inspecting-data.md](inspecting-data.md)). Log query is a later phase.

### Log admission limits

Defaults ([crates/ravel-otlp/src/logs_limits.rs](../../crates/ravel-otlp/src/logs_limits.rs)),
not currently configurable via `ravel-server` flags:

| Limit | Default |
|---|---|
| `max_records_per_request` | 100,000 |
| `max_attributes_per_record` | 128 |
| `max_attribute_key_len` | 256 bytes |
| `max_attribute_value_len` | 8,192 bytes |
| `max_body_len` | 65,536 bytes |
| `max_resource_attributes` | 128 |
| `max_scope_attributes` | 64 |

The body and attribute-value ceilings are deliberately wider than the metric
equivalents: a log body carries a message or a stack trace where a metric
label value carries an identifier. The asymmetry is intentional.

Admission ordering: these limits are checked after the whole request has
been decoded into memory, on both transports. They bound per-record work and
what reaches the shard buffer; they do not bound decode-time allocation,
which only the transport's body/message size limit bounds.

### Log rejections

Same partial-success contract as metrics, with
`ExportLogsPartialSuccess.rejected_log_records` and a combined
`error_message`. Admitted records are still ingested and acknowledged. The
`error_message` aggregates by distinct reason with a per-reason count and is
capped in length, so a request rejected wholesale does not produce a
response string proportional to its record count.

Every rejection reason ([crates/ravel-otlp/src/logs_limits.rs](../../crates/ravel-otlp/src/logs_limits.rs)):

| Rejection | Meaning |
|---|---|
| `TooManyRecords` | Whole request exceeds `max_records_per_request`; nothing in the request is admitted. |
| `TooManyResourceAttributes` | A `Resource` has more attributes than `max_resource_attributes`. Resource attributes are part of log stream identity, so nothing under that resource can be given one; every record under it is rejected. |
| `TooManyScopeAttributes` | An instrumentation scope has more attributes than `max_scope_attributes`. Also part of stream identity, so every record under that scope is rejected. |
| `TooManyAttributes` | One record has more attributes than `max_attributes_per_record`; that record is rejected. |
| `AttributeKeyTooLong` | An attribute key exceeds `max_attribute_key_len`; that one attribute is dropped, not the record. |
| `AttributeValueTooLong` | An attribute value's payload exceeds `max_attribute_value_len` (nested list and map entries count toward it); that one attribute is dropped, not the record. |
| `BodyTooLong` | The record body, after normalization to a string, exceeds `max_body_len`; that record is rejected. |
| `UnsupportedBodyKind` | The body is an OTLP `ArrayValue`, `KvlistValue`, or string-table reference. A structured body has no lossless string form, so the record is rejected rather than stringified by guess. |
| `MissingAttributeValue` | An attribute arrived with its `value` field unset; that one attribute is dropped and reported, never silently discarded. |
| `UnsupportedAttributeValue` | An attribute value is a string-table reference (`strindex`), which carries no value of its own; that one attribute is dropped. |
| `Grouped` | Not a reason of its own: carries one of the reasons above plus the number of records it applies to, for a rejection that covers a whole resource or scope. Reported as that inner reason with a count. |

Body normalization: a `StringValue` body passes through verbatim. `BoolValue`
and `IntValue` become their plain string form, `DoubleValue` uses the same
float formatting the metrics path uses, and `BytesValue` becomes a hex
string. A record with no body at all normalizes to an empty body, which is
legal OTLP, not a rejection.

Malformed `trace_id`/`span_id` byte lengths normalize to absent rather than
being padded or truncated: padding would fabricate an id that never existed.
A record with neither `time_unix_nano` nor `observed_time_unix_nano` set
(legal OTLP) takes the server's ingest timestamp for both.

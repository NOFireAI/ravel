# Ingest

## Endpoints

`ravel-server` accepts OTLP metrics and logs on two transports. `--listen-http`
(default `127.0.0.1:4318`) and `--listen-grpc` (default `127.0.0.1:4317`) bind
them:

- `POST /v1/metrics` over HTTP, body is a binary-encoded
  `ExportMetricsServiceRequest` (`Content-Type: application/x-protobuf`).
- `opentelemetry.proto.collector.metrics.v1.MetricsService/Export` over
  gRPC.
- `POST /v1/logs` over HTTP, body is a binary-encoded
  `ExportLogsServiceRequest` (`Content-Type: application/x-protobuf`).
  Responds with a binary `ExportLogsServiceResponse`.
- `opentelemetry.proto.collector.logs.v1.LogsService/Export` over gRPC.

All four are present only when `ravel-server` runs in `--mode all` (the
default) or `--mode gateway`. `--mode query` starts none of them.

Authentication, the strict/buffered mode header, the commit-token
header, and the status-code mapping are identical on all four. A log export
is a metrics export with a different payload and a different keyspace
underneath. Traces are also ingested, over the same two transports: `POST
/v1/traces` over HTTP and `opentelemetry.proto.collector.trace.v1.TraceService/Export`
over gRPC. No transport accepts profiles yet.

## Authentication

Every request must resolve to a tenant. If it does not, Ravel rejects it with
`401 Unauthorized`. The default (and only always-on) resolver is a static
bearer-token map. One or more `--tenant-token TOKEN=TENANT` flags build it:

```sh
ravel-server --tenant-token devtoken=acme --tenant-token other=other-co ...
```

A request must send `Authorization: Bearer devtoken` to resolve as
tenant `acme`. There is no default tenant and no anonymous access. A
deployment is unauthenticated only if you never pass `--tenant-token`,
which is a conscious choice, not an oversight.

`--dev-insecure-tenant-header` adds a second resolver, tried only if the
bearer lookup fails. It reads the tenant name directly from an
`x-ravel-tenant` request header, with no token. If `--listen-http` does not
bind a loopback address (`127.0.0.1` or `::1`), `ravel-server` refuses to
start with this flag set. It exists for local development against a
loopback-only server, not for any deployment reachable over a network.

## Strict vs. buffered acknowledgement

Every write has a mode. The default is strict:

- **Strict**: the HTTP or gRPC call does not return until every shard your
  points landed in has flushed. A flush writes the shard's segment object and
  commit record durably to the object store. The response carries a commit
  token for each shard that flushed
  (`x-ravel-commit-token` over HTTP, comma-separated if more than one
  shard). After you have that ack, the data survives the crash of any Ravel
  process, because it survives everything the object store survives
  ([docs/consistency-model.md](../consistency-model.md)).
- **Buffered**: the call returns as soon as Ravel validates the request and
  enqueues it into its shard's in-memory buffer, before any flush. This is
  lower latency but not durable. A crash between the ack and the next flush
  loses that buffered window, bounded by `max_flush_delay` (500ms default).
  Ravel issues no commit token, because there is nothing yet to point one at.

To use buffered mode for one request, send `x-ravel-ingest-mode: buffered` on
an HTTP export. For strict mode, omit it or send any other value. Today Ravel
honors this header for any tenant with no additional gate. The ingest design
doc describes a gate on buffered mode per tenant config, but that gate is not
implemented yet.

## Partial success and rejections

A single `ExportMetricsServiceRequest` can contain a mix of good and bad
data points. Ravel never silently drops a point. It counts every rejected
point (or group of points) and returns it in the OTLP
`ExportMetricsPartialSuccess` message, with `rejected_data_points` and a
combined `error_message`. Ravel still ingests and acknowledges the admitted
points normally.

Every rejection reason ([crates/ravel-otlp/src/limits.rs](../../crates/ravel-otlp/src/limits.rs)):

| Rejection | Meaning |
|---|---|
| `TooManyDataPoints` | The whole request exceeds `max_data_points_per_request`. Ravel admits nothing in the request. |
| `TooManyResourceAttributes` | A `Resource` has more attributes than `max_resource_attributes`. Ravel rejects every point under it. |
| `MetricNameTooLong` | The metric name (before sanitization) exceeds `max_metric_name_len`. Ravel rejects every point on that metric. |
| `EmptyMetricName` | The metric name sanitizes to empty. Ravel rejects every point on that metric. |
| `TooManyAttributes` | One data point has more attributes than `max_attributes_per_point`. |
| `LabelNameTooLong` | A label name (after sanitization) exceeds `max_label_name_len`. This also applies to the synthesized `job` label after its `namespace/name` join. |
| `LabelValueTooLong` | A label value exceeds `max_label_value_len`. |
| `DuplicateLabelName` | Two attributes sanitize to the same label name (or a data-point attribute collides with a synthesized `job`/`instance` label). |
| `ComplexAttributeValue` | An attribute value is an array, kvlist, or bytes value, which has no label representation. For a resource attribute, this rejects every point under that resource. |
| `MissingValue` | The data point has neither an int nor a double value set. |
| `UnsupportedMetricType` | The metric is a Histogram, ExponentialHistogram, or Summary. Phase 1 supports only Gauge and cumulative Sum. Ravel rejects the whole metric's points; it never drops them as if they did not exist. |
| `UnsupportedTemporality` | A Sum metric has delta (or unspecified) temporality. Ravel accepts only cumulative sums. |
| `ZeroTimestamp` | The data point's event timestamp is zero. |
| `FutureSkew` | The event timestamp is ahead of ingest time by more than `max_future_skew_ns`. |
| `TooOld` | The event timestamp is behind ingest time by more than `max_ingest_lag_ns`. |
| `OversizedSeriesComponent` | A series identity component (tenant, metric name, or label set) is too large to encode. |

Two behaviors are worth knowing about, both intentional:

- String attribute values pass through verbatim. Ravel canonicalizes bools,
  ints, and doubles to their string form (`true`, `3`, `3.5`).
- Label and metric name sanitization replaces each disallowed character
  with `_` in place. It does not shift or prefix. A metric named `1foo` and
  one named `_foo` both sanitize to `_foo` and become the same series. This
  is a documented consequence of the sanitization rule, not a bug.

## Job and instance labels

If the resource has `service.name`, its points get a `job` label. The value is
`service.namespace/service.name` if a namespace is present, else just
`service.name`. `service.instance.id`, if present, becomes `instance`. Ravel
flattens a configurable allowlist of other resource attributes into labels,
and replaces dots with underscores. The default allowlist is
`k8s.namespace.name`, `k8s.pod.name`, `k8s.container.name`, `host.name`,
`deployment.environment.name`, `cloud.provider`, `cloud.region`. Ravel drops
any resource attribute that is not on this list and not one of the three
above; it does not store it as a label.

## Commit tokens and read-your-write

![ingest commit sequence](../diagrams/ingest-commit-sequence.svg)

A strict-mode ack returns one commit token for each shard that flushed. Each
token is self-locating: it is base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`. Pass any of
those tokens back as `min_commit_token` on a query
([docs/guides/query.md](query.md#min_commit_token)). The catalog then GETs
that exact commit record directly, instead of relying on a listing that
might not yet include it. If the catalog cannot resolve the token, the query
fails with a 5xx `unavailable` error. It does not silently serve a snapshot
that predates your write.

## Admission limits

Defaults ([crates/ravel-otlp/src/limits.rs](../../crates/ravel-otlp/src/limits.rs)).
`ravel-server` flags cannot currently configure them:

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

Ravel never trusts a data point's event timestamp for discovery. It buckets
commit records by ingest hour, not event hour. A query then only has to
look at buckets near "now" to find recent writes. The skew bounds keep
event time close enough to ingest time for this to hold:

- More than 10 minutes in the future (`FutureSkew`): rejected. A point far in
  the future would sit in an ingest-hour bucket that a query does not check
  yet. It would also be indistinguishable from clock skew or a hostile sender
  that tries to make data invisible to normal query ranges.
- More than 2 hours in the past (`TooOld`): rejected. A point that old could
  otherwise land in an ingest-hour bucket that a query has already finished
  reading, and the reader would never revisit it. This is also why
  [scripts/demo.sh](../../scripts/demo.sh) regenerates its OTLP fixture with
  fresh timestamps on every run.

Both bounds are inclusive. A skew or lag exactly equal to the limit is
accepted; one nanosecond past it is rejected.

Logs and spans enforce the same window at admission (ADR-0051 §4). For a
**span**, the bounded timestamp is its **end** (`end_ts_ns`), on both edges,
and `end_ts < start_ts` is rejected outright. The lag bound anchors on the
end, not the start (ADR-0051 amendment, 2026-08-13): a long-running span that
started more than `max_ingest_lag_ns` ago but ended within the window is
admitted; only a span reported more than `max_ingest_lag_ns` after it *ended*
is `TooOld`. The listing window stays sound because any span overlapping a
query range has its end at or after the range start.

Ravel also checks its own receiver clock at admission (ADR-0051): a reading
below a compiled floor (2020-01-01T00:00:00Z) or one that
yields no representable ingest-hour bucket rejects the whole request with
`503` / gRPC `UNAVAILABLE`, counted under
`ravel_admission_rejected_total{reason="clock"}`. This is the replica's
fault, not the request's, and is retryable against a healthy replica. The
same floor extends the fail-loud flush-open check.

## Logs

Everything above about authentication, strict vs. buffered acknowledgement,
and commit tokens applies unchanged to `POST /v1/logs` and the gRPC
`LogsService`. This section covers what differs.

```sh
curl -X POST http://127.0.0.1:4318/v1/logs \
  -H 'authorization: Bearer devtoken' \
  -H 'content-type: application/x-protobuf' \
  --data-binary @logs.pb
```

A strict-mode log export returns `200` with a binary
`ExportLogsServiceResponse` body and one `x-ravel-commit-token` for each shard
that flushed, exactly like a metrics export. An unresolvable tenant returns
`401`, an undecodable protobuf body returns `400`, and a write that the log
pipeline cannot accept returns `503`.

Log records are durable in RLOG objects under the tenant's `l` keyspace after
the strict ack returns. **Logs are queryable via SQL**: the `logs` table is
registered on the `POST /api/v1/sql` endpoint (ravel-sql's `LOGS_TABLE`), and
[query.md](query.md#sql-over-the-logs-table) documents its schema and usage.
PromQL does not query logs: `ravel-promql` has no logs-reading path, so log
data is reachable only through SQL. You can also read a log object back
directly with `ravel-cli rlog inspect`
([inspecting-data.md](inspecting-data.md)).

### Log admission limits

Defaults ([crates/ravel-otlp/src/logs_limits.rs](../../crates/ravel-otlp/src/logs_limits.rs)).
`ravel-server` flags cannot currently configure them:

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
equivalents. A log body carries a message or a stack trace, where a metric
label value carries an identifier. The asymmetry is intentional.

Admission ordering: Ravel checks these limits after it decodes the whole
request into memory, on both transports. They bound per-record work and
what reaches the shard buffer. They do not bound decode-time allocation;
only the transport's body/message size limit bounds that.

### Log rejections

The partial-success contract is the same as metrics, with
`ExportLogsPartialSuccess.rejected_log_records` and a combined
`error_message`. Ravel still ingests and acknowledges the admitted records. The
`error_message` aggregates by distinct reason with a per-reason count, and its
length is capped. A request rejected wholesale therefore does not produce a
response string proportional to its record count.

Every rejection reason ([crates/ravel-otlp/src/logs_limits.rs](../../crates/ravel-otlp/src/logs_limits.rs)):

| Rejection | Meaning |
|---|---|
| `TooManyRecords` | The whole request exceeds `max_records_per_request`. Ravel admits nothing in the request. |
| `TooManyResourceAttributes` | A `Resource` has more attributes than `max_resource_attributes`. Resource attributes are part of log stream identity, so no record under that resource can get one. Ravel rejects every record under it. |
| `TooManyScopeAttributes` | An instrumentation scope has more attributes than `max_scope_attributes`. Scope attributes are also part of stream identity, so Ravel rejects every record under that scope. |
| `TooManyAttributes` | One record has more attributes than `max_attributes_per_record`. Ravel rejects that record. |
| `AttributeKeyTooLong` | An attribute key exceeds `max_attribute_key_len`. Ravel drops that one attribute, not the record. |
| `AttributeValueTooLong` | An attribute value's payload exceeds `max_attribute_value_len` (nested list and map entries count toward it). Ravel drops that one attribute, not the record. |
| `BodyTooLong` | The record body, after normalization to a string, exceeds `max_body_len`. Ravel rejects that record. |
| `UnsupportedBodyKind` | The body is an OTLP `ArrayValue`, `KvlistValue`, or string-table reference. A structured body has no lossless string form, so Ravel rejects the record rather than stringify it by guess. |
| `MissingAttributeValue` | An attribute arrived with its `value` field unset. Ravel drops and reports that one attribute; it never silently discards it. |
| `UnsupportedAttributeValue` | An attribute value is a string-table reference (`strindex`), which carries no value of its own. Ravel drops that one attribute. |
| `Grouped` | Not a reason of its own. It carries one of the reasons above plus the number of records it applies to, for a rejection that covers a whole resource or scope. Ravel reports it as that inner reason with a count. |

Body normalization: a `StringValue` body passes through verbatim. `BoolValue`
and `IntValue` become their plain string form. `DoubleValue` uses the same
float formatting that the metrics path uses. `BytesValue` becomes a hex
string. A record with no body at all normalizes to an empty body, which is
legal OTLP, not a rejection.

Malformed `trace_id`/`span_id` byte lengths normalize to absent; Ravel does
not pad or truncate them. Padding would fabricate an id that never existed.
A record with neither `time_unix_nano` nor `observed_time_unix_nano` set
(legal OTLP) takes the server's ingest timestamp for both.

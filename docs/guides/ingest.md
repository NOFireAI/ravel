# Ingest

## Endpoints

`ravel-server` accepts OTLP metrics, logs, and traces on two transports.
`--listen-http` (default `127.0.0.1:4318`) and `--listen-grpc` (default
`127.0.0.1:4317`) bind them:

- `POST /v1/metrics` over HTTP, body is a binary-encoded
  `ExportMetricsServiceRequest` (`Content-Type: application/x-protobuf`).
- `opentelemetry.proto.collector.metrics.v1.MetricsService/Export` over
  gRPC.
- `POST /v1/logs` over HTTP, body is a binary-encoded
  `ExportLogsServiceRequest` (`Content-Type: application/x-protobuf`).
  Responds with a binary `ExportLogsServiceResponse`.
- `opentelemetry.proto.collector.logs.v1.LogsService/Export` over gRPC.
- `POST /v1/traces` over HTTP, body is a binary-encoded
  `ExportTraceServiceRequest` (`Content-Type: application/x-protobuf`).
- `opentelemetry.proto.collector.trace.v1.TraceService/Export` over gRPC.

All six are present only when `ravel-server` runs in `--mode all` (the
default) or `--mode gateway`. `--mode query` starts none of them. No
transport accepts profiles.

Authentication, the acknowledgement mode header (strict or buffered), the
commit-token header, and the status-code mapping are identical on all six.

## Compressed requests

Ravel accepts gzip-compressed OTLP bodies on both transports. A stock
OpenTelemetry Collector and a stock Grafana Alloy both default to gzip, so no
client configuration is needed.

### OTLP HTTP

`POST /v1/metrics`, `/v1/logs`, and `/v1/traces` dispatch on `Content-Encoding`:

- `gzip`, or its RFC 9110 alias `x-gzip`: Ravel decompresses the body and
  then decodes it. The comparison is case-insensitive: `GZIP`, `gzip`, and
  `X-Gzip` are the same coding.
- An absent header, an empty value, or `identity`: Ravel reads the body as
  it is, byte for byte the uncompressed path.
- Any other single coding (`deflate`, `br`, ...), and any multi-coding list
  (`gzip, gzip`, `deflate, gzip`): rejected with `415 Unsupported Media
  Type`. Ravel chains no decoders and never decodes one member of a list.
  The 415 body names what is supported.

Two independent size caps bound a gzip request:

- The **compressed** body is capped at 16 MiB by the wire body limit
  (`DefaultBodyLimit`). The same cap bounds an uncompressed body.
- The **decompressed** body is capped at 64 MiB. A larger body returns
  `413 Payload Too Large`. Ravel enforces the cap *while* it inflates the
  body: the decoder is read through `take(cap + 1)`. A decompression bomb is
  therefore refused as it expands, before 64 MiB is allocated.

The two caps are independent, as they are for Remote Write. A 16 MiB
compressed body that inflates past 64 MiB is a 413. A body over 16 MiB
compressed never reaches the decompressor.

Ravel decodes a gzip body as a whole stream with multi-member semantics. A
concatenated multi-member gzip stream is legal gzip that ordinary tooling
produces. Ravel decodes it in full, under the single 64 MiB cap across all
members. Trailing bytes after a well-formed stream ends are
`400 Bad Request`, never silently truncated.

### OTLP gRPC

The three gRPC services accept gzip via tonic's `accept_compressed`. The
decompressed size is bounded at **16 MiB**, not the 64 MiB of HTTP. tonic
applies `max_decoding_message_size` to both the compressed frame and the
decompressed output, so the one 16 MiB knob caps both halves. A batch that
inflates to 40 MiB is accepted over HTTP and rejected over gRPC with
`resource_exhausted`.

Ravel does not compress responses (`send_compressed` is off). OTLP export
responses are small partial-success records.

### Byte-rate charging differs by path

The per-tenant ingest byte rate is charged on **different bases** by path.
Account for this when you size limits:

| Path | Charged quantity |
|---|---|
| OTLP HTTP / gRPC | Decompressed size |
| Prometheus Remote Write | Compressed size |

OTLP charges the decompressed size, so two tenants that send identical
telemetry are charged identically, with or without client-side compression.
A gzip OTLP client spends its byte-rate allowance at the rate of its
decompressed telemetry. Its wire rate understates its token spend by its
compression ratio. Remote Write charges the compressed body length.

For a gzip request, Ravel first compares the compressed size with the
available tokens of the tenant. The compressed length is a strict lower
bound on the decompressed length. If the compressed size exceeds the
available tokens, the request is rejected with `429 Too Many Requests`
before anything is decompressed, and it consumes no tokens. If the pre-check
passes, Ravel inflates the body and charges the decompressed size.

## Authentication

Every request must resolve to a tenant. If it does not, Ravel rejects it with
`401 Unauthorized`. There is no default tenant and no anonymous access.

The default resolver is a static bearer-token map. It is the only resolver
that is always on. One or more `--tenant-token TOKEN=TENANT` flags build it:

```sh
ravel-server --tenant-token devtoken=acme --tenant-token other=other-co ...
```

A request must send `Authorization: Bearer devtoken` to resolve as
tenant `acme`. A deployment is unauthenticated only if you pass no
`--tenant-token` and no `--tenant-token-file`.

`--tenant-token-file PATH` (env `RAVEL_TENANT_TOKEN_FILE` for the path) reads
the pairs from a file, so that a token does not sit in argv or a process
listing:

- The file holds one `TOKEN=TENANT` pair per line. Blank lines and `#`
  comments are ignored.
- Each line is split on the first `=`, like `--tenant-token`. A token value
  that contains `=` is mis-parsed the same way by both.
- `--tenant-token` and `--tenant-token-file` are mutually exclusive.
  `ravel-server` refuses to start with both set.

`--dev-insecure-tenant-header` adds a second resolver, tried only if the
bearer lookup fails. It reads the tenant name directly from an
`x-ravel-tenant` request header, with no token. Use it only for local
development against a loopback-only server. If `--listen-http` does not bind
a loopback address (`127.0.0.1` or `::1`), `ravel-server` refuses to start
with this flag set.

## Strict vs. buffered acknowledgement

Every write has an acknowledgement mode. The default is strict. To use
buffered mode for one request, send `x-ravel-ingest-mode: buffered` as an
HTTP header or as gRPC metadata on the export. For strict mode, omit it or
send any other value. Ravel honors the header for any tenant. No per-tenant
setting enables or refuses buffered mode.

**Strict.** The HTTP or gRPC call does not return until every shard that
your points landed in has flushed. A flush writes the segment object and the
commit record of the shard durably to the object store. The response carries
a commit token for each shard that flushed (`x-ravel-commit-token` over
HTTP, comma-separated if more than one shard). After you have that ack, the
data survives the crash of any Ravel process, because it survives everything
the object store survives
([docs/consistency-model.md](../consistency-model.md)).

**Buffered.** The call returns as soon as Ravel admits the request and
enqueues it to its shard actor, before any flush. Latency is lower, but the
write is not durable, and Ravel issues no commit token.

Acked buffered rows can be lost in these cases:

- **A crash between the ack and the flush.** `max_flush_delay` (2s default)
  bounds when the flush of the shard is triggered, not when it completes.
  The flush task then waits for a `max_inflight_flushes` permit on its shard
  before it issues its store calls. A shard whose permits are held by a
  stalled flush widens the window that a crash loses.
- **Store calls that cannot complete in time.** Acked rows are dropped, with
  no crash, when the store calls of the flush cannot complete in time:
  `max_flush_lifetime`, the budget for those store calls, defaults to
  3600 s. It runs from the
  moment the permit is granted, not from flush open, and it is not tunable
  from the server. This case is therefore a stuck backend and not a queue
  wait. `ravel_ingest_abandoned_retry_exhausted_total` counts these flushes.
- **A flush past its flush-open deadline.** The flush is abandoned without
  taking a permit, and its rows are lost the same way.
  `ravel_ingest_abandoned_queue_deadline_total` counts these flushes.

A flush queued behind a stalled co-resident prefix reaches the store once the
stall clears. A co-resident stall by itself is therefore not a buffered-mode
loss. See [docs/consistency-model.md](../consistency-model.md).

## Partial success and rejections

A single `ExportMetricsServiceRequest` can contain a mix of good and bad
data points. Ravel ingests and acknowledges the admitted points normally. It
counts every rejected point (or group of points) and returns the count in
the OTLP `ExportMetricsPartialSuccess` message, with `rejected_data_points`
and a combined `error_message`. Ravel never silently drops a point.

Every rejection reason:

| Rejection | Meaning |
|---|---|
| `TooManyDataPoints` | The whole request exceeds `max_data_points_per_request`. Ravel admits nothing in the request. |
| `TooManyExplodedPoints` | The request fits `max_data_points_per_request` in data points, but the normalized points its classic histograms and summaries explode into do not. Ravel admits nothing in the request. The reported rejected count stays in data points, the unit the sender sent. |
| `TooManyResourceAttributes` | A `Resource` has more attributes than `max_resource_attributes`. Ravel rejects every point under it. |
| `MetricNameTooLong` | The metric name (before sanitization) exceeds `max_metric_name_len`. Ravel rejects every point on that metric. |
| `EmptyMetricName` | The metric name sanitizes to empty. Ravel rejects every point on that metric. |
| `TooManyAttributes` | One data point has more attributes than `max_attributes_per_point`. |
| `TooManyHistogramBuckets` | One classic `Histogram` data point has more `explicit_bounds` than `max_histogram_buckets`. Only that data point is rejected. |
| `LabelNameTooLong` | A label name (after sanitization) exceeds `max_label_name_len`. This also applies to the synthesized `job` label after its `namespace/name` join. |
| `LabelValueTooLong` | A label value exceeds `max_label_value_len`. |
| `DuplicateLabelName` | Two attributes sanitize to the same label name (or a data-point attribute collides with a synthesized `job`/`instance` label). |
| `ComplexAttributeValue` | An attribute value is an array, kvlist, or bytes value, which has no label representation. For a resource attribute, this rejects every point under that resource. |
| `MissingValue` | The data point has neither an int nor a double value set. |
| `UnsupportedTemporality` | A Sum, Histogram, or ExponentialHistogram metric has delta (or unspecified) temporality. Ravel accepts only cumulative aggregations. |
| `ZeroTimestamp` | The data point's event timestamp is zero. |
| `FutureSkew` | The event timestamp is ahead of ingest time by more than `max_future_skew_ns`. |
| `TooOld` | The event timestamp is behind ingest time by more than `max_ingest_lag_ns`. |
| `OversizedSeriesComponent` | A series identity component (tenant, metric name, or label set) is too large to encode. |
| `HistogramMinMaxDropped` | Informational. The point is stored; its `min`/`max` fields are not, because they have no Prometheus-convention representation. Zero rejected points; see [Zero-count partial success](#zero-count-partial-success). |
| `HistogramExemplarsDropped` | Informational. The point is stored; some of its exemplars were malformed or fell past the per-series admission cap. Applies to any metric type, not only histograms. Zero rejected points. |
| `IntegerValuePrecisionLoss` | Informational. The point is stored, but its `as_int` value has a magnitude above 2^53 and was stored as the nearest `f64`. Zero rejected points. |

Two behaviors of normalization:

- String attribute values pass through verbatim. Ravel canonicalizes bools,
  ints, and doubles to their string form (`true`, `3`, `3.5`).
- Label and metric name sanitization replaces each disallowed character
  with `_` in place. It does not shift or prefix. A metric named `1foo` and
  one named `_foo` both sanitize to `_foo` and become the same series.

### Zero-count partial success

Some rejections cost the sender nothing:

- A histogram data point whose `min` and `max` fields have no
  Prometheus-convention representation is stored without them.
- Exemplars past the per-series admission cap are not carried.
- An OTLP `as_int` value with a magnitude above 2^53 is stored as the
  nearest `f64`.
- A log record or a span with one bad attribute is stored without that
  attribute.

In each case the unit was admitted, so it contributes zero to the rejected
count.

Ravel reports these anyway. **Every OTLP surface emits a partial success
whenever anything was rejected. It does not wait for a unit count above
zero.** A drop of this kind comes back as `rejected_data_points = 0` (or
`rejected_log_records = 0`, or `rejected_spans = 0`) together with a
populated `error_message` that names it. The OTLP proto documents
`error_message` as a channel for warnings on an otherwise successful
response.

The transport decides whether the report reaches you:

- **OTLP over HTTP and gRPC**, for metrics, logs, and spans: reported, as
  above.
- **OTAP** (metrics only): not reported. `BatchStatus` carries one
  `status_message` string, and Ravel uses it for the active-series-cap drop
  count and the commit tokens. No normalization-layer rejection,
  informational or not, reaches it. The drop is still visible in the
  per-tenant rejection counters.
- **Remote Write**: not reported. That surface has no partial-success
  message. Its only per-request feedback is the
  `x-prometheus-remote-write-samples-written` family of headers, which count
  what was admitted.

## Delta temporality metrics

Ravel stores cumulative metrics only. A `Sum`, `Histogram`, or
`ExponentialHistogram` whose `aggregation_temporality` is delta (or
unspecified) is rejected as `UnsupportedTemporality`. The response reports
every point under that metric as rejected. Temporality is a property of the
metric, so a metric cannot carry a mix.

This restriction is permanent. A conversion from delta to cumulative must
hold the running total for every series between requests, and a Ravel
compute process keeps no durable local state.

You have two fixes.

**Configure the sender for cumulative temporality.** Most OpenTelemetry SDKs
support this through the
`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=cumulative` environment
variable. This avoids the conversion and its memory. It is the better fix
where you can configure the sender.

**Convert in the collector.** The collector is a stateful process that owns
local memory. The `deltatocumulative` processor
(`otel/opentelemetry-collector-contrib`) holds the per-series accumulators
and emits cumulative points:

```yaml
processors:
  deltatocumulative:
    # Forget a series after this long without a point, so a churning series
    # set cannot grow memory without bound.
    max_stale: 5m
    # Ceiling on tracked series. Points for an untracked series past it are
    # dropped by the processor, not buffered and not forwarded.
    max_streams: 1000000
  batch:

service:
  pipelines:
    metrics:
      receivers: [otlp]
      # Convert before batching, so batches carry converted points.
      processors: [deltatocumulative, batch]
      exporters: [otlphttp]
```

Plan for two consequences:

- **`max_streams` is a data-loss ceiling.** The memory of the processor is
  proportional to the number of live series. Size `max_streams` above the
  active series count of the tenant. When the processor tracks `max_streams`
  series, it drops a point for any series that it does not already track.
  The point is not buffered and not forwarded unconverted. Nothing about it
  reaches Ravel, and no Ravel rejection counter moves. The only signal is
  the `otelcol_deltatocumulative_datapoints_total{error="limit"}` counter of
  the collector. Alert on it.
- **A collector restart looks like a counter reset.** The first point of
  each series after a collector restart re-bases the accumulator of that
  series. PromQL's `rate` and `increase` handle resets, so query results
  stay correct. A dashboard that reads a raw counter value shows the drop.

To confirm that the rejections stopped, watch
`ravel_admission_rejected_total{reason="structural"}`
([observability guide](observability.md#reading-the-reason-label)).

## Metric metadata and OTLP name suffixing

Ravel captures the type, help, and unit of each metric at ingest time and
serves them through
[`GET /api/v1/metadata`](query.md#get-apiv1statusbuildinfo-and-get-apiv1metadata).
Every path supplies metadata:

- OTLP reads `Metric.description` (help) and `Metric.unit` alongside the
  name, and infers the type from the data shape.
- Remote Write v1 and v2 supply the `MetricMetadata`/per-series metadata
  that they parse.

Capture is best-effort and off the acknowledgement path. A point is acked on
its data write. It never waits on the metadata record and never fails from
it.

OTLP metric names also get the standard OpenTelemetry-to-Prometheus suffixes
that a Prometheus exporter adds. The same metric then lands under one series
name, whether it is ingested over OTLP or scraped through a collector. Ravel
maps and appends the unit (`s` becomes `_seconds`, `By` becomes `_bytes`).
Then a monotonic `Sum` gets `_total`. A monotonic counter named `foo` with
`unit: "By"` ingests as `foo_bytes_total`.

The transform applies at ingest time, to the name string only. It does not
touch series identity or any stored format. A dashboard built against the
unsuffixed OTLP names sees it as a one-time naming change.

## Job and instance labels

Ravel derives labels from resource attributes:

- If the resource has `service.name`, its points get a `job` label. The
  value is `service.namespace/service.name` if a namespace is present, else
  `service.name`.
- `service.instance.id`, if present, becomes `instance`.
- Ravel flattens a configurable allowlist of other resource attributes into
  labels, and replaces dots with underscores. The default allowlist is
  `k8s.namespace.name`, `k8s.pod.name`, `k8s.container.name`, `host.name`,
  `deployment.environment.name`, `cloud.provider`, `cloud.region`.

Ravel drops any resource attribute that is not on this list and not one of
the three above. It does not store it as a label. The allowlist is fixed at
build time and is not configurable per tenant.
`ravel_ingest_resource_attrs_dropped_total` counts the drop. The count
includes the case where two resources that differ only in a dropped
attribute collapse into one series. See
[the observability guide](observability.md#resource-attributes-outside-the-allowlist).

### Finding a skewed writer

An event-time skew alert
([observability guide](observability.md#reading-the-reason-label)) cannot be
attributed to one writer from `/metrics` alone:

- `ravel_admission_rejected_total{reason="skew"}` is broken out by
  `tenant_hash`, `signal` and `reason`, and by nothing that names a sender.
- A skew-rejected point is never stored, so it produces no ingested series
  that carries the `instance` of the sender.
- The `instance` label that Prometheus attaches beside that counter is the
  scrape target. That is the Ravel pod that rejected the point, not the pod
  that sent it.
- The counter cannot be narrowed per shard. `shard` is never combined with
  `tenant_hash` on any sample, and this counter is per tenant.

To find the writer, query the admitted series of the tenant. The points of a
lagging pod that pass admission still carry its late timestamps. The newest
sample time per `instance` therefore shows which pod has a clock that trails
the rest.

This works only if the collector config gives each pod a distinct
`service.instance.id` (for example, populated from the pod name via the
downward API). A static or unset value collapses every replica of a job onto
one `instance` label. One pod with a broken clock is then indistinguishable
from the rest of the fleet.

### Per-pod resource attributes and shard spread

A distinct per-pod resource attribute is also the precondition for `--shards`
to spread log ingest across shards. A log stream id is the hash of its
resource attributes plus scope ([docs/ingest.md](../ingest.md)), and
`shard_for_log` routes on that hash modulo the shard count.

When every pod of a collector fleet sends the same resource attribute set,
the fleet hashes to one stream id and therefore one shard actor. Examples
are a static or unset `service.instance.id`, or the same `host.name` behind
a shared load balancer. A higher `--shards` moves the whole tenant to a
different single shard. It does not divide the writes of the tenant across
more shards.

The
[`ravel_ingest_shard_*` family](observability.md#per-shard-ingest-skew-ravel_ingest_shard_)
shows this directly: one `shard` series carries the load and the rest read
zero. To fix it, give each pod a distinct `service.instance.id` in the
collector config. Do not change the `--shards` flag.

## Commit tokens and read-your-write

![ingest commit sequence](../diagrams/ingest-commit-sequence.svg)

A strict-mode ack returns one commit token for each shard that flushed. Each
token is self-locating: it is base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`. Pass any of
those tokens back as `min_commit_token` on a query
([docs/guides/query.md](query.md#min_commit_token)). The catalog then GETs
that commit record directly and does not rely on a listing that can lag
behind it. If the catalog cannot resolve the token, the query fails with a
5xx `unavailable` error. It does not silently serve a snapshot that predates
your write.

## Admission limits

Defaults. No `ravel-server` flag configures them:

| Limit | Default |
|---|---|
| `max_data_points_per_request` | 100,000 |
| `max_attributes_per_point` | 64 |
| `max_histogram_buckets` | 160 |
| `max_summary_quantiles` | 64 |
| `max_resolved_label_bytes_per_request` | 256 MiB |
| `max_label_name_len` | 256 bytes |
| `max_label_value_len` | 4,096 bytes |
| `max_metric_name_len` | 512 bytes |
| `max_resource_attributes` | 128 |
| `max_future_skew_ns` | 10 minutes |
| `max_ingest_lag_ns` | 2 hours |

`max_data_points_per_request` is checked twice: against the data points that
the request carries on the wire, and against the normalized points that
those data points expand into.

- One classic `Histogram` data point becomes one point per explicit bound
  plus `+Inf`, `_sum`, and `_count`.
- One `Summary` data point becomes one point per quantile plus `_sum` and
  `_count`.

The count that a rejection reports back to the sender stays in wire data
points.

`max_histogram_buckets` bounds the `explicit_bounds` of a single classic
`Histogram` data point. It is a memory-safety bound: the sender chooses the
bound list, and each bound becomes a stored series with its own copy of the
labels of the point. The default is an order of magnitude above the widest
bound list that a real exporter emits. The `DefBuckets` of the Prometheus Go
client is 11 bounds, and the default explicit boundaries of the
OpenTelemetry SDK are 15. Exponential (native) histograms are not exploded
and are not subject to the cap. `max_summary_quantiles` is the same bound on
the quantiles of one `Summary` data point.

`max_resolved_label_bytes_per_request` bounds the label bytes that
normalization would build for the whole request, counting every bucket's and
quantile's copy of the point's labels. A request over it is rejected whole
with a partial success, and a request under it is charged against the ingest
buffer byte budget until it is written; see
[Resolved-label bytes](admission-limits.md#resolved-label-bytes). The same
bound, with the same default, applies to logs and spans.

## Event-time skew bounds

Ravel buckets commit records by ingest hour, not event hour. It never trusts
the event timestamp of a data point for discovery. A query then looks only
at buckets near "now" to find recent writes. The skew bounds keep event time
close enough to ingest time for this to hold:

- More than 10 minutes in the future (`FutureSkew`): rejected. If admitted,
  such a point sits in an ingest-hour bucket that a query does not check
  yet. It is also indistinguishable from clock skew, or from a hostile
  sender that tries to make data invisible to normal query ranges.
- More than 2 hours in the past (`TooOld`): rejected. If admitted, such a
  point can land in an ingest-hour bucket that a query has already finished
  reading, and the reader never revisits it. For the same reason
  [scripts/demo.sh](../../scripts/demo.sh) regenerates its OTLP fixture with
  fresh timestamps on every run.

Both bounds are inclusive. A skew or lag equal to the limit is accepted. One
nanosecond past it is rejected.

Logs and spans enforce the same window at admission. For a **span**, the
bounded timestamp is its **end** (`end_ts_ns`), on both edges:

- `end_ts < start_ts` is rejected.
- A long-running span that started more than `max_ingest_lag_ns` ago but
  ended within the window is admitted.
- A span reported more than `max_ingest_lag_ns` after it *ended* is
  `TooOld`.

The listing window stays sound because any span that overlaps a query range
has its end at or after the range start.

Ravel also checks its own receiver clock at admission. A reading below a
compiled floor (2020-01-01T00:00:00Z), or one that yields no representable
ingest-hour bucket, rejects the whole request with `503` / gRPC
`UNAVAILABLE`. The rejection counts under
`ravel_admission_rejected_total{reason="clock"}`. The fault is in the
replica and not in the request, so the request is retryable against a
healthy replica. The same floor extends the flush-open check. See
[Receiver-clock floor](admission-limits.md#receiver-clock-floor).

## Logs

Authentication, acknowledgement modes and commit tokens apply unchanged to
`POST /v1/logs` and the gRPC `LogsService`. This section covers what
differs.

```sh
curl -X POST http://127.0.0.1:4318/v1/logs \
  -H 'authorization: Bearer devtoken' \
  -H 'content-type: application/x-protobuf' \
  --data-binary @logs.pb
```

A strict-mode log export returns `200` with a binary
`ExportLogsServiceResponse` body and one `x-ravel-commit-token` for each
shard that flushed, like a metrics export. The error responses:

- An unresolvable tenant returns `401`.
- An undecodable protobuf body returns `400`.
- A write that the log pipeline cannot accept returns `503`.

After the strict ack returns, log records are durable in RLOG objects under
the `l` keyspace of the tenant. **Logs are queryable two ways**:

- Over SQL. The `logs` table is registered on the `POST /api/v1/sql`
  endpoint. [query.md](query.md#sql-over-samples-logs-and-spans) documents
  its schema and usage.
- Over PromQL, through the reserved `ravel_log_lines` and `ravel_log_bytes`
  metric names. [query.md](query.md#promql-over-logs) documents the label
  mapping and routing rule.

You can also read a log object back directly with `ravel-cli rlog inspect`
([inspecting-data.md](inspecting-data.md)).

### Log admission limits

Defaults. No `ravel-server` flag configures them:

| Limit | Default |
|---|---|
| `max_records_per_request` | 100,000 |
| `max_attributes_per_record` | 128 |
| `max_attribute_key_len` | 256 bytes |
| `max_attribute_value_len` | 8,192 bytes |
| `max_body_len` | 65,536 bytes |
| `max_resource_attributes` | 128 |
| `max_scope_attributes` | 64 |
| `max_resolved_label_bytes_per_request` | 256 MiB; each record counts its own copy of its resource and scope attributes |
| Attribute nesting | 15 kvlists around a value; an array level costs half a kvlist level, so 31 arrays fit |
| Entries in one array or kvlist | 1,048,576 |

The last two rows are not configurable limits. They are what log storage can
hold, and they apply to every resource, scope and record attribute.

The body and attribute-value ceilings are wider than the metric equivalents.
A log body carries a message or a stack trace, where a metric label value
carries an identifier.

Ravel checks these limits after it decodes the whole request into memory, on
both transports. They bound per-record work and what reaches the shard
buffer. They do not bound decode-time allocation. Only the body or message
size limit of the transport bounds that.

### Log rejections

The partial-success contract is the same as for metrics, with
`ExportLogsPartialSuccess.rejected_log_records` and a combined
`error_message`. Ravel still ingests and acknowledges the admitted records.
The `error_message` aggregates by distinct reason with a per-reason count,
and its length is capped. A request rejected wholesale therefore does not
produce a response string proportional to its record count.

A dropped attribute is reported the same way, with `rejected_log_records` at
0, because the record was stored. See
[Zero-count partial success](#zero-count-partial-success) for the rule and
for which transports carry it.

Every rejection reason:

| Rejection | Meaning |
|---|---|
| `TooManyRecords` | The whole request exceeds `max_records_per_request`. Ravel admits nothing in the request. |
| `TooManyResourceAttributes` | A `Resource` has more attributes than `max_resource_attributes`. Resource attributes are part of log stream identity, so no record under that resource can get one. Ravel rejects every record under it. |
| `TooManyScopeAttributes` | An instrumentation scope has more attributes than `max_scope_attributes`. Scope attributes are also part of stream identity, so Ravel rejects every record under that scope. |
| `TooManyAttributes` | One record has more attributes than `max_attributes_per_record`. Ravel rejects that record. |
| `AttributeKeyTooLong` | An attribute key exceeds `max_attribute_key_len`. Ravel drops that one attribute, not the record. |
| `AttributeValueTooLong` | An attribute value's payload exceeds `max_attribute_value_len` (nested list and map entries count toward it, and every list, map and map entry counts at least one byte, so a value made of empty ones is bounded too). On a record, Ravel drops that one attribute and stores the record. On a resource or scope, Ravel rejects every record under it. |
| `AttributeTooDeeplyNested` | An array or kvlist attribute value nests deeper than log storage holds: more than 15 kvlists around a value, with an array level costing half a kvlist level. On a record, Ravel drops that one attribute and stores the record. On a resource or scope, the attribute is part of stream identity, so Ravel rejects every record under that resource or scope. |
| `AttributeTooManyEntries` | An array or kvlist inside an attribute value holds more than 1,048,576 entries. Ravel checks this before the value-length limit, so such a value is reported under this reason. On a record, Ravel drops that one attribute and stores the record. On a resource or scope, Ravel rejects every record under it. |
| `BodyTooLong` | The record body, after normalization to a string, exceeds `max_body_len`. Ravel rejects that record. |
| `UnsupportedBodyKind` | The body is a string-table reference, which indexes a table the record does not carry, so there is nothing to store. Array and map bodies are converted, not rejected; see below. |
| `MissingAttributeValue` | An attribute arrived with its `value` field unset. Ravel drops and reports that one attribute; it never silently discards it. |
| `UnsupportedAttributeValue` | An attribute value is a string-table reference (`strindex`), which carries no value of its own. Ravel drops that one attribute. |
| `Grouped` | Not a reason of its own. It carries one of the reasons above plus the number of records it applies to, for a rejection that covers a whole resource or scope. Ravel reports it as that inner reason with a count. |

### Log body normalization

- A `StringValue` body passes through verbatim.
- `BoolValue` and `IntValue` become their plain string form.
- `DoubleValue` uses the same float formatting that the metrics path uses.
- `BytesValue` becomes a hex string.
- A record with no body normalizes to an empty body. This is legal OTLP,
  not a rejection.

An `ArrayValue` or `KvlistValue` body is stored as JSON text. The rendering is
canonical, so two exports of the same body always produce byte-identical
stored text:

- Map keys follow the rule that orders attributes in stream identity: a byte
  ordering on the key and then on the encoded value. This is not the order
  of the sender, and not lexicographic ordering of the JSON text. Two
  entries with the same key are both kept, ordered by their values.
- Array elements keep the order of the sender, which is part of the value.
- Nested arrays and maps render recursively under the same rules. The depth
  limit of attribute values also bounds the nesting. A body past that depth,
  or one that holds an unset value or a nested string-table reference, is
  rejected as `UnsupportedBodyKind`. The sender lost the body, so the record
  is reported that way and not as an attribute problem.
- A bytes value inside the body renders as a lowercase hex string.
- A non-finite double renders as the JSON string `"NaN"`, `"+Inf"`, or
  `"-Inf"`, because JSON has no literal for them. A query predicate must
  match those three strings. A top-level double body of the same value takes
  the same forms.

`max_body_len` bounds the converted text like any other body, so a large
structured body can still be rejected as `BodyTooLong`. Ravel applies the
bound while it produces the text. Conversion stops at the first byte that
carries the text past `max_body_len` and rejects there, without rendering
the rest. The `len` that such a rejection reports is that stopping point,
one byte past the limit, not the length of the full text.

`ravel_ingest_body_conversions_total` counts conversions per tenant and
signal. It is not a rejection counter, and it is not a count of stored
records. It is incremented at normalization, before the active-stream cap
and before the write, so a counted record can still be dropped by the cap or
lost with a failed write. Use it when a query returns JSON text where a
reader expected a plain message. The
[observability guide](observability.md#reading-the-reason-label) holds the
normative description.

Malformed `trace_id`/`span_id` byte lengths normalize to absent. Ravel does
not pad or truncate them. A record with neither `time_unix_nano` nor
`observed_time_unix_nano` set (legal OTLP) takes the ingest timestamp of the
server for both.

## Bulk import (`ravel-cli load --parquet`)

To load an existing structured dataset offline (a Parquet export, an archive
migration, a historical backfill), use `ravel-cli load`. It imports a
Parquet file into the signal that `--signal` names. OTLP is the only
*networked* way to write to Ravel.

```sh
ravel-cli load --parquet events.parquet --tenant acme --mapping map.toml --shards 4
ravel-cli load --signal metrics --parquet samples.parquet --tenant acme \
  --mapping metrics.toml --shards 4
ravel-cli load --signal spans --parquet traces.parquet --tenant acme \
  --mapping spans.toml --shards 4
```

`--signal` defaults to `logs`. `--signal metrics` loads the metrics signal
(see [Loading metrics](#loading-metrics)). `--signal spans` loads the spans
signal (see [Loading spans](#loading-spans)).

The loader is an in-process caller of the same log ingest router that OTLP
uses: the same shard actors, flush cadence, and commit protocol. It is not a
parallel write path. It builds the router against the provisioned shard
count of the target tenant. The count is validated against, or written to,
the durable provisioning record, as the server does at first touch. The
loader writes with strict acknowledgement and awaits every write before it
exits. A run that returns success therefore has no buffered data that is
not flushed.

### Loading spans

`--signal spans` provisions or validates the spans signal of the tenant,
builds a `SpanIngestRouter` from the same configuration, and writes every
batch with strict acknowledgement. One input row is one span:

```toml
[spans]
trace_id_column       = "trace_id"   # 16-byte binary or 32-character hex string
span_id_column        = "span_id"    # 8-byte binary or 16-character hex string
parent_span_id_column = "parent"     # optional; a null or empty cell is a root
name_column           = "name"
start_ts_column       = "start"
start_ts_unit         = "nanos"      # seconds | millis | micros | nanos
end_ts_column         = "end"
end_ts_unit           = "nanos"
status_code_column    = "status"     # optional; OTLP's 0 unset / 1 ok / 2 error
status_message_column = "status_msg" # optional
attrs_map_column      = "attrs"      # optional; Map<Utf8, Utf8>, read and written

# Merged into the span's one attrs map. A key may not appear in both attribute
# lists; such a mapping is refused, so this merge never resolves a collision.
[[spans.resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[spans.attribute]]
key = "http.method"
column = "method"
type = "str"                         # str | i64 | f64 | bool | bytes
```

#### The attributes map column

`attrs_map_column` names one map column of string keys and string values
(plain or dictionary-encoded). Its entries are merged into the attributes of
the span at span precedence and stored as written. It holds the attributes
that the two lists do not name. `ravel-cli export --signal spans` writes
those attributes into this column, so a spans export under this mapping
re-loads every entry that it writes. An export leaves out the entries past
the cap below and counts the spans that lost one.

Rules for the map column:

- A null cell or a null value is an attribute that the row does not carry.
  An entry with a null value is skipped before any of the checks below.
- An entry whose key or value is over its length cap drops that attribute,
  counted in `attrs_dropped`, as OTLP drops it.
- A row is refused when its map holds a key that a
  `[[spans.resource_attribute]]` or `[[spans.attribute]]` entry also names,
  whether or not the cell of that entry holds a value.
- A row is refused when its map holds one key twice.
- A row is refused when its map holds a reserved key.
- The entries count toward the loader per-record cap of 1024 together with
  the `[[spans.attribute]]` values.
- A column that is not a map of strings is refused when the columns of the
  batch are resolved.

One span carries one attrs map with unique keys, and the loader never
decides silently which of two values reaches the record.

#### Same record as OTLP

**A span loaded here is stored as the same record the same span sent over
OTLP produces.**

- Attribute values are coerced to the strings that the `Map<Utf8, Utf8>` of
  RSPAN holds, by the rules of OTLP: an integer and a boolean verbatim, a
  float through the same Go-compatible formatter that the `le` label uses,
  bytes as lowercase hex.
- The resource-over-span merge is `ravel_rspan::merge_attrs`.
- The status column goes through the enum mapping of `ravel-otlp`. A value
  outside `0..=2` normalizes to unset here as it does there, including one
  too wide for `i64`.
- An empty parent cell is a root span, as the empty `parent_span_id` field
  of OTLP is. A file that writes roots as empty bytes or `""` loads
  unchanged.
- An attribute value longer than the 8192-byte cap drops that attribute and
  keeps the span, as the OTLP path does. The load summary prints how many
  values were dropped that way (`attrs_dropped`). On a load, that count
  takes the place of the `AttributeValueTooLong` partial-success entry of
  the OTLP path.
- The count is taken where each span is built. The `attrs_dropped` of a
  FAILED load therefore also covers the batches that the failure abandoned,
  whose spans are in no object. The line printed there says so.
- A null attribute cell is an attribute that the row does not carry, as an
  OTLP `KeyValue` that carries no value is dropped as
  `MissingAttributeValue`.
- An empty attribute value is a value and is stored, unlike an empty metric
  label.

#### Differences from OTLP

This list is complete. For the same input, nothing else about the stored
record differs between a Parquet load and an OTLP export.

- **A null `start_ts`, `end_ts` or `name` cell is refused.** OTLP has no null
  for any of the three. A timestamp that OTLP omits is a zero, and a zero
  takes the same fallbacks here: load time for the start, and the start for
  the end. A name that OTLP omits is the empty string. In a file that the
  operator controls, a null there is a mapping or export mistake.
- **A negative timestamp is refused.** The two timestamps of OTLP are
  unsigned and have no negative to express. If admitted, a negative start
  beside a positive end stores a span whose interval overlaps nearly every
  query window. The usual cause is a declared `start_ts_unit`/`end_ts_unit` that
  does not match the column, and the refusal names both.
- **A non-empty `parent_span_id` of the wrong width is refused.** OTLP drops
  the field and admits the span as a root. A mapped column that produces
  unusable ids is a mapping mistake that the whole file shares, and a
  re-rooted span tree is not visible in the data.
- **Attribute keys and the two attribute-count caps are checked against the
  `--mapping`, not per span.** The load is refused before any row is built
  when a key is one of these:
  - empty
  - longer than the 256-byte OTLP cap
  - reserved for a span field this version does not map
  - declared in both attribute lists

  OTLP drops the attribute of an over-long key and admits an empty key. The
  load is also refused for a mapping with more than 1024
  `[[spans.attribute]]` columns (the loader per-record cap, standing in for
  OTLP's 128 `max_attributes_per_span`) or more than 128
  `[[spans.resource_attribute]]` columns (OTLP's own
  `max_resource_attributes`, which rejects the spans under an over-cap
  resource).

The past-event-time lag bound is relaxed on every load path, which admits a
historical backfill. The future-skew bound is kept and anchored on the end
of the span, as OTLP anchors it.

#### Shapes to convert first

**The mapping reads one flat row per span.** Convert a trace export of any
other shape before you load it. Four shapes need conversion, and this
version cannot express a mapping for any of them:

- A file that nests the spans of a trace in one row (a list or struct column
  per trace). Flatten it to one row per span.
- Attributes held in a `Struct` column, or in a `Map` column whose keys or
  values are not strings. Pivot them to one scalar column per attribute key,
  because `[[spans.attribute]]` names a column and a value type. A map of
  strings to strings, plain or dictionary-encoded, is read whole as
  `attrs_map_column`.
- A duration column. Turn it into an absolute `end_ts` in a unit that
  `end_ts_unit` names.
- A status written as a string (`"OK"`, `"ERROR"`). Convert it to the 0/1/2
  integer of OTLP, because `status_code_column` reads that enum and nothing
  else.

A mapping that points at an unconverted column of any of these shapes fails
the load. It does not import part of the file. A column whose type cannot
supply the field is refused, and the refusal names the type that it found.
The refusal of an id column also names the column. A duration read as an
`end_ts` is refused because it ends before its span starts.

Span events and span links are not mappable in this version. A mapping that
names them is refused by name, not as a typo. The same refusal covers the
reserved `attrs` keys under which the OTLP path stores span kind, trace
state, span flags, events and links (`_kind`, `_trace_state`, `_flags`,
`_events_raw`, `_links_raw`).

An id column that cannot carry an id of the right width is refused when the
columns of the batch are resolved, before any row is built or written. Ravel
never pads or truncates an id.

#### Read cursors on a spans load

Like a metrics load, a spans load reads **one sequential cursor** and has no
decode/encode queue. `--read-cursors` and `--decode-queue-batches` change
nothing, and the loader warns when either was set to a value it ignores. A
value of 0 for either is still rejected. Everything that shapes the objects
(`--shards`, `--batch-rows`, `--target-bytes`, `--max-inflight-flushes`,
`--max-flush-delay`, `--pipeline-depth`) applies unchanged. Spans bucket by
load time, as logs and metrics do.

### The columnar fast path

The Parquet that a load reads is columnar, and the RLOG object that it
writes is columnar too. The loader builds the storage-native columnar batch
directly from each Arrow `RecordBatch` and hands it to the router as a
column batch. It skips the per-row record pivot of the record path. The
measured CPU cost is in
[docs/ingest.md](../ingest.md#cpu-cost-of-the-columnar-load-path-vs-the-row-path-measured).

- Every Arrow downcast and the `ts_unit` scaling are resolved once per
  column.
- Stream identity is hashed once per distinct resource-attribute tuple, not
  once per row.
- Admission checks (future skew, the length caps) run over whole columns
  and still report a rejected row by its absolute file index.

The commit protocol, object key layout, strict-ack contract, and the RLOG
format are unchanged. This is a CPU path: a columnar load writes
byte-for-byte the same objects as a record-path load.

When a mapped string column arrives **dictionary-encoded**, the loader passes
the dictionary through. The writer then pays string encoding and token-bloom
cost once per distinct value, not once per row. Both paths produce identical
output.

A column arrives dictionary-encoded in two cases:

- The embedded Arrow schema of the file types it as an Arrow `Dictionary`.
- It is a top-level UTF-8 `BYTE_ARRAY` column whose type is plain `Utf8` and
  whose every data page, in every row group, is dictionary-encoded. The
  loader reads such a column as an Arrow `Dictionary` whether or not the
  file carries an Arrow schema.

A string column decodes to a plain Arrow string column and stays on the
per-row string path in three cases:

- It has any plainly encoded page. A writer falls back to plain when its
  dictionary outgrows its page limit, as a unique-per-row column does.
- The Arrow schema of the file types it as `LargeUtf8`.
- It is nested below the top level.

The plain-page rule has one exception. When the footer of the file records
no page encoding statistics, the loader has only the encodings list of the
chunk. That list names the dictionary encoding for a chunk that fell back to
plain too. Such a chunk is read as an Arrow `Dictionary`, with identical
values. Only the per-block work differs.

A column that the Arrow schema of the file types as `Utf8View` (a common
polars output) is not read. The loader reads `Utf8`, `LargeUtf8` and
dictionary-encoded string columns only, and refuses a `Utf8View` column at
its first non-null cell.

A mapped `trace_id` or `span_id` column loads whether it is plain or
dictionary-encoded. A dictionary-encoded hex id column stores the same ids
as its plain copy, and a null cell stores no id in either form.

### The `--mapping` TOML

The mapping declares how source Parquet columns become record fields. It
carries one signal section only (`[logs]`, `[metrics]` or `[spans]`), and
that section must match `--signal`. A mapping whose logs keys sit at the
document root, with no section, is read as the `[logs]` section. A file that
mixes the two spellings is refused.

In the logs section, resource attributes determine stream identity. Declare
them separately from record attributes, which never enter identity:

```toml
ts_column = "timestamp"
ts_unit   = "millis"        # seconds | millis | micros | nanos

body_column            = "message"   # optional
severity_number_column = "sev_num"   # optional (integer column)
severity_text_column   = "level"     # optional (string column)
trace_id_column        = "trace_id"  # optional (16-byte binary or 32-hex string)
span_id_column         = "span_id"   # optional (8-byte binary or 16-hex string)
attrs_map_column       = "attrs"     # optional; export only, load never reads it

# Resource attributes: part of stream identity.
[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

# Record attributes: typed values in the record's `attrs`, never stream identity.
[[attribute]]
key = "http.status_code"
column = "status"
type = "i64"                # str | i64 | f64 | bool | bytes
```

In the logs section `attrs_map_column` is write-side only. `ravel-cli export`
writes every record attribute that no `[[attribute]]` entry covers into that
one map column, and `ravel-cli load` ignores it (see
[What round-trips](#what-round-trips)). The `[spans]` section takes the same
key, and a spans load reads it back (see [Loading spans](#loading-spans)).

**Date columns.** Map an Arrow `Date32` or `Date64` source column with
`type = "i64"`. The value is stored in its native unit, unchanged: a
`Date32` stores **days since the Unix epoch**, and a `Date64` stores
**milliseconds since the Unix epoch**. Neither is rescaled to nanoseconds.
Neither can be the `ts_column`: a date is not a valid event-time source, and
the loader rejects it there. A query that compares against a mapped date
column compares against that raw day or millisecond integer, not a
timestamp. For example, a `Date32` for 2024-05-16 is the integer `19876`.

### Loading metrics

`--signal metrics` provisions or validates the metrics signal of the tenant.
It builds an `IngestRouter` from the same configuration that the logs load
uses. It writes every batch with strict acknowledgement, as the logs path
does. The mapping and the normalization differ.

```toml
[metrics]
name_column  = "metric"   # a column, OR name = "http.server.duration"
value_column = "value"
ts_column    = "ts"
ts_unit      = "millis"   # seconds | millis | micros | nanos
unit         = "s"        # optional UCUM unit (see below)
kind         = "counter"  # optional: gauge (default) | counter

[[metrics.label]]
name   = "job"
column = "svc"

# Optional classic-histogram shape. With it, one input row is one bucket.
[metrics.histogram]
le_column    = "le"
sum_column   = "sum"
count_column = "count"
```

A null label cell is omitted from the series. So is a cell that holds the
empty string: every ingest path drops an empty attribute value before it
builds the label set, so `{job=""}` and `{}` are one series. A non-string
label column is stringified. A float goes through the same Go-compatible
formatter that the `le` label uses, so two columns that carry the same
number never produce two series.

#### Metric name normalization

**A metric loaded here lands on the same `SeriesId` as the same metric
admitted over OTLP.** A bulk import and a live OTLP feed of the same metric
are therefore one series. The loader applies the OTLP name pipeline
(`ravel_otlp::normalize`) in the same order:

1. The raw name is checked against the metric-name length cap.
2. Every character outside the Prometheus metric-name set is rewritten to
   `_`, so `http.server.duration` becomes `http_server_duration`. Each
   mapped label name goes through the same rewrite for label names, so
   `http.method` becomes `http_method`.
3. The UCUM value of the `unit` key selects a suffix by the
   OTLP-to-Prometheus unit table (`s` gives `_seconds`, `By` gives `_bytes`,
   `1` gives `_ratio` on a gauge). The suffix is appended unless the name
   already ends with it.
4. `kind = "counter"` sets `is_monotonic_sum` on every point and appends
   `_total`, as a monotonic OTLP `Sum` does, again unless the name already
   ends with it.

So `name = "http.server.request"`, `unit = "1"`, `kind = "counter"` stores
`http_server_request_total`.

Declare `unit` when the source data has one. A mapping that omits it stores
an unsuffixed name, which is a *different series* from the same metric that
arrives over OTLP with its unit set. See
[Metric metadata and OTLP name suffixing](#metric-metadata-and-otlp-name-suffixing)
for the suffixes.

`kind` cannot be set together with `[metrics.histogram]`. The combination is
refused. OTLP has no monotonic histogram: every series that a classic
histogram explodes into is non-monotonic, and its family name takes no
`_total`.

#### Classic histograms

With `[metrics.histogram]`, **one input row is one bucket of one data
point**. A data point is a contiguous run of rows that share a metric name,
label set and `ts`:

| ts | le | value | sum | count | svc |
| --- | --- | --- | --- | --- | --- |
| 1700000000000 | 0.1 | 2 | 12.5 | 7 | api |
| 1700000000000 | 1.0 | 3 | 12.5 | 7 | api |
| 1700000000000 | 10.0 | 1 | 12.5 | 7 | api |

- The `value` column is the **own** count of that bucket (the OTLP
  `bucket_counts` convention), not a running total. The loader accumulates.
  De-accumulate an already-cumulative Prometheus `_bucket` export before you
  load it. The three rows above store cumulative bucket values 2, 5, 6.
- `le` is the explicit upper bound of that bucket and must be finite. The
  `+Inf` bucket is **not a row**. The loader synthesizes it from the `count`
  column, as in OTLP, where `explicit_bounds` carries only the finite
  bounds.
- Bounds must strictly increase in row order. A data point can carry at
  most 160 of them (the OTLP `max_histogram_buckets` limit). The limit is
  checked as rows arrive. A mapping that makes a whole file one data point
  (a `ts_column` that names a constant, or a missing `[[metrics.label]]`) is
  refused at the 161st row of that group, and the refusal names its first
  row.
- `sum` and `count` describe the whole data point, so every row of one group
  must repeat the same values. A row that disagrees is refused, and the
  refusal names the first row of its group. A null `sum` cell emits no
  `_sum` series, like an OTLP data point with no `sum` field.
- The group explodes into `<name>_bucket` (one per bound plus `+Inf`),
  `<name>_sum` and `<name>_count`. These are the same series that the
  `explode_histogram` of `ravel_otlp::normalize` produces.

Counts are whole numbers. A fractional `value`, `count` or bucket count is
refused, not truncated. An integer column is read as an integer, so a count
above 2^53 survives.

**Sort the rows of each data point together.** The grouping reads a
contiguous run. A group whose identity was closed earlier in the file is
refused and not exploded twice, because two explosions of one `(series, ts)`
write two conflicting cumulative ladders. Interleaved bucket rows are a
rejection, not a silent misread.

A metrics load reads **one sequential cursor**, not the K stride cursors of
the logs path. A stride read interleaves far-apart file regions inside one
batch, which splits every contiguous run. `--read-cursors` and
`--decode-queue-batches` change nothing here, and the loader warns when
either was set to a value it ignores. A value of 0 for either is still
rejected, as on the logs path. Everything that shapes the objects
(`--shards`, `--batch-rows`, `--target-bytes`, `--max-inflight-flushes`,
`--max-flush-delay`, `--pipeline-depth`) applies unchanged.

A data point can span a batch boundary. The open group is carried into the
next batch and closed there. Its rows are credited to the write that carries
its points and to no earlier one. `rows_written`, and the
`next --skip-rows` offset that a failed run prints, therefore always land on
a group boundary. A resume at that offset loads the next data point whole,
not a truncated one.

#### Load-time bucketing

As for logs, a metric sample buckets by the *flush-open wall clock*, not by
its event time. For a thirty-day-old sample:

- The sample lands in the ingest hour of today.
- Retention and GC for it run from the **load hour**.
- A folded catalog sees the bulk objects in the load hour.
- The event range of the sample decides query overlap.

Query bulk-loaded metrics with a window that reaches now. Sort the input by
event time (see [Retention and sort order](#retention-and-sort-order)):
unsorted input costs every later query the fetch of the bulk objects.

### Admission rules on load

- **Future skew: kept.** The loader enforces the same `max_future_skew_ns`
  bound that OTLP does. If the loader relaxed this bound, a far-future
  record buckets by the wall clock of today while every later query lists
  from `query_range.start - max_ingest_lag`. That listing does not reach the
  bucket of today, so such a record is permanently undiscoverable.
- **A negative timestamp: refused, for all three signals.** The timestamps
  of OTLP are unsigned and have no negative to express.
  - A logs or metrics row whose `ts` falls before the Unix epoch is a row
    rejection. The rejection names the unit that the value was read in: the
    declared `ts_unit` for an integer column, or the unit of the column for
    a native Arrow `Timestamp` column, which the declared `ts_unit` does not
    rescale.
  - A timestamp of 0 is the epoch and loads.
  - A spans load refuses a negative start or end, and names each of the two
    in the unit that it was read in, by the same rule: `start_ts_unit` or
    `end_ts_unit` for an integer column, the unit of the column for a native
    Arrow `Timestamp` column.
  - An end cell of 0 takes the start. A negative start with a zero end
    therefore refuses an end that the end column does not hold. The refusal
    then says that the end was taken from `start_ts` because `end_ts` is 0.
  - A start cell of 0 takes the load time. A negative end beside a zero
    start therefore refuses a start that the start column does not hold. The
    refusal then says that the start was taken from load time because
    `start_ts` is 0.
  - A unit conversion never turns a positive value negative, so the refusal
    always means that the column holds a negative cell. A mis-declared unit
    lands rows at the wrong time without a refusal.
- **Length caps (attribute key length, attribute value length, body length):
  kept**, identical to those of the OTLP path. They bound field sizes for
  any sender, offline or not.
- **Past-event-time lag: relaxed (not enforced).** A backfill or migration
  needs its real event times. A record buckets by the *flush-open wall
  clock*, not by its event time, so an old-event record still lands in the
  ingest-hour bucket of today. The record is discoverable when the listing
  window of the query reaches that bucket. A query with a normal
  `start`/`end` window does reach it: that window is compared against
  event-range overlap, and the listing upper bound is
  `now + max_future_skew`, which reaches the bucket of today. See
  [late and skewed data](../consistency-model.md#late-and-skewed-data) for
  the paired admission and discoverability bound. **Query bulk-loaded data
  with a window that reaches now, not just the event times of the records.**
- **Per-record attribute cap: relaxed** to a loader-specific 1024, from the
  OTLP cap of the signal it stands in for: 128 attributes per log record, 64
  per metric data point, 128 per span. Bulk import is an offline action that
  an operator starts over a file that the operator controls, which is a
  different threat model than a networked sender. A row over the 1024 cap is
  rejected. The 1024 cap is a *per-record* axis. It is unrelated to the
  1000-distinct-`(name, type)` dynamic-column budget of the RLOG object.
  Past that per-object budget, extra columns fold into the `attrs_raw`
  overflow column of the object and are not rejected, as for OTLP-ingested
  data. A row within the 1024 cap whose columns push the object past 1000
  distinct columns is therefore not rejected.
- **Per-tenant admission control (active-stream cap, stream-creation rate,
  byte rate): bypassed by construction.** This control lives in the HTTP
  layer of the server, above the router that the loader calls directly. The
  loader does not go through it, and a single offline bulk load has no
  equivalent concept. Bulk-loaded volume is therefore not evidence that the
  limits of the admission controller were exercised. The CLI prints this
  warning before every run.

### `--batch-rows` and object count

`load` writes each batch as one Strict write. At the default
`--target-bytes 1`, each write flushes at once, and one flush is one RLOG
object per involved shard. So at that default `--batch-rows` sets the
batch size (default 10000) and with it how many RLOG objects a load leaves
behind. A 100M-row load at the default is on the order of 10000 flushes,
each an RLOG object per shard.

Object count is a first-order query-cost variable. Every later query over
the affected range pays the per-object cost (LIST, footer read, per-object
decode setup). A larger `--batch-rows` writes fewer, larger objects, with
less per-object overhead and more memory held per batch. A smaller one
writes more, smaller objects.

`--batch-rows 0` is rejected with an error.

### Object size by bytes: `--target-bytes`

`--target-bytes` above `1` makes each shard merge several batches' slices
into one buffer and flush it as one object once the buffer's estimate
reaches the target, so objects grow without growing the batch.

The target counts the buffer's estimated **uncompressed** content (body,
severity text, stream attributes, attribute names and values, and fixed
per-row fields), not the stored object size. Stored objects come out much
smaller. On ClickBench `hits.parquet` the estimate counts about 7.4 KB per
row and a stored object holds about 102 bytes per row, so stored objects
are about 65 to 75 times below the target: a target of 375,000,000 stored
5.7 MB objects (median, 66 times), 1,650,000,000 about 22 MB (75 times),
and 1,850,000,000 25.1 MB (mean, 74 times). The ratio is a
property of that corpus, not a rule; measure your own stored objects
before sizing the target from it.

A Strict write's ack waits for the flush that holds its rows, and the
loader keeps at most `--pipeline-depth` writes in flight, so one object
merges at most `--pipeline-depth` batches' slices. Above `1` a batch's ack
can wait for later batches to fill the buffer. Set `--pipeline-depth` to
at least the number of batches whose slices fill one object on a shard;
otherwise every flush waits out the router's age trigger
(`--max-flush-delay`, 2s by default). A buffer that never reaches the
target flushes on that trigger, or at the end of the input, so
`--max-flush-delay` must also be long enough for one object to fill. When
two checks 250 ms apart both find the decoder waiting for room in
`--load-memory-bytes` and the bytes charged not lower than at the first,
the loader flushes every shard buffer early, so a target the budget cannot
hold yields smaller objects rather than a stalled load.

#### Measured recipe for large objects

On ClickBench `hits.parquet`, loaded into S3-compatible storage on a
16-vCPU host with 128 GB of memory, this stored objects of 24.5 MB
(median; 25.1 MB mean) in 896 s at a 7.03 GB peak loader RSS:

```sh
ravel-cli load ... --batch-rows 100000 --target-bytes 1850000000 \
  --max-flush-delay 30s --pipeline-depth 32 --load-memory-bytes 6500000000
```

Each setting does a separate job, and dropping any one of them changes the
outcome:

- `--target-bytes 1850000000` stored 25.1 MB mean objects on this
  corpus, 74 times below the target: the run wrote 408 objects (the mean
  is over the 407 above 100 KB), split size 390, age 14, final 4, so the
  target is what closed almost all of them,
  not the age trigger. At 1,650,000,000 the objects were about 22 MB
  (median, 75 times below) and at 375,000,000 5.7 MB (66 times below).
  The ratio was measured at these three target values only; scaling
  `--target-bytes` above 1,850,000,000 for still larger objects was not
  measured.
- `--max-flush-delay 30s` lets a buffer live long enough to fill. At the
  2s default, 798 of 916 objects closed on age and the median object was
  10.2 MB.
- `--pipeline-depth` sets the load time once objects are large, because
  each Strict write waits for its batch's flush. At a 1,650,000,000
  target with the other flags above, depth 16 took 1,990 s, depth 24
  took 1,139 s and depth 32 took 858 s.
- `--load-memory-bytes` grows with the depth, since more batches are held
  at once: at a 1,650,000,000 target, 5,000,000,000 at depth 16 (4.34 GB
  peak loader RSS), 5,500,000,000 at 24 (5.77 GB) and 6,500,000,000 at 32
  (6.42 GB). At 1,850,000,000 and depth 32 the same budget peaked at
  7.03 GB.

Smaller hosts loaded the same corpus with `--batch-rows 100000
--target-bytes 1850000000 --max-flush-delay 30s`. The 16 GB host kept the
depth and budget above; the 8 and 4 GB hosts lowered both:

| host memory | other flags | load | peak loader RSS | median object |
|---|---|---|---|---|
| 16 GB | `--pipeline-depth 32 --load-memory-bytes 6500000000` | 1,203 s | 6.04 GB | 24.5 MB |
| 8 GB | `--pipeline-depth 24 --load-memory-bytes 3500000000` | 1,965 s | 4.92 GB | 23.9 MB |
| 4 GB | `--pipeline-depth 16 --read-cursors 2 --load-memory-bytes 1200000000` | 3,905 s | 1.95 GB | 17.1 MB |

On the 8 GB host the budget bound: the decoder waited 9 times. On the
4 GB host it bound hard: the decoder waited 187 times and the loader
flushed shard buffers early to make room, so objects there came out
smaller (2.9 MB at the 10th percentile).

### Load memory: `--load-memory-bytes`

A logs load holds its built batches under one byte budget,
`--load-memory-bytes`. Each batch is charged before it is built, at an
estimate from the previous batch's bytes per row (zero for the first
batch), and corrected to its measured in-memory size once built, so the
first batch and any batch wider than its estimate are built partly
uncharged. The charge stays through the decode queue, the write window
(`--pipeline-depth`), the shard buffers and the flush that writes its
rows. When the budget is full the decoder waits; it does not fail.

With `--load-memory-bytes` set, a load whose first batch alone does not
fit the budget is refused before
anything is written, with a message naming that batch's size, the budget,
where the budget came from and the floor, and suggesting a smaller
`--batch-rows` or a larger `--load-memory-bytes`. A later batch that does
not fit fails the load the same way once the writes already in flight
have resolved, and the failure reports what they made durable.

Unset, the budget is the host's memory (`MemTotal`, or a lower cgroup
memory limit) less an estimated floor for what batches do not account for:
128 MiB of process baseline, 32 MiB per read cursor and 72 MiB per
concurrent flush (`--shards` x `--max-inflight-flushes`). These are
estimates from profiling one corpus, not calibrated figures. Where host
memory cannot be read, the budget falls back to 4 GiB and a warning says
so. A derived budget smaller than one batch, on a small host or under a
large floor, does not refuse the load: the loader admits one batch at a
time and prints a warning naming the batch, the budget, its source and the
floor. Set `--load-memory-bytes` to get the refusal instead.

The load summary prints the budget, where it came from, the floor, the
peak bytes charged, the largest batch, and how many times the decoder
waited for room:

```text
  load memory      : budget <BYTES> bytes (<SOURCE>), floor <BYTES> bytes, peak <BYTES> bytes, largest batch <BYTES> bytes, decoder waits <N>
```

`<SOURCE>` is `--load-memory-bytes`, `host memory <BYTES> bytes less the
floor`, or `fallback, host memory unreadable`. The peak is the most bytes
charged at once, not held: it can exceed the budget when one batch alone
is larger than it. `decoder waits 0` means the budget never bound.

The budget covers batches, not the whole process, and memory outside it
still scales with `--batch-rows`: the read cursors' decode holds about one
batch of rows in total, and each flush's working set scales with its
per-shard slice. The floor constants were measured at 500,000-row batches.
Process memory is
roughly the budget plus the floor, plus whatever the object store holds:
`--store memory` keeps every written object in the process, so a
memory-store load grows past the budget with the data it writes.

The [measured recipe](#measured-recipe-for-large-objects) above gives the
peak loader RSS of each run against its budget. On the 4 GB host, under a
1,200,000,000-byte budget, the peak charge reached 1,198,284,910 bytes.
Size a host from the run whose target and depth you use.

A metrics or spans load ignores `--load-memory-bytes` and warns when it is
set.

### The dynamic-column budget and its warnings

Each RLOG object gives a typed column to the first `max_dynamic_columns`
(default 1000) distinct `(attribute name, type)` pairs that it holds,
ordered lexicographically by name. Anything past that budget folds into the
`attrs_raw` overflow column of the object. An overflowed attribute is
**still queryable through `attrs['<key>']`**. It gets **no typed column**,
so a typed predicate or aggregate over it is unavailable, and a SQL filter
over it pays a per-row string cast.

After a load, `load` reads the cumulative dynamic-column counters of the run
and prints one of two warnings to stderr when they apply:

- An **overflow** warning when any object crossed the budget. It names the
  count of overflowed `(name, type)` pairs.
- A distinct **near-cap** warning when nothing overflowed but the widest
  object reached **90% or more** of `max_dynamic_columns`.

Both name the same fix. Reduce the number of distinct attribute columns per
stream (map fewer columns, or split the load so that each object stays under
the budget), or accept `attrs`-only access for the overflow keys.

To give an overflowed key a typed column at query time, declare it with
`ravel-cli typed-attr-column set` (see
[query.md](query.md#declaring-typed-attribute-columns)). A declaration does
not change what the object already stored, so the flow is load, then
declare, then query.

### RLOG compression with `--zstd-level`

`--zstd-level <LEVEL>` (default `3`) sets the zstd level of every page and
section that the RLOG objects of a logs load compress with zstd.

- It accepts the range of zstd, -131072 to 22. A level outside it is refused
  before anything is read.
- A page or section is stored compressed only when that is smaller than
  storing it raw. The encoding of a page is chosen by stored size, so the
  level can also change which encoding a page keeps.
- zstd is lossless. The level changes object size and never the records
  read back.
- The level applies only to the objects that the load writes. Compaction
  decodes the records of the objects it merges and writes them again at its
  own level. A compacted object therefore carries no trace of the level of
  the load.
- A metrics or spans load ignores the flag and prints a warning when it is
  set to anything but 3.
- `ravel-server` has no matching flag yet. Its log flush writes at the
  default level.

### Clustering key and bloom scope

A tenant's config record can carry a clustering key and a bloom scope
(fields 13 and 14, see [catalog-and-mvcc.md](../catalog-and-mvcc.md)). Two
read-only commands print them:

```sh
ravel-cli clustering-key show --tenant acme
ravel-cli bloom-scope show --tenant acme
```

`clustering-key show` prints one of three states:

- never set: `never set a clustering key (clustering generation 0)`, also
  printed when the tenant has no config record.
- `has no clustering key at generation N (cleared, or never set and given a
  generation by a bloom scope change or by a typed attribute column change
  under the undeclared scope)`: the record carries field 13 with no columns.
- set: the generation, bucket width (`1h`, `6h` or `1d`) and one
  `column:type` line per key column in key order. The type is the declared
  type of the record for that column. A key that names a column that the
  override does not declare is refused.

A tenant with no typed attribute column override prints `deployment-default`
in place of each type. The declaration of the deployment lives in server
flags that the command cannot read. The key is then checked for shape
only (its generation, column count, duplicate columns and bucket width), and
a set key ends with a `note:` line that says so.

The note also says that the log ingest flush of this build resolves a key
against the override of the record alone. The flush therefore leaves such a
key unresolved and writes the log objects of the tenant without it.

`bloom-scope show` prints `all`, `undeclared` or `text`. A tenant with no
config record reads as `all`.

For either command, a failed read of the record, or a stored value that the
catalog refuses, is an error with a non-zero exit and nothing on stdout.

### Setting a tenant's clustering key and bloom scope

Three commands write the two fields:

```sh
ravel-cli clustering-key set --tenant acme --column region --column code \
  --bucket-width 6h --readers-rolled-out
ravel-cli clustering-key clear --tenant acme --readers-rolled-out
ravel-cli bloom-scope set --tenant acme --scope text --readers-rolled-out
```

Each command writes a version-3 config record. It swaps the record of the
tenant in place with `CasVersion` against the version that the command read,
and carries every other field through unchanged. When the tenant had no
record, it creates one with `lifecycle_state=active`.

If another writer changed, created or deleted the record between that read
and the write, the command fails with a CAS conflict error and writes
nothing. The error says to re-read and retry. Run the command again.

Each command ends its output with a `note:` line that names the staleness
bound described below.

`--readers-rolled-out` is required on all three. It asserts that every
process that reads the tenant config of this bucket runs a release that
reads record version 3. A process that does not refuses the record, and the
ingest and lifecycle of that tenant fail closed. Without the flag the
command exits 2 with a usage error, before any store request.

- `clustering-key set` takes one to four `--column` names in key order and a
  `--bucket-width` of `1h`, `6h` or `1d`. It sets the key at the stored
  clustering generation plus one. Every column must be a typed attribute
  column that the config record of the tenant declares (`ravel-cli
  typed-attr-column set`), each named once. A tenant with no typed attribute
  column override declares none, so every key is refused there. A refused
  key prints the reason of the catalog, exits non-zero and writes nothing.
- `clustering-key clear` removes the key at the stored generation plus one,
  so the clear ranks above every key set before it. It is refused, and
  writes nothing, when the tenant never set a key or the key is already
  cleared.
- `bloom-scope set --scope` chooses which string columns the BLOOM section
  of an RLOG object covers:
  - `all` (the default) covers `body`, `severity_text` and every string
    attribute column.
  - `undeclared` drops the string columns that the config record of the
    tenant declares as typed.
  - `text` covers `body` and `severity_text` only.

  A column outside the scope is scanned and not pruned on. A change
  increments the clustering generation and leaves the clustering key as it
  is. When the scope is already stored, the command prints that it is
  already set and writes nothing.

  Under `undeclared`, a config write that changes the set of declared typed
  attribute column names (`ravel-cli typed-attr-column set` adding or
  removing a column) also takes the next generation, because it moves which
  columns the BLOOM section covers. A retype or a reorder of the same names
  does not. A generation therefore names one key, one scope and, under
  `undeclared`, one set of typed attribute column names.

After a `set`, the RLOG objects that the log flushes of the tenant write
carry the key and the scope. This holds for ingest and for `ravel-cli load`
alike. The log ingest flush of a server has three exceptions:

- **Staleness.** A flushing process reads the record through the same
  bounded-staleness tenant config read that supplies its indexed fields. For
  up to its staleness horizon (60 s) after a change, it can still write the
  earlier key and scope.
- **An unresolved layout.** A flush that cannot resolve a layout writes the
  unkeyed default: no sort descriptor, generation 0 and a BLOOM section over
  every string column. Three things make a layout unresolved:
  - a key column that the typed columns of the record do not declare
  - a key that the RLOG writer refuses to record
  - a scope value that the build does not know

  Each such flush adds one to the `ingest_clustering_key_unresolved_total`
  count of the tenant. See [ingest.md](../ingest.md) for when a layout is
  unresolved and where the count is read.
- **A failed config read.** While its config read fails, the flush serves
  the layout that it last read, however old, or the default layout when it
  never read one.

With a key, the rows of an object sort by stream, time bucket, the key
columns and then timestamp. The footer records the key as a sort descriptor
beside the clustering generation. Objects already written keep their order
and their filters until compaction rewrites them. L1 compaction and the
erasure rewrite take the sort descriptor and bloom coverage of the input
with the highest generation. They re-sort every part by that descriptor and
compress at zstd level 9.

A worked example follows. It declares two typed columns, clusters on both at
6h, and keeps the blooms on the text columns. It then loads, and inspects
one of the objects that the load wrote
([inspecting-data.md](inspecting-data.md) lists the objects of a tenant).

```sh
ravel-cli typed-attr-column set acme region:str code:i64
ravel-cli clustering-key set --tenant acme --column region --column code \
  --bucket-width 6h --readers-rolled-out
ravel-cli bloom-scope set --tenant acme --scope text --readers-rolled-out
ravel-cli load --parquet events.parquet --tenant acme --mapping map.toml
ravel-cli rlog inspect "t/<tenant hash>/l/l0/<shard>/<object>.rlog"
```

The `clustering-key set` sets generation 1 and the scope change takes
generation 2, so the inspected object reads, among its other lines:

```
version: 5
sort_descriptor: bucket_width=6h key_columns=2
  key[0] name=region type=str
  key[1] name=code type=i64
clustering_generation: 2
bloom_coverage (2 column(s)):
  column_id=4 name=severity_text kind=fixed
  column_id=5 name=body kind=fixed
```

A query that filters on `region` or another string attribute column returns
the same rows as before. It scans that column and does not prune on its
filter.

### Failed loads

A row that fails a kept check (future skew, a length cap, or the 1024
attribute cap) is rejected **fail-fast**. The run stops at the first bad
row, prints a per-row error, and exits non-zero. Batches durable before that
row stay durable.

A failed flush (an object-store PUT failure) exits non-zero. A failure
mid-file is a **partial load, not a rollback**. The run prints:

- the commit tokens durable from batches completed before the failure
- any shard of the failing batch that acked its commit durably before a
  sibling shard failed

The loader shards a batch across the shards of the target signal and waits
for the ack of every shard. When one shard fails while a sibling committed,
the token of the sibling is recovered from the write error and reported. The
printed list is **exact** for that partial-flush case.

The list is a lower bound only when the ack round of the failing batch did
not resolve at all: an ack-deadline timeout, or a shard channel that died at
send time. No per-shard ack is observed then, and a commit can land without
an observable ack.

#### Resume with `--skip-rows`

`--skip-rows N` drops the first `N` rows of the file, by file-absolute
position, before any row reaches mapping or admission checks. The drop is
exact at any setting, because the position of each row in the file decides
it. It does not depend on how many read cursors are open or in what order
they hand rows out. Every run reports how many rows it dropped as
`rows_skipped`, alongside `rows_written`. A run that fails mid-file prints
both figures with the error.

**`rows_skipped + rows_written` is a valid resume offset only when the failed
run used `--read-cursors 1 --pipeline-depth 1`.** With those two settings,
the rows that landed are a contiguous prefix of the file:

- With K read cursors the loader reads K far-apart partitions of the file
  concurrently. At any instant the landed rows are spread across K regions,
  not one leading run.
- At a pipeline depth above 1 several writes are in flight at once, and a
  batch submitted after the failing one can still commit. The loader waits
  for those writes and reports their commit tokens, but they sit after the
  gap that the failure left.

Under either, the landed set has holes, and no single offset describes it.
A re-run with `rows_skipped + rows_written` then re-ingests rows that
already committed and skips rows that never landed anywhere.

To make a bulk load resumable, run it with
`--read-cursors 1 --pipeline-depth 1` **from the start**. That is slower,
because it gives up the concurrent reads and the overlapped writes:

```sh
ravel-cli load --parquet hits.parquet --tenant acme --mapping hits.toml \
  --read-cursors 1 --pipeline-depth 1
# ... fails, printing rows_skipped 0, rows_written 4200000
ravel-cli load --parquet hits.parquet --tenant acme --mapping hits.toml \
  --read-cursors 1 --pipeline-depth 1 --skip-rows 4200000
```

A `--skip-rows` larger than the row count of the file is reported. The load
exits 0 having written nothing, and says which offset was asked for against
how many rows the file holds. A value equal to the row count is the
completed-resume case and stays silent.

Even at those two settings the offset is a floor, not an exact boundary. One
batch spans every shard that its rows hash to, and the batch that failed can
have committed on some of those shards and not others. Those rows are in the
printed durable token list and are not counted in `rows_written`, so a
resume at `rows_skipped + rows_written` re-ingests them. The error is
one-sided: the offset never skips a row that landed, and can only repeat
one. A duplicate is visible in the data where a gap is not.

A metrics load always reads one cursor, so only `--pipeline-depth 1` is left
to set. On a classic-histogram mapping the offset is also a group boundary:
`rows_written` counts only the rows of data points whose points a write
acked. A resume therefore loads the next data point whole and does not start
part-way through its buckets (see
[Classic histograms](#classic-histograms)).

At the default settings, use `--skip-rows` as a positional tool. Split one
file across several runs at offsets that **you** chose (rows `0..10000000`
in one run, `--skip-rows 10000000` in the next). The boundary is then known
up front and not inferred from a crash.

In both uses this is **not** deduplication and carries **no idempotency
marker**. The loader trusts the offset that it is given and cannot check it
against what landed. A value that is too low re-ingests rows that already
committed and duplicates them. A value that is too high silently drops rows
that never landed anywhere. The operator is responsible for the offset.

### Retention and sort order

Retention and GC key on ingest-hour buckets, which the loader derives from
*load* time. A bulk-loaded record with an old event timestamp is therefore
retained for the full retention window measured from its load time, not from
the real age of the data.

An RLOG object that spans a wide event-time range overlaps the event range
of every later query at resolve time. With unsorted input, every later query
over the affected stream fetches the bulk-loaded objects, for any query
window. Sort the input by event time before the load where the mapping
allows it. This is a performance recommendation, not a correctness
requirement.

## Bulk export (`ravel-cli export`)

`export` is the inverse of `load`. It reads the stored records of a tenant
from object storage and writes them to a Parquet file. The same `--mapping`
TOML decides which column each field lands in.

```sh
ravel-cli export --signal logs --tenant acme \
  --start 2024-01-01T00:00:00Z --end 2024-01-02T00:00:00Z \
  --parquet acme-day.parquet --mapping map.toml --shards 4
```

Run with the same mapping that a load of that data used, it produces a file
that `ravel-cli load` reads back:

```sh
ravel-cli load --parquet acme-day.parquet --tenant acme-copy --mapping map.toml
```

A metrics export can refuse instead. See [Metrics export](#metrics-export).

**Logs, metrics and spans.** `--signal` has no default and accepts `logs`,
`metrics` and `spans`. The `--mapping` file must carry the section that the
signal names, under the same section rules that `load` applies. The
subsections apply as follows:

- The window, memory, listing-window and deletion subsections, and the
  `--shards` and `--parquet` notes, apply to every signal.
- The sort-order subsection applies to logs and metrics.
- [What round-trips](#what-round-trips) is about logs, except where a bullet
  says otherwise.
- `--signal metrics` has its own rules for duplicates, names and what
  round-trips, in [Metrics export](#metrics-export).

### Spans export

`--signal spans` exports every stored span whose start time falls in the
window: one row per span, with no deduplication, sorted by start time, then
trace id, then span id.

- Every `[spans]` field is written: the trace, span and parent ids (a null
  parent is a root span), the name, `start_ts` and `end_ts` each in its own
  declared unit, and the status code and message.
- Each mapped attribute is written in its declared type. Spans store
  attributes as strings, so an attribute is written as the typed value that
  a load turns back into the same string.
- Every mapped field round-trips. The export refuses the whole window by
  name, and writes nothing, when a mapped field of a span does not re-load
  as stored. The cases are:
  - a start or end finer than its declared unit
  - a start that a load re-times or refuses
  - a mapped attribute whose stored string its declared type cannot
    reproduce (`"007"` declared `i64`)

  Refusals are counted per kind and name the first offending span in output
  order.
- With `attrs_map_column` set, the export writes every stored attribute
  that the mapping does not name into that one map column, as stored. The
  reserved attributes are the exception, and the per-span cap of the load
  limits the column. A load refuses a row whose
  `[[spans.attribute]]` values and map entries together exceed 1024. The map
  therefore holds at most 1024 less the written `[[spans.attribute]]` values
  of the span, and keeps the entries first in ascending byte order of key.
  A load under the same mapping reads each written entry back at its stored
  string. A span that lost entries to the cap is counted like any other
  loss.
- What the file cannot carry is written without, not refused. The report
  line `spans_with_unwritten_data` counts the spans that lost some of it.
  The full list is in the `ravel-cli` export module documentation.

### Export window

`--start` and `--end` are RFC 3339 instants and the window is **half-open**:

- Each takes a trailing `Z` or a numeric offset such as `+02:00`. An offset
  is converted to UTC, so `2024-01-01T02:00:00+02:00` and
  `2024-01-01T00:00:00Z` name the same instant. A timestamp with no offset
  is refused.
- A record is exported when its event time is at or after `--start` and
  strictly before `--end`. An export of one day and then of the next day
  with adjoining bounds therefore covers both days, with no row written
  twice and none dropped between them.
- `--end` must be after `--start`. An empty window is refused, not reported
  as a successful export of nothing.

The catalog is resolved once, at one snapshot, and every object that the
export reads comes from that resolution. A compaction or a flush that lands
while the export runs does not change what it writes.

Garbage collection is the exception. The snapshot names objects and does
not hold them. If a GC pass deletes an object that this snapshot already
named, the GET of that object fails with not-found, and the whole export
fails with that error. An example is an object that a compaction superseded
shortly before the export started. The export does not retry and does not
skip the object, because a skip writes a file with missing records and
nothing in the output to say so. Rerun the export. The fresh resolve does
not name the deleted object.

### Memory use

The output is sorted by event time, so every record in the window is decoded
and held in memory before the first row is written. Peak memory is
proportional to the record count of the window, not to the batch size of the
output file. There is no spill to disk. Export a wide range as several
narrower windows. The half-open bound makes that safe.

### Listing window and ingest lag

On a default deployment, no extra flag is needed. If the server runs with a
raised `--max-ingest-lag`, pass the same value to `export`.
`--max-ingest-lag` takes the same humantime duration that
`ravel-server --max-ingest-lag` does, and refuses zero as the server does.
Nothing on the bucket records what the server was configured with.

The catalog lists ingest-hour buckets from `--start` minus `max_ingest_lag`
forward, and `export` defaults that lag to the same 2 hours as the server.
The reach-back finds the bucket of a record whose event time falls in a
later ingest hour than the bucket that it was written into.

With a narrower lag, `--start` minus the lag no longer reaches that earlier
hour. The listing starts in the hour of the export window, that bucket is
never listed, and the export reports a clean, short result. An export must
therefore resolve with the same value that the resolves of the server use.
Otherwise it answers a different window than a query over the same range.

### Row order

Rows are written in event-time order, regardless of the order in which the
underlying objects hold them. A later load of the file wants that order (see
[Retention and sort order](#retention-and-sort-order)). The command prints
`rows_written` along with how many segments it read and how many the catalog
pruned. A window that reached nothing therefore says so, and does not leave
an empty file unexplained.

### Deleted data

An export reads through the same visibility rules that a query does. A row
that a query cannot see is a row that the export does not write.

- Records dropped by retention, and objects superseded by compaction, are
  already absent from the snapshot that the export resolves.
- Subjects with an erasure request in flight are excluded from the decoded
  records. The export uses the same predicates and the same function that
  the SQL log scan applies to logs and the SQL spans scan applies to spans.
  A subject erased but not yet rewritten out of its objects is therefore not
  exported.
- The predicates are matched against the merged resource, scope and record
  attributes of each record or span, as a query sees them. A subject named
  only in a resource attribute (a `[[resource_attribute]]` mapping entry, or
  an OTLP resource attribute) or a scope attribute is excluded too. For
  spans, a request with a time window is matched on the start time of the
  span.
- For metrics, the same predicates are matched against the labels of each
  series, by the same functions that the query engine applies to its fetch.
  A request with no time window excludes every sample of a matching series.
  A windowed request excludes the samples of the matching series inside its
  window.

### What round-trips

For logs, every field that the mapping names round-trips: event time, body,
severity number and text, trace and span ids, and each declared resource
attribute and typed attribute column. A record that has no value for a
mapped attribute key gets a null in that column, and a later load reads the
null back as the same absent attribute. An attribute stored under a type
that the mapping does not declare for that key is refused by name, not
written as a null.

A logs round trip has four limits. Only the last applies to metrics as well:

- **Only what the mapping names.** A resource or record attribute that the
  mapping does not declare is not in the output. Setting
  `attrs_map_column = "attrs"` adds one `Map<Utf8, Utf8>` column that holds
  every record attribute that no typed column covers, stringified the way
  SQL stringifies `attrs['<key>']`. That column is for reading the data
  elsewhere. `load` does not read it back, so attributes that reach the file
  only through it do not survive a reload.
- **Some stored fields have no mapping key.** No mapping key can name the
  observed timestamp of a record (`observed_ts_ns`), its `flags`, its
  instrumentation scope name and version, or its scope attributes. The
  export drops them. A reload stores the observed timestamp equal to the
  event time, flags of zero, and an empty scope, which is what `load` writes
  for any Parquet file.
- **`ts_unit` truncates.** The timestamp column is written in the unit that
  the mapping declares. A mapping with `ts_unit = "millis"` writes
  millisecond values, and a reload of that file gets timestamps truncated to
  the millisecond. Use `ts_unit = "nanos"` when the round trip has to be
  exact. A metrics export refuses such a sample instead (see
  [Metrics export](#metrics-export)).
- **Retention restarts on reload.** A load of an exported file is an
  ordinary bulk load. The reloaded records bucket by the time of the new
  load, not by their event time or their original ingest hour. This holds
  for metrics too.

### Shards and output file

`--shards` is the configured shard count of the tenant, the same value that
a load of that tenant uses. The durable provisioning record of the tenant
supplies the real per-hour shard generations on top of it.

`--parquet` names the output path. The export replaces an existing file, but
only once it has finished:

- The rows go to a temporary file beside the target, named
  `.<file name>.<pid>.<n>.tmp`.
- After the Parquet writer closes, the temporary file is synced to disk and
  renamed over the target. The directory is synced after the rename, so the
  replace survives a power loss.
- An export that fails part-way leaves the previous file as it was, not a
  truncated file with no footer.
- The temporary file is removed on every failure path that the export
  returns from. A SIGINT or a panic mid-write leaves it behind.

The replace is a rename and not a write into the existing file. As a result:

- A symlink at `--parquet` is itself replaced by the new file. The file that
  it pointed to is left unchanged.
- The mode of the new file comes from the default creation mode and your
  umask, not from the file that it replaces. The owner and ACLs of the old
  file are not carried over.
- The path must name a regular file in a writable directory. A path under
  `/dev` (such as `/dev/stdout`), an existing directory, and any other
  existing non-regular file are refused before the export reads anything
  from object storage.

### Metrics export

```sh
ravel-cli export --signal metrics --tenant acme \
  --start 2024-01-01T00:00:00Z --end 2024-01-02T00:00:00Z \
  --parquet acme-metrics.parquet --mapping metrics.toml --shards 4
```

The output carries the columns that the `[metrics]` section names:

- `ts_column` as an integer in `ts_unit`
- `name_column` as a string (absent when the mapping uses a `name` literal)
- `value_column` as a 64-bit float
- one string column per `[[metrics.label]]`, null where the series does not
  carry that label

The command prints `rows_written`, `series_written`, `series_skipped`,
`segments_read`, `segments_pruned`, `erasure_predicates` and
`samples_deduplicated`.

**One row per series and timestamp.** The store can hold more than one sample
for a series at one timestamp: the same file loaded twice, or two writers
that send the same point. A query serves one of them, the one from the most
recent write. Writes are ordered by creation time, writer epoch and
sequence, then by the position of the sample in the write. Among exact ties
on that order, the query serves the sample whose value has the greatest bit
pattern.

The export writes that same sample and counts every other one in
`samples_deduplicated`. Two loads of one file therefore export as one row
per sample, whether the two copies carry the same value or not. Values are
compared by bit pattern throughout: a NaN keeps its payload, and `-0.0` is
written as `-0.0`.

**Names are written so that a load lands on the same series.** `load` does
not store the names that a file carries as written. It sanitizes metric and
label names the way OTLP does, appends the unit suffix for the `unit` of a
mapping and `_total` for `kind = "counter"`, and drops an empty label value.
A stored name already carries those suffixes. For each series, the export
therefore writes the name that a load with the same mapping turns back into
the stored one:

- The stored name itself, when the load leaves it unchanged. That is every
  stored name under a mapping with no `unit` and no `kind`. Under a mapping
  with either, it is every name that already ends in what the load appends
  (`cpu_seconds` under `unit = "s"`, `requests_total` under
  `kind = "counter"`).
- Otherwise the stored name less its trailing `_total`. A counter with a
  unit is stored as, for example, `net_rx_bytes_total`. A load appends
  `_bytes` to that again, because it does not end in `_bytes`. The export
  therefore writes `net_rx_bytes`, and the load adds `_total` back.
- Otherwise the stored name less its unit suffix, or less both the unit
  suffix and `_total`. This reaches a name that the suffixes took past the
  512-byte metric-name limit. A raw name of 510 bytes loaded under
  `unit = "s"` and `kind = "counter"` is stored 524 bytes long, over the
  limit that a load applies to the name cell. The export writes the 510-byte
  name, and the load appends `_seconds_total` again.

Each name is checked against the naming rule of the load before anything is
written. A series that no candidate reproduces is refused by name, with the
name that a load gives it. For example, a series stored as `cpu` cannot be
exported under a mapping with `unit = "s"`, because a load names it
`cpu_seconds`. Export with the `unit` and `kind` that the data was loaded
with, or with neither. With neither, every sanitized name loads back
unchanged.

Under a `name` literal the file has no name column, because a load names
every row from the literal. Only the series that the literal names are
exported. Series with other names in the window are counted in
`series_skipped` and are not written under the name of the literal.

The other refusals follow the same rule: the export never writes a file
that re-loads onto different series.

- A series that carries a label that no `[[metrics.label]]` names is
  refused, because a load of the file drops that label. A mapped label name
  matches a stored one after sanitizing, so `host.name` in the mapping
  matches a stored `host_name`.
- A sample whose event time is not a whole number of the `ts_unit` of the
  mapping is refused, not truncated, because the truncated sample re-loads
  at a different timestamp. Use a finer `ts_unit`.
- A `[metrics.histogram]` mapping is refused. The load explodes each of its
  rows into `_bucket`, `_sum` and `_count` series and accumulates the bucket
  counts, and nothing stored says which of those series were one data point.
  To round-trip classic-histogram series, export them with a scalar mapping
  that has no `[metrics.histogram]`, no `unit` and no `kind`, uses
  `name_column`, and has a `[[metrics.label]]` for `le`. A load of that file
  with the same scalar mapping reproduces the same series.
- A series that holds native (exponential) histogram samples in the window
  is refused, because no mapping can carry them.

A refused export writes no file. With those refusals, a load of an exported
file with the same mapping, into any tenant, reproduces every exported
sample that the reload admits. The label sets, the timestamps and the value
bit patterns are the same.

The reload applies the row checks that every load applies, and the export
does not repeat them. A series written through another ingest path can hold:

- a label value longer than the label value length limit
- more labels than the per-record cap of the loader
- a timestamp before the Unix epoch

Any of these stops the reload at that row with a `row N:` error that names
the reason. Batches before that row stay loaded, and the error lists their
commit tokens.

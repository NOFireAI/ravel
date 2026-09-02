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

Authentication, the strict/buffered mode header, the commit-token
header, and the status-code mapping are identical on all six. A log or trace
export is a metrics export with a different payload and a different keyspace
underneath.

## Compressed requests

Ravel accepts gzip-compressed OTLP bodies on both transports. A stock
OpenTelemetry Collector and a stock Grafana Alloy both default to gzip, so no
client configuration is needed.

### OTLP HTTP

`POST /v1/metrics`, `/v1/logs`, and `/v1/traces` dispatch on `Content-Encoding`:

- `gzip`, or its RFC 9110 alias `x-gzip`, is decompressed and then decoded.
  The comparison is case-insensitive: `GZIP`, `gzip`, and `X-Gzip` are the same
  coding.
- An absent header, an empty value, or `identity` is the body as-is, byte for
  byte the uncompressed path. An uncompressed client sees no change.
- Any other single coding (`deflate`, `br`, ...), and any multi-coding list
  (`gzip, gzip`, `deflate, gzip`), is rejected with `415 Unsupported Media
  Type`. Ravel chains no decoders and never guesses at one member of a list.
  The 415 body names what is supported.

Two independent size caps bound a gzip request:

- The **compressed** body is capped at 16 MiB by the existing wire body limit
  (`DefaultBodyLimit`). This is the same Layer 1 cap that bounds an
  uncompressed body.
- The **decompressed** body is capped at 64 MiB, returning `413 Payload Too
  Large`. The cap is enforced *while* the body is inflated, not after: the
  decoder is read through `take(cap + 1)`, so a decompression bomb is refused
  as it expands rather than after 64 MiB has been allocated.

The two caps are independent, exactly as they are for Remote Write. A 16 MiB
compressed body that would inflate past 64 MiB is a 413; a body over 16 MiB
compressed never reaches the decompressor.

A gzip body is decoded as a whole stream with multi-member semantics: a
concatenated multi-member gzip stream (legal gzip that ordinary tooling
produces) is decoded in full, under the single 64 MiB cap across all members.
Trailing bytes after a well-formed stream ends are `400 Bad Request`, never
silently truncated: silent truncation would be data loss on an acknowledged
write.

### OTLP gRPC

The three gRPC services accept gzip via tonic's `accept_compressed`. Here the
decompressed size is bounded at **16 MiB**, not HTTP's 64 MiB. tonic applies
`max_decoding_message_size` to both the compressed frame and the decompressed
output (checking the framed length first, then limiting the inflate buffer to
the same value), so the one 16 MiB knob caps both halves. Raising it to match
HTTP would also raise the ceiling on uncompressed gRPC messages, which is a
separate change; Ravel accepts the asymmetry. A batch that inflates to
40 MiB is accepted over HTTP and rejected over gRPC with `resource_exhausted`.

Ravel does not compress responses (`send_compressed` is off): OTLP export
responses are small partial-success records, so compressing them buys nothing.

### Byte-rate charging differs by path

The per-tenant ingest byte rate (Layer 2) is charged on **different bases** by
path, and an operator sizing limits must know it:

| Path | Charged quantity |
|---|---|
| OTLP HTTP / gRPC | Decompressed size |
| Prometheus Remote Write | Compressed size |

OTLP charges the decompressed size so that two tenants
sending identical telemetry are charged identically regardless of a client-side
compression setting. Remote Write still charges the compressed body length;
this asymmetry is deliberate, and the inconsistency is acknowledged
rather than papered over. The practical effect: a gzip OTLP client's
effective byte-rate allowance drops relative to an uncompressed client of the
same nominal rate, because it is now charged for what it actually sent rather
than what it put on the wire.

To keep rejection cheap for an already-over-rate tenant, the gzip path does a
compressed-size pre-check first: the compressed length is a strict lower bound
on the decompressed length, so if even the compressed size exceeds the tenant's
available tokens the request is rejected `429 Too Many Requests` before
anything is decompressed and without consuming tokens. Only if the pre-check
passes is the body inflated and the real charge made on the decompressed size.

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
  loses that buffered window, bounded by `max_flush_delay` (2s default).
  Ravel issues no commit token, because there is nothing yet to point one at.

To use buffered mode for one request, send `x-ravel-ingest-mode: buffered` as
an HTTP header or as gRPC metadata on the export. For strict mode, omit it or
send any other value. Ravel honors
the header for any tenant; there is no per-tenant setting that enables or
refuses buffered mode.

## Partial success and rejections

A single `ExportMetricsServiceRequest` can contain a mix of good and bad
data points. Ravel never silently drops a point. It counts every rejected
point (or group of points) and returns it in the OTLP
`ExportMetricsPartialSuccess` message, with `rejected_data_points` and a
combined `error_message`. Ravel still ingests and acknowledges the admitted
points normally.

Every rejection reason:

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
| `UnsupportedTemporality` | A Sum, Histogram, or ExponentialHistogram metric has delta (or unspecified) temporality. Ravel accepts only cumulative aggregations. |
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

## Metric metadata and OTLP name suffixing

Ravel captures each metric's type, help, and unit at ingest time and serves it
back through [`GET /api/v1/metadata`](query.md#get-apiv1statusbuildinfo-and-get-apiv1metadata).
Every path supplies it: OTLP reads `Metric.description` (help) and `Metric.unit`
alongside the name and infers the type from the data shape; Remote Write v1 and
v2 stop discarding the `MetricMetadata`/per-series metadata they already parse.
Capture is best-effort and off the acknowledgement path, so a point is acked on
its data write and never waits on or fails from the metadata record.

OTLP metric names also get the standard OpenTelemetry-to-Prometheus suffixes a
Prometheus exporter would add, so the same metric ingested over OTLP or scraped
through a collector lands under one series name. The unit is mapped and appended
(`s` becomes `_seconds`, `By` becomes `_bytes`), then a monotonic `Sum` gets
`_total`: a monotonic counter named `foo` with `unit: "By"` ingests as
`foo_bytes_total`. This is an ingest-time transform on the name string only; it
does not touch series identity or any stored format. It is a one-time,
intentional naming change for dashboards built directly against the unsuffixed
OTLP names.

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

Defaults. No `ravel-server` flag configures them:

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

Logs and spans enforce the same window at admission. For a
**span**, the bounded timestamp is its **end** (`end_ts_ns`), on both edges,
and `end_ts < start_ts` is rejected outright. The lag bound anchors on the
end, not the start: a long-running span that
started more than `max_ingest_lag_ns` ago but ended within the window is
admitted; only a span reported more than `max_ingest_lag_ns` after it *ended*
is `TooOld`. The listing window stays sound because any span overlapping a
query range has its end at or after the range start.

Ravel also checks its own receiver clock at admission: a reading
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
the strict ack returns. **Logs are queryable over SQL**: the `logs` table is
registered on the `POST /api/v1/sql` endpoint, and
[query.md](query.md#sql-over-samples-logs-and-spans) documents its schema and usage.
PromQL does not query logs, so log data is reachable only through SQL. You can
also read a log object back directly with `ravel-cli rlog inspect`
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

Every rejection reason:

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

## Bulk import (`ravel-cli load --parquet`)

OTLP is the only *networked* way to write logs. For loading an existing
structured dataset offline (a Parquet export, an archive migration, a
historical backfill), `ravel-cli load` imports a Parquet file into the logs
signal:

```sh
ravel-cli load --parquet events.parquet --tenant acme --mapping map.toml --shards 4
```

The loader is an in-process caller of the same log ingest router OTLP uses --
the same shard actors, flush cadence, and commit protocol, not a parallel write
path. It builds the router against the target tenant's provisioned shard count
(validated against, or written to, the durable provisioning record exactly as
the server does at first touch) and writes with strict acknowledgement, awaiting
every write before it exits, so a run that returns success has no
buffered-but-unflushed data.

### The columnar fast path

The Parquet a load reads is already columnar, and the RLOG object it writes is
columnar too. The loader builds the storage-native columnar batch directly from
each Arrow `RecordBatch` and hands it to the router as a column batch,
skipping the per-row record pivot the record path builds and the per-row
gather the writer runs to undo it. Every Arrow downcast and the
`ts_unit` scaling are resolved once per column, stream identity is hashed once
per distinct resource-attribute tuple rather than once per row, and admission
checks (future skew, the length caps) are applied over whole columns while still
reporting a rejected row by its absolute file index. The commit protocol, object
key layout, strict-ack contract, and the RLOG format itself are unchanged: this
is a CPU path, and a columnar load writes byte-for-byte the same objects a
record-path load would.

When a mapped string column arrives **dictionary-encoded** -- an Arrow
`Dictionary` column, which a Parquet file carries when its writer embedded Arrow
dictionary schema metadata -- the loader passes the dictionary through so the
writer pays string encoding and token-bloom cost once per distinct value instead
of once per row. A plain `BYTE_ARRAY` string column (the common case for a
Parquet file not written by Arrow, including a dictionary-page-encoded one whose
file carries no Arrow schema) decodes to a plain Arrow string column and stays on
the per-row string path; both produce identical output.

### The `--mapping` TOML

The mapping declares how source Parquet columns become record fields. Resource
attributes determine stream identity and are declared separately from
record attributes, which never enter identity:

```toml
ts_column = "timestamp"
ts_unit   = "millis"        # seconds | millis | micros | nanos

body_column            = "message"   # optional
severity_number_column = "sev_num"   # optional (integer column)
severity_text_column   = "level"     # optional (string column)
trace_id_column        = "trace_id"  # optional (16-byte binary or 32-hex string)
span_id_column         = "span_id"   # optional (8-byte binary or 16-hex string)

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

**Date columns.** An Arrow `Date32` or `Date64` source column is mapped with
`type = "i64"`, and its value is stored in its native unit, unchanged: a
`Date32` stores **days since the Unix epoch**, a `Date64` stores **milliseconds
since the Unix epoch**. Neither is rescaled to nanoseconds, and neither may be
used as the `ts_column` (a date is not a valid event-time source; the loader
rejects it there). So a query comparing against a mapped date column compares
against that raw day or millisecond integer, not a timestamp -- e.g. a `Date32`
for 2024-05-16 is the integer `19876`.

### Which admission rules this path keeps, relaxes, and bypasses

- **Future skew: kept.** The loader enforces the same `max_future_skew_ns`
  bound OTLP does. Relaxing it would let a record bucket by today's wall clock
  while every later query lists from `query_range.start - max_ingest_lag`, which
  does not reach today's bucket, so the record would be permanently
  undiscoverable.
- **Length caps (attribute key length, attribute value length, body length):
  kept**, identical to the OTLP path's. These
  bound field sizes regardless of who is sending; the offline/trusted framing
  does not change that.
- **Past-event-time lag: relaxed (not enforced).** A backfill or migration needs
  its real event times, and rewriting them would corrupt the source semantics
  this path exists to preserve. This is sound because a record buckets by the
  *flush-open wall clock*, not by its event time,
  so an old-event record still lands in today's ingest-hour bucket. Its
  *discoverability* then depends on the query's listing window reaching that
  bucket: a caller querying with a normal `start`/`end` window already does,
  since that window is compared against event-range overlap and the listing
  upper bound is `now + max_future_skew` (which reaches today's bucket). See
  [late and skewed data](../consistency-model.md#late-and-skewed-data) for the
  paired admission/discoverability bound this relies on. **Query bulk-loaded data with
  a window that reaches now, not just the records' event times.**
- **Per-record attribute cap: relaxed** from OTLP's 128 to a loader-specific
  1024. Bulk import is an operator-initiated, offline action over a file the
  operator already controls -- a different threat model than a networked sender.
  This 1024 cap is a *per-record* axis and is unrelated to the RLOG object's
  1000-distinct-`(name, type)` dynamic-column budget: past that per-object
  budget, extra columns fold into the object's `attrs_raw` overflow column
  rather than being rejected, exactly as they do for OTLP-ingested data. A row
  over the 1024 cap is rejected; a row within it whose columns push the object
  past 1000 distinct columns is not -- its overflow columns fold into
  `attrs_raw`.
- **Per-tenant admission control (active-stream cap, stream-creation rate,
  byte rate): bypassed by construction.** This control lives in the server's
  HTTP layer, above the router the loader calls directly. The loader does not go
  through it, and there is no equivalent concept for a single offline bulk load.
  Bulk-loaded volume is therefore not evidence the admission controller's limits
  were exercised. The CLI prints this warning before every run.

### `--batch-rows`: the object-count lever

`load` writes one Strict flush per batch, and one flush is one RLOG object per
involved shard. `--batch-rows` sets the batch size (default 10000). It is
therefore the lever that controls how many RLOG objects a load leaves behind:
a 100M-row load at the default is on the order of 10000 flushes, each an RLOG
object per shard. Object count is a first-order query-cost variable, because
per-object cost (LIST, footer read, per-object decode setup) is paid on every
later query over the affected range. A larger `--batch-rows` writes fewer,
larger objects (less per-object overhead, more memory held per batch); a
smaller one writes more, smaller objects.

`--batch-rows 0` is rejected with an error rather than silently clamped: a zero
would otherwise hide a misconfigured value that changes layout.

### The dynamic-column budget and its warnings

Each RLOG object gives a real typed column to the first
`max_dynamic_columns` (default 1000) distinct `(attribute name, type)` pairs it
holds, ordered lexicographically by name; anything past that budget folds into
the object's `attrs_raw` overflow column. An overflowed attribute is **still
queryable through `attrs['<key>']`**, but it gets **no typed column**, so a
typed predicate or aggregate over it is unavailable and a SQL filter over it
pays a per-row string cast.

After a load, `load` reads the run's cumulative dynamic-column counters and
prints one of two warnings to stderr when they apply:

- an **overflow** warning when any object crossed the budget, naming the count
  of overflowed `(name, type)` pairs;
- a distinct **near-cap** warning when nothing overflowed but the widest object
  reached **90% or more** of `max_dynamic_columns` -- pressure surfaced before
  the cap, not only after it.

Both name the same fix: reduce the number of distinct attribute columns per
stream (map fewer columns, or split the load so each object stays under the
budget), or accept `attrs`-only access for the overflow keys. To give an
overflowed key a typed column at query time, declare it with
`ravel-cli typed-attr-column set` (see [query.md](query.md#declaring-typed-attribute-columns));
declaring does not change what the object already stored, so the two-step flow
is load, then declare, then query.

### Failure, retention, and performance

A row that fails a kept check (future skew, a length cap, or the 1024 attribute
cap) is rejected **fail-fast**: the run stops at the first bad row, prints a
per-row error, and exits non-zero. Batches durable before that row stay durable.

A failed flush (an object-store PUT failure) exits non-zero and prints the
commit tokens durable from batches completed before the failure, plus any shard
of the failing batch itself that acked its commit durably before a sibling
shard failed. The loader shards a batch across the target signal's shards and
waits for every shard's ack; when one shard fails while a sibling committed,
the sibling's token is recovered from the write error and reported, so the
printed list is **exact** for that partial-flush case. It remains a lower bound
only when the failing batch's ack round did not resolve at all -- an
ack-deadline timeout, or a shard's channel dying at send time -- because no
per-shard ack is observed then, and a commit can land without an observable
ack. A failure mid-file is a genuine **partial load, not a rollback**. There is
**no resumability or deduplication**: re-running after any failure re-ingests
the whole file from the start.

Retention and GC key on ingest-hour buckets, which the loader derives from
*load* time. A bulk-loaded record with an old event timestamp is therefore
retained for the full retention window measured from when it was loaded, not
from the data's real age.

An RLOG object spanning a wide event-time range overlaps every later query's
event range at resolve time, so unsorted input makes every subsequent query over
the affected stream fetch the bulk-loaded objects regardless of the query's
window. Sort input by event time before load where the mapping allows it. This
is a performance recommendation, not a correctness requirement.

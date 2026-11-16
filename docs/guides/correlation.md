# Correlation: from a metric to a trace

An exemplar links a metric sample to the trace that produced it. Ravel
stores exemplars from OTLP ingest, caps them at admission, and serves them
over the Prometheus exemplar endpoint.

## What an exemplar is

An exemplar is one reference from one metric sample to one trace. A sample
records that a value happened. An exemplar records one request that
contributed to that value. The exemplar carries a trace id, an optional span
id, the sample value, a timestamp, and optional attributes. An operator uses
the exemplar to open the trace behind a metric point.

Prometheus and OpenTelemetry both treat an exemplar as illustrative. An
exemplar is a sampled signal. It does not record every request.

## How Ravel stores an exemplar

Ravel stores exemplars from OTLP ingest only. Remote Write and OTAP decode
the exemplars that a request carries and then discard them. A metric ingested
over those paths has no exemplars.

Ravel stores exemplars in the RSEG `EXEMPLARS` section (kind 10). Each object
holds at most one `EXEMPLARS` section. The section is present only when at
least one sample in the object carried an exemplar. An object without the
section is valid and has no exemplars.

Each exemplar record attaches to one series through a `series_index`. The
`series_index` is the position of the series in the object's sorted
`SERIES_IDS` section. One record holds the `series_index`, the timestamp, the
value, the trace id, the span id, and the attributes. The trace id is 16
bytes and the span id is 8 bytes. An all-zero id means absent.

Records are sorted by `(series_index, ts_ns)`, ascending. Two records can
share a key. Two records with the same key can differ in the trace id, the
span id, the value, or the attributes. A reader that collapses records must
key on every field, or it loses the fields outside the key.

To see the stored exemplars in one object, run the segment inspector.

```sh
ravel-cli segment inspect "t/.../m/l0/0000/....rseg"
```

The inspector prints the section-level count and one line per record. Each
line gives the `series_index`, the `ts_ns`, the `value`, the `trace_id`, the
`span_id`, and the attributes.

```
exemplar_count: 1
  exemplar[0]: series_index=0 ts_ns=1650000000000000000 value=42.5 trace_id=abababababababababababababababab span_id=cdcdcdcdcdcdcdcd attrs=trace_state=sampled=1
```

An object with no exemplars prints `exemplar_count: 0` and lists no
`EXEMPLARS` section.

## The admission cap

Ravel caps exemplars at admission. The cap keeps at most one exemplar per
series per window. The default window is 10 seconds. The cap keeps the newest
exemplar within each window and drops the rest.

The cap is a security control. Without the cap, a client can attach a
distinct trace id to every sample. That input multiplies the object size and
defeats the dictionary in the format. The cap bounds the exemplar cost per
series.

The cap runs per shard actor, with no cross-shard coordination.

`GET /metrics` exposes two counters under the ingest family:

- `ravel_ingest_exemplars_written_total` counts the exemplars that Ravel
  stored on flushed objects.
- `ravel_ingest_exemplars_dropped_total` counts the exemplars that the cap
  discarded.

Both counters carry the `mode` and `signal` labels. Both carry only the
`signal="metrics"` series, because exemplars ride on metric points. A rising
drop count means that the cap is engaging.

## Erasure removes matching exemplars

A subject-erasure request removes an exemplar the same way it removes a
sample. The rewrite output drops each exemplar that matches the predicate of
the request. This includes an exemplar whose own timestamp falls inside the
erasure window while its enclosing object survives. The object survives when
other series or samples in it did not match.

## How to query exemplars

Query exemplars over `GET`/`POST /api/v1/query_exemplars`. The endpoint takes
the Prometheus `query`, `start`, and `end` parameters. It reads exemplars
from the segments that the `[start, end]` window matches. It keeps a returned
exemplar only when the exemplar's own timestamp falls inside `[start, end]`.
It ignores `offset` and `@`, which matches Prometheus.

To request the exemplars for a metric selector, run the following command.

```sh
curl -G http://127.0.0.1:4318/api/v1/query_exemplars \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=http_request_duration_seconds{method=\"get\"}" \
  --data-urlencode "start=<unix seconds>" \
  --data-urlencode "end=<unix seconds>"
```

The response is the Prometheus exemplar shape. The `data` field is an array of
objects. Each object holds `seriesLabels` and an `exemplars` array. Each
exemplar holds `labels`, `value`, and `timestamp`.

```json
{
  "status": "success",
  "data": [
    {
      "seriesLabels": {
        "__name__": "http_request_duration_seconds",
        "method": "get"
      },
      "exemplars": [
        {
          "labels": {
            "trace_id": "abababababababababababababababab",
            "span_id": "cdcdcdcdcdcdcdcd"
          },
          "value": "42.5",
          "timestamp": 1650000000
        }
      ]
    }
  ]
}
```

A `stats` object rides next to `data` and carries the request's cost
counters. A Prometheus-shaped client reads only `status` and `data`, so the
`stats` object does not affect it.

Ravel deduplicates the results. During compaction the same exemplar can be
readable from two segments at once. Ravel deduplicates on the exemplar's full
stored identity, which covers the series, the timestamp, the trace id, the
span id, the value, and the attributes. An exact duplicate collapses to one
exemplar. Two exemplars that differ in any stored field both survive.

## Configure the Grafana link

Grafana reads a label from the exemplar and opens a trace in a tracing data
source. The conventional label is `trace_id`. The trace id and the span id
ride in `labels` under the `trace_id` and `span_id` keys. Both ids are
hex-encoded. An all-zero id is absent, so Ravel omits its label.

To configure the metric-to-trace link, do these steps.

1. Open the Prometheus data source configuration in Grafana.
2. Find the Exemplars section in the configuration.
3. Add an internal link.
4. Set the label name to `trace_id`.
5. Select the tracing data source that holds the traces.
6. Save the configuration.

When a user clicks an exemplar on a metric panel, Grafana reads the
`trace_id` label and opens the trace. If the `trace_id` label is absent, the
exemplar carries no trace id and Grafana opens no trace.

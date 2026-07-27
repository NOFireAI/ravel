# ADR-0016: OTLP explicit-bounds histograms and summaries explode to Prometheus-convention series

Status: Accepted (2026-07-27). Implementation plan and tickets:
docs/ingest-breadth-plan.md (track B).

## Context

ravel-otlp normalizes only Gauge and Sum today; OTLP `Histogram`
(explicit bucket bounds), `Summary`, and `ExponentialHistogram` points
are rejected with `UnsupportedMetricType`. This ADR decides the first
two. `ExponentialHistogram` is ADR-0017: it genuinely cannot be
flattened without losing the sparse bucket structure, so it is a
different decision with a different cost.

An OTLP `HistogramDataPoint` is one atomic message per timestamp: count,
optional sum, `bucket_counts` (per-bucket, not cumulative; length =
len(explicit_bounds) + 1), strictly increasing `explicit_bounds`,
optional min/max, exemplars, flags. It is semantically a classic
Prometheus histogram but not wire-identical: Prometheus represents the
same data as N independent float series (`name_bucket{le="X"}` with
cumulative counts, `name_sum`, `name_count`), which is exactly what
Remote Write transmits and exactly what Ravel's scalar sample path
already stores. `SummaryDataPoint` is the same story with
`{quantile="X"}` value series instead of cumulative buckets.

## Alternatives

1. A structured histogram value type in the segment format, one atomic
   sample per timestamp. Preserves point-in-time atomicity across
   buckets and avoids series multiplication, but:
   - It requires opening the format-change procedure for data that has
     a lossless scalar representation, while RW-ingested classic
     histograms (ADR-0015) arrive pre-exploded as N series and would be
     stored scalar regardless. One logical shape would then live in two
     storage representations depending on arrival protocol, and every
     query-path feature (histogram_quantile, rate over buckets,
     label_values on le) would have to unify them forever.
   - The atomicity it preserves is not something the Prometheus
     ecosystem relies on: RW receivers (Prometheus itself, Mimir,
     Thanos) ingest classic histogram series with no cross-series
     atomicity, and PromQL is specified against the exploded shape.
   Rejected.
2. Explode into Prometheus-convention scalar series (chosen): each
   `HistogramDataPoint` becomes one sample on each of
   `{name}_bucket{le="<bound>"}` (cumulative counts, one per explicit
   bound), `{name}_bucket{le="+Inf"}` (= count), `{name}_sum` (when sum
   is present), and `{name}_count`; each `SummaryDataPoint` becomes
   `{name}{quantile="<q>"}` per quantile plus `{name}_sum` and
   `{name}_count`. Zero storage change; the query engine sees one
   representation of classic histograms regardless of ingest protocol.

The trade is stated, not hidden: explosion multiplies series (a
10-bucket histogram is 13 series) and drops cross-bucket write
atomicity (bucket series of one point can even land via different
shards and become visible at slightly different times). Both costs are
the ones the entire Prometheus ecosystem already pays and its query
semantics are defined against; RSEG v2's schema-sharing catalog
(ADR-0014) specifically amortizes the per-series metadata cost, since
all bucket series of a histogram share one label schema differing only
in the `le` value ordinal.

## Decision

Option 2, implemented in ravel-otlp (extension of the existing
normalizer, not a new crate: the input surface is still OTLP).

Exactness rules, the load-bearing details:

- Cumulative accumulation: `le=bound_i` carries
  sum(bucket_counts[0..=i]); `+Inf` carries the point's `count`.
  Accumulation is overflow-checked; fixed64 counts convert to f64 with
  the same precision caveat every Prometheus counter already has
  (exact to 2^53).
- Label-value formatting for `le` and `quantile` must match the
  OTel-to-Prometheus mapping used by the collector's
  prometheusremotewrite exporter (Go shortest-representation float
  formatting) byte-for-byte. This is series identity: the same
  histogram arriving OTLP-exploded and RW-classic must produce
  identical SeriesIds, and Rust's default shortest f64 formatting
  differs from Go's in edge cases (exponent forms). The formatting
  function is pinned by golden vectors generated from the Go
  implementation.
- Cumulative temporality only; delta points are rejected with the
  existing `UnsupportedTemporality` (delta-to-cumulative conversion
  requires cross-request state, which stateless compute forbids). This
  matches the existing Sum rule.
- Data-point flags: a `NoRecordedValue` point maps to Prometheus stale
  markers on the exploded series, matching the collector's mapping, so
  staleness semantics survive the protocol boundary.
- Sanitization, resource-attribute mapping, admission limits, and skew
  bounds are unchanged from the existing gauge/sum path; the exploded
  series pass through the same `build_point`-equivalent checks
  per-series.
- min/max fields have no Prometheus-convention representation and are
  dropped with a per-request counter (visible, not silent); same for
  histogram exemplars pending ADR-0017's exemplar decision.
- Rejection stays atomic per data point: if any exploded series of a
  point fails admission (label limits, skew), the whole data point is
  rejected with one Rejection, so a histogram is never stored with
  some buckets missing. Nothing is partially exploded.

## Consequences

- No storage or format change; no new dependencies; the change is
  contained in ravel-otlp (and its differential tests).
- The `UnsupportedMetricType` rejection surface shrinks to
  ExponentialHistogram only (until ADR-0017 storage lands).
- Correctness gate, extending the ADR-0011 discipline: (a) golden
  differential vectors against the collector's prometheusremotewrite
  mapping for bucket/quantile label formatting and staleness flags;
  (b) cross-protocol identity: the same logical histogram ingested as
  OTLP and as RW classic series (ADR-0015) yields identical SeriesIds
  and samples.
- Admission accounting changes shape: one OTLP data point admits as N
  normalized points. Per-request data-point limits keep counting OTLP
  data points (the wire unit senders reason about), while downstream
  buffer sizing sees the multiplied count; the plan's tickets carry a
  measurement note for max_data_points_per_request sizing.
- ravel-otap's normalizer mirrors ravel-otlp point-for-point and gains
  the same explosion when its metrics tables carry histogram/summary
  payloads; that lands as a follow-up ticket under the OTLP-vs-OTAP
  differential gate, not silently.

# Analytics stage

Normative for the `ravel-analytics` crate (ADR-0028). The crate is a
post-evaluation analytics stage: pure per-series functions over
`(timestamp_ns, f64)` slices, with no clock, IO, object-store, or catalog
dependency. Every result is a deterministic function of the input slice and
the parameters.

The `POST /api/v1/analytics` endpoint (in `ravel-server`)
exposes these operations over a range query; its request/response schema and
error table are the [Endpoint](#endpoint) section at the end of this document.
What follows first specifies the crate's two operations, which the endpoint
calls once per series of the evaluated matrix.

## Why a separate stage

Neither existing query surface can host these operations (ADR-0028 context):

- PromQL parsing is delegated to a closed function table (ADR-0007), and the
  PromQL differential gate has no Prometheus reference for a change point
  function.
- The SQL surface admits a float-folding aggregate only when it is a
  deterministic sequential scalar algorithm matched bit-for-bit by an
  independent reference (ADR-0022); that bar excludes the stddev/variance
  family, and a structured per-series detector result does not fit a flat
  aggregate.

So the operations live in their own crate and behind their own endpoint,
keeping change point detection outside the aggregation layer.

## NaN and staleness

Staleness markers never reach the stage; the evaluator excludes them from
range windows. Remaining NaN values in the evaluated matrix are excluded from
both operations, and each per-series result reports the excluded count
(`nan_excluded`). NaN is detected by IEEE class (`f64::is_nan`), never by an
equality comparison, so every NaN payload and both signed zeros are handled
by class, not by bit accident.

## `change_point`

Detects the most significant change in one series and classifies it.

### Algorithm

1. **Segmentation.** PELT (Pruned Exact Linear Time) partitions the series to
   minimize a penalized cost `sum_segments C(segment) + k * beta`, where `k`
   is the number of change points and `beta` is the BIC penalty. The
   per-segment cost is the Gaussian negative log-likelihood with each
   segment's own maximum-likelihood mean and variance, with the additive
   constants that are identical for every segmentation of a fixed-length
   series dropped:

   ```text
   C(segment) = n * ln(variance_MLE)
   ```

   Each additional segment estimates two parameters (a mean and a variance),
   so `beta = 2 * ln(N)`. PELT's pruning is exact for this cost. Values are
   centered on their mean before the prefix sums are formed (offset-invariant,
   and it conditions the variance). A segment's variance is floored both
   absolutely (`1e-12`) and relative to the whole series' variance
   (`0.10`); the relative floor bounds the reward for isolating a short
   low-variance run, which otherwise over-segments an otherwise stationary
   series. PELT produces only segments of at least 15 samples; shorter
   excursions (spikes and dips) are detected separately.

2. **Classification.** From the segmentation the most significant change is
   the boundary with the largest Gaussian cost reduction. Ordered hypothesis
   tests, each invariant to a time translation (they use sample indices, not
   timestamp values) and, for the level tests, to a value offset (they use
   mean differences and variances), assign the kind:

   1. **Spike / Dip** (checked first, independent of the segmentation, so it
      catches excursions shorter than a segment): a point whose deviation
      from the robust center (median, scaled MAD) exceeds 6 z-scores while
      such extremes stay rare enough (at most `N/50`) to be an isolated
      excursion rather than a whole regime. A peak above the center is a
      `Spike`, below is a `Dip`.
   2. **TrendChange**: the best single-break broken-line fit has a residual
      below half of both the best single-break piecewise-constant fit and the
      single-line fit, so the slope changes. Located at the broken-line
      breakpoint, searched over every admissible index.
   3. **DistributionChange**: across the dominant PELT boundary the variance
      ratio exceeds 4 while the means stay within 4 pooled-noise standard
      deviations. Located at that boundary (a piecewise-constant residual is
      blind to a variance shift, the means being equal).
   4. **StepChange**: the means differ by more than 4 pooled-noise standard
      deviations across the refined piecewise-constant breakpoint.

   A series with no PELT boundary, or with a boundary that trips no
   hypothesis, is `Stationary`. A series with fewer than 22 evaluated points
   is `Indeterminable`.

The classification thresholds above are heuristics tuned against the fixture
corpus; they are the crate's contract and change only with the corpus and this
document.

### Parameters

- `downsample: bool`. When a series has more than 2000 non-NaN points,
  detection is an error unless this is set, in which case the series is
  reduced to at most 2000 points by deterministic fixed-stride bucket
  averaging: bucket `k` spans `[floor(k*n/2000), floor((k+1)*n/2000))`, its
  value is the mean of that range, and its timestamp is the first timestamp in
  it. The result then carries `downsampled: true` and the original non-NaN
  point count. Approximation is opt-in and visible (a Ravel invariant); it is
  never applied silently.

### Result

`kind`, `ts_ns` (the location of the change, `None` for `Stationary` and
`Indeterminable`), `score` (the significance: a Gaussian cost reduction in
nats for the segment-based kinds, a robust z-score for spike/dip; `0.0` when
no change is reported), `downsampled`, `original_points` (non-NaN points
before any downsampling), and `nan_excluded`.

### Budget

PELT runs on at most 2000 points; its worst case is quadratic in that cap. The
per-series point cap and the per-call 1000-series cap bound the
stage's cost, and it can never see more data than a legal range query returns.

## `summary`

Robust per-series summary statistics.

- **median**: the exact median (the central order statistic for an odd count,
  the average of the two central ones for an even count).
- **mad**: the median absolute deviation, the median of `|x - median|`.
- **percentiles**: each requested quantile, by Prometheus interpolation
  (linear interpolation between the two ranks straddling `q * (n - 1)`).
- **stddev**, **variance**: the population standard deviation and variance.

Selection statistics (median, MAD, percentiles) sort a copy of the non-NaN
values by `f64::total_cmp`, a total order over all bit patterns in which
`-0.0` sorts below `0.0` and the infinities sort at the ends; they are
therefore invariant to the input order. Moment statistics (variance, stddev)
use a sequential two-pass fold in the evaluation-grid order: pass one sums and
divides for the mean, pass two folds the squared deviations, with naive IEEE
addition (not Kahan). Because f64 addition is not associative, the moments
depend on the grid order and are **not** permutation-invariant; this is
intentional (ADR-0028 decision 3 ties them to "the evaluation grid order"),
and only the selection statistics are asserted permutation-invariant.

### Parameters

- `percentiles: Vec<f64>`, each in `[0, 1]`. A value outside that interval
  (NaN included) is an error.

### Result

`median`, `mad`, `percentiles` (each request paired with its value, in request
order), `stddev`, `variance`, and `nan_excluded`. When every point is NaN the
statistics are all NaN (no panic).

## Error taxonomy

`AnalyticsError` (typed; out-of-contract input never panics or returns silent
wrong data, ADR-0028 decision 6):

- `SeriesTooLong { max, got }`: a `change_point` series exceeded the 2000-point
  cap and the caller did not set `downsample`.
- `EmptySeries`: the input series was empty (either operation).
- `InvalidPercentile { got }`: a requested percentile was outside `[0, 1]`
  (NaN included).

## Evidence (ADR-0028 decision 6)

No Prometheus differential exists for this surface, so the crate defines its
own required evidence, all in `crates/ravel-analytics`:

- **Fixture corpus** (`tests/fixtures/*.csv`, generated by
  `examples/gen_fixtures.rs`; regenerate with `cargo run -p ravel-analytics
  --example gen_fixtures`): synthetic series with known ground truth for step,
  spike, dip, trend break, variance shift, stationary noise, and a constant
  series. Each file's header records the expected kind, the ground-truth
  change index, and the tolerance window (in samples). `tests/fixtures.rs`
  asserts the correct kind and, for a located change, a location within the
  tolerance window. The generator is std-only with a fixed-seed xorshift PRNG,
  so the corpus is byte-for-byte reproducible.
- **Property tests** (`tests/properties.rs`): no panics on adversarial input
  (empty, single point, all-NaN, NaN/Inf/-0.0 mixtures, constant series);
  determinism (same input, bit-identical output); time-translation invariance
  of detected locations; value-offset invariance of `StepChange`; permutation
  invariance of the summary's selection statistics.
- **Summary vs naive reference**: the summary is asserted bit-identical
  against an independent naive reference fold over the ADR-0022 adversarial
  value pool (NaN payloads of both signs, both infinities, both signed zeros,
  a denormal, and cancellation-prone values).
- **Typed-error tests**: each `AnalyticsError` variant is asserted, plus a
  downsampling test proving an over-cap series yields `downsampled: true` with
  `original_points` preserved.

This evidence set is the merge bar for any change to the crate.

## Endpoint

`POST /api/v1/analytics` (ADR-0028 decision 2) runs a range evaluation and
applies one op to each series of the result. It lives in `ravel-server`
alongside `/api/v1/sql`, shares the query listener, and needs no cargo
feature: the analytics stage links only the pure `ravel-analytics` crate, no
DataFusion. The evaluation is the *same* one `/api/v1/query_range` runs
(identical planner, budgets, staleness filtering, and wall deadline), so the
endpoint sees exactly the matrix that endpoint would return, then converts
each evaluated sample's timestamp to nanoseconds (the unit the crate expects)
and calls the op once per series.

### Request

A JSON body (like `/api/v1/sql`, and unlike the form-encoded Prometheus-shaped
endpoints):

| Field | Type | Meaning |
|---|---|---|
| `query` | string | PromQL range query. |
| `start` | number or string | Window start: Unix float seconds or RFC3339. |
| `end` | number or string | Window end: Unix float seconds or RFC3339. |
| `step` | number or string | Grid step: Prometheus duration (`30s`, `5m`) or bare float seconds. |
| `op` | object | The analytic to apply (tagged; see below). |
| `timeout` | number, optional | Wall deadline in seconds; clamped to the server maximum, can only lower it. |
| `min_commit_token` | array of string, optional | Read-your-write commit tokens. |
| `allow_partial` | bool, optional | Consent to a partial federated answer; default `false`. See "Partial federated coverage" below. |

`start`, `end`, and `step` accept the identical syntax `/api/v1/query_range`
accepts. A JSON number is accepted wherever a bare-seconds string would be, so
`30`, `"30"`, and `"30s"` all mean thirty seconds.

`op` is a tagged object; `type` selects the analytic:

```json
{"type": "change_point", "downsample": false}
{"type": "summary", "percentiles": [0.5, 0.9, 0.99]}
```

- `change_point`: `downsample` (bool, default `false`) opts in to the
  fixed-stride bucket averaging described above for a series over the
  2000-point cap.
- `summary`: `percentiles` (array of f64 in `[0, 1]`, default `[]`) is the
  list of quantiles to report, in request order.

An unknown `op` type, a missing required field, or any other malformed body is
a `400`.

### Response

A JSON envelope in the Prometheus response style, with one entry per series of
the evaluated matrix:

```json
{
  "status": "success",
  "data": {
    "resultType": "analytics",
    "result": [
      {"metric": {"__name__": "http_requests_total", "job": "api"},
       "result": { ... op result ... }}
    ]
  },
  "stats": { "accounting": { ... }, "estimate": { ... } },
  "partial": false,
  "warnings": []
}
```

Each entry's `metric` is the series' label set; its `result` is the op's
result object, serialized by server-local serde structs (the crate carries no
serde). Fields are snake_case.

`stats` sits beside `data`, not inside it. It carries what this query spent
on object storage, and the pre-execution upper envelope of that spend
(ADR-0044). docs/guides/operations.md, section "Per-query cost accounting",
gives the field list.

`partial` and `warnings` sit beside `data` too, on every response, complete
or not. `partial` is `true` only when the query federated across a remote
that `skip_unavailable` let it skip; `warnings` names which one. See
"Partial federated coverage" below.

### Partial federated coverage

A federated query (ADR-0071) can skip an unreachable remote when that
remote's `skip_unavailable` opt-in is set. By default the analytics
endpoint refuses to hand back an answer built on incomplete coverage: it
fails with a `503`/`unavailable` naming the degraded cluster, and names
`allow_partial` as the remedy in the message itself. Setting
`allow_partial: true` in the request body opts in; the response is then a
normal `200` with `partial: true` and `warnings` naming the skipped
cluster(s). This mirrors the consent gate `/api/v1/query` and
`/api/v1/query_range` apply to the same situation.

`change_point` result:

| Field | Type | Meaning |
|---|---|---|
| `kind` | string | One of `spike`, `dip`, `step_change`, `trend_change`, `distribution_change`, `stationary`, `indeterminable`. |
| `ts_ns` | integer or null | Nanosecond timestamp of the change; null for `stationary` and `indeterminable`. |
| `score` | number | Significance (Gaussian cost reduction in nats, or a robust z-score for spike/dip); `0.0` when no change is reported. |
| `downsampled` | bool | Whether the series was downsampled before detection. |
| `original_points` | integer | Non-NaN point count before any downsampling. |
| `nan_excluded` | integer | NaN points excluded from detection. |

`summary` result:

| Field | Type | Meaning |
|---|---|---|
| `median` | number | Exact median. |
| `mad` | number | Median absolute deviation. |
| `percentiles` | array | Each `{"quantile": q, "value": v}` in request order. |
| `stddev` | number | Population standard deviation. |
| `variance` | number | Population variance. |
| `nan_excluded` | integer | NaN points excluded from the summary. |

A non-finite statistic (for example every point NaN, so the moments are NaN)
serializes as JSON `null`, since JSON has no literal for `NaN` or the
infinities.

### Worked examples

`change_point` over a series that steps from a low to a high level halfway
through:

```sh
curl -X POST http://127.0.0.1:4318/api/v1/analytics \
  -H "Authorization: Bearer devtoken" -H "Content-Type: application/json" \
  -d '{"query":"cpu_seconds","start":0,"end":600,"step":"10s",
       "op":{"type":"change_point"}}'
# {"status":"success","data":{"resultType":"analytics","result":[
#   {"metric":{"__name__":"cpu_seconds"},
#    "result":{"kind":"step_change","ts_ns":300000000000,"score":51.4,
#              "downsampled":false,"original_points":60,"nan_excluded":0}}]}}
```

`summary` with the median, the 90th, and the 99th percentile:

```sh
curl -X POST http://127.0.0.1:4318/api/v1/analytics \
  -H "Authorization: Bearer devtoken" -H "Content-Type: application/json" \
  -d '{"query":"latency_ms","start":0,"end":3600,"step":"30s",
       "op":{"type":"summary","percentiles":[0.5,0.9,0.99]}}'
# {"status":"success","data":{"resultType":"analytics","result":[
#   {"metric":{"__name__":"latency_ms"},
#    "result":{"median":12.0,"mad":3.0,
#              "percentiles":[{"quantile":0.5,"value":12.0},
#                             {"quantile":0.9,"value":40.0},
#                             {"quantile":0.99,"value":88.0}],
#              "stddev":14.2,"variance":201.6,"nan_excluded":0}}]}}
```

### Status codes

The endpoint keeps the exact status mapping `/api/v1/query_range` uses for
evaluator errors (including the redaction of storage-layer faults, which would
otherwise leak an object key or tenant hash), and adds the analytics-stage
mappings:

| Condition | Status | `errorType` |
|---|---|---|
| Missing/authless credentials | 401 | `unauthorized` |
| Malformed body, unknown `op` type | 400 | `bad_data` |
| `start`/`end`/`step`/`timeout`/`min_commit_token` parse error | 400 | `bad_data` |
| Query result is not a range vector | 400 | `bad_data` |
| PromQL parse error, non-positive step, inverted range, time overflow | 400 | `bad_data` |
| Unsupported PromQL construct | 422 | `execution` |
| Query engine budget breach (segments, series, samples, points) | 422 | `execution` |
| Per-call series cap (over 1000 series) | 422 | `execution` |
| `AnalyticsError::SeriesTooLong` (over 2000 points, no `downsample`) | 422 | `execution` |
| `AnalyticsError::InvalidPercentile` (quantile outside `[0, 1]`) | 422 | `execution` |
| `AnalyticsError::EmptySeries` | 422 | `execution` |
| Query deadline exceeded | 504 | `timeout` |
| Transient storage fault, unsatisfiable commit token | 503 | `unavailable` |
| Partial federated coverage without `allow_partial: true` | 503 | `unavailable` |
| Permanent data corruption | 500 | `internal` |

`AnalyticsError` messages carry only the caller's own parameters (a
percentile, a point count) and are echoed; storage-layer faults are redacted
to a fixed class message and logged in full server-side.

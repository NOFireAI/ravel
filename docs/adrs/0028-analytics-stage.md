# ADR-0028: Post-evaluation analytics stage for change point detection and robust statistics

Status: Accepted (2026-07-28). Decides issue #216. Sibling constraints:
ADR-0007 (PromQL differential gate) and ADR-0022 (SQL floating aggregate
exactness) are why neither existing query surface hosts this capability.

## Context

Elastic ships `CHANGE_POINT` as an ES|QL processing command: PELT (Pruned
Exact Linear Time) segmentation with a BIC penalty, classifying spikes,
dips, step changes, trend changes, and distribution changes per series,
with automatic downsampling of long series. Notably, Elastic placed it
outside the aggregation layer as a processing stage over query results.
Ravel wants the same capability, plus the exact statistical summaries in
the ES|QL aggregate surface (`MEDIAN`, `MEDIAN_ABSOLUTE_DEVIATION`,
`PERCENTILE`, `STD_DEV`, `VARIANCE`) that Ravel's SQL subset excludes.

Neither existing surface can host this:

- PromQL. Parsing is delegated to promql-parser 0.10 (ADR-0007), whose
  function table is closed; a novel name like `changepoint_over_time`
  fails at parse. ADR-0007 also defines a PromQL feature as done only
  when it passes differential tests against a pinned Prometheus binary,
  and Prometheus computes no change point function, so no reference
  exists. Hosting the feature there requires a parser fork and a gate
  exemption.
- SQL. ADR-0022 decision 1 admits a compute path only if it is a
  deterministic sequential scalar algorithm matched bit for bit by an
  independent reference over the adversarial value pool. That bar
  excluded the stddev/variance family. A change point detector also
  returns a structured per-series result (type, location, score), which
  does not fit a flat SQL aggregate.

Ravel stores metrics only. ES|QL's text-oriented analytics
(`CATEGORIZE`, `GROK`, `DISSECT`) have no input here and are out of
scope by data model, not by choice.

## Alternatives

1. Post-evaluation analytics stage in a new pure crate, exposed by a
   dedicated endpoint (chosen). Mirrors Elastic's own placement of
   `CHANGE_POINT` outside aggregation. Touches no frozen contract: no
   parser fork, no proto or format change, no entry into the ADR-0022
   admission regime, and the evaluator pipeline with its budgets and
   staleness handling is consumed as is.
2. PromQL function family. Rejected: closed parser table, no Prometheus
   reference for the differential gate, and ADR-0007's definition of
   done cannot be met. Revisit only if upstream Prometheus grows the
   feature.
3. SQL UDAF in ravel-sql. Rejected for v1: the ADR-0022 evidence burden
   applies to every float-folding op, and the structured detector output
   does not fit an aggregate. The summary statistics could individually
   clear the bar later; that would be a separate ADR-0022-scoped
   decision and does not block this stage.

## Decision

1. **Placement.** New crate `crates/ravel-analytics`: pure functions
   over per-series `(timestamp_ns, f64)` slices. No IO, no clock, no
   object-store or catalog dependency; may depend on ravel-types only.
   Every result is a deterministic function of the input slice.
2. **Endpoint.** `POST /api/v1/analytics` in ravel-server. Request body:
   a PromQL `query` with `start`, `end`, `step` (identical semantics to
   `/api/v1/query_range`), plus an `op` object selecting one analytic
   and its parameters. Execution runs the existing range evaluation
   unchanged (same planner, budgets, staleness filtering, 30 s
   deadline), then applies the op to each series of the matrix. The
   response is a JSON envelope in the Prometheus response style with
   `resultType: "analytics"`, one entry per series carrying `metric`
   labels and the op's result object.
3. **Op surface, v1.** Two ops, both mapped from the ES|QL analytic
   surface:
   - `change_point`: PELT segmentation with a BIC penalty over a
     Gaussian cost (mean and variance). Per series it reports
     `type` (one of `spike`, `dip`, `step_change`, `trend_change`,
     `distribution_change`, `stationary`, `indeterminable`),
     `timestamp` of the most significant change, and a significance
     `score`. Fewer than 22 evaluated points returns `indeterminable`
     with a typed reason, matching Elastic's floor.
   - `summary`: exact `median`, `mad`, `percentile` (caller-supplied
     list of quantiles, Prometheus interpolation), `stddev`, and
     `variance` per series. Selection-based statistics sort by
     `f64::total_cmp`; moment-based statistics use a sequential
     two-pass fold. All are deterministic functions of the input
     multiset and the evaluation grid order.
4. **Budgets and approximation.** `change_point` runs on at most 2000
   points per series. A longer series is an error by default; the
   caller opts in with `downsample: true`, which applies deterministic
   fixed-stride bucket averaging to at most 2000 points, and the
   response then carries `downsampled: true` with the original point
   count. This keeps the "approximation is opt-in and visible"
   invariant that Elastic's silent auto-downsampling would break. An
   analytics call processes at most 1000 series; more is a typed
   error, consistent with the query engine's budget-breach-is-an-error
   rule. `summary` runs on the full evaluated series (bounded by the
   existing 11,000-point range budget).
5. **NaN and staleness.** Staleness markers never reach the stage (the
   evaluator excludes them from range windows). Remaining NaN values in
   the evaluated matrix are excluded from detection and summaries, and
   each per-series result reports the excluded count. NaN
   classification uses bit patterns, never `==`.
6. **Testing gate.** No Prometheus differential exists for this
   surface, so the stage defines its own required evidence, all in
   crates/ravel-analytics:
   - A committed fixture corpus of synthetic series with known ground
     truth (step at a known index, spike, trend break, variance shift,
     stationary noise, constant series), generated offline by a script
     checked in beside the fixtures. Detection must report the correct
     type and a location within a documented tolerance window for
     every fixture.
   - Property tests: no panics on adversarial input (empty, single
     point, all-NaN, NaN/Inf/-0.0 mixtures, constant series);
     determinism (same input, bit-identical output); time-translation
     invariance of detected locations; value-offset invariance of
     `step_change` detection; permutation invariance of `summary` over
     the input multiset.
   - `summary` results asserted bit-identical against independent
     naive reference folds over the ADR-0022 adversarial value pool.
   - Corrupt or out-of-contract input produces typed errors, never
     panics or silent wrong data.
7. **Documentation.** docs/analytics.md is the normative doc for the
   crate and the endpoint: op semantics, parameters, response schema,
   error taxonomy, and the evidence list above. The CLAUDE.md doc map
   gains a ravel-analytics row, docs/README.md indexes the new doc, and
   README.md documents the endpoint, all in the same commits as the
   behavior.

## Consequences

- A new public API surface exists that the Prometheus differential gate
  does not cover. Its correctness rests on the fixture and property
  gate in decision 6; that gate is the merge bar for any change to the
  crate.
- The stage inherits query-engine limits by construction: it can never
  see more data than a legal range query returns, and its own series
  and point caps bound detector cost (PELT worst case is quadratic in
  the capped 2000 points).
- No new external dependency: PELT, BIC, and the summary statistics are
  implemented in the crate.
- SQL and PromQL stay untouched. If ES|QL parity later demands more ops
  (sampling, forecasting), each lands as a new op behind the same
  endpoint and evidence regime, one ADR amendment per op family.
- The ES|QL mapping is partial by design: `CATEGORIZE` and the text
  commands stay out of scope while Ravel stores no log text.

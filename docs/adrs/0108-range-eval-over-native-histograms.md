# ADR-0108: Histogram-aware range evaluation for PromQL

Status: proposed

## Context

Native histograms flow end to end on the instant path: ingest admits them
(RW1/RW2/OTLP), storage round-trips them (`query_histograms`, ADR-0096's
queryfrag v3 frames), and the evaluator computes over them
(`FloatHistogram` math: `histogram_rate`, `sum_histograms`,
`avg_histograms`, `detect_reset`, `copy_to_scale`; instant
`rate/increase/delta` dispatch; `histogram_quantile/fraction/count/sum/avg`).
Range-top-level `histogram_quantile`/`histogram_fraction` work because each
grid step re-runs the instant path (issue #1081).

Range evaluation everywhere else silently destroys histogram data
(issue #525, p0):

1. **Silent zeros.** `eval_instant_over_grid`
   (`crates/ravel-promql/src/functions/mod.rs:567-583`) collapses each
   `InstantSample` into `Sample { ts_ns, value }`, discarding the
   `histogram` field. A histogram element's float `value` is a meaningless
   `0.0` placeholder by construction (`eval.rs:91-137`), so any range query
   whose per-step result contains histogram elements (top-level
   aggregates, binary expressions) emits an all-zero series with HTTP 200.
2. **Silent empties.** `eval_range_selector` (`eval.rs:1026-1089`) and the
   range reduction machinery (`eval_range_matrix_reduction`,
   `eval.rs:1105-1199`) fetch floats only; they never call
   `source.query_histograms`. Bare histogram selectors vanish; the range
   arms of `rate/increase/delta` ignore their histogram input
   (`functions/mod.rs:415-445`) even though the window reducer
   `histogram_extrapolated_rate` (`functions/rate.rs:108-142`) is ported,
   tested, and already fed by the instant arm; the entire `*_over_time`
   family reduces floats only (`functions/over_time.rs`).
3. **False absence.** `absent_over_time(h[15m])` builds its answer from the
   float-only fetch, sees no samples while histogram data flows, and
   affirmatively returns 1 (`over_time.rs:268-298`). A liveness alert on a
   histogram stream fires permanently inverted.

The one shape operators use in Grafana,
`histogram_quantile(0.99, sum(rate(h[5m])))`, works, which makes every
other shape more deceptive: the first query tried succeeds, suggesting full
support. Prometheus 3.x evaluates histograms in all of these paths.

The differential gate missed this because the corpus's only `kind: range`
native-histogram entries wrap in `histogram_quantile`/`histogram_fraction`
(`corpus/histogram_native.txt:142-157`), and the comparator has no concept
of a histogram-valued sample (`comparator.rs:95-161` reads only the JSON
`value` string).

The SQL surface is a related but separate gap: the `samples` table exposes
only scalar `value` and excludes histogram samples entirely
(`crates/ravel-sql/src/schema.rs:64-71`), and the generated
`docs/sql-conformance.md` never mentions the exclusion.

## Decision

Per path, exactly two acceptable end states: **bit-exact Prometheus 3.x
semantics** (verified against the pinned v3.13.1 binary by the difftest
lane) or a **typed 422 `Unsupported` refusal** (the existing
`Error::Unsupported { construct }` guard pattern, `eval.rs:968-977`,
mapped to 422 execution in `ravel-query/src/http/error.rs`). Silent zeros
and silent empties are forbidden in both.

`absent_over_time` (and `absent`) counts histogram samples as presence
under either state.

The default is semantics, because the hard part is already built: the
histogram math layer and the histogram window reducers exist and are
tested; the entire defect is range-side plumbing. Specifically:

1. **Range result channel.** Evaluator range results carry per-step
   histogram elements alongside floats. This stays internal to
   `ravel-promql` plus the `ravel-query` HTTP rendering; `ravel-types`
   `Sample` is untouched, and distributed fan-out already carries
   histograms (ADR-0096 queryfrag v3 frames).
2. **Grid path** (`RangeCore::Generic` — top-level aggregates and binary
   expressions): `eval_instant_over_grid` materializes histogram elements
   into the range matrix instead of dropping them, making range output
   faithful to the instant path per step. Per-operator aggregation
   semantics follow the oracle: `sum` and `avg` over histogram elements
   produce histogram elements (`sum_histograms`/`avg_histograms`),
   `count` produces floats, and the operators Prometheus leaves undefined
   for histograms (`min`, `max`, `stddev`, `stdvar`, `quantile`,
   `topk`, `bottomk`) drop-and-annotate exactly as the pinned binary
   does. `by`/`without` grouping preserves element type end to end;
   grouping code that rebuilds samples is held to the same standard as
   the arithmetic it wraps. Binary expressions stay faithful to whatever
   the instant path computes today; when #524 fixes instant binop
   semantics, range inherits the fix with no further work here, and
   binop-specific corpus entries wait for that fix rather than pinning
   today's known-wrong instant answers.
3. **Selector path**: `eval_range_selector` adds a `query_histograms`
   pass with per-step `pick_histogram` under the same left-open lookback
   rule the instant selector uses.
4. **Counter functions**: the range arms of `rate/increase/delta` wire in
   the existing `histogram_extrapolated_rate`, mirroring the instant arm.
5. **`*_over_time`**: `sum_over_time` and `avg_over_time` produce
   histograms, `count_over_time` a float, using the existing
   `sum_histograms`/`avg_histograms` helpers; `last_over_time` returns
   the histogram element it saw. Every remaining member
   (`min/max/stddev/stdvar/quantile_over_time`, `present_over_time`,
   `predict_linear`, `irate/idelta/resets/changes/deriv`) follows the
   oracle's behavior for histogram inputs exactly — drop-and-annotate
   where Prometheus drops, semantics where Prometheus defines them —
   verified per member against the pinned binary rather than assumed.
   Matching the oracle's drop-and-annotate behavior is itself bit-exact
   (warnings compare presence-only) and is categorically different from
   today's silent empties, which drop series the oracle returns.
6. **Instant-path aggregation parity.** New corpus entries can expose
   divergences on the instant side too (an aggregation the evaluator
   computes differently from the pinned binary over histogram inputs).
   Those are in scope here: fix them where the corpus proves divergence,
   so range and instant entries go green together. Binary-operator
   arithmetic remains #524's scope.
7. **Absent**: `absent_over_time`/`absent` treat a non-empty histogram
   fetch as presence.
8. **Refusals**: any path that cannot reach bit-exact parity returns
   `Error::Unsupported` with a named construct, registered as a rejection
   row in the difftest scoring table and surfaced in the generated
   conformance section of `docs/query-engine.md`. The expected set is
   near-empty; it is an escape hatch per path, not a phase.
9. **HTTP JSON**: matrix elements gain Prometheus' `histograms` field
   (`MatrixResult`/`range_value_to_json` in
   `crates/ravel-query/src/http/json.rs`), per element type per
   timestamp, so Grafana and the difftest client see the standard
   encoding.
10. **Difftest**: the comparator learns histogram-valued samples, comparing
    a canonical semantic form (schema, zero threshold/count, spans, bucket
    counts, `count`/`sum` floats under the existing bit-exact/ULP rules;
    the per-entry `tolerance:` field applies to histogram floats such as
    extrapolated rates computed independently by both engines), because
    two engines may encode the same histogram with different span
    layouts. New corpus entries cover, at minimum: range
    `sum(rate(h[5m]))`, range `rate(h[5m])`, bare `h` as a range query,
    `count_over_time(h[5m])`, `absent_over_time(h[15m])` with histogram
    data flowing, plus rejection-mode entries for anything refused. Each
    new range shape also gets an instant-kind counterpart entry proving
    both endpoints return the same class of answer (the asymmetry
    requirement). Each new test is demonstrated failing before its fix
    lands.
11. **SQL documentation**: the `samples` table's histogram exclusion is
    documented by editing the generator behind `docs/sql-conformance.md`
    and regenerating, never by hand-editing the artifact.
6. **Absent**: `absent_over_time`/`absent` treat a non-empty histogram
   fetch as presence.
7. **Refusals**: any path that cannot reach bit-exact parity returns
   `Error::Unsupported` with a named construct, registered as a rejection
   row in the difftest scoring table and surfaced in the generated
   conformance section of `docs/query-engine.md`. The expected set is
   near-empty; it is an escape hatch per path, not a phase.
8. **HTTP JSON**: matrix elements gain Prometheus' `histograms` field
   (`MatrixResult`/`range_value_to_json` in
   `crates/ravel-query/src/http/json.rs`), so Grafana and the difftest
   client see the standard encoding.
9. **Difftest**: the comparator learns histogram-valued samples, comparing
   a canonical semantic form (schema, zero threshold/count, spans, bucket
   counts, `count`/`sum` floats under the existing bit-exact/ULP rules),
   because two engines may encode the same histogram with different span
   layouts. New corpus entries cover, at minimum: range
   `sum(rate(h[5m]))`, range `rate(h[5m])`, bare `h` as a range query,
   `count_over_time(h[5m])`, `absent_over_time(h[15m])` with histogram
   data flowing, plus rejection-mode entries for anything refused. Each
   new test is demonstrated failing before its fix lands.
10. **SQL documentation**: the `samples` table's histogram exclusion is
    documented by editing the generator behind `docs/sql-conformance.md`
    and regenerating, never by hand-editing the artifact.

```mermaid
flowchart TD
    R["range request<br/>/api/v1/query_range"] -> RR["eval_range_annotated"]
    RR --> RC{"resolve_range_core"}
    RC -- Selector --> RS["eval_range_selector"]
    RC -- Call --> RF["functions::eval_range_call"]
    RC -- Binary/Aggregate --> GRID["eval_instant_over_grid<br/>per-step instant re-eval"]

    GRID --> AGG{"aggregation over<br/>histogram elements"}
    AGG -- "sum / avg" --> AH["histogram element out<br/>sum/avg_histograms"]
    AGG -- "count" --> AF["float out"]
    AGG -- "min/max/stddev/stdvar/<br/>quantile/topk/bottomk" --> AD["drop + annotate like oracle"]
    AH --> GM

    RS --> FQ["source.query (floats)"]
    RS --> HQ["source.query_histograms<br/>NEW: per-step pick_histogram"]
    FQ --> M["histogram-capable range matrix"]
    HQ --> M

    RF --> RED{"function kind"}
    RED -- "rate/increase/delta" --> HR["histogram_extrapolated_rate<br/>NEW: wired on range arm"]
    RED -- "sum/avg/count_over_time" --> HO["sum/avg_histograms helpers<br/>NEW: histogram branches"]
    RED -- "float-only over_time" --> FD["drop + annotate like Prometheus"]
    RED -- "absent_over_time" --> HA["presence = float OR histogram<br/>FIX: was float-only"]
    HR --> M
    HO --> M
    FD --> M
    HA --> M

    GRID --> GM["materialize value AND histogram<br/>FIX: dropped histogram"]
    GM --> M

    M --> J["json.rs: values + histograms field<br/>NEW: histograms rendering"]
    J --> CMP{"difftest comparator<br/>NEW: histogram-aware"}
    CMP -- "bit-exact" --> OK["pass"]
    CMP -- "cannot match" --> REF["Error::Unsupported<br/>422 execution"]
```

## Rejected alternatives

- **Guard-first refusals everywhere, semantics deferred.** It needs the
  same presence-detection fetches and the same corpus/comparator work, yet
  ends with the common Grafana shapes answering 422 indefinitely. Kept
  only as the per-path escape hatch in decision 8.
- **Keep dropping, add warnings.** A warning annotation does not help an
  alerting surface; the `absent_over_time` false positive survives intact,
  and a flat zero line with a warning header is still a wrong panel.
  Silently wrong data is precisely the defect being fixed.
- **Extend `ravel_types::Sample` with an optional histogram payload.**
  Touches a cross-crate contract for an evaluator-internal need; the
  parallel-channel approach changes no persistent or wire format, and
  ADR-0096's histogram frames already cover distribution.
- **Hand-edit `docs/sql-conformance.md`.** The file is generated;
  hand edits are overwritten on regeneration. Fix the generator.
- **Compare histograms byte-for-byte in the comparator.** Legitimate
  engines emit different span layouts for equal histograms; byte equality
  would fail on correct answers. Compare the canonical semantic form or
  not at all.

## Consequences

- The instant/range asymmetry disappears: every query shape returns the
  same class of answer on both endpoints, data or typed refusal.
- Panels outside the `histogram_quantile` idiom start working, or fail
  loudly with a 422 naming the construct, instead of plotting zeros.
- The comparator gains first-class histogram equality, which issue #524
  (instant binop coercion) reuses for its own corpus entries.
- The `promql-difftest` CI lane enforces every new entry against the
  pinned real Prometheus, so the silent-destruction class cannot
  regress unnoticed.
- `Value`/`RangeMatrix` growth ripples through `ravel-promql` call sites;
  the compiler bounds the churn, and no persistent format moves.
- Range evaluation adds a histogram read channel next to the float one.
  Its cost is bounded per query window, not per grid step: histogram
  reads ride the same read-cache gating as float scans (ADR-0102) or are
  hoisted to one window fetch per selector, never re-fetched per step of
  `eval_instant_over_grid`. Float-only tenants keep their current fetch
  pattern apart from the presence probes this decision requires.

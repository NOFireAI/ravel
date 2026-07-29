# ADR-0030: PromQL subquery point-cap divergence from Prometheus

Status: Accepted (2026-07-29). Written while resolving issue #224, which
listed five `promql-difftest` corpus mismatches found once the harness ran
against the real pinned Prometheus v3.13.1 binary. Two of them
(subquery.txt's `error_subquery_grid_exceeds_the_point_cap` and
`error_nested_subquery_inner_grid_exceeds_the_point_cap`, mismatches #4 and
#5) are not float-precision residue like the ADR-0025 cases; they are a
deliberate budget-enforcement divergence, and this ADR records the decision
to accept it. The other three mismatches (#2 and #3, float residue; #1, in
a separate crate) are out of this ADR's scope.

## Context

Ravel enforces a resolution cap of 11,000 range-evaluation points per query
(`DEFAULT_MAX_RANGE_POINTS`, docs/query-engine.md "Budgets", issue #77). The
cap is checked independently at every subquery evaluation node, against that
node's own grid, before the grid is built, at any nesting depth. A subquery
whose own grid alone exceeds the cap is rejected before allocation.

The two corpus entries exercise exactly that path:

| Entry | Query | Grid points requested |
|---|---|---|
| `error_subquery_grid_exceeds_the_point_cap` | `max_over_time(diff_gauge_walk[100d:1s])` | 8,640,001 |
| `error_nested_subquery_inner_grid_exceeds_the_point_cap` | `avg_over_time(max_over_time(diff_gauge_walk[50d:1s])[1m:1s])` | 4,320,001 (inner) |

Both were added as `mode: error` on the assumption, never checked against a
live binary at the time (the P10 completion report flagged this), that
Prometheus would reject an equivalently oversized subquery grid with a
comparable error class. With the real binary in the loop, that assumption is
false: **Prometheus succeeds on both; Ravel errors.**

## Investigation

The task posed two possibilities: (a) intentional, expected divergence, or
(b) the pinned binary actually has an equivalent limit and the harness is
missing a flag that would make it also error. Option (b) was checked first
and ruled out.

Prometheus v3.13.1 has no per-subquery-node step-count cap. Its only query
resource limits are:

- `--query.max-samples` (default 50,000,000): a budget on the number of
  samples held in memory during evaluation, counted as they are yielded.
  For these queries the underlying series is sampled every 30s over a
  ~1170s span, so a `[100d:1s]` or `[50d:1s]` subquery window yields on the
  order of a few thousand points (bounded by data coverage and the 5m
  staleness lookback), nowhere near any sane `max-samples`. Lowering
  `max-samples` far enough to trip these two would break unrelated corpus
  entries and still would not model a per-node *grid-size* cap, because it
  counts yielded samples, not requested grid steps.
- The `query_range` API's outer 11,000-step check. Both entries are
  `kind: instant`, so this check never runs.

There is no `scripts/fetch-prometheus.sh` or `prometheus_process.rs` flag
that makes the pinned binary reject these queries. Option (b) is not viable.

A second axis of divergence reinforces the point: Ravel maps a cap rejection
to `Error::TooManyPoints` -> `ApiError::Unsupported`, a 422 "execution"-class
budget error (crates/ravel-query/src/http/error.rs), whereas Prometheus'
own outer resolution limit is a 400 "bad_data" error. Even a hypothetical
query that made both engines error on resolution would still mismatch on
`errorType` class under the differential comparator.

## Decision

Accept the divergence (option (a)). The per-subquery-node point cap is a
deliberate Ravel budget with no Prometheus counterpart; it is not a
differential property of PromQL semantics, so there is nothing to make the
two engines agree on here.

The two corpus entries stay in subquery.txt as `mode: error`, with a comment
block recording the verified finding and citing this ADR. `mode: error`
still encodes Ravel's own expected behavior, and the cap's own behavior
stays covered by ravel-promql's `eval.rs` unit tests
(`DEFAULT_MAX_RANGE_POINTS`, the `TooManyPoints` mapping, and the
first-outer-step and nested-node rejection tests). They are not removed:
deleting them would drop subquery.txt below the coverage assertions
`corpus.rs`'s `subquery_corpus_file_parses_cleanly` test makes (at least
nine entries, at least one `ExpectError`), and `corpus.rs` is outside this
corpus-only change's scope.

This is a documentary allowlist, in the spirit of ADR-0025's per-entry
allowlist but for an error-status divergence rather than a float tolerance.
ADR-0025's tolerance mechanism does not apply: the difference here is not a
few bits of a shared f64 result, it is one engine erroring and the other
succeeding.

## Consequences

- Against the live pinned binary (the `RAVEL_DIFFTEST=1` job, not the
  default `cargo test`), these two entries remain divergent: the comparator
  (`comparator.rs`) has only `ExpectError` (both engines must error, matched
  by `errorType` class) and the success modes, and no mode for "Ravel errors
  by design, Prometheus succeeds." Turning these entries green rather than
  known-divergent needs a new comparator mode expressing a one-sided,
  by-design error. That is a harness change to `ravel-promql-difftest/src`,
  outside this corpus-only change, and is the specified follow-up. Until it
  lands, whoever runs the differential job against the binary should treat
  these two, and only these two, as the accepted divergence this ADR
  records.
- The cap itself is unaffected: it stays a documented, enforced budget
  (query-engine.md, issue #77), and its correctness stays owned by
  ravel-promql's unit tests, which do not depend on Prometheus agreeing.
- This ADR does not change any code. It records a decision about what the
  differential corpus is allowed to assert, alongside ADR-0021 (the
  comparator's semantics) and ADR-0025 (the float-tolerance allowlist).

## Alternatives

1. **Accept and document (chosen).** Lowest risk, faithful to the entries'
   intent, no unverifiable corpus changes.
2. Lower Prometheus' `--query.max-samples` so it also errors. Not chosen: it
   does not fire on these sparse-data subqueries, it would break unrelated
   corpus entries, and it models a memory budget, not a per-node grid cap.
3. Remove the two entries from the differential corpus (mirroring issue
   #177's move of non-differential aggregate cases to unit tests). Not
   chosen here: it would break `corpus.rs`'s own coverage assertions, and
   `corpus.rs` is outside this change's file scope. A future harness change
   could revisit this together with the one-sided-error comparator mode.
4. Reframe the entries as the shared outer-resolution cap (`kind: range`
   with more than 11,000 outer points). Not chosen: it would abandon the
   per-node-cap intent, and it fails anyway, because Ravel's cap error is
   422 "execution" while Prometheus' outer resolution error is 400
   "bad_data", so the `errorType` classes still would not match.

# ADR-0022: Floating aggregate exactness: allowlisted v1 subset, sequential sum and avg UDAFs, second-moment family excluded

Status: Proposed (2026-07-28). Resolves the half of review finding F7
(docs/reviews/2026-07-27-arrow-datafusion-plan-review.md) that the plan
deferred to a future ADR, and issue #160. Companion to ADR-0013; amends
the v1 aggregate subset in docs/arrow-datafusion-plan.md section 2.
Grouped min/max total-order semantics (issue #143) are a sibling gap
decided separately in ADR-0023; this ADR does not touch them.

## Context

The SQL surface's exactness rule (ADR-0013; docs/arrow-datafusion-plan.md
section 2) requires every DataFusion-executed operator to match an
independent reference bit-for-bit, compared by `f64::to_bits`, NaN
payloads and -0.0 significant. v1 executes aggregation single-partitioned
above the sort-preserving merge, so input order is deterministic and the
remaining variable is the accumulator algorithm itself.

`avg`/`mean` are excluded today as an interim measure: rejected at
validation (crates/ravel-sql/src/validate.rs, `reject_avg`) with an error
naming `SUM`/`COUNT` as the workaround, and deregistered in
`build_session` (crates/ravel-sql/src/session.rs), pending exactly this
ADR (review F7). Issue #160 then found the same design question asked a
second time: `stddev`, `var`, `stddev_pop`, `var_pop`, `covar_samp`,
`covar_pop`, and `corr` carry the same floating-mean accumulator property
that disqualified `avg`, yet remain registered and reachable through the
v1 endpoint, unverified by the differential gate. The finding generalizes
further than its own list: validation is a blacklist (`reject_avg`,
`reject_grouped_min_max`, nothing else), so every other aggregate
DataFusion registers by default (`median`, the `regr_*` regression
family, `approx_*`, `string_agg`, `array_agg`,
`first_value`/`last_value`/`nth_value`, the bit and bool aggregates) is
also reachable and also unverified.

What the pinned datafusion 54.1.0 accumulators actually do
(datafusion-functions-aggregate, read narrowly for this ADR):

- `avg` (`average.rs`, `AvgAccumulator`): per input batch it calls
  arrow's `compute::sum` kernel, which reduces lane-parallel partial
  accumulators whose lane count is architecture-dependent, then folds
  per-batch results with `+=`. No portable sequential reference can be
  bit-identical to it. This is the same root cause behind the ungrouped
  `sum` restriction the B3 gate already documents
  (crates/ravel-sql/tests/differential.rs): ungrouped `sum` is proptested
  only over values whose partial sums are exactly representable, a
  recorded deviation from the full-pool gate.
- The second-moment family (`variance.rs`, `stddev.rs`, `covariance.rs`,
  `correlation.rs`): the ungrouped accumulators are sequential Welford
  folds over (count, mean, m2) and their two-variable co-moment
  variants, deterministic in input order. But the same functions also
  ship dedicated `GroupsAccumulator` implementations selected by
  physical plan shape, and grouped correlation uses a different
  algorithm entirely, a sum-of-products state (count, sum_x, sum_y,
  sum_xy, sum_xx, sum_yy). The Partial/Final merge formula reconstitutes
  the mean as `mean*count/new_count + mean2*count2/new_count`, which is
  not a bit-level identity even when a single partial state merges into
  an empty accumulator: last-ULP drift in general, overflow to infinity
  for large-magnitude means. One SQL function name therefore maps to at
  least two floating-point algorithms plus a lossy merge, chosen by
  planner internals that can change on any upgrade without a release
  note.

## Alternatives

1. Keep the built-in accumulators and build references that mirror them
   (F7 amendment (b); issue #160's second option). Rejected. For `avg`
   it is impossible: the lane-parallel batch sum has no portable
   sequential equivalent. For the second-moment family it would mean
   pinning, per function, both the Welford path and the grouped
   sum-of-products path, the planner's mode and accumulator selection,
   the lossy merge formula, and the evaluate-time NaN special cases. The
   reference stops being independent and becomes a mirror that agrees
   with whatever upstream does, which is the failure mode F7 warned
   about, relocated from the tolerance knob into the reference itself.
2. Custom bit-exact UDAFs for the whole family, `avg` and second-moment
   alike. Owned semantics, stable across upgrades, but the second-moment
   half is seven-plus functions of co-moment recurrences, grouped
   adapters, reference implementations, and golden/proptest suites, with
   no recorded demand on the SQL surface. Rejected as a bundle; the
   `avg` half survives into the decision.
3. Exclude everything permanently, `avg` included, mirroring the grouped
   min/max treatment. Cheapest and honest, but `avg` is a baseline
   aggregate every SQL consumer expects, its exact semantics are
   trivially pinnable (`sum` and `count` are already admitted and gated;
   division is one correctly rounded IEEE operation), and the plan
   promised `avg` returns when an ADR pins its semantics. Rejected for
   `avg`, adopted for the second-moment family.
4. Hybrid with allowlist enforcement (chosen): admit `avg` as a custom
   sequential UDAF, replace the built-in `sum` with the same summation
   so `avg(x)` equals `sum(x)/count(x)` bit-for-bit on the engine
   surface, exclude the second-moment family, and flip enforcement from
   blacklist to allowlist so exclusion is the default state for
   everything not explicitly admitted.

## Decision

1. **Admission rule.** An aggregate enters the v1 SQL subset only when
   its entire compute path is a deterministic sequential scalar
   algorithm, written down in Ravel's docs, and matched bit-for-bit by
   an independent reference executor over the full adversarial value
   pool (NaN with varied payloads, +/-Inf, -0.0, denormals,
   large-magnitude and cancellation-prone values), in grouped and
   ungrouped form, under the existing single-partition rule. When a
   DataFusion built-in cannot meet this portably, Ravel either replaces
   it with a custom UDAF or excludes the function. No tolerance
   comparisons, ever.
2. **Allowlist enforcement.** The admitted set is `count`, `sum`, `min`,
   `max`, plus `avg`/`mean` once decision 4 is implemented.
   `build_session` becomes the hard boundary: it enumerates the
   registered UDAFs and deregisters every name not in the admitted set,
   replacing today's enumerated `avg`/`mean` deregistration, so a
   DataFusion upgrade that registers new default aggregates fails
   closed. validate.rs replaces `reject_avg` with a walk that rejects,
   with a typed error naming the admitted set, any function call whose
   bare lowercased name is a known excluded aggregate; a CI test asserts
   that this name list plus the allowlist exactly covers the UDAF names
   the default session registers, so a version bump that adds an
   aggregate breaks the test instead of silently widening the surface.
   `reject_grouped_min_max` remains a separate check owned by ADR-0023.
3. **Summation semantics** (for `sum`, and inside `avg`): the left fold
   of plain IEEE f64 addition over the non-null input values in the
   deterministic (series_id, ts) order, initialized with the first value
   rather than a zero seed. Empty input yields NULL; a group of all
   -0.0 values sums to -0.0. Naive summation, not Kahan: compensation
   buys no exactness here (the gate compares against a reference running
   the identical algorithm either way), it would diverge from the
   grouped `sum` fold already shipped and golden-pinned, and it would
   break the `SUM`/`COUNT` identity the `avg` rejection message has been
   promising as the workaround. Higher-accuracy summation is a different
   decision and re-enters through this same admission rule if ever
   wanted.
4. **`avg`/`mean` are admitted via a custom UDAF**: the decision-3 sum
   divided by the non-null row count in one correctly rounded IEEE
   division; a zero count yields NULL, never NaN or infinity. The row
   materialization cap keeps counts far below 2^53, so the count is
   exact as f64. Registered under both names, replacing the built-in
   whose lane-reduced batch sum is unpinnable.
5. **The built-in `sum` is replaced** by a custom UDAF with decision-3
   semantics. This lifts the restricted proptest pool for ungrouped
   `sum` (the documented B3 deviation) to the full adversarial pool. Two
   edge behaviors change: ungrouped sums over values with inexact
   partial sums move from architecture-dependent lane-order bits to the
   sequential fold's bits, and an all-(-0.0) group changes from +0.0
   (the built-in grouped accumulator's zero seed) to -0.0. Golden pins
   update in the same commit.
6. **The second-moment family is excluded**: `stddev`, `stddev_pop`,
   `var`, `var_pop`, `covar_samp`, `covar_pop`, `corr`, their aliases,
   and the `regr_*` regression aggregates, along with every other
   default aggregate outside the admitted set, all through decision 2.
   Readmission of any of them is a custom UDAF meeting decision 1 with
   its recurrence named in the plan document; that is an implementation
   ticket plus a plan amendment, not a new ADR.
7. **Gate evidence before an admitted function ships**: (a) golden cases
   with stored expected bits for architecture-independent results:
   exact finite sums, signed infinities, -0.0 preservation, empty and
   all-NULL inputs; (b) golden NaN cases asserting engine-vs-reference
   bit equality on the same host plus the properties that are
   architecture-independent (the result is NaN, the sign of infinite
   results), because NaN payload propagation through f64 addition is
   hardware-chosen; this differs from min/max, whose stored golden NaN
   bits are sound because `total_cmp` selects an input value and never
   synthesizes one; (c) proptest over the full adversarial pool, grouped
   and ungrouped, asserting bit-identical results; (d) the suite re-runs
   on every DataFusion version bump per the plan's upgrade policy.
8. **Sequencing**: two tickets. First, exclusion: decision 2 lands with
   `avg`/`mean` still excluded, closing issue #160's live unverified
   surface immediately. Second, admission: decisions 3 to 5 and 7 land
   together, flipping `avg`/`mean` into the allowlist in the same commit
   as the UDAFs and their gate evidence.

## Consequences

- `avg` returns to v1 with pinned, documented,
  architecture-independent semantics; the F7 interim rejection ends when
  the admission ticket lands, and the rejection error keeps naming
  `SUM`/`COUNT` until then.
- Issue #160 is resolved by exclusion, and the aggregate surface becomes
  fail-closed under dependency upgrades: the reachable aggregates are
  the enumerated admitted set, nothing else, enforced at both validation
  and registration. The audit acceptance test
  (`stddev_and_variance_family_must_be_handled_like_avg`) passes without
  a per-function reject list to maintain.
- `sum`'s bits change in the two edges named in decision 5. The prior
  ungrouped behavior was architecture-dependent and never a stable
  contract, and -0.0 preservation is the IEEE-consistent answer, but any
  consumer bit-comparing results across the upgrade sees the change.
- Ravel takes on two small custom UDAFs to maintain; in exchange the
  differential gate loses its one documented deviation and the reference
  executor stays independent instead of mirroring upstream internals.
- Dispersion statistics stay unavailable on the SQL surface. A user can
  compose them from admitted aggregates (`sum(v*v)`, `sum(v)`,
  `count(v)`); each admitted aggregate is exact, and the numerical
  behavior of the composition is the user's own visible expression,
  consistent with the exactness invariant.
- ADR-0023 continues to own grouped min/max total-order semantics;
  nothing here changes its scope.

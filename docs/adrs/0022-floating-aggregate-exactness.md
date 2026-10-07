# ADR-0022: Floating aggregate exactness: allowlisted v1 subset, avg admitted via a sequential UDAF, second-moment family excluded

Status: Accepted. Amended by ADR-0825: decision 3's grouped execution
path (the plain-accumulator-behind-the-adapter clause) is replaced by a
sequential-order GroupsAccumulator with identical bits, and integer input
moves to exact i128 accumulation instead of the f64 fold. Amended by the
DISTINCT ON amendment below: `first_value` stays registered for the
optimizer's `DISTINCT ON` rewrite while statements that name it are still
refused.

Companion to ADR-0013; amends the v1 aggregate subset it defines. Grouped
min/max total-order semantics are a sibling gap decided separately in
ADR-0023; this ADR does not touch them. Whether the built-in `sum` should
also move to a sequential-fold UDAF (for architecture-independent results
and so it agrees bit-for-bit with the `avg` UDAF this ADR admits) is
intentionally out of scope: `sum` is already an admitted, shipping, gated
aggregate, and changing its output bits is a separate decision from the
admission policy this ADR sets. That question is tracked, not decided, in
ADR-0024 (Proposed).

## Context

The SQL surface's exactness rule (ADR-0013) requires every
DataFusion-executed operator to match an
independent reference bit-for-bit, compared by `f64::to_bits`, NaN
payloads and -0.0 significant. v1 executes aggregation single-partitioned
above the sort-preserving merge, so input order is deterministic and the
remaining variable is the accumulator algorithm itself.

`avg`/`mean` are excluded today as an interim measure: rejected at
validation (crates/ravel-sql/src/validate.rs, `reject_avg`) with an error
naming `SUM`/`COUNT` as the workaround, and deregistered in
`build_session` (crates/ravel-sql/src/session.rs), pending exactly this
ADR. A later review then found the same design question asked a
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

1. Keep the built-in accumulators and build references that mirror them.
   Rejected. For `avg`
   it is impossible: the lane-parallel batch sum has no portable
   sequential equivalent. For the second-moment family it would mean
   pinning, per function, both the Welford path and the grouped
   sum-of-products path, the planner's mode and accumulator selection,
   the lossy merge formula, and the evaluate-time NaN special cases. The
   reference stops being independent and becomes a mirror that agrees
   with whatever upstream does, which is the failure mode the review
   warned about, relocated from the tolerance knob into the reference
   itself.
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
   sequential UDAF with its own internal, independent summation over its
   own operands (not derived from or shared with the public `sum`
   aggregate), exclude the second-moment family, and flip enforcement
   from blacklist to allowlist so exclusion is the default state for
   everything not explicitly admitted. This does not make `avg(x)`
   bit-identical to `sum(x)/count(x)` against today's built-in `sum` (a
   lane-parallel kernel, unpinnable per the Context section); that
   coherence property is real but is a reason to consider changing
   `sum`, a separate, already-shipping, already-gated surface, which
   ADR-0024 takes up on its own rather than folding into this admission
   decision.

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
   registered UDAFs and deregisters every name not in the admitted set
   (except `first_value`, which the DISTINCT ON amendment below keeps),
   replacing today's enumerated `avg`/`mean` deregistration, so a
   DataFusion upgrade that registers new default aggregates fails
   closed. validate.rs replaces `reject_avg` with a walk that rejects,
   with a typed error naming the admitted set, any function call whose
   bare lowercased name is a known excluded aggregate; a CI test asserts
   that this name list plus the allowlist exactly covers the UDAF names
   the default session registers, so a version bump that adds an
   aggregate breaks the test instead of silently widening the surface.
   `reject_grouped_min_max` remains a separate check owned by ADR-0023.
3. **Summation semantics inside `avg`'s own UDAF** (this does not touch
   the public `sum` aggregate, which stays DataFusion's built-in,
   unchanged; see ADR-0024): `avg`'s internal numerator is the left fold
   of plain IEEE f64 addition over its own non-null input values in the
   deterministic (series_id, ts) order, initialized with the first value
   rather than a zero seed. Empty input yields NULL; a group of all -0.0
   values folds to -0.0. Naive summation, not Kahan: compensation buys no
   exactness here, since the gate compares against a reference running
   the identical algorithm either way, and higher-accuracy summation is a
   different decision that re-enters through this same admission rule if
   ever wanted. Amended by ADR-0825: this f64 fold only runs when `avg`'s
   argument resolves to Float64. An argument that resolves to an admitted
   integer type instead coerces to Int64 and sums exactly in `i128`,
   which needs no fold-order pinning at all, integer addition is
   associative.
4. **`avg`/`mean` are admitted via a custom UDAF**: decision 3's internal
   fold divided by the non-null row count in one correctly rounded IEEE
   division; a zero count yields NULL, never NaN or infinity. The row
   materialization cap keeps counts far below 2^53, so the count is
   exact as f64. Registered under both names, replacing the built-in
   `avg`/`mean` whose lane-reduced batch sum is unpinnable. This
   replacement is scoped to the `avg`/`mean` names only and has no effect
   on the separately-registered `sum` UDAF.
5. **The second-moment family is excluded**: `stddev`, `stddev_pop`,
   `var`, `var_pop`, `covar_samp`, `covar_pop`, `corr`, their aliases,
   and the `regr_*` regression aggregates, along with every other
   default aggregate outside the admitted set, all through decision 2.
   Readmission of any of them is a custom UDAF meeting decision 1 with
   its recurrence documented; that is an implementation task plus a doc
   amendment, not a new ADR.
6. **Gate evidence before an admitted function ships**: (a) golden cases
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
   on every DataFusion version bump per the upgrade policy. Items (b) and
   (c) are narrowed for NaN results by the NaN propagation amendment
   below.
7. **Sequencing**: two steps. First, exclusion: decision 2 lands with
   `avg`/`mean` still excluded, closing the live unverified surface
   immediately. Second, admission: decisions 3, 4 and 6 land together,
   flipping `avg`/`mean` into the allowlist in the same commit as the
   UDAF and its gate evidence.

## Consequences

- `avg` returns to v1 with pinned, documented,
  architecture-independent semantics; the interim rejection ends when
  the admission change lands, and the rejection error keeps naming
  `SUM`/`COUNT` until then.
- The unverified-aggregate gap is resolved by exclusion, and the aggregate surface becomes
  fail-closed under dependency upgrades: the reachable aggregates are
  the enumerated admitted set, nothing else, enforced at both validation
  and registration. The DISTINCT ON amendment below keeps `first_value`
  registered for one optimizer rewrite and enforces its exclusion on the
  planned statement instead of at registration. The audit acceptance test
  (`stddev_and_variance_family_must_be_handled_like_avg`) passes without
  a per-function reject list to maintain.
- The public `sum` aggregate is untouched by this ADR: its bits, and its
  documented restricted-pool gate deviation (crates/ravel-sql/tests/
  differential.rs), stay exactly as they are today. `avg(x)` computed by
  the new UDAF is therefore not guaranteed bit-identical to
  `sum(x)/count(x)` computed via the current built-in `sum` in every
  case; ADR-0024 (Proposed, undecided) takes up whether to change that.
- Ravel takes on one small custom UDAF (`avg`/`mean`) to maintain; the
  differential gate gains full-pool coverage for it without needing to
  mirror upstream's unpinnable internals.
- Dispersion statistics stay unavailable on the SQL surface. A user can
  compose them from admitted aggregates (`sum(v*v)`, `sum(v)`,
  `count(v)`); each admitted aggregate is exact, and the numerical
  behavior of the composition is the user's own visible expression,
  consistent with the exactness invariant.
- ADR-0023 continues to own grouped min/max total-order semantics;
  nothing here changes its scope.

## Amendment: NaN propagation

<!-- amendment-applies: sections="Decision" pointer="NaN propagation amendment" -->

Dated 2026-10-04. Tracked in issue #2558.

Decision 6 asked for two things that cannot both hold for a result that is
NaN. Item (b) asked for engine-versus-reference bit equality of NaN results
on the same host, on the reasoning that NaN payload propagation through f64
addition is hardware-chosen. Item (c) asked the full-pool proptest to assert
bit-identical results. Both assumed that one host gives one answer.

It does not. When both operands of an addition are NaN, IEEE 754 requires a
quiet NaN and leaves which operand's sign and payload it carries to the
implementation. On x86-64 `addsd` returns the first operand's. The compiler
chooses which operand is first: LLVM treats `fadd` as commutative and may
swap them. The engine's fold and the reference's fold are the same
expression, `acc + v`, compiled in two different crates, so they can differ
on one host whenever the two crates are optimised differently.

That was observed. Every gate build compiled both crates unoptimised, and
the folds agreed. With third-party dependencies at `opt-level = 2`, grouped
`sum`, whose fold lives in DataFusion, returned `0x7FF8000000000001` for a
group where the reference returned `0xFFF8000000000001`: the same NaN with
the other sign, on the first generated case. The differential gate already
carried this carve-out for `avg`, described in its module header; grouped
`sum` was still compared bit for bit.

What the gate asserts from now on, for `avg` and for grouped `sum`:

- A result that is NaN on either side must be NaN on both. Its sign and
  payload are not compared.
- Every other result is compared bit for bit, including signed zeros and
  signed infinities.

What this does not change:

- Ravel returns a conforming result. Neither bit pattern above is wrong.
- Nothing stored is affected. The difference is in the result of an
  aggregate over NaN inputs, computed at query time, and only for a group
  that holds NaNs of different sign or payload.
- min/max are untouched: `total_cmp` selects an input value and never
  synthesizes one, so their NaN bits stay asserted, as decision 6 says.

A user-visible consequence, which the user documentation must not
contradict: the sign and payload of a NaN returned by `sum`, `avg` or `mean`
are not specified, and may differ between builds of Ravel.

## Amendment: DISTINCT ON and the registered `first_value`

<!-- amendment-applies: sections="Decision|Consequences" pointer="DISTINCT ON amendment" -->
<!-- amendment-supersedes: phrase="deregisters every name not in the admitted set" pointer="DISTINCT ON amendment" -->

Dated 2026-10-07. Tracked in issue #2629.

DataFusion's `replace_distinct_aggregate` optimizer rule rewrites every
`SELECT DISTINCT ON (...)` into an aggregate grouped by the ON expressions,
whose other output columns are `first_value(column ORDER BY <the statement's
ORDER BY>)`. The rule looks `first_value` up by name in the session registry,
after the statement has been planned. Decision 2's deregistration removed it,
so every `DISTINCT ON` failed with "There is no UDAF named first_value in the
registry".

What changes:

1. **`first_value` stays registered, and stays excluded.** `build_session`
   spares the names in `REWRITE_ONLY_AGGREGATES` (crates/ravel-sql/src/
   session.rs), today `first_value` alone, from the deregistration loop.
   The name stays in validate.rs's `EXCLUDED_AGGREGATES`, so the text walk
   still refuses a statement that names it, with the same
   `ExcludedAggregate` error. For this one name, the fail-closed boundary
   that deregistration provided moves to the planned statement: the
   executor walks the plan `SessionContext::sql` returns, subqueries
   included, before any optimizer rule runs, and refuses any
   `first_value` call it finds, as an aggregate or as a window aggregate.
   That plan holds only what the statement itself wrote, so the check
   covers every spelling that resolves to the function, a quoted
   identifier the text walk does not match included, and never sees the
   calls the rewrite adds later. The window registry still deregisters
   `first_value` (ADR-0097 decision 6).
2. **`DISTINCT ON` is admitted only with a total-order `ORDER BY`.** Every
   selected column must be a plain column that an `ORDER BY` term names.
   This is the condition the spill-eligibility check already applies to a
   `DISTINCT ON` node (ADR-0954). A statement that fails it, including one
   with no `ORDER BY`, is refused on the planned statement with
   `ValidationError::DistinctOnOrderNotTotal`, an HTTP 400. Under a total
   order, two rows that tie on the `ORDER BY` agree in every returned
   column, so the row each ON group keeps does not depend on scan order,
   partition count, or the order partial `first_value` states merge in,
   and decision 1's exactness holds. Under a tied order the rewrite keeps
   whichever tied row it meets first, which the statement does not
   determine.
3. **Parallel final aggregation is unchanged.** ADR-0094 already
   classifies a `DISTINCT ON` by its ON keys; with a non-float key the
   final `first_value` aggregation fans out across partitions, which is
   exact under a total order.

Evidence: `tests/distinct_on.rs` in ravel-sql runs three total-order
statements over three segments at 4, 8 and 16 partitions with the final
aggregation fanned out, five times each, and asserts the exact rows,
including a group whose candidates are `0.0` and `-0.0` and one whose
candidates are two NaN payloads, each split across segments. The same file
asserts the refusals. `session::tests::distinct_on_keeps_an_input_order_row_unless_its_order_is_total`
shows, on the bare session, that a tied order returns a different row when
the same two rows arrive in the other order, and a total order does not.
`admitted_and_excluded_cover_all_registries_for_every_table` now requires
every rewrite-only name to be excluded and `build_session` to register
exactly the admitted aggregates plus the rewrite-only ones.

The condition is conservative. `SELECT DISTINCT ON (series_id) series_id, ts,
value FROM samples ORDER BY series_id, ts DESC` is refused although `ts` is
unique within a series; adding `value` as a trailing term admits it.
`first_value` itself is not admitted: readmitting it under its own name is a
decision 1 admission, not a consequence of this amendment.

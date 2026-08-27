# ADR-0825: Grouped aggregation accumulator path: flat per-group state, exact integer avg

Status: Accepted. Amends ADR-0022 (decision 3's grouped execution path for
`avg`) and ADR-0094 (decision 1's classification gains an integer-`avg`
arm). Sibling to ADR-0023, whose float min/max UDAF shares the mechanism
decided here. Issue #825.

## Context

### The measurement

The first CPU profile of the query path (issue #825): a cold `--runs 1`
pass over the 43-statement ClickBench corpus, pprof armed. pprof samples
on-CPU time only, so object-store waits are invisible; this is a pure
compute profile.

- `GroupsAccumulatorAdapter::invoke_per_accumulator` (merge_batch path):
  **42.67% of all CPU**.
- `GroupedHashAggregateStream::group_aggregate_batch`: 43.83%.
- `RepartitionExec::pull_from_input`: 21.62%.
- All RLOG decode combined: ~5%.
- `SequentialAvgAccumulator`'s own methods: `merge_batch` 0.38%,
  `update_batch` 0.12%.

The 42.67% is adapter machinery, not arithmetic. It also explains the two
failing statements: q33 (`GROUP BY "WatchID","ClientIP"` with
`AVG("ResolutionWidth")`, roughly 10^8 nearly-unique groups) exhausts the
8 GiB per-query pool; q29 (`AVG(length("Referer"))`, `MIN("Referer")`,
regexp group key) dies in the aggregate stream's spill attempt (`Memory
Exhausted while SpillPool (DiskManager is disabled)`). q32 is q33 plus a
row filter and completes in 27.1 s, second slowest in the corpus. A cost
that appears when a filter is removed and cardinality rises is a per-group
cost curve, not a scan cost.

One thing this profile does NOT say: that the open I/O tickets (#790, #815,
#520, #811) are low-value. They target object-store waits, which an
on-CPU profile cannot see at all. This profile ranks compute; it says
nothing about wall-clock spent waiting. The correct reading is narrower:
of the CPU the query path burns, the adapter is the single largest owner,
and no decode-side ticket can recover more than its ~5-15% share.

### The mechanism

`crates/ravel-sql/src/avg.rs` returns `false` from
`groups_accumulator_supported`, so grouped execution wraps the plain
accumulator in DataFusion's `GroupsAccumulatorAdapter`
(`datafusion-functions-aggregate-common-54.1.0/src/aggregate/
groups_accumulator.rs`). Read narrowly, the adapter costs, per group and
per batch:

- Per group, resident: an `AccumulatorState` holding a
  `Box<dyn Accumulator>` (a separate heap allocation for the 24-byte
  `(sum, count)` state) plus a retained `indices: Vec<u32>` scratch
  buffer. `size()` is `accumulator.size() + size_of_val(self) +
  indices.allocated_size()`.
- Per batch: one pass pushing every row index into its group's `indices`
  vec, a full `take_arrays` copy of the batch reordered group-contiguous,
  a `slice_and_maybe_filter` plus a virtual `f(state.accumulator, ...)`
  call per touched group, and two `state.size()` recomputations per
  touched group for allocation tracking.

For a `GROUP BY` over 10^8 keys the resident state alone is roughly
100+ bytes per group against the ~25 bytes the accumulator's actual state
needs, and the per-batch work is dominated by bookkeeping whose cost is
proportional to touched groups, not rows. That is where the 42.67% and
q33's pool exhaustion both come from. Fixing memory accounting (#740)
would change how precisely this cost is charged, not its size.

One adapter property is load-bearing for the decision below: within a
group, the adapter preserves input row order (indices are pushed in scan
order and concatenated in order), so each group's values are folded in
row order. A replacement that iterates rows in order and folds into flat
per-group state performs the identical per-group fold and produces
identical bits.

### Who actually holds the determinism guarantee

ADR-0022 decision 3 requires `avg`'s numerator to be a sequential IEEE
f64 fold in the deterministic `(series_id, ts)` order; ADR-0094 and its
issue #771 amendment record that the partial-state merge is f64 addition
and therefore order-dependent, which keeps `avg` out of the repartitioned
final. The consumers of that guarantee, named:

- ADR-0013's differential gate: bit-for-bit equality against an
  independent reference, `f64::to_bits`, NaN payloads and -0.0
  significant. This is enforced, internal, and applies to the whole SQL
  surface.
- The product invariant "exact semantics by default": a query rerun over
  unchanged data returns the same bits, logs and metrics SQL alike. The
  SQL session builder is shared; there is no logs-only or metrics-only
  `avg`.
- PromQL is NOT a consumer: `crates/ravel-promql/src/aggregate.rs` has
  its own `group_avg` evaluator and never touches this UDAF.

So the split the profiling question suggests ("load-bearing for metrics
but not for logs SQL") is the wrong axis: the guarantee is per-surface,
not per-signal. The axis that does split cheaply is input type. Metrics
SQL aggregates Float64 sample values, so it keeps the float path by
construction; the ClickBench statements aggregate integers
(`ResolutionWidth`, `IsRefresh`, `length(...)`), where exactness can be
had by arithmetic instead of by ordering.

And the framing's central premise is a false dilemma. "A
`GroupsAccumulator` is fast BECAUSE it vectorises, which the ordering
requirement forbids" conflates two separable properties. What the
ordering requirement forbids is lane-parallel reduction (arrow's
`compute::sum`), which reorders the fold. What makes a
`GroupsAccumulator` fast is mostly flat per-group state and the absence
of per-group boxing, slicing, and dynamic dispatch. A scalar row-order
loop over flat state keeps the fold order bit-for-bit and sheds the
adapter. The 42.67% is not the price of determinism; it is the price of
not having written that loop. ADR-0022 decision 3's "no sequential
vectorized groups accumulator for v1" was a scoping choice, and this ADR
is the ADR that ends it.

### A recorded pre-existing gap (reported, not fixed here)

ADR-0094's Context records the confirmed plan shape: a `Partial`
`AggregateExec` per scan partition, collapsed by `CoalescePartitionsExec`
into one stream feeding the single `Final`. When the logs scan runs with
`fetch_concurrency > 1`, one group's partial states arrive at the `Final`
in completion order, which is not stable across runs, and
`SequentialAvgAccumulator::merge_batch` folds partial sums in arrival
order. avg.rs's own bit-reproducibility argument assumes "one partial
state per group". For grouped `avg` over a multi-partition logs scan that
assumption does not hold, and the result bits can vary run to run in the
last ULPs today. This contradiction between avg.rs's stated precondition
and the actual plan predates this ADR. Decision 2 makes integer-input
`avg` immune by construction; the float path's residual gap needs its own
ticket and is reported in this task's final message, per the repo rule.

### min/max

`crates/ravel-sql/src/session.rs` registers `total_order_min_udaf` and
`total_order_max_udaf` (ADR-0023). Their `groups_accumulator_supported`
returns false only for float input; non-float input delegates to the
built-in's vectorised grouped path (`crates/ravel-sql/src/minmax.rs`). So
float `min`/`max` share the adapter path and its costs; q29's
`MIN("Referer")` does not (Utf8 input, built-in `MinMaxBytes` grouped
accumulator, already vectorised). Unlike `avg`, the total-order fold is
associative and commutative (minmax.rs says so, and ADR-0103 decision 4
leans on the same fact), so a vectorised float min/max
`GroupsAccumulator` is deterministic with no ordering precondition at
all. Its absence is also pure scoping debt, but it is not on this
corpus's hot path.

## Decision

1. **Float-input `avg`/`mean`: a custom sequential-order
   `GroupsAccumulator`, bit-identical to today.** Flat state: a Float64
   sums vector (null until a group's first value, preserving the
   first-value seed and the all-`-0.0` behavior) and an Int64 counts
   vector. `update_batch` iterates rows in input order and folds each
   value into its group's slot with plain IEEE addition; `merge_batch`
   folds partial sums the same way; no arrow reduction kernels anywhere.
   Because the adapter also folds each group in input row order, this
   produces identical bits to the shipped path on the same input stream.
   No semantics change, no gate deviation, ADR-0022 decisions 3 and 4
   untouched except for the sentence that forbade a groups accumulator.
2. **Integer-input `avg`/`mean`: exact integer accumulation, deterministic
   by construction.** The UDAF stops delegating coercion for integer
   arguments: resolved integer inputs (Int8 through Int64, UInt8 through
   UInt32; Int64 is the only integer the ADR-0101 declared-column
   vocabulary admits in practice) coerce to Int64 and keep that type in
   the analyzed plan; the return type stays Float64. The accumulator sums
   into i128 (flat vector per group), with checked addition surfacing a
   typed internal error on the unreachable overflow (exceeding i128 needs
   more than 2^63 rows of i64 extremes in one group; the row cap sits far
   below). Partial state is `(Decimal128(38, 0) sum, Int64 count)`;
   Decimal128 carries the i128 exactly for any sum reachable under the
   row cap, and the pack is checked, not assumed. Evaluation is two
   documented, portable roundings: i128 to f64 (round to nearest, ties to
   even, Rust's defined `as` semantics) then an IEEE division by the count.
   **This is not in general the correctly rounded exact mean.** Converting
   the sum first can lose information that a rational-to-f64 rounding of
   `sum/count` would keep, so for a sum beyond 2^53 the result may differ
   by an ulp from the exactly rounded value. That is accepted, and it is
   not what this decision buys: the property being bought is that every
   partitioning and merge order yields the SAME bits, not that those bits
   are the closest f64 to the true rational mean. Exact rational rounding
   is a strictly further step, is not required by ADR-0094's admission
   rule, and is not taken here. The float path has never had it either.
   Whatever this evaluates to, it evaluates to it identically everywhere. The fold and merge are integer addition:
   associative and commutative, so every partitioning and every merge
   order yields identical bits. The determinism guarantee for integer
   `avg` is thereby kept by exactness instead of by ordering, satisfying
   ADR-0022 decision 1's admission rule with a stronger property than the
   sequential-fold clause it was written around. This is the route
   ADR-0094's rejected-alternatives section and the #771 amendment both
   left open by name.
3. **Amend ADR-0094 decision 1: `avg`/`mean` over a resolved integer
   input is exact-typed.** The #771 amendment's second obstacle (after
   coercion, `avg(int_col)` and `avg(float_col)` are indistinguishable by
   resolved type) is dissolved by decision 2, which removes the coercion
   rather than reaching around it: the analyzed plan now carries Int64,
   and the classifier gains one arm reading it. `avg` over float input
   remains ineligible **for ADR-0094's repartitioned final specifically**,
   as does any float `sum`/`min`/`max` and any float group key. That is a
   statement about parallel-final admission, not about whether these
   aggregates may have a `GroupsAccumulator`: decision 4 gives float
   `min`/`max` exactly that, and the two are independent; the #771 amendment's rejection of classifier-only
   admission stands and is superseded only because the arithmetic
   underneath changed.
4. **Float-input `min`/`max`: a custom total-order `GroupsAccumulator`**,
   flat `Option<f64>`-per-group state folded under `f64::total_cmp`,
   replacing the adapter path the same way. The fold is associative, so
   there is no ordering precondition to preserve and no semantics
   question: same bits, less machinery. Included because it is the same
   defect class in a sibling this ADR already had to read; sequenced
   second because no statement in this corpus exercises it. What is
   scoped OUT: admitting float `min`/`max` aggregates to ADR-0094's
   repartitioned final. ADR-0103 decision 4 already argues the
   order-insensitivity, but that admission changes plan shape for a class
   of queries this profile never measured, and it belongs to its own
   evidence-carrying amendment, not to this ADR's ride-along.
5. **Scope: per resolved input type, global.** One rule for every tenant,
   every signal, both tables, no configuration. Metrics SQL keeps today's
   float semantics because its values are Float64, not because a switch
   says so. No flag: a knob that changes result bits is worse than either
   semantics it selects, because it makes the answer a function of
   deployment.

## What this ADR does not do

- **Drop or loosen float `avg` determinism.** No tolerance policy, no
  "logs are approximate" carve-out. ADR-0013's gate stays bit-exact;
  ADR-0094 already rejected the unconditional-repartition route and this
  ADR does not reopen it.
- **Kahan or compensated summation.** It changes bits without restoring
  associativity, so it buys neither exactness (the gate compares against
  a reference running the same algorithm) nor parallel admission.
  ADR-0022 decision 3 declined it once already.
- **Rewrite `avg(x)` to `sum(x)/count(x)` in the plan.** The #771
  amendment listed it; decision 2 is strictly cleaner: it keeps one
  function name mapping to one documented algorithm instead of splitting
  `avg`'s semantics across a rewrite (with `sum`'s empty-group NULL and
  overflow-wrap edges) and a native path.
- **Treat #740 as the fix.** Accounting that charges the adapter's
  allocations more precisely changes when q33 fails, not whether the
  state fits. The accounting work is still worth landing for its own sake
  (a pool that undercounts is a pool that lies), but it is downstream of
  this ADR for these statements, not upstream.
- **Touch the public `sum` aggregate** (ADR-0024's open question) or any
  frozen contract. The partial-state schema change in decision 2 is
  process-internal physical-plan state; it never crosses the Flight wire
  (ADR-0071 workers ship scan fragments, not aggregate states) and is not
  a persisted format.
- **Per-signal or per-tenant semantics.** Rejected under decision 5.

## Consequences

- **Acceptance, adapter share:** on the same 43-statement cold pass with
  the same pprof harness, `GroupsAccumulatorAdapter` machinery falls from
  42.67% of on-CPU samples to under 5% (after decisions 1, 2, and 4 the
  only remaining adapter users are non-owned delegate types that this
  corpus never exercises; the figure is asserted by the profiling lane,
  present exactly once and inside the band, per the measurement rules).
- **Acceptance, q33:** completes under the 8 GiB pool with decision 2
  plus decision 3 (exact integer `avg` admits the statement to the
  repartitioned final; flat state cuts resident aggregate state to ~25
  bytes/group before keys and table overhead). Pre-registered band: q33
  completes within 2x q32's post-change wall clock. Honest fallback,
  stated up front: at roughly 10^8 nearly-unique groups the group keys
  and hash table alone approach the budget; if q33 still exhausts the
  pool with adapter state gone, the residual is partial-stage state, it
  is #737's problem by construction, and the pool-exhausted error must
  then name a figure consistent with keys-plus-table, not accumulators.
- **Acceptance, q29:** completes (its `AVG(length(...))` becomes integer
  and its group key is Utf8, so the whole statement becomes exact-typed
  and leaves both the adapter and the serial final).
- **Determinism tests that fail when the property is violated, not
  comments asserting it:** (a) a partition-shuffle proptest for integer
  `avg`: random re-partitionings and merge orders of one adversarial
  input must yield identical bits, which fails if any order-dependent
  step is reintroduced; (b) a bit-equality proptest driving the decision
  1 accumulator and `SequentialAvgAccumulator` over the full adversarial
  pool (NaN payloads, -0.0, denormals, cancellation) on identical input
  order, which fails if the flat fold diverges from the sequential fold;
  (c) plan-shape tests: `avg(float_col)` keeps the single-partition
  final, `avg(int_col)` fans out.
- **A visible semantics change for integer `avg`, stated as a headline:**
  results can differ from today in the last ULPs whenever the running sum
  crossed 2^53, because the exact sum rounds once (twice counting the
  conversion) instead of at every step. The new value is the correctly
  rounded mean of the exact sum: deterministic, portable, and closer to
  the true mean than any step-rounded fold. The differential gate's
  reference switches to the same integer algorithm in the same commit;
  golden files that pinned step-rounded integer bits are regenerated
  deliberately, never patched to pass.
- The #771 pinned test
  `avg_over_int_column_carries_a_float64_partial_sum_state` reddens by
  design (the state becomes Decimal128): it guarded the old premise and
  is rewritten in the same commit as decision 2 to pin the new state
  schema instead. `analyzer_coerces_avg_argument_to_float64` likewise
  flips to assert integer preservation.
- avg.rs's module doc, session.rs's determinism comment, ADR-0022's
  decision 3 sentence, and docs/query-engine.md's exact-typed note all
  change in the implementing commits; ADR-0022 and ADR-0094 carry
  amendment pointers to this ADR.
- The adapter share this ADR removes was measured on a corpus with no
  float `avg` and no float `min`/`max`; decision 1 and decision 4 are
  justified by mechanism identity, not by this profile, and their lanes
  carry their own bit-equality gates rather than borrowing q33's numbers.

# ADR-0103: Order-insensitive aggregation pushdown

Status: Accepted (amends ADR-0071)

## Context

ADR-0071's distributed read fan-out moves scan I/O to slice workers but
still funnels every fetched raw sample back to the coordinator for
aggregation. Its own Consequences section names the result explicitly:
"the coordinator remains the ceiling for high-cardinality final
aggregation until a future pushdown ADR" (docs/adrs/0071, line 258) —
this is that ADR, for the subset of aggregates it names as safe to
attempt first: `count`, `min`, `max`, and distinct group-key enumeration
(`GROUP BY` without a value-aggregate, i.e. "list the distinct
series/groups matching this selector"). These four are order-free under
any total order the merge already establishes: summing per-worker
counts, taking the min/max of per-worker mins/maxes, and unioning
per-worker distinct-group sets are all associative and commutative, so a
worker-side partial plus a coordinator-side combine is exact *if* the
partials are computed over a duplicate-free partition of the data — the
qualifier this epic's design turns on. Order-sensitive aggregates
(`sum`, `avg`, `stddev`, quantiles) are explicitly out of scope.

**The reason this isn't a small wire-format addition: a series can be
duplicated across workers, and pushdown must not silently produce a
wrong answer when that happens.** Two independent mechanisms put more
than one worker's data in play for the same series, and both had to be
closed, not just the first one this ADR started from:

1. **Shard assignment is generation-versioned, not fixed.** ADR-0052
   introduced online resharding: a tenant's `shard_for(series_id,
   shard_count) = series_id_prefix % shard_count`
   (`crates/ravel-types/src/lib.rs:472`) uses a `shard_count` that
   changes across generations, each with its own `activation_hour`
   (the `ShardGeneration` record, `crates/ravel-catalog/src/
   provisioning.rs:411`). Existing data is deliberately never rewritten
   or re-keyed when `shard_count` changes (ADR-0052: "no rewrite, no
   delete") — a series written under generation 0's shard count and
   again under generation 1's lands under two different shard *values*
   for the same series identity, because the modulus changed.
   `partition_snapshot` (`crates/ravel-query/src/distrib/
   partition.rs:102`) groups segments by `seg.shard` and never splits
   one shard's segments across slices, but it says nothing about one
   SERIES's segments, which can carry two different shard values and
   therefore land in two different slices — i.e. on two different
   workers.
2. **Federation independently unions same-series data across cluster
   boundaries.** `Federation`/`FederationOutcome`
   (`crates/ravel-query/src/distrib/federation.rs:124-147`) fans a query
   out to remote clusters and folds their decoded scalar series into the
   coordinator's own merge pool "under a shared `SeriesId`" — the same
   series can have runs from the local cluster AND a remote one, with no
   relationship to shard generations at all. A worker inside the local
   cluster has no visibility into a remote's data, so a local partial for
   a federated query is never complete on its own.

In both cases, `merge_series_runs` (`crates/ravel-query/src/
engine.rs:2308`, doc comment from 2295 — today's coordinator-side dedup,
"the dedup belt") is what currently produces the correct answer: it is
fundamentally per-series and cross-run, k-way-merging every run of one
series together and picking a winner at each timestamp by dedup priority
(`is_greater`, engine.rs:2130), specifically because two runs of the same
series can disagree at an overlapping timestamp and only one sample is
correct. A worker computing `count`/`min`/`max` locally over only the
samples IT holds, with no visibility into another worker's or another
cluster's runs of the same series, cannot know whether a timestamp it's
counting is about to be dropped as a stale duplicate, or whether a
min/max it's reporting is the actual winner once an overlapping sample
elsewhere is resolved. Summing/combining such partials is not exact in
this case — it is a **silent wrong answer**, the exact class of defect
this repo's invariants forbid ("Exact semantics by default. Approximation
is opt-in and visible.").

This is also why "above the existing dedup belt" (the epic's own scope
line) needs a precise reading: it does NOT mean "run pushdown after the
belt already ran" (the belt is coordinator-side and cross-worker/
cross-cluster; if pushdown could safely run after it, there would be
nothing left to push down — the belt already produced the exact answer
locally). It means the opposite: pushdown must only be attempted when the
belt's cross-source case provably cannot arise for the queried series, so
each worker's own LOCAL merge (the same per-run dedup logic, scoped to
only the runs that one worker holds) is already the exact per-worker
answer, with nothing left for a cross-source belt to reconcile.

## Decision

### 1. Eligibility gate: no reshard-generation split AND no federation, evaluated on the resolved snapshot

Pushdown is attempted only when BOTH hold for the query:

**(a) No federation.** If the query has a non-`None` `Federation`
context (any remote configured), pushdown is unconditionally ineligible.
A local partial cannot be complete for a series that may also have
remote runs, and this ADR does not attempt to combine a worker's local
partial with a remote's raw samples through the belt — that composition
is real future work, out of scope here. This closes mechanism 2 above by
exclusion, not by design; it is the cheap, correct answer for a v1.

**(b) No reshard-generation split, checked against the segments the
query actually resolved, not against the query's own event-time window.**
An earlier draft of this gate checked whether the query's *event-time*
window crossed a generation's `activation_hour`. That is unsound: the
catalog resolves segments by *ingest hour*, and `Catalog::
window_hour_bounds` (`crates/ravel-catalog/src/catalog.rs:1202`)
deliberately extends the ingest-hour scan range past the event window's
own end, out to `now_ns + clock_skew_allowance_ns` — exactly so a
late-arriving write (old event timestamp, recent ingest time) is still
found. A query whose event window sits entirely inside generation g's
active hours can therefore still resolve segments ingested after a later
reshard, landing the same series under two shard values despite the
event-time check saying "fine." Checking ingest-hour boundaries instead
is closer but still not sufficient on its own: ADR-0052's own scan rule
tolerates stragglers routed under either the retiring or the activating
generation for `DEFAULT_SCAN_SLACK_HOURS` hours around the boundary
(`FLUSH_BOUND_SLACK_HOURS` + `TOLERATED_CLOCK_SKEW_HOURS` =
2 + 1 = 3, `crates/ravel-catalog/src/provisioning.rs:360-400`) — a window
sitting just outside the naive boundary-crossing check can still resolve
segments from both generations if it falls inside that slack margin.

The gate that is actually sound: after `partition_snapshot` resolves the
concrete set of segments the query will scan, walk their ingest-hour
buckets and their generation history for the tenant (the same
`ShardGeneration` records `scan_count`/`max_scan_count_over_range` read,
`crates/ravel-catalog/src/provisioning.rs:472,534`) and check that every
resolved segment's ingest hour falls inside ONE generation's *stable*
interval — `[activation_hour + DEFAULT_SCAN_SLACK_HOURS,
next_activation_hour)` — never the boundary-plus-slack region itself. If
any two resolved segments fall in different generations, or either falls
inside a slack margin, pushdown is ineligible for this query; the
coordinator falls back to the raw-fetch path unconditionally, the same
fallback an unsupported signal already takes per ADR-0071. This check
reads the same generation-history object the snapshot resolve itself
used (no separately-cached, possibly staler copy — a generation appended
between the two reads must not be invisible to the gate while already
visible in the resolved data), and it is post-resolve, not a pre-check
on the query's own stated window: it costs one pass over an already-
materialized segment list plus one lookup into already-fetched
provisioning state, not a new store read.

**Why this makes per-worker pushdown exact once both (a) and (b) hold:**
with no federation, every sample for the query comes from the local
cluster's own segments. With every resolved segment inside one stable
generation, `shard_for` is provably constant for the tenant throughout
the query's actual scan set, so every series the query touches is
provably held by exactly one worker: no series is split, so no
cross-worker duplicate can exist for it, so each worker's own local merge
(merging only the runs it holds) is already the exact per-series answer
for that worker's share, and coordinator-side combination of exact
per-worker partials is exact.

This is a coarse, tenant-and-window-level check (real per-segment data,
but one boolean per query, not a per-series decision) — conservative in
the same direction as before: a tenant that reshards rarely loses
eligibility only for the query shapes that actually resolve segments
spanning the reshard, and only until those segments age out of scan
range. A future refinement narrowing eligibility to the specific series
NOT affected by a boundary (rather than disqualifying the whole query) is
explicitly out of scope here.

### 2. New wire frame: `PartialAggregate`, additive to `pb::FetchResponse`

A new oneof member on `FetchResponse` (alongside `series=1`/`hist=2`/
`summary=3`/`log_record=4`/`span=5`; no member is reserved for this today,
so this is a genuinely new field number, not a slot ADR-0071 held open),
carrying one entry per group (series identity for `count`/`min`/`max`, or
bare series identity for group-only enumeration):

- `series_id`, `labels` (same shape `SeriesFrame` already carries).
- `count: Option<uint64>`, `min_bits: Option<fixed64>`, `max_bits:
  Option<fixed64>` — present depending on which aggregates the query
  actually asked for; a bit-pattern encoding for min/max, matching how
  `Run.value_bits` already crosses f64 values (`proto/ravel/
  queryfrag.proto:156`: "raw f64 bit patterns (fixed64), never proto
  doubles, so NaN payloads, -0.0 ... survive the wire byte-for-byte").

No dedup-key metadata rides in this frame. Under decision 1's eligibility
gate, none is needed: a worker's partial already reflects its own
provably-complete, provably-exclusive view of each series for the
window. This is deliberately the cheaper, narrower design the rejected
"compact per-group dedup digest" option (see Rejected) would have
required, and decision 1 is what makes it unnecessary.

This bumps `PROTOCOL_VERSION` (currently 3, `crates/ravel-query/src/
distrib/codec.rs:52`, set by ADR-0096) to 4, staged the same way
ADR-0096 staged its bump: proto field additions and decode-side support
land with no behavior change first, the coordinator opts a query into
pushdown (and the version flip) only once eligibility (decision 1) is
verified and the encoder is live, in one final commit per the
by-now-established pattern in this repo.

### 3. Coordinator-side combine

For `count`: sum every worker's `count` for a matching group. For `min`/
`max`: take the min/max of every worker's reported bound, compared by
the same `total_cmp`-based total order ADR-0023's min/max UDAF already
uses (`crates/ravel-sql/src/minmax.rs` — "the fold is associative and
commutative" under `total_cmp`, never `PartialOrd`), so NaN and
sign-of-zero handling stays consistent with every other typed-aggregate
path in this codebase. For group enumeration: union the per-worker
distinct series sets (a series belongs to exactly one worker under
decision 1's eligibility gate, so this union has no overlaps to resolve
— a real invariant to assert with a test, not assume).

### 4. Planner integration: two independent gates, neither one reused wholesale from ADR-0094

Pushdown requires BOTH an aggregate-expression-shape gate and decision
1's eligibility gate to pass; neither implies the other, and they are not
the same check:

- **Expression-shape gate.** The query's aggregate must be exactly one
  of `count` (any type), `min`/`max` (any type including float, using
  the ADR-0023 total-order UDAF's own reasoning for why float min/max
  IS order-insensitive), or a bare distinct-group enumeration. This ADR
  does NOT reuse `plan_is_exact_typed`
  (`crates/ravel-sql/src/executor.rs:1469`, ADR-0094 decision 1)
  as-is: that classifier admits non-float `sum` (not one of this ADR's
  four aggregates) and rejects float `min`/`max` (which this ADR's wire
  frame is explicitly built to carry, via the bit-pattern encoding in
  decision 2) — reusing it verbatim would admit the wrong aggregate and
  reject the ones this feature exists for. This ADR's classifier is a
  new, narrower function sharing no code with ADR-0094's beyond the
  general principle both apply (float ordering needs a documented total
  order, never bare equality/`PartialOrd`).
- **This expression-shape gate only applies in the SQL lane.**
  `plan_is_exact_typed`/the new classifier operate over a DataFusion
  `LogicalPlan`, which exists only for SQL queries. The PromQL/native
  fetch lane (where `FetchResponse` and decision 1's eligibility gate
  live) has no `LogicalPlan` to classify; pushdown eligibility for a
  bare PromQL `count`/`min`/`max over time`-shaped query is decided by
  the query's own AST shape in `ravel-promql`, a separate, simpler check
  (no arbitrary expression trees to rule out) that this ADR also
  specifies but does not detail further here — it is implementation
  work for the executing task, not a design decision.
- **Eligibility gate.** Decision 1's reshard/federation check, which is
  about the QUERY'S TENANT AND RESOLVED SEGMENTS, not the aggregate
  expression, and applies identically to both lanes.

## Rejected alternatives

- **A compact per-group dedup digest instead of the eligibility gate.**
  Workers ship enough per-sample provenance (a bloom filter or a summary
  of dedup keys touched) alongside the bare partial, so the coordinator
  can detect and correct a cross-worker collision without falling back
  to raw fetch. Rejected: this reintroduces most of the wire cost
  pushdown exists to avoid (a digest sized to catch real collisions
  isn't much cheaper than the samples themselves at the cardinalities
  where pushdown matters), and it's strictly more design and testing
  surface than a cheap, correct, coarse eligibility check that makes the
  digest unnecessary for the common case (queries that don't straddle a
  reshard and aren't federated). Not ruled out forever — if reshard
  frequency or federation usage turns out high enough that the coarse
  gate disables pushdown too often in practice, this is the fallback to
  revisit, with real operational data instead of a guess.
- **Ignore the reshard-splitting and federation risks; ship count/min/max
  pushdown unconditionally.** Rejected outright: this is exactly the
  silent wrong-answer class this repo's invariants forbid, for the sake
  of a performance win that a correct-by-construction eligibility check
  gets almost as cheaply.
- **Check eligibility against the query's stated event-time window
  instead of the resolved segment set.** This was this ADR's own first
  draft, and it is unsound: `Catalog::window_hour_bounds` resolves
  ingest hours out to `now`, independent of the query's own end, so an
  event-time-only check can pass while the resolve still spans a
  reshard via a late-arriving write. Corrected to a post-resolve,
  per-segment check (decision 1(b)) before this ADR shipped.
- **Support federated pushdown by merging a local partial with remote raw
  samples through the belt.** Deferred, not rejected outright: doing this
  correctly means teaching the belt to accept a mix of exact partials and
  raw runs for the same series, which is more design surface than a v1
  needs. Excluding federated queries from pushdown entirely (decision
  1(a)) is the correct, simple starting point; revisit only if federation
  usage overlaps enough with pushdown-eligible query shapes to be worth
  the added complexity.
- **Scope eligibility per-series instead of per-query.** A future
  refinement, not this ADR: it requires per-series shard-history lookups
  during planning (more cost, more code) to recover pushdown for the
  specific series NOT affected by a given tenant's reshard. The coarse
  per-query gate is correct and simple; this is a measured optimization
  for later, only if reshard frequency in practice makes the coarse gate
  too conservative.
- **Fold this into ADR-0094 instead of a new ADR.** Rejected: ADR-0094
  answers "where does the final aggregation stage run" (same process,
  more DataFusion partitions); this ADR answers "which process holds the
  data being aggregated" (coordinator vs. worker, over a network
  protocol version bump). Different frozen contracts (none vs.
  `PROTOCOL_VERSION`), different failure modes, different scope of
  review. One ADR per decision.

## Consequences

A query that safely qualifies for pushdown (expression-shape gate passes,
not federated, and every resolved segment sits inside one shard
generation's stable interval) fetches worker-computed partials instead of
raw samples, directly reducing the wire volume and the coordinator-CPU
cost ADR-0071 named as the ceiling. A query that straddles a reshard
boundary, falls inside a generation's slack margin, or is federated falls
back to today's raw-fetch-and-merge path unconditionally — never silently
wrong, only ineligible for the optimization, exactly the "approximation
is opt-in and visible" standard this repo holds everywhere else (here,
the visible signal is "not accelerated," not "may be wrong").

The eligibility gate is necessarily conservative: because it is
evaluated post-resolve against the segments a query actually needs, a
tenant that reshards is disqualified from pushdown for any query whose
scan set touches the affected hours for as long as those hours remain in
range — potentially a long time for a query over an old, wide window.
This is the correct trade for a v1; a per-series refinement (Rejected,
above) is the identified path to narrow it later.

This needs a production-scale engage-threshold measurement before
defaulting on, the same S3-backed methodology epic #361 (ADR-0094,
ADR-0102) already established: pushdown's win depends on how much wire
volume it actually removes at a given cardinality and worker count,
which is not knowable from first principles.

No format change to any persistent object: `PartialAggregate` is a
transient wire frame like `HistogramRecord`/the provenance columns
(ADR-0096), not a durable artifact. The migration-class question (A-D,
ADR-0066) is N/A for the same reason ADR-0096's was.

## Diagram

```mermaid
flowchart TB
    Q[Query: count/min/max/group] --> G1{"Aggregate expression\nshape eligible?\n(new classifier, per-lane)"}
    G1 -->|no| Fallback[Raw fetch + coordinator merge\n(today's path, unchanged)]
    G1 -->|yes| G2{"Federated query?"}
    G2 -->|yes| Fallback
    G2 -->|no| G3{"Resolve segments, then:\nall inside one shard\ngeneration's stable interval?"}
    G3 -->|no, spans a reshard\nor its slack margin| Fallback
    G3 -->|yes| Push["Worker computes local partial\n(after its OWN local per-run dedup)"]
    Push --> Combine["Coordinator combines partials\n(sum / min / max / set-union)"]
    Combine --> Result[Query result]
    Fallback --> Merge["merge_series_runs\n(cross-worker/cross-cluster dedup belt)"]
    Merge --> Result
```

## Amendment: narrowed to single-step `_over_time` range functions; decision 3's combine is a collect, not a sum

T1-T3 shipped the eligibility gate, the `PartialAggregate` wire frame, and
worker-side partial computation exactly as decided above. Wiring a real
caller (T4) surfaced two errors in this ADR's original framing, caught
before T4 was dispatched rather than after a checkpoint review, this
time — the same discipline that caught T2's premature version bump.

**The delivered surface is per-series aggregation over samples, which
only matches PromQL's `count_over_time`/`min_over_time`/`max_over_time`
range functions (`crates/ravel-promql/src/functions/over_time.rs`:
`fn count_over_time(samples: &[Sample], w: RangeWindow)` and siblings) —
not the outer, cross-series `count()`/`min()`/`max()`/`group()`
aggregate operators PromQL also has** (`crates/ravel-promql/src/
aggregate.rs`'s `eval_aggregate`, which counts/reduces over how many
*series* have a sample at an instant, not how many *samples* one series
has over a window). The two are different aggregation axes. This ADR's
Context section listed "count, min, max, group" without distinguishing
them; only the within-series, over-a-window shape is in scope for T4.
Cross-series aggregate-operator pushdown (PromQL's outer aggregators,
and SQL's `COUNT(*)`/`GROUP BY` over a table scan) needs its own combine
semantics — a union/count of *series*, not a per-series value reduction
— and is explicitly deferred as unscoped future work, not something the
shipped `PartialAggregate` frame serves without a redesign.

**Decision 3's "coordinator sums every worker's count" is corrected to
"the coordinator collects one partial per series."** Decision 1's
eligibility gate guarantees every series in scope lives on exactly one
worker; there is never a second worker's partial for the same series to
sum against. The combine step's real job is assembling the per-series
map from however many workers each contributed a disjoint subset of
series, not arithmetic across workers on one series. This is a stronger
exactness guarantee than "sum," not a weaker one, and doesn't change
decision 1/2's design, only this document's earlier prose about what
decision 3 does.

**T4 ships `count_over_time` pushdown only. `min_over_time`/
`max_over_time` pushdown is excluded from T4**, because T3's worker fold
does not compute the value PromQL's own reducers compute. T3 folds
min/max under `f64::total_cmp` (ADR-0023's convention, correct for
SQL-style typed aggregates), but `min_over_time`/`max_over_time`
(`crates/ravel-promql/src/functions/over_time.rs`) use plain IEEE
comparison with NaN overwritten by any non-NaN sample (the window's
result is NaN only when every sample is NaN) and `-0.0` never displacing
an already-seen `0.0`. The two folds disagree on any window containing a
NaN (`total_cmp` ranks NaN above every other value; `_over_time` treats
it as absent unless it's the only sample) and on `{0.0, -0.0}`
(`total_cmp` picks `-0.0` as the min; `_over_time` keeps `0.0`). Reusing
the shipped partial for `min_over_time`/`max_over_time` would return a
visibly wrong number on exactly the values this repo's float-comparison
discipline exists to get right. Min/max pushdown needs either a second,
PromQL-semantics fold on the worker or a proof that no query the gate
admits can ever carry a NaN/-0.0 sample (unlikely — staleness markers
alone are NaN-encoded, see below); until one of those exists, `want_min`/
`want_max` stay unused by any real caller. Count is unaffected: a sample
count under total order and under plain comparison count the same
samples, so `count_over_time` pushdown ships in T4 without this problem.

**Staleness markers must be filtered before counting, or a stale sample
inflates the pushed-down count.** The evaluator drops any sample encoded
as `STALE_NAN_BITS` from every matrix selection before a range function
sees it (`crates/ravel-promql/src/eval.rs`). T3's worker fold has no such
filter — it counts every merged sample. A window whose only "sample" is
a staleness marker (the series went stale inside the window) must
contribute 0 to `count_over_time`'s pushed-down partial, not 1. T4's
worker-side change (or a T3 fast-follow, whichever lands first) must
filter `STALE_NAN_BITS` AFTER `merge_soa_runs`, not before: the merge's
own dedup tie-break (`is_greater`, a bit-pattern tuple comparison,
unrelated to `total_cmp`) decides which of two candidates at the same
timestamp survives, exactly as it does on the raw path today, so
filtering staleness pre-merge on the worker would let a losing real
sample win a slot the raw path resolves to the marker (or vice versa),
diverging from the answer the same query gets on the local path. The
acceptance test must place a staleness marker inside the exact reduction
window and confirm it is excluded from the count.

**A zero-count partial maps to the absence of an output sample, never a
literal `0.0` point.** The raw evaluation path never emits a point for a
series whose window is empty (the reducer is never invoked); a fully
erased series is a real case where the shipped worker reports
`count: Some(0)` (post-erasure, correctly). If T4's coordinator-side
conversion turns that `0` into an actual `0.0` sample in the resulting
instant vector, a downstream `count(count_over_time(m[5m]))` composition
counts a phantom series that would not exist on the raw path. T4 must
convert `count: Some(0)` to "no output sample for this series," matching
the raw path's own behavior.

**Four additional eligibility and precision requirements for T4,
necessary because of how `_over_time` functions actually work:**

- **Single evaluation step only, and the argument must be a literal
  matrix selector, not a subquery.** A *range* query evaluates
  `count_over_time(m[5m])` at N independent steps, each needing its own
  sub-window reduction; one whole-fetch-window partial per series cannot
  serve more than one step. A *subquery* argument
  (`count_over_time(m[1h:5m])`) evaluates at exactly one OUTER instant —
  passing a naive "single evaluation step" check — but its correct value
  depends on the inner `5m`-step evaluation grid with its own lookback
  propagation, not a raw sample count over the outer `1h` window; pushing
  it down would silently return the wrong number. Eligibility (decision
  1) gains a fourth, independent condition: the query evaluates
  `count_over_time` exactly once AND its matrix argument, after
  unwrapping any parentheses, is a literal matrix selector, never a
  subquery (these are the only two shapes a Matrix-typed function
  argument can take in this implementation — `crates/ravel-promql/src/
  functions/mod.rs`'s `matrix_arg`). Both conditions apply regardless of
  shard-generation stability or federation status.
- **A literal selector's `offset`/`@` modifiers shift the reduction
  window; the pushed-down bounds must be computed from the
  offset/@-resolved selector timestamp, never from the query's own
  evaluation instant.** `count_over_time(m[5m] offset 1h)` at a single
  instant passes every eligibility condition above, but its window is
  `(sel_ts - range, sel_ts]` where `sel_ts` is the offset/@-resolved
  selector timestamp (`crates/ravel-promql/src/eval.rs`'s
  `eval_matrix_selector`), not `(eval_ts - range, eval_ts]`. T4 must
  compute the reduction bounds (next bullet) from `sel_ts`, the same
  value the raw path already resolves for this exact selector, and the
  acceptance test suite must include one case with a nonzero `offset`
  confirming the pushed-down count matches the raw-path count for a
  window that would give a different answer if computed from `eval_ts`
  instead. (`@` inside a *range* query is already excluded by the
  single-step condition above; only the instant-query case needs this
  bullet.)
- **The reduction window is not the fetch window, and its start bound is
  exclusive.** The coordinator's fetch window is deliberately padded left
  for PromQL lookback (`crates/ravel-query/src/engine.rs`'s `padded`
  window construction, docs/query-engine.md's `padded_range`), a superset
  of `count_over_time`'s own `(start, end]` argument (PromQL range
  windows are left-open — `crates/ravel-promql/src/functions/mod.rs`'s
  `range_window`). On the distributed path today the mismatch is wider
  than just lookback padding: the worker (`run_slice_metrics`) does not
  consult `FetchRequest.window_start_ns`/`window_end_ns` at all for
  metrics, and reduces over everything the pinned segments hold. T4 must
  add an explicit reduction-bounds field (new, additive — T3 shipped no
  such field, only `want_count`/`want_min`/`want_max`) to
  `PartialAggregateRequest`, carrying the offset/@-resolved `(start,
  end]` from the bullet above, and the worker must filter to strictly
  that range before counting. The acceptance test must place one sample
  exactly at the exclusive start bound (`sel_ts - range`, must be
  EXCLUDED) and one exactly at the inclusive end bound (must be
  INCLUDED), not just a sample somewhere in the padding region.
- **Selector-plan dedup must not starve a shared consumer, and "same
  pushdown decision" means identical reduction bounds too.** The
  coordinator dedups fetch plans by matcher-set equality
  (`crates/ravel-query/src/engine.rs`'s `distinct_plans`,
  `count_over_time(a[5m]) + a` shares one matcher-set fetch between a
  pushdown-eligible consumer and a raw-sample consumer). Eligibility for
  a given matcher set requires every consumer of that deduped plan to
  want the identical pushdown — the same aggregate AND the same
  reduction bounds — since matcher equality says nothing about window
  equality (`count_over_time(a[5m]) + count_over_time(a[10m])` dedups to
  one fetch plan under today's matcher-only equality but needs two
  different reduction bounds). If any consumer needs raw samples, or two
  consumers of one deduped plan want different bounds, the whole shared
  fetch stays raw. This is a coordinator-side planning check, not a
  per-worker one.
- **A duplicate series id across collected partials is a hard error, not
  last-wins.** Decision 1's gate guarantees each series lives on exactly
  one worker when eligible, so the coordinator's collect step (corrected
  above) should never see the same series id twice; if it does, treat it
  as a gate failure (fall back to raw fetch for the whole query, or hard
  error) rather than silently keeping one of the two values. A
  `SnapshotInvalidated` retry rebuilds the collected partial set from
  scratch; it must never fold a retry's partials on top of the first
  attempt's.

**F1 (carried from T3's checkpoint review) becomes a required fix in
T4's diff, not an optional note:** once pushdown is live, a slice
mixing metric and native-histogram data can reach the native-histogram
refusal path (`crates/ravel-query/src/distrib/service.rs`, the
`!histograms.is_empty()` check), whose summary currently reports
`QueryAccountingSnapshot::default()` instead of the accounting snapshot
that already paid for the fetch.

**Stale wire-contract comments to fix in the same diff:**
`proto/ravel/queryfrag.proto`'s `PartialAggregate` doc comment says the
coordinator "sums the counts and folds the bounds under the ADR-0023
total_cmp total order" — now false on both counts (collect, not sum;
min/max combine doesn't ship in T4). Separately, `service.rs`'s
`replaces` helper doc describes the ADR-0023 total order as what "the
coordinator's own combine" uses — also stale once min/max combine isn't
live. Update both comments to match what actually ships.

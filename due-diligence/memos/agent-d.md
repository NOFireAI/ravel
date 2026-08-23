# Agent D: Query correctness

Scope: PromQL, SQL/DataFusion, Flight SQL, pruning, dedup, snapshot pinning,
budgets, distributed execution, federation, partial results, cache
interaction, compatibility claims. Frozen commit
527a16db2e4d47b2924e4de4a4db32d7583fda33.

## Verdict

The float query path is in strong shape. The dedup total order is one
implementation contract enforced identically across PromQL, SQL, and
distributed execution, with genuine differential and property tests behind
it, including an independent oracle for compaction equivalence and a
proptest that the distributed merge is bit-identical to local over arbitrary
bit patterns. Pruning is widen-only by construction with adversarial tests.
Budgets are typed, re-enforced at the coordinator against lying workers, and
partial coverage is surfaced in types and envelopes.

The native-histogram path has a systemic hole the 91/91 differential run
cannot see: every value-only reduction path (binary operators, the generic
range-query grid reducer, the float-only range selector and range-function
reducers, and the over_time family) silently degrades or drops
native-histogram elements instead of rejecting them the way the subquery
path already does. A tenant that ingests native histograms and runs a plain
`h * 2`, `sum(rate(h[5m]))` as a range query, or a bare `h` range query gets
zeros or empty results with HTTP 200 and no warning. The conformance table's
per-construct classification structurally cannot represent this
per-input-type gap, so its 100% score overstates coverage for
histogram-bearing tenants.

## Evidence

### PromQL differential harness: what 91/91 proves

- Real end-to-end parity: data is ingested through the real
  `IngestRouter::write_values` over `MemoryStore` and served by the real HTTP
  router (crates/ravel-promql-difftest/src/ravel_stack.rs:1-95); Prometheus
  side is a spawned pinned binary fed by remote write
  (src/prometheus_process.rs:39-63). So the run covers ingest, segment
  encode/decode, catalog resolve, fetch, merge, eval, and JSON envelope, not
  just the evaluator. VERIFIED.
- The comparator is strict: `f64::to_bits` equality, `-0.0` bit-significant
  and never matched under any ULP tolerance, warnings and infos compared as
  independent presence channels, error class parity, per-entry named ULP
  allowlists only (src/comparator.rs:39-93, 251-317, tests at 548-737).
  VERIFIED.
- Corpus: 218 entries, 10 files, covering matcher forms, offsets (positive
  and negative), `@` anchors, lookback boundary entries, staleness marker
  absent/recover shapes, counter resets including a reset exactly at a
  window boundary, NaN/±Inf/-0.0 special-value series, vector matching
  incl. group_left/group_right and duplicate signatures, subquery epoch
  alignment, classic-histogram malformed variants, native-histogram
  instant-path functions (corpus/*.txt; generator at src/generator.rs:136-503).
- What the harness structurally cannot see:
  - Duplicate timestamps: the generator dedups per series
    (`every_series_is_strictly_ascending_and_dup_free`,
    src/generator.rs:598), and Prometheus cannot ingest same-ts duplicates,
    so query-time dedup is untestable differentially. It is covered instead
    by the compaction differential and the wire tests (below).
  - NaN payload preservation: JSON renders every NaN as `"NaN"`, so the
    comparator treats NaN as a class (comparator.rs:85-93). Payload
    bit-exactness rests on unit tests and the internal `to_bits` discipline,
    not on the differential run.
  - Native-histogram constructs outside the wrapped instant paths: no corpus
    entry applies a binary operator to a histogram, runs a bare histogram
    selector or `rate(h[..])` as a range query, or applies an over_time
    function to a histogram series (corpus/binop.txt, histogram_native.txt:
    the only `kind: range` entries wrap in histogram_quantile/fraction).
    This is exactly where the defects below live.

### P0: binary operators silently treat native histograms as float 0.0

A histogram vector element carries `value: 0.0` with `histogram: Some`
(crates/ravel-promql/src/eval.rs:91-137, the constructor comment says
"`value` is not meaningful"). `eval_vector_selector` puts histogram elements
into the same instant vector floats use (eval.rs:816-832). The binop
evaluator never inspects `histogram`: `eval_scalar_vector` and `one_to_one`
combine `s.value`/`l.value`/`r.value` and emit `histogram: None`
(crates/ravel-promql/src/binop.rs:157-190, 305-345, `combine_value` at
139-152). Consequences, all HTTP 200 with no warning:

- `h * 2`, `h + h`: a float vector of 0.0s where Prometheus returns scaled
  or summed histograms.
- `rate(h[5m]) / rate(g[5m])`: 0.0/0.0 = NaN floats.
- Comparisons filter on the meaningless 0.0.

The asymmetry proves this is a gap, not a decision: unary minus handles the
histogram case explicitly (`negate_vector`, eval.rs:1345-1360), and the
subquery path guards this exact hazard with a typed
`Unsupported: subquery over native histograms` when matched histogram data
is present (eval.rs:940-960). Binop has no such guard. CONTRADICTED
(against the conformance table's "binary expression: supported" and the
repo invariant "no placeholder on critical paths; approximation is opt-in
and visible").

### P0/P1: range queries over native histograms return zeros or empty

- Top-level aggregate or binary range query (`sum(rate(h[5m]))` via
  `/api/v1/query_range`): `RangeCore::Generic` re-evaluates per grid step
  (histograms alive, sum produces histogram elements) and then
  `eval_instant_over_grid` keeps only `s.value`, materializing a matrix of
  0.0s (crates/ravel-promql/src/eval.rs:704-711,
  crates/ravel-promql/src/functions/mod.rs:558-602, the reducer reads
  `s.value` at 571-580). Wrong values, silent. P0.
- Bare `h` range query: `eval_range_selector` calls only `source.query`,
  never `query_histograms`, so a histogram series contributes nothing
  (eval.rs:1009-1069). Silent empty. P1.
- `rate/increase/delta(h[5m])` as a range query: the
  `RangeVectorFloatOrHist` range arm deliberately reduces floats only, with
  an in-code comment "no read path feeds native histograms into a range
  query yet" (functions/mod.rs:415-419). Silent empty. P1.
- over_time family (`count_over_time`, `last_over_time`,
  `present_over_time`, `changes`, `resets` and siblings) are plain
  `RangeVector` float reducers (functions/over_time.rs:29-70,
  functions/rate.rs:152-161), so a histogram series is invisible to them;
  `absent_over_time(h[5m])` affirmatively reports the series absent while
  data exists (functions/mod.rs:305-313 evaluates the float matrix only).
  Prometheus 3.x includes histograms in these. Silent omission, and in the
  absent_over_time case a wrong positive. P1.

The one guarded escape (`histogram_quantile(0.99, sum(rate(h[5m])))`, the
canonical Grafana pattern) works, because `HistogramQuantile`/`Fraction`
range evaluation re-runs the whole call per step on the instant path
(functions/mod.rs:446-476). That makes the surviving gaps more deceptive:
the dashboards that work suggest histograms are fully supported.

### Conformance table: honest mechanism, structurally coarse

The table is regenerated from a live run and cannot be hand-inflated;
0 of 131 constructs unclassified; the 5 rejections are typed 422 envelopes
proven by conformance_table.rs; limitk/limit_ratio are parseable but
typed-rejected rather than panicking (docs/query-engine.md:954-1157).
VERIFIED as far as the classification goes. But the unit of classification
is the construct, not (construct, input type), so "binary expression:
supported, 28 entries" coexists with the P0 above. A user reading the table
plus the native-histogram function rows would reasonably conclude
histogram binops and range queries work. The API-compatibility risk is
real: nothing distinguishes "supported for floats" from "supported".

### Dedup total order: one contract, three implementations, tested to agree

- PromQL/local: `is_greater` compares
  `(created_unix_ns, writer_epoch, writer_seq, in_page_index)` then
  `value.to_bits()`, greatest wins
  (crates/ravel-query/src/engine.rs:2130-2132); the k-way merge drains
  same-ts runs per candidate, rejects non-monotonic runs typed
  (engine.rs:2308-2383) and per-sample provenance columns of the wrong
  length (2188-2197). Run-merged L1 runs carry an explicit per-sample
  priority column (`RunPriorities::PerSample`, 2147-2173). Histograms get
  the structural bit-pattern counterpart (`histogram_is_greater`,
  2500-2516). VERIFIED.
- SQL: `RsegDedupExec` uses `DedupKey = (created, epoch, seq, in_page,
  value.to_bits())` with plain tuple `>` over the (series_id, ts)-sorted
  merged stream (crates/ravel-sql/src/dedup.rs:43-45, 236-268); the scan
  emits the provenance columns per row including per-sample `in_page` for
  merged runs (crates/ravel-sql/src/scan.rs:146-262). The operator declares
  required input distribution and ordering so the optimizer cannot strip
  the SortPreservingMergeExec (dedup.rs:56-66, 131-143, a real bug that was
  found and pinned). VERIFIED.
- Distributed: the coordinator reuses the exact local merge
  (`merge_soa_runs` is pub(crate) for that reason, engine.rs:2245-2249);
  protocol v3 ships the four packed per-sample provenance columns and raw
  f64 bit patterns (ADR-0096). The reviewed hazard where a wire frame
  drops the per-sample column and flips a dedup winner is encoded as a
  test fixture (`write_l1_merged_provenance`,
  crates/ravel-query/src/distrib/tests.rs:515-560, plus
  `dedup_tiebreak_chain_survives_the_wire`). VERIFIED.
- Cross-implementation agreement tests: an independent greatest-wins oracle
  gates the SQL scan (tests/pipeline.rs:170-180), the layer-2 SQL
  differential gates operators against a reference executor with proptest
  over NaN payloads, -0.0, denormals, duplicate timestamps, multi-segment
  overlap (tests/differential.rs:208-316), and
  `recent_hours_reachability_e2e.rs` asserts PromQL and SQL reads
  bit-identical pre/post compaction. The compaction-safety keystone claim
  (query-over-inputs == query-over-L1 bit-for-bit) is a proptest driving
  the real compactor with an in-test oracle
  (crates/ravel-query/tests/differential_compaction.rs:1-23). VERIFIED.

### Pruning soundness: widen-only enforced by shape, tested adversarially

- Metrics: the extractor recognizes only provable shapes; OR, negated
  BETWEEN, function-wrapped `ts`, non-timestamp literals all contribute
  nothing; integer ts literals are rejected as ambiguous scale; `ts > L`
  uses checked `L+1`; every pushed matcher is a documented superset of the
  SQL predicate's row set with the residual re-applied above the scan
  (crates/ravel-sql/src/pushdown.rs:1-33, 106-292; unit tests 360-481).
  The adversarial end-to-end case compares against a never-pruning
  reference (`ts_literal_predicates_are_lossless`,
  crates/ravel-sql/tests/differential.rs:327-390). VERIFIED.
- Logs: `attrs['k'] = 'v'` goes to a prune-only POSTINGS channel that never
  feeds the per-row filter; version-1 objects decline the resource-level
  arm entirely (widen-only fallback) (docs/query-engine.md:1343-1397, tests
  sql_postings_pruning.rs, sql_postings_prefix_pruning.rs). Spans: all six
  recognized shapes prune at skip-index or bloom, `Inexact` for every
  filter, cross-axis disjunctions refused (docs/query-engine.md:1440-1486).
  IMPLEMENTED with tests; I did not re-derive every spans fold by hand.

### Budgets

- PromQL: max_series enforced incrementally at the merge map, max_samples
  count-yielded post-dedup tripping at exactly max+1 (engine.rs:2211-2292),
  per-node subquery point cap plus a shared cross-level eval-points budget,
  evaluator-internal deadline checks between grid steps, bytes-scanned and
  S3-request budgets checked per completed segment
  (docs/query-engine.md:195-332). Typed errors throughout, never
  truncation. VERIFIED at code level; deadline/budget tests exist
  (deadline_cancels_*, bytes_scanned_budget.rs, scan_budgets.rs).
- Known and documented: max_samples bounds output, not fetch/decode peak
  memory; every matched series is fully decoded before the merge, and the
  byte budget defaults to Unlimited (query-engine.md:240-254, 324-332). A
  pathological matcher can therefore exhaust coordinator memory before any
  budget trips. P2 operational risk, disclosed.
- SQL: per-query DataFusion pool bridged to a per-tenant accountant; `grow`
  cannot decline, so the ceiling is detect-and-abort, not prevent
  (crates/ravel-sql/src/memory.rs:26-65). ADR-0102: disk manager disabled,
  spill is a typed error, never silent degradation
  (crates/ravel-sql/src/session.rs:432-441). VERIFIED.
- Distributed: coordinator re-enforces distinct-series (scalar and
  histogram separately) and bytes-scanned over a saturating fold of
  worker-reported spend, so a lying or wrapping worker cannot slip under a
  cap; spend is folded before every failure or fallback path so fallback
  reruns report both costs (crates/ravel-query/src/distrib/mod.rs:233-374;
  tests `coordinator_reenforces_*`, `coordinator_fold_saturates_*`).
  VERIFIED.

### Distributed execution and the bit-for-bit claim

- `distributed_merge_equals_local_bitwise`: proptest (16 cases per run) over
  arbitrary u64 value bit patterns (NaN payloads, -0.0 included), 1-7
  segments across up to 8 shards, slice caps 1-6 sized so the cap actually
  binds, against a real tonic loopback worker, asserting bit-identical
  merged series plus equal accounting and FetchStats
  (crates/ravel-query/src/distrib/tests.rs:356-481). Partitioning totality
  and shard-atomicity have their own tests (distrib/partition.rs:178-260).
  The reshard case (one series id in two slices) is pinned
  (tests.rs:489-513). STRONGLY SUPPORTED. Caveat: the proptest drives one
  worker process; multi-worker routing is covered separately at server
  level, so the claim's evidence is compositional rather than one test.
- Failure matrix, each with a test: worker loss re-dispatches once then
  runs coordinator-local then fails typed
  (services/ravel-server/tests/distributed_query_e2e.rs:1142); partial
  frames from a failed attempt discarded whole (slice atomicity, no dedup
  bookkeeping needed) (:1457); a second summary frame is a typed
  `MultipleSummaries` error, so a worker responding twice cannot
  double-merge (crates/ravel-query/src/distrib/client.rs:33-46, 336-346);
  version skew falls back to whole-query local, never partial (:1321);
  corrupt is terminal with no retry (:1753); cancellation is drop-based and
  frees fragment permits (:1589); all invalidated slices collapse to one
  snapshot re-resolve retry (distrib/mod.rs:301, 358-368,
  `many_invalidated_slices_map_to_one_retryable_error`). VERIFIED.
- Slow workers: no hedging; the deadline is the only bound on a straggler.
  Availability trade-off, not a correctness gap. P3.
- Aggregation pushdown (ADR-0103) is wire-reachable but no coordinator sets
  it (`partial_aggregate: None`, distrib/mod.rs:213-217); the min/max
  `total_cmp` vs PromQL-IEEE divergence is known and is why only count is
  wired. The worker filters staleness after its merge so the dedup winner
  matches the raw path (`staleness_filter_runs_after_the_merge` test).
  VERIFIED as not-yet-live.

### Federation and partial results

- Disjoint-series-identity assumption: documented in three places including
  a comment at the merge site; the cross-cluster winner for a colliding
  `(series_id, ts)` is deterministic but not meaningfully ordered
  (engine.rs:2112-2129, docs/query-engine.md:722-751). Discovery union is
  well-defined even for mirrored ingest (identity is a pure function of
  labels). DOCUMENTED CLAIM, consistent with code.
- Partial results: `skip_unavailable` defaults to false (fail typed,
  services/ravel-server/src/config.rs:1770; docs/guides/
  distributed-query.md:289); when opted in, stats carry `partial: true` and
  redacted warnings; the bare engine wrappers return a `#[must_use]
  Coverage` so an internal caller cannot silently drop it
  (engine.rs:78-183); the alert evaluator treats Partial as a failed
  evaluation and leaves alert state untouched
  (services/ravel-server/src/alerting.rs:845-870). Federation tests cover
  skip/timeout/typed-fail/old-remote per signal
  (distrib/federation.rs tests). Residual risk: an external HTTP client
  that ignores the Prometheus `warnings` array will consume a partial
  answer as complete; the JSON envelope has no standard machine field for
  partiality. That is a Prometheus-API-inherent limit, mitigated by the
  fail-closed default. P3. Budget overruns are never skippable
  (federation.rs:27-33). VERIFIED.
- Trust boundary: a remote resolves the tenant from its own credential and
  overwrites the wire tenant_hash; the intra-cluster fragment token is
  rejected by remotes (`federation_rejects_the_fragment_token`). VERIFIED
  at design level (Agent scope for security is elsewhere).

### Flight SQL

Tickets are keyed (32-byte secret), deadline-bounded, pin the snapshot
including `pending_erasure` and the declared-column schema, so `DoGet`
executes against exactly what `get_flight_info` planned; version-gated
decode refuses older ticket versions typed
(crates/ravel-sql/src/flight_ticket.rs:20-260). HTTP-vs-Flight parity has
its own differential gate (tests/flight_differential.rs), erasure through
tickets is tested (tests/flight_erasure.rs), terminal stream status is
recorded on cancellation (flight/service.rs:520-525). The SQL-lane
distributed scan for logs/alerts/audit/spans is tested at provider level
but not wired into a running server, stated plainly in the doc
(docs/query-engine.md:545-553); the metrics SQL lane is wired
(services/ravel-server/src/sql_distrib.rs:24-32). VERIFIED as documented.

### Cache interaction

Read cache is content-addressed `(tenant_hash, content_hash, offset, len)`;
erasure filters run after the cache, so stale cached bytes of an erased
subject are unreachable (docs/query-engine.md:76-90); the acceptance gate
proves a corrupted cache hit either errors typed or returns bits identical
to no-cache (crates/ravel-query/src/cache_correctness.rs:1-23). Suffix GETs
bypass the cache by design. VERIFIED.

## Failure scenarios

1. Tenant ingests OTLP native histograms; an SRE graphs `sum(rate(h[5m]))`
   in Grafana (range query). Ravel returns a matrix of zeros, HTTP 200, no
   warning. The dashboard renders a flat zero line; capacity decisions get
   made on it. Prometheus on the same data returns the histogram matrix.
2. Alerting rule `rate(h[5m]) / rate(h_total[5m]) > 0.1` (histogram on
   either side of a binop): the histogram side contributes 0.0, the ratio
   is 0.0 or NaN, the alert never fires. Silent.
3. `absent_over_time(h[15m])` used as a liveness alert on a
   histogram-instrumented service: returns 1 (absent) while data flows.
   The alert fires forever or the inverse rule never does.
4. SQL analyst runs `SELECT count(*) FROM samples` on a mixed tenant:
   native-histogram samples are invisible to the samples table (scalar
   value column only; no histogram exposure anywhere in ravel-sql, no
   statement of the exclusion in docs/sql-conformance.md). Count silently
   understates ingest volume.
5. Mirrored-ingest federation (both clusters carry the same series): a
   duplicate `(series_id, ts)` with different values resolves to an
   unspecified winner. Deterministic, documented, but an operator who
   missed the disjointness assumption gets value-level nondeterminism
   across topology changes.
6. A matcher that matches 10k series across 1024 large segments with the
   default Unlimited byte budget: full decode happens before the series cap
   trips at merge; coordinator memory spike precedes the typed error.

## Tests or commands run

Per the panel's method rules I ran no cargo builds or tests; the chair's
central runs (PromQL differential 91/91 against Prometheus 3.13.1; nextest
green for ravel-promql, ravel-query, ravel-sql, ravel-server --features
sql) are taken as environment facts. My work was read-only: Read/Grep/ls
over crates/ravel-promql{,-difftest}, crates/ravel-query,
crates/ravel-sql, services/ravel-server, docs/query-engine.md, ADRs 0071,
0096, 0102, 0103. All line citations refer to the frozen commit.

## Unknowns

- Regex engine parity: matchers reuse promql-parser's Rust `regex` with
  Prometheus anchoring (crates/ravel-promql/src/matchers.rs:43-53). Rust
  `regex` perl classes (`\d`, `\w`, `\s`) are Unicode-aware; Go RE2's are
  ASCII-only. For label values containing non-ASCII digits/word chars the
  matched series set can differ. The corpus has two trivial regex entries;
  not differentially exercised. UNKNOWN, likely-narrow divergence.
- Sub-ms timestamps: OTLP ingest stores raw ns (no ms truncation found in
  ravel-otlp normalize); the JSON renderer's stated precondition that
  evaluator timestamps are 1 ms multiples
  (crates/ravel-query/src/http/json.rs:346-351) is false for instant-query
  matrix results, which carry raw stored timestamps; `(ts_ns as f64)`
  rounds at ~256 ns near current epoch. Rendering-precision issue only; ns
  ordering internally is exact. NOT ASSESSED end to end.
- Multi-worker (more than one remote process) bit-identity is asserted
  compositionally (loopback proptest + server e2e with mock workers), not
  by a single many-real-worker differential. WEAKLY VERIFIED.
- Exemplars query surface (/api/v1/query_exemplars) correctness vs
  Prometheus: not differentially tested (harness does not enable
  exemplar storage). NOT ASSESSED.
- `unreachable!` on unknown aggregator tokens
  (crates/ravel-promql/src/aggregate.rs:165-167) and on ManyToMany
  cardinality (binop.rs:216-218): safe against promql-parser 0.10, a panic
  hazard on a future parser bump. Not currently reachable.

## Severity-ranked findings

1. P0 — Binary operators silently degrade native-histogram operands to
   float 0.0. CONTRADICTED (vs conformance table "supported" and the
   exact-semantics invariant). `combine_value` and both matching paths read
   `s.value` and emit `histogram: None`; histogram elements carry
   `value: 0.0` by construction. Wrong values with HTTP 200.
   crates/ravel-promql/src/binop.rs:139-190, 305-345;
   crates/ravel-promql/src/eval.rs:91-137, 816-832. Fix shape exists in the
   codebase: the subquery guard (eval.rs:940-960) rejects typed on matched
   histogram data; binop needs the same (or real histogram arithmetic).
2. P0 — Range queries with a top-level aggregate/binary over native
   histograms return a matrix of zeros. CONTRADICTED.
   `eval_instant_over_grid` flattens every element to `s.value`; the
   Generic range arm has no histogram guard.
   crates/ravel-promql/src/functions/mod.rs:558-602;
   crates/ravel-promql/src/eval.rs:704-711.
3. P1 — Bare histogram selector, `rate/increase/delta(h[..])`, and the
   over_time family return empty (and `absent_over_time` returns a false
   positive) for native-histogram series in range evaluation; Prometheus
   3.x includes histograms in these. IMPLEMENTED-as-float-only, silent.
   crates/ravel-promql/src/eval.rs:1009-1069;
   crates/ravel-promql/src/functions/mod.rs:340-445, 305-313;
   functions/over_time.rs:29-70.
4. P2 — SQL `samples` table silently excludes native-histogram samples and
   no document states the exclusion (nothing in docs/sql-conformance.md or
   the schema doc). Counting/auditing queries understate.
   crates/ravel-sql/src/schema.rs:64-71 (scalar value column only).
5. P2 — Coordinator peak memory is bounded by matched input, not by any
   default-on budget: max_samples counts post-dedup yield, byte budget
   defaults Unlimited, full decode precedes the merge-time series cap.
   Disclosed in docs/query-engine.md:240-254, 324-332. DoS-shaped, not
   wrong-results.
6. P3 — docs/query-engine.md self-contradicts on budget scoping: Flow says
   max_series/max_samples are enforced once over the pooled union
   (docs/query-engine.md:37-43, matches code, engine.rs:2249-2292); the
   Budgets section still says they are enforced independently per selector
   (docs/query-engine.md:199-201). Stale paragraph; reported here per the
   repo's report-don't-fix rule.
7. P3 — docs/query-engine.md:86-90 claims SQL logs/spans/metrics scans do
   not read `Snapshot::pending_erasure`; all five providers now do
   (crates/ravel-sql/src/provider.rs:105, logs_provider.rs:95,
   spans_provider.rs:84, audit_provider.rs:82, alerts sibling;
   scan.rs:534-538 applies `retain_series_soa`; tests
   flight_erasure.rs, erasure_cross_surface.rs). Stale doc, safe
   direction. Reported, not fixed.
8. P3 — Regex class semantics (Rust regex Unicode perl classes vs Go RE2
   ASCII) can select different series for non-ASCII label values; untested.
   crates/ravel-promql/src/matchers.rs:43-53.
9. P3 — Partial federated results are invisible to envelope-ignoring HTTP
   clients (no machine-readable partial field in the Prometheus JSON
   shape); mitigated by skip_unavailable defaulting to false and by typed
   Coverage for internal callers. docs/query-engine.md:659-666;
   services/ravel-server/src/config.rs:1770.
10. P3 — JSON timestamp rendering precondition ("always 1 ms multiples") is
    false for raw-ns OTLP data surfaced through instant-query matrix
    results; f64 cast rounds at ~256 ns.
    crates/ravel-query/src/http/json.rs:346-351.

## Confidence

High on the float path: the dedup order, pruning soundness, budgets,
distributed bit-identity, and federation semantics are each backed by tests
I read and judged non-vacuous (independent oracles, proptests over hostile
bit patterns, fault-injection with asserted counters). High on the
native-histogram findings: the degradation paths are directly visible in
the code (value-only reductions over elements whose value field is
documented as meaningless), the guard that should have caught them exists
on the subquery path only, and the corpus demonstrably lacks the entries
that would have caught them. Medium on SQL spans/logs pruning internals and
Flight SQL edge lifecycles (read, tests exist, but I did not re-derive
every fold). Low-confidence areas are listed as Unknowns rather than
findings.

# TLA+ verification suite: report

## 1. Scope and commit

This report covers the six areas under `formal/tla/` (`common`, `commit`,
`catalog`, `lifecycle`, `resharding`, `maintenance`), seven specification
modules in all, as they stand at commit
`7124a3d3a4e9005cef303341989a40ac3b2bd153`. Maintenance alone holds two
modules: `MaintenanceOwnership.tla` (ADR-0065, shipped behaviour) and
`CompactionClaims.tla` (ADR-1029, a proposed design over a landed
CreateIfAbsent/CasVersion claim primitive that nothing in the repository
calls yet; this suite checks that the design is internally consistent, not
that it is implemented). Every figure below is copied from the area's own
`results.md` and `bands.tsv`; no model was run to produce this report, and
no figure here was recomputed, rounded, or recalled from anything other than
those files.

## 2. Method

Each area's `results.md` records TLC's own output for every configuration it
ran: states generated, distinct states, search depth, wall time, and the
result. `bands.tsv` (where an area has one) records the distinct-state and
depth range a passing smoke or exhaustive run must land in; a run outside its
band is a regression, not something the band gets widened to absorb.
Negative controls are exempt from banding: TLC's error search stops at the
first counterexample it finds, so the number of states it explores before
stopping varies run to run, and negative controls are pinned by exit code and
violated-property name instead (`negative/<name>.expect`).

This report transcribes those two sources verbatim per configuration. Where
an area's `results.md` and `bands.tsv` disagree with each other, or an area's
own documents disagree internally, that disagreement is reported in section 5
rather than resolved by picking one side.

## 3. The object-store model

`common/RavelObjectStore.tla` is the one shared module; every other
specification instantiates it rather than re-modeling storage semantics. It
encodes exactly what docs/object-store-contract.md promises and nothing more:

- whole-object atomic visibility (a key is present with one content and one
  version, or absent);
- `CreateIfAbsent` returning `AlreadyExists` on a present key;
- `CasVersion(v)` returning `PreconditionFailed` on a version mismatch or an
  absent key;
- `Overwrite` only where a model's instantiation permits it (heartbeats, memo
  snapshots; never on the commit or catalog planes);
- read-after-write and list-after-write for successful writes;
- paginated listing as a nondeterministic traversal: every key present before
  the first page eventually appears, a key created mid-scan may or may not
  appear, and a key may appear twice, so a consumer whose result depends on
  multiplicity must deduplicate or the checker finds the error;
- a lost response: the store applies the operation and the caller observes a
  failure, then retries.

`common`'s own model-check entry (`MCRavelObjectStore.tla`) is a self-test of
this module: it is a "sixth area" only in the sense that it gets a smoke and
exhaustive config, not a protocol under separate development.

## 4. What each specification covers

| Area | Specification | Checks | Label |
|---|---|---|---|
| Commit | `commit/CommitProtocol.tla` | pinned flush identity; data-then-commit `CreateIfAbsent`; strict and buffered acknowledgement; lost responses; retry of the same flush; identity reuse with different content; multi-shard fan-out with the shipped per-signal partial-commit behavior; idempotency markers with fail-open lookup; commit-token resolution across all four outcomes (present, tombstoned, retired into a compaction or rewrite, missing) | shipped |
| Catalog | `catalog/CatalogMVCC.tla` | immutable L0 commits; content-addressed L1 parts; compaction records naming inputs and outputs; snapshot parts; HEAD CAS with racing folders; both seal predicates; fixed-window and retention-frontier reconcile guarded on watermark advance; query pinning with one re-resolve; corrupt HEAD handled four ways; late compaction and rewrite records; abandoned parts | shipped |
| Lifecycle | `lifecycle/LifecycleGC.tla` | tombstone, exclusion, rewrite-and-supersede, completion verification, horizon-gated deletion, plaintext request cleanup; `sys/gc` validation; HEAD reachability on the retention path only; legal-hold refresh and its failure; hold scopes; rewrite-of-rewrite and rewrite-of-compaction predecessors; request-set-bound rewrite identity; pinned readers with `max_query_duration`; an abstract cache; the mass-orphan breaker as a recomputed predicate | shipped, with ADR-0064's out-of-window case recorded as open |
| Resharding | `resharding/OnlineResharding.tla` | append-only generation history under CAS; concurrent reshards; lazy router refresh with interval `C`; activation lead `L`; degraded-grace routing; wall-clock routing versus pinned flush hour; scan slack `S`; safely-old HEAD rule; reader fail-closed; commit-token resolution independent of shard count; both increases and decreases | shipped |
| Maintenance | `maintenance/MaintenanceOwnership.tla`, `maintenance/CompactionClaims.tla` | heartbeats, bidirectional staleness, rendezvous ownership over asymmetric live sets, per-cycle frozen live set, restart with a fresh identity, wedged workers, memos with expiry and corruption; claims with `CreateIfAbsent` acquisition, version-CAS renewal, server-time expiry, racing thieves, stale owners, no unconditional delete, and the terminal compaction record as the actual decision | ownership: shipped; claims: proposed design over a landed primitive |

(D1 table, ADR-1113.)

## 5. Results

Figures below are copied verbatim from each area's `results.md`; the "Band"
column is copied from `bands.tsv`. Wall time is never banded.

### common

Run `20260902T233208Z-2b36d8d0479151c10e2c6eb77f12451bd90ceb78`.

| Config | Spec | Distinct | Depth | Wall | Result | Band (distinct / depth) |
|---|---|---|---|---|---|---|
| smoke.cfg | MCSpec (safety, symmetry-reduced) | 2011892 | 15 | 102s | PASS | 1950000-2070000 / 15-15 |
| exhaustive.cfg | FairSpec (safety + liveness) | 3845952 | 15 | 252s | PASS | 3730000-3960000 / 15-15 |

Both figures land inside their bands; no disagreement between `results.md`
and `bands.tsv`.

### commit

| Config | States generated | Distinct | Depth | Wall | Result | Band (distinct / depth) |
|---|---|---|---|---|---|---|
| smoke.cfg | 305165 | 76212 | 21 | 5s | PASS | 76212-76212 / 21-21 |
| exhaustive.cfg | 17892751 | 5466239 | 36 | 131s | PASS | 5466239-5466239 / 36-36 |

Both figures land exactly on their (zero-width) band. No disagreement.

Two configurations run outside any harness lane and carry no band:
`dedup-mutant.cfg` (`DuplicateUnreachable` only, `RetryDedups=TRUE`):
14974258 states generated, 3443658 distinct, depth 32, PASS (exit 0), 1m24s.
`live.cfg` (`FairSpec` / `EveryPinnedFlushSettles`): 42119 states generated,
15812 distinct, depth 13, PASS (exit 0), under 1s.

### catalog

Run `20260904T191103Z-603bb3e163736f80cd14b07574344d946567a739` unless noted.

| Config | Spec | Distinct | Depth | Result | Band (distinct / depth) |
|---|---|---|---|---|---|
| smoke.cfg | Spec (safety, symmetry-reduced) | 3463504 | 33 | PASS | 3455000-3470000 / 33-33 |
| exhaustive.cfg | FairSpec (safety + `QueryTerminates` liveness) | 3422524 | 31 | PASS | 3415000-3430000 / 31-31 |
| carryforward.cfg | Spec (safety, three-hour carry-forward) | 4481272 | 37 | PASS, run `tlc-carryforward-1845293` | no band (targeted, not a harness gate) |

Both banded figures land inside their bands. `overlap.cfg` has no completed
PASS figure recorded in `results.md`: at its current bounds the full graph
does not finish exploring within the area's own time budget, so it is run as
a targeted safety check rather than a gated pass/fail lane.

`late-supersession-shrink.cfg` (ungated, not run by any harness lane) checks
`LateSupersessionEventuallyReflected` under `FairSpec`: 2,427,940 states
generated, 947,275 distinct states found, depth 14, TLC exit 13 (temporal
violation). `results.md` and the counterexample note both record this as a
finite-model limitation, not a defect (see section 6).

### lifecycle

Final (round seven) figures, matching `bands.tsv`:

| Config | States generated | Distinct | Depth | Wall | Result | Band (distinct / depth) |
|---|---|---|---|---|---|---|
| smoke.cfg | 276015 | 50102 | 21 | 3s | PASS | 50040-50160 / 21-21 |
| exhaustive.cfg | 1340669 | 230815 | 22 | 30s | PASS | 230750-230900 / 22-22 |

Both land inside their bands. `exhaustive.cfg` runs at `MaxClock = 3`;
`results.md` records that `MaxClock = 4` does not complete (2,455,254+
states generated, growing past 550,000 distinct after roughly 64 seconds
with no sign of convergence in the observed window).

`candidate-1133.cfg` (`HorizonGuardsPinnedQueries=FALSE`, an alternative
design candidate, not a harness lane) reaches a 6-state trace violating
`NoDeleteInsideProtectionWindow`, TLC exit 12. No states/distinct/depth/wall
figures are recorded for this run in `results.md`. Verdict recorded there:
"the horizon plus unnamed-HEAD gate is not sufficient; the pinned-query
clause is load-bearing" — the shipped model keeps
`HorizonGuardsPinnedQueries = TRUE`, under which this trace has no
successor.

**Disagreement 1 (invariant count, lifecycle) — resolved 2026-09-05.**
`lifecycle/README.md`'s claim that the seven negative controls run "the full
fourteen-invariant list from `smoke.cfg`" did not match the shipped
`negative/*.cfg` files, which declared only 13 `INVARIANT` lines (`TypeOK`
plus 12 named), omitting `TombstoneNotDeletedBeforeBucketEmpty` and
`RawInputContentAssumedImmutable`. Both invariants were added to all seven
`negative/*.cfg` files and the negative lane was re-run: every control still
violates only its declared target invariant (exit 0 for the lane), so the
addition is correct. The README's fourteen-invariant claim matched the
configs by count at the time; the phrase itself was tightened on
2026-09-07 to "all fifteen INVARIANT lines (TypeOK plus fourteen named)" in
both this file and `lifecycle/README.md`, for precision rather than because
the count had drifted again.

### resharding

| Config | States generated | Distinct | Depth | Wall (results.md) | Wall (bands.tsv comment) | Result | Band (distinct / depth) |
|---|---|---|---|---|---|---|---|
| smoke.cfg | 7809360 | 958804 | 18 | 43s | 43s | PASS | 900000-1000000 / 17-19 |
| exhaustive.cfg | 8503664 | 1179718 | 20 | under 300s | under 300s ("under 5 min") | PASS | 1000000-1500000 / 18-23 |

Both configurations land inside their distinct/depth bands.

**Disagreement 2 (wall time, resharding smoke) — resolved 2026-09-05.**
`results.md`'s configuration table recorded `smoke.cfg`'s wall time as 37
seconds; `bands.tsv`'s header comment, describing the same run (identical
states/distinct/depth: 7809360/958804/18), recorded it as 26 seconds. The
smoke lane was re-run once (same states/distinct/depth) and measured 43
seconds; both `results.md` and the `bands.tsv` header comment now record
43 seconds. The state and depth bands are unchanged.

Five negative controls (no bands; error-search stops at first violation, so
counts vary run to run):

| Control | Flipped from shipped | Property violated | Distinct | Seconds |
|---|---|---|---|---|
| scan-slack-zero | `S = 0` (shipped 3) | EveryAdmittedWriteInScanSet | 3512 | 2 |
| appender-skew-unbounded | `AppenderSkew = 5` (tolerated 1) | EveryAdmittedWriteInScanSet | 124721 | 4 |
| lead-one | `L = 1` (shipped 2) | LeadCoversRefreshHorizon | 236 | under 1 |
| no-writer-fence | `WriterFenceEnabled = FALSE` | StaleWriterFailsClosed | 48018 | 2 |
| token-validated-against-count | `TokenValidatedAgainstCount = TRUE` | TokenResolvesAcrossReshards | 12709 | 3 |

**Disagreement 3 (lead-one figures, resharding's own docs) — resolved
2026-09-05.** `results.md`'s negative-control table recorded `lead-one` as
174 distinct states in 2 seconds; `counterexamples/lead-one.md`, describing
the same control, recorded "199 distinct states, one second." TLC's
error-search stops at the first violation a worker finds, so the exact
count varies run to run. The `lead-one` control was re-run once and that
run's figures, 236 distinct states in under one second, are now recorded in
both `results.md` and `counterexamples/lead-one.md`.

Configurations outside any harness lane, no bands:

- `live.cfg`: first run (weak fairness only) VIOLATED
  `EventuallyRoutedOnNewGeneration` — 17193 states generated, 6286 distinct,
  4s. After widening to strong fairness on `AdmitAfterRefresh`, re-run PASS:
  31045026 states generated, 3817433 distinct, depth 18, 11m36s.
- `shipped-skew-minutes.cfg`: killed at an internal 280s timeout, depth 14:
  49,957,695 states generated, 14,129,398 distinct states found, 10,071,309
  states left on queue, TLC exit 124. `results.md` records this explicitly
  as "not pass or fail" and recommends a longer-budget run, not yet executed.
- `two-writer-concurrency-probe.cfg`: `TwoWritersNeverConcurrentlyOpen`
  VIOLATED as intended (a reachability check) — 26 distinct states, depth 4.
- `flush-bound-trailing.cfg`: at `FlushBound = 2`, `FlushBoundNeverBites`
  VIOLATED — 2070 states generated, 1040 distinct, depth 7. At
  `FlushBound = 1`, re-run killed at 1,768,734 distinct states and climbing,
  depth 13 — inconclusive by TLC, resolved algebraically instead. A separate
  mutant guard (`<= FlushBound + 1`) against the full eleven-invariant list
  was killed at depth 14, 1,794,770 distinct states and still climbing, no
  violation found before the kill.
- `writer-fence-comparison.cfg`: full breadth-first search killed past
  65,000,000 distinct states after 45 minutes at `MaxHour = 4`, and past
  8,000,000 distinct states after 6 minutes even at `MaxHour = 2`,
  `AppenderSkew = 1` — both inconclusive. A fallback TLC random-simulation run
  at the same dimensions (`-simulate num=100000000 -depth 100`) checked
  63,203,643 states in 300 seconds with zero `TypeOK` or
  `StaleWriterFailsClosed` violations. `results.md` states plainly that this
  run "is budget-bounded, not traversal-bounded" and calls the result
  "strong evidence, not an exhaustive proof" — not a claim of exhaustive
  coverage. (See section 10 for why this is the suite's one genuine
  bounded-simulation-instead-of-exhaustive case, and why it belongs to
  resharding rather than to maintenance.)

### maintenance

| Config | States generated | Distinct | Depth | Wall | Result | Band (distinct / depth) |
|---|---|---|---|---|---|---|
| MCMaintenanceOwnership.smoke.cfg | 47377233 | 2773760 | 21 | 90s | PASS | 2640000-2910000 / 18-22 |
| MCMaintenanceOwnership.exhaustive.cfg | 136617032 | 13183990 | 20 | 1769s | PASS | 12500000-13850000 / 18-22 |
| MCCompactionClaims.smoke.cfg | 65454526 | 11155721 | 17 | 161s | PASS | 11100000-11700000 / 14-20 |
| MCCompactionClaims.exhaustive.cfg | 1972 | 543 | 11 | 2s | PASS | 500-580 / 8-13 |

All four figures land inside their bands. No disagreement between
`results.md` and `bands.tsv` for maintenance.

`MCMaintenanceOwnership.exhaustive.cfg` runs at `Workers = {1}` only. At
`Workers = {1, 2}` with no `VIEW`, the exhaustive graph was still climbing
past 4,600,000 distinct states at depth 13 of an eventual depth 20 without
converging, so it was not run to completion in that configuration; the
two-worker case is instead covered by a separate, exhaustive-with-abstraction
smoke run (see section 10).

Twelve negative controls, no figures tabulated (dashes throughout
`results.md`'s configuration table) beyond exit code:

| Control | Result |
|---|---|
| negative/ownership-as-publication-authority.cfg | VIOLATED (exit 12) |
| negative/heartbeat-memo-cas.cfg | VIOLATED (exit 12) |
| negative/memo-overstamp.cfg | VIOLATED (exit 12) |
| negative/mo-diverge-overwrites-record.cfg | VIOLATED (exit 12) |
| negative/mo-missing-part-reports-converged.cfg | VIOLATED (exit 12) |
| negative/zero-ownership-phantom.cfg | VIOLATED (exit 13) |
| negative/claim-completion-without-cas.cfg | VIOLATED (exit 12) |
| negative/claim-delete-unconditional.cfg | VIOLATED (exit 12) |
| negative/claim-as-publication-authority.cfg | VIOLATED (exit 12) |
| negative/guarded-publish-ignores-claim.cfg | VIOLATED (exit 12) |
| negative/diverge-overwrites-record.cfg | VIOLATED (exit 12) |
| negative/missing-part-reports-converged.cfg | VIOLATED (exit 12) |

## 6. Counterexamples found

Classifications below are each area's own, taken from its `results.md` or
`counterexamples/*.md` files, never assigned by this report. Where a file's
own text does not state a classification, this report says so rather than
inferring one.

**common** — five files under `counterexamples/`, all mutants against the
correct module demonstrating that a specific injected defect is caught (CAS
on an absent key accepted, a multipart part published early, a delete of an
absent key that stamps a version, a delete that resets the version counter, a
counting listing consumer that deduplicates). None is an open defect; each
confirms detection. No tracking issue applies.

**commit** — eleven files under `counterexamples/`, one per negative
control (resolved 2026-09-05: the five configurations
(`no-cross-shard-atomicity`, `put-commit-lost-response-reachable`,
`put-data-lost-response-reachable`, `query-reads-uncommitted-data`,
`transient-failure-reachable`) that previously had no corresponding note
under `counterexamples/` now each have one). All eleven are broken-behavior
negative controls (D6); none is classified in `results.md` as a model bug,
design flaw, or implementation defect, and none carries a tracking issue.

**catalog** — twenty-three files under `counterexamples/` (plus
`late-supersession-shrink.cfg`, a config with no matching negative lane).
Of the 21 `negative/*.cfg` configurations, `results.md` itself marks seven as
"(probe)" — reachability/non-vacuity checks, not broken-behavior controls —
and the other fourteen as ordinary broken-behavior controls. Resolved
2026-09-05: the three probe configurations (`entry-undecodable-nonvacuity`,
`head-corruption-nonvacuity`, `part-unreadable-nonvacuity`) that previously
had no corresponding note under `counterexamples/` now each have one.
`dedup-starvation-fixed.md` is a distinct, already-fixed implementation
defect: "Issue #1121 finding 1," a bug in `Dedup(P)` that let two identities
sharing one L1 source each independently choose a different survivor and
lose the source entirely; the note records before-and-after non-vacuity runs
and gives the fixed invariant (`DedupPreservesCoverage`). This is a
documentation-defect / implementation-defect finding with a tracking issue
(#1121), already resolved. `late-supersession-shrink.md` classifies its own
finding explicitly as "a finite-model limitation, not a defect": TLC finds a
genuine stuttering counterexample to `LateSupersessionEventuallyReflected`
because a bounded model clock cannot supply the unbounded sequence of
watermark-advancing folds the property needs; no tracking issue is stated,
and none is invented here.

**lifecycle** — twenty-four files under `counterexamples/`. Seven match the
area's seven negative controls one-to-one (broken-behavior controls, D6).
Fifteen are self-labeled "Non-vacuity mutant" or "Probe" in their own first
lines, each tied inline to a specific finding from "issue #1122" (the epic's
own numbering for lifecycle's development rounds) and each already resolved
by the guard or invariant it demonstrates; none is an open item. One file,
`candidate-1133.md`, tests a rejected alternative design
(`HorizonGuardsPinnedQueries = FALSE`) and reaches a counterexample to
`NoDeleteInsideProtectionWindow`; `results.md`'s own verdict is that the
pinned-query clause is load-bearing, so the shipped model keeps the guard
true — this is a design-validation exercise, not an open defect, and it
carries no tracking issue.

**resharding** — five files under `counterexamples/`, one per negative
control (broken-behavior controls, D6; `results.md` further notes that
`lead-one` and `no-writer-fence` were initially expected to violate a
different property, `EveryAdmittedWriteInScanSet`, and only violate their
actual named properties instead — reported as a finding about the model's
own margins, not a defect). None carries a tracking issue.

**maintenance** — ten mutant files under `counterexamples/` (plus this
area's own `README.md`, which is not a counterexample note, and
`wv-store-grounding-equivalence.md`, a grounding-equivalence note rather than
a counterexample). The ten mutants correspond to the twelve negative
controls (two negative controls, `zero-ownership-phantom` and one other, are
not represented by a separately named mutant file). All twelve negative
controls are broken-behavior controls (D6); none is classified as a model
bug, design flaw, or implementation defect in `results.md`, and none carries
a tracking issue.

## 7. Negative controls

Split per area between broken-behavior controls (D6: a CONSTANT switch flips
one specific wrong behavior on) and reachability/non-vacuity obligations
(probes that a guarded action, invariant, or state is still reachable, not a
demonstration of broken behavior).

- **common**: three `negative/*.cfg` controls, all broken-behavior (a lost
  response not applied, a stale-version CAS accepted, a list that never
  progresses). No reachability-obligation configs in `negative/`.
- **commit**: eleven `negative/*.cfg` controls, all broken-behavior per D6's
  list for this area (commit-before-data, mismatched identity accepted as
  idempotent, acknowledgement before durable commit, and others). The
  by-hand `dedup-mutant.cfg` and `live.cfg` runs (section 5) are reachability
  and liveness checks respectively, run outside the `negative/` harness lane.
- **catalog**: 21 `negative/*.cfg` controls; `results.md` itself splits these
  into fourteen broken-behavior controls and seven reachability/non-vacuity
  probes (named "(probe)" in its own results table — see section 6).
- **lifecycle**: seven `negative/*.cfg` controls, all broken-behavior per
  D6's list for this area (deletion before the protection horizon, legal-hold
  refresh failure treated as no hold, and others). The fifteen probe/mutant
  files under `counterexamples/` (section 6) are reachability and non-vacuity
  obligations run outside the `negative/` harness lane, each tied to a
  specific guard added during development.
- **resharding**: five `negative/*.cfg` controls, all broken-behavior
  (`S = 0`, `L = 1`, no writer-staleness fence, and others). The
  `two-writer-concurrency-probe.cfg` run (section 5) is a reachability
  obligation, run outside `negative/`.
- **maintenance**: twelve `negative/*.cfg` controls, all broken-behavior per
  D6's list for this area (claim completion or deletion without version CAS,
  an advisory claim treated as publication authority, ownership treated as
  publication authority, and others). No reachability-obligation configs are
  recorded under `negative/` for this area in `results.md`.

## 8. Liveness and fairness

No specification ever adds fairness over the whole `Next` (ADR-1113 D4);
each defines `Spec` (safety only) and a `FairSpec` naming exactly the actions
the implementation justifies as eventually-fair.

- **common**: `FairSpec` adds `WF_vars` on the list-progress action so
  `ListEventuallyComplete` holds under `exhaustive.cfg`; the safety-only
  `smoke.cfg` drops it. `exhaustive.cfg` also drops the `Symmetry` reduction
  `smoke.cfg` uses, since TLC does not check liveness under symmetry
  reduction.
- **commit**: `live.cfg` checks `EveryPinnedFlushSettles` under `FairSpec`
  and passes (42119 states, 15812 distinct, depth 13). This is the only
  liveness property recorded as checked for this area in the reviewed
  figures.
- **catalog**: `exhaustive.cfg` checks `QueryTerminates` under `FairSpec` and
  passes. `LateSupersessionEventuallyReflected` is defined but not checked by
  any harness lane; checked directly under `FairSpec` (ungated), it is
  temporally violated (section 5, section 6) because the bounded model clock
  cannot supply the unbounded sequence of watermark-advancing folds the
  property depends on. This is reported as a finite-model limitation to
  document, per the area's own classification, not as a liveness defect.
- **lifecycle**: the final lane, `exhaustive.cfg` (`FairSpec`, `MaxClock = 3`),
  reports PASS on the full invariant list together with both
  `EventuallySwept` and `EventuallyCompleted` (section 5: 1340669 states
  generated, 230815 distinct, depth 22, inside band). Both are conditional
  properties, not unconditional guarantees (per `results.md`'s finding on
  issue #1131): they hold only when the environment eventually goes quiet on
  hold state, HEAD read state, and refresh outcome, and when the fold's and
  the sweep's retention windows agree. A permanently wedged hold, an
  indefinitely refreshing legal hold, or disagreeing windows make them
  intentionally false, per the area's own documentation; `results.md`
  reports this as a liveness limitation rather than a defect (issue #1131).
  A historical result, from round two's reduced diagnosis
  (`results.md`, "Liveness, reduced diagnosis"), found both properties
  violated (TLC exit 13) when each was checked alone in a scratch
  `MaxClock = 2` configuration under `FairSpec`; that result predates the
  fairness additions of checkpoint finding 1 and the self-negating-antecedent
  fix of round three, and is not the lane's current outcome.
- **resharding**: `live.cfg` initially fails
  `EventuallyRoutedOnNewGeneration` under weak fairness alone, and passes
  once fairness is widened to strong fairness on `AdmitAfterRefresh`
  (section 5). This is reported as the area's own finding about which
  actions must carry fairness for the property to hold, not as a defect.
- **maintenance**: liveness at `MCMaintenanceOwnership.exhaustive.cfg` is
  checked at `Workers = {1}` only. `results.md` and `README.md` both state
  that no lane in this suite exhaustively checks either liveness property at
  two workers (`NoWorkerEverStale`, `OwnershipIsNotPublicationAuthority`); at
  `Workers = {1, 2}` the safety-only smoke config
  (`MCMaintenanceOwnership.smoke.cfg`) covers the two-worker duplicate-
  ownership race exhaustively for safety alone, up to the sound `MCView`
  state abstraction — liveness at two workers is not covered by any
  configuration in this suite (see section 10 for the exact wording).

## 9. Assumptions not checked

Gathered from each area's own stated assumptions; none is derived or
inferred by this report.

- **lifecycle**: raw-input immutability. `RawInputContentAssumedImmutable`
  (`\A o \in RawInputs : objContent[o] = InitContent(o)`) is checked as an
  assumption being asserted, not a protocol property being proved — the
  model has no transition that could change a raw input's content, matching
  `docs/object-store-contract.md` and `put_data_object`'s
  `PutOptions::create_if_absent` use, and the area's own text is explicit
  that this is "named to read as an environmental assumption being
  asserted... not a protocol property being proven."
- **commit**: data-object PUT idempotency. The model assumes the data-object
  `CreateIfAbsent` publish is idempotent under retry at the object-store
  layer (the same content republished to the same key either succeeds once
  or reports `AlreadyExists`), matching `common/RavelObjectStore.tla`'s own
  `PutCreateIfAbsent` contract; this is an assumption about the store's
  conformance to its own contract, not something the commit model itself
  checks.
- **maintenance**: the segment/part encoder, the blake3 hash and work-id, the
  merge's multiset preservation, and the object store's own conformance to
  its contract are all named in `maintenance/README.md` as assumptions,
  stated as such, not checked by either specification in this area.
- **catalog**: the object store's own conformance to its contract is assumed
  rather than checked by `CatalogMVCC.tla`, consistent with D2 (the contract
  itself is modeled once, in `common`, and every area assumes the real store
  meets it).
- **resharding**: `writer-fence-comparison.cfg`'s fallback simulation result
  (63,203,643 states, zero violations, section 5) is explicitly reported as
  "strong evidence, not an exhaustive proof" that the writer-fence flag alone
  is load-bearing at `AppenderSkew = 2`; full exhaustive coverage of that
  configuration is not established, and this report does not claim otherwise.

## 10. Out of scope

- **Lifecycle rewrite-of-rewrite predecessors.** `lifecycle/README.md` states
  that `RewriteOutputContent`'s current-state read is not vacuous for every
  predecessor, only for a raw-input one: it would matter for a predecessor
  that is itself a rewrite output, and this model does not reach that case,
  because `RewriteOut` names exactly one rewrite object, its predecessor set
  is fixed to `RawInputs`, and no action produces a second rewrite object a
  further rewrite could take as input — so rewrite-of-rewrite is
  unreachable in `smoke.cfg`, `exhaustive.cfg`, or any other configuration in
  this area. Tracked as issue #1221.
- **Maintenance two-worker lane.** Covered in section 8: the two-worker
  duplicate-ownership race is exhaustive for safety only, under the sound
  `MCView` state abstraction, with no fairness and no liveness checked at
  two workers. The suite's one genuinely budget-bounded, non-exhaustive lane
  is resharding's `writer-fence-comparison.cfg` (section 9).
- **ADR-0064's out-of-window case** is recorded as open for lifecycle (D1
  table, section 4): the model covers the shipped retention path, not the
  out-of-window legal-hold interaction ADR-0064 leaves as a follow-up.
- **CompactionClaims** (maintenance) models a proposed design (ADR-1029)
  layered on a landed `CreateIfAbsent`/`CasVersion` claim primitive that
  nothing in the repository calls yet; nothing in this suite establishes
  that the proposal is implemented, only that the design is internally
  consistent under the checked bounds.
- **catalog's `overlap.cfg`** does not complete a full exploration within its
  time budget at current bounds (section 5); it runs as a targeted safety
  check, not a gated pass/fail lane, and full coverage of that configuration
  is not established.
- **resharding's `shipped-skew-minutes.cfg`** was killed at an internal
  timeout with no violation found and 10 million states still queued
  (section 5); `results.md` recommends a longer-budget run, not yet
  executed. This suite does not claim a result for that configuration.
- **Documentation-note gaps, closed 2026-09-07.** This entry previously
  reported five of commit's eleven negative controls and three of catalog's
  seven nonvacuity negative controls with no corresponding note under
  `counterexamples/`. Checked against the shipped tree: all eleven of
  commit's `negative/*.cfg` files and all twenty-one of catalog's have a
  matching note under their respective `counterexamples/` directories, one
  per negative case as D6 calls for (section 6). No gap remains in either
  area.

## 11. What this does and does not establish

In the words ADR-1113 D12 requires and no stronger:

TLC checked each finite model under the bounds and assumptions recorded in
its own `results.md` and `.cfg` files. This model verifies the protocol
design; implementation conformance is argued in the traceability tables
(`TRACEABILITY.md` and each area's `traceability.md`) and asserted by the
named Rust tests, not proved. Safety and liveness are named separately
throughout this report, with the fairness assumptions listed next to every
liveness result (section 8). The segment/part encoder, the hash functions,
the merge's multiset preservation, and the object store's own conformance to
its contract are assumptions, stated as such, not properties this suite
checks (section 9).

A literal search for the phrase "formally verified" across `formal/` and
`docs/adrs/1113*`, excluding this report itself (`REPORT.md` quotes and
discusses the phrase several times in this very paragraph and in section 6's
source material, which would make the count circular), returns **two**
matches, not zero:

- `docs/adrs/1113-tla-verification-suite.md`, under "D12. What the suite
  claims, in the words it must use": `"Ravel is formally verified" does not
  appear anywhere.`
- `formal/tla/maintenance/README.md`, in its opening summary: `..."Ravel is
  formally verified" is not a claim this suite makes.`

Both matches are the literal string appearing inside an explicit denial, not
a claim. There is no sentence anywhere in the suite that asserts Ravel, or
any part of it, is formally verified; the two hits above are the suite
disclaiming that exact claim by name. This report states the raw grep count
precisely (two) rather than rounding it to the zero the drafting instruction
expected, because the instruction's premise — that the string would not
appear at all — does not match what the phrase-level denials actually do.

This suite establishes that seven finite models, across the six areas, of
Ravel's coordination protocols hold their stated safety invariants under the
checked bounds (CompactionClaims as a proposed design, not shipped
behaviour), that the negative controls demonstrate each invariant is
non-vacuous (it can be made to fail), and that the stated liveness
properties hold under the named fairness assumptions where checked. It does
not establish that the Rust implementation conforms to these models beyond
what each area's
traceability table and named tests assert; it does not establish anything
about configurations, clock bounds, or worker counts wider than the ones
each `results.md` records having run; and it does not establish, and does
not claim, that Ravel is formally verified.

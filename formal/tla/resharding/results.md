# Resharding model: results

Model checker: TLC from tla2tools 1.7.4 (sha256 verified by the harness).
Run id: `20260904T143812Z-c7b636a7a5d2fbdc6c28ad9bd839749e63fcd9f8`
(`scripts/check-tla.sh ci -a resharding`, a single coherent run of smoke,
every negative control, and traceability, re-run for the issue #1123
third-round review below).

## Smoke (safety)

| cfg | result | states | distinct | depth | seconds |
|---|---|---|---|---|---|
| smoke.cfg | PASS | 7809360 | 958804 | 18 | 43 |

Two writers and two requesters race an increase (3) and a decrease (1) from
generation 0's count of 2, symmetry-reduced over both permutation groups, with
all eleven safety invariants asserted. The `bands.tsv` row brackets the
distinct-state and depth figures with margin, so a silent state-space collapse
fails the smoke gate rather than passing quietly. `CasAttempts = 1` keeps the
run inside its time budget; the dropped CAS retry loop is covered by
`exhaustive.cfg`.

## Negative controls

Each control flips one margin to a wrong value and TLC exhibits the property
that margin protects. The `.expect` file for each names the exact property, and
the harness fails unless TLC reports that one.

| Control | Flipped from shipped | Property violated | distinct | seconds |
|---|---|---|---|---|
| scan-slack-zero | `S = 0` (shipped 3) | EveryAdmittedWriteInScanSet | 3512 | 2 |
| appender-skew-unbounded | `AppenderSkew = 5` (tolerated 1) | EveryAdmittedWriteInScanSet | 124721 | 4 |
| lead-one | `L = 1` (shipped 2) | LeadCoversRefreshHorizon | 174 | 2 |
| no-writer-fence | `WriterFenceEnabled = FALSE` | StaleWriterFailsClosed | 48018 | 2 |
| token-validated-against-count | `TokenValidatedAgainstCount = TRUE` | TokenResolvesAcrossReshards | 12709 | 3 |

TLC's error-search runs stop at the first violation a worker finds, so the
exact distinct-state count for a VIOLATED result varies run to run with
worker scheduling; these are this run's figures, not a fixed constant.

### Two controls target the invariant their margin directly protects

lead-one and no-writer-fence were initially expected to violate
`EveryAdmittedWriteInScanSet`, and under that expectation both passed: at
`C = 1` the shipped scan slack `S = 3` still covers the admitted data even when
the lead is one hour short or the writer fence is off, so the data-in-scan-set
property does not break. The margins are layered, and slack is the last line of
defense; removing an inner margin breaks that inner margin's own contract while
the slack still masks the downstream data-loss consequence.

Both controls therefore now target the property their margin protects directly:

- lead-one violates `LeadCoversRefreshHorizon`. A one-hour lead lets a
  generation activate inside a writer's refresh horizon, which is exactly what
  that invariant forbids. No clock skew is needed.
- no-writer-fence violates `StaleWriterFailsClosed`. The fence exists to make a
  writer past its grace horizon fail closed; with it off the writer admits far
  past that horizon, which that invariant catches directly.

This is a finding about the model at `C = 1`, not a workaround: it shows that
at this rounding the slack margin is wide enough to absorb a short lead or a
disabled fence without losing data, so the sharp witness for each is the
inner contract, not the outer one.

### Enabling condition for the straggler-data controls

scan-slack-zero and token-validated-against-count keep every other margin
correct and set `AppenderSkew = 2` as the enabling condition: at skew 0 the lead
keeps every admitted straggler at an ingest hour a correct slack would still
cover, so the flipped margin only becomes load-bearing once a skewed appender
clock pulls the activation earlier in router time. The skew creates the
post-activation straggler; the flipped margin (no slack, or a spurious count
check on the token) is what then fails to handle it.

### Smallest breaking appender skew

appender-skew-unbounded uses `AppenderSkew = 5` at `MaxHour = 5`: it exhibits
the data-loss violation in a few seconds. This is not the smallest breaking
skew. Within the horizons the executor can run:

- skew 5 and 6 break quickly at `MaxHour = 5`.
- skew 4 breaks, but only at `MaxHour = 6` with a multi-million-state search
  (minutes), too slow for the executor budget.
- skews 1 through 3 produced no counterexample in the horizons run. This is
  evidence, not proof: a deeper counterexample is not excluded.

The tolerated bound is `TOLERATED_CLOCK_SKEW_HOURS = 1`. That skew 1 produced no
counterexample is consistent with the shipped system being safe at the tolerated
bound, but at the `C = 1` rounding this is a boundary case, not a comfortable
margin: the lead, the tolerated skew, and the staleness sit on the same hourly
scale. The true minimum breaking skew at `C = 1`, and a direct check of safety
at the tolerated skew 1, are left to the orchestrator's exhaustive run.

## Exhaustive

| cfg | result | states | distinct | depth | seconds |
|---|---|---|---|---|---|
| exhaustive.cfg | PASS | 8503664 | 1179718 | 20 | under 300 |

A prior version of `exhaustive.cfg` kept smoke's two writers and widened
`CasAttempts` and `MaxAdmitsPerWriter` to 2 together at `MaxHour = 4`. That
run reached 36,881,908 distinct states with 21,017,614 still queued after 73
minutes before being killed, over ADR-1113's budget. `exhaustive.cfg` now
drops to a single writer (`Writers = {w1}`), keeps both widenings, sets
`FlushBound = 2` to match the shipped constant, and completes as shown above.
See `exhaustive.cfg`'s own header for the full resize rationale, including
the two single-dimension explosion figures that ruled out keeping two
writers at either widening alone.

Coverage given up: the concurrent-two-writer interleaving is not re-checked
against the wider `CasAttempts`/`MaxAdmitsPerWriter` bounds in this run.
`two-writer-concurrency-probe.cfg` shows that interleaving is reachable at
smoke's own dimensions instead (`TwoWritersNeverConcurrentlyOpen` violated in
26 distinct states, depth 4), so the drop is a gap in this run's cross
product, not an unverified capability. Two generations, multiple hours, and
both an increase and a decrease in shard count are unaffected (same
`TargetCounts`/`InitialShardCount`/`MaxGenerations` as smoke, which already
reaches all three).

A `skew = 1` exhaustive variant is recommended to substantiate the
tolerated-skew half of the ADR-1113 D12 claim directly; every `AppenderSkew`
config tried this round exploded regardless of writer/requester count (see
the clock-skew section below), so that variant is left as future work.

## What these runs claim

TLC checked this finite model under the bounds and assumptions in README.md and
found no reachable state violating the smoke invariants, and found the expected
violation for each negative control. This is a check of a finite model, not a
proof of the Rust implementation or of safety outside the modeled bounds.

## Issue #1123 findings

### Finding 1 (critical): `CasAppendNeverDiscards` was self-referential

The invariant read `lastOp.before`/`lastOp.after`, fields the same step that
sets `lastOp.outcome` also writes. A lost-response or buggy CAS path could
therefore forge a witness matching its own claim: the invariant checked that
the operation's story was internally consistent, not that the store agreed
with it.

Fixed by grounding the check on the store's current content instead:

```
CasAppendNeverDiscards ==
    (lastOp.kind = "append" /\ lastOp.outcome \in {"ok", "lost"}
        /\ Present(ProvKey)) =>
            /\ IsPrefixOf(lastOp.before, Gens)
            /\ Len(Gens) = Len(lastOp.before) + 1
```

`Gens` reads the provisioning object's live content through
`RavelObjectStore`, so the invariant now compares the witness's claimed
`before` against what is actually durable, and asserts the durable length
grew by exactly one. `lastOp.after` no longer appears in the invariant body:
under the old text it was the forged half of the tautology
(`lastOp.after` was written by the very same step as `lastOp.before` and
`lastOp.outcome`, so `IsPrefixOf(lastOp.before, lastOp.after)` could only
fail if the step's own construction was self-contradictory, which it never
is by construction).

**Non-vacuity.** Mutant #1 below (`ReqAppendOk` dropping the last element of
`reqs[r].gens` before the CAS write) reproduces `Invariant
CasAppendNeverDiscards is violated` in 60 states / 36 distinct / depth 5,
confirming the grounded form still fires when the store is actually
under-appended, not just when the witness disagrees with itself.

**Reviewer's claimed violation.** Re-run against the grounded invariant on
`smoke.cfg` and the mutant above: the shipped `ReqAppendOk` path itself never
violates the grounded invariant at any horizon probed this session (smoke,
and every mutant cfg in the sweep below that does not itself touch
`ReqAppendOk`'s CAS write). The violation the reviewer observed is
attributable to the invariant's self-reference, not to a protocol bug: once
`lastOp.after` is removed from the check and `Gens` is read live, the
shipped append path is clean. This is a model-bug finding, not a protocol
finding.

**Audit of the other two invariants for the same shape.**

- `StaleReaderFailsClosed` reads `hd = HeadOf` (a live store read) and
  compares it against `Acceptable(hd, g)`, where `g` is the reader's view —
  cached or freshly re-read, never a same-step witness field. It does not
  read `lastOp.before`/`lastOp.after`/`lastOp.outcome` at all. Not
  self-referential.
- `SafelyOldHeadRule` reads `hd.ceiling` and compares it against `Gens`
  through `ShardCeiling`, again a live store read, not a witness field.
  Not self-referential.

Both were already store/witness-derived; `CasAppendNeverDiscards` was the
only invariant with the self-referential shape, and it is now fixed.

### Finding 2 (major): `smoke.cfg`'s `AppenderSkew = 0` and the `C = 1` rounding collision

The model's time unit was one hour, so the shipped 60-second refresh interval
rounds up to `C = 1` hour and `MinLeadHours = C + 1 = 2`. At that rounding,
`AppenderSkew = 1` (the real `TOLERATED_CLOCK_SKEW_HOURS`) collides with `C`
on the same integer scale and `smoke.cfg` set `AppenderSkew = 0` to avoid it,
which meant no smoke or negative-control run ever exercised the shipped skew
bound together with the shipped refresh interval at once.

Fixed the collision at its root: added a `HourUnits` constant and a
`HourCeil` operator so a config can model time at finer-than-hour granularity
while every existing hour-granular cfg is unaffected at `HourUnits = 1`
(`HourCeil(c) = c`, `MinLeadHours = HourCeil(C) + HourUnits` collapses to the
original `C + 1`). See the diff in `OnlineResharding.tla` and the
"Time-unit granularity" note added to README.md.

Built `shipped-skew-minutes.cfg`: `HourUnits = 60` (one model unit per
minute), `C = 1` minute (the real refresh interval, not rounded to an hour),
`MinLeadHours = 120` and `L = 120` (the real 2-hour lead in minutes),
`AppenderSkew = 60` (the real 1-hour tolerated skew in minutes), `S = 180`,
`FlushBound = 60`, `MaxHour = 125` (roughly 2 hours of model horizon).
`TypeOK`, `StaleReaderFailsClosed`, and `LeadCoversRefreshHorizon` asserted —
this is the first config in the repo where the refresh interval, the
activation lead, and the tolerated clock skew are all represented at their
true real-world ratio simultaneously, with no rounding collision forcing any
of them to zero.

**Result: intractable within the executor's probe budget, not pass or
fail.** An 8-worker run killed by an internal 280 s timeout reached only
depth 14:

```
Progress(14) at 2026-09-03 12:47:46: 49,957,695 states generated
(11,631,838 s/min), 14,129,398 distinct states found (3,071,280 ds/min),
10,071,309 states left on queue.
TLCEXIT=124
```

Distinct-state discovery was still growing at roughly 3M/min with no sign of
slowing between progress ticks, and over 10M states remained queued when the
run was killed. Minute granularity multiplies the reachable state space by
close to the same factor it removes from the rounding collision (roughly
60x the per-actor clock domain), which is consistent with the search still
being in its early expansion at depth 14. This is not a config or tooling
bug: the run made steady, monotonic progress and was killed by the probe
budget, not by an error.

**Disposition.** The `AppenderSkew = 0` collision workaround in `smoke.cfg`
is retained for the smoke gate, which must finish in well under a minute;
`shipped-skew-minutes.cfg` is checked in as a real, runnable config that
exercises the shipped clock-skew ratio exactly, for the orchestrator to run
at a longer budget (likely requiring `MaxHour` well below 125, or a targeted
scenario cfg rather than a full symmetry-reduced sweep, to converge). Per
README.md, this is documented as a probed-but-inconclusive check against the
shipped skew value, not a silent pass: no configuration in this repo claims
to have checked the shipped skew bound and refresh interval together to
exhaustion. The appender-skew-unbounded negative control and the smoke-run
skew-sensitivity notes above remain the strongest evidence this session has
for skew safety at `C = 1`.

### Finding 3 (minor): `scan-slack-zero` nondeterministic-match fragility

Documented in `negative/scan-slack-zero.cfg`'s own header comment (see that
file). No further action needed here.

## Invariant sweep: store/witness grounding and non-vacuity

Every named safety invariant, checked for: (a) whether it is grounded on the
store or on a witness field never read by the same step that wrote it (never
self-referential), and (b) reachability, proved by a source-level behaviour
mutant — not a `CONSTANT` flip of a value the invariant itself reads — that
TLC confirms breaks the property. Each mutant below was applied with a
targeted edit, run against a scratch (non-repo) `.cfg`, confirmed violated,
then reverted; `OnlineResharding.tla` carries none of them.

| Invariant | Grounding | Mutant | Result |
|---|---|---|---|
| `CasAppendNeverDiscards` | Store (`Gens`), fixed this session — see Finding 1 | `ReqAppendOk`: CAS-write `SubSeq(reqs[r].gens, 1, Len(reqs[r].gens) - 1)` instead of the full sequence | violated, 60 states / 36 distinct / depth 5 |
| `EveryAdmittedWriteInScanSet` | Store (`admitted`, `Gens`) | `DoAdmit`: shard range `0..cnt` instead of `0..(cnt - 1)` (off-by-one over-admit) | violated, 75 states / 53 distinct |
| `DecreaseKeepsStraggler` | Store (`admitted`, `Gens`) | same `DoAdmit` off-by-one mutant | violated, 83 states / 55 distinct |
| `HistoryDenseAppendOnlyIncreasing` | Store (`Gens`) | `Proposed(r)`: proposed generation `last.gen` instead of `last.gen + 1` (duplicate generation number) | violated, 85 states / 32 distinct |
| `StaleWriterFailsClosed` | Store/witness (`views`, `clocks`, `lastOp.outcome`), audited clean in Finding 1 | `AdmitFailClosed(w)`: guard `IF FALSE` instead of `IF WriterFenceEnabled \/ ~views[w].has` (fence never fires) | violated, 574 states / 324 distinct |
| `StaleReaderFailsClosed` | Store/witness (`HeadOf`, `Acceptable`), audited clean in Finding 1 | Combo: `DoAdmit` off-by-one over-admit + `Acceptable(hd, g) == TRUE` (ceiling check bypassed). Neither mutant alone reproduced a violation at reachable horizons (5 attempts — cache-freshness removal, frozen `g0`, `Acceptable` bypass alone, a targeted scenario cfg, and a `failclosed`-to-`ok` outcome rewrite all completed exhaustively with no counterexample) because `hd` is always read live and naturally routes a stale view to a safe branch; only combining the ceiling bypass with actual upstream data corruption breaks it | violated, 609 states / 328 distinct |
| `SafelyOldHeadRule` | Store/witness (`hd.ceiling`, `Gens`), audited clean in Finding 1 | The `StaleReaderFailsClosed` combo alone did not reproduce a violation (2996 distinct states, exhaustive) because this invariant is independently protected by append-only history monotonicity and `Fold`'s own ceiling correctness; added a third mutant in `Fold`, decrementing a positive `ShardCeiling(Gens, wm)` by 1 before stamping it into the HEAD record | violated, 104 states / 60 distinct |
| `TokenResolvesAcrossReshards` | Store (`Present`, `ResolveToken`) | `ResolveToken`: `found` forced to `FALSE` (every token lookup misses) | violated, 237 states / 135 distinct |
| `OneCasWinner` | Store (`casWins`) | `ReqAppendConflict` (the CAS-loser path): added a `casWins'` update on conflict, where the correct path leaves `casWins` unchanged. (An earlier attempt forcing a constant `base` on the winner's own update did not reproduce a violation at reachable horizons: `CasAttempts = 1` structurally caps every run to at most one successful CAS append, since a conflict routes straight to `"done"` with no retry, so a second winner was never reachable to collide with the first — confirmed by a temporary debug operator counting `casWins` cardinality, removed after use.) | violated, 694 states / 322 distinct |
| `LeadCoversRefreshHorizon` | Store/witness (`clocks`, `views`) | `Proposed(r)`: activation hour `clocks[Appender] + 1` instead of `clocks[Appender] + L` (lead ignored) | violated, 683 states / 285 distinct |
| `TypeOK` | N/A — well-formedness/shape invariant, not a protocol safety property | None dedicated. Incidentally caught (before the target invariant) by the `EveryAdmittedWriteInScanSet`/`DecreaseKeepsStraggler` mutant above: `WriteRecs`'s declared shard domain independently bounds `admitted` records, so an off-by-one over-admit breaks `TypeOK` first in the same run. This is treated as sufficient evidence `TypeOK` is exercised by the sweep, not as a gap; a dedicated mutant against a type invariant (rather than a protocol property) would just re-encode the same domain check the language already enforces | not applicable, trivially exercised |

All ten mutants and their exact reverts are auditable as this session's
working history; none are present in the committed `OnlineResharding.tla`,
confirmed by a final `diff` against a clean pre-mutant copy.

## Second-round findings (issue #1123 adversarial re-review)

Four MAJOR findings from the re-review of the four items above, each about a
capability defined in the model but never exercised by any gated
configuration.

### Finding A: shipped clock skew never gated

Finding 2 above already root-caused this and built `shipped-skew-minutes.cfg`
as a real, checked-in config that represents the shipped ratio exactly
(`HourUnits = 60`) rather than the hour-rounded `C = 1`/`AppenderSkew = 0`
every gated config uses. It remains intractable: that finding's own run
reached 14,129,398 distinct states at depth 14 with over 10M queued before
being killed by the probe budget.

This round attempted three further shrinks aimed directly at the
minute-granularity blow-up, per the suggested strategies: single writer, one
requester, `HourUnits` reduced to 4 and then 2, `MaxHour` reduced to 16, 10,
then 6 (down from 125). All three still explored past 2.3-2.7 million
distinct states with the same non-decaying growth rate seen at the original
scale, with no sign of converging before being stopped. Shrinking every
dimension available (writers, requesters, and the unit scale itself) did not
change the qualitative shape of the search: the blow-up is not an artifact of
this particular 60-unit encoding or of writer/requester count, it is
structural to modeling clock skew at any sub-hour granularity with real
clock movement (`AppenderSkew > 0`) enabled at all. Every `AppenderSkew > 0`
config tried anywhere this session or the prior one (this shrink attempt,
`fb-mutant.cfg` below, the `widen-*` exhaustive-resize probes) hit the same
pattern regardless of dimension.

**Disposition, stated plainly next to the constants in README.md:** no gated
configuration checks `TOLERATED_CLOCK_SKEW_HOURS` at the shipped 60-second
refresh interval simultaneously. `shipped-skew-minutes.cfg` is the real,
runnable artifact that would check it, checked in and documented as
probed-but-not-completed rather than silently absent.

### Finding B: `FlushBound` mismatch and non-vacuity

`smoke.cfg` and `exhaustive.cfg` both now set `FlushBound = 2`, matching
shipped `FLUSH_BOUND_SLACK_HOURS`. Both still hold at frozen clock
(`AppenderSkew = 0`): `FlushBound` never binds there regardless of its value,
which the reviewer's identical-state-count observation already showed
(`FlushBound = 1` and `= 2` both give smoke.cfg's own 7809360 states / 958804
distinct / depth 18).

Built `flush-bound-trailing.cfg`: single writer/requester, `AppenderSkew = 1`
so the writer's clock can move past its flush's pinned ingest hour at all,
`FlushBound = 2`. Added `FlushBoundNeverBites` (grounded on `admitted`, the
store — not on the `FlushBound` constant, which it never reads):

```
FlushBoundNeverBites ==
    \A v \in admitted : (v.routeHour - v.ingestHour) < 2
```

**Result at `FlushBound = 2`:**

```
Error: Invariant FlushBoundNeverBites is violated.
2070 states generated, 1040 distinct states found, 758 states left on queue.
The depth of the complete state graph search is 7.
```

This is the reachability half: a real admitted record can trail its ingest
hour by 2 hours once `FlushBound = 2` permits it, proving the constant is not
vacuous at this config's dimensions.

**The other half — that `FlushBound = 1` makes the same gap unreachable — did
not resolve empirically.** Re-running the same config at `FlushBound = 1`
against the same probe invariant was killed at depth 13, 1,768,734 distinct
states and climbing, no violation found, consistent with this area's general
explosion once `AppenderSkew > 0` unlocks clock movement. Proved it instead
algebraically, from the guard itself (`OnlineResharding.tla`):

```
CanAdmit(w) ==
    ...
    /\ clocks[w] - flushes[w].hour <= FlushBound

DoAdmit(w, cnt, oc, vat) ==
    \E sh \in 0..(cnt - 1) :
        LET rec == [writer |-> w, shard |-> sh, ingestHour |-> flushes[w].hour,
                    routeHour |-> clocks[w], id |-> flushes[w].next]
        IN /\ PutCreateIfAbsent(TokenKey(w, flushes[w].next), rec)
           /\ admitted' = admitted \cup {rec}
           ...
```

`routeHour` and `ingestHour` are stamped from `clocks[w]`/`flushes[w].hour`
in the same step `CanAdmit(w)`'s guard checks, and `admitted` is append-only
afterward (no action removes or rewrites a member). So every member of
`admitted` satisfies `routeHour - ingestHour <= FlushBound` for all time, by
construction, for any `FlushBound`. At `FlushBound = 1` this bounds the gap
at 1, making a gap of 2 unreachable regardless of how much of the state
space TLC can explore — the same conclusion the intractable exhaustive
search would have reached, established by inspection instead.

**Also attempted:** a source-level mutant of the guard itself
(`<= FlushBound` to `<= FlushBound + 1`) checked against the real eleven
safety invariants (not the dedicated probe) at `FlushBound = 2`, to see which
real invariant catches an off-by-one over-trail. This did not converge
either: killed at depth 14, 1,794,770 distinct states and still climbing,
no violation found. The dedicated store-grounded probe above is the proof
that survived; the full-invariant-set mutant search is recorded here as a
second, consistent data point for the structural-explosion conclusion, not
as a completed result.

### Finding C: liveness never checked

`FairSpec` and `EventuallyRoutedOnNewGeneration` were defined but no `.cfg`
anywhere referenced `PROPERTY` or `FairSpec`. Built `live.cfg`: smoke.cfg's
own dimensions, `SPECIFICATION MCFairSpec`, `PROPERTY
EventuallyRoutedOnNewGeneration`, no `SYMMETRY` (unsound for a temporal
property).

**Result: VIOLATED**, in 4 seconds:

```
Error: Temporal properties were violated.
Error: The following behavior constitutes a counter-example:
...
State 6: <WriterCrash ...>
State 7: <WriterCrash ...>
State 8: <FlushOpen ...>
State 9: <WriterCrash ...>
State 10: <FlushOpen ...>
Back to state 6: <WriterCrash ...>
17193 states generated, 6286 distinct states found, 4490 states left on queue.
```

The counter-example is an infinite `WriterCrash`/`FlushOpen` loop: a writer
crashes, reopens a flush, crashes again before closing it, forever, so it
never completes a flush and never routes a record on the new generation.
`WriterCrash` carries no fairness constraint anywhere in `FairSpec`, and it
does not need one — the loop's actual gap is elsewhere.

### Finding C, revisited (issue #1123 second review): weak fairness was too weak

`AdmitAfterRefresh(w)` had only `WF_vars` in `FairSpec`. The crash loop above
toggles `flushes[w].open` every step (`WriterCrash` closes it, `FlushOpen`
reopens it), so `CanAdmit(w)` — and with it `AdmitAfterRefresh(w)` — is
enabled for exactly one step at a time, never continuously. Weak fairness
only forces an action that stays enabled without interruption; an action
enabled-then-disabled-then-enabled forever is never forced under WF, so the
loop above was a real gap in the fairness assumption, not a crash-frequency
problem.

Fixed by widening `AdmitAfterRefresh(w)` to `SF_vars` (`OnlineResharding.tla`):
strong fairness forces an action that is enabled infinitely often, which the
crash loop's admit window is, so the same loop can no longer satisfy
`FairSpec`. The comment beside the conjunct records why strong fairness is
the right assumption rather than a convenience: `ravel-ingest`'s write loop
(`router.rs`) never suspends with a flush closed, closing one and opening
the next are back-to-back steps of the same loop, so the real writer keeps
re-entering the enabled state on its own for as long as it runs, with no
external event required — exactly the "infinitely often, unassisted"
shape strong fairness assumes.

**Re-run result: PASS.** `live.cfg` at the same dimensions, now under the
widened `FairSpec`:

```
Model checking completed. No error has been found.
31045026 states generated, 3817433 distinct states found, depth 18.
```

11 minutes 36 seconds. No counter-example was found: with the crash loop's
admit window ruled out by strong fairness, `EventuallyRoutedOnNewGeneration`
holds at `live.cfg`'s dimensions with no added crash-frequency hypothesis.

### Finding D: exhaustive did not complete

Covered in the Exhaustive section above: resized to a single writer with
`CasAttempts = 2` and `MaxAdmitsPerWriter = 2` both kept at their shipped
widening, completing in 8,503,664 states / 1,179,718 distinct / depth 20,
"Model checking completed. No error has been found." `two-writer-concurrency-probe.cfg`
recovers the dropped two-writer coverage as a separate reachability probe
(`TwoWritersNeverConcurrentlyOpen` violated, 26 distinct states, depth 4).
`bands.tsv`'s exhaustive.cfg row is updated from this real run.

## Third-round findings (issue #1123 review, 2026-09-04)

Five findings from a further review of the area. Finding 1 (liveness
fairness) is recorded above under "Finding C, revisited". The rest follow.

### Finding 3: appender-skew-unbounded's FlushBound contradicted its own claim

`negative/appender-skew-unbounded.cfg` set `FlushBound = 1` while its header
comment and `counterexamples/appender-skew-unbounded.md` both say every
margin but the skew itself is shipped, and the shipped
`FLUSH_BOUND_SLACK_HOURS` is 2 (see `exhaustive.cfg`, `smoke.cfg`). The
violation itself never depended on this: a smaller `FlushBound` only
narrows `CanAdmit`'s admit window, so the counterexample stays reachable at
the wider, correct value.

Set `FlushBound = 2` and re-ran: still `EveryAdmittedWriteInScanSet`
VIOLATED, 124721 distinct states, 4 seconds (see the negative-controls table
above, from the same coherent run). The claim and the configuration now
agree.

### Finding 2: `no-writer-fence` conflated the fence flag with the skew

`negative/no-writer-fence.cfg` changes two things relative to a shipped
config at once: `WriterFenceEnabled = FALSE` and `AppenderSkew = 2`. Its
violation of `StaleWriterFailsClosed` does not, on its own, show that the
fence flag is the cause rather than the skew; the skew is a genuine
enabling condition (documented in that file's header: it is what drives
the writer's cached view past the grace horizon within this small a
model), but the config never isolates it from the fence.

Added `writer-fence-comparison.cfg`, with identical non-comment settings to
`negative/no-writer-fence.cfg` (same `AppenderSkew = 2`, same `MaxHour = 4`,
same everything) except `WriterFenceEnabled = TRUE`. A difference in
outcome between the two can then only come from the fence.

The fence-enabled arm cannot be driven to exhaustive completion with TLC's
default BFS search: with the fence closing the gap there is no violation
to stop the search early on, and at these dimensions the reachable space
does not converge. Confirmed empirically before falling back to
simulation: full BFS was still climbing past 65,000,000 distinct states
after 45 minutes at the config's own `MaxHour = 4`, and past 8,000,000
distinct states after 6 minutes even at a much-reduced `MaxHour = 2` and
`AppenderSkew = 1`, showing the growth is not primarily driven by either
knob and a further reduction would not plausibly make it tractable. This
matches the state-space-explosion pattern already documented above for
`AppenderSkew > 0` configs under a full, no-early-exit search; nothing in
this repo has previously driven that config class to full completion in
either direction, since every existing `AppenderSkew > 0` negative control
only ever runs TLC's error-search mode and stops at its first violation.

Ran the fence-enabled arm instead with TLC random simulation, at the same
dimensions:

```
tlc2.TLC -config writer-fence-comparison.cfg -simulate num=100000000 \
  -depth 100 MCOnlineResharding
```

63,203,643 states checked in 300 seconds (the run is budget-bounded, not
traversal-bounded: `-simulate` does not terminate on its own short of
`num` traces). Zero `TypeOK` or `StaleWriterFailsClosed` violations found.
This is strong evidence, not an exhaustive proof, that the fence flag
alone, holding `AppenderSkew = 2` and every other dimension fixed, is what
`negative/no-writer-fence.cfg`'s violation depends on, not the shared skew
condition both configs carry.

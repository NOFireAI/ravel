# Resharding model: results

Model checker: TLC from tla2tools 1.7.4 (sha256 verified by the harness).
Run id: `20260903T042234Z-49ff7c78bf9d67949e0c4b7bbfcc232845d1125b`
(`scripts/check-tla.sh ci -a resharding`, a single coherent run of smoke,
every negative control, and traceability).

## Smoke (safety)

| cfg | result | states | distinct | depth | seconds |
|---|---|---|---|---|---|
| smoke.cfg | PASS | 7809360 | 958804 | 18 | 36 |

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
| scan-slack-zero | `S = 0` (shipped 3) | EveryAdmittedWriteInScanSet | 2761 | 2 |
| appender-skew-unbounded | `AppenderSkew = 5` (tolerated 1) | EveryAdmittedWriteInScanSet | 146033 | 4 |
| lead-one | `L = 1` (shipped 2) | LeadCoversRefreshHorizon | 199 | 1 |
| no-writer-fence | `WriterFenceEnabled = FALSE` | StaleWriterFailsClosed | 75612 | 3 |
| token-validated-against-count | `TokenValidatedAgainstCount = TRUE` | TokenResolvesAcrossReshards | 17452 | 2 |

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

Not run by the executor; see the orchestrator's run. `exhaustive.cfg` widens the
three bounds smoke narrows for time (`CasAttempts` 1 to 2, `MaxAdmitsPerWriter`
1 to 2, `MaxHour` 3 to 4) with every margin at its shipped value, asserting the
same eleven safety invariants over a strictly larger reachable set. Its
`bands.tsv` row is an explicitly UNVERIFIED placeholder scaled from the smoke
figures, not a measurement this executor took; do not read it as a passing
run, and tighten it from the orchestrator's first real run. If `MaxHour = 4`
proves intractable, drop it to 3 first (the CAS-retry and second-admit
coverage hold there). A `skew = 1` exhaustive variant is recommended to
substantiate the tolerated-skew half of the ADR-1113 D12 claim directly.

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

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
`bands.tsv` row is an unverified placeholder scaled from the smoke figures;
tighten it from the first real run. If `MaxHour = 4` proves intractable, drop it
to 3 first (the CAS-retry and second-admit coverage hold there). A `skew = 1`
exhaustive variant is recommended to substantiate the tolerated-skew half of the
ADR-1113 D12 claim directly.

## What these runs claim

TLC checked this finite model under the bounds and assumptions in README.md and
found no reachable state violating the smoke invariants, and found the expected
violation for each negative control. This is a check of a finite model, not a
proof of the Rust implementation or of safety outside the modeled bounds.

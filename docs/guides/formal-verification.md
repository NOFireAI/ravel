# Formal verification with TLA+

TLA+ models check Ravel's coordination protocols. A harness under
`formal/tla/` runs the models and gates them.

## What the suite is

The suite holds five models of Ravel's coordination protocols. Each model
instantiates one shared object-store module,
[common/RavelObjectStore.tla](../../formal/tla/common/RavelObjectStore.tla),
and does not model storage semantics again.

- The [commit](../../formal/tla/commit/README.md) model checks flush
  publication and acknowledgement, including retry and read-your-write.
- The [catalog](../../formal/tla/catalog/README.md) model checks the fold,
  snapshots, compaction, and MVCC.
- The [lifecycle](../../formal/tla/lifecycle/README.md) model checks
  retention, erasure, legal holds, and garbage collection.
- The [resharding](../../formal/tla/resharding/README.md) model checks
  generation-versioned online resharding.
- The [maintenance](../../formal/tla/maintenance/README.md) model checks
  maintenance ownership and a proposed design for advisory compaction
  claims.

The [consistency model](../consistency-model.md) states the promises that
these protocols keep in production.

TLC checked each finite model under the bounds and assumptions recorded in
its own `results.md` and configuration files. The models verify the protocol
design. They do not prove that the implementation conforms: see
[What it does not establish](#what-it-does-not-establish).

## What TLC checked

Each row of the table is one exhaustive configuration: its area, its
specification module, the host that measured it, its distinct-state count,
and its wall time. Every exhaustive configuration runs under a wall-clock
ceiling of 3600 seconds by default. A configuration can set a different
ceiling: see [Per-configuration budgets](#per-configuration-budgets).

| Area | Specification | Host | Distinct states | Wall time |
|---|---|---|---|---|
| common | `RavelObjectStore.tla` | fleet executor | 3845952 | 252 seconds |
| commit | `CommitProtocol.tla` | fleet executor | 5466239 | 131 seconds |
| catalog | `CatalogMVCC.tla` | fleet executor | 3422524 | 510 seconds |
| lifecycle | `LifecycleGC.tla` | GitHub hosted ubuntu-24.04, workers auto, Xmx2g | 2835448 | 1048 seconds |
| resharding | `OnlineResharding.tla` | fleet executor | 1179718 | under 300 seconds |
| maintenance | `MaintenanceOwnership.tla` | fleet executor; GitHub hosted ubuntu-24.04, workers auto, Xmx2g | 13183990 | 1769 seconds; 4285 seconds on the hosted runner, under its 5400 s budget |
| maintenance | `MaintenanceOwnership.tla` | GitHub hosted ubuntu-24.04, workers auto, Xmx2g | 12448134 (TIMEOUT at 3600 s, 1,450,354 states still queued; projected 4,000 to 4,200 s to finish) | 3600 seconds (killed) |
| maintenance | `CompactionClaims.tla` | fleet executor | 543 | 2 seconds |

The fleet executor and the GitHub-hosted runner are different machines. The
fleet executor measured the 1769-second `MaintenanceOwnership.tla` figure.
The hosted-runner row next to it is the nightly lane's own runner. That
runner timed out on the same configuration before it finished, roughly 2.3x
slower per distinct state. Because of that gap, this one configuration
carries a budget override. See
[`formal/tla/maintenance/results.md`](../../formal/tla/maintenance/results.md)
for the full measurement that this projection rests on.

### Per-configuration budgets

The optional sixth column of `bands.tsv`, `budget_s`, overrides the
3600-second default for one exhaustive config on the lane that reads it.
`check_one_model` resolves it by cfg name, the same way it resolves the
distinct/depth band. A row sets the column when one config on a given runner
measurably needs more than the rest.

`MCMaintenanceOwnership.exhaustive.cfg` sets `budget_s = 5400` in
`formal/tla/maintenance/bands.tsv`. That is about 30% headroom over the
4,000 to 4,200 s hosted-runner projection. Every other exhaustive config in
the suite keeps the 3600 s default.

The nightly workflow (`.github/workflows/tla-nightly.yml`) runs the six areas
as a matrix, one job per area. So the larger budget of one area extends only
the job of that area.

### Negative configurations

Each area splits its negative configurations into two kinds:

- A control is a variant of the correct model that is broken on purpose. When
  TLC checks that variant, it must report a violation.
- An obligation is a predicate that must fail. Its failure proves that the
  model can reach a state that the protocol does not forbid.

Catalog carries the largest share of obligations. Seven of its twenty-one
negative configurations are reachability probes, and the other fourteen are
broken-behavior controls.

## What it does not establish

The suite checks finite models. It does not check the Rust implementation,
and it does not prove that the implementation conforms. The traceability
tables argue conformance: they tie each checked property to a Rust path and,
for most rows, a named regression test. Rust tests assert conformance where
a test exists. The traceability index records the rows that still lack one.

The suite states these assumptions and does not check them:

- The lifecycle model assumes that raw-input content never changes after it
  is written.
- The commit model assumes that the data-object publish is idempotent under
  retry.
- The maintenance model assumes that the segment and part encoder, the hash
  function, and the merge preserve their inputs.
- The catalog model assumes that the object store meets its own contract.
  Every other area outside common makes the same assumption.

These cases are outside the scope of the suite:

- The lifecycle model cannot reach a rewrite-of-rewrite predecessor. No
  shipped action produces a second rewrite object for a further rewrite to
  consume.
- The maintenance model checks the two-worker ownership race for safety only.
  No lane in the suite checks liveness at two workers.
- The exhaustive configuration of the commit model checks safety only. The
  `live` lane checks its liveness property at smaller bounds. The exhaustive
  configuration with that property did not finish inside the time budget of
  its lane.

These items are open or have no result:

- One lifecycle case from an earlier retention decision stays open on the
  shipped retention path.
- The proposed compaction-claims design sits on a claim primitive that
  nothing in the repository calls yet.
- The overlap configuration of catalog does not finish inside its time
  budget. It runs as a targeted check, with no gated pass or fail.
- The skew configuration of resharding was killed at an internal timeout
  with ten million states still queued. The suite records no result for it.

Two liveness results hold only under stated conditions. The
`EventuallySwept` and `EventuallyCompleted` properties of the lifecycle model
pass when hold state, HEAD read state, and refresh outcome all eventually
stop changing. They also require the retention windows of the fold and of
the sweep to agree. A permanently wedged hold or a disagreeing window makes
both properties false. That result is by design and is not a defect.

Three follow-ups stay open:

- Issue 1221 tracks the unreached rewrite-of-rewrite case.
- Issue 1243 tracks extending the traceability checker to accept more than
  one Rust reference per row.
- Issue 1244 tracks two wording fixes to the suite report.

## Run the suite

Install a Java 17 or later runtime before you run the smoke, negative, or
exhaustive lane. Install GNU timeout before you run those lanes. On macOS,
run `brew install coreutils` to get it. The traceability lane runs without
Java or GNU timeout.

Each lane is one command:

- `scripts/check-tla.sh smoke` checks safety in every area, at a 300-second
  budget per configuration.
- `scripts/check-tla.sh live` checks liveness under fairness, at the same
  300-second budget. It runs in each area whose `live.cfg` has a matching row
  in that area's `bands.tsv`, so a measured band is the opt-in. The lane
  reports an area with a `live.cfg` and no band as SKIP, and that area does
  not fail the lane.
- `scripts/check-tla.sh negative` checks that every negative control fails
  the way its `.expect` file states.
- `scripts/check-tla.sh traceability` checks that every Rust path and symbol
  in every traceability table exists.
- `scripts/check-tla.sh exhaustive` checks full safety and liveness where a
  configuration states a `PROPERTY`, under the
  [budget](#per-configuration-budgets) of each configuration.

Add `-a <area>` to any of these commands to scope the check to one area.

Two commands combine lanes under one run ID:

- `scripts/check-tla.sh ci` runs smoke, live, negative, and traceability.
- `scripts/check-tla.sh all` runs `ci`, then `exhaustive`.

The pull-request job runs the `ci` kind. The nightly job runs exhaustive.

Each command exits with one of these codes:

- 0 on a pass.
- 1 when a check fails.
- 2 when no usable Java or GNU timeout exists. The harness prints the
  reason.

A single TLC run reports exit 12 for a safety violation and exit 13 for a
liveness violation. GNU timeout kills a run past its budget and reports exit
124.

## Read the results

Each area records its own figures in `results.md`: states generated,
distinct states, search depth, wall time, and the result. Where the area sets
one, `bands.tsv` records the distinct-state and depth range that a passing
configuration must land in. The harness fails a run outside its band. Such a
run is a regression, and a wider band is not the correction.

Each `counterexamples/` note records the exact `Invariant <name> is
violated` line that TLC printed, next to the property it names. A mutant is
the correct model with one behavior broken on purpose, by a one-line edit and
with no switch. A mutant runs once to show that the invariant it protects can
fail. Then the edit is reverted.

A regression test can claim that it reproduced a counterexample before the
fix. That claim is in the commit message of the test and is not in the files
of the suite.

## Trace properties to code

Each area keeps a `traceability.md` table, and
[TRACEABILITY.md](../../formal/tla/TRACEABILITY.md) indexes all six. Every
row names a TLA+ action or property, its meaning, one Rust path and symbol,
an existing test, and any test still needed. The traceability lane checks
that every named path and symbol resolves in the real source tree.

Four rows across the suite still have no test:

- One row is in lifecycle. The pair `CompleteErasure` and
  `CompletionImpliesNoPreRewriteExposure` names a gate that the code computes
  and that a production symbol writes the completion object for. No test
  drives that write through the served-set branch end-to-end.
- Three rows are in maintenance, inside the proposed compaction-claims
  design. One test pins `GuardedPublish`, `AbandonPublish`, and
  `LostClaimNeverPublishesThroughGuardedPath` at the claim primitive. No code
  outside that primitive calls it yet. A production test for these three
  follows the first shipped caller.

## Add or change a model

1. Write each invariant to observe the store, or a witness of what the store
   returned. Never write an invariant that reads bookkeeping that the action
   itself sets.
2. Add a negative control, with an `.expect` file, for every behavior that
   the property forbids.
3. Prove with a mutant that each control is not vacuous. Record the exact
   TLC violation line of the mutant in a counterexample note.
4. Keep every gated configuration inside its time budget. Record its
   distinct-state count and wall time in `results.md` and `bands.tsv`, in the
   same commit as the model.
5. Add one traceability row for the property, naming one Rust path and
   symbol. Never write a line number in a markdown file.
6. Run the smoke, live, negative, traceability, and exhaustive lanes before
   you commit.

See the [suite README](../../formal/tla/README.md) for the file layout.

## Background

The suite's scope and layout are
[ADR-1113](../adrs/1113-tla-verification-suite.md), decisions D1 and D5.
Its claim language is decision D12. Its negative-control convention is
decision D6, and its traceability convention is decision D8. The suite-wide
figures in this guide come from [REPORT.md](../../formal/tla/REPORT.md).

# Formal verification with TLA+

TLA+ models check Ravel's coordination protocols, and a harness under
`formal/tla/` runs and gates them. This guide states what the suite
checks and what it does not. It also states how to run it and how to
trace a property to the code.

## What the suite is

The suite holds five models of Ravel's coordination protocols, each built
on one shared object-store module,
[common/RavelObjectStore.tla](../../formal/tla/common/RavelObjectStore.tla).
Every model instantiates that module instead of remodeling storage
semantics.

The [commit](../../formal/tla/commit/README.md) model checks flush
publication and acknowledgement, including retry and read-your-write. The
[catalog](../../formal/tla/catalog/README.md) model checks the fold,
snapshots, compaction, and MVCC. The
[lifecycle](../../formal/tla/lifecycle/README.md) model checks retention,
erasure, legal holds, and garbage collection. The
[resharding](../../formal/tla/resharding/README.md) model checks
generation-versioned online resharding. The
[maintenance](../../formal/tla/maintenance/README.md) model checks
maintenance ownership and a proposed design for advisory compaction
claims. The [consistency model](../consistency-model.md) states the
promises these protocols keep in production.

TLC checked each finite model under the bounds and assumptions recorded
in its own `results.md` and configuration files. This model verifies the
protocol design. Implementation conformance is argued in the
traceability tables and asserted by Rust tests where a test exists, not
proved. The traceability index records the rows still without one.

## What TLC checked

Every exhaustive configuration runs under a per-configuration wall-clock
ceiling: 3600 seconds by default, or the value the config's `bands.tsv` row
sets in its optional `budget_s` column when one config on a given runner
measurably needs more than the rest (see "Per-configuration budgets"
below). The table below names each area, its specification module, the host
the figures were measured on, and the exhaustive configuration's
distinct-state count and wall time.

| Area | Specification | Host | Distinct states | Wall time |
|---|---|---|---|---|
| common | `RavelObjectStore.tla` | fleet executor | 3845952 | 252 seconds |
| commit | `CommitProtocol.tla` | fleet executor | 5466239 | 131 seconds |
| catalog | `CatalogMVCC.tla` | fleet executor | 3422524 | 510 seconds |
| lifecycle | `LifecycleGC.tla` | fleet executor | 230815 | 30 seconds |
| resharding | `OnlineResharding.tla` | fleet executor | 1179718 | under 300 seconds |
| maintenance | `MaintenanceOwnership.tla` | fleet executor | 13183990 | 1769 seconds |
| maintenance | `MaintenanceOwnership.tla` | GitHub hosted ubuntu-24.04, workers auto, Xmx2g | 12448134 (TIMEOUT at 3600 s, 1,450,354 states still queued; projected 4,000 to 4,200 s to finish) | 3600 seconds (killed) |
| maintenance | `CompactionClaims.tla` | fleet executor | 543 | 2 seconds |

The fleet executor and the GitHub-hosted runner are different machines: the
1769-second `MaintenanceOwnership.tla` figure is what the fleet executor
measured, and the hosted-runner row next to it is the nightly lane's own
runner timing out on the same configuration before finishing, roughly 2.3x
slower per distinct state. That gap is why this one configuration carries a
per-configuration budget override rather than the default. See
[`formal/tla/maintenance/results.md`](../../formal/tla/maintenance/results.md)
for the full measurement this projection rests on.

### Per-configuration budgets

`bands.tsv`'s optional sixth column, `budget_s`, overrides the 3600-second
default for one exhaustive config on the lane that reads it (`check_one_model`
resolves it the same way it resolves the distinct/depth band, by cfg name).
`MCMaintenanceOwnership.exhaustive.cfg` sets `budget_s = 5400` in
`formal/tla/maintenance/bands.tsv`, about 30% headroom over the 4,000 to
4,200 s hosted-runner projection above; every other exhaustive config in the
suite keeps the 3600 s default. The nightly workflow
(`.github/workflows/tla-nightly.yml`) runs the six areas as a matrix, one job
per area, instead of one job running all six in sequence: a single area's
larger budget then only extends that area's own job, not a shared job
covering every area.

Each area splits its negative configurations into two kinds. A control
is a deliberately broken variant of the correct model. When TLC checks
that variant, it must report a violation. An obligation is a
predicate that must fail, and its failure proves the model can reach a
state the protocol does not forbid. Catalog carries the largest share of
obligations: seven of its twenty-one negative configurations are
reachability probes, and the other fourteen are broken-behavior
controls.

## What it does not establish

The suite checks finite models, not the Rust implementation. The
traceability tables tie each checked property to a Rust path and, for
most rows, a named regression test. The index records the rows that
still lack one. That link, not the model, ties the suite to the code.

Several facts are assumptions the suite states but does not check. The
lifecycle model assumes raw-input content never changes after it is
written. The commit model assumes the data-object publish is idempotent
under retry. The maintenance model assumes the segment and part encoder,
the hash function, and the merge preserve their inputs. The catalog
model assumes the object store meets its own contract, the same
assumption every other area outside common makes.

Several cases sit outside the suite's scope. The lifecycle model cannot
reach a rewrite-of-rewrite predecessor. No shipped action produces a
second rewrite object for a further rewrite to consume, so the model
cannot reach that case. Issue 1221 tracks this gap. The maintenance
model checks the two-worker ownership race for safety only. No lane in
the suite checks liveness at two workers. The commit model's exhaustive
configuration checks safety only. Its liveness property is checked at
the `live` lane's smaller bounds instead, since adding it to the
exhaustive configuration did not finish inside that lane's time budget.

One lifecycle case from an earlier retention decision stays open on the
shipped retention path. The proposed compaction-claims design sits on a
claim primitive that nothing in the repository calls yet. Catalog's
overlap configuration does not finish inside its time budget. It runs as
a targeted check, not a gated pass or fail lane. Resharding's skew
configuration was killed at an internal timeout with ten million states
still queued, and the suite records no result for it.

Two liveness results hold only under stated conditions. When hold state,
HEAD read state, and refresh outcome all eventually stop changing, the
lifecycle model's `EventuallySwept` and `EventuallyCompleted` properties
pass. They also require the fold's and the sweep's retention windows to
agree. A permanently wedged hold or a disagreeing window makes both
properties false by design, not by defect.

Three follow-ups stay open. Issue 1221 tracks the unreached
rewrite-of-rewrite case above. Issue 1243 tracks extending the
traceability checker to accept more than one Rust reference per row.
Issue 1244 tracks two wording fixes to the suite report.

## Run the suite

Install a Java 17 or later runtime before you run the smoke, negative, or
exhaustive lane. Install GNU timeout before you run those lanes. On
macOS, run `brew install coreutils` to get it. If Java or GNU timeout is
missing, the harness exits with code 2 and prints the reason. The
traceability lane runs without Java or GNU timeout.

Run `scripts/check-tla.sh smoke` to check safety in every area, at a
300-second budget per configuration. Run `scripts/check-tla.sh live` to
check liveness under fairness, at the same 300-second budget, in each
area whose `live.cfg` has a matching row in that area's `bands.tsv` (a
measured band is the opt-in); an area with a `live.cfg` and no band is
reported SKIP and does not fail the lane. Run `scripts/check-tla.sh
negative` to check that every negative control fails the way its
`.expect` file states. Run `scripts/check-tla.sh traceability` to check
that every Rust path and symbol in every traceability table exists. Run
`scripts/check-tla.sh exhaustive` to check full safety and liveness
where a configuration states a `PROPERTY`, at a 3600-second budget per
configuration. Add `-a <area>` to any of these commands to scope the
check to one area.

Run `scripts/check-tla.sh ci` to run smoke, live, negative, and
traceability under one run ID. Run `scripts/check-tla.sh all` to run
`ci`, then `exhaustive`, under one run ID.

Each command exits 0 on a pass. When a check fails, it exits 1. When no
usable Java or GNU timeout exists, it exits 2. A single TLC run reports
exit 12 for a safety violation. It reports exit 13 for a liveness
violation. GNU timeout kills a run past its budget and reports exit
124.

The pull-request job runs the `ci` kind: smoke, negative, live (for the
areas that have banded a `live.cfg`), and traceability. The nightly job
runs exhaustive.

## Read the results

Each area records its own figures in `results.md`: states generated,
distinct states, search depth, wall time, and the result. `bands.tsv`
records the distinct-state and depth range a passing configuration must
land in, where the area sets one. The harness fails a run outside its
band, since a run outside its band is a regression, not a wider band to
absorb it.

Each `counterexamples/` note records the exact `Invariant <name> is
violated` line TLC printed, next to the property it names. A mutant is
the correct model with one behavior broken on purpose, by a one-line
edit rather than a switch. A mutant runs once to show the invariant it
protects can fail, then the edit is reverted. A regression test's claim
that it reproduced a counterexample before the fix lives in that test's
own commit message, not in the suite's files.

## Trace a property to the code

Each area keeps a `traceability.md` table, and
[TRACEABILITY.md](../../formal/tla/TRACEABILITY.md) indexes all six.
Every row names a TLA+ action or property, its meaning, one Rust path
and symbol, an existing test, and any test still needed. The
traceability lane checks that every named path and symbol resolves in
the real source tree, and it needs no Java runtime.

Five rows across the suite still have no test. Two are in lifecycle.
`RequestErasure` has no production code that writes the erasure request
marker. Only a key builder and tests exist. The pair `CompleteErasure`
and `CompletionImpliesNoPreRewriteExposure` names a gate the code
computes, but no production symbol writes the completion object.

Three rows are in maintenance, inside the proposed compaction-claims
design. `GuardedPublish`, `AbandonPublish`, and
`LostClaimNeverPublishesThroughGuardedPath` are pinned at the claim
primitive by one test there. No code outside that primitive calls it
yet. A production test for these three follows the first shipped
caller.

## Add or change a model

Write each invariant to observe the store, or a witness of what the
store returned. Never write an invariant that reads bookkeeping the
action itself sets.

Add a negative control, with an `.expect` file, for every behavior the
property forbids. Prove each control is not vacuous with a mutant.
Record the mutant's exact TLC violation line in a counterexample note.

Keep every gated configuration inside its time budget. Record its
distinct-state count and wall time in `results.md` and `bands.tsv`, in
the same commit as the model.

Add one traceability row for the property, naming one Rust path and
symbol. Never write a line number in a markdown file.

Run the smoke, live, negative, traceability, and exhaustive lanes before
you commit. See the [suite README](../../formal/tla/README.md) for the file
layout.

## Background

The suite's scope and layout are
[ADR-1113](../adrs/1113-tla-verification-suite.md), decisions D1 and D5.
Its claim language is decision D12. Its negative-control convention is
decision D6, and its traceability convention is decision D8. The
suite-wide figures in this guide are copied from
[REPORT.md](../../formal/tla/REPORT.md).

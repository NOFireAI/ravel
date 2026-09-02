# Results: common object-store model

Entry module: `MCRavelObjectStore.tla` (self-test over `RavelObjectStore.tla`).
Figures below are the pre-registered bands the harness run should land in;
the exact per-run figures are written to `.cache/tla/last-run.tsv`. Model
state counts are deterministic for a fixed config, so the bands are tight.

Toolchain: TLC 2.19 (tla2tools 1.7.4), Temurin JRE 21, `-workers auto`.

| Config | Spec | Distinct states | Depth | Wall time | Result |
|---|---|---|---|---|---|
| smoke.cfg | MCSpec (safety, symmetry-reduced) | 86206 | 11 | ~2s | PASS |
| exhaustive.cfg | FairSpec (safety + liveness) | 440448 | 12 | ~30s | PASS |
| negative/lost-response-not-applied.cfg | MCSpec | short prefix | n/a | ~1s | ReadAfterWrite violated, exit 12 |
| negative/cas-accepts-stale-version.cfg | MCSpec | short prefix | n/a | ~1s | CasNeedsFreshVersion violated, exit 12 |

Bands (a run outside these is a regression to investigate, not to widen):

- smoke distinct states: 86206, depth exactly 11.
- exhaustive distinct states: 440448, depth exactly 12.
- Both negatives: TLC exit exactly 12, and the named invariant (not another)
  reported violated. The `.expect` file pins both.

The smoke config applies `SYMMETRY Symmetry` (permutations of `Clients`) to
shrink the safety search; the exhaustive config drops symmetry because TLC
does not check liveness under symmetry reduction, and adds `WF_vars` on the
list-progress action so `ListEventuallyComplete` can hold.

# Results: common object-store model

Entry module: `MCRavelObjectStore.tla` (self-test over `RavelObjectStore.tla`).
The per-run figures are written to `.cache/tla/last-run.tsv`; the enforced
bands live in `bands.tsv` and `scripts/check-tla.sh` fails a PASS run whose
distinct-state count or depth falls outside them.

Toolchain: TLC 2.19 (tla2tools 1.7.4), Temurin JRE 21, `-workers auto`.
Figures below are from run `RUN_ID_PLACEHOLDER` (`scripts/check-tla.sh all`).

| Config | Spec | Distinct states | Depth | Wall time | Result |
|---|---|---|---|---|---|
| smoke.cfg | MCSpec (safety, symmetry-reduced) | 2011892 | 15 | ~31s | PASS |
| exhaustive.cfg | FairSpec (safety + liveness) | 3845952 | 15 | ~329s | PASS |
| negative/lost-response-not-applied.cfg | MCSpec | short prefix | n/a | ~1s | LostResponseEffectApplied violated, exit 12 |
| negative/cas-accepts-stale-version.cfg | MCSpec | short prefix | n/a | ~1s | CasOutcomeMatchesEffect violated, exit 12 |

Bands (a run outside these is a regression to investigate, not to widen; see
`bands.tsv`):

- smoke distinct states in [1950000, 2070000], depth in [15, 15].
- exhaustive distinct states in [3730000, 3960000], depth in [15, 15].

The safety and liveness models run to a fixed complete state graph, so their
distinct-state count and depth are deterministic; the bands carry a few percent
of margin only to absorb a future toolchain change. The negative configs are
NOT deterministic: they stop at the first counterexample TLC finds, and under
`-workers auto` which state that is varies between runs, so a negative gets no
band. Each negative is pinned instead by its `.expect` file: TLC must exit
exactly 12 and report exactly the named invariant.

The smoke config applies `SYMMETRY Symmetry` (permutations of `Clients`) to
shrink the safety search; the exhaustive config drops symmetry because TLC does
not check liveness under symmetry reduction, and adds `WF_vars` on the
list-progress action so `ListEventuallyComplete` can hold.

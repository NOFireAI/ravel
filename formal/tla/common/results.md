# Results: common object-store model

Entry module: `MCRavelObjectStore.tla` (self-test over `RavelObjectStore.tla`).
The per-run figures are written to `.cache/tla/last-run.tsv`; the enforced
bands live in `bands.tsv` and `scripts/check-tla.sh` fails a PASS run whose
distinct-state count or depth falls outside them.

Toolchain: TLC 2.19 (tla2tools 1.7.4), OpenJDK 25 on an arm64 macOS laptop
(the fleet executor's earlier run on Temurin JRE 21, x86_64 Linux, produced
the same distinct-state counts and depths), `-workers auto`.
Figures below are from run
`20260902T233208Z-2b36d8d0479151c10e2c6eb77f12451bd90ceb78`
(`scripts/check-tla.sh all`). Wall times are host-dependent and are not
banded.

| Config | Spec | Distinct states | Depth | Wall time | Result |
|---|---|---|---|---|---|
| smoke.cfg | MCSpec (safety, symmetry-reduced) | 2011892 | 15 | 102s | PASS |
| exhaustive.cfg | FairSpec (safety + liveness) | 3845952 | 15 | 252s | PASS |
| negative/lost-response-not-applied.cfg | MCSpec | short prefix | n/a | 2s | LostResponseEffectApplied violated, exit 12 |
| negative/cas-accepts-stale-version.cfg | MCSpec | short prefix | n/a | 3s | CasOutcomeMatchesEffect violated, exit 12 |
| negative/list-never-progresses.cfg | FairSpec (ListStalls) | short prefix | n/a | 5s | ListEventuallyComplete violated, exit 13 |

Mutants demonstrated against the correct module (each in a scratch copy,
recorded under `counterexamples/`): CAS on an absent key accepted
(`CasOutcomeMatchesEffect`), a multipart part published early
(`MultipartInvisibleUntilComplete`), a delete of an absent key that stamps a
new version onto the record (`DeleteIdempotent`), a delete that resets the version counter
(`VersionsNeverReused`), and a counting listing consumer that deduplicates
(`ListingConsumersConsistent`). Each reports TLC exit 12 naming that
invariant.

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
exactly the expected code (12 for a safety violation, 13 for a temporal one)
and report exactly the named property. For the exit-13 control the harness runs
a generated config that declares only `ListEventuallyComplete`, since TLC 1.7.4
prints no property name on a temporal violation.

The smoke config applies `SYMMETRY Symmetry` (permutations of `Clients`) to
shrink the safety search; the exhaustive config drops symmetry because TLC does
not check liveness under symmetry reduction, and adds `WF_vars` on the
list-progress action so `ListEventuallyComplete` can hold.

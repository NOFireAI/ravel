# Results: catalog MVCC model

Entry module: `MCCatalogMVCC.tla` over `CatalogMVCC.tla`. The per-run figures
are written to `.cache/tla/last-run.tsv`; the enforced bands live in
`bands.tsv`, and `scripts/check-tla.sh` fails a PASS run whose distinct-state
count or depth falls outside them.

Toolchain: TLC2 2.19 (tla2tools 1.7.4, sha256 verified by the harness),
Temurin OpenJDK 21.0.12 on x86_64 Linux, `-workers auto` on a 4-core host.

| Config | Spec | Distinct states | Depth | Wall time | Result |
|---|---|---|---|---|---|
| smoke.cfg | Spec (safety, symmetry-reduced) | 1739129 | 32 | 20s | PASS |
| exhaustive.cfg | FairSpec (safety + liveness) | 4656716 | 34 | 555s | PASS |
| negative/compaction-swaps-record.cfg | Spec | short prefix | n/a | short | CompactionPreservesMultiset violated, exit 12 |
| negative/head-names-unwritten-part.cfg | Spec | short prefix | n/a | short | HeadNamesOnlyCompleteParts violated, exit 12 |
| negative/metrics-dedup-dropped.cfg | Spec | short prefix | n/a | short | SignalDedupContract violated, exit 12 |
| negative/reconcile-on-tick.cfg | Spec | short prefix | n/a | short | ReconcileOnlyOnWatermarkAdvance violated, exit 12 |
| negative/snapshot-changes-mid-attempt.cfg | Spec | short prefix | n/a | short | PinnedSnapshotStableWithinAttempt violated, exit 12 |
| negative/sweep-superseded-no-head-gate.cfg | Spec | short prefix | n/a | short | HeadNamedObjectNeverDeleted violated, exit 12 |

Bands (a run outside these is a regression to investigate, not to widen; see
`bands.tsv`):

- smoke distinct states in [1700000, 1780000], depth in [32, 32].
- exhaustive distinct states in [4600000, 4710000], depth in [34, 34].

The safety and liveness models run to a fixed complete state graph, so their
distinct-state count and depth are deterministic; the bands carry a few percent
of margin only to absorb a future toolchain change. The negative configs are
NOT deterministic: they stop at the first counterexample TLC finds, and under
`-workers auto` which state that is varies between runs, so a negative gets no
band. Each negative is pinned instead by its `.expect` file: TLC must exit
exactly 12 (a safety violation) and report exactly the named invariant.

The smoke config applies `SYMMETRY Symmetry` (permutations of `Clients`) and
runs `Spec` for a fast safety-only search; the exhaustive config drops symmetry
because TLC does not check liveness under symmetry reduction, adds a second
compaction identity so the two-input-set dedup conflict is non-vacuous, and runs
`FairSpec` with `WF_vars` on the clock, fold progress, and query progress so
`QueryTerminates` can hold.

Both configs run a single folder. The model's HEAD has one folder writer, so a
literal two-folder CAS race is not enumerated; the version-matched CAS is still
exercised, because `DoUnsupportedHead` bumps the HEAD version under an in-flight
fold, so a fold reaches its CAS with a stale base version and takes the losing
branch (HEAD untouched, folder rebases). The staged part it abandons is then
swept by the catalog-object pass, so both sides of the version-match guard are
covered without a second folder.

`LateSupersessionEventuallyReflected` is defined in the spec but not checked by
any config: it is a recorded shrink. Under the F16/F17 design (reconcile runs
only on a watermark-advancing fold), a compaction landing in an already-folded
hour is reflected only by a later fold whose watermark advances past that hour.
A bounded model clock saturates its finite watermark, so a compaction published
after the final advance is never re-reconciled and TLC reports a stuttering
counter-example. The stale window it exposes is safe by design (query-time
dedup, pinned by `SignalDedupContract`), so this is a finite-model liveness
limitation, not a defect. The shrunk trace and analysis are in
`counterexamples/late-supersession-shrink.md`. `QueryTerminates`, which holds
under the bounded clock, is checked in its place.

## Exhaustive constants

`Keys = {hk}`, `Content = {hd, nc}` (`NoContent = nc`), `Clients = {f1}`,
`Hours = {0, 1}`, `Records = {rA, rB}`, `CompIds = {g1, g2}`, `MaxClock = 3`,
`MaxOps = 3`, `FoldSealDelay = 1`, `MaintSealDelay = 0`, `ProtectionHorizon = 1`,
`RetentionHorizon = 2`, `LagBound = 1`, `DedupBySignal = TRUE`, all six negative
switches FALSE.

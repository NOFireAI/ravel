# Review-driven TLC evidence

This directory holds TLC evidence produced during code review, beyond what
the negative-control gate runs on every commit. It is not a walk of the
shipped negative controls: those mutants live in the model itself
(`Broken*` actions gated by CONSTANTS) and their prose walks are under
`../negative/counterexamples/`. Two kinds of report live here instead:

- **Mutant probes** (`*-mutant.md`): a reviewer-crafted mutant applied to a
  scratch copy (`/tmp`, never the repo), run to show a declared invariant
  actually depends on real store state and is not a tautology -- for
  example, that an invariant reading a store witness cannot be fooled by an
  action that self-reports the wrong outcome.
- **Equivalence checks** (`*-equivalence.md`): two scratch copies of a spec,
  differing only in a fix under review, run against a deliberately weakened
  invariant to show the fix is non-vacuous where the shipped invariant
  suite alone can't distinguish the two formulas. `wv-store-grounding-
  equivalence.md` is the first such entry, covering the F2 fix that grounds
  `DoPublish`'s `wv` in `ContentOf(rk)` instead of the `firstRecord` witness.

No correct-form configuration (`MCSpec`/`MCFairSpec` with every switch at
its default) has ever produced a counterexample. If one ever does, its walk
belongs here too.

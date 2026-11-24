**Mutant 4 passed, so all four mutants passed.** The smoke and mutants part of the wave is complete.

Mutant 4 (`26cedb3cee83cfcc7e6fecaa52d4b11d-67-29`, `resharding/token-validated-against-count`):
- Target: `resharding: a commit token whose record is durable resolves across a reshard` had **68 counterexamples**, beside 74 true. The true evaluations are from the forged read, which was refused as unsatisfiable 37 times, as predicted.
- Predicted collateral that matched:
  - `catalog: a token read returns each acked record of its write exactly once` was false 36 times (199 true). This is the driver's view of the same refusal.
  - `resharding: a token resolved at a shard index above the active count` was unreached (0/0).
- Predicted collateral that differed: `catalog: compaction preserves the acked multiset` was predicted false but was unreached (0/0). This is the same pattern as in mutants 2 and 3, where the eventually's last step is not reached.
- The straggler sometimes (`reshard: a write routed before a decrease landed at a retired shard in its activation hour`) was 28 true. No other Always was false, no Unreachable was hit, and there were no container exits.

**Summary** (pass rule: ADR-4326 decision 3):

| Run | Result | Target counterexamples | Billed (`snouty runs usage`) |
|---|---|---|---|
| reshard-smoke `fe4abcfa…` | met | (none expected) | 15.2 h |
| mutant-1 `no-writer-fence` `ecc94304…` | pass | 26 | 14.9 h |
| mutant-2 `mo-diverge-overwrites-record` `2196edfa…` | pass | 38 | 15.0 h |
| mutant-3 `mo-missing-part-reports-converged` `00f89f53…` | pass | 36 | 14.6 h |
| mutant-4 `token-validated-against-count` `26cedb3c…` | pass | 68 | pending |

Billed so far is 59.7 h, plus mutant 4. The planned figure for these five runs was 75 h. Every cap-12 bill is inside the pre-registered 14.2 to 15.2 h band, and none reached stop rule 6's 17 h.

Not claimed (per the amendment): that the stale-writer fence, the 6 to 4 decrease, the straggler path or forged-token validation were exercised under fault injection. They run in the eventually.

Carried into the long run's re-registration:
- The drawn decrease, straggler and forged-read rule (in progress) brings those paths under faults. It needs new images, local falsification of all four mutants again, and the same 126-property set.
- `catalog: compaction preserves the acked multiset` was unreached in all three runs whose mutant corrupts record, part or token state (2, 3 and 4), while it was 8 in the smoke and 2 in mutant 1. The long run is plain, so the smoke's band applies there.
- `reshard: HEAD's footprint named data of a folded post-activation hour at every shard the increase added` is low-rate (3 true in 27 evaluations in the smoke), so a long-run band of at least 1 true is right, but it is the row most likely to miss.

There are no findings against the product from these five runs.

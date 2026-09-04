# Results: commit protocol model

Entry module `MCCommitProtocol.tla` over `CommitProtocol.tla`, which
instantiates the shared store module. Per-run figures land in
`.cache/tla/last-run.tsv`; the enforced bands live in `bands.tsv` and the
harness fails a PASS run whose distinct-state count or depth falls outside
them.

Toolchain: TLC 2.19 (tla2tools 1.7.4), OpenJDK 25 on an arm64 macOS laptop,
`-workers auto`. Wall times are host-dependent and are not banded.

| Config | Spec | Distinct states | Depth | Wall time | Result |
|---|---|---|---|---|---|
| smoke.cfg | Spec (safety) | 5466239 | 36 | 52s | PASS |
| exhaustive.cfg | Spec (safety, retry budget and deadline) | 30385359 | 38 | 4m49s | PASS |
| live.cfg | FairSpec (liveness) | 649 | n/a | under 1s | PASS |
| negative/no-cross-shard-atomicity.cfg | Spec, reachability obligation | short prefix | n/a | under 1s | NoCrossShardAtomicityUnreachable violated, exit 12 (required) |
| negative/at-least-once-duplicate-reachable.cfg | Spec, reachability obligation | short prefix | n/a | under 1s | DuplicateUnreachable violated, exit 12 (required) |
| negative/commit-before-data.cfg | Spec | short prefix | n/a | under 1s | NoCommitWithoutData violated, exit 12 |
| negative/mismatched-identity-idempotent.cfg | Spec | short prefix | n/a | under 1s | OneIdentityOneContent violated, exit 12 |
| negative/ack-before-commit.cfg | Spec | short prefix | n/a | under 1s | StrictAckImpliesDurable violated, exit 12 |
| negative/marker-before-all-shards.cfg | Spec | short prefix | n/a | under 1s | MarkerImpliesAllShardsDurable violated, exit 12 |
| dedup-mutant.cfg | Spec | full | n/a | under 1s | no violation, exit 0: with RetryDedups set the duplicate is unreachable, which is what proves the obligation above is not vacuous. Run by hand, not in the negative lane |

The safety models run to a complete state graph, so their distinct-state
count and depth are deterministic and the bands carry a few percent of margin
only to absorb a future toolchain change. The negative controls stop at the
first counterexample TLC finds, which under `-workers auto` is not
deterministic, so they carry no band; each is pinned instead by its `.expect`
file, which names both the exit code and the property that must be reported.

## What the correct-form runs found

Two invariants failed on the first correct-form run and both were errors in
the model, not in the protocol. Both are recorded in README because they are
evidence the invariants are load-bearing rather than decorative:

- `RetrySamePinnedFlushIdempotent` caught a model that let a crashed flush
  re-pin its own identity with different content. The shipped writer mints a
  fresh writer id on restart, so the identity is now retired and accidental
  reuse is a separate action defended by the content-hash compare.
- `NoPublishAfterAbandon` caught a model that claimed an abandoned flush
  leaves nothing durable. A commit whose response was lost is durable even
  though its writer then reports an error. The invariant now reads the
  `publishedAt` witness and states what the decision record actually claims:
  no store write is issued after the deadline.

No counterexample in this area indicates a defect in the implementation.

# Results: commit protocol model

Entry module `MCCommitProtocol.tla` over `CommitProtocol.tla`, which
instantiates the shared store module. Per-run figures land in
`.cache/tla/last-run.tsv`; the enforced bands live in `bands.tsv` and the
harness fails a PASS run whose distinct-state count or depth falls outside
them.

Toolchain: TLC 2.19 (tla2tools 1.7.4), Eclipse Temurin 21.0.12.1 JRE, Linux
aarch64, `-workers auto` (4 workers, 4 cores), host `rp1`. Wall times are
host-dependent and are not banded. Run id
`20260903T213519Z-dcd6f6a1c1f69b4d520a8b9bfa33ee24994f8a72` unless noted
otherwise.

## Configuration runs

| Config | States generated | Distinct | Depth | Seconds | Result |
|---|---|---|---|---|---|
| smoke.cfg | 97311 | 29064 | 24 | 2 | PASS |
| negative/ack-before-commit.cfg | 2 | 2 | n/a | 1 | VIOLATED as required (StrictAckImpliesDurable) |
| negative/at-least-once-duplicate-reachable.cfg | 64817 | 26486 | n/a | 2 | VIOLATED as required (DuplicateUnreachable) |
| negative/commit-before-data.cfg | 44 | 41 | n/a | 1 | VIOLATED as required (NoCommitWithoutData) |
| negative/deadline-reachable.cfg | 21228 | 9420 | n/a | 2 | VIOLATED as required (AbandonUnreachable) |
| negative/marker-before-all-shards.cfg | 779 | 505 | n/a | 1 | VIOLATED as required (MarkerImpliesAllShardsDurable) |
| negative/mismatched-identity-idempotent.cfg | 26950 | 12816 | n/a | 2 | VIOLATED as required (OneIdentityOneContent) |
| negative/no-cross-shard-atomicity.cfg | 243 | 174 | n/a | 1 | VIOLATED as required (NoCrossShardAtomicityUnreachable) |
| negative/query-reads-uncommitted-data.cfg | 77 | 65 | n/a | 1 | VIOLATED as required (NoUncommittedDataVisible) |
| traceability | n/a | n/a | n/a | n/a | PASS, 21 rows resolve |
| exhaustive.cfg | 397331556 | 111604592 | not reached | 2247 | STOPPED, not a violation |

The negative rows are the required outcome: each config disables a guard or
flips a broken-behaviour switch, and TLC finding the counterexample proves
the corresponding property is load-bearing, not vacuous. Each is pinned by
its `.expect` file, which names the exit code and the property the log must
report; all seven `.expect` files were checked against the logs above and
match exactly.

`exhaustive.cfg` did not complete in this session. It was launched under
`scripts/check-tla.sh all -a commit` and left running with a disk-floor and
30-minute watchdog. The watchdog process did not survive as a running
background job (a defect in how it was backgrounded, not in the model or
the script), so the run continued unwatched past the 30-minute cap; it was
found still running at 37 minutes 27 seconds on the next poll and stopped
by hand at that point (`TLC exit 137`, SIGKILL). The figures above are the
last progress line TLC printed before the kill, not a completed state
graph, and `bands.tsv` carries no exhaustive row for this reason: banding
a partial count would assert a state-space size that was never actually
established. A completed exhaustive figure remains open for a future run
with a working watchdog and a longer session budget.

## By-hand runs

| Run | Spec/Invariant | States generated | Distinct | Depth | Result |
|---|---|---|---|---|---|
| dedup-mutant.cfg | DuplicateUnreachable only, RetryDedups=TRUE | 433976430 | 81903514 | 34 | PASS (exit 0), "Model checking completed. No error has been found.", 25min 00s |
| live.cfg | FairSpec / EveryPinnedFlushSettles | 42119 | 15812 | 13 | PASS (exit 0), under 1s |

`dedup-mutant.cfg` is the proof that `DuplicateUnreachable` is not
vacuously true: with the dedup guard enabled (`RetryDedups=TRUE`), TLC
explored the complete state graph at these bounds and found no state where
a retried commit produces a duplicate. Paired with the negative control
below (same obligation, guard disabled, counterexample found in 2 seconds),
this shows the guard is what makes the property hold, not an unreachable
antecedent.

## Mutant audit

One behaviour mutation per invariant, either a scratch copy of the two
`.tla` files outside the repository (mutated file and mutation described
below, temp dir removed afterward) or, where an existing negative-control
switch already is that mutation, a citation of that control instead of a
redundant new one. Every row's TLC line was read from this session's own
logs or console output, not carried over from an earlier run.

| Invariant | What it observes | Mutation | TLC line |
|---|---|---|---|
| NoCommitWithoutData | the store's data-object presence for a flush before its commit record can exist | cited control: `negative/commit-before-data.cfg`, `CommitBeforeData=TRUE` lets `PutCommit` fire before the data PUT | `Error: Invariant NoCommitWithoutData is violated.` |
| NoUncommittedDataVisible | `queryAnswer`, the read path's own output, against `Visible` | scratch mutation: changed `RunQuery` to answer `Visible(f) \/ DataPresent(f)` unconditionally, ignoring `QueryReadsDataDirectly`, run against a scratch `mutant.cfg` (smoke.cfg bounds, switch at its correct FALSE value) | `Error: Invariant NoUncommittedDataVisible is violated.` |
| StrictAckImpliesDurable | `DurableSet` (store witness) whenever a strict ack was recorded | cited control: `negative/ack-before-commit.cfg`, `AckAtEnqueue=TRUE` lets the ack fire before the commit PUT succeeds | `Error: Invariant StrictAckImpliesDurable is violated.` |
| OneIdentityOneContent | the content actually durable under a commit key versus the pinned content for that identity | cited control: `negative/mismatched-identity-idempotent.cfg`, `SkipHashCompare=TRUE` drops the content-hash compare on identity reuse | `Error: Invariant OneIdentityOneContent is violated.` |
| SplitBrainStopsTheShard | `shardDead` for a shard that produced a split-brain outcome | scratch mutation: dropped the `shardDead' = IF split THEN ... ELSE shardDead` effect, replaced with `shardDead' = shardDead` (split-brain no longer kills the shard), run against `smoke.cfg` | `Error: Invariant SplitBrainStopsTheShard is violated.` |
| RetrySamePinnedFlushIdempotent | the durable content written by a retried `PutCreateIfAbsent` against what the writer pinned | scratch mutation: changed the retry's `Store!PutCreateIfAbsent(k, pinned[f])` to write the sentinel `NoC` instead, run against `smoke.cfg` | `Error: Invariant RetrySamePinnedFlushIdempotent is violated.` |
| NoPublishAfterAbandon | the `publishedAt` witness against `Expired(f)`, i.e. no store write after the flush-lifetime deadline | scratch mutation: removed the `~Expired(f)` guard from `PutCommit`'s conjunction list, run against a scratch cfg with room for the deadline to elapse (`FlushLifetime=1, MaxTicks=3`, single writer/shard) | `Error: Invariant NoPublishAfterAbandon is violated.` |
| MarkerImpliesAllShardsDurable | `AllShardsDurable` before the idempotency marker is written | cited control: `negative/marker-before-all-shards.cfg`, `MarkerAfterFirstShard=TRUE` writes the marker after only one shard is durable | `Error: Invariant MarkerImpliesAllShardsDurable is violated.` |
| PartialReportingMatchesSignal | per-shard reporting against the declared `Signal`'s shard set | diagnostic cfg (not a repo file): `Signal="metrics"`, `Shards={s1,s2}`, `AckAtEnqueue=TRUE`, invariant listed alone. Real: genuinely violable in isolation, but in every shipped cfg it is strictly weaker than `StrictAckImpliesDurable` and always shadowed by it (same states violate both, and TLC reports the earlier-listed invariant first), so no shipped cfg reports it directly | `Error: Invariant PartialReportingMatchesSignal is violated.` (diagnostic cfg only; shipped cfgs report `StrictAckImpliesDurable` on the same states) |
| TokenNeverServesStale | `ResolveToken`'s reported outcome against a live `Store!Present` read for the commit key | scratch mutation: changed `ResolveToken` to set `outcome \|-> "served"` unconditionally instead of from the live presence read, so `present` can be FALSE while `outcome` still claims "served", run against `smoke.cfg` | `Error: Invariant TokenNeverServesStale is violated.` |
| DuplicateUnreachable | reachability of a state with two distinct successful commits for one pinned identity under retry | cited control: `negative/at-least-once-duplicate-reachable.cfg`, `RetryDedups=FALSE` (dedup guard absent, ordinary at-least-once retry) | `Error: Invariant DuplicateUnreachable is violated.` |

`NoUncommittedDataVisible` used to read `(P /\ Q) => Q` over `Visible` and
`DurableSet`, both defined identically from the store: a propositional
tautology, true for every assignment regardless of state, that no mutation
could ever fail. The fix adds a `RunQuery` action and a `queried`/
`queryAnswer` pair of variables modeling the read path as independent
state, and restates the invariant over `queryAnswer` (the query's own
output) rather than over `Visible`/`DurableSet` directly. The negative
control `negative/query-reads-uncommitted-data.cfg` (`QueryReadsDataDirectly
= TRUE`) makes `RunQuery` answer from data presence instead of the commit
record, and TLC reports the violation:

```
Error: Invariant NoUncommittedDataVisible is violated.
```

77 states generated, 65 distinct states found, matching the `.expect` file
(`exit=12`, `property=NoUncommittedDataVisible`).

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

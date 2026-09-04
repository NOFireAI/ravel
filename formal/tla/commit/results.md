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
| negative/deadline-reachable.cfg | 1501 | 856 | n/a | 1 | VIOLATED as required (AbandonUnreachable) |
| negative/marker-before-all-shards.cfg | 779 | 505 | n/a | 1 | VIOLATED as required (MarkerImpliesAllShardsDurable) |
| negative/mismatched-identity-idempotent.cfg | 26950 | 12816 | n/a | 2 | VIOLATED as required (OneIdentityOneContent) |
| negative/no-cross-shard-atomicity.cfg | 243 | 174 | n/a | 1 | VIOLATED as required (NoCrossShardAtomicityUnreachable) |
| negative/query-reads-uncommitted-data.cfg | 77 | 65 | n/a | 1 | VIOLATED as required (NoUncommittedDataVisible) |
| traceability | n/a | n/a | n/a | n/a | PASS, 21 rows resolve |
| exhaustive.cfg | 442076218 | 118898952 | not reached | 3600 | TIMEOUT, not a violation |

The negative rows are the required outcome: each config disables a guard or
flips a broken-behaviour switch, and TLC finding the counterexample proves
the corresponding property is load-bearing, not vacuous. Each is pinned by
its `.expect` file, which names the exit code and the property the log must
report; all eight `.expect` files were checked against the logs above and
match exactly.

`exhaustive.cfg` carries the smallest constants that still satisfy every
coverage requirement on this suite: `Shards={s1,s2}` (two-shard
interleaving, required by the marker and partial-reporting invariants, and
the only cfg in this suite that covers it — `smoke.cfg` runs one shard),
`Contents={c1,c2}` (the minimum cardinality `OneIdentityOneContent`'s
identity-reuse coverage needs — a single content value can't distinguish
reuse-with-same-content from reuse-with-different-content),
`FlushLifetime=1`/`MaxTicks=2` (the
floor `ASSUME FlushLifetime > 0` and deadline-reachability allow), and
`MaxRetries=0`/`Writers={w1}` (already at their floor). The one coverage
item given up versus the pre-task cfg is `MaxRetries`: it now runs at 0
instead of 1, dropping the second-retry interleaving from the exhaustive
lane specifically (the retry path itself is still exercised at
`MaxRetries=1` elsewhere, in `negative/at-least-once-duplicate-reachable.cfg`
and `dedup-mutant.cfg`).

Even at this floor, `exhaustive.cfg` does not complete: run via
`scripts/check-tla.sh exhaustive -a commit`, it hit the script's own
3600-second internal budget (`EXHAUSTIVE_BUDGET` in `scripts/check-tla.sh`)
without the state queue ever plateauing (442,076,218 states generated,
118,898,952 distinct, 31,290,754 left on queue, monotonically increasing
for the full hour). A second independent run under a 2700-second external
wrapper showed the same non-converging trajectory. `bands.tsv` carries no
exhaustive row for this reason: banding a partial count would assert a
state-space size that was never actually established. Separately: the
task that produced this cfg cited a 45-minute exhaustive budget "mandated
by ADR-1113"; the ADR (`docs/adrs/1113-tla-verification-suite.md`) states
45 minutes only for the PR-gating `tla` workflow's smoke/negative/
traceability job, and gives exhaustive its own nightly job at a 120-minute
budget, while `scripts/check-tla.sh` itself hardcodes 3600 seconds (60
minutes) for `exhaustive` — three different numbers across the task
framing, the ADR, and the script. `scripts/check-tla.sh` sits outside
this task's scope (`formal/tla/commit/`), so the mismatch is reported
here and in the final task report rather than edited.

## By-hand runs

| Run | Spec/Invariant | States generated | Distinct | Depth | Result |
|---|---|---|---|---|---|
| dedup-mutant.cfg | DuplicateUnreachable only, RetryDedups=TRUE | 253155064 | 186215950 | not reached | STOPPED, not completed (see prose below) |
| live.cfg | FairSpec / EveryPinnedFlushSettles | 42119 | 15812 | 13 | PASS (exit 0), under 1s |

`dedup-mutant.cfg` used to complete in 25 minutes (433,976,430 generated,
81,903,514 distinct, depth 34), and was re-run by hand this task to
re-confirm `DuplicateUnreachable` after the `RunQuery` model change. It did
not converge: the new `queried`/`queryAnswer` variables and `RunQuery`
action added for the `NoUncommittedDataVisible` fix (see the mutant-audit
section below) enlarge the reachable state space for every cfg that
includes them, not only `exhaustive.cfg`. The run was resumed once from a
TLC checkpoint (`-recover`, at 154,574,468 states already examined) and
then stopped by hand after the state queue kept growing for a further 15
minutes with no plateau (253,155,064 generated, 186,215,950 distinct,
43,984,712 left on queue at the stop). No counterexample was found before
the stop, which is consistent with `DuplicateUnreachable` continuing to
hold at these bounds, but an incomplete state graph is not proof of it;
this row records a stopped run, not a pass, and the obligation should be
re-run to completion in a session with a longer budget. The negative
control below (same obligation, dedup guard disabled, counterexample found
in 2 seconds) is unaffected by this and still demonstrates the guard is
load-bearing.

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

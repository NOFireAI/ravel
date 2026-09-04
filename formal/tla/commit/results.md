# Results: commit protocol model

Entry module `MCCommitProtocol.tla` over `CommitProtocol.tla`, which
instantiates the shared store module. Per-run figures land in
`.cache/tla/last-run.tsv`; the enforced bands live in `bands.tsv` and the
harness fails a PASS run whose distinct-state count or depth falls outside
them.

Toolchain: TLC 2.19 (tla2tools 1.7.4), Eclipse Temurin 21.0.12.1 JRE, Linux
x86_64, `-workers auto` (8 workers, 8 cores). Wall times are host-dependent
and are not banded. Run id
`20260904T184559Z-ae2b218f783b8a3e325b1e85e35f2f2bdfef63c1` unless noted
otherwise, from a single `scripts/check-tla.sh all -a commit` invocation
covering smoke, negative, traceability and exhaustive together.

## Configuration runs

| Config | States generated | Distinct | Depth | Seconds | Result |
|---|---|---|---|---|---|
| smoke.cfg | 305165 | 76212 | 21 | 5 | PASS |
| negative/ack-before-commit.cfg | 2 | 2 | n/a | 1 | VIOLATED as required (StrictAckImpliesDurable) |
| negative/at-least-once-duplicate-reachable.cfg | 64817 | 26486 | n/a | 2 | VIOLATED as required (DuplicateUnreachable) |
| negative/commit-before-data.cfg | 44 | 41 | n/a | 1 | VIOLATED as required (NoCommitWithoutData) |
| negative/deadline-reachable.cfg | 1501 | 856 | n/a | 1 | VIOLATED as required (AbandonUnreachable) |
| negative/marker-before-all-shards.cfg | 779 | 505 | n/a | 1 | VIOLATED as required (MarkerImpliesAllShardsDurable) |
| negative/mismatched-identity-idempotent.cfg | 26950 | 12816 | n/a | 2 | VIOLATED as required (OneIdentityOneContent) |
| negative/no-cross-shard-atomicity.cfg | 243 | 174 | n/a | 1 | VIOLATED as required (NoCrossShardAtomicityUnreachable) |
| negative/put-commit-lost-response-reachable.cfg | 21 | 20 | n/a | 1 | VIOLATED as required (PutCommitLostResponseUnreachable) |
| negative/put-data-lost-response-reachable.cfg | 2 | 2 | n/a | 1 | VIOLATED as required (PutDataLostResponseUnreachable) |
| negative/query-reads-uncommitted-data.cfg | 77 | 65 | n/a | 1 | VIOLATED as required (NoUncommittedDataVisible) |
| negative/transient-failure-reachable.cfg | 2 | 2 | n/a | 1 | VIOLATED as required (TransientFailureUnreachable) |
| traceability | n/a | n/a | n/a | n/a | PASS, 21 rows resolve |
| exhaustive.cfg | 17892751 | 5466239 | 36 | 131 | PASS |

The negative rows are the required outcome: each config disables a guard,
flips a broken-behaviour switch, or (the three new `*-reachable.cfg` rows)
asserts an action's own enabling conjuncts must never hold, and TLC finding
the counterexample proves the corresponding property is load-bearing or the
action reachable, not vacuous. Each is pinned by its `.expect` file, which
names the exit code and the property the log must report; all eleven
`.expect` files were checked against the logs above and match exactly.

`smoke.cfg` moved from `MaxRetries=0` to `MaxRetries=1` (issue #1120): at
`MaxRetries=0`, the three store-level retry actions
(`PutDataLostResponse`, `PutCommitLostResponse`, `TransientFailure`) are
permanently disabled, since each requires `retries[f] < MaxRetries`, and
no cfg that lists the full safety `INVARIANT` set ever set `MaxRetries`
above 0. `smoke.cfg` is now that cfg: it checks the same eleven safety
invariants as before, with the retry actions reachable, in 4s against the
300s smoke ceiling. The three new `negative/*-reachable.cfg` probes above
prove each retry action fires under these exact constants, and the mutant
audit below shows `RetrySamePinnedFlushIdempotent` is non-vacuous
specifically through a retry action, not just through the initial write.

`exhaustive.cfg` carries the smallest constants that still satisfy every
coverage requirement on this suite: `Shards={s1,s2}` (two-shard
interleaving, required by the marker and partial-reporting invariants, and
the only cfg in this suite that covers it — `smoke.cfg` runs one shard),
`Contents={c1,c2}` (the minimum cardinality `OneIdentityOneContent`'s
identity-reuse coverage needs — a single content value can't distinguish
reuse-with-same-content from reuse-with-different-content),
`FlushLifetime=1`/`MaxTicks=2` (the
floor `ASSUME FlushLifetime > 0` and deadline-reachability allow), and
`MaxRetries=0`/`Writers={w1}` (already at their floor). Three coverage
items are given up versus the pre-task cfg. `MaxRetries` runs at 0 instead
of 1, dropping the second-retry interleaving from the exhaustive lane
specifically (the retry path itself, including the three retry actions
against the full safety invariant list, is exercised at `MaxRetries=1` in
`smoke.cfg`, and the dedup guard specifically in
`negative/at-least-once-duplicate-reachable.cfg` and `dedup-mutant.cfg`).
`NoUncommittedDataVisible` is dropped from this cfg's
`INVARIANT` list and `CheckQuery` is set `FALSE`: the `RunQuery` action
added for that invariant (see the mutant-audit section below) is a
single-fire action whose firing point can land at any reachable state, and
that timing choice alone was enough to stop this cfg from converging (see
below). `smoke.cfg` (`CheckQuery=TRUE`) and the dedicated negative/mutant
coverage in `negative/query-reads-uncommitted-data.cfg` still exercise the
invariant at floor bounds. `TokenNeverServesStale` is dropped the same way,
with a new `CheckToken` constant gating `ResolveToken` and the
`TombstoneBucket`/`SupersedeRecord` actions that feed it: the same
single-fire, any-reachable-state pattern that made `RunQuery` costly here.
`smoke.cfg` (`CheckToken=TRUE`) still proves it exhaustively at floor
bounds. `exhaustive.cfg` already gave up `MaxRetries` and `CheckQuery`
coverage the same way, so `CheckToken` follows existing precedent rather
than setting a new one.

At the previous floor (`CheckQuery` unconditionally wired in, no gate),
`exhaustive.cfg` did not complete: run via `scripts/check-tla.sh exhaustive
-a commit`, it hit the script's own 3600-second internal budget
(`EXHAUSTIVE_BUDGET` in `scripts/check-tla.sh`) without the state queue
ever plateauing (442,076,218 states generated, 118,898,952 distinct,
31,290,754 left on queue, monotonically increasing for the full hour).
With `CheckQuery=FALSE`, the same cfg completes: 642,136,435 states
generated, 148,881,235 distinct, depth 38, in 2728s (45m27s, `-workers
auto` resolving to 8 workers on an 8-core host; wall time is host-dependent
and not banded). This figure was itself superseded by the `CheckToken`
shrink described below, and `bands.tsv` never carried a row for it: the
642,136,435/148,881,235/depth-38 figure is pre-shrink and kept here only
for reference, not the verified record. The figure `bands.tsv` actually
carries, and the one this record names as verified, is the post-`CheckToken`
17,892,751 generated/5,466,239 distinct/depth 36 figure below. Separately,
on the three budget figures that appear in ADR-1113 and the
harness: they are not in conflict, and an earlier task spec conflated them.
Sixty minutes is the per-configuration ceiling for `exhaustive`, which is
what `scripts/check-tla.sh` encodes as `EXHAUSTIVE_BUDGET=3600` and the
only one this configuration must meet. Forty-five minutes is the total
budget of the PR-gating `tla` job, which runs smoke, negative and
traceability and does not run `exhaustive` at all. One hundred and twenty
minutes is the total budget of the nightly workflow, which is where
`exhaustive` actually runs. Each number governs a different thing.

With `CheckQuery=FALSE` alone, `exhaustive.cfg` fit the script's 3600s
budget (2728s, 76%) but not with real margin: a second host measured this
same cfg at 4142s, over budget, and reported PASS only because that host
lacked `timeout`/`gtimeout` to enforce the ceiling, which `check-tla.sh`
announces but does not fail on. A gate whose pass/fail outcome depends on
whether the host has `timeout` installed is not a gate. `ResolveToken`
(and `TombstoneBucket`/`SupersedeRecord`, which feed it) were the
remaining unconditionally-enabled, single-fire, any-reachable-state
actions, supporting only `TokenNeverServesStale`, and exhibiting the same
blowup mechanism `RunQuery` did before `CheckQuery` existed. Adding a
`CheckToken` constant, identical in mechanism to `CheckQuery`, and setting
it `FALSE` here with `TokenNeverServesStale` dropped from this cfg's
`INVARIANT` list cuts the run from 148,881,235 distinct/depth 38/2728s to
17,892,751 generated, 5,466,239 distinct, depth 36, 181s (131s on this
session's host), about 5% of the 3600s ceiling and well under the 1500s
target with real margin on either host. `smoke.cfg` (`CheckToken=TRUE`)
re-run after the `CheckToken` change produced byte-identical figures to
its pre-change baseline (97,311 generated, 29,064 distinct, depth 24, at
the `MaxRetries=0` this cfg carried at the time), confirming the gate is
inert when enabled and that `TokenNeverServesStale` is still proved
exhaustively, just at `smoke.cfg`'s one-shard bounds instead of
`exhaustive.cfg`'s two. `smoke.cfg`'s figures moved again, to
305,165/76,212/depth 21, when `MaxRetries` was later raised to 1 for
issue #1120 (see the configuration table above); that change did not
revisit the `CheckToken` gate, so this paragraph's conclusion still holds.

Two-shard reachability for the coverage this cfg exists to keep was
checked directly, not assumed. A scratch probe module
(`MCProbe.tla`, outside the repository, extending `MCCommitProtocol`)
asserted the negation of each invariant's non-vacuous antecedent as a
must-be-violated `INVARIANT`, run under `exhaustive.cfg`'s exact
(post-shrink) constants:

- `~(marker = "written")`: VIOLATED (exit 12, depth 8), proving the
  marker-write state, which requires `AllShardsDurable` across both
  shards simultaneously, is reached.
- `~(\E f \in FlushIds : ackKind[f] = "strict" /\ ~AllShardsDurable)`:
  VIOLATED (exit 12, depth 6), proving a strict ack fires while the two
  shards are not yet uniformly durable, the genuine cross-shard partial
  interleaving `PartialReportingMatchesSignal` exists to catch.

The flush-lifetime deadline obligation was re-checked rather than assumed
carried over: `negative/deadline-reachable.cfg`, which pins these same
constants, still reports `AbandonUnreachable` VIOLATED (exit 12) after the
`CheckToken` change, at unchanged bounds.

## By-hand runs

| Run | Spec/Invariant | States generated | Distinct | Depth | Result |
|---|---|---|---|---|---|
| dedup-mutant.cfg | DuplicateUnreachable only, RetryDedups=TRUE | 14974258 | 3443658 | 32 | PASS (exit 0), 1m24s |
| live.cfg | FairSpec / EveryPinnedFlushSettles | 42119 | 15812 | 13 | PASS (exit 0), under 1s |

`dedup-mutant.cfg` used to complete in 25 minutes (433,976,430 generated,
81,903,514 distinct, depth 34). After the `RunQuery` action and its
`queried`/`queryAnswer` variables were added for the `NoUncommittedDataVisible`
fix (see the mutant-audit section below), the same cfg stopped converging
for the same reason as `exhaustive.cfg` above: a run resumed once from a
TLC checkpoint (`-recover`, at 154,574,468 states already examined) was
stopped by hand after the state queue kept growing for a further 15
minutes with no plateau (253,155,064 generated, 186,215,950 distinct,
43,984,712 left on queue at the stop). No counterexample was found before
that stop, which was consistent with `DuplicateUnreachable` continuing to
hold at these bounds, but an incomplete state graph was not proof of it.

Adding `CheckQuery = FALSE` to `dedup-mutant.cfg` (the same lever as
`exhaustive.cfg`; this cfg does not list `NoUncommittedDataVisible` among
its invariants either, so the query machinery was never needed here)
restores completion: run by hand with the module path set explicitly
(`-DTLA-Library=".../formal/tla/common:.../formal/tla/commit"`, needed
because this cfg is not invoked through `scripts/check-tla.sh`), it
reports "Model checking completed. No error has been found." at
433,976,430 states generated, 81,903,514 distinct, depth 34, in 17m44s —
matching the pre-`RunQuery` baseline exactly, to the state. The fix
restores this cfg's original state space with zero regression to the
`DuplicateUnreachable` coverage it exists to prove. The negative control
below (same obligation, dedup guard disabled, counterexample found in 2
seconds) is unaffected by any of this and still demonstrates the guard is
load-bearing.

Adding `CheckToken = FALSE` to `dedup-mutant.cfg`, on top of its existing
`CheckQuery = FALSE` and for the same reason (this cfg does not list
`TokenNeverServesStale` either), cuts it further. Run by hand the same way
as before, it reports "Model checking completed. No error has been
found." at 14,974,258 states generated, 3,443,658 distinct, depth 32, in
1m24s, down from 17m44s. `DuplicateUnreachable` coverage is unaffected;
the negative control (`negative/at-least-once-duplicate-reachable.cfg`,
same obligation, dedup guard disabled) still finds its counterexample in
2 seconds.

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
| PartialReportingMatchesSignal | `ackKind[f] = "strict"` against `Visible(f)`, for every signal (restated; no longer gated on `Signal`) | diagnostic cfg (not a repo file): same constants as `negative/ack-before-commit.cfg` but `Signal="logs"` instead of `"metrics"`, `AckAtEnqueue=TRUE`, invariant listed alone so TLC reports it instead of being shadowed by `StrictAckImpliesDurable`. `Signal="logs"` is the case the retired `~ReportsPartial` guard used to exempt: under the old formula (`~ReportsPartial => (ackKind[f]="strict" => Visible(f))`), `ReportsPartial` held for logs, so the antecedent was false and this same behaviour would have passed vacuously. The restated invariant has no such exemption and catches it | `Error: Invariant PartialReportingMatchesSignal is violated.` (2 states generated, 2 distinct, depth 2; diagnostic cfg only, since every shipped cfg still lists `StrictAckImpliesDurable` first and shadows this one on the same states) |
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

```text
Error: Invariant NoUncommittedDataVisible is violated.
```

77 states generated, 65 distinct states found, matching the `.expect` file
(`exit=12`, `property=NoUncommittedDataVisible`).

`RetrySamePinnedFlushIdempotent` under retries specifically (issue #1120):
the table row above already exercises this invariant, but not through a
retry action. To show it is also non-vacuous once `MaxRetries=1` makes the
retry actions reachable, a second scratch mutation changed
`PutCommitLostResponse` to write the sentinel `"NoC"` to the commit key
instead of `pinned[f]`, and to set `phase' = "committed"` directly instead
of leaving `phase` unchanged at `"data"`, run against `smoke.cfg`'s
constants (`MaxRetries=1`) with only `RetrySamePinnedFlushIdempotent`
listed as `INVARIANT`. TLC reports:

```text
Error: Invariant RetrySamePinnedFlushIdempotent is violated.
```

280 states generated, 179 distinct, depth 5. The counterexample's
state-4 transition is labeled `<PutCommitLostResponse line 325, col 5 to
line 338, col 149 of module CommitProtocol>`, and that state carries
`retries=1`, `pinned=c2`, and the mutated commit-key content `"NoC"`,
which together attribute the violation to the retry action firing, not
merely to some earlier write.

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

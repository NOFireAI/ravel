## Pre-registration: reshard-ownership Antithesis wave (#4340)

DRAFT, not posted.

### Summary

Six Antithesis runs of the reshard-ownership profile (`WORKLOAD_PROFILE=reshard-ownership`, test name `ravel-reshard-ownership`, `-mutant` for a mutant run), all from images built at main 4c5d058dd (stamp below). D7 of the step 5 decision is the plan; D6 gives the pre-registered rows.

| Run | Selector | Cap | Planned h | Passes when | Miss, or stop |
|---|---|---|---|---|---|
| reshard-smoke | plain | 12 | 15 | every Always 0 counterexamples; every D6 row met; failing set inside the expected set | any owned Always false, or a D6 row vacuous: stop the wave |
| reshard-mutant-1 | `resharding/no-writer-fence` | 12 | 15 | `resharding: grace routing continues only within the lead horizon...` has >= 1 counterexample | property passes or is unreached |
| reshard-mutant-2 | `maintenance/mo-diverge-overwrites-record` | 12 | 15 | `maintenance: a loser observing a divergent input-set hash fails closed...` has >= 1 counterexample | same |
| reshard-mutant-3 | `maintenance/mo-missing-part-reports-converged` | 12 | 15 | `maintenance: a loser reports Converged only when the winner part is observed present...` has >= 1 counterexample | same |
| reshard-mutant-4 | `resharding/token-validated-against-count` | 12 | 15 | `resharding: a commit token whose record is durable resolves across a reshard` has >= 1 counterexample | same |
| reshard-long | plain | 24 | 27 | as the smoke | as the smoke |
| Total | | 84 | 102 | | wave ceiling 117 h with one rerun |

Order: the smoke runs first. The mutants follow once the smoke is triaged, at most 3 at once, and each launch also waits for its local falsification on these images under this profile, which has not run yet (mutant section). The long run is last. Free budget: 338.7 h (1200 - 861.3 billed, `snouty runs usage --rollup month`).

The expected set for the smoke is `expected-set.tsv`: 126 SDK properties, 84 expected-hit, 38 expected-unreached, 4 risk.

### Baseline from earlier runs

The baseline is the two latest catalog-maintenance runs, both at cap 12 and both exported with `snouty runs properties --json`:

| Run | Ref | Billed | SDK properties | Failing |
|---|---|---|---|---|
| `bc8e756efa914bbe84e83a62abf95098-67-29` (catalog-nofence) | 64760f617 (release/v0.35.0) | 14.5 h | 91 | 32: 28 unreached (6 Always, 19 Reachable, 3 Sometimes), and 4 `No unexpected container exits` (maint1, maint2, ravel1, ravel2, 3 counterexamples each) |
| `9e1b5871bdc5889df2eba28e374461b2-67-29` (catalog-fence-exact) | 970c6037b | 14.7 h | 93 | 30, all unreached (6 Always, 21 Reachable, 3 Sometimes) |

Unreached in both runs (true 0, false 0), 28 properties, none of them in an area this profile owns:

- Always (6): `commit: a strict metrics ack names a durable commit record`, `commit: a swept-token fallback never adds a rewrite's parts beside an object the rewrite replaced`, `commit: an idempotency marker implies every shard's commit is durable`, `erasure: a completion is written only when no pre-rewrite object is still served`, `erasure: a rewrite's output is its inputs minus the erased records`, `gc: no object is deleted inside its protection window`.
- Reachable (19): `commit:` flush abandoned, idempotency marker written, put retried, strict ack sent; `erasure:` a completion .done was written, a rewrite record published; `fault: ingest/data-put-refused fired`, `fault: ingest/encode-cancelled fired`; `gc:` a legal hold is placed, a legal hold is released, retention physical sweep delete; `logs ingest:` a keyed request replayed a stored idempotency receipt, a keyed request was refused because its marker lookup failed, a write found its shard actor unavailable, the idempotency marker write failed after a durable commit; `metrics ingest:` a strict write timed out waiting for its shard ack, a write committed on some shards before a sibling shard failed, a write found its shard actor unavailable, a write was answered Abandoned.
- Sometimes (3): `commit: a marker's durability evidence read completes before it gives up`, `commit: a strict metrics ack's durable-commit evidence read completes before it gives up`, `commit: an abandoned flush's commit record is absent at the abandon`.

Split between the two runs: `gc: a failed hold re-read skips a delete batch` (nofence 5/0, fence-exact unreached) and `server: a graceful drain did not complete cleanly` (nofence 12/0, fence-exact unreached). The nofence run's 12 container exits are the drain timeout under a proxy outage (runs.md entry for bc8e756e), and they match its 12 drain hits.

Both runs predate the forced tamper races, the reshard profile and every `maintenance:`, `resharding:` and `reshard:` literal; neither ref holds those literals (`git grep` at 64760f617 and 970c6037b finds none). Absent from both exports: those literals, the two tamper sometimes, the two tamper cap unreachables and `clock: HEAD changed while a clock move landed`. The two `maintain:` literals of #4561 are in fence-exact only. The fold, record and sweep races, the token reads and the audit are present and passing in both. So the baseline bands the `catalog:`, `clock:` and other-area rows, and every reshard and maintenance row has local evidence only.

Volume figures from the baseline that bound eventually-only rows: `catalog: the system recovers once faults stop` 34 and 49 examples; `catalog: compaction preserves the acked multiset`, evaluated at the end of the eventually, 7 and 13.

### Expected set for reshard-smoke

Source: the plain local reshard run at the image ref (`rs/`, 126 messages in `assertions.tsv`, the `hit:false` registration lines of every `sdk-*.jsonl` including `sdk-cli.jsonl`), joined with the two baseline exports. One row per message in `expected-set.tsv`; class counts:

| Class | Always | Reachable | Sometimes | Unreachable | AlwaysOrUnreachable | Total |
|---|---|---|---|---|---|---|
| expected-hit | 28 | 30 | 26 | 0 | 0 | 84 |
| expected-unreached | 6 | 20 | 3 | 8 | 1 | 38 |
| risk | 1 | 3 | 0 | 0 | 0 | 4 |

Expected-hit holds 76 messages hit locally and 8 unhit locally that both baselines reached under faults or drawn rules (for example `catalog: HEAD was overwritten with garbage through raw S3`, 85 and 120). Every `REQUIRED`, `RESHARD_REQUIRED` and `RESHARD_PER_NODE` literal of `local-run.sh` is expected-hit. Expected-unreached is the 28 of the baseline, `resharding: generation append CAS lost`, the 8 Unreachable and the 1 AlwaysOrUnreachable (the last 9 show Passing at 0/0).

**Set-level checks** (each is a miss if false):

1. The export holds exactly 126 SDK properties. Another count means the catalogue differs from the local run at the same images; reconcile it before triage.
2. Every Always and AlwaysOrUnreachable has 0 counterexamples, whatever its class. A false evaluation means the code reached it, so it is a finding in any area.
3. Every Unreachable has 0 hits.
4. The gate covers the owned areas (`catalog:`, `resharding:`, `maintenance:`) and the driver's literals (`catalog:`, `clock:`, `reshard:`) only, as the profile's expected-set rule does. Among those, the failing set is exactly `resharding: generation append CAS lost`, plus at most the two seeded-memo risk rows. Any other owned failing property is a miss to triage before any other launch. An other-area row that goes unreached is noted, never a miss. Predicted total failing: 29 (the 28 of the baseline plus CAS lost) to 39 (plus the 4 risk rows and the 6 other-area rows the baselines reached thinly, such as `logs ingest: a write committed on some shards before a sibling shard failed` at 3 and 5).
5. Outside the SDK: `No unexpected container exits` passes (fence-exact) or fails only with exits that are the drain timeout under a proxy outage (nofence), checked from the exit logs. `No Antithesis errors` and `No Antithesis session errors` pass.

**Bands.** Two baselines at two refs, plus a profile that changes the eventually, give no basis for a tight count. Every expected-hit row's band is >= 1 (a sometimes: >= 1 true; its false evaluations are not violations), and every Always row's is 0 false. The TSV quotes the local hits and the baseline counts beside each band. Volume predictions, recorded so that a surprise shows:

| Figure | Predicted | Miss |
|---|---|---|
| `catalog: the system recovers once faults stop`, examples | 34 to 100 (two evaluations per eventually that reaches the fence's thaw-read, baseline 34 and 49) | below 10 |
| `reshard: the clocks passed the writers' lead horizon while the provisioning record's reads were refused`, examples | 5 to 50 (a guess bounded by the baseline: an eventually step, so at most the recovers count, and early in the eventually, so above the multiset's 7 and 13) | 0: D6 row 2 vacuous, mutant-1 not launched |
| `reshard: a write routed before a decrease landed at a retired shard in its activation hour`, true examples | 3 to 50 (the same guess, lower because a cycle can end without a straggler) | 0: D6 row 3 vacuous, mutant-4 not launched |
| `catalog: compaction preserves the acked multiset`, examples | 1 to 13 (at or below baseline: the profile lengthens the eventually; the local reshard run took 30m44s against 26m26s for the plain run, and `END_LIMIT` allows up to 1920 s more) | 0: no eventually reached its end, a vacuous run |

**How the D6 rows are checked.** The properties export carries counts and moments only, no process and no details. A per-node, per-file or per-condition part of a row is checked by reading one example's log with `snouty runs logs <run> <input_hash> <vtime>` at its moment; where that is not readable, the part stays local-only evidence and the report says so.

D6 row 1, verbatim: "`resharding: generation append CAS won` hit ≥ 1 in `sdk-cli.jsonl` (the `ravel-cli-ant` process), and the driver's `reshard: a generation activated` hit ≥ 1 from a post-jump write acked at a shard ≥ 4. Miss: unreached means lead or jump arithmetic wrong, run vacuous." Check: both >= 1. Only `ravel-cli-ant` appends a generation, so a CAS won hit is that process by construction (local: hit in `cli` only); the driver's reachable fires only for a token at a shard the increase added, so "at a shard ≥ 4" is by construction too. The append's two Always (`two concurrent reshards on the same base version...`, `a lost race never drops a committed generation`) and `a generation append's history re-read completes` are expected-hit beside it.

D6 row 2, verbatim: "both reached ≥ 1 on each node under `refuse-read t/<h>/l/prov`, each node acked ≥ 1 write before the rule. Miss: fenced unreached means the read refusal did not span the jump or a node had no cached view; the always false is a real fence bug." Check: `resharding: a stale writer routed on a grace-extended view` and `resharding: a writer past the lead horizon was fenced` >= 1, `resharding: grace routing continues only within the lead horizon...` >= 1 true and 0 false. Per node: one example of the driver's fence sometimes, whose details must show `grace_writes` acked and `fenced_writes` `Refused { status: 503 }` for node 0 and node 1 (locally both were).

D6 row 3 (added to the inherited row), verbatim: "`resharding: a token resolved at a shard index above the active count` hit ≥ 1 (straggler burst) and `resharding: a commit token whose record is durable resolves across a reshard` evaluated ≥ 1 and true (forged token). Miss: never evaluated means the forged read did not run; false is the #4520 class." The inherited row stands: `catalog: compaction preserves the acked multiset` and `catalog: a token read returns each acked record of its write exactly once` hold across the reshard (0 false; false after activation is the #4520 class). The driver's `reshard: a forged token for a record never written was refused as unsatisfiable` >= 1 is the evidence the forged read ran.

D6 row 4 (added to the audit row), verbatim: "after the decrease activates, every new write's shards are below 4, and the fold of a post-activation hour names shard 4 and 5 entries." Check: `reshard: a write routed after the decrease activated was acknowledged` >= 1, `reshard: an acknowledged write sits below the shard count it was routed at` 0 false, `reshard: HEAD's footprint named data of a folded post-activation hour at every shard the increase added` >= 1 true.

D6 row 5, verbatim: "`catalog: the system recovers once faults stop` holds after each `thaw-read`." Check: 0 false, and at least one example's details carry `after: thaw-read`.

**The maintenance properties and the tamper races.** `maintenance: a loser observing a divergent input-set hash fails closed and never overwrites the record` is evaluated only in the divergent-record race; expected >= 1 true, 0 false, beside its reachability `maintenance: a loser observed a divergent input set and alarmed` >= 1. `maintenance: a loser reports Converged only when the winner part is observed present in the store` is evaluated where a loser converges, which the plain record race gives (`catalog: a server's compaction record PUT lost to a concurrent compaction`, baseline 95 and 63); expected >= 1 true, 0 false. On clean code the missing-part arm fails closed before that property, so the driver's sometimes is its only evidence. The two tamper sometimes, `catalog: a server's compaction record PUT lost to a record whose input-set hash diverges from its own` and `... whose part is missing from the store`, are expected >= 1 true each (locally `race_s` 96 and 232 of the 870 s budget). Their false evaluations are races where the tamper did not land and are not violations. Triage rule for a false divergent-input always: it is a finding unless that example's race details show one of the two residuals the README names (the release came after the hold cap, or an undo landed between the loser's GET and HEAD while faults ran), read from `tamper_in_place_at_release`, `released_at_age_ms` and `undo`.

**The two cap unreachables.** `catalog: a record race's undo started past its fold-seal deadline` and `catalog: a record race's tamper was still in the store at its undo deadline` are expected 0 hits. A hit is a finding: a tamper may have been carried toward the fold seal, so every catalog result in that timeline is suspect until the timeline is read.

**Risk rows (4).**

- `maintenance: a seeded memo entry is never fresher than its snapshot` (must-hit Always) and its reachability `maintenance: a memo was seeded from a snapshot`. They are evaluated only when a maintain worker warm-starts from a peer's memo snapshot, a timing race the driver does not force. Local rate: hit in 7 of the 8 plain reshard runs on record. #4667's runs hit it in 2 of 3 (the miss had no warm start), #4687's run hit it, the three runs for #4691 hit it, and so did `rs/` at the image ref (in `maint1`), so the last 5 are in a row. It also went unreached in #4667's token-validated mutant run. No Antithesis evidence: the literal is in neither baseline. A miss (0/0) fails the run on this assertion alone and means no timeline warm-started a worker. That is a harness gap whose fix is a forced situation (a maintain restart, like `ravel2`'s), not a product finding. A false evaluation is a finding.
- `gc: a failed hold re-read skips a delete batch` and `server: a graceful drain did not complete cleanly`: other areas, split between the baselines; either outcome is expected. A drain hit is read together with any container-exit failure.

**Expected-unreached, named.**

- `resharding: generation append CAS lost`: no driver action runs two concurrent reshards, so it shows Failing 0/0. A hit means two reshards raced; read that timeline.
- The fence, the decrease with its straggler burst and the forged-token read run only in the eventually, after faults stop (README, "Not here"). Their rows are expected-hit, but each count is bounded by how many timelines reach that part of the eventually, and none of them is exercised under faults. The increase also runs at a drawn sealing jump, so the append and activation rows may be hit under faults.
- ADR-4326 decision 4 lists a third maintain worker for this profile. It is not built (the compose runs `maint1` and `maint2`), so no row depends on three maintainers.
- No property checks `EveryAdmittedWriteInScanSet` (README, "Not here").

### Mutant expectations

ADR-4326 decision 3's pass rule: the mutant's named property fails in its own Antithesis run with a counterexample whose condition is false. A run where the property is never reached does not count. A run where it passes, or where only another property fails, means the property is vacuous, and that blocks the wave. Each run is judged against the smoke's expected set, plus the collateral listed for it below.

**Launch precondition, not yet met.** Every local falsification below predates the image ref, and the maintenance pair has never been falsified under this profile. The resharding pair was falsified under the reshard profile on images at 2ca1a8670 (#4667). The maintenance pair was falsified at fde1bf7c7 under the catalog-maintenance profile (#4666, carried into #4687, which reran plain runs only). Before each mutant's launch, `WORKLOAD_PROFILE=reshard-ownership RAVEL_ANTITHESIS_MUTANT=<name> scripts/local-run.sh <out>` on the `-mutants` images at 4c5d058dd must exit 0 with its `mutant OK` line. That is about 30 minutes of gcp1 per mutant and no Antithesis hours.

| Run | Property that must go false | Local falsification | Collateral expected | Miss |
|---|---|---|---|---|
| mutant-1 `resharding/no-writer-fence` | `resharding: grace routing continues only within the lead horizon of the last refresh; past it the writer stops` | #4667 (and #4663, its closed predecessor, same evidence): exit 0, `violated.tsv` holds only this property, false on ravel1 and ravel2 (`now_ns` past `refreshed_at_ns` + horizon, count 6) | `resharding: a writer past the lead horizon was fenced` unreached (Failing 0/0), since the fence is switched off | the property has 0 counterexamples; or the driver's fence sometimes is 0, so the situation never ran |
| mutant-2 `maintenance/mo-diverge-overwrites-record` | `maintenance: a loser observing a divergent input-set hash fails closed and never overwrites the record` | #4666 and #4687, at fde1bf7c7: exit 0, the property `Always 1 0`, race `race_s` 96, `record_bytes_changed_after_release: true` | `catalog: a published compaction record is never replaced` false as well (the publish.rs comment predicts it) | 0 counterexamples; or the divergent tamper sometimes is 0 |
| mutant-3 `maintenance/mo-missing-part-reports-converged` | `maintenance: a loser reports Converged only when the winner part is observed present in the store` | #4666 and #4687, at fde1bf7c7: exit 0, `violated.tsv` holds only this property, `Always 2 1` (one true from the plain record race), `race_s` 94, undo `put back`; at da1a68011 the false line named the absent part in `absent_parts` | none; true examples from the plain record race beside the counterexamples | 0 counterexamples; or the missing-part tamper sometimes is 0 |
| mutant-4 `resharding/token-validated-against-count` | `resharding: a commit token whose record is durable resolves across a reshard` | #4667: exit 0, false on both nodes for a straggler at shard 4 whose record is durable (`answer: unsatisfiable`); #4663 the same | `catalog: a token read returns each acked record of its write exactly once` and `catalog: compaction preserves the acked multiset` false (the driver's view of the same refusal); `resharding: a token resolved at a shard index above the active count` unreached; true examples of the property from the forged read beside the counterexamples | 0 counterexamples; or the straggler sometimes is 0 |

Any other property that goes false in a mutant run is triaged as in the smoke: a finding unless it is the collateral listed here.

### Budget and stop rules

Billing basis, from `snouty runs usage` and runs.md: recent cap-12 runs billed 14.2 to 15.2 h (the baselines 14.5 and 14.7, perf-smoke 14.9, perf-mutant 14.5, faults-smoke 15.2), and cap-24 runs billed 25.0 to 26.9 h. ADR-4326 decision 7's "billed hours within cap + 1" band no longer holds: overshoot at cap 12 is now 2.2 to 3.2 h.

| Item | h |
|---|---|
| reshard-smoke | 15 |
| reshard-mutant-1 to -4, 15 each | 60 |
| reshard-long | 27 |
| Planned total | 102 |
| Rerun reserve (one cap-12 run) | 15 |
| Wave ceiling | 117 |
| Free now (1200 - 861.3) | 338.7 |
| Left after the planned wave | 236.7 |

D7's 126.5 h also priced the ingest-durability `commit/late-commit-past-seal` run and the lifecycle `complete-ignores-served-set` rerun (29 h at D7's rates). They are outside this wave and this pre-registration.

Stop rules:

1. Smoke, owned Always false (any `catalog:`, `resharding:`, `maintenance:`, `maintain:`, `reshard:` or `clock:` counterexample): no mutant and no long run until it is triaged to a finding, a fix, or a named harness residual.
2. Smoke, D6 row vacuous: fence sometimes 0 means no mutant-1; straggler sometimes 0 means no mutant-4; `resharding: generation append CAS won` or `reshard: a generation activated` 0 means no mutant and no long run. Fix the harness, then rerun the smoke from the reserve.
3. Smoke, an owned or driver row failing outside set-level check 4, or an SDK property count other than 126: triage before any other launch.
4. Smoke, eventually volume: multiset 0 means skip the long run, since it would be vacuous the same way.
5. Seeded-memo miss alone (proposal, needs the orchestrator's yes, since ADR-4326 decision 7's smoke band is "every new SUT assertion reached"): the smoke counts as met for everything else, and the mutants may go ahead. The long run waits for the forced maintain restart. Without that yes, a memo miss stops the wave like rule 2.
6. Billing: a cap-12 run that bills over 17 h, or the long run over 30 h, stops further launches until the bill is explained.
7. Mutants: at most 3 at once (mutants 1 to 3, then 4). A mutant whose property passes stops the remaining mutants and the long run, because a vacuous property blocks the wave (ADR-4326 decision 3).
8. Long run: only after the smoke is met and all four mutants failed their properties.
9. No launch that would take this wave's billed total past 117 h without the owner's go.

### Stamp

**Ref.** main 4c5d058dde769a8f00e038b3f80d16952b2f4cc5, tree 578c38b967013aa664a6f383d9ff0b32b9035b4c ("test(antithesis): gate the seeded-memo property in the reshard runs"). origin/main is now 144819de6, one commit later, which touches only `crates/ravel-sql`, `docs/query-engine.md` and a changelog entry. `launch.sh` refuses unless the checkout equals the images' `org.ravel.antithesis.ref`, so every launch runs from a checkout at 4c5d058dd.

**Images** (built on gcp1, `scripts/build.sh` and `MUTANTS=1 scripts/build.sh`, both rc 0; every image labelled `org.ravel.antithesis.ref=4c5d058dd...`):

| Tag | Image ID | Features |
|---|---|---|
| `ravel-antithesis-server:catalog-maintenance` | sha256:0eda02e5b2ce65b211456acd68aaef5af2344004379365872c3bafddf8cb31fb | sql,antithesis |
| `ravel-antithesis-driver:catalog-maintenance` | sha256:e6e4aacbba9de7dcaa057ecef10355605a8e2c8d2591b227d0cd5979397e8d16 | |
| `ravel-antithesis-server:catalog-maintenance-mutants` | sha256:45d96490606f6cfdc3ebc3897d53112360eeae69761d826c10c3e3ca4c9e68d3 | sql,antithesis,antithesis-mutants |
| `ravel-antithesis-driver:catalog-maintenance-mutants` | sha256:26b90e1f0ab3724aa8e3e3490fc9feff96f0bf31dedd25b9d9c336deb163cbfc | |

Build ok lines (both builds): `ravel-server`, `driver`, `driver-reshard`, `s3-proxy` and `ravel-cli-ant` each "links the shim and carries sancov sections"; `driver` "holds the reshard literal: no", `driver-reshard` "yes"; and `ok ravel-antithesis-server:catalog-maintenance-mutants: /symbols/ravel-server registers the 9 mutants (catalog 5, resharding 2, maintenance 2)`.

**Local runs at those images** (gcp1, `/home/builder/logs/pre/`):

- `WORKLOAD_PROFILE=reshard-ownership scripts/local-run.sh`, 06:24 to 06:55Z, rc 0: `OK: no always-type assertion violated or unhit, every required assertion held, every container healthy, every clock jumped`. `assertions: 126, would fail: 39`. `violated.tsv`, `never-hit-owned.tsv` and `never-hit-pending.tsv` 0 bytes. `sdk-cli.jsonl: collected from ravel-cli-ant`. s3proxy: 0 lines with `Too many open files`, open-file limit 65536. Clock jump 366210 s. Reshard figures: activation write at shards 0 to 5 (hour 498754); fence with both grace writes acked and both fenced writes `Refused { status: 503 }`; straggler burst 40 sent, 40 acked; forged token at shard 4, active count 4, refused unsatisfiable; footprint named shards 4 and 5 (79 tokens); tamper races `race_s` 232 (missing part) and 96 (divergent record) of 870, both `held_status` 412 and `tamper_in_place_at_release: true`; seeded memo held in `maint1`.
- Plain `scripts/local-run.sh` (catalog-maintenance), 06:55 to 07:21Z, rc 0: the same OK line. `assertions: 119, would fail: 48`. `violated.tsv` 0 bytes. No `sdk-cli.jsonl`. 0 `Too many open files`. Clock jump 361011 s.

**Baseline**: `bc8e756efa914bbe84e83a62abf95098-67-29` (14.5 h) and `9e1b5871bdc5889df2eba28e374461b2-67-29` (14.7 h), exported 2026-11-24 with `snouty runs properties --json`.

**Expected set**: `expected-set.tsv`, 126 rows plus a header, sha256 1a48f198aa971a1c8c2952630ad444268f2feabc29a88066dac01fa26e7f995c, built from `rs/` and the two exports by `tools/expected_set.py`.

**Budget**: `snouty runs usage --rollup month` shows 2026-11: 46 runs, 861.3 h, and 0 in every earlier month; 1200 - 861.3 = 338.7 h free. No run is in progress (`snouty runs list`).

**Verified state, 2026-11-24T09:04Z**:

- Verified: all four image IDs and their ref label, re-inspected on gcp1 and unchanged since the build. The 9 mutant names registered in the mutant server image (build check), the four of this wave among them. Both local runs' OK lines and 0-byte `violated.tsv`. The baseline exports and bills as above. gcp1 at 414 GB free, load 0.08.
- Not verified, and required before launch: the images pushed to the Antithesis registry and `snouty validate` on the reshard config; each mutant's local falsification at 4c5d058dd under this profile (mutant section); `snouty runs usage` rechecked just before each launch.

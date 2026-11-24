## Re-registration: reshard-long (#4340)

DRAFT, not posted.

### Summary

This re-registers the reshard-ownership long run that the amendment (https://github.com/NOFireAI/ravel/issues/4340#issuecomment-7129615266) deferred. Its base is the wave pre-registration (https://github.com/NOFireAI/ravel/issues/4340#issuecomment-7127390349): the smoke's expected set, set-level checks, D6 rows, triage rules and stop rules all carry over unchanged unless a section below says otherwise. What changes:

- **Images.** New images at main 6b362218e, which carries #4697's drawn decrease rule (`decrease_drawn`). Stamp below.
- **Cap.** 24, planned about 27 h, stop at 30 h (rule 6).
- **Volume bands.** Scaled for cap 24 from the smoke's figures.
- **New bands.** The drawn decrease, its straggler read and the forged read under faults.
- **Claims.** The decrease, the straggler path and the forged read may now be claimed under faults. The fence stays eventually-only.

| Run | Selector | Cap | Planned h | Passes when | Miss, or stop |
|---|---|---|---|---|---|
| reshard-long | plain, `WORKLOAD_PROFILE=reshard-ownership`, test name `ravel-reshard-ownership` | 24 | 27 | set-level checks 1 to 5 met; every D6 row met; every volume band below met | any owned Always false, a D6 row vacuous, or a volume band missed. The fault-time rows F1 to F7 gate only the fault-time claims, not the run |

Rule 8 holds: the smoke was met (`fe4abcfa…`) and all four mutants passed (`ecc94304…`, `2196edfa…`, `00f89f53…`, `26cedb3c…`). All four mutants were falsified again locally on the new mutant images, each with its `mutant OK` line (stamp). The plain local reshard gate missed only the seeded-memo pair, in both attempts (stamp and the expected-set section).

### Expected set

The same `expected-set.tsv` as the smoke: 126 SDK properties, sha256 `1a48f198aa971a1c8c2952630ad444268f2feabc29a88066dac01fa26e7f995c` (rechecked 2026-11-24). Its classes are unchanged:

| Class | Always | Reachable | Sometimes | Unreachable | AlwaysOrUnreachable | Total |
|---|---|---|---|---|---|---|
| expected-hit | 28 | 30 | 26 | 0 | 0 | 84 |
| expected-unreached | 6 | 20 | 3 | 8 | 1 | 38 |
| risk | 1 | 3 | 0 | 0 | 0 | 4 |

#4697 adds no assertion literal. The plain local reshard run at the new images has the same 126 rows as the run at 4c5d058dd: its `assertions.tsv` message column is identical, and so are its type and message columns (stamp). Set-level check 1's 126 therefore stands.

Set-level checks 1 to 5 apply as written for the smoke. Check 4's predicted failing total stays 29 to 39 (the smoke had 31).

**The seeded-memo pair is a required row** in this run (owner decision, delegated, under stop rule 5's conditions): `maintenance: a seeded memo entry is never fresher than its snapshot` must be hit with >= 1 true and 0 false, and `maintenance: a memo was seeded from a snapshot` must be hit (>= 1). The smoke hit both 413 times under Antithesis. A miss (0/0) in the long run is a harness gap, not a finding, unless `maintenance: a memo snapshot was written` or `maintenance: a worker heartbeat was written` is also 0; then it is triaged as a possible snapshot or heartbeat defect. A false evaluation of the Always is a finding (stop rule 1). Locally at these images, both plain reshard runs missed the pair with snapshots and heartbeats at 2 each (stamp). That is the local warm-start timing gap the risk row names. A script-only `local-run.sh` change that forces a maint restart is in progress separately; it changes no image.

### Volume bands, cap 24

Source: the true column of `snouty runs properties --json`, as in the smoke's `properties.tsv`. Cap-24 runs have billed 25.0 to 26.9 h against 14.2 to 15.2 h at cap 12, so this run gets about 1.75 times the smoke's compute. Each centre below is the smoke's figure times 1.75. A figure inside its band is met. A figure outside it is a miss, to be explained before the report claims the row. A figure at 0 on a D6 row is a vacuous row (stop rule 2).

Every row below is counted in the eventually, so it scales with the number of timelines that reach that step of the eventually, not with cap hours as such. The smoke reached the eventually's fault stop in 28 timelines (the count of `eventually: <ANTITHESIS_STOP_FAULTS> 3600 exited` lines). A long run that reaches the eventually less often will undershoot every row together. The report states the stop-line count beside the bands so that this case is visible.

| Figure | Smoke (cap 12) | Centre | Band (met) | Miss |
|---|---|---|---|---|
| `catalog: the system recovers once faults stop`, true | 58 | 100 | 30 to 240 | below 30 or above 240; any false is set-level check 2 |
| `reshard: the clocks passed the writers' lead horizon while the provisioning record's reads were refused`, true (the fence) | 19 | 33 | 10 to 80 | 0: D6 row 2 vacuous; 1 to 9, or above 80: volume miss |
| `reshard: a write routed before a decrease landed at a retired shard in its activation hour`, true (stragglers, both `reason` values) | 20 | 35 | 10 to 100 | 0: D6 row 3 vacuous; 1 to 9, or above 100: volume miss |
| `catalog: compaction preserves the acked multiset`, true | 8 | 14 | 4 to 32 | 0: no eventually reached its end, a vacuous run (stop rule 4's reasoning); 1 to 3, or above 32: volume miss |
| `reshard: HEAD's footprint named data of a folded post-activation hour at every shard the increase added`, true | 3 (of 27 evaluations) | 5 | 1 to 30 | 0 true: D6 row 4's footprint part vacuous |
| `eventually: <ANTITHESIS_STOP_FAULTS> 3600 exited` lines (`snouty runs search --count`) | 28 | 49 | 15 to 120 | outside the band: explains a miss on every row above |

Notes:

- The straggler band counts both `reason` values. A timeline where the drawn rule read a straggler back skips the eventually's own decrease cycles, so the drawn rule moves examples from the eventually to before it rather than adding them. The drawn share has its own band below.
- The footprint row is the one most likely to miss: it was 3 true of 27 in the smoke, and 0 true of 30 in mutant 3.
- The multiset row went unreached in mutants 2, 3 and 4, whose mutants corrupt record, part or token state. This run is plain, so the smoke's figure is its basis.

### The drawn decrease under faults

The drawn rule, `decrease_drawn`, fires with probability 0.02 per call once its preconditions hold (the sealing jump reached the driver and the increase's activation hour began), at most once per timeline. It adds no literal. Its evidence is `reason` `drawn` in the details of two driver assertions, and its moment relative to the fault stop. Other drawn rules also write `"reason": "drawn"` into their details (the clock jumps and several `catalog:` token reads, in the plain local run's `sdk-driver.jsonl`), so every query below selects the assertion by its message first and reads `details.reason`, not a substring of the event.

"Under faults" means: before the `eventually: <ANTITHESIS_STOP_FAULTS> 3600 exited` line of the same timeline. Faults may run then; it does not mean a fault was active at that instant. The check is a fold over each timeline that sets a flag at that line and keeps the target assertion's true hits seen before it. The queries are in `runs/long-prereg/queries/` and are quoted in full at the end of this section.

**Controls, on the smoke** (`fe4abcfa…`, images at 4c5d058dd, no drawn rule):

- The under-faults fold, for the three rows together, matched 0 events before the stop line and 124 after it. 124 is exactly the smoke's true total for the three rows (20 + 52 + 52), so the fold sees every hit and puts each on the right side of the stop. Each per-row fold below matched 0.
- The `reason` field does not exist at 4c5d058dd, so the drawn predicate `details.reason == "drawn"` cannot match there (0). The same predicate shape on a key that does exist, `details.planned == 40` on the straggler row, matched 20, the row's full true count. The local forced run's `sdk-driver.jsonl` shows `"reason": "drawn"` in the straggler row's details (stamp).
- A log-line predicate, `output_text` starting with `reshard: decrease burst: `, matched 20 on the smoke, one per burst. The drawn log-line queries below use the same form and matched 0.

**Three log-line counts**, which do not depend on assertion details:

- S, starts: lines starting `reshard: drawn decrease: starting`.
- C, completed: lines `reshard: drawn decrease: <n> straggler(s) found, <m> read back`, any m.
- R, read back: the same lines with m >= 1.

S above 0 with F1 at 0 means the details query is wrong, not that the draw never fired. S − C is the number of drawn cycles cut off before their reads.

**A residual seen locally, which can also happen under Antithesis.** The drawn rule releases the driver's write gate after its burst and then takes a read gate for the straggler reads. An eventually that is waiting on the gate (forced locally by `WORKLOAD_EVENTUALLY_AFTER_SECS`; drawn at 0.003 per call under Antithesis, `run_eventually` in `main.rs`) takes it first and halts the drawn worker. The burst and its straggler assertion (with `reason` `drawn`) have then run under faults, but the straggler reads and the forged read have not, and the eventually runs its own decrease cycle (`drawn_reached` stays false). The forced local run at 1500 s showed exactly this (stamp). So F1 and F2 can be met while F3 to F5 are 0, and C below S counts how often.

| Row | Check | Predicted | Miss |
|---|---|---|---|
| F1. `reshard: a write routed before a decrease landed at a retired shard in its activation hour`, true with `details.reason == "drawn"` (`q-straggler-drawn`, `--count`) | >= 1, and one example read with `snouty runs logs <run> <input_hash> <vtime>`: `reason` `drawn`, `stragglers` non-empty, and the timeline's `reshard: drawn decrease: starting` line and the burst both before that timeline's stop line | 1 to 40 (a guess: the 0.02 draw needs a drawn sealing jump first) | 0 with S = 0: the 0.02 draw never fired in the run. 0 with S >= 1: every drawn burst found no straggler, or the query is wrong (read one start's timeline). Either way the fault-time claim is vacuous, and the report claims nothing under faults for the decrease, the straggler path or the forged read |
| F2. The same row, true before the stop line (`q-uf-straggler`) | >= 1 | equal to F1 | 0 while F1 >= 1: every drawn example came after the stop, which the rule's preconditions should prevent; triage the rule |
| F3. `resharding: a token resolved at a shard index above the active count`, true before the stop line (`q-uf-resolved`) | >= 1 | about 2 per read-back timeline R (the export counts one example per call site and process: the smoke's 52 is 2 x 26, two nodes over 26 forced-read timelines) | 0: no straggler read resolved under faults; no claim for this row. Read S, C and R: C < S is the cut-off residual above, and C = S with R = 0 is the retried-straggler residual #4697 names (every straggler needed a retry). Neither is a product finding |
| F4. `resharding: a commit token whose record is durable resolves across a reshard`, true before the stop line (`q-uf-durable`), and 0 false anywhere | >= 1 true | at least F3, plus the drawn forged reads | 0 true: the straggler and forged reads never evaluated it under faults; no claim for this row. Any false: set-level check 2, the #4520 class |
| F5. `reshard: a forged token for a record never written was refused as unsatisfiable`, true before the stop line (`q-uf-forged`), with `q-forged-drawn` (`details.reason == "drawn"`) as the cross-check | >= 1 before the stop | about 1 per completed cycle C (one example per call site and process: the smoke's 26 is one per forged-read timeline, not one per node) | 0 before the stop while C >= 1: the drawn forged read holds no gate, so an eventually can stop faults while it retries, or all three attempts got 503 or no answer; the forged-read claim is not made. A `q-forged-drawn` hit after the stop line is that case and does not count |
| F6. `reshard: drawn decrease: starting` more than once in one timeline (`q-drawn-twice`) | 0 | 0 | >= 1: the once-per-timeline claim is broken; a harness finding |
| F7. S, C and R (`q-drawn-starts`, `q-drawn-completed`, `q-drawn-readback`) | reported | S >= F1's timelines; C <= S; R <= C | reported, not a miss on its own; they decide which miss text above applies |

The two `resharding:` rows are the server's and carry no `reason`. F3 and F4 tie them to the fault phase by their moment, not by the drawn rule. Before this run, no reshard path evaluated them before the stop (the smoke's 0), so any hit before the stop comes from the drawn rule.

Queries (each run as `snouty runs search --count <run> '<query>'`; the files are in `runs/long-prereg/queries/`):

```
q-straggler-drawn: filter(ev => ev.antithesis_assert?.hit && ev.antithesis_assert.condition && ev.antithesis_assert.message == "reshard: a write routed before a decrease landed at a retired shard in its activation hour" && ev.antithesis_assert.details?.reason == "drawn")
q-forged-drawn:    the same, for "reshard: a forged token for a record never written was refused as unsatisfiable"
q-uf-<row>:        fold((s, ev) => { const t = ev.output_text || ""; if (/^eventually: .* 3600 exited/.test(t)) { return [[], {"stopped": true}]; } const a = ev.antithesis_assert; if (!s.stopped && a && a.hit && a.condition && a.message == "<row>") { return [[ev], s]; } return [[], s]; }, {"stopped": false})
q-drawn-twice:     fold((s, ev) => { const t = ev.output_text || ""; if (t.startsWith("reshard: drawn decrease: starting")) { const n = s.n + 1; return [n > 1 ? [ev.add_fields({n: n})] : [], {"n": n}]; } return [[], s]; }, {"n": 0})
q-drawn-starts:    filter(ev => (ev.output_text || "").startsWith("reshard: drawn decrease: starting"))
q-drawn-completed: filter(ev => /^reshard: drawn decrease: \d+ straggler\(s\) found, \d+ read back/.test(ev.output_text || ""))
q-drawn-readback:  filter(ev => /^reshard: drawn decrease: \d+ straggler\(s\) found, [1-9]\d* read back/.test(ev.output_text || ""))
```

The two regexes were tested in node against the #4697 proof line `reshard: drawn decrease: 3 straggler(s) found, 3 read back` (both match) and against `... 2 straggler(s) found, 0 read back` (completed matches, read-back does not). No local run at 6b362218e printed such a line (stamp).

### Claims

Per the amendment, the long run's report may claim, each only where its band above is met:

- the 6 to 4 decrease with its straggler burst under faults (F1, F2);
- the straggler path, a token resolved above the active count, under faults (F3, F4);
- the forged-token read refused as unsatisfiable under faults (F5), with the durable-token Always holding (F4, 0 false);
- everything the smoke could claim: the mutants' falsifiability, deterministic reachability, the increase, the activation writes and grace routing under faults, and the catalog races across both reshards from fault-varied pre-states.

It does not claim the stale-writer fence under faults. The fence stays eventually-only, for the amendment's two reasons: its recovery Always assumes faults have stopped, and its SUT Always already evaluates under faults through natural read failures and the drawn 9000 s sealing jump.

### Budget and stop rules

| Item | h |
|---|---|
| Wave billed so far (`snouty runs usage`): smoke 15.2, mutants 14.9, 15.0, 14.6, 14.6 | 74.3 |
| reshard-long, planned | 27 |
| Wave total after it | 101.3 |
| Wave ceiling (rule 9) | 117 |
| Free now (`snouty runs usage --rollup month`: 2026-11, 51 runs, 935.5 h; 1200 - 935.5) | 264.5 |
| Free after the long run | 237.5 |

Cap-24 runs have billed 25.0 to 26.9 h. Stop rule 6 applies: a bill over 30 h stops further launches until it is explained.

The wave's stop rules carry over. For this run:

- Rules 1 and 3: any Always or AlwaysOrUnreachable counterexample, an owned or driver row failing outside set-level check 4, or an SDK property count other than 126, is triaged before any other launch.
- Rule 2: a D6 row at 0 makes the run's D6 claim vacuous for that row. Proposed, not carried over: the run is not rerun from the reserve without the owner's go, since the reserve (15 h) is a cap-12 run.
- Rule 5: the seeded-memo pair is a required row here (above).
- Rule 10: container exits outside the drain-timeout shape, `No Antithesis errors` or session errors failing, or an incomplete run, are triaged; an incomplete run is never counted met.
- New: F1 at 0 is not a stop and not a product finding. The run is then judged as a cap-24 smoke, and the report states that the fault-time claim is vacuous.

### Stamp

**Ref.** main 6b362218e33decc01c73f22ef6c4483dad6b58e7, tree c47d305079aa62da2d6dcdd9c7cdd5469f70dcdc ("fix(antithesis): skip the eventually's decrease only after a read"). It carries #4697 (cbb41eb2c and 6b362218e). The worktree `/home/builder/wt/long` on gcp1 is at that commit, clean (`git status --porcelain` 0 lines) and holds no root-owned file, rechecked after the last local run (2026-11-24T23:39Z), when the four image IDs were also re-inspected and unchanged.

**Images** (gcp1, `scripts/build.sh all` and `MUTANTS=1 scripts/build.sh all`, both rc 0, 2026-11-24T17:56Z to 18:21Z; every image labelled `org.ravel.antithesis.ref=6b362218e33decc01c73f22ef6c4483dad6b58e7`, and each local run re-inspected all four IDs and found them unchanged):

| Tag | Image ID | Features |
|---|---|---|
| `ravel-antithesis-server:catalog-maintenance` | sha256:14ad88ad75fb7a405e0e8b25ddae0af33afbc620633314daf4ade514ca5c400e | sql,antithesis |
| `ravel-antithesis-driver:catalog-maintenance` | sha256:f14ce08a5484cfee8a371d295ca4667b4c82ad4eabf559fe6ec9e837994ea911 | |
| `ravel-antithesis-server:catalog-maintenance-mutants` | sha256:185245a7c448035f99a13eccfd0be8a24aa41410c8ba5df15fa3c43b992ddeef | sql,antithesis,antithesis-mutants |
| `ravel-antithesis-driver:catalog-maintenance-mutants` | sha256:e140dc26413f1ffc7ae95b181c9872d7d56d3b8c7135a7ac7f58b7b4d791c4fb | |

Build ok lines (both builds): `ravel-server`, `driver`, `driver-reshard`, `s3-proxy` and `ravel-cli-ant` each "links the shim and carries sancov sections"; `driver` "holds the reshard literal: no", `driver-reshard` "yes"; and `ok ravel-antithesis-server:catalog-maintenance-mutants: /symbols/ravel-server registers the 9 mutants (catalog 5, resharding 2, maintenance 2)`.

**Local runs at those images** (gcp1, `/home/builder/logs/long/`, as root, each under `flock /root/.gcp-docker.lock`):

- Plain `WORKLOAD_PROFILE=reshard-ownership scripts/local-run.sh`, two attempts, **both rc 1**, on the seeded-memo risk row only:
  - `rs-plain-1`, 18:21 to 18:51Z: `assertions: 126, would fail: 41`, `FAIL: always-type assertion never hit: maintenance: a seeded memo entry is never fresher than its snapshot`. The would-fail set is the 4c5d058dd run's 39 plus the memo pair.
  - `rs-plain2`, 22:11 to 22:43Z: `assertions: 126, would fail: 39`, the same FAIL line. Against 4c5d058dd: plus the memo pair, minus `catalog: HEAD was overwritten with garbage through raw S3` and `catalog: inspect reported a corrupt HEAD` (both hit this time).
  - The four memo rows, the same in both (`assertions.tsv`, type, hits, true):
    ```
    Reachable 2 2 maintenance: a memo snapshot was written
    Reachable 0 0 maintenance: a memo was seeded from a snapshot
    Always    0 0 maintenance: a seeded memo entry is never fresher than its snapshot
    Reachable 2 2 maintenance: a worker heartbeat was written
    ```
    This is the shape the risk row names (no timeline warm-started a worker), and nothing else failed. Both forced runs hit the pair (1 and 1) beside snapshots 2 and heartbeats 2. The `assertions.tsv` message column of both is identical to the 4c5d058dd run's (`/home/builder/logs/pre/rs/assertions.tsv`, 126 rows), and so are the type and message columns together.
- Plain catalog-maintenance `scripts/local-run.sh`, 18:51 to 19:17Z, rc 0: `OK: no always-type assertion violated or unhit, every required assertion held, every container healthy, every clock jumped`. `assertions: 119, would fail: 46`.
- Forced drawn, `WORKLOAD_FORCE_DRAWN_DECREASE=1 WORKLOAD_PROFILE=reshard-ownership scripts/local-run.sh <out> <secs>`, two attempts, both print the OK line, and neither ran the drawn straggler reads or the drawn forged read:
  - `rs-forced-1`, 1500 s (the README's figure), 19:17 to 20:01Z, local-run rc 0: `OK: no always-type assertion violated or unhit, every required assertion held, every container healthy, every clock jumped`, `assertions: 126, would fail: 37`, message and type columns identical to 4c5d058dd's. The drawn lines:
    ```
    reshard: drawn decrease: starting (forced by WORKLOAD_FORCE_DRAWN_DECREASE: true)
    reshard: decrease burst: {"acked":40,"activation_hour":498830,"cycle":0,"planned":40,"reason":"drawn","sent":40,...,"stragglers":[244,246,247]}
    eventually (forced by WORKLOAD_EVENTUALLY_AFTER_SECS): stopping faults
    ```
    The straggler assertion fired with `"reason": "drawn"` (`sdk-driver.jsonl`). The forced eventually was waiting on the driver's gate and took it as the drawn rule released it, so the drawn worker halted before its reads: no `straggler(s) found, <m> read back` line, no drawn `forged token read`, and the eventually ran its own decrease cycle (`"reason":"eventually"`) and forged read. This is the cut-off residual in the drawn section.
  - `rs-forced2`, 2400 s (more than the README's figure, chosen as a guess to give the sealing jump more time), 22:43 to 23:38Z, local-run rc 0: the same OK line, `assertions: 126, would fail: 37`, columns identical. No drawn line at all: the drawn 9000 s sealing jump never came before the forced eventually (the only 9000 s jump and the increase are the eventually's), so the rule's preconditions never held.
  - So at 6b362218e the drawn burst and its straggler assertion have local evidence (rs-forced-1), and the drawn straggler reads, the eventually's skip after a read and the drawn forged read (F3 to F5's paths) have none. #4697's proof showed them at bcb358685, before 6b362218e changed the skip-after-read logic. Both attempts are stopped here, without a third.
- Mutants, under `WORKLOAD_PROFILE=reshard-ownership` on the `-mutants` images, each rc 0:
  - `mutant OK: resharding/no-writer-fence violated "resharding: grace routing continues only within the lead horizon of the last refresh; past it the writer stops"` (20:01 to 20:33Z). `violated.tsv` holds only that property; `resharding: a writer past the lead horizon was fenced` never held, as predicted.
  - `mutant OK: maintenance/mo-diverge-overwrites-record violated "maintenance: a loser observing a divergent input-set hash fails closed and never overwrites the record"` (20:33 to 21:04Z). `violated.tsv` also holds `catalog: a published compaction record is never replaced`, the predicted collateral.
  - `mutant OK: maintenance/mo-missing-part-reports-converged violated "maintenance: a loser reports Converged only when the winner part is observed present in the store"` (21:04 to 21:38Z). `violated.tsv` holds only that property.
  - `mutant OK: resharding/token-validated-against-count violated "resharding: a commit token whose record is durable resolves across a reshard"` (21:38 to 22:11Z). `violated.tsv` also holds `catalog: a token read returns each acked record of its write exactly once` and `catalog: compaction preserves the acked multiset`, the predicted collateral.

Seeded-memo local rate at these images, `maintenance: a memo was seeded from a snapshot` hit or not, over every reshard-profile run: rs-plain-1 0, rs-plain2 0, rs-forced-1 1, m-fence 2, m-diverge 1, m-missing 1, m-token 0, rs-forced2 1. At 4c5d058dd it was 7 of 8 plain runs. The smoke under Antithesis hit it 413 times.

**Budget**: `snouty runs usage --rollup month` on gcp1, 2026-11-24: 2026-11, 51 runs, 935.5 h, and 0 in every earlier month; 1200 - 935.5 = 264.5 h free. No run in progress (`snouty runs list`).

**Not verified, and required before launch:**

- The plain reshard local gate printed FAIL at these images on the seeded-memo pair only (2 of 2), with snapshots and heartbeats present. By owner decision (delegated) it is the known local warm-start gap, with a script-only `local-run.sh` fix in progress separately and no image change. The pair stays a required row of the long run (expected-set section).
- The drawn straggler reads and drawn forged read have no local evidence at 6b362218e (forced runs above).
- The images pushed to the Antithesis registry, and `snouty validate` on the reshard config, from a checkout at 6b362218e.
- `snouty runs usage` rechecked just before the launch.

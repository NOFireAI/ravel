## Pre-registration: late-commit fence runs, plain and mutant (#4561)

### Summary

Two Antithesis runs of the catalog-maintenance workload (default profile, test name `ravel-catalog-maintenance`, `-mutant` for the mutant run), both from images built at main d23dbb245, which holds #4715 (the late-records race) and #4718 (the fence model and the mutant). The basis is ADR-4326's late-replay amendment (#4707) and its late-fence mutant amendment (#4717, corrected in 8289d383b), which sets this mutant's band and carries the owner's go for the run.

| Run | Selector | Cap | Planned h | Passes when | Miss |
|---|---|---|---|---|---|
| r4561-plain | plain | 12 | 15 | the race is reached (`catalog: a server planned a bucket holding a late commit record` >= 1 true); both fence always-assertions hold (`catalog: a server's compaction record names no late commit record` and `maintain: a published record names no late commit record`, each >= 1 true and 0 false); `maintain: a plan left a late commit record out` >= 1 true; every Always in any area has 0 counterexamples; the failing set is inside the expected set | the race unreached; any Always counterexample; an owned expected-hit row unreached |
| r4561-mutant | `catalog/plan-takes-late-record` | 12 | 15 | all three: the race reached (>= 1 true), the driver's `catalog: a server's compaction record names no late commit record` >= 1 counterexample, and the SUT's `maintain: a published record names no late commit record` >= 1 counterexample | any of the three missing (table under "Mutant") |
| Total | | 24 | 30 | | no rerun inside this reservation |

Both runs launch together. Free budget: 237.2 h (1200 - 962.8 billed, `snouty runs usage --rollup month`).

The expected set is `expected-set.tsv`: 121 SDK assertion messages, 72 expected-hit, 47 expected-unreached, 2 risk, with a mutant column that differs from the plain one on three rows.

### What the runs show

On main the fence is ADR-4561 R4: a plan leaves out every commit record the store stamped at or after its bucket's late-commit cutoff `C(H)`. The plain run checks it from both sides under faults: the SUT at every publish (`maintain: a published record names no late commit record`, which compares the record's inputs with the listing's classification) and the driver on the stored record of the one bucket the race builds (`catalog: a server's compaction record names no late commit record`). The mutant makes the plan take late records while the listing still classifies them, so both checks must go false. Requiring both shows that each check can fail; either alone could pass with the other vacuous (the amendment's all-of rule).

Both properties go false only through the late-records race. The code at d23dbb245 says so and the local mutant run agrees. A plan publishes only from at least two commit records (`DEFAULT_MIN_COMPACTION_INPUTS`). Each seeded bucket holds one record, late, so no plan of it publishes with or without the mutant. The live hours' records are on time. In the local mutant run the SUT property went false once, and that evaluation's `late_commit_key` is the replayed key, in the race's bucket.

### Baselines and what each row rests on

| Export | Profile, ref | Billed | Holds |
|---|---|---|---|
| `bc8e756efa914bbe84e83a62abf95098-67-29` (nofence) | catalog-maintenance, 64760f617 | 14.5 h | the catalog, clock and other-area rows; no `maintain:` literal |
| `9e1b5871bdc5889df2eba28e374461b2-67-29` (fence-exact) | catalog-maintenance, 970c6037b | 14.7 h | as nofence, plus both `maintain:` fence literals |
| `fe4abcfa6d7f5a4429d801c8da9439e2-67-29` (reshard smoke) | reshard-ownership, 4c5d058dd | 15.2 h | the tamper races, the maintenance literals, the memo rows |
| `8532623cd79865a4ac4040da2315c709-67-29` (reshard long) | reshard-ownership, 6b362218e | 27.2 h | as the smoke |

The two catalog-maintenance exports predate the tamper races, the early multiset check (#4705), the maintain restart (#4703), and the race's two driver literals. The reshard exports hold the first three of those but run another driver, which reshards. So:

- the catalog, clock and other-area rows are banded by all four exports;
- the maintenance rows (memo, heartbeat, loser, tamper) by the two reshard exports only;
- the two `maintain:` fence rows by fence-exact (285/0 and 451 true) and the reshard exports (289/0 and 507/0; 449 and 794 true);
- the `resharding:` rows by none: this profile reshards nothing, so they are expected unreached whatever the reshard exports show;
- the race's two driver rows by the local loop only. No Antithesis run has held them.

### Expected set (both runs)

Source: the plain local run at the image ref (`lr-plain-b`, 121 messages in `assertions.tsv`, every `sdk-*.jsonl` including the `hit:false` registration lines; no `sdk-cli.jsonl`, since only `driver-reshard` calls `ravel-cli-ant`), the mutant local run (`lr-mut`, the same 121 messages), and the four exports. One row per message in `expected-set.tsv`; columns `class` and `band_or_reason` are the plain run, `mutant_class` and `mutant_band` the mutant run (`as plain` where they agree).

| Class | Always | Reachable | Sometimes | Unreachable | AlwaysOrUnreachable | Total |
|---|---|---|---|---|---|---|
| expected-hit | 25 | 25 | 22 | 0 | 0 | 72 |
| expected-unreached | 10 | 24 | 4 | 8 | 1 | 47 |
| risk | 0 | 2 | 0 | 0 | 0 | 2 |

Expected-hit holds the 67 messages hit locally and 5 unhit locally that every export reached (all other areas, thin in places: `logs ingest: a write committed on some shards before a sibling shard failed` at 3 to 5). Every one of the 29 `REQUIRED` literals of `local-run.sh` is expected-hit. Expected-unreached is 28 other-area messages at Failing 0/0 in every export, 10 `resharding:` messages unhit locally, the 8 Unreachable and the 1 AlwaysOrUnreachable (the last 9 show Passing at 0/0). Risk is `gc: a failed hold re-read skips a delete batch` and `server: a graceful drain did not complete cleanly`, each split across the exports; either outcome is expected, and a drain hit is read together with any container exit.

**Set-level checks** (each is a miss if false):

1. The export holds exactly 121 SDK assertion properties, plus `Antithesis SDK version is supported` and `main/first_start_workload` (123 rows in the triage's SDK table). The mutant images register no extra literal (`inv_mutant!` emits only a const assert), so 121 holds for both runs. Another count is reconciled before triage.
2. Plain run: every Always and AlwaysOrUnreachable has 0 counterexamples, whatever its area. Mutant run: the same, except the two fence properties.
3. Every Unreachable has 0 hits, in both runs.
4. Owned rows (`catalog: `, `clock: `) and the rows `local-run.sh` requires by name (`maintenance:`, `maintain:`, `commit: a commit token's stale-answer evidence read evaluated`): each expected-hit row is reached, and none of them is failing (in the mutant run, except the two fence properties). An other-area expected-hit row that goes unreached is noted, never a miss. Predicted failing total, plain: 38 (the expected-unreached Always, Reachable and Sometimes rows) to 45 (plus both risk rows and the 5 thin other-area rows). Mutant: the plain figure plus the 2 fence properties, 40 to 47.
5. Outside the SDK: `No unexpected container exits` passes, or fails only with exits that are the drain timeout under a proxy outage, checked from the exit logs. `No Antithesis errors` and `No Antithesis session errors` pass.

**Bands.** Every expected-hit row's band is >= 1 (a sometimes: >= 1 true; its false evaluations are not violations), and every Always row's is 0 false. The TSV quotes the local hits and each export's counts beside each band. Volume predictions, recorded so that a surprise shows:

| Figure | Predicted | Miss |
|---|---|---|
| `catalog: a server planned a bucket holding a late commit record`, true, each run | 1 to 20 (basis below) | 0: the race unreached |
| `maintain: a published record names no late commit record`, evaluations, plain | 150 to 600 (one per compaction publish; fence-exact 285, reshard smoke 289, reshard long 507 at cap 24) | below 50 |
| `maintain: a plan left a late commit record out`, true, plain | 150 to 900 (fence-exact 451, reshard smoke 449, reshard long 794 at cap 24; mostly the seeded hours' late records) | 0 |
| `catalog: compaction preserves the acked multiset`, true | 1 to 30 (the early check right after recovery, a guess from the 8 to 14 lineages per run that lived past 120 s after their stop; the final check adds 0 or 1 lineage) | 0: the early check never ran, a harness miss and not a product finding |
| `catalog: the system recovers once faults stop`, true | 20 to 100 (reshard runs 25 to 58, catalog-maintenance 34 and 49) | below 10 |

### How the race is reached, and why its band is a guess

The race has never run in Antithesis. It has two ways in.

- **The drawn rule** `race_late_records`: weight 1 in the main group, then `weighted_booleans(0.02)`, only once HEAD is known and until one race reached its situation. That is the unreadable-HEAD sweep's rule and rate. In the six reshard-ownership runs that sweep's situation, `catalog: HEAD stayed unreadable while the clocks passed the protection horizon`, had 13 to 23 true examples. The unfolded-record sweep, at the same rate, had 3 to 18. That most of those came from the drawn rule is an inference, not a measurement: the #4340 lineage count covers four of the six runs and counts lineages that lived 1630 s past their stop (0 or 1 per run), not lineages that reached the eventually's sweep near +1270 s, and an export's examples include branches of one lineage. The race has more ways to fail under faults than a sweep: the capture within 30 s, the server's abandonment (13.1, 14.1 and 17.4 s in the three local runs, capped at 1028 s), two more on-time writes, and a maintenance publish within 180 s, all before the fold seals the hour. Hence a lower band than the sweeps'.
- **The eventually**: up to three races, after the unreadable-HEAD sweep. In the reshard smoke that sweep ended 1421.5 s after the stop line. About 290 s of that are reshard steps this profile does not run, and this profile adds the early multiset check (about 60 s in Antithesis), so the race starts near +1190 s and reaches its situation near +1260 to +1450 s. In the four reshard runs investigated for #4340, 0 or 1 lineage per run lived 1630 s past its stop, and the long run's deepest driver line came at +1200 s. Expected contribution: 0 or 1 lineage per run.

Locally the plain runs reached the race in the eventually (both of them) and the mutant run reached it through the drawn rule.

The band, 1 to 20 true examples per run, is in export examples, so it counts branches, not independent timelines.

A race count of 0 in either run means the race was unreached, not a product finding. It blocks, and the run is not rerun blind: the next step is a harness change (for example, an earlier place in the eventually), with its own local proof and the owner's go.

### Mutant

ADR-4326 decision 3 and the late-fence mutant amendment: the mutant passes only when both properties are false, each with a counterexample, and the race is reached. The local proof at these images ran first (stamp below). A property with no evaluation shows as Failing at 0/0, the same as one that went false; only a counterexample count of 1 or more counts.

| Race sometimes, true | Driver property, counterexamples | SUT property, counterexamples | Verdict |
|---|---|---|---|
| >= 1 | >= 1 | >= 1 | pass |
| >= 1 | 0 | >= 1 | blocks: the driver's audit missed what the server published; a harness finding |
| >= 1 | >= 1 | 0 | blocks: the server's classification broke, or its check is vacuous; a finding |
| >= 1 | 0 | 0 | blocks: both checks vacuous; a finding |
| 0 | any | any | blocks: the race was unreached, so the run is vacuous; no blind rerun |

**How the race rows are checked.** The properties export carries counts and moments only. So one example of each row is read with `snouty runs logs <run> <input_hash> <vtime>` at its moment:

- Plain run: one example of `catalog: a server planned a bucket holding a late commit record`. Its driver line must say the replayed key "is not among" the compaction record's inputs, and the record's `LastModified` must be at or after `C(H)`.
- Mutant run: one counterexample of each property, in the same timeline where one exists. The driver line must say the replayed key "is among" the inputs, and the SUT counterexample's `late_commit_key` detail must equal that replayed key. A SUT counterexample whose key is not a replayed key, or that sits in a timeline with no race, is the "late by itself" case below.

Where a log cannot be read, the part stays local-only evidence and the report says so.

Further rules for the mutant run:

- The driver property's true evaluations are expected 0: every reached race's plan under the mutant takes the replayed record. A true is read from its example (a plan that did not list the record).
- The SUT property's true evaluations are expected: every publish of a bucket with no late record.
- A SUT counterexample in a timeline the race did not reach means a record landed late by itself under faults. It is read from its example and reported. It does not stand in for a reached race.
- Collateral: none expected. The local mutant run's `violated.tsv` held exactly the two properties, and its only other report was the driver property never holding, which is the mutant working. Any other Always with a counterexample is a finding unless its example shows the mutant's direct effect, a compaction record that names a late record (for example, a sweep then removing the late record as a superseded input).

### Budget and stop rules

Billing basis, from `snouty runs usage`: the 16 cap-12 runs from 2026-11-21 to 2026-11-24 (15 in the listing, which starts at 11:18Z on 2026-11-21, plus a1-smoke `45946ec9f6cf740ec612c29068c09249-67-29`, 14.7 h) billed 14.2 to 15.2 h (median 14.6). Two runs: 28.4 to 30.4 h against the 30 h reservation, so the top of the band overshoots it by 0.4 h. There is no rerun reserve.

| Item | h |
|---|---|
| r4561-plain | 15 |
| r4561-mutant | 15 |
| Reservation | 30 |
| Free now | 237.2 |
| Left after both | about 207 |

Stop rules:

1. Plain run, any Always or AlwaysOrUnreachable counterexample in any area, or any Unreachable hit: a finding. It is triaged and filed; the mutant run's result is still read on its own terms.
2. Plain run, race unreached: the fence properties hold without the race's evidence, and that is reported as a miss. No rerun without the owner's go. If the mutant run's race is unreached too, it is a reach gap in the harness and the next step is a harness change.
3. Mutant run: the table above. Every row but the first blocks, and none is rerun blind.
4. Billing: a run billing over 17 h stops any further launch until the bill is explained. Any rerun is outside this reservation and needs the owner's go.
5. A run that did not complete, or set-level check 1 or 5 false: triaged before any further launch. An incomplete run is never counted met.

### What counts as a finding

- Any Always or AlwaysOrUnreachable counterexample in the plain run, whatever its area; in the mutant run, any outside the two fence properties unless it is the mutant's direct effect (above).
- Any Unreachable hit, in either run.
- In the mutant run, any one-sided result in the table (the driver's audit or the SUT's check failing to see the late record).
- A container exit outside the drain-timeout shape.

An owned expected-hit row unreached, the race unreached, or the multiset check never evaluated is a miss in the harness, not a product finding.

### Stamp

**Ref.** main d23dbb245ea2a4caaf05059c47a1d6b75b18eb56 ("test(antithesis): register the catalog/plan-takes-late-record mutant"), the merge of #4718, and origin/main at the build. `launch.sh` refuses unless the checkout equals the images' `org.ravel.antithesis.ref`, so both launches run from a checkout at d23dbb245.

**Images** (built on the gcp2 host, `scripts/build.sh all` and `MUTANTS=1 scripts/build.sh all`, both rc 0, 10:15 to 10:41Z; every image labelled `org.ravel.antithesis.ref=d23dbb245ea2...`):

| Tag | Image ID | Features |
|---|---|---|
| `ravel-antithesis-server:catalog-maintenance` | sha256:7dacbc7ab8b8761736c849e7feca7e4268ed6c88c8a0bdfc4afbd71ba9357b1d | sql,antithesis |
| `ravel-antithesis-driver:catalog-maintenance` | sha256:d3f2d6ab4828780f98f3eb291440fa50d8cad0eb13076e50f73072d160ada170 | |
| `ravel-antithesis-server:catalog-maintenance-mutants` | sha256:8da6d9864d459299aa6b1e8bec1f1ff9dec266a9cdc35b47e197ad06b8ebc10d | sql,antithesis,antithesis-mutants |
| `ravel-antithesis-driver:catalog-maintenance-mutants` | sha256:25df6d948b4b0c6bddd482ccbd0d7bff08dfb7b9a91e3972196daa4a0593dea9 | |

Build check lines: `ravel-server`, `driver`, `driver-reshard`, `s3-proxy` and `ravel-cli-ant` each "links the shim and carries sancov sections" in both builds; `driver` "holds the reshard literal: no", `driver-reshard` "yes"; `catalog: 6 mutants (expected 6)`, `resharding: 2`, `maintenance: 2`, and `ok ravel-antithesis-server:catalog-maintenance-mutants: /symbols/ravel-server registers the 10 mutants (catalog 6, resharding 2, maintenance 2)`.

**Local runs at those images** (0 Antithesis hours, ADR-4326 decision 3):

- Plain `scripts/local-run.sh`, two runs, both rc 0, 0-byte `violated.tsv` and `never-hit-owned.tsv`, the same 121 messages and the same hit set. The second (`lr-plain-b`, 11:12 to 11:39Z) printed `OK: no always-type assertion violated or unhit, every required assertion held, every container healthy, every clock jumped` and `assertions: 121, would fail: 45`; s3proxy 0 lines with `Too many open files`, open-file limit 65536; no `sdk-cli.jsonl`; `maint1` warm-started its memo 21 s after its restart. The first run's console output was overwritten on the host, so this stamp cites the second; the first run's artifacts and exit status are kept. Race in both: reached in the eventually; the server gave the captured record up after 14.1 s (13.1 s in the first); the compaction record names 2 inputs and not the late record; all four late literals held (`maintain: a published record names no late commit record` 2/0, `maintain: a plan left a late commit record out` 2 true of 4).
- `RAVEL_ANTITHESIS_MUTANT=catalog/plan-takes-late-record scripts/local-run.sh` (`lr-mut`, 11:40 to 12:08Z), rc 0: `mutant OK: catalog/plan-takes-late-record violated all of: "catalog: a server's compaction record names no late commit record" "maintain: a published record names no late commit record"`. `assertions: 121, would fail: 49`. `violated.tsv` holds exactly those two. The one other report, `required assertion never held: catalog: a server's compaction record names no late commit record`, is that property's only evaluation being false. The race was reached through the drawn rule; the server gave the record up after 17.4 s; the compaction record names 27 inputs, the late record among them; the driver property 1 false of 1, the SUT property 1 false of 3, its `late_commit_key` the replayed key.

**Expected set**: `expected-set.tsv`, 121 rows plus a header, sha256 f10490fd236e3645d53a36d1eb5f052905bc1c97962c9aa9dc6abe67913eb73d, built from the `lr-plain-b` and `lr-mut` catalogues and the four exports (the first plain run's hit set is the same).

**Budget**: `snouty runs usage --rollup month` shows 2026-11: 52 runs, 962.8 h; 1200 - 962.8 = 237.2 h free. No run in progress (`snouty runs list`: the latest completed 3 days ago).

**Verified state, 2026-11-28T12:15Z**:

- Verified: the four image IDs and their ref label, inspected before the local runs, after the two plain runs and after the mutant run, unchanged. The 10 mutant names registered in the mutant server image. All three local runs' exit status and `violated.tsv`; the plain OK line and the mutant OK line. The bills and usage above. The host idle afterwards, with no `ravel-antithesis-catalog` container left.
- Not verified, required before launch: the images pushed to the Antithesis registry and `snouty validate` on the config (both done by `launch.sh` and `snouty launch`); `snouty runs usage` rechecked just before the launches; the four image IDs re-inspected and equal to this table.

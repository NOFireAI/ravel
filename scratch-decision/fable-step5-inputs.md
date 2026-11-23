EXPECTS_REF: no

gate_tier: script-only
disk_floor_gb: 10

READ-ONLY DECISION TASK. You are deciding, not implementing. Make no commits, no edits and no pushes; the deliverable is your final message. Never ask for confirmation: decide, and end with the report.

First command, before reading anything: time git config user.email "fleet-executor@nofire.ai". If it took more than 30 seconds, stop and report "DEGRADED EXECUTOR: git config took <N>s" with uname -a and nproc.

Tree check, second command: git rev-parse HEAD must print 04c7a48e00c79947daf33071cfcda6cf8e1b4288. If it does not, stop and report the mismatch. Every file:line you cite must be on this tree; verify with git show HEAD:<path> or grep before citing.

ROLE: you answer the open design decisions for Phase E step 5 of #4340 in the Ravel repo (the reshard-and-ownership Antithesis profile on the catalog-maintenance topology). An earlier Fable decision (report embedded below as PLAN) chose this phase and its build list. Two hand-check passes on a GCP build box (embedded below as CHECKS-1 and CHECKS-2) found that parts of that plan's reachability claims were wrong. You decide the corrected build list.

Read on this tree:
- antithesis/catalog-maintenance/README.md (topology, the forced races, the clock jump, mutants);
- antithesis/catalog-maintenance/driver/src/proxy.rs (Rules::admit and the rules);
- driver/src/main.rs (SHARDS and the --shards call sites);
- antithesis/catalog-maintenance/ravel/Dockerfile (the cli stage);
- antithesis/catalog-maintenance/scripts/local-run.sh (how SDK files are judged);
- crates/ravel-ingest/src/generation.rs around the fence and grace;
- crates/ravel-catalog/src/catalog.rs around the inv_mutant! "resharding/token-validated-against-count" site and the "resharding:" token assertions;
- formal/tla/resharding/antithesis-mutants.tsv and formal/tla/maintenance/antithesis-mutants.tsv;
- docs/adrs/4326-tla-invariants-as-antithesis-properties.md (decisions 2, 3, 5, the forced-situation and per-workload amendments).
Two related items are in flight and not on this tree: PR #4625 (mutant scripts accept resharding/ and maintenance/ names; the build check requires 9 mutants as catalog 5, resharding 2, maintenance 2) and issue #4633 (s3proxy at a 1024 open-file limit under its 512-connection cap).

DECIDE, each with a one-line decision, the reasons (file:line on this tree, or the CHECKS line), and the rejected alternative:
D1. CLI instrumentation. Choose between (a) a second, antithesis-instrumented ravel-cli used only for provision reshard, with its own SDK output file that local-run.sh judges as SUT output, and (b) building the image's one ravel-cli with --features antithesis. Consider what every other driver CLI call (sweeps, compacts, folds, the record race's --no-claim compact) would start asserting, and what existing README claims and #4601's server-sweep wait rest on. Say how the choice is shown to change nothing for the existing catalog-maintenance runs.
D2. Stale-writer fence driver path. CHECKS-1 shows refuse-key cannot reach it (GETs are forwarded); CHECKS-2 B shows a full s3proxy outage reaches it and falsifies resharding/no-writer-fence. Choose between (a) a new proxy rule that refuses GETs, or every method, on one key (t/<h>/l/prov), (b) relying on Antithesis's own network and node faults, and (c) another option. Say what else a prov-GET refusal breaks (maintainers' shard ceiling, readers' scan count) and how the expected set handles it. Say what the acceptance proof must be.
D3. Shard count in the driver. SHARDS = 4 and the "--shards", "4" call sites: omit the flag, read the count from the provisioning record, or pass the current ceiling. Use the measured refusal of compact-tenant --shards 4 after activation, and the untested incremental fold.
D4. Token reads across a reshard. CHECKS-2 A: a decrease plus a write timed in the last half second before activation reaches "a token resolved at a shard index above the active count", and makes resharding/token-validated-against-count falsify "a commit token whose record is durable resolves across a reshard". In a plain run that Always is evaluated only after an UnsatisfiableToken, which a real straggler never produces. Decide whether the driver forges or keeps an unsatisfiable token, accepts the Always as unreached in plain runs (and how the expected set says so), or something else. Decide whether token-validated-against-count joins the first mutant wave, given that PLAN excluded it for lack of a site and it now has one.
D5. The corrected build list: one PR per behaviour change, in order, each with its files and its acceptance proof. Include #4633 if it must land before the smoke run.
D6. Pre-registration changes to PLAN section 5 and the mutant reachability notes in section 6. Give the corrected rows; quote the old row for each one you change.
D7. The run plan and budget in PLAN section 3. Keep it within 338 h free, minus what perf-measure-1 billed (14.5 h, inside a separate 45 h reservation, so it leaves 338 h unchanged). Say whether anything is dropped or added.

Rules: cite only what you verified on this tree, or quote CHECKS. Where CHECKS and the tree disagree, say so. No hedged answers: pick one option per decision. Build your report incrementally if it grows large (write the decisions one by one in your final message; keep it under about 2500 words).

==== PLAN (the earlier Fable decision, b5b04b51, at de04f0b77) ====
**0. Tree**


de04f0b774c7ecdf9f3e6fd3848991a243ee1c60 Sun Nov 22 15:34:18 2026 +0000
version = "0.35.0"

Tripwire 0.001 s. Clone shallow (1 commit, no tags): v0.33.0 comparison done from CHANGELOG and ADR-4233, not git diff.

**1. DECISION:** Overturn. Build E, the reshard-and-ownership profile on the catalog-maintenance topology (T11 #4340), not C.

**2. Why**

- **C's core mutant is vacuous.** Ingest fences only when version > policy.baseline(family) (crates/ravel-ingest/src/compat.rs:626). Without format-drill, RSEG/RLOG/RSPAN windows are single-version (crates/ravel-segment/src/format.rs:110, crates/ravel-logseg/src/footer.rs:111, crates/ravel-rspan/src/footer.rs:89), so no class A activation exists on a release-shaped build and ensure_fenced (shard.rs:796, log_shard.rs:1173, span_shard.rs:462) never reaches its raise. Earlier claim "one inv_mutant! covers all three sites": wrong.
- **Compaction-record v2 is not written by compaction.** Ordinary publish stamps format_version: 1 (crates/ravel-maintain/src/publish.rs:702); v2 is the superseding re-encode path (crates/ravel-maintain/src/rewrite.rs:1297), reachable only through ravel-cli maintain migrate --reencode-compaction-parts (services/ravel-cli/src/main.rs:2858); ravel-server has no such flag. Earlier claim "post-activation compaction records are version 2": wrong unless the driver runs migrate. The fence that can fire is maintenance's before_put (crates/ravel-maintain/src/compat_policy.rs:574-584).
- **"epoch regresses" mutant cannot fire.** activate requires version == active + 1 and a writable version (crates/ravel-maintain/src/compat_activate.rs:320-341); one activator, one activatable family, one epoch step. Same shape as #4552.
- **C claims that held:** compat activate exists, exit 4 on incomplete walk (services/ravel-cli/src/compat.rs:51, :416), resume on rerun (crates/ravel-maintain/src/compat_activate.rs:319); compaction-record needs no certify (is_class_a compat.rs:141-143, certify refuses others :598) and no clock move; drill targets v0.33.0 (scripts/compat/released-binary-drill.sh:4, :368; drill-lib.sh:74, :273 present). Formats shared: RSEG 7 (format.rs:122), RLOG 5 (logseg footer.rs:44), RSPAN 5 (rspan footer.rs:29), kidx 2 default (crates/ravel-catalog/src/snapshot_format/kidx.rs:48; 3 only behind --fold-metrics-series-index, refused on sys/compat stores, CHANGELOG.md:93-104), HEAD 2 shipped in 0.32.0 (CHANGELOG.md:1700), provisioning 3 default / 4 only after a fence (provisioning.rs:63, :99), 0.35.0 retires no reader (CHANGELOG.md:155). No false mixed-version failure before a fence.
- **E's blockers from the stale report are gone.** libfaketime jump runs in the Antithesis config (ADR-4326 amendment, docs/adrs/4326-tla-invariants-as-antithesis-properties.md:280-330); 150 min jump (antithesis/catalog-maintenance/driver/src/clock.rs:55) exceeds MIN_LEAD_HOURS = 2 (services/ravel-cli/src/provision.rs:33). ravel-cli already in the driver image (antithesis/catalog-maintenance/driver/Dockerfile:33). Earlier "needs 2 h jump, unwritten assertions, ravel-cli in image": wrong on all three.
- **T10 part 1 is on main.** Mutants resharding/no-writer-fence, maintenance/mo-diverge-overwrites-record, mo-missing-part-reports-converged registered (crates/ravel-invariant/src/lib.rs:496-500) with sites (generation.rs:1648, publish.rs:496, :637) and assertions (generation.rs:1651/1675/1684; provisioning.rs:1978-2026; publish.rs:566/653). token-validated-against-count registered, no site (catalog.rs has no resharding: literal): part 2 (#4520) not landed; #4339 still open.
- **E gaps that are real:** no profile dir (antithesis/reshard-ownership absent); resharding and maintenance not enrolled (only catalog/commit/common/lifecycle have antithesis-mutants.tsv); mutant-config.sh accepts catalog/ names only (antithesis/catalog-maintenance/scripts/mutant-config.sh:30-34); driver model assumes no reshard (driver/src/token.rs:349); seed uses --shards 4 (driver/src/seed.rs:135). All driver and doc work, zero hours.
- **Bug-finding value.** E exercises shipped code every deployment runs: writer fence under stale views across a clock move, two maintainers racing on one bucket, token and scan set across a count change (#4520 flagged the token-hour vs reader-hour gap: a real open question). C exercises an operator path nobody can take above baseline on a release build.

**3. Run plan** (free 338 h after the 45 h #4559 reservation)

| Run | Variant / mutant | Cap | h |
|---|---|---|---|
| reshard-smoke | plain, reshard + stale-writer + ownership races | 12 | 14.5 |
| reshard-mutant-1 | resharding/no-writer-fence | 12 | 14.5 |
| reshard-mutant-2 | maintenance/mo-diverge-overwrites-record | 12 | 14.5 |
| reshard-mutant-3 | maintenance/mo-missing-part-reports-converged | 12 | 14.5 |
| reshard-long | plain | 24 | 25 |
| ingest-durability mutant | commit/late-commit-past-seal (new, #4561 logs+spans) | 12 | 14.5 |
| lifecycle mutant rerun | lifecycle/complete-ignores-served-set (after #4552 fix) | 12 | 14.5 |

Total 112 h. Reserve left 226 h. Smoke first; mutants only after local-loop falsification; at most 3 concurrent.

**4. Build list** (one PR each)

1. Harness: proxy thread cap + socket timeouts (#4568): antithesis/catalog-maintenance/driver/src/proxy.rs. Spawns still uncapped at :511, :527.
2. Harness: real-time pause ≥ head_cache_ttl + margin after each horizon jump (#4571): driver/src/clock.rs, driver/src/main.rs, README "The sweep jumps".
3. Enroll areas: formal/tla/resharding/antithesis-mutants.tsv, formal/tla/maintenance/antithesis-mutants.tsv, sixth column in both traceability.md; scripts/check-tla.sh traceability green.
4. Mutant plumbing: scripts/mutant-config.sh, launch.sh, local-run.sh, build.sh (registered-name check) accept resharding/ and maintenance/ from the profile's tsv.
5. Reshard profile: antithesis/reshard-ownership/{README.md,config/docker-compose.yaml,weekly} reusing catalog-maintenance images; driver src/reshard.rs (ravel-cli provision reshard --tenant … --signal logs --shard-count 6 --lead-hours 2, then the existing jump), src/token.rs writer-identity rule for a rebuilt shard set, src/audit.rs new-count key check, src/main.rs properties.
6. Forced situations for the two maintenance mutants: driver/src/race.rs, forced.rs (hold-record + interleaved live write; hold-record + raw S3 delete of a winner part), README.
7. #4561: property on logs and spans shards stated against the maintain seal (crates/ravel-ingest/src/log_shard.rs, span_shard.rs), new mutant commit/late-commit-past-seal in crates/ravel-invariant/src/lib.rs MUTANTS and formal/tla/commit/antithesis-mutants.tsv.
8. #4552 fix: services/ravel-server/src/maintain.rs ~4166 and served_after_completion.

**5. Pre-registered smoke properties**

- resharding: generation append CAS won reached ≥ 1; new driver reshard: a generation activated reached ≥ 1. Miss: not reached means lead or jump arithmetic wrong, run vacuous.
- resharding: grace routing continues only within the lead horizon… holds; …a writer past the lead horizon was fenced reached ≥ 1 and …a stale writer routed on a grace-extended view reached ≥ 1. Miss: fenced never reached means the refuse-key starvation did not span the jump; any false means real fence bug.
- resharding: two concurrent reshards…exactly one wins and …a lost race never drops a committed generation hold; generation append CAS lost reached ≥ 1 only via a driver double-reshard action. Miss: false.
- Inherited catalog: compaction preserves the acked multiset and catalog: a token read returns each acked record of its write exactly once hold across the reshard. Miss: false after activation is the #4520 class, a real bug.
- maintenance: a loser observing a divergent input-set hash fails closed… and …reports Converged only when the winner part is observed present hold, each evaluated ≥ 1 (forced). Miss: never evaluated means the forced race did not land.
- Driver audit: ≥ 1 data object under a shard index ≥ 4 after activation. Miss: zero means reshard never took effect.
- catalog: the system recovers once faults stop holds.

**6. Mutant reachability**

- resharding/no-writer-fence: driver sets s3proxy refuse-key on t/<h>/l/prov for the servers, jumps 150 min, sends live writes. View stale ≥ min_lead_hours (generation.rs:1215-1217), fence site reached, mutant routes instead, always-property false.
- maintenance/mo-diverge-overwrites-record: hold-record on maint1's record PUT, one live write lands a new L0 in the bucket, maint2 compacts and publishes, release. Loser's input set differs, mutant overwrites, byte-comparing re-GET falsifies never overwrites.
- maintenance/mo-missing-part-reports-converged: hold-record on maint1, maint2 wins, driver deletes one winner part via raw S3, release. Loser's part check finds it absent, mutant reports Converged, property false.
- commit/late-commit-past-seal (new): hold-key on a commit record PUT across its flush deadline, release after seal. Mutant keeps the PUT alive, record lands past sealed_at_ns, property false.
- lifecycle/complete-ignores-served-set: existing completion-before-fold situation, after the #4552 fix.
- resharding/token-validated-against-count: no site on main; not in this wave.

**7. Not doing, and follow-up order**

Not doing: C (no activatable class A version without format-drill; v2 only via CLI migrate; two of three mutants vacuous); D (#4545 activation open, kidx 3 off by default; the "kidx 2 vs 3 contradiction needs an ADR amendment" claim is wrong, amendment landed at docs/adrs/4233-retained-data-compatibility-contract.md:1051); F dropped; format-drill image later.

Order: #4568, #4571 (harness, before smoke), #4552, #4561, #4573 (deterministic MemoryStore test first, no run), #4541 dropped until a lifecycle rerun.

**8. GCP box prerequisites before driver code**

1. MUTANTS=1 antithesis/catalog-maintenance/scripts/build.sh; confirm the server binary registers all 17 names in lib.rs:484-501, not just the five catalog ones build.sh checks.
2. Plain local-run.sh of catalog-maintenance at de04f0b: empty violated.tsv, both sweep jumps reach the driver.
3. By hand in the local loop: ravel-cli provision reshard after seeding, then the jump; confirm resharding: generation append CAS won and activation in server logs. Then refuse-key on the prov key plus jump; confirm a writer past the lead horizon was fenced on the plain build.
4. Each mutant falsified locally (local-run.sh mutant mode exit 0) before its Antithesis run.
5. Fault-free scripts/compat/released-binary-drill.sh preflight then warm-start and read phases against current main as a format-sharing stamp only: needs .dd-tools/v033, cosign (drill:364), rustfs binary (drill:39).
6. snouty runs usage recheck, 45 h reservation confirmed, pre-registration comment on #4340 before launch.

==== CHECKS-1 (box prerequisites, at main 473432ffd) ====
I ran all three tasks on gcp1. The plain run passed, and every resharding assertion you named fired. Two parts did not work as the task assumed, though:
- **2a:** the stock driver image cannot emit "CAS won". I got it from a ravel-cli I built by hand.
- **2d:** refuse-key cannot fence a writer. I got the fence by stopping s3proxy instead.

## Task 1: build and plain local run — PASS
- scripts/build.sh all as builder in the scratch worktree /home/builder/wt/rs4340 at 473432ffd. Both images carry the label org.ravel.antithesis.ref=473432ffd66e..., and the build printed the three instrumentation ok lines (ravel-server, driver, s3-proxy).
- scripts/local-run.sh /home/builder/logs/rs4340-run1 ended with:
  - OK: no always-type assertion violated or unhit, every required assertion held, every container healthy, every clock jumped
  - rc=0
- violated.tsv is 0 bytes. No FAIL lines, and the #4617 token-read line did not appear, so no rerun was needed.
- The script also printed would fail: 50. That list is other areas' assertions that went unreached, which the script does not gate; the run still passed.

## Task 2: by-hand reshard loop
I brought the stack up the same way local-run.sh does: same compose files, project name ravel-antithesis-catalog. Setup completed after 10 s, and I never touched /workload-active, so the driver stayed idle the whole time. Writes were hand-encoded OTLP protobuf, 32 streams with one record each, sent with curl from the driver container to /v1/logs (strict mode, Bearer cm-token).
- The tenant hash is 646219ab819d2b585711f6f13f4368c7.
- The provisioning key t/646219ab819d2b585711f6f13f4368c7/l/prov already existed after setup, before any live write. I did not establish which component created it.
- The seed data sits on shard 0002 only. The first live writes spread over shards 0 to 3.

**2a. Reshard — PASS, but only with a ravel-cli I built**
- The image's ravel-cli contains the literal "resharding: generation append CAS won" 0 times. ravel/Dockerfile builds it in its cli stage without --features antithesis. The server binary does contain it, but no production server path calls append_generation (only a test in maintain.rs does).
- So I built ravel-cli with --features antithesis (no sancov flags), copied it into the driver as ravel-cli-ant, and ran:
  docker exec -e ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/sdk-reshard.jsonl driver ravel-cli-ant --store s3 --tenant-hash-unkeyed provision reshard --tenant cm-tenant --signal logs --shard-count 6 --lead-hours 2
- Output: l: resharded shard_count 4 -> 6, appended generation 1 activating at unix hour 498721 (in 2h), rc=0.
- Hits in that file:
  - {"m":"resharding: generation append CAS won","hit":true,"c":true,"d":{"key":"t/646219ab819d2b585711f6f13f4368c7/l/prov"}}
  - "two concurrent reshards on the same base version: exactly one wins..." c=true, with stored history [gen0 count 4 act 0, gen1 count 6 act 498721]
  - "a lost race never drops a committed generation" c=true
  - "a generation append's history re-read completes" c=true
- 65 s later, writes to both nodes still landed on shards 0 to 3 only, as expected before activation.

**2b. Clock jump by hand — PASS, no driver hook needed**
- docker exec driver sh -c "printf '+%s\n' 9000 > /faketime/offset.next && mv /faketime/offset.next /faketime/offset". This is the same write-then-rename that clock.rs write_offset does.
- Five seconds later every container (rustfs, ravel1, ravel2, maint1, maint2, driver) read 1795399720 or 1795399721 against host 1795390720, which is hour 498722, one hour past activation.
- I did not do the driver's HEAD freeze and 50 s quiet wait. Total offset stayed at or below 36000 s, well under the 90300 s sweep horizon.

**2c. Routing after activation — PASS**
- Six writes after the jump all returned 200, with tokens at hour 498722.
- Each write produced 6 commit records under l/c/<shard>/20261123T02/.
- L0 counts by shard: 0000:10 0001:10 0002:16 0003:10 0004:6 0005:6. Example key: t/646219ab819d2b585711f6f13f4368c7/l/l0/0004/5a23d813-...1795399721.00000000000000000000.ac5bf7cf5ce26143.rseg.
- SQL on both nodes returned 32 records for every write 900001 to 900010.
- After a further jump to +18000, the server folds sealed hour 498722. HEAD then read {"shard_count":6,"watermark_hour":498722,...} with a 36-entry part (6 writes × 6 shards), and the counts were unchanged.
- The violation check (local-run.sh's jq, run over all five containers' SDK files plus the CLI's) was empty.

**2d as written (refuse-key on prov) — FAIL, no fence**
- refuse-key t/646219ab819d2b585711f6f13f4368c7/l/prov -> ok, then a jump to +27000 and writes to both nodes: both returned 200 and committed to 6 shards. No server resharding assertion was hit.
- Cause: Rules::admit in proxy.rs forwards every request that is not a PUT (line 278, and the unit tests pin GET as forwarded). The view refresh is a GET of l/prov, so the rule blocks nothing the fence depends on.

**2d by another route (docker stop s3proxy) — PASS**
- Both nodes had last refreshed at faked time about 1795417901 (hour 498727). I stopped s3proxy and waited 70 s.
- A write to ravel1 got 503 timed out waiting for shard ack after 13 s. ravel1's log shows ERROR ravel_ingest::generation: provisioning record read failed ... (or routed on the cached view while it is inside its grace window).
- SDK hits on ravel1:
  - "a stale writer routed on a grace-extended view" c=true
  - "grace routing continues only within the lead horizon of the last refresh; past it the writer stops" c=true, with now_ns 1795417996123310519, refreshed_at_ns 1795417901827322805, count 6
- I then jumped to +36000 (hour 498729, which is the refresh hour plus 2) and wrote to both nodes. Both clients saw 503 provisioning view stale: refusing to route on a view older than the refresh interval, after 6.6 s and 2.5 s.
- SDK hits:
  - ravel1: "resharding: a writer past the lead horizon was fenced","c":true with now_ns 1795427031384843208, refreshed_at_ns 1795417901827322805
  - ravel2: the same assertion with refreshed_at_ns 1795417902563473479
- After docker start s3proxy, writes returned 200 again. The write that timed out in the grace window left no objects. The final violation check was empty.
- Both 503s come from a total store outage. What a client sees in the grace window when only the prov GET fails was not measured.

## Task 3: cleanup — done
- docker compose down -v (rc 0). This also removed another session's exited containers with the same names.
- Worktree removed. The hand-built ravel-cli target directory and /tmp/rs4340 deleted.
- df -h / (used/avail): 200G/768G at start, 220G/748G before cleanup, 217G/751G after. The remaining 17 GB is the rebuilt images and Docker build cache; I did not prune them.
- The shared ravel-antithesis-{server,driver}:catalog-maintenance tags are now built at 473432ffd.
- Logs and SDK files are kept on gcp1 in /home/builder/logs/rs4340-run1/ and /home/builder/logs/rs4340-hand/.

## Driver hooks the step 5 reshard profile needs
1. **An antithesis ravel-cli.** The cli stage of ravel/Dockerfile must build with --features antithesis. Otherwise "CAS won", "CAS lost" and the two always-checks on the append can never fire, because only ravel-cli appends generations. Side effect, read from the script but not measured: the CLI writes a hit:false line for every assertion into the driver's /tmp/sdk.jsonl. local-run.sh treats every message in that file as the driver's own, so unreached always-assertions from other areas would become FAIL lines unless the CLI gets its own output file.
2. **The reshard rule.** ravel-cli --store s3 --tenant-hash-unkeyed provision reshard --tenant cm-tenant --signal logs --shard-count 6 --lead-hours 2. The flags are exactly these; --lead-hours defaults to 2 and is refused below 2. The record already exists after setup, so no provision adopt step is needed.
3. **Timing.** Activation is the CLI's current hour plus 2. Any jump of at least 7200 s past the reshard always activates it; my 9000 s jump landed one hour past. The fence needs the hour of now + grace_lead to reach the refresh hour plus 2.
4. **A proxy rule that refuses GETs** (or all methods) on one key, for t/<hash>/l/prov. A driver cannot stop a container, and without this rule the fence is reachable only through a full s3proxy outage or an Antithesis fault. Each node needs an acked write first, because try_grace_extend returns silently for a tenant with no cached view.
5. **Hard-coded shard count 4 in driver/src/main.rs.** Sites: const SHARDS: u32 = 4 (line 70), used by the sweep loops (lines 824, 2768), the compactable-bucket scan (lines 1424, 1434) and the shard draw (line 2487); and the literal "--shards", "4" at lines 1194, 1208, 1318 and 1957. What I measured:
   - maintain compact-tenant --shards 4 after activation is refused with rc 1: Error: --shards 4 disagrees with tenant "cm-tenant"'s provisioning record for signal Logs, whose shard ceiling at hour 498729 is 6; rerun with --shards 6 or omit --shards. Omitting --shards works (rc 0). I only saw this after activation.
   - catalog fold --shards 4 was harmless in what I ran: two no-op folds, and one HEAD rebuild after I corrupted HEAD (with the servers' HEAD PUTs frozen). The rebuild wrote HEAD shard_count:6, and SQL counts stayed intact. I did not exercise an incremental fold of a new post-activation hour with this flag.
   - maintain sweep --shard 4 --dry-run works (rc 0), but the driver only ever sweeps shards 0 to 3.
6. **Token reads across the reshard.** Two assertions never fired:
   - "a commit token whose record is durable resolves across a reshard" (an always-check) runs only when a token comes back unsatisfiable.
   - "a token resolved at a shard index above the active count" (a reachability check) needs a token whose shard is at or above the active count for its hour.
   
   Both need something like a forged or never-durable token at shard 4 or 5 in an hour from before activation. A 6-shard token read after activation returned 32 records and hit neither. I did not test a token minted before the reshard and read after activation.

## Not exercised, or unexplained
- Not reached: "CAS lost" (needs two concurrent reshards).
- The postings build aborted ... bad magic bytes shard=2 ingest_hour_bucket=498704 WARN appears 139 times in the plain run's compose.log too. It concerns the seeded hours and predates the reshard.
- HEAD's part for hour 498727 went from 16 entries to 12 across the rebuild, with identical SQL counts. I did not explain it.
==== CHECKS-2 (hand checks, at main 900773f21) ====
I ran all four checks. Every assertion you asked about was reached and quoted below, but the decrease you described does not do it alone: the shard-4/5 tokens have to be written in the half-second before the decrease activates.

**Before/after disk:** gcp2 had 421G free before and after (419G at peak, while the CLI target existed). gcp1 had 751G free before and after; I only read logs there. The wait-perf1 unit does not exist on gcp2 (Unit wait-perf1.service could not be found); I did not look further.

**Setup:** I built ravel-cli from main 900773f21 with --features antithesis as builder (Finished release ... DONE rc=0) and copied it into the driver as ravel-cli-ant. The stack ran as compose project rs4340e on the -mutants images. For A and D the selector was empty (RAVEL_ANTITHESIS_MUTANT=). Logs, SDK snapshots and helper scripts are in /root/logs/rs4340e/ on gcp2.

## A. Decrease reshard and straggler token: as written, neither assertion fires (FAIL); with a straggler token, reachable PASS
- **Reshards:**
  - 4→6: resharded shard_count 4 -> 6, appended generation 1 activating at unix hour 498722.
  - After a +9000 s jump, six writes returned tokens on shards 0–5 at hour 498722.
  - 6→4 is accepted: l: resharded shard_count 6 -> 4, appended generation 2 activating at unix hour 498724 (in 2h), rc=0, with "generation append CAS won","c":true.
- **Slack:** pub const DEFAULT_SCAN_SLACK_HOURS: u32 = FLUSH_BOUND_SLACK_HOURS + TOLERATED_CLOCK_SKEW_HOURS;, which is 2 + 1 = 3. I jumped to hour 498728 (activation + 4) and waited for the HEAD watermark to reach 498725.
- **Your recipe:** tokens written in the 6-shard era cannot fire either assertion, and the slack jump changes nothing for them.
  - The reachable check is token.shard >= active_shard_count(&generations, token.ingest_hour_bucket). It is judged at the token's own hour, where the count is 6.
  - "durable resolves" is only evaluated after an UnsatisfiableToken.
  - Reads before and after the slack jump returned {"rows":32,"distinct_rec":32,"total":32,"maxn":1,"minn":1} on both nodes, with 0 hits of either assertion.
- **What does fire it is a real straggler:** a write routed under 6 shards just before 498724 starts, whose flush lands in hour 498724.
  - I put the clocks 20 s before the boundary and sent writes every 0.3 s.
  - The writes sent 0.451 s and 0.150 s before the boundary came back with shards 0–5 at hour 498724. The one sent 0.752 s before stayed in 498723; later writes used shards 0–3.
  - Reading them: {"m":"resharding: a token resolved at a shard index above the active count","c":true,"d":{"answer":"resolved","commit_key":"t/.../l/c/0004/20261123T04/d23b690f-....00000000000000000014.cmt",...}}, one line each on ravel1 and ravel2.
  - The SDK writes this line once per process, so the only line names shard 4 from before the slack jump. Shard 5 reads and post-slack reads succeeded but wrote no new line.
- **"durable resolves" (plain):** not reached (0 hits); that is what the code predicts.
- **Same tokens with resharding/token-validated-against-count** (ravel1/ravel2 recreated, same store):
  - Client: 503 requested commit token is not yet visible; retry.
  - SDK: {"m":"resharding: a commit token whose record is durable resolves across a reshard","c":false,"d":{"answer":"unsatisfiable","commit_key":".../l/c/0004/20261123T04/d23b690f-...014.cmt",...}} on both nodes.
- **Exactly once:** every token read returned 32 distinct records with n=1. Plain SQL counts were 32 for every write.
- **Violation jq** over all server, driver and CLI SDK files from the plain run: empty.

## B. no-writer-fence under the outage fence: PASS, the mutant is falsifiable
1. Reshard 4→6, +9000 s jump, writes on both nodes. Both nodes refreshed at hour 498722.
2. docker stop s3proxy, waited 70 s, wrote. Both writes got 503 timed out waiting for shard ack after about 12–13 s. The SDK showed "grace routing continues only...","c":true and "a stale writer routed on a grace-extended view","c":true.
3. Jumped to hour 498724 (refresh hour + 2) and wrote. Both writes got 503 timed out waiting for shard ack after 12.4 s and 12.0 s, not the provisioning view stale refusal the plain build gave in the previous report.
   - ravel1: {"m":"resharding: grace routing continues only within the lead horizon of the last refresh; past it the writer stops","c":false,"d":{"count":"6","now_ns":"1795406709527999419","refreshed_at_ns":"1795402178463689915",...}}
   - ravel2: the same with "c":false, now_ns 1795406722124749451.
   - "a writer past the lead horizon was fenced" was not hit, which is expected with the fence disabled.
4. After docker start s3proxy, writes returned 200. The violation jq lists only this assertion.
- **Unexplained:** the two past-horizon writes that got 503s later show 32 rows each in SQL. ravel2 logged commit record published for its flush at hour 498724 on shards 0–5 at 04:05:52, after s3proxy came back. No log line says why it was published. The two in-grace writes left nothing. I ran no plain control of step 3 at this commit.

## C. The 16→12 change on gcp1: explained by compaction timing; key-level diff could not be done from the logs
- No part dump at watermark 498727 was kept (inspect.json and inspect2.json stop at 498719 and 498722), so I cannot list the vanished object keys from the logs.
- The fold entry_count values are whole-HEAD totals, and they reconcile: 22 = 6+16, 46 = 10+36, 16 = 10+6, 26 = 10+16, rebuild 22 = 10+12. The 16 and 12 are derived (26−10 and 22−10), not observed.
- The log order explains them:
  - maint1 compacted shards 0 and 3 of hour 498727 at 09:44:08.
  - The fold to watermark 498727 ran at 09:44:13 (entry_count=26).
  - maint2 compacted shards 1, 2, 4 and 5 at 09:44:20.
  - The rebuild ran at 09:45:37.
- So the old part named 8 L0 inputs for shards 1, 2, 4 and 5 that compaction then replaced with 4 L1 outputs. The 4 entries that went away are superseded compaction inputs, not shard 4/5 data and not duplicates.
- **SQL read path:** hours at or below the watermark come only from HEAD's parts (extract_into). Listing starts at window.watermark_hour.saturating_add(1). The superseded L0 objects were still in the store, so counts did not change.

## D. Rebuild reproduction on gcp2: PASS, explained, not a bug
- **Before:**
  - A no-op fold: no_op: true, watermark_hour: Some(498725).
  - HEAD's second part had 227 entries: 223 L0 for hours 498722–498724, which were compacted after their fold, plus 4 L1.
  - Every live bucket in the store had an L1 record.
- **Rebuilds:** I ran refuse-key on HEAD, overwrote HEAD with garbage, then ran catalog fold three times: with --shards 4, with no flag, and with --shards 6.
  - Each printed rebuilt: true, entry_count: 28.
  - Each produced the same part 20261123T05.31d30811a29e29c0.csnap with 22 entries; the decoded entry lists are byte-identical.
- **Diff:** every bucket's L0 entries were replaced by its single L1 entry (−223 L0, +18 L1). No bucket was lost.
- **Checks after:** SQL counts were identical before and after on both nodes, and catalog verify said snapshot matches the sealed commit history.
- **--shards:** catalog fold --shards 4 is accepted and has no effect on a rebuild. compact-tenant --shards 4 --dry-run refuses: Error: --shards 4 disagrees with ... shard ceiling at hour 498728 is 6. An incremental fold of a new post-activation hour with --shards 4, as the driver runs it, is still untested.

## Short answers
- **Decrease vs forged token:** the decrease is the right route to "a token resolved at a shard index above the active count" and to the token-validated-against-count mutant, but only with writes timed just before activation. A plain run still needs a token whose record is absent (forged or never durable). "durable resolves" is must_hit: true and is only evaluated on an unsatisfiable token, which a real straggler never is.
- **no-writer-fence:** falsifiable on the outage path. It goes c=false on both nodes once the clock reaches refresh hour + 2.
- **16→12:** explained. It is compaction finishing after the fold that sealed the hour, which the rebuild then reflected. It is not a product bug and the --shards flag plays no part. No reproduction is needed.

## What I could not do, and other findings
- I could not diff the gcp1 case by object key (no dumps were kept). D shows the same thing at key level.
- s3proxy has a soft open-file limit of 1024. It logged Too many open files 255 times in A, and 4 token reads on ravel2 got 503 upstream storage temporarily unavailable; they passed on retry, but I did not save the retry output. This is a harness bug outside this task's scope.
- The d2.txt transcript for D was not saved because the tee was denied; the per-step files are there (d-fold-*.txt, d-*-entries.txt).
- The build log is in /home/builder/logs/, with a copy in /root/logs/rs4340e/.
- The first docker ps -a, docker images and one read-only docker run (a glibc check) ran outside the docker lock.

Cleanup: both compose stacks were taken down with down -v, only buildx_buildkit_ocipush0 is left, and the source and CLI build directory are deleted. Logs are kept.
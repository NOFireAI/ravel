Report below. Executor healthy (git config 0.001 s, x86_64, 16 vCPU). Clone was depth 1, so I deepened by one commit; HEAD~1 is 04c7a48e0 and the scratch commit adds only the inputs file. All citations are from main 04c7a48e0.

**Tree versus PLAN and CHECKS.** Three PLAN premises are stale on this tree: #4568 landed (`proxy.rs:68-82`, MAX_CONNECTIONS 512, IO_TIMEOUT 60 s), #4571 landed (README.md:418, driver `settle` at main.rs:1088 and the 150 s wait at main.rs:101), and both areas are enrolled (`formal/tla/resharding/antithesis-mutants.tsv:4,6`, `formal/tla/maintenance/antithesis-mutants.tsv:10-11`). token-validated-against-count now has a site, `crates/ravel-catalog/src/catalog.rs:4605`. CHECKS-1's driver line numbers match the tree except the last `--shards` literal, which is main.rs:1956, not 1957. Scripts still accept catalog/ names only (`mutant-config.sh:30-34`, `build.sh:93-99`, `local-run.sh:39`); that is #4625, in flight.

## D1. CLI instrumentation: (a), a second binary `ravel-cli-ant` used only for `provision reshard`

Reasons:
- Only ravel-cli appends generations. The reachable sites are `crates/ravel-catalog/src/provisioning.rs:1978,1983` and the always-checks at :2014,:2021,:2026; CHECKS-1 2a: the image's ravel-cli holds "CAS won" 0 times because `ravel/Dockerfile:53` builds the cli stage without `--features antithesis`.
- Building the one CLI with antithesis changes every other call's evidence. ravel-cli calls `ravel_invariant::init()` (`services/ravel-cli/src/main.rs:3183`), which runs `antithesis_init` (`crates/ravel-invariant/src/lib.rs:131`) and emits the catalog. The driver spawns the CLI with inherited env (`driver/src/cli.rs:59-65`, no env override), so in the local loop every CLI line lands in the driver's `/tmp/sdk.jsonl` (`local/docker-compose.local.yaml:17`). `local-run.sh:393` derives "the driver's own" assertions from `sdk-driver.jsonl`; `:398-410` then gates every unreached must-hit always in that set. CHECKS-1 reports "would fail: 50" of other areas today; under (b) those 50 become FAIL lines in every plain catalog-maintenance run.
- README claims rest on a silent CLI. README.md:430-436: the driver's own `ravel-cli` sweep "reports no SDK assertions", so the driver waits up to 150 s for the servers' sweep to evaluate `catalog: a sweep never deletes an object the current HEAD names`. Under (b) the CLI sweep evaluates it from a non-SUT process and the wait no longer proves a server sweep ran. Same for the record race's `--no-claim` compact (main.rs:1519-1527, 1683-1691): the CLI would evaluate `publish.rs` loser assertions, and could satisfy the maintenance "evaluated ≥ 1 (forced)" requirement from the wrong process. README.md:694-697 and :745-776 describe the expected set on the premise that the driver file holds only `catalog: ` and `clock: ` driver literals.
- Precedent exists for the stage: `antithesis/ingest-durability/ravel/Dockerfile:40-46` builds ravel-cli with `--features antithesis` and sancov; copy that stage as `cli-ant`, never with `antithesis-mutants` (README.md:694 stays true).

Shape: new Dockerfile stage `cli-ant` in `ravel/Dockerfile`, copied to `/usr/local/bin/ravel-cli-ant` in the server image and by `driver/Dockerfile:33`'s sibling line; a second `Cli { bin: "ravel-cli-ant" }` in the driver, run with `run_env` setting `ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/sdk-cli.jsonl` (CHECKS-1 2a did exactly this by hand); `local-run.sh:353-356` copies `driver:/tmp/sdk-cli.jsonl` as `sdk-cli.jsonl` when present, so it enters the `sdk-*.jsonl` globs at :363-385 (violated, never-hit) but never `driver-assertions.txt`. Build check: a fourth instrumentation line for `ravel-cli-ant` (`build.sh:80-87`).

Shown to change nothing: the existing `cli` stage and the `ravel-cli` path are untouched, and the plain catalog-maintenance driver never runs `ravel-cli-ant`. Proof: a plain `local-run.sh` on the new images passes, `sdk-cli.jsonl` is absent, and `diff` of the message column of `assertions.tsv` against the CHECKS-1 run (`/home/builder/logs/rs4340-run1/`) is empty. Plus `grep -c "generation append CAS won"` on the two binaries: stock 0, `-ant` ≥ 1.

Rejected: (b). Cheaper Dockerfile, but it rewrites the expected set of a passing workload and turns the server-sweep wait into decoration.

## D2. Fence path: (a), a proxy rule refusing reads of one key

Decision: new rule `refuse-read <key>` and `thaw-read <key>`: answer every GET and HEAD of exactly that key `503 SlowDown` unsent, in a set of its own checked before the non-PUT early return at `proxy.rs:278`; `pass` clears it (`:226-232`). The driver sets it on `t/<h>/l/prov`, after each node has acked one write, then jumps.

Reasons:
- CHECKS-1 2d: "Rules::admit in proxy.rs forwards every request that is not a PUT (line 278, and the unit tests pin GET as forwarded)". Tree agrees: `proxy.rs:278-280`, tests at :887 and :918.
- The fence is reached only when the refresh GET fails and the cached view ages past the horizon: `route_cached` returns Stale (`generation.rs:1591-1596`), the re-read fails (`:1378` "provisioning record read failed"), `try_grace_extend` fences at `:1645-1660`, and returns None for a tenant with no cached view (`:1645`), hence the acked write first.
- CHECKS-2 B shows the outage path falsifies the mutant: `"grace routing continues only within the lead horizon..."` `c:false` on both nodes at refresh hour + 2. A key-scoped refusal reaches the same code with the rest of the store healthy, so token and scan assertions keep being evaluated instead of every read 503-ing.
- The forced-situation amendment allows exactly "one store request delayed in the network or refused" (ADR-4326 :445-449).

What else the refusal breaks, and the expected set: the servers' every query reads the record (`catalog.rs:1277-1299`, `read_scan_generations` on each resolve), so SQL and token reads on ravel1/2 fail for the window; server folds and maint compaction read it for the ceiling (`fold.rs:5648-5650`, CLI `maintain.rs:919-922`) and skip. The driver's own CLI reaches rustfs directly (README.md:18), so its fold, compact and audit are unaffected. Handling: the fence rule runs under the write gate as the other forced races do, sends no read of its own, and counts a non-200 read inside the window as a store fault, not a lost record; `catalog: the system recovers once faults stop` is required after `thaw-read`. No new always-assertion. The clock move's quiet wait reads HEAD directly (`clock.rs:153-186`), so it is unaffected.

Acceptance proof: unit tests in `proxy.rs` pinning GET and HEAD of the key Refuse, GET of another key Forward, and every PUT rule unchanged; then a plain local run where `sdk-ravel1/2.jsonl` show `resharding: a stale writer routed on a grace-extended view` hit before the jump and `resharding: a writer past the lead horizon was fenced` hit after, with the client error `provisioning view stale: refusing to route on a view older than the refresh interval` (`crates/ravel-ingest/src/error.rs:76`) and no `docker stop`; then `RAVEL_ANTITHESIS_MUTANT=resharding/no-writer-fence` exits 0 with that always false.

Rejected: (b) Antithesis faults. Not controllable: the fence needs the refusal to span a driver-scheduled jump, and a random partition during the quiet wait fails the HEAD-still gate instead. (c) refuse every method on the key: servers never PUT prov, so it buys nothing and would also block a concurrent CLI reshard if one ever routed through the proxy.

## D3. Shard count: omit `--shards` on every CLI call that accepts omission, and bound the driver's loops by the ceiling read from the provisioning record

Reasons:
- Measured refusal, CHECKS-1 hook 5: `compact-tenant --shards 4` after activation is refused, "rerun with --shards 6 or omit --shards"; the CLI's flag is `Option<u32>` with "Omit to resolve it from the tenant's durable shard-count provisioning record" (`services/ravel-cli/src/main.rs:2620-2625`, error at `maintain.rs:919-922`).
- `catalog fold --shards` (main.rs:2907-2908) and `rlog footprint --shards` (:2253-2254) default to 4 and only seed `config.shard_count`, used when no record exists (`catalog.rs:1284-1286,1299`); the fold stamps the ceiling from the record (`fold.rs:5648-5650`), which is why CHECKS-1's rebuild wrote `shard_count:6` under `--shards 4`. Dropping the literal removes a second source of truth at no behaviour change for a 4-shard tenant.
- The loops at `driver/src/main.rs:824,1424,1434,2487,2768` sweep and scan shards `0..SHARDS` (`:70`). After activation shards 4 and 5 would never be swept or scanned; the audit's new-count check needs the real count. Read the ceiling from the record (the driver already reads S3 directly for the audit), cache it in an `AtomicU32`, refresh after each reshard activation. The seed keeps `--shards 4` (`seed.rs:134-136`): it runs before any reshard.
- The untested incremental fold: the acceptance test covers it rather than relying on the flag.

Acceptance proof: plain catalog-maintenance run unchanged (same `assertions.tsv` message set); in the reshard profile, a post-activation live hour folded by the driver's CLI (no flag) yields a HEAD part naming entries under shards 4 and 5, checked by the audit's new-count key check, and every sweep pass after activation logs shards 0..5.

Rejected: pass the current ceiling explicitly. It reintroduces the race the CLI's refusal exists for: a driver value read before activation and used after it is refused, and compaction silently stops.

## D4. Token reads: forge a never-written token for the plain run, make real stragglers for the mutant, and add the mutant to the wave

Decision: (1) The driver forges an unsatisfiable token: `CommitToken` has public fields and `encode` (`crates/ravel-types/src/lib.rs:526-536`); take an acked token, set `shard` at or above the active count for its hour and an unused `seq`. The server answers UnsatisfiableToken, and `assert_token_resolves_across_reshards` evaluates the always with `record_absent = true` (`catalog.rs:3122-3128, 5152-5170`), so it is reached and true in plain runs. (2) The decrease (6→4 after the increase) is followed by a burst of writes in the last 2 s before activation at 50 ms spacing, giving real stragglers; CHECKS-2 A: writes "0.451 s and 0.150 s before the boundary came back with shards 0–5 at hour 498724" and reading them hit `resharding: a token resolved at a shard index above the active count` (`catalog.rs:3131-3139`). Both are required sometimes/reachable lines in the local loop; the straggler cycle repeats once if it produced none. (3) `resharding/token-validated-against-count` joins the wave as reshard-mutant-4: the site is `catalog.rs:4605-4613`, and CHECKS-2 A shows the straggler read under it: `"a commit token whose record is durable resolves across a reshard","c":false`.

Reasons for forging over accepting unreached: the assertion is must-hit (`inv_always_if_known` at `ravel-invariant/src/lib.rs:249-256` expands to an Always) and resharding becomes an owned area once the profile's tsv names it (`local-run.sh:391`, ADR-4326 :418-428), so an unreached line fails every plain run; an exception in the expected set would need wording that contradicts the per-workload amendment. A forged token is a legitimate client input (a token for a write that was never acked), and the README's token rules already retry one unsatisfiable answer (`main.rs:109-111`).

Rejected: keep an unsatisfiable token via a held commit PUT. The record then exists later and the retry resolves it, which exercises nothing new and complicates the multiset.

## D5. Corrected build list, in order

1. **#4633, s3proxy open-file limit** (compose `ulimits: nofile` on `s3proxy`, or lower MAX_CONNECTIONS to fit 1024). Must land before the smoke: `proxy.rs:31-34` relays two threads per connection, so a 512 cap needs more than 1024 descriptors, and CHECKS-2 saw 255 `Too many open files` with an idle driver. Proof: plain run with `grep -c "Too many open files"` on the proxy log equal to 0, asserted.
2. **#4625, mutant plumbing** (in flight): scripts accept resharding/ and maintenance/ names, build check 9 mutants. Proof: `MUTANTS=1 build.sh` prints the 9-name ok line.
3. **cli-ant stage** (D1): `ravel/Dockerfile`, `driver/Dockerfile`, `scripts/build.sh`, `driver/src/cli.rs`, `scripts/local-run.sh`. Proof as in D1.
4. **refuse-read rule** (D2): `driver/src/proxy.rs` and tests, README "The forced races". Proof as in D2.
5. **shard count** (D3): `driver/src/main.rs` call sites and loops, `driver/src/audit.rs`. Proof as in D3.
6. **reshard profile**: `antithesis/reshard-ownership/{README.md,config/docker-compose.yaml,weekly}`, `driver/src/reshard.rs` (increase 4→6 with `--lead-hours 2`, jump 9000 s, fence situation, decrease 6→4 with straggler burst, forged token read), `token.rs:347-350` writer-identity rule relaxed for a rebuilt shard set, `audit.rs` new-count check, `main.rs` properties. Proof: plain local run passes with every D6 required line hit, `violated.tsv` empty.
7. **forced situations for the two maintenance mutants** (`race.rs`, `forced.rs`, README), unchanged from PLAN item 6. Proof: each mutant falsified locally, exit 0.
8. **#4561** and 9. **#4552**, unchanged from PLAN items 7 and 8; neither blocks the smoke.

PLAN items 1, 2 and 3 are dropped as already on main. Each mutant run waits for its local falsification (ADR-4326 :104-112).

## D6. Pre-registration corrections

Section 5, changed rows (old quoted, then new):

- Old: "resharding: generation append CAS won reached ≥ 1; new driver reshard: a generation activated reached ≥ 1." New: `resharding: generation append CAS won` hit ≥ 1 in `sdk-cli.jsonl` (the `ravel-cli-ant` process), and the driver's `reshard: a generation activated` hit ≥ 1 from a post-jump write acked at a shard ≥ 4. Miss: unreached means lead or jump arithmetic wrong, run vacuous.
- Old: "…fenced reached ≥ 1 and …a stale writer routed on a grace-extended view reached ≥ 1. Miss: fenced never reached means the refuse-key starvation did not span the jump". New: both reached ≥ 1 on each node under `refuse-read t/<h>/l/prov`, each node acked ≥ 1 write before the rule. Miss: fenced unreached means the read refusal did not span the jump or a node had no cached view; the always false is a real fence bug.
- Old: "Inherited catalog: compaction preserves the acked multiset and catalog: a token read returns each acked record of its write exactly once hold across the reshard." Add: `resharding: a token resolved at a shard index above the active count` hit ≥ 1 (straggler burst) and `resharding: a commit token whose record is durable resolves across a reshard` evaluated ≥ 1 and true (forged token). Miss: never evaluated means the forged read did not run; false is the #4520 class.
- Old: "Driver audit: ≥ 1 data object under a shard index ≥ 4 after activation." Add: after the decrease activates, every new write's shards are below 4, and the fold of a post-activation hour names shard 4 and 5 entries.
- New row: `catalog: the system recovers once faults stop` holds after each `thaw-read`.

Section 6, changed rows:

- Old: "resharding/no-writer-fence: driver sets s3proxy refuse-key on t/<h>/l/prov for the servers, jumps 150 min, sends live writes." New: driver sets `refuse-read` on the key after an acked write per node, jumps so the hour reaches refresh hour + 2, writes; expected falsification is `grace routing continues only…` false on both nodes and `fenced` unhit (CHECKS-2 B).
- Old: "resharding/token-validated-against-count: no site on main; not in this wave." New: site `catalog.rs:4605`; a 6→4 decrease with writes in the last half second before activation yields durable records at shard 4 or 5 in the activation hour; reading them under the mutant makes `durable resolves` false (CHECKS-2 A). In this wave.

Unchanged: the two maintenance rows, late-commit-past-seal, complete-ignores-served-set.

## D7. Run plan and budget

Add `reshard-mutant-4: resharding/token-validated-against-count`, cap 12, 14.5 h. Nothing dropped.

| Runs | h |
|---|---|
| PLAN's seven runs | 112 |
| reshard-mutant-4 | 14.5 |
| Total | 126.5 |
| Reserve left of 338 | 211.5 |

perf-measure-1's 14.5 h sits inside its own 45 h reservation, so 338 is unchanged. Order unchanged: #4633 and #4625 land, smoke first, mutants only after local falsification, at most 3 concurrent.
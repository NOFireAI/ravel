# Review of PR #2024: fetch whole objects on loopback again, split cache flags

Reviewer: Claude Fable 5.1 (fleet executor), 2026-09-27.
Head reviewed: see the commit list under Gates.
Implements ADR-2023.

## Verdict

Merge now. Do not hold the PR for the two-arm acceptance run and do not push
another code round on it. Decisions 1 and 2 stand on their own evidence:
decision 1 reverts a shipped 0.18.0 regression whose cause was measured on
one box with one variable changed (0.123 against 0.400 queries per second),
and decision 2 removes a coupling that committed 12 GB to a catalog cache
serving zero hits. Decision 3 (the 40% loopback share) is the only unmeasured
piece, and every plausible outcome of the run changes one constant and the
ADR text, which is cheaper as a follow-up than as a hold on a PR already at
the three-round limit. The code is correct for every input I could construct.
Six mutations of the guarded production lines each fail their test at the
line the test's own prove-the-test note names, and all six gates exit 0 on
this head. Nothing is blocking. Three should_fix items are all text: the
rationale in the `--cache-max-bytes` rustdoc still justifies the 40% share
by the ranged plan's block cache, which decision 1 turned off by default;
ADR-2023 does not record the control arm or the tie rule; and its claim that
the decision 4 error-ratio bound guards the shrunken remainder covers only
the SQL path now that main charges PromQL fetches to that same remainder.
Fix those in the ADR-acceptance edit that records the run's figures, file
the three nits as one follow-up issue, and cut v0.19.0 only once the
candidate arm passes decision 4.

## Findings

Ordered by severity. Line numbers are on this HEAD.

1. **should_fix. Stale rationale for the 40% share.**
   services/ravel-server/src/config.rs:1476-1481 (the `--cache-max-bytes`
   rustdoc) says the larger share exists because "under concurrent queries
   the ranged read plan's block cache needs its working set resident to keep
   those misses off that disk (ADR-2023)". Decision 1 makes whole-object
   reads the loopback default, and ADR-2023's own Consequences justify the
   share by whole objects staying resident. docs/guides/caching.md:117
   carries a different muddled version ("since a loopback store has no
   network cost to amortize with a larger cache"). Failure scenario: the
   next person tuning `LOOPBACK_CACHE_MEMORY_PERCENT` reasons from the wrong
   mechanism, or an operator on a loopback store reads that the share is for
   the ranged plan, sees `cost-based` on their startup line, and sets
   `--cache-max-bytes` back down. The clap help (first paragraph) and the
   flag reference are correct; only the second rustdoc paragraph and the
   guide sentence are wrong. One sentence each fixes it.

2. **should_fix. ADR-2023 does not record the control arm or the tie rule.**
   docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md
   decisions 3 and 4. Decision 4 measures the combination of decisions 1 and
   3, so a pass cannot be attributed to the share. The control arm (same
   build, `--cache-max-bytes 7689077760`, which is 25% of the
   30,756,311,040-byte budget) and the pre-registered reading (the share is
   credited only if the candidate beats the control on hot sum or QPS by
   more than the 15% noise floor; a tie records decision 3 as
   unmeasured-neutral) exist only in the issue thread. Failure scenario: the
   candidate passes decision 4 through decision 1 alone, the ADR moves to
   Accepted, and the 40% share reads as validated when it was never
   isolated. Fix in the acceptance edit: add the control arm and the rule to
   decision 4, then the figures.

3. **should_fix. The error-ratio guard covers SQL only after the rebase.**
   Same ADR, Consequences, lines 108-111: "The shared SQL/fetch remainder on
   a loopback store shrinks by the extra share ... The error-ratio bound in
   decision 4 is what guards it." Since main's d8a574b1 ("charge PromQL
   fetches to the process memory budget"), every PromQL engine reserves from
   that remainder and a fetch that does not fit fails with
   `FetchMemoryExhausted`, returned as 503. The SQL path's own RSEG, RLOG and
   RSPAN fetchers still reserve against private unlimited budgets
   (docs/query-engine.md:644-676), and ClickBench is SQL only, so decision 4
   exercises the SQL executor's reservations and never the PromQL fetch path
   that lost the headroom. On the ClickBench box the remainder goes from
   21,529,417,728 bytes (70% of the budget) to 16,915,971,072 (55%), a
   4,613,446,656-byte loss. There is no double counting: the carve and the
   remainder are disjoint by construction (config.rs:3136), main.rs:540
   still passes `memory_remainder_bytes` as the process budget, and this
   PR's query.rs and lib.rs changes are comments and tests only. Failure
   scenario: a loopback metrics deployment with a wide PromQL fanout returns
   503s that a 25% share would not have produced, and nothing in decision 4
   would have caught it. Fix: state in Consequences which path the bound
   guards and name the PromQL exposure, or add a PromQL check to decision 4.

4. **nit (deferred from round 3). Zero-budget message names one flag.**
   config.rs:3273: "no --cache-max-bytes value can satisfy this check
   against a 0-byte budget". There are two cache flags now. Tests at
   config.rs:8471 and config.rs:8505 pin the substring, so the fix touches
   three lines. Failure scenario: an operator on a 0 budget with
   `--catalog-cache-max-bytes` set tries lowering that flag next, because
   the message only rules out the other one. Follow-up.

5. **nit. `--catalog-cache-max-bytes 0` is a silent disable.**
   config.rs:1487-1503 (the flag's help). A `0` reaches
   `query::build_catalog` as the disabled sentinel: no byte cache, no
   metrics handle, and no disk tier even with `--cache-dir` (query.rs:184,
   query.rs:234). `--cache-max-bytes 0` instead builds a zero-capacity
   fetch cache with its counters (store.rs:59). The help says neither, and
   `0` passes `check_memory_budget` as a 0 cap. Failure scenario: an
   operator sets `0` to keep the catalog cache small, loses the
   `cache="catalog"` metrics the observability guide tells them to read,
   and the two "zero" flags behave differently. Follow-up: document it, or
   refuse `0` the way `--logs-max-fetch-run-bytes` does.

6. **nit. Index and Refs omit ADR-0088.**
   docs/adrs/README.md:181 says ADR-2023 amends ADR-1170, ADR-1196 and
   ADR-0996; the PR also amends ADR-0088. ADR-2023's own Refs line (line 5)
   lists none of ADR-0088, ADR-1196 or ADR-0996. Failure scenario: a reader
   following the index to every document ADR-2023 touched misses the
   ADR-0088 table row. Fold into the acceptance edit.

Checked and found sound (the property, and the input that would have
falsified it):

- `resolve_logs_fetch_policy` (config.rs:4569) never consults the store or
  endpoint: unset resolves `cost-based`/`default` on `--store s3` with
  `http://127.0.0.1:9000`, `http://localhost:9000`, a remote endpoint, no
  endpoint, and `--store memory`; an explicit `cost-based`, `byte-minimal`
  or `latency-first` on loopback resolves with source `flag`. The withdrawn
  `derived-loopback-endpoint` string survives only inside two test doc
  comments (config.rs:9884, 9889); no other crate references it.
- `store_is_loopback` (config.rs:4548) requires both `--store s3` and an
  endpoint `is_loopback_endpoint` accepts, the same predicate the plaintext
  refusal uses, so a stray `RAVEL_S3_ENDPOINT` under `--store memory`
  cannot change the share (mutation D below).
- The fetch-cache match (config.rs:3096) orders the explicit-flag arm before
  the loopback arm, so `--cache-max-bytes` wins verbatim on loopback, and
  the loopback arm requires known memory, so the unknown-memory path falls
  back to 256 MiB with source `fallback` on loopback too. 40% of the
  30,064,771,072-byte reference budget is 12,025,908,428, as the test pins;
  40% of the ClickBench 30,756,311,040-byte budget is 12,302,524,416, the
  ADR's 12.3 GB.
- The catalog match (config.rs:3115) reads only `flags.catalog_cache_max_bytes`
  and never `store_is_loopback`, so the catalog share is 5% on every host
  shape: 1,503,238,553 on the reference host with or without
  `--cache-max-bytes 4096`.
- `check_memory_budget` (config.rs:3317) sums both caps and refuses at or
  above the budget; `--disable-cache` and unknown memory still bypass it.
  With the flags decoupled, a single `--cache-max-bytes 20000000000` no
  longer refuses on the reference host (20,000,000,000 + 1,503,238,553 is
  below 30,064,771,072), and the tests were updated to set both flags. That
  is a behaviour change for an operator who relied on one big value being
  refused; the CHANGELOG covers the direction that matters (a low value no
  longer shrinks the catalog cache).
- `MemoryBudgetExceeded::fmt` (config.rs:3252) names both flags on the
  fixable arm; the zero-budget arm is finding 4.
- The rebase left no semantic conflict with d8a574b1 beyond finding 3:
  `ServerConfig::process_memory_budget_bytes` is still the remainder,
  `MemoryBudget::new` at lib.rs:2026 is built from it once, and the fetch
  and hand-off shares inside `MemoryBudget` are counters over reservations,
  not sizes derived from `cache_max_bytes`, so a larger cache changes what
  is reported as hand-off overlap, not what is charged twice.
- Every guide and reference sentence describing a loopback `byte-minimal`
  default now lives under an amendment heading or a Superseded status
  (docs/adrs/1196 lines 101 and 150, docs/adrs/2014, docs/adrs/0996's
  earlier amendment) or was rewritten; a grep for "loopback" beside
  "byte-minimal" outside ADR-2014 and ADR-2023 finds only those, the
  rewritten guide rows, and the runbook row. `cli_reference` passes, so the
  flag reference matches the clap help.
- `crates/ravel-bench/src/bin/sql_latency_bench.rs` and
  `crates/ravel-object-store/src/s3.rs` changes are comments only.

## Mutation checks

Each mutation edited one production line in
services/ravel-server/src/config.rs, ran the one test that guards it with
`timeout 540 cargo test -p ravel-server --lib <test>`, then restored the
file with `git checkout --` and confirmed a clean diff. Every run exited 101
(one test failed, 770 filtered out). Failure lines are quoted from the run.

1. **Reintroduce the withdrawn loopback arm** in `resolve_logs_fetch_policy`
   (`None if self.store_is_loopback() => (ByteMinimal,
   "derived-loopback-endpoint")` ahead of the plain `None` arm).
   Test: `resolve_logs_fetch_policy_resolves_cost_based_by_default_even_on_a_loopback_endpoint`.
   Failed at config.rs:9909 (case 1, the loopback IPv4 literal):
   `left: (ByteMinimal, "derived-loopback-endpoint")` /
   `right: (CostBased, "default")`. Matches the test's own prove-the-test
   note (a).

2. **Loopback arm uses the 25% constant** (`percent_of(memory_budget_bytes,
   LOOPBACK_CACHE_MEMORY_PERCENT)` to `CACHE_MEMORY_PERCENT`).
   Test: `loopback_store_carves_a_larger_fetch_cache_share`.
   Failed at config.rs:8770: `left: 7516192768` / `right: 12025908428`.

3. **Catalog match reads the fetch flag** (`match
   (flags.catalog_cache_max_bytes, ...)` to `match (flags.cache_max_bytes,
   ...)`), which restores the pre-ADR-2023 coupling.
   Test: `the_catalog_cache_derives_at_its_own_share_independent_of_the_fetch_flag`.
   Failed at config.rs:8721: `left: 12345678` / `right: 1503238553`.

4. **Drop the `--store s3` conjunct** from `store_is_loopback` (the
   `matches!(self.store, StoreKind::S3)` conjunct replaced by `true`).
   Test: `only_a_store_s3_loopback_endpoint_gets_the_larger_share`.
   Failed at config.rs:8835 (the `--store memory` with a loopback-shaped
   endpoint case): `left: 12025908428` / `right: 7516192768`.

5. **Hard caps forget the catalog cap** (`cache_max_bytes.saturating_add(
   catalog_cache_max_bytes)` to `cache_max_bytes` at config.rs:3134; the
   same sed also touched a doc comment at 9407, which is inert).
   Test: `startup_refuses_hard_caps_over_the_memory_budget`.
   Failed at config.rs:9152: `hard caps of 40,000,000,000 must exceed the
   30,064,771,072 budget` with the mutant resolving
   `memory_hard_caps_bytes: 20000000000, memory_remainder_bytes:
   10064771072` and starting instead of refusing.

6. **CLI wiring drops the predicate** (`store_is_loopback:
   self.store_is_loopback()` to `store_is_loopback: false` in
   `performance_flags`), the one line between the predicate and the
   derivation.
   Test: `loopback_store_carves_a_larger_fetch_cache_share` (it drives
   `Cli::resolve_performance`, so it covers the wiring, not only the pure
   function). Failed at config.rs:8770: `left: 7516192768` /
   `right: 12025908428`.

Not mutated: the explicit-flag-wins arm ordering (the test's note (c)
describes it and the arm order is visible at config.rs:3096-3100), and
`MemoryBudgetExceeded::fmt`'s flag names (a string test; finding 4 covers
the one it gets wrong).

## Gates

Head: e62d8107 "docs(changelog): note the catalog cache growth when
--cache-max-bytes is low". The clone is shallow (depth 1), so HEAD is one
grafted commit; `git log origin/main..HEAD` lists only it, and the reviewed
diff is `origin/main..HEAD` (origin/main 7864862b): 23 files, 890 insertions,
394 deletions. Host: x86_64, 16 vCPU, 30 GiB; `CARGO_BUILD_JOBS=4`. The
tripwire `git config` took 0.002 s.

| Command | Exit |
|---|---|
| `mkdir -p .gate-logs && git check-ignore -q .gate-logs && scripts/guards/check-disk-headroom.sh .gate-logs 5 && df -h /tmp . "$HOME"` | 0 (296 GB free) |
| `timeout 540 cargo fmt --all --check` | 0 |
| `timeout 540 cargo clippy -p ravel-server --all-targets -- -D warnings` | 0 (2 m 26 s) |
| `timeout 540 cargo test -p ravel-server --lib config` | 0 (250 passed, 521 filtered out) |
| `timeout 540 cargo test -p ravel-server --test cli_reference` | 0 (3 passed) |
| `python3 scripts/check_docs.py` | 0 ("docs gate: clean.") |
| `scripts/guards/check-amendment-integrity.sh` | 0 ("clean (116 amendment(s) over 165 file(s))") |

Logs are under `.gate-logs/` in the checkout (gitignored). No timeout (124)
occurred, so no run was split.

## Outcome playbook

The bar (decision 4) and the arms, recomputed:

| Figure | Bar | Candidate arm | Control arm |
|---|---|---|---|
| `--cache-max-bytes` | | unset, derives 12,302,524,416 (40%) | 7,689,077,760 (25%) |
| shared remainder | | 16,915,971,072 (55%) | 21,529,417,728 (70%) |
| concurrent QPS | at least 0.40 | | |
| error ratio | at most 0.058 | | |
| cold sum | 1,567.7 s to 1,916.1 s (10% either side of 1,741.9) | | |
| hot sum | at most 274.5 s | | |

The share is credited only when the candidate beats the control by more
than the 15% noise floor on hot sum (candidate at most 0.85 times the
control) or on QPS (candidate at least 1.15 times the control). Read the
error ratio as well, on both arms: the remainder is what the SQL executor
reserves from under ten concurrent statements, and the candidate has
4,613,446,656 fewer bytes of it, so a candidate error ratio above the
control's by more than the floor is the share's cost showing, whatever the
QPS says.

1. **Candidate passes and beats the control.** Keep 40%. Record both arms'
   figures in decision 4, add the control arm and the rule (finding 2), move
   ADR-2023 to Accepted, and cut v0.19.0 from the merged main.

2. **Candidate passes and ties the control.** Record decision 3 as
   unmeasured-neutral in the ADR. My recommendation is then to revert the
   constant to 25% in the acceptance follow-up rather than keep it: the
   share has a measured cost (4.6 GB of remainder on the reference box, the
   PromQL exposure in finding 3, and a disk tier that also grows to 40% on
   the disk the store reads from) and a benefit the run could not see. The
   likely reason for a tie is that the host page cache already holds the
   11.24 GB corpus for RustFS on a 30 GB box, so the fetch cache saves an
   HTTP round trip and a decode, not a disk read, and 7.69 GB of it is
   enough for the statements that repeat. If the owner prefers to keep the
   mechanism for a later measurement, keep the code and set
   `LOOPBACK_CACHE_MEMORY_PERCENT` to 25 so the source string still
   distinguishes the path without changing the carve. Either way, release:
   decisions 1 and 2 are what the release needs.

3. **Candidate fails, control passes.** The share is the cause, since it is
   the only variable. Revert to 25% (drop decision 3, the constant, the
   `budget-carve-loopback` source and their tests, and the CHANGELOG bullet)
   before tagging, and record the two arms in the ADR as the measurement
   that rejected it. Release with decisions 1 and 2.

4. **Both fail.** The share is not the cause and decision 1 alone does not
   reach the bar on that box today. This is a live possibility: the
   same-box explicit `cost-based` arm measured 0.380 to 0.400 QPS and an
   error ratio of 0.101, which already sits at the QPS bar and above the
   0.058 error bar, and the 0.502 QPS the bot's v0.17.0 run reached on
   another box suggests box-to-box variance of the same order as the noise
   floor. Do not release on the acceptance claim. First classify the errors
   (deadline, memory budget exhausted, 503) from the driver's output, then
   run v0.17.0 on the same box to learn whether the 0.058 bar is
   reproducible there at all. If v0.17.0 also misses it, the bar is a
   box artifact: re-register the bar against that measurement and re-read
   the candidate. If v0.17.0 passes and the candidate does not, something
   between v0.17.0 and the candidate other than the fetch policy regressed
   the concurrent phase; bisect with the fetch policy held explicit, and
   hold v0.19.0 until the cause is named. Decision 3 stays unmeasured in
   that outcome; do not credit or blame it.

## Release and upstream advice

1. **Cut v0.19.0 only after the candidate arm passes decision 4**, and
   only from merged main. The Unreleased section already carries the three
   ADR-2023 bullets and main's scrub-resume and PromQL-budget entries. The
   acceptance build differs from the release by main's two commits and the
   changelog note; neither can move a SQL-only benchmark (PromQL charging
   touches the PromQL engines, scrub runs in the maintain loop), so accept
   the pre-rebase measurement for the bar and say so in the ADR rather than
   re-running on the tag. Before tagging: apply findings 1 to 3 in the
   ADR-acceptance edit, move ADR-2023 to Accepted with the figures, file
   findings 4 to 6 as one follow-up issue.

2. **Then pin v0.19.0 in a new upstream ClickBench PR for the stock entry.**
   If an upstream PR for 0.18.0 is still open, replace it rather than let
   it land: its concurrent phase (0.123 QPS, error ratio 0.140) is the
   regression this PR withdraws, and a published 0.18.0 entry would show
   it.

3. **Do not ship the tuned entry (explicit `byte-minimal` plus a larger
   query pool) now.** Byte-minimal's collapse is in the concurrent phase,
   and ClickBench publishes that phase. A larger query pool addresses SQL
   memory, and the measured bottleneck is IOPS: about 5.5 structural GETs
   per object (footer suffix, directory sections, block stats) pinned gp2
   at its 3,000 IOPS burst (iostat 3,220 r/s, 50 KB average, 87% util),
   and a 40% cache moved QPS only from 0.123 to 0.170 with error ratios
   between 0.089 and 0.307 across the byte-minimal arms. A pool cannot buy
   IOPS. Ship the tuned entry when one of two things is true: it passes
   the same concurrent bar in a fresh two-phase run, or the structural
   reads are coalesced (one suffix read that covers footer, directory and
   stats, or those sections cached across statements) so the per-object
   request count under `byte-minimal` drops toward one. That coalescing is
   the engineering follow-up that would make the ranged plan viable on
   loopback again, and it belongs on the issue that tracks the tuned entry.
   gp3 would not change this: its baseline is the same 3,000 IOPS.

## ADR-2023 notes

- **Missing: the control arm and the tie rule** (finding 2). Decision 4
  as written cannot attribute anything to decision 3.
- **Overclaimed: "The error-ratio bound in decision 4 is what guards"
  the remainder shrink** (Consequences, lines 108-111). Since d8a574b1 the
  remainder also backs PromQL fetches, which ClickBench never issues
  (finding 3).
- **Missing: the competing explanation for the hot-time claim**
  (Consequences, line 106: "expected to cut hot times, since whole objects
  held in a corpus-sized cache serve every statement"). On a 30 GB host the
  kernel page cache holds the 11.24 GB corpus for RustFS after the cold
  pass, so a Ravel-side miss costs a loopback HTTP round trip and a decode,
  not a disk read. State that as the hypothesis a tie in the two-arm run
  would confirm, so the result has a reading either way.
- **Inconsistent: two error ratios for one 0.123 QPS figure.** The Context
  table (line 24) gives the byte-minimal, 7.69 GB arm as 0.123 QPS and
  error ratio 0.245; the second table (line 45) gives v0.18.0 stock as
  0.123 QPS and error ratio 0.140. The same QPS to three digits from two
  different runs is possible but unlikely; say which run each row is, or
  correct the one that cites the wrong ratio.
- **The cold bar is two-sided and should be one-sided** (line 83, "within
  10% of v0.17.0's 1,741.9 s"). A cold sum below 1,567.7 s is not a
  failure. Write "at most 1,916.1 s".
- **Decision 2's refusal wording** ("whose two caps together exceed
  `memory_budget_bytes`"). The code refuses at or above the budget
  (`memory_hard_caps_bytes >= memory_budget_bytes`, config.rs:3320), per
  ADR-1170's 2026-09-07 amendment. Write "reach or exceed".
- **Decision 3's corpus arithmetic holds but rests on two constants it
  does not name.** A whole-object cache holds the corpus only while every
  object fits `CACHE_MAX_ENTRY_BYTES` (64 MiB, store.rs:21) and the entry
  count stays under `CACHE_MAX_ENTRIES` (1,000,000, store.rs:25). A folded
  ClickBench tenant is about 8,400 sealed segments, so both hold; naming
  them makes the "above its 11.24 GB corpus" sentence checkable. 12.3 GB
  is 9% above the corpus, which leaves little for LRU churn from
  non-repeating statements; the hot-sum result is what tells whether that
  margin matters.
- **The mechanism claim lacks its evidence.** Context says the ranged plan
  "moved twice the disk bytes of `cost-based` for a third of the
  throughput" but the iostat figures that show the IOPS ceiling (3,220
  r/s, 50 KB average, 87% util against gp2's 3,000 IOPS burst) are not in
  the ADR. Put them beside the claim; they are the reason decision 1 is a
  revert rather than a cache-size change.
- **Rejected alternatives omit the one that addresses the mechanism.**
  "Keep `byte-minimal` and coalesce its structural reads" is neither
  rejected nor deferred in the text. List it as deferred, with a pointer to
  the tuned-entry issue, so the ranged plan's future on loopback is
  recorded as an open engineering item rather than closed by this ADR.
- **Refs and index** (finding 6): add ADR-0088, ADR-1196 and ADR-0996 to
  the Refs line; add ADR-0088 to the README row.
- **Status.** Proposed is right until decision 4 has figures; the
  acceptance edit is where it moves.

Recomputed and confirmed: 40% of 30,756,311,040 is 12,302,524,416 (12.3 GB);
25% is 7,689,077,760 (the control arm); 15 points is 4,613,446,656 (the
"about 4.6 GB"); 30,756,311,040 minus two 12,000,000,000 caps is
6,756,311,040 (the "6.76 GB" remainder of the 12 GB arm); 40% of the
30,064,771,072-byte reference budget is 12,025,908,428 (the ADR's 12.03 GB
and the test's pinned value); 5% of it is 1,503,238,553.

## Not checked

- The two acceptance runs. I have no access to the instances or their
  output; the playbook above is written against the pre-registered
  reading and the figures in the task brief.
- The full workspace gates, the `sql` and `flight-sql` feature lanes, and
  the ravel-server integration tests. Only the six listed gates ran.
  `services/ravel-server/tests/logs_fetch_policy_e2e.rs` was read, not
  run; every policy it asserts on is passed as an explicit flag, so it does
  not depend on the withdrawn default.
- The PR's individual commits. The clone is shallow, HEAD is one grafted
  commit, so commit messages, authorship and the wip-fold state of the
  branch could not be reviewed; the diff was reviewed as a whole.
- The 11.24 GB corpus figure, the iostat figures and the error ratios
  0.245, 0.140, 0.307 and 0.101: taken from the ADR and the brief, not
  recomputed from run artifacts.
- `is_loopback_endpoint`'s coverage of `::1` and the whole 127.0.0.0/8
  block: relied on the existing tests in crates/ravel-object-store/src/s3.rs,
  which this PR touches only in a comment.
- The page-cache explanation for a tie is a hypothesis, not a measurement.

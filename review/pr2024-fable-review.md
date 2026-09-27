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

(pending)

## Gates

(pending)

## Outcome playbook

(pending)

## Release and upstream advice

(pending)

## ADR-2023 notes

(pending)

## Not checked

(pending)

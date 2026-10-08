# Issue #2633: does a shorter jemalloc decay bound retention under load?

Measurement only. No product code was changed. The rerun in
`investigations/2633-rerun/` (task db9e7a3c) found that with jemalloc's
background purge thread on, retention (`resident` minus `allocated`) under
concurrent Parquet SQL load still reached 5.4 GB. It inferred that the
cause is jemalloc's dirty-page decay window (`dirty_decay_ms`, 10 s by
default). This run tests that inference directly: the same server binary,
with and without `_RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:0`,
two runs each, interleaved A1, B1, A2, B2.

## Summary

| Row | Figure | A1 | A2 | B1 | B2 | Band | Verdict |
|---|---|---|---|---|---|---|---|
| 1 | B retention at its peak-gap sample | | | **4.331 GB** | **4.650 GB** | pass < 1.0, miss >= 1.5 (both B runs) | **MISS** |
| 2 | Mean peak VmRSS, B vs A | 9.885 | 10.190 | 8.833 | 8.815 | pass >= 2.0 GB lower, miss < 1.0 GB lower | **neither**: 1.214 GB lower |
| 3 | Mean concurrency qps, B vs A | 7.997 | 7.262 | 6.647 | 6.941 | pass within 10 %, miss >= 20 % lower | **neither**: 10.9 % lower |
| 4 | A retention at its peak-gap sample | 6.537 GB | 6.107 GB | | | reported | 0.43 GB apart |

In words: a 1 s decay cuts typical retention under load by about two
thirds (median 3.58 and 3.80 GB in A against 1.27 and 1.39 GB in B), and
mean VmRSS over the concurrency phase by about 2 GB. It does not bound the
peak. Both B runs still had a 5 s sample with 4.3 to 4.7 GB of retention,
so the pre-registered row 1 misses. The peak-VmRSS saving (1.21 GB) and the
throughput cost (10.9 % lower qps) both landed between their pass and miss
bands. The two A runs alone differ by 9.2 % in qps, so two runs per arm do
not separate a 10.9 % cost from run-to-run noise.

All four arms are valid: 43 of 43 serial statements answered with no error
in every arm, and every arm's startup stamp read
`allocator_background_thread=true source="server"`.

## Environment

- Host: `ip-172-31-18-6` (fleet executor, amd64 class), Linux
  7.0.0-1011-aws x86_64, nproc 16, MemTotal 32,132,612 kB, about 210 GB
  free on the checkout volume. The same host as the baseline and the rerun.
- Commit measured: `5ad3f6a` (docs: add ADR-2430 authenticated .pqm
  manifest), which contains #2652 and #2653. Toolchain rustc 1.97.1.
- Server: `cargo build --release --locked -p ravel-server --features sql`
  (no `heap-profiling`), copied out as `ravel-server-5ad3f6a`, sha256
  `b65e02d0d58e4997ff1959f2a1f1d24c4264ec938166f8dba1d987f42acf1f9a`.
  All four arms ran this one binary.
- Bench and CLI: `cargo build --release --locked -p ravel-cli -p
  ravel-bench --features ravel-bench/sql-latency --bin ravel-cli --bin
  clickbench_parquet_bench`, built with `bench-arm-a-20-files.diff`
  applied, then the source was reverted before the server build.
  `clickbench_parquet_bench` sha256
  `dc801e8c2bc9a97f0d6a2f42c22a0ce4196592e853f3edc117b231d59ef396ad`,
  `ravel-cli` sha256
  `5bfc2fdfcbce110f8076607b23abfdd74c868b4ae1365d0860280863238f7b88`.
- Store: RustFS 1.0.0 (build 2026-09-16, binary sha256
  `222eedc3d9baabf6516702d9fbf230270c3ca49b50f562d3461c96e2cc6ae6ad`) on
  loopback `127.0.0.1:39000`, data on the checkout volume.
- Data: ClickBench `hits_compatible/athena_partitioned/hits_0..hits_19.parquet`,
  20 files, 2,711,471,866 bytes (the baseline's exact total), uploaded once
  to `s3://clickbench-parquet/hits/`.

## Setup

As in `investigations/2633/README.md`, Commands section:

- `ravel-cli ... --tenant-hash-unkeyed tenant parquet-grant add --tenant
  clickbench --location s3://clickbench-parquet/hits/ --profile rustfs`,
  then `ravel-cli ... --tenant-hash-unkeyed store qualify` (qualified,
  suite v2). Once, before A1.
- Server per arm: `--store s3 ... --tenant-hash-unkeyed --parquet-profiles
  profiles.json --tenant-token "<token>=clickbench;ddl"
  --memory-budget-bytes 8000000000`, with a fresh `RAVEL_AUDIT_TOKEN_KEY`
  and token per arm. Resolved budget identical in all four
  `server-stamps.txt`: cache 3.2e9, catalog cache 4e8, remainder 4.4e9,
  `sql_max_query_bytes` = `sql_tenant_max_bytes` = 3,960,000,000, fetch
  concurrency 32.
- Arm A: `_RJEM_MALLOC_CONF` unset. Arm B:
  `_RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:0`. Each arm's
  `malloc-conf.txt` records what the start script set; the variable's
  presence or absence was also read back from `/proc/<pid>/environ` of the
  running server. jemalloc printed no invalid-conf warning in any server
  log.
- Sampler: `sampler.py` from the rerun branch, 5 s, started right after the
  startup-stamp check.
- Bench: `clickbench_parquet_bench --arm a --location
  s3://clickbench-parquet/hits/ --reference ref-dummy --prereg
  prereg-2633.toml ... --sql-max-query-bytes 3960000000
  --sql-tenant-max-bytes 3960000000 --concurrency-seconds 360` (10
  connections, the default): serial pass, then the concurrency phase.
- After the bench: 120 s idle, still sampling, then SIGTERM and wait for
  exit. Every server logged `shutdown complete`. One server at a time.

Timeline (unix seconds; t = 0 is the concurrency phase start):

| arm | server start | concurrency start | bench end | shutdown complete (UTC) |
|---|---|---|---|---|
| A1 | 1791481971.3 | 1791482003.5 | 1791482364.5 | 18:01:31 |
| B1 | 1791482538.8 | 1791482574.8 | 1791482935.0 | 18:11:01 |
| A2 | 1791483070.9 | 1791483096.9 | 1791483457.9 | 18:19:44 |
| B2 | 1791483590.0 | 1791483618.1 | 1791483978.8 | 18:28:25 |

## Results per arm

All memory figures in GB (1e9 bytes). "Retention" is `resident -
allocated`; "uncharged live" is `allocated - accounted`, with accounted
the pre-registered sum (SQL and fetch reservations plus fetch and catalog
cache resident).

| figure | A1 | B1 | A2 | B2 |
|---|---|---|---|---|
| peak gap (`resident - accounted`) | 6.691 at +328.3 s | 4.913 at +249.6 s | 7.063 at +269.5 s | 5.003 at +112.5 s |
| of which retention | **6.537** (97.7 %) | **4.331** (88.1 %) | **6.107** (86.5 %) | **4.650** (92.9 %) |
| of which uncharged live heap | 0.155 (2.3 %) | 0.582 (11.9 %) | 0.956 (13.5 %) | 0.353 (7.1 %) |
| handoff overlap at that sample | 0.013 | 0.309 | 0.050 | 0.013 |
| median retention over the phase (72 samples) | 3.584 | 1.270 | 3.796 | 1.393 |
| p90 retention over the phase | 4.819 | 2.115 | 4.919 | 2.367 |
| phase samples with retention >= 1.5 | 72 of 72 | 23 of 72 | 72 of 72 | 33 of 72 |
| peak `resident` | 10.831 at +193.3 s | 9.814 at +279.6 s | 11.054 at +269.5 s | 9.260 at +12.5 s |
| peak VmRSS | 9.885 at +253.3 s | 8.833 at +199.6 s | 10.190 at +269.5 s | 8.815 at +12.5 s |
| mean VmRSS over the phase | 8.458 | 6.236 | 8.319 | 6.512 |
| concurrency qps | 7.997 | 6.647 | 7.262 | 6.941 |
| completed / errors | 2,887 / 38 | 2,394 / 52 | 2,621 / 45 | 2,504 / 46 |
| idle retention at +60 s | 0.086 | 0.088 | 0.087 | 0.086 |
| idle retention at +120 s | 0.086 | 0.088 | 0.086 | 0.086 |
| serial pass | 43 of 43 answered | 43 of 43 | 43 of 43 | 43 of 43 |
| background-thread gauge = 1 | 104 of 104 rows | 105 of 105 | 103 of 103 | 103 of 103 |

Concurrency errors by kind, classified from every server-logged `sql query
error redacted` line (the count matches the bench's in every arm):

| kind | A1 | B1 | A2 | B2 |
|---|---|---|---|---|
| HTTP 422, tenant memory budget exhausted | 24 | 30 | 26 | 30 |
| HTTP 422, query pool full, spill disabled | 10 | 18 | 14 | 16 |
| HTTP 422, process memory budget exhausted | 3 | 1 | 2 | 0 |
| fetch memory exhausted, sent as HTTP 503 "upstream storage temporarily unavailable" | 1 | 3 | 3 | 0 |
| total | 38 | 52 | 45 | 46 |

Errors by statement are in each arm's `analysis.txt`. q19, q33, q34 and
q35 refuse most often in every arm, as in the baseline.

## What the rows say

- **The decay setting took effect.** With the background thread on in both
  arms, B's median retention is about a third of A's, and in A every one of
  the 72 phase samples carries at least 1.5 GB of retention, against 23 and
  33 in B.
- **It does not bound the peak.** B's retention is spiky: in B1 it ran
  0.9, 2.3, **4.3**, 2.1, 0.9 GB across five consecutive samples
  (+240 s to +260 s), in B2 1.0, **4.7**, 2.0 GB. A 1 s decay returns the
  pages within seconds, but a burst of frees inside one window (several
  heavy aggregations finishing together) still leaves 4 to 5 GB resident
  for that moment. Since the peak is what an OOM sees, row 1's miss is the
  operative result for #2633: shortening the decay alone is not a bound.
- **Peak VmRSS fell by 1.21 GB on average**, less than the 2.0 GB fall in
  mean VmRSS over the phase, for the same reason: the peaks are bursts.
- **Throughput fell 10.9 %**, just outside the pass band. The two A runs
  differ by 9.2 % from each other (7.997 and 7.262) and the two B runs by
  4.2 %, so this run cannot tell whether the cost is real. B also refused
  more statements (52 and 46 against 38 and 45), which is consistent with
  a cost but is within the same spread.
- **Idle is the same in both arms** (0.086 to 0.088 GB by +60 s), as the
  rerun found with the background thread alone.
- **Uncharged live heap at the peak-gap sample is small** in all four arms
  (0.16 to 0.96 GB), because each arm's peak gap fell on a
  retention-dominated sample, as in the rerun.

## Deviations

1. **Idle hold 120 s, not the rerun's 300 s or more**, as this task
   specified. `analyze.py` is kept unmodified, and its IDLE-LAST line
   requires a sample at least 300 s after bench end, so in every arm it
   prints everything up to that point and then stops with `IndexError`
   (recorded in `analysis.txt` with exit 1). `decay.py` supplies the idle
   figures at +60 s and +120 s and every other per-arm figure above, and
   exits non-zero if an idle sample is missing (it exited 0 in every arm).
2. **Dummy reference made of zero-byte `qNN.json` files.** The bench
   graded every statement `fail` rather than the baseline's `none`, so the
   bench exits 1 with "D7 check" violations (fail verdicts plus refusals of
   statements the scratch prereg does not list). Answers were not
   compared in either case; this has no bearing on the memory figures, and
   the validity rule (serial statement answered or typed budget-refused)
   is read from each statement's `error` field, which is null for all 172.
3. **Page cache not dropped** between arms (no root), as in the baseline.
   The data sits on the local disk and was read by every arm.
4. **Error kinds come from the server log**, because the bench report keeps
   only each statement's first error. One kind is not a typed 422: a
   fetch-memory refusal (`segment fetch failed: fetch memory exhausted`)
   reaches the client as HTTP 503 "upstream storage temporarily
   unavailable" (1, 3, 3 and 0 times). These occurred only in the
   concurrency phase, where the validity rule does not apply.
5. **Rows 2 and 3 landed between their pass and miss bands.** The
   pre-registration defines no verdict for that region; it is reported as
   "neither" with the figure.
6. **Peak VmRSS is below peak `resident`** in every arm (by 0.4 to
   1.0 GB). Both are reported as measured: VmRSS is the kernel's count from
   `/proc/<pid>/status`, `resident` is jemalloc's own estimate, and the two
   were not reconciled here.
7. **RustFS commit not confirmed.** The RustFS binary on this host reports
   1.0.0 but no commit; its sha256 is recorded above. Port 39000 was free;
   the RustFS instances on 9000 and 19000 belonged to other work and were
   left untouched.

## Files

| file | contents |
|---|---|
| `<arm>/samples.tsv` | every 5 s sample, raw series included |
| `<arm>/bench-report.json`, `<arm>/bench.log` | bench report and log |
| `<arm>/t-server-start`, `<arm>/t-bench-end` | the timestamps `analyze.py` and `decay.py` read |
| `<arm>/server-stamps.txt` | resolved defaults, background-thread stamp, listening and shutdown lines |
| `<arm>/server-errors.txt` | every server-logged query error, tenant hash removed |
| `<arm>/malloc-conf.txt` | the `_RJEM_MALLOC_CONF` the start script set |
| `<arm>/analysis.txt` | `analyze.py` output, then `decay.py` output |
| `analyze.py`, `sampler.py` | unmodified from the rerun branch |
| `decay.py` | the per-arm figures `analyze.py` does not print |
| `prereg-2633.toml`, `bench-arm-a-20-files.diff` | the scratch prereg and the local bench patch, unchanged |

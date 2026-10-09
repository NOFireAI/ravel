# Issue #2633: the uncharged heap with the aggregate hold

Measurement only. No product code was changed. PR #2685 made the SQL
memory pool keep a non-spillable `GroupedHashAggregateStream` consumer's
shrunk bytes charged until the consumer unregisters
(`crates/ravel-sql/src/memory.rs`). This run repeats arm A of the decay
experiment (`investigations/2633-decay/` on
`task/c3d2ad79-81ac-4554-8d4f-28a3be9c9ff0/result`: default jemalloc
decay, `_RJEM_MALLOC_CONF` unset) twice, R1 and R2, on one server binary
built at a commit that contains the hold, and reports the rows
pre-registered on #2633.

## Summary

All figures in GB (1e9 bytes). "Uncharged" is `allocated - accounted +
handoff_overlap` per sample, with accounted the decay README's sum (SQL and
fetch reservations plus fetch and catalog cache resident).

| Row | Figure | R1 | R2 | Band | Verdict |
|---|---|---|---|---|---|
| 1 | max uncharged over all live samples | **2.478** at unix 1791584073.167 (22:14:33 UTC, t = -12.4 s, serial pass) | **1.816** at unix 1791584904.477 (22:28:24 UTC, t = +242.5 s) | pass < 2.0, miss >= 3.0 (each run) | R1 **neither**, R2 **pass**; overall **neither** (no miss) |
| 2 | median uncharged over the concurrency phase | 0.597 | 0.538 | reported; same figure for decay A1 / A2: 0.820 / 0.729 | reported |
| 3 | concurrency qps, mean of R1 and R2 | 8.046 | 8.008 | mean **8.027**, 5.2 % above 7.63; pass within 15 %, miss >= 25 % lower | **PASS** |
| 4 | typed 422 refusals | **53** (55 errors in all) | **87** (90 errors in all) | reported; decay A: 38 and 45 errors in all, of which 37 and 42 typed 422 | reported |
| 5 | peak retention (`resident - allocated`) | 6.632 at t = +162.6 s | 5.998 at t = +147.5 s | reported (decay A: 6.537, 6.107) | reported |
| 5 | peak VmRSS | 9.583 at t = +97.6 s | 9.769 at t = +287.5 s | reported (decay A: 9.885, 10.190) | reported |
| 5 | mean VmRSS over the phase | 8.161 | 8.143 | reported (decay A: 8.458, 8.319) | reported |

t = 0 is each run's concurrency phase start.

Components at each run's row 1 maximum:

| component | R1 (t = -12.4 s) | R2 (t = +242.5 s) |
|---|---|---|
| allocated | 5.054 | 4.708 |
| accounted | 3.674 | 3.458 |
| handoff_overlap | 1.099 | 0.566 |
| sql_reserved | 0.000 | 0.138 |
| fetch_reserved | 1.099 | 0.629 |
| fetch_cache_resident | 2.575 | 2.692 |
| catalog_cache_resident | 0.000 | 0.000 |
| resident | 5.494 | 7.288 |
| VmRSS | 5.334 | 6.984 |
| uncharged | **2.478** | **1.816** |

In words: during the concurrency phase the uncharged heap stayed under
2.0 GB in both runs (phase maxima 1.333 and 1.816; median 0.597 and 0.538,
against 0.820 and 0.729 for the decay A runs computed the same way). Row 1
still does not pass, because R1's largest sample (2.478 GB) fell in the
serial pass, at a moment when no SQL bytes were reserved and the whole
fetch reservation (1.099 GB) was handoff overlap. The comparison is weak in
both directions: decay A1's own maximum was 3.428 GB (a miss) and decay
A2's 1.914 GB (a pass), so two runs per arm span the full band.
Throughput did not fall (mean qps 8.027 against 7.63). Refusals rose:
53 and 87 typed 422s against 37 and 42, mostly "query pool full, spill
disabled" (17 and 42, against 10 and 14), which is the direction expected
when shrunk aggregate bytes stay charged. Retention and VmRSS are about
where decay A left them: the hold does not touch allocator retention,
which still reaches 6.0 to 6.6 GB at its peak.

Both runs are valid: 43 of 43 serial statements answered with no error in
each, every startup stamp read `allocator_background_thread=true
source="server"`, and `ravel_process_allocator_background_thread` read 1 in
every sampled row (105 and 104).

## Environment

- Host: `ip-172-31-18-6` (fleet executor, amd64 class), Linux
  7.0.0-1011-aws x86_64, nproc 16, MemTotal 32,132,612 kB, about 190 GB
  free on the checkout volume. The same host as the baseline, the rerun and
  the decay experiment.
- Commit measured: `6b794db9` (ci(object-store): pin the expected skips in
  the verify-protection cases), which contains PR #2685. Toolchain rustc
  1.97.1.
- Server: `cargo build --release --locked -p ravel-server --features sql`
  on a clean tree, copied out as `ravel-server-6b794db9`, sha256
  `180872c31750c3edd3e7a9dea38b05832df272df8fed30d8b2872d9ae4983cf1`.
  Both runs ran this one binary.
- Bench and CLI: `cargo build --release --locked -p ravel-cli -p
  ravel-bench --features ravel-bench/sql-latency --bin ravel-cli --bin
  clickbench_parquet_bench`, built after the server with
  `bench-arm-a-20-files.diff` applied; the source was reverted right after.
  `clickbench_parquet_bench` sha256
  `92b3d47d11b40666e9c0c046f932088bf434162ac7b1624eaf9104454eafbba9`,
  `ravel-cli` sha256
  `8760daea220407ea350652182c4d01c249444768b6ef6c38600430cbded52a4b`.
- Store: RustFS 1.0.0 (build 2026-09-16, binary sha256
  `222eedc3d9baabf6516702d9fbf230270c3ca49b50f562d3461c96e2cc6ae6ad`, the
  same binary as the decay experiment) on loopback `127.0.0.1:39000`, data
  on the checkout volume, throwaway credentials.
- Data: ClickBench `hits_compatible/athena_partitioned/hits_0..hits_19.parquet`,
  downloaded from `datasets.clickhouse.com`, 20 files, 2,711,471,866 bytes
  (the baseline's exact total), uploaded once to
  `s3://clickbench-parquet/hits/` (20 objects, the same byte total read
  back from the store).

## Setup

As in the decay README, arm A:

- `ravel-cli <store flags> --tenant-hash-unkeyed --parquet-profiles
  profiles.json tenant parquet-grant add --tenant clickbench --location
  s3://clickbench-parquet/hits/ --profile rustfs`, then `ravel-cli <store
  flags> --tenant-hash-unkeyed store qualify`. Qualify passed on the first
  attempt (`qualified (suite v2)`) and both servers started on the record.
  `profiles.json` is one S3 profile `rustfs` for the same endpoint,
  `force_path_style` and `allow_http` on, static credentials from
  `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- Server per run: `--store s3 ... --tenant-hash-unkeyed --parquet-profiles
  profiles.json --tenant-token "<token>=clickbench;ddl"
  --memory-budget-bytes 8000000000`, with a fresh `RAVEL_AUDIT_TOKEN_KEY`
  and token per run and `_RJEM_MALLOC_CONF` unset (`malloc-conf.txt`; the
  variable was also absent from `/proc/<pid>/environ` of each running
  server). The resolved budget is identical in both `server-stamps.txt` and
  matches the decay runs: cache 3.2e9, catalog cache 4e8, remainder 4.4e9,
  `sql_max_query_bytes` = `sql_tenant_max_bytes` = 3,960,000,000, fetch
  concurrency 32.
- Sampler: `sampler.py`, 5 s, started right after the startup-stamp check.
- Bench: `clickbench_parquet_bench --arm a --location
  s3://clickbench-parquet/hits/ --reference ref-dummy --prereg
  prereg-2633.toml ... --sql-max-query-bytes 3960000000
  --sql-tenant-max-bytes 3960000000 --concurrency-seconds 360` (10
  connections, the default): serial pass, then the concurrency phase.
- After the bench: 120 s idle, still sampling, then SIGTERM and wait for
  exit. Both servers logged `shutdown complete`. One server at a time; the
  R1 server had exited before the R2 server started.

Timeline (unix seconds):

| run | server start | concurrency start | bench end | shutdown complete (UTC) |
|---|---|---|---|---|
| R1 | 1791584056.9 | 1791584085.6 | 1791584446.4 | 22:22:58 |
| R2 | 1791584633.4 | 1791584662.0 | 1791585022.7 | 22:32:33 |

## Results per run

| figure | R1 | R2 | decay A1 | decay A2 |
|---|---|---|---|---|
| row 1, max uncharged over all live samples | 2.478 (serial) | 1.816 (phase) | 3.428 (phase) | 1.914 (phase) |
| max uncharged within the concurrency phase | 1.333 | 1.816 | 3.428 | 1.914 |
| max uncharged within the serial pass | 2.478 | 0.251 | 1.197 | 0.748 |
| row 2, median uncharged over the phase (72 samples) | 0.597 | 0.538 | 0.820 | 0.729 |
| p90 uncharged over the phase | 0.943 | 0.971 | 1.741 | 1.243 |
| phase samples with uncharged >= 2.0 | 0 | 0 | 5 | 0 |
| max uncharged while idle | 0.025 | 0.024 | 0.021 | 0.024 |
| peak gap (`resident - accounted`), decay.py | 6.788 at +162.6 s | 6.222 at +147.5 s | 6.691 | 7.063 |
| median retention over the phase | 3.545 | 3.728 | 3.584 | 3.796 |
| peak `resident` | 10.663 at +167.6 s | 10.136 at +357.5 s | 10.831 | 11.054 |
| concurrency qps | 8.046 | 8.008 | 7.997 | 7.262 |
| completed / errors | 2,903 / 55 | 2,889 / 90 | 2,887 / 38 | 2,621 / 45 |
| idle retention at +60 s / +120 s | 0.100 / 0.098 | 0.085 / 0.085 | 0.086 / 0.086 | 0.087 / 0.086 |
| serial pass | 43 of 43 answered | 43 of 43 | 43 of 43 | 43 of 43 |

The decay A columns are this run's `hold.py` and `phases.py` applied to the
decay branch's committed `samples.tsv` (`comparison.txt`), and the decay
README's own figures where `decay.py` already printed them.

Concurrency errors by kind, from every server-logged `sql query error
redacted` line (the count matches the bench's in both runs):

| kind | R1 | R2 | decay A1 | decay A2 |
|---|---|---|---|---|
| HTTP 422, tenant memory budget exhausted | 34 | 42 | 24 | 26 |
| HTTP 422, query pool full, spill disabled | 17 | 42 | 10 | 14 |
| HTTP 422, process memory budget exhausted | 2 | 3 | 3 | 2 |
| typed 422 in all (row 4) | **53** | **87** | 37 | 42 |
| fetch memory exhausted, sent as HTTP 503 | 2 | 3 | 1 | 3 |
| total | 55 | 90 | 38 | 45 |

Errors by statement are in each run's `analysis.txt`. q19 and q33 refuse
most often in both runs, as in every earlier run.

## What the rows say

- **The phase figure moved down.** Within the concurrency phase the
  uncharged heap peaked at 1.333 and 1.816 GB and its median fell from
  0.820 / 0.729 (decay A) to 0.597 / 0.538. Neither run had a phase sample
  at 2.0 GB or more, against 5 in decay A1. That is consistent with the
  hold charging aggregate bytes that were previously released early, but
  the run-to-run spread is as large as the change: decay A2 alone would
  have passed row 1.
- **Row 1's R1 maximum is a serial-pass sample.** At t = -12.4 s, SQL
  reservations read 0 and the fetch reservation (1.099 GB) was entirely
  handoff overlap, so the figure is `allocated - fetch_cache_resident`:
  2.478 GB of heap that neither the SQL pool nor the cache held at that
  instant. It is one 5 s sample; the other five serial-pass samples read
  0.004 to 0.104 GB. The bench report carries no per-statement timestamps, so the
  statement in flight cannot be named; a backward estimate from the
  statement durations puts it roughly in q19 to q29. A heap profile was out
  of scope for this task.
- **Refusals rose and throughput did not fall.** Typed 422s went from 37
  and 42 to 53 and 87, mainly "query pool full, spill disabled", while qps
  stayed at 8.0. Refused statements finish early, so more refusals and
  unchanged qps are compatible.
- **Allocator retention is unchanged**, as expected: the hold changes
  charging, not freeing. Peak retention 6.6 and 6.0 GB, median 3.5 and
  3.7 GB, peak VmRSS 9.6 and 9.8 GB, all inside the decay A range.

## Deviations

1. **Idle hold 120 s**, as specified. `analyze.py` is kept unmodified, and
   its IDLE-LAST line requires a sample at least 300 s after bench end, so
   in both runs it prints everything up to that point and then stops with
   `IndexError` (recorded in `analysis.txt` with exit 1), as in the decay
   runs. `decay.py` and `hold.py` exited 0 in both runs.
2. **`handoff_overlap` equals `fetch_reserved`** in 105 of 105 live R1
   samples and 100 of 104 R2 samples (103 of 104 and 103 of 103 in decay
   A). Adding it back therefore almost always removes the whole fetch
   reservation from accounted, and row 1 reduces to `allocated -
   sql_reserved - fetch_cache_resident - catalog_cache_resident`. The
   figure is computed exactly as pre-registered; this is reported, not
   corrected.
3. **Row 1's verdict for the pair.** The band is stated per run and the
   runs disagree (R1 between the bands, R2 under 2.0). The pre-registration
   gives no rule for combining them; the pair is reported as "neither",
   with no miss.
4. **Row 4 comparison figures.** The decay A figures named in the
   pre-registration (38 and 45) are total error counts, which include one
   and three fetch-memory refusals sent as HTTP 503. Both the totals and
   the typed-422-only counts are reported.
5. **Row 1 maximum outside the concurrency phase.** The pre-registered
   figure is over all live samples, and R1's maximum is in the serial pass.
   The phase-only maximum is reported beside it.
6. **Dummy reference and scratch prereg** as in the decay experiment: a
   `ref-dummy` directory with `VERSION` = `datafusion-cli 54.1.0` and
   zero-byte `q01.json`..`q43.json`. The bench grades every statement
   `fail` and exits 1 with "D7 check" violations (53 and 52); answers were
   not compared. The validity rule is read from each serial statement's
   `error` field, which is null for all 86.
7. **Page cache not dropped** between runs (no root).
8. **Data re-downloaded.** The decay experiment's store data did not
   survive on this host, so the 20 files were downloaded again and their
   total size checked against the baseline's (identical). Per-file
   checksums were not compared with the baseline, which records sizes only.
9. **Catalog cache resident read 0** in every sample of both runs, as it
   did in every decay A sample.
10. **RustFS commit not confirmed.** The binary reports 1.0.0 and no
    commit; its sha256 matches the one the decay README records. Port 39000
    was free; the RustFS on 9000 belonged to other work and was left
    untouched. The loopback store was stopped after R2.

## Files

| file | contents |
|---|---|
| `<run>/samples.tsv` | every 5 s sample, raw series included |
| `<run>/bench-report.json`, `<run>/bench.log` | bench report and log |
| `<run>/t-server-start`, `<run>/t-bench-end` | the timestamps the scripts read |
| `<run>/server-stamps.txt` | resolved defaults, background-thread stamp, listening and shutdown lines |
| `<run>/server-errors.txt` | every server-logged query error, tenant hash removed |
| `<run>/malloc-conf.txt` | the `_RJEM_MALLOC_CONF` the start script set (unset) |
| `<run>/analysis.txt` | `analyze.py`, `decay.py` and `hold.py` output |
| `hold.py` | rows 1 to 5 and the validity check |
| `phases.py` | row 1's figure by phase, and the overlap equality count |
| `comparison.txt` | `hold.py` and `phases.py` over the decay A1 and A2 samples |
| `analyze.py`, `decay.py`, `sampler.py` | unmodified from the decay branch |
| `prereg-2633.toml`, `bench-arm-a-20-files.diff` | the scratch prereg and the local bench patch, unchanged |

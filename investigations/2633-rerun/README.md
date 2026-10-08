# Issue #2633 rerun: the gap after #2652 and #2653

Measurement only. This repeats the scaled reproduction in
`investigations/2633/` (task 6cd1ef60, branch
`task/6cd1ef60-6bf8-4058-830c-884fcdd4f760/result`) on main at `5378afd2a`.
That commit has two merged fixes: #2652, the jemalloc background purge
thread, and #2653, the read cache storing an exact-size copy. The bands are
the ones pre-registered on #2633 before the run.

## Provenance

Fleet task db9e7a3c ran the measurement on `ravel-clickbench-ubuntu-2`
(`ip-172-31-18-6`, 16 vCPU, 30 GB), the same host as the baseline, and
finished it. During the heap-profile attribution afterwards, its `jeprof`
and `addr2line` processes took the executor's cgroup to its 28.8 GB limit
and the kernel OOM-killed the executor. The task never committed.

The fleet requeued the task to a 15 GB host, where its memory tripwire
stopped it. The files here were copied off the first host's work directory
by hand. Nothing was re-measured.

The heap dump at the peak-gap sample did not symbolize before the kill, so
it attributes to a single unresolved frame (`jp/c-peak.txt`). The dumps at
the end of the concurrency phase and at the largest uncharged-live sample
did symbolize (`jp/c-end.txt`, `jp/c-maxlive.txt`).

## Setup

As in the baseline, with these exceptions:

- the server binary was built at `5378afd2a`;
- the server ran on port 4318 and RustFS on 39000;
- the concurrency phase ran 360.4 s and completed 2,840 queries, against
  the baseline's 2,626;
- after the bench, the server stayed up idle for 499 s, sampled every 5 s,
  then got SIGTERM.

The startup stamp shows
`allocator_background_thread=true source="server"`, and
`ravel_process_allocator_background_thread` read 1 in all 186 sampled rows.

## Results against the pre-registration

| Row | Figure | Baseline | This run | Band | Verdict |
|---|---|---|---|---|---|
| 0 | Serial pass errors | 0 | 0 (43 of 43 answered) | no other error | **valid** |
| 1 | Background thread | off | stamp `true`, gauge 1 in 186 of 186 rows | as stated | **valid** |
| 2 | Retention at the peak-gap sample | 2.54 GB | **5.43 GB** | pass < 1.0, miss >= 1.5 | **MISS** |
| 3 | Idle retention, 300 s or more after the last query | 2.0 GB | **0.084 GB** | pass < 0.3 | **PASS** |
| 4 | Idle `allocated` minus fetch cache | 0.40 GB | **0.023 GB** | pass < 0.15 | **PASS** |
| 5 | Peak gap over the phase | 4.22 GB | **6.30 GB** | reported only | worse |

All 84 concurrency errors are typed HTTP 422 memory-budget refusals, the
same kind as the baseline's 46.

## What the rows say

- **Idle is fixed.** After the last query, retention fell from 1.89 GB at
  +4 s to 0.085 GB by +64 s and stayed there. At the same point
  `allocated` minus the fetch cache fell from 0.40 GB to 0.024 GB. The
  hyper `BytesMut` read-buffer group the baseline found at idle does not
  appear in the end-of-phase profile's top sites. So the 0.4 GB was
  cache-pinned slices (candidate (b)), and #2653 removed it.
- **Under load it is not fixed.** At the peak-gap sample (+84.8 s):
  - resident 9.28 GB, active 3.88 GB, allocated 3.84 GB, accounted
    2.98 GB;
  - `resident - active`, dirty pages not yet returned, is 5.40 GB, 86% of
    the gap.

  Peak VmRSS was 10.44 GB at +129.8 s, against the baseline's 9.01 GB.
  Across the concurrency phase the median retention was 3.60 GB, against
  the baseline's 1.60 GB.
- **The background thread is on, so retention under load is not purging
  that has stopped.** It is what jemalloc's decay keeps: freed pages are
  returned over `dirty_decay_ms`, 10 s by default, and under 10 concurrent
  aggregations the freed volume inside one decay window is several GB.
  Lowering the decay time is the lever. That is a mechanism inferred from
  the figures, not one this run tested.
- **The run-to-run difference is large.** The baseline's peak-gap sample
  came at +357 s and this one at +85 s. One run each cannot separate the
  fixes' effect on retention under load from ordinary variation in which
  heavy statements overlap. Row 2's miss says #2652 alone does not bound
  retention under load. It does not say #2652 made it worse.

## Uncharged live heap

At the largest uncharged-live sample (+234.8 s), `allocated` minus
accounted was 1.87 GB; the handoff overlap was 1.27 GB there, so up to that
much is double-counted. The end-of-phase profile's top sites by group:

- the fetch cache's S3 bodies, `object_store::util::collect_bytes`, 2.79 GB,
  which is charged;
- DataFusion query state, about 2.2 GB in total: `ArrowBytesViewMap`
  0.84 GB, hashbrown tables 0.63 GB, `ByteViewGroupValueBuilder` 0.19 GB,
  count accumulators 0.12 GB and coalesce buffers 0.09 GB.

The query-state group still runs past the SQL reservation, as in the
baseline. That is the third task, which the plan left until this rerun.

## Files

| File | Contents |
|---|---|
| `samples.tsv` | every 5 s sample, with the background-thread gauge and handoff overlap columns |
| `bench-report.json`, `bench.log` | the bench's report and log |
| `server-stamps.txt` | resolved defaults and the background-thread stamp |
| `analyze.py` | the band computation; run it in this directory |
| `t-server-start`, `t-bench-end` | the timestamps `analyze.py` reads |
| `jp/` | `jeprof` text, cumulative and collapsed outputs for the base, peak, max-live and end dumps |
| `jp.sh` | the attribution script whose `addr2line` fan-out OOM-killed the executor |
| `sampler.py`, `attr.py`, `prereg-2633.toml`, `bench-arm-a-20-files.diff` | carried over from the baseline |

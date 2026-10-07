# Issue #2633: allocator gap under concurrent Parquet SQL

Measurement only. No product code was changed and no fix is proposed. This
note reproduces the gap between jemalloc-resident memory and the components
the ADR-1170 budget charges, at a smaller scale than the original OOM. It
splits the gap into allocator retention and uncharged live heap, and names
the allocation sites the heap profile points at.

## Summary

| Question | Answer |
|---|---|
| Band 1: peak gap | **4.219 GB** at +357.4 s into the concurrency phase. **PASS** (band is >= 1.2 GB). |
| Band 2: split at that sample | Retention (resident - allocated) **2.540 GB, 60.2 %**. Uncharged live heap (allocated - accounted) **1.679 GB, 39.8 %**. Verdict: **split**, leaning toward retention. |
| Band 3: sites | Two groups hold all of the live-heap share. DataFusion/arrow query-execution state above the SQL reservation is about 1.27 GB (76 %). Hyper HTTP/1 `BytesMut` buffers from S3 GETs are about 0.51 GB (30 %). The cache is over-charged relative to the profile by -0.10 GB. Coverage of 60 % holds **at group level only**; see the limits below. |
| OOM | **No.** The server survived the run. Peak VmRSS was 9.01 GB on a 32 GB host. There were 46 clean typed HTTP 422 memory-budget refusals, and the server shut down cleanly on SIGTERM afterwards. |
| Validity | Valid. All 43 serial statements answered, none refused, and no other errors occurred. |

## Environment

- Host: ip-172-31-18-6 (fleet executor, amd64 class), Linux 7.0.0-1011-aws
  x86_64, nproc 16, MemTotal 32,132,612 kB, about 209 GB free on disk.
- Commit measured: `2f7728f` (fix(bench): judge an unreachable engine once
  in the D7 check). Toolchain 1.97.1.
- Server binary: `cargo build --release --locked -p ravel-server --features
  sql,heap-profiling`. It was copied to `ravel-server-2f7728f`, sha256
  `86c2b001be2d7bc81cd9879925b1e7dacdbc5fae01c9cadb9a45894d13b1af98`.
- Bench and CLI: `cargo build --release --locked -p ravel-cli -p ravel-bench
  --features ravel-bench/sql-latency --bin ravel-cli --bin
  clickbench_parquet_bench`. These were built with the local patch in
  `bench-arm-a-20-files.diff`; see the deviations.
- Store: RustFS 1.0.0 (static musl release binary, commit d47f54bf) on
  loopback `127.0.0.1:39000`, with data on the local disk.
- Data: ClickBench `hits_compatible/athena_partitioned/hits_0..hits_19.parquet`.
  That is 20 files and 2,711,471,866 bytes, uploaded to
  `s3://clickbench-parquet/hits/`.

  | file | bytes | file | bytes |
  |---|---|---|---|
  | hits_0 | 122,446,530 | hits_10 | 101,513,258 |
  | hits_1 | 174,965,044 | hits_11 | 118,419,888 |
  | hits_2 | 230,595,491 | hits_12 | 149,514,164 |
  | hits_3 | 192,507,052 | hits_13 | 146,132,022 |
  | hits_4 | 140,929,275 | hits_14 | 151,121,699 |
  | hits_5 | 122,286,439 | hits_15 | 103,098,894 |
  | hits_6 | 122,927,935 | hits_16 | 101,067,219 |
  | hits_7 | 123,902,333 | hits_17 | 116,867,853 |
  | hits_8 | 122,735,269 | hits_18 | 133,119,589 |
  | hits_9 | 133,629,314 | hits_19 | 103,692,598 |

## Commands

Store (credentials are throwaway loopback values, shown as placeholders):

```sh
RUSTFS_ACCESS_KEY=<key> RUSTFS_SECRET_KEY=<secret> \
  rustfs server --address 127.0.0.1:39000 --console-address 127.0.0.1:39001 <data-dir>
```

Grant and qualification (once):

```sh
ravel-cli --store s3 --s3-endpoint http://127.0.0.1:39000 --s3-bucket ravel \
  --s3-region us-east-1 --s3-access-key <key> --s3-secret-key <secret> \
  --tenant-hash-unkeyed --parquet-profiles profiles.json \
  tenant parquet-grant add --tenant clickbench \
  --location s3://clickbench-parquet/hits/ --profile rustfs
ravel-cli <same store flags> --tenant-hash-unkeyed store qualify
```

`profiles.json` is a single S3 profile named `rustfs` for the same endpoint.
It uses `force_path_style` and `allow_http`, and reads static credentials
from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

Server (cwd is the dump directory, so interval dumps land there):

```sh
AWS_ACCESS_KEY_ID=<key> AWS_SECRET_ACCESS_KEY=<secret> RAVEL_AUDIT_TOKEN_KEY=<64 hex> \
_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19,lg_prof_interval:31 \
ravel-server-2f7728f --store s3 --s3-endpoint http://127.0.0.1:39000 \
  --s3-bucket ravel --s3-region us-east-1 --s3-access-key <key> --s3-secret-key <secret> \
  --tenant-hash-unkeyed --parquet-profiles profiles.json \
  --tenant-token "<token>=clickbench;ddl" --memory-budget-bytes 8000000000
```

Resolved budget (server-stamps.txt and `/metrics`):

- process budget 8,000,000,000;
- `ravel_memory_budget_bytes` (remainder) 4,400,000,000;
- fetch cache cap 3.2e9 and catalog cache cap 4e8;
- `sql_max_query_bytes` = `sql_tenant_max_bytes` = 3,960,000,000;
- fetch, store GET and SQL partition concurrency 32.

Sampler (`sampler.py`, 5 s):

```sh
python3 sampler.py http://127.0.0.1:4318/metrics <server-pid> samples.tsv 5
```

Bench (serial pass, then 10 connections for 360 s):

```sh
TOKEN=<token> clickbench_parquet_bench --server http://127.0.0.1:4318 --token-env TOKEN \
  --arm a --location s3://clickbench-parquet/hits/ --reference ref-dummy \
  --prereg prereg-2633.toml --server-log server-run.log --out bench-report.json \
  --sql-max-query-bytes 3960000000 --sql-tenant-max-bytes 3960000000 \
  --concurrency-seconds 360
```

Profiles:

```sh
jeprof --text --show_bytes ravel-server-2f7728f <dump>
jeprof --text --show_bytes --cum ravel-server-2f7728f <dump>
jeprof --text --show_bytes --base=<base dump> ravel-server-2f7728f <peak dump>
jeprof --text --lines --show_bytes --cum [--base=<base dump>] ravel-server-2f7728f <peak dump>
jeprof --collapsed --show_bytes [--base=<base dump>] ravel-server-2f7728f <dump> > c.txt
python3 attr.py c.txt <title> 40
```

## Timeline (unix seconds)

| event | time |
|---|---|
| server start, sampler start | 1791351977.1 |
| bench start | 1791352017.904 |
| serial pass | 1791352018.1 to 1791352049.7 |
| base dump `i137` | 1791352049.32, which is 0.39 s before the concurrency phase |
| concurrency phase start (t = 0 below) | 1791352049.707 |
| peak-gap sample | 1791352407.087 (t = +357.4 s) |
| peak dump `i2864` | 1791352407.095, 0.008 s after the sample |
| bench end | 1791352410.203 |
| server SIGTERM, clean shutdown | about 1791352920 (t = +870 s) |

## Shape check (serial pass)

All 43 statements answered and none was refused. The pre-registered rule
is "answered, or refused with a typed memory-budget error". Any other error
would invalidate the run, so the run is valid. Cold and hot latencies in
seconds:

| q | cold | hot | q | cold | hot | q | cold | hot |
|---|---|---|---|---|---|---|---|---|
| q01 | 0.028 | 0.015 | q16 | 0.128 | 0.109 | q31 | 0.170 | 0.158 |
| q02 | 0.039 | 0.028 | q17 | 0.332 | 0.296 | q32 | 0.199 | 0.179 |
| q03 | 0.051 | 0.035 | q18 | 0.276 | 0.278 | q33 | 0.755 | 0.728 |
| q04 | 0.059 | 0.032 | q19 | 0.695 | 0.567 | q34 | 0.652 | 0.674 |
| q05 | 0.113 | 0.098 | q20 | 0.029 | 0.027 | q35 | 0.696 | 0.652 |
| q06 | 0.177 | 0.126 | q21 | 0.382 | 0.177 | q36 | 0.117 | 0.105 |
| q07 | 0.034 | 0.017 | q22 | 0.208 | 0.192 | q37 | 0.125 | 0.124 |
| q08 | 0.027 | 0.026 | q23 | 0.831 | 0.514 | q38 | 0.076 | 0.074 |
| q09 | 0.146 | 0.134 | q24 | 1.167 | 0.474 | q39 | 0.064 | 0.066 |
| q10 | 0.184 | 0.168 | q25 | 0.129 | 0.115 | q40 | 0.809 | 0.802 |
| q11 | 0.103 | 0.076 | q26 | 0.134 | 0.129 | q41 | 0.068 | 0.051 |
| q12 | 0.097 | 0.078 | q27 | 0.159 | 0.150 | q42 | 0.058 | 0.048 |
| q13 | 0.258 | 0.228 | q28 | 0.175 | 0.175 | q43 | 0.044 | 0.039 |
| q14 | 0.297 | 0.303 | q29 | 1.150 | 1.029 | | | |
| q15 | 0.250 | 0.216 | q30 | 0.054 | 0.051 | | | |

Concurrency phase:

- 10 tasks for 360.38 s, with 2,626 completed and 46 errors (error ratio
  0.0172), qps 7.29, and no unreachable engine.
- All 46 errors were HTTP 422 typed "query memory budget exhausted"
  refusals. The reason was the query pool being full with spill disabled,
  against either the 3,960,000,000 tenant limit or the 4,400,000,000
  process limit.
- Errors by statement: q14 2, q16 1, q17 2, q18 1, q19 8, q24 1, q29 2,
  q33 16, q34 5, q35 6, q36 1, q40 1.
- The bench exits 1 with "D7 check: 50 violations". 43 of them are
  "verdict none", because the reference is a dummy and answers are not
  compared. The other 7 are refusals on statements the scratch prereg did
  not list (q14, q16, q17, q18, q24, q36, q40). Neither kind bears on this
  measurement.

## Sampler assertion

The sampler has 189 rows from 1791351977.1 to 1791352917.1, and every
scrape succeeded. Every row from 30 s after start onward carries three
nonzero allocator stats. jemalloc exports exactly three: `allocated`,
`active` and `resident`. There are 72 rows inside the concurrency phase.

Accounted = `ravel_memory_reserved_bytes{component="sql"}` +
`{component="fetch"}` + `ravel_cache_resident_bytes{cache="fetch"}` +
`{cache="catalog"}`. Gap = `resident` - accounted. Both are columns in
`samples.tsv`.

## Band 1: peak gap

The peak gap was **4,218,616,553 B (4.219 GB)** at 1791352407.087. That is
t = +357.4 s into the concurrency phase and 389.2 s after bench start.
**PASS** (>= 1.2 GB). Every one of the 72 concurrency-phase samples had a
gap of at least 1.2 GB, and the median gap was 2.682 GB. The discarded
calibration run peaked at 1.59 GB.

## Band 2: split at the peak-gap sample

| figure | bytes |
|---|---|
| resident | 7,862,902,784 |
| active | 5,366,157,312 |
| allocated | 5,323,019,168 |
| accounted | 3,644,286,231 |
| - SQL reservation | 936,122,749 |
| - fetch reservation | 16,512,543 |
| - fetch cache resident | 2,691,650,939 |
| - catalog cache resident | 0 |
| (handoff overlap, informational) | 16,512,543 |
| VmRSS / RssAnon | 7,493,611,520 / 7,418,830,848 |

| part of the gap | bytes | share of gap |
|---|---|---|
| resident - allocated (retention) | 2,539,883,616 | **60.2 %** |
| of which resident - active | 2,496,745,472 | 59.2 % |
| of which active - allocated | 43,138,144 | 1.0 % |
| allocated - accounted (uncharged live heap) | 1,678,732,937 | **39.8 %** |

Verdict: **split**, leaning toward retention. No numeric threshold for
"retention" versus "split" was pre-registered (see the deviations), so
this label is a judgment. Neither part is below about 40 %.

The split depends on which sample is read. The pre-registered sample (the
largest gap) is where retention dominates. Other samples in the series:

| sample | t | resident | allocated | accounted | gap | retention | uncharged live |
|---|---|---|---|---|---|---|---|
| peak gap (pre-registered) | +357.4 s | 7.863 | 5.323 | 3.644 | 4.219 | 2.540 | 1.679 |
| peak resident and peak uncharged live | +157.4 s | 9.656 | 8.899 | 5.681 | 3.974 | 0.757 | **3.217** |
| peak retention | +162.4 s | 8.891 | 6.291 | 5.611 | 3.281 | 2.601 | 0.680 |
| peak SQL reservation (3.568) | +307.4 s | 9.271 | 8.049 | 6.718 | 2.553 | 1.223 | 1.330 |
| idle, 457 s after the last query | +867.4 s | 5.099 | 3.092 | 2.692 | 2.407 | 2.007 | 0.400 |
| median over the concurrency phase | | | | | 2.682 | 1.602 | 1.068 |

All figures in GB. Two observations follow from the series. Retention did
not decay while idle: 2.0 GB stayed resident above allocated for more than
seven minutes with no queries. At the +157.4 s sample,
`ravel_memory_handoff_overlap_bytes` equalled the whole fetch reservation
(1,246,127,171 B). That means the pre-registered "accounted" sum counts
those bytes twice, and the uncharged live heap there is understated by up
to that amount.

## Band 3: allocation sites

### Method

The flat `jeprof --text` attribution puts 100 % on `prof_backtrace_impl`.
The profiler's own frame is the leaf of every stack, so the literal top-40
files (`jeprof-*-text.txt`) are kept as required but do not name sites.
Sites are therefore attributed from `jeprof --collapsed`, using the method
of `docs/internal/loader-memory-2613.md`. `attr.py` cuts each stack at the
allocator entry and takes the first frame that is not std, core, alloc or
hashbrown plumbing. It also reports the innermost `ravel_*` frame. The
`--cum` and `--lines --cum` files carry the Ravel file:line frames.

Dumps used (all are `lg_prof_interval` interval dumps; see the deviations):

| role | dump | when | sampled total |
|---|---|---|---|
| peak | `jeprof.3034432.2864.i2864.heap` | 0.008 s after the peak-gap sample | 5,702,495,657 B |
| base | `jeprof.3034432.137.i137.heap` | 0.39 s before the concurrency phase | 3,228,780,270 B |
| peak minus base (`--base`) | | | 2,473,715,303 B |
| supplementary: peak uncharged live | `jeprof.3034432.1305.i1305.heap` | 0.006 s before the +157.4 s sample | 7,267,433,128 B |

The sampled total at the peak (5.702 GB) is 7.1 % above `allocated`
(5.323 GB). The decomposition below scales site bytes by 5.323 / 5.702 =
0.9335.

### Top sites at the peak dump (sampled bytes)

| # | site (first non-plumbing frame) | bytes | in diff vs base | group |
|---|---|---|---|---|
| 1 | `object_store::util::collect_bytes` (S3 GET body) | 2,781,321,133 | 81,672,409 | fetch cache |
| 2 | `allocator_api2 Global::alloc_impl`: hashbrown tables in DataFusion `GroupValuesColumn::collect_vectorized_process_context` (620 MB), the repartition-side aggregate (98 MB) and `ArrowBytesViewMap` (21 MB) | 746,043,962 | 746,043,962 | query state |
| 3 | `bytes::BytesMut::reserve_inner`: hyper h1 `Buffered` read buffer of the reqwest connection to the store | 541,470,670 | 27,477,976 | HTTP buffers |
| 4 | DataFusion `PrimitiveGroupValueBuilder<Int64>` | 281,246,891 | 281,246,891 | query state |
| 5 | DataFusion `CountGroupsAccumulator::merge_batch` | 248,946,272 | 248,946,272 | query state |
| 6 | DataFusion `PrimitiveGroupsAccumulator<Int64, Sum>` | 246,134,149 | 246,134,149 | query state |
| 7 | arrow `Buffer: FromIterator<f64>`, from `ravel_sql::avg::ExactIntegerAvgGroupsAccumulator::evaluate` | 152,050,424 | 152,050,424 | query state |
| 8 | DataFusion `PrimitiveGroupValueBuilder<Int32>` | 148,009,753 | 148,009,753 | query state |
| 9 | `[u8]::to_vec` in DataFusion `MinMaxBytesState::set_value` | 97,537,974 | 97,013,366 | query state |
| 10 | arrow_select coalesce `byte_view::BufferSource::next_buffer` (RepartitionExec output coalescer) | 87,450,775 | 87,450,775 | query state |
| 11 | DataFusion `ArrowBytesViewMap::append_value` | 69,797,228 | 69,797,228 | query state |
| 12 | arrow `MutableBuffer::reallocate` in `take_list` | 60,273,457 | 60,273,457 | query state |
| 13 | arrow_select coalesce `InProgressPrimitiveArray<Int64>` | 54,104,434 | 54,104,434 | query state |
| 14 | arrow_select coalesce `InProgressPrimitiveArray<Decimal128>` | 37,941,803 | 37,941,803 | query state |
| 15 | parquet `serialized_reader::decode_page` | 23,981,236 | 23,981,236 | query state |
| 16 | `ravel_sql::avg::ExactIntegerAvgGroupsAccumulator::resize` | 18,727,334 | 18,727,334 | query state |

The full top-40 lists by site, by crate and by innermost Ravel frame are in
`sites-peak.txt`, `sites-diff.txt`, `sites-base.txt` and
`sites-maxlive.txt`. `sites-peak-stacks.txt` shows the caller chains behind
each of the top 14 peak sites.

### Decomposition of the 1.679 GB uncharged live heap

At the peak sample, scaled to `allocated`:

| group | profile bytes (scaled) | charged as | uncharged | share of 1.679 GB |
|---|---|---|---|---|
| query-execution state (everything outside rows 1 and 3) | 2,221.3 MB | SQL 936.1 + fetch 16.5 MB | **1,268.7 MB** | **75.6 %** |
| hyper `BytesMut` read buffers | 505.4 MB | nothing | **505.4 MB** | **30.1 %** |
| S3 GET bodies (`collect_bytes`) | 2,596.2 MB | fetch cache 2,691.7 MB | -95.5 MB | -5.7 % |
| total | 5,323.0 MB | 3,644.3 MB | 1,678.7 MB | 100 % |

Coverage: the top 14 query-state sites in the table above hold 2,272 MB of
the 2,380 MB sampled query-state bytes (95.5 %). Together with the
`BytesMut` site, they therefore contain all of the live-heap share. **Band
3 passes at group level: well over 60 %.**

The profile cannot say which individual query-state bytes are the charged
936 MB and which are the uncharged 1.27 GB. The SQL reservation is one
aggregate number, not a per-allocation tag. So the per-site uncharged share
is not measurable from this data. Only the group total is.

The base dump separates the two groups cleanly. At the base dump, no query
was running and the SQL and fetch reservations were 0, while
allocated - accounted was 0.695 GB. The base profile is 83.6 %
`collect_bytes` (2.700 GB) and 15.9 % `BytesMut` (0.514 GB). That makes the
`BytesMut` group a standing uncharged amount of about 0.5 GB that does not
depend on query load. The diff from base to peak (2.474 GB) is 95.6 %
query-execution state.

At the supplementary max-live dump (+157.4 s, 3.217 GB uncharged), the
query-state group is larger and string-keyed:

- `allocator_api2` hash tables 1.217 GB;
- `ArrowBytesViewMap::append_value` 689 MB;
- `ByteViewGroupValueBuilder<StringView>` 214 + 81 MB;
- `CountGroupsAccumulator::merge_batch` 363 MB;
- parquet `decode_page` 247 MB, under
  `ravel_parquet::boundary::BoundaryStream::poll_next` (309 MB innermost
  Ravel frame).

The sampled total there (7.267 GB) is 1.63 GB *below* `allocated`
(8.899 GB). That discrepancy is not explained by this data.

### Candidates, with Ravel file:line where frames resolve

1. **Query-execution state above the SQL reservation (about 1.27 GB, 76 %
   of the live-heap share at the peak).** DataFusion hash-aggregate group
   tables and accumulators, repartition coalesce buffers and parquet page
   decode. These stacks run on DataFusion-spawned tasks, so most of them
   carry no Ravel frame between the allocation and the runtime. The Ravel
   frames that do resolve are:
   - `services/ravel-server/src/sql.rs:222` (`handle`) and `:257` (`run`),
     the SQL entry above the executor.
   - `crates/ravel-parquet/src/boundary.rs:257`
     (`BoundaryStream::poll_next`): parquet decode under the Ravel stream,
     48 MB at the peak and 309 MB at the max-live dump.
   - `crates/ravel-sql/src/avg.rs:796` (`ExactIntegerAvgGroupsAccumulator::evaluate`):
     the emitted `Float64Array`, 152 MB at the peak. This is output built at
     emit time. Its `size()` (avg.rs:856) reports only the `sums`/`counts`
     capacity, so the emitted array is outside what the accumulator
     reports.
   - `crates/ravel-sql/src/avg.rs:724-725` (`resize`), 18.7 MB at the peak
     and 478 MB at dump `i2890` (end of the run). The accounting there uses
     capacity (avg.rs:856), so this is a size, not a candidate for
     mis-charging by itself.
2. **Hyper HTTP/1 read buffers on the S3 GET path (about 0.51 GB, 30 %,
   present at idle).** The stacks are entirely in hyper's dispatcher task,
   with no Ravel frame. The GET path that drives them is
   `crates/ravel-object-store/src/s3.rs:2318` (`get_one`,
   `result.bytes().await`), under `crates/ravel-object-store/src/s3/connector.rs:110`
   (`scope`). This profile cannot tell apart two explanations:
   - (a) buffers retained by pooled store connections;
   - (b) `Bytes` held in the fetch cache that share a larger hyper buffer
     allocation. `crates/ravel-cache/src/cache.rs:372` charges an entry as
     `value.len()`, not as the size of the allocation it pins.

   One point leans toward (b). At the end, only one established connection
   to the store remained, yet allocated - cache was still 0.400 GB. That
   idle point has no dump, so it is not conclusive.
3. **S3 GET bodies (`collect_bytes`) are charged.** At 2.78 GB sampled,
   they match the 2.69 GB fetch cache resident. Corpus bytes are below the
   3.2 GB cap, so there was no eviction, and the site's bytes were identical
   (2,781,321,133) at the max-live, peak and final dumps. The path is
   `crates/ravel-parquet/src/reader.rs:361` (`fetch_once`) / `:250`
   (`read_reserved`) / `:699` (`get_byte_ranges`), then
   `crates/ravel-cache/src/cache.rs:255`/`:287` (`get_or_fetch`), then
   `crates/ravel-cache/src/single_flight.rs:120`/`:174`, then
   `crates/ravel-object-store/src/s3.rs:2318`. These bytes are not part of
   the uncharged share.
4. **Retention (60 % of the gap at the peak sample) has no allocation
   site.** It is resident - active: dirty pages jemalloc had not returned.
   It stayed at 2.0 GB after 457 s idle. That is consistent with decay
   running only on allocator activity (no background purge thread). This
   is an observation, not a tested mechanism.

## OOM

The server was **not** OOM-killed:

- peak VmRSS was 9.01 GB at t = +282.4 s;
- peak jemalloc resident was 9.656 GB at t = +157.4 s;
- the host has 32 GB.

Under pressure it refused 46 statements cleanly with typed HTTP 422
memory-budget errors. It stayed up for 7.5 min after the run, and exited on
SIGTERM with "shutdown complete". The same pid was alive from start to
SIGTERM; the kernel log itself was not readable without root.

## Deviations from the pre-registration

1. **Concurrency phase 360 s, not 600 s.** A tool call is limited to 540 s,
   and the bench runs the serial pass and the concurrency phase in one
   process, so the phase could not be split across calls. This is the
   pre-registered fallback.
2. **Bench patched locally to accept 20 files.** Arm A expects 100 mounted
   files. `report.rs` was changed from `Arm::A => 100` to `Arm::A => 20`
   (`bench-arm-a-20-files.diff`). The binary was built with the patch and
   the source was reverted. Nothing is committed outside this directory.
3. **Dummy reference.** The bench requires a reference directory. A dummy
   one (`VERSION datafusion-cli 54.1.0`, empty `qNN.json`) was used, so
   answers were not compared. The 50 D7 violations and the bench's exit 1
   come from this and from the scratch prereg; neither is a finding.
4. **Scratch prereg file.** `prereg-2633.toml` holds values chosen only to
   get the bench to run: memory cap 3.96e9, Arm B 1000 s, registered
   failures q19/q29/q33/q34/q35, RLOG ceiling 101.7 s and qps floor 0.4.
   None of them are claims.
5. **Store port 39000, not the default.** Port 9000 was held by a foreign
   RustFS (left untouched) and 9100 by node_exporter.
6. **Extra setup flags that the plain command line lacked.** These were
   needed against a fresh bucket:
   - `--tenant-hash-unkeyed` on both the CLI and the server, since a fresh
     bucket has no tenant-hash key;
   - a one-off `ravel-cli store qualify`, since the server refuses an
     unqualified store;
   - `RAVEL_AUDIT_TOKEN_KEY`, since redacted audit text needs a key.
7. **Interval dumps, not an on-demand dump.** The server exposes no
   prof.dump endpoint, and gdb is not installed. The peak dump is the
   `lg_prof_interval` dump 0.008 s after the peak-gap sample. The base dump
   is the last interval dump before the concurrency phase (0.39 s before).
8. **A calibration run preceded the measured run.** It used a separate
   server process, was discarded and peaked at a gap of 1.59 GB. The
   measured run used a fresh server.
9. **The page cache was not dropped** (no root). The store sits on local
   disk, and all 20 files had just been uploaded.
10. **Flat jeprof attribution is degenerate.** Every stack's leaf is
    `prof_backtrace_impl`, so the literal `--text` top 40 is kept but sites
    come from `--collapsed` plus `attr.py` (first non-plumbing frame).
11. **No pre-registered threshold** separated "retention" from "split".
    The verdict "split, leaning toward retention" is a judgment on 60/40.
12. **The pre-registered accounted sum double counts handoff overlap.**
    `ravel_memory_handoff_overlap_bytes` is the part of the fetch
    reservation also charged to the cache. It was 16.5 MB at the peak
    sample (negligible) but 1.246 GB at the +157.4 s sample. The figures
    above use the pre-registered sum unchanged.
13. **A supplementary dump was attributed** (`i1305`, the largest
    uncharged-live sample), because the pre-registered peak-gap sample is
    the one where retention dominates. It does not replace the
    pre-registered peak dump.

## Files

| file | contents |
|---|---|
| `samples.tsv` | every 5 s sample, plus every raw series of the scraped families |
| `bench-report.json` | bench report of the measured run |
| `server-stamps.txt` | resolved performance defaults, CPU gate and listening lines |
| `jeprof-peak-text.txt`, `jeprof-peak-cum.txt` | `jeprof --text --show_bytes` (and `--cum`) top 40, peak dump |
| `jeprof-diff-text.txt`, `jeprof-diff-cum.txt` | the same with `--base` = the base dump |
| `jeprof-base-text.txt`, `jeprof-maxlive-text.txt` | top 40 of the base and supplementary dumps |
| `jeprof-peak-lines-cum.txt`, `jeprof-diff-lines-cum.txt` | `--lines --cum`, with file:line frames |
| `sites-*.txt`, `sites-peak-stacks.txt` | `attr.py` attribution and caller chains |
| `jeprof.3034432.{137,1305,2864}.*.heap` | the three raw dumps used (under 100 KB each; needs the binary built from 2f7728f with the same features) |
| `sampler.py`, `attr.py` | the sampler and attribution scripts |
| `prereg-2633.toml`, `bench-arm-a-20-files.diff` | the scratch prereg and the local bench patch |

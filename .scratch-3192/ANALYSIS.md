# #3192 cache access traces: analysis

Builds: P = 94ca085a83827da04a3ce89a370a913ffaa97b11, Q = 36702ffc9feec4b57802d4c0d45c6477213fce06
(Q = P + 4eb19ac8b "hand logs scan partitions units from one shared queue" + two test commits).
Each build was made with `trace.patch` applied:
`cargo build --release -p ravel-bench --features sql-latency --bin sql_latency_bench`.

Executor: amd64, 16 vCPU, CARGO_BUILD_JOBS=4.

Run (two fresh processes per build):

    RAVEL_CACHE_TRACE=.scratch-3192/trace-<B>-<n>.tsv .gate-logs/bench-<B> --generate \
      --records 500000 --records-per-object 20000 --runs 4 --cache-bytes 1044922 \
      --corpus .scratch-3192/corpus.json > .scratch-3192/report-<B>-<n>.json

The store backend is memory.

The generated lane has 25 RLOG objects totalling 2,089,843 bytes. The query is
`SELECT count(*) FROM logs WHERE duration_ms >= 1000`. There are 8 partitions, and
fetch and store-get concurrency are both 8. The figures reproduce the readout:

| process | run 1 (hits/misses/bytes) | runs 2-4 (hits/misses/bytes) |
|---|---|---|
| P-1 | 0/51/0 | 29/22/256956 |
| P-2 | 0/51/0 | 29/22/256186 |
| Q-1 | 0/51/0 | 26/25/7150 |
| Q-2 | 0/51/0 | 26/25/7150 |

Trace format: `seq cache op key intent outcome extra`.
- `C<n>` is a `ravel_cache::Cache` instance.
- `cat:*` is a ravel-catalog in-process cache.
- `acct` marks an accounting site in log_fetcher.rs.
- `RUN` and `RUNEND` bracket one bench run. RUNEND carries the run's per_run_accounting figures.

Read-cache keys print as `<first 8 bytes of content_hash hex>:<offset>:<len>`. The
content hash includes the tenant, so a key differs between processes. Object sizes
are distinct and match across all four processes, so sizes identify objects.
`sN` is the scan's segment index from the report's `scan_timing.segments`. It is
matched to an object by lining up `open_start_ns` order with trace lookup order
over all 16 runs (`analysis/vote.py`; `analysis/seg2sz.json`). s0 and s8-s24 are
unambiguous. s1-s7 open nearly concurrently, so their mapping is a majority vote.

## Validity gate

`analysis/valid.py` reads each run's events between RUN and RUNEND and computes:
- hits = `C2 get hit` + `cat:* get hit`
- misses = `C2 get miss` − `acct none front_section_peek_miss_uncounted` + `cat:* get miss`
- bytes = sum of `bytes=` over those hits

It compares the result with the report's `per_run_accounting` and with RUNEND.

| process | run | trace h/m/bytes | report h/m/bytes | C2 h/m/bytes | catalog h/m/bytes | match |
|---|---|---|---|---|---|---|
| P-1 | 1 | 0/51/0 | 0/51/0 | 0/25/0 | 0/26/0 | exact |
| P-1 | 2 | 29/22/256956 | 29/22/256956 | 3/22/249806 | 26/0/7150 | exact |
| P-1 | 3 | 29/22/256956 | 29/22/256956 | 3/22/249806 | 26/0/7150 | exact |
| P-1 | 4 | 29/22/256956 | 29/22/256956 | 3/22/249806 | 26/0/7150 | exact |
| P-2 | 1 | 0/51/0 | 0/51/0 | 0/25/0 | 0/26/0 | exact |
| P-2 | 2 | 29/22/256186 | 29/22/256186 | 3/22/249036 | 26/0/7150 | exact |
| P-2 | 3 | 29/22/256186 | 29/22/256186 | 3/22/249036 | 26/0/7150 | exact |
| P-2 | 4 | 29/22/256186 | 29/22/256186 | 3/22/249036 | 26/0/7150 | exact |
| Q-1 | 1 | 0/51/0 | 0/51/0 | 0/25/0 | 0/26/0 | exact |
| Q-1 | 2 | 26/25/7150 | 26/25/7150 | 0/25/0 | 26/0/7150 | exact |
| Q-1 | 3 | 26/25/7150 | 26/25/7150 | 0/25/0 | 26/0/7150 | exact |
| Q-1 | 4 | 26/25/7150 | 26/25/7150 | 0/25/0 | 26/0/7150 | exact |
| Q-2 | 1 | 0/51/0 | 0/51/0 | 0/25/0 | 0/26/0 | exact |
| Q-2 | 2 | 26/25/7150 | 26/25/7150 | 0/25/0 | 26/0/7150 | exact |
| Q-2 | 3 | 26/25/7150 | 26/25/7150 | 0/25/0 | 26/0/7150 | exact |
| Q-2 | 4 | 26/25/7150 | 26/25/7150 | 0/25/0 | 26/0/7150 | exact |

All 16 process-runs match exactly. The `acct` lines also agree with the C2 split:
`acct hit whole` equals the C2 hit count and `acct miss whole_fetched` equals the C2
miss count in every run.

The `front_section_peek`, `extent` and `block_peek` sites never fire. Every
read-cache access in this lane is a whole-object lookup.

The C2 counts alone do NOT equal per_run_accounting, because `cache_hits`,
`cache_misses` and `cache_bytes` pool C2 with the catalog caches. Only C2 plus the
catalog matches. Raw output is in `analysis/validity.txt`.

## 1. Cache instances, and what the 26 "footer hits" are

Instances created in each process (identical in P and Q):

- `C1`: max_bytes=536870912, max_entries=512, max_entry_bytes=268435456. Created before the corpus loop and never accessed.
- `C2`: max_bytes=1044922, max_entries=255, max_entry_bytes=1044922. This is the bench's `--cache-bytes` read cache (`build_read_cache`), the `ReadCache::Ram` the logs fetcher uses. It is the only `Cache` with any get, admit or evict events.
- `C3`: max_bytes=536870912, max_entries=512, max_entry_bytes=268435456. Created after C2 and never accessed.
- No `tiered_get_or_fetch` events, so no TieredCache is in the path.
- Catalog caches:
  - `cat:RecordCache`, keyed by commit record `.cmt`, 25 keys.
  - `cat:HeadCache`, key `Logs`.
  - `CompactionRecordCache` and `DecodedCache<V>` are never accessed.

The 26 warm-run hits common to P and Q are not footer hits, and none of them come
from the read cache:

- 25 are `cat:RecordCache get hit` on the 25 commit records (`...000000000000000000001.cmt` to `...025.cmt`), each with bytes=286, totalling 7,150.
- 1 is `cat:HeadCache get hit` on `Logs` with bytes=0.

This also accounts for exactly 7,150 cache_bytes in Q.

The matching 26 misses in run 1 are the same 25 records plus the head. A further 25
RecordCache misses and 1 HeadCache miss occur before RUN 1, during the bench's own
catalog read, and fall outside per_run_accounting.

In P, the extra 3 hits (29 − 26) and their 249,806 or 249,036 bytes are C2 whole-object hits.

## 2. P: the run-2 hit keys and how they reached main

Each P process has exactly three C2 hits in runs 2, 3 and 4, always the same three
keys. Full histories are in `analysis/histories-P.txt`.

P-1 (hits s1, s9 and s17, sizes 83303, 82406 and 84097; all dealt to partition 1):

    s1/e7178eb2  run1: 208 get:miss; 211 admit:small; 214 fetch
                 run2: 262 fifo_hit(freq_after=1,small); 263 get:hit; 347 evict_small:to_main(freq=1)
    s9/85f2de52  run1: 216 get:miss; 219 admit:small; 222 fetch
                 run2: 301 fifo_hit(freq_after=1,small); 302 get:hit; 348 evict_small:to_main(freq=1)
    s17/98abf5c9 run1: 224 get:miss; 227 admit:small; 230 fetch
                 run2: 330 fifo_hit(freq_after=1,small); 331 get:hit; 349 evict_small:to_main(freq=1)

P-2 (hits s5, s13 and s21, sizes 82023, 84247 and 82766; all dealt to partition 5):

    s5/506a46d9  run1: 208 get:miss; 211 admit:small; 214 fetch
                 run2: 270 fifo_hit(freq_after=1,small); 271 get:hit; 353 evict_small:to_main(freq=1)
    s13/12d4988f run1: 216 get:miss; 219 admit:small; 222 fetch
                 run2: 286 fifo_hit(freq_after=1,small); 287 get:hit; 355 evict_small:to_main(freq=1)
    s21/da3d7361 run1: 224 get:miss; 227 admit:small; 230 fetch
                 run2: 324 fifo_hit(freq_after=1,small); 325 get:hit; 356 evict_small:to_main(freq=1)

How they reached main:

1. P deals segment i to partition i % 8. In run 1 one partition starts late; the
   other seven start between 0.6 and 0.77 ms. P-1's late partition is partition 1,
   with first open_start at 4,335 µs. P-2's is partition 5, at 6,138 µs. Its three
   segments are therefore looked up last and serially, as lookups 23, 24 and 25 of
   run 1 (trace seq 208, 216 and 224). They are the last three objects admitted to
   small, so they are the youngest residents when run 1 ends.
2. With main empty, the small queue fills to about 13 entries (about 1.09 MB) and
   evicts from its head. At the end of run 1 it holds the last 12 admitted objects.
   None of them was ever hit, so every earlier eviction is `to_ghost` (freq 0), and
   the 1-entry ghost drops each one at the next eviction.
3. In run 2 every partition starts on time, so the late partition's three segments
   come early: s1, s9 and s17 at lookup positions 2, 9 and 14 in P-1. Each miss
   before them admits a new object and evicts the oldest small resident. In P-1
   that is the nine older run-1 residents (s15 s22 s16 s19 s18 s20 s21 s24 s23),
   evicted at seq 279-343.
   The three youngest are still resident when they are looked up, so they hit
   with an interactive `fifo_hit` that sets freq to 1.
4. When the queue reaches them (seq 347-349 in P-1, 353-356 in P-2),
   `fifo_evict_small` finds freq=1 and moves them to main. The same eviction pass
   sends the next head, freq 0, to ghost. After that the cache shows
   `small=10 main=3/249806B` in P-1 and `main=3/249036B` in P-2.
5. Main is never evicted (`fifo_evict_main` count 0 in every run): small stays far
   above its 10% target, so every later eviction is taken from small. The three
   objects are hit in main in runs 3 and 4, giving 3 hits per warm run.

`main_via_ghost` never fires in P. Main is reached only through a small-queue hit
followed by aging out of small.

## 3. Q: per-object events, why none reaches main, and touches per run

Full histories for every object in runs 1 and 2 are in `analysis/histories-Q.txt`.
Every Q object follows the same shape, shown here for Q-1 s0 and s12:

    s0/e7137e59  run1: 57 get:miss; 80 admit:small; 84 fetch; 158 evict_small:to_ghost; 164 fifo_ghost_drop:dropped
                 run2: 260 get:miss; 299 admit:small; 305 fetch; 398 evict_small:to_ghost; 407 fifo_ghost_drop:dropped
    s12/49f9bbcc run1: 123 get:miss; 149 admit:small; 154 fetch
                 run2: 278 evict_small:to_ghost; 284 fifo_ghost_drop:dropped; 332 get:miss; 364 admit:small; 368 fetch; 456 evict_small:to_ghost

In all 8 Q runs there are no C2 hits, no `to_main`, no `main_via_ghost` and no main
evictions.

Why none reaches main:

- Q publishes units from one shared queue in a fixed order. Each segment is one
  unit (`segments_opened` 25, `reopens` 0), and an idle partition takes the next
  unit. Lookup order is therefore segment order s0..s24 in every run, with only
  jitter among the first 8 concurrent opens (section 4).
- The working set is 2,089,843 bytes against a 1,044,922-byte cache, about 12
  objects. In a cyclic scan of 25 objects through a FIFO that holds 12, each miss
  in run 2 evicts the oldest resident, and the oldest resident is always the next
  run-1 object the scan will need.
- As a result, every object is evicted, to ghost with freq 0, before its next
  lookup. s12 is evicted at seq 278 and looked up at 332. s24 is evicted at 352
  and looked up at 434.
- Because no object is ever hit while resident in small, freq never leaves 0, and
  `evict_small` always takes the `to_ghost` branch.
- Ghost capacity is min(1044922/1044922, 255) = 1, so a ghost entry survives only
  until the next eviction (`fifo_ghost_drop` follows each `to_ghost` within a few
  seq). `main_via_ghost` would need an object looked up right after its own
  eviction. In this scan the evicted object's next use is about 12 lookups later.

The same late-start effect happens in Q run 1: Q-1 partition 4 and Q-2 partition 2
start late, and both take zero units, because the other partitions drain the
shared queue first. No segment is pinned to the late partition, so no group of
objects is pushed to the end of run 1 and then to the front of run 2. That push is
what gave P its three hits.

Touches per run (Q and P alike):

- Each object is looked up exactly once per run: 25 C2 `get` events over 25
  distinct keys, at most 1 per key, and at most 1 `acct` line per key.
- No single-flight follower or late serve occurs: every `get_or_fetch` is
  `fetched role=leader from_cache=false`, and no `whole_lateserve` acct line appears.
- No carried in-memory copy causes a second use. Each segment is a single unit
  (25 opens, 0 reopens), so no second run of the same segment could consume the
  carried object.
- Each miss is followed by exactly one store GET: 25 per run in Q and 22 in P
  warm runs, matching `object_store_get_requests`.

## 4. Order of first lookup of the 25 objects per run

The order uses segment labels; `*` marks a C2 hit. Raw output is in
`analysis/lookup-order.txt`.

    P-1 run 1: s0 s6 s4 s3 s7 s2 s5 s14 s8 s13 s11 s15 s12 s10 s22 s16 s19 s18 s20 s21 s23 s24 s1 s9 s17
    P-1 run 2: s0 s1* s3 s5 s6 s4 s2 s7 s9* s8 s11 s14 s13 s17* s10 s12 s16 s15 s19 s22 s21 s18 s24 s20 s23
    P-1 run 3: s0 s1* s2 s3 s6 s5 s4 s7 s9* s14 s10 s13 s12 s8 s17* s11 s15 s22 s18 s21 s20 s16 s19 s23 s24
    P-1 run 4: s0 s1* s2 s4 s5 s7 s6 s3 s9* s12 s13 s14 s11 s17* s10 s8 s15 s20 s21 s22 s19 s16 s18 s23 s24
    P-2 run 1: s0 s3 s4 s2 s6 s7 s1 s15 s11 s10 s9 s12 s14 s8 s23 s19 s18 s17 s20 s16 s22 s24 s5 s13 s21
    P-2 run 2: s0 s1 s2 s3 s4 s5* s6 s7 s13* s11 s21* s10 s12 s14 s15 s9 s8 s19 s18 s22 s20 s23 s17 s16 s24
    P-2 run 3: s0 s1 s2 s3 s4 s5* s6 s7 s13* s10 s12 s21* s11 s14 s15 s9 s8 s18 s20 s19 s22 s23 s17 s16 s24
    P-2 run 4: s0 s1 s2 s3 s4 s5* s6 s7 s13* s21* s11 s12 s10 s15 s14 s9 s8 s19 s20 s23 s18 s22 s17 s16 s24
    Q-1 run 1: s0 s3 s2 s5 s1 s4 s6 s7 s8 s9 s10 s11 s12 s13 s14 s15 s16 s17 s18 s19 s20 s21 s22 s23 s24
    Q-1 run 2: s0 s1 s3 s5 s6 s4 s2 s7 s8 s9 s10 s11 s12 s13 s14 s15 s16 s17 s18 s19 s20 s21 s22 s23 s24
    Q-1 run 3: s0 s1 s2 s3 s4 s5 s6 s7 s9 s8 s10 s11 s12 s13 s14 s15 s17 s16 s18 s19 s20 s21 s22 s23 s24
    Q-1 run 4: s0 s2 s1 s3 s5 s6 s4 s7 s8 s9 s10 s11 s12 s13 s14 s15 s16 s17 s18 s19 s20 s21 s22 s23 s24
    Q-2 run 1: s0 s2 s6 s5 s1 s3 s4 s7 s8 s10 s12 s9 s13 s11 s14 s15 s16 s17 s18 s19 s20 s21 s22 s23 s24
    Q-2 run 2: s0 s1 s2 s3 s4 s5 s6 s7 s8 s9 s10 s11 s12 s13 s14 s15 s16 s17 s18 s19 s20 s21 s22 s23 s24
    Q-2 run 3: s0 s1 s2 s5 s6 s4 s7 s3 s8 s9 s10 s11 s12 s13 s14 s15 s16 s17 s18 s19 s20 s21 s22 s23 s24
    Q-2 run 4: s0 s2 s4 s3 s1 s5 s6 s7 s8 s9 s10 s11 s12 s13 s14 s15 s17 s16 s18 s19 s20 s21 s22 s23 s24

Segment-to-object map, by size:

| segment | size | segment | size | segment | size |
|---|---|---|---|---|---|
| s0 | 84554 | s9 | 82406 | s18 | 83728 |
| s1 | 83303 | s10 | 84365 | s19 | 82463 |
| s2 | 84581 | s11 | 84703 | s20 | 82345 |
| s3 | 82527 | s12 | 85742 | s21 | 82766 |
| s4 | 81870 | s13 | 84247 | s22 | 83844 |
| s5 | 82023 | s14 | 84167 | s23 | 83489 |
| s6 | 84364 | s15 | 82409 | s24 | 82270 |
| s7 | 85060 | s16 | 83118 | | |
| s8 | 85402 | s17 | 84097 | | |

How the orders differ:

- P runs 2-4 follow the deal: partition p reads p, p+8, p+16 (and 24 for p=0),
  interleaved by timing.
- In P's run 1, the late partition's three segments come last: s1/s9/s17 in P-1
  and s5/s13/s21 in P-2.
- Q follows segment order in every run, apart from jitter among s0-s7.

## Out-of-scope observations

- `cache_hits`, `cache_misses` and `cache_bytes` in per_run_accounting pool the
  ravel-catalog caches (RecordCache and HeadCache) with the read cache. A reading
  of "26 footer hits" as read-cache hits, or as footer hits at all, is not
  supported by the trace: they are commit-record and head hits.
- In run 1, one of the 8 partition streams starts about 3.6-5.4 ms after the other
  seven. P-1 partition 1 starts at 4,335 µs, P-2 partition 5 at 6,138 µs, and the
  others at about 0.6-0.77 ms. Under P's deal this decides which objects end up
  cached, so P's 3 warm hits are a timing artifact of cold-run scheduling, not a
  property of the access pattern. The trace does not show why that stream starts late.
- C1 and C3 (512 MiB each) are constructed and never touched in this lane.

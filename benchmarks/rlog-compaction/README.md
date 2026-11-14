# RLOG compaction profile

`rlog_compaction_profile` (crates/ravel-bench/src/bin/rlog_compaction_profile.rs,
issues #3819 and #3877, ADR-3853 task T7) measures where the CPU of one RLOG
L0 -> L1 compaction goes. It changes no library behaviour.

## What it does

1. Generates one sealed logs bucket (one tenant, one shard, one ingest hour)
   of synthetic L0 objects on an in-memory `MemoryStore`, together with
   their commit records. Generation runs on every core and is not measured.
2. Reads the shape back from the written objects (see "Bucket shape").
3. Runs `ravel_maintain::compact_bucket` over the bucket once, with the
   configuration `ravel-cli maintain compact-bucket` resolves when no knob is
   set and one merge runs at a time. The memory target is sized from the
   host's memory.
4. Reports the wall time and process CPU of that one call, records/s per
   core, L0 and L1 bytes, the L1 part count and the merge read mode. It also
   reports a timeline built from store-operation timestamps and the
   compactor's tracing events, the request ledger per phase, and the
   per-thread CPU.
5. With `--page-choice-ab`, it then re-encodes every published L1 part from
   its decoded records, once with `PageChoice::Exhaustive` and once with
   `PageChoice::Estimated`, using the compactor's writer settings.
   `CompactorConfig` does not expose the page choice, so this is the only way
   to compare the two. The Exhaustive re-encode should reproduce the
   published bytes; `parts_byte_identical_to_published` checks that.
6. With the `profiling` feature and `RAVEL_BENCH_PROFILE_SVG` set, it samples
   the compaction call at 997 Hz. It writes a flamegraph, the top 15
   exclusive leaves, the top inclusive frames and the samples split by phase
   using stack markers. It also splits the samples by merge level, using
   the `rlog::level1_worker` and `rlog::level2_worker` frames.
7. With `--scratch-dir`, it lets the call take the ADR-3853 two-level merge
   (issue #4141). The worker, run block and range knobs set the same
   `CompactorConfig` fields as `ravel-cli maintain`'s flags of the same
   names. The report splits the call's wall time and process CPU into
   level 1 and level 2 at the compactor's events, and gives each level's
   CPU per input record.

One JSON document goes to stdout. A human-readable table goes to stderr.

## Bucket shape

At the defaults (`--inputs 230 --streams 2477`, seed 3819) each L0 object
has:

- 2,200 to 2,480 streams (capped at the 2,477 in the bucket), drawn from
  2,477 streams in the bucket. Each stream has the resource attributes
  `app`, `host` and `pod`.
- A log-normal record count with median 10,900 and sigma 0.35, clamped to
  5,000..29,000. Records are sorted by (stream, timestamp) and span 700 ms;
  objects follow each other in time.
- 58 named fields per record: `body` plus 57 attributes. Five attributes are
  `i64`: `bytes_sent`, `duration_ms`, `replicas`, `retries` and `status`.
  The other 52 attributes are strings, and so is `body`. Each attribute is
  null in 40% to 70% of records.
- Blocks of 8,192 records (`RlogConfig::default()`). Every full block spans
  at least 1,000 stream refs. Only the last block of an object can be
  shorter than 8,192 records, and it spans fewer streams.

The test `synthetic_bucket_matches_the_hour_498274_shape` checks the stream
count, the field set and types, and the block spans. It builds 8 full-size
objects one at a time, not the whole 230-input bucket.

## Commands

The plain build is used for timing:

```sh
cargo build --release -p ravel-bench --bin rlog_compaction_profile
B=target/release/rlog_compaction_profile    # or $CARGO_TARGET_DIR/release/...

$B                          # defaults: 230 inputs, compactor default zstd level (3)
$B --zstd-level 3           # L1 parts at zstd level 3
$B --zstd-level 1
$B --page-choice-ab         # plus the Exhaustive / Estimated re-encode
$B --inputs 60              # smaller bucket; the realism band is not applied
```

To take the two-level merge, give it a scratch directory and a resident
ceiling below the bucket's resident need. With the ceiling at 4 GiB, the
460-input bucket plans cursor mode and routes two-level (profile-4141.md):

```sh
S=.gate-logs/scratch        # local disk, never a tmpfs HOME
$B --inputs 460 --scratch-dir $S --merge-resident-ceiling-bytes 4294967296
$B ... --run-block-records 8192 --level1-workers 1 --level2-workers 2
$B ... --level2-workers 1   # one level-2 worker, pipelined encode
$B ... --range-records 1000000000000   # one range
```

| Flag | `CompactorConfig` field | Needs `--scratch-dir` |
|---|---|---|
| `--scratch-dir` | `scratch_dir` | |
| `--merge-resident-ceiling-bytes` | `merge_resident_ceiling_bytes` | no |
| `--level1-workers` | `level1_workers` | yes |
| `--level2-workers` | `level2_workers` | yes |
| `--run-block-records` | `rlog_run_block_target_records` | yes |
| `--range-records` | `rlog_range_records` | no |

The bin refuses (exit 2) a flag that needs a scratch directory without one,
and a zero worker count, run block or range quantum, the same way the CLI
does. Pinning one worker count leaves the other derived, and the joint
derivation can move it: `--level2-workers 1` alone derives W1 = 2 on the
460-input bucket. To change W2 alone, pin both.

The profiled build needs frame pointers. Build it from `crates/ravel-bench`
with `RUSTFLAGS` unset (see `crates/ravel-bench/PROFILING.md`):

```sh
cd crates/ravel-bench
cargo profile-build --bin rlog_compaction_profile
RAVEL_BENCH_PROFILE_SVG=/tmp/rlog-compaction.svg \
  "$CARGO_TARGET_DIR"/release/rlog_compaction_profile
```

The sampler drops samples whose leaf is in libc (memcpy, malloc), so the
sample count is lower than the process CPU. To sample libc too, set
`RLOG_COMPACTION_PROFILE_SAMPLE_LIBC=1`, at the signal-safety risk
`ravel_bench::profiling` describes. A profiled run is slower than an
unprofiled one. Read it for attribution, not timing.

To compare against jemalloc, the allocator ravel-cli installs:

```sh
cargo build --release -p ravel-bench --features compaction-jemalloc --bin rlog_compaction_profile
```

Other knobs: `--streams`, `--min-streams-per-object`,
`--max-streams-per-object`, `--median-records`, `--min-records`,
`--max-records`, `--records-sigma`, `--seed`.

## Output (stdout JSON)

- `host`: arch, kernel, `available_parallelism`, `mem_total_kib`.
- `generate_wall_s`: time spent building the bucket (not measured).
- `shape`: values read back from the objects: `objects`,
  `distinct_streams_in_bucket`, and min/median/max/mean/sum spreads for
  `records_per_object`, `streams_per_object`, `blocks_per_object`,
  `row_groups_per_object`, `bytes_per_object` and `ts_span_ms_per_object`.
  Also `records_per_stream_per_object_x100`, `objects_consecutive_in_time`,
  and `first_object_fields`, with each field's type, null share and
  distinct count.
- `run`:
  - `config`: the resolved compactor settings, including
    `rlog_zstd_level` and `rlog_l1_part_memory_target_bytes`.
  - Counts and sizes: `records_in`, `records_out`, `l0_objects`,
    `l0_bytes`, `l1_bytes`, `l1_l0_ratio`, `l1_parts`, `l1_part_bytes`,
    `l1_parts_reported_by_outcome`, `publish_outcome`, `merge_read_mode`.
  - CPU and time: `wall_s`, `process_cpu_s`, `cores_busy (cpu/wall)`,
    `records_per_s_wall`, `records_per_s_per_core (records / process cpu)`,
    `busiest_thread_cpu_s`, `threads_cpu_s`.
  - Memory: `peaks`, `memory_target_flushes`.
  - `timeline`: event times and store-operation unions per phase.
  - `request_ledger`: requests and wire bytes per phase.
  - `two_level`:
    - `route`: `two-level` or `single-level`.
    - `two_level_events`: the count of "rlog two-level merge (ADR-3853)"
      events.
    - `event_fields`: that event's fields, among them W1, W2, group size,
      runs, run blocks, ranges and encode builders.
    - `ledger`: the request ledger's level-1 groups and workers, level-2
      workers and ranges, and encode builders.
    - `split`: `level1_wall_s`, `level1_cpu_s`, `level1_cpu_us_per_record`,
      the same three for level 2, `plan_*`, `publish_*` and
      `levels_share_of_process_cpu`. Level 1 runs from the first read-mode
      event to the two-level event, so it includes the run catalog load and
      the level-2 plan. Level 2 runs from there to "compaction merge
      ended". Both divide by `records_in`. On a single-level run, the level
      figures are null.
  - `profile`: null unless the run is profiled. When profiled, it holds
    `samples`, `phases`, `levels` (samples per merge level, each with its
    own top 15 exclusive leaves), `leaf_exclusive_top15`,
    `inclusive_top10_distinct_below_95pct` and the other profile fields.
- `page_choice_ab`: null, or two rows (Exhaustive, then Estimated). Each row
  has `bytes`, `l1_l0_ratio`, `encode_thread_cpu_s`, `encode_wall_s`,
  `decode_thread_cpu_s` and `parts_byte_identical_to_published`.
- `checks`: `realism_band_records_per_s_per_core`, `realism` (pass, the
  miss, or why the band was not applied) and `failures`.

## Bands and exit status

The bin exits 1, after printing the document, when any of these is true:

- **Realism band missed.** At `--inputs 230`, records/s per core (records
  divided by process CPU) must fall within **8,000 to 70,000** inclusive.
  Outside that range, the run is not profiling the production compactor:
  either the compactor is broken, or the host is starved. At any other
  `--inputs` the band is not applied, and `checks.realism` says so.
- **A figure is missing or duplicated.** The 10 figures in `RUN_FIGURES`
  (records in and out, L0 and L1 bytes, L1/L0, parts, read mode, wall,
  process CPU, records/s per core) must each appear exactly once in the
  serialized `run`, with a non-null value.
- `records_in` differs from `records_out`.
- `l1_parts` (from the compaction record) differs from
  `l1_parts_reported_by_outcome` (from the compaction outcome).
- `publish_outcome` is anything other than `Published`.
- `shape.objects` differs from `--inputs`.
- With `--page-choice-ab`: the rows are not Exhaustive then Estimated, or a
  row has no bytes, ratio or encode CPU.
- On a profiled run: there are no samples, or the phase, leaf or inclusive
  table is empty, or attribution failed (`ravel_bench::profiling`).
- With `--scratch-dir` (`check_two_level`), any of these:
  - the route is not two-level, or the two-level event did not fire exactly
    once;
  - one of the 7 `TWO_LEVEL_FIGURES` in `two_level.split` is not reported
    exactly once with a positive value;
  - the levels cover less than **0.80** of the process CPU, or more than
    1.0;
  - a given knob's value differs from the one in `run.config` or is missing
    there, or, for the worker counts, differs from the event's or is
    missing there.

`realism_band_refuses_outside_8k_to_70k` checks the band's edges.

## Results

- [profile-2026-11-05.md](profile-2026-11-05.md): the ADR-3853 T7
  measurement, re-run on 2026-11-06.
- [profile-4141.md](profile-4141.md): two-level CPU per record by arm
  (issue #4141, measured 2026-11-13 and 2026-11-14). At 460 inputs and
  U = 4 GiB, the phase-B default costs 50.6 us/record at level 1 and 49.3
  at level 2. Run block 8,192, the range cut and W1 up to 4 move neither
  level by 10% or more. W2 = 1 with pipelined encode costs 25-30% more at
  level 2. A single-level merge costs 53.8 us/record.

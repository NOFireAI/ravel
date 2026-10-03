# Stage 0: resolved-row memory measurement (issue #2469)

Host: `Linux ci-16gb-fsn1-1 6.8.0-60-generic #63-Ubuntu SMP PREEMPT_DYNAMIC Tue Apr 15 19:04:15 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux`

Command: `cargo run --release -p ravel-bench --bin logseg_resolved_row_alloc`

Method: each shape runs `WARMUP + RUNS` unsampled encodes (`stage0::ROW_SAMPLE` off) and reads the step delta (`after_resolve_rows` live bytes minus `after_columns` live bytes, median over the post-warmup runs) off the existing whole-encode label hook from issue #2428, then one sampled encode (`stage0::ROW_SAMPLE` on) whose per-row instrumentation in `resolve_row` and the row-resolution loop in `build_object` accumulates live-byte and allocation-count deltas into five `stage0` bucket counters plus a whole-row-call counter, read back after `finish()` returns. Two further counters, sampled directly around the once-per-encode setup before the loop and the row-ordering step after it, account for the step delta's two components outside `resolve_row`'s per-row cost; see the deviation note below.

Calibration: 10000 consecutive `stats_sampler()` calls with nothing in between measured 0 bytes and 0 allocations of overhead (`StatsAlloc::stats()` only reads atomics, so this is expected to be exactly zero; it was not subtracted from anything below because it measured zero).

## Deviation from the specified method

The step delta's window (`after_columns` to `after_resolve_rows` in `build_object`) covers the per-row loop (which `resolve_row`'s per-component sampling and the per-row whole-call sampling both measure) plus two things outside it: the once-per-encode setup between `after_columns` firing and the loop starting (building `stream_seeds`, one `StreamSeed` per distinct stream, then `StampScratch::prepare` and `rows: Vec::with_capacity(self.records.len())`), and the row-ordering step that runs once after the loop (`rows.sort_by(...)` in the unclustered path, or `clustered_permutation` plus `permute` in the clustered path).

The first run against `1_stream` found the sum of per-row whole-call deltas undershooting the step delta by far more than 5% (7,367,670 vs 11,368,286 bytes, a 4,000,616 byte gap). `size_of::<ResolvedRow>()` is 200 bytes; 200 * 20000 = 4000000 bytes, matching the gap to within 616 bytes -- initially read as corroborating a stable-sort auxiliary-buffer hypothesis, since that buffer is also sized `size_of::<ResolvedRow>() * records.len()`. Measuring the sort/permute step directly (sampling immediately around the `match &cluster { ... }` block) found it contributes close to zero net live bytes: a transient buffer that is allocated and freed within the same call nets to zero in a live-bytes delta by construction, which the two candidate explanations cannot be told apart by magnitude alone, since `Vec::with_capacity(records.len())` for `ResolvedRow` elements is sized identically. Measuring the pre-loop setup directly instead accounted for the `1_stream` gap: `rows`'s backing buffer is still live when `after_resolve_rows` fires, so it shows up as net-positive bytes in the step delta the way a transient, already-freed buffer cannot, and `stream_seeds` is trivial at one stream.

A second run against `20000_streams`, with the fix in place, still undershot (sum of per-row and pre-loop-setup deltas 11,400,400 vs step delta 19,050,336, a 7,649,936 byte gap) because the first fix's sampling window started after `stream_seeds` was already built. `stream_seeds` holds one `StreamSeed` per distinct stream, so its cost scales with stream count, not row count, and is negligible at 1 stream but dominant at 20,000: moving the sampling window to start immediately after `after_columns` fires, before `stream_seeds` is built, closed this gap too.

None of this is a `ResolvedRow` field or a per-row cost, so none of it belongs in the five `ResolvedRow`-field buckets without misattributing it: the row Vec's backing buffer holds every row, it is not part of any one of them; `stream_seeds` is keyed by stream, not row. Rather than restructure `resolve_row` or add a second hook mechanism, `build_object` now also samples directly around the `match &cluster { ... }` block and around the pre-loop setup, starting right after `after_columns` fires (both gated by the same `ROW_SAMPLE` flag, accumulated into two new `stage0` pairs -- `ROW_ORDER_BYTES`/`ROW_ORDER_ALLOCS` and `ROW_SETUP_BYTES`/`ROW_SETUP_ALLOCS` -- distinct from the five `ResolvedRow`-field buckets). The per-shape sections below report both as their own lines, and the "sum of per-row whole-call deltas equals the step delta within 5%" assertion now compares the step delta against the per-row sum plus both directly-measured costs, not the per-row sum alone. This is a deviation from the dispatch's literal assertion wording (which did not anticipate step-delta components outside `resolve_row`), made because the alternative was either a false failure on correct instrumentation or silently misattributing non-row cost to a `ResolvedRow` field.

## ResolvedRow field to bucket assignment

| field | bucket |
|---|---|
| `stream_ref` | everything else (scalar, no allocation) |
| `ts_ns` | everything else (scalar, no allocation) |
| `observed_ts_ns` | everything else (scalar, no allocation) |
| `severity_num` | everything else (scalar, no allocation) |
| `severity_text` | everything else (`String` clone) |
| `body` | everything else (`String` clone) |
| `trace_id` | everything else (fixed `[u8; 16]`, no allocation) |
| `span_id` | everything else (fixed `[u8; 8]`, no allocation) |
| `flags` | everything else (scalar, no allocation) |
| `attrs_raw` | overflow attributes (`canonical_attr_bytes(&overflow)`) |
| `columns` | column map nodes (`BTreeMap` inserts plus the final `cols.into_iter().collect()`); the `ColumnValue::Str`/`::Bytes` bytes each entry holds are counted under owned string and byte values instead, see note below |
| `indexed_terms` | stamp scratch work (`stamp.finish`) |
| `stat_winners` | stamp scratch work (`stamp.finish`) |

Note: `resolve_value`'s `ColumnValue::Str`/`::Bytes` allocation happens once per attribute before the branch that decides whether it lands in `columns` or in the overflow path, so it is counted as its own bucket (owned string and byte values) rather than split into `columns` vs `attrs_raw`; separating it per destination would require restructuring `resolve_row`'s loop body. Likewise, `stamp.push_columnar` and `stamp.push_overflow` take a second clone of the same value fed to the scratch (distinct from the first, counted under owned string and byte values); that second clone's cost is counted under stamp scratch work rather than split out, since it is inseparable from the scratch call it is an argument to without restructuring.

## 1_stream (1 streams)

Step delta (median over 5 runs, sampling off): 11368286 bytes

Rows sampled: 20000 (expected 20000)

Sum of per-row whole-call deltas (sampling on): 7367670 bytes, 120003 allocations

Row-ordering step (sort/permute, after the loop, not a `ResolvedRow` field): 0 bytes, 1 allocations

Per-encode setup (`stream_seeds` build, one `StreamSeed` per distinct stream, plus `StampScratch::prepare` and the row `Vec::with_capacity`; scales with stream count not row count; not a `ResolvedRow` field): 4000616 bytes, 12 allocations

Bytes per resolved row (step delta / 20000): 568.41

| bucket | bytes | share of step delta | allocations/row | band | verdict |
|---|---|---|---|---|---|
| column map nodes | 3200000 | 28.15% | 2.000 | share [30%, 60%], allocs [2, 8] | share below band, allocs inside |
| owned string and byte values | 370000 | 3.25% | 1.000 | share [25%, 55%], allocs [4, 12] | share below band, allocs below band |
| overflow attributes | 0 | 0.00% | 0.000 | share [0%, 10%], allocs [0, 2] | inside |
| stamp scratch work | 3200336 | 28.15% | 1.000 | no pre-registered band (folds into "everything else" in the epic's expectations) | n/a |
| everything else | 597334 | 5.25% | 2.000 | share [0%, 15%], allocs [0, 4] | inside |
| row-ordering step (not a `ResolvedRow` field, see deviation note above) | 0 | 0.00% | 0.000 | no pre-registered band (outside the epic's `ResolvedRow`-bucket expectations) | n/a |
| per-encode setup: stream_seeds + stamp/row-vec init (not a `ResolvedRow` field, see deviation note above) | 4000616 | 35.19% | 0.001 | no pre-registered band (outside the epic's `ResolvedRow`-bucket expectations) | n/a |
| unattributed remainder | 0 | 0.00% | n/a | < 5% | inside |

Bytes per resolved row band [550, 950]: inside (measured 568.41)

## 1000_streams (1000 streams)

Step delta (median over 5 runs, sampling off): 11739536 bytes

Rows sampled: 20000 (expected 20000)

Sum of per-row whole-call deltas (sampling on): 7330336 bytes, 120003 allocations

Row-ordering step (sort/permute, after the loop, not a `ResolvedRow` field): 0 bytes, 1 allocations

Per-encode setup (`stream_seeds` build, one `StreamSeed` per distinct stream, plus `StampScratch::prepare` and the row `Vec::with_capacity`; scales with stream count not row count; not a `ResolvedRow` field): 4409200 bytes, 9012 allocations

Bytes per resolved row (step delta / 20000): 586.98

| bucket | bytes | share of step delta | allocations/row | band | verdict |
|---|---|---|---|---|---|
| column map nodes | 3200000 | 27.26% | 2.000 | share [30%, 60%], allocs [2, 8] | share below band, allocs inside |
| owned string and byte values | 370000 | 3.15% | 1.000 | share [25%, 55%], allocs [4, 12] | share below band, allocs below band |
| overflow attributes | 0 | 0.00% | 0.000 | share [0%, 10%], allocs [0, 2] | inside |
| stamp scratch work | 3200336 | 27.26% | 1.000 | no pre-registered band (folds into "everything else" in the epic's expectations) | n/a |
| everything else | 560000 | 4.77% | 2.000 | share [0%, 15%], allocs [0, 4] | inside |
| row-ordering step (not a `ResolvedRow` field, see deviation note above) | 0 | 0.00% | 0.000 | no pre-registered band (outside the epic's `ResolvedRow`-bucket expectations) | n/a |
| per-encode setup: stream_seeds + stamp/row-vec init (not a `ResolvedRow` field, see deviation note above) | 4409200 | 37.56% | 0.451 | no pre-registered band (outside the epic's `ResolvedRow`-bucket expectations) | n/a |
| unattributed remainder | 0 | 0.00% | n/a | < 5% | inside |

## 20000_streams (20000 streams)

Step delta (median over 5 runs, sampling off): 19050336 bytes

Rows sampled: 20000 (expected 20000)

Sum of per-row whole-call deltas (sampling on): 7400336 bytes, 120003 allocations

Row-ordering step (sort/permute, after the loop, not a `ResolvedRow` field): 0 bytes, 1 allocations

Per-encode setup (`stream_seeds` build, one `StreamSeed` per distinct stream, plus `StampScratch::prepare` and the row `Vec::with_capacity`; scales with stream count not row count; not a `ResolvedRow` field): 11650000 bytes, 180016 allocations

Bytes per resolved row (step delta / 20000): 952.52

| bucket | bytes | share of step delta | allocations/row | band | verdict |
|---|---|---|---|---|---|
| column map nodes | 3200000 | 16.80% | 2.000 | share [30%, 60%], allocs [2, 8] | share below band, allocs inside |
| owned string and byte values | 360000 | 1.89% | 1.000 | share [25%, 55%], allocs [4, 12] | share below band, allocs below band |
| overflow attributes | 0 | 0.00% | 0.000 | share [0%, 10%], allocs [0, 2] | inside |
| stamp scratch work | 3200336 | 16.80% | 1.000 | no pre-registered band (folds into "everything else" in the epic's expectations) | n/a |
| everything else | 640000 | 3.36% | 2.000 | share [0%, 15%], allocs [0, 4] | inside |
| row-ordering step (not a `ResolvedRow` field, see deviation note above) | 0 | 0.00% | 0.000 | no pre-registered band (outside the epic's `ResolvedRow`-bucket expectations) | n/a |
| per-encode setup: stream_seeds + stamp/row-vec init (not a `ResolvedRow` field, see deviation note above) | 11650000 | 61.15% | 9.001 | no pre-registered band (outside the epic's `ResolvedRow`-bucket expectations) | n/a |
| unattributed remainder | 0 | 0.00% | n/a | < 5% | inside |


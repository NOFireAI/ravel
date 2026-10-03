# Stage 0c: row versus columnar memory and wall-time measurement (issue #2475)

Host: `Linux ip-172-31-18-6 7.0.0-1011-aws #11~24.04.1-Ubuntu SMP PREEMPT Mon Aug 10 15:20:57 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux`

Uptime before: `19:13:05 up 29 days, 22:52,  5 users,  load average: 2.59, 2.71, 2.63`

Uptime after: `19:13:52 up 29 days, 22:53,  5 users,  load average: 2.19, 2.57, 2.59`

Command: `cargo run --release -p ravel-bench --bin logseg_row_vs_columnar_alloc`

Method: per shape, three memory arms (arm R: `RlogWriter::push` per record then `finish`; arm C-kept: `ColumnarLogBatch::from_records` then `push_columnar` then `finish`, with the cloned source records kept alive until `finish` returns; arm C-dropped: the same, but the cloned source records are dropped immediately after `from_records` returns and before `push_columnar`), each sampled with `stats_alloc` (1 warm-up, 5 iterations, min/max reported), baseline taken immediately before the arm's first allocation (the corpus clone). `build_object` and `build_object_columnar` both fire the shared `stage0::HOOK` at 7 matching labels; arm R's samples come from the row path's existing labels, arm C's from the new labels added to `build_object_columnar` by this task. A separate wall-time loop (hook recording turned off via an internal flag, since `stage0::HOOK`'s `OnceLock` cannot be literally uninstalled once set for the memory phase above) runs 3 warm-up runs then 5 measured runs of 10 iterations per arm per shape, arms and shapes interleaved inside each run, cloning the corpus outside the timed region.

Note: `stats_alloc`'s counters are running totals (`bytes_allocated`, `bytes_deallocated`, `bytes_reallocated`); `StatsAlloc` itself reports no high-water mark. Each PEAK figure below is therefore the largest of a fixed, finite set of sampled points (manual samples plus the 7 hook labels), not the arm's true peak live-byte figure -- an allocation spike between two sampled points is invisible to this method. Every PEAK reported here is a LOWER bound on that arm's real peak. This applies to every arm identically, so a ratio of two lower bounds (`C-dropped PEAK / R PEAK`, `C-kept PEAK / R PEAK`) is an estimate with no guaranteed direction: if the true peaks differ from the sampled lower bounds by different amounts, the ratio of the lower bounds can over- or understate the ratio of the true peaks.

## 1_stream

Encoded object length: 122896 bytes (row and columnar byte-identical, asserted)

### Arm R (row path)

| label | min live bytes | max live bytes |
|---|---|---|
| pushed | 18032454 | 18032454 |
| before_index | 18032838 | 18032838 |
| after_index | 18032938 | 18032938 |
| after_columns | 18035298 | 18035298 |
| after_resolve_rows | 29403584 | 29403584 |
| after_blocks | 40597112 | 40597112 |
| after_sections | 39473281 | 39473281 |
| before_return | 39473281 | 39473281 |

PEAK: 40597112 bytes at label `after_blocks`

TOTAL_ALLOCATED: 77688134 to 77688134 bytes (min to max over 5 sampled runs)

### Arm C-kept (columnar, records kept)

| label | min live bytes | max live bytes |
|---|---|---|
| batch_built | 25802148 | 25802148 |
| before_index | 25803996 | 25803996 |
| after_index | 25804488 | 25804488 |
| after_columns | 25807552 | 25807552 |
| after_resolve_rows | 28953029 | 28953029 |
| after_blocks | 42899277 | 42899277 |
| after_sections | 41775446 | 41775446 |
| before_return | 41775446 | 41775446 |

PEAK: 42899277 bytes at label `after_blocks`

TOTAL_ALLOCATED: 83628186 to 83628186 bytes (min to max over 5 sampled runs)

### Arm C-dropped (columnar, records dropped)

| label | min live bytes | max live bytes |
|---|---|---|
| batch_built | 25802148 | 25802148 |
| records_dropped | 15054838 | 15054838 |
| before_index | 15056806 | 15056806 |
| after_index | 15057298 | 15057298 |
| after_columns | 15060362 | 15060362 |
| after_resolve_rows | 18205839 | 18205839 |
| after_blocks | 32152087 | 32152087 |
| after_sections | 31028256 | 31028256 |
| before_return | 31028256 | 31028256 |

PEAK: 32152087 bytes at label `after_blocks`

TOTAL_ALLOCATED: 83628258 to 83628258 bytes (min to max over 5 sampled runs)

PEAK ratio C-dropped/R: 0.7920 (79.20% of R)

PEAK ratio C-kept/R: 1.0567 (105.67% of R)

C-dropped PEAK reduction vs R: 20.80% (pre-registered band [25%, 45%]): below band

C-kept PEAK delta vs R: +5.67% (pre-registered band [-10%, +10%]): inside

## 1000_streams

Encoded object length: 34256 bytes (row and columnar byte-identical, asserted)

### Arm R (row path)

| label | min live bytes | max live bytes |
|---|---|---|
| pushed | 18032920 | 18032920 |
| before_index | 18100232 | 18100232 |
| after_index | 18143256 | 18143256 |
| after_columns | 18257504 | 18257504 |
| after_resolve_rows | 29997040 | 29997040 |
| after_blocks | 38516095 | 38516095 |
| after_sections | 39084095 | 39084095 |
| before_return | 39084095 | 39084095 |

PEAK: 39084095 bytes at label `after_sections`

TOTAL_ALLOCATED: 68617892 to 68617892 bytes (min to max over 5 sampled runs)

### Arm C-kept (columnar, records kept)

| label | min live bytes | max live bytes |
|---|---|---|
| batch_built | 25950040 | 25950040 |
| before_index | 25951888 | 25951888 |
| after_index | 26074680 | 26074680 |
| after_columns | 26598216 | 26598216 |
| after_resolve_rows | 29743693 | 29743693 |
| after_blocks | 41015468 | 41015468 |
| after_sections | 41583468 | 41583468 |
| before_return | 41583468 | 41583468 |

PEAK: 41583468 bytes at label `after_sections`

TOTAL_ALLOCATED: 74820012 to 74820012 bytes (min to max over 5 sampled runs)

### Arm C-dropped (columnar, records dropped)

| label | min live bytes | max live bytes |
|---|---|---|
| batch_built | 25950040 | 25950040 |
| records_dropped | 15202264 | 15202264 |
| before_index | 15204232 | 15204232 |
| after_index | 15327024 | 15327024 |
| after_columns | 15850560 | 15850560 |
| after_resolve_rows | 18996037 | 18996037 |
| after_blocks | 30267812 | 30267812 |
| after_sections | 30835812 | 30835812 |
| before_return | 30835812 | 30835812 |

PEAK: 30835812 bytes at label `after_sections`

TOTAL_ALLOCATED: 74820084 to 74820084 bytes (min to max over 5 sampled runs)

PEAK ratio C-dropped/R: 0.7890 (78.90% of R)

PEAK ratio C-kept/R: 1.0639 (106.39% of R)

C-dropped PEAK reduction vs R: 21.10% (pre-registered band [25%, 45%]): below band

C-kept PEAK delta vs R: +6.39% (pre-registered band [-10%, +10%]): inside

## 20000_streams

Encoded object length: 521855 bytes (row and columnar byte-identical, asserted)

### Arm R (row path)

| label | min live bytes | max live bytes |
|---|---|---|
| pushed | 18134010 | 18134010 |
| before_index | 19467370 | 19467370 |
| after_index | 20155514 | 20155514 |
| after_columns | 22397762 | 22397762 |
| after_resolve_rows | 41448098 | 41448098 |
| after_blocks | 48410268 | 48410268 |
| after_sections | 55373104 | 55373104 |
| before_return | 55373104 | 55373104 |

PEAK: 55373104 bytes at label `after_sections`

TOTAL_ALLOCATED: 95559867 to 95559867 bytes (min to max over 5 sampled runs)

### Arm C-kept (columnar, records kept)

| label | min live bytes | max live bytes |
|---|---|---|
| batch_built | 29863650 | 29863650 |
| before_index | 29865498 | 29865498 |
| after_index | 32145426 | 32145426 |
| after_columns | 42037762 | 42037762 |
| after_resolve_rows | 45183239 | 45183239 |
| after_blocks | 54898129 | 54898129 |
| after_sections | 61860965 | 61860965 |
| before_return | 61860965 | 61860965 |

PEAK: 61860965 bytes at label `after_sections`

TOTAL_ALLOCATED: 106301051 to 106301051 bytes (min to max over 5 sampled runs)

### Arm C-dropped (columnar, records dropped)

| label | min live bytes | max live bytes |
|---|---|---|
| batch_built | 29863650 | 29863650 |
| records_dropped | 19014784 | 19014784 |
| before_index | 19016752 | 19016752 |
| after_index | 21296680 | 21296680 |
| after_columns | 31189016 | 31189016 |
| after_resolve_rows | 34334493 | 34334493 |
| after_blocks | 44049383 | 44049383 |
| after_sections | 51012219 | 51012219 |
| before_return | 51012219 | 51012219 |

PEAK: 51012219 bytes at label `after_sections`

TOTAL_ALLOCATED: 106301123 to 106301123 bytes (min to max over 5 sampled runs)

PEAK ratio C-dropped/R: 0.9212 (92.12% of R)

PEAK ratio C-kept/R: 1.1172 (111.72% of R)

C-dropped PEAK reduction vs R: 7.88% (pre-registered band [10%, 30%]): below band

C-kept PEAK delta vs R: +11.72% (pre-registered band [-10%, +10%]): above band

## Encode wall time

### 1_stream

| arm | median run-mean (ns) | min | max |
|---|---|---|---|
| R | 79198391 | 77752131 | 83751596 |
| C | 77637853 | 76484710 | 80616566 |

Ratio C/R per run: 0.9803, 0.9837, 0.9729, 0.9803, 0.9626

Ratio C/R median 0.9803, min 0.9626, max 0.9837 (pre-registered band [0.85, 1.15]): inside

### 1000_streams

| arm | median run-mean (ns) | min | max |
|---|---|---|---|
| R | 64258670 | 62522765 | 68497680 |
| C | 59232074 | 57870813 | 61828698 |

Ratio C/R per run: 0.9255, 0.9256, 0.9279, 0.9205, 0.9026

Ratio C/R median 0.9255, min 0.9026, max 0.9279 (pre-registered band [0.85, 1.15]): inside

### 20000_streams

| arm | median run-mean (ns) | min | max |
|---|---|---|---|
| R | 96767662 | 90488364 | 106748159 |
| C | 96580982 | 89718019 | 109418850 |

Ratio C/R per run: 0.9777, 0.9915, 1.0175, 0.9981, 1.0250

Ratio C/R median 0.9981, min 0.9777, max 1.0250 (pre-registered band [0.85, 1.15]): inside


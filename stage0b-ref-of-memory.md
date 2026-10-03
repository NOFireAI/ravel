# Stage 0b: ref_of map memory measurement (issue #2428)

Host: `Linux ci-16gb-fsn1-1 6.8.0-60-generic #63-Ubuntu SMP PREEMPT_DYNAMIC Tue Apr 15 19:04:15 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux`

Command: `cargo run --release -p ravel-bench --bin logseg_ref_of_alloc`

Stats fields used: `live_bytes = bytes_allocated - bytes_deallocated + bytes_reallocated` (the allocator's cumulative running totals, sampled via `StatsAlloc::stats()`; `allocations`/`deallocations`/`reallocations` operation counts are not used). A baseline is sampled immediately before `RlogWriter::new` in each iteration and subtracted from every later sample in that iteration. `TOTAL_ALLOCATED` is the separate, unsubtracted `bytes_allocated` delta from that same baseline to immediately after `finish()` returns.

**`stats_alloc` reports running totals, not a high-water mark.** A byte freed and a new byte allocated between two samples cancel out in `live_bytes`, so a transient allocation that peaked and was freed between two label points is invisible here. `PEAK_SAMPLE` is therefore a LOWER bound on the true peak live memory, and `SHARE` (the map's share of `PEAK_SAMPLE`) is correspondingly an UPPER bound on the map's share of the true peak: the true peak can only be larger, never smaller, which can only shrink the map's true share.

Map entry type `(LogStreamId, u32)` size_of: 20 bytes.

## 1_stream (1 streams)

Map entries (mode 1 check): 1 (expected 1)

| label | mode0 live bytes (median [min..max]) | mode2 live bytes (median [min..max]) |
|---|---|---|
| pushed | 7285120 [7285120..7285120] | 7285120 [7285120..7285120] |
| before_index | 7285504 [7285504..7285504] | 7285504 [7285504..7285504] |
| after_index | 7285604 [7285604..7285604] | 7285504 [7285504..7285504] |
| after_columns | 7287964 [7287964..7287964] | 7287864 [7287864..7287864] |
| after_resolve_rows | 18656250 [18656250..18656250] | 18656150 [18656150..18656150] |
| after_blocks | 29849778 [29849778..29849778] | 29849678 [29849678..29849678] |
| after_sections | 28725947 [28725947..28725947] | 28725847 [28725847..28725847] |
| before_return | 28725947 [28725947..28725947] | 28725847 [28725847..28725847] |

INDEX_BYTES (mode0 after_index - before_index, median over 5 runs): 100

INDEX_BYTES (mode2, must be 0): 0

PEAK_SAMPLE (mode0): 29849778 bytes, at label `after_blocks`

PEAK_SAMPLE (mode2): 29849678 bytes, at label `after_blocks`

SHARE (mode0 INDEX_BYTES / mode0 PEAK_SAMPLE): 0.0003%

TOTAL_ALLOCATED per encode (median over 5 runs): mode0 66940800 bytes, mode2 66940700 bytes

INDEX_BYTES band [60, 200]: inside (measured 100)

SHARE band (< 0.01%): inside (measured 0.0003%)

## 1000_streams (1000 streams)

Map entries (mode 1 check): 1000 (expected 1000)

| label | mode0 live bytes (median [min..max]) | mode2 live bytes (median [min..max]) |
|---|---|---|
| pushed | 7285120 [7285120..7285120] | 7285120 [7285120..7285120] |
| before_index | 7352432 [7352432..7352432] | 7352432 [7352432..7352432] |
| after_index | 7395456 [7395456..7395456] | 7352432 [7352432..7352432] |
| after_columns | 7509704 [7509704..7509704] | 7466680 [7466680..7466680] |
| after_resolve_rows | 19249240 [19249240..19249240] | 19206216 [19206216..19206216] |
| after_blocks | 27768295 [27768295..27768295] | 27725271 [27725271..27725271] |
| after_sections | 28336295 [28336295..28336295] | 28293271 [28293271..28293271] |
| before_return | 28336295 [28336295..28336295] | 28293271 [28293271..28293271] |

INDEX_BYTES (mode0 after_index - before_index, median over 5 runs): 43024

INDEX_BYTES (mode2, must be 0): 0

PEAK_SAMPLE (mode0): 28336295 bytes, at label `after_sections`

PEAK_SAMPLE (mode2): 28293271 bytes, at label `after_sections`

SHARE (mode0 INDEX_BYTES / mode0 PEAK_SAMPLE): 0.1518%

TOTAL_ALLOCATED per encode (median over 5 runs): mode0 57870092 bytes, mode2 57827068 bytes

INDEX_BYTES band [43000, 43100]: inside (measured 43024)

SHARE band (< 1%): inside (measured 0.1518%)

## 20000_streams (20000 streams)

Map entries (mode 1 check): 20000 (expected 20000)

| label | mode0 live bytes (median [min..max]) | mode2 live bytes (median [min..max]) |
|---|---|---|
| pushed | 7285120 [7285120..7285120] | 7285120 [7285120..7285120] |
| before_index | 8618480 [8618480..8618480] | 8618480 [8618480..8618480] |
| after_index | 9306624 [9306624..9306624] | 8618480 [8618480..8618480] |
| after_columns | 11548872 [11548872..11548872] | 10860728 [10860728..10860728] |
| after_resolve_rows | 30599208 [30599208..30599208] | 29911064 [29911064..29911064] |
| after_blocks | 37561378 [37561378..37561378] | 36873234 [36873234..36873234] |
| after_sections | 44524214 [44524214..44524214] | 43836070 [43836070..43836070] |
| before_return | 44524214 [44524214..44524214] | 43836070 [43836070..43836070] |

INDEX_BYTES (mode0 after_index - before_index, median over 5 runs): 688144

INDEX_BYTES (mode2, must be 0): 0

PEAK_SAMPLE (mode0): 44524214 bytes, at label `after_sections`

PEAK_SAMPLE (mode2): 43836070 bytes, at label `after_sections`

SHARE (mode0 INDEX_BYTES / mode0 PEAK_SAMPLE): 1.5456%

TOTAL_ALLOCATED per encode (median over 5 runs): mode0 84710977 bytes, mode2 84022833 bytes

INDEX_BYTES band [688100, 688200]: inside (measured 688144)

SHARE band (< 5%): inside (measured 1.5456%)


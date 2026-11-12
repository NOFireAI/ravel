# Bucket-bytes prediction (ADR-4143 decision 2)

`predict.py` predicts the scratch bytes a write-once bucketed spill retry
would write for ClickBench statements 19, 34 and 35 if it routed their
string keys (ADR-4143 decision 3), with and without combining each chunk
before it is written (decision 4). It compares each prediction with the
smallest sort-merge `spill_bytes_written` M8 measured for that statement and
box, and prints the decision 2 verdict. `results.txt` is the output of the
full-corpus run this README reports.

Verdict: **build decision 3 alone.** Uncombined routing is below the
threshold for every statement, box and `bits` from 4 to 8, at 0.265 to 0.510
of the threshold.

## Running it

The script needs Python 3 with `duckdb`, `pyarrow` and `numpy` (the run
below used Python 3.12.3, duckdb 2.0.0, pyarrow 26.0.1, numpy 2.5.4) and the
single-file ClickBench corpus,
`https://datasets.clickhouse.com/hits_compatible/hits.parquet` (99,997,497
rows). Keep the corpus, the virtualenv and the state file out of the tree,
under `.dd-tools/`:

```sh
python3 -m venv .dd-tools/venv
.dd-tools/venv/bin/pip install duckdb pyarrow numpy
mkdir -p .dd-tools/corpus
curl -fL -o .dd-tools/corpus/hits.parquet \
  https://datasets.clickhouse.com/hits_compatible/hits.parquet

.dd-tools/venv/bin/python benchmarks/clickbench/bucket-bytes/predict.py \
  --corpus .dd-tools/corpus/hits.parquet \
  --state .dd-tools/bucket-bytes-state.json --budget-seconds 30
```

The run is split into stages per statement: `head`, `slice0` to `slice4`,
`sample` and `routed`. Each finished stage is saved to `--state`. With
`--budget-seconds`, the script starts no new stage after the budget and
exits 3. Rerun the same command until it exits 0, which prints the report on
stdout. Progress lines go to stderr with a `#` prefix. A state file written
with other arguments is refused. Exit 1 with a message means a check failed
(see the claims audit).

To smoke-test on a corpus prefix:

```sh
.dd-tools/venv/bin/python benchmarks/clickbench/bucket-bytes/predict.py \
  --corpus .dd-tools/corpus/hits.parquet --state .dd-tools/smoke-state.json \
  --rows 2000000 --slice-rows 1000000 --sample-slice 1
```

`--memory` (default `6GB`) caps DuckDB. The sample-slice stages hold about
three copies of a 20M-row URL column in Python, so the full run needs about
11 GB of free memory. Each stage takes 2 to 6 minutes on 8 threads; the full
run took about 75 minutes.

## Model

These are the routed rows, per statement:

| statement | routed columns, in order | key types | state |
|---|---|---|---|
| 19 | `UserID`, `m`, `SearchPhrase`, `COUNT(*)[count]` | Int64, Int32, Utf8View | Int64 |
| 34 | `URL`, `COUNT(*)[count]` | Utf8View | Int64 |
| 35 | `Int64(1)`, `URL`, `COUNT(*)[count]` | Int64, Utf8View | Int64 |

`m` is `date_part('minute', ts)`, which DataFusion types Int32. No key is a
float, so every key is admissible under decision 3.

The figures for each statement, box and `bits` are computed as follows:

- **Rows.** Every corpus row reaches the final and is routed once. That is
  ADR-4143's premise that the partial aggregate gives up (see "Partial
  aggregate" below). Any reduction by the partial only lowers the bytes.
- **Chunk budget.** Decision 3's per-chunk budget is
  `(S - table(bits)) / (2^bits + 1)`. `table(bits)` depends on `T`, which
  this script does not know for these statements, so it uses the smallest
  budget decision 1 can give that `bits`: `table = 0.75 x S`, giving
  `budget_min_B = floor(S / 4) / (2^bits + 1)`. `budget_max_B` is the
  largest budget, at a table just above `0.375 x S`.
  - A smaller budget means more batches and worse compression, so the bytes
    are an upper bound across `T`.
  - The budget charged per row is 16 B per view, the fixed widths, an 8 B
    count, one validity bit per column, and the string's length when it is
    over 12 B (out-of-line). 64 B of alignment per buffer is subtracted.
  - A chunk seals when its charge would exceed the budget or at 8,192 rows.
    A single row larger than the budget is a chunk of its own.
- **Uncombined, corpus order (`unc_ipc_B`).** The full corpus is read in
  five contiguous slices of 20M rows each. Each slice is cut into chunks in
  corpus order and written as one Arrow IPC stream with lz4_frame
  compression, one record batch per chunk. Each of the `16 x 2^bits` scratch
  files would carry its own schema message and end-of-stream marker, so
  `empty_stream_B` is added once for every file the slices did not already
  write.
- **Routing penalty (`routed_penalty`).** The route writes each bucket's rows
  to that bucket's own chunks, not consecutive corpus rows. Corpus rows
  40M to 60M (slice 2, the sample) are written both ways:
  - corpus order;
  - routed: rows sorted by bucket, corpus order kept within a bucket, each
    bucket cut into its own chunks.

  The ratio of the routed bytes to the corpus-order bytes, both counted with
  per-file framing, is the routing penalty.
  `routed_unc_ipc_B = unc_ipc_B x routed_penalty`.
  `unc_pred_B = max(unc_ipc_B, routed_unc_ipc_B)` is the uncombined
  prediction that the verdict uses.
- **Bucket.** The top `4 + bits` bits of DuckDB's 64-bit `hash()` of the key
  columns. This is the exchange partition (P = 16) and the router's bucket
  together. The executor draws those from two independent hashes; one
  uniform hash gives the same distribution.
- **Combined (`comb_rows`, `comb_ipc_B`).**
  - The bucket's chunks, cut as above, are each deduplicated by key, with
    counts summed.
  - `comb_rows` is exact over the full corpus. Keys are identified by their
    64-bit hash (see the audit for the 4 collisions).
  - The compressed bytes per combined row are measured on the sample slice
    (`sample_comb_ipc_B / sample_comb_rows`), then multiplied by `comb_rows`,
    with per-file framing added.
- **Threshold (`threshold_B`).** ADR-4143 decision 2's table: the smallest M8
  sort-merge `spill_bytes_written` for that statement and box.
- **Verdict per statement and box.** One of:
  - `uncombined_below`: `unc_pred_B` is below the threshold at every `bits`;
  - `combined_below_at_every_bits`, `_at_some_bits` or `_at_no_bits`,
    otherwise.

  The final verdict follows decision 2. "Build decision 3 alone" requires
  every cell to be `uncombined_below`. "With decision 4" requires every cell
  to be below with combining. Anything else is "build neither".

### Which bytes are counted

Every `_ipc_B` figure, and `unc_pred_B`, `routed_unc_ipc_B`,
`unc_sample_scaled_B` and `empty_stream_B`, counts compressed Arrow IPC
stream bytes, as the writer emits them to a counting sink
(`pa.MockOutputStream`). That includes:

- the schema message;
- each record batch's metadata;
- the body: each buffer lz4_frame-compressed, with its 8-byte length prefix
  and padding;
- the end-of-stream marker.

There are no retries, no read-back, and no filesystem or object overhead.
`raw` figures count Arrow buffer bytes before compression: views, fixed
values, the copied out-of-line string bytes, and counts. No validity buffer
is written, because no routed column has nulls.

The writer is Arrow C++ (pyarrow), standing in for arrow-rs, which
DataFusion's spill writer uses. Both write lz4_frame per buffer. arrow-rs
also writes a buffer uncompressed when compression would grow it, which can
only lower its bytes. `budget_min_B`, `budget_max_B` and `floor_charge_B`
are memory-charge figures, `threshold_B` is sort-merge
`spill_bytes_written` as M8 reported it, and `tunes_up_to_T_B` is a table
size `T`; none of them is bytes this script writes.

## Results

The per-statement lines of `results.txt` are listed below.

| statement | results.txt | distinct keys | raw B/row (full) | raw B/row (sample) | probe windows above 0.1 |
|---|---|---|---|---|---|
| 19 | line 3 | 56,384,822 | 43.86 | 47.52 | 12,204 of 12,206 (min 0.0073, median 0.5603) |
| 34 | line 23 | 18,342,019 | 114.64 | 139.91 | 11,044 of 12,206 (min 0.0001, median 0.3801) |
| 35 | line 43 | 18,342,019 | 122.76 | 148.03 | 11,044 of 12,206 (min 0.0001, median 0.3801) |

The full table follows: one row per statement, box and `bits`, from the
`bits=` lines of `results.txt`. Ratios are computed from the two columns
named. Every row has `unc_rows=99997497`.

| line | stmt | box | bits | budget_min_B | floor fits min/max | unc_ipc_B | routed_penalty | unc_pred_B | comb_rows | comb_ipc_B | threshold_B | unc_pred / threshold | comb / threshold |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 7 | 19 | c6a.large | 4 | 479,203 | yes/yes | 895,277,016 | 1.377 | 1,232,615,385 | 57,438,750 | 1,451,339,079 | 4,649,336,784 | 0.265 | 0.312 |
| 8 | 19 | c6a.large | 5 | 246,862 | yes/yes | 908,382,720 | 1.385 | 1,257,962,508 | 57,405,863 | 1,481,014,738 | 4,649,336,784 | 0.271 | 0.319 |
| 9 | 19 | c6a.large | 6 | 125,330 | yes/yes | 956,242,992 | 1.369 | 1,309,081,742 | 57,384,425 | 1,558,255,212 | 4,649,336,784 | 0.282 | 0.335 |
| 10 | 19 | c6a.large | 7 | 63,150 | no/yes | 991,533,312 | 1.362 | 1,350,142,633 | 57,395,610 | 1,604,502,150 | 4,649,336,784 | 0.290 | 0.345 |
| 11 | 19 | c6a.large | 8 | 31,698 | no/no | 1,046,742,928 | 1.351 | 1,414,189,721 | 57,430,914 | 1,677,445,109 | 4,649,336,784 | 0.304 | 0.361 |
| 12 | 19 | c6a.xlarge | 4 | 1,117,770 | yes/yes | 894,778,432 | 1.379 | 1,234,251,254 | 57,447,334 | 1,451,323,835 | 3,582,127,824 | 0.345 | 0.405 |
| 13 | 19 | c6a.xlarge | 5 | 575,820 | yes/yes | 894,914,768 | 1.395 | 1,248,531,704 | 57,343,985 | 1,464,884,708 | 3,582,127,824 | 0.349 | 0.409 |
| 14 | 19 | c6a.xlarge | 6 | 292,339 | yes/yes | 902,683,928 | 1.401 | 1,264,722,674 | 57,350,772 | 1,487,025,850 | 3,582,127,824 | 0.353 | 0.415 |
| 15 | 19 | c6a.xlarge | 7 | 147,303 | yes/yes | 937,463,904 | 1.417 | 1,328,853,783 | 57,361,508 | 1,559,163,422 | 3,582,127,824 | 0.371 | 0.435 |
| 16 | 19 | c6a.xlarge | 8 | 73,938 | no/yes | 983,168,488 | 1.373 | 1,350,375,293 | 57,365,477 | 1,604,681,437 | 3,582,127,824 | 0.377 | 0.448 |
| 17 | 19 | c6a.2xlarge | 4 | 2,257,260 | yes/yes | 894,778,432 | 1.379 | 1,234,251,254 | 57,447,334 | 1,451,323,835 | 3,290,644,448 | 0.375 | 0.441 |
| 18 | 19 | c6a.2xlarge | 5 | 1,162,831 | yes/yes | 894,856,256 | 1.395 | 1,248,688,486 | 57,343,985 | 1,464,884,708 | 3,290,644,448 | 0.379 | 0.445 |
| 19 | 19 | c6a.2xlarge | 6 | 590,360 | yes/yes | 895,002,536 | 1.404 | 1,256,652,542 | 57,293,521 | 1,467,926,200 | 3,290,644,448 | 0.382 | 0.446 |
| 20 | 19 | c6a.2xlarge | 7 | 297,468 | yes/yes | 902,483,600 | 1.401 | 1,264,720,824 | 57,254,503 | 1,482,534,179 | 3,290,644,448 | 0.384 | 0.451 |
| 21 | 19 | c6a.2xlarge | 8 | 149,312 | yes/yes | 936,479,032 | 1.431 | 1,340,310,958 | 57,257,633 | 1,566,758,308 | 3,290,644,448 | 0.407 | 0.476 |
| 27 | 34 | c6a.large | 4 | 479,203 | yes/yes | 2,947,155,656 | 0.971 | 2,947,155,656 | 23,602,713 | 2,731,833,357 | 8,603,687,856 | 0.343 | 0.318 |
| 28 | 34 | c6a.large | 5 | 246,862 | yes/yes | 2,993,698,832 | 0.957 | 2,993,698,832 | 23,566,964 | 2,783,480,970 | 8,603,687,856 | 0.348 | 0.324 |
| 29 | 34 | c6a.large | 6 | 125,330 | yes/yes | 3,091,274,176 | 0.944 | 3,091,274,176 | 23,560,208 | 2,857,457,005 | 8,603,687,856 | 0.359 | 0.332 |
| 30 | 34 | c6a.large | 7 | 63,150 | no/yes | 3,224,624,432 | 0.929 | 3,224,624,432 | 23,585,357 | 2,968,905,534 | 8,603,687,856 | 0.375 | 0.345 |
| 31 | 34 | c6a.large | 8 | 31,698 | no/no | 3,578,044,192 | 0.917 | 3,578,044,192 | 23,673,533 | 3,301,718,349 | 8,603,687,856 | 0.416 | 0.384 |
| 32 | 34 | c6a.xlarge | 4 | 1,117,770 | yes/yes | 2,925,105,592 | 0.971 | 2,925,105,592 | 22,873,198 | 2,673,623,194 | 6,831,250,656 | 0.428 | 0.391 |
| 33 | 34 | c6a.xlarge | 5 | 575,820 | yes/yes | 2,939,360,288 | 0.958 | 2,939,360,288 | 22,700,153 | 2,690,847,234 | 6,831,250,656 | 0.430 | 0.394 |
| 34 | 34 | c6a.xlarge | 6 | 292,339 | yes/yes | 2,978,645,568 | 0.947 | 2,978,645,568 | 22,685,483 | 2,742,662,376 | 6,831,250,656 | 0.436 | 0.401 |
| 35 | 34 | c6a.xlarge | 7 | 147,303 | yes/yes | 3,061,465,656 | 0.936 | 3,061,465,656 | 22,686,924 | 2,814,055,104 | 6,831,250,656 | 0.448 | 0.412 |
| 36 | 34 | c6a.xlarge | 8 | 73,938 | no/yes | 3,198,131,200 | 0.923 | 3,198,131,200 | 22,711,467 | 2,906,170,495 | 6,831,250,656 | 0.468 | 0.425 |
| 37 | 34 | c6a.2xlarge | 4 | 2,257,260 | yes/yes | 2,921,245,984 | 0.971 | 2,921,245,984 | 22,746,775 | 2,663,055,356 | 6,040,848,496 | 0.484 | 0.441 |
| 38 | 34 | c6a.2xlarge | 5 | 1,162,831 | yes/yes | 2,924,859,800 | 0.958 | 2,924,859,800 | 22,189,179 | 2,648,056,475 | 6,040,848,496 | 0.484 | 0.438 |
| 39 | 34 | c6a.2xlarge | 6 | 590,360 | yes/yes | 2,938,486,128 | 0.947 | 2,938,486,128 | 22,146,200 | 2,681,006,780 | 6,040,848,496 | 0.486 | 0.444 |
| 40 | 34 | c6a.2xlarge | 7 | 297,468 | yes/yes | 2,977,476,728 | 0.938 | 2,977,476,728 | 22,145,872 | 2,739,695,047 | 6,040,848,496 | 0.493 | 0.454 |
| 41 | 34 | c6a.2xlarge | 8 | 149,312 | yes/yes | 3,059,527,664 | 0.931 | 3,059,527,664 | 22,162,907 | 2,821,211,206 | 6,040,848,496 | 0.506 | 0.467 |
| 47 | 35 | c6a.large | 4 | 479,203 | yes/yes | 2,955,700,760 | 0.971 | 2,955,700,760 | 23,655,277 | 2,740,221,926 | 8,598,806,208 | 0.344 | 0.319 |
| 48 | 35 | c6a.large | 5 | 246,862 | yes/yes | 3,008,223,160 | 0.957 | 3,008,223,160 | 23,631,003 | 2,794,268,551 | 8,598,806,208 | 0.350 | 0.325 |
| 49 | 35 | c6a.large | 6 | 125,330 | yes/yes | 3,116,999,816 | 0.944 | 3,116,999,816 | 23,630,362 | 2,873,196,000 | 8,598,806,208 | 0.362 | 0.334 |
| 50 | 35 | c6a.large | 7 | 63,150 | no/yes | 3,272,933,552 | 0.928 | 3,272,933,552 | 23,665,373 | 3,008,116,049 | 8,598,806,208 | 0.381 | 0.350 |
| 51 | 35 | c6a.large | 8 | 31,698 | no/no | 3,654,333,360 | 0.916 | 3,654,333,360 | 23,763,674 | 3,366,166,298 | 8,598,806,208 | 0.425 | 0.391 |
| 52 | 35 | c6a.xlarge | 4 | 1,117,770 | yes/yes | 2,929,808,544 | 0.971 | 2,929,808,544 | 22,887,335 | 2,674,030,386 | 6,833,919,088 | 0.429 | 0.391 |
| 53 | 35 | c6a.xlarge | 5 | 575,820 | yes/yes | 2,946,449,264 | 0.958 | 2,946,449,264 | 22,754,642 | 2,700,119,250 | 6,833,919,088 | 0.431 | 0.395 |
| 54 | 35 | c6a.xlarge | 6 | 292,339 | yes/yes | 2,990,723,776 | 0.946 | 2,990,723,776 | 22,739,103 | 2,753,753,039 | 6,833,919,088 | 0.438 | 0.403 |
| 55 | 35 | c6a.xlarge | 7 | 147,303 | yes/yes | 3,083,498,568 | 0.936 | 3,083,498,568 | 22,746,630 | 2,829,210,183 | 6,833,919,088 | 0.451 | 0.414 |
| 56 | 35 | c6a.xlarge | 8 | 73,938 | no/yes | 3,229,877,168 | 0.922 | 3,229,877,168 | 22,778,462 | 2,937,168,449 | 6,833,919,088 | 0.473 | 0.430 |
| 57 | 35 | c6a.2xlarge | 4 | 2,257,260 | yes/yes | 2,925,596,384 | 0.971 | 2,925,596,384 | 22,747,324 | 2,664,633,764 | 6,044,972,448 | 0.484 | 0.441 |
| 58 | 35 | c6a.2xlarge | 5 | 1,162,831 | yes/yes | 2,929,487,296 | 0.958 | 2,929,487,296 | 22,206,488 | 2,651,459,009 | 6,044,972,448 | 0.485 | 0.439 |
| 59 | 35 | c6a.2xlarge | 6 | 590,360 | yes/yes | 2,945,716,872 | 0.947 | 2,945,716,872 | 22,162,727 | 2,685,407,548 | 6,044,972,448 | 0.487 | 0.444 |
| 60 | 35 | c6a.2xlarge | 7 | 297,468 | yes/yes | 2,989,449,080 | 0.939 | 2,989,449,080 | 22,166,021 | 2,749,306,427 | 6,044,972,448 | 0.495 | 0.455 |
| 61 | 35 | c6a.2xlarge | 8 | 149,312 | yes/yes | 3,081,615,296 | 0.932 | 3,081,615,296 | 22,186,393 | 2,831,845,303 | 6,044,972,448 | 0.510 | 0.468 |

ADR-4143 decision 1's derivation puts statement 33 at `bits` 7 on c6a.large,
5 on c6a.xlarge and 4 on c6a.2xlarge. Statements 19, 34 and 35 have their
own `T`, so this table covers every `bits` from 4 to 8 rather than assuming
those operating points.

### Verdict

`results.txt` lines 62 to 70 give every statement and box
`uncombined_below`, at every `bits`. Line 71 gives
`final_verdict='build decision 3 alone'`. The largest `unc_pred_B` relative
to its threshold is 0.510 (statement 35, c6a.2xlarge, `bits` 8, line 61).

The margin as a compression ratio: written raw, with no compression, the
uncombined bytes would exceed the threshold for statements 34 and 35 on
c6a.xlarge and c6a.2xlarge. The verdict therefore depends on the scratch
format compressing these rows. The ratio needed is `unc_raw_B /
threshold_B`, set against the lowest `unc_ratio` measured for that
statement:

| statement | unc_raw_B | needed on c6a.large / xlarge / 2xlarge | lowest measured `unc_ratio` |
|---|---|---|---|
| 19 | 4,336,120,455 | 0.93 / 1.21 / 1.32 | 4.142 |
| 34 | 11,438,655,233 | 1.33 / 1.67 / 1.89 | 3.197 |
| 35 | 12,238,635,209 | 1.42 / 1.79 / 2.02 | 3.349 |

The routing penalty is above 1 only for statement 19, at up to 1.431. Even
dividing statement 19's lowest ratio by that, 4.142 / 1.431 = 2.89 is
still above the 1.32 it needs.

### Findings against ADR-4143

- **Partial aggregate (premise holds).** Ravel's partial aggregate gives up
  once `groups / rows > 0.1`. It judges first on 8,192 rows, then
  cumulatively. In corpus order, statement 19 has 12,204 of its 12,206
  windows of 8,192 rows above 0.1 (results.txt line 3), and statements 34
  and 35 have 11,044 of 12,206 (lines 23 and 43; median 0.3801). A
  partition gives up within its first few windows, consistent with the ADR's
  "the partial aggregate of these statements gives up early".
- **The ADR's risk does not occur on these boxes.** The ADR warns that
  "without combining, the write-once route can write more bytes than the
  sort-merge it replaces". Uncombined routing comes to 0.265 to 0.510 of
  the M8 sort-merge bytes (table above). The raw bytes alone would exceed
  some thresholds (the compression table above). Compression at 3.2x to
  4.8x is what removes the risk.
- **Combining saves little.** On the sample slice, the same rows routed and
  then combined are compared with the same rows routed uncombined, using the
  same chunks (fields `sample_comb_ipc_B` and `sample_routed_ipc_B` on each
  `bits=` line):
  - For URL (statements 34 and 35), combining keeps 5.44M to 5.86M of 20M
    rows (`sample_comb_rows`), but the bytes fall only to 0.837 to 0.918 of
    routed uncombined.
  - For statement 19 it keeps 12.07M to 12.14M rows, and the bytes fall only
    to 0.971 to 0.979.

  lz4 already absorbs a repeated key within a chunk, so removing the repeat
  saves its compressed form, not its raw bytes. Had decision 2 missed,
  decision 4 would not have closed a gap of more than about 16%.
- **Combined bytes are biased high.** `comb_ipc_B` scales the sample slice's
  bytes per combined row. That slice is heavier than the corpus:
  - 139.91 against 114.64 raw B/row for URL (lines 23 and 43);
  - 47.52 against 43.86 for statement 19 (line 3).

  Scaling the sample's uncombined bytes per row the same way overstates the
  measured full-corpus figure by 1.27x to 1.29x for statement 19 and 1.35x
  to 1.38x for URL (`unc_sample_scaled_B / unc_ipc_B` on each line), so
  `comb / threshold` is likely overstated by a similar factor. The routing penalty
  is a ratio within the one slice, so this does not carry into
  `unc_pred_B`. This makes the combined column look worse than uncombined
  for statement 19. The like-for-like comparison is the previous bullet.
- **The string floor stops tuning earlier on c6a.large than decision 1
  says.** Decision 1 says the 1,024-row floor binds "only past about 31.8 GB
  on c6a.large". That figure is for the fixed-width floor: with
  `tune_limit` (predict.py line 195) a floor of 66,432 B gives
  31,770,204,160 B at `bits` 8. Decision 3's string floor charge (1,024 rows
  of fixed columns, views and validity, plus 64 KiB of key bytes) is larger:
  90,368 B for statement 34, 98,688 B for 35 and 102,912 B for 19. On c6a.large, no `bits` then satisfies both of
  decision 1's conditions once `T` passes:
  - 21,430,627,328 B for statement 34;
  - 20,331,588,608 B for 35;
  - 19,773,615,104 B for 19.

  All three limits are at `bits` 7 (results.txt lines 24, 44 and 4). Each
  extra bit doubles the floor area needed, while the area available grows
  by less. Past that `T`, decision 3 returns `chunk_too_small` and the
  retry cannot tune on c6a.large. Statement 33's measured `T` (12.65 to
  12.78 GB) is under every limit. The `T` of statements 19, 34 and 35 is not
  measured here. This contradicts no stated figure, since decision 1's 31.8
  GB is stated for the fixed-width floor, but decision 3 does not restate it
  for string routes.
- **`floor_fits_min=no`** appears at c6a.large `bits` 7 and 8 and c6a.xlarge
  `bits` 8. It means the smallest budget `bits` can be given is below the
  floor. Decision 1's second condition takes another bit in that case, or
  nothing fits (previous bullet). The bytes on those lines describe a chunk
  size the executor would not use, and they are reported as upper bounds
  only.

### Limits of the model

- **Corpus order and arrival order.** The final receives rows in the
  exchange's arrival order, which interleaves scan partitions. Corpus order
  keeps the corpus's locality, so compression may be lower in a real run.
  The compression table above gives the margin: the verdict holds while
  routed rows compress at least 2.02x for statement 35 on c6a.2xlarge.
- **Greedy byte-budget split.** Each chunk is filled to its byte budget.
  The executor's chunks hold at most `routed_chunk_rows` rows and `D` data
  bytes, fixed per derivation, and can seal earlier. More, smaller batches
  compress worse. The trend over `bits` in the table shows the size of that
  effect: going from `bits` 4 to 8 divides the budget by about 15 and
  raises `unc_ipc_B` by 1.05x to 1.24x (1.24x for statement 35 on
  c6a.large).
- **Hash identity.** Combined rows are counted by 64-bit DuckDB hash.
  Statements 34 and 35 have 18,342,015 distinct hashes for 18,342,019
  distinct URLs (lines 23 and 43), so `comb_rows` can be up to 4 rows low.
  The check `distinct_keys <= comb_rows` still passes everywhere.
- **Writer.** pyarrow (Arrow C++) rather than arrow-rs (see "Which bytes are
  counted").

## Claims audit

Every sentence above that asserts a figure or property, and what produces
it. Line numbers refer to `predict.py` and `results.txt` at this commit.

| claim | produced by |
|---|---|
| corpus has 99,997,497 rows; run used all of them, slices of 20M, sample rows 40M to 60M | results.txt line 1; predict.py line 704 refuses any other count |
| every row is written uncombined exactly once (`unc_rows=99997497` on every row of the table) | `unc_rows` on every `bits=` line; predict.py line 625 refuses otherwise |
| corpus read in order | predict.py lines 243 and 258 refuse rows out of `file_row_number` order |
| distinct keys 56,384,822 / 18,342,019 / 18,342,019 | `distinct_keys`, results.txt lines 3, 23, 43 (exact `GROUP BY ALL` count, predict.py line 437) |
| `distinct_keys <= comb_rows <= 99,997,497` | predict.py line 627 refuses otherwise |
| distinct hashes 18,342,015 for URL, so `comb_rows` up to 4 low | `distinct_hashes`, results.txt lines 23, 43 |
| combined counts sum to the slice's rows; combined chunks match routed chunks | predict.py lines 518 and 521 refuse otherwise |
| hand-built Utf8View arrays decode to their input strings | predict.py line 421 refuses otherwise |
| key and state types | results.txt lines 2, 22, 42 (`keys=`, `state=`); `m` Int32 is datafusion-functions 55.2.0 `date_part`, mirrored at predict.py line 91 |
| raw B/row 43.86 / 114.64 / 122.76 full, 47.52 / 139.91 / 148.03 sample | `raw_row_bytes_mean`, `sample_raw_row_bytes_mean`, results.txt lines 3, 23, 43 |
| `unc_raw_B` 4,336,120,455 / 11,438,655,233 / 12,238,635,209 | `unc_raw_B` on lines 7, 27, 47 (the same on every line of a statement); 11,438,655,233 / 99,997,497 = 114.39 B, the charge of line 23 less two validity bits |
| probe windows above 0.1, minima, medians | `probe_*` fields, results.txt lines 3, 23, 43; window computation predict.py line 429 |
| floor charges 102,912 / 90,368 / 98,688 B | `floor_charge_B`, results.txt lines 3, 23, 43; predict.py line 136 |
| string-floor tuning limits on c6a.large | `tunes_up_to_T_B`, results.txt lines 4, 24, 44; predict.py line 195 |
| a 66,432 B floor gives 31,770,204,160 B at `bits` 8 (reproducing decision 1's 31.8 GB) | `tune_limit(32585819, 66432)` from predict.py line 195, run by hand |
| every table column (budget, floor fits, bytes, penalty, rows, threshold) | the named field on the `bits=` line in the table's `line` column |
| `unc_ipc_B` formula | predict.py line 608 |
| routing penalty and `unc_pred_B` formula | predict.py lines 620 to 622 |
| `comb_ipc_B` formula | predict.py line 616 |
| sample scaling overstates by 1.27x to 1.29x (statement 19) and 1.35x to 1.38x (URL) | `unc_sample_scaled_B / unc_ipc_B` on lines 7 to 21, 27 to 41, 47 to 61; predict.py line 613; e.g. line 27: 4,057,545,917 / 2,947,155,656 = 1.377 |
| combined vs routed on the same slice: 0.837 to 0.918 (URL), 0.971 to 0.979 (statement 19); combined rows 5.44M to 5.86M and 12.07M to 12.14M | `sample_comb_ipc_B / sample_routed_ipc_B` and `sample_comb_rows` on lines 7 to 21, 27 to 41, 47 to 61 |
| compression ratios needed, lowest measured 4.142 / 3.197 / 3.349 | `unc_raw_B / threshold_B`; minimum `unc_ratio` over lines 7 to 21, 27 to 41, 47 to 61 |
| 1.05x to 1.24x from `bits` 4 to 8 | `unc_ipc_B` of each box's `bits` 8 line over its `bits` 4 line; 1.24x is line 51 / line 47 = 3,654,333,360 / 2,955,700,760, 1.05x is line 21 / line 17 |
| budget divided by about 15 from `bits` 4 to 8 | `budget_min_B` 479,203 on line 7 against 31,698 on line 11; predict.py line 182 |
| statement 33's `T` of 12.65 to 12.78 GB, decision 1's 31.8 GB, and the quoted risk and partial sentences | ADR-4143 lines 48, 112 to 118, 141 and 145 |
| verdicts per cell and final | results.txt lines 62 to 71; predict.py `verdicts`, lines 645 to 669 |
| thresholds and box shares | predict.py lines 68 and 75, copied from ADR-4143 decision 2 and ADR-3565 amendment 2 item 4 |
| lz4_frame compression, one stream per scratch file | predict.py line 367; framing added at lines 608, 616, 620 |
| the partial's probe is 8,192 rows at ratio 0.1, strict, re-judged cumulatively | `SKIP_PARTIAL_AGGREGATION_PROBE_ROWS` and `_RATIO` in ravel-sql's session module, mirrored at predict.py lines 84 and 85 |

# Report-only bench tools

Tools in `crates/ravel-bench` that measure a question and write a report. They
change nothing any crate writes or reads, and they have no pass/fail band of
their own beyond the consistency checks each one states.

## logbody_report

Issue #2977. It measures, over a corpus of log lines, three things a log body
format change would need numbers for before anyone builds it:

1. **Substring needles.** For each needle: the total block count, the blocks
   holding an exact byte-substring match (and the matching line count), the
   blocks the production word-token Bloom keeps when the needle is a single
   token (otherwise `"not applicable: not a single token"`), and the blocks a
   per-block trigram Bloom over the distinct byte trigrams of the block's
   bodies keeps at false-positive targets 1% and 0.1%. `trigram_sizing` gives
   that filter's bytes per block and its share of the compressed object bytes,
   and the false-positive keeps against the exact count. The trigram filter is
   built in the module (`logbody_report::trigram`); there is no production
   trigram index. `trigram_set_blocks` counts the blocks whose exact trigram
   set holds every needle trigram, which is what ADR-0105's exact postings
   (GRAM_IDX, reserved and not implemented) would keep.
2. **Body pages.** Per block: the body page PAGE_DIR records, the block-local
   page the writer computes (tag 1 or 7) and its decode throughput, and a
   template split encoded with the production encoders: the block's distinct
   templates as one string page, each row's template index as one integer
   page, and every variable in row order as one string page. The split rule is
   printed in the report as `body_encoding.template_rule` and is the `RULE`
   constant in `logbody_report::template`. The three pages' sum is compared
   with the block-local page and with PAGE_DIR's body pages plus their
   row-group dictionary pages. Every line is rebuilt from the decoded pages
   and compared byte for byte; `mismatch_count` must be 0.
3. **Integer pages.** For `ts`, `stream_ref`, `severity_num`, and `flags`: the
   PAGE_DIR page size against a FastLanes-style transposed bit-packed layout
   (1024-value vectors, 16 lanes, scalar code; frame-of-reference and
   lane-delta variants), with decode throughput for all three. A FastLanes
   page pads its last vector to 1024 values, so its throughput is given twice:
   over the page's real values, and over `padded_values`, the values it
   unpacks. The two differ only on a page that is not a multiple of 1024
   values, which is every page when `--block-records` is under 1024.
   `observed_ts` is listed under `excluded` when the writer stores it as a
   column reference.

Each record is one corpus line as its body, in one stream, with a strictly
increasing synthetic timestamp; the report's `stamp` states the exact rules.
Records are written with `RlogWriter` at the production block size unless
`--block-records` says otherwise, and read back with `RlogReader`; a body that
reads back different from its line fails the run.

### Running it

```sh
cargo run --release -p ravel-bench --bin logbody_report -- \
  --corpus HDFS.log.gz --needles needles.txt --out logbody-hdfs.json
```

`--corpus` takes plain text or gzip (by the `.gz` extension). `--needles` is
one needle per line, kept verbatim except the line ending; empty lines are
skipped. `--max-lines N` stops early. `--decode-runs` (default 5) sets how many
timed decodes each throughput figure takes the median of. `--block-records`
and `--object-records` override the block size and the records per object.
The binary writes the report even when it finds a problem (an empty section, a
template mismatch), then exits 1; it exits 0 only when every section has
figures and every body rebuilt. A run error (an unreadable input, a body or
integer page that reads back different from what was written) exits 1 without
a report.

The `stamp` section records the corpus path, the line count, the corpus
file's byte count and sha256 (of the file as stored, so of the compressed
bytes for `.gz`), the stored body bytes, the git sha (`GITHUB_SHA`, else `git
rev-parse HEAD` in the working directory, else `unknown`), the CPU model, the
block geometry, and the run date.

A small corpus and needle set live in `crates/ravel-bench/fixtures/logbody/`;
the module's tests run the report over them at 64 records per block.

### Corpora

The intended corpora are the LogHub datasets from
[logpai/loghub](https://github.com/logpai/loghub): HDFS, BGL, and Apache.
They are not checked in. Results are posted on issue #2977, each with its
report's `stamp`.

### REI arm (selected n-gram index)

Issue #3709 (ADR-3692 Plan row T11). With `--rei-needles` and
`--rei-corpus`, the report also measures a REI-style selected-gram block
index against the exact per-block trigram set, over the same blocks, and
prints a pre-registered go/no-go. The selection rule, the two arms, the
figures and the CPU clock are in the module doc of
`crates/ravel-bench/src/logbody/mod.rs`. The selected arm's grams are 3 to 6
byte substrings of the corpus's template needles, kept when at most 10% of
blocks hold them (`--rei-df-threshold`), dropping one a kept shorter
substring already matches block for block, rarest first under a byte cap of
5% of the compressed object bytes (`--rei-budget-share`); `--rei-max-gram`
sets the longest gram (3 to 7). The needles file pins all three per corpus;
a run passing any other value is refused before the corpus is read, and the
report's first line states the values it ran with. An exact residual scan
gives the true holders, and an arm that drops a holding block fails the run.

The needles and every band live in
`crates/ravel-bench/fixtures/logbody/rei_needles.toml`: per corpus (HDFS, BGL,
OpenSSH and Apache from LogHub), its block geometry, five template needles
and one id needle, each with the reason it was chosen. The id needle is the
negative control and is not part of the selection workload. Every figure is
printed once as `rei figure KEY = VALUE band [LOW, HIGH] ok|MISS`; a figure
that is missing, printed twice or outside its band exits 1, after the report
is written.

```sh
cargo run --release -p ravel-bench --bin logbody_report -- \
  --corpus HDFS.log --needles crates/ravel-bench/fixtures/logbody/needles.txt \
  --out hdfs.json --rei-needles crates/ravel-bench/fixtures/logbody/rei_needles.toml \
  --rei-corpus hdfs
cargo run --release -p ravel-bench --bin logbody_report -- \
  --rei-needles crates/ravel-bench/fixtures/logbody/rei_needles.toml \
  --rei-decide hdfs.json bgl.json ssh.json apache.json
```

`--block-records` defaults to the corpus's pre-registered value and a run
with any other value is refused. The per-corpus decision is GO when the
selected arm keeps at most 10% of blocks on at least 3 of the 5 template
needles, its index is at most 5% of the compressed bytes, and its build CPU
is at most twice zstd's over the same blocks. `--rei-decide` reads written
reports and prints the overall line: GO only when every pre-registered corpus
has exactly one report, that report is GO, and it has no band miss; it exits
0 only on GO. The 2026-11-04 run's output is committed as
`crates/ravel-bench/fixtures/logbody/rei_run_2026-11-04.txt`.

## string_fingerprint_report

Issue #3710 (ADR-3692 Plan row T12), the sibling experiment to the REI arm
above (T11): a report-only measurement of a learned, conservative per-block
string summary for equality and `LIKE '%literal%'` predicates on a declared
`Str` column, against the production bloom and `Contains` baselines, with a
pre-registered go/no-go. Nothing outside the report reads or writes the
summary; no production format changes. The hypothesis under test
(arXiv:2507.10391, "Instance-Optimized String Fingerprints") is that a
lightweight, per-workload secondary index can approximate equality and
substring predicates soundly (never a false negative) and generalize past the
workload it was built from.

### The summary

`string_fingerprint::summary` cuts every value of a block into byte grams of
`gram_len` bytes and maps every possible gram to one bit of a per-block
bitset. A gram learned from the training workload owns a bit; every other
gram hashes into a shared fallback region (`Layout::bit_of`,
`summary.rs:100`). A block sets the bit of every gram it holds, and a literal
is pruned on a block when any of its grams maps to a clear bit (`survives`,
`summary.rs:210`). A block holding a value equal to or containing the literal
holds every one of its grams, so the test has no false negative for any
learned set and fallback size (`summary.rs:12-19`); the proptest
`survives_is_exactly_the_subset_test` (`summary.rs:543`) pins that, and
`measure` refuses a summary or baseline that keeps fewer blocks than hold the
literal.

The learned set is chosen greedily: each distinct gram of the training
literals is ranked by the number of training literals holding it times the
number of blocks lacking it (`summary.rs:356-360`), and grams are learned in
that order while `fallback_min_bits` fallback bits still fit the byte budget.
The budget is `budget_share` of the column's zstd-compressed bytes; what the
learned table leaves is spent on whole bytes of bitset per block, so
`summary_bytes_share` is at most `budget_share` by construction and the
question the run answers is what false-positive rate that budget buys.

Relation to arXiv:2507.10391 ("Instance-Optimized String Fingerprints",
Stoian, Thürauf, Zimmerer, van Renen, Kipf). What was checked, on 2026-11-05:
the abstract and the arXiv HTML body. The abstract says only that
fingerprints are a lightweight secondary index approximating `LIKE`
predicates, optimized for a workload by mixed-integer optimization and
generalizing to unseen predicates. The rest of this paragraph comes from the
body: the evaluation attaches a fingerprint column, one fixed-width
fingerprint per row (section 1, "Applications"; a per-partition use is
named there and block-level "string zonemaps" are left to section 4,
Future Work); each character is assigned to exactly one bin, one bit per
bin (section 2, constraint 1b of section 2.1), over single characters, with
larger n-grams also left to section 4; the partition is chosen by a
mixed-integer program solved with Gurobi over a seen workload (sections 2.1
and 3); and a pattern is tested with the subset test, which cannot produce
a false negative (sections 1, 2 and 3.1). This summary keeps the subset test
and the partition of every possible gram into bits (each learned gram is a
bin of one, the hash partitions the rest), and departs in three ways: one
bitset per RLOG block rather than per row, `gram_len`-byte grams rather than
characters, and a greedy rank rather than a solver (`summary.rs:21-31`).

### Workload

Per subject, `string_fingerprint::workload::draw` draws, from the subject's
own values with the pre-registered seed and before the summary is built, a
training set and a disjoint evaluation set: per kind, `literals_per_kind`
literals in each set, half present and half absent
(`workload.rs:254-265`). Present equality literals are values of
`gram_len..=eq_max_len` bytes; absent ones are such a value with one ASCII
letter or digit replaced, kept only when no value equals it. Present `LIKE`
literals are substrings of `like_min_len..=like_max_len` bytes; absent ones
are perturbed the same way, kept only when no block contains them. Every
literal of a kind is distinct, so no evaluation literal was a training
literal; evaluation literals do share grams with training literals, which is
the generalization being tested. They come from the same column, so "unseen"
means a literal not used at build time, not a different distribution.

`seen/*` figures are over the training literals and `unseen/*` over the
evaluation literals, never pooled. Each false-positive share is the pooled
count of (literal, block) pairs where a block that does not hold the literal
is kept, over all such pairs.

### Baselines

Each subject is written as a declared `Str` attribute with `RlogConfig`
defaults (block 8192 records, zstd level 3) and read back through
`ravel_logseg`'s reader:

- Equality: `ScanStats::blocks_after_bloom` per evaluation equality literal
  (`string_fingerprint_report.rs:485`). The writer builds no postings for a
  declared attribute, so equality pruning is skip index plus bloom, and the
  bloom only tests a literal of at most 64 bytes (`BLOOM_EXACT_MAX`,
  `mod.rs:67`). `production/equals/*` covers every evaluation equality
  literal; `bloom/equals/*` and `summary/equals_bloom_testable/*` cover the
  ones the bloom can test, counted in `eval_equals_bloom_testable`.
- `LIKE`: `ScanStats::contains_cells_tested` from a full scan that selects the
  column (`ColumnSelection::fixed_only().with_attr`) and drains every block
  (`string_fingerprint_report.rs:496-518`), and again over only the blocks
  the summary keeps; the rows each scan returns must match the in-module
  truth. `summary/like/cells_tested_share` is the second over the first.

### Decision and saturation

GO per subject requires `unseen/equals/false_positive_share` and
`unseen/like/false_positive_share` both at most 0.10, `summary_bytes_share` at
most 0.01 and `build_cpu_over_zstd` at most 2 (`DECISION_RULE`, `mod.rs:76`).
Saturation is a separate verdict, not a NO-GO leg: the values over 1024 bytes,
cut into blocks the production way and summarized alone, are tested with the
evaluation `LIKE` literals; at most 0.30 false positives is body-eligible,
above it body-ineligible, and a subject with no such value is untested.
Overall GO needs every pre-registered subject GO. A figure outside its
pre-registered band prints `MISS` and does not change the decision; it does
change the exit code (below). Build and
zstd CPU are the report thread's `CLOCK_THREAD_CPUTIME_ID`, both on one
thread.

### Pre-registration and running it

`crates/ravel-bench/fixtures/string_fingerprint/subjects.toml` fixes, per
subject, every run parameter (gram length, budget share, fallback bits, zstd
level, block and object records, line count, seed, workload lengths) and a
prediction with a miss band, an expected-absent reason, or an unpredicted
reason for every figure; its header states the basis of each prediction. It
was committed alone before the first run of this design, and no band was
edited after it. The report prints the parameters on its `params` line and
refuses a parameter flag that differs from the file before it reads the
corpus (`refuse_param_drift`, `string_fingerprint_report.rs:171`), and
likewise a `--corpus-column` other than the subject's registered `column`.
Each run prints a stamp line: git HEAD, `uname -m`, nproc, memory, the
corpus file's name, sha256 and bytes, zstd level and CPU clock. `--decide`
prints a stamp line too: git HEAD and the name and sha256 of every report
file it read.

Where the commits live. The landing rewrites every commit's authorship and
folds the `wip:` commits, so no SHA of the run sequence exists on `main`.
The sequence is kept on origin at
`refs/heads/task/0626efcb-c224-4018-a9bf-558480a98866/result`, and every SHA
below is a commit on that branch: round 0 is 2eee496, the pre-registration
commit is 93ab7d8, and each run was recorded in its own commit after it
(529f286 Apache through 4064e8d Referer, then 048dc32 for `decide.txt`).
A run file's stamp names the commit on that branch the run was made from,
which is the commit before the one that records it: `apache_body.txt` says
93ab7d8, `openssh_body.txt` 529f286, and so on. The stamp in the current
`decide.txt` was written by a later fix round, which regenerated that file
from the committed reports to add the stamp; it names that round's commit,
on that round's task branch, not on the branch above.

Exit codes, also in `--help`:

| Code | Measure mode | `--decide` |
|---|---|---|
| 0 | report written, no structural problem, every figure in its band | overall GO |
| 1 | an error, or a structural problem (a missing, duplicate, unexpected or not-pre-registered figure; the report is still written) | a report could not be read, or the reports are not a decision: a subject missing or reported twice, a report with a problem, or a subject not pre-registered |
| 2 | report written, no structural problem, at least one `MISS` | not used |
| 3 | not used | every subject reported once without a problem, overall NO-GO |

`--decide` also prints each subject's miss and problem counts, one
`string_fingerprint decide subject=... misses=N problems=N` line after the
subject's decision line.

```sh
cargo build --release -p ravel-bench --features parquet-baseline --bin string_fingerprint_report
string_fingerprint_report --describe --corpus-lines HDFS.log
string_fingerprint_report --subject hdfs_body --corpus-lines HDFS.log --out hdfs_body.json
string_fingerprint_report --subject clickbench_url --corpus-parquet hits_0.parquet \
  --corpus-column URL --out clickbench_url.json
string_fingerprint_report --decide apache_body.json openssh_body.json bgl_body.json hdfs_body.json \
  clickbench_url.json clickbench_title.json clickbench_searchphrase.json clickbench_referer.json
```

Subjects: the four LogHub bodies (Apache, OpenSSH, BGL, HDFS from
[Zenodo record 8196385](https://zenodo.org/record/8196385), every line), and
the URL, Title, SearchPhrase and Referer columns ClickBench q21 to q24 filter,
from `hits_compatible/athena_partitioned/hits_0.parquet` (all 1,000,000
rows). Neither corpus is checked in; the sha256 of every file is in
`subjects.toml` and in each run's stamp line.

The ClickBench corpus changed between rounds. Round 0 read a
10,000,000-row prefix of the 14.8 GB `hits.parquet`, a scope reduction its
own pre-registration header records: a full column scan ran about an hour
at the throughput it measured. This round reads `hits_0.parquet`, the first
of the 100 partitions of the same dataset, whole. No commit or
pre-registration of this round states the reason for the switch; what the
record does show is that it trades a row prefix of a file for a whole,
separately published file whose sha256 pins every row read, at a tenth of
the rows. The consequences:

- 123 blocks per ClickBench subject instead of round 0's 1,221, so every
  ClickBench false-positive share is over 123 blocks per literal.
- Every ClickBench saturation arm rests on one block, because every value
  over 1024 bytes fits in one: URL has 36 such values, SearchPhrase 4,
  Referer 99 and Title 2 (`saturation/long_values`; `saturation/blocks` is 1
  on all four).
- Round 0's ClickBench figures are not comparable with these: a different
  file, a tenth of the rows and a tenth of the blocks, besides the redesign
  of the summary itself. The pre-registration took a tenth of round 0's
  compressed bytes as the basis of its byte and bit-count predictions, and
  on Title and SearchPhrase those missed low.

### 2026-11-05 run

Every run's output and JSON report are in
`crates/ravel-bench/fixtures/string_fingerprint/run_2026-11-05/`, one
`<subject>.txt` and `<subject>.json` each, with the `--decide` output in
`decide.txt`. Host: x86_64, 16 CPUs, 32 GB.

Result: **NO-GO overall, NO-GO on all eight subjects.** Every subject fails
the unseen `LIKE` bar and the CPU bar; the bytes bar holds by construction.

| Subject | unseen `=` FP | unseen `LIKE` FP | build CPU / zstd | bytes share | saturation |
|---|---|---|---|---|---|
| apache_body | 0.142 | 0.569 | 7.5 | 0.0100 | untested |
| openssh_body | 0.007 | 0.286 | 11.0 | 0.0100 | untested |
| bgl_body | 0.003 | 0.202 | 9.5 | 0.0100 | untested |
| hdfs_body | 0.058 | 0.259 | 9.2 | 0.0100 | untested |
| clickbench_url | 0.714 | 0.848 | 3.9 | 0.0100 | ineligible (0.364) |
| clickbench_title | 0.485 | 0.453 | 8.0 | 0.0100 | eligible (0.026) |
| clickbench_searchphrase | 0.639 | 0.698 | 3.0 | 0.0099 | ineligible (0.588) |
| clickbench_referer | 0.738 | 0.840 | 3.9 | 0.0100 | ineligible (0.556) |

Against the baselines on the same evaluation literals:

- Equality on the ClickBench columns and Apache, where all 40 evaluation
  literals are bloom-testable: the production bloom keeps 0.011 to 0.017 of
  non-holding blocks, the summary 0.142 (Apache) to 0.738 (Referer). The
  summary's share over the bloom's, per subject: Apache 12x, Title 29x,
  Referer 45x, URL 49x, SearchPhrase 56x. Equal false-positive rate was
  never reached, so no cost-at-equal-rate comparison could be made. The
  report does not measure the bloom's bytes, so `summary_bytes_share` is a
  size figure for the summary alone, not a size comparison with the bloom.
- Equality on OpenSSH, BGL and HDFS: the 64-byte cap on equality literals
  (`eq_max_len = 64`, the bloom's own limit) has one pre-registered
  exception. These three corpora have no line of 64 bytes or fewer
  (`describe` `at_most_64_bytes=0`), so they take `eq_max_len = 1024` and
  their equality literals are whole lines, from corpora in which almost
  every line is distinct (`distinct` equals `values` on OpenSSH; 4
  duplicates in 4,747,963 lines on BGL; 2,118 in 11,175,629 on HDFS). The
  evaluation equality literals are 78 to 149 bytes on OpenSSH, 126 to 271
  on BGL and 118 to 174 on HDFS. The bloom cannot test any of them, so
  production keeps every block (false-positive share 1.0), while the summary
  keeps 0.003 to 0.058: a long, near-unique line has grams most blocks lack.
  The summary fails the CPU bar on all three (9.2 to 11.0).
- `LIKE`: scanning only the summary's kept blocks tests 0.32 (BGL) to 0.94
  (Referer) of the cells the full `Contains` scan tests. The share is below
  1.0 on every subject, so the summary saves `Contains` work everywhere, not
  only on the LogHub equality literals above.

Saturation: every ClickBench saturation verdict rests on one block. The
values over 1024 bytes number 36 on URL, 4 on SearchPhrase, 99 on Referer
and 2 on Title, and each subject's fit in a single block. On URL,
SearchPhrase and Referer that block keeps 0.36 to 0.59 of non-holder pairs,
so the summary reads body-ineligible there; Title's 0.026 reads
body-eligible. One block per verdict is too small a sample to call the
summary body-eligible or body-ineligible in general. The LogHub corpora have
no value over 1024 bytes, so the run cannot answer the saturation question
for log bodies.

Prediction misses: 41, every one printed as `MISS` in the subject's run
file and listed under `checked.misses` in its JSON; no band moved after the
pre-registration commit. Each is the figure, its value and its band:

- apache_body (5): `build_cpu_over_zstd` 7.5 [1, 6]; `seen/equals` 0.035
  [0.1, 0.9]; `summary/equals_bloom_testable` 0.142 [0.3, 1]; `unseen/equals`
  0.142 [0.3, 1]; `unseen/equals_absent` 0.200 [0.4, 1].
- openssh_body (7): `build_cpu_over_zstd` 11.0 [1, 6]; `fallback_density`
  0.438 [0.5, 0.97]; `seen/equals` 0.0079 [0.1, 0.95]; `unseen/equals`
  0.0069 [0.6, 1]; `unseen/equals_absent` 0.0050 [0.6, 1]; `unseen/like`
  0.286 [0.5, 1]; `unseen/like_absent` 0.279 [0.5, 1].
- bgl_body (8): `build_cpu_over_zstd` 9.5 [1, 6]; `fallback_density` 0.222
  [0.4, 0.97]; `seen/equals` 0.0034 [0.1, 0.95];
  `summary/like/cells_tested_share` 0.319 [0.5, 1]; `unseen/equals` 0.0033
  [0.5, 1]; `unseen/equals_absent` 0.0014 [0.5, 1]; `unseen/like` 0.202
  [0.5, 1]; `unseen/like_absent` 0.107 [0.5, 1].
- hdfs_body (7): `build_cpu_over_zstd` 9.2 [1, 6]; `fallback_density` 0.195
  [0.2, 0.85]; `summary/like/cells_tested_share` 0.468 [0.6, 1];
  `unseen/equals` 0.058 [0.7, 1]; `unseen/equals_absent` 0.036 [0.7, 1];
  `unseen/like` 0.259 [0.5, 1]; `unseen/like_absent` 0.133 [0.5, 1].
- clickbench_url: none.
- clickbench_title (7): `bits_per_block` 6,616 [7,300, 13,600];
  `build_cpu_over_zstd` 8.0 [1, 6]; `compressed_bytes` 10,301,520
  [11,300,000, 21,000,000]; `summary/like/cells_tested_share` 0.754 [0.8, 1];
  `unseen/equals_absent` 0.297 [0.4, 1]; `unseen/like` 0.453 [0.5, 1];
  `unseen/like_absent` 0.437 [0.5, 1].
- clickbench_searchphrase (7): `bits_per_block` 408 [900, 2,100];
  `compressed_bytes` 677,428 [1,600,000, 3,100,000];
  `summary/equals_bloom_testable` 0.639 [0.7, 1]; `unseen/equals` 0.639
  [0.7, 1]; `unseen/equals_absent` 0.530 [0.7, 1]; `unseen/like` 0.698
  [0.7, 1]; `unseen/like_absent` 0.623 [0.7, 1].
- clickbench_referer: none.

Every miss is low except `build_cpu_over_zstd`, which was predicted 2.5 and
read 7.5 to 11.0 where it missed. On OpenSSH, BGL and HDFS the fallback
region was sparser than predicted and the false-positive shares came in
under their bands, `unseen/equals` at 1/87 of its band's floor on OpenSSH
and 1/150 on BGL. Title and SearchPhrase compressed to 0.64 and 0.28 of a tenth of
round 0's 10,000,000-row figure, the basis of their byte and bit-count
predictions.

Corrections to round 0 (2eee496 on the task branch named above), whose
figures this run replaces. Its commit
message described a summary built from "the column's frequent 3-grams"; its
vocabulary was drawn from the seen literals (`summary.rs:1-3` at 2eee496).
Its writeup said a production "exact-residual scan" gave the true holders;
its `Contains` baseline selected no column and drained no block, so it tested
no cell. This run computes the holders in-module and requires the production
scan to return the same rows. And it said the unseen measure "tests
generalization directly"; its unseen equality literals always survived by
construction, and its unseen `LIKE` literals were synthetic tokens (such as
`sfp_unseen_like_hdfs_zzqqxyz`), absent from the corpus as substrings, that
shared no 3-gram with the vocabulary, so the summary could not prune them.
Its unseen shares of 1.0 were fixed by design, not measured.

A small corpus and subject spec for the module's own tests live in
`crates/ravel-bench/fixtures/string_fingerprint/` (`corpus.txt`,
`test_subjects.toml`).

## structural_fetch_report and fulltext_sidecar_report

Issue #3711 (ADR-3692 work package 4, Plan row T13): two report-only
experiments with a pre-registered go/no-go each. Nothing outside
`ravel-bench` changes; both use the public APIs of `ravel-logseg`,
`ravel-query` and `ravel-object-store`, and no feature ships.

### Arm 1: structural decode for a journey row fetch

Lance (arXiv:2504.15247) stores small values in independently compressed
mini-blocks of a few KiB with a per-page index of where each starts, so a
point lookup reads one mini-block per column instead of a whole page. RLOG
compresses one page per block per column (8192 rows). The question: for 10
matched rows of a 10 MiB RLOG object whose block set is already known, does a
structural read cut wire bytes to 0.25x or less, with GETs at most 1.5x and a
wall clock no worse, at 30 ms per GET?

Today's side is the production path, run for real: `RlogWriter` writes the
object into a `MemoryStore` behind `InstrumentedStore` and a 30 ms per-GET
delay, `plan_segment_routed` plans with the known block set, and
`scan_accounted_with_tenant_subset_raw` scans every column.

The structural side is a FAITHFUL SIMULATION, not a real decode: no
structural reader exists. Every real page is decompressed, cut into
`ceil(stored / mini_block_bytes)` slices, and each slice recompressed on its
own at the writer's zstd level, so mini-block sizes are measured. A row is
placed in mini-block `floor(row * chunks / rows)` (`mini_block_of`,
`structural_fetch.rs:278`), exact for fixed-width columns and proportional
otherwise; dictionary pages are read whole. The needed ranges are coalesced
(`coalesce`, `structural_fetch.rs:290`: sort, then merge a range starting at
most `gap` bytes past the previous end) and issued as real ranged GETs
against the simulated layout in the same delayed store. The plan is the
production plan plus 4 bytes of index per mini-block. Decode CPU is
modelled as today's measured CPU times the structural decompressed bytes over
today's at the same gap; structural wall is measured GET time plus that CPU.
The latency model has no bandwidth term: a GET costs 30 ms whatever its size,
with at most 16 in flight.

Both sides are measured at four configurations, each with real GETs. The
unprefixed figures are the production default, both sides coalesced at
1,887,437 bytes (`DEFAULT_LOG_REQUEST_COST_BYTES`, the fetcher's effective
gap) on an L0 segment. Ten targets about 1.1 MB apart sit inside that gap,
so both sides span the targets there and `ratio/*` is a layout and
recompression ratio that decides nothing. `gap_64k/*`, `gap_256k/*` and
`gap_1m/*` are both sides at that gap on an L1 segment: today's side is the
production fetcher with `BlockRangeFetcher::with_coalesce_gap` and nothing
else changed, and the structural ranges are coalesced at the same gap. L1
because an L0 read is bridged to at most four runs
(`MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT`) before the coverage crossover is
checked, which spans the targets whatever the gap. `l0_64k/*` is that L0
case at 64 KiB, with the structural ranges bridged to the same four runs by
the same rule (`bound_runs`, a copy of the fetcher's crate-private rule),
reported and not counted. The decision is `gap_64k/ratio/*`.

### Arm 2: compressed full-text sidecar

LogCloud (PVLDB vol. 18, p. 2362) keeps a compressed full-text index (an
FM-index) beside each cold log object so a rare-token search reads a small
index and then only matching rows. This arm measures a different index
than LogCloud's FM-index, one that answers whole-token queries only where
an FM-index also answers substrings: a row-granular inverted index over the
production body tokenizer's tokens, in zstd blocks of 64 prefix-shared terms
with delta-coded row ordinals, plus a block index, the row map (rows per RLOG
block, `RowMap`, `fulltext_sidecar.rs:182`) and an 8-byte trailer. A probe is
one 64 KiB suffix GET and one term-block GET; a hit then fetches its rows
through the production `plan_segment_routed` for the named blocks. The build
and probe are real, not simulated.

The baselines are the production structures on the same objects: the BLOOM
section `RlogWriter` wrote, with its verdict through
`scan_accounted_with_tenant` and `HasWord(body)` over the full time window,
and a POSTINGS section from the production `encode_postings_section` over body
tokens to block indices (production builds no body postings; it is what a
sidecar would replace or stand beside). Every returned row on every path is
checked against the corpus. For a pure `HasWord` query (no `KeyEquals` arm,
no tier-2 routed block set) production narrows by BLOOM only at decode, over
a buffer that already holds every time-candidate block, so its scans move
the whole object whatever the bloom says (findings c and d below). Production
does have a bloom-guided route: on an object above the whole-object bound,
`LogSegmentFetcher::fetch_keyed_plan` places BLOOM and POSTINGS for the
content arms of a query that carries a `KeyEquals` arm or a tier-2 routed
set, and prunes blocks before any BLOCKS byte is read. Only the pure
`HasWord` query lacks it. The decision baseline is that route applied to the
pure `HasWord` query, simulated with real ranged GETs (`guided_rounds`,
`fulltext_sidecar.rs:726`): the production suffix probe, then PAGE_DIR and
BLOOM where the probe missed them, then the pages of the blocks the
production bloom kept, each round coalesced at the request cost. The two
production scans are reported beside it.

The 100 GB scale is extrapolated (`extrapolate`, `fulltext_sidecar.rs:509`):
`S = scale_raw_bytes / (raw_bytes / objects)`; per sampled token, cost =
`sum(measured cost of each object it occurs in) + (S - those objects) *
mean miss cost`; the ratio is the mean sidecar cost over the mean bloom cost
across the 30 tokens. A rare token (at most 3 rows) is assumed to stay that
rare at scale. Deletion: one row holding a term no other row holds is erased,
the object rewritten by the production writer, and the sidecar rebuilt (an
ordinal shift moves every later row, so it cannot be edited in place); the
erased term must be gone and the next row found at its new place.

### Pre-registration and running it

`crates/ravel-bench/fixtures/t13/subjects.toml` fixes every parameter and a
prediction with a band, or an unpredicted reason, for every figure of the
four subjects (HDFS and BGL from LogHub, Zenodo 8196385, one per arm each);
its header states the basis (a calibration on a generated HDFS-shaped corpus
scaled to the LogHub line lengths). It was committed alone (8a4ee91 on the
task branch) before any LogHub run, and no band was edited after it.
Addendum 1 (committed alone, 6fc2c49 on the fix-round branch, before any run
at the new gaps) moves the structural decision to `gap_64k/ratio/*`, adds the
`gap_*`, `l0_64k` and `today/data_over_object` figures with their bands,
and removes the planned-only `floor_gap/*`; its header says what moved and
why. No sidecar band moved. Each binary refuses a `--param` that differs
from the file before it reads the corpus, prints a stamp (git HEAD,
`uname -m`, nproc, memory, corpus name, sha256 and bytes, CPU clock), exits
0 in band, 2 on a band miss, 1 on a problem, and `--decide` exits 0 for GO
and 3 for NO-GO. `--decide` recomputes each report's decision and band
check from its saved figures and the current `subjects.toml`, and refuses a
report whose saved decision or check disagrees, so a JSON edited by hand or
written under an earlier registration cannot decide.

```sh
structural_fetch_report --subject hdfs_structural \
    --corpus-lines HDFS.log --out hdfs_structural.json
fulltext_sidecar_report --subject hdfs_sidecar \
    --corpus-lines HDFS.log --out hdfs_sidecar.json
structural_fetch_report --decide hdfs_structural.json bgl_structural.json
fulltext_sidecar_report --decide hdfs_sidecar.json bgl_sidecar.json
```

### 2026-11-06 run

Outputs, JSON reports and `--decide` output are in
`crates/ravel-bench/fixtures/t13/run_2026-11-06/`. Host: x86_64, 16 CPUs,
32 GB, release build.

Structural arm: **undecided; round 0's NO-GO is withdrawn.** Round 0 decided
on `ratio/*` at the production gap, where ten targets about 1.1 MB apart sit
inside the 1,887,437-byte gap and both sides span the targets by
construction, so that ratio measures layout and recompression, not the
structural read. Addendum 1 moves the decision to `gap_64k/ratio/*` on an L1
segment. The LogHub runs at the new gaps were not made: the fix-round
executor (aarch64, 4 CPUs, 8 GB) had no network route to Zenodo and no copy
of either corpus, so no `gap_*` or `l0_64k` figure exists for HDFS or BGL
and the arm has no decision. `structural_fetch_report --decide` over the
round 0 reports now refuses them, since they carry no `gap_64k` figure. The
pre-registered expectation is NO-GO on both subjects, decided by
`gap_64k/ratio/gets` (predicted 1.64 HDFS, 1.71 BGL: 22 to 23 structural
data ranges against about 13 today) and `gap_64k/ratio/wall` near 1.0, with
`gap_64k/ratio/bytes` passing narrowly (0.24, 0.23).

The round 0 figures below are the production gap on L0. The ratio rows are
reported as a layout and recompression ratio and decide nothing. Every
`today/plan_bytes` band starts at 262,144 bytes, the fixed suffix probe, so
it can only miss high.

| Figure | hdfs_structural | bgl_structural | Bar |
|---|---|---|---|
| object bytes, rows, blocks | 11,101,600, 540,672, 66 | 11,029,764, 573,440, 70 | |
| today GETs, bytes | 3, 10,647,076 | 2, 11,291,908 | |
| today after the 262,144-byte plan | 2 ranged GETs, 10,384,932 (0.935 of the object) | the whole object, 11,029,764, one GET | |
| today decode CPU, wall (ms) | 27.9, 121.2 | 75.8, 138.0 | |
| structural GETs, bytes | 2, 11,374,888 | 2, 10,025,669 | |
| structural decompressed bytes | 928,318 of 12,039,421 | 1,129,981 of 11,755,333 | |
| structural modelled CPU, wall (ms) | 2.15, 64.4 | 7.28, 69.6 | |
| ratio/bytes (layout ratio) | 1.068 | 0.888 | report |
| ratio/gets | 0.667 | 1.0 | report |
| ratio/wall | 0.532 | 0.504 | report |
| floor_gap ranges, bytes (planned only, no GET issued) | 22, 480,525 | 23, 437,519 | report |

Today's side restricted neither subject's read to the ten target blocks.
The plan is the ranged plan for the known blocks (both objects are above the
9,437,185-byte whole-object bound), but on L0 the fetcher bridges the block
reads to at most four runs, which spans the targets. On HDFS the bridged
read stayed ranged: two data GETs of 10,384,932 bytes. On BGL it crossed the
0.75 coverage crossover and read the whole object, re-reading the plan's
bytes. Why HDFS's bridged read, at 0.935 of the object, stayed under the
crossover was not established here. Round 0's report said the routed plan
"skipped the edge blocks"; it skipped about 7% of the object, not the
untargeted blocks.

Band misses, kept as measured: hdfs `today/bytes` (10,647,076 under
10,747,904) and `today/decode_cpu_ms` (27.9 under 30); bgl
`structural/layout_bytes` (9,756,688 under 10,485,760: BGL recompresses
smaller in 8 KiB mini-blocks), and with it `structural/bytes` and
`ratio/bytes` (0.888 under 0.9). The `floor_gap` figures are planned only,
no GET issued: at 64 KiB the structural plan would move 0.045x (HDFS) and
0.039x (BGL) of round 0's today bytes. They are superseded by the measured
`gap_64k/structural/*` figures, and their GET ratio is not a measured
figure.

The one measured small-gap comparison is the generated 32-block object the
exact-figure test pins (`pins_the_exact_figures_of_a_generated_object`, not
a LogHub subject, deciding nothing). At `gap_64k` on L1 today read 14 GETs,
3,742,501 bytes; the structural side 13 GETs, 673,618 bytes (262,144 plan,
6,016 index, 405,458 data): ratio/bytes 0.180, ratio/gets 0.929, GO on that
object. At the production gap it gives 0.989, the round 0 shape, and
`l0_64k` reads the whole object on today's side (2 GETs, 10,846,562 bytes)
against four bridged structural ranges (7,706,827 bytes).

The structural CPU model scales today's measured decode CPU at the same gap.
Where today reads the whole object (BGL above, and on the generated object
the production gap, `gap_1m` and `l0_64k`), its decode CPU is far above a
ranged read's (on the generated object, debug build, about 1,076 ms against
118 ms), so the modelled structural CPU there is inflated too. That does
not touch `gap_64k` on the generated object, where today's read is ranged,
and today's LogHub read at `gap_64k` is predicted ranged
(`gap_64k/today/data_over_object` band 0.11 to 0.25); a whole-object read
there misses that band.

Sidecar arm: **NO-GO overall, NO-GO on both subjects.** The rare-token bytes
bar holds against the bloom-guided route; the size bar fails on both, and the
build CPU bar on BGL.

| Figure | hdfs_sidecar | bgl_sidecar | Bar |
|---|---|---|---|
| objects, lines, raw bytes | 10, 5,242,880, 729,267,580 | 9, 4,718,592, 738,614,793 | |
| object bytes, bloom section | 81,283,745, 3,742,284 | 76,898,472, 8,677,394 | |
| postings bytes, terms | 9,816,814, 1,007,144 | 20,391,383, 4,632,997 | |
| sidecar bytes, entries | 42,354,324, 98,096,683 | 44,624,476, 109,681,304 | |
| sidecar/over_postings | 4.31 FAIL | 2.19 FAIL | <= 1.2 |
| sidecar over BLOOM section | 11.3 | 5.14 | report |
| sidecar over POSTINGS plus BLOOM | 3.12 | 1.54 | report |
| build sidecar, compaction CPU (ms) | 17,527, 14,854 | 32,748, 17,379 | |
| build/cpu_ratio | 1.18 ok | 1.88 FAIL | <= 1.5 |
| rewrite amplification | 1.52 | 1.58 | report |
| rare tokens, rows, candidates | 30, 55, 14,918 | 30, 51, 712,956 | |
| bloom kept, false positive blocks | 256, 226 | 269, 220 | |
| measured bloom-guided GETs, bytes | 947, 385,008,908 | 900, 548,689,582 | |
| measured sidecar GETs, bytes | 630, 283,205,558 | 685, 317,188,706 | |
| production default GETs, bytes | 300, 2,438,512,350 | 300, 2,314,818,480 | |
| crossover off GETs, bytes | 300, 2,438,512,350 | 330, 2,314,733,670 | |
| rare/bytes_ratio_100gb (vs bloom-guided) | 0.0978 ok | 0.0459 ok | <= 0.1 |
| vs production default path | 0.0140 | 0.0104 | report |
| rare/gets_ratio_100gb | 0.658 | 0.699 | report |
| delete rebuild CPU (ms), over compaction | 2,249, 1.50 | 3,075, 1.84 | costed |
| delete sidecar bytes over object | 0.529 | 0.596 | costed |
| delete residual terms (correctness check) | 0 ok | 0 ok | <= 0 |

Extrapolation inputs (both measured): HDFS `S = 1e11 / (729,267,580 / 10) =
1371.24`, mean miss bytes bloom-guided 1,162,157.9, sidecar 107,654.9,
production default 8,097,652.9, mean miss GETs 3.041 and 2.000, giving
1,594,808,053 bloom-guided and 155,984,207 sidecar bytes per token. BGL `S =
1e11 / (738,614,793 / 9) = 1218.50`, miss bytes 1,922,461.3, 80,171.5 and
8,513,742.2, miss GETs 3.120 and 2.178, giving 2,343,501,109 and 107,540,196.
The HDFS ratio sits just under its bar. Its bloom-guided miss cost,
1,162,157.9 bytes, is the 262,144-byte suffix probe, plus the BLOOM section
(374,228.4 bytes on the mean object, 3,742,284 over 10), plus 525,785.5
bytes of PAGE_DIR and kept pages. About 1,369 of the 1,371.24 objects are
misses per token, so the bar sits 34,965,983 bytes per token away: a
bloom-guided miss cost about 25.5 KB (2.2%) lower, or a sidecar miss cost
about 2.6 KB (2.4%) higher, flips the HDFS pass to a fail. BGL has more
room (0.0459). A sidecar hit fetches its rows
through the production routed plan, which reads the whole object below the
plan whole-object bound (about 8 MB per matching pair), while the bloom-guided
route reads only kept pages, so the comparison is conservative against the
sidecar; misses dominate at scale (at most `(1371.24 - 1) * 107,654.9`, about
147.5 of 156.0 MB per HDFS token, since every sampled token matches at least
one object).

Band misses, kept as measured: hdfs `rare/candidates` (14,918: HDFS holds
far fewer tokens on at most three rows than predicted), `rare/rows` (55),
`rare/sidecar_bytes` (283,205,558, the whole-object hit fetch above) and
`rare/bytes_ratio_measured` (0.736); bgl `rare/rows` (51),
`rare/sidecar_bytes` and `rare/bytes_ratio_measured` (0.578) for the same
reason, and `rare/default_path_gets` 300 and `rare/no_crossover_gets` 330
against 270. Those are 270 + 30 and 270 + 2 * 30, which fits exactly one of
the nine objects being above the plan whole-object bound, the case the
registration names. The run did not record per-object sizes, so that count
is inferred from the GET totals, not measured.

The size ratio `sidecar/over_postings` divides by a POSTINGS section built
for this experiment over every body token, with no cap. Production builds no
body postings, and its writer caps a POSTINGS section at 10,000 distinct
terms (`postings_max_distinct`), which every object here exceeds
(`postings/capped_objects` 10 of 10 and 9 of 9), so no production section
of that size exists to compare against. Against the BLOOM section production does write
for the body, the sidecar is 11.3x (HDFS) and 5.14x (BGL); against that
BLOOM section and the experiment's uncapped POSTINGS together, 3.12x and
1.54x.

`delete/residual_terms` (band [0, 0]) is a correctness check fixed by
construction, not a prediction: the sidecar is rebuilt from the rewritten
object, so a residual term means the build itself is wrong.
`delete/erased_only_terms` has a band floor of 0, a count's own floor, so it
can only miss high.

### Production findings to file as issues

These are production behaviours the measurement ran into; none is changed
here, and each is stated as narrowly as the evidence supports. Round 0
listed four "findings outside the experiments"; this list corrects them.

- (a) Calibration observation, not a defect. The log fetcher's coalescing
  gap is `request_cost_bytes.max(DEFAULT_LOG_COALESCE_GAP)`
  (`log_fetcher.rs:5798`), by design under ADR-0904: with the default
  request cost that is 1,887,437 bytes, so ranged reads bridge holes up to
  about 1.8 MiB and the 64 KiB `DEFAULT_LOG_COALESCE_GAP` is only a floor.
  A point lookup of ten blocks about 1.1 MB apart therefore reads one
  range spanning them. Whether 1,887,437 bytes is the right request cost
  for row fetches at 30 ms per GET is the open calibration question.
- (b) The plan's whole-object fallback is triggered by any content, stream
  or POSTINGS arm, not by `HasWord` alone. Above the plan whole-object
  bound such reads are ranged first and then go whole through the coverage
  crossover; below the bound the object is read whole directly. LogHub
  evidence: the HDFS sidecar objects (about 8.1 MB, below the bound) read
  whole on both the default and the crossover-off path, 300 GETs and
  2,438,512,350 bytes each, for 30 rare tokens over 10 objects.
- (c) The 0.75 coverage crossover (`DEFAULT_LOG_COVERAGE_THRESHOLD`) is
  decided after the numeric arms prune blocks and before BLOOM or POSTINGS
  is consulted. On a version-4 object coverage counts the projected page
  bytes, so "a rare-token `HasWord` reads the whole object" holds for an
  all-column or wide projection only; a narrow projection can stay ranged.
  LogHub evidence: the BGL structural object, all columns projected, was
  read whole after the plan (11,029,764 bytes in one GET).
- (d) "The bloom saves decode CPU, never bytes" holds only on the unkeyed
  path (a pure `HasWord`, no `KeyEquals` arm, no tier-2 routed set), where
  BLOOM and POSTINGS are resolved at decode over a buffer that already
  holds every time-candidate block. `fetch_keyed_plan` places BLOOM and
  POSTINGS before any BLOCKS byte and does save bytes. Separately, nothing
  prunes a whole object by token before its suffix is read: there is no
  object-level token pruning. LogHub evidence: on the BGL sidecar objects
  the default path moved 2,314,818,480 bytes and the crossover-off path
  2,314,733,670, 84,810 bytes less over 30 tokens and nine objects.

## Selective investigations (benchmarks/investigations)

Issue #3699 (ADR-3692 decision 8, T1). Not a `ravel-bench` report: a box
flow that generates a seeded spans and logs corpus, loads it with `ravel-cli
load`, folds it through the key-index state matrix (unfolded, folded
without leaves, folded with leaves, mixed, compaction in flight, erasure in
flight, and many parts over N ingest hours), and runs the statements of
`statements.toml` cold, each cold call after a `ravel-server` restart and
a page-cache drop (required on the S3 lane). Of the 46 statements, 43 are
`cold_only`; the other three (`spans_lookup_norange`, `spans_lookup_1h`,
`logs_trace_1h`) also run warm, repeated against the running server with no
restart.
`evaluate.py` judges every answer against the generator's oracle and every
figure against `benchmarks/investigations/bands.toml` (ADR-3692 Acceptance
rows 1 to 17), and prints pass, fail or incomplete; timing is judged on the
real-S3 lane only. `benchmarks/investigations/README.md`
covers both lanes, and `benchmarks/investigations/investigations.test.sh`,
run by CI's `doc-scripts` job, holds the harness's cases, including a
committed local-lane run under `fixtures/local-smoke/` that it re-evaluates
byte for byte.

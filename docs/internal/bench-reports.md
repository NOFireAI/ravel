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

Relation to arXiv:2507.10391 ("Instance-Optimized String Fingerprints"): the
paper keeps a fixed-width fingerprint per row, partitions single characters
into bins (one bit per bin) and picks the partition by mixed-integer
optimization over a seen workload; an unseen pattern is tested against the
same partition and the subset test keeps it sound. This summary keeps the
subset test and the partition of every possible gram into bits (each learned
gram is a bin of one, the hash partitions the rest), and departs in three
ways: one bitset per RLOG block rather than per row, `gram_len`-byte grams
rather than characters, and a greedy rank rather than a solver
(`summary.rs:21-31`).

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
pre-registered band prints `MISS` and does not change the decision. Build and
zstd CPU are the report thread's `CLOCK_THREAD_CPUTIME_ID`, both on one
thread.

### Pre-registration and running it

`crates/ravel-bench/fixtures/string_fingerprint/subjects.toml` fixes, per
subject, every run parameter (gram length, budget share, fallback bits, zstd
level, block and object records, line count, seed, workload lengths) and a
prediction with a miss band, an expected-absent reason, or an unpredicted
reason for every figure; its header states the basis of each prediction. It
was committed alone (93ab7d8) before the first run of this design, and no
band was edited after it. The report prints the parameters on its `params`
line and refuses a parameter flag that differs from the file before it reads
the corpus (`refuse_param_drift`, `string_fingerprint_report.rs:171`). Each
run prints a stamp line: git HEAD, `uname -m`, nproc, memory, the corpus
file's name, sha256 and bytes, zstd level and CPU clock.

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
rows). Round 0 used a 10,000,000-row prefix of `hits.parquet`; this run uses
the first partition whole, a smaller and different sample. Neither corpus is
checked in; the sha256 of every file is in `subjects.toml` and in each run's
stamp line.

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
  bloom is better by one to two orders of magnitude at these sizes; equal
  false-positive rate was never reached, so no cost-at-equal-rate comparison
  could be made.
- Equality on OpenSSH, BGL and HDFS, where no line is 64 bytes or fewer:
  production keeps every block (false-positive share 1.0) and the summary
  0.003 to 0.058. These literals are whole log lines of over 100 bytes, so
  most blocks lack at least one of their grams; this is the one place the
  summary beats production, and it fails the CPU bar there (9.2 to 11.0).
- `LIKE`: scanning only the summary's kept blocks tests 0.32 (BGL) to 0.94
  (Referer) of the cells the full `Contains` scan tests.

Saturation: on URL, SearchPhrase and Referer the long-value block keeps 0.36
to 0.59 of non-holder pairs, so the summary is body-ineligible there. Title's
0.026 comes from one block holding two values and is too small a sample to
call the summary body-eligible in general. The LogHub corpora have no value
over 1024 bytes, so the run cannot answer the saturation question for log
bodies.

Prediction misses (each printed as `MISS` in the run files; no band moved
after the pre-registration commit): `build_cpu_over_zstd` was predicted 2.5
and read 3.0 to 11.0, over its band [1, 6] on Apache, OpenSSH, BGL, HDFS and
Title. On OpenSSH, BGL and HDFS the fallback region was sparser than
predicted (0.44, 0.22, 0.20 against 0.8, 0.75, 0.5), and every unseen
false-positive share came in under its band, the equality ones by two orders
of magnitude on OpenSSH and BGL; on Apache only the equality shares missed
low. On ClickBench, Title and SearchPhrase compressed to 0.64 and 0.28 of a
tenth of round 0's 10,000,000-row figure, the basis of their byte and
bit-count predictions, which missed low; SearchPhrase's unseen shares (0.53
to 0.70) came in under bands starting at 0.7.

Corrections to round 0 (2eee496), whose figures this run replaces. Its commit
message described a summary built from "the column's frequent 3-grams"; its
vocabulary was drawn from the seen literals (`summary.rs:1-3` at 2eee496).
Its writeup said a production "exact-residual scan" gave the true holders;
its `Contains` baseline selected no column and drained no block, so it tested
no cell. This run computes the holders in-module and requires the production
scan to return the same rows. And it said the unseen measure "tests
generalization directly"; its unseen equality literals always survived by
construction and its unseen `LIKE` literals were byte-disjoint from the
corpus, so its unseen shares of 1.0 were fixed by design, not measured.

A small corpus and subject spec for the module's own tests live in
`crates/ravel-bench/fixtures/string_fingerprint/` (`corpus.txt`,
`test_subjects.toml`).

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

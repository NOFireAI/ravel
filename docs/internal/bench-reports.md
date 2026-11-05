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

The summary (`string_fingerprint::summary`) is a shared vocabulary of
features selected from a *seen* predicate workload (equality literals
verbatim; `LIKE` literals decomposed into fixed-length byte grams), with each
block storing only its own bitset over that vocabulary -- a different shape
than the REI arm's shared postings list, and conservative for any vocabulary
by construction (a block is never pruned unless `BlockSummary::build` found
the feature truly absent). An equality literal outside the vocabulary always
survives unconditionally: an equality fingerprint has no way to generalize to
a value it was never told about, so `unseen/equals/false_positive_share`
measures that ceiling directly, not a defect in one run's literal selection.
A `LIKE` literal can still be pruned when its grams happen to already be
vocabulary entries built from other, seen literals, which is the
generalization the hypothesis predicts and `unseen/like/false_positive_share`
tests directly.

Every subject (a corpus and a column) is written as a declared `Str`
attribute under one fixed column name, `value`, with `RlogConfig` defaults,
so the production bloom baseline (`ScanStats::blocks_after_bloom`) and
`Contains` baseline (`ScanStats::contains_cells_tested`) both apply
unmodified -- including for the LogHub body subjects, which would otherwise
have no production `Contains` baseline to compare against if written as the
record body instead. GO per subject requires
`unseen/equals/false_positive_share` and `unseen/like/false_positive_share`
both at most 0.10, `summary_bytes_share` at most 0.01, `build_cpu_over_zstd`
at most 2, and `saturation/false_positive_share` (the unseen `LIKE` measure
restricted to blocks holding a value over 1024 bytes) at most 0.30, or the
subject is body-ineligible (no block that long, reported as a vacuous pass);
overall GO requires every pre-registered subject to be GO. A production
exact-residual scan over the same blocks gives the true holders; the summary
keeping fewer blocks than that scan holds (a false negative) is a hard
error, never a softly reported figure.

```sh
cargo run --release -p ravel-bench --bin string_fingerprint_report -- \
  --subject hdfs_body --corpus-lines HDFS.log --out hdfs_body.json
cargo run --release -p ravel-bench --bin string_fingerprint_report --features parquet-baseline -- \
  --subject clickbench_url --corpus-parquet hits.parquet --corpus-column URL \
  --max-lines 10000000 --out clickbench_url.json
cargo run --release -p ravel-bench --bin string_fingerprint_report -- \
  --decide hdfs_body.json bgl_body.json openssh_body.json apache_body.json \
           clickbench_url.json clickbench_title.json clickbench_searchphrase.json clickbench_referer.json
```

The eight subjects, their corpora, parameters, and seen/unseen literal sets
(each with a stated reason) are pre-registered in
`crates/ravel-bench/fixtures/string_fingerprint/subjects.toml`: four LogHub
body subjects (HDFS, BGL, OpenSSH, Apache from
[Zenodo record 8196385](https://zenodo.org/record/8196385), full corpus) and
four ClickBench `hits_compatible` columns (URL, Title, SearchPhrase, Referer;
`https://datasets.clickhouse.com/hits_compatible/hits.parquet`, a
10,000,000-row prefix -- about 10% of the full ~100,000,000-row file, a
documented scope reduction from a full-corpus run given this tool's
per-command execution budget). Neither corpus is checked in.

The 2026-11-05 run's output (every figure, band, and decision line) is
committed as
`crates/ravel-bench/fixtures/string_fingerprint/string_fingerprint_run_2026-11-05.txt`.
Result: **NO-GO overall**, and NO-GO on every one of the eight subjects.
`unseen/equals/false_positive_share` was 1.0 on every subject, which the
module doc identifies as a structural ceiling of an equality fingerprint
rather than an implementation defect: no vocabulary built only from seen
literals can prune a block for an equality literal it has never seen.
`unseen/like/false_positive_share` was also 1.0 on every subject (the
pre-registered unseen `LIKE` literals were chosen to be byte-disjoint from
the corpus, so none of their grams could already be vocabulary entries from
other seen literals). The four ClickBench subjects additionally failed the
saturation leg at 1.0 against a 0.30 bar; the four LogHub subjects reported
it as a vacuous pass (no value in those corpora exceeds 1024 bytes).
`openssh_body` additionally failed the CPU bar (`build_cpu_over_zstd` =
11.91, bar 2). No subject came close on the bytes bar (`summary_bytes_share`
was at most 0.0003 against a 0.01 bar) or the seen-equals false-positive
share (0 on every subject).

A small corpus and subject spec for the module's own orchestration test live
in `crates/ravel-bench/fixtures/string_fingerprint/` (`corpus.txt`,
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

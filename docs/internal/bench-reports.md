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

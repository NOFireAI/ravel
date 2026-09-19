# ADR-0070 tier B bench baselines

This directory holds the committed criterion baselines that the tier B
regression compare (ADR-0070 decision 3) diffs a PR's bench run against. It is
the one path under `bench/` that is tracked; all other bench output stays
gitignored.

## What tier B compares

The pure-CPU, store-independent criterion set ADR-0070 names:

- `segment_encode` groups (crate `ravel-bench`)
- `series_id_hash` (`ravel-bench`)
- `merge_kway_vs_materialized` (`ravel-query`)
- `bytes_slice_vs_copy` (`ravel-query`)
- `logseg_encode` (`ravel-logseg`)
- `logseg_scan` (`ravel-logseg`)
- `otap decode` (`ravel-otap`)

Anything that touches object storage is not here; it belongs in the exact
byte/request gates (tier A), which are deterministic and hard-fail.

## How a baseline is produced and compared

```sh
# record a baseline (raise the sampling env knobs for a real reference run)
scripts/bench-tier-b.sh record bench/baselines/tier-b.json "<environment label>"

# compare a working tree to it (advisory: never fails)
scripts/bench-tier-b.sh compare bench/baselines/tier-b.json

# the same, enforcing: exits non-zero on a regression past +15%
scripts/bench-tier-b.sh compare bench/baselines/tier-b.json --enforce
```

The comparison reads criterion's `estimates.json` files, never criterion's
stdout, so it is immune to `CARGO_TERM_COLOR=always` injecting ANSI codes into
the numbers.

## Provenance is load-bearing

Each baseline file carries a `_meta.label` naming the environment it was
recorded on. A criterion timing number only means something relative to a
baseline taken on the same hardware under the same load. ADR-0070 states that
tier B runs, and the baseline is recorded, on the self-hosted reference runner.
A baseline recorded anywhere else is a demonstration of the machinery, not a
usable baseline, and its label must say so.

A hand-recorded baseline is only usable for a comparison once five things are
stamped into it, in the `_meta.label` text or a `_meta` field:

- **Host** -- the machine class the numbers were measured on (CPU, core
  count, whether the box is shared). A criterion number carries no meaning
  without this; see ADR-0070 decision 3.
- **Binary SHA** -- the commit the `cargo bench` binaries were built from.
  Without it, a regression against the baseline cannot be attributed to a
  code range.
- **Corpus** -- what data the bench set ran over. For tier B specifically
  this is n/a as a separate field: every named bench (`segment_encode`,
  `series_id_hash`, `logseg_encode`, `logseg_scan`, `otap decode`,
  `merge_kway_vs_materialized`, `bytes_slice_vs_copy`) generates its own
  synthetic input in-process at a cardinality fixed by the knobs below,
  there is no external or ingested corpus to name. A future bench added to
  this set that reads recorded or ingested data must stamp what it read.
- **Knobs** -- `BENCH_SAMPLE_SIZE`, `BENCH_WARMUP`, `BENCH_MEASURE`, and
  `RAVEL_BENCH_MAX_SERIES`, exactly as described below.
- **Flush cadence** -- n/a for tier B. Every bench in this set is pure-CPU
  and store-independent (that is the property that makes a timing
  comparison meaningful at all, per ADR-0070 decision 3); none of them
  touch an ingest or flush path. A bench that did would need to stamp it.

A baseline missing the fields that do apply to it (host, binary SHA, knobs)
is not usable for a comparison, the same way `bench-tier-b.sh compare`
already refuses a pair with no `_meta.knobs` (below). The committed
`tier-b.json` is exactly this case: it carries a host label but no binary
SHA, so treat it as a demonstration of the machinery only, never as a
regression reference.

The sampling knobs are load-bearing in the same way. `RAVEL_BENCH_MAX_SERIES`
is part of the `segment_encode` bench id, so a re-record at a different
cardinality renames that arm. The compare then reports it as MISSING on one
side and ignores an extra on the other, and the arm leaves the comparison
without failing anything. A re-record must use the same `BENCH_SAMPLE_SIZE`,
`BENCH_WARMUP`, `BENCH_MEASURE`, and `RAVEL_BENCH_MAX_SERIES` the
`bench-compare` workflow pins, or the workflow's pins must move with it.

`bench-tier-b.sh record` stamps them, so a baseline recorded through it is
checkable. `tier-b.json` as committed predates the stamping and carries no
`_meta.knobs`: the compare reports `NOT RECORDED` for it and an enforcing run
refuses the pair. That is deliberate. The knobs it was recorded at are not
fully recoverable from the file, and writing values nobody observed is the
drift this check exists to catch. Re-recording on the reference runner, which
this baseline needs anyway, fixes it.

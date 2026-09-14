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

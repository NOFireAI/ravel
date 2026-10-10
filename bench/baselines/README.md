# Bench baselines

This directory is the one path under `bench/` that is tracked; all other bench
output stays gitignored. It holds two kinds of baseline:

- `tier-b.json`: the criterion baseline that the tier B regression compare
  (ADR-0070 decision 3) diffs a bench run against. It was recorded on a fresh
  c6a.4xlarge (16 cores, x86_64, us-east-1) with `scripts/bench/fresh-box.sh`,
  at the commit and knobs its `_meta` stamps, and the instance was terminated
  after the run. Compare a run against it only when that run also comes from a
  fresh c6a.4xlarge at the same knobs; another host class measures the host,
  not the change.
- `s3-envelope.json`: the real-S3 envelope of the weekly `bench-s3` lane, the
  min and max of each compared figure over five of its runs. Summarised from
  the lane's own reports with `scripts/bench/s3-envelope.py` (below).

Both comparisons are advisory (ADR-0070 decision 3 as amended 2026-09-19).

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
baseline taken on the same hardware under the same load. This repository has
no self-hosted reference runner and will not get one, so a tier B baseline is
recorded on a fresh EC2 instance launched for the run and terminated after it,
not on a shared or long-lived box. The real-S3 envelope below is a different
kind of baseline: a range over runs of the `bench-s3` lane on a shared hosted
runner, comparable only to runs of that same lane.

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
`tier-b.json` carries all of them: its label names the instance type, cores,
architecture, binary commit and knobs, and `_meta.knobs` holds the four
sampling knobs.

The sampling knobs are load-bearing in the same way. `RAVEL_BENCH_MAX_SERIES`
is part of the `segment_encode` bench id, so a re-record at a different
cardinality renames that arm. The compare then reports it as MISSING on one
side and ignores an extra on the other, and the arm leaves the comparison
without failing anything. A re-record must use the same `BENCH_SAMPLE_SIZE`,
`BENCH_WARMUP`, `BENCH_MEASURE`, and `RAVEL_BENCH_MAX_SERIES` the
`bench-compare` workflow pins, or the workflow's pins must move with it.

`bench-tier-b.sh record` stamps them, so a baseline recorded through it is
checkable. The committed `tier-b.json` was recorded through it at
`BENCH_SAMPLE_SIZE=30`, `BENCH_WARMUP=2`, `BENCH_MEASURE=5` and
`RAVEL_BENCH_MAX_SERIES=2000`, the values the `bench-compare` workflow pins.

## Recording a tier B baseline on a fresh instance

`scripts/bench/fresh-box.sh` launches one EC2 instance, clones the repository
at the given commit, installs the toolchain from `rust-toolchain.toml`, runs
`scripts/bench-tier-b.sh record` with the five stamps above in the label, copies
the baseline back, and terminates the instance. Every input is a flag (or a
`FRESH_BOX_*` environment variable) with no default; `--help` lists them, and a
missing one exits 64 naming it before anything is launched.

```sh
scripts/bench/fresh-box.sh --dry-run \
  --instance-type c6a.4xlarge --ami ami-... --subnet subnet-... \
  --security-group sg-... --region eu-central-1 \
  --access ssh --key-name KEY --identity-file ~/.ssh/KEY.pem --ssh-user ubuntu \
  --volume-gb 120 --max-minutes 180 --repo-url https://github.com/OWNER/ravel.git \
  --commit <40-character sha> --out bench/baselines/tier-b.json \
  --sample-size 30 --warmup 2 --measure 5 --max-series 2000
```

`--dry-run` prints every `aws`, `ssh` and `scp` command and runs none; drop it
to launch. The label stamps the host from the instance's own `nproc`,
`uname -m` and instance type, the binary commit, "Corpus: none" (every tier B
bench generates its own input), the four knobs, and "No flush cadence:
store-independent". Use the knob values the `bench-compare` workflow pins, or
move its pins with the new baseline.

Each run generates one run id and prints it before launching. It is the
`run-instances` `--client-token`, which EC2 uses to treat a retried launch
call as the same launch rather than a second one, and the value of the instance tag
`ravel-fresh-box-run`. An EXIT trap terminates the id `run-instances` returned
together with every instance under that tag that is not terminated, which
finds the instance when the launch call failed after AWS accepted it or
printed no usable id, and then polls until each reports `shutting-down` or
`terminated`. When it cannot confirm that, or has no id and the tag lookup
fails, it prints `COULD NOT CONFIRM TERMINATION` with the ids or the run tag
and exits 70: terminate that instance by hand.

The trap cannot run when the launcher is killed with SIGKILL or the operator's
machine goes away. For that the instance's user-data runs
`shutdown -h +MAX_MINUTES` at boot, and the instance is launched with
`--instance-initiated-shutdown-behavior terminate`, so it terminates itself
`--max-minutes` after boot. That input is required: pick a value that covers
the cold build and the bench run, since the instance ends at that point
whether or not the run finished. It relies on the AMI's cloud-init running
user-data scripts.

The script avoids bash 4 features so that it runs under bash 3.2, the
`/bin/bash` of macOS.

The cases are in `scripts/bench/fresh-box.test.sh`, run with stub `aws`, `ssh`
and `scp` commands in CI's doc-scripts job. `FRESH_BOX_TEST_BASH=/bin/bash`
runs every case under that bash instead of the first `bash` on `PATH`.

## The real-S3 envelope

`s3-envelope.json` holds the `environment` of the lane's load point, one
summary entry per run under `runs`, the computed `envelope` (min and max per
figure), and `_meta`: the workflow, the run ids and dates, the `runs-on` label
the lane ran on, the region, the flush delay, the load point command line, and
an `unexplained` list of what the runs show that nobody has explained yet.

The weekly lane's "Compare against the committed real-S3 envelope" step runs
`scripts/bench-s3-compare.py` against it after every run. Latency figures warn
past the envelope max plus 25 percent; GET, LIST, the three billed attempt counts
(`*_attempts`, what S3 charges on) and both byte counts past the max plus 10
percent; PUT, accepted points and matched series must equal the
envelope exactly. A pair whose environment differs is refused as not a
comparison (exit 2). The step never fails the job; `--enforce` exits 1 on a
figure outside its band, for a local run.

To re-record it, take five runs of the lane at the same load point (five
scheduled runs, or dispatch it five times), download each run's
`bench-s3-report` artifact, and summarise them. Never edit the file by hand:
`scripts/test_bench_s3_compare.py` recomputes every min and max from `runs`
and fails on a disagreement.

```sh
scripts/bench/s3-envelope.py summarise --out bench/baselines/s3-envelope.json \
  --runs-on ubuntu-latest \
  --report RUN_ID:YYYY-MM-DD:path/to/report-s3.json \
  --unexplained "text, naming its issue"
scripts/bench/s3-envelope.py check bench/baselines/s3-envelope.json
```

Give `--report` once per run and `--unexplained` once per open item.
`--runs-on` is the `runs-on` value of `.github/workflows/bench-s3.yml`, verbatim;
a test asserts the two agree. Restate every open `unexplained` item: nothing is
carried over from the previous file. Then update the "Measured envelope" table
in `docs/guides/cost-model.md`, whose figures a test checks against this file.

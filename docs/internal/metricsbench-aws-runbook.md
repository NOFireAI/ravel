# Running MetricsBench on AWS

This page takes MetricsBench from an AWS account to a published reference
run: Ravel, Prometheus, VictoriaMetrics and Mimir replaying the same Remote
Write stream and answering the same PromQL corpus, on one EC2 box. Real S3
is the only lane whose Ravel figures are publishable (ADR-0927 decision 10).

What `run.sh` does on the box, step by step, is in
[deploy/metricsbench/README.md](../../deploy/metricsbench/README.md). That
page is normative for the harness. This page covers what surrounds it: the
account, the box, the commands in order, the outputs, and the failures
that look like something else.

The scripts are under
[deploy/metricsbench/aws/](../../deploy/metricsbench/aws/):

| Script | Runs on | What it does |
|---|---|---|
| `make-buckets.sh` | your machine | creates the four empty real-S3 buckets for a prefix, each with a policy granting only the instance role |
| `launch.sh` | your machine | uploads the source tarball and `box-run.sh`, then starts a self-terminating instance |
| `user-data.tpl` | the box, at boot | schedules the shutdown, installs the AWS CLI, fetches `box-run.sh` and checks its checksum |
| `box-run.sh` | the box | builds the harness at the pinned commit, runs `ci`, runs the profile, and uploads the outputs |

The same self-terminating box pattern runs the pinned-walk expiry
measurement for issue #3779:
[walk-expiry-aws-runbook.md](walk-expiry-aws-runbook.md).

## 1. Choose the instance

All four engines run at once, each capped at 8 GiB and 2 CPUs. That fairness
rule is the equal envelope in ADR-0927. Four 8 GiB engines need 32 GiB
before the harness, s3s-fs and the page cache, so use an m6a.4xlarge: 16
vCPU and 64 GiB.

A c6a.4xlarge has 32 GiB, which leaves no room. The reference run R4 was an
m6a.4xlarge in us-east-1. State the instance type with every figure: a
figure from another type is not like-for-like (#940).

`launch.sh` gives the box 150 GB of gp3, the size every run on #2821 used.

## 2. Create the results bucket and the instance role

1. **The results bucket.** One bucket, `MB_RESULTS_BUCKET`, receives each
   run's source tarball and box script, and then its logs and outputs under
   `metricsbench/<run-id>/`.
2. **The instance role.** It needs `s3:GetObject` and `s3:PutObject` on the
   results bucket. It needs nothing else in IAM:
   every run bucket grants it access through its own bucket policy
   (`make-buckets.sh`).
3. **The instance profile.** Wrap the role in an instance profile,
   `MB_INSTANCE_PROFILE`.

No credentials are stored anywhere. Ravel and Mimir reach S3 through the
instance role. On the box they run in containers, one network hop past the
instance, so `launch.sh` sets the metadata hop limit to 2. A box launched
with the default limit of 1 fails its first S3 call from a container.

The ravel-server images under `ghcr.io/nofireai/ravel-server` are public, so
the box needs no registry login.

## 3. Set the environment

On the machine you launch from:

```sh
export AWS_PROFILE=<profile> AWS_REGION=us-east-1
export MB_RESULTS_BUCKET=<results bucket>
export MB_SUBNET_ID=<subnet with outbound internet>
export MB_SECURITY_GROUP_ID=<security group allowing outbound traffic>
export MB_INSTANCE_PROFILE=<instance profile name>
export MB_INSTANCE_ROLE_ARN=arn:aws:iam::<account>:role/<role>
```

The subnet needs outbound internet for apt, rustup, crates.io and the image
registries. Put an S3 gateway endpoint on it if NAT traffic is billed:
Ravel's query phase fetches its objects from S3 again on every pass, because
its fetch cache is far smaller than the tenant.

## 4. Pin what you measure

Every run measures one Ravel image, by digest, and one harness commit:

```sh
git fetch origin main
H=$(git rev-parse origin/main)
gh workflow run publish-images.yml --ref main   # wait for it, then:
docker buildx imagetools inspect ghcr.io/nofireai/ravel-server:manual-$(git rev-parse --short=7 "$H")
```

Use the digest from the last command as
`ghcr.io/nofireai/ravel-server:manual-<sha7>@sha256:<digest>`. `launch.sh`
refuses an image without a digest.

Write the expected figures and their bands on the tracking issue **before**
launch, and link that comment from the readout. A result that gets its
expectation afterwards cannot surprise anyone.

## 5. Launch

**The real-S3 lane, for the publishable figures.** Make two fresh bucket
sets, one for the `ci` smoke and one for the run:

```sh
deploy/metricsbench/aws/make-buckets.sh mbr-<tag> mbr-<tag>-ci
deploy/metricsbench/aws/launch.sh "$H" "<image@digest>" "$H" 1793595495000 r1 mbr-<tag>
```

**The portable s3s-fs lane, for correctness on the same build:**

```sh
deploy/metricsbench/aws/launch.sh "$H" "<image@digest>" "$H" 1793595495000 f1
```

The arguments, in order:
1. the harness commit;
2. the Ravel image;
3. the Ravel source commit, which is the harness commit for a `manual-<sha>` image. A commit this checkout has is resolved to its full id; anything else must already be a full 40-character id, or `launch.sh` refuses it with exit 64 before uploading anything;
4. the anchor, in milliseconds;
5. a label;
6. the bucket prefix, real S3 only.

Reuse the anchor of the run you compare against: the stream is a pure
function of the anchor and the seed.

`launch.sh` prints the instance id, the run id and the results prefix.

**Optional environment:**
- `MB_PROFILE`, default `cardinality`;
- `MB_RSS_BAND`, one `--rss-band` value, default `prometheus:2147483648:6442450944`: the 2 to 6 GiB Prometheus band pre-registered for `cardinality` (#2821 comment 6590940753);
- `MB_OBJECTS_BAND`, default `14000:16500`: the data-object band pre-registered for `cardinality` (#2821 comment 6611923577);
- `MB_CAP_MIN`, the self-termination cap, default 2,160 minutes (36 h);
- `MB_INSTANCE_TYPE`;
- `MB_STAGE1`, `1` to run the ADR-3694 Stage 1 steps in both the ci smoke and
  the measured run (`cardinality` or `ci` only; `launch.sh` refuses another
  profile, and any value other than `1`, `0` or unset, with exit 64). The
  steps, the records and the band check are in
  [deploy/metricsbench/README.md](../../deploy/metricsbench/README.md),
  "Stage 1". After each `run.sh`, the box runs `stage1-check.sh` on its
  outputs: the ci smoke against `deploy/metricsbench/stage1-bands.ci.json`,
  the measured run against `MB_STAGE1_BANDS`;
- `MB_STAGE1_BANDS`, the measured run's Stage 1 band file, a path relative to
  the repository that must exist at the harness commit. Required with
  `MB_STAGE1=1` for every profile but `ci`, which defaults to
  `deploy/metricsbench/stage1-bands.ci.json`; `launch.sh` refuses it unset
  where it is required, a path that is not a file at the harness commit, and
  the variable set without `MB_STAGE1=1`, with exit 64;
- `MB_STAGE1_ARMS`, `MB_CONTROL_IMAGE` and `MB_CONTROL_SOURCE_COMMIT`, the
  Stage 1 arms of issue #3969 (`run.sh`'s `MB_STAGE1_ARMS`, a comma list of
  `extra-hour`, `l1` and `erasure`, and the control image, digest-pinned,
  with the commit it was built from). They reach the measured run only, not
  the ci smoke, and the box pulls the control image beside the main one;
  `launch.sh` refuses any of them without `MB_STAGE1=1`, a control image
  without `@sha256:` and one without its commit, with exit 64. The
  extra-hour arm waits until its hour lands above the routed watermark and
  the l1 arm until the newest ingest hour seals, each up to about an hour
  (predicted, not measured); the erasure arm first waits until every ingest
  hour in its scope seals (the last one starts within 5 min after the
  submit; its end plus 1 h 5 min, so up to about 2 h 10 min after the
  submit), then for its rewrite, at most 120 polls 15 s apart. It ends with
  exit 70 when the maintain container is found dead, either 60 s after it
  starts, before the seal wait, or at a poll (run.test.sh case zm). The
  rewrite runs in a one-off `ravel-server --mode maintain` container, started
  right after the pending pass; no ravel-server flag turns compaction off, so
  each of its ticks also runs retention, compaction and the GC and catalog
  sweeps over the tenant. The nightly workflow sets none of them.

Both bands default only for `cardinality`. Another profile's count and memory
differ, so `launch.sh` refuses a different `MB_PROFILE` unless both bands
are set, and chosen in that run's pre-registration.

A `cardinality` run takes 8 to 10 h. R4 took 8.7 h, F4 9.25 h.

## 6. Watch

The box uploads `status.txt` and `box.log` to
`s3://$MB_RESULTS_BUCKET/metricsbench/<run-id>/` at each stage, and a live
copy of the box log and container stats every 10 minutes:

```sh
aws s3 cp s3://$MB_RESULTS_BUCKET/metricsbench/<run-id>/status.txt -
aws s3 cp s3://$MB_RESULTS_BUCKET/metricsbench/<run-id>/box.log - | grep -E '^(=== |metricsbench-run: )'
aws s3 cp s3://$MB_RESULTS_BUCKET/metricsbench/<run-id>/cpu.log - | tail -1
```

`status.txt` gains these lines in order:
- `build exit=0`;
- `ci exit=0`;
- `<profile>-start exit=0`;
- `<profile> exit=N`, where N is `run.sh`'s exit code;
- `DONE exit=N`, carrying the same code (20 after a failed `ci`).

With `MB_STAGE1=1`, `ci-stage1-check exit=N` follows `ci exit=0` and
`<profile>-stage1-check exit=N` follows `<profile> exit=N`, N being
`stage1-check.sh`'s exit code; its output is uploaded as
`out-ci/stage1-check.txt` and `out-run/stage1-check.txt`, after the run's
outputs. A failed ci check ends the box with `DONE exit=22`; a failed
measured check ends it with `DONE exit=23` when `run.sh` exited 0, and with
`run.sh`'s code otherwise. A box that finds no band file for the measured
profile ends with `DONE exit=21` before it builds anything.

The box terminates itself 30 minutes after `DONE` with 20, 22 or 23, 2
minutes after a setup failure (`DONE` with 10 to 19, or 21), 5 minutes after
any other `DONE`, or at the cap. Nothing on it depends on the
machine that launched it, so an expired session or a closed laptop does not
stop a run.

The ingest step writes `ingest.json` only when it ends, after all four
engines have ingested and been read back. That is about 3 h into a
`cardinality` run; R4's ended 2.9 h in. Until then, `cpu.log` shows which engine is working.

## 7. Read the outputs

The run's outputs land under `out-run/` (and `out-ci/` for the smoke), as
listed in deploy/metricsbench/README.md:
- `report.json`, `stamps.json`, `provenance.json` and `ingest.json`;
- `memory-summary.json` and `ravel-objects.json`;
- `readback-folded.json` and `query.log`;
- `diagnostics.json`, the largest ULP gap per entry against the oracle;
- the effective configuration and the step logs.

Read the run's own verdict before any figure:
- `stamps.json` `status` is `passed`;
- `diagnostic` is `false`;
- `deferred_failures` and `failed_after_ingest` are empty;
- `store_kind` is `s3` for a publishable run.

The exit codes are in the README's table. The two seen most often:
- **70** is a failed step. After a failed ingest read-back, the run queries
  as a diagnostic and `validate` refuses the report.
- **75** is a memory-band breach or an OOM-killed container. The folded
  steps still ran, so the Ravel figures are complete.

Count Ravel's data objects on the basis the band uses: `l0` `.rseg` keys
only (`ravel_objects.data`), not the whole bucket. The bucket also holds a
commit record per object and the qualify probes, so its total is about
twice the data count.

## 8. Render and publish

Render the report with the harness's own renderer, built at the run's
commit:

```sh
cargo build --release --locked -p ravel-bench --bin metricsbench_report
target/release/metricsbench_report render --report out-run/report.json > render.txt
```

It validates the report first and refuses a failed or diagnostic one. Its
`SKIP` lines are bands the run did not pre-register.

Publish a reference run as a GitHub release asset, at a tag on the run's
commit that no workflow reacts to (`publish-images.yml` reacts only to
`v<major>.<minor>.<patch>`):
- a tarball of the run's prefix, with a `SHA256SUMS` over every file;
- the tarball's own checksum;
- the rendered table;
- release notes stating the run id, the commit, the image digest, the
  instance, the buckets, the denominators, and what the run does not cover.

Mark it a pre-release, so the software release stays the latest. Scan the
files for credentials before uploading: every secret-named field in the
effective configuration should be empty on a real-S3 run.

The first reference run is
[`metricsbench-r4-20261103`](https://github.com/NOFireAI/ravel/releases/tag/metricsbench-r4-20261103).

## 9. Clean up

Run buckets keep their data after the box terminates, and `run.sh` refuses
a non-empty one. So a rerun needs a fresh prefix, and old buckets need
deleting by hand:

```sh
aws s3 rb s3://<bucket> --force
```

Keep a reference run's buckets for as long as its figures are cited.

## When something fails

| What you see | What it is |
|---|---|
| `ci exit=1` and `service "createbuckets" didn't complete successfully` on a real-S3 box | A run bucket already holds objects. Make a fresh prefix. |
| The first container S3 call fails on a new box | The metadata hop limit is 1. `launch.sh` sets it to 2; a box launched another way needs `HttpPutResponseHopLimit=2`. |
| Mimir's read-back answers 500, `grpc: trying to send message larger than max` | Mimir's gRPC ceilings are below the 1,000,000-series listing. They are 512 MiB on main since #3743; an older harness has 100 MiB. |
| Ravel's read-back answers 504 at 120 s on real S3 | Fetch concurrency is the bound. `ravel.yml` sets `--fetch-concurrency 32` since #3758 (ADR-0927 amendment 2026-11-03); at the derived 8 the listing takes about 270 s. |
| `fetch memory exhausted: host available memory ... below the ... byte floor` on several statements | Memory outside the budget reached the host floor. #3747 fixed the sample path; any return is a regression. |
| Exit 75 with `metricsbench-mimir-1 was OOM-killed` | Mimir outgrew its 8 GiB envelope. R3 and F4 hit this; it is a comparator result, and the Ravel figures are still complete. |
| `query.log` mismatch lines cut off before both values | The log truncates long answers. Use `diagnostics.json` for the per-entry ULP gap. |
| `harness_commit = unavailable: ... not inside a git checkout` in the render | Expected: the box builds from a `git archive` tarball. The commit is in `provenance.json` and in the run id. |
| A watcher reports the instance unreadable | Usually expired AWS credentials on your machine. The box is unaffected and keeps uploading. |

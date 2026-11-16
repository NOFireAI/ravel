# Running the ClickBench benchmark on GCP

This page runs ClickBench against a published Ravel release on one GCE
instance, with a GCS bucket as the store through Ravel's native GCS backend
(`--store gcs`, [ADR-3456](../adrs/3456-native-gcs-object-store.md)). It is
the GCP counterpart of
[clickbench-aws-runbook.md](clickbench-aws-runbook.md), and it uses the same
box script, `benchmarks/clickbench/kit/box/run.sh`, switched by cloud.

Use the native GCS store only. Ravel's S3 path cannot run against GCS's
S3-compatible XML API: GCS ignores the S3 conditional headers, so
create-if-absent and compare-and-swap do not exist there, and `store qualify`
refuses such a bucket.

## What differs from the AWS kit

| | AWS kit (`launch.sh`) | GCP path (`launch-gcp.sh`) |
|---|---|---|
| Arms | two, A and B, judged against each other | one, A, judged against a pre-registration |
| Binaries | built from each arm's SHA on the box (frame pointers, no LTO) | the release's published `ravel-server` and `ravel-cli`, checked against pinned digests |
| Store | loopback RustFS on the box | a fresh GCS bucket per run, `--store gcs --gcs-bucket` |
| Fetch policy the server resolves | latency-first (`default-loopback`) | cost-based (`default`) |
| Credentials | the instance profile | the attached service account, through the metadata server; no keys |
| Bands | `bands/rlog.toml`, judged on the box and by `fetch.sh` | the run's `prereg.toml`, judged by `fetch-gcp.sh` |
| Result | two `results.json` | `results.json` and one ClickBench results file, `<machine>.json` |

The load geometry, the phases, the cold and hot protocol (a server restart and
a page cache drop before every cold try) and the concurrent rounds are the
same as the AWS kit's. That is the kit lane, not the ClickBench entry's lane:
the entry loads with another geometry, a bloom scope and a clustering key, so
its object count and data size are not this run's (issue #3087).

## 1. One-time setup

You need `gcloud` signed in to an account that can create service accounts,
buckets and instances in the project, `cosign` on the laptop to verify the
release, and the release tag fetched into your checkout
(`git fetch origin tag v0.32.0`).

```sh
benchmarks/clickbench/kit/setup-gcp.sh benchmarks/clickbench/kit/runs/gcp-v0.32.0/run.toml
```

It is safe to run again. It creates the service account named by
`service_account` if it is missing (it never creates a key for it), creates
the long-lived results bucket `bucket` (regional, Standard, uniform access, no
versioning, soft delete off), and grants the account
`roles/storage.objectAdmin` on that bucket. It grants nothing at the project
level. Pass `--dry-run` to print the changes first.

## 2. Write the run file and the pre-registration

Copy `benchmarks/clickbench/kit/run.gcp.example.toml`; every key is described
there. `runs/gcp-v0.32.0/run.toml` is the file the v0.32.0 run used. Take the
two digests from the release's `SHA256SUMS`. The `perf` phase is refused on
GCP at `check-run`, before a box exists, because the box installs no perf.

The run file names a `prereg.toml`. It holds one `[figures]` table with every
figure `kit.py prereg` judges, each an exact value or an inclusive
`[low, high]` band: the rows written and `COUNT(*)`, statements answered and
null tries, data size, l0 object count, load time, cold and hot sums,
concurrent QPS and error ratio, and the state stamps (store, bucket identity,
fold precondition, fetch policy, release tag and digests, machine). Post it on
the tracking issue before you launch. `launch-gcp.sh` refuses a file that
leaves a figure out, and ships the file in the kit, so the run is judged by
the bands that were posted.

## 3. Launch

```sh
benchmarks/clickbench/kit/launch-gcp.sh --dry-run benchmarks/clickbench/kit/runs/gcp-v0.32.0/run.toml
benchmarks/clickbench/kit/launch-gcp.sh benchmarks/clickbench/kit/runs/gcp-v0.32.0/run.toml
```

Read every command the dry run prints. The real launch:

1. checks that the tag resolves to `sha_a` in your checkout and that the
   mapping is the tag's;
2. downloads the release's `SHA256SUMS`, `SHA256SUMS.sig` and
   `SHA256SUMS.pem`, verifies the signature with `cosign verify-blob` against
   the release workflow's identity for the tag, and checks that it lists the
   two pinned digests;
3. asks GCE for the architecture of `instance_type` and refuses a failed
   lookup and any reported value but `X86_64`, because only the linux-amd64
   release artifacts are pinned (arm64 machine types are refused). GCE
   reports no architecture for x86 types, so an empty answer passes;
4. checks that the service account exists and holds objectAdmin on the
   results bucket;
5. uploads `kit.tgz`, `kit.sha256` and the resolved `run.toml` to
   `gs://<bucket>/<prefix>/runs/<run-id>/in/`;
6. creates the store bucket `<store_bucket_prefix>-<suffix>`, where the
   suffix is the run id without its `clickbench-` head, lowercased (at most
   63 characters in all), and grants the
   account objectAdmin on it;
7. creates the instance and grants the account
   `roles/compute.instanceAdmin.v1` on that one instance, so the box can delete
   itself.

If step 6 or 7 fails, it deletes what it created. It prints the run id, the
instance, the store bucket and the commands for the next two steps, and exits.

### How the box ends

The box is gone within 600 minutes in three ways:

- It deletes itself through the Compute API at the end of the run, after
  uploading `DONE` or `FAILED`.
- GCE deletes it at 590 minutes (`--max-run-duration 35400s
  --instance-termination-action DELETE`).
- Its startup script arms `shutdown -h +600`. A guest shutdown only stops a
  GCE instance, and a stopped instance still bills its disk, so this is the
  last layer, not the first. `teardown-gcp.sh` deletes a stopped instance.

The startup script refuses to run twice. If the instance reboots, it uploads
`FAILED step=user-data reboot` and deletes the instance instead of measuring
again on a half-used box.

### What the box does

`box/run.sh` runs the same steps as on AWS, with these differences:

- step `release` downloads the two binaries and `SHA256SUMS` and fails the box
  on any digest mismatch, in place of the toolchain, clone and build steps;
- step `store` creates the box's tenant token only: there is no store
  process, and no S3 variable is set anywhere, because `--store gcs` refuses
  one given through the environment (ADR-3456 D6);
- `store qualify` runs against the bucket, and its output is kept as
  `qualify.out` in the evidence;
- step `sql-smoke` starts the server with the run's flags and sends
  `SELECT 1` before the load, so a binary without the SQL surface fails
  there;
- after `declare`, step `declared` checks that `typed-attr-column show`
  reports as many columns as `kit.py mapping-columns` counts: the mapping
  entries that are not f64, which is what `typed-attr-column set` declares
  (104 for `hits.mapping.toml`). Step `footprint`
  lists the bucket and stamps its objects and bytes by key class;
- the server runs without an `LD_PRELOAD`, as the release ships;
- uploads, listings and the self-delete use the metadata server's token with
  curl, so the image needs no `gcloud`.

## 4. Collect and judge

```sh
benchmarks/clickbench/kit/fetch-gcp.sh --run-toml benchmarks/clickbench/kit/runs/gcp-v0.32.0/run.toml <run-id>
```

It polls `out/A/` for `DONE` or `FAILED`, and exits 6 if the instance is gone
with neither. Where background commands are stopped after about ten minutes,
run it with `--timeout 540` under a relaunch loop; exit 3 means the timeout
elapsed and it is safe to start again.

On `DONE` it downloads the results under `clickbench-runs/<run-id>/`, writes
`<machine>.json` in ClickBench's results format, and runs `kit.py prereg`
against the pre-registration from the run's own `kit.tgz`. It exits 0 when
every figure is inside its band, 1 on a miss, and 2 when a figure is missing
or emitted twice, or the box wrote `FAILED`.

The bases of the ClickBench results file:

- `result`: each statement's three tries in seconds, try 1 cold and tries 2
  and 3 hot, `null` for a try that did not answer;
- `load_time`: the seconds of the load, compact, fold and declare steps, from
  `progress.log`. The listings between them are not counted. The entry's
  figure is its whole `./load`, which has no compaction step;
- `data_size`: the bytes of every key under the tenant's `t/<hash>/` prefix
  after the declare step, which is the entry's `data-size` basis;
- `concurrent_qps` and `concurrent_error_ratio`: the kit's pooled figures over
  its concurrent rounds (the mean of the rounds' QPS; all rounds' failed calls
  over all their calls). Upstream measures one 600 s window.

## 5. Tear down

```sh
benchmarks/clickbench/kit/teardown-gcp.sh benchmarks/clickbench/kit/runs/gcp-v0.32.0/run.toml <run-id>
```

It deletes every instance carrying the run's label, in any status, and the
store bucket with its objects. It keeps the results bucket and the service
account. It then lists the run's instances and checks the bucket again, and
exits 1 if anything remains. Quote its output in the readout.

## Tests

`benchmarks/clickbench/kit/kit.test.sh` covers the GCP path with stubs for
`gcloud`, `curl` (the metadata server, the GCS and Compute JSON APIs and the
release downloads), `cosign` and `systemd-run`. No case reaches GCP.

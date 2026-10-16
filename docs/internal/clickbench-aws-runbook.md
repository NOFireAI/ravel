# Running the ClickBench benchmark on AWS

A start-to-finish runbook for standing up the AWS environment, loading the
`hits` corpus into Ravel, and running a measured ClickBench pass.

[clickbench.md](clickbench.md) is the reference for what the harness measures
and how to read its report. This guide is the operational half: the
infrastructure, and the exact commands in the order they run.

For the ClickBench Parquet lane (ADR-2040 D7), use sections 1 to 6 here to
stand up the box, then follow
[ClickBench Parquet lane](clickbench.md#clickbench-parquet-lane) in
clickbench.md. That lane measures against loopback RustFS on the box, not
against the S3 bucket this runbook creates.

Total: about 90 minutes of wall clock, most of it the dataset download, the
first build, and the load.

## What you will build

| Piece | Purpose |
|---|---|
| S3 bucket, one region | The tenant's object storage. Ravel's only durable backend. |
| IAM user + access key | The credentials Ravel uses to reach the bucket. |
| SSM parameters | Where those credentials live, so no script contains a secret. |
| IAM instance role | Lets the box read those parameters. |
| EC2 instance | Builds Ravel, loads the corpus, runs the harness. |

**Put the bucket and the instance in the same region.** Same-region S3 to EC2
transfer is not billed; cross-region is. A benchmark that moves hundreds of GB
per pass is expensive in the wrong region and its request/byte trade no longer
reflects a realistic deployment.

## 1. Choose the instance

| Type | vCPU | RAM | Use |
|---|---|---|---|
| `c6a.4xlarge` | 16 | 32 GB | Default. Compute-optimised, and the type the public ClickBench results for other systems use, so numbers are comparable. |
| `r6a.4xlarge` | 16 | 128 GB | When the read cache must exceed the corpus (see step 8). |

**Root volume: 300 GB gp3.** The build tree, the dataset, and the cargo target
directory together use over 150 GB, and a full disk surfaces mid-link as a
misleading `linking with cc failed`, not as an out-of-space error.

Use **Amazon Linux 2023**. The bootstrap below assumes `dnf` and `/usr/lib64`.

## 2. Create the bucket and credentials

```sh
export AWS_REGION=us-east-1
export BUCKET=ravel-clickbench-$(openssl rand -hex 3)

aws s3api create-bucket --bucket "$BUCKET" --region "$AWS_REGION"
echo "bucket: $BUCKET"
```

Ravel reaches the bucket with an access key rather than the instance role, so
the same credentials work unchanged against RustFS or any other S3-compatible
store.

```sh
aws iam create-user --user-name ravel-clickbench

cat > /tmp/ravel-s3-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::$BUCKET", "arn:aws:s3:::$BUCKET/*"]
  }]
}
EOF

aws iam put-user-policy --user-name ravel-clickbench \
  --policy-name ravel-clickbench-s3 --policy-document file:///tmp/ravel-s3-policy.json

aws iam create-access-key --user-name ravel-clickbench > /tmp/ravel-key.json
```

## 3. Store the credentials in SSM

Every script reads credentials from Parameter Store at run time. Nothing holds
a secret in a file, an environment file, or a shell history.

```sh
AK=$(jq -r .AccessKey.AccessKeyId     /tmp/ravel-key.json)
SK=$(jq -r .AccessKey.SecretAccessKey /tmp/ravel-key.json)

aws ssm put-parameter --name /ravel-clickbench/access-key --type SecureString --value "$AK"
aws ssm put-parameter --name /ravel-clickbench/secret-key --type SecureString --value "$SK"
aws ssm put-parameter --name /ravel-clickbench/bucket     --type String       --value "$BUCKET"

shred -u /tmp/ravel-key.json /tmp/ravel-s3-policy.json
```

## 4. Create the instance role

The instance needs only to read those three parameters and decrypt the two
SecureStrings. It needs no S3 permission of its own: Ravel authenticates to S3
with the access key from step 2.

```sh
cat > /tmp/trust.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role --role-name ravel-clickbench-box \
  --assume-role-policy-document file:///tmp/trust.json

ACCT=$(aws sts get-caller-identity --query Account --output text)

# Step 3 wrote the SecureStrings without --key-id, so they are encrypted under
# the account's default SSM key, which is the key this policy must name. If you
# passed --key-id to put-parameter instead, set KMS_KEY_ID to that key's id.
KMS_KEY_ID=$(aws kms describe-key --key-id alias/aws/ssm \
  --query KeyMetadata.KeyId --output text)

cat > /tmp/ssm-read.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "ssm:GetParameter",
      "Resource": "arn:aws:ssm:$AWS_REGION:$ACCT:parameter/ravel-clickbench/*"
    },
    {
      "Effect": "Allow",
      "Action": "kms:Decrypt",
      "Resource": "arn:aws:kms:$AWS_REGION:$ACCT:key/$KMS_KEY_ID",
      "Condition": {
        "StringEquals": {
          "kms:ViaService": "ssm.$AWS_REGION.amazonaws.com",
          "kms:EncryptionContext:PARAMETER_ARN": [
            "arn:aws:ssm:$AWS_REGION:$ACCT:parameter/ravel-clickbench/access-key",
            "arn:aws:ssm:$AWS_REGION:$ACCT:parameter/ravel-clickbench/secret-key",
            "arn:aws:ssm:$AWS_REGION:$ACCT:parameter/ravel-clickbench/bucket"
          ]
        }
      }
    }
  ]
}
EOF

aws iam put-role-policy --role-name ravel-clickbench-box \
  --policy-name ssm-read --policy-document file:///tmp/ssm-read.json

aws iam create-instance-profile --instance-profile-name ravel-clickbench-box
aws iam add-role-to-instance-profile \
  --instance-profile-name ravel-clickbench-box --role-name ravel-clickbench-box
```

Add `AmazonSSMManagedInstanceCore` as well if you want Session Manager shell
access instead of SSH:

```sh
aws iam attach-role-policy --role-name ravel-clickbench-box \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
```

## 5. Launch

```sh
AMI=$(aws ssm get-parameter \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)

aws ec2 run-instances \
  --image-id "$AMI" \
  --instance-type c6a.4xlarge \
  --iam-instance-profile Name=ravel-clickbench-box \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=300,VolumeType=gp3}' \
  --key-name <your-key-pair> \
  --instance-initiated-shutdown-behavior terminate \
  --user-data '#!/bin/sh
shutdown -h +600' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=ravel-clickbench}]' \
  --query 'Instances[0].InstanceId' --output text
```

Every measurement box self-terminates ten hours after its first boot: the
user-data arms `shutdown -h +600` and the shutdown behaviour is `terminate`,
so a box whose driver session died is gone by the next morning instead of
billing until someone notices. Ten hours covers the longest run in this
runbook (bootstrap, load, fold and a full concurrent pass) with margin; a
run pre-registered to need more raises the figure in its own launch, and
says so on its issue. `scripts/bench/fresh-box.sh` carries the same figure
as the default of `--max-minutes`.

The timer covers the first boot only. cloud-init runs a user-data script
once per instance, not once per boot, so a box that was stopped and started
again (section 11) comes up with no shutdown scheduled; re-arm it by hand
right after the start:

```sh
sudo shutdown -h +600 "ravel-clickbench safety shutdown"
```

The re-armed timer terminates the box and discards the corpus with it, the
same as the first one: a box being kept for another session needs
`aws ec2 stop-instances` before the timer runs out.

An IMDSv2 token is required to read instance metadata from on the box:

```sh
T=$(curl -sX PUT http://169.254.169.254/latest/api/token \
      -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
curl -s -H "X-aws-ec2-metadata-token: $T" \
  http://169.254.169.254/latest/meta-data/instance-type
```

**With the kit.** `benchmarks/clickbench/kit/launch.sh run.toml` (copy
`benchmarks/clickbench/kit/run.example.toml` and fill it in) replaces this
section for an A/B pass. It refuses a run.toml with a missing required key
or a key or table it does not know (a `[bands]` entry for a lane the kit does
not run included), a band file that still holds a `FILL-IN` value in a
section `band_sections` selects, and a missing, unknown or removed band key,
naming each, and refuses with exit 65 when the instance profile
cannot be read or `simulate-principal-policy` says its role cannot read the
run's `in/` prefix or write its `out/` prefix, naming the role. It then uploads the kit to
`s3://<bucket>/<prefix>/runs/<run-id>/in/` and launches one instance per arm
under that run id, each with the instance profile, a 500 GB gp3 root volume,
`--instance-initiated-shutdown-behavior terminate` and a user-data that arms
`shutdown -h +600`. It prints the run id, both instance ids and the `out/`
prefix; `--dry-run` prints every command and both user-data scripts instead.

## 6. Bootstrap the box

Run as root. Builds the toolchain, both binaries, and downloads the dataset in
parallel with the build.

```sh
#!/bin/bash
# bootstrap.sh -- toolchain, checkout, release binaries, dataset.
export HOME=/root
set -uo pipefail
# Clear every marker a previous attempt could have left, and drop a partial
# dataset and checkout, so a retry never reuses stale failure state and the
# clone below never fails on a non-empty destination.
rm -f /root/BOOT_DONE /root/BOOT_FAILED /root/DATASET_FAILED /root/CLONE_SHA /root/hits.parquet
rm -rf /root/ravel
exec > /root/bootstrap.log 2>&1
fail() { echo "FAILED: $1"; touch /root/BOOT_FAILED; exit 1; }
trap '[ -f /root/BOOT_DONE ] || touch /root/BOOT_FAILED' EXIT

dnf install -y git gcc gcc-c++ make cmake pkgconfig openssl-devel perl wget jq \
               gperftools gperftools-libs gdb perf sysstat \
  || fail "dnf install"

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /root/rustup-init.sh || fail "fetch rustup"
sh /root/rustup-init.sh -y --default-toolchain none || fail "rustup install"
source /root/.cargo/env

git clone https://github.com/NOFireAI/ravel.git /root/ravel || fail "git clone"
cd /root/ravel || fail "no checkout"
# Pin the exact commit the clone checked out. Both binaries here and the
# re-build in step 8 build from this SHA, so a later push to main never lands
# in the measured binaries. Recorded for step 8 to read.
CLONE_SHA=$(git rev-parse HEAD) || fail "resolve clone commit"
git checkout -q --detach "$CLONE_SHA" || fail "detach clone commit"
echo "$CLONE_SHA" > /root/CLONE_SHA
rustc --version || fail "toolchain resolve"   # resolves via rust-toolchain.toml

# Download runs alongside the build; both must succeed.
( wget -q 'https://datasets.clickhouse.com/hits_compatible/hits.parquet' \
    -O /root/hits.parquet || touch /root/DATASET_FAILED ) &

cargo build --release -p ravel-cli || fail "build ravel-cli"
cargo build --release -p ravel-bench --features sql-latency,profiling \
  --bin sql_latency_bench || fail "build sql_latency_bench"

wait
[ -f /root/DATASET_FAILED ] && fail "download hits.parquet"
ls -la /root/hits.parquet

touch /root/BOOT_DONE
echo "BOOT DONE"
```

`hits.parquet` is about 14 GB. Watch with `tail -f /root/bootstrap.log`; the
run is finished when `/root/BOOT_DONE` or `/root/BOOT_FAILED` appears.

**With the kit.** Nobody logs in. The user-data checks `kit.tgz` and
`run.toml` against sha256 digests that `launch.sh` wrote into the user-data
itself (not against a file in S3, so rewriting both objects there does not
pass), then every file in the kit against its `SHA256SUMS`, writes the arm
name to `/root/kit/ARM` and starts `box/run.sh` under `nohup`; a failed check
uploads `FAILED` to the arm's `out/` prefix and shuts the box down before
`run.sh` starts. `run.sh` runs this section's packages, toolchain, clone of
the arm's SHA and frame-pointer build itself, then the section 7 load,
compact, fold and declare against a loopback RustFS store on the gp3 root
volume whose keys it generates on the box, so no SSM parameter is read. The
kit uses that store rather than this runbook's S3 bucket for three reasons:
each arm's data is private to its own box, so the two arms cannot read or
overwrite each other's tenant; the instance role needs no access beyond the
run's `in/` and `out/` prefixes; and it is the store the ClickBench Parquet
lane (ADR-2040 D7) measures against. The cost is that a kit figure includes
no network round trip to S3, so it is not comparable with a pass against the
bucket. Each step is stamped in `/root/kit/progress.log`. Every key the box
generates (the RustFS access and secret keys, the tenant token and the audit
token key, all in `/root/kit/secrets.env`, which is never uploaded) is
replaced by `[redacted]` in a copy of the logs before `evidence.tgz` is
written from it, and in the last 20 lines of `run.log` before `FAILED`
quotes them. A key still present after that refuses the tarball, and
`FAILED` then says the tail was withheld instead of quoting it; `FAILED` is
still uploaded. The audit token key reaches the server only under
`audit_mode = "required"` with redacted audit text, the one mode that reads
it.

## 7. Load, compact, fold, declare

Every step below needs the credentials in the environment. Export them once per
shell:

```sh
export HOME=/root
source /root/.cargo/env
CLI=/root/ravel/target/release/ravel-cli

AK=$(aws ssm get-parameter --name /ravel-clickbench/access-key --with-decryption --query Parameter.Value --output text)
SK=$(aws ssm get-parameter --name /ravel-clickbench/secret-key --with-decryption --query Parameter.Value --output text)
B=$(aws ssm get-parameter  --name /ravel-clickbench/bucket --query Parameter.Value --output text)

export RAVEL_S3_BUCKET=$B RAVEL_S3_REGION=us-east-1 \
       RAVEL_S3_ACCESS_KEY=$AK RAVEL_S3_SECRET_KEY=$SK \
       RAVEL_S3_ACCESS_KEY_ID=$AK RAVEL_S3_SECRET_ACCESS_KEY=$SK
```

### Two startup preconditions, both of which stop the server dead

Neither is a failure of the box or the bucket, and both surface only when the
server first tries to serve, which on this path is after a twenty-minute load.
Satisfy them before you start, not after.

**The bucket's qualification record must match the binary's suite version.**
The server refuses to start against a backend qualified by an older suite:

```
Error: store backend is not qualified (sys/qualification); refusing to start
  ... recorded under suite version 1, but this binary requires at least
  version 2 ...
```

A bucket qualified for an earlier build hits this whenever the suite version is
bumped. Re-qualify with the binary you are about to run, which rewrites the
record:

```sh
"$CLI" --store s3 --s3-auth instance-role store qualify
```

**The query audit is off by default.** Since #2791 `--audit-mode` defaults to
`off`: the server writes no query-audit record, awaits no audit PUT per
statement, and needs no tokenization key, so an unkeyed benchmark server
starts with no audit setting at all. Runs before #2791 measured the audited
path, two sequential audit PUTs per statement, so a figure compared across
that change must say which basis it used.

To measure the audited path, enable the audit and give its default `redacted`
text policy a key; this benchmark passes `--tenant-hash-unkeyed` so there is
no deployment secret to derive one from. Pick one and keep it for the whole
run, because the choice decides what the audit trail records:

```sh
# Either: the redacted default with a key.
#   ... --audit-mode required
export RAVEL_AUDIT_TOKEN_KEY=$(openssl rand -hex 32)

# Or: record query text verbatim, defensible here because the corpus is public
# and the statements are the published ClickBench set.
#   ... --audit-mode required --audit-text plaintext
```

`--audit-text plaintext` without `--audit-mode required` is refused at
startup: it tunes a pipeline `off` never installs. Say which setting a
published run used: `--audit-mode required` and `--audit-text plaintext` are
each a deviation from the stock defaults such a run otherwise measures.

### Load

```sh
TENANT=clickbench-v4

/usr/bin/time -f 'wall=%e rss_kb=%M user=%U sys=%S' \
  $CLI --store s3 load \
    --parquet /root/hits.parquet \
    --tenant "$TENANT" \
    --mapping /root/ravel/benchmarks/clickbench/hits.mapping.toml \
    --shards 8 \
    --read-cursors 8 \
    --batch-rows 65536
```

`--shards 8` sets the tenant's shard count; every later command must pass the
same value or it addresses a different shard set. `--read-cursors` sets load
parallelism.

### Compact

The load writes many small L0 objects. Compaction merges them, which is the
state a query benchmark should measure.

```sh
/usr/bin/time -f 'wall=%e rss_kb=%M' \
  $CLI --store s3 maintain compact-tenant \
    --tenant "$TENANT" --signal logs --shards 8
```

### Fold

Folding builds the catalog the query planner reads. **A fold seals an ingest
hour only after `max_flush_lifetime + clock_skew_allowance + fold_safety_margin`
has elapsed past the end of that hour**, so a fold run immediately after a load
seals nothing on its own. Once the load and the compaction have both exited,
nothing else writes to this tenant, so assert that with `--writers-stopped`: the
fold then seals through the hour it runs in, without waiting out the margin.

Do not use `load --fold-after-load` in this sequence. It seals at the end of the
load, before compaction, and a later fold in the same hour has nothing left to
seal, so it does not pick up the compaction records and the pass measures the
uncompacted layout. The watermark advances only once the natural seal margin
passes the next hour, or on another `--writers-stopped` fold (or
`--fold-after-load` load) run in a later hour. A second `--fold-after-load` into the tenant in the same hour is refused
before it writes anything.
`--fold-after-load` fits a sequence with no compaction after the load, such as
the `--compaction pre` flow in `docs/internal/clickbench.md`.

```sh
$CLI --store s3 catalog fold \
  --tenant "$TENANT" --shards 8 --signal logs --writers-stopped \
  > /root/fold.out 2>&1
grep -E "watermark_hour|seal_through_hour|buckets_folded|entry_count|part_bytes" /root/fold.out
```

Check the output before continuing. `--signal` is not optional in practice: a
fold defaulting to the wrong signal on a logs-only tenant succeeds and seals
nothing. `seal_through_hour` names the hour the fold sealed through, and
`entry_count` should be at least 0.8 of the number of objects the load wrote.
If the fold covered under 80% of them, the shard count or signal is wrong.

### Declare the typed columns

```sh
$CLI --store s3 typed-attr-column set "$TENANT" \
  --from-mapping /root/ravel/benchmarks/clickbench/hits.mapping.toml
```

This writes about 104 declared columns. The `sql_latency_bench --tenant` lane
reads the durable declaration directly, so it sees a fresh declaration
immediately; anything querying through `ravel-server` must wait out the
declared-column cache staleness horizon first.

## 8. Run the pass

Build with frame pointers so a profile taken from the same binary resolves, and
**pin the binary under a name carrying its commit SHA**. Two hosts with
different binaries at the same path turn a missing flag into a wrong default.

```sh
cd /root/ravel || exit 1
# The commit the bootstrap clone built, never a moving origin/main. Stop if the
# record is missing or the checkout does not land on it: a build from whatever
# is checked out would carry an unknown SHA under a pinned name.
HEAD_SHA=$(cat /root/CLONE_SHA) && [ -n "$HEAD_SHA" ] \
  || { echo "no /root/CLONE_SHA: the bootstrap did not finish"; exit 1; }
git checkout -q --detach "$HEAD_SHA" || { echo "cannot check out $HEAD_SHA"; exit 1; }
[ "$(git rev-parse HEAD)" = "$HEAD_SHA" ] || { echo "HEAD is not $HEAD_SHA"; exit 1; }
# A checkout can match HEAD_SHA and still carry edits; a binary built from
# them would not be the commit its pinned name claims.
[ -z "$(git status --porcelain)" ] || { echo "checkout is dirty; reset it before building"; exit 1; }

export CARGO_TARGET_DIR=/root/target-fp
RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --release -p ravel-bench --features sql-latency,profiling \
    --bin sql_latency_bench

mkdir -p /root/bin
PINNED=/root/bin/sql_latency_bench-$HEAD_SHA
cp $CARGO_TARGET_DIR/release/sql_latency_bench "$PINNED"
```

Run it under tcmalloc. Under glibc, resident memory ratchets up through arena
growth and reads as a leak that is not one.

```sh
TCMALLOC=$(ls /usr/lib64/libtcmalloc.so.4 /usr/lib64/libtcmalloc.so 2>/dev/null | head -1)
CACHE_BYTES=25769803776    # 24 GiB, larger than the 9.84 GB corpus

mkdir -p /root/explain

LD_PRELOAD="$TCMALLOC" "$PINNED" \
  --tenant clickbench-v4 --store s3 --compaction post --window-hours 200000 \
  --sql-max-segments 1000000 --deadline-secs 900 --continue-on-error \
  --cache-bytes "$CACHE_BYTES" \
  --sql-tenant-max-bytes 16451897344 \
  --sql-max-query-bytes 16451897344 \
  --fetch-concurrency 128 \
  --explain --explain-dir /root/explain \
  --corpus /root/ravel/benchmarks/clickbench/hits.corpus.json \
  --runs 3 \
  > /root/bench.json 2> /root/bench.stderr
```

Flags that change what is measured:

| Flag | Effect |
|---|---|
| `--runs 3` | Run 0 is cold; runs 1 and 2 are warm. Both figures come from one pass. |
| `--cache-bytes` | **Must exceed the corpus.** Smaller and every run re-reads everything, so "warm" numbers are three cold runs. |
| `--compaction post` | Measures the compacted layout. |
| `--fetch-concurrency` | Bounds in-flight object-store GETs. |
| `--logs-fetch-policy` | The logs read shape, resolved the way the server resolves its own flag of the same name. Unset means `cost-based`, which at the reference intra-region profile reads every log object whole in one covering GET, the shape a stock server has, loopback `--s3-endpoint` or not (ADR-2023 decision 1). Passes taken before this flag existed range-read every object above 512 KiB per block, so their GET counts are not comparable with a default pass; `byte-minimal` reproduces that older shape. The report's `logs_fetch_policy` records the policy as requested, and `logs_block_range_threshold_effective` and `logs_request_cost_bytes_effective` record what it resolved to. |
| `--logs-block-range-threshold` | Object size in bytes above which a log object is range-read per block instead of read whole. Only meaningful under `byte-minimal`; `cost-based` at the reference profile overrides it with whole-object reads. Record it whenever a run sets it, or the pass cannot be reproduced. |
| `--logs-request-cost-bytes` | The explicit request-cost rate the routing weighs a GET at, in bytes. Unset means the rate the policy resolves at the profile; set, it wins over the policy-derived rate under every policy, `byte-minimal` included, so a run that combines the two must record both. Same reproduction rule as the threshold. |
| `--explain --explain-dir` | Captures a plan per statement. A null result cannot be diagnosed without it. |
| `--continue-on-error` | One failing statement does not abandon the pass. |

**The exit code is not the pass/fail signal.** It is non-zero whenever any
statement fails. No statement fails by design on this corpus: the per-query
pool above (16,451,897,344 bytes, the server's derived 50% share on the
reference box) holds q33's 10,855,811,936-byte peak. Assert the measured and failed identities instead, as step
9 does; a non-zero exit is a statement to name, not a known cost.

These flags go to `sql_latency_bench`, which runs the engine in process and
has no memory-budget derivation: its pools are exactly what the flags say.
The server's derived figures, including the 90%-of-remainder cap that
ADR-1170 (amended 2026-10-03 by issue #2367) places on a derived pool, apply
to `ravel-server` runs only; see
[`docs/internal/clickbench.md`](clickbench.md#deriving-the-reference-sizes)
for the worked S3 and loopback figures and how they move with
`MemAvailable`.

Because the derived figures depend on a `MemAvailable` reading, two stock
passes on the same instance type can resolve different ceilings. A published
stock result records the `MemAvailable` reading next to the resolved
`performance default resolved` lines in the entry. Comparing two passes
against each other (an A/B, a regression check) pins `--memory-budget-bytes`
to one value on both runs instead, so the budget is not the variable that
differs between them.

**With the kit.** `run.toml`'s `memory_budget_bytes` is that pinned value,
the same on both arms. `box/run.sh` writes the binaries and every server flag
to `/root/kit/arm.env` once and every server start sources it, so a restart
runs the flags the first start ran. The load must report `rows_written` equal
to `run.toml`'s `rows_expected` (99,997,497 for `hits.parquet`), or the box
fails at step `load`. After the declare, a `verify` step sends the corpus's
`q01` (`SELECT COUNT(*) FROM logs`) once through the same endpoint and window
the benchmark uses; an answer other than `rows_expected` fails the box at step
`verify` before any statement is timed, and the three counts are stamped as
`provenance.row_count`. Its `benchmark` phase runs each statement
cold (server restarted, page cache dropped) and then twice hot, and every
call goes through `run_query`, which records it in
`out/benchmark/calls.jsonl`. Every request body carries an explicit `start`
and `end` spanning `run.toml`'s `window_hours` (default 200000, the window
`sql_latency_bench --window-hours 200000` uses), because the SQL endpoint
otherwise reads only the last hour. Each answer's row count is checked against
the statement's `expected_rows` in `hits.corpus.json`, or, where that is null,
against the cold try's count; a mismatch is recorded as a `wrong_answer` error
and counts in the pooled error rate. `results.json` has the shape of
`sql_latency_bench`'s report: an `entries` array of `id`, `min_ms` (the hot
figure: the best of tries 2 and 3), `cold_ms` (try 1), `median_ms` and
`max_ms` (over all three tries) and `per_run_accounting` (null over HTTP), a
`failed` array of `id`, `run` and `error`, a `timing_definition` string that
states those definitions, and the provenance and band verdicts as further
top-level keys. `sql_latency_bench`'s own `min_ms` is over all three runs, so
the two are not the same figure. The box
then writes `results.json`, `evidence.tgz` and `progress.log`, uploads them
with `DONE` (or `FAILED`, naming the step) to its `out/<arm>/` prefix, and
terminates itself.

`run.toml`'s `phases` must name `benchmark` and can name three more, run in
the order listed, before the results are written. Each of their keys is
optional; `launch.sh` writes any key the file leaves out into the uploaded
`run.toml` at its default, and `check-run` refuses a value of the wrong type,
naming the key.

- `concurrent` reproduces upstream ClickBench's `bench_concurrent_qps`. Its
  keys are `concurrent_rounds` (default 3), `concurrent_connections` (10),
  `concurrent_duration_s` (600) and `concurrent_seed` (`"clickbench"`). Each
  round stops the server, drops the page cache, starts it and waits for
  readiness, then runs every one of the 43 statements, registered failures
  included and none weighted, on each connection `c` from 1 to
  `concurrent_connections`. Connection `c` visits them in the order
  `random.Random(seed_int).shuffle(list(range(43)))` gives, where `seed_int`
  is the first 8 bytes of the sha256 of `"<seed>-<c>"` read big-endian, and
  loops over that order. A connection checks the deadline (`concurrent_duration_s`
  after the round starts) before each call; a call in flight at the deadline
  completes and is counted. The deadline is whole seconds, `date +%s` plus
  `concurrent_duration_s`, as upstream computes it. No call has a client
  timeout. Readiness in this phase uses upstream's check, not `/readyz`: a
  POST of `SELECT 1` to `/api/v1/sql` with `curl --fail-with-body` and no
  timeout, its body naming the same window as every other call. The server
  start before each round polls that check, and a watchdog runs it every
  5 s and restarts a server that fails it, polling up to 60 times 1 s apart
  for readiness, without telling the connections, so calls during the outage
  are errors. A refusal of `SELECT 1` under memory pressure therefore
  restarts the server even while `/readyz` answers. Unlike upstream, the
  watchdog's last sleep ends at the deadline and no check runs after it.
  Every call is a line of `out/concurrent/calls.jsonl` with the benchmark's
  fields plus `round` and `connection`. Every phase's call lines carry
  `curl_exit`, and `http_status` is 0 when nothing answered. The figures
  follow upstream: a call succeeds when it gets a 2xx answer and its
  transfer completed
  (curl exit 0, as `curl --fail-with-body` reports it) and is an error
  otherwise, and a round's QPS is its successful calls over its nominal
  duration (`concurrent_duration_s`, not the measured wall time), 0 when
  the round made calls and none succeeded and null only when it made no
  call; its error ratio is errors over calls. A 2xx
  answer with the wrong row count is a success under that rule and is
  counted separately as `wrong_answer`; `outcome` and `refusal_class` stay
  in the call lines as diagnostics. `results.json`'s `concurrent` block
  holds each round's `qps`, `error_ratio`, `ok`, `err`, `wrong_answer` and
  `wall_s`, and the rounds pooled two ways: `error_ratio_weighted` (all
  rounds' errors over all rounds' calls) and `error_ratio_mean_of_rounds`
  (the mean of the rounds' ratios), with `qps_pooled` the mean of the
  rounds' non-null QPS, so a round whose calls all failed counts as 0. The
  bands judge `error_ratio_weighted`; the mean of rounds is reported only
  for comparison with earlier readouts. The pooled error rate
  (`error_pooled`, judged by `error_pooled_max_delta` and
  `error_pooled_max`) covers the benchmark phase only: neither the
  concurrent calls, which have their own band, nor the `q29_probe` and
  `perf` calls are in it, so its verdict does not change with which phases
  a run names.
- `q29_probe` restarts the server and sends the corpus's q29 alone,
  `q29_probe_warmup` times untimed (default 1), then `q29_probe_runs` times
  timed (default 3). `out/q29_probe/calls.jsonl` records each call's
  duration and the response's `stats` object. `results.json`'s `q29_probe`
  block holds every timed run's duration and their `min_s` and `median_s`;
  the warmup is in neither.
- `perf` needs `perf_record = true` (default false), and `check-run` refuses
  it otherwise. Per statement it sends one warm call, then one hot call under
  `perf record -F 99 -g -p <server pid>`, and keeps the samples as folded
  stacks in `out/perf/<NN>.folded`, which `evidence.tgz` carries; each
  statement's `perf.data` is removed whether or not its profile succeeded.

The rlog lane's band file judges the `concurrent` and `q29_probe` phases in
its existing file, `bands/rlog.toml`, not a file of their own: B's
`qps_pooled` at least `qps_min_ratio` times A's, B's `error_ratio_weighted`
at most A's plus `concurrent_error_pooled_max_delta`, and B's q29 `min_s` at
most `q29_probe_max_ratio` times A's, with absolute counterparts that are
placeholders until the reference machine sets them. A band whose phase did
not run on both arms is inconclusive, never a pass. `fetch.sh` prints each
arm's per-round QPS and error ratio beside its pooled figures, and on a
missed concurrent error band it prints that arm's errors by statement as
`count / total`, read from `out/concurrent/calls.jsonl` in its
`evidence.tgz`.

## 9. Check the report before reading the headline

A number printed and not asserted on is decoration. Write the expected figures
and their bands down **before** the run, then let a script fail the pass rather
than reading totals by eye. Two statements measured out of forty-three and a
zero exit code look identical to a clean pass otherwise.

The bands below are the per-class cost-class guards registered on issue #913
(comment "Band registration for T2's second half"). Each statement in
`benchmarks/clickbench/hits.corpus.json` carries a typed `class`
(`metadata_decomposable` / `selective` / `full_value`); the script reads those
classes from the corpus and asserts the band each class is held to. **These are
no-regression guards, not targets:** every band names the target it is not yet
at and the issue that closes the gap. Do not loosen a guard to match a target.

```python
#!/usr/bin/env python3
# analyse.py -- integrity first, per-class bands next, headline last.
# Exits non-zero on any miss. Usage: analyse.py <bench.json> [hits.corpus.json]
#
# Every band is a NAMED CONSTANT in the one block below, so re-registering a
# band on #913 is a one-line edit here. Each band is a no-regression guard: the
# comment names the target it is not yet at and the issue that closes the gap.
import json, sys

# --- Pre-registered bands (issue #913, "Band registration for T2's second
# half"). Re-register by editing exactly one line here. ---------------------
CORPUS_BYTES        = 12.03e9      # total corpus size; the % bands are off this

# Integrity (asserted BEFORE any band): all 43 statements measure, none fails.
# q33 fits since the derived per-query SQL pool became the tenant's share
# (50% of memory, 16,451,897,344 bytes on the reference box, against its
# 10,855,811,936-byte peak); measured 43 of 43 at main 4aa5293bb602.
EXPECTED_MEASURED   = 43
EXPECTED_FAILED_Q   = set()        # by identity, not a count: a statement that
                                   # fails is named, never summed away

# Class M (metadata-decomposable): answerable from metadata, no data read.
M_Q01_DATA_GETS_MAX = 0            # q01 issues zero data (scan-phase) reads
M_TOTAL_GETS_MAX    = 59_800       # target: 0 for all four; #850
M_TOTAL_BYTES       = 4.84e9       # target: 0; #850

# Class S (selective): a small predicate touches a small slice of the corpus.
S_EACH_FRAC_MAX     = 0.055        # q37..q43 EACH <= 5.5% of corpus bytes
S_GROUP_BYTES_MAX   = 33.2e9       # q20..q24 GROUP TOTAL; target: 5% each

# Fetch amplification: scan-phase WIRE bytes per STORED page byte decoded.
# TWO bands, because they are two populations and one number cannot be both.
# Measured on the ratio-0 baseline (#913): all-42 5.794, Class-F-only 7.302.
# Class F is the higher of the two; the corpus-wide figure is pulled down by
# Class S at 2.875 over 25.4 GB of scan bytes. Banding the Class-F population
# against the corpus-wide number would fail on the baseline it came from.
CORPUS_AMPLIFICATION_MAX = 6.1     # all measured rows; measured 5.794 (+5%)
F_AMPLIFICATION_MAX      = 7.7     # Class F rows only; measured 7.302 (+5%)
                                   # target for both: 1.25, provisional
# Both bands were registered over the 42 statements that measured on the
# #913 baseline, before q33 fit the per-query pool. q33 is measured (the
# integrity check above demands it) but kept OUT of both amplification
# populations until the bands are re-registered with it (#2421); a band
# applied to a population it was not measured on is not a band. Its own
# ratio is still printed, unbanded, so a regression stays visible.
AMPLIFICATION_EXCLUDED   = {"q33"}

# Operator pre-registration for THIS run's wall clock (a property of the run,
# not of #913). Fill as (LO, HI) before the pass, or leave None for a loud SKIP.
HOT_S_BAND          = None
COLD_S_BAND         = None

def load(path):
    docs, buf = [], ""
    for line in open(path, errors="replace"):
        buf += line
        try:
            docs.append(json.loads(buf)); buf = ""
        except Exception:
            pass
    return docs

def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values(): yield from walk(v)
    elif isinstance(o, list):
        for v in o: yield from walk(v)

def rows(path):
    """Report rows keyed by full statement id.

    Duplicate full ids are collected here, BEFORE the dict overwrites them.
    A later duplicate silently replacing an earlier one would keep the
    measured count correct while changing which figures are checked, so the
    duplicate has to be caught at the point it is lost."""
    out, dup_id = {}, set()
    for d in load(path):
        for n in walk(d):
            sid = n.get("id")
            if isinstance(sid, str) and sid[:1] == "q":
                if sid in out:
                    dup_id.add(sid)
                out[sid] = n
    return out, dup_id

def q_prefix(sid):            # "q07" from "q07_min_max_eventdate"
    return sid.split("_", 1)[0]

def corpus_classes(path):     # {q_prefix: class-string} from the corpus file
    doc = json.load(open(path))
    out = {}
    for e in doc["entries"]:
        out[q_prefix(e["id"])] = e.get("class")
    return out

report = sys.argv[1]
corpus = sys.argv[2] if len(sys.argv) > 2 else \
    "/root/ravel/benchmarks/clickbench/hits.corpus.json"

r, dup_id = rows(report)
timed  = {k: v for k, v in r.items() if isinstance(v.get("min_ms"), (int, float))}
failed = sorted(k for k, v in r.items() if v.get("error"))
cls    = corpus_classes(corpus)

# Report rows keyed by q-prefix, so a band phrased over q<NN> can find its row.
# A prefix that maps to two measured rows is ambiguous and fails, not silently
# picks one.
by_q, dup_q = {}, set()
for sid, v in timed.items():
    q = q_prefix(sid)
    if q in by_q: dup_q.add(q)
    by_q[q] = v

fails = []

def cold(q):
    """Cold-run (run 0) accounting for statement q, or None if not measured.
    Cold is the object-store cost; warm runs hit cache. A band figure that is
    EXPECTED but not emitted (statement absent, or no accounting) is a FAIL,
    never a skip."""
    v = by_q.get(q)
    if v is None:
        fails.append(f"{q}: expected a measured row, none present")
        return None
    if q in dup_q:
        fails.append(f"{q}: two measured rows share this prefix; ambiguous")
        return None
    pra = v.get("per_run_accounting") or []
    if not isinstance(pra, list):
        fails.append(f"{q}: per_run_accounting is not a list ({pra!r})")
        return None
    if not pra:
        fails.append(f"{q}: no per_run_accounting; cold cost not emitted")
        return None
    # Validate the record before handing it to a caller that will `.get` it.
    # A null or scalar run-0 entry would otherwise raise AttributeError inside
    # scan_wire and abort the analysis with a traceback instead of a verdict.
    if not isinstance(pra[0], dict):
        fails.append(f"{q}: per_run_accounting[0] is not an object ({pra[0]!r})")
        return None
    return pra[0]

def byte_count(v, what):
    """A byte figure from the report, or None with a recorded failure.

    The producer emits these as u64, but this script reads arbitrary JSON and
    must not trust it. Three values would otherwise pass a naive numeric check
    and corrupt a band silently:

      * `true` -- `bool` is a subclass of `int` in Python, so it would add 1.
      * `NaN`  -- `json.loads` accepts it, and every comparison against it is
                  False, so `amp > band` would report the band as MET. A
                  malformed report would read as a pass, which is worse than
                  reading as a failure.
      * a negative -- impossible for a byte count, and it would drag a total
                  down toward a passing figure.
    """
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        fails.append(f"{what}: expected a byte count, got {v!r}")
        return None
    if v != v or v in (float("inf"), float("-inf")):      # NaN or +/-inf
        fails.append(f"{what}: byte count is not finite ({v!r})")
        return None
    if v < 0:
        fails.append(f"{what}: byte count is negative ({v!r})")
        return None
    return v

def finite_nonneg(v, what):
    """An object-store count or byte field from the report, or None with a
    recorded failure.

    The class totals below sum these, so the same three traps `byte_count`
    documents apply: `bool` is an `int` subclass and would add 1; `NaN` poisons
    every later comparison so a band would read as MET; a negative would drag a
    total down toward a passing figure. An invalid field is a failed pass, not
    a zero: a total computed over fewer fields than the class has statements
    would pass a band it never measured."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        fails.append(f"{what}: expected a finite non-negative number, got {v!r}")
        return None
    if v != v or v in (float("inf"), float("-inf")):      # NaN or +/-inf
        fails.append(f"{what}: value is not finite ({v!r})")
        return None
    if v < 0:
        fails.append(f"{what}: value is negative ({v!r})")
        return None
    return v

def scan_wire(acc, q):
    """Scan-phase WIRE bytes for statement `q`, or None with a recorded failure.

    The report carries per-phase wire BYTES but no per-phase GET count, so
    scan-phase bytes is the data-read signal. At the zero point it is exact:
    zero scan bytes == zero data GETs.

    Every phase entry is checked to be a mapping first. A null or scalar entry
    would make `p.get` raise AttributeError and abort the whole analysis, which
    is worse than a recorded failure: the run ends with a traceback instead of
    a verdict, and a crash is easy to misread as a harness problem rather than
    a malformed report."""
    phases = acc.get("wire_bytes_by_phase")
    if phases is None:
        phases = []
    if not isinstance(phases, list):
        fails.append(f"{q}: wire_bytes_by_phase is not a list ({phases!r})")
        return None
    scan = []
    for i, p in enumerate(phases):
        if not isinstance(p, dict):
            fails.append(f"{q}: wire_bytes_by_phase[{i}] is not an object ({p!r})")
            return None
        if p.get("phase") == "scan":
            scan.append(p)
    # Exactly one, not "the first one". Returning the first and ignoring the
    # rest would silently drop scan bytes from the total and hand a wrong
    # amplification verdict back as if it were measured. This is the same
    # present-exactly-once rule the integrity gate applies to report rows,
    # applied one level down.
    if not scan:
        fails.append(f"{q}: no scan-phase wire_bytes entry in the measured run")
        return None
    if len(scan) > 1:
        fails.append(f"{q}: {len(scan)} scan-phase wire_bytes entries; expected "
                     "exactly one, and summing them would hide which is real")
        return None
    return byte_count(scan[0].get("wire_bytes"), f"{q} scan-phase wire_bytes")

# Membership every band relies on. Held here AND in the corpus data; if the two
# disagree the band is being applied to the wrong statement, so fail loudly.
CLASS_M      = ["q01", "q02", "q07", "q08"]
CLASS_S_EACH = ["q37", "q38", "q39", "q40", "q41", "q42", "q43"]
CLASS_S_GRP  = ["q20", "q21", "q22", "q23", "q24"]
for q in CLASS_M:
    if cls.get(q) != "metadata_decomposable":
        fails.append(f"{q}: corpus class {cls.get(q)!r}, expected metadata_decomposable")
for q in CLASS_S_EACH + CLASS_S_GRP:
    if cls.get(q) != "selective":
        fails.append(f"{q}: corpus class {cls.get(q)!r}, expected selective")

hot  = sum(v["min_ms"] for v in timed.values()) / 1000.0
colds = sum((v.get("cold_ms") or 0) for v in timed.values()) / 1000.0

print(f"measured {len(timed)}  failed {len(failed)} {failed}")
print(f"hot  {hot:.2f} s")
print(f"cold {colds:.2f} s")

# --- Integrity FIRST: a total over a different statement count, or over a run
# with unexpected failures, is not comparable and no band on it stands. -----
#
# Counts alone are not integrity. They are satisfied by the wrong 43 rows: an
# omitted Class-F statement replaced by an unrecognised `q` row keeps the count
# at 43, and "no failures" is also satisfied by a statement that never ran. So
# the IDENTITIES are checked, not the totals.
if dup_id:
    fails.append(f"duplicate report rows for {sorted(dup_id)}; "
                 "a later row overwrote an earlier one")

corpus_q  = set(cls)                                    # every prefix the corpus defines
covered_q = {q_prefix(k) for k in timed} | {q_prefix(k) for k in failed}
missing   = corpus_q - covered_q
unknown   = covered_q - corpus_q
if missing:
    fails.append(f"corpus statements absent from the report: {sorted(missing)}")
if unknown:
    fails.append(f"report rows not in the corpus: {sorted(unknown)}")

if len(timed) != EXPECTED_MEASURED:
    fails.append(f"measured {len(timed)}, expected {EXPECTED_MEASURED}")

# Exactly the expected failure, by identity. Not "at most N": a pass with zero
# failures and a missing statement would clear a <= check while measuring less
# than the run it is compared against.
failed_q = {q_prefix(k) for k in failed}
if failed_q != EXPECTED_FAILED_Q:
    fails.append(f"failed statements {sorted(failed_q)}, expected exactly "
                 f"{sorted(EXPECTED_FAILED_Q)}")

# --- Class M --------------------------------------------------------------
acc = cold("q01")
if acc is not None:
    sw = scan_wire(acc, "q01")
    if sw is not None:
        print(f"class M q01 data (scan) wire bytes {sw}")
        if sw > M_Q01_DATA_GETS_MAX:
            fails.append(f"class M q01 data reads {sw} bytes, must be {M_Q01_DATA_GETS_MAX}")
m_gets = m_bytes = 0
m_ok = True
for q in CLASS_M:
    acc = cold(q)
    if acc is None:
        m_ok = False; continue
    g = finite_nonneg(acc.get("object_store_get_requests"), f"class M {q} object_store_get_requests")
    b = finite_nonneg(acc.get("object_store_bytes"), f"class M {q} object_store_bytes")
    if g is None or b is None:
        m_ok = False; continue
    m_gets  += g
    m_bytes += b
if m_ok:
    print(f"class M total {m_gets} GETs, {m_bytes/1e9:.2f} GB")
    if m_gets > M_TOTAL_GETS_MAX:
        fails.append(f"class M total {m_gets} GETs > {M_TOTAL_GETS_MAX} (target 0; #850)")
    if m_bytes > M_TOTAL_BYTES:
        fails.append(f"class M total {m_bytes/1e9:.2f} GB > {M_TOTAL_BYTES/1e9:.2f} GB (target 0; #850)")

# --- Class S: assert the BYTE half; SKIP the object-count half LOUDLY ------
S_EACH_BYTES_MAX = S_EACH_FRAC_MAX * CORPUS_BYTES
for q in CLASS_S_EACH:
    acc = cold(q)
    if acc is None:
        continue
    b = finite_nonneg(acc.get("object_store_bytes"), f"class S {q} object_store_bytes")
    if b is None:
        continue
    print(f"class S {q} {b/1e9:.2f} GB ({100*b/CORPUS_BYTES:.1f}% of corpus)")
    if b > S_EACH_BYTES_MAX:
        fails.append(f"class S {q} {b/1e9:.2f} GB > {S_EACH_FRAC_MAX*100:.1f}% "
                     f"({S_EACH_BYTES_MAX/1e9:.2f} GB) of corpus bytes")
s_grp_bytes = 0
s_grp_ok = True
for q in CLASS_S_GRP:
    acc = cold(q)
    if acc is None:
        s_grp_ok = False; continue
    b = finite_nonneg(acc.get("object_store_bytes"), f"class S {q} object_store_bytes")
    if b is None:
        s_grp_ok = False; continue
    s_grp_bytes += b
if s_grp_ok:
    print(f"class S q20..q24 group total {s_grp_bytes/1e9:.2f} GB")
    if s_grp_bytes > S_GROUP_BYTES_MAX:
        fails.append(f"class S q20..q24 group {s_grp_bytes/1e9:.2f} GB > "
                     f"{S_GROUP_BYTES_MAX/1e9:.2f} GB (target: 5% each)")
# The #913 Class-S target is "<=5% of corpus bytes AND object count". The report
# records GETs, and GETs are not objects (q37..q43 issue ~9,694 requests against
# a 3,469-object corpus, 2.8x more requests than objects exist). Comparing GETs
# to "5% of object count" compares two different quantities, so the object half
# is not asserted here. Closing it needs a distinct-objects-touched counter in
# the read path; until then this is a stated gap, not a passed check.
print("SKIP class S object-count half: report has GETs, not distinct objects "
      "touched; see #913 (needs a distinct-objects counter)")

# --- Fetch amplification: corpus-wide AND Class-F, as two separate figures -
#
# The Class-F band must be measured over Class-F rows only. Summing every class
# into one ratio and calling it "class F" is a different quantity: Class S at
# 2.875 over 25.4 GB of scan bytes drags the corpus figure well below the
# Class-F one (5.794 against 7.302 on the ratio-0 baseline).
def amplification(qs, label, band):
    scan_total = decoded_total = 0
    seen = 0
    for q in qs:
        acc = cold(q)                      # `cold` already FAILS on an absent row
        if acc is None:
            return
        sw = scan_wire(acc, q)
        if sw is None:
            return
        # The denominator gets the same treatment as the numerator: absent or
        # malformed is a FAIL, not a zero. `or 0` would let a row with no
        # decoded-byte figure still count toward `seen` while contributing
        # nothing to the denominator, which understates it and inflates the
        # ratio -- an amplification computed from a partial denominator reads
        # as a real measurement.
        dec = byte_count(acc.get("page_stored_bytes_decoded"),
                         f"{label} amplification: {q} page_stored_bytes_decoded")
        if dec is None:
            return
        scan_total    += sw
        decoded_total += dec
        seen += 1
    if seen == 0:
        fails.append(f"{label} amplification: no rows in this population; "
                     "a band over nothing is not a passed check")
        return
    if decoded_total == 0:
        fails.append(f"{label} amplification: no decoded page bytes; not emitted")
        return
    amp = scan_total / decoded_total
    print(f"{label} fetch amplification {amp:.3f}  (n={seen})")
    if amp > band:
        fails.append(f"{label} amplification {amp:.3f} > {band} "
                     f"(target 1.25, provisional)")

f_all = sorted(q for q in by_q if cls.get(q) == "full_value")
f_qs = [q for q in f_all if q not in AMPLIFICATION_EXCLUDED]
if not f_all:
    fails.append("no Class-F statements found in the corpus; the Class-F band "
                 "cannot be evaluated and must not silently pass")
elif not f_qs:
    fails.append(f"every Class-F statement is in AMPLIFICATION_EXCLUDED "
                 f"({sorted(AMPLIFICATION_EXCLUDED)}); the Class-F band cannot "
                 f"be evaluated and must not silently pass")
amplification(sorted(q for q in by_q if q not in AMPLIFICATION_EXCLUDED), "corpus-wide", CORPUS_AMPLIFICATION_MAX)
amplification(f_qs,         "class F",     F_AMPLIFICATION_MAX)
# The excluded statements' own ratios, printed and never banded: a figure
# that is not printed cannot be seen to regress before the re-registration.
for q in sorted(AMPLIFICATION_EXCLUDED):
    if q in by_q:
        amplification([q], f"{q} (unbanded)", float("inf"))

# --- Operator wall-clock bands: assert if pre-registered, else SKIP loudly -
if HOT_S_BAND is None:
    print("SKIP hot band: not pre-registered for this run")
elif not (HOT_S_BAND[0] <= hot <= HOT_S_BAND[1]):
    fails.append(f"hot {hot:.2f}s outside {HOT_S_BAND}")
if COLD_S_BAND is None:
    print("SKIP cold band: not pre-registered for this run")
elif not (COLD_S_BAND[0] <= colds <= COLD_S_BAND[1]):
    fails.append(f"cold {colds:.2f}s outside {COLD_S_BAND}")

if fails:
    print("\nPRECONDITION FAILURES -- the pass does not stand:")
    for f in fails: print("  -", f)
    sys.exit(3)
print("\nin band")
```

```sh
python3 analyse.py /root/bench.json /root/ravel/benchmarks/clickbench/hits.corpus.json
```

Rules that make a pass trustworthy:

- **Compare against a report file, never a typed-in number.** Read the previous
  pass's JSON; a hardcoded baseline cannot be checked against the run that
  produced it.
- **Never pipe a measurement through `tail`, `head`, or `grep`.** The pipeline's
  exit code is the last stage's, so a real failure reads as a pass. Redirect to
  a file and inspect the file.
- **Assert the statement count before reading any total.** A total that improved
  by dropping statements is the exact failure this check exists to catch.
- **Quote every headline with its statement count and selection rule.** A total
  over 42 statements and a total over 41 are different measurements, and mixing
  them in one row is not a comparison.
- **Confirm the change under test is in the tree.** Resolve the commit and
  assert it, or the pass attributes to code that is not there.
- **Run one thing at a time.** A concurrent build makes wall clock
  unattributable. Check for running `cargo`, `rustc`, or bench processes first.
- **The Class-S object-count half is a stated gap, not a passed check.** The
  script asserts the byte half and prints an explicit `SKIP` for the object
  half, because the report counts GET requests, not distinct objects touched;
  the comment above that `SKIP` in the script gives the figures.

**With the kit.** The bands are pre-registered in the lane's band file under
`benchmarks/clickbench/bands/`, and the box asserts the absolute ones itself:
a miss writes `DONE` with `bands=miss`. `benchmarks/clickbench/kit/fetch.sh
--run-toml run.toml <run-id>` polls both arms' `out/` prefixes every 60 s (no
single aws call over 90 s), downloads `results.json` and `evidence.tgz`, prints
each arm's absolute bands and then arm B's relative bands against arm A, and
exits 0 on a pass, 1 on a miss, 2 on `FAILED` or a malformed `results.json`,
3 on timeout and 4 when S3 is unreachable or a download fails with anything
but a missing key (a 403 and a missing bucket included). Two arms whose
dataset, mapping or corpus digest, `hardware.cpu`, RustFS version, audit mode, query window length
(`window_hours`) or resolved server settings differ, or that were judged by
different band sections, are not compared and exit 2, naming the field; each
arm's binary is not compared, since that is what differs by design. Arm B's
relative bands also miss when fewer statements than the band file's
`min_both_answered` were answered by both arms, when either arm answered none,
and when a statement both answered returned a different row count on B than
on A; each such miss names the statements. The script above reads an arm's
`results.json` unchanged, and its sums match the kit's. Its cost-class bands
need `per_run_accounting`, which the HTTP endpoint does not return, so on a
kit report it lists each statement as `no per_run_accounting; cold cost not
emitted` and exits 3; the kit's own bands are the verdict for a kit run.

## 10. Profiling a statement

The `profiling` feature emits a flamegraph. Use `--runs 1` so one execution
maps to one profile, and profile only a binary built with frame pointers --
`$PINNED` from step 8 is one, because that build carries
`RUSTFLAGS="-C force-frame-pointers=yes"`. Without them the unwinder cannot walk
out of inlined generic code and Ravel's own hot path lands in `[unknown]`.

```sh
LD_PRELOAD="$TCMALLOC" RAVEL_BENCH_PROFILE_SVG=/root/profile/q35.svg \
  "$PINNED" \
    --tenant clickbench-v4 --store s3 --compaction post --window-hours 200000 \
    --sql-max-segments 1000000 --deadline-secs 900 \
    --cache-bytes "$CACHE_BYTES" --fetch-concurrency 128 \
    --corpus /root/q35.corpus.json --runs 1
```

Build a single-statement corpus with `jq`:

```sh
jq '{version: .version, entries: [.entries[] | select(.id == "q35_top_urls_const_col")]}' \
  /root/ravel/benchmarks/clickbench/hits.corpus.json > /root/q35.corpus.json
```

Two checks in the binary fail the run rather than hand back a profile nobody
should read (issue #884). Both print to stderr and exit non-zero:

- Before sampling starts, it counts x86-64 frame-pointer prologues (`push %rbp;
  mov %rsp, %rbp`) in its own executable segments and refuses below a measured
  floor of 4,800. A build with the flag measures around 10,000 and one without
  it around 1,500, so this catches a `$PINNED` binary that was rebuilt without
  the `RUSTFLAGS`, which is otherwise indistinguishable from a good one until
  the flamegraph comes back empty. On an architecture or executable format the
  scan does not understand it says so and continues, never skipping silently.
- When the profile is summarised, the share of samples that resolved to no
  symbol (`[unknown]`) is printed once and, above 2%, refused with a message
  naming the measured share, the threshold, and the likely cause. A
  frame-pointer profile of this corpus measures 0.00%.

A profiler reporting 0% for a function and a symbol that never resolved look
identical. Before concluding a path is gone, check the symbol is present
(`nm`) and confirm with a positive control.

## 11. Teardown

Stopping through the API preserves the EBS volume, so the corpus and pinned
binaries survive and the box restarts ready to run. Terminating discards
them; the load must be repeated. Only `aws ec2 stop-instances` preserves the
volume: the box is launched with `--instance-initiated-shutdown-behavior
terminate` (section 5), so an in-guest `shutdown`, `poweroff` or `halt`
terminates the instance and discards the volume with it. A started box has
no safety timer until it is re-armed (section 5).

```sh
aws ec2 stop-instances      --instance-ids <id>   # keeps the volume, keeps billing for it
aws ec2 terminate-instances --instance-ids <id>   # discards everything
```

Removing the rest:

Read the bucket name out of SSM into `B` **before** deleting anything: the
parameter is deleted last, so the recursive object removal and the bucket
delete both have a name to use.

```sh
B=$(aws ssm get-parameter --name /ravel-clickbench/bucket \
  --query Parameter.Value --output text)

aws iam remove-role-from-instance-profile \
  --instance-profile-name ravel-clickbench-box --role-name ravel-clickbench-box
aws iam delete-instance-profile --instance-profile-name ravel-clickbench-box
aws iam delete-role-policy --role-name ravel-clickbench-box --policy-name ssm-read
aws iam delete-role --role-name ravel-clickbench-box

aws iam delete-user-policy --user-name ravel-clickbench --policy-name ravel-clickbench-s3
aws iam list-access-keys --user-name ravel-clickbench \
  --query 'AccessKeyMetadata[].AccessKeyId' --output text \
  | xargs -n1 -I{} aws iam delete-access-key --user-name ravel-clickbench --access-key-id {}
aws iam delete-user --user-name ravel-clickbench

aws s3 rm "s3://$B" --recursive
aws s3api delete-bucket --bucket "$B"

aws ssm delete-parameter --name /ravel-clickbench/access-key
aws ssm delete-parameter --name /ravel-clickbench/secret-key
aws ssm delete-parameter --name /ravel-clickbench/bucket
```

An idle instance bills at its full on-demand rate. Stop it when a run finishes.

**With the kit.** A kit box does not wait to be stopped: after uploading
`DONE` or `FAILED` it terminates itself through the instance role and then
runs `shutdown -h now` in every case, which the terminate behaviour turns
into a termination; a run that failed before reading its region uploads
nothing and only shuts down. From the moment `run.sh` starts writing `DONE`
or `FAILED` until that final shutdown, a signal (a hangup, an interrupt or
a terminate) also runs `shutdown -h now`, once. The `shutdown -h +600`
timer covers a box whose run hangs.
Nothing on its volume is kept; the results live under the run's `out/`
prefix. If `launch.sh` fails on the second arm, it terminates the first. The
run's `s3://<bucket>/<prefix>/runs/<run-id>/` objects stay until removed
with `aws s3 rm --recursive`.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `linking with cc failed` mid-build | Out of disk. Check free space; the error names the wrong cause. |
| Fold reports zero entries | Ran too soon after the load, or the wrong `--signal`/`--shards`. Use `--writers-stopped` once every writer has exited, and pass the tenant's real shard count. |
| Warm runs read the whole corpus | `--cache-bytes` is below the corpus size. Every run is cold. |
| Every declared column projects NULL | Queried before the declaration was visible. The staleness horizon applies to anything going through `ravel-server`. |
| `InvalidAccessKeyId` | The shell has no exported credentials, or SSM returned an empty value. Re-run the export block. |
| Resident memory climbs across runs | glibc arena growth, not a leak. Run under tcmalloc via `LD_PRELOAD`. |
| Bench exits non-zero, report looks fine | Non-zero whenever any statement fails, and none is expected to fail now that the per-query pool holds q33. Name the failing statement from the report (the identity checks in step 9 do); do not read the exit code as the verdict. |

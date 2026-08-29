# Running the ClickBench benchmark on AWS

A start-to-finish runbook for standing up the AWS environment, loading the
`hits` corpus into Ravel, and running a measured ClickBench pass.

[clickbench.md](clickbench.md) is the reference for what the harness measures
and how to read its report. This guide is the operational half: the
infrastructure, and the exact commands in the order they run.

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
the same credentials work unchanged against MinIO or any other S3-compatible
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
      "Resource": "*",
      "Condition": {"StringEquals": {"kms:ViaService": "ssm.$AWS_REGION.amazonaws.com"}}
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
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=ravel-clickbench}]' \
  --query 'Instances[0].InstanceId' --output text
```

An IMDSv2 token is required to read instance metadata from on the box:

```sh
T=$(curl -sX PUT http://169.254.169.254/latest/api/token \
      -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
curl -s -H "X-aws-ec2-metadata-token: $T" \
  http://169.254.169.254/latest/meta-data/instance-type
```

## 6. Bootstrap the box

Run as root. Builds the toolchain, both binaries, and downloads the dataset in
parallel with the build.

```sh
#!/bin/bash
# bootstrap.sh -- toolchain, checkout, release binaries, dataset.
export HOME=/root
set -uo pipefail
rm -f /root/BOOT_DONE /root/BOOT_FAILED
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
seals nothing. Force it with `--max-flush-lifetime 0s`:

```sh
$CLI --store s3 catalog fold \
  --tenant "$TENANT" --shards 8 --signal logs --max-flush-lifetime 0s
```

Check the output before continuing. `--signal` is not optional in practice: a
fold defaulting to the wrong signal on a logs-only tenant succeeds and seals
nothing.

```sh
$CLI --store s3 catalog fold \
  --tenant "$TENANT" --shards 8 --signal logs --max-flush-lifetime 0s \
  > /root/fold.out 2>&1
grep -E "watermark_hour|buckets_folded|entry_count|part_bytes" /root/fold.out
```

`entry_count` should be close to the number of objects the load wrote. If the
fold covered under 80% of them, the shard count or signal is wrong.

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
cd /root/ravel
git fetch origin main --quiet
git checkout -q --detach origin/main
HEAD_SHA=$(git rev-parse HEAD)

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
CACHE_BYTES=25769803776    # 24 GiB, larger than the 12 GB corpus

mkdir -p /root/explain

LD_PRELOAD="$TCMALLOC" "$PINNED" \
  --tenant clickbench-v4 --store s3 --compaction post --window-hours 200000 \
  --sql-max-segments 1000000 --deadline-secs 900 --continue-on-error \
  --cache-bytes "$CACHE_BYTES" \
  --sql-tenant-max-bytes 17179869184 \
  --sql-max-query-bytes 8589934592 \
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
| `--explain --explain-dir` | Captures a plan per statement. A null result cannot be diagnosed without it. |
| `--continue-on-error` | One failing statement does not abandon the pass. |

**The exit code is not the pass/fail signal.** It is non-zero whenever any
statement fails, and one statement fails by design on this corpus. Assert the
measured and failed counts instead, as step 9 does.

## 9. Check the report before reading the headline

A number printed and not asserted on is decoration. Write the expected figures
and their bands down **before** the run, then let a script fail the pass rather
than reading totals by eye. Two statements measured out of forty-three and a
zero exit code look identical to a clean pass otherwise.

```python
#!/usr/bin/env python3
# analyse.py -- integrity first, headline last. Exits non-zero on a miss.
import json, sys

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
    out = {}
    for d in load(path):
        for n in walk(d):
            sid = n.get("id")
            if isinstance(sid, str) and sid[:1] == "q":
                out[sid] = n
    return out

r = rows(sys.argv[1])
timed  = {k: v for k, v in r.items() if isinstance(v.get("min_ms"), (int, float))}
failed = sorted(k for k, v in r.items() if v.get("error"))

hot  = sum(v["min_ms"] for v in timed.values()) / 1000.0
cold = sum((v.get("cold_ms") or 0) for v in timed.values()) / 1000.0

# Cold is run 0; warm is every run after it. Summing all runs together hides
# which is which, and a cold figure quoted in a warm sentence overstates cost.
cb = cg = wb = wg = 0
for v in timed.values():
    pra = v.get("per_run_accounting") or []
    if pra:
        cb += pra[0].get("object_store_bytes") or 0
        cg += pra[0].get("object_store_get_requests") or 0
        for x in pra[1:]:
            wb += x.get("object_store_bytes") or 0
            wg += x.get("object_store_get_requests") or 0

print(f"measured {len(timed)}  failed {len(failed)} {failed}")
print(f"hot  {hot:.2f} s")
print(f"cold {cold:.2f} s")
print(f"cold {cb/1e9:.2f} GB in {cg} GETs")
print(f"warm {wb/1e9:.2f} GB in {wg} GETs")

fails = []
# Integrity: a total over a different statement count is not comparable.
if len(timed) != EXPECTED_MEASURED:
    fails.append(f"measured {len(timed)}, expected {EXPECTED_MEASURED}")
if len(failed) > EXPECTED_FAILED:
    fails.append(f"{len(failed)} failures, expected at most {EXPECTED_FAILED}")
# Then the pre-registered bands for this specific change.
if not (HOT_LO <= hot <= HOT_HI):
    fails.append(f"hot {hot:.2f}s outside {HOT_LO}-{HOT_HI}")
if not (COLD_LO <= cold <= COLD_HI):
    fails.append(f"cold {cold:.2f}s outside {COLD_LO}-{COLD_HI}")

if fails:
    print("\nPRECONDITION FAILURES -- the pass does not stand:")
    for f in fails: print("  -", f)
    sys.exit(3)
print("\nin band")
```

```sh
python3 analyse.py /root/bench.json
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

## 10. Profiling a statement

The `profiling` feature emits a flamegraph. Use `--runs 1` so one execution
maps to one profile.

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

A profiler reporting 0% for a function and a symbol that never resolved look
identical. Before concluding a path is gone, check the symbol is present
(`nm`) and confirm with a positive control.

## 11. Teardown

Stopping preserves the EBS volume, so the corpus and pinned binaries survive
and the box restarts ready to run. Terminating discards them; the load must be
repeated.

```sh
aws ec2 stop-instances      --instance-ids <id>   # keeps the volume, keeps billing for it
aws ec2 terminate-instances --instance-ids <id>   # discards everything
```

Removing the rest:

```sh
aws ssm delete-parameter --name /ravel-clickbench/access-key
aws ssm delete-parameter --name /ravel-clickbench/secret-key
aws ssm delete-parameter --name /ravel-clickbench/bucket

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

aws s3 rm "s3://$BUCKET" --recursive
aws s3api delete-bucket --bucket "$BUCKET"
```

An idle instance bills at its full on-demand rate. Stop it when a run finishes.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `linking with cc failed` mid-build | Out of disk. Check free space; the error names the wrong cause. |
| Fold reports zero entries | Ran too soon after the load, or the wrong `--signal`/`--shards`. Use `--max-flush-lifetime 0s` and pass the tenant's real shard count. |
| Warm runs read the whole corpus | `--cache-bytes` is below the corpus size. Every run is cold. |
| Every declared column projects NULL | Queried before the declaration was visible. The staleness horizon applies to anything going through `ravel-server`. |
| `InvalidAccessKeyId` | The shell has no exported credentials, or SSM returned an empty value. Re-run the export block. |
| Resident memory climbs across runs | glibc arena growth, not a leak. Run under tcmalloc via `LD_PRELOAD`. |
| Bench exits non-zero, report looks fine | Expected when any statement fails. Assert the measured and failed counts; do not read the exit code as the verdict. |

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

# Integrity (asserted BEFORE any band): 43 statements, one fails by design.
EXPECTED_MEASURED   = 42
EXPECTED_FAILED_Q   = {"q33"}      # by identity, not a count: q33 exhausts the
                                   # 8 GiB per-query budget (#837)

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
    if not pra:
        fails.append(f"{q}: no per_run_accounting; cold cost not emitted")
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
    for i, p in enumerate(phases):
        if not isinstance(p, dict):
            fails.append(f"{q}: wire_bytes_by_phase[{i}] is not an object ({p!r})")
            return None
        if p.get("phase") == "scan":
            return byte_count(p.get("wire_bytes"), f"{q} scan-phase wire_bytes")
    fails.append(f"{q}: no scan-phase wire_bytes entry in the measured run")
    return None

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
# Counts alone are not integrity. They are satisfied by the wrong 42 rows: an
# omitted Class-F statement replaced by an unrecognised `q` row keeps the count
# at 42, and "at most one failure" is also satisfied by zero failures plus one
# statement that never ran. So the IDENTITIES are checked, not the totals.
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
    m_gets  += acc.get("object_store_get_requests") or 0
    m_bytes += acc.get("object_store_bytes") or 0
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
    b = acc.get("object_store_bytes") or 0
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
    s_grp_bytes += acc.get("object_store_bytes") or 0
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

f_qs = sorted(q for q in by_q if cls.get(q) == "full_value")
if not f_qs:
    fails.append("no Class-F statements found in the corpus; the Class-F band "
                 "cannot be evaluated and must not silently pass")
amplification(sorted(by_q), "corpus-wide", CORPUS_AMPLIFICATION_MAX)
amplification(f_qs,         "class F",     F_AMPLIFICATION_MAX)

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
- **The Class-S object-count half is a stated gap, not a passed check.** Issue
  #913 phrases the Class-S target as "<=5% of corpus bytes AND object count".
  The script asserts the byte half and prints an explicit `SKIP` for the object
  half: the report records object-store GET requests, and GETs are not distinct
  objects (q37..q43 issue about 9,694 requests against a 3,469-object corpus,
  2.8x more requests than objects exist), so comparing GETs to a fraction of the
  object count compares two different quantities. Closing the gap needs a
  distinct-objects-touched counter in the read path; until that counter exists,
  the object half stays skipped rather than asserted on the wrong quantity.

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

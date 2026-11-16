# Measuring pinned-walk expiry on AWS

This page runs the issue #3779 item 1 measurement: how often a paged
`ravel_query_sql` walk whose order needs a pinned segment set expires, and
why. It covers one self-terminating EC2 box, the command that launches it,
what it costs, and how to read what it uploads. The pre-registration is the
bands file the bin enforces,
[`mcp_walk_expiry.bands.toml`](../../crates/ravel-bench/src/bin/mcp_walk_expiry.bands.toml).
The table in section 5 restates it.

The scripts are under
[deploy/walk-expiry/aws/](../../deploy/walk-expiry/aws/):

| Script | Runs on | What it does |
|---|---|---|
| `launch.sh` | your machine | uploads the source tarball and `box-run.sh`, then starts one instance that terminates on shutdown |
| `user-data.tpl` | the box, at boot | arms `shutdown -h +600`, installs the AWS CLI, fetches `box-run.sh` and checks its checksum |
| `box-run.sh` | the box | builds `mcp_walk_expiry` at the pinned commit, runs it, uploads the results or a `FAILED` marker, and shuts down |
| `launch.test.sh` | CI's `doc-scripts` job | checks the `--dry-run` output |

## 1. What the bin measures

`mcp_walk_expiry` starts two servers in one process over the store bucket: a
`--mode all` server (ingest, query, scheduled fold, `POST /mcp`) and a
`--mode maintain` server (compaction). Shards, flush cadence and
concurrency, fold and maintenance intervals, and the ingest and query
concurrency limits are the values `ravel-server`'s own flag parser gives
with no flags set. A few settings `main` derives from the host or from flags
the probe does not pass are set directly: the query audit is off, the query
budgets and the S3 request budget are the library defaults, the read and
catalog caches are 256 MiB in memory, admission limits are the defaults, and
there is no retention. A budget can refuse a page, which the run counts as a
failed walk, but it does not change which segments a resolve lists. A load task posts the MetricsBench
workload's families (10,000 active series, one scrape every 15 s) over
Remote Write 1.0, each scrape at the instant it is stamped with.

A walk is page one plus three redemptions. Each page runs through
`QueryService::sql_execute_at` with the request `ravel_query_sql` builds for
it, and later pages resolve at page one's t0. A walk expires at the first
page whose segment set differs from page one's. Each expiry is labelled from
the key diff: commit, compaction (an `l0/` key replaced by an `l1/` key of the
same ingest hour), fold or retention, or other.

The grid is two statements, four windows and three delays (1 s, 10 s,
60 s), 24 cells of 60 walks each, at most 6 walks at once. Twenty S1 walks
over Wp and twenty over Wo also go through `POST /mcp` itself, at the 10 s
delay, as the cross-check.

Two statement details differ from the design text. S2 is
`SELECT series_id, count(*) AS c ... GROUP BY series_id ORDER BY c DESC,
series_id`. The explicit `series_id` term is there because the page planner
appends no tiebreak for it: without one every series with the same count is
one group, and `ravel_query_sql` cannot page the result at all. Both
statements group, so the planner never calls their order total, and the probe
breaks each page where the adapter does: it holds back a group still open at
the page's end.

## 2. The 10-hour cap and the window ages

The box must terminate within 10 hours. `user-data.tpl` arms
`shutdown -h +600` before anything else, and `launch.sh` sets the instance's
shutdown behaviour to terminate. The design's 26 hours of load before the
first walk, and its Wo window `[t0-26h, t0-25h)`, do not fit.

The run keeps the four window roles and shrinks the old windows' ages
(option b). It does not run the server under a faster injected clock (option
a). The Remote Write samples carry their own wall-clock timestamps, S3 stamps
its objects with real time, and the MCP adapter reads its own system clock.
Running ingest, fold and compaction on an accelerated clock would measure a
system whose clocks disagree, which no deployment runs.

| Window | Range | Role |
|---|---|---|
| Wp | `[t0-1h, t0)` | ends at the mint |
| Wf | `[t0-1h, t0+1h)` | still open during the walk |
| Wr | `[t0-3h, t0-2h)` | a few hours old, possibly compacting |
| Wo | `[t0-5h, t0-4h)` | old and sealed (the design had `[t0-26h, t0-25h)`) |

Wo's newest ingest hour ends at most at t0-3h plus the commit lag. At the
shipped defaults compaction may take an hour 65 minutes after it ends, on
its next 5-minute maintenance pass, and a fold seals it 80 minutes after it
ends, on its next 5-minute fold pass. So every Wo hour is compacted and sealed about 1.5 hours before
any Wo walk starts. The `wo_sealed` stamp checks each Wo walk against the
fold watermark rather than trusting this arithmetic.

The walks run in three phases, each starting once its windows hold data:
Wp and Wf from 1 h 05 min after load start, Wr from 3 h 05 min, Wo from
5 h 05 min. Inside a phase the cells' walks are interleaved, so each cell's
starts spread over the whole phase. A phase lasts more than an hour and
covers every point in the hour, but the design's 3-hour spread is not kept.
The walk phase stops at 7 h 30 min after load start. Any cell still short of
its walks then is reported missing, and the run fails.

## 3. Prepare the account

1. **The results bucket**, `WE_RESULTS_BUCKET`, receives each run's source
   tarball and box script, and then its log, results and marker under
   `walk-expiry/<run-id>/`.
2. **The store bucket**, passed as the second argument, is the Ravel store.
   It must be empty, and in the box's region. Use a new one per run: an old
   run's objects would be read as this run's history.
3. **The instance profile**, `WE_INSTANCE_PROFILE`, is the sandbox profile.
   Its role needs `s3:GetObject` and `s3:PutObject` on the results bucket,
   and `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket`
   on the store bucket. The probe reaches S3 through the instance role
   (`RAVEL_S3_AUTH=instance-role`). No credentials are stored anywhere.

## 4. Launch

From a checkout that has the commit you measure:

```sh
export AWS_PROFILE=<profile> AWS_REGION=us-east-1
export WE_RESULTS_BUCKET=<results bucket>
export WE_INSTANCE_PROFILE=<sandbox instance profile>
export WE_SUBNET_ID=<subnet with outbound internet>
export WE_SECURITY_GROUP_ID=<security group allowing outbound traffic>
git fetch origin main
commit=$(git rev-parse origin/main)
deploy/walk-expiry/aws/launch.sh --dry-run "$commit" <store-bucket> <label>
deploy/walk-expiry/aws/launch.sh "$commit" <store-bucket> <label>
```

`launch.sh` refuses a commit that is not 40 lowercase hex characters, a
store bucket given as a URL or equal to the results bucket, and a label
outside `[a-z0-9-]`. It prints `instance=<id> run=<run-id>
prefix=s3://<results bucket>/walk-expiry/<run-id>`. `--dry-run` prints every
`git` and `aws` command and the rendered user data, and runs none of them.

The box builds `--release --features walk-expiry` from the uploaded source
and copies the binary to `/we/bin/<commit>/mcp_walk_expiry`. The run's
binary-path stamp refuses any path that does not name a 40-character
commit.

The subnet needs outbound internet for apt, rustup and crates.io.

## 5. The pre-registration

Every figure is emitted exactly once and must fall inside its band, or the
bin exits non-zero. All are predictions; there is no prior measurement.
c is the median gap between commit rounds and L the largest event-time lag
of a commit, both from the stamps; Δ is the cell's delay. The table is the
bands file's `[production.bands]`.

| Figure | Prediction | Band | Miss means |
|---|---|---|---|
| `finish_share.{s1,s2}.wp.d{1s,10s,60s}` | exp(-min(3Δ, L)/c) | prediction ± 0.15, clipped to [0, 1] | the overlap model of ADR-1374 item 5 is wrong |
| `finish_share.{s1,s2}.wf.d{1s,10s,60s}` | exp(-3Δ/c) | prediction ± 0.15, clipped to [0, 1] | the overlap model of ADR-1374 item 5 is wrong |
| `commit_cause_share.wp_wf` | ≥ 95% | [0.95, 1.0] | something besides ingest moves a live window |
| `finish_share.{s1,s2}.wr.d60s` | 90% | [0.70, 1.0] | compaction or fold expiries matter |
| `finish_share.{s1,s2}.wo.d{1s,10s,60s}` | 100% | [0.98, 1.0] | an unexplained set change |
| `commit_expiries.wr_wo` | 0 | [0, 0] | a bug (a non-overlapping commit kept), not a rate |
| `fold_expiries.wo.s2` | 0 | [0, 0] | an old hour sealed again |
| `short_walks` | 0 | [0, 0] | a vacuous cell, void it |
| `failed_walks` | 0 | [0, 0] | the probe measured errors, not expiries |
| `unresolved_hour_keys` | 0 | [0, 0] | the compaction label could not be decided for some expiry |
| `decision.wr_compaction_fold_share.d60s` | decision input, threshold 0.10 | [0, 1.0] | not a share |
| `mcp_crosscheck.abs_diff.s1.{wp,wo}.d10s` | within 10 pp | [0, 0.10] | the probe does not reproduce what `ravel_query_sql` does |

`failed_walks` and `unresolved_hour_keys` are not in the design. Without
them, a walk that ended on a query error, or an expiry whose compaction label
could not be decided, would be counted without being visible. The decision
figure has no pass band. Above 0.10, open the L0-identity digest ticket (a
ravel-catalog Snapshot change). Otherwise, close the checkbox with the
figure.

What the shorter run does not keep:

- **Wo is 4 to 5 hours old, not 25 to 26.** The 100% prediction and its
  [0.98, 1.0] band stand, but they now test hours sealed about 1.5 hours
  earlier, not a day earlier. A set change that only shows up on day-old
  hours is out of reach.
- **Starts spread over each phase**, at least 1 hour, not over 3 hours.
- **The cross-check's band is tight for its size.** Twenty walks have a
  sampling error of up to 11 pp on a share near one half. The band holds
  only if both shares sit near 0 or 1, which is what the predictions expect
  at Δ = 10 s.

The stamps void the run. They are read from `[production.stamps]`:

- every 10-minute ingest-rate window of the walk phase within 10% of the
  generated rate, and no Remote Write request refused;
- c and L from a listing of the last 10 minutes' commits, every 10
  minutes, each listing holding at least 10 commit rounds (commits within
  500 ms of a round's first are one round: the shards flush together);
- hour-end-to-first-L1 latency, at least one observed;
- the fold seal time of every ingest hour a Wr or Wo walk read, and every
  Wo walk's newest hour sealed at its t0;
- no page with a pending erasure;
- retention at least 3 days (the run configures none, which never
  age-deletes);
- the binary's path names its commit.

## 6. Wall time and cost

Expected wall time, none of it measured yet:

| Step | Time |
|---|---|
| boot, apt, rustup, release build | about 30 min |
| warmup before the first walk | 1 h 05 min |
| phase Wp/Wf (12 cells and 20 MCP walks) | about 2 h 25 min to 2 h 55 min |
| phase Wr (6 cells) | about 1 h 10 min to 1 h 25 min |
| wait for Wo's data, then phase Wo (6 cells and 20 MCP walks) | about 1 h 15 min to 1 h 30 min |
| report and upload | a few minutes |

The bin finishes about 6 h 20 min to 6 h 50 min after load start, and stops
itself at 7 h 30 min. The box shuts down a minute after the upload, so it
runs about 7 to 7.5 hours, and never past the 10-hour cap.

Cost, at us-east-1 on-demand list prices (check them before launching):
the c6a.4xlarge is about $0.61 an hour, about $4.50 for the expected run and
$6.12 at the 10-hour cap. The 100 GB gp3 volume adds about $0.10. S3
requests are estimated at $1 to $3, mostly commit PUTs and resolve LISTs.
Expect about $6 to $8 in total, at most about $9.

## 7. Fetch and read the results

```sh
prefix=s3://<results bucket>/walk-expiry/<run-id>
aws s3 ls "$prefix/"
aws s3 cp "$prefix/box.log" -          # uploaded every 10 minutes while it runs
aws s3 cp --recursive "$prefix/out/" ./walk-expiry-out/
```

The box uploads `DONE` on success, or `FAILED` holding the stage and exit
code (`stage=build exit=16`, `stage=run exit=1`). A `FAILED` from user data
(`stage=user-data`) means the box script never started. `out/` holds:

- `summary.txt`: one line per figure with its value, band and prediction,
  then every `FIGURE:`, `VOID:` and `ERROR:` line, the decision, and
  `RESULT: PASS` or `RESULT: FAIL`;
- `result.json`: the configuration, the stamps, every cell's tally (walks,
  finished, expired, short, failed and the count of each cause label), every
  figure with its band, the figure errors, the void reasons and the
  decision.

The bin exits 0 only when every figure is present once and in band, every
cell ran all its walks, and no stamp voids the run. It exits 1 naming each
failure and 2 when it could not measure at all. A `stage=run exit=1` still
uploads `out/`. Read `summary.txt` first: a `VOID:` line means the figures
are not a result, whatever they say.

To run the same bin locally in seconds, against a `MemoryStore` and with
wide bands:

```sh
cargo run -p ravel-bench --features walk-expiry --bin mcp_walk_expiry -- \
    --profile smoke --store memory --out-dir /tmp/walk-expiry-smoke
```

## 8. Before launching

The owner's standing approval covers MetricsBench boxes for #2821 only.
Post the pre-registration in section 5 on #3779 and get the owner's go
before you launch.

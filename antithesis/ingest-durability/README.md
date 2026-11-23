# ingest-durability

An Antithesis workload (index: [antithesis/README.md](../README.md)). Tests Ravel's ingest acknowledgement and visibility contract
(docs/consistency-model.md) under faults: a strictly acknowledged write is
never lost, is readable with its commit tokens, and is visible to queries once
faults stop; no stored record or sample is a phantom, and no stored log record
is stored more often than the attempts that could have written it (sample-level
duplication is not asserted for metrics).

## Deployment

`config/docker-compose.yaml`:

- `rustfs`: the only durable component, a RustFS container (the release and
  digest the repository's quickstart stacks and the object-store contract CI
  lane already run). RustFS supports every operation Ravel requires,
  including create-if-absent and compare-and-swap. RustFS's documentation
  (docs/operations/durability-modes.md in rustfs/rustfs) describes its default
  mode, `strict`, as what it has always shipped: payload, metadata and
  directory entries are fsynced before a PUT is acknowledged. The compose file
  sets that mode explicitly, but the 1.0.0 image does not log the mode it
  resolved, so this rests on RustFS's documentation rather than on an
  observation. Ravel promises only the store's own durability floor, so a store
  that lost acked PUTs would show up as a Ravel loss. When a durability failure
  follows a `rustfs` kill, suspect the store first. MinIO was not chosen because nothing in this repository runs
  against it, while RustFS is the backend the contract CI lane checks.
- `init`: a one-shot that creates the bucket and runs `ravel-cli store
  qualify`, which ravel-server requires before it serves.
- `ravel1`, `ravel2`: two `ravel-server --mode all` processes over one bucket.
  Each one ingests, serves queries and folds the catalog. Two `all` processes
  over one bucket are supported: the fold's HEAD compare-and-swap serializes
  them (docs/catalog-and-mvcc.md). There is no `maintain` process, so nothing
  compacts or deletes. The flush delays are 500 ms (strict) and 5 s (idle).
- `driver`: the workload, excluded from faults.

## Driver and model

The driver is Rust, a [hegel](https://hegel.dev) concurrent state machine
(`driver`). Ravel has no client library: the driver speaks
OTLP/HTTP and the SQL endpoint itself. Each rule picks a node at random:

- `write_logs` and `write_metrics` send one OTLP/HTTP export. The mode is
  strict (60%), `strict-arrival` (20%) or buffered (20%). Some writes are gzip.
  Some have a short 2 s client timeout, which drops the request while the server
  is still working on it. Logs fan out over 1 to 4 streams, so one request can
  span several shards. Half of the log writes carry an
  `x-ravel-idempotency-key`. A write with no definite outcome (no response,
  503, 429) is retried up to twice, the way an OTLP exporter retries. An
  acknowledged keyed write is sometimes deliberately retried with the same key.
- `read_with_tokens` reads one acknowledged write back through
  `POST /api/v1/sql` with its `min_commit_token`s.
- `scan_without_token` reads every workload row in the window, with no token.
- `eventually` is a rare draw. It stops faults (through the
  `ANTITHESIS_STOP_FAULTS` command the platform sets in every container; unset
  in the local loop, which injects none), waits for both nodes to serve, checks
  every acknowledged write and then ends the timeline.

Every record carries its write id and its index within the write: the
`wl.write` and `wl.rec` attributes on logs, and the value
`write_id * 100 + index` on samples. A query result therefore maps every
stored row to exactly one record sent. The model registers a write and an
in-flight attempt before the request is sent. Each attempt is then recorded as
one of:

| Attempt | Meaning |
|---|---|
| acked | strict 200, no partial success, an `x-ravel-commit-token` header |
| buffered ack | buffered 200; never asserted present |
| partial / refused / no response | may or may not be stored, now or later, for the rest of the timeline |

A write's records must be present once any attempt is acked. Each record may
be stored at most once per attempt. An acknowledgement whose token header is
byte-identical to an earlier one's is an idempotency replay and adds no copy.

## Properties

Guarantees (`assert_always` / `assert_unreachable`), each citing the section of
docs/consistency-model.md it implements:

| Assertion message | Where | Contract |
|---|---|---|
| a read with an acked log write's commit tokens returns every record of that write | driver | Acknowledgement semantics (strict); Read-your-write |
| a read with an acked metrics write's commit tokens returns every sample of that write | driver | same |
| every stored workload log record belongs to a write the workload sent | driver | Acknowledgement semantics; Visibility (atomic per object) |
| every stored workload sample matches a sample the workload sent | driver | same |
| no log record is stored more often than the attempts that could have written it | driver | Duplicates and idempotency (at-least-once, at most one copy per delivery) |
| a keyed log retry that replays the original commit token stores no new copy | driver | Opt-in client idempotency key, replay contract |
| a clean strict 200 response carries a commit token | driver | Acknowledgement semantics (strict) |
| a buffered-mode acknowledgement carries no commit token | driver | Buffered mode |
| a well-formed write was refused as non-retryable (HTTP 400) | driver, unreachable | Rejection |
| the system recovers once faults stop | driver, eventually | Architecture: every process is disposable |
| acked writes are readable with their commit tokens once faults stop | driver, eventually | Acknowledgement semantics: no crash may lose a strict ack |
| acked writes are visible without a token once faults stop | driver, eventually | Catalog snapshot staleness (listing-immediate freshness in healthy operation) |
| logs ingest: an idempotency marker read back undecodable | ravel-server, unreachable | markers are immutable, written once with create-if-absent |

A no-token read during faults asserts only the upper bounds: no phantoms and
no excess copies. Freshness without a token is not guaranteed while faults
run. The no-token visibility assertion in the eventually can also fail for the
documented clock exceptions under "Catalog snapshot staleness". A large
forward clock skip that seals an hour while a commit is still in flight into
it is one such case. Triage a failure of that assertion against the token-read
assertion beside it: when token reads pass, the data is durable and only
visibility is in question.

Exploration assertions:

- In the driver: the workload starts; each write outcome (acked, buffered
  ack, no response, 503, 429, partial success); a strict ack spanning more
  than one shard; a write acknowledged on retry after an indefinite attempt; a
  keyed retry that replays the original token, and one that stores a new copy;
  a token read refused as unavailable; a no-token read that sees an acked
  write; a record found more than once; a write with no definite outcome later
  found stored.
- In ravel-server, behind the off-by-default `antithesis` cargo feature
  (`services/ravel-server/src/{logs_ingest,ingest,otlp_http,main}.rs`): an
  idempotency replay; a marker lookup that failed closed; a marker write that
  failed after a durable commit; a partial multi-shard commit (logs and
  metrics); `AckTimeout`, `Abandoned` and `ShardUnavailable` answers (logs and
  metrics); a graceful drain started by a signal; and a drain that did not
  complete cleanly.

Some of these may stay unreached even in long runs, so read their absence as
coverage, not as a finding. The 429 shed needs a process-wide budget to fill.
`ShardUnavailable` needs a dead shard actor. An unclean drain needs a stop
fault during a store outage. A partial success needs a clock skip larger than
the 10 minute future-skew bound in the middle of a request. The coverage
variant (below) reaches the 429 shed, partial successes and drains on purpose.

The flush, retry and abandon logic itself lives in `ravel-ingest`. It carries
no assertion sites yet, so its outcomes are observed at the handler instead.
The image's ravel-server and ravel-cli are both built with the `antithesis`
feature, which also reaches ravel-commit, ravel-ingest, ravel-catalog,
ravel-maintain and ravel-fleet through `ravel-invariant` (ADR-4326), so an
assertion site added in one of those crates reports from this workload without
an image change.

## Coverage variant

`config-coverage/docker-compose.yaml` runs the same images in a config that
reaches outcomes the base config almost never does. Ravel itself is
unchanged; three levers do it:

- `--max-pending-strict-acks 2` on ravel1 and ravel2. The driver runs four
  rules at once, so a third strict write to one node is shed with 429 at the
  request head, before its body is read (logs and metrics).
- Poison records. With `WORKLOAD_COVERAGE=1`, about one strict, unkeyed log
  write in five also carries one or two records whose body is over the 64 KiB
  log body limit, and about one strict metrics write in five carries a point
  with a label value over the 4096-byte limit. Ravel rejects those records one
  by one, with an OTLP partial success, and stores the others. A poison record
  has a record index (logs) or a value index (metrics) past the write's own
  records, so if one is ever stored, the existing phantom and foreign-sample
  assertions fail. Poison writes are never keyed, because a keyed retry can
  replay the first receipt, which carries no partial success.
- Graceful drains. ravel1 and ravel2 run under `ravel/supervise.sh`
  (`/usr/local/bin/ravel-antithesis-supervise`), which starts ravel-server
  as its child. About once a minute, the driver's `drain` rule writes a
  nonce to `/control/drain-<host>`, a volume shared with the nodes. Once the
  child has answered `/readyz` and 25 more polls of 0.2 s have passed
  (counted in sleeps, so a clock jump does not shorten the wait), the
  supervisor moves the nonce to `/control/draining-<host>`, sends the child
  SIGTERM, writes the exit code to `/control/drained-<host>`, and starts it
  again. At startup it answers a request or an unfinished drain left by a
  killed container as `interrupted`, so every drain it took gets an answer.
  A stop of the container is passed on to ravel-server; once ravel-server
  has exited, the supervisor exits 143 when ravel-server ended the way a stop
  ends it (0, 1 for a drain cut short by the shutdown timeout, or 143), and
  with ravel-server's own code otherwise, so a crash during a stop still
  shows. Any other exit the supervisor did not ask for ends it with
  ravel-server's exit code, as in the base config. The driver keeps one drain in flight at a time. When the
  eventually starts, it withdraws a request the supervisor has not taken and
  waits up to 180 s for the answer to one it has.

A strict 2xx whose partial success rejected exactly the poison records, with
a commit token, counts as an acknowledgement of the write's other records:
the poison is rejected for its size alone, so the count names exactly those
records. A partial success that rejected more (a clock skip can also reject
good records) stays an outcome-unknown attempt, as in the base config.

Properties added by the variant. They are reported only when
`WORKLOAD_COVERAGE=1`: the driver sends them through `assert_raw` and
catalogues them at startup, so a base run's property list does not change.

| Assertion message | Kind | Contract |
|---|---|---|
| a write shed at the request head on every attempt stores nothing | always | Rejection: a head-of-request 429 (pending strict-ack bound, in-flight ceiling) touches no shard; checked against every no-token scan |
| a strict write's partial success rejects every poison record | always | Rejection, per record |
| a strict partial success that rejected only the poison records carries a commit token | always | Acknowledgement semantics (strict): the accepted records are committed |
| a write answered with an error status carries no commit token | always | Acknowledgement semantics: only a commit issues a token |
| a driver-requested drain is answered or reported interrupted | always | Graceful shutdown: a drain ends, within `--shutdown-timeout`; false when a drain the supervisor took has no answer 180 s after faults stop |
| a driver-requested drain exits with status 0 or 1 | always | Graceful shutdown: a drain completes, or reports its failure and exits 1 |
| a driver-requested drain exited cleanly | sometimes | same |
| a strict write with poison records was acknowledged with commit tokens | reachable | |
| a token read checked an acknowledged write that carried poison records | reachable | |
| a metrics write was answered 429 | reachable | |

The flush deferral cap's 429 is left out of the head-shed property: a log
write that spans several shards can commit on one shard before another
refuses it. ravel-server reports ready a little before it installs its
SIGTERM handler (#4493), so a drain can still end with exit code 143 despite
the wait after readiness. The driver logs that case
(`exited 143: SIGTERM arrived before ravel-server installed its shutdown
handler`) and leaves it out of the exit-code property. It is not an
assertion: a reachable that fault-free timing rarely reaches would fail most
runs.

`CONFIG=config-coverage` selects the variant in `scripts/local-run.sh` and
`scripts/launch.sh`. A run is named `ravel-ingest-durability-coverage`
with `attrs.variant=coverage`; mutant runs stay on the base config. The
local loop then also fails unless every `required` assertion in
`local/coverage-required.tsv` was hit with its condition true. The `phase-b`
lines there (a partial multi-shard commit, `ShardUnavailable`, metrics
`Abandoned`) need fault switches inside Ravel that are not built yet; the
loop reports them and does not gate on them.

`WORKLOAD_COVERAGE_SELF_CHECK=1` shows that each always-property of the
variant can fail. The driver then reads a 429 as if it carried a commit
token, a partial success as if it carried none, expects one poison record
more than it sent, and counts every acknowledged write whose id is a
multiple of 7 as shed at the head; the supervisors kill the first drain with
SIGKILL and take every later one without answering. The drawn eventually is
off, so the forced one runs after the broken drains. The run passes only if
every `self-check` line of `local/coverage-required.tsv` was violated.

```sh
CONFIG=config-coverage antithesis/ingest-durability/scripts/local-run.sh out/coverage1 900
CONFIG=config-coverage WORKLOAD_EVENTUALLY_AFTER_SECS=600 \
    antithesis/ingest-durability/scripts/local-run.sh out/coverage2 900
CONFIG=config-coverage WORKLOAD_COVERAGE_SELF_CHECK=1 WORKLOAD_EVENTUALLY_AFTER_SECS=180 \
    antithesis/ingest-durability/scripts/local-run.sh out/coverage-self-check 900
CONFIG=config-coverage antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "<description>"
```

`LOCAL_COMPOSE_OVERRIDE=<file>` layers one more compose file over a local
run, for example to name image tags other than `:ingest-durability` on a
build host that other work shares.

## Fault-switch variant

`config-faults/` is the coverage variant with Ravel's own Antithesis-only
fault switches on in both servers (ADR-4326, fault-switch amendment; #4496):
`RAVEL_ANTITHESIS_FAULTS=ingest/encode-cancelled=0.05,ingest/data-put-refused=0.05`.
A cancelled encode makes a shard answer ShardUnavailable for that flush, and
a refused data PUT abandons it, so a write that spans shards can commit on
some and fail on others. These are the outcomes the 10 s strict-ack deadline
otherwise hides from store faults (the "phase-b" lines of
`local/coverage-required.tsv`). The driver's model already treats such
answers as "may or may not be stored".

Each server reads `RAVEL_ANTITHESIS_FAULTS` once at startup, and
`ANTITHESIS_STOP_FAULTS` does not reach it. So once faults stop, the
eventually writes `/control/faults-off`, asks each node's supervisor for a
drain and waits for each to restart: the supervisor starts every child after
that file appears without `RAVEL_ANTITHESIS_FAULTS`, including after a
container restart. The driver asserts "every node restarted with its fault
switches off before the eventually's checks", and every recovery check runs
after it.

`local/faults-required.tsv` gates the local loop: both switches must fire,
the switch-off restart must succeed, and a strict partial success must be
seen. The shard-level reachables are listed and reported, not gated.

```sh
CONFIG=config-faults WORKLOAD_EVENTUALLY_AFTER_SECS=600 \
    antithesis/ingest-durability/scripts/local-run.sh out/faults1 900
CONFIG=config-faults antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "<description>"
```

For a deterministic check of one switch, layer an override that sets its
rate to 1 and the other to 0 with `LOCAL_COMPOSE_OVERRIDE`.

## Perf variant

`config-perf/` is the base config with `WORKLOAD_PERF=1` on the driver
(`driver/src/perf.rs`). In the eventually, after faults stop, the driver
measures two figures and reports them, and once the system recovers it asserts
three Always bounds on them (#4559): P1, every node's first strict ack within
60 s of faults stopping; P2, every post-recovery strict write answered within
the client timeout; P3, every recovered node's median strict-ack latency
within 2 s. `WORKLOAD_PERF=1` together with `WORKLOAD_COVERAGE=1` is refused:
the driver exits 1 before setup complete.

- t0 is taken immediately after the eventually runs `ANTITHESIS_STOP_FAULTS`.
  The driver tells apart three results: the variable is absent (`unset`), the
  command ran and exited 0 (`ok`), or it could not be spawned or exited
  non-zero (`failed`). On `failed` the variant prints
  `perf skipped reason=stop-faults-failed`, hits the unreachable below and
  measures nothing; the rest of the eventually runs as in the base variant.
- Figure (a), time to first strict ack (TTFA), per node. From t0, beside the
  existing recovery wait, one prober per node sends a probe write to its own
  node every 1 s, each with a 5 s client timeout, with at most 5 in flight. It
  stops at the first answer the driver classifies as acked, or 300 s after
  t0. The sample is the time from t0 to the receipt of that first ack. No ack
  within 300 s gives the sample `none`. Both probers are joined before the
  recovery assertion's exit and before the 5 s settle sleep.
- Figure (b), strict-ack latency after recovery, per probe. Only when the
  system recovered and both TTFA samples acked: after the 5 s settle sleep,
  one thread per node sends 10 probe writes to its own node, one after the
  other, alternating logs (even) and metrics (odd), each with the 15 s write
  timeout (except under the self-check below, which shortens probes 2, 5 and 8
  of each node to a 1 ms timeout). The interval is the HTTP call alone. Per
  node, the median is the
  element at index 5 of the 10 latencies sorted ascending, with every
  probe that was not acknowledged counted as infinity, so it is finite only
  when at least 6 probes acked.

A probe write is strict, unkeyed, not gzip, one log record in one stream or
one sample. It is registered in the model before it is sent, past the
`WORKLOAD_MAX_WRITES` cap, and judged and recorded the way a rule's write is,
with no retry. The ack probes run before the eventually takes its list of
acknowledged writes, so its token reads and scans check the probe writes
too. No probe takes the rule gate or a stop helper. Probe writes go through
the same judging as rule writes (`judge_reply`), so in the perf variant they
can also satisfy the existing reachability assertions about write outcomes,
such as "a log write was answered 503".

The clock is the driver's monotonic clock (`mono_ms`). Each figure also
carries the signed wall-clock difference over the same interval
(`wall_ms`, negative if the wall clock went back), and the log lines carry
both, for comparison against the event log's vtime.

A sample of `none`, a probe that was not acknowledged and a median of `inf`
count against every "within" rung of their ladder; none is dropped. Every
assertion goes through `assert_raw` and is catalogued only when
`WORKLOAD_PERF=1`, so other variants' property lists do not change.

| Assertion message | Kind | Evaluated |
|---|---|---|
| a node's first strict ack came within 5 s of faults stopping | sometimes | once per node per eventually |
| a node's first strict ack came within 15 s of faults stopping | sometimes | once per node per eventually |
| a node's first strict ack came within 30 s of faults stopping | sometimes | once per node per eventually |
| a node's first strict ack came within 60 s of faults stopping | sometimes | once per node per eventually |
| a node's first strict ack came within 120 s of faults stopping | sometimes | once per node per eventually |
| a node's first strict ack came within 300 s of faults stopping | sometimes | once per node per eventually |
| a strict write after recovery was acknowledged within 1 s | sometimes | once per ack probe |
| a strict write after recovery was acknowledged within 2 s | sometimes | once per ack probe |
| a strict write after recovery was acknowledged within 5 s | sometimes | once per ack probe |
| a strict write after recovery was acknowledged within 10 s | sometimes | once per ack probe |
| a strict write after recovery was answered within the client timeout | sometimes | once per ack probe; false only for no response |
| a node's median strict-ack latency after recovery is within 1 s | sometimes | once per node per eventually |
| a node's median strict-ack latency after recovery is within 2 s | sometimes | once per node per eventually |
| a node's median strict-ack latency after recovery is within 5 s | sometimes | once per node per eventually |
| a node's median strict-ack latency after recovery is within 8 s | sometimes | once per node per eventually |
| a perf interval saw the wall and monotonic clocks disagree by more than 1 s | sometimes | once per acked TTFA sample and per ack probe |
| the driver could not stop faults before measuring | unreachable | hit when stop-faults failed |
| the perf probes ran in an eventually | reachable | once per eventually, with the summary line |
| every node's first strict ack comes within 60 s of faults stopping | always | once per node per eventually, only in a recovered timeline |
| every strict write after recovery is answered within the client timeout | always | once per ack probe, only when the ack probes ran |
| every recovered node's median strict-ack latency is within 2 s | always | once per node per eventually, only when the ack probes ran |

That is 21 assertions catalogued at startup under `WORKLOAD_PERF=1`: 16
Sometimes, 1 Reachable, 1 Unreachable and the 3 Always above.

The Always bands, each with its own source:

| Always | Band | Source |
|---|---|---|
| P1, first strict ack after recovery | 60 s | #4559 comment 7088441883, perf-measure-1: the next rung above the smallest rung with zero false over 952 TTFA samples (30 s) |
| P3, median strict-ack latency | 2 s | #4559 comment 7088441883, perf-measure-1: the next rung above the smallest rung with zero false over 938 medians (1 s) |
| P2, post-recovery write answered | client timeout | deadline, not a measurement: the 15 s client write timeout (`WRITE_TIMEOUT`, `driver/src/main.rs`) |

t0 is not always free of faults (#4559 comment 7088652995): in about one
history in four the platform stops, once kills, one container -- a ravel node
in all but one case -- and restarts it 0.0012 to 8.54 s after the fault pause
and after t0 (the largest TTFA on such a history was 14570 ms). The 60 s band
was derived over all histories, those included.

P1 and P3 are tripwires on a measured band, not checks of a documented
guarantee: docs/consistency-model.md gives no recovery-time bound and no
strict-ack latency bound. Its Visibility semantics section gives visibility
latency as a sum (flush delay + flush-permit wait + data PUT + commit PUT)
with a p99 target that tracks `--max-flush-delay`, not a fixed constant, and
the strict ack deadline there bounds when the server *answers*
(`WriteError::AckTimeout`), not when it acknowledges; a strict ack follows the
commit PUT. P2 is a deadline on that answer, not a latency band.

"Within N s" means `mono_ms <= N * 1000`. The log lines, one `println` each,
fields in this order:

```text
perf t0 stop_faults=<ok or unset>
perf skipped reason=stop-faults-failed
perf ttfa node=<n> mono_ms=<integer or none> wall_ms=<integer or none> probes=<integer> outcome=<acked or none> last=<text without spaces>
perf ack-start node=<n> i=<i>
perf ack node=<n> i=<i> signal=<logs or metrics> mono_ms=<integer> wall_ms=<integer> outcome=<acked or refused-<status> or partial or no-response>
perf median node=<n> mono_ms=<integer or inf>
perf summary ttfa=<TTFA lines printed> ack=<perf ack lines printed> recovered=<true or false>
```

`probes` is the probe writes sent so far and `last` the outcome of the
latest answer that was not an ack, or `-`. The summary is printed once per
eventually in which stop-faults did not fail, before the durability checks or
before the not-recovered exit.

`CONFIG=config-perf` selects the variant in `scripts/local-run.sh` (it needs
`WORKLOAD_EVENTUALLY_AFTER_SECS`) and `scripts/launch.sh` (test
`ravel-ingest-durability-perf`, `attrs.variant=perf`); neither takes a
mutant. The local gate reads `local/perf-required.tsv`. In place of the base
"no always-type assertion violated" check, a perf run gates on the exact set
of violated always-type assertions: it must equal, whole line for whole line,
the `<mode>-violated` messages for the mode (none in the clean mode). Its line
counts and latency bands are the local expectations of the plan, predicted and
not yet measured:

- Clean run: exactly 1 `perf t0`, 1 `perf summary`, 2 `perf ttfa`,
  20 `perf ack` and 2 `perf median` lines and no `perf skipped` line; every
  `perf ack` line acked with 500 <= `mono_ms` <= 1500; every `perf ttfa`
  line acked with 0 <= `mono_ms` <= 1500 and `probes` 1 or 2; every
  `required` line hit with its condition true. The TTFA floor is 0, not the
  500 ms flush delay: a strict waiter's buffer flushes on the delay counted
  from its oldest row, and buffered rows the rules wrote before t0 can
  already be older than the delay, so the first probe can flush at the next
  tick. No Always must fail: all three (P1, P2, P3) are hit with their
  condition true.
- `PERF_LEVER=slow-ack` layers `local/docker-compose.perf-slow-ack.yaml`
  (`--max-flush-delay 2400ms`, `--max-flush-delay-idle 5s`): the same
  counts, every `perf ack` line acked with 2400 <= `mono_ms` <= 3400, no
  `slow-ack-never` line hit true and every `slow-ack-required` line hit true.
  It is meant to show that the figures move with the flush cadence. P3, and
  only P3, must fail: the 2.4 s median is outside the 2 s band, while P1 and
  P2 hold. 2400 ms is near the server's limit: ravel-server refuses to start
  when `--max-flush-delay` plus a fixed 500 ms reserve is not under its 3 s
  strict visibility budget (`validate_strict_visibility_budget`), that is at
  2500 ms or more.
- `PERF_LEVER=no-ack` layers `local/docker-compose.perf-no-ack.yaml`, which
  sets `RAVEL_ANTITHESIS_FAULTS` to `ingest/data-put-refused=1` on ravel1 and
  ravel2: every data PUT is abandoned, so no strict write can be
  acknowledged and the server answers it 503. No flush delay can do this,
  because of the 2500 ms limit above. Expected: 1 `perf t0`, 1 summary,
  2 `perf ttfa` lines, both `outcome=none`, no `perf ack` and no
  `perf median` line, and the `no-ack-never` line never hit true. P1, and only
  P1, must fail: both TTFA samples are `none`. P2 and P3 are not evaluated at
  all, because no ack probe runs.
- `WORKLOAD_PERF_SELF_CHECK=1` (no `PERF_LEVER`) layers
  `local/docker-compose.perf-self-check.yaml`, which sends ack probes 2, 5 and
  8 of each node with a 1 ms client timeout, so those three answer
  no-response. P2, and only P2, must fail: three no-response probes per node
  make it false, while the other seven keep the median finite (P3 holds) and
  the TTFA probes are untouched (P1 holds). Predicted bands: 1 `perf t0`,
  1 summary, 2 `perf ttfa` lines acked as in the clean mode, 20 `perf ack`
  lines of which exactly 6 are no-response (probes 2, 5 and 8, each
  0 <= `mono_ms` <= 1000) and 14 acked with 0 <= `mono_ms` <= 1500, 2 `perf
  median` lines, no `perf skipped` line, and every `required` line hit true.
  The acked floor is 0 here, not the clean mode's 500: a probe abandoned after
  1 ms can leave its row in the shard's buffer, so the next probe of that
  signal flushes on the delay counted from that older row. That is a reason
  for a loose floor, not a verified server behaviour.

`scripts/launch.sh` also takes `PERF_LEVER=slow-ack` with `CONFIG=config-perf`
(not with a mutant): it launches from a temporary copy of `config-perf/` whose
`--max-flush-delay` reads 2400ms instead of 500ms, the same value the local
lever file uses (the script ties the two counts so they cannot drift), as test
`ravel-ingest-durability-perf-mutant` with `attrs.variant=perf` and
`attrs.mutant=perf-lever/slow-ack`, on the plain images.
`PERF_LEVER=no-ack` is local only; `scripts/launch.sh` refuses it.
`WORKLOAD_PERF_SELF_CHECK` is never passed to a launched run.

`LOCAL_RUN_ANALYZE_ONLY=1` re-scores a kept run: it skips every docker command
and wait and runs the collection and all gates over the files already in the
out directory.

```sh
CONFIG=config-perf WORKLOAD_EVENTUALLY_AFTER_SECS=180 antithesis/ingest-durability/scripts/local-run.sh out/perf1 900
CONFIG=config-perf PERF_LEVER=slow-ack WORKLOAD_EVENTUALLY_AFTER_SECS=180 antithesis/ingest-durability/scripts/local-run.sh out/perf-slow 900
CONFIG=config-perf PERF_LEVER=no-ack WORKLOAD_EVENTUALLY_AFTER_SECS=60 antithesis/ingest-durability/scripts/local-run.sh out/perf-noack 900
CONFIG=config-perf WORKLOAD_PERF_SELF_CHECK=1 WORKLOAD_EVENTUALLY_AFTER_SECS=180 antithesis/ingest-durability/scripts/local-run.sh out/perf-self 900
LOCAL_RUN_ANALYZE_ONLY=1 CONFIG=config-perf WORKLOAD_EVENTUALLY_AFTER_SECS=180 antithesis/ingest-durability/scripts/local-run.sh out/perf1
CONFIG=config-perf PERF_LEVER=slow-ack antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "<description>"
CONFIG=config-perf antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "<description>"
```

## Patterns

None adopted. Ravel has no stateful client library, so the stateful-clients
pattern does not apply. Every node state worth marking is visible inside one
node, at the handler that sees the outcome, so log forwarding is not needed.
The normal log level is enough, so retroactive logging is not needed either.

## Out of scope

The scope is ingest durability only:

- Spans, OTLP/gRPC, Prometheus Remote Write and OTAP are out. They reach the
  same router and commit path after decode, and the logs and metrics pipelines
  cover that path.
- Warm-up, the partition cap, bulk load (`ravel-cli load`), compaction,
  retention and GC (no `maintain` process), resharding, and multi-tenant
  admission limits are out.

## Known findings the harness works around

- ravel-server used to exit about 3 s after start when the store did not
  answer a startup read, even on a transient connect error (#4325), and nothing
  here restarts it. The server now retries its startup store reads while the
  store returns a retryable error, up to `--startup-store-deadline`. The compose
  file sets that to 300 s, far longer than the 15 s default, because no probe
  or supervisor restarts a container that exits. The entrypoint no longer waits
  for the store before starting the server, so runs exercise that retry. When
  "the system recovers once faults stop" fails, look first for "refusing to
  start" in the down node's log.
- `snouty validate` on an Apple Silicon Mac needs Docker Desktop's Rosetta
  emulation. Under its QEMU fallback, the amd64 `ravel-cli` in `init` crashes
  with a segmentation fault, and setup never completes. Validate on an amd64 host,
  or turn Rosetta on. `SKIP_VALIDATE=1 scripts/launch.sh ...` launches without
  the local validation, for use once the same config passed on an amd64 host.

## New external dependencies

`antithesis_sdk` 0.4, pinned once in `crates/ravel-invariant`, and
`antithesis-instrumentation` 0.1, in ravel-server and ravel-cli, each only
under an `antithesis` feature. No published build enables that feature, and
scripts/guards/check-release-profile.sh refuses one that does.
They add `libloading`, `linkme`, `rand` 0.8, `rand_chacha` 0.3 and
`rustc_version_runtime` to Cargo.lock. The driver is its own Cargo workspace,
with its own lock file, and the repository's workspace never builds it.

## Mutants

The commit area's negative configs (formal/tla/commit/negative/) that break a
safety property are switches in the shipped code, each an
`inv_mutant!("commit/<stem>")` call (ADR-4326 decision 3). They compile in only
with ravel-server's `antithesis-mutants` feature, which only the
`ingest-durability-mutants` image tag carries: `MUTANTS=1 scripts/build.sh`
builds ravel-server with `sql,antithesis,antithesis-mutants`, stamps the list
into the `org.ravel.antithesis.server-features` label, and checks that
`/symbols/ravel-server` holds every mutant name (a plain build checks that it
holds none). The plain build is unchanged.

`RAVEL_ANTITHESIS_MUTANT=commit/<stem>` in ravel1's and ravel2's environment
turns one switch on. A server built without the feature ignores the variable,
so `scripts/launch.sh` and `scripts/local-run.sh` refuse a mutant run unless
the `-mutants` server image carries the feature label, and refuse any name
that is not a mutant line of formal/tla/commit/antithesis-mutants.tsv. That
file names the property each mutant must make false; the configs it marks
`none:` are reachability obligations or out of scope, not switches.

| Mutant | Switch | Property it makes false |
|---|---|---|
| `commit/ack-before-commit` | a metrics shard's `commit_flush` (crates/ravel-ingest/src/shard.rs) acks its strict waiters before the data object and the commit record are written | `commit: a strict metrics ack names a durable commit record` (crates/ravel-ingest/src/router.rs) |
| `commit/commit-before-data` | `put_data_object` (crates/ravel-commit/src/publish.rs) returns success without the PUT | `commit: a commit record never exists without its data object` (crates/ravel-commit/src/publish.rs) |
| `commit/mismatched-identity-idempotent` | every shard actor draws one fixed writer identity (`draw_writer_identity`, crates/ravel-ingest/src/router.rs), so two writers reuse a commit key, and a commit record that already exists with other content is taken as an idempotent match (crates/ravel-commit/src/publish.rs) | `commit: one commit identity holds one content hash` (crates/ravel-commit/src/publish.rs) |
| `commit/marker-before-all-shards` | a keyed log write's receipt waits for its first shard's ack only (`await_strict_acks`, crates/ravel-ingest/src/log_router.rs), so the idempotency marker is written ahead of the other shards' commits | `commit: an idempotency marker implies every shard's commit is durable` (crates/ravel-ingest/src/idempotency.rs) |

`ack-before-commit` changes the metrics path only; the log path's acks are
untouched, so the driver's log read-back cannot fail under it. The driver's
metrics read-back (`a read with an acked metrics write's commit tokens returns
every sample of that write`) may fail as a knock-on effect; the gated property
is the server's own.

A mutant run passes when its property was evaluated false at least once. The
local loop gates on exactly that and lists any other always-property that went
false, without gating on it. An Antithesis mutant run is named
`ravel-ingest-durability-mutant` with the `attrs.mutant` parameter set to the
mutant (`none` on a plain run); its report should show the expected property
failing.

## Build, run locally, launch

Build on a Linux amd64 host, never on a laptop:

```sh
antithesis/ingest-durability/scripts/build.sh            # both images plus the instrumentation checks
antithesis/ingest-durability/scripts/local-run.sh out/run1 300
WORKLOAD_EVENTUALLY_AFTER_SECS=90 antithesis/ingest-durability/scripts/local-run.sh out/run2 600
antithesis/ingest-durability/scripts/export-images.sh s3://<bucket>/<prefix>
```

Then, on the machine that launches runs:

```sh
antithesis/ingest-durability/scripts/import-images.sh s3://<bucket>/<prefix>
antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "<description>"
```

The compose file names the workload's images by local-only tags,
`ravel-antithesis-server:ingest-durability` and
`ravel-antithesis-driver:ingest-durability`, plus the pinned
`ghcr.io/rustfs/rustfs:1.0.0` digest. snouty pushes a local-only image into
the tenant registry, but skips an image it finds served by another registry,
which Antithesis could not pull. The ECR tags
(`819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis/...`) only
move the images from the build host to the launching machine.

Instrumentation uses the compile-time coverage flags with codegen-units 1.
The Antithesis server and cli builds turn the release profile's thin LTO off,
because compile-time instrumentation does not survive LTO. The published
image keeps LTO. Unstripped binaries are in `/symbols` of each image.

Once #4308 lands, the workload also runs weekly from
`.github/workflows/antithesis-weekly.yml` (Tuesday 03:07 UTC, at most 240
compute hours), which calls `scripts/build.sh all` and `scripts/launch.sh` as
they are. The `weekly` marker file in this directory opts the workload in
(ADR-4326 decision 4); until #4308 lands, nothing reads it.

A smoke run is a plain run with a small budget; a mutant run builds, moves and
launches the `-mutants` images. On the build host:

```sh
MUTANTS=1 antithesis/ingest-durability/scripts/build.sh
RAVEL_ANTITHESIS_MUTANT=commit/commit-before-data \
    antithesis/ingest-durability/scripts/local-run.sh out/mutant1 300
MUTANTS=1 antithesis/ingest-durability/scripts/export-images.sh s3://<bucket>/<prefix>
```

Then, on the machine that launches runs:

```sh
antithesis/ingest-durability/scripts/launch.sh 1 "ingest-durability smoke"
MUTANTS=1 antithesis/ingest-durability/scripts/import-images.sh s3://<bucket>/<prefix>
RAVEL_ANTITHESIS_MUTANT=commit/commit-before-data \
    antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "ingest-durability mutant"
```

The `-mutants` archives are saved as `<name>-mutants.tar.zst` beside the plain
ones, so one prefix holds both. `MUTANTS=1` needs a tag ending in `-mutants`
and a `-mutants` tag needs `MUTANTS=1` (`scripts/image-tag.sh`), so a mutant
build never lands under the plain tag.

The harness expects these fault flags (`scripts/launch.sh`):

```sh
--fault-tolerance high_availability --fault-exclusions '^driver$' --setup-timeout 10m
```

Driver environment for the local loop: `WORKLOAD_EVENTUALLY=off` turns the
eventually off, `WORKLOAD_EVENTUALLY_AFTER_SECS=N` runs it after N seconds
(inside the next rule, so no rule returns once it starts and the Hegel engine
cannot spin empty rounds, #4502; if no rule takes the request within 600 s the
timer thread runs it), and `WORKLOAD_MAX_WRITES` caps the writes per timeline (the compose file sets 2500; the eventually is proven at about 2400 writes).
`WORKLOAD_INJECT_MISSING_RECORD=1` is a self-check: the model then expects one
record more than is sent for each log write whose id is 3 modulo 50 (about one
log write in 50; ids are shared with metrics writes, so about one write of any
signal in 75 at the default rule weights), and the presence assertions must
fail. `WORKLOAD_PERF=1` turns on the perf variant (see "Perf variant"; the
`config-perf` compose file sets it). `WORKLOAD_PERF_SELF_CHECK=1` (only with
`WORKLOAD_PERF=1`, which the driver enforces by exiting 1 otherwise) is the
perf self-check: ack probes 2, 5 and 8 of each node then use a 1 ms client
timeout so P2 fails; `scripts/local-run.sh` sets it from
`WORKLOAD_PERF_SELF_CHECK=1` through the self-check compose file, and
`scripts/launch.sh` never passes it.

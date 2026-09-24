# ADR-0070: Request-class scheduling for object-store traffic and a CI benchmark regression gate

Status: Accepted

## Context

Every process shares one `Arc<dyn ObjectStoreBackend>` built at a single
site (`services/ravel-server/src/store.rs::build_store`). Foreground,
ack-bearing traffic (ingest data and commit PUTs, query segment GETs,
resolve LISTs) and background traffic (compaction, fold, sweep, scrub,
audit retention) meet in the same connection pool with no request
prioritization and no global cap. The starvation is measured, not
hypothesized: the `s3_e2e` MinIO panel saturated ingest at
3,923 accepted points/s with 70 s visibility lag, and the bench's
min-token resolve probe repeatedly timed out queued behind the ingest
PUT backlog. Under the same load, reader p99 was 417 ms against 26 ms on
an idle store.

Concurrency limits exist but were never derived from measurement, and
they do not cover the storm sources. Query fetch holds a 16-permit GET
semaphore and 8 segment futures; catalog resolve holds 16; compaction
part-fetch holds 16; fold 8. Sweep, scrub, and audit retention -- the
exact background storms -- have no cap at all: they list and delete
sequentially but unboundedly interleave with everything else. The
fetcher's GET knob (`with_max_concurrent_gets`) has zero non-test
callers, so none of these numbers can even be swept without plumbing.

Two facts constrain the design:

- **Request class is not decidable from the key.** Foreground query
  fetch and background compaction, fold, and scrub GET the same
  `t/<hash>/<sig>/l0/...` and `l1/...` objects. A key-sniffing
  decorator (the KmsRoutingStore shape, "one seam, zero threading")
  cannot classify reads. Class is a property of the caller, so it must
  attach where callers get their store handle.
- **Timing cannot be a hard CI gate on shared hosts.** The reference
  host (`ci-16gb-fsn1-1`) routinely carries load 3.5-5.8 from
  co-resident CI; GitHub-hosted runners are noisier still. The repo
  already encodes the honest answer in
  `crates/ravel-bench/tests/catalog_byte_gates.rs`: bytes and request
  counts are exact and deterministic, "this makes the gate unlosable."
  Meanwhile nothing gates performance at all: a past eager-decode
  regression merged silently, and every benchmark panel is a
  manual, one-shot run.

ADR-0067's depth panel left cells pending: depth 3 under
~300 ms injected RTT (committed as zero-byte placeholders) and a
multi-tenant bench shape (the single-tenant cadence never wants a
second in-flight flush). Both block the `max_inflight_flushes` and
adaptive-flush default decisions.

## Decision

![Two-class store scheduling](assets/0070-request-classes.svg)

### 1. Two request classes, attached per handle, one shared scheduler

`ravel-object-store` gains a `ClassedStore` wrapper constructed once in
`build_store`: it wraps the instrumented store and hands out two
`Arc<dyn ObjectStoreBackend>` handles -- `foreground()` and
`background()` -- that share one `RequestScheduler`. Wiring changes at
the construction sites only: ingest, query, and catalog receive the
foreground handle; the maintain driver, fold, sweep, scrub, and audit
retention receive the background handle. No trait change, no
per-request parameter, no key sniffing.

The scheduler is deliberately minimal: a weighted pair of semaphores.
Foreground requests are admitted up to `fg_permits`; background
requests are admitted up to `bg_permits` and additionally yield when
foreground waiters exist (strict-priority-with-floor: background never
starves completely -- it keeps a configurable floor of at least one
permit -- and never delays a foreground acquire by more than one
in-flight request). Metrics per class ride the existing
`InstrumentedStore` op labels extended with `{class}`.

### 2. Off by default until the panel sizes it

`--store-scheduling` defaults off: both handles pass through
unscheduled, byte-for-byte today's behavior. The flag flips on (and the
defaults freeze) only on evidence from the decision-4 panel, per the
epic's "semaphore defaults change only on panel evidence" rule. One
exception lands immediately regardless of the flag: sweep, scrub, and
audit retention acquire from the background class even in pass-through
mode, which for the first time bounds their in-flight store requests
(default cap 8, operator-tunable). Today they are unbounded; a bound of
8 is strictly safer and does not depend on panel calibration.

### 3. The CI gate is two-tier: exact gates hard, timing gates advisory

- **Tier A (hard, any runner):** extend the byte/request-count gate
  family (`catalog_byte_gates` shape) with counts for the read path and
  ingest publish path. Deterministic, MemoryStore-only, unlosable;
  runs in the normal check job.
- **Tier B (advisory, reference runner only):** a criterion smoke
  compare over the stable pure-CPU set (`segment_encode` groups,
  `series_id_hash`, `merge_kway_vs_materialized`, `bytes_slice_vs_copy`,
  `logseg_encode`, `logseg_scan`, `otap decode`) against a committed
  baseline under `bench/baselines/`, threshold +/-15%, posting a PR
  comment, never failing the build, and running only on the
  self-hosted reference runner where the baseline was recorded.
  Promotion to enforcing happens after a probation
  window shows acceptable false-positive rate, and only for regressions
  beyond 15% sustained across two consecutive runs of the same PR head.

### 4. One measurement panel closes the open loops

A single local panel session (MinIO + toxiproxy, the ADR-0067 panel's
fresh-data-dir/fresh-bucket/drift-canary methodology) produces:
the real-S3/non-loopback rerun listed as pending; the
GET-concurrency sweep {8, 16, 32, 64, 128} under concurrent ingest
(needs the fetch/catalog knobs plumbed to bench flags first);
the depth-3-at-300ms cells; and the multi-tenant shape
(needs an ingest_bench `--tenants` flag). The bench report JSON gains a
counted `resolve_starvation_timeouts` field so starvation stops being a
grep over raw logs. Panel results freeze the scheduler defaults
(decision 2) and the depth/adaptive-flush defaults (ADR-0067's open
decision).

## Rejected alternatives

- **Key-sniffing class decorator.** Reads are unclassifiable by key
  (foreground and background read identical objects); it would
  misclassify exactly the traffic that matters.
- **Per-request priority parameter on the trait.** Threads a parameter
  through every call site in nine crates for information the handle
  already carries; the trait stays clean.
- **A full scheduler (deadlines, aging, per-tenant queues).** No
  evidence requires it; the measured problem is two-class starvation.
  Weighted-pair semantics are explainable in one sentence and testable
  with FaultStore's hold/release ordering gate.
- **Hard timing gates on shared runners.** The reference host is never
  quiet and hosted runners are noisier; a hard timing gate would train
  everyone to ignore red. Exact gates stay hard, timing stays advisory
  until probation proves otherwise.
- **Skipping the panel and sizing semaphores analytically.** The
  existing constants came from exactly that method and the arithmetic
  shows they can be off by 4x against real RTT; the panel is the
  cheaper mistake.

## Consequences

- Background storms stop being able to starve acks once the flag flips;
  before that, sweep/scrub/audit-retention gain their first concurrency
  bound with no behavior change elsewhere.
- Every store construction site changes once (handle selection); new
  callers must choose a class, which is the point.
- The reference runner becomes CI infrastructure (self-hosted runner
  labels on the existing dual-role box); Tier B never fails the build.
  (Correction: an offline runner leaves the job queued until one appears or
  GitHub times the queue out, not skipped. See the amendment below.)
- The panel is local-only work (MinIO + toxiproxy on a workstation, per
  the depth-panel methodology); code legs are fleet-dispatchable, the
  workflow leg iterates on live Actions runs.
- The benchmark discipline holds: every number states its
  environment; loopback panels stay labeled as loopback.

## Proposed amendment (issue #533): make tier B advisory-that-can-block

<!-- amendment-applies: none reason="a recorded proposal that adopts nothing; it retires no earlier wording, and the 2026-09-19 amendment below resolves it as not adopted" -->

Status: Proposed. Needs an owner's decision; not adopted by the change that
adds this text. Resolved as not adopted by the 2026-09-19 amendment below.

Issue #533 asks for a tier B that is "advisory-that-can-block, not silently
informational". That is a different decision from decision 3 above, which
records tier B as advisory only: "posting a PR comment, never failing the
build". This section records the tension rather than resolving it silently.

The machinery landed for #533 makes both behaviours reachable from one tool.
`scripts/bench-compare.py compare` (driven by `scripts/bench-tier-b.sh`) is
advisory by default and exits non-zero only with `--enforce`. The
`bench-compare.yml` workflow exposes the enforcing path only on a manual
dispatch. Nothing can block a merge today.

The workflow carries no pull_request trigger yet, and that is a deployment
fact rather than a change to decision 3. No runner carrying the
`ravel-reference` label is registered on this repository, and a job whose
labels match no online runner is queued for about 24 hours and then failed,
not skipped. Wiring the PR path before the runner exists would put a
permanently pending check on every pull request in the repository. The
trigger goes in with the runner. Two things that must hold when it does: the
job stays gated on the head repository matching this one, because this
repository is public and the job runs pull request code on persistent
hardware; and the baseline must be re-recorded on that runner, since the
committed one is labelled as a demonstration and its sampling knobs are not
recorded, which an enforcing compare now refuses.

What decision 3 already requires before enforcing, unchanged by this proposal:

- Enforcement turns on only after a probation window shows an acceptable
  false-positive rate. On shared and self-hosted runners under co-resident
  load, a raw +15% single-run threshold will fire on noise; the probation
  window is what measures how often.
- Even then, a regression blocks only when it is beyond 15% and sustained
  across two consecutive runs of the same PR head. The current
  `bench-compare.py` compares a single run to the baseline; the two-consecutive
  -runs-of-the-same-head condition is not yet implemented and is a prerequisite
  for wiring `--enforce` onto pull_request.

Recommendation: keep the automatic path advisory until the probation data
exists, then promote by (a) implementing the two-consecutive-runs condition in
the compare tool, (b) recording a real baseline on the reference runner, and
(c) switching the pull_request step to `--enforce`. The threshold, the named
bench set, and the reference-runner-only constraint stay as decision 3 sets
them. This is a policy change on a required-check surface and needs an owner's
call, not a fleet executor's; it is filed here so the decision is made with the
probation condition in view rather than by default.

## Amendment (2026-09-19): decision 3 resolved as advisory, not blocking; no self-hosted runner

<!-- amendment-applies: sections="Proposed amendment (issue #533): make tier B advisory-that-can-block" pointer="2026-09-19 amendment" -->

The owner decided on 2026-09-19 that this repository gets no self-hosted
GitHub Actions runner. The repository is public, and a self-hosted runner
executes workflow code from forked pull requests unless every entry point
is gated; the `ravel-reference` label this ADR and `bench-compare.yml`
were written against will not be registered.

This resolves the issue #533 proposal above: tier B stays advisory, not
blocking. Nothing runs the comparison on every pull request, and a figure
that is not measured on every change cannot gate a merge. The proposed
amendment's promotion path -- implement the two-consecutive-runs
condition, record a baseline on the reference runner, switch
`pull_request` to `--enforce` -- is not adopted; it depended on a runner
that will not exist.

`bench-compare.yml`'s job is disabled (`if: false`), not merely left
`workflow_dispatch`-only: a manual dispatch, including one with
`enforce=true`, runs no step and performs no comparison. Its
`ravel-reference` runner label is removed from the job and from
`.github/actionlint.yaml` rather than left pointing at hardware that will
never register, which per the base decision would otherwise queue every
dispatched run for about 24 hours and then fail it. The comparison
machinery itself (`scripts/bench-tier-b.sh`, `scripts/bench-compare.py`) is
unchanged and still runnable by hand or from a workstation; only the
workflow's ability to run it in CI is gated off. Restoring the job is an
owner's decision on where it runs -- a future gate, if wanted, runs on a
GitHub-hosted runner against an object store the workflow provisions
itself (for example a MinIO container service), not against a persistent
self-hosted box holding a warm cache and a `pull-requests: write` token --
and the committed baseline needs re-recording with knobs recorded before
any enforcing comparison would trust it.

The committed baseline under `bench/baselines/` remains a demonstration
recorded on a fleet executor box, not a reference measurement; see its
`_meta` block and `bench/baselines/README.md`.

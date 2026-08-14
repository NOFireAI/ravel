# ADR-0053: CI latency and delivery process hardening

Status: Accepted

## Context

Four days of measured evidence across 321 CI runs and 131 merged PRs:

- CI median wall time grew from 10.1m to 24-25m over the window. For a
  clean code PR with auto-merge, CI is 95-100% of PR lifetime; the
  `coverage` job (22-26m, required) alone sets the merge critical path. It
  has no threshold gate: `--summary-only` exits 0 regardless of the
  number, so the only failures it can produce are test failures `check`
  already catches, or infra failures. It also runs the workspace suite
  twice (text summary, then JSON, as two separate `cargo llvm-cov`
  invocations).
- `required_status_checks.strict` is false and there is no merge queue.
  Two individually green PRs combined by rebase-merge broke main at least
  three times in the window (one instance: a 4th `start()` parameter
  vs three new 3-arg call sites left main red ~40 minutes; same
  shape twice more). The local cold-gate rule cannot catch this class: it
  gates the pre-rebase tree.
- `k8s-integration` is the only lane that proves the operator reconciles
  and the shipped images start, and it caught a real regression (the
  deployment-keyed tenant hash change), but it is not a required check,
  so that change auto-merged while red and crash-looped fresh
  RavelCluster deployments (fixed the next day). The job is also flaky
  (readiness timeout with no diagnostics dump, plus a kubectl download
  failure mode) and has
  breached its own documented remedy trigger (ci.yml names 25m warm-cache
  as the split-the-binary-build threshold; observed 17m/20m/44m).
- All 10 compiling jobs fan out in parallel behind the `changes` job. A
  `cargo fmt` failure at second five still burns 22-26m of coverage and up
  to 44m of k8s work. There is no `concurrency:` group (a push to an open
  PR leaves the superseded run executing to completion) and no
  `timeout-minutes` anywhere (default 360m; two manual re-runs already
  produced 360m+ "run durations").
- The only path granularity is docs-only (every changed file `\.md$`). A
  one-line change in any crate runs every lane. Six mutually non-sharing
  compile configurations exist across jobs (dev-metadata, ci-profile
  codegen, dev codegen, dev+sql feature, coverage-instrumented, release);
  within `check` alone, clippy, nextest (`ci` profile) and doctests (dev
  profile) cannot share artifacts. Seven jobs compete for the 10 GB GHA
  cache budget.
- Correctness gaps: only `k8s-integration` uses `--locked`; a stale
  `Cargo.lock` passes all eight required checks. Four cargo features
  (`ravel-server/otap`, `ravel-server/flight-sql` with `ravel-sql`,
  `ravel-bench/parquet-baseline`, `ravel-bench/flight-egress`) are never
  compiled by any CI job, leaving e2e tests unreachable and three bench
  binaries never built. `scripts/test_process_metrics.py` has no runner.
- Process friction outside ci.yml: executors append pure fmt/style fixup
  commits instead of formatting before committing (9 of 268 main commits),
  and `wip:` headers reach protected main because the merge path never
  squashes or rewords. Three ADR number collisions in one day from
  parallel work. Derived doc counts (docs/query-engine.md corpus count)
  broke a textually clean parallel merge once and are checked by nothing.
  A reviewed, merged read-cache once shipped that no production code
  path ever constructed; crate-level tests cannot catch dead wiring.

## Decision

Seven decisions, ordered by leverage.

### D1. Merge queue on main

Enable the GitHub merge queue for `main`; add a `merge_group` trigger to
ci.yml so required checks run on the queued merge result. This tests the
exact tree that will become main, closing the green+green=red class
structurally instead of procedurally.

### D2. Correct the required-check set

`coverage` leaves the required set and stops running on PRs
entirely; it runs on pushes to main (badge and JSON artifact unchanged)
and its double test execution collapses to one `--no-report` run followed
by two `report` invocations. `k8s-integration` joins the required set, but
only after D5 makes it fast and non-flaky. Net effect: the PR-blocking
critical path drops from coverage's 22-26m to `check`'s 13-17m
immediately, and the one lane that catches shipped-artifact regressions
starts gating merges.

### D3. Fail fast, cancel dead work, bound everything

A new first-stage `lint` job runs `cargo fmt --all --check` then workspace
clippy plus the sql-feature clippy (metadata-only compile, ~4-5m). The
expensive lanes (`k8s-integration`, main-push `coverage`) gain
`needs: lint`; the cheap lanes keep fanning out behind `changes` so a
green run pays no extra serialization on its critical path. `check` and
`sql` drop their now-duplicate fmt/clippy steps. Every job gets
`timeout-minutes`; the workflow gets a per-ref `concurrency:` group with
`cancel-in-progress: true` for PR refs.

### D4. Run less, share more

- Extend the `changes` job to per-area outputs and skip lanes that cannot
  be affected: `k8s-integration` only when server, operator, deploy,
  kind/demo scripts, Dockerfile, or workspace-level files change; `sql`
  only on ravel-sql, ravel-server, ravel-query, or workspace-level
  changes; `promql-difftest` only on promql, query, difftest, or
  workspace-level changes; `object-store-contract` and `bench-smoke` only
  on object-store, bench, commit, catalog, ingest, or workspace-level
  changes. Workspace-level (Cargo.toml, Cargo.lock, rust-toolchain.toml,
  .github, proto) always runs everything. Skipped required checks still
  satisfy branch protection. Mappings are conservative; when in doubt a
  lane runs.
- Unify `check` on the `ci` profile: nextest already uses it; doctests
  move to `--profile ci`, collapsing one full compile configuration.
- `docker-build` stops cloning full history to decide "no"; it consumes a
  `changes` output.
- `free-disk-space` (0.5-4m per job) remains only where a lane actually
  needs the space (k8s-integration, coverage, docker builds) and is
  dropped elsewhere.

### D5. Make k8s-integration trustworthy, then required

Execute the remedy ci.yml already names: split the release binary build
into a build job that uploads the two binaries as artifacts;
k8s-integration downloads them, assembles images, and runs kind. Add the
diagnostics dump on readiness timeout (describe pods,
events, operator logs), pin or retry the kubectl download, and raise the
readiness timeout to cover measured startup. When it holds green for a
probation window on main, flip it required (D2).

### D6. Close the correctness gaps

`--locked` on every cargo invocation in CI and in gates.sh. A new cheap
`features` lane runs `cargo check` on the never-compiled feature combos
(`ravel-server --features otap`, `--features flight-sql`, `ravel-bench
--features parquet-baseline,flight-egress`). A generated-docs step
regenerates the derived corpus count in docs/query-engine.md and diffs it.
`scripts/test_process_metrics.py` gets a runner in the lint lane or the
script pair is deleted.

### D7. Delivery process determinism (skills and scripts, not ci.yml)

- fleet-task-spec: the spec template makes `cargo fmt --all` plus scoped
  clippy a mandatory step before every commit, so style fixups never ride
  result branches.
- merge flow (merge-fleet-result skill, fleet-result-merge.sh): squash or
  reword `wip:`/fixup/style commits before the PR is opened; `wip:`
  headers must not reach main.
- deliver-epic: ADR numbers are claimed by committing a one-line stub to
  main via PR at the moment the number is chosen, before the ADR is
  written; the epic acceptance checklist requires an end-to-end
  reachability test proving the feature is constructed and attached in the
  shipping binary (the read-cache dead-wiring lesson; the "prove the read
  cache is attached" test is the template).
- gates.sh: use nextest with the `ci` profile where nextest is installed,
  matching CI's configuration so local and CI runs exercise the same
  artifacts.

## Rejected alternatives

- `required_status_checks.strict: true` instead of a merge queue: every
  update-branch re-burns the full CI critical path and serializes merges
  by hand at a measured ~33 PRs/day; the queue batches and tests the same
  combined tree without manual churn.
- Keep `coverage` required with a threshold gate: a hard threshold on a
  codebase growing 2,000+ lines/day generates noise merges can trip over,
  while the job still duplicates `check`'s test run. Visibility, not
  gating, is what coverage provides here.
- `needs: check` on expensive lanes for fail-fast: serializes 13-17m in
  front of k8s/coverage on every green run. The metadata-only `lint` job
  gives the same early kill for one-third the serialized cost.
- Self-hosted beefy runner or larger hosted runners: measured queue time
  is 0.0-0.7m; compute concurrency is not the bottleneck, the critical
  path shape is. ci-16gb stays reserved for fleet execution.
- nextest test sharding across runners: real complexity (partition
  stitching, per-shard caches) targeted at `check`'s 9-11m test step, and
  cheaper wins (D2-D4) come first. Revisit if `check` exceeds ~20m.
- Remote build cache (sccache to S3, Bazel-class systems): premature
  against a 10 GB GHA budget not yet proven insufficient after D4 reduces
  the configuration count.
- ADR numbers derived from issue numbers: breaks the contiguous
  sequence readers navigate; a claim-stub PR is one minute of work.

## Consequences

- PR-blocking CI drops from 22-26m to 13-17m at once (D2), and further as
  D4 trims lanes per area; docs-only stays at seconds.
- Main can no longer break from two green PRs merging around each other;
  the queue's own CI run becomes the last line of defense, replacing the
  unreliable "cold-gate the merged tree locally" step for this class.
- A red fmt/clippy PR dies in ~5m having spent ~3 job-minutes, not ~90.
- k8s-integration becomes a required, diagnosable gate; operator
  regressions of that shape cannot auto-merge red again.
- Stale lockfiles, dead feature code, and derived-doc drift become CI
  failures instead of post-merge archaeology.
- Merge queue adds per-PR queue latency (one extra CI cycle when the
  queue is contended); accepted in exchange for structural integrity.
- Coverage numbers update only on main; PR authors lose the per-PR
  coverage summary. Accepted: it gated nothing and cost 25m.
- ci.yml becomes the most-edited file of this work; all ci.yml tasks are
  same-file and therefore serialize into distinct waves by design.

## Amendment: main-push coverage ratchet

External review finding J-7 (epic #1061) observes that coverage is
measured and badged but never gated, so the number can only fall
silently. Read quickly, that asks this ADR to reverse one of its own
rejected alternatives:

> Keep `coverage` required with a threshold gate: a hard threshold on a
> codebase growing 2,000+ lines/day generates noise merges can trip
> over, while the job still duplicates `check`'s test run. Visibility,
> not gating, is what coverage provides here.

That rejection stands. It priced a specific mechanism, a PR-required
threshold check, and both of its named costs are real: threshold noise
that blocks merges, and a duplicated workspace test run sitting on the
merge critical path D2 just cut from 22-26m to 13-17m. Nothing observed
since changes that price, and this amendment does not pay it. No
coverage measurement returns to pull requests, the merge queue, or the
required-check set. The PR-blocking critical path is untouched.

What the rejection did not price is the failure mode J-7 names: on
main, the number can decay for weeks and nothing raises its hand. The
badge is visibility only for someone who happens to look. This
amendment makes the existing main-push visibility self-alarming.

### Decision

The main-push `coverage` job (D2) gains a ratchet step after it renders
`coverage.json`. No new job, no new test execution, no PR trigger.

- The job stores a full-precision baseline (workspace line-coverage
  percent plus the main SHA that produced it) as a second file in the
  badge gist it already writes. The badge file itself stays unchanged
  as a rendered one-decimal display value.
- After measuring, the job compares the fresh percent against the
  stored baseline. At or above baseline minus a deadband: pass; the
  badge updates to the fresh number, and the baseline updates only if
  the fresh number exceeds it (a high-water mark; a first run with no
  stored baseline seeds it). Below baseline minus deadband: the step
  fails the job, and neither badge nor baseline is updated, so the
  baseline holds its pre-drop value and every subsequent main push
  stays red until coverage recovers or the drop is deliberately
  accepted. The baseline never moves down on a green run: without the
  high-water rule, repeated drops each just inside the deadband would
  re-base downward every push and decay silently forever. With it,
  total unalarmed decay is bounded by one deadband below the best
  number main has ever held.
- The deadband and an optional explicit re-baseline value live in a
  small checked-in config the job reads (start at 0.25 percentage
  points; retune from observed jitter without a new ADR). Accepting a
  legitimate drop is therefore a one-line PR whose commit message
  carries the justification. It is never a bot commit to protected
  main, and never silent.
- A failed gist read skips the comparison with a loud workflow warning
  instead of failing the job. The threat model is silent decay, not
  adversarial bypass, and infra flake must not fabricate red runs on
  main. A successfully read baseline that the fresh number undershoots
  fails hard.

### Why this satisfies J-7 without unwinding D2

J-7's harm clause is "the number cannot decay unobserved". Under this
ratchet a real drop turns main's own push run red, and because main
merges land one rebase (or one queue batch) per push, the red run
attributes the drop to a one-PR-wide window. The failure is sticky: it
repeats on every push until someone restores coverage or re-bases the
floor with a written reason. Decay is now loud, attributed, and
persistent, which is what "cannot decay unobserved" requires.

What this deliberately does not provide is prevention at merge time. A
PR that lowers coverage still merges; main tells us immediately
afterward. Prevention is exactly the mechanism the rejected alternative
priced at 22-26 minutes per PR plus a duplicated suite, and J-7's own
wording targets unobserved decay, not the existence of a PR gate. If a
recurring pattern emerges of drops that a PR-time signal would have
cheaply prevented, that is new evidence, and a future amendment can
weigh a diff-coverage advisory comment (never a required check) against
it. Epic #1061's exit criterion "a floor that fails the build" is met
by the main-push build failing; the epic ledger should read it so.

### Consequences

- PR-blocking CI: unchanged, `check`'s 13-17m. Nothing in this
  amendment runs on a PR ref.
- A slow legitimate decline (new code consistently a little under the
  workspace average) will eventually trip the ratchet even though no
  single merge was at fault. Accepted, and intended: the ratchet
  converts slow decline from a silent trend into a periodic deliberate
  decision, recorded in a re-baseline commit.
- The gate reads the workspace line percent, so one crate's collapse
  can hide inside another's growth. The job's per-crate table in the
  step summary is the diagnostic for a tripped ratchet. Per-crate
  ratchets are rejected for now: 26 independent floors multiply flap
  for marginal signal. Revisit only if a real decay is shown to have
  hidden this way.
- Baseline state lives in the badge gist, outside the repo. Accepted:
  it is already the trust anchor for the published number, writes
  require the same `GIST_PAT` secret only main-push runs hold, and the
  authority to accept a lower floor stays in-repo behind PR review.
- Added cost per main push: one gist read, one compare, one gist write.
  Zero added test execution anywhere.

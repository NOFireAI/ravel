# Scripts reference

Full reference for the scripts under `scripts/` and `bin/`. `CLAUDE.md`
lists each one in a line; read the entry here before you rely on a
script's exit codes, flags or environment variables.


Use these instead of retyping the same shell each time; they exist
because the ad-hoc version of each has broken in practice (a stale SSE
connection, a pushed-but-broken main).

- `bin/agent-cli`: use agent-cli first, and prefer it over re-reading
  files or guessing commands. It answers the lookups sessions here kept
  repeating, one subcommand each, with terse output and `--json`:
  - PR state, findings, replies, review requests and merge lines: `pr`,
    `pr-findings`, `pr-reply`, `pr-review`, `pr-wait`, `pr-landed`,
    `merge-line`, `fresh`.
  - Issues and epic ledgers: `issue`, `issue-tick`, `issue-note`, `epic`.
  - Fleet tasks: `task` (status and the full final report), `dispatch`,
    `spec-lint`.
  - Local checks: `docs-check`, `authorship`, `box`, `wt`, `main`, `adr`.

  `bin/agent-cli <command> --help` gives the details of each, and
  `scripts/test_agent_cli.py` pins its parsing.
- `scripts/gates.sh [-p CRATE ...] [--dry-run]`: the Gates list above. No
  args runs the full workspace gate; `-p CRATE` (repeatable) scopes
  clippy/test/doc to the named crates and every workspace crate that
  depends on them, transitively (the closure `affected-tests.sh -n`
  prints). It also runs the `sql`, `flight-sql`, `esql`, `agents` and
  `mcp` feature lanes, always in workspace mode, and in scoped mode `sql`,
  `agents` and `mcp` when `ravel-server` is in the closure, `flight-sql`
  when `ravel-sql` or `ravel-server` is, and `esql` when `ravel-esql`,
  `ravel-sql` or `ravel-server` is; the agents lane's page-size-2 run
  follows `ravel-agents-runtime` or `ravel-agents-sim`. In workspace mode it also runs
  `scripts/gates-sdk.sh` (node 22; `RAVEL_GATES_SKIP_SDK=1` skips it).
  That is `--tier merge`, the default and the merge gate.
  `--tier executor -p CRATE` is the executor's gate in fleet specs: fmt
  and the source guards, clippy, tests and doctests for the named crates,
  one `cargo check` of the rest of the closure with the lane features it
  reaches, and the clippy and tests of each lane covering a named crate.
  A scoped run writes no
  gates-pass receipt in either tier. `--dry-run` prints the closure and every cargo
  command the same arguments would run, then exits 0 without building;
  `scripts/gates.test.sh` pins that selection. In a dispatched fleet clone (a checkout under
  the fleet work root) an unscoped run is refused with exit 64: the
  executor's gate is the scoped form, and `RAVEL_GATES_ALLOW_UNSCOPED=1` is
  the escape hatch for a deliberate workspace gate, which
  `verify-dispatch-gates.sh` and `fleet-result-merge.sh` set for their
  own runs.
- `scripts/disk-reap.sh [-y]`: reclaims disk from merged clean worktrees
  and orphaned cargo target dirs (dry run by default; `-y` applies). Run
  it when free space drops below ~20 GB, and after a land worktree's PR
  merges. A volume filled to zero bytes free stops every Bash command
  and makes gates fail with fake errors; the script encodes the cleanup
  heuristics that reclaim the space safely.
- `scripts/fleet-watch.sh <watch-url> [poll-interval-seconds]`: waits on
  a `fleet_dispatch`/`fleet_status` task by polling its watch endpoint in
  a loop. The SSE stream it wraps drops the connection almost immediately
  in this environment, so a single long-lived `curl -N` never sees the
  terminal event; this retries instead. Prints the terminal event and
  exits 0 once one arrives. Prefer `fleet-watch-managed.sh` below when a
  harness lifetime cap applies (it always does in Claude Code sessions).
- `scripts/fleet-watch-managed.sh <watch-url> [budget-s] [poll-s]`: the
  same polling, in bounded runs sized to survive the harness's 10-minute
  background cap. Exit 0 = terminal event printed; exit 75 = budget
  elapsed, relaunch (arm it under a Monitor until-loop that relaunches
  on 75). Prefer `fleet-watch-loop.sh` below, which is that loop.
- `scripts/fleet-watch-loop.sh <watch-url> [budget-s] [poll-s]`: the
  relaunch loop around `fleet-watch-managed.sh`, so a Monitor command is
  the script name and nothing else. Arm this rather than hand-writing the
  loop: a command that reports the 75 instead of relaunching ends the
  watch, and every later terminal event is lost with nothing to say the
  watch died. One stdout line per event, each worth a notification:
  `TERMINAL <data: line>`, `WATCH-ERROR code=N`, or `WATCH-GAVE-UP` once
  `FLEET_WATCH_MAX_SECONDS` (default 18000) elapses.
- `scripts/epic-status.sh <epic-issue> [--fresh]`: one-call "where are we
  at?": reads the epic ledger (cached 60 s) and reconciles every task id
  in it against `refs/heads/task/*` on origin and the task-branch PRs.
  This is the mandatory pre-dispatch ledger reconciliation as a single
  command.
- `scripts/fleet-dispatch-intent.sh intent|record|failed ...`: intent-first
  dispatch bookkeeping on the epic ledger. `intent <epic> <ticket> <sha>`
  refuses while a previous intent for the ticket is unresolved, runs the
  fresh-ref guard, and posts a marker comment; `record`/`failed` close it
  out after the `fleet_dispatch` call. A start push lost to a 5xx then
  leaves a record instead of a ghost task, and a retry cannot
  double-dispatch. Use it around every dispatch. An unreadable intent
  history exits 69: UNKNOWN is not the same as no dangling intent, and the
  moment GitHub is unreliable is the moment a dispatch is most likely to be
  a retry of one that already started. `intent` also runs the
  duplicate-work guard, so a ticket another pull request already addresses
  refuses here (65/66/69) rather than depending on someone remembering to
  check: this is the one chokepoint every dispatch passes through. Pass
  `DISPATCH_PATHS="a,b"` with the files the task is predicted to touch so
  an open pull request already on them refuses too, and
  `DISPATCH_SKIP_DUPLICATE_CHECK=1` for a deliberate second dispatch (a fix
  round, a continuation after a ceiling kill).
- `scripts/epic-orchestrator.sh <command> <epic>`: the per-epic state index
  behind a resumable orchestration, at `.claude/epic-state/<epic>.json` in
  the PRIMARY checkout (a wave's worktree is deleted at the end of its
  wave). `record <kind>` writes each transition; `task-dispatched` also
  appends the ledger line to the epic issue BODY and reads it back, because
  `epic-status.sh` parses the body and nothing else. `reconcile` rewrites
  the index from live GitHub and fleet control-plane state and blocks the
  next dispatch (65) on any drift. It finds each task's merge PR by its
  exact branch, so an old landed task reads LANDED however far back it
  merged. A task with a start ref and no result ref is settled by the
  control plane's `GET /v1/tasks/<id>`: RUNNING does not block; DEAD blocks
  unless another task on the same ticket landed, completed, or is running
  (SUPERSEDED); an unreachable control plane leaves it UNRESOLVED, which
  blocks. The URL comes from `FLEET_CP_URL`, else `FLEET_PUBLIC_URL`, else
  `~/.fleet/cp.env`. `resume-set --reason` parks
  a recoverable interruption with the next backoff delay (900, 1800, 3600;
  below 900 the PreToolUse guard refuses the wakeup) and exits 69, or exits
  75 on a fatal error or an exhausted budget. `classify` and `backoff` are
  the same decisions on their own, for a caller that only needs one.
- `scripts/ci-sweep-cancelled.sh [-y]`: finds cancelled ci runs on open PR
  head SHAs and reruns them (dry run by default). A cancelled required
  check blocks auto-merge the same as a red one, and nothing retries it.
  It refuses to rerun a run whose job ran within a minute of that job's own
  `timeout-minutes`, since rerunning a timeout only times out again and
  hides a budget problem; it prints the job and both numbers and exits 3.
- `scripts/fleet-result-inspect.sh <task-id>`: fetches a dispatched
  task's result branch and prints its commits and diff scope vs `main`,
  for review before merging. Never trust an executor's own "gates green"
  claim; look at what actually landed.
- `scripts/fleet-result-merge.sh <task-id> <message-file> [-p CRATE ...]`:
  cleans `wip:`/fixup commits out of the reviewed result branch, runs
  `gates.sh`, runs `scripts/guards/assert-struct-literals-current.sh`
  against the branch it is about to push (any non-zero exit refuses the
  PR; `ALLOW_STALE_SHAPE_SITES=1` passes exit 1 only), and opens a PR
  against `main` (`main` is protected; the
  script never pushes or merges it directly). The PR opens WITHOUT
  auto-merge by default (standing rule, 2026-08-26): a bot review posts as
  a comment, not a required status check, so `--auto` used to merge before
  that review landed (#749/#750 shipped with 6 real findings unaddressed
  this way). After opening the PR it posts `@claude-fleet review` on it,
  which is the trigger for the fleet review (ADR-1586). Wait for the
  `claude-fleet[bot]` review and fix or answer every finding not marked
  `nit`. A review whose findings are all nits is clean: do not push for
  the nits and do not ask for another round, since a push moves the head
  and needs a fresh review. After 3 review rounds on
  one PR, stop and hand it to a person. Check with
  `scripts/pr-review-status.sh <pr-number>`, then run the exact merge
  command it prints once clean (it pins `--match-head-commit` to the SHA
  it just checked, so a stale check can't land unreviewed code).
  `FLEET_MERGE_AUTO=1` restores the old auto-merge behavior for the rare
  case that genuinely does not need the wait. Task refs are left on
  origin until the PR is confirmed
  merged; see the merge-fleet-result skill for that step. Write the PR
  message to `<message-file>` first (trailers included; line 1 is the
  title, the body starts at line 3); this script does not construct one
  for you. Run `fleet-result-inspect.sh` first: this script does not
  pause for review, it assumes you already decided the scope is correct.
  `FLEET_MERGE_SKIP_GATES=1` skips the local `gates.sh` run and lets the
  PR's required checks be the gate; the cost is learning about a red PR
  from CI instead of immediately. The precondition is mechanical:
  `gates.sh` writes a receipt keyed by tree hash on every full
  clean-tree run, and the merge script refuses the skip unless the
  history about to be pushed has a receipt younger than 24 h. An amend,
  conflict resolution, or manual edit changes the tree and voids the
  receipt: rerun the gates. The script also runs `assert-gh-auth.sh`
  first and `assert-clean-authorship.sh` on the rewritten history before
  it pushes. A receipt written by `verify-dispatch-gates.sh --with-gates`
  satisfies this same check: the authorship rewrite and wip-fold change
  commit ids, not the tree, so the tree-hash-keyed receipt still matches.
- `scripts/pr-review-status.sh <pr-number>`: one-line status for the
  wait-for-the-review-then-merge-by-hand flow -- `mergeStateStatus`, the
  CI check rollup (pass/pending/fail counts, a skipped count when it is
  nonzero, and failing check names), and the state of the fleet review:
  the `claude-fleet[bot]` review at the head commit, its inline-comment
  count, and the bot's task comment classified as
  none/running/done/dead/unknown so a missing review says whether to wait,
  to re-trigger, that nobody asked, or that the state needs reading by
  hand. A push never starts a review, so a head that moved after the last
  request needs a fresh `@claude-fleet review` comment. Freshness is `review.commit_id` against `headRefOid`, and the
  bot never approves, so COMMENTED is the success state. The REST
  comments endpoint carries no resolved/unresolved field (that's a
  GraphQL review-thread concept), so a nonzero comment count needs a
  human/session read of `gh api repos/.../pulls/<n>/comments` to judge
  whether each was already fixed or answered; the script only tells you
  there's something to read. A review the bot could not parse (body starting
  `Review task finished without a structured result`) has no findings to
  count, so it is refused with no merge line rather than called clean, and
  `--confirm-addressed` does not clear it: read the body, re-trigger the
  review, or re-run with `ALLOW_UNSTRUCTURED_REVIEW=1` if the body is a
  complete review.
- `scripts/verify-dispatch-gates.sh [--with-gates] <ref> <scratchpad-dir>`:
  the tier-1 gate check behind the `verify-dispatch` skill: an isolated
  worktree outside the repo, a cold `CARGO_TARGET_DIR`, and the full
  workspace gate list, regardless of which crate the branch touched. Run
  this (via the `verify-dispatch` skill, which adds narrow adversarial
  checks on top) before merging any fleet result: a crate-scoped or
  warm-cache gate run can let a broken branch through. `--with-gates` (or
  `VERIFY_WITH_GATES=1`) runs `scripts/gates.sh` itself inside that cold
  worktree instead of five hand-listed cargo commands, which also covers
  the `sql`/`flight-sql`/`ravel-bench` feature lanes and leaves a
  gates-pass receipt that `FLEET_MERGE_SKIP_GATES=1
  scripts/fleet-result-merge.sh` can then reuse to skip its own second
  full build.
- `scripts/affected-tests.sh [-n] -p CRATE [-p CRATE ...]`: runs tests
  for the named crates plus every workspace crate that depends on them
  (transitively), with the `ci` cargo profile; `-n` prints the affected
  set without running. When ravel-server is in the closure it also runs
  `-p ravel-server --features sql` and `-p ravel-server --features
  esql`, and when ravel-sql or ravel-server is, `-p ravel-server -p
  ravel-sql --features flight-sql`, on the same profile. Full-workspace tests still run at merge time
  (verify-dispatch-gates.sh and PR CI), so executors only pay for the
  blast radius of their change; fleet specs gate with `scripts/gates.sh
  --tier executor -p`, which reads its closure from this script's `-n`. Doctests included
  (nextest skips them, the script runs them separately).


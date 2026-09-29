---
name: verify-dispatch
description: Use before merging any fleet-dispatched branch, or to audit one retroactively - runs a cold-cache workspace-wide gate in an isolated worktree plus narrow adversarial subagents per known defect class, and reports PASS/FAIL with file:line evidence; never trust an executor's own "gates green" claim
---

# Verifying a fleet-dispatched branch

An executor's report that gates passed is not evidence; `reference.md`
beside this file lists defects that shipped behind that claim. This skill
runs two tiers and keeps them separate in the report and in what happens
next:

- **Tier 1** is deterministic: a named command exited non-zero, full stop.
- **Tier 2** is narrow semantic checks that flag things worth five minutes
  of a human's attention. It is not proof, and a tier-2-only finding never
  triggers automatic action.

Run it before merging any fleet result (then continue with
merge-fleet-result), or to audit a branch or historical commit.

## Inputs

In order of preference:

1. A fleet task id: resolve `git ls-remote origin
   refs/heads/task/<id>/result`. Empty means it landed and its task refs
   were deleted. `main` is rebase-only, so there is no merge commit: find
   the landed commits on `main` (for example `git log --grep` on the
   ticket number in their `Fixes:`/`Refs:` trailer), verify the newest,
   and diff against the parent of the oldest for the tier-2 scope.
2. A merge-commit SHA, for history from before `main` became rebase-only:
   verify `<merge>^2`, diff against `<merge>^1`.
3. Any other ref `git worktree add` accepts.

Print the exact SHA resolved before doing anything else.

## Tier 1: deterministic gates

```sh
scripts/verify-dispatch-gates.sh --with-gates <ref> <scratchpad-dir>
```

`<scratchpad-dir>` is outside this repo's working tree (the session's
scratchpad is right): a worktree left behind inside the repo shows up as
untracked content in every session's `git status` on the shared checkout,
unless its path happens to be ignored.

The script resolves `<ref>` to a SHA, creates a detached worktree there,
sets a fresh `CARGO_TARGET_DIR` (the cold cache that defeats incremental
masking; leave `RUSTC_WRAPPER`/sccache alone, it does not mask errors),
stops at the first failure with the exact command and exit code, and
always removes the worktree.

- `--with-gates` (or `VERIFY_WITH_GATES=1`) runs the worktree's own
  `scripts/gates.sh` workspace-wide, including the `sql`, `flight-sql`
  and `ravel-bench` feature lanes, and on a clean tree writes a
  gates-pass receipt keyed by tree hash. That receipt lets the merge step
  run `FLEET_MERGE_SKIP_GATES=1 scripts/fleet-result-merge.sh` instead of
  repeating the build. Print the `Gates receipt: <path>` line it emits.
- Without the flag it runs `cargo fmt --all --check`, then `cargo build`
  and `cargo clippy` with `--workspace --all-targets`, `cargo test
  --workspace` and `cargo test --doc --workspace`. That skips the feature lanes
  and writes no receipt, so the merge must not set
  `FLEET_MERGE_SKIP_GATES=1`. Use it only when `--with-gates` cannot run.

The script's exit code is authoritative. Non-zero is tier-1 FAIL: capture
the command, exit code and the last ~40 lines of output, and skip tier 2.

## Tier 2: narrow adversarial review (only on tier-1 PASS)

Get the diff scope (`git diff <base>...<ref> --stat`, plus full hunks for
anything non-trivial). Dispatch one subagent per class below, in parallel
in a single message, scoped to the changed hunks only. Tell each one: it
reviews the diff for one narrow pattern, cites file:line for any finding,
and says plainly when the pattern does not appear rather than reaching
for something else. Never ask a generic "find bugs in this diff"; its
clean verdict is worth nothing.

1. **Grouped/aggregate float correctness.** A new or changed aggregate,
   UDAF or `GROUP BY` accumulator handles NaN, `-0.0` vs `0.0`, and
   all-equal or all-infinite groups through a total order
   (`f64::total_cmp`), not `partial_cmp` seeded from `f64::MAX`/`MIN`.
2. **Error redaction.** A new or changed catch-all arm that maps internal
   errors to a generic client status (`Internal`, `Unavailable`, a bare
   5xx): every variant folded in actually needs redaction, rather than a
   caller-fixable error (a bad regex argument, an ambiguous match) being
   reported as "storage unavailable".
3. **Fail-open validation asymmetry.** A new decode or reader path for a
   persistent versioned format enforces every invariant the writer or a
   sibling version's reader enforces, instead of trusting "the writer
   would not produce that".
4. **Sort/ordering invariant drift.** A new writer for a section
   documented as sorted or order-dependent actually sorts, rather than
   using insertion order.
5. **Rename drift outside compiled code.** A renamed or removed public
   field, function or type: grep the whole workspace for the old name in
   doc comments, feature-gated bench binaries and fixture files. Compiled
   references are tier 1's job.
6. **Unguarded indexing on untrusted input.** Indexing a collection with
   a value from parsed input has an explicit bounds check, not just a
   `usize` conversion that rejects only negatives. Tier 1 misses this
   unless a test already feeds the malicious input.
7. **Vacuous tests.** For each test the branch cites as proof: which
   single production line flips to make it fail? Flag it if none does.
   Known shapes: a FaultStore test that never asserts the fault fired, a
   fixture below the threshold that gates the path under test, one
   literal reused across cases claiming separation, an input too small to
   exercise the property.
8. **Diff scope vs declared scope.** Compare `git diff --name-status
   <base>..<ref>` with the task's stated crates and docs. Flag every
   deletion and every path outside the declared scope; deletions of files
   the task never mentions are a flag at any confidence.

Each subagent returns a verdict (clean / flag), confidence (high /
medium / low), and file:line evidence when flagged.

## Report format

```
VERDICT: PASS | FAIL
REF: <resolved SHA>, <one-line source description>

TIER 1: PASS | FAIL
  <if FAIL: exact command, exit code, evidence (file:line or output tail)>

TIER 2: <n> findings (only run if tier 1 passed)
  [class] file:line - one-line claim (confidence: high/medium/low)
  ...
  Tier 2 findings need a human read before acting. They are narrow
  heuristic checks, not proof. Never auto-file or auto-redispatch on a
  tier-2-only result.
```

## What happens next

A tier-2 finding beside a tier-1 PASS goes to the user in the same turn
and triggers nothing automatically. Only a tier-1 FAIL drives the loop:

1. `gh issue create` with the full tier-1 report, linked to the
   originating ticket if there is one.
2. Re-dispatch a fix task whose spec quotes the exact command and its
   output, not "gates failed, fix it".
3. Re-run this skill on the fix's result branch.
4. Tier-1 PASS: continue with merge-fleet-result, passing
   `FLEET_MERGE_SKIP_GATES=1` if tier 1 ran with `--with-gates`.
5. A second consecutive tier-1 FAIL: stop, give the user both reports,
   and dispatch no third attempt without explicit direction.

## Validating a change to this skill

A known-good branch passing proves nothing; a tier 1 that ran nothing
passes too. Check out the parent of a known fix, or the commit its
message names as the root cause (find `fix(` commits with detailed bodies
via `git log --grep`), and confirm tier 1 fails with the symptom the fix's
message describes.

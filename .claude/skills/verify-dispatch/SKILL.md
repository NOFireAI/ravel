---
name: verify-dispatch
description: Use before merging any fleet-dispatched branch, or to audit one retroactively - runs a cold-cache workspace-wide gate in an isolated worktree plus narrow adversarial subagents per known defect class, and reports PASS/FAIL with file:line evidence; never trust an executor's own "gates green" claim
---

# Verifying a fleet-dispatched branch

An executor's own report that gates passed is not evidence. Real defects
have shipped behind that exact claim: a cross-crate field rename that
broke an untouched crate's build, a stale test fixture after a type
gained a required field, an unguarded array index that panics on a
corrupt-but-plausible input, a grouped-aggregate UDAF that silently used
the wrong float ordering, an error-redaction catch-all that swallowed
real 422s as fake 503s, and a format writer that dropped a sort
invariant its own reader still required. None of those would have shown
up if the only check was "did the executor say clippy was clean."

This skill runs two tiers. Tier 1 is deterministic and has zero false
positives: a named command exited nonzero, full stop. Tier 2 is a set of
narrow, scoped semantic checks that flag things worth five minutes of a
human's attention; it is not proof, and a tier-2-only finding never
triggers automatic action on its own. Keep these separate in the report
and in what happens next.

## When to use this

- Before merging any fleet task's result branch (this supersedes plain
  "re-run gates locally" in the merge-fleet-result skill; run this
  first, then follow merge-fleet-result's merge/push/cleanup steps once
  tier 1 passes).
- To retroactively audit a branch, or to check a specific historical
  commit (useful for validating this skill itself against a known-bad
  state; see "Validating this skill" below).

## Inputs

Accepts, in order of preference:

1. A fleet task-id: resolve via `git ls-remote origin
   refs/heads/task/<id>/result`. If that comes back empty, the branch was
   already merged and its remote ref deleted (the merge workflow deletes
   task refs right after a successful merge); fall back to asking
   for the merge commit instead, and verify `<merge>^2` (the second parent,
   the branch's own tip before the merge), not the merge commit itself.
2. A merge-commit SHA: verify `<merge>^2`, diff against `<merge>^1` (main
   before the merge) for the tier-2 hunk scope.
3. Any other ref `git worktree add` accepts (a branch, a tag, a raw SHA).

Always print the exact SHA resolved before doing anything else: an
ambiguous or moved ref is a debugging trap later.

## Procedure

### Tier 1: deterministic gates

Run `scripts/verify-dispatch-gates.sh <ref> <scratchpad-dir>`, where
`<scratchpad-dir>` is a path **outside this repo's working tree** (the
session's scratchpad directory is exactly right for this; never a
subdirectory of the repo itself: an untracked worktree left inside the
repo blocks the next `fleet_dispatch` call, which refuses to run against
a dirty primary checkout).

The script:

- Resolves `<ref>` to a SHA and creates a detached-HEAD worktree at that
  exact commit under the scratchpad path (not the ref name: a branch
  already checked out elsewhere, including `main` in the primary
  checkout, cannot be checked out a second time by `git worktree add`).
- Sets a **fresh `CARGO_TARGET_DIR`** for this run only, not the repo's
  shared target directory. This is what "cold cache" means here: it
  defeats incremental-compile masking, which is a real, previously
  observed failure mode (an executor's own claimed-green branch that
  didn't actually compile once the incremental cache was invalidated).
  It does **not** mean disabling sccache: sccache is keyed on inputs and
  doesn't mask a genuine compile error, it just avoids redoing
  already-correct work, so leave `RUSTC_WRAPPER` alone.
- Runs, workspace-wide regardless of which crate the branch touched (a
  crate-scoped `-p` run is exactly what let a cross-crate rename break an
  untouched crate's build):
  `cargo fmt --all --check`, `cargo build --workspace --all-targets`,
  `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`, `cargo test --doc --workspace`.
- Stops at the first failure, printing the exact command and its real
  exit code, and always removes the worktree on exit (pass or fail).

Treat the script's own exit code as authoritative. If it's nonzero, tier 1
is FAIL: capture the command, exit code, and the last ~40 lines of its
output as evidence, and skip tier 2 entirely (there's nothing to review
if it doesn't build).

### Tier 2: narrow adversarial review (only on tier-1 PASS)

Get the diff scope first: `git diff <merge-base-or-parent>...<ref>
--stat` and the full hunks for anything non-trivial. Then dispatch one
subagent per failure class below, in parallel (Agent tool, all
invocations in a single message), scoped to **only the diff's changed
hunks**, not the whole codebase, and not "find bugs in this diff"
generically. A generic adversarial pass produces plausible-sounding
findings that then have to be individually disproven; a narrow,
concretely-specified check is what actually catches something.

Give each subagent this framing: it is reviewing a diff for one specific,
narrow pattern; it must cite file:line for any finding; if the pattern
does not appear in the diff at all, say so plainly rather than reaching
for something else to flag.

1. **Grouped/aggregate float correctness.** Any new or modified
   aggregate, UDAF, or `GROUP BY` accumulator: check that NaN, `-0.0`
   vs `0.0`, and an all-equal or all-infinite group are handled through a
   documented *total* order (`f64::total_cmp` or equivalent) rather than
   `partial_cmp` seeded from `f64::MAX`/`f64::MIN`. (This class is real:
   DataFusion's own grouped `MIN`/`MAX` shipped exactly this bug. A NaN
   poisoned later comparisons, `-0.0`/`0.0` compared `Equal` so arrival
   order decided the winner, and an all-infinite group never displaced
   the seed.)
2. **Error redaction.** Any new or modified catch-all / wildcard match
   arm that maps an internal error type to a generic client-facing status
   (`Internal`, `Unavailable`, a bare 5xx): check that every variant
   folded into that arm actually needs redaction (its `Display` text can
   carry backend-derived content) rather than being swept in only
   because it wasn't explicitly handled. A real evaluator-level error
   (a bad regex argument, an ambiguous match) reported back as a fake
   "storage unavailable" hides the actual, fixable problem from the
   caller.
3. **Fail-open validation asymmetry.** Any new decode/reader path for a
   persistent, versioned format: check that every invariant the format's
   writer, or a sibling version's reader, already enforces is enforced
   here too, not silently trusted because "the writer wouldn't produce
   that." A tampered or buggy object exercises exactly the path that
   skipped the check.
4. **Sort/ordering invariant drift.** Any new writer for a format section
   documented as sorted or otherwise order-dependent (a dictionary, a
   monotonic index): check the new writer actually sorts, rather than
   using first-occurrence or insertion order because it's "similar
   enough." Silent drift here doesn't fail a test; it fails a byte-size
   or performance gate much later, far from the change that caused it.
5. **Cross-crate rename/field drift in non-compiled references.** Any
   renamed or removed public field, function, or type: grep the *whole
   workspace*, not just the touched crate, for the old name, including
   doc comments, a bench binary gated behind a feature so it isn't in the
   default build, or a fixture file. Most of this class is already tier
   1 (if it's compiled code, `cargo build --workspace --all-targets`
   already caught it); this subagent only adds value for references
   tier 1's compiler pass cannot see.
6. **Unguarded indexing / bounds on adversarial input.** Any new code
   that indexes a collection (an array, a dictionary, a decoded buffer)
   using a value that came from parsed or otherwise untrusted input:
   check there's an explicit bounds check before the index, not just a
   type-level `usize` cast that only rejects negative values. (The known
   shape of this bug: a `usize::try_from` guarded negative keys but not
   out-of-range positive ones, and the panic path only surfaced once a
   fuzz/property test was added; ordinary unit tests
   never construct a corrupt key. Tier 1 will **not** catch this class
   unless a test already exercises the malicious input; that gap is
   exactly why this check exists.)

7. **Vacuous tests.** Any new or modified test the branch cites as proof
   of a fix or of coverage: check it can actually detect the defect it
   names. Concrete tells, all shipped before: a fault-injection test that
   never asserts the FaultStore occurrence counter fired; a fixture
   sized below the threshold constant that gates the path under test
   (a 370-byte segment "testing" the paged-fetch path that only runs
   above `DEFAULT_WHOLE_OBJECT_THRESHOLD`); one identical literal (a
   tenant hash, a key) reused across cases that claim to prove
   cross-tenant separation; a tie-break test on an input so small the
   unfixed code passes it too. The question to answer per test: which
   single line of production code flips to make this test fail? If no
   such line exists, flag it. A vacuous test found after merge costs a
   full extra dispatch round to replace.
8. **Diff scope vs declared scope.** Compare `git diff --name-status
   <merge-base>..<ref>` against the task's stated crates and docs. Flag
   every deletion and every touched path outside the declared scope. A
   result branch can silently delete diagrams and their references from
   unrelated docs with every CI gate passing; only a scope comparison
   catches it. Deletions of files the task never mentions are a flag
   at any confidence level.

Each subagent returns: verdict (clean / flag), confidence (high / medium
/ low), and file:line evidence if flagged. A "clean" verdict from a
narrow, well-specified check is worth something; a "clean" verdict from
"did you find any bugs" is not; don't ask the latter.

## Report format

```
VERDICT: PASS | FAIL
REF: <resolved SHA>, <one-line source description>

TIER 1: PASS | FAIL
  <if FAIL: exact command, exit code, evidence (file:line or output tail)>

TIER 2: <n> findings (only run if tier 1 passed)
  [class] file:line — one-line claim (confidence: high/medium/low)
  ...
  Tier 2 findings need a human read before acting. They are narrow
  heuristic checks, not proof. Never auto-file or auto-redispatch on a
  tier-2-only result.
```

## Wiring into the dispatch flow

Only a **tier-1 FAIL** drives the auto-retry loop below. A tier-2 finding
alongside a tier-1 PASS surfaces directly to the user in the same turn;
it never triggers automatic filing or redispatch, because most of the
real defects listed above (everything except the cross-crate build
breakage) needed a dedicated audit or a differential test harness to
surface, not a generic reviewer, and a subagent's narrow check is a
lead, not a verdict.

On tier-1 FAIL:

1. `gh issue create` with the full tier-1 report (command, exit code,
   evidence), linked to the originating ticket if one exists.
2. Re-dispatch a fix task to the fleet. The spec must include the exact
   failure (not "gates failed, fix it"; include the command and its
   output), so the executor isn't guessing at what broke.
3. Re-run this skill against the fix's result branch.
4. Tier-1 PASS now: proceed to the normal merge flow
   (`scripts/fleet-result-merge.sh` / the merge-fleet-result skill).
5. Tier-1 FAIL again (2nd consecutive failure): stop. Surface both
   reports to the user directly. Do not dispatch a third attempt without
   explicit direction.

## Validating this skill

Before trusting a change to this skill or its script, don't just run it
against branches that already merged clean: a known-good branch passing
proves nothing (it would pass with a tier-1 gate that never actually ran
anything, too). Validate against real pre-fix regressions instead: check
out a commit one step before a known fix (its parent, or the specific
buggy commit itself) and confirm tier 1 correctly fails with the same
symptom the fix commit's message describes. The repo's history has
ready-made cases: search `git log --grep` for `fix(` commits
with detailed bodies, then verify against `<fix-commit>^` or the specific
commit the fix's own message names as the root cause.

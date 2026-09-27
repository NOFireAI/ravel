# deliver-epic: why the rules exist

Background for SKILL.md; nothing here changes the procedure.

- **Epic assignee as the claim.** Ownership that lives only in one
  session's head collides as soon as a second session picks "the next
  obvious thing".
- **ADR number is the issue number.** GitHub allocates issue numbers
  atomically, so parallel epics cannot collide and no reservation stub is
  committed before approval.
- **Diagram in every ADR.** A prose-only ADR has cost an extra approval
  round trip.
- **End-to-end reachability test.** Crate-level tests pass against code no
  production path builds: a merged, crate-tested cache shipped that no
  caller constructed.
- **No two tasks in one crate per wave.** Two tasks dispatched into one
  crate with disjoint file lists collided: one added a parameter to a
  function the other grew a new call site for, the merge was textually
  clean, and the build broke on `E0061`.
- **Same-file tasks are one task.** Splitting same-file work across
  dispatches produced two divergent rewrites of the same code that needed
  manual reconciliation.
- **Intent script, never a direct dispatch.** A check done in your head is
  not on the ticket for the next session, and a skip flag used once to get
  past a refusal gets used again; a guard routinely worked around has
  stopped being one.
- **Record before watching.** A dropped session with unrecorded task ids
  orphans running work. `gh` exits 0 on a body edit another session
  overwrote, which is why the orchestrator reads the line back.
- **Watcher under a Monitor.** `nohup fleet-watch.sh ... &` returns
  control at once, so the tool marks the call complete before the loop
  runs and every later terminal event is lost; a `run_in_background`
  watcher dies at the harness's background cap.
- **Checkpoint before the PR, reviewer isolated.** Skipping the checkpoint
  moves round one of review into public, where each finding costs a CI
  cycle: one epic ran 10 review rounds on a single PR, and about half of
  all its findings were unpaired prose claims a checkpoint would have
  caught. A reviewer dispatched without worktree isolation reverted a
  hand-authored, uncommitted fix to its committed state, with no stash and
  no reflog entry.
- **Remove worktrees per wave.** Each wave's and reviewer's worktree
  carries its own build-cache target directory; batching removal to the
  end of an epic compounds disk use across every remaining wave.

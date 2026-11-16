# Counterexample: turn committed while its manual effect is in doubt

Config: `negative/commit-while-in-doubt.cfg`
(`CommitWhileInDoubt = TRUE`; smoke bounds with `Policies = {"manual"}`).
Expected: `PolicyControlsRedispatch` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant PolicyControlsRedispatch is violated.

Trace (19 states, depth 19). Action names are read off the state.

    State 1:  <Initial predicate>
    State 2:  CreateHead
    State 3:  Read(w1)
    State 4:  Takeover(w1) (epoch 1)
    State 5:  Read(w1)
    State 6:  BeginTurn(w1) (turn 1)
    State 7:  Read(w1)
    State 8:  RecordStep(w1, h1)
    State 9:  Read(w1)
    State 10: CommitIntent(w1, h1) (policy manual)
    State 11: Read(w1)
    State 12: AuthorizeFirst(w1) (attempt 1)
    State 13: Dispatch(w1) (provider call 1, attempt 1, executed)
    State 14: LoseResponse(w1)
    State 15: Read(w1) (attempt 1 ambiguous)
    State 16: Reauthorize(w1) (manual: effect "indoubt", nothing dispatched)
    State 17: Cancel (cancel request on turn 1)
    State 18: Read(w1)
    State 19: BrokenCommitWhileInDoubt(w1) (input consumed, result
              "cancelled", effect still in doubt)

## What the switch breaks

An effect in doubt blocks its turn: `complete_turn` and `fail_turn` refuse
with `EffectInDoubt` (`NoEffectInDoubt`) until an admin reconciles the
effect. A cancel request does not lift that: the owner waits. The switch
commits the cancelled turn over the in-doubt effect.

## Why it is a real bug if it ever held

Attempt 1 did execute (state 13). After state 19 the input is published as
cancelled, so the caller is told nothing happened while the provider holds
an executed side effect nobody will reconcile: the turn that would have
waited for the admin is gone. `PolicyControlsRedispatch` requires that no
turn result lands while the effect is in doubt.

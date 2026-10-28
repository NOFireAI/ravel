# Counterexample: ambiguous manual attempt dispatched again

Config: `negative/ambiguous-manual-attempt-retried.cfg`
(`AmbiguousManualAttemptRetried = TRUE`; smoke bounds with
`Policies = {"manual"}`).
Expected: `PolicyControlsRedispatch` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant PolicyControlsRedispatch is violated.

Trace (17 states, depth 17). Action names are read off the state, as in the
other counterexamples here.

    State 1:  <Initial predicate>
    State 2:  CreateHead
    State 3:  Read(w1)
    State 4:  Takeover(w1) (epoch 1)
    State 5:  Read(w1)
    State 6:  BeginTurn(w1) (turn 1)
    State 7:  Read(w1)
    State 8:  RecordStep(w1, h1)
    State 9:  Read(w1)
    State 10: CommitIntent(w1, h1) (effect "intent", policy manual)
    State 11: Read(w1)
    State 12: AuthorizeFirst(w1) (effect "authorized", attempt 1; w1 holds it)
    State 13: Dispatch(w1) (provider log <<attempt 1, executed>>)
    State 14: LoseResponse(w1) (w1 back to idle, holding nothing)
    State 15: Read(w1) (attempt 1 authorized, no result: ambiguous)
    State 16: BrokenAmbiguousManualAttemptRetried(w1) (w1 holds attempt 1)
    State 17: Dispatch(w1) (provider log <<attempt 1>>, <<attempt 1>>)

## What the switch breaks

`authorize_attempt` answers an unchanged `AttemptAuthorized` for a repeat of
the attempt that is already authorized. That reply is only safe for the
caller whose own authorization CAS lost its response. A runner that reads an
authorized attempt it does not hold must treat it as possibly dispatched;
under manual, its next authorization publishes `InDoubt` and dispatches
nothing. The switch takes the idempotent reply as permission and dispatches
the same attempt a second time.

## Why it is a real bug if it ever held

The manual policy exists for tools whose duplicate execution is unsafe and
which the provider cannot deduplicate. State 17 executes attempt 1 twice
with no reconcile audit object in between. `PolicyControlsRedispatch`
requires each attempt to be dispatched at most once, and every manual
attempt after the first to rest on an admin's audited authorization.

# Counterexample: late effect result replaces the recorded one

Config: `negative/late-result-overwrites.cfg`
(`LateResultOverwrites = TRUE`; smoke bounds with
`Policies = {"at_least_once"}`).
Expected: `StepTranscriptCompatible` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant StepTranscriptCompatible is violated.

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
    State 10: CommitIntent(w1, h1) (policy at_least_once)
    State 11: Read(w1)
    State 12: AuthorizeFirst(w1) (attempt 1)
    State 13: Dispatch(w1) (provider call 1, attempt 1, executed)
    State 14: LoseResponse(w1)
    State 15: Read(w1) (attempt 1 ambiguous)
    State 16: Reauthorize(w1) (attempt 2, the policy allows a retry)
    State 17: Dispatch(w1) (provider call 2, attempt 2, executed)
    State 18: ResultCallback(1) (effect completed with call 1's result,
              result attempt 1)
    State 19: BrokenLateResultOverwrites(2) (effect result replaced by call
              2's, result attempt 2)

## What the switch breaks

Under at_least_once two executions of one effect are allowed, and both may
report a result. `record_effect_result` keeps the first one recorded: a
result for another attempt on a completed effect answers
`LateResultIgnored`, and a different result for the same attempt is a typed
`IdempotencyConflict`. The switch lets the second result overwrite the
journal.

## Why it is a real bug if it ever held

The turn may already have replayed the first result into its output. After
state 19 a replay reads call 2's result while the committed output was
computed from call 1's, so two replays of one turn disagree.
`StepTranscriptCompatible` requires a completed effect record to be written
once and never rewritten.

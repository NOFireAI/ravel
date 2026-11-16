# Counterexample: late effect result replaces the recorded one

Config: `negative/late-result-overwrites.cfg`
(`LateResultOverwrites = TRUE`; smoke bounds with
`Policies = {"at_least_once"}`).
Expected: `StepTranscriptCompatible` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant StepTranscriptCompatible is violated.

Trace (19 states, depth 19; 4703 distinct states found). Action names are
read off the state.

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
    State 18: ResultCallback(2) (effect completed with call 2's result,
              result attempt 2)
    State 19: BrokenLateResultOverwrites(1) (effect result replaced by call
              1's, result attempt 1)

## What the switch breaks

Under at_least_once two executions of one effect are allowed, and both may
report a result. `Kernel::record_effect_result` keeps the one recorded: a
result for another attempt on a completed effect answers
`LateResultIgnored`, and a different result for the same attempt is a typed
`IdempotencyConflict` (`crates/ravel-agents-core/src/kernel.rs:1835`). A
result from an attempt older than the journaled one is also
`LateResultIgnored` while the effect is open (`kernel.rs:1856`), and the
correct model refuses it too (`ResultAdmitted`), so call 1's result reaches
the effect only after it completed, through the switch.

## Why it is a real bug if it ever held

The turn may already have replayed the recorded result into its output.
After state 19 a replay reads call 1's result while the committed output was
computed from call 2's, so two replays of one turn disagree.
`StepTranscriptCompatible` requires a completed effect record to be written
once and never rewritten.

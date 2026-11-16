# Counterexample: result recorded for a cancelled input

Config: `negative/cancelled-result-recorded.cfg`
(`CancelledResultRecorded = TRUE`; smoke bounds with all three policies).
Expected: `CancellationRespected` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant CancellationRespected is violated.

Trace (18 states, depth 18; 9163 distinct states found). Action names are
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
    State 10: CommitIntent(w1, h1) (policy manual)
    State 11: Read(w1)
    State 12: AuthorizeFirst(w1) (attempt 1)
    State 13: Cancel (cancel request on turn 1)
    State 14: Dispatch(w1) (provider call 1, attempt 1, executed)
    State 15: LoseResponse(w1)
    State 16: Read(w1) (w1 sees the request)
    State 17: CommitCancelled(w1) (input settled cancelled; effect still
              "authorized", attempt 1)
    State 18: BrokenCancelledResultRecorded(1) (effect completed with call
              1's result)

## What the switch breaks

Once the effect's input has a cancelled result, `Kernel::record_effect_result`
answers `LateResultIgnored` and leaves HEAD unchanged
(`crates/ravel-agents-core/src/kernel.rs:1851` computes `input_cancelled`,
`kernel.rs:1856` answers). The correct model's `ResultAdmitted` refuses the
result the same way. The switch records call 1's result on the cancelled
input's effect.

## Why it is a real bug if it ever held

The input was published cancelled in state 17, and its turn will never
replay the effect. Recording a result after that rewrites the journal of a
settled input: a reader of HEAD sees a completed effect for an input whose
published outcome is cancelled.
`CancellationRespected` requires that once the input is cancelled no HEAD
write changes its effect.

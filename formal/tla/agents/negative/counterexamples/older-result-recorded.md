# Counterexample: result from an older attempt recorded

Config: `negative/older-result-recorded.cfg`
(`OlderResultRecorded = TRUE`; smoke bounds with
`Policies = {"at_least_once"}`).
Expected: `EffectResultFromExecution` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant EffectResultFromExecution is violated.

Trace (17 states, depth 17; 2717 distinct states found). Action names are
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
    State 16: Reauthorize(w1) (attempt 2)
    State 17: BrokenOlderResultRecorded(1) (effect completed with call 1's
              result, result attempt 1, journaled attempt 2)

## What the switch breaks

`Kernel::record_effect_result` answers `LateResultIgnored` and leaves HEAD
unchanged for a result whose attempt is older than the journaled one
(`crates/ravel-agents-core/src/kernel.rs:1856`), and the runner reports that
answer as evidence, not as the effect's result
(`crates/ravel-agents-runtime/src/effects.rs:517`, `record_result`). The
correct model's `RecordHeldResult` and `ResultCallback` admit a result only
for the journaled attempt (`ResultAdmitted`). The switch records call 1's
result while attempt 2 is journaled.

## Why it is a real bug if it ever held

Attempt 2 was authorized and its dispatcher may still execute it. The
journal now says the effect completed with attempt 1's result, so the turn
replays a result that the attempt it is waiting on did not produce, and
attempt 2's own result, when it arrives, is refused as late against a
completed record. `EffectResultFromExecution` requires the recorded result
attempt to equal the journaled attempt and the recorded result to come from
a provider call for that attempt.

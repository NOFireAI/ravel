# Counterexample: replayed step with another input rewrites the transcript

Config: `negative/step-input-drift.cfg`
(`StepInputDrift = TRUE`; smoke bounds with `Hashes = {h1, h2}`).
Expected: `StepTranscriptCompatible` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant StepTranscriptCompatible is violated.

Trace (10 states, depth 10). Action names are read off the state.

    State 1:  <Initial predicate>
    State 2:  CreateHead
    State 3:  Read(w1)
    State 4:  Takeover(w1) (epoch 1)
    State 5:  Read(w1)
    State 6:  BeginTurn(w1) (turn 1)
    State 7:  Read(w1)
    State 8:  RecordStep(w1, h1) (step completed with input hash h1)
    State 9:  Read(w1)
    State 10: BrokenStepInputDrift(w1, h2) (step record now carries h2)

## What the switch breaks

A step key is journaled once with its tool identity and input hash. A
replay of the same key with another hash is non-deterministic replay:
`record_step` refuses it by blocking the turn (`block_turn`, reply
`Blocked` with `NonDeterministicReplay`) and leaves the record as it was. The
switch writes the new hash over the old record instead.

## Why it is a real bug if it ever held

The step's completed output was produced from input h1. After state 10 the
transcript claims it came from h2, so a later replay that computes h2 reads
back an output computed from other input and continues as if it matched.
`StepTranscriptCompatible` requires every HEAD write to keep a completed
step's hash and an effect's identity unchanged.

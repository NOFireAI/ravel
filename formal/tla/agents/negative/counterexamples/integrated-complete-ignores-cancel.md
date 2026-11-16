# Counterexample: cancelled turn committed as completed

Config: `negative/integrated-complete-ignores-cancel.cfg`
(`CompleteIgnoresCancel = TRUE`; `MCAgentIntegrated` smoke bounds).
Expected: `CancellationRespected` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant CancellationRespected is violated.

Trace (25 states, depth 25). Action names are read off the state.

    State 1:  <Initial predicate> (policy at_least_once)
    State 2:  CreateHead
    State 3:  ClientRead
    State 4:  PutPayload
    State 5:  EnqueueCas (client input at accept_seq 0)
    State 6:  Ack
    State 7:  Read(w1)
    State 8:  Takeover(w1) (epoch 1)
    State 9:  Read(w1)
    State 10: BeginTurn(w1) (turn 1)
    State 11: Read(w1)
    State 12: RecordStep(w1)
    State 13: Read(w1)
    State 14: CommitIntent(w1) (policy at_least_once)
    State 15: Read(w1)
    State 16: AuthorizeFirst(w1) (attempt 1)
    State 17: Dispatch(w1) (provider call 1, attempt 1, executed)
    State 18: LoseResponse(w1)
    State 19: ResultCallback(1) (effect completed from call 1)
    State 20: Read(w1)
    State 21: PutTurnOutput(w1) (turn 1 output written)
    State 22: Cancel (cancel request on turn 1)
    State 23: CompleteTurn(w1) (CAS fails: HEAD moved)
    State 24: Read(w1) (reads the cancel request)
    State 25: BrokenCompleteIgnoresCancel(w1) (result "completed" for a
              cancelled input)

## What the switch breaks

A cancel request on the active turn is committed at the turn's next commit
as a cancelled result (`CommitKind`, `CommitCancelled`). The switch reads
the request and commits the turn completed with the output it already
wrote.

## Why it is a real bug if it ever held

The client's cancel at state 22 was accepted: it is on HEAD's `cancels`.
After state 25 the input's one logical completion says completed and the
state version moved to the turn's output, so the client is told its
cancelled request ran to completion and the agent's state carries the
work the cancel was meant to stop. `CancellationRespected` requires every
result for a cancelled input to be cancelled. The external effect itself
is not undone by a cancel (it already ran at state 17); what the cancel
controls is the published outcome and the state version.

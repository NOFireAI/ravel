# Counterexample: completion rebase loses a requested cancellation

Config: `negative/rebase-ignores-cancel-request.cfg`
(`RebaseIgnoresCancelRequest = TRUE`, smoke bounds: one tenant, one agent,
two clients, two workers, MaxRevision 6, MaxEpoch 2, MaxTimerGen 0).
Expected: `CancellationRespected` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant CancellationRespected is violated.

Trace (10 states, depth 10). TLC labels every step `MCNext` because the
next-state relation is conjoined with the witness; the action names below
are read off the client and worker program counters and the HEAD fields
each step changes.

    State 1: <Initial predicate>
    State 2: CreateHead
    State 3: ReadHead (client c1 at "dep")
    State 4: CasHead (c1 acked, accept_seq 0 pending)
    State 5: Takeover (w1 owns epoch 1)
    State 6: BeginTurn (turn 1 bound to accept_seq 0)
    State 7: PutTurnOutput (w1 at "dep", output object written)
    State 8: Cancel (c1 done; active turn 1 carries creq TRUE, input still pending)
    State 9: CompleteTurn (CAS on the version read in state 7 fails; w1 at "rebase")
    State 10: BrokenRebaseIgnoresCancelRequest (completed result for accept_seq 0)

## What the switch breaks

After a failed completion CAS the rebase re-reads HEAD and, when owner,
epoch, active turn and base state version still match, commits the turn on
the new root. The correct rebase computes the result from that new root, so
a cancellation recorded on it turns the result into a cancelled one. The
switch commits the result computed from the root it read before the output
PUT, ignoring the request.

## Why it is a real bug if it ever held

`CancellationRespected` requires that an input in the cancel set ends with a
cancelled result. In state 10 accept_seq 0 is in the cancel set and its result
is completed, and the state version moved: a client that was answered
CancelRequested in state 8 sees its input take effect. The kernel's
`complete_turn` reads the turn's `CancellationState` from the root it is
applied to, which is what the correct model (switch FALSE) does.

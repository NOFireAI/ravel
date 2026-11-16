# Counterexample: tool call dispatched before its intent (end to end)

Config: `negative/integrated-dispatch-before-intent.cfg`
(`DispatchBeforeIntent = TRUE`; `MCAgentIntegrated` smoke bounds).
Expected: `DispatchRequiresCommittedAuthorization` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant DispatchRequiresCommittedAuthorization is violated.

Trace (14 states, depth 14). Action names are read off the state.

    State 1:  <Initial predicate> (policy provider_idempotency)
    State 2:  CreateHead
    State 3:  ClientRead
    State 4:  PutPayload
    State 5:  EnqueueCas (client input at accept_seq 0)
    State 6:  Read(w1)
    State 7:  Takeover(w1) (epoch 1)
    State 8:  Read(w1)
    State 9:  BeginTurn(w1) (turn 1 on accept_seq 0)
    State 10: Read(w1)
    State 11: RecordStep(w1)
    State 12: Read(w1)
    State 13: BrokenDispatchBeforeIntent(w1) (worker ready to execute
              attempt 1; the journal holds no intent)
    State 14: Dispatch(w1) (provider call 1, attempt 1, executed)

## What the switch breaks

The same switch as `negative/dispatch-before-intent.cfg` in
`MCAgentEffects`, here behind a real enqueue and takeover: the handler's
tool call goes out after the step is recorded and before `CommitIntent` and
`AuthorizeFirst` journal it.

## Why it is a real bug if it ever held

The provider executed attempt 1 (state 14) while HEAD records no intent
and no authorized attempt. A crash or takeover now replays the handler,
which journals a fresh intent and authorizes attempt 1 again: the executed
call is invisible to the recovery policy, so even `manual` re-executes it.
`DispatchRequiresCommittedAuthorization` requires every provider call to
name an attempt HEAD has authorized.

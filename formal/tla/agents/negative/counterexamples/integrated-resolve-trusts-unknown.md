# Counterexample: unknown enqueue outcome answered without a re-read

Config: `negative/integrated-resolve-trusts-unknown.cfg`
(`ResolveTrustsUnknown = TRUE`; timer race bounds: one worker, one timer
generation, `Policies = {"at_least_once", "manual"}`).
Expected: `AcceptedImpliesRecoverable` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant AcceptedImpliesRecoverable is violated.

Trace (8 states, depth 8). Action names are read off the state.

    State 1: <Initial predicate> (policy at_least_once)
    State 2: CreateHead
    State 3: ClientRead
    State 4: PutPayload
    State 5: ScheduleTimer (generation 1 armed: HEAD moves under the client)
    State 6: EnqueueCasLostResponse (the CAS on the read version failed and
             its response was lost: outcome unknown)
    State 7: BrokenResolveTrustsUnknown (client takes the handle the CAS
             would have returned)
    State 8: Ack (client answered; HEAD holds no receipt)

## What the switch breaks

A lost enqueue response leaves the outcome unknown: the CAS may have
applied or failed. `Resolve` settles it by reading HEAD and answering only
from a receipt it finds there; otherwise the client retries the enqueue.
The switch answers with the handle the CAS would have produced, without
the read.

## Why it is a real bug if it ever held

At state 6 the CAS failed because the timer schedule moved HEAD. The
client is told its request was accepted at `accept_seq` 0, and nothing on
HEAD records it: the input is never run, and a later enqueue by anyone can
take `accept_seq` 0 for a different input. `AcceptedImpliesRecoverable`
requires an answered handle to be one of HEAD's receipts. This config needs
the timer: in the smoke bounds no other writer moves HEAD before the first
enqueue, so the enqueue CAS cannot fail there.

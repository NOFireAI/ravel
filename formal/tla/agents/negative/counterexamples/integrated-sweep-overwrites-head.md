# Counterexample: sweep fires a timer by overwriting HEAD

Config: `negative/integrated-sweep-overwrites-head.cfg`
(`SweepOverwritesHead = TRUE`; timer race bounds: one worker, one timer
generation, `Policies = {"at_least_once", "manual"}`).
Expected: `NoLostConcurrentEnqueue` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant NoLostConcurrentEnqueue is violated.

Trace (9 states, depth 9). Action names are read off the state.

    State 1: <Initial predicate> (policy manual)
    State 2: CreateHead
    State 3: ScheduleTimer (generation 1 armed)
    State 4: ClientRead
    State 5: PutPayload
    State 6: WriteHint (wake hint for generation 1)
    State 7: SweepRead(1) (sweep holds the HEAD with no input)
    State 8: EnqueueCas (client input at accept_seq 0, receipt recorded)
    State 9: BrokenSweepOverwritesHead (HEAD replaced by the sweep's read
             plus the timer input, which takes accept_seq 0; the client's
             input and receipt are gone)

## What the switch breaks

`SweepFire` writes the fired generation with a CAS on the version
`SweepRead` returned; a failed CAS leaves HEAD alone and the sweep
starts over from the hint. The switch writes the same content with an
unconditional PUT.

## Why it is a real bug if it ever held

The sweep's read (state 7) is one write behind by the time it fires. The
overwrite drops the client's accepted input and its receipt, and reuses
`accept_seq` 0 for the timer message. The enqueue CAS at state 8
succeeded, so the client is answered from its receipt and its request is
lost after acknowledgement. `NoLostConcurrentEnqueue` requires every write
to keep the inputs and the inbox entries of the HEAD it replaces.

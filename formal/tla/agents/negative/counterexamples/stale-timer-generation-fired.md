# Counterexample: sweep fires a timer generation the root no longer arms

Config: `negative/stale-timer-generation-fired.cfg`
(`StaleTimerGenerationFired = TRUE`; bounds: one tenant, one agent, one
client, one input, two workers, MaxRevision 4, MaxEpoch 2, MaxTimerGen 2,
`VIEW View`, `SYMMETRY WorkerSymmetry`, the eleven invariants).
Expected: `TimerGenerationDeliveredOnce` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant TimerGenerationDeliveredOnce is violated.

Trace (7 states, depth 7). TLC labels every step `MCNext` because the
next-state relation is conjoined with the witness; the action names below
are read off the HEAD fields, the hint object and the `co` and `due`
counters each step changes.

    State 1: <Initial predicate>
    State 2: CreateHead (rev 1, no owner, timer generation 0)
    State 3: ScheduleTimer (generation 1 armed, rev 2; no owner needed)
    State 4: Destroy (tombstone set, rev 3; the armed timer stays armed)
    State 5: WriteHint (hint <<"hint", 1, 1>> for incarnation 1, generation 1)
    State 6: TimerDue (due generation 1)
    State 7: BrokenStaleTimerGenerationFired (timer input enqueued on the
             tombstoned root, rev 4, timer still armed)

## What the switch breaks

A due hint is only a pointer. The sweep may enqueue the timer message only
through a CAS on a root where the hint's generation is still armed, the agent
is not destroyed and the hint names the current incarnation; that CAS marks
the generation delivered. The switch enqueues from any due hint that fails
that check, and leaves the timer state as it was.

## Why it is a real bug if it ever held

Generation 1 is armed (state 3) and the agent is destroyed (state 4), which
leaves the timer armed but refuses every later fire (`Kernel::fire_timer`
answers `AgentGone` through `check_incarnation`). The hint is still in the
store, and the broken sweep enqueues a timer input on the tombstoned root
(state 7) without marking the generation delivered.
`TimerGenerationDeliveredOnce` requires every new timer input to move its
generation from armed to delivered in the same write, so this fire is one
the kernel would refuse. A cancelled or superseded generation gives the same
violation in a longer trace; TLC reports the shortest. The correct model
(switch FALSE) deletes the stale hint instead.

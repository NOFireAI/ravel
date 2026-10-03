---
name: profile-hotspot
description: Use when something is slow, or flaky in a way that smells like slow, and nobody knows which phase owns the time. Timer bisection plus the discipline rules that keep a measurement on a loaded machine from producing a confident wrong answer. Triggers: "why is this slow", "profile this", "where does the time go", a test that flakes on timing.
---

# Finding where the time goes

Use this when something is slow, or flaky in a way that smells like slow, and
nobody knows which phase owns the time. It is a measurement procedure, not a
fix procedure. It ends when you can name the dominant phase with a number.

The method is timer bisection: time the top-level phases, find the one that
dominates, descend into it, repeat. It is crude and it works. What makes it
work is not the timers, it is the discipline rules in "Measuring on a machine
you do not control" below. Skipping those is how a measurement produces a
confident wrong answer.

## The procedure

1. **Find the cheapest reproducer.** A single test beats a benchmark beats a
   running server. If a test already exhibits it, use that test. Prebuild the
   binary (`cargo build --tests`) so compile time is not inside your numbers.

2. **Time the top-level phases.** Wrap each with `std::time::Instant` and
   print. Do not guess which phase matters; time all of them, including the
   ones you are certain are fast. In one real investigation the three phases
   everyone suspected summed to under 10 ms out of 4,670 ms.

3. **Read the share, not the duration.** One phase at >90% means descend into
   it and ignore everything else. Several phases at 20-30% means you are
   measuring at the wrong granularity, or the cost is genuinely spread and
   this method will not find a single answer.

4. **Descend and repeat** until the dominant leaf is a loop, a call, or an
   allocation you can name and count.

5. **Count the thing, do not just time it.** Once you reach the leaf, print
   the count as well as the duration. `bucket_listing 4705 ms` is a symptom;
   `buckets=496089` is the root cause. A per-item cost of 9.5 microseconds is
   fine and says the item cost is not the problem, the count is.

6. **Revert the instrumentation.** It is scaffolding. Do not commit timers.
   If the codebase should have emitted this without you, that is its own
   ticket (see "The durable fix" below).

## Measuring on a machine you do not control

These are the rules that separate a measurement from an anecdote. Every one
of them exists because breaking it has produced a confident wrong answer
that reached a user.

- **Report the load average next to every number.** `uptime`. A developer box
  shared with other tenants swings between load 1 and load 60, and every
  timing swings with it.
- **Never conclude from one run.** Five runs minimum, report the spread. A
  single sample on a moving machine tells you about the machine.
- **Never compare two numbers taken at different times.** This is the one that
  bites hardest. "Branch takes 30s, main takes 5.7s" was measured minutes
  apart and was pure load difference; interleaved A/B runs showed the two were
  identical and that *main failed too*.
- **Interleave any A/B.** Alternate the two arms within one loop so drift
  cannot favour either:
  ```sh
  for i in 1 2 3; do
    for arm in branch main; do ... ; done
  done
  ```
- **A passing run is not evidence your change fixed it.** If the failure is
  load-dependent, every arm passes sometimes. Two "isolating" experiments
  can both get lucky draws and be read as proof of a fix that is not there.

## When the reproducer will not reproduce

An idle machine often will not hit the threshold (a deadline, a timeout) that
made the problem visible. That does not block the work, and it must not be
reported as "cannot reproduce".

The threshold breach and the fixed cost are different things. The fixed cost
is present at any load; it is simply smaller than the threshold when the box
is quiet. Measure the fixed cost. In one real case the breach needed load
40-60, but the 4.7-second `resolve` was plainly visible at load 1.

## Before you widen a test timeout

A flaky concurrent test invites one reflex fix: make the window bigger.
Run one check first. If every assertion before the failing wait already
passed, the operation itself completed, and the test is missing a
completion signal, not margin. Widening then reproduces the same failure
at a larger number: a real case widened 2 s to 6 s and failed at 6.76 s,
because each reader's polling loop was bounded by a wall clock set before
the writers were joined. No window fixes a race between two independently
bounded clocks. The real fix was a `writers_flushed` completion flag the
readers wait on.

So: widen a timeout only after you can say which synchronization signal
already proves the waited-for work is done. If no such signal exists,
add one; that is the fix.

## What is not an answer

- Raising the deadline or timeout.
- `#[ignore]`, `#[serial]`, or a retry.
- Reducing what the reproducer does until it fits.

Each hides the measurement instead of making it. If one of them is
nonetheless the right call, that is a recommendation to report, not a change
to make quietly. "The cost is irreducible, and here are the numbers that show
it" is a complete and successful outcome.

## The durable fix

Hand-rolled timers answer one question and are thrown away. The next question
starts from zero.

ADR-0044 decision 5 specifies `tracing` spans on the query path for exactly
this reason, and notes that without them "a slow query cannot be attributed to
a phase". Once those spans are implemented, steps 2-4 above become reading
span durations from a documented command, and this skill shrinks to its
discipline rules.

Until then: if you find yourself hand-instrumenting a path that a shipped span
should already cover, say so in your report. That is a finding about the
codebase, not just about your bug.

## The shape of a good report

A phase table with min/median/max, the dominant phase named with its share,
the descent to a root cause with a count, and an explicit list of which prior
hypotheses the measurement refuted.

That last part matters. An investigation can be driven by several confident
hypotheses that all turn out wrong. Recording which leads died saves the next
person from re-walking them.

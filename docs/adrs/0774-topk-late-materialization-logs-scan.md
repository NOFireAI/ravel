# ADR-0774: TopK late materialization for the logs scan

Status: Proposed.

This file claims the number. The decision it records is implemented under
issue #774 (epic #680); the sections below are filled in by that change.

The number is issue #774's, the ticket that produced this ADR, rather than
epic #680's. The README's rule ("the number is the GitHub issue number of
the epic that produced it") exists so parallel sessions cannot collide on a
number, and epic #680 has several tickets in flight at once: two of them
taking the epic number would collide exactly as sequential numbering did.
A ticket number is allocated by the same atomic counter and is unique per
change.

## Context

On the ClickBench reference tenant (100M rows, 8,424 objects, 105 declared
columns) a wide `ORDER BY ... LIMIT k` over `logs` does not finish, while
the same statement over two columns finishes in 24.8 s. The scan decodes
every projected column of every surviving block before the filter and the
TopK see a row, so the cost of a ten-row answer is the cost of
materializing the whole table.

## Decision

To be written.

## Consequences

To be written.

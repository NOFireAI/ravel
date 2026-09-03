# exactly-once-dedup

Negative control over a reachability obligation, so its `.expect` is
`exit=0` rather than a violation.

`reach-dup.cfg` runs `DuplicateUnreachable` on the correct model with no
idempotency key, and TLC MUST report it violated: a retry after a lost
acknowledgement leaves two durable commit records holding the same content,
which is the at-least-once behaviour logs and spans are documented to have.

`negative/exactly-once-dedup.cfg` is that same configuration with
`RetryDedups = TRUE`, which makes the retry silently deduplicate. The
duplicate becomes unreachable, TLC finds no violation, and the run exits 0.
A run that still reported a violation would mean the switch does nothing.

The pair exists because `AtLeastOnce` alone cannot detect a regression to
exactly-once delivery: exactly-once satisfies it. Only an obligation that the
duplicate is REACHABLE fails when the behaviour disappears.

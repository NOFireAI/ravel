# ADR-0052: Online resharding (epic EK)

Status: stub — number claimed, full ADR in progress. Part of epic
issue #461, program #450.

## Context

`shard_count` is immutable per (tenant, signal); changing it today is a
documented forbidden data-loss operation (S3-06/S5-14, flagged by four
reviewers in the adversarial review). Ingest parallelism, key layout,
catalog fan-out, and the client-visible commit-token format are all
pinned to a day-one guess. A tenant that outgrows its initial
`shard_count` has no path forward except a bespoke full-tenant rewrite.

Depends on epic EC (#453) making `shard_count` a durable,
startup-checked property (EC5) before this epic's design can assume a
stable, observable value to reshard from.

Full Context/Decision/Rejected-alternatives/Consequences to follow in a
subsequent commit once design research completes.

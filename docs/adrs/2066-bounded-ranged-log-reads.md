# ADR-2066: Bounded ranged log reads

- Status: Proposed
- Date: 2026-09-27
- Refs: #2066, #2023, #1506, ADR-0107, ADR-0699, ADR-0996, ADR-1196, ADR-2014, ADR-2023

## Context

Ravel reads a log object in one of two shapes. `cost-based`, the default,
reads every touched object whole in one GET at the reference cost profile,
because intra-region transfer is free and requests are billed (ADR-1196).
`byte-minimal` reads the footer, the directories and the projected column
chunks as ranges (ADR-0107, ADR-0699).

On a store served from a local disk the bill is not the constraint; the disk
is. The ClickBench reference machine (c6a.4xlarge, RustFS on loopback, a
500 GB gp2 volume) shows the two shapes hitting the two ceilings of that
disk:

- **Whole-object reads are bandwidth-bound.** A cold statement reads roughly
  the whole 11.24 GB corpus in about 42.6 s, about 264 MB/s, which is the
  volume's 250 MiB/s throughput cap. v0.19.0's cold sum is 1,712.2 s.
- **Ranged reads are request-bound.** v0.18.0 made `byte-minimal` the
  loopback default (ADR-2014). Cold fell to 1,197.6 s, but under the ten
  concurrent queries ClickBench runs, `iostat` read 3,220 reads/s at a 50 KB
  average and 87% utilisation against the volume's 3,000 IOPS burst, and
  throughput fell from about 0.40 to 0.123 queries per second. ADR-2023
  withdrew that default.

The ranged plan spends more requests than it has to. On the ClickBench
corpus (2,617 objects, about 4.3 MB and one short row group each), a
narrow projection of one object costs 4 GETs:
1. the suffix probe;
2. FIELD_DIR;
3. STREAM_DIR;
4. one chunk run.

FIELD_DIR and STREAM_DIR are adjacent at the front of the object but are
fetched separately (`log_fetcher.rs`, `place_front_sections`). ADR-0996's
description of that step as one coalesced GET is stale. Chunk runs have no
per-object cap: `coalesce_byte_extents` fuses runs whose gap is within the
request cost, but a projection over many row groups of a large L1 part can
still issue many runs. The metrics path already bounds this: an L0 segment
fetch issues at most 4 page-range GETs, bridging the smallest gaps
(`bound_runs`, `fetcher.rs`).

## Decision

1. **A ranged log read issues as few GETs as its extents allow.** No format
   change:
   - When both front sections are needed, STREAM_DIR and FIELD_DIR are
     fetched in one GET covering both.
   - Chunk runs per object are capped at 4, as the metrics path caps an L0
     segment's page ranges (`MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT`): after
     already-covered runs are dropped, runs beyond the cap are merged across
     their smallest gaps. The bridged bytes are fetched, reserved and
     charged like any other. The existing covering-read check (a read that
     would cover most of the object becomes one whole-object GET) still
     applies after bridging.
   - A narrow projection of a one-row-group object then costs at most 3
     GETs: probe, front sections, one run.

   This applies under every policy that reads ranges. `cost-based` at the
   reference profile is unchanged: it reads whole objects.

2. **Measured, not assumed.** Decision 1 changes no default. A fresh
   reference-machine run under explicit `--logs-fetch-policy byte-minimal`,
   before and after, registered on #2066 first, records the GETs per object
   and the cold and concurrent figures, so its effect on the ranged plan is
   known rather than inferred from the request count.

## Rejected alternatives

**Make `byte-minimal` the loopback default again with decision 1.** Fewer
requests per object narrows the gap, but the concurrent loss ADR-2023
measured was mostly cache reuse, not request count: whole objects in a
corpus-sized fetch cache serve the concurrent phase nearly hot, while ranged
reads cache byte extents that differ by projection and keep returning to the
disk. Cutting 4 GETs to 3 does not change that.

**Choose the read shape per object by estimated device time** (a
`device-time` policy scoring `max(bytes / bandwidth, disk_ops / iops)` from a
measured store device profile). Considered and not adopted. The estimate is
per object and blind to cache reuse, which is what decided the concurrent
phase; for ClickBench's narrow statements it would pick ranged reads, the
shape that lost that phase. Its inputs are also properties of one disk: the
264 MB/s wall is the reference machine's gp2 volume serving RustFS, and on
remote S3, where requests are billed, whole-object reads are already right.

**Give `cost-based` a time term.** ADR-1196 rejected this for remote S3,
where time depends on the network and the instance, not on anything the
store profile describes. That reasoning stands.

**Change the object layout.** On the ClickBench corpus each object is already
one short row group with each column contiguous, so grouping columns inside
an object changes little there. Object size is the larger lever: 2,617
objects of about 4.3 MB, left uncompacted because the queries run before any
ingest hour seals. Studied separately, starting with a compacted-corpus
measurement (#2066).

## Consequences

- Ranged reads cost fewer requests under every policy that uses them,
  including explicit `byte-minimal` and `latency-first`.
- Bridging gaps reads some bytes the query does not need. The cap trades
  those bytes for requests, and the covering-read check bounds the trade at
  a whole-object read.
- ADR-0996's description of the front sections as one GET was stale; with
  decision 1 it becomes true.

# Diagrams (text)

## Current execution, one partition, fast path (code-observed)

```
poll_next ---> NextSegment ---> Opening ------------> Columnar/Rows ---> NextSegment ...
               pop_front        poll OpenFuture        next_block()        (segment N+1
               build future     (1 whole-object GET,   build batches        opens only
               (no I/O)         limiter permit,        emit BATCH_ROWS      after N is
                                cache/carry/GET)       synchronous          exhausted)
time --->      |<-- open stall (open_elapsed) -->|<-- decode_build + emit -->|
```

Per partition exactly one segment is in any state; P partitions run this
independently, so P opens can be in flight at once and the partition's
critical path is ceil(S/P) x (open + decode). Measured under a 20 ms
injected GET stall: 107 / 65 / 43 ms for P = 8 / 16 / 32 (LEDGER runs 5, 6).

## Planning path (block predicate present)

```
partition k: Planning --wait on OnceCell--> NextSegment --> Opening --> ...
partition 0 (or whichever polls first): compute_plan_counts
    buffer_unordered(P) over segments: prune each (whole-object read under
    u64::MAX), carry the first P completed objects' bytes; the scan
    re-fetches the other S - P (measured 32 of 40 at P = 8).
```

## Buffer lifetime (code-observed)

```
GET ----> Bytes (whole object)  or  AssemblyBuffer (object-sized, ranged path)
             |                              |
             v                              v  into_bytes() aliases, no copy
        LogSegmentScan { bytes } ------------ lives until the segment is exhausted
             |  next_block() decodes one block at a time from it
             v
        decoded block + its RecordBatches  <-- the only part reserved against
             |                                  the DataFusion pool (try_grow)
             v
        emitted batch (reservation moves with it), released next poll
```

The compressed object bytes are never reserved against any budget; the
`AssemblyBufferPool` bounds only buffers that have been returned (idle).

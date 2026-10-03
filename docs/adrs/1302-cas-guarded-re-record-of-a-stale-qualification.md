# ADR-1302: CAS-guarded re-record of a stale store-qualification record

Status: Accepted (2026-09-12). Supersedes ADR-0050 section 6's write-once
qualification record. Issue #1302.

## Context

ADR-0050 section 6 records a passing store qualification at `sys/qualification`
and states two things this decision revisits: the record is written "via
`CreateIfAbsent`", and "Qualification is once per bucket, not per boot". Both
read as a write-once record: created once by a deliberate `ravel-cli store
qualify` run and never changed.

The conformance suite has since grown. `CONFORMANCE_SUITE_VERSION` moved from 1
to 2 when the suite added the concurrent single-winner `CreateIfAbsent` probe,
the lexicographic listing order probe, the cross-page listing probe, and the
delete-visibility probe. A record written under version 1 was never checked
against any of those, so `ravel-server` startup refuses it as below the
running binary's floor (`services/ravel-server/src/qualification.rs`).

A write-once record makes that floor unreachable on an already-qualified
bucket. `CreateIfAbsent` fails against the existing record, so a re-run cannot
replace it, and every bucket qualified before the bump is stranded: the server
refuses to start, and the one command that could clear the refusal cannot
write. The record must therefore be re-recordable when it predates the current
suite version.

Re-recording opens a hazard the original write-once rule did not have. The only
failure mode that matters here is a stale record that still reads as a current
pass: a missing record fails closed (the server refuses to start), but a stale
record that satisfies the floor fails open, asserting a guarantee nothing
checked. Between the failed `CreateIfAbsent` and the replacing write there is a
read of the existing record and a decision based on its version. A concurrent
`qualify` from a newer binary can write a higher-version record inside that
window. An unconditional overwrite would then replace that higher-version
record with this run's lower version, silently reinstalling exactly the
stale-but-passing condition the whole re-record path exists to clear. It is
reachable only with mixed-binary operators running `qualify` concurrently on one
bucket, but it is reachable through this decision's own new code path.

## Decision

1. **`CONFORMANCE_SUITE_VERSION` is 2, and server startup refuses a record
   below that floor.** This records the bump that motivates the rest of the
   decision.

2. **Qualification is once per bucket at a given suite version, not
   write-once.** `ravel-cli store qualify` still attempts the first write via
   `CreateIfAbsent`. On `AlreadyExists` it reads the stored record and decides
   by suite version: an equal-or-newer record is left untouched and reported (a
   repeated run at the current version stays a no-op), and only a record written
   under an older suite version is replaced with the current pass.

3. **The replacing write is guarded by `CasVersion` on the version the run just
   read, not an unconditional `Overwrite`.** This closes the downgrade window in
   the Context. If a concurrent newer binary changed the record between the read
   and the write, the guarded write fails with `PreconditionFailed`; the run
   re-reads and re-decides against the now-current record. An equal-or-newer
   record found on the re-read is left untouched, so the losing run of a
   downgrade race succeeds as a no-op rather than reinstalling its older
   version. A record still older is overwritten again, bounded by a small retry
   count; exhausting the bound fails with a message naming the concurrent
   contention rather than retrying forever, which would be an operator-visible
   hang.

4. **`ravel-cli store qualify` remains the only production writer of
   `sys/qualification`.** `ravel-server` startup reads the record and never
   writes it, so the read-record-then-write race is confined to concurrent
   `qualify` invocations, which item 3 handles.

## Consequences

- A `CONFORMANCE_SUITE_VERSION` bump no longer strands already-qualified
  buckets: a re-run upgrades the stored record in place.
- Mixed-binary operators running `qualify` concurrently on one bucket cannot
  downgrade a higher-version record to a lower one; the higher version always
  survives.
- `sys/qualification` is no longer an immutable object. Documents that
  described it as write-once are corrected in the same change:
  docs/object-store-contract.md and docs/catalog-and-mvcc.md.
- The record's JSON shape and key are unchanged, so this is not a format
  change and needs no format version bump; an already-written record still
  decodes byte for byte.

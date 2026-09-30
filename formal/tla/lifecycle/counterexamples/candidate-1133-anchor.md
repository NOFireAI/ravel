# Issue #1133 implementation: STOP, no anchor field exists

This is not a TLA counterexample; the model side of #1133 is already
recorded in `candidate-1133.md` ("CONFIRMED unsafe": the horizon plus
unnamed-HEAD gate is not sufficient without the pinned-query clause). This
file records why the Rust-side fix requested by the issue could not be
implemented as specified, per the task's step-0 premise check.

## What the issue asks for

Add an age term to `bucket_gate`/`object_gate`
(`crates/ravel-maintain/src/reachability.rs`), anchored on "the covering
snapshot part that `covering_parts` already fetches, whose
`SnapshotPartHeader.created_unix_ns` (proto/ravel/catalog.proto, field 8,
today commented 'informational') every fold path stamps with `now`."

## What the code actually has

`SnapshotPartHeader` (proto/ravel/catalog.proto:8-24) has exactly eight
fields: `format_version`(1), `tenant_hash`(2), `signal`(3),
`shard_count`(4), `watermark_hour`(5), `entry_count`(6),
`entries_uncompressed_len`(7), `min_hour`(8). **There is no
`created_unix_ns` field on this message, at field 8 or any other number.**

Field 8 with the comment `// informational` is real, but it sits on a
different message: `SnapshotHead.created_unix_ns`
(proto/ravel/catalog.proto:111). That is the exact field the issue's own
stall argument says must NOT be the anchor, because every fold rewrites
`SnapshotHead` with a fresh timestamp, so a gate anchored there would
never open on a fold cadence shorter than the age term and both sweeps
would stall permanently.

Confirmed at every construction site of `SnapshotPartHeader`, all of
which build the struct as a full literal with no `..Default::default()`
(so the struct provably has no fields beyond the eight listed):

- `crates/ravel-catalog/src/snapshot_format/part.rs:76-85`
  (`encode_part_ranged`), the only production writer.
- `crates/ravel-catalog/src/cache.rs:1852-1861` (test fixture).
- `crates/ravel-catalog/tests/snapshot_format_corrupt.rs:57-58`.

`SnapshotEntry.created_unix_ns` (proto/ravel/catalog.proto:26-42, field
14) does exist and is stamped from the source record's own
`created_unix_ns` (e.g. `crates/ravel-catalog/src/catalog.rs:4285`,
`:4360`, `:4941`), but it is an entry-level (per-object) timestamp, not
the part-level header field the issue names, and using it instead would
be a different anchor design than the one #1133's TLA-backed writeup
specifies -- and would still leave the uncovered-hour case (step 0(c))
open the same way, since an entry only exists for objects a part already
covers.

## Why this is a stop, not a workaround

Adding `created_unix_ns` to `SnapshotPartHeader` is an additive change to
a frozen persistent format (docs/segment-format.md's contract rule:
proto schemas require an ADR and a version bump, never an in-place
edit). It also requires:

- editing `proto/ravel/catalog.proto`, which this task's scope
  explicitly excludes ("Leave proto/ untouched ... report it rather than
  editing it"), and
- a write-path change in `crates/ravel-catalog` (`encode_part_ranged` and
  every fold call site that builds a `SnapshotPartHeader`), which this
  task's scope marks read-only and calls out by name as a stop condition
  ("crates/ravel-catalog is read-only for you: if the fix needs a change
  there, that is a STOP condition").

Implementing the gate without a real anchor would mean either a compile
failure (the field genuinely is not there) or silently substituting a
different anchor (`SnapshotHead`'s, which the issue itself rules out as
stall-prone, or `SnapshotEntry`'s, an unauthorized design change that
does not cover step 0(c)) -- exactly the "wrong fix here deletes live
data" outcome the step-0 gate exists to prevent.

## Disposition

STOP: (a). No code or gate under `crates/ravel-maintain` or
`services/ravel-server` was changed. The correctness gap #1133 describes
in `LifecycleGC.tla` / `candidate-1133.cfg` is real and still open; a
follow-up needs an ADR to add a part-level creation timestamp to
`SnapshotPartHeader` (or an equivalent already-durable anchor this
analysis did not find) before the Rust gate in
`crates/ravel-maintain/src/reachability.rs` can implement
`HorizonGuardsPinnedQueries` faithfully.

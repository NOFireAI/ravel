# ADR-0032: RLOG compaction, and a signal-generic ravel-maintain

Status: Accepted

Builds on ADR-0018 (L0/L1 compaction), ADR-0019 (age-based retention),
ADR-0027 (single-version pre-release), and ADR-0029 (RLOG v1 log segment).
This ADR adds the piece the log-storage design left unresolved: RLOG's
footer carries no compaction identity today, so it cannot be compacted
as specified without a format amendment.

## Context

Metrics (RSEG) compaction is largely built: ADR-0018's phases P1-P5 are
shipped (protos, v4/v5 writer and reader, the `ravel-maintain` compactor,
and catalog resolver integration). What remains open against RSEG alone is
the P6 sweeper, P7 retention physical delete, and P8 maintain-mode/CLI.

Logs (RLOG) have none of this. RLOG v1 (ADR-0029) shipped format-only,
and ingest (`ravel-ingest` log shard actors) merged today, writing one L0
`.rlog` object per flush at the same cadence metrics used pre-compaction
(target 8 MiB, max 500ms flush delay). The log-storage design spec
already named the consequence: "L0 flush objects are small and
per-writer; without compaction, listing and query fan-out degrade with
uptime" - the exact problem ADR-0018 solved for metrics, now accruing
for logs from the moment ingest went live.

Two gaps block simply reusing ADR-0018's machinery for logs:

**1. `ravel-maintain` is RSEG-specific, not signal-generic.** Its
`Bucket` type already carries a `Signal` field and `scan.rs` (seal
detection), `publish.rs` (CreateIfAbsent + convergence), and `config.rs`
are written generically over signal. But `build.rs` and `read.rs` import
`ravel_segment::{SegmentWriter, Footer, SeriesEntryV4}` directly - there
is no seam for a second codec. Every test and code path exercises
`Signal::Metrics` only.

**2. `LogFooter` (proto/ravel/logseg.proto) has no compaction identity.**
RSEG's footer carries `level`, `part_index`, and `input_set_hash`
specifically so a reader can tell an L1 part from an L0 object and
recover its input provenance (docs/segment-format.md:72-76).
`LogFooter` has none of these fields - only per-writer identity
(`tenant_hash`, `shard`, `writer_id`, `writer_epoch`, `writer_seq`) and
whole-object summary stats. Writing an L1 `.rlog` part today would be
indistinguishable from an L0 object to any reader. This is a
frozen-format change and goes through the format-change procedure, not
an in-place edit.

RSEG's footer also carries a fourth compaction field,
`base_created_unix_ns` - the minimum per-run `created_unix_ns` over all
inputs, which the writer uses as the base for delta-encoding each run's
original creation time. That field exists only because RSEG's
correctness core needs to recover each run's exact creation time after
compaction merges runs from different objects together, to preserve the
`(created_unix_ns, writer_epoch, writer_seq, in_page_index)` cross-segment
dedup tiebreak (ADR-0018's correctness core). RLOG has no analogous need:
per the design spec, "the write path makes retry duplicates structurally
impossible; distinct submissions of identical content are distinct
records" - there is no cross-writer record-level dedup for compaction to
preserve, so nothing in a merged RLOG object would ever read a
recovered per-run creation time. RLOG's compaction identity needs only
`level`, `input_set_hash`, and `part_index`.

The catalog/commit layer needs no such change. `ravel-commit`'s
`compaction_record_key` / tombstone key builders already take a `Signal`
and are tested with `Signal::Logs` (crates/ravel-commit/src/keys.rs:1334,
1494); `docs/catalog-and-mvcc.md`'s key layout already reserves the `l`
compaction and tombstone key shapes. The MVCC publish-then-exclude
protocol (ADR-0018) and the resolver's `SegmentLevel`/`SegmentRef`
exclusion (`ravel-catalog/src/{catalog,cache}.rs`, landed with P5) are
signal-generic already. The gap is entirely in the codec layer
(`ravel-maintain` + `ravel-logseg`), not the transaction layer.

Building this now, ahead of RLOG query (design spec phase 3, not yet
started), is safe: compaction operates purely on committed L0/L1 catalog
state and the resolver already excludes superseded inputs generically.
Nothing about a future `logs` SQL table needs to exist first, and
waiting for it would let the small-object problem compound in any
environment ingesting logs before query ships.

## Decision

**Add compaction identity to `LogFooter`, additively, and bump the RLOG
trailer version to 2.** New fields (next available numbers, 14-16):
`level` (uint32, 0 = L0 flush object, 1 = L1 compacted part),
`input_set_hash` (bytes, over the sorted input list, same canonical
convention as RSEG), `part_index` (uint32, part ordinal within one
compaction output). All RLOG writers - both the L0 flush path in
`ravel-ingest` and the new L1 compactor path - move to trailer version 2
in the same change; `level`/`input_set_hash`/`part_index` default to
`0`/empty/`0` on L0 objects. The version bump is not a judgment call:
`docs/log-segment-format.md:3` states the rule directly - "Persistent
contract (ADR-0029). Any change bumps the trailer version" - with no
carve-out for additive fields, so this format doc's own versioning rule
is stricter than the proto comment's "additive changes, readers skip
unknown fields" language might suggest on its own; the trailer version
gates on exact equality in the reader (`ravel-logseg/src/footer.rs:188`,
`if version != VERSION`), so a v1 object is rejected outright once the
reader moves to v2, which is exactly what makes the next paragraph's
no-dual-reader decision a real decision and not a formality.
`docs/log-segment-format.md` and `proto/ravel/logseg.proto` are amended
in the same commit as the reader that consumes the new fields, per the
format-change procedure.

**No dual-reader path for RLOG v1.** RLOG merged into `main` today; no
deployment outside development holds RLOG objects, and no release has
shipped that depends on them. This is the same fact pattern ADR-0027
already decided for RSEG, and it gets the same answer: one supported
version, older RLOG v1 test fixtures regenerated, dev/test stores wiped
as needed. This is a one-time decision at RLOG's actual pre-release
state, not a standing policy to skip the dual-reader question on future
changes once real data exists.

**Generalize `ravel-maintain` behind a per-signal codec seam, not a
second crate.** Extract a trait (name TBD at implementation time, e.g.
`SegmentCodec`) covering what `build.rs`/`read.rs` need per signal:
decode an input object's footer/identity, stream-merge N inputs into
size-capped output parts, encode an output object. `scan.rs`,
`publish.rs`, `config.rs`, and the crash-recovery/convergence logic stay
untouched and fully shared. Implement the trait twice: a thin wrapper
around the existing RSEG logic (behavior-preserving refactor, gated by
the existing RSEG differential/crash-matrix test suite staying green
unchanged), and a new RLOG implementation doing what the design spec
already specifies - linear merge of sorted `STREAM_DIR`s with a global
`stream_ref` remap, re-sort by `(stream_ref, ts)`, rebuilt `FIELD_DIR`,
rebuilt `SKIP_IDX` and `BLOOM` over the merged blocks, same 8192-record
block target.

**Build the sweeper and retention physical delete
signal-generically, now, instead of RSEG-only.** Both operate one layer
below the codec: given a durable record (`CompactionRecord`'s superseded
input list, or a `RetentionTombstone`'s retired set) and a horizon past
which no in-flight reader can still need the old objects, delete the
named keys. Neither needs to know RSEG or RLOG bytes - only object keys
and the record that authorizes deleting them. Writing this once against
the key/record layer serves both signals immediately; writing it
RSEG-only now and generalizing later would mean re-touching the same
crash-matrix-sensitive deletion path twice.

**Maintain-mode/CLI covers both signals from the start** -
compaction and retention run per (tenant, signal, shard), so the ops
surface (cadence config, metrics, logging) is signal-generic by
construction once the codec seam exists.

## Rejected alternatives

- **A separate `ravel-maintain-logs` crate.** Rejected: `scan.rs` /
  `publish.rs` / `config.rs` are already correct and generic; forking
  them would duplicate the 13-row crash matrix (ADR-0018 §3.6) across
  two crates that must stay in lockstep, doubling the maintenance
  surface for a seam that a trait already covers cleanly.
- **Keep `ravel-maintain` RSEG-only and give RLOG its own from-scratch
  compactor.** Rejected for the same reason: the transaction-layer
  machinery (seal detection, `CreateIfAbsent` convergence, abandonment,
  advisory cursor CAS) is signal-agnostic today and already tested; a
  second implementation would drift from the first the next time either
  gets a crash-path fix.
- **Reuse RLOG v1's existing field numbers for compaction identity
  instead of adding new ones.** Rejected: violates the additive-only
  proto rule (frozen contracts, format-change procedure) and would make a
  v1 footer byte-ambiguous with a v2 footer that
  happens to zero those fields.
- **Keep a v1 RLOG reader path alongside v2 "to be safe."** Rejected on
  the facts: RLOG has been in `main` for one day, nothing outside
  development has ever read one, and ADR-0027 already established that
  Ravel pre-release does not carry compatibility surface for data nobody
  holds. A dual-reader path here would be maintaining compatibility with
  the project's own git history, not with any deployment.
- **RSEG-only sweeper/retention now, generalize when RLOG needs it.**
  Rejected: the deletion path is the most crash-sensitive part of this
  whole area (ADR-0018's crash matrix rows 5-9, 12 are explicitly
  sweeper/retention races, still only partially covered). Writing it
  against the generic key/record layer costs nothing
  extra today and avoids a second pass through that crash matrix later.
- **Ship RLOG compaction only after the `logs` query table (design spec
  phase 3) lands.** Rejected: query and compaction are independent
  consumers of the same committed catalog state; the resolver's
  supersession logic is already signal-generic from RSEG's P5, so
  nothing about compaction depends on a `logs` table existing. Waiting
  serializes two independent phases for no correctness reason, while the
  small-object problem the design spec names keeps compounding under
  live log ingest.
- **Native object-store lifecycle rules for deleting superseded/retired
  RLOG objects.** Rejected for the same reason ADR-0019 rejected it for
  RSEG retention: store-native TTL cannot distinguish "superseded but
  maybe still resolvable by an in-flight reader" from "safe to delete,"
  so deletion must stay a durable-record-then-horizon-gated-sweep, never
  bucket-native expiry.

## Consequences

- RLOG gains a version 2 trailer before any release depends on version
  1; `ravel-ingest`'s log writer path picks up the same one-line bump
  RSEG's L0 writer would need, populating the new fields at their L0
  defaults.
- `ravel-maintain` gains a codec seam that P6/P7/P8 are re-scoped to build
  against generically rather than RSEG-only.
- The RLOG differential/crash-matrix test suites gain the RSEG side's
  shape: a compacted-vs-uncompacted keystone differential test, and
  crash-matrix coverage for the merge/publish/sweep paths specific to
  RLOG's merge (STREAM_DIR remap correctness under partial-input
  crashes).
- `docs/log-segment-format.md` and `proto/ravel/logseg.proto` are
  amended in the same commits as the code that implements each change,
  per the doc-currency rule.

## Amendment 2026-08-26: a part never exceeds the size cap; a stream may span parts

Status: accepted. Supersedes the one-stream-per-part rule that the RLOG
merge implemented under "stream-merge N inputs into size-capped output
parts" above. Issue #711.

### What changed

The original RLOG compactor split parts on stream boundaries only: it
merged one whole stream into the in-progress part, then compared the
part's accumulated record-byte estimate against `max_l1_part_bytes` and
opened a new part if the cap was reached. The invariant that produced -
"a stream never straddles two parts", the log analogue of RSEG's
series-boundary split - is replaced by a pure size bound:

> A part never exceeds `max_l1_part_bytes` of estimated live record heap.
> A stream may span consecutive parts.

The cap is now checked after every merged record, so a part closes
wherever in the merged record sequence the cap falls.

### Why

An RLOG stream is one `(resource, scope)` pair. A tenant sending one
OTLP resource and scope - a single service, the ordinary shape for logs -
puts its entire hour into one stream, so a stream-boundary split rule
gives that bucket no split point at all. Measured on one (shard, hour)
bucket of a 16-shard ClickBench logs tenant (about 270 RLOG inputs,
350 MB compressed, 3M rows of 105 attributes, one stream):
`ravel-cli maintain compact-bucket --signal logs` ran over 12 minutes at
45.7 GB resident on one core. The k-way block-streaming merge of
ADR-0065 decision 4 had already bounded the read side to one decoded
block per input; what was left unbounded was the writer, which held every
merged record of the stream as a row-major `LogRecord`.

The size estimate was the second half of the defect. It counted payload
bytes (body, severity text, attribute keys and values) while the writer
holds `LogRecord`s, whose Rust representation for a wide row is an order
of magnitude larger: per-row `String` and `Vec` headers and allocations,
a per-row copy of the resource/scope blob, and one `(String, AttrValue)`
slot per attribute, which for 105 attributes is 12-16 KB of heap per row
against roughly 1 KB of payload. A nominal 256 MiB cap therefore
permitted several GB of live records even where a split point existed.
`estimate_record` now charges the record's Rust-side heap, term by term
(the formula is stated above the function in `crates/ravel-maintain/src/rlog.rs`),
so 256 MiB of estimate is roughly 256 MiB of heap.

Both halves are needed. Moving the check alone would still let a part
grow past the cap by the estimate's error factor; fixing the estimate
alone would still leave a single-stream bucket with nowhere to split.

### What readers must tolerate

The bytes of a part are unchanged: each part is still written by the
frozen `ravel-logseg` writer, from records still pushed in global
`(stream_id, ts)` order, so every part is individually sorted and
self-describing exactly as before. Only the partitioning of records into
parts changed. Concretely:

- **Two consecutive parts of one compaction may carry the same
  `stream_id`.** `CompactionPart.first_series_id` and `last_series_id`
  are then *adjacent* rather than strictly disjoint: part `k`'s `last`
  may equal part `k+1`'s `first`. Their event-time ranges remain
  non-overlapping and ascending, because records enter parts in ts order
  within a stream.
- **Nothing may prune on those bounds assuming disjointness.** Today
  nothing does: `ravel_catalog`'s `build_l1_segment_ref` turns every part
  into its own `SegmentRef` and the resolver unions them, filtering on
  event time only. A future pruning optimization over the series bounds
  must treat them as a closed interval that a neighbour may share, not as
  a partition.
- **Record conservation is unaffected.** The compaction gate
  (`conserve_exact`) and ADR-0064's erasure gate both sum
  `part.sample_count`; splitting repartitions records and never adds or
  drops one.
- **`sum(part.series_count)` is no longer a distinct-stream count.** A
  stream that straddles a boundary is counted once per part it appears
  in. That aggregate is reported (`maintain status`, `CompactionRecord`)
  and never used as a gate, so this is a reporting change, not a
  correctness one. A caller that needs the true distinct-stream count of
  a compaction must union the parts' `STREAM_DIR`s.
- **Part counts rise, and parts get smaller on disk than the cap
  suggests.** The cap now bounds estimated heap, not encoded bytes, so a
  wide-row bucket produces more, individually smaller, L1 objects than
  the same nominal cap produced before. That is the intended trade:
  bounded compactor memory in exchange for more objects. Operators who
  want fewer, larger parts raise `max_l1_part_bytes` deliberately, with
  the memory cost now visible in the same number.

### Input read concurrency

The same issue covers the compaction read path, which was a chain of
sequential awaits: one commit-record GET per input, then one catalog load
per input, then one block GET per cursor advance. A new
`CompactorConfig::input_read_concurrency` (default 8) bounds how many of
those are in flight, and each stream cursor prefetches one block ahead.
This is a latency change only. Output bytes cannot depend on it: inputs
are re-sorted into canonical `(writer_id, writer_epoch, writer_seq)`
order after loading, catalogs stay aligned to that order, and the merge
is a deterministic k-way merge over it. A test asserts the parts are
byte-identical at concurrency 1 and 8.

### The erasure rewriter shares this merge (amended 2026-08-26)

As first written, this amendment left `build_rewrite_logs` (the ADR-0064
erasure rewriter for logs) out of scope: it fetched every input object
whole and pushed every survivor into a single `RlogWriter` with no size
cap, the same unbounded-writer shape this amendment removed from the
compactor, reached on the same tenant shape.

That follow-up landed. `build_rewrite_logs` now runs the same k-way
block-streaming merge, with the erasure predicate as a per-record filter
and `input_read_concurrency` bounding its catalog loads, so it inherits
every bound described above: one decoded block per input plus one
in-progress part, never the bucket. A logs erasure rewrite therefore
emits N parts, exactly as a compaction does, and for the same reason.
Nothing downstream assumed one part: ADR-0064's conservation gate is
already stated over `sum(output sample_count)`, the parts are already
PUT under `part_index`-keyed content-addressed keys, and the catalog
already unions every part of a `RewriteRecord`.

The RSPAN erasure rewriter (`build_rewrite_spans`) still has the
whole-object, single-writer shape; it was not part of that follow-up.

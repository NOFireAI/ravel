# ADR-0054: RSPAN v3, block bloom filters and a service_name column

## Context

ADR-0045 decision 2 specified two things for "RSPAN v2": a per-block bloom
over `service.name` and span-name tokens, and a dictionary-encoded
`service_name` column (id 9) lifted out of the attrs map. Neither shipped.
Trailer version 2 went live carrying only the other half of decision 2
(`min/max_duration_ns`, `status_mask` in `BlockEntry`); `docs/span-segment-
format.md` states plainly that RSPAN "has no BLOOM section", and
`service.name` still lives inside the opaque `attrs` blob, found only by a
linear scan at query time (`crates/ravel-sql/src/spans_scan.rs`).

Found while implementing #431 (the inspector could not print a bloom's
offset/length/crc32c because none exists) and reconfirmed by #550: the
normative doc and the ADR disagree about whether v2 ever had a bloom, and
neither the bloom nor the column is reachable from any query path today.
`ravel-sql/src/spans_pushdown.rs` already extracts a `service_name = <lit>`
predicate from SQL; it has nothing to prune with.

Issue #550 asks for a decision: drop the bloom/column from the spec, or
build them. This ADR builds them.

Trailer version 2 is live: real objects exist with `min/max_duration_ns`
and `status_mask` in `SKIP_IDX` but no bloom and no `service_name` column.
Per the frozen-format rule, that field set cannot change in place under
version 2. This requires a version bump.

ADR-0045 decision 3 already reserved "RSPAN v3" for a different, unstarted
body of work: attribute columns and span events (issue #434, blocked on
this issue). Claiming v3 for the bloom and the column here collides with
that reservation. Decision 2 below resolves the collision by renumbering
decision 3's work to v4; issue #434 is retitled accordingly in the same
change as this ADR merges. No code exists for decision 3 yet (verified: no
branch, no stub), so the renumbering has no code cost, only a documentation
one.

## Decision

### 1. RSPAN v3: block bloom filters and a service_name column

Trailer version 3, retiring version 2 (ADR-0045 decision 4: single
supported version, no dual reader; Ravel is pre-release and no RSPAN v1
data exists outside development buckets, so the same holds for v2 by the
time v3 ships).

**Bloom storage.** ADR-0045 decision 2's literal wording ("offset, length,
crc32c in the skip-index entry") is not what RLOG actually does, and RLOG
is the thing decision 2 says to copy "exactly." RLOG stores its bloom as
its own mandatory top-level section (`kind::BLOOM = 5` in
`ravel-logseg`'s footer, positionally addressed: entry *i* covers block
*i*, no offset/length/crc32c duplicated per block in `SKIP_IDX`). RSPAN v3
follows RLOG's actual shape, not the ADR's paraphrase of it: a new
mandatory section, `kind::BLOOM` in `ravel-rspan`'s own kind space (next
free value, 3), built and read through `ravel-codec`'s existing
`bloom_section.rs` with zero new bloom logic. `BlockEntry` gains no new
fields for this. The rejected alternative and why it lost is in the
Rejected section below.

Field scope: two logical fields under this one section, `service.name`
and span `name`, each its own field id in the same blocked-bloom scheme
RLOG already uses (512-bit blocks, k=7, `m_bits =
next_pow2(max(512, ceil(n * 9.585)))`, three 64-bit hashes from disjoint
BLAKE3 digest ranges keyed by `seed || field_id || token`). Token rule:
same as RLOG's tokenizer (lowercase, split on non-alphanumeric, 64-byte
truncation), reused verbatim so a service name or span name query can hit
either signal's bloom through one code path.

**`service_name` column.** Column id 9, dictionary-encoded, block-local
(distinct values written once per block, rows store a varint index into
that block's dictionary — RSPAN has no FIELD_DIR/STREAM_DIR section for a
segment-wide dictionary the way RLOG's dynamic columns do, and a
block-local dictionary needs no new section to introduce it). The writer
lifts `service.name` out of the `attrs` map into this column at write time
and does not duplicate it in the blob; `merge_attrs`
(`crates/ravel-rspan/src/record.rs:108`) is unchanged, it still produces
the map, the column extraction happens after.

**Pruning wiring.** `SpansPushdown`'s existing `service_name` extraction
(`ravel-sql/src/spans_pushdown.rs`) currently has nothing to prune with;
`spans_scan.rs` currently derives `service_name` from `attrs["service.name"]`
by linear scan at read time regardless. v3 gives `SkipIndex::candidate_blocks`
a bloom-backed prune for `service_name = <literal>` and `name = <literal>`
predicates (membership test only; a bloom is a false-positive-only filter,
per the widen-only pruning rule ADR-0013 already established, so a
negative test excludes a block and a positive test changes nothing — the
scan still verifies). `spans_scan.rs` reads the column directly once it
exists, no more attrs lookup for `service_name`.

### 2. Renumber ADR-0045 decision 3 to v4

Amend ADR-0045 in place: decision 3's heading becomes "RSPAN v4: attribute
columns and span events", its body unchanged except every "version 3" /
"v3" becomes "version 4" / "v4". Decision 4's "v3 retires v2" becomes "v3
retires v2 (ADR-0054); v4 retires v3 (this decision)". Issue #434's title
and body get the same v3→v4 substitution as part of this ADR's landing
(doc-only edit, no code exists yet to move). #435 depends on #434 and only
mentions "v3" in the sense of "whatever the next version prints" — its
body needs the same substitution for clarity, though it carries no version
number of its own.

## Rejected alternatives

**Embed bloom offset/length/crc32c per block in `SKIP_IDX`'s
`BlockEntry`, as ADR-0045 decision 2 literally described.** Rejected:
it duplicates a shape RLOG doesn't use despite the ADR's own stated intent
to match RLOG exactly, it grows every `BlockEntry` for a section some
readers never touch (a query with no service/name predicate never opens
the bloom at all), and positional addressing by block index already gives
O(1) lookup with no extra indirection to store or checksum separately —
RLOG proves the pattern works at RLOG's scale, which is larger than
RSPAN's.

**Amend ADR-0045 to drop the bloom and the column instead of building
them.** This is the alternative issue #550 offered. Rejected per explicit
user decision: both are wanted, and `service.name` equality is the entry
point of most trace investigation queries (service dependency workflows,
per decision 2's own justification), so leaving it an unpruned linear
attrs scan forever is the wrong tradeoff.

**Keep decision 3's work at "v3" and give this work v4 instead.** Rejected
because this work is ready to ship now, decision 3's work has not started
and has no branch or stub, and issue #434 already names #550 as its
blocker — building #550 as v4 while nothing v3-shaped exists yet would
leave v3 permanently unused. Renumbering the not-yet-started work is
strictly cheaper.

**Give the bloom a per-segment (not per-block) granularity.** Rejected:
per-block matches `SKIP_IDX`'s existing block-level pruning granularity
and RLOG's precedent; a per-segment bloom would only prune whole objects,
which the catalog's existing coarse filtering already does at a different
layer, and would waste the finer pruning `candidate_blocks` already does
for duration/status.

## Consequences

- Trailer version bump: `crates/ravel-rspan/src/footer.rs`'s `VERSION`
  constant moves from 2 to 3; the doc comment above it gets the same
  treatment `rlog.rs`'s `OUTPUT_FORMAT_VERSION` got after issue #482 (cite
  the live constant, `footer::VERSION`, in every test assertion — never a
  mirrored literal).
- `docs/span-segment-format.md` amended in the same change: new BLOOM
  section, its mandatory status, the block-local `service_name` dictionary
  encoding, corrected "no BLOOM section" language, updated trailer version
  table.
- Checksum coverage: the new BLOOM section gets its own `Section.crc32c`
  like every other RSPAN section; per-block-within-the-bloom, RLOG's
  `bloom_section.rs` framing already carries a per-entry crc32c, reused
  unchanged.
- Fuzz/property tests extended for v3 round-trip and corrupt/truncated
  BLOOM section inputs (missing BLOOM is a mandatory-section violation,
  same `Corrupted` typed error path as a missing `SKIP_IDX` today).
- `ravel-cli rspan inspect` prints the new column and the bloom's presence
  (not its bits — same as RLOG's inspector, which reports bloom stats, not
  contents).
- Compaction (`ravel-maintain::rspan_codec`) rebuilds blooms from merged
  content on every compact, same as RLOG's compaction already does; no new
  merge logic, the bloom is a derived artifact, not merged input.
- ADR-0045 gets its decision-3 renumbering; issue #434's title and body
  get the matching edit, in the same change that lands this ADR.
- No dual reader: any RSPAN v2 object outside development buckets must be
  compacted (which rewrites to the current version) before v3 ships if it
  is meant to remain queryable; per decision 4's own terms this is
  Ravel's accepted pre-release posture, not a new exception carved out
  here.

Refs: #550

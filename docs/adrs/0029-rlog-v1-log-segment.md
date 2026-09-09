# ADR-0029: RLOG v1: columnar log segment format, a sibling to RSEG

Status: Accepted

Decides the log storage format. This ADR records the decision;
`docs/log-segment-format.md` is the normative byte-level contract and
ships in the same change. This is a new persistent format, added under
the format-change procedure (ADR-0010 §4): a new ADR, an explicit
version, a checksum-coverage review, and fuzz/property/corrupt-input
suites.

## Context

Ravel stores metrics only. Logs are the second first-class signal
(`l` signal letter, already reserved in docs/catalog-and-mvcc.md). Log
records are not series-of-float samples: a record carries a
variable-length text body, a per-record attribute set whose keys differ
across a tenant, and search predicates dominated by word and phrase
match. The dominant read is "does this block contain this word in this
column, in this time range, for these streams?", answered by proof-based
pruning under the ADR-0013 invariant (a structure may prune only what it
proves absent).

RSEG (docs/segment-format.md) is a frozen contract whose grammar is
series-of-float-pages: a label dictionary, a per-series catalog, and
timestamp/value page pairs keyed by a 128-bit series id. Nothing in that
grammar carries variable-length text columns, per-object dynamic field
sets, or in-object text indexes.

## Alternatives

1. **Amend RSEG.** Add text columns, a dynamic field directory, and
   token blooms as new section kinds and page encodings under a new
   trailer version. Rejected: the additions exceed the base grammar they
   amend (they are not a superset of series-of-float-pages the way v2-v5
   each were of their predecessor), and every RSEG reader and the whole
   metrics test surface would churn to carry a grammar the metrics path
   never uses. An amendment buys reuse of the trailer and footer shape,
   which a sibling format copies at near-zero cost anyway.

2. **A general-purpose columnar container (Parquet or Arrow IPC) plus
   sidecar index objects.** Rejected: an off-the-shelf container cannot
   host tokenized per-block blooms inside the object, so the text indexes
   become sidecar objects. That splits one flush across two objects and
   breaks the single-object atomicity Ravel's commit protocol depends on
   (a commit record resolves exactly one data object; ADR-0002,
   ADR-0010 §7). Ravel's discipline is one immutable, self-describing
   object with its own footer; RLOG keeps it.

3. **A new sibling format in a new crate (chosen).** RLOG v1 in
   `ravel-logseg`: the RSEG conventions (16-byte trailer, protobuf
   footer, crc32c checksum discipline, suffix-GET reader protocol,
   untrusted-input parsing) with a log-shaped body (stream directory,
   dynamic typed field directory, columnar row blocks, multi-level skip
   index, per-block token blooms). Shares conventions, shares no bytes.

## Decision

RLOG v1 exists as specified in `docs/log-segment-format.md`:

- **Trailer.** 16 bytes, same shape as RSEG: `footer_len` u32,
  `footer_crc32c` u32, `version` u16 (`= 1`), `signal` u8 (`= 2`, logs),
  `reserved` u8 (`= 0`), `magic` `[u8;4] = "RLG1"`. `magic` names the
  format family; the reader dispatches on `signal` and `version`.
- **Footer.** `LogFooter` protobuf (`proto/ravel/logseg.proto`, package
  `ravel.logseg.v1`): identity, summary (skip-index level 2), and a
  repeated `Section` table. Field numbers are frozen; only additive
  changes with new field numbers are permitted.
- **Five mandatory sections**, kinds 1..5: STREAM_DIR, FIELD_DIR, BLOCKS,
  SKIP_IDX, BLOOM. Unknown section kinds MUST be skipped (forward
  compatibility). STREAM_DIR, FIELD_DIR, SKIP_IDX are whole-section zstd;
  BLOCKS and BLOOM are containers whose entries are individually
  addressable so one block is readable alone.
- **Records sort by `(stream_ref ascending, ts_ns ascending)`.**
  Clustering by stream makes the stream column near-free, timestamps
  near-monotonic within a run, and per-stream dictionaries tight.
- **Per-page tagged encodings.** A nine-entry encoding registry; the
  writer measures and picks the smallest per page; the tag makes the
  choice self-describing. Unknown tags are a typed decode error, never a
  guess.
- **Multi-level skip index** (levels 0 and 1 in SKIP_IDX, level 2 in the
  footer), fanout 64, and **per-block blocked token blooms** over
  field-scoped word tokens. Both prune only by proof: a bloom negative is
  proof of absence (skip the block); a positive proves nothing (scan and
  re-evaluate exactly). A missing or corrupt index section degrades to
  scanning, never to wrong results; corrupt BLOCKS data is a loud
  `Corrupted` error.
- **Constants (frozen in the format doc).** magic `"RLG1"`, version `1`,
  signal `2`, block target 8192 records, block cap 8 MiB uncompressed,
  max 1000 dynamic columns, zstd level 3 default, page compression floor
  512 bytes, bloom target FPR 1%, token max 64 bytes, skip-index fanout
  64.

### Stream identity

A log stream is the 128-bit BLAKE3 truncation of the canonical encoding
of the OTLP resource attributes plus scope name, version, and
attributes. Per-record attributes never enter identity; this bounds
stream cardinality to roughly one per service instance, which is what
makes sorting by stream tractable. Canonicalization follows the ADR-0005
discipline (attributes sorted, values encoded with type tags, no lossy
stringification, nested values encoded recursively) under its own version
domain string `ravel-logstream-v1`. It lives in `ravel-types` beside
series identity.

### Checksum coverage

Every byte a reader interprets is covered by a checksum verifiable on
that reader's access path (ADR-0010 §4). RLOG refines the spec's
"section-crc" wording to keep this true for the two sections a reader
touches one entry at a time:

- `footer_crc32c` covers the footer protobuf bytes plus every trailer
  byte except the crc field itself, exactly as RSEG.
- STREAM_DIR, FIELD_DIR, SKIP_IDX are always read whole; each carries one
  `Section.crc32c` over its stored (compressed) bytes.
- BLOCKS is read one block at a time, so a whole-section crc cannot guard
  a single-block read. Each block's crc32c lives in that block's SKIP_IDX
  level-0 entry (`block_crc32c`) and covers the block's complete stored
  bytes; the reader verifies it before decoding the block.
- BLOOM is read one entry at a time, so each entry carries its own
  crc32c in the BLOOM container framing, covering that entry's stored
  bytes.

The format doc records this as a normative checksum-coverage map. This is
a deliberate refinement of the whole-section wording, forced by
ADR-0010 §4; doc and code carry the refined rule so they never diverge.

### Dual-reader question

No log data exists in any prior format: RLOG v1 is the first log format
Ravel has ever written. v1 therefore carries no dual-reader obligation at
introduction. The obligation begins the moment v1 objects exist: from the
first shipped writer, every future reader accepts v1 indefinitely, and
any future version bump keeps the v1 read path until retention clears the
last v1 object (docs/consistency-model.md), exactly as RSEG's version
dispatch does.

### Versioning rule

- The trailer `version` bumps for any change to the layout; readers
  dispatch on it and accept every version they were ever able to write.
- Stream identity bumps its domain string (`ravel-logstream-v2`, never an
  in-place edit to v1), the same rule series identity follows.
- The `LogFooter` proto only ever gains fields with new field numbers;
  no field is renumbered or reused.
- New encoding tags and new section kinds may be added without a version
  bump, because an unknown tag is already a typed decode error and an
  unknown section kind is already skipped; a change that alters the
  meaning of an existing tag, kind, or field is a version bump.

## Consequences

- Logs get a purpose-built format whose read cost scales with surviving
  data, not object size, and whose text search prunes by proof. The
  metrics path (RSEG) is untouched; no frozen metrics byte moves.
- A second format is a second maintenance surface: its own decoder fuzz
  targets, golden-bytes tests, and a keystone differential suite
  (pruned scan equals naive scan) gate it, mirroring RSEG's discipline.
- The format leaves room for later acceleration (new section kinds for
  trigram/substring indexes) without a version bump, because unknown
  kinds are skipped and pruning is always proof-based.
- Ingest (ravel-otlp, ravel-ingest), query (ravel-sql `logs` table), and
  lifecycle (ravel-maintain) are later phases against this frozen
  contract; this decision is format-only.

## References

- Normative format: `docs/log-segment-format.md`.
- Precedent: ADR-0004 (RSEG v1), ADR-0005 (series identity),
  ADR-0010 §4/§7 (checksum coverage, identity binding), ADR-0013
  (pruning soundness). docs/catalog-and-mvcc.md (`l` keyspace).

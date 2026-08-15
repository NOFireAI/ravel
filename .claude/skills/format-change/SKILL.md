---
name: format-change
description: Use before changing any persistent format - RSEG layout, proto schemas, series identity, commit tokens, object key layout - these are frozen contracts with a mandatory procedure
---

# Changing a persistent format

Persistent formats outlive processes and deployments. Readability across
versions follows one of two regimes, and the format's ADR must say which:

- Pre-first-release (ADR-0027, and ADR-0032/0045/0054 for RLOG/RSPAN): a
  single supported version. Old-format objects are wiped or re-ingested;
  no dual reader is kept. This is temporary and expires at first public
  release.
- Post-first-release (ADR-0066 decision 1): an N/N-1 window for the bulk
  data-object formats (Class A below). Writers always emit the current
  version N; readers accept N and N-1. Data written today is readable by
  the next version, not by every future version: N-1 support is deleted
  once every bucket's recorded format floor (ADR-0066 decision 3) is >= N,
  and that deletion is its own reviewed change citing the floors.

The frozen contracts:

- RSEG segment layout (docs/segment-format.md)
- Protobuf schemas under proto/ (field numbers are frozen; only additive
  changes with new field numbers are allowed)
- Canonical series identity and commit token encoding (crates/ravel-types;
  both carry version domain strings)
- Object key layout (docs/catalog-and-mvcc.md)

## Procedure

1. Write the ADR first: context, alternatives, decision, consequences.
   Get the format doc amended in the same change so doc and code never
   diverge. State the format's migration class (A-D, ADR-0066 decision 4)
   and its convergence plan: A = bulk data objects (RSEG/RLOG/RSPAN),
   converged by retention, rewrite-on-touch, and the `maintain migrate`
   job; B = derived catalog objects, rebuilt by the fold; C = immutable
   metadata records, additive-only; D = identity/domain-hash encodings,
   contained per bucket, never generically migrated.
2. Version explicitly. RSEG bumps the trailer version; identity encodings
   get a new domain string (ravel-series-v2, not an edit to v1); protos
   add fields, never renumber or reuse; key layouts add new prefixes.
   Version constants stay single-sourced in each format crate
   (`SUPPORTED_VERSIONS`, `VERSION_V6`, `footer::VERSION`); the writer,
   the reader gate, `audit-versions`, `migrate`, and the compactor's
   `OUTPUT_FORMAT_VERSION` all read them, so a bump edits one constant, not
   the sixteen hand-mirrored sites ADR-0049 measured.
3. Answer the dual-reader question in the ADR. For a Class A format
   post-release, this is ADR-0066 decision 1's readers-before-writers
   rule: land the N-1 reader (and roll the fleet onto it) before any
   release writes N+1; a version bump ships the reader window first. The
   reader keeps both paths until every bucket's format floor reaches N;
   deleting the N-1 reader is a separate reviewed change that cites those
   floors (ADR-0066 decision 3), never a wipe-and-hope.
4. Review checksum coverage. Every byte a reader interprets must be under
   a checksum the reader can afford to verify on its access path (ADR-0010
   section 4 is the precedent: a page crc that skipped the encoding byte
   allowed silent misdecoding).
5. Extend fuzz and property tests to cover both versions, plus corrupt
   and truncated inputs for the new one.
6. Update ravel-cli inspectors to print the new version's fields.

If a change cannot follow this procedure, it does not happen. There is no
fast path for format changes.

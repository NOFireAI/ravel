---
name: format-change
description: Use before changing any persistent format - RSEG layout, proto schemas, series identity, commit tokens, object key layout - these are frozen contracts with a mandatory procedure
---

# Changing a persistent format

Persistent formats outlive processes and deployments. Data written today
must be readable by every future version. The frozen contracts:

- RSEG segment layout (docs/segment-format.md)
- Protobuf schemas under proto/ (field numbers are frozen; only additive
  changes with new field numbers are allowed)
- Canonical series identity and commit token encoding (crates/ravel-types;
  both carry version domain strings)
- Object key layout (docs/catalog-and-mvcc.md)

## Procedure

1. Write the ADR first: context, alternatives, decision, consequences.
   Get the format doc amended in the same change so doc and code never
   diverge.
2. Version explicitly. RSEG bumps the trailer version; identity encodings
   get a new domain string (ravel-series-v2, not an edit to v1); protos
   add fields, never renumber or reuse; key layouts add new prefixes.
3. Answer the dual-reader question in the ADR: does deployed code need to
   read both versions? If any stored data exists in the old format, the
   answer is yes, and the reader keeps both paths until retention clears
   the old data.
4. Review checksum coverage. Every byte a reader interprets must be under
   a checksum the reader can afford to verify on its access path (ADR-0010
   section 4 is the precedent: a page crc that skipped the encoding byte
   allowed silent misdecoding).
5. Extend fuzz and property tests to cover both versions, plus corrupt
   and truncated inputs for the new one.
6. Update ravel-cli inspectors to print the new version's fields.

If a change cannot follow this procedure, it does not happen. There is no
fast path for format changes.

---
name: format-change
description: Use before changing any persistent format - RSEG layout, proto schemas, series identity, commit tokens, object key layout - these are frozen contracts with a mandatory procedure
---

# Changing a persistent format

Persistent formats outlive processes and deployments. ADR-4233 is the
retained-data compatibility contract and docs/compatibility.md is its
normative statement: read both before you start. Two regimes apply by
release, and the format's ADR must say which:

- Pre-baseline (v0.33.x and earlier): a single supported version per
  family. No pre-baseline release is a target for new format work.
- From the ADR-4233 baseline release onward: a two-version window with
  activation and certification. A release that introduces N+1 of a family
  ships the reader AND the writer for N and N+1, and never two new versions
  of one family. **A new format version is activated, never just
  deployed**: every writer takes its version from `sys/compat`
  (`ravel_catalog::compat`), so installing the release changes nothing it
  writes until an operator activates N+1. A writer that emits a
  compile-time constant instead of the policy's version is a contract
  violation. Releases before v0.33 are outside the contract.

Within the window, each `sys/compat` family follows one regime
(`KNOWN_FAMILIES`; docs/compatibility.md "Regimes by family"):

- **Class A, certified** (`rseg`, `rlog`, `rspan`): writers emit the active
  version. The N reader is retired only by a later release that starts on
  a `compat certify` certificate (ADR-4233 decision 7). That release's ADR
  adds its own corpus and a boundary lane with the previous release's
  image.
- **Class B, catalog** (`catalog-head`, `csnap`, `npost`, `cstat`, `kidx`):
  no certificate. The fold checks the active version as a ceiling, and a
  release that drops a catalog version rebuilds parts below its window
  from commit records on its first fold; its ADR states that rebuild cost.
- **Content-versioned ceilings** (`catalog-head`, `compaction-record`):
  the active version is a ceiling, not the emitted version. A writer
  refuses when the active version is not one its build can write, and
  otherwise emits the version its content needs, at or below the active
  one (ADR-4233 ceiling amendment).
- **Immutable records, single version** (`commit-record`,
  `rewrite-record`, `retention-tombstone`): never rewritten, so their old
  readers are never retired under ADR-4233; retiring one needs its own ADR.

A format that is not a `sys/compat` family (CAS-mutable sys records, the
provisioning record, tenant config, metadata, idempotency markers, `enc`)
keeps ADR-0066 R1's readers-first rule. The baseline's metadata sink,
idempotency markers and `enc` writer carry no `sys/compat` gate: a bump of
any of them must add the gate first (ADR-4233 prerequisite amendment).

Every new exported format-version constant gets a row in
docs/compatibility.md's inventory in the same change.

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
2. Version explicitly. A change to a versioned binary layout bumps that
   layout's version: RSEG, RLOG and RSPAN trailers and section versions
   (POSTINGS, KEY_IDX) and the envelopes of the catalog's `.csnap`,
   `.npost`, `.cstat` and `.kidx` objects. A proto record carried inside
   one of them (a `.csnap` entry or part header) follows the record rule
   below instead, as ADR-0873's `SnapshotEntry` field 15 did.
   Identity encodings get a new domain string (ravel-series-v2, not an
   edit to v1); key layouts add new prefixes. Protos add fields with new
   numbers and never renumber or reuse one. A catalog HEAD change raises
   the HEAD version for a HEAD that carries the new field (ADR-4233, H2;
   the version is a function of content, `head_format_version_for`, as
   `retired_hours` does), because an older build's HEAD rebuild strips a
   field it does not know. Any other record may add a
   field with no bump only when an older reader that ignores the field and
   an older writer that rewrites the record without it (a CAS replace or a
   rebuild) both leave query answers, durability, retention and fencing
   unchanged. Otherwise the record stamps a higher version exactly when it
   carries the field (the `ProvisioningRecord` format_version 4 pattern),
   or bumps. The ADR states which case applies and why.
   Version constants stay single-sourced in each format crate
   (`SUPPORTED_VERSIONS`, `SegmentVersion::WINDOW`, `footer::VERSION`); the
   writer, the reader gate, `audit-versions`, `migrate`, and the compactor's
   `OUTPUT_FORMAT_VERSION` all read them, so a bump edits one constant, not
   the sixteen hand-mirrored sites ADR-0049 measured.
3. Answer the dual-reader question in the ADR. For a `sys/compat` family
   from the baseline on: the release that introduces N+1 widens the
   reader window and the writable set to {N, N+1}, its writers keep
   emitting N until `compat activate`, and every producer of the family
   (ingest flushes, compaction, migrate, the force-2 re-encode, erasure,
   audit, alerts, legal hold, the fold) reads its version from the policy.
   Retiring the N reader is a separate later release: for class A it
   starts only on an uncontradicted certificate with the late-write
   watermark past it (ADR-4233 decision 7); for class B it rebuilds from
   commit records. Never a wipe-and-hope. The operator procedure is
   docs/guides/operations/maintenance.md, "Rolling a format bump".
4. Review checksum coverage. Every byte a reader interprets must be under
   a checksum the reader can afford to verify on its access path (ADR-0010
   section 4 is the precedent: a page crc that skipped the encoding byte
   allowed silent misdecoding).
5. Extend fuzz and property tests to cover both versions, plus corrupt
   and truncated inputs for the new one.
6. Update ravel-cli inspectors to print the new version's fields.
7. When a format's version or reader window changes (including dropping an
   old reader), add a changelog fragment naming every signal that format
   carries (RLOG: logs, alerts, audit; RSPAN: spans; RSEG: metrics);
   `scripts/guards/check-format-bump-release-note.sh` fails the pull request
   otherwise (ADR-2708 D5d).

If a change cannot follow this procedure, it does not happen. There is no
fast path for format changes.

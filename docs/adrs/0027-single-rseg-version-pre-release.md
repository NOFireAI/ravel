# ADR-0027: Single supported RSEG version until first release

Status: Accepted (2026-07-28). Implementation: #179, building on the
RSEG v5 implementation (#176, ADR-0026). This ADR changes the format
lifecycle policy, not any byte of the v5 layout.

## Context

RSEG has five versions. v1 is the original layout with the
SERIES_TABLE catalog, v2 replaced that catalog with the columnar
SERIES_IDS/SERIES_META pair (ADR-0014), v3 added native histogram
pages (ADR-0017), v4 is the multi-run compaction output (ADR-0018),
and v5 adds the sparse id index and chunked SERIES_META for the
compacted tier (ADR-0026). Every bump kept the earlier versions
readable: the reader dispatches on trailer versions 1 through 5,
golden-byte and differential suites pin the old layouts, ravel-bench
carries per-version builders for cross-version comparisons, and
docs/segment-format.md is written as a v1 baseline plus four
amendment layers.

That compatibility surface protects nobody today. Ravel has not been
released; no deployment outside development holds RSEG objects, and
development object stores can be wiped or re-ingested at will. The
cost is real, though: every format change adds a version while keeping
every older read path, its tests, and its documentation alive, and
each new capability lands as an amendment to an amendment. Pre-release
the priority is iteration speed on the format, not compatibility with
data nobody depends on.

## Decision

1. Until the first public release, exactly one RSEG version is
   supported: the latest, currently v5. The reader accepts trailer
   version 5 only; versions 1 through 4 fail closed with the same
   typed UnsupportedVersion error as unknown future versions. The
   write surface is the single v5 writer.
2. All writers emit v5, L0 flush writers included. This supersedes
   ADR-0026's "L0 never emits v5" clause. Below the 4096-series
   threshold a v5 object omits the sparse sections and its grammar is
   identical to v4's, differing only in the trailer version bytes, so
   small L0 objects pay nothing for the bump; the emission threshold,
   not the writer tier, is what protects small objects. The one-flip
   question tracked in #140 (which version L0 should move to)
   dissolves: there is only one version to write.
   Addendum (2026-07-28, from the #179 implementation): an L0 flush is
   not a compaction output, so the raw-sample write adapters stamp
   sentinel compaction-meta values: level 0, part_index 0, an all-zero
   input_set_hash, and per-run created_unix_ns equal to
   ingest_bounds.max_ingest_ts_ns. This is the normative L0 provenance
   convention; the compactor and sweeper key off these fields.
3. v1-v4 write paths, reader fallbacks, golden files, fuzz seeds, and
   bench builders are deleted, not deprecated. v5 goldens become the
   baseline. Corrupt-input seeds carrying trailer versions 1-4 stay,
   pinning that old objects are rejected with a typed error and never
   half-parsed.
4. Retired section kinds are reserved forever. SERIES_TABLE (kind 2)
   left the format with v1 and its number is never reused; the frozen
   section-kind registry records retired kinds explicitly. A stray old
   object must stay detectably foreign, never plausibly parseable.
5. Only retention is dropped; the change discipline stays. A layout
   change still requires an ADR and a version bump, never an in-place
   edit under the same version number: even development stores hold
   mixed-version objects mid-rollout, and a misread must fail closed
   rather than decode wrong data. What changes is that the previous
   version's read and write support is deleted in the same change that
   introduces the new version.
6. No migration tooling. Development object stores holding pre-v5
   objects are wiped or re-ingested. Acceptable only because nothing
   has shipped.
7. This policy expires at the first public release. The shipped
   version freezes, the readers-before-writers rollout discipline and
   compatibility windows return, and removing support for a released
   version becomes a breaking change requiring its own ADR.
8. Documentation follows the code: docs/segment-format.md becomes a
   self-contained specification of the current version, not a baseline
   plus an amendment chain. History lives in the ADRs. ADR-0014 is
   superseded (the v2 layout is gone); ADR-0017 and ADR-0018 are
   amended, not superseded, because the histogram value model and the
   compaction design they decided continue in v5; ADR-0026 is amended
   on its reader-version-range and L0-emission clauses by points 1 and
   2 above.

## Consequences

- Each future format iteration deletes as much as it adds: reader,
  writer, test, and documentation surface stay proportional to one
  version, not to the version count.
- Any pre-v5 object left in a development store becomes unreadable,
  visibly (typed UnsupportedVersion), never silently. Wipe or
  re-ingest.
- Cross-version byte-comparison benches lose their comparison basis
  and are removed; their recorded numbers in BENCHMARKS.md stand as
  the historical record. #94, which gates a v2 decode ratio against
  the removed v1 path, becomes obsolete; #140 is superseded by
  decision 2.

# ADR-0026: RSEG v5: sparse id index and chunked SERIES_META for the compacted tier

Status: Accepted (2026-07-28). Decides issue #175 from the issue #167
experiment. This ADR records the decision; docs/segment-format.md gains
its "RSEG v5 amendment" section when the implementation lands, and until
then nothing in this ADR changes any stored byte. The flag-gated
prototype merged from #167 (crates/ravel-segment/src/experiment.rs) is
measurement-grade and is replaced, not promoted, by the implementation.

## Context

A selective read against an RSEG object fetches the whole catalog:
SERIES_IDS and SERIES_META are whole-section objects, so a point lookup
against a 100k-series object costs 3.05MB to answer a question about
one series (issue #97, re-measured in #167). At the L0 flush shape
(hundreds of series) this does not matter; the 64KiB suffix probe
already covers most of the object. It matters exactly where compaction
is about to take us: L1 objects (ADR-0018) merge many L0 runs and grow
per-object series counts into the range where the catalog fetch
dominates selective-query cost. This was also the one measured read
shape where a Parquet layout still beat RSEG (1.6MB for the same
lookup).

The #167 experiment prototyped two pieces behind a writer flag and
measured GET counts and bytes (backend-independent) at 500 / 10k / 100k
series:

- SERIES_IDX: every Kth series id plus byte offsets, a new section.
  Additive: readers that do not know the kind skip it. Alone it removes
  only the SERIES_IDS fetch (3.05MB to 1.47MB at 100k).
- Chunked SERIES_META: per-block zstd frames instead of one
  whole-section frame, chunk directory carried in SERIES_IDX. Not
  additive (changes the meta layout). This is where the order of
  magnitude comes from: point lookup at 100k drops to 92KB, a 33x
  reduction, under Parquet's 1.6MB.

Write-side cost at 100k: index 0.063% of object bytes, chunk
compression loss +0.33%, total +0.48%. At 500 series the index costs
instead of paying: the 10-series matcher fetches 162KB with the index
against 49KB without, and the whole object is smaller than the suffix
probe. The experiment's decision rule (point lookup at 100k under
400KB, growth under 2%) was met with wide margin.

## Decision

1. RSEG v5 exists: trailer `version = 5`, the v4 grammar (ADR-0018)
   plus two sections: SERIES_IDX (kind 8) and chunked SERIES_META
   (kind 9, replacing the kind 6 whole-section form when present).
   Both pieces ship together in v5; the additive half is not worth a
   separate rollout (measured 2x, not 33x). Kinds 8 and 9 enter the
   frozen section-kind registry with the implementation.

2. v5 is the default compaction output format. This amends ADR-0018,
   whose output was v4: the compactor (compaction-retention-plan phase
   4, issue #111) writes v5. Within v5 the sparse sections are
   optional: they are emitted when the output object has
   `series_count >= 4096` and omitted below that, where the object
   simply carries the v4-shaped catalog and readers use the legacy
   whole-catalog path. One output version, presence signaled by the
   sections themselves.

3. L0 writers never emit v5. The measured small-object regression is a
   property of the L0 shape itself, not a tuning artifact. The L0
   default path remains ADR-0017/issue #140 (v3, readers before
   writers), and this ADR does not change it.

4. The emission threshold is 4096 series. The measurement brackets the
   crossover between 500 (loses) and 10k (wins 4.2x on point lookup);
   4096 is the conservative power of two inside the bracket. It is a
   writer-side constant, not a contract: changing it later changes no
   reader behavior, because presence is signaled per object.

5. K = 512 for both the sparse-id stride and the chunk stride, as
   measured: index stays under 0.1% of object bytes and the frame count
   stays low enough that chunked zstd loses only 0.33% against the
   whole-section baseline.

6. Partial fetches verify what they touch. Whole-section crc32c cannot
   check a range-GET, so SERIES_IDX entries carry per-id-window and
   per-meta-chunk crc32c alongside offsets. SERIES_IDX itself is small
   (4.7KB at 100k), always fetched whole, and covered by its ordinary
   Section.crc32c. This preserves the reader rule that every byte used
   is checksum-verified (ADR-0010 §4) on the sparse path.

7. Composition with the metric index (ADR-0020, issues #120/#121):
   postings prune which segments a query reads; SERIES_IDX prunes what
   a query reads within one segment. The combined selective path is:
   snapshot resolve, postings segment pruning, per-segment footer
   probe, then the sparse probe when the sections are present and the
   legacy whole-catalog read when they are not.

8. Rollout keeps the readers-before-writers discipline used for v2 and
   v3: readers accept versions 1-5 and the sparse read path lands
   before any writer emits version 5. Since no production compactor
   exists yet (#111 open), there is no flag-flip moment for existing
   data; v5 becomes the default by being what the compactor writes from
   its first release.

## Consequences

- The last read shape where Parquet beat RSEG inverts: 92KB against
  1.6MB for a point lookup at 100k series, at +0.48% stored bytes.
- The case for capping L1 output size on catalog-fetch grounds (#148)
  weakens: lookup cost is bounded by the index, not by object size.
  The size budget question becomes one of compaction work units and
  blast radius, not read amplification.
- The spec grows a fifth version. The mitigations are the ones already
  in force: readers dispatch on the trailer version, every version
  remains readable indefinitely, and the golden-bytes, fuzz, and
  differential suites extend to v5.
- The #167 prototype code is deleted when the implementation lands;
  its tests (flag-off byte identity, old-reader section skip) migrate
  to the production paths.

## References

- Issue #167 (experiment, measurement tables), #175 (this decision),
  #97 (read-path accounting), #148 (L1 size budget), #140 (v3
  rollout), #111 (compactor), #120/#121 (postings).
- docs/segment-format.md (v1-v4; v5 amendment to follow with the
  implementation), docs/compaction-retention-plan.md, ADR-0014,
  ADR-0017, ADR-0018, ADR-0020.
- bench: crates/ravel-bench selective_read_accounting,
  section_bytes_report; deterministic byte gates in
  tests/catalog_byte_gates.rs (#166).

# Ravel architecture diagrams

Hand-authored SVGs. Each file has its own legend. Colors: mid blue for
services and actors, amber for immutable objects in the object store, green
for checksum-verified regions, dashed gray/blue for reserved or planned
items. All diagrams use plain SVG shapes and text, no embedded images, and
stay under 60 KB.

## architecture.svg

Shows the full system: OTLP gRPC and OTLP HTTP clients plus the planned
OTAP path on the ingest side, Prometheus API consumers on the query side.
Ingest flows through the gateway, ingest router, and shard actors down to
L0 RSEG objects and commit records. Query flows from the /api/v1 surface
through the PromQL evaluator, query workers, and catalog resolution. The
object store sits in the middle as the single durable center; every box
above it is disposable and stateless.

Illustrates: docs/architecture.md, docs/adrs/0001-object-native-l0.md.

Updated 2026-07-27 to add the SQL query path (ravel-sql, DataFusion, ADR-0013):
in progress, not yet wired to a service endpoint. Last verified against the
code: 2026-07-27 (Phase 1 -- OTLP ingest, commit, catalog, PromQL selector
queries -- is complete and running end to end; OTAP ingest is scaffolded but
not wired into the gateway).

## ingest-commit-sequence.svg

A vertical sequence diagram of one strict-mode flush: pin the flush
identity, PUT the data object with create-if-absent and a checksum, PUT the
commit record with create-if-absent and an idempotency check on
AlreadyExists, then ack with a commit token. Marks the three crash points
from the crash matrix and what each one leaves behind: nothing stored, an
invisible orphan, or a visible segment where a client retry stores a
duplicate.

Illustrates: docs/ingest.md, docs/catalog-and-mvcc.md (commit sequence),
docs/consistency-model.md (crash matrix), docs/adrs/0002-commit-protocol.md.

Last verified against the code: 2026-07-27 (Phase 1 complete; ingest and
commit crates match this sequence end to end against MinIO/S3).

## rseg-layout.svg

The byte layout of an RSEG v6 object (the only supported version,
ADR-0027 and ADR-0047). The main drawing is the v6 layout: LABEL_DICT and SERIES_IDS,
the run-major SERIES_META catalog (or, at/above the 4096-series threshold,
the sparse SERIES_IDX kind 8 + chunked SERIES_META_CHUNKS kind 9 pair),
the TS_PAGES / VAL_PAGES / HIST_PAGES containers with per-page headers
(enc, comp, and a crc computed over series_id plus enc plus comp plus
payload), the protobuf footer, and the 16-byte trailer with its field
breakdown. Brackets show exactly what footer_crc32c, a section's crc32c,
each sparse partial-fetch crc32c (id window, meta chunk frame), and a
page's crc each cover. A short history note points at the ADRs that built
up the layout.

Illustrates: docs/segment-format.md (the self-contained v5 spec),
docs/adrs/0004-rseg-format.md,
docs/adrs/0010-spec-amendments-review-1.md (§4, checksum coverage),
docs/adrs/0014, 0017, 0018, 0026, 0027.

Last verified against the code: 2026-07-28 (v5 is the only readable and
writable version, ADR-0027; frozen and proved byte-identical by the
golden_bytes_v5 test; the sparse sections are byte-gated < 1% of object at
the 10k shape by the deterministic catalog_byte_gates test in ravel-bench).

## query-path.svg

The query path from an incoming PromQL request to the JSON response:
catalog snapshot resolution (the listing window plus exact-key token
reads), a suffix GET of each segment's footer, series pruning through
SERIES_META and LABEL_DICT, range coalescing into a small number of GETs,
page decode, cross-segment dedup order, the PromQL evaluator, and the
Prometheus JSON envelope.

Illustrates: docs/query-engine.md, docs/catalog-and-mvcc.md (snapshot
resolution, dedup order), docs/segment-format.md (reader protocol).

Last verified against the code: 2026-07-27 (Phase 1 complete; this is the
live `/api/v1` PromQL path. A second, SQL query path over the same segments
now exists in `ravel-sql` -- see architecture.svg's SQL query path panel --
but it is not yet wired to a request-response diagram of its own since it
has no HTTP/Flight endpoint yet).

## tenancy-key-layout.svg

The bucket key tree under t/<tenant_hash>/: the metrics signal prefix with
its l0 data keys and commit keys, the reserved logs/spans/profiles
prefixes, and the future catalog snapshot and HEAD locations. Breaks down
each key's components: writer id, epoch, seq, and the blake3-derived
hash16 for data keys; the ingest hour bucket for commit keys.

Illustrates: docs/catalog-and-mvcc.md (key layout), docs/adrs/0009 (not
read directly; referenced by docs/catalog-and-mvcc.md for tenant_hash),
docs/adrs/0010-spec-amendments-review-1.md (§3 writer identity, §13 tenant
hash).

Last verified against the code: 2026-07-27 (Phase 1 complete; this is the
live object-store key layout).

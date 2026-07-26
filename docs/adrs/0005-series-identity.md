# ADR-0005: BLAKE3-128 canonical series identity with stored-label collision verification

Status: Accepted (2026-07-26)

## Context

Metric routing, dedup, indexing, and PromQL grouping all need a stable series
identity. Prometheus identity is (metric name, label set); OTel adds resource
and scope, which map into labels during normalization (Phase 1 flattens
resource attributes into labels under the standard mapping; revisit for
native OTel querying).

## Decision

`series_id = BLAKE3(canonical_bytes)[0..16]` where canonical_bytes is:

```
"ravel-series-v1\0"
u16_le(len(tenant)) tenant_utf8
u16_le(len(metric_name)) name_utf8
u16_le(label_count)
for each label sorted by name bytes:
  u16_le(len(name)) name_utf8 u16_le(len(value)) value_utf8
```

Unit and type are metadata, not identity (matches Prometheus). Tenant is
inside the hash so cross-tenant collisions are impossible even if physical
prefixes ever merge. Segments always store the full label set alongside the
id; any component that groups by id can verify labels, and the ingest path
rejects a batch that maps two distinct label sets to one id (probability
~2^-64 at birthday bound; detection makes it fail-loud, not silent).

## Consequences

- 16-byte ids keep series tables and postings compact; 128 bits is enough for
  10^12 series with negligible collision probability.
- The canonical encoding version string allows a v2 identity without silent
  aliasing.
- Routing uses `hash(tenant, series_id)` per the partitioning spec.

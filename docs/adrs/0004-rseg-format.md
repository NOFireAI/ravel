# ADR-0004: RSEG v1: hand-specified layout, protobuf footer, per-page compression

Status: Accepted

## Context

The on-object segment format is a persistent contract: versioned, checksummed,
suffix-readable, forward-compatible, and independent of Rust memory layout.
Query workers must plan reads from a single small suffix fetch.

## Alternatives

1. Parquet: mature, but metric-specific encodings (Gorilla XOR, per-series
   pages keyed by a 128-bit series id) fight the model, and footer control is
   limited.
2. rkyv/postcard structs: positional/memory-derived formats make forward
   compatibility and cross-language tooling risky.
3. Hand-specified container: fixed magic/version trailer, independently
   compressed pages addressed by a protobuf-encoded footer.

## Decision

Option 3, specified byte-exactly in `docs/segment-format.md` and
`proto/ravel/segment.proto`. Protobuf gives the footer deterministic encoding
(no maps, fields written in tag order by prost) and unknown-field tolerance
for forward compatibility. Pages are individually compressed and checksummed
so any page is readable without touching others. Parsers treat all lengths and
offsets as untrusted (bounds-checked; fuzz targets required). `unsafe_code`
is denied workspace-wide.

## Consequences

- One suffix GET (default 64 KiB) resolves the footer in the common case; a
  second ranged GET covers oversized footers.
- Two-layer codec model (transform codec + general compressor) is recorded per
  page, enabling per-block codec selection later without a format break.
- Protobuf decode cost on the footer is negligible relative to a GET round
  trip; measured before optimizing.

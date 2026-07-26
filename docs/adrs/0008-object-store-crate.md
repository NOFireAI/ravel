# ADR-0008: Wrap `object_store` crate behind our ObjectStoreBackend trait

Status: Accepted (2026-07-26)

## Context

We need S3, MinIO, and later GCS/Azure adapters with conditional creation,
CAS, ranged/suffix reads, multipart, and listing. Writing raw SDK adapters
triples surface area; but coupling the codebase to a third-party trait would
let its semantics leak into our correctness arguments.

## Alternatives

1. AWS SDK directly + hand-rolled MinIO/GCS variants.
2. `object_store` crate (Apache Arrow project) used directly everywhere.
3. Our own `ObjectStoreBackend` trait; `object_store` as one adapter behind it.

## Decision

Option 3. Our trait (see `docs/object-store-contract.md`) expresses exactly
the capabilities Ravel's protocols rely on (`create-if-absent`, etag CAS,
suffix ranges, strongly consistent list-after-write) with a capability matrix
per backend. `object_store` provides the S3/MinIO implementation
(`PutMode::Create`, `UpdateVersion` preconditions, `GetRange::Suffix`).
The in-memory reference implementation and the fault-injecting wrapper
implement the trait natively so tests exercise *our* contract, not the
dependency's.

## Consequences

- Swapping or adding backends (GCS generation preconditions, Azure etags)
  never touches commit-protocol code.
- We accept `object_store`'s dependency tree in exchange for battle-tested
  retry/multipart handling; audited via cargo-deny.
- Capability probes are explicit: production refuses to start on a backend
  lacking mandatory capabilities rather than degrading silently.

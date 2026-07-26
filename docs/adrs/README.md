# Architecture Decision Records

One decision per document. Status: Proposed | Accepted | Superseded.

| # | Title | Status |
|---|-------|--------|
| [0001](0001-object-native-l0.md) | Object-native L0, no local WAL | Accepted |
| [0002](0002-commit-protocol.md) | Two-object commit protocol with create-if-absent commit records | Accepted |
| [0003](0003-catalog-discovery.md) | Listing-based discovery first, immutable catalog snapshots second | Accepted |
| [0004](0004-rseg-format.md) | RSEG v1: hand-specified layout, protobuf footer, per-page compression | Accepted |
| [0005](0005-series-identity.md) | BLAKE3-128 canonical series identity with stored-label collision verification | Accepted |
| [0006](0006-query-engine.md) | Custom signal-aware engine first; Arrow/DataFusion evaluated at Phase 3 | Accepted |
| [0007](0007-promql-approach.md) | promql-parser crate for parsing, own evaluator, differential testing gate | Accepted |
| [0008](0008-object-store-crate.md) | Wrap `object_store` crate behind our ObjectStoreBackend trait | Accepted |
| [0009](0009-tenant-isolation.md) | Tenant-hashed prefixes, gateway auth, dev-mode header tenancy behind flag | Accepted |
| [0010](0010-spec-amendments-review-1.md) | Spec amendments from the first adversarial design review | Accepted |

# Traceability: RavelObjectStore invariants to their Rust source

Each row maps a TLA+ action or property in `MCRavelObjectStore.tla` /
`RavelObjectStore.tla` to the object-store contract behavior it pins, to the
Rust symbol that implements that behavior, and to the contract test that
exercises it (ADR-1113 D8). `scripts/check-tla.sh traceability` resolves the
Rust references: a reference is `crates/<path>.rs::Sym1::Sym2...`, and the
check requires the file to exist and every `::`-separated symbol to appear in
it. A reference to a `.tla` file is rejected: a traceability row must cite the
implementation, not the model. The "new test needed" column names a gap where
no contract test exists yet, or `none` when the existing test covers it.

| TLA+ action or property | Meaning | Rust path and symbol | Existing test | New test needed |
|---|---|---|---|---|
| PutCreateIfAbsent / CreateIfAbsentWinnerUnique | CreateIfAbsent has at most one winner per presence interval; a second create on a present key returns AlreadyExists | crates/ravel-object-store/src/lib.rs::PutMode::CreateIfAbsent | crates/ravel-object-store/tests/contract.rs::assert_create_if_absent_atomicity | none |
| PutCasVersion / CasOutcomeMatchesEffect | CasVersion applies only against the current version of a present key; a stale version or an absent key is PreconditionFailed and applies nothing | crates/ravel-object-store/src/memory.rs::MemoryStore::put | crates/ravel-object-store/tests/contract.rs::assert_cas_version_semantics | none |
| WriteVisible / ReadAfterWrite | A successful write is immediately visible, tagged with a fresh version drawn from the global monotonic counter | crates/ravel-object-store/src/memory.rs::next_id | crates/ravel-object-store/tests/contract.rs::assert_cas_version_semantics | none |
| Delete / DeleteIdempotent | Delete of an absent key returns Ok and changes no observable state; the version counter is never reset, so create/delete/create mints a fresh version and a CAS on a pre-delete token fails | crates/ravel-object-store/src/memory.rs::delete | crates/ravel-object-store/tests/contract.rs::assert_idempotent_delete | none |
| MultipartComplete / MultipartInvisibleUntilComplete | Nothing an in-progress multipart upload has staged is visible before Complete; Complete publishes with Overwrite semantics | crates/ravel-object-store/src/memory.rs::put_multipart | crates/ravel-object-store/tests/contract.rs::assert_multipart_upload | none |
| ListReturn / ListProgress / ListEventuallyComplete | Every key present when a traversal began is eventually returned; a key may be delivered more than once, so a multiplicity-sensitive consumer must deduplicate | crates/ravel-object-store/src/lib.rs::list_all | crates/ravel-object-store/tests/contract.rs::assert_paginated_listing_completeness | none |
| PutOverwriteLostResponse / LostResponseEffectApplied | A lost-response write applied its durable effect even though the caller observed Failure; the S3 error mapping is what turns a backend ack loss into that Failure | crates/ravel-object-store/src/s3.rs::map_put_error | crates/ravel-object-store/tests/contract.rs::assert_cas_version_semantics | a lost-ack durability test asserting the effect landed after a Failure is observed |
| TransientFailure / TransientLeavesNothing | A transient failure applies nothing and returns a retryable Failure; the caller may retry safely | crates/ravel-object-store/src/lib.rs::StoreError | crates/ravel-object-store/tests/contract.rs::assert_idempotent_delete | a transient-then-retry test asserting no partial effect from the failed attempt |

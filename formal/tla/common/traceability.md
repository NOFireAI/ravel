# Traceability: RavelObjectStore invariants to their source

Each row maps a contract requirement to the invariant (or liveness property)
that pins it in `MCRavelObjectStore.tla`, and to the source symbol that
encodes the behavior. `scripts/check-tla.sh traceability` parses the third
column: a repo-relative path, optionally `path:Symbol`, and fails if the path
is missing or the symbol is absent from it. Keep this table to exactly three
columns.

| Requirement | Invariant / property | Source ref |
|---|---|---|
| CreateIfAbsent has at most one winner per presence interval | CreateIfAbsentWinnerUnique | formal/tla/common/RavelObjectStore.tla:PutCreateIfAbsent |
| CAS applies only against the current version | CasNeedsFreshVersion | formal/tla/common/RavelObjectStore.tla:PutCasVersion |
| A durable write is visible to a later read | ReadAfterWrite | formal/tla/common/RavelObjectStore.tla:WriteVisible |
| Delete is idempotent and total | DeleteIdempotent | formal/tla/common/RavelObjectStore.tla:Delete |
| No multipart part is visible before Complete | MultipartInvisibleUntilComplete | formal/tla/common/RavelObjectStore.tla:MultipartComplete |
| Every started listing eventually returns its snapshot | ListEventuallyComplete | formal/tla/common/RavelObjectStore.tla:ListProgress |
| Contract semantics under test | (module) | docs/object-store-contract.md |

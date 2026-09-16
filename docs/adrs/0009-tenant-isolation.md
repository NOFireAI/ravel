# ADR-0009: Tenant-hashed prefixes, gateway auth, dev-mode header tenancy behind flag

Status: Accepted

## Context

Multi-tenancy is defense-in-depth: authenticated resolution, scoped physical
prefixes, no tenant names leaked via object listings, no tenant id trusted
from request bodies.

## Decision

- Physical prefixes use `tenant_hash = hex(blake3("ravel-tenant-v1" || tenant_id)[0..16])`;
  human-readable tenant ids never appear in object keys.
- The gateway resolves tenants from authenticated identity. Phase 1 ships
  static bearer-token → tenant maps from config; OIDC/JWT follows. A
  `--dev-insecure-tenant-header` flag (default off, refuses to enable outside
  loopback binds) accepts `x-ravel-tenant` for local development and is
  logged loudly at startup.
- Query frontend authorizes per tenant before planning; workers receive the
  resolved tenant in signed internal context (Phase 1: in-process, so the
  boundary is the API layer).
- Per-tenant quotas (series, bytes/s, query bytes scanned) enforced at
  gateway/frontend from config; hard limits precede allocation.

## Consequences

- Tenant names are not readable from object keys. The default hash is
  unkeyed, so anyone with bucket-list access can confirm a guessed tenant
  id offline; deployments needing enumeration resistance configure the
  keyed tenant hash (ADR-0010 §13).
- Static token maps are a stopgap; OIDC support follows when the gateway
  hardens.

## Amendment (2026-09-07): the loopback refusal covers every listener the shared resolver backs

The Decision above promised that `--dev-insecure-tenant-header` "refuses to
enable outside loopback binds". As shipped, `Cli::validate` tested only
`--listen-http`. But the dev-header resolver joins one shared resolver chain
(`services/ravel-server/src/tenant.rs`), and that single chain backs every
public listener: HTTP, remote-write, OTLP gRPC, and Flight SQL (through the
Flight auth path). So the flag was reachable, unguarded, on a non-loopback
`--listen-grpc`, letting an unauthenticated `x-ravel-tenant` header forge
tenant identity on the public gRPC and Flight SQL surfaces (issue #1293).

This is the class of defect the resolver-chain doc comment already names: a
resolver that trusts an unauthenticated header has no business backing a
public listener. The ADR's promise is refusal on a reachable port, not
narrowing.

The guard now refuses the flag unless both `--listen-http` and `--listen-grpc`
bind loopback addresses. Both defaults are loopback and no shipped manifest
sets the flag, so the tightened guard breaks nothing that ships.

The clean fix is by construction: exclude the dev-header resolver (and any
resolver trusting an unauthenticated header) from the gRPC and Flight
resolver chains entirely, rather than gating a config flag. That was not done
here because `config.tenant_resolver` is a single `Arc` consumed by five call
sites and re-wrapped by `DurableBearerResolver` in `lib.rs`; splitting it
means threading two resolvers through `start()`, `gateway_state`, the Flight
service, and `FragmentService::new`, which collides with other in-flight
changes to `lib.rs`. That resolver split is the stated follow-up.

The ingest-router sibling (`services/ravel-ingest-router`) carried the same
unguarded flag with no loopback check at all. There the resolved tenant is a
routing key and the proxy forwards the original headers so the upstream
re-authenticates, so this is forged routing affinity rather than forged
identity, a lower guard than the gateway's. It now carries the same loopback
requirement on its bound listeners.

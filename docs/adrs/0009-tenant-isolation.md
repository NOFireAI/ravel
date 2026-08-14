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

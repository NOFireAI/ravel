# ADR-2014: Default to byte-minimal fetching when the object store is on loopback

- Status: Proposed
- Date: 2026-09-26
- Refs: #2014, #1463, #1196, ADR-1196, ADR-0996, ADR-0904, #1707

## Context

ADR-1196 kept `cost-based` as the default logs fetch policy. At the reference
profile `s3-intra-region-2026`, transfer is free and requests are billed, so
`cost-based` resolves to one whole-object GET per touched object. ADR-1196
rejected `byte-minimal` as a default for two reasons. At the default
concurrency it measured 712.4 s against 525.0 s on real S3 (36% slower). At
the concurrency where it won, it was not yet memory-safe in a long-lived
server.

Both measurements were taken against real S3, where the cold path is bound by
the NIC and every request crosses the network. A store on the same host has
neither property. Ravel's ClickBench entry runs a single-node RustFS on
loopback, at the benchmark maintainer's request, and so can any single-host
deployment of Ravel. There the cold path is bound by the local disk. Whole-object
reads then read every touched object in full from that disk, whatever the
statement projects. On the ClickBench reference machine, 40 of the 42 statements
that return a number each read the whole 11.2 GB dataset cold. At the gp2
volume's rate that is 42.6 to 46.6 s per statement.

Measured on that machine (c6a.4xlarge, 500 GB gp2), RustFS 1.0.0 on
`127.0.0.1`, v0.16.1, true cold (restart and page-cache drop before each
statement), 42 statements, one pass per arm (#1463, comment 5841140007):

| policy | concurrency | cold | hot |
|---|---|---|---|
| `cost-based` (default) | 32 (derived) | 1,720.4 s | 272.3 s |
| `byte-minimal` | 32 (derived) | 1,186.6 s | 88.5 s |
| `latency-first` | 256 | 1,187.5 s | 80.9 s |

`byte-minimal` at the derived concurrency is 31% faster cold and 68% faster hot,
and no statement is more than 5% slower cold than under `cost-based`. Raising
concurrency to 256 changes cold by 0.1%, and the 9% hot difference is inside
this host's single-pass noise floor (about 15%). On a local store the disk,
not request latency, is the constraint, and concurrency does not relieve it.

ADR-0996 rejected "auto-detecting the billing shape from the endpoint"
(alternative 6), because S3-compatible endpoints do not disclose billing and a
guessed cost profile in provenance is worse than a declared one.

## Decision

**When `--logs-fetch-policy` is not given and the S3 endpoint is loopback, the
default policy is `byte-minimal`.** Every other deployment keeps `cost-based`,
exactly as ADR-1196 decided.

1. *Loopback* is decided by the same predicate that already decides whether
   plaintext is allowed to the endpoint (#1707): the endpoint's host is
   `localhost`, a loopback IPv4 literal, or the loopback IPv6 literal. A name
   that merely resolves to loopback is not loopback. There is one predicate,
   in `ravel-object-store`, and the fetch default calls it; it is not copied.
2. An explicit `--logs-fetch-policy` always wins, including an explicit
   `cost-based` on a loopback endpoint.
3. Concurrency is untouched. The default derives fetch concurrency exactly as
   today. The measurement above shows no gain from raising it on a local store,
   and leaving it alone keeps this default clear of ADR-1196's memory
   precondition, which concerns high concurrency.
4. The choice is logged, not silent. The `logs fetch policy resolved` line
   carries a `policy_source` of `flag` (given explicitly), `default` (not given,
   endpoint not loopback) or `derived-loopback-endpoint` (not given, endpoint
   loopback).
5. The cost profile is not touched. The stamped profile stays whatever
   `--store-cost-profile` or the reference profile says. This decision derives
   a fetch policy from where the store is. It does not derive a price from the
   endpoint.

This does not reopen ADR-0996's alternative 6. That alternative guessed a
*billing shape* from an endpoint, which an endpoint cannot disclose. A loopback
endpoint discloses no billing either; what it establishes is locality. The
store is on this host and is reached without a network, and that fact is
exactly what makes the cold path disk-bound. The decision rests on locality,
records it as the source, and leaves the declared prices alone.

## Rejected alternatives

**Leave the default alone and document `--logs-fetch-policy byte-minimal` for
local stores.** Correct and zero code. Rejected because the default is what a
single-host deployment and the stock ClickBench entry run, and the stock
default on a local store is the slower plan by 31% cold with no cost argument
in its favour: there is no request bill on loopback.

**Default to `latency-first` with raised concurrency on loopback.** It measured
the same cold time as `byte-minimal` at the derived concurrency, so it would add
a concurrency change, and the memory question ADR-1196 attaches to it, for no
measured gain.

**Ship a `local` cost profile and select it for loopback endpoints.** Rejected
for ADR-1196's own reason: a profile is prices, and a local store's advantage
is not a price. Pricing bytes above zero to obtain a routing would misstate the
bill in every stamp that names the profile.

**Probe the store at startup (a timed ranged read against a whole read) and pick
the faster plan.** Measures the right thing, but a startup probe is noisy on a
cold host, costs requests on every start, and makes the default depend on
machine state at boot. Locality is a stable property; bandwidth is not.

## Consequences

- A single-host deployment against a loopback store, and the stock ClickBench
  entry, read ranges instead of whole objects by default. Measured: cold down 31%
  and hot down 68% on the ClickBench reference machine.
- Deployments against any non-loopback endpoint, including every real S3
  deployment, see no change in behaviour or bill.
- The request count on a loopback store rises, as `byte-minimal` spends requests
  to save bytes. A store running on this host bills no requests, so there this
  is not a cost.
- The predicate reads the endpoint's authority, not what answers there. A
  loopback endpoint that fronts a tunnel or proxy to a remote, request-billed
  store also derives `byte-minimal`, and there the extra requests are billed.
  That deployment keeps the old behaviour with an explicit
  `--logs-fetch-policy cost-based`, which always wins.
- One more derived default is on the startup log, with its source, and the
  tests pin all three sources and that an explicit flag wins on loopback.
- Measured one query at a time only. Under ten concurrent queries the default
  cut throughput by about a factor of three on the reference machine, because
  its block-granular cache did not fit the corpus: see the concurrency
  amendment below.

## Amendment (2026-09-26, ADR-2023): concurrency

<!-- amendment-applies: sections="Consequences" pointer="concurrency amendment" -->

The Consequences above were measured one query at a time. The ClickBench
driver's concurrent phase (ten connections, one server) measured 0.123 queries
per second under this default against 0.400 under `cost-based` on the same
instance, and 0.320 with a fetch cache that held the corpus (#2014). ADR-2023
keeps this default and sizes the fetch cache for a loopback store instead, and
decouples the catalog cache from `--cache-max-bytes`.

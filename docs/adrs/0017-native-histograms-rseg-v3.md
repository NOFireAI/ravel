# ADR-0017: Native exponential histograms: span-based value model, RSEG v3 after RSEG v2 phase 6 closes

Status: Accepted (2026-07-27); amended by ADR-0027 (2026-07-28): v3 as
a readable version is retired, while the histogram value model and
HIST_PAGES sections decided here continue in v5. Implementation plan
and tickets:
docs/ingest-breadth-plan.md (track C). The byte-exact v3 grammar is a
deliverable of that track's first ticket, produced under the
format-change procedure; this ADR records the decision, the internal
value model, the sequencing relative to RSEG v2, and the exemplar
deferral.

## Context

OTLP `ExponentialHistogramDataPoint` and the Prometheus native
`Histogram` message (carried by Remote Write 1.0 and 2.0 alike; RW1
backported it, this is not a 2.0-only concern) are the same underlying
concept: base-2^(2^-scale) exponential bucketing with a zero bucket,
sparse per-side bucket populations, and int-or-float counts. Their wire
shapes differ:

- OTLP: `scale`, per side one `Buckets{offset, bucket_counts[]}` -- a
  single contiguous run of u64 counts starting at a bucket index, no
  internal gaps expressible; optional sum/min/max; u64 count;
  `zero_count`, `zero_threshold`; ns timestamps.
- Prometheus: `schema` (same meaning as scale; additionally -53 means
  custom bucket boundaries via `custom_values`, which has no OTLP
  equivalent), per side a genuinely sparse list of
  `BucketSpan{offset-from-previous-span, length}` with implicit-zero
  gaps between spans, counts as either sint64 deltas (integer
  histograms) or f64 absolutes (float histograms); `count`/`zero_count`
  as int-or-float oneofs; `reset_hint`; ms timestamps.

Unlike classic histograms (ADR-0016), this data cannot be exploded into
scalar series without destroying it: bucket indexes are
scale-relative (a series' bucket set re-buckets when the producer
rescales), populations are sparse and unbounded in index range, and
PromQL native-histogram semantics (rate, sum, histogram_quantile on
native histograms) are defined over the structured value, not over
per-bucket series. Storing native histograms means a new value type in
the segment format, which triggers the format-change procedure.

The timing problem: RSEG v2 (ADR-0014) is mid-rollout. Phases 1-3
(spec, writer, reader) are merged; phases 4-6 (fuzz hardening,
inspector, rollout/default flip = issue #34) are in flight or queued,
and the writer default is still v1.

## Alternatives

### For the representation

1. Lossy explosion into scalar series (per-bucket synthetic labels).
   Breaks under rescaling, discards sparse structure, and every gap
   between what was sent and what is stored violates "exact semantics
   by default". Rejected.
2. Contiguous internal model (OTLP's shape), padding Prometheus span
   gaps with explicit zeros. Lossless in counts but not in structure
   (a round-trip inflates wide-gap histograms with dense zero runs;
   memory and bytes grow with gap width, which the sender deliberately
   avoided paying), and it forgets `reset_hint` and the int/float
   distinction unless bolted on. Rejected.
3. Span-based internal model, the superset (chosen): per side, a list
   of (offset, run of counts) spans. OTLP's contiguous run is trivially
   one span; Prometheus spans map 1:1; conversion in either direction
   is lossless (emitting OTLP from a gapped histogram pads only at the
   protocol boundary, if ever needed, not in storage). Counts keep the
   int/float duality (u64 absolutes for integer histograms -- RW deltas
   are decoded and validated to non-negative absolutes at ingest -- f64
   for float histograms); scale/schema stored as sint32 with the -53
   custom-boundaries case carried via an optional `custom_values`
   array so the RW/OTLP asymmetry is representable, not rejected by
   the format; `reset_hint` carried (query-side counter-reset
   correctness needs it); optional sum; min/max not carried (no
   Prometheus-side source, no query surface; dropped with counters at
   ingest, symmetric with ADR-0016).

### For the storage sequencing (the mandatory decision)

1. Bundle a new histogram page/section kind into RSEG v2 before the
   phase-6 default flip (#34), on the "version bump already paid"
   argument that justified bundling VAL_RAW_F64 alignment (ADR-0014
   section 4 precedent). Rejected: the amortization argument does not
   transfer. Alignment was a <= 7-byte writer pad with zero new reader
   logic, bundled into a then-unimplemented spec. Histogram storage is
   a new page grammar, new codecs, a catalog change (SERIES_META must
   distinguish value kinds and address a third page container), a new
   `SeriesInput`/sample model through ravel-ingest, and a dedup-rule
   extension -- injected into a format whose writer, reader, golden
   fixtures, and in-flight fuzz phase (P4) are already merged. It would
   reopen phases 2-4, stall the encode-wall fix that motivated v2
   behind a design that has not been through its own measurement and
   review, and couple two unrelated rollouts into one deployment
   event. It also cannot be smuggled in "additively" without a version
   bump: a new page enc value or section kind that old v2 readers
   would meet changes the persistent contract, and the format-change
   skill requires explicit versioning for exactly this reason.
2. RSEG v3 on top of v2, after #34 closes (chosen). Costs a third
   permanent reader-dispatch branch under the no-compactor dual-reader
   rule (ADR-0014 Consequences: every version is readable forever, a
   permanent test-matrix row). Mitigations: v3 is specified as a
   strict superset of v2 (v2's catalog and page grammar unchanged;
   v3 adds a histogram page container, the catalog columns to address
   it, and the new page encoding), so the v3 reader is the v2 reader
   plus extensions rather than a parallel implementation, and the
   fuzz/golden matrix grows by the delta, not by a third full grammar.
   The v2 rollout machinery being built for #34 (version-valued config,
   readers-before-writers ordering, mixed-population tests) is reused
   verbatim.
3. Defer entirely; keep rejecting native histograms at admission.
   Honest but wrong on the merits: native histograms are the default
   direction of the Prometheus ecosystem, PROGRESS.md names them as
   Phase 2 scope, and rejection is sender-visible loss (ADR-0015
   documents that RW senders do not retry admission-rejected
   histograms). Deferral of the *storage* is however exactly what
   happens until v3 lands: ingest rejects native histograms at
   admission with typed, counted, stats-header-visible rejections --
   never a lossy explosion, never a silent drop.

## Decision

Span-based value model (representation alternative 3); RSEG v3
sequenced strictly after RSEG v2 phase 6 (issue #34) closes (sequencing
alternative 2), with admission-time rejection as the interim behavior.
Track C of docs/ingest-breadth-plan.md starts with the v3 spec-and-plan
document (the rseg-v2-plan.md equivalent: byte-exact grammar, checksum
coverage table, tri-version reader story, phased tickets); no v3
implementation ticket is dispatched before #34 is closed. Series
identity is untouched: a native histogram series is one series under
the existing ADR-0005 hash; only the sample value type is new.

Issue #34 needs one note added now (this is the only interaction with
the v2 rollout): the segment-version ingest config it threads through
must be version-valued (an integer/enum, not a write_v2 boolean), and
its rollout notes should state that a v3 following the same
readers-before-writers ordering is planned. Phase 6's scope and gates
are otherwise unchanged.

Exemplars (OTLP `Exemplar`; RW1/RW2 exemplars): accept-and-drop with
per-protocol counters, on both the OTLP and RW paths, as an explicit
non-goal of this phase. Prometheus itself holds exemplars in a bounded
in-memory buffer, not in TSDB blocks, so dropping matches upstream
durability expectations rather than undercutting them. Storage is
deliberately NOT half-designed here: per the format-change discipline,
an exemplar section enters the format only through a future ADR that
(a) specifies the exemplar query surface first (there is none today --
no /api/v1/query_exemplars, no trace store to join against), and (b)
decides whether to ride the v3 version bump. That ADR is a named
decision point in the v3 spec ticket, so the bundle-one-bump-beats-two
economics (ADR-0014 section 4) are weighed exactly when the version is
being paid, with a consumer spec in hand instead of speculative format
surface (the rseg-v2-plan section 3.8 rule).

## Consequences

- Until v3 lands: RW and OTLP native histogram points are rejected at
  admission, typed and counted, visible in partial-success responses
  and RW stats headers. Float samples in the same request are
  unaffected. This window is bounded by the v2 phase-6 flip plus
  track C's phases.
- Three permanent format versions once v3 ships. Accepted with eyes
  open; the compactor, when it exists, is the only path that ever
  retires old versions, and v3's spec must keep the v2 delta minimal
  precisely to bound the permanent test-matrix cost.
- The ingest pipeline's sample model (`NormalizedPoint`, shard-actor
  buffers, `SeriesInput`, cross-segment dedup) generalizes from scalar
  f64 to an enum of scalar and histogram values in track C; the dedup
  total order extends to histograms by structural comparison with
  f64 fields compared by bit pattern, preserving the exactness rule.
- PromQL native-histogram evaluation (rate/sum/histogram_quantile over
  structured values, scale down-conversion at query time exactly as
  Prometheus does it) is its own query-engine track, already
  anticipated by ADR-0007 ("native histogram arithmetic, Phase 2+");
  this ADR only guarantees the stored representation is lossless so
  that track has correct inputs.
- The RW/OTLP asymmetries are carried, not erased: `custom_values`
  (schema -53) is representable in the model and initially rejected at
  admission behind its own typed rejection until the v3 spec ticket
  decides its enablement; conversions between span-based storage and
  OTLP's contiguous shape are lossless by construction in the storage
  direction and zero-padded only at the protocol-output boundary if a
  future OTLP-shaped read surface ever needs them.

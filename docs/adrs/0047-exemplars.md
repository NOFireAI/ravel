# ADR-0047: Exemplars: an RSEG section, a capped admission, and a correlation surface

Status: Accepted (2026-08-02)

## Context

Ravel ingests OpenTelemetry metrics, logs, and traces, and it can correlate
none of them. Exemplars are the standard bridge from a metric sample to
the trace that produced it, and Ravel counts them and throws them away on
every ingest path:

- OTLP: `crates/ravel-otlp/src/normalize.rs:607` records an informational
  drop with a count.
- OTAP: `crates/ravel-otap/src/normalize.rs:1872` does the same, having
  decoded the exemplar batches at :444 to count them.
- Remote Write: `crates/ravel-remote-write/src/normalize.rs:200`
  accumulates `exemplars_dropped` per series.

`docs/segment-format.md` states the storage side plainly: for native
histograms, "`min`/`max` and exemplars are not carried." There is no
exemplar section in RSEG at any version.

So the drop is deliberate, counted, and visible, which is the right way to
have shipped without them. It is now the single largest gap between what
Ravel accepts and what it can answer: a user looking at a latency spike
cannot reach the traces behind it, and Ravel stores those traces.

Every ingest path already decodes exemplars far enough to count them. The
missing pieces are storage, compaction, and query.

## Decision

### 1. A new RSEG section, kind 10, `EXEMPLARS`

Section kinds 1 through 9 are taken (`crates/ravel-segment/src/format.rs:69-96`);
kind 2 is retired and its number is reserved forever. `EXEMPLARS` takes
kind 10, and the section-kind registry in `docs/segment-format.md` records
it as frozen.

The section is present only when at least one sample in the object carried
an exemplar. Absent is always legal, and readers already skip unknown
section kinds, so an object without it is simply an object with no
exemplars.

Layout, run-major to mirror the catalog, sorted by `(series_index,
ts_ns)` so a per-series probe is a binary search:

```
count: u32
per record:
  series_index:  varint     index into SERIES_IDS
  ts_delta:      ivarint    from footer.min_event_ts_ns
  value:         f64 LE     bit pattern; never compared with ==
  trace_id:      [u8;16]    all zero if absent
  span_id:       [u8;8]     all zero if absent
  attr_count:    varint
  attr pairs:    (name_ord varint, value_ord varint) into LABEL_DICT
```

Attribute keys and values reuse `LABEL_DICT`, so exemplar attributes cost
ordinals rather than strings, and the dictionary is already there.

### 2. Capped at admission, with a visible drop count

An exemplar path with no cap is a cardinality attack surface: exemplars
carry a trace id, which is high-entropy by construction, so an adversary
that attaches one to every sample multiplies object size by the exemplar
record width and defeats every dictionary in the format.

Admission keeps at most one exemplar per series per configurable window
(default 10 s), keeping the newest within each window. Exemplars beyond
the cap are dropped and counted, reusing the drop counters all three
ingest paths already have. This is a sampled signal by nature: Prometheus
and OTLP both treat exemplars as illustrative rather than complete, so
capping them is not an approximation of a value Ravel promised to keep
exactly.

The cap is enforced per shard actor, with no cross-shard coordination, the
same shape the cardinality limiter takes.

### 3. Compaction copies them verbatim

ADR-0018's correctness core is that L1 preserves every input sample with
its exact dedup priority tuple, which is what makes a snapshot containing
both an L1 part and its inputs query-equivalent to one containing only the
inputs. Exemplars must not weaken that.

They cannot, because an exemplar carries no dedup priority of its own: it
is attached to a `(series, timestamp)` and inherits whatever that sample's
priority is. Compaction therefore copies exemplar records verbatim with
their `series_index` remapped to the output's `SERIES_IDS` ordering, and
never merges, dedups, or re-samples them. Two inputs that both carry an
exemplar for the same `(series, ts)` both survive into the output, exactly
as their samples do.

### 4. Query surface

Exemplars ride along with the samples a query already fetched, and are
never a reason to fetch more. A query that wants them asks for them; a
query that does not pays one extra section read only when it asks.

Prometheus's `/api/v1/query_exemplars` is the compatible surface and is
what Grafana calls, so Ravel serves that shape rather than inventing one.
Its response carries the exemplar's labels, value, and timestamp, and
Ravel's trace id and span id ride in the exemplar labels under the
conventional `trace_id` and `span_id` keys, which is what Grafana's
exemplar-to-trace link reads.

### 5. Version bump, single supported version

RSEG's trailer version goes from 5 to 6. ADR-0027 fixed the pre-release
rule: one supported version at a time, earlier versions rejected with a
typed `UnsupportedVersion` rather than carried by a dual reader. That
still holds, so v6 retires v5 in the change that introduces it.

## Rejected alternatives

1. **Store exemplars in a sibling object rather than inside the segment.**
   Rejected: it doubles the object count and the commit protocol's work
   for a signal that is small and always read alongside its samples. It
   would also need its own GC reachability rule for no benefit.

2. **Attach exemplars to the histogram record inside `HIST_PAGES`.**
   Rejected: it would restrict exemplars to histogram series, when
   counters carry them too, and it would change a page grammar that
   compaction currently copies verbatim as an opaque blob.

3. **Keep every exemplar, uncapped.** Rejected per decision 2. The trace
   id is high-entropy, so an uncapped path is a cardinality amplifier
   whose worst case is set by the client rather than by Ravel.

4. **Sample exemplars at query time instead of at ingest.** Rejected: the
   bytes are already stored by then, so it saves nothing that matters and
   makes the result depend on when it was asked.

5. **Invent a Ravel-native exemplar endpoint.** Rejected: Grafana already
   speaks `/api/v1/query_exemplars`, and the point of exemplars is the
   click-through from a metric panel to a trace. A bespoke shape would
   deliver the storage and not the workflow.

6. **Carry `min`/`max` for native histograms in the same change.** They
   are dropped by the same normalize paths and are a genuine gap, but they
   are a different decision with a different storage shape. Bundling them
   would widen a frozen-format change for convenience. Named here so the
   omission is deliberate.

## Consequences

- One frozen-format change, following the format-change procedure in full:
  version bump to 6, `docs/segment-format.md` amended in the same change,
  checksum coverage reviewed for the new section, fuzz and property tests
  extended to the new grammar plus corrupt and truncated inputs, and
  `ravel-cli` taught to print exemplars.
- The new section is covered by the existing `Section.crc32c`, verified
  before any of its content is decoded, like every other section.
- Ingest gains a per-shard exemplar cap and reuses the three existing drop
  counters, so the number dropped stays visible rather than becoming
  invisible once storage exists.
- Compaction copies exemplars verbatim, preserving ADR-0018's overlap
  harmlessness unchanged.
- Object size grows only for tenants that send exemplars, bounded by the
  cap: roughly 40 bytes plus attributes per kept exemplar.
- Ravel gains metric-to-trace correlation, which is the whole point, and
  the spans it links to are already stored and, after epic #427, queryable.
- `min`/`max` for native histograms stay dropped. Named gap, not an
  oversight.

## Amendment (2026-08-03): duplicate sort keys are legal

Decision 1 gave the EXEMPLARS section a `(series_index, ts_ns)` sort order.
The implementation read that as strictly ascending and rejected an equal
key, and the writer collapsed a run of equal keys to its last record.

That contradicts decision 3. Compaction is a verbatim page copy that never
drops a record, and `crates/ravel-maintain/src/publish.rs` enforces it with
a record-count conservation gate that abandons the run on any mismatch. The
admission cap is scoped to one flush, so a retried write gives two L0
objects that each hold an exemplar for the same series at the same
timestamp. Under the strict rule the compactor could not encode both, which
is the case decision 3 names explicitly.

This amendment makes the format match decision 3:

- Records are ascending by `(series_index, ts_ns)`. Two records can share a
  key.
- Readers reject only a descending key, still as `ExemplarRecordsUnsorted`.
  The early-exit probe needs no more than that to stop safely, and it
  already returns every record at the target key.
- The writer sorts with a stable sort and collapses nothing. Records that
  share a key keep the caller's order, so the encoded bytes stay a function
  of the input order alone.

This needs no version bump. The reader now accepts a superset of what it
accepted before, so every existing v6 object stays readable, and ADR-0027
keeps exactly one supported RSEG version before release.

The rejected alternative was to dedup at compaction, keeping the first
exemplar in the inputs' canonical order. It would make exemplars the only
non-verbatim signal in the compactor, under a gate whose stated premise is
that nothing is ever dropped.

Note what this format does NOT promise. Two records sharing
`(series_index, ts_ns)` can differ in any other field: trace id, span id,
value, or attributes. The writer preserves them verbatim and checks nothing
beyond the sort order. An earlier revision of this amendment said in passing
that such records "differ only in trace id"; that was an observation about the
duplicate-delivery case, not a guarantee, and issue #475 read it as one and
built a query-time dedup key on it. Any reader that collapses records must key
on every field it would otherwise lose.

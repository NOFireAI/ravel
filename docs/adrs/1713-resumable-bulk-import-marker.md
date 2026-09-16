# ADR-1713: resumable bulk import marker

Status: Accepted (2026-09-16). Issue #1713. Amends ADR-0089 (its "no
deduplication" consequence) and adds one key shape under the `idem/` prefix
of the object key layout (docs/catalog-and-mvcc.md); migration class C,
CAS-mutable, under ADR-0066 decision 4 and its R1 amendment.

## Context

`ravel-cli load` re-ingests a whole file after a failure. The command's own
help says so: "There is NO resumability or deduplication: re-running after a
failure re-ingests the whole file from the start"
(`services/ravel-cli/src/main.rs:349-352`). ADR-0089's last consequence
records the same limit, and a logs re-ingest is user-visible duplication
because logs and spans have no query-time dedup
(`docs/consistency-model.md:124`).

The loader already knows exactly which rows are durable. Every batch is one
`WriteMode::Strict` write (`services/ravel-cli/src/load.rs:1358-1381`), and a
Strict ack means every involved shard's data object and commit record are
stored (`crates/ravel-ingest/src/router.rs:27-33`). Writes are pipelined to
`DEFAULT_PIPELINE_DEPTH = 4` (`load.rs:122`) but resolved strictly
oldest-first, so the token list grows in submission order
(`load.rs:1266-1271`). After a write failure the loader waits for every
later write and records what it committed, because a handed-off write
cannot be cancelled (`load.rs:1659-1672`). What it lacks is a durable place
to put that knowledge that the next run reads.

The existing idempotency marker cannot carry it. It is keyed per request as
`t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`
(`crates/ravel-ingest/src/idempotency.rs:9-10`), pinned to one admission
hour (`idempotency.rs:160-164`), written once with `CreateIfAbsent`
(`idempotency.rs:312`), and swept by the hour in its name
(`crates/ravel-maintain/src/sweep.rs:2160-2162`). A bulk file's commits
spread over many hours and a resume lands in a later hour still, so an
hour-keyed, write-once marker fits neither the lifetime nor the update
pattern.

Two facts shape the offset. Rows are not read as one cursor: the loader
opens K stride cursors, each owning a contiguous partition of row groups,
with K defaulting to `min(shards, row_group_count)`
(`load.rs:2076-2082, 2087-2104`), and a batch holds up to K spans, one per
cursor (`load.rs:1883-1921`). And the store offers a compare-and-swap put:
`PutMode::CasVersion(Version)` fails with `StoreError::PreconditionFailed`
when the version moved (`crates/ravel-object-store/src/lib.rs:58-66`), and
it is a mandatory backend capability (`docs/object-store-contract.md:213-225`).

The key layout is a frozen contract. The `idem/` prefix is additive and no
resolve or read path lists it; only `sweep_idempotency_markers` does, and it
skips a key that does not parse as `<keyhash32>.<ingest_hour>.idm`, with a
warning per key (`docs/catalog-and-mvcc.md:354-371`,
`sweep.rs:2173-2181`). ADR-0066 decision 4 files idempotency markers as
class C, and its R1 amendment says a record rewritten whole under CAS must
carry a `format_version` that bumps on every additive change, readers before
writers (`docs/adrs/0066-format-migration-machinery.md:184-238`).

## Decision

1. **One load marker per (tenant, signal, file), keyed by the file digest.**
   The key is `t/<tenant_hash>/<signal>/idem/<filedigest32>.ldm`, where
   `filedigest32` is the first 16 bytes, hex, of a streaming
   `blake3` keyed hash of the file bytes under the domain string
   `ravel-load-v1`. `blake3` and `hex` are already in ravel-cli
   (`services/ravel-cli/Cargo.toml:47, 63`). The suffix `.ldm` is new; the
   `.idm` shape and its sweep rule are untouched. This is the class C key
   layout addition: a new suffix under an existing additive prefix, no
   change to any existing key's meaning.

2. **The payload is a versioned, checksummed record.** Layout: magic `RLDM`,
   `format_version: u16 = 1`, crc32c over magic, version and payload, then
   the payload: file digest (32 bytes), file size, mapping digest (blake3 of
   the mapping TOML bytes), signal, `read_cursors: u16`, `batch_rows: u32`,
   `complete: bool`, `updated_at_ns`, a random `loader_id`, and
   `read_cursors` entries of `(row_group_start, row_group_end,
   rows_committed)`. Readers accept exactly `{1}`; a writer that reads a
   newer version refuses to resume rather than rewrite it (the R1 rule). The
   PUT carries `UploadChecksum::Crc32c` as the `.idm` write does.

3. **The offset is the acked contiguous prefix, per cursor, advanced by CAS
   after every acked batch.** The loader creates the marker with
   `CreateIfAbsent` before its first write. When the oldest in-flight write
   resolves with a Strict receipt, the loader adds that batch's per-cursor
   span lengths to `rows_committed` and rewrites the marker with
   `CasVersion(previous version)`. Because writes resolve oldest-first, the
   marker never names a row whose batch has not acked, and every batch it
   names is durable. `PreconditionFailed` means another loader is advancing
   the same file for the same tenant: the run stops with a typed error and
   the harvested tokens, and writes nothing more.

4. **A rerun resumes or refuses; it never silently starts over.** On
   `AlreadyExists` the loader reads the marker and checks the file digest,
   file size and mapping digest. A mismatch is an error. `complete = true`
   prints "already loaded" and exits 0. Otherwise the run reopens the same
   K partitions (the marker's `read_cursors` and `batch_rows` win over the
   flags), skips `rows_committed` rows in each partition, takes the marker
   over by one CAS that replaces `loader_id`, and continues. `--force`
   deletes the marker and loads from the start; it is the only way to
   reload a completed file.

5. **A write failure records an exact boundary; a crash may not.** After a
   failed write the loader drains the window as today and writes the
   marker with the exact acked prefix, so the rerun lands each remaining
   row once. After a process crash the marker can trail the store by up to
   `pipeline_depth` acked batches, and the rerun re-ingests those rows. The
   CLI prints that bound on resume.

6. **The sweep learns the `.ldm` shape and never deletes it.** Load markers
   are bounded by the number of distinct files loaded, one small object
   each. `ravel-cli idem inspect` decodes them, and `--force` or an explicit
   `ravel-cli idem forget-load` removes one.

7. **The loader's flush boundary aligns with the marker by construction.**
   With `--target-bytes 1` (the default, `load.rs:82`) one batch is one
   object per shard and one ack. With a larger target several batches may
   share one flush, but each batch's ack still arrives only after that
   flush commits, so the acked prefix stays exact; the marker is updated
   per ack, never per row.

```mermaid
sequenceDiagram
    participant L as ravel-cli load
    participant S as object store
    L->>S: PUT idem/<digest>.ldm CreateIfAbsent
    alt AlreadyExists
        S-->>L: marker (version v0)
        L->>L: check digest, size, mapping; skip rows_committed per cursor
        L->>S: PUT marker CasVersion(v0) with new loader_id
    end
    loop each batch, pipelined
        L->>S: data PUTs + commit PUTs (Strict write)
        S-->>L: ack, oldest first
        L->>S: PUT marker CasVersion(v_n): rows_committed += batch spans
        S-->>L: v_n+1 (or PreconditionFailed: another loader, stop)
    end
    L->>S: PUT marker CasVersion: complete = true
```

## Rejected alternatives

- **Write the offset once at the end.** A crash or a kill before the end
  leaves no marker, which is today's behaviour. The failure the marker
  exists for is the one that prevents the final write.
- **Reuse the `.idm` marker with the file digest as the client key.** Its
  key carries an ingest hour, its sweep deletes it by that hour after 24 h,
  and it is write-once; each of those would need a special case in the
  request path that has nothing to do with bulk load.
- **A `--skip-rows N` flag alone.** Useful, and it lands separately as its
  own small change, but the number the operator types comes from a
  terminal log, and with K cursors one number does not describe the
  loader's position.
- **One global row offset instead of per-cursor offsets.** The K cursors
  advance independently through disjoint row-group ranges; a single row
  number would either force `--read-cursors 1` (a large throughput cost on
  wide files) or describe a position no cursor has.
- **Overwrite the marker without CAS.** Two concurrent loads of one file
  would each believe they own the offset and both ingest every row. CAS
  costs one conditional PUT per batch of 10,000 rows, beside the data and
  commit PUTs that batch already made.
- **Store per-batch commit tokens in the marker and dedupe on resume.**
  Tokens identify commits, not rows; deduplicating rows already in the
  store would need a read path logs do not have.

## Consequences

- ADR-0089's consequence "re-running the loader re-ingests the whole file
  with no deduplication" no longer holds for a rerun of the same file,
  tenant and mapping; the guide, the command help and
  `docs/consistency-model.md`'s load paragraph state the new behaviour and
  the crash-gap bound from decision 5.
- What changes for an operator: a failed load is rerun with the same
  command; a completed file cannot be loaded twice without `--force`; a
  changed mapping on the same file is refused rather than applied to the
  remaining rows; `idem inspect` shows how far a load got.
- Format-change procedure: class C, CAS-mutable. The dual-reader question
  is answered by exact version gating; no older build reads the key, and
  the sweep is the one lister, updated in the same change. Checksum
  coverage is the crc32c over every byte the reader interprets plus the
  upload checksum. The codec gets round-trip and corrupt-input property
  tests with typed errors, and the seed file is checked in.
- The marker holds the mapping digest, not the mapping; an operator who
  edits the mapping mid-file has to `--force` and accept duplicates.
- Follow-up tasks:
  1. `--skip-rows` as its own change, independent of the marker.
  2. Marker codec, key builder and CAS write path in `ravel-ingest`, with
     property tests.
  3. Loader integration: create, per-ack advance, resume, `--force`, the
     resume message, and a `FaultStore` test that fails a PUT at row K,
     reruns, and asserts the file's row count through the read path.
  4. Sweep recognition of `.ldm` and the `idem inspect` and `forget-load`
     subcommands.
  5. Doc updates: docs/catalog-and-mvcc.md key table, docs/guides/ingest.md,
     docs/consistency-model.md, and the generated CLI reference.

#!/usr/bin/env python3
"""Predict the scratch bytes of a write-once bucketed spill retry (ADR-4143 decision 2).

For ClickBench statements 19, 34 and 35, on each M8 box (c6a.large, c6a.xlarge,
c6a.2xlarge) and each `bits` value from 4 to 8, this computes the bytes the
bucketed retry of ADR-4143 decision 3 would write to its scratch files when the
partial aggregate gives up and every corpus row is routed, with and without the
per-chunk combining of decision 4, and compares them with the smallest
sort-merge `spill_bytes_written` M8 measured (ADR-4143 decision 2's table).

Every byte figure is compressed Arrow IPC stream bytes as written (lz4_frame,
one message per chunk, plus one schema message and end-of-stream marker per
scratch file), with no retries counted. See README.md beside this file.

Usage (Python 3 with duckdb, pyarrow and numpy; run from the repository root):
    predict.py prep      --parquet PATH --work DIR   # once, about 5 minutes
    predict.py selftest  --work DIR                  # view layout round trip
    predict.py run       --work DIR [--statement 19|34] [--box B] [--bits N]
    predict.py report    --work DIR                  # every line and the verdict

`run` appends one JSON record per (statement, box, bits) to DIR/results.jsonl
and skips a combination already there, so it can be split across calls.
`report` exits 1 when a figure is missing, duplicated or outside its band.
"""

import argparse
import json
import math
import os
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

CORPUS_ROWS = 99_997_497

# ADR-3565 amendment 2 item 4, the `S = floor((L - H) / C)` row of its table.
SHARE_BYTES = {
    "c6a.large": 32_585_819,
    "c6a.xlarge": 76_008_368,
    "c6a.2xlarge": 153_493_747,
}
BOXES = list(SHARE_BYTES)

# ADR-4143 decision 2: the smallest M8 sort-merge `spill_bytes_written` per
# statement and box, in GB of 10^9 bytes.
THRESHOLD_GB = {
    "19": {"c6a.large": 4.65, "c6a.xlarge": 3.58, "c6a.2xlarge": 3.29},
    "34": {"c6a.large": 8.60, "c6a.xlarge": 6.83, "c6a.2xlarge": 6.04},
    "35": {"c6a.large": 8.60, "c6a.xlarge": 6.83, "c6a.2xlarge": 6.04},
}

# The retry's final partition count (ADR-3565 amendment 2 item 4: `P` is 16).
PARTITIONS = 16
BITS = [4, 5, 6, 7, 8]
# `routed_chunk_rows` stays at most 8,192 (ADR-4143 decision 1).
MAX_CHUNK_ROWS = 8192
# A Utf8View value up to 12 bytes is inlined in its 16-byte view; a longer one
# is copied into the chunk's data block (ADR-4143 decision 3, "Copies, not views").
INLINE_LIMIT = 12

# The routed columns of each statement's final aggregate, keys then the COUNT
# state, as (name, Arrow type). Statement 35 groups by `1, "URL"`; DataFusion
# 55.2's EliminateGroupByConstant removes the literal key, so its final
# aggregate routes exactly statement 34's columns and shares its records.
STATEMENTS = {
    "19": [("UserID", pa.int64()), ("m", pa.int32()), ("SearchPhrase", pa.string_view()),
           ("count", pa.int64())],
    "34": [("URL", pa.string_view()), ("count", pa.int64())],
}
ALIASES = {"35": "34"}
REPORTED = ["19", "34", "35"]

# The table a bucket may hold under decision 1 lies in (0.375 S, 0.75 S]: at
# most three quarters of the share, and more than half that, or `bits - 1`
# would have satisfied the rule. The chunk budget is the rest of the share
# over `2^bits + 1` chunks, so it lies in [0.25 S, 0.625 S) / (2^bits + 1).
BUDGET_SHARE = {"low": 0.25, "high": 0.625}


def chunk_budget(box, bits, end):
    return int(BUDGET_SHARE[end] * SHARE_BYTES[box]) // (2**bits + 1)


def schema_of(statement):
    return pa.schema([pa.field(name, kind, nullable=True) for name, kind in STATEMENTS[statement]])


def ipc_options():
    return ipc.IpcWriteOptions(compression="lz4")


def file_framing_bytes(statement):
    """The bytes one scratch file adds beyond its messages: schema and end of stream."""
    sink = pa.MockOutputStream()
    with ipc.new_stream(sink, schema_of(statement), options=ipc_options()):
        pass
    return sink.size()


# --------------------------------------------------------------------------
# prep: one DuckDB pass over the corpus into per-row integer arrays
# --------------------------------------------------------------------------


def prep(parquet, work):
    import duckdb

    os.makedirs(work, exist_ok=True)
    con = duckdb.connect(os.path.join(work, "prep.duckdb"))
    con.execute(f"SET temp_directory='{os.path.join(work, 'duckdb-tmp')}'")
    con.execute("SET memory_limit='20GB'")
    src = f"read_parquet('{parquet}', file_row_number=true)"

    nulls = con.execute(
        f"SELECT count(*), count(UserID), count(EventTime), count(SearchPhrase), count(URL) FROM {src}"
    ).fetchone()
    print(f"prep: rows={nulls[0]} non-null UserID={nulls[1]} EventTime={nulls[2]} "
          f"SearchPhrase={nulls[3]} URL={nulls[4]}")
    if len(set(nulls)) != 1 or nulls[0] != CORPUS_ROWS:
        sys.exit(f"prep: expected {CORPUS_ROWS} rows with no nulls in the key columns")

    # Dictionaries of the two string keys. Ids are dense and arbitrary; the
    # bucket hash is DuckDB's 64-bit hash of the string itself.
    for name in ("URL", "SearchPhrase"):
        con.execute(f"""
            CREATE OR REPLACE TABLE dict_{name} AS
            SELECT (row_number() OVER () - 1)::UINTEGER AS id, v, hash(v) AS h
            FROM (SELECT DISTINCT {name} AS v FROM {src})""")
        tbl = con.sql(f"SELECT id, v FROM dict_{name} ORDER BY id").to_arrow_table()
        values = tbl.column("v").cast(pa.large_string()).combine_chunks()
        with pa.OSFile(os.path.join(work, f"dict_{name}.arrow"), "wb") as sink:
            with ipc.new_file(sink, pa.schema([("v", pa.large_string())])) as writer:
                writer.write_batch(pa.record_batch([values], names=["v"]))
        hashes = con.execute(f"SELECT h FROM dict_{name} ORDER BY id").fetchnumpy()["h"]
        np.save(os.path.join(work, f"hash_{name}.npy"), hashes.astype(np.uint64))
        print(f"prep: distinct {name}={len(values)}")

    # Per row, in corpus order: UserID, the minute, both string ids, and the
    # dense id and hash of statement 19's key. ts is EventTime in Unix seconds;
    # date_part('minute', ts) is the minute of the UTC timestamp.
    con.execute(f"""
        CREATE OR REPLACE TABLE r AS
        SELECT s.file_row_number AS rn, s.UserID AS user_id,
               ((s.EventTime // 60) % 60)::INTEGER AS m,
               p.id AS phrase_id, u.id AS url_id
        FROM {src} s
        JOIN dict_SearchPhrase p ON s.SearchPhrase = p.v
        JOIN dict_URL u ON s.URL = u.v""")
    con.execute("""
        CREATE OR REPLACE TABLE k19 AS
        SELECT (row_number() OVER () - 1)::UINTEGER AS id, user_id, m, phrase_id,
               hash(user_id, m, phrase_id) AS h
        FROM (SELECT DISTINCT user_id, m, phrase_id FROM r)""")
    cols = con.execute("""
        SELECT r.rn, r.user_id, r.m, r.phrase_id, r.url_id, k.id AS k19, k.h AS h19
        FROM r JOIN k19 k USING (user_id, m, phrase_id)
        ORDER BY r.rn""").fetchnumpy()
    rn = np.asarray(cols["rn"])
    if len(rn) != CORPUS_ROWS or not np.array_equal(rn, np.arange(CORPUS_ROWS)):
        sys.exit("prep: the joined rows are not exactly the corpus rows in file order")
    np.save(os.path.join(work, "user_id.npy"), np.asarray(cols["user_id"], dtype=np.int64))
    np.save(os.path.join(work, "m.npy"), np.asarray(cols["m"], dtype=np.int32))
    np.save(os.path.join(work, "phrase_id.npy"), np.asarray(cols["phrase_id"], dtype=np.uint32))
    np.save(os.path.join(work, "url_id.npy"), np.asarray(cols["url_id"], dtype=np.uint32))
    np.save(os.path.join(work, "k19.npy"), np.asarray(cols["k19"], dtype=np.uint32))
    np.save(os.path.join(work, "h19.npy"), np.asarray(cols["h19"], dtype=np.uint64))
    print(f"prep: distinct statement 19 keys={con.execute('SELECT count(*) FROM k19').fetchone()[0]}")
    con.close()
    os.remove(os.path.join(work, "prep.duckdb"))


# --------------------------------------------------------------------------
# Loading a statement's per-row view
# --------------------------------------------------------------------------


def load_dict(work, name):
    with pa.memory_map(os.path.join(work, f"dict_{name}.arrow")) as source:
        return ipc.open_file(source).read_all().column("v").combine_chunks()


def string_lengths(values):
    offsets = np.frombuffer(values.buffers()[1], dtype=np.int64)[values.offset:values.offset + len(values) + 1]
    return np.diff(offsets)


class Rows:
    """One statement's routed rows in corpus order, as integer arrays.

    `key` is a dense key id, `hash` the bucket hash of the key, and `charge8`
    the bytes a row adds to an open chunk, in eighths of a byte: the fixed
    widths (16 B per view), the long string bytes copied into the data block,
    and one validity bit per column.
    """

    def __init__(self, work, statement):
        self.statement = statement
        self.strings = {}
        if statement == "34":
            urls = load_dict(work, "URL")
            self.strings["URL"] = (urls, np.load(os.path.join(work, "url_id.npy")))
            self.key = self.strings["URL"][1]
            self.hash = np.load(os.path.join(work, "hash_URL.npy"))[self.key]
            self.distinct = len(urls)
            self.ints = {}
        else:
            phrases = load_dict(work, "SearchPhrase")
            self.strings["SearchPhrase"] = (phrases, np.load(os.path.join(work, "phrase_id.npy")))
            self.key = np.load(os.path.join(work, "k19.npy"))
            self.hash = np.load(os.path.join(work, "h19.npy"))
            self.distinct = int(self.key.max()) + 1
            self.ints = {
                "UserID": np.load(os.path.join(work, "user_id.npy")),
                "m": np.load(os.path.join(work, "m.npy")),
            }
        fixed = 0
        for _, kind in STATEMENTS[statement]:
            fixed += 16 if kind == pa.string_view() else kind.bit_width // 8
        ncols = len(STATEMENTS[statement])
        self.charge8 = np.full(len(self.key), 8 * fixed + ncols, dtype=np.int64)
        for values, ids in self.strings.values():
            lengths = string_lengths(values)
            long_bytes = np.where(lengths > INLINE_LIMIT, lengths, 0)
            self.charge8 += 8 * long_bytes[ids]
        if len(self.key) != CORPUS_ROWS:
            sys.exit(f"statement {statement}: {len(self.key)} rows, expected {CORPUS_ROWS}")


def chunk_ids(charge8, budget):
    """Cut a row sequence into chunks of at most `budget` bytes or 8,192 rows.

    A row costs max(charge / budget, 1 / 8192) of a chunk; a chunk holds the
    rows whose cumulative cost before them falls in [k, k + 1). Taking the
    larger of the two costs seals a chunk no later than the greedy rule
    "seal when the rows or the bytes fill" would, so the chunks are never
    larger than decision 3's and the combining figure is never optimistic.
    """
    unit = 8 * budget * MAX_CHUNK_ROWS
    cost = np.maximum(charge8 * MAX_CHUNK_ROWS, 8 * budget)
    before = np.cumsum(cost) - cost
    return before // unit


# --------------------------------------------------------------------------
# Writing chunks as Arrow IPC with compact Utf8View columns
# --------------------------------------------------------------------------


def view_parts(values):
    """The 16-byte views of a compact large_string array, with long values'
    offsets into the concatenation of its long values, and that concatenation."""
    n = len(values)
    offsets = np.frombuffer(values.buffers()[1], dtype=np.int64)[values.offset:values.offset + n + 1]
    data = np.frombuffer(values.buffers()[2], dtype=np.uint8) if values.buffers()[2] else np.zeros(0, np.uint8)
    lengths = np.diff(offsets)
    starts = offsets[:-1]
    views = np.zeros((n, 16), dtype=np.uint8)
    views[:, 0:4] = lengths.astype("<u4").view(np.uint8).reshape(n, 4)
    pos = np.arange(INLINE_LIMIT)
    take = starts[:, None] + pos[None, :]
    present = pos[None, :] < lengths[:, None]
    gathered = data[np.minimum(take, max(len(data) - 1, 0))] if len(data) else np.zeros((n, INLINE_LIMIT), np.uint8)
    inline = np.where(present, gathered, 0).astype(np.uint8)
    long_mask = lengths > INLINE_LIMIT
    views[:, 4:16] = np.where(long_mask[:, None], 0, inline)
    views[long_mask, 4:8] = inline[long_mask, 0:4]
    long_lengths = np.where(long_mask, lengths, 0)
    long_offsets = np.cumsum(long_lengths) - long_lengths
    if long_mask.any():
        sel = np.repeat(starts[long_mask], lengths[long_mask]) + (
            np.arange(long_lengths.sum()) - np.repeat(long_offsets[long_mask], lengths[long_mask]))
        long_data = data[sel]
    else:
        long_data = np.zeros(0, np.uint8)
    return views, long_mask, long_offsets, long_lengths, long_data


def view_chunk(parts, start, end):
    """A Utf8View array of rows [start, end) whose one data buffer holds only
    that chunk's long values, as decision 3's chunk does."""
    views, long_mask, long_offsets, long_lengths, long_data = parts
    v = views[start:end].copy()
    lm = long_mask[start:end]
    base = long_offsets[start] if end > start else 0
    stop = long_offsets[end - 1] + long_lengths[end - 1]
    if lm.any():
        rel = (long_offsets[start:end][lm] - base).astype("<u4")
        v[lm, 8:12] = 0
        v[lm, 12:16] = rel.view(np.uint8).reshape(-1, 4)
    block = long_data[base:stop]
    buffers = [None, pa.py_buffer(v.tobytes()), pa.py_buffer(block.tobytes())]
    return pa.Array.from_buffers(pa.string_view(), end - start, buffers)


class ChunkWriter:
    """Writes chunks as messages of one IPC stream to a byte-counting sink."""

    def __init__(self, statement):
        self.statement = statement
        self.schema = schema_of(statement)
        self.sink = pa.MockOutputStream()
        self.writer = ipc.new_stream(self.sink, self.schema, options=ipc_options())
        self.header = self.sink.size()
        self.messages = 0
        self.rows = 0
        self.column_bytes = 0

    def write(self, columns):
        batch = pa.record_batch(columns, schema=self.schema)
        self.writer.write_batch(batch)
        self.messages += 1
        self.rows += batch.num_rows
        for column in columns:
            for buf in column.buffers():
                if buf is not None:
                    self.column_bytes += buf.size

    def message_bytes(self):
        """The bytes of the messages written, without the stream's schema and end marker."""
        self.writer.close()
        return self.sink.size() - self.header - 8


def write_rows(rows, order, counts, boundaries, writer, batch_rows=1_000_000):
    """Write rows `order` (corpus row indices) with COUNT states `counts`, one
    message per chunk; `boundaries` are the chunk start positions plus the end."""
    n = len(order)
    b = 0
    nb = len(boundaries) - 1
    while b < nb:
        lo = boundaries[b]
        e = b + 1
        while e < nb and boundaries[e + 1] - lo <= batch_rows:
            e += 1
        hi = boundaries[e]
        idx = order[lo:hi]
        string_parts = {}
        for name, (values, ids) in rows.strings.items():
            string_parts[name] = view_parts(values.take(pa.array(ids[idx])))
        ints = {name: arr[idx] for name, arr in rows.ints.items()}
        cnt = counts[lo:hi]
        for c in range(b, e):
            s, t = boundaries[c] - lo, boundaries[c + 1] - lo
            columns = []
            for name, kind in STATEMENTS[rows.statement]:
                if name == "count":
                    columns.append(pa.array(cnt[s:t], type=pa.int64()))
                elif kind == pa.string_view():
                    columns.append(view_chunk(string_parts[name], s, t))
                else:
                    columns.append(pa.array(ints[name][s:t], type=kind))
            writer.write(columns)
        b = e
    if writer.rows != n:
        sys.exit(f"wrote {writer.rows} rows, expected {n}")


def boundaries_of(chunk):
    """Start positions of each run of equal chunk ids, plus the end."""
    if len(chunk) == 0:
        return np.array([0], dtype=np.int64)
    change = np.flatnonzero(chunk[1:] != chunk[:-1]) + 1
    return np.concatenate([[0], change, [len(chunk)]]).astype(np.int64)


# --------------------------------------------------------------------------
# One (statement, box, bits) combination
# --------------------------------------------------------------------------


def measure(rows, box, bits, framing):
    n = len(rows.key)
    ones = np.ones(n, dtype=np.int64)
    buckets_total = PARTITIONS * 2**bits
    bucket = (rows.hash >> np.uint64(64 - int(math.log2(buckets_total)))).astype(np.int64)
    files = int(np.count_nonzero(np.bincount(bucket, minlength=buckets_total)))
    record = {"statement": rows.statement, "box": box, "bits": bits, "files": files,
              "distinct_keys": rows.distinct}

    # a. Uncombined, in corpus order, cut into chunks of the low budget.
    budget = chunk_budget(box, bits, "low")
    chunk = chunk_ids(rows.charge8, budget)
    bounds = boundaries_of(chunk)
    writer = ChunkWriter(rows.statement)
    write_rows(rows, np.arange(n), ones, bounds, writer)
    record["uncombined"] = {"rows": writer.rows, "messages": writer.messages,
                            "column_bytes": writer.column_bytes,
                            "bytes": writer.message_bytes() + files * framing,
                            "budget": budget}

    # Rows in bucket order, corpus order within a bucket: what each scratch file receives.
    order = np.argsort(bucket, kind="stable")
    sorted_bucket = bucket[order]
    sorted_charge = rows.charge8[order]
    starts = np.searchsorted(sorted_bucket, np.arange(buckets_total))
    record["combined"] = {}
    for end in ("low", "high"):
        budget = chunk_budget(box, bits, end)
        # Chunk ids restart in every bucket: subtract each bucket's cost before it.
        unit = 8 * budget * MAX_CHUNK_ROWS
        cost = np.maximum(sorted_charge * MAX_CHUNK_ROWS, 8 * budget)
        before = np.cumsum(cost) - cost
        bucket_base = before[np.minimum(starts, n - 1)][sorted_bucket]
        local = (before - bucket_base) // unit
        global_chunk = np.concatenate([[0], np.cumsum(
            (local[1:] != local[:-1]) | (sorted_bucket[1:] != sorted_bucket[:-1]))])
        if end == "low":
            routed = ChunkWriter(rows.statement)
            write_rows(rows, order, ones, boundaries_of(global_chunk), routed)
            record["routed_uncombined"] = {"rows": routed.rows, "messages": routed.messages,
                                           "column_bytes": routed.column_bytes,
                                           "bytes": routed.message_bytes() + files * framing,
                                           "budget": budget}
        composite = global_chunk.astype(np.int64) * np.int64(rows.distinct) + rows.key[order].astype(np.int64)
        _, first, counts = np.unique(composite, return_index=True, return_counts=True)
        emit = np.argsort(first, kind="stable")
        first = first[emit]
        counts = counts[emit].astype(np.int64)
        writer = ChunkWriter(rows.statement)
        write_rows(rows, order[first], counts, boundaries_of(global_chunk[first]), writer)
        record["combined"][end] = {"rows": writer.rows, "messages": writer.messages,
                                   "column_bytes": writer.column_bytes,
                                   "bytes": writer.message_bytes() + files * framing,
                                   "budget": budget, "count_sum": int(counts.sum())}
    return record


def results_path(work):
    return os.path.join(work, "results.jsonl")


def read_results(work):
    out = []
    if os.path.exists(results_path(work)):
        with open(results_path(work)) as f:
            out = [json.loads(line) for line in f if line.strip()]
    return out


def run(work, statements, boxes, bits_values):
    done = {(r["statement"], r["box"], r["bits"]) for r in read_results(work)}
    for statement in statements:
        todo = [(b, k) for b in boxes for k in bits_values if (statement, b, k) not in done]
        if not todo:
            continue
        rows = Rows(work, statement)
        framing = file_framing_bytes(statement)
        for box, bits in todo:
            t0 = time.time()
            record = measure(rows, box, bits, framing)
            record["seconds"] = round(time.time() - t0, 1)
            with open(results_path(work), "a") as f:
                f.write(json.dumps(record) + "\n")
            print(f"run: statement {statement} {box} bits {bits}: {record['seconds']} s", flush=True)


# --------------------------------------------------------------------------
# selftest: the hand-built views decode to the source strings
# --------------------------------------------------------------------------


def selftest(work):
    rows = Rows(work, "34")
    n = 200_000
    order = np.arange(n)
    chunk = chunk_ids(rows.charge8[:n], chunk_budget("c6a.large", 8, "low"))
    bounds = boundaries_of(chunk)
    sink = pa.BufferOutputStream()
    schema = schema_of("34")
    urls, ids = rows.strings["URL"]
    with ipc.new_stream(sink, schema, options=ipc_options()) as w:
        parts = view_parts(urls.take(pa.array(ids[order])))
        for c in range(len(bounds) - 1):
            s, t = bounds[c], bounds[c + 1]
            w.write_batch(pa.record_batch([view_chunk(parts, s, t),
                                           pa.array(np.ones(t - s, np.int64))], schema=schema))
    decoded = ipc.open_stream(sink.getvalue()).read_all()
    expected = urls.take(pa.array(ids[:n])).cast(pa.string())
    got = decoded.column("URL").combine_chunks().cast(pa.string())
    if not got.equals(expected):
        sys.exit("selftest: decoded URLs differ from the source")
    data_bytes = sum(b.size for batch in decoded.to_batches()
                     for b in batch.column(0).buffers()[2:] if b is not None)
    lengths = string_lengths(urls)[ids[:n]]
    want = int(np.where(lengths > INLINE_LIMIT, lengths, 0).sum())
    if data_bytes != want:
        sys.exit(f"selftest: data blocks hold {data_bytes} B, the long values are {want} B")
    rows_per_chunk = np.diff(bounds)
    print(f"selftest: {n} rows in {len(bounds) - 1} chunks round-trip; data blocks {data_bytes} B "
          f"= long value bytes {want} B; rows per chunk min {rows_per_chunk.min()} "
          f"max {rows_per_chunk.max()}")


# --------------------------------------------------------------------------
# report: one line per statement, box and bits, then the verdict
# --------------------------------------------------------------------------


def gb(x):
    return x / 1e9


def report(work):
    records = read_results(work)
    by_key = {}
    for r in records:
        key = (r["statement"], r["box"], r["bits"])
        if key in by_key:
            sys.exit(f"report: duplicate record for {key}")
        by_key[key] = r
    failures = []
    verdicts = {}
    print("statement box bits files distinct_keys budget_low_B budget_high_B "
          "uncombined_rows uncombined_GB routed_uncombined_GB "
          "combined_low_rows combined_low_GB combined_high_rows combined_high_GB threshold_GB")
    for statement in REPORTED:
        source = ALIASES.get(statement, statement)
        for box in BOXES:
            threshold = THRESHOLD_GB[statement][box]
            uncombined_ok = []
            combined_ok = []
            for bits in BITS:
                r = by_key.get((source, box, bits))
                if r is None:
                    failures.append(f"missing: statement {source} {box} bits {bits}")
                    continue
                u, ru = r["uncombined"], r["routed_uncombined"]
                lo, hi = r["combined"]["low"], r["combined"]["high"]
                for label, part in (("uncombined", u), ("routed_uncombined", ru)):
                    if part["rows"] != CORPUS_ROWS:
                        failures.append(f"{statement} {box} {bits}: {label} rows {part['rows']} != {CORPUS_ROWS}")
                for label, part in (("combined low", lo), ("combined high", hi)):
                    if not r["distinct_keys"] <= part["rows"] <= CORPUS_ROWS:
                        failures.append(f"{statement} {box} {bits}: {label} rows {part['rows']} outside "
                                        f"[{r['distinct_keys']}, {CORPUS_ROWS}]")
                    if part["count_sum"] != CORPUS_ROWS:
                        failures.append(f"{statement} {box} {bits}: {label} counts sum to {part['count_sum']}")
                if hi["rows"] > lo["rows"]:
                    failures.append(f"{statement} {box} {bits}: larger chunks combined fewer rows")
                for label, part in (("uncombined", u), ("routed_uncombined", ru),
                                    ("combined low", lo), ("combined high", hi)):
                    if not 0 < part["bytes"] <= part["column_bytes"] + 1024 * (part["messages"] + r["files"]):
                        failures.append(f"{statement} {box} {bits}: {label} bytes {part['bytes']} outside "
                                        f"(0, column bytes plus 1 KiB framing per message and file]")
                # The scratch file holds rows in bucket order; take the larger
                # of the two uncombined figures so the verdict never rests on
                # the order that compresses better.
                uncombined_bytes = max(u["bytes"], ru["bytes"])
                uncombined_ok.append(gb(uncombined_bytes) < threshold)
                combined_ok.append(gb(lo["bytes"]) < threshold)
                print(f"{statement} {box} {bits} {r['files']} {r['distinct_keys']} {lo['budget']} "
                      f"{hi['budget']} {u['rows']} {gb(u['bytes']):.3f} {gb(ru['bytes']):.3f} "
                      f"{lo['rows']} {gb(lo['bytes']):.3f} {hi['rows']} {gb(hi['bytes']):.3f} {threshold:.2f}")
            if len(combined_ok) != len(BITS):
                continue
            if all(uncombined_ok):
                verdict = "uncombined below threshold"
            elif all(combined_ok):
                verdict = "combined below threshold at every bits"
            elif any(combined_ok):
                verdict = "combined below threshold at some bits"
            else:
                verdict = "combined below threshold at no bits"
            verdicts[(statement, box)] = verdict
            print(f"verdict: statement {statement} {box}: {verdict}")
    for failure in failures:
        print(f"FAIL {failure}")
    if failures or len(verdicts) != len(REPORTED) * len(BOXES):
        return 1
    values = set(verdicts.values())
    if values == {"uncombined below threshold"}:
        decision = "build decision 3 alone"
    elif values <= {"uncombined below threshold", "combined below threshold at every bits"}:
        decision = "build decision 3 with decision 4"
    else:
        decision = "build neither decision 3 nor decision 4"
    print(f"decision 2: {decision}")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["prep", "selftest", "run", "report"])
    parser.add_argument("--work", required=True)
    parser.add_argument("--parquet")
    parser.add_argument("--statement", choices=list(STATEMENTS))
    parser.add_argument("--box", choices=BOXES)
    parser.add_argument("--bits", type=int, choices=BITS)
    args = parser.parse_args()
    if args.command == "prep":
        if not args.parquet:
            parser.error("prep needs --parquet")
        prep(args.parquet, args.work)
    elif args.command == "selftest":
        selftest(args.work)
    elif args.command == "run":
        run(args.work,
            [args.statement] if args.statement else list(STATEMENTS),
            [args.box] if args.box else BOXES,
            [args.bits] if args.bits else BITS)
    else:
        return report(args.work)
    return 0


if __name__ == "__main__":
    sys.exit(main())

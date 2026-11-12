#!/usr/bin/env python3
"""Predict the scratch bytes a write-once bucketed spill retry would write for
ClickBench statements 19, 34 and 35 (ADR-4143 decision 2, issue #4152).

For each statement, M8 box (c6a.large, c6a.xlarge, c6a.2xlarge) and `bits`
value 4 to 8, the script reports:

- uncombined bytes: every corpus row routed once, as the scratch files would
  hold it (the group keys as routed plus the COUNT state, Int64), written in
  corpus order as Arrow IPC stream messages with lz4_frame compression, one
  record batch per chunk of ADR-4143 decision 3's byte budget. Measured over
  the full corpus, in contiguous slices. The route writes each bucket's rows
  to its own chunks, so the same slice that measures combining is also
  written routed and uncombined; the ratio of its routed to its corpus-order
  bytes (the routing penalty) scales the full-corpus figure, and the
  prediction is the larger of the two.
- combined bytes (ADR-4143 decision 4): rows hashed to P x 2^bits buckets in
  corpus order, each bucket's rows cut into chunks of the same budget, and
  each chunk deduplicated by key (counts summed) before it is written. The
  row count is exact over the full corpus; the compressed bytes per combined
  row are measured on one contiguous slice and multiplied by that count.
- the threshold: the smallest M8 sort-merge `spill_bytes_written` of that
  statement on that box (ADR-4143 decision 2's table).

and a verdict per statement and box, then one final verdict per ADR-4143
decision 2. Every byte figure is compressed Arrow IPC bytes as the writer
emits them (no retries, no read-back), unless its name says `raw`.

Usage:
    python predict.py --corpus .dd-tools/corpus/hits.parquet \
        --state .dd-tools/bucket-bytes-state.json [--budget-seconds 540]
    python predict.py --corpus ... --state ... --rows 2000000 \
        --slice-rows 1000000 --sample-slice 1   # smoke run on a corpus prefix

A run with --budget-seconds starts no stage past the budget and exits 3;
the same command resumes from --state. Exit 0 prints the report.
"""

import argparse
import dataclasses
import json
import os
import sys
import time

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

CORPUS_ROWS = 99_997_497

# ADR-3565 amendment 2 item 4: the retry's final partition count.
P = 16
P_BITS = 4
BITS = (4, 5, 6, 7, 8)
# ADR-4143 decision 1: `routed_chunk_rows` is at most 8,192.
MAX_CHUNK_ROWS = 8192
# ADR-4143 decision 3: the variable-width floor is 1,024 rows plus 64 KiB of
# key bytes; strings up to 12 bytes are inlined in their view.
FLOOR_ROWS = 1024
FLOOR_DATA_BYTES = 64 * 1024
VIEW_INLINE = 12
VIEW_BYTES = 16

# ADR-3565 amendment 2 item 4's table: S = floor((L - H) / C) per box.
BOX_SHARE = {
    "c6a.large": 32_585_819,
    "c6a.xlarge": 76_008_368,
    "c6a.2xlarge": 153_493_747,
}

# ADR-4143 decision 2's table: the smallest M8 sort-merge spill_bytes_written.
THRESHOLD = {
    19: {"c6a.large": 4_649_336_784, "c6a.xlarge": 3_582_127_824, "c6a.2xlarge": 3_290_644_448},
    34: {"c6a.large": 8_603_687_856, "c6a.xlarge": 6_831_250_656, "c6a.2xlarge": 6_040_848_496},
    35: {"c6a.large": 8_598_806_208, "c6a.xlarge": 6_833_919_088, "c6a.2xlarge": 6_044_972_448},
}

# Ravel's partial aggregate probe (crates/ravel-sql/src/session.rs,
# SKIP_PARTIAL_AGGREGATION_PROBE_ROWS and _RATIO): a partition gives up when
# groups / rows > 0.1 after its first 8,192 rows.
PROBE_ROWS = 8192
PROBE_RATIO = 0.1

MIX = np.uint64(0x9E3779B97F4A7C15)

# date_part('minute', ts) returns Int32 (datafusion-functions 55.2.0,
# src/datetime/date_part.rs:185). EventTime is Unix seconds in UTC.
MINUTE = "CAST((EventTime // 60) % 60 AS INTEGER)"


@dataclasses.dataclass(frozen=True)
class Column:
    name: str
    expr: str
    arrow: pa.DataType
    width: int  # bytes per row in the chunk; 0 for the string column


@dataclasses.dataclass(frozen=True)
class Statement:
    number: int
    sql: str
    fixed_before: tuple  # fixed-width keys routed before the string key
    string: Column
    fixed_after: tuple  # fixed-width keys routed after the string key

    @property
    def key_exprs(self):
        return [c.expr for c in self.fixed_before] + [self.string.expr] + [
            c.expr for c in self.fixed_after]

    @property
    def fixed(self):
        return self.fixed_before + self.fixed_after

    @property
    def routed_columns(self):
        """Columns of a routed chunk in order: keys, then the COUNT state."""
        return self.fixed_before + (self.string,) + self.fixed_after + (COUNT,)

    def fixed_row_bytes(self):
        """Bytes per row a chunk is charged before any out-of-line string
        data: fixed widths, one view, and one validity bit per column (in
        eighths of a byte, returned as (bytes, validity_bits))."""
        widths = sum(c.width for c in self.fixed) + VIEW_BYTES + COUNT.width
        return widths, len(self.routed_columns)

    def buffers(self):
        """Buffers a chunk allocates: values and validity per fixed column,
        views, validity and one data block for the string column."""
        return 2 * (len(self.fixed) + 1) + 3

    def floor_charge(self):
        """b(c_min) of ADR-4143 decision 3: 1,024 rows of fixed columns,
        views and validity, each buffer rounded to 64 bytes, plus 64 KiB."""
        total = 0
        for c in self.fixed + (COUNT,):
            total += round64(FLOOR_ROWS * c.width) + round64(-(-FLOOR_ROWS // 8))
        total += round64(FLOOR_ROWS * VIEW_BYTES) + round64(-(-FLOOR_ROWS // 8))
        return total + FLOOR_DATA_BYTES


COUNT = Column("COUNT(*)[count]", "CAST(1 AS BIGINT)", pa.int64(), 8)

STATEMENTS = {
    19: Statement(
        19,
        'SELECT "UserID", date_part(\'minute\', ts) AS m, "SearchPhrase", COUNT(*) FROM logs '
        'GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10',
        (Column("UserID", "UserID", pa.int64(), 8), Column("m", MINUTE, pa.int32(), 4)),
        Column("SearchPhrase", "SearchPhrase", pa.string_view(), 0),
        (),
    ),
    34: Statement(
        34,
        'SELECT "URL", COUNT(*) AS c FROM logs GROUP BY "URL" ORDER BY c DESC LIMIT 10',
        (),
        Column("URL", "URL", pa.string_view(), 0),
        (),
    ),
    35: Statement(
        35,
        'SELECT 1, "URL", COUNT(*) AS c FROM logs GROUP BY 1, "URL" ORDER BY c DESC LIMIT 10',
        (Column("Int64(1)", "CAST(1 AS BIGINT)", pa.int64(), 8),),
        Column("URL", "URL", pa.string_view(), 0),
        (),
    ),
}


def round64(n):
    return (n + 63) // 64 * 64


def log(msg):
    print(f"# {time.strftime('%H:%M:%S')} {msg}", file=sys.stderr, flush=True)


def chunk_budget(share, bits):
    """ADR-4143 decision 3's byte budget per chunk at the largest table the
    bits rule allows, table(bits) = 0.75 x S, so S - table = floor(0.25 x S):
    the smallest budget decision 1 can hand that `bits`, for every T."""
    return (share // 4) // ((1 << bits) + 1)


def chunk_budget_max(share, bits):
    """The largest budget at that `bits`: table(bits) just above 0.375 x S,
    the least table for which `bits` - 1 would not do."""
    return (share - (3 * share + 7) // 8) // ((1 << bits) + 1)


def tune_limit(share, floor):
    """The largest T for which ADR-4143 decision 1's two `bits` conditions
    hold at some `bits`, with `table(b) = ceil(2T / (P x 2^b))`, and that
    `bits`."""
    best_t, best_b = 0, None
    for b in range(21):
        table_max = min(3 * share // 4, share - ((1 << b) + 1) * floor)
        if table_max <= 0:
            continue
        t = table_max * P * (1 << b) // 2
        if t > best_t:
            best_t, best_b = t, b
    return best_t, best_b


# --------------------------------------------------------------------------
# Reading the corpus
# --------------------------------------------------------------------------


def connect(threads, memory):
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{memory}'")
    con.execute("SET temp_directory='.dd-tools/duck-tmp'")
    con.execute("SET preserve_insertion_order=true")
    return con


def source(corpus):
    return f"read_parquet('{corpus}', file_row_number=true)"


def cost8_expr(st):
    """A row's chunk charge in eighths of a byte (validity is one bit)."""
    widths, validity_bits = st.fixed_row_bytes()
    s = st.string.expr
    return (f"({8 * widths + validity_bits} + 8 * CASE WHEN strlen({s}) > {VIEW_INLINE} "
            f"THEN strlen({s}) ELSE 0 END)::BIGINT")


def read_hash_cost(con, corpus, st, rows):
    """Per corpus row, in corpus order: the 64-bit key hash and the charge."""
    q = (f"SELECT file_row_number AS rn, hash({', '.join(st.key_exprs)}) AS h, "
         f"{cost8_expr(st)} AS cost8 FROM {source(corpus)} WHERE file_row_number < {rows}")
    t = con.sql(q).to_arrow_table()
    rn = t.column("rn").to_numpy()
    if len(rn) != rows or not np.array_equal(rn, np.arange(rows, dtype=rn.dtype)):
        raise SystemExit(f"statement {st.number}: rows not read in corpus order or count {len(rn)} != {rows}")
    h = t.column("h").to_numpy().astype(np.uint64, copy=False)
    cost8 = t.column("cost8").to_numpy().astype(np.int32)
    return h, cost8


def read_slice(con, corpus, st, start, end):
    """Key columns, key hash and charge of corpus rows [start, end)."""
    cols = [f"{c.expr} AS \"{c.name}\"" for c in st.fixed] + [f"{st.string.expr} AS s"]
    q = (f"SELECT file_row_number AS rn, {', '.join(cols)}, hash({', '.join(st.key_exprs)}) AS h, "
         f"{cost8_expr(st)} AS cost8 FROM {source(corpus)} "
         f"WHERE file_row_number >= {start} AND file_row_number < {end}")
    t = con.sql(q).to_arrow_table()
    rn = t.column("rn").to_numpy()
    if not np.array_equal(rn, np.arange(start, end, dtype=rn.dtype)):
        raise SystemExit(f"statement {st.number}: slice {start}..{end} not read in corpus order")
    fixed = {c.name: t.column(c.name).to_numpy().astype(c.arrow.to_pandas_dtype(), copy=False)
             for c in st.fixed}
    strings = t.column("s").cast(pa.large_string()).combine_chunks()
    h = t.column("h").to_numpy().astype(np.uint64, copy=False)
    cost8 = t.column("cost8").to_numpy().astype(np.int32)
    return fixed, strings, h, cost8


# --------------------------------------------------------------------------
# Chunking, routing and combining
# --------------------------------------------------------------------------


def greedy_chunks(cost8, group_starts, group_ends, budget8, cap):
    """Start index of every chunk when each group's rows (contiguous, in
    order) are cut greedily: a chunk takes rows while their charge fits the
    budget and it holds fewer than `cap` rows; a row larger than the budget is
    a chunk of its own (ADR-4143 decision 3)."""
    cum = np.zeros(len(cost8) + 1, dtype=np.int64)
    np.cumsum(cost8, out=cum[1:], dtype=np.int64)
    keep = group_starts < group_ends
    pos, ends = group_starts[keep], group_ends[keep]
    starts = []
    while pos.size:
        starts.append(pos)
        fit = np.searchsorted(cum, cum[pos] + budget8, side="right") - 1
        nxt = np.maximum(np.minimum(np.minimum(fit, pos + cap), ends), pos + 1)
        more = nxt < ends
        pos, ends = nxt[more], ends[more]
    return np.sort(np.concatenate(starts)) if starts else np.zeros(0, dtype=np.int64)


def chunk_ids(n, starts):
    flags = np.zeros(n, dtype=np.int32)
    flags[starts] = 1
    ids = np.cumsum(flags, dtype=np.int32)
    ids -= 1
    return ids


def route(h, bits):
    """Rows in bucket order (stable, so corpus order inside a bucket) and each
    bucket's [start, end). The bucket is the hash's top P_BITS + bits bits:
    the exchange's partition and the router's bucket together, which the
    exec draws from two independent hashes (bucketed_final_aggregate.rs:888)."""
    nb = 1 << (P_BITS + bits)
    bucket = (h >> np.uint64(64 - P_BITS - bits)).astype(np.uint16)
    order = np.argsort(bucket, kind="stable")
    counts = np.bincount(bucket, minlength=nb)
    ends = np.cumsum(counts)
    return order, ends - counts, ends


def combine_count(h_sorted, cid):
    """Rows left once each chunk is deduplicated by key."""
    key = h_sorted ^ (cid.astype(np.uint64) * MIX)
    key.sort()
    return int(np.count_nonzero(key[1:] != key[:-1])) + 1 if key.size else 0


def combine_rows(h_sorted, cid):
    """Indices (into the sorted rows) of each chunk's first row per key, in
    order, and the number of rows each one stands for (its summed COUNT)."""
    key = h_sorted ^ (cid.astype(np.uint64) * MIX)
    o = np.argsort(key, kind="stable")
    ks = key[o]
    first = np.ones(len(ks), dtype=bool)
    first[1:] = ks[1:] != ks[:-1]
    group_start = np.flatnonzero(first)
    counts = np.diff(np.append(group_start, len(ks)))
    idx = o[group_start]
    sort = np.argsort(idx, kind="stable")
    return idx[sort], counts[sort].astype(np.int64)


# --------------------------------------------------------------------------
# Writing chunks as the scratch file holds them
# --------------------------------------------------------------------------


def view_parts(strings):
    """Utf8View views for `strings` (a LargeStringArray) and the
    concatenation of its out-of-line values, as each chunk copies them into
    its own data block (ADR-4143 decision 3, "Copies, not views")."""
    n = len(strings)
    offsets = np.frombuffer(strings.buffers()[1], dtype=np.int64)[strings.offset:strings.offset + n + 1]
    data = np.frombuffer(strings.buffers()[2], dtype=np.uint8)
    lens = np.diff(offsets)
    starts = offsets[:-1]
    is_long = lens > VIEW_INLINE
    views = np.zeros((n, VIEW_BYTES), dtype=np.uint8)
    views[:, 0:4] = lens.astype("<u4").view(np.uint8).reshape(n, 4)
    for j in range(VIEW_INLINE):
        sel = np.flatnonzero(np.where(is_long, j < 4, lens > j))
        views[sel, 4 + j] = data[starts[sel] + j]
    long_values = pc.filter(strings, pa.array(is_long))
    lo = np.frombuffer(long_values.buffers()[1], dtype=np.int64)[long_values.offset:long_values.offset + len(long_values) + 1]
    long_data = np.frombuffer(long_values.buffers()[2], dtype=np.uint8)[lo[0]:lo[-1]]
    cumlong = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(np.where(is_long, lens, 0), out=cumlong[1:])
    return views, long_data, cumlong, is_long


def schema_of(st):
    return pa.schema([pa.field(c.name, c.arrow, nullable=False) for c in st.routed_columns])


def ipc_options():
    return ipc.IpcWriteOptions(compression="lz4", use_threads=True)


def empty_stream_bytes(st):
    sink = pa.MockOutputStream()
    with ipc.new_stream(sink, schema_of(st), options=ipc_options()):
        pass
    return sink.size()


def write_chunks(st, fixed, strings, counts, starts):
    """Write rows already in chunk order as one IPC stream, one record batch
    per chunk. Returns (compressed bytes written, raw buffer bytes, rows
    written)."""
    n = len(strings)
    views, long_data, cumlong, is_long = view_parts(strings)
    cid = chunk_ids(n, starts)
    rel = cumlong[:-1] - cumlong[starts][cid]
    views[is_long, 8:12] = 0
    views[is_long, 12:16] = rel[is_long].astype("<u4").view(np.uint8).reshape(-1, 4)
    schema = schema_of(st)
    bounds = np.append(starts, n)
    sink = pa.MockOutputStream()
    raw = 0
    written = 0
    with ipc.new_stream(sink, schema, options=ipc_options()) as writer:
        for k in range(len(starts)):
            s, e = int(bounds[k]), int(bounds[k + 1])
            d0, d1 = int(cumlong[s]), int(cumlong[e])
            buffers = [None, pa.py_buffer(views[s:e])]
            if d1 > d0:
                buffers.append(pa.py_buffer(long_data[d0:d1]))
            sv = pa.Array.from_buffers(pa.string_view(), e - s, buffers, null_count=0)
            arrays = ([pa.array(fixed[c.name][s:e], type=c.arrow) for c in st.fixed_before] + [sv]
                      + [pa.array(fixed[c.name][s:e], type=c.arrow) for c in st.fixed_after]
                      + [pa.array(counts[s:e], type=pa.int64())])
            raw += (e - s) * (sum(c.width for c in st.fixed) + VIEW_BYTES + 8) + (d1 - d0)
            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
            writer.write_batch(batch)
            written += batch.num_rows
    return sink.size(), raw, written


def check_views(st, strings):
    """The hand-built views decode to the original strings."""
    n = min(len(strings), 50_000)
    part = strings.slice(0, n)
    part = pa.array(part.to_pylist(), type=pa.large_string())
    views, long_data, cumlong, is_long = view_parts(part)
    views[is_long, 12:16] = cumlong[:-1][is_long].astype("<u4").view(np.uint8).reshape(-1, 4)
    sv = pa.Array.from_buffers(pa.string_view(), n, [None, pa.py_buffer(views), pa.py_buffer(long_data)],
                               null_count=0)
    sv.validate(full=True)
    if not sv.cast(pa.large_string()).equals(part):
        raise SystemExit(f"statement {st.number}: hand-built Utf8View does not decode to its input")


# --------------------------------------------------------------------------
# The run
# --------------------------------------------------------------------------


def probe_windows(h):
    """groups / rows of every whole PROBE_ROWS window in corpus order."""
    w = len(h) // PROBE_ROWS
    m = np.sort(h[:w * PROBE_ROWS].reshape(w, PROBE_ROWS), axis=1)
    distinct = 1 + np.count_nonzero(m[:, 1:] != m[:, :-1], axis=1)
    return distinct / PROBE_ROWS


def exact_distinct(con, corpus, st, rows):
    q = (f"SELECT count(*) FROM (SELECT {', '.join(st.key_exprs)} FROM {source(corpus)} "
         f"WHERE file_row_number < {rows} GROUP BY ALL)")
    return con.sql(q).fetchall()[0][0]


def cfg(box, bits):
    return f"{box}|{bits}"


def save(state, path):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def stage_head(con, args, st, budget8):
    """Distinct keys, the probe check and the combined rows, full corpus."""
    rows = args.rows
    log(f"statement {st.number}: distinct keys")
    distinct = exact_distinct(con, args.corpus, st, rows)
    log(f"statement {st.number}: hash and charge of {rows} rows")
    h, cost8 = read_hash_cost(con, args.corpus, st, rows)
    ratios = probe_windows(h)
    head = dict(distinct=distinct, distinct_hashes=int(np.unique(h).size), hashed_rows=int(len(h)),
                raw_row_bytes_mean=float(cost8.mean() / 8), probe_windows=int(len(ratios)),
                probe_ratio_min=float(ratios.min()), probe_ratio_median=float(np.median(ratios)),
                probe_windows_above=int(np.count_nonzero(ratios > PROBE_RATIO)), comb={})
    log(f"statement {st.number}: combined rows over the full corpus")
    for bits in BITS:
        order, gs, ge = route(h, bits)
        cost_sorted = cost8[order]
        h_sorted = h[order]
        del order
        for box in args.boxes:
            starts = greedy_chunks(cost_sorted, gs, ge, budget8[(box, bits)], MAX_CHUNK_ROWS)
            cid = chunk_ids(rows, starts)
            head["comb"][cfg(box, bits)] = [combine_count(h_sorted, cid), int(len(starts))]
            del cid, starts
        del cost_sorted, h_sorted
    return head


def stage_slice(con, args, st, budget8, i):
    """Uncombined bytes of slice i, every config, in corpus order."""
    start = i * args.slice_rows
    end = min(start + args.slice_rows, args.rows)
    fixed, strings, hs, cs = read_slice(con, args.corpus, st, start, end)
    if i == 0:
        check_views(st, strings)
    n = end - start
    ones = np.ones(n, dtype=np.int64)
    out = dict(rows=n, raw_row_bytes_mean=float(cs.mean() / 8), unc={})
    for box in args.boxes:
        for bits in BITS:
            starts = greedy_chunks(cs, np.array([0]), np.array([n]), budget8[(box, bits)], MAX_CHUNK_ROWS)
            b, r, w = write_chunks(st, fixed, strings, ones, starts)
            out["unc"][cfg(box, bits)] = [b, r, int(len(starts)), w]
    return out


def stage_sample(con, args, st, budget8):
    """Combined bytes of the sample slice: routed, chunked, combined, written."""
    i = args.sample_slice
    start = i * args.slice_rows
    end = min(start + args.slice_rows, args.rows)
    fixed, strings, hs, cs = read_slice(con, args.corpus, st, start, end)
    n = end - start
    out = dict(rows=n, comb={})
    for bits in BITS:
        order, gs, ge = route(hs, bits)
        cost_sorted = cs[order]
        h_sorted = hs[order]
        taken_fixed = {c.name: fixed[c.name][order] for c in st.fixed}
        taken_strings = strings.take(pa.array(order))
        for box in args.boxes:
            starts = greedy_chunks(cost_sorted, gs, ge, budget8[(box, bits)], MAX_CHUNK_ROWS)
            cid = chunk_ids(n, starts)
            idx, counts = combine_rows(h_sorted, cid)
            if int(counts.sum()) != n:
                raise SystemExit("combined counts do not sum to the slice's rows")
            cstarts = np.flatnonzero(np.diff(cid[idx], prepend=-1) != 0)
            if len(cstarts) != len(starts):
                raise SystemExit("a combined chunk is empty or split")
            cf = {c.name: taken_fixed[c.name][idx] for c in st.fixed}
            b, r, w = write_chunks(st, cf, taken_strings.take(pa.array(idx)), counts, cstarts)
            out["comb"][cfg(box, bits)] = [b, r, int(len(starts)), w]
        del order, cost_sorted, h_sorted, taken_fixed, taken_strings
    return out


def stage_routed(con, args, st, budget8):
    """Uncombined bytes of the sample slice as the route writes them: each
    bucket's rows, in corpus order, cut into its own chunks."""
    i = args.sample_slice
    start = i * args.slice_rows
    end = min(start + args.slice_rows, args.rows)
    fixed, strings, hs, cs = read_slice(con, args.corpus, st, start, end)
    n = end - start
    ones = np.ones(n, dtype=np.int64)
    out = dict(rows=n, routed={})
    for bits in BITS:
        order, gs, ge = route(hs, bits)
        cost_sorted = cs[order]
        taken_fixed = {c.name: fixed[c.name][order] for c in st.fixed}
        taken_strings = strings.take(pa.array(order))
        for box in args.boxes:
            starts = greedy_chunks(cost_sorted, gs, ge, budget8[(box, bits)], MAX_CHUNK_ROWS)
            b, r, w = write_chunks(st, taken_fixed, taken_strings, ones, starts)
            out["routed"][cfg(box, bits)] = [b, r, int(len(starts)), w]
        del order, cost_sorted, taken_fixed, taken_strings
    return out


def run_statement(con, args, st, state, deadline):
    """Run every stage state does not hold yet. Returns False when the
    deadline stopped it before the last stage."""
    budget8 = {(box, bits): 8 * (chunk_budget(BOX_SHARE[box], bits) - 64 * st.buffers())
               for box in args.boxes for bits in BITS}
    mine = state["stmt"].setdefault(str(st.number), {})
    stages = (["head"] + [f"slice{i}" for i in range(-(-args.rows // args.slice_rows))] + ["sample", "routed"])
    for stage in stages:
        if stage in mine:
            continue
        if time.time() > deadline:
            return False
        t = time.time()
        log(f"statement {st.number}: stage {stage}")
        if stage == "head":
            mine[stage] = stage_head(con, args, st, budget8)
        elif stage == "sample":
            mine[stage] = stage_sample(con, args, st, budget8)
        elif stage == "routed":
            mine[stage] = stage_routed(con, args, st, budget8)
        else:
            mine[stage] = stage_slice(con, args, st, budget8, int(stage[5:]))
        mine[stage]["seconds"] = round(time.time() - t, 1)
        save(state, args.state)
    return True


def report(args, st, mine, out):
    rows = args.rows
    head = mine["head"]
    slices = [mine[k] for k in sorted(mine) if k.startswith("slice")]
    sample = mine["sample"]
    routed = mine["routed"]
    sample_slice = mine[f"slice{args.sample_slice}"]
    empty = empty_stream_bytes(st)
    floor = st.floor_charge()
    distinct = head["distinct"]
    print(f"stmt={st.number} corpus_rows={rows} hashed_rows={head['hashed_rows']} distinct_keys={distinct} "
          f"distinct_hashes={head['distinct_hashes']} raw_row_bytes_mean={head['raw_row_bytes_mean']:.2f} "
          f"sample_raw_row_bytes_mean={sample_slice['raw_row_bytes_mean']:.2f} "
          f"floor_charge_B={floor} empty_stream_B={empty} probe_windows={head['probe_windows']} "
          f"probe_ratio_min={head['probe_ratio_min']:.4f} probe_ratio_median={head['probe_ratio_median']:.4f} "
          f"probe_windows_above_{PROBE_RATIO}={head['probe_windows_above']}", file=out, flush=True)
    for box in args.boxes:
        t_max, b_max = tune_limit(BOX_SHARE[box], floor)
        print(f"stmt={st.number} box={box} floor_charge_B={floor} tunes_up_to_T_B={t_max} at_bits={b_max}",
              file=out, flush=True)
    if head["hashed_rows"] != rows or sum(s["rows"] for s in slices) != rows:
        raise SystemExit(f"statement {st.number}: rows read differ from {rows}")
    results = {}
    for box in args.boxes:
        for bits in BITS:
            k = cfg(box, bits)
            files = P << bits
            budget = chunk_budget(BOX_SHARE[box], bits)
            budget_max = chunk_budget_max(BOX_SHARE[box], bits)
            unc_b = sum(s["unc"][k][0] for s in slices) + (files - len(slices)) * empty
            unc_raw = sum(s["unc"][k][1] for s in slices)
            unc_chunks = sum(s["unc"][k][2] for s in slices)
            unc_rows = sum(s["unc"][k][3] for s in slices)
            s_b, _, _, s_rows = sample_slice["unc"][k]
            unc_scaled = (s_b + (files - 1) * empty) * rows / s_rows
            comb_rows, comb_chunks = head["comb"][k]
            sc_b, sc_raw, sc_chunks, sc_rows = sample["comb"][k]
            comb_b = sc_b * comb_rows / sc_rows + (files - 1) * empty
            r_b, _, r_chunks, r_rows = routed["routed"][k]
            # The routing penalty: the sample's routed bytes over its corpus-order
            # bytes, both with one stream per file, applied to the full corpus.
            routed_penalty = (r_b + (files - 1) * empty) / (s_b + (files - 1) * empty)
            routed_b = unc_b * routed_penalty
            unc_pred = max(unc_b, routed_b)
            thr = THRESHOLD[st.number][box]
            if unc_rows != rows or r_rows != s_rows:
                raise SystemExit(f"statement {st.number} {k}: wrote {unc_rows} uncombined rows, not {rows}")
            if not distinct <= comb_rows <= rows:
                raise SystemExit(f"statement {st.number} {k}: combined rows {comb_rows} outside [{distinct}, {rows}]")
            results[(box, bits)] = dict(unc=unc_pred, comb=comb_b, thr=thr)
            print(f"stmt={st.number} box={box} bits={bits} budget_min_B={budget} budget_max_B={budget_max} "
                  f"floor_fits_min={'yes' if budget >= floor else 'no'} "
                  f"floor_fits_max={'yes' if budget_max >= floor else 'no'} files={files} "
                  f"unc_rows={unc_rows} unc_chunks={unc_chunks} unc_raw_B={unc_raw} unc_ipc_B={unc_b} "
                  f"unc_ratio={unc_raw / unc_b:.3f} unc_sample_scaled_B={unc_scaled:.0f} "
                  f"sample_routed_chunks={r_chunks} sample_routed_ipc_B={r_b} routed_penalty={routed_penalty:.3f} "
                  f"routed_unc_ipc_B={routed_b:.0f} unc_pred_B={unc_pred:.0f} "
                  f"comb_rows={comb_rows} comb_chunks={comb_chunks} comb_rows_per_chunk_routed={rows / comb_chunks:.0f} "
                  f"sample_rows={sample['rows']} sample_comb_rows={sc_rows} sample_comb_chunks={sc_chunks} "
                  f"sample_comb_raw_B={sc_raw} sample_comb_ipc_B={sc_b} sample_comb_ratio={sc_raw / sc_b:.3f} "
                  f"comb_ipc_B={comb_b:.0f} threshold_B={thr} "
                  f"unc_vs_threshold={'below' if unc_pred < thr else 'above'} "
                  f"comb_vs_threshold={'below' if comb_b < thr else 'above'}", file=out, flush=True)
    return results


def verdicts(all_results, boxes, out):
    cells = {}
    for number, results in all_results.items():
        for box in boxes:
            unc = [results[(box, b)]["unc"] < results[(box, b)]["thr"] for b in BITS]
            comb = [results[(box, b)]["comb"] < results[(box, b)]["thr"] for b in BITS]
            if all(unc):
                v = "uncombined_below"
            elif all(comb):
                v = "combined_below_at_every_bits"
            elif any(comb):
                v = "combined_below_at_some_bits"
            else:
                v = "combined_below_at_no_bits"
            cells[(number, box)] = v
            print(f"verdict stmt={number} box={box} {v} "
                  f"unc_below_bits={[b for b, ok in zip(BITS, unc) if ok]} "
                  f"comb_below_bits={[b for b, ok in zip(BITS, comb) if ok]}", file=out, flush=True)
    values = set(cells.values())
    if values == {"uncombined_below"}:
        final = "build decision 3 alone"
    elif values <= {"uncombined_below", "combined_below_at_every_bits"}:
        final = "build decision 3 with decision 4"
    else:
        final = "build neither"
    print(f"final_verdict={final!r} cells={len(cells)}", file=out, flush=True)
    return final


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--rows", type=int, default=CORPUS_ROWS,
                    help="treat the first ROWS corpus rows as the corpus (a smoke run)")
    ap.add_argument("--slice-rows", type=int, default=20_000_000)
    ap.add_argument("--sample-slice", type=int, default=2,
                    help="the slice the combined bytes per row are measured on")
    ap.add_argument("--statements", type=int, nargs="+", default=sorted(STATEMENTS))
    ap.add_argument("--boxes", nargs="+", default=list(BOX_SHARE))
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--memory", default="6GB")
    ap.add_argument("--state", required=True,
                    help="JSON file holding each finished stage; a rerun resumes from it")
    ap.add_argument("--budget-seconds", type=float, default=float("inf"),
                    help="start no new stage after this many seconds; exit 3 if unfinished")
    args = ap.parse_args()
    deadline = time.time() + args.budget_seconds
    if args.slice_rows < 10_000_000 and args.rows == CORPUS_ROWS:
        raise SystemExit("the sample slice must hold at least 10 million rows")
    if args.sample_slice * args.slice_rows >= args.rows:
        raise SystemExit("the sample slice lies past the corpus")

    con = connect(args.threads, args.memory)
    total = con.sql(f"SELECT count(*) FROM read_parquet('{args.corpus}')").fetchall()[0][0]
    print(f"corpus_rows_in_file={total} expected={CORPUS_ROWS} rows_used={args.rows} "
          f"slice_rows={args.slice_rows} sample_slice={args.sample_slice} "
          f"sample_rows={args.sample_slice * args.slice_rows}..{min((args.sample_slice + 1) * args.slice_rows, args.rows)}",
          flush=True)
    if total != CORPUS_ROWS:
        raise SystemExit(f"corpus holds {total} rows, expected {CORPUS_ROWS}")
    run = dict(rows=args.rows, slice_rows=args.slice_rows, sample_slice=args.sample_slice,
               boxes=args.boxes, corpus_rows_in_file=total)
    state = dict(run=run, stmt={})
    if os.path.exists(args.state):
        with open(args.state) as f:
            state = json.load(f)
        if state["run"] != run:
            raise SystemExit(f"{args.state} was written by a run with other arguments: {state['run']}")
    for number in args.statements:
        if not run_statement(con, args, STATEMENTS[number], state, deadline):
            log("budget spent; rerun the same command to resume")
            sys.exit(3)
    all_results = {}
    for number in args.statements:
        st = STATEMENTS[number]
        print(f"stmt={number} keys=" + ",".join(f"{c.name}:{c.arrow}" for c in st.routed_columns[:-1])
              + f" state={COUNT.name}:{COUNT.arrow} sql={st.sql!r}", flush=True)
        all_results[number] = report(args, st, state["stmt"][str(number)], sys.stdout)
    verdicts(all_results, args.boxes, sys.stdout)


if __name__ == "__main__":
    main()

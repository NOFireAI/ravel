use arrow::array::DictionaryArray;
use arrow::datatypes::Int32Type;
use proptest::prelude::*;

use super::*;
use crate::load::test_support::*;

/// Write `batch` to Parquet and read it back through the LOADER's reader, so
/// a column the file dictionary-encodes arrives as an Arrow `Dictionary`
/// (#660) exactly as it does under `ravel-cli load --parquet`.
fn roundtrip_parquet(batch: &RecordBatch) -> RecordBatch {
    let (_dir, pq) = write_parquet(batch);
    read_parquet(&pq, true)
}

/// The end-to-end byte-identity anchor (ADR-0109 decision 7): the same
/// records, built row-wise and column-wise, encode to identical RLOG bytes
/// across a corpus of nulls in every mapped column, each `TsUnit`, an
/// out-of-`u8` severity number, an all-null attribute column, duplicate
/// mapped keys (winner plus residual), and both dictionary-encoded and plain
/// string columns.
///
/// It is also the #660 anchor: the rich fixture is read twice from the same
/// file, once through the loader's dictionary-preserving schema and once
/// through plain inference, and the two RLOG objects must be equal and hash
/// to the pinned BLAKE3.
///
/// Prove-the-test: change `observed_ts_ns` to `push(0)` (instead of
/// `raw_ts`), or drop the `TsUnit` scaling in `TsSrc::get` (return the raw
/// value), or set `use_dict` to `false` unconditionally -- each flips a byte
/// and the `assert_eq!` on the objects fails. Confirmed by making the
/// `observed_ts_ns` flip: the objects diverged and the assertion tripped.
#[test]
fn columnar_load_matches_row_load_byte_for_byte() {
    use arrow::array::DictionaryArray;
    use arrow::datatypes::Int32Type;

    // Each TsUnit: an integer ts column scaled by the declared unit must
    // land identically on both paths.
    for (unit, raw) in [
        (TsUnit::Seconds, 1_700_000_000_i64),
        (TsUnit::Millis, 1_700_000_000_000),
        (TsUnit::Micros, 1_700_000_000_000_000),
        (TsUnit::Nanos, 1_700_000_000_000_000_000),
    ] {
        let b = roundtrip_parquet(&batch(vec![
            ("ts", i64_col(vec![raw, raw])),
            ("a", str_col(vec!["v", "v"])),
        ]));
        let mut m = base_mapping();
        m.ts_unit = unit;
        m.attributes = vec![attr("a", "a", ColType::Str)];
        assert_paths_match(&b, &m);
    }

    // A rich batch: nulls in every optional/attribute column, an out-of-u8
    // severity, an all-null attribute column, duplicate mapped keys, and a
    // dictionary column beside a plain one.
    let ts = Arc::new(Int64Array::from(vec![
        NOW_NS,
        NOW_NS + 1,
        NOW_NS + 2,
        NOW_NS + 3,
    ])) as ArrayRef;
    let body = Arc::new(StringArray::from(vec![
        Some("hello"),
        None,
        Some(""),
        Some("world"),
    ])) as ArrayRef;
    // 300 is out of u8 range and must normalize to 0 on both paths; row 2 is
    // null (also 0).
    let sev = Arc::new(Int64Array::from(vec![
        Some(9_i64),
        Some(300),
        None,
        Some(0),
    ])) as ArrayRef;
    let svc = Arc::new(StringArray::from(vec![
        Some("api"),
        None,
        Some("web"),
        Some("api"),
    ])) as ArrayRef;
    let allnull = Arc::new(Int64Array::from(
        vec![None, None, None, None] as Vec<Option<i64>>
    )) as ArrayRef;
    let dup_a = Arc::new(Int64Array::from(vec![Some(1_i64), Some(2), None, Some(4)])) as ArrayRef;
    let dup_b = Arc::new(Int64Array::from(vec![
        Some(10_i64),
        None,
        Some(30),
        Some(40),
    ])) as ArrayRef;
    let dictcol = Arc::new(
        vec![Some("x"), Some("y"), None, Some("x")]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
    ) as ArrayRef;
    let plaincol = Arc::new(StringArray::from(vec![
        Some("p"),
        None,
        Some("q"),
        Some("p"),
    ])) as ArrayRef;

    let rich = batch(vec![
        ("ts", ts),
        ("body", body),
        ("sev", sev),
        ("svc", svc),
        ("allnull", allnull),
        ("dupA", dup_a),
        ("dupB", dup_b),
        ("dictcol", dictcol),
        ("plaincol", plaincol),
    ]);
    // One file, read two ways: `on` is the loader's reader, which applies
    // #660's dictionary-preserving schema; `off` lets the reader infer on
    // its own, which is what the loader opened before #660.
    let (_rich_dir, rich_pq) = write_parquet(&rich);
    let on = read_parquet(&rich_pq, true);
    let off = read_parquet(&rich_pq, false);

    let dict_idx = on.schema().index_of("dictcol").expect("dictcol present");
    let plain_idx = on.schema().index_of("plaincol").expect("plaincol present");

    // A column arrow-written as a `DictionaryArray` comes back a Dictionary
    // either way: `ArrowWriter` embeds the Arrow schema that says so.
    assert!(
        matches!(off.column(dict_idx).data_type(), DataType::Dictionary(_, _)),
        "an arrow-written dictionary column survives the Parquet round trip as a Dictionary"
    );
    assert!(
        matches!(on.column(dict_idx).data_type(), DataType::Dictionary(_, _)),
        "the loader's schema leaves an already-dictionary column as it is"
    );

    // #605's expectation, flipped on purpose by #660. `plaincol` was written
    // from a plain `StringArray`, so the embedded Arrow schema calls it Utf8
    // and the reader infers Utf8 (`off`) even though the file
    // dictionary-encodes the column. The loader's schema reads the chunk
    // encodings instead and types it a Dictionary (`on`).
    assert!(
        matches!(off.column(plain_idx).data_type(), DataType::Utf8),
        "without the loader's schema a plain-written string column arrives Utf8"
    );
    assert_eq!(
        on.column(plain_idx).data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        "the loader's reader keeps the file's dictionary on a plain-written string column"
    );

    let mut m = base_mapping();
    m.body_column = Some("body".to_string());
    m.severity_number_column = Some("sev".to_string());
    m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
    m.attributes = vec![
        attr("allnull", "allnull", ColType::I64),
        attr("dup", "dupA", ColType::I64),
        attr("dup", "dupB", ColType::I64),
        attr("dictkey", "dictcol", ColType::Str),
        attr("plainkey", "plaincol", ColType::Str),
    ];

    let col_off = assert_paths_match(&off, &m);
    let col_on = assert_paths_match(&on, &m);

    // The load-bearing invariant: the extra dictionary the loader's reader
    // now carries changes nothing the writer emits. Same 4 records, same
    // object, byte for byte, with the schema on and off.
    let bytes_off = columnar_object(col_off.clone());
    let bytes_on = columnar_object(col_on.clone());
    assert_eq!(
        bytes_on, bytes_off,
        "the RLOG object must not depend on whether a column arrived dictionary-encoded"
    );
    // Pinned so a drift in either direction is a test failure, not a silent
    // re-baseline of both sides at once. The value moves only with the
    // writer: this one is the RLOG version 5 layout (ADR-2135) with each
    // page's encoding chosen by stored size (#2140). No string chunk here
    // stores smaller on a row-group dictionary (#2144), so that change
    // leaves it as it was.
    const RICH_OBJECT_BLAKE3: &str =
        "ea47c829f32093565b57cab1ff344a55180f7b350a620ad287ff2ec739871c22";
    assert_eq!(
        blake3::hash(&bytes_off).to_hex().as_str(),
        RICH_OBJECT_BLAKE3,
        "object bytes without the dictionary-preserving schema"
    );
    assert_eq!(
        blake3::hash(&bytes_on).to_hex().as_str(),
        RICH_OBJECT_BLAKE3,
        "object bytes with the dictionary-preserving schema"
    );

    let dict_pos = col_on
        .dyn_columns
        .iter()
        .position(|c| c.name == "dictkey")
        .expect("dictkey column");
    let plain_pos = col_on
        .dyn_columns
        .iter()
        .position(|c| c.name == "plainkey")
        .expect("plainkey column");

    // Without the loader's schema, only the arrow-written dictionary column
    // reaches the StrColumnDict fast path (#605's original expectation).
    assert!(
        col_dict(&col_off, dict_pos).is_some(),
        "the arrow-written dictionary column passes through as a StrColumnDict"
    );
    assert!(
        col_dict(&col_off, plain_pos).is_none(),
        "without the loader's schema the plain-written column stays plain"
    );
    // With it, so does the plain-written one, because the file
    // dictionary-encodes it (#660).
    assert!(
        col_dict(&col_on, dict_pos).is_some(),
        "the arrow-written dictionary column still passes through as a StrColumnDict"
    );
    assert!(
        col_dict(&col_on, plain_pos).is_some(),
        "the plain-written but dictionary-encoded column now passes through as a StrColumnDict"
    );
}

/// With no id column mapped, the columnar path reads its mapped dictionary
/// columns in place: none is flattened ahead of it (only a mapped id column
/// is, which `the_columnar_path_resolves_each_mapped_id_column_once` pins),
/// and a dictionary attribute still reaches the `StrColumnDict` fast path.
/// The row path resolves the same batch's dictionary columns once each, and
/// the two build the same batch.
///
/// An all-null chunk over an empty dictionary is part of the batch, so the
/// columnar path's per-cell answer for it (`str_src`'s all-null path) is
/// what this load exercises too.
#[test]
fn the_columnar_path_resolves_no_dictionary_column_when_no_id_column_is_mapped() {
    const ROWS: usize = 64;
    let dict = |vals: Vec<&str>| -> ArrayRef {
        Arc::new(
            vals.into_iter()
                .map(Some)
                .collect::<DictionaryArray<Int32Type>>(),
        )
    };
    let empty_dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![None::<i32>; ROWS]),
        Arc::new(StringArray::from(Vec::<&str>::new())),
    ));
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
    let b = batch(vec![
        ("ts", i64_col(ts)),
        ("body", dict(vec!["hello"; ROWS])),
        ("svc", dict(vec!["api"; ROWS])),
        ("cat", dict(vec!["alpha"; ROWS])),
        ("gone", empty_dict),
    ]);
    let mut m = base_mapping();
    m.body_column = Some("body".to_string());
    m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
    m.attributes = vec![
        attr("cat", "cat", ColType::Str),
        attr("gone", "gone", ColType::Str),
    ];

    let counters = dict_counters();
    let col = build_columnar_or_panic(&b, &m);
    assert_eq!(
        counters.columns(),
        0,
        "with no id column mapped, the columnar path flattens none of the four mapped \
             dictionary columns"
    );
    assert_eq!(
        counters.cell_keys(),
        0,
        "nor resolves a dictionary key per cell"
    );
    assert_eq!(col.num_rows, ROWS, "every row is built");
    let pos = col
        .dyn_columns
        .iter()
        .position(|c| c.name == "cat")
        .expect("cat column");
    assert!(
        col_dict(&col, pos).is_some(),
        "the dictionary attribute keeps its StrColumnDict"
    );

    let counters = dict_counters();
    let matched = assert_paths_match(&b, &m);
    assert_eq!(
        matched, col,
        "the columnar build is the same on a second run"
    );
    assert_eq!(
        counters.columns(),
        4,
        "the row reference resolves each mapped dictionary column once"
    );
}

/// The columnar path flattens a mapped dictionary id column once per
/// batch, ahead of the row loop, and no other mapped dictionary column:
/// two id columns beside a dictionary body resolve exactly two columns,
/// whatever the row count is.
#[test]
fn the_columnar_path_resolves_each_mapped_id_column_once() {
    const ROWS: usize = 64;
    let trace_hex = hex::encode([1u8; 16]);
    let span_hex = hex::encode([2u8; 8]);
    let dict = |vals: Vec<&str>| -> ArrayRef {
        Arc::new(
            vals.into_iter()
                .map(Some)
                .collect::<DictionaryArray<Int32Type>>(),
        )
    };
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
    let b = batch(vec![
        ("ts", i64_col(ts)),
        ("body", dict(vec!["hello"; ROWS])),
        ("trace_id", dict(vec![trace_hex.as_str(); ROWS])),
        ("span_id", dict(vec![span_hex.as_str(); ROWS])),
    ]);
    let mut m = base_mapping();
    m.body_column = Some("body".to_string());
    m.trace_id_column = Some("trace_id".to_string());
    m.span_id_column = Some("span_id".to_string());

    let counters = dict_counters();
    let col = build_columnar_or_panic(&b, &m);
    assert_eq!(
        counters.columns(),
        2,
        "the two id columns are resolved once each, over {ROWS} rows, and the body is not"
    );
    assert_eq!(
        counters.cell_keys(),
        0,
        "no dictionary key is resolved per cell"
    );
    assert_eq!(col.num_rows, ROWS, "every row is built");
    assert_eq!(
        assert_paths_match(&b, &m),
        col,
        "the row path builds the same batch"
    );
}

// ---- #689: the dynamic-column slot table, against the map build ----

/// [`build_columnar_batch`] as it stood before #689, copied verbatim: every
/// cell resolves its destination column through a
/// `BTreeMap<(String, u8), _>` entry lookup keyed by a freshly cloned
/// attribute name, and a per-row `HashSet` decides the first-occurrence
/// winner. This is the differential oracle for the slot-table build. The two
/// must agree on every field of the batch: the RLOG object the columnar
/// writer produces is byte-identical only for identical batches, and the
/// RSEG layout is a frozen contract.
fn build_columnar_batch_reference(
    spans: &[(RecordBatch, u64)],
    mapping: &Mapping,
    limits: &LogIngestLimits,
    now_ns: i64,
) -> Result<ColumnarLogBatch, ColBuildError> {
    use std::collections::{BTreeMap, HashMap, HashSet};

    let total_rows: usize = spans.iter().map(|(b, _)| b.num_rows()).sum();
    let mut batch = ColumnarLogBatch::new();
    batch.num_rows = total_rows;
    if total_rows == 0 {
        return Ok(batch);
    }

    batch.ts_ns.reserve(total_rows);
    batch.observed_ts_ns.reserve(total_rows);
    batch.severity_num.reserve(total_rows);
    batch.flags.reserve(total_rows);
    batch.residual_attrs = vec![Vec::new(); total_rows];

    // Dynamic columns, keyed by (name, type byte) as `from_records` keys
    // them, so their materialized order matches. `col_dict` tracks whether
    // every winning cell of a column came from a dictionary-encoded Arrow
    // source.
    let mut col_cells: BTreeMap<(String, u8), Vec<Option<AttrValue>>> = BTreeMap::new();
    let mut col_dict: BTreeMap<(String, u8), bool> = BTreeMap::new();

    // Stream identity: hashed once per distinct resource tuple, keyed by the
    // STREAM_DIR blob (the canonical resource bytes) so the blake3 in
    // `log_stream_id` runs once per distinct tuple rather than once per row
    // (ADR-0109 decision 6). `stream_dir` is the id-ascending directory.
    let mut row_stream_id: Vec<LogStreamId> = Vec::with_capacity(total_rows);
    let mut stream_dir: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();
    let mut stream_cache: HashMap<Vec<u8>, LogStreamId> = HashMap::new();

    let mut grow = 0usize;
    for (span, file_base) in spans {
        let cols = ColumnIndex::locate(span, mapping).map_err(ColBuildError::Batch)?;

        // Prepare every reader once per span (downcast resolved here, not
        // per cell).
        let ts = ts_src(span.column(cols.ts), mapping.ts_unit);
        let body = cols.body.map(|i| str_src(span.column(i)));
        let sev_num = cols.severity_number.map(|i| int_src(span.column(i)));
        let sev_text = cols.severity_text.map(|i| str_src(span.column(i)));
        let trace = cols.trace_id.map(|i| id_src(span.column(i)));
        let span_id_src = cols.span_id.map(|i| id_src(span.column(i)));
        let resource: Vec<(usize, AttrSrc)> = cols
            .resource
            .iter()
            .map(|(ci, mi)| {
                (
                    *mi,
                    attr_src(
                        span.column(*ci),
                        mapping.resource_attributes[*mi].value_type,
                    ),
                )
            })
            .collect();
        let record: Vec<(usize, AttrSrc)> = cols
            .record
            .iter()
            .map(|(ci, mi)| {
                (
                    *mi,
                    attr_src(span.column(*ci), mapping.attributes[*mi].value_type),
                )
            })
            .collect();

        for local in 0..span.num_rows() {
            let file_row = file_base + local as u64;
            let row_err = |reason: String| ColBuildError::Row {
                row: file_row,
                reason,
            };

            // 1. ts (required), not negative, and 2. future-skew bound, in
            // build_record order.
            let raw_ts = match ts.get(local).map_err(row_err)? {
                Some(t) => t,
                None => {
                    return Err(row_err(format!(
                        "ts column {:?} is null",
                        mapping.ts_column
                    )));
                }
            };
            if raw_ts < 0 {
                return Err(row_err(negative_ts_rejection(
                    raw_ts,
                    span.column(cols.ts).data_type(),
                    mapping.ts_unit,
                )));
            }
            let skew_ns = raw_ts.saturating_sub(now_ns);
            if skew_ns > limits.max_future_skew_ns {
                return Err(row_err(format!(
                    "timestamp is {skew_ns} ns ahead of load time, more than the max future \
                         skew of {} ns",
                    limits.max_future_skew_ns
                )));
            }

            // 3. body (optional) and its length cap.
            let body_val = match &body {
                Some(s) => s.get(local).map_err(row_err)?.unwrap_or_default(),
                None => String::new(),
            };
            if body_val.len() > limits.max_body_len {
                return Err(row_err(format!(
                    "body is {} bytes, more than the limit of {}",
                    body_val.len(),
                    limits.max_body_len
                )));
            }

            // 4. severity number (out-of-u8 normalizes to 0) and severity
            // text.
            let severity_num = match &sev_num {
                Some(s) => s
                    .get(local)
                    .map_err(row_err)?
                    .and_then(|v| u8::try_from(v).ok())
                    .unwrap_or(0),
                None => 0,
            };
            let severity_text = match &sev_text {
                Some(s) => s.get(local).map_err(row_err)?.unwrap_or_default(),
                None => String::new(),
            };

            // 5. trace/span ids: exact length or absent.
            let trace_id = match &trace {
                Some(s) => s
                    .get(local)
                    .map_err(row_err)?
                    .and_then(|b| <[u8; 16]>::try_from(b.as_slice()).ok()),
                None => None,
            };
            let span_id = match &span_id_src {
                Some(s) => s
                    .get(local)
                    .map_err(row_err)?
                    .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok()),
                None => None,
            };

            // 6. resource attributes (stream identity), checked in mapping
            // order.
            let mut resource_attrs: Vec<(String, AttrValue)> = Vec::with_capacity(resource.len());
            for (mi, src) in &resource {
                let spec = &mapping.resource_attributes[*mi];
                if let Some(v) = src.get(local).map_err(row_err)? {
                    check_attr(&spec.key, &v, limits).map_err(row_err)?;
                    resource_attrs.push((spec.key.clone(), v));
                }
            }

            // 7. record attributes: check, count for the per-record cap, and
            // split first-occurrence winner vs within-record residual
            // exactly as `from_records`.
            let mut present_record = 0usize;
            let mut taken: HashSet<(String, u8)> = HashSet::new();
            for (mi, src) in &record {
                let spec = &mapping.attributes[*mi];
                if let Some(v) = src.get(local).map_err(row_err)? {
                    check_attr(&spec.key, &v, limits).map_err(row_err)?;
                    present_record += 1;
                    let key = (spec.key.clone(), field_type_of(spec.value_type).to_u8());
                    if taken.insert(key.clone()) {
                        col_cells
                            .entry(key.clone())
                            .or_insert_with(|| vec![None; total_rows])[grow] = Some(v);
                        let flag = col_dict.entry(key).or_insert(true);
                        *flag &= src.is_dict();
                    } else {
                        batch.residual_attrs[grow].push((spec.key.clone(), v));
                    }
                }
            }
            if present_record > LOADER_MAX_ATTRIBUTES_PER_RECORD {
                return Err(row_err(format!(
                    "record has {present_record} attributes, more than the loader per-record \
                         cap of {LOADER_MAX_ATTRIBUTES_PER_RECORD}"
                )));
            }

            // 8. stream identity: hash once per distinct resource tuple.
            let blob = stream_attrs_bytes(&resource_attrs, "", "", &[]);
            let stream_id = match stream_cache.get(&blob) {
                Some(id) => *id,
                None => {
                    let id = log_stream_id(&resource_attrs, "", "", &[]);
                    stream_cache.insert(blob.clone(), id);
                    id
                }
            };
            stream_dir.entry(stream_id).or_insert_with(|| blob.clone());
            row_stream_id.push(stream_id);

            // Fixed columns, appended in row order.
            batch.ts_ns.push(raw_ts);
            batch.observed_ts_ns.push(raw_ts);
            batch.severity_num.push(severity_num);
            batch.flags.push(0);
            batch.severity_text.push(severity_text.as_bytes());
            batch.body.push(body_val.as_bytes());
            match trace_id {
                Some(t) => {
                    batch.trace_id.extend_from_slice(&t);
                    batch.trace_id_validity.push(true);
                }
                None => batch.trace_id_validity.push(false),
            }
            match span_id {
                Some(s) => {
                    batch.span_id.extend_from_slice(&s);
                    batch.span_id_validity.push(true);
                }
                None => batch.span_id_validity.push(false),
            }

            grow += 1;
        }
    }

    // Stream directory: id-ascending dense refs, matching `from_records`.
    let mut ref_of: HashMap<LogStreamId, u32> = HashMap::with_capacity(stream_dir.len());
    for (i, (id, blob)) in stream_dir.into_iter().enumerate() {
        ref_of.insert(id, i as u32);
        batch.stream_ids.push(id);
        batch.stream_attrs.push(blob);
    }
    batch.stream_refs = row_stream_id.iter().map(|id| ref_of[id]).collect();

    // Materialize dynamic columns in (name, type) order; attach a
    // StrColumnDict to a Str/Bytes column whose every winning cell came from
    // a dictionary source. If no column carries a dictionary, leave
    // `dyn_col_dicts` empty (its default), so a plain load is byte-identical
    // to `from_records` without `with_dictionaries`.
    let mut dicts: Vec<Option<StrColumnDict>> = Vec::with_capacity(col_cells.len());
    let mut any_dict = false;
    for ((name, ty_byte), cells) in col_cells {
        let field_type = FieldType::from_u8(ty_byte).unwrap_or(FieldType::Bytes);
        let mut validity = Bitmap::new();
        let mut dense = Vec::new();
        for cell in cells {
            match cell {
                Some(v) => {
                    validity.push(true);
                    dense.push(v);
                }
                None => validity.push(false),
            }
        }
        let use_dict = matches!(field_type, FieldType::Str | FieldType::Bytes)
            && col_dict
                .get(&(name.clone(), ty_byte))
                .copied()
                .unwrap_or(false);
        if use_dict {
            any_dict = true;
            dicts.push(Some(str_column_dict_from_cells(&dense)));
        } else {
            dicts.push(None);
        }
        batch.dyn_columns.push(DynColumn {
            name,
            field_type,
            cells: dense,
            validity,
        });
    }
    if any_dict {
        batch.dyn_col_dicts = dicts;
    }

    Ok(batch)
}

/// A 64-bit mix, so a generated case carries a seed instead of megabytes of
/// literal cell data: every cell is derived from (seed, column, row).
fn mix(seed: u64, col: usize, row: usize) -> u64 {
    let mut h = seed ^ 0x9e37_79b9_7f4a_7c15;
    h = h
        .wrapping_add((col as u64).wrapping_mul(0xff51_afd7_ed55_8ccd))
        .rotate_left(31);
    h = h
        .wrapping_add((row as u64).wrapping_mul(0xc4ce_b9fe_1a85_ec53))
        .rotate_left(27);
    h ^= h >> 33;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^ (h >> 29)
}

/// One generated attribute column: its source Parquet column, the record key
/// it maps to, its declared type, and whether the Arrow array arrives
/// dictionary-encoded (which drives the `StrColumnDict` decision).
#[derive(Debug, Clone)]
struct GenCol {
    column: String,
    key: String,
    ty: ColType,
    dict: bool,
}

/// Derive `n` attribute columns from a seed. Keys are drawn from a pool of
/// `key_span` names, so distinct source columns collide on one
/// `(name, type)` slot (exercising the within-row residual path) and one
/// name splits across types (two slots). "k10" sorting before "k2" keeps the
/// slot order non-numeric, the same order the map produced.
fn gen_cols(n: usize, seed: u64, key_span: usize) -> Vec<GenCol> {
    (0..n)
        .map(|i| {
            let h = mix(seed, i, 0);
            let ty = match h % 5 {
                0 => ColType::Str,
                1 => ColType::I64,
                2 => ColType::F64,
                3 => ColType::Bool,
                _ => ColType::Bytes,
            };
            GenCol {
                column: format!("c{i}"),
                key: format!("k{}", (h >> 8) as usize % key_span.max(1)),
                ty,
                dict: matches!(ty, ColType::Str) && (h >> 20).is_multiple_of(3),
            }
        })
        .collect()
}

/// Build one span's Arrow array for `col`, covering rows `start..start+len`
/// of the logical batch. A cell is null when its mix falls under
/// `null_pct`.
fn gen_array(
    col: &GenCol,
    ci: usize,
    seed: u64,
    start: usize,
    len: usize,
    null_pct: u8,
) -> ArrayRef {
    let present = |row: usize| mix(seed, ci, row) % 100 >= u64::from(null_pct);
    let cell = |row: usize| mix(seed, ci.wrapping_add(7), row.wrapping_add(1));
    let text = |row: usize| format!("v{}", cell(row) % 997);
    match col.ty {
        ColType::I64 => Arc::new(Int64Array::from(
            (0..len)
                .map(|k| present(start + k).then(|| cell(start + k) as i64))
                .collect::<Vec<Option<i64>>>(),
        )),
        // No NaN and no -0.0: the batch comparison is a value comparison, and
        // those two are exactly the payloads it could not decide.
        ColType::F64 => Arc::new(Float64Array::from(
            (0..len)
                .map(|k| present(start + k).then(|| (cell(start + k) % 1_000_000) as f64 / 8.0))
                .collect::<Vec<Option<f64>>>(),
        )),
        ColType::Bool => Arc::new(BooleanArray::from(
            (0..len)
                .map(|k| present(start + k).then(|| cell(start + k).is_multiple_of(2)))
                .collect::<Vec<Option<bool>>>(),
        )),
        ColType::Str => {
            let vals: Vec<Option<String>> = (0..len)
                .map(|k| present(start + k).then(|| text(start + k)))
                .collect();
            // A span with no present value gets the plain encoding: a
            // dictionary array with zero distinct values makes arrow's
            // `normalized_keys` panic, so it is not a shape `str_src` can be
            // handed here (see the report on #689).
            if col.dict && vals.iter().any(Option::is_some) {
                let arr: DictionaryArray<Int32Type> = vals.iter().map(|v| v.as_deref()).collect();
                Arc::new(arr)
            } else {
                Arc::new(StringArray::from(vals))
            }
        }
        ColType::Bytes => Arc::new(
            (0..len)
                .map(|k| present(start + k).then(|| text(start + k).into_bytes()))
                .collect::<BinaryArray>(),
        ),
    }
}

/// Assemble `n_spans` record batches over `rows` logical rows, plus the
/// mapping that reads them: a non-null `ts`, a low-cardinality resource
/// column so the stream directory holds several streams, and one column per
/// [`GenCol`].
fn gen_spans_and_mapping(
    rows: usize,
    n_spans: usize,
    cols: &[GenCol],
    seed: u64,
    null_pct: u8,
) -> (Vec<(RecordBatch, u64)>, Mapping) {
    let mut spans = Vec::with_capacity(n_spans);
    let base = rows / n_spans.max(1);
    let extra = rows % n_spans.max(1);
    let mut start = 0usize;
    for s in 0..n_spans.max(1) {
        let len = base + usize::from(s < extra);
        if len == 0 {
            continue;
        }
        let mut arrays: Vec<(String, ArrayRef)> = Vec::with_capacity(cols.len() + 2);
        arrays.push((
            "ts".to_string(),
            Arc::new(Int64Array::from(
                (0..len)
                    .map(|k| NOW_NS - ((start + k) as i64 % 1_000_000) * 1_000)
                    .collect::<Vec<i64>>(),
            )) as ArrayRef,
        ));
        arrays.push((
            "res".to_string(),
            Arc::new(StringArray::from_iter_values(
                (0..len).map(|k| format!("svc{}", mix(seed, 4_242, start + k) % 4)),
            )) as ArrayRef,
        ));
        for (ci, c) in cols.iter().enumerate() {
            arrays.push((
                c.column.clone(),
                gen_array(c, ci, seed, start, len, null_pct),
            ));
        }
        spans.push((
            RecordBatch::try_from_iter(arrays).expect("record batch"),
            start as u64,
        ));
        start += len;
    }
    let mut mapping = base_mapping();
    mapping.resource_attributes = vec![AttrMap {
        key: "service.name".to_string(),
        column: "res".to_string(),
        value_type: ColType::Str,
    }];
    mapping.attributes = cols
        .iter()
        .map(|c| AttrMap {
            key: c.key.clone(),
            column: c.column.clone(),
            value_type: c.ty,
        })
        .collect();
    (spans, mapping)
}

/// The reference build refuses a negative timestamp at the same row, with
/// the same reason, as the production build.
#[test]
fn the_reference_build_refuses_a_negative_timestamp_like_production() {
    let spans = vec![(batch(vec![("ts", i64_col(vec![NOW_NS, -5, NOW_NS]))]), 10)];
    let mapping = base_mapping();
    let limits = LogIngestLimits::default();
    let refusal = |result: Result<ColumnarLogBatch, ColBuildError>| match result {
        Err(ColBuildError::Row { row, reason }) => (row, reason),
        Err(ColBuildError::Batch(r)) => panic!("expected a row rejection, got batch: {r}"),
        Ok(_) => panic!("expected a row rejection, got a batch"),
    };
    let got = refusal(build_columnar_batch(&spans, &mapping, &limits, NOW_NS));
    let want = refusal(build_columnar_batch_reference(
        &spans, &mapping, &limits, NOW_NS,
    ));
    assert_eq!(
        got,
        (
            11,
            "timestamp is before the Unix epoch (-5 ns, read as ts_unit = nanos); the column \
                 holds a negative value"
                .to_string()
        )
    );
    assert_eq!(want, got, "the reference refuses the same row the same way");
}

/// Assert the slot-table build and the pre-#689 map build produce the same
/// batch for one generated case.
fn assert_same_batch(
    rows: usize,
    n_spans: usize,
    n_cols: usize,
    key_span: usize,
    null_pct: u8,
    seed: u64,
) {
    let cols = gen_cols(n_cols, seed, key_span);
    let (spans, mapping) = gen_spans_and_mapping(rows, n_spans, &cols, seed, null_pct);
    let limits = LogIngestLimits::default();
    let got = match build_columnar_batch(&spans, &mapping, &limits, NOW_NS) {
        Ok(b) => b,
        Err(ColBuildError::Batch(r)) => panic!("slot-table build failed the batch: {r}"),
        Err(ColBuildError::Row { row, reason }) => {
            panic!("slot-table build rejected row {row}: {reason}")
        }
    };
    let want = match build_columnar_batch_reference(&spans, &mapping, &limits, NOW_NS) {
        Ok(b) => b,
        Err(ColBuildError::Batch(r)) => panic!("reference build failed the batch: {r}"),
        Err(ColBuildError::Row { row, reason }) => {
            panic!("reference build rejected row {row}: {reason}")
        }
    };

    assert_eq!(
        got.dyn_columns.len(),
        want.dyn_columns.len(),
        "dynamic column count"
    );
    let got_keys: Vec<(&str, FieldType)> = got
        .dyn_columns
        .iter()
        .map(|c| (c.name.as_str(), c.field_type))
        .collect();
    let want_keys: Vec<(&str, FieldType)> = want
        .dyn_columns
        .iter()
        .map(|c| (c.name.as_str(), c.field_type))
        .collect();
    assert_eq!(
        got_keys, want_keys,
        "dynamic column (name, field_type) sequence, in order"
    );
    for (g, w) in got.dyn_columns.iter().zip(&want.dyn_columns) {
        assert_eq!(g.cells, w.cells, "cells of column {:?}", g.name);
        assert_eq!(
            g.validity.len(),
            w.validity.len(),
            "validity length of column {:?}",
            g.name
        );
        assert_eq!(
            g.validity.bytes(),
            w.validity.bytes(),
            "validity of column {:?}",
            g.name
        );
    }
    assert_eq!(got.dyn_col_dicts, want.dyn_col_dicts, "dictionary columns");
    assert_eq!(
        got.residual_attrs, want.residual_attrs,
        "within-row residual attributes"
    );
    assert_eq!(got, want, "the whole batch");
}

/// A fixed 48-column case (mixed types, dictionary and plain strings, key
/// collisions, 20% nulls) that runs on every test run, independent of the
/// proptest budget below.
#[test]
fn slot_table_build_matches_map_build_48_columns() {
    assert_same_batch(1_000, 3, 48, 20, 20, 0x5EED_0000_0000_0001);
}

/// Every attribute column null across the whole batch: the map held no entry
/// for such a column, so the slot table must materialize none either.
#[test]
fn slot_table_build_drops_all_null_columns() {
    assert_same_batch(64, 1, 48, 20, 100, 0x5EED_0000_0000_0002);
    let cols = gen_cols(48, 0x5EED_0000_0000_0002, 20);
    let (spans, mapping) = gen_spans_and_mapping(64, 1, &cols, 0x5EED_0000_0000_0002, 100);
    let batch = match build_columnar_batch(&spans, &mapping, &LogIngestLimits::default(), NOW_NS) {
        Ok(b) => b,
        Err(_) => panic!("all-null attribute columns are not a rejection"),
    };
    assert!(
        batch.dyn_columns.is_empty(),
        "an all-null mapped column materializes no dynamic column"
    );
}

/// 60 distinct streams, presented in row order `svc00`..`svc59` (first
/// appearance is lexicographic column order, not id-ascending): the
/// production build's binary-search ref resolution (#2441) must land on the
/// same `stream_ids`/`stream_refs` as the reference's `HashMap` resolution,
/// not just agree by accident on a batch small enough that the two orders
/// happen to coincide.
#[test]
fn many_streams_out_of_order_match_reference() {
    const N: usize = 60;
    let ts = Arc::new(Int64Array::from(vec![NOW_NS; N])) as ArrayRef;
    let res = Arc::new(StringArray::from_iter_values(
        (0..N).map(|i| format!("svc{i:02}")),
    )) as ArrayRef;
    let spans = vec![(batch(vec![("ts", ts), ("res", res)]), 0u64)];

    let mut mapping = base_mapping();
    mapping.resource_attributes = vec![attr("service.name", "res", ColType::Str)];
    let limits = LogIngestLimits::default();

    let unwrap_batch = |result: Result<ColumnarLogBatch, ColBuildError>, who: &str| match result {
        Ok(b) => b,
        Err(ColBuildError::Batch(r)) => panic!("{who} build failed the batch: {r}"),
        Err(ColBuildError::Row { row, reason }) => {
            panic!("{who} build rejected row {row}: {reason}")
        }
    };
    let got = unwrap_batch(
        build_columnar_batch(&spans, &mapping, &limits, NOW_NS),
        "production",
    );
    let want = unwrap_batch(
        build_columnar_batch_reference(&spans, &mapping, &limits, NOW_NS),
        "reference",
    );

    assert_eq!(want.stream_ids.len(), N, "every row's stream is distinct");
    assert_ne!(
        want.stream_refs,
        (0..N as u32).collect::<Vec<u32>>(),
        "fixture must present streams out of id-ascending order, or this test proves nothing"
    );

    assert_eq!(got.stream_ids, want.stream_ids, "stream directory ids");
    assert_eq!(got.stream_refs, want.stream_refs, "per-row stream refs");
    assert_eq!(got, want, "the whole batch");
}

proptest! {
    // 24 cases, not the default 256: a case at the top of the range
    // materializes 4096 x 120 cells twice, once per implementation, so the
    // default turns this into a multi-minute test without covering anything
    // the slot table can get wrong that 24 cases do not reach.
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The slot-table build and the pre-#689 map build agree on every field
    /// of the produced batch, across row counts, column counts, key
    /// collisions, type mixes, null densities and span splits.
    #[test]
    fn slot_table_build_matches_map_build(
        rows in 1usize..=4096,
        n_cols in 1usize..=120,
        key_span in 1usize..=120,
        null_pct in 0u8..=100,
        n_spans in 1usize..=3,
        seed in any::<u64>(),
    ) {
        assert_same_batch(rows, n_spans, n_cols, key_span, null_pct, seed);
    }
}

/// A timing report, never an assertion: with `RAVEL_LOAD_BATCH_TIMING=1`,
/// time both builds on a 65,536-row x 105-column batch (ClickBench `hits`
/// width) and print the two wall times. Skipped otherwise, so a normal test
/// run pays nothing for it.
#[test]
fn build_columnar_batch_timing_report() {
    if std::env::var("RAVEL_LOAD_BATCH_TIMING").ok().as_deref() != Some("1") {
        return;
    }
    const ROWS: usize = 65_536;
    const COLS: usize = 105;
    const SEED: u64 = 0xC0FF_EE00_1234_5678;

    let cols = gen_cols(COLS, SEED, COLS);
    let (spans, mapping) = gen_spans_and_mapping(ROWS, 1, &cols, SEED, 10);
    let limits = LogIngestLimits::default();

    let t0 = Instant::now();
    let want = build_columnar_batch_reference(&spans, &mapping, &limits, NOW_NS);
    let map_elapsed = t0.elapsed();
    let t1 = Instant::now();
    let got = build_columnar_batch(&spans, &mapping, &limits, NOW_NS);
    let slot_elapsed = t1.elapsed();

    assert!(want.is_ok(), "the reference build succeeds");
    assert!(got.is_ok(), "the slot-table build succeeds");
    println!(
        "build_columnar_batch over {ROWS} rows x {COLS} columns: map build {map_elapsed:?}, \
             slot-table build {slot_elapsed:?}"
    );
}

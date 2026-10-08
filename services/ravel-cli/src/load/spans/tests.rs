use arrow::array::DictionaryArray;
use arrow::datatypes::Int32Type;

use super::*;
use crate::load::test_support::*;

/// The smallest legal spans mapping plus one attribute of each kind.
const MAPPING_TOML: &str = r#"
[spans]
trace_id_column = "trace_id"
span_id_column  = "span_id"
name_column     = "name"
start_ts_column = "start_ns"
start_ts_unit   = "nanos"
end_ts_column   = "end_ns"
end_ts_unit     = "nanos"

[[spans.attribute]]
key = "http.method"
column = "method"
type = "str"
"#;

fn bin_col(vals: Vec<Vec<u8>>) -> ArrayRef {
    let refs: Vec<&[u8]> = vals.iter().map(|v| v.as_slice()).collect();
    Arc::new(BinaryArray::from(refs))
}

/// One span's Parquet file and mapping file on disk, for a test that
/// drives the CLI entry point rather than [`load_spans`].
fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("spans.parquet");
    let mapping_path = dir.path().join("mapping.toml");
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]])),
        ("span_id", bin_col(vec![vec![2u8; 8]])),
        ("name", str_col(vec!["op"])),
        ("start_ns", i64_col(vec![NOW_NS])),
        ("end_ns", i64_col(vec![NOW_NS])),
        ("method", str_col(vec!["GET"])),
    ]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    std::fs::write(&mapping_path, MAPPING_TOML).expect("write mapping");
    (dir, pq, mapping_path)
}

/// `--read-cursors 0` and `--decode-queue-batches 0` are rejected on
/// the spans path with the same messages the other paths give: a lever
/// this path ignores is still not one that may take a value its own
/// documentation calls invalid.
#[tokio::test]
async fn zero_levers_are_rejected() {
    let (_dir, pq, mapping_path) = fixture();
    for (read_cursors, decode_queue_batches, want) in [
        (Some(0), DEFAULT_DECODE_QUEUE_BATCHES, READ_CURSORS_ZERO),
        (None, 0, DECODE_QUEUE_BATCHES_ZERO),
    ] {
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(ravel_object_store::memory::MemoryStore::new());
        let mut sink: Vec<u8> = Vec::new();
        let err = run_warning_to(
            store,
            &pq,
            "acme",
            &mapping_path,
            SignalArg::Spans,
            1,
            10_000,
            0,
            read_cursors,
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            decode_queue_batches,
            DEFAULT_TARGET_BYTES,
            None,
            RlogZstdLevel::DEFAULT,
            None,
            NOW_NS,
            &mut sink,
        )
        .await
        .expect_err("a zero lever is rejected before anything is written");
        assert_eq!(err.to_string(), want);
    }
}

/// The entry point prints the spans admission-bypass warning and names
/// both levers a spans load ignores.
#[tokio::test]
async fn the_entry_point_warns_about_the_levers_it_ignores() {
    let (_dir, pq, mapping_path) = fixture();
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let mut sink: Vec<u8> = Vec::new();
    run_warning_to(
        store,
        &pq,
        "acme",
        &mapping_path,
        SignalArg::Spans,
        1,
        10_000,
        0,
        Some(4),
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        8,
        DEFAULT_TARGET_BYTES,
        None,
        RlogZstdLevel::DEFAULT,
        None,
        NOW_NS,
        &mut sink,
    )
    .await
    .expect("an ignored lever is a warning, not a failure");
    let emitted = String::from_utf8(sink).expect("warnings are utf-8");
    assert!(
        emitted.contains(SPANS_ADMISSION_BYPASS_WARNING),
        "the spans admission-bypass warning reaches the CLI's stream: {emitted}"
    );
    assert!(
        emitted.contains("a spans load ignores --read-cursors 4 and --decode-queue-batches 8"),
        "both ignored levers are named: {emitted}"
    );
}

/// Two mapped attributes cannot share a key, in either list: the
/// stored `attrs` is one map and the merge would silently pick one.
#[test]
fn a_duplicate_attribute_key_is_refused() {
    let text = format!(
        "{MAPPING_TOML}\n[[spans.resource_attribute]]\nkey = \"http.method\"\ncolumn = \
                 \"m2\"\ntype = \"str\"\n"
    );
    let err = parse_spans_mapping(&text).expect_err("one key, two columns");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("declares the attribute key \"http.method\" twice"),
        "the refusal names the key: {message}"
    );
}

/// `attrs_map_column` is a `[spans]` key, as it is a logs one, and
/// `deny_unknown_fields` still refuses a key that is not.
#[test]
fn attrs_map_column_is_accepted_and_an_unknown_key_is_still_refused() {
    let text = MAPPING_TOML.replace("[spans]\n", "[spans]\nattrs_map_column = \"rest\"\n");
    let mapping = parse_spans_mapping(&text).expect("attrs_map_column is a [spans] key");
    assert_eq!(mapping.attrs_map_column.as_deref(), Some("rest"));
    assert_eq!(
        parse_spans_mapping(MAPPING_TOML)
            .expect("valid mapping")
            .attrs_map_column,
        None
    );
    let typo = MAPPING_TOML.replace("[spans]\n", "[spans]\nattrs_map_colum = \"rest\"\n");
    let LoadError::Setup(message) =
        parse_spans_mapping(&typo).expect_err("deny_unknown_fields rejects a typo")
    else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("unknown field `attrs_map_colum`"),
        "the refusal names the key: {message}"
    );
}

/// A map whose keys and values are dictionary-encoded strings passes
/// the column check and reads each row's entries through the same
/// dictionary resolution a plain string column gets.
#[test]
fn a_dictionary_encoded_attrs_map_is_read_as_strings() {
    use arrow::array::{DictionaryArray, MapArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Fields, Int32Type};

    let keys: DictionaryArray<Int32Type> = vec!["zone", "peer", "zone"].into_iter().collect();
    let values: DictionaryArray<Int32Type> = vec![Some("eu"), Some("db"), Some("eu")]
        .into_iter()
        .collect();
    let fields = Fields::from(vec![
        Field::new("keys", keys.data_type().clone(), false),
        Field::new("values", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(
        fields.clone(),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        None,
    );
    let map: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            entries,
            None,
            false,
        )
        .expect("map array"),
    );

    check_attrs_map_column(map.data_type(), "attrs")
        .expect("dictionary-encoded strings are strings");
    let text = MAPPING_TOML.replace("[spans]\n", "[spans]\nattrs_map_column = \"attrs\"\n");
    let mapping = parse_spans_mapping(&text).expect("valid mapping");
    let limits = SpanIngestLimits::default();
    let mapped_keys = mapping.mapped_keys();
    let resolved = SpansAttrsMap::resolve(&map, "attrs", &mapped_keys).expect("map resolves");
    let mut dropped = 0;
    let rows: Vec<Vec<(String, String)>> = (0..2)
        .map(|row| {
            read_span_attrs_map(&resolved, row, 0, &limits, &mut dropped).expect("entries read")
        })
        .collect();
    let pair = |key: &str, value: &str| (key.to_string(), value.to_string());
    assert_eq!(
        rows,
        vec![
            vec![pair("zone", "eu"), pair("peer", "db")],
            vec![pair("zone", "eu")]
        ]
    );
    assert_eq!(dropped, 0);
}

/// A mapped attribute key over the OTLP key-length cap is refused
/// before any row is read, since the mapping alone decides it.
#[test]
fn an_oversized_attribute_key_is_refused_at_the_otlp_bound() {
    let limit = SpanIngestLimits::default().max_attribute_key_len;
    let key = "k".repeat(limit + 1);
    let text = format!(
        "{MAPPING_TOML}\n[[spans.attribute]]\nkey = \"{key}\"\ncolumn = \"x\"\ntype = \
                 \"str\"\n"
    );
    let err = parse_spans_mapping(&text).expect_err("over the key-length cap");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains(&format!(
            "is {} bytes, more than the attribute-key limit of {limit}",
            limit + 1
        )),
        "the refusal names both lengths: {message}"
    );
}

/// Every key ravel-otlp reserves for a span field RSPAN has no column
/// for is refused in either attribute list. The list is ravel-otlp's
/// own, so a key added there is covered here without an edit.
#[test]
fn every_reserved_attribute_key_is_refused_in_either_list() {
    use ravel_otlp::traces_normalize::RESERVED_ATTR_KEYS;

    for key in RESERVED_ATTR_KEYS {
        for list in ["attribute", "resource_attribute"] {
            let text = format!(
                "{MAPPING_TOML}\n[[spans.{list}]]\nkey = \"{key}\"\ncolumn = \"x\"\ntype \
                         = \"str\"\n"
            );
            let err = parse_spans_mapping(&text).expect_err("a reserved key is refused");
            let LoadError::Setup(message) = err else {
                panic!("expected a setup error for {key:?} in {list}");
            };
            assert!(
                message.starts_with(&format!(
                    "--mapping [spans] names the reserved attribute key {key:?}, which \
                             this version does not map"
                )),
                "the refusal names {key:?} in {list}: {message}"
            );
        }
    }
}

/// A logs load uses both levers a sequential load ignores, so it has
/// no warning to give whatever they are set to.
#[test]
fn a_logs_load_has_no_unused_lever_warning() {
    assert_eq!(unused_lever_warning(Some(4), 8, SignalArg::Logs), None);
    assert!(unused_lever_warning(Some(4), 8, SignalArg::Metrics).is_some());
}

/// A non-default `--zstd-level` on a metrics or spans load is named as
/// ignored; the default, or any level on a logs load, is not.
#[test]
fn metrics_and_spans_warn_on_a_non_default_zstd_level_and_logs_does_not() {
    let nineteen = RlogZstdLevel::new(19).expect("in range");
    assert_eq!(unused_zstd_level_warning(nineteen, SignalArg::Logs), None);
    assert_eq!(
        unused_zstd_level_warning(RlogZstdLevel::DEFAULT, SignalArg::Spans),
        None
    );
    assert_eq!(
        unused_zstd_level_warning(nineteen, SignalArg::Metrics).as_deref(),
        Some(
            "warning: a metrics load ignores --zstd-level 19. The level applies only to \
                     the RLOG objects a logs load writes."
        )
    );
    assert_eq!(
        unused_zstd_level_warning(nineteen, SignalArg::Spans).as_deref(),
        Some(
            "warning: a spans load ignores --zstd-level 19. The level applies only to \
                     the RLOG objects a logs load writes."
        )
    );
}

/// A `--load-memory-bytes` on a metrics or spans load is named as ignored;
/// an unset flag, or any budget on a logs load, is not.
#[test]
fn metrics_and_spans_warn_on_a_set_load_memory_budget_and_logs_does_not() {
    assert_eq!(
        unused_load_memory_warning(Some(6_000_000_000), SignalArg::Logs),
        None
    );
    assert_eq!(unused_load_memory_warning(None, SignalArg::Spans), None);
    assert_eq!(
        unused_load_memory_warning(Some(6_000_000_000), SignalArg::Metrics).as_deref(),
        Some(
            "warning: a metrics load ignores --load-memory-bytes 6000000000. The budget \
                     applies only to the batches a logs load holds."
        )
    );
    assert_eq!(
        unused_load_memory_warning(Some(6_000_000_000), SignalArg::Spans).as_deref(),
        Some(
            "warning: a spans load ignores --load-memory-bytes 6000000000. The budget \
                     applies only to the batches a logs load holds."
        )
    );
}

/// The future-skew bound is kept and the past-lag bound is relaxed,
/// both anchored on the span's END exactly as `checked_span_interval`
/// anchors them.
#[test]
fn future_skew_is_kept_and_past_lag_is_relaxed() {
    let limits = SpanIngestLimits::default();
    let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
    let build = |start_ns: i64, end_ns: i64| {
        let batch = batch(vec![
            ("trace_id", bin_col(vec![vec![1u8; 16]])),
            ("span_id", bin_col(vec![vec![2u8; 8]])),
            ("name", str_col(vec!["op"])),
            ("start_ns", i64_col(vec![start_ns])),
            ("end_ns", i64_col(vec![end_ns])),
            ("method", str_col(vec!["GET"])),
        ]);
        let mapped_keys = mapping.mapped_keys();
        let cols =
            SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
        build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, &mut 0)
    };

    // The start stays well inside the bound in both cases, so only the
    // END decides: a start-anchored check would admit the second span
    // and this assertion would fail.
    let at_bound = NOW_NS + limits.max_future_skew_ns;
    let span = build(NOW_NS, at_bound).expect("an end exactly at the bound is admitted");
    assert_eq!(span.end_ts_ns, at_bound);
    let over = build(NOW_NS, at_bound + 1)
        .expect_err("an end one ns past the bound is rejected, with its start in window");
    assert!(
        over.contains("more than the max future skew"),
        "the rejection names the bound: {over}"
    );

    // Thirty days old: far past `max_ingest_lag_ns`, and admitted.
    let old = NOW_NS - 30 * 86_400 * 1_000_000_000;
    let span = build(old, old).expect("the past-lag bound is relaxed on this path");
    assert_eq!(span.start_ts_ns, old);
    assert!(
        limits.max_ingest_lag_ns < NOW_NS - old,
        "the fixture really is past the OTLP lag bound"
    );
}

/// One dictionary-encoded `Utf8` column over `vals`, the shape a
/// Parquet trace export's name, id and string attribute columns reach
/// the loader as.
fn dict_str_col(vals: Vec<&str>) -> ArrayRef {
    let arr: DictionaryArray<Int32Type> = vals.into_iter().map(Some).collect();
    Arc::new(arr)
}

/// Each mapped dictionary column is resolved ONCE per batch, and the
/// row loop resolves no dictionary key of its own.
///
/// `normalized_keys` builds a key vector the size of the whole batch on
/// every call, so a per-cell resolution costs O(rows^2) per dictionary
/// column. Counting both resolutions pins the shape rather than a
/// duration: one per dictionary column, none per cell, whatever the row
/// count is.
#[test]
fn dictionary_columns_are_resolved_once_per_batch() {
    const ROWS: usize = 256;
    let limits = SpanIngestLimits::default();
    let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
    let trace_hex = hex::encode([1u8; 16]);
    let span_hex = hex::encode([2u8; 8]);
    // Four dictionary columns (both ids, the name, the one mapped
    // attribute) beside two plain integer columns.
    let batch = batch(vec![
        ("trace_id", dict_str_col(vec![trace_hex.as_str(); ROWS])),
        ("span_id", dict_str_col(vec![span_hex.as_str(); ROWS])),
        ("name", dict_str_col(vec!["op"; ROWS])),
        ("start_ns", i64_col(vec![NOW_NS; ROWS])),
        ("end_ns", i64_col(vec![NOW_NS; ROWS])),
        ("method", dict_str_col(vec!["GET"; ROWS])),
    ]);

    let counters = dict_counters();
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    assert_eq!(
        counters.columns(),
        4,
        "each of the four dictionary columns is resolved exactly once"
    );
    assert!(
        !matches!(
            cols.col(&batch, cols.name).data_type(),
            DataType::Dictionary(_, _)
        ),
        "the row readers index a resolved column, not the dictionary: {:?}",
        cols.col(&batch, cols.name).data_type()
    );

    for row in 0..ROWS {
        let span = build_span(&batch, &cols, &mapping, &limits, NOW_NS, row, &mut 0)
            .expect("every row builds");
        assert_eq!(span.name, "op", "the resolved column reads the same values");
        assert_eq!(span.trace_id, [1u8; 16]);
        assert_eq!(span.span_id, [2u8; 8]);
        assert_eq!(
            span.attrs,
            vec![("http.method".to_string(), "GET".to_string())]
        );
    }
    assert_eq!(
        counters.cell_keys(),
        0,
        "no row reader resolves a dictionary key of its own, over {ROWS} rows"
    );
    assert_eq!(
        counters.columns(),
        4,
        "the row loop resolves no further columns"
    );
}

/// One `attrs` map cell: `None` is a null cell, a `None` key or value
/// a null one.
type DictMapCell = Option<Vec<(Option<String>, Option<String>)>>;

/// A map column whose keys and values are both dictionary-encoded
/// `Utf8`, one cell per row.
fn dict_attrs_map(cells: &[DictMapCell]) -> ArrayRef {
    use arrow::array::StructArray;
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Fields;

    let entries: Vec<&(Option<String>, Option<String>)> =
        cells.iter().flatten().flatten().collect();
    let keys: DictionaryArray<Int32Type> = entries.iter().map(|(key, _)| key.as_deref()).collect();
    let values: DictionaryArray<Int32Type> =
        entries.iter().map(|(_, value)| value.as_deref()).collect();
    let mut offsets = vec![0i32];
    for cell in cells {
        let len = cell.as_ref().map_or(0, Vec::len);
        offsets.push(offsets[offsets.len() - 1] + len as i32);
    }
    // Arrow declares a map's key field non-nullable, and a struct
    // refuses a null in a non-nullable child, so a fixture with a null
    // key has to declare the field nullable to be built at all.
    let null_keys = keys.null_count() > 0;
    let fields = Fields::from(vec![
        Field::new("keys", keys.data_type().clone(), null_keys),
        Field::new("values", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(
        fields.clone(),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        None,
    );
    Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(offsets.into()),
            entries,
            Some(NullBuffer::from(
                cells.iter().map(Option::is_some).collect::<Vec<_>>(),
            )),
            false,
        )
        .expect("map array"),
    )
}

fn some_entries(entries: &[(&str, Option<&str>)]) -> DictMapCell {
    Some(
        entries
            .iter()
            .map(|(key, value)| (Some(key.to_string()), value.map(str::to_string)))
            .collect(),
    )
}

/// One row per `attrs_map_column` outcome, over a map whose keys and
/// values are dictionary-encoded: plain entries, a null cell, a null
/// value, an over-cap value, each per-entry refusal (a null key among
/// them, alone and with a null value), the per-record cap,
/// and a refusal that sits after the cap is passed, which the row
/// still reports rather than the cap.
fn dict_attrs_map_fixture() -> (SpansMapping, RecordBatch) {
    let cap = LOADER_MAX_ATTRIBUTES_PER_RECORD;
    let long = "v".repeat(SpanIngestLimits::default().max_attribute_value_len + 1);
    let numbered = |count: usize| -> Vec<(Option<String>, Option<String>)> {
        (0..count)
            .map(|i| (Some(format!("k{i:04}")), Some("v".to_string())))
            .collect()
    };
    let mut reserved_after_cap = numbered(cap);
    reserved_after_cap.push((Some("_kind".to_string()), Some("server".to_string())));
    let mut duplicate_after_cap = numbered(cap);
    duplicate_after_cap.push((Some("k0000".to_string()), Some("w".to_string())));
    let cells: Vec<DictMapCell> = vec![
        some_entries(&[("zone", Some("eu")), ("peer", Some("db"))]),
        None,
        some_entries(&[
            ("zone", None),
            ("peer", Some("db")),
            ("blob", Some(long.as_str())),
            ("zone", Some("us")),
        ]),
        some_entries(&[("peer", Some("db")), ("http.method", Some("POST"))]),
        some_entries(&[("_kind", Some("server"))]),
        some_entries(&[("zone", Some("eu")), ("zone", Some("us"))]),
        Some(vec![
            (Some("peer".to_string()), Some("db".to_string())),
            (None, Some("x".to_string())),
        ]),
        Some(vec![(None, None)]),
        some_entries(&[]),
        Some(numbered(cap - 1)),
        Some(numbered(cap)),
        Some(numbered(cap + 5)),
        Some(reserved_after_cap),
        Some(duplicate_after_cap),
        some_entries(&[("zone", Some("eu"))]),
    ];
    let rows = cells.len();
    let text = MAPPING_TOML.replace("[spans]\n", "[spans]\nattrs_map_column = \"attrs\"\n");
    let mapping = parse_spans_mapping(&text).expect("valid mapping");
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]; rows])),
        ("span_id", bin_col(vec![vec![2u8; 8]; rows])),
        ("name", str_col(vec!["op"; rows])),
        ("start_ns", i64_col(vec![NOW_NS; rows])),
        ("end_ns", i64_col(vec![NOW_NS; rows])),
        ("method", str_col(vec!["GET"; rows])),
        ("attrs", dict_attrs_map(&cells)),
    ]);
    (mapping, batch)
}

/// The map's key and value dictionaries are resolved once, when the
/// batch's columns are, and no entry resolves a dictionary key of its
/// own: a per-entry resolution costs O(entries) each.
#[test]
fn attrs_map_dictionaries_are_resolved_once_per_batch() {
    let limits = SpanIngestLimits::default();
    let (mapping, batch) = dict_attrs_map_fixture();
    let counters = dict_counters();
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    assert_eq!(
        counters.columns(),
        2,
        "the map's key and value dictionaries, each once"
    );
    for row in 0..batch.num_rows() {
        let _ = build_span(&batch, &cols, &mapping, &limits, NOW_NS, row, &mut 0);
    }
    assert_eq!(
        counters.cell_keys(),
        0,
        "no entry resolves a dictionary key of its own"
    );
    assert_eq!(counters.columns(), 2, "the row loop resolves no column");
}

/// A map type with the given key and value types.
fn map_type(key: DataType, value: DataType) -> DataType {
    let fields = arrow::datatypes::Fields::from(vec![
        Field::new("keys", key, false),
        Field::new("values", value, true),
    ]);
    DataType::Map(
        Arc::new(Field::new("entries", DataType::Struct(fields), false)),
        false,
    )
}

fn assert_not_a_string_map(data_type: DataType) {
    let err = check_attrs_map_column(&data_type, "attrs")
        .expect_err("a map that is not of strings is refused");
    assert_eq!(
        err,
        format!(
            "attrs_map_column \"attrs\" has type {data_type:?}; expected a map of string \
                     keys to string values"
        )
    );
}

/// A map whose values are not strings is refused by name.
#[test]
fn an_attrs_map_of_non_string_values_is_refused() {
    check_attrs_map_column(&map_type(DataType::Utf8, DataType::Utf8), "attrs")
        .expect("a map of strings passes");
    assert_not_a_string_map(map_type(DataType::Utf8, DataType::Int64));
}

/// A map whose keys are not strings is refused by name.
#[test]
fn an_attrs_map_of_non_string_keys_is_refused() {
    check_attrs_map_column(&map_type(DataType::LargeUtf8, DataType::Utf8), "attrs")
        .expect("a map of strings passes");
    assert_not_a_string_map(map_type(DataType::Int64, DataType::Utf8));
}

/// Every row's stored attributes and dropped count, or its refusal,
/// exactly as the spans load builds them.
#[test]
fn a_dictionary_encoded_attrs_map_reads_every_outcome_exactly() {
    let cap = LOADER_MAX_ATTRIBUTES_PER_RECORD;
    let limits = SpanIngestLimits::default();
    let (mapping, batch) = dict_attrs_map_fixture();
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    /// A row's stored attributes and dropped count, or its refusal.
    type Outcome = Result<(Vec<(String, String)>, u64), String>;
    let got: Vec<Outcome> = (0..batch.num_rows())
        .map(|row| {
            let mut dropped = 0;
            build_span(&batch, &cols, &mapping, &limits, NOW_NS, row, &mut dropped)
                .map(|span| (span.attrs, dropped))
        })
        .collect();

    let pair = |key: &str, value: &str| (key.to_string(), value.to_string());
    let method = pair("http.method", "GET");
    let mut at_cap = vec![method.clone()];
    at_cap.extend((0..cap - 1).map(|i| pair(&format!("k{i:04}"), "v")));
    let over_cap = |count: usize| {
        format!(
            "span carries {count} attributes with its attrs_map_column entries, more \
                     than the loader per-record cap of {cap}"
        )
    };
    let collision = "attrs_map_column \"attrs\" holds the key \"http.method\", which the \
                             mapping also reads from the column \"method\". A span carries one \
                             merged attrs map with unique keys, so one of the two would never \
                             reach the record; drop the key from the map or the entry from the \
                             mapping.";
    let reserved = "attrs_map_column \"attrs\" holds the reserved attribute key \
                            \"_kind\", which holds a span field this version does not map";
    let null_key = "attrs_map_column \"attrs\" holds a null key";
    let twice = |key: &str| {
        format!(
            "attrs_map_column \"attrs\" holds the key {key:?} twice. A span carries one \
                     merged attrs map with unique keys, so one of the two would never reach the \
                     record."
        )
    };
    let want: Vec<Outcome> = vec![
        Ok((
            vec![method.clone(), pair("peer", "db"), pair("zone", "eu")],
            0,
        )),
        Ok((vec![method.clone()], 0)),
        Ok((
            vec![method.clone(), pair("peer", "db"), pair("zone", "us")],
            1,
        )),
        Err(collision.to_string()),
        Err(reserved.to_string()),
        Err(twice("zone")),
        Err(null_key.to_string()),
        Err(null_key.to_string()),
        Ok((vec![method.clone()], 0)),
        Ok((at_cap, 0)),
        Err(over_cap(cap + 1)),
        Err(over_cap(cap + 6)),
        Err(reserved.to_string()),
        Err(twice("k0000")),
        Ok((vec![method, pair("zone", "eu")], 0)),
    ];
    assert_eq!(got.len(), want.len());
    for (row, (got, want)) in got.iter().zip(&want).enumerate() {
        assert_eq!(got, want, "row {row}");
    }
}

/// The rows `--skip-rows` keeps read the outcomes they read in the whole
/// batch. The batch slice it takes starts the map's offsets past 0 while
/// the key and value children stay the whole batch's, so an entry is
/// found by the offset itself, not by its distance from the first.
#[test]
fn a_sliced_attrs_map_reads_the_rows_it_kept() {
    let limits = SpanIngestLimits::default();
    let (mapping, batch) = dict_attrs_map_fixture();
    type Outcome = Result<(Vec<(String, String)>, u64), String>;
    let outcomes = |batch: &RecordBatch| -> Vec<Outcome> {
        let mapped_keys = mapping.mapped_keys();
        let cols =
            SpansColumnIndex::resolve(batch, &mapping, &mapped_keys).expect("columns resolve");
        (0..batch.num_rows())
            .map(|row| {
                let mut dropped = 0;
                build_span(batch, &cols, &mapping, &limits, NOW_NS, row, &mut dropped)
                    .map(|span| (span.attrs, dropped))
            })
            .collect()
    };
    let whole = outcomes(&batch);
    for cut in 1..batch.num_rows() {
        let sliced = batch.slice(cut, batch.num_rows() - cut);
        let first = sliced
            .column_by_name("attrs")
            .expect("the map column")
            .as_map()
            .value_offsets()[0];
        assert_ne!(first, 0, "the slice at {cut} starts the offsets past 0");
        assert_eq!(outcomes(&sliced), whole[cut..], "the slice at {cut}");
    }
}

/// A map key a `[[spans.resource_attribute]]` names is refused as one a
/// `[[spans.attribute]]` names is: both lists feed the one merged map.
#[test]
fn an_attrs_map_key_a_resource_attribute_names_is_refused() {
    let limits = SpanIngestLimits::default();
    let text = format!(
        "{}\n[[spans.resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype \
                 = \"str\"\n",
        MAPPING_TOML.replace("[spans]\n", "[spans]\nattrs_map_column = \"attrs\"\n")
    );
    let mapping = parse_spans_mapping(&text).expect("valid mapping");
    let cells = vec![
        some_entries(&[("zone", Some("eu"))]),
        some_entries(&[("service.name", Some("other"))]),
    ];
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]; 2])),
        ("span_id", bin_col(vec![vec![2u8; 8]; 2])),
        ("name", str_col(vec!["op"; 2])),
        ("start_ns", i64_col(vec![NOW_NS; 2])),
        ("end_ns", i64_col(vec![NOW_NS; 2])),
        ("method", str_col(vec!["GET"; 2])),
        ("svc", str_col(vec!["cart"; 2])),
        ("attrs", dict_attrs_map(&cells)),
    ]);
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    let got: Vec<Result<Vec<(String, String)>, String>> = (0..2)
        .map(|row| {
            build_span(&batch, &cols, &mapping, &limits, NOW_NS, row, &mut 0).map(|span| span.attrs)
        })
        .collect();
    let pair = |key: &str, value: &str| (key.to_string(), value.to_string());
    assert_eq!(
        got,
        vec![
            Ok(vec![
                pair("http.method", "GET"),
                pair("service.name", "cart"),
                pair("zone", "eu"),
            ]),
            Err(
                "attrs_map_column \"attrs\" holds the key \"service.name\", which the \
                         mapping also reads from the column \"svc\". A span carries one merged \
                         attrs map with unique keys, so one of the two would never reach the \
                         record; drop the key from the map or the entry from the mapping."
                    .to_string()
            ),
        ]
    );
}

/// A map child whose dictionary is empty is answered row by row rather
/// than taking the batch down in arrow's `normalized_keys`, which
/// asserts the dictionary is non-empty. Every entry of such a child is
/// null: a null value skips its entry, a null key refuses its own row,
/// and the rows around it load.
#[test]
fn an_attrs_map_child_with_an_empty_dictionary_is_answered_per_row() {
    use arrow::array::StructArray;
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Fields;

    const ROWS: usize = 3;
    let limits = SpanIngestLimits::default();
    let text = MAPPING_TOML.replace("[spans]\n", "[spans]\nattrs_map_column = \"attrs\"\n");
    let mapping = parse_spans_mapping(&text).expect("valid mapping");
    let empty_dictionary = || -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![None]),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ))
    };
    let plain = |value: &str| -> ArrayRef { Arc::new(StringArray::from(vec![value])) };
    // Row 1 holds the map's one entry; rows 0 and 2 hold none.
    let batch_of = |keys: ArrayRef, values: ArrayRef| -> RecordBatch {
        let fields = Fields::from(vec![
            Field::new("keys", keys.data_type().clone(), keys.null_count() > 0),
            Field::new("values", values.data_type().clone(), true),
        ]);
        let entries = StructArray::new(fields.clone(), vec![keys, values], None);
        let map = MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0, 0, 1, 1].into()),
            entries,
            None,
            false,
        )
        .expect("map array");
        batch(vec![
            ("trace_id", bin_col(vec![vec![1u8; 16]; ROWS])),
            ("span_id", bin_col(vec![vec![2u8; 8]; ROWS])),
            ("name", str_col(vec!["op"; ROWS])),
            ("start_ns", i64_col(vec![NOW_NS; ROWS])),
            ("end_ns", i64_col(vec![NOW_NS; ROWS])),
            ("method", str_col(vec!["GET"; ROWS])),
            ("attrs", Arc::new(map) as ArrayRef),
        ])
    };
    let outcomes = |batch: &RecordBatch| -> Vec<Result<Vec<(String, String)>, String>> {
        let mapped_keys = mapping.mapped_keys();
        let cols =
            SpansColumnIndex::resolve(batch, &mapping, &mapped_keys).expect("columns resolve");
        (0..ROWS)
            .map(|row| {
                build_span(batch, &cols, &mapping, &limits, NOW_NS, row, &mut 0)
                    .map(|span| span.attrs)
            })
            .collect()
    };
    let method = || Ok(vec![("http.method".to_string(), "GET".to_string())]);

    assert_eq!(
        outcomes(&batch_of(plain("zone"), empty_dictionary())),
        vec![method(), method(), method()],
        "an empty value dictionary skips its null entry"
    );
    assert_eq!(
        outcomes(&batch_of(empty_dictionary(), plain("eu"))),
        vec![
            method(),
            Err("attrs_map_column \"attrs\" holds a null key".to_string()),
            method(),
        ],
        "an empty key dictionary refuses only the row holding its entry"
    );
}

/// The map's dictionary children are read in place: the resolved child
/// holds the batch's own dictionary values, not a copy of the values its
/// entries reference. A copy of a `Utf8` child overflows its i32 offsets
/// once the referenced bytes pass 2 GiB, failing the whole batch where
/// each over-cap entry is dropped and counted.
#[test]
fn attrs_map_dictionaries_are_read_in_place() {
    let (mapping, batch) = dict_attrs_map_fixture();
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    let resolved = cols.attrs_map.as_ref().expect("the mapping names a map");
    let source = batch
        .column_by_name("attrs")
        .expect("the map column")
        .as_map();
    for (name, child, own) in [
        ("key", &resolved.keys, source.keys()),
        ("value", &resolved.values, source.values()),
    ] {
        let MapChild::Dictionary { values, .. } = child else {
            panic!("the {name} child is read as a dictionary");
        };
        assert!(
            Arc::ptr_eq(values, own.as_any_dictionary().values()),
            "the {name} child reads the batch's own dictionary values"
        );
    }
}

/// A Parquet round trip over a dictionary-encoded map whose 2049
/// entries all reference one over-cap value: every entry is dropped
/// and counted, and the load succeeds. The referenced bytes stay far
/// below the i32 offset range, so this does not exercise the offset
/// overflow; the in-place read is pinned by
/// `attrs_map_dictionaries_are_read_in_place`.
#[tokio::test]
async fn an_over_cap_dictionary_value_many_entries_reference_is_dropped() {
    use ravel_object_store::memory::MemoryStore;

    const ROWS: usize = 2049;
    let long = "v".repeat(SpanIngestLimits::default().max_attribute_value_len + 1);
    let cells: Vec<DictMapCell> = (0..ROWS)
        .map(|_| some_entries(&[("blob", Some(long.as_str()))]))
        .collect();
    let text = MAPPING_TOML.replace("[spans]\n", "[spans]\nattrs_map_column = \"attrs\"\n");
    let mapping = parse_spans_mapping(&text).expect("valid mapping");
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]; ROWS])),
        (
            "span_id",
            bin_col(
                (0..ROWS as u64)
                    .map(|i| (i + 1).to_be_bytes().to_vec())
                    .collect(),
            ),
        ),
        ("name", str_col(vec!["op"; ROWS])),
        ("start_ns", i64_col(vec![NOW_NS; ROWS])),
        ("end_ns", i64_col(vec![NOW_NS; ROWS])),
        ("method", str_col(vec!["GET"; ROWS])),
        ("attrs", dict_attrs_map(&cells)),
    ]);
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("spans.parquet");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    // The load reads the map's children as dictionaries, so the entries
    // go through the in-place read this test is about.
    let schema = match reader_schema_for_path(&pq).expect("the footer parses") {
        Some(schema) => schema,
        None => ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&pq).expect("open parquet"),
        )
        .expect("reader")
        .schema()
        .clone(),
    };
    let map_type = schema
        .field_with_name("attrs")
        .expect("the map column")
        .data_type();
    let DataType::Map(entries, _) = map_type else {
        panic!("the attrs column reads as a map, not {map_type:?}");
    };
    let DataType::Struct(children) = entries.data_type() else {
        panic!("a map's entries are a struct");
    };
    for child in children {
        assert!(
            matches!(child.data_type(), DataType::Dictionary(_, _)),
            "the map's {} child reads as a dictionary, not {:?}",
            child.name(),
            child.data_type()
        );
    }

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut report = SpansLoadReport::default();
    load_spans_into(
        &mut report,
        store,
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        NOW_NS,
        Arc::new(SystemClock),
    )
    .await
    .expect("the load succeeds");
    assert_eq!(report.rows_processed, ROWS as u64, "every span is kept");
    assert_eq!(
        report.attributes_dropped, ROWS as u64,
        "every entry's over-cap value is dropped and counted"
    );
}

/// `MAPPING_TOML` plus every other string column a spans row reader
/// reads: the parent id, the status message and a resource attribute.
fn every_string_column_mapping() -> SpansMapping {
    let text = MAPPING_TOML.replace(
        "[spans]\n",
        "[spans]\nparent_span_id_column = \"parent\"\nstatus_message_column = \
                 \"status_msg\"\n",
    ) + "\n[[spans.resource_attribute]]\nkey = \"service.name\"\ncolumn = \
                 \"svc\"\ntype = \"str\"\n";
    parse_spans_mapping(&text).expect("valid mapping")
}

/// Five spans whose every string column [`every_string_column_mapping`]
/// maps is dictionary encoded, each row distinct, with null keys and an
/// empty parent among them. Row 2's name is null and refuses its row.
fn every_string_column_dict_batch() -> RecordBatch {
    const ROWS: usize = 5;
    let opt_dict = |vals: Vec<Option<String>>| -> ArrayRef {
        opt_dict_col(vals.iter().map(Option::as_deref).collect())
    };
    let hex_u64 = |i: u64| Some(hex::encode(i.to_be_bytes()));
    let some = |s: &str| Some(s.to_string());
    batch(vec![
        (
            "trace_id",
            opt_dict(
                (0..ROWS)
                    .map(|i| Some(hex::encode([1 + (i % 2) as u8; 16])))
                    .collect(),
            ),
        ),
        (
            "span_id",
            opt_dict((0..ROWS as u64).map(|i| hex_u64(i + 1)).collect()),
        ),
        (
            "parent",
            opt_dict(vec![None, some(""), hex_u64(1), hex_u64(1), hex_u64(4)]),
        ),
        (
            "name",
            opt_dict(vec![some("a"), some("b"), None, some("a"), some("c")]),
        ),
        ("start_ns", i64_col(vec![NOW_NS; ROWS])),
        ("end_ns", i64_col(vec![NOW_NS; ROWS])),
        (
            "status_msg",
            opt_dict(vec![some("ok"), None, some("ok"), some(""), some("boom")]),
        ),
        (
            "method",
            opt_dict(vec![
                some("GET"),
                some("PUT"),
                None,
                some("GET"),
                some("POST"),
            ]),
        ),
        (
            "svc",
            opt_dict(vec![
                some("cart"),
                some("web"),
                some("cart"),
                None,
                some("web"),
            ]),
        ),
    ])
}

/// The spans row path reads every mapped dictionary column in place:
/// each non-null cell is read out of the batch's own dictionary values,
/// not out of a copy of the values the keys reference. A copy of a
/// `Utf8` column overflows its i32 offsets once those bytes pass 2 GiB,
/// failing the whole batch where each over-cap value is dropped and
/// counted.
#[test]
fn mapped_dictionary_columns_are_read_in_place() {
    let limits = SpanIngestLimits::default();
    let mapping = every_string_column_mapping();
    let batch = every_string_column_dict_batch();
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    for name in [
        "trace_id",
        "span_id",
        "parent",
        "name",
        "status_msg",
        "method",
        "svc",
    ] {
        let i = batch.schema().index_of(name).expect("a mapped column");
        let row = (0..batch.num_rows())
            .find(|row| batch.column(i).is_valid(*row))
            .expect("a non-null cell");
        let (read_from, _) = cols.cell(&batch, i, row).expect("the cell reads");
        assert!(
            Arc::ptr_eq(read_from, batch.column(i).as_any_dictionary().values()),
            "the spans row path reads {name} out of the batch's own dictionary values"
        );
    }

    let span = |row: usize| build_span(&batch, &cols, &mapping, &limits, NOW_NS, row, &mut 0);
    let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
    let first = span(0).expect("row 0 builds");
    assert_eq!(
        (
            first.trace_id,
            first.span_id,
            first.parent_span_id,
            first.name.as_str(),
            first.status_message.as_deref(),
        ),
        ([1u8; 16], 1u64.to_be_bytes(), None, "a", Some("ok"))
    );
    assert_eq!(
        first.attrs,
        vec![pair("http.method", "GET"), pair("service.name", "cart")]
    );
    let second = span(1).expect("row 1 builds");
    assert_eq!(
        (second.parent_span_id, second.status_message.as_deref()),
        (None, None),
        "an empty parent is a root and a null message is no message"
    );
    assert_eq!(
        span(2).expect_err("a null name is refused"),
        "name column \"name\" is null"
    );
    let fourth = span(3).expect("row 3 builds");
    assert_eq!(
        (
            fourth.parent_span_id,
            fourth.status_message.as_deref(),
            fourth.attrs
        ),
        (
            Some(1u64.to_be_bytes()),
            None,
            vec![pair("http.method", "GET")]
        ),
        "an empty message is no message and a null resource key no attribute"
    );
}

/// The rows `--skip-rows` keeps read the outcomes they read in the whole
/// batch. The slice it takes keeps each dictionary's whole values array
/// and shifts only the keys, so a cell is found by the sliced key, not
/// by the row's position in the values.
#[test]
fn a_sliced_batch_reads_the_dictionary_rows_it_kept() {
    let limits = SpanIngestLimits::default();
    let mapping = every_string_column_mapping();
    let batch = every_string_column_dict_batch();
    let mapped_keys = mapping.mapped_keys();
    let outcomes = |batch: &RecordBatch| -> Vec<Result<NormalizedSpan, String>> {
        let cols =
            SpansColumnIndex::resolve(batch, &mapping, &mapped_keys).expect("columns resolve");
        (0..batch.num_rows())
            .map(|row| build_span(batch, &cols, &mapping, &limits, NOW_NS, row, &mut 0))
            .collect()
    };
    let whole = outcomes(&batch);
    assert_eq!(
        whole.iter().filter(|o| o.is_ok()).count(),
        4,
        "every row but the null-name one builds"
    );
    let name = batch.schema().index_of("name").expect("the name column");
    for cut in 1..batch.num_rows() {
        let sliced = batch.slice(cut, batch.num_rows() - cut);
        let cols =
            SpansColumnIndex::resolve(&sliced, &mapping, &mapped_keys).expect("columns resolve");
        let row = (0..sliced.num_rows())
            .find(|row| sliced.column(name).is_valid(*row))
            .expect("a non-null name");
        let (read_from, _) = cols.cell(&sliced, name, row).expect("the cell reads");
        assert!(
            Arc::ptr_eq(read_from, batch.column(name).as_any_dictionary().values()),
            "the slice at {cut} reads the whole batch's own dictionary values"
        );
        assert_eq!(outcomes(&sliced), whole[cut..], "the slice at {cut}");
    }
}

/// A Parquet round trip over a dictionary-encoded attribute column
/// whose 2049 rows all reference one 1 MiB value: every value is over
/// the cap, dropped and counted, and the load succeeds. The referenced
/// bytes pass the i32 offset range (2049 MiB), so the copy arrow's
/// `take` made of this column failed the batch with an offset overflow
/// and no row number.
#[tokio::test]
async fn a_dictionary_attribute_referencing_past_the_offset_range_loads() {
    use parquet::file::properties::WriterProperties;
    use ravel_object_store::memory::MemoryStore;

    const ROWS: usize = 2049;
    const VALUE_LEN: usize = 1 << 20;
    assert!(
        ROWS * VALUE_LEN > i32::MAX as usize,
        "the referenced bytes pass the i32 offset range"
    );
    let long = "v".repeat(VALUE_LEN);
    let method: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![0; ROWS]),
        Arc::new(StringArray::from(vec![long.as_str()])),
    ));
    let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]; ROWS])),
        (
            "span_id",
            bin_col(
                (0..ROWS as u64)
                    .map(|i| (i + 1).to_be_bytes().to_vec())
                    .collect(),
            ),
        ),
        ("name", str_col(vec!["op"; ROWS])),
        ("start_ns", i64_col(vec![NOW_NS; ROWS])),
        ("end_ns", i64_col(vec![NOW_NS; ROWS])),
        ("method", method),
    ]);
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("spans.parquet");
    let file = std::fs::File::create(&pq).expect("create parquet");
    // The dictionary page must hold the one value, or the writer falls
    // back to plain pages and the column no longer reads as a
    // dictionary.
    let props = WriterProperties::builder()
        .set_dictionary_page_size_limit(4 * VALUE_LEN)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), Some(props))
        .expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    assert!(
        std::fs::metadata(&pq).expect("the file").len() < 4 * VALUE_LEN as u64,
        "the file holds the value once, in its dictionary page"
    );

    let schema = match reader_schema_for_path(&pq).expect("the footer parses") {
        Some(schema) => schema,
        None => ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&pq).expect("open parquet"),
        )
        .expect("reader")
        .schema()
        .clone(),
    };
    let method_type = schema
        .field_with_name("method")
        .expect("the method column")
        .data_type();
    assert!(
        matches!(method_type, DataType::Dictionary(_, _)),
        "the method column reads as a dictionary, not {method_type:?}"
    );

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut report = SpansLoadReport::default();
    load_spans_into(
        &mut report,
        store,
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        NOW_NS,
        Arc::new(SystemClock),
    )
    .await
    .expect("the load succeeds");
    assert_eq!(report.rows_processed, ROWS as u64, "every span is kept");
    assert_eq!(
        report.attributes_dropped, ROWS as u64,
        "every row's over-cap value is dropped and counted"
    );
}

/// A dictionary chunk whose dictionary is empty is answered rather than
/// aborting inside arrow's `normalized_keys`, which asserts the values
/// array is non-empty: an all-null chunk resolves to an all-null column
/// of the value type (#708's shape, which a Parquet writer emits), and
/// a key that names a value in an empty dictionary is corrupt input and
/// a typed error.
#[test]
fn an_empty_dictionary_chunk_is_a_typed_error_not_a_panic() {
    let keys = Int32Array::from(vec![None, None]);
    let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let arr: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(keys, values));

    let Some(RowColumn::Plain(resolved)) =
        resolve_dictionary_column(&arr).expect("an all-null chunk is answered, not refused")
    else {
        panic!("a dictionary column resolves, to a column of its value type");
    };
    assert_eq!(
        resolved.data_type(),
        &DataType::Utf8,
        "the column resolves to its value type"
    );
    assert_eq!(resolved.len(), arr.len());
    for row in 0..resolved.len() {
        assert_eq!(
            read_string(&resolved, row).expect("no error"),
            None,
            "every row of an empty-dictionary column is null"
        );
    }

    // The per-cell path reaches the same guard. Arrow asserts on the
    // empty values array whatever the key's nullness is, so this is
    // where the abort was.
    let err = dictionary_key(&arr, 0).expect_err("an empty dictionary names no value");
    assert_eq!(err, EMPTY_DICTIONARY);
}

/// The `Dictionary` fallback arms of [`read_id`] and
/// [`id_cell_is_empty`], which no load reaches because every row path
/// reads resolved columns: a column handed to them unresolved reads by
/// the value its key names.
#[test]
fn unresolved_dictionary_id_cells_read_by_value() {
    let span_hex = hex::encode([2u8; 8]);
    let ids = dict_str_col(vec![span_hex.as_str(), ""]);

    assert_eq!(read_id::<8>(&ids, 0).expect("reads"), Some([2u8; 8]));
    assert_eq!(read_id::<8>(&ids, 1).expect("reads"), None);
    assert!(!id_cell_is_empty(&ids, 0).expect("reads"));
    assert!(
        id_cell_is_empty(&ids, 1).expect("reads"),
        "an empty VALUE is an empty parent, though its key is not"
    );
}

/// A span that ends before it starts is rejected rather than stored
/// with an interval no query window can mean anything against.
#[test]
fn an_end_before_its_start_is_rejected() {
    let limits = SpanIngestLimits::default();
    let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]])),
        ("span_id", bin_col(vec![vec![2u8; 8]])),
        ("name", str_col(vec!["op"])),
        ("start_ns", i64_col(vec![NOW_NS])),
        ("end_ns", i64_col(vec![NOW_NS - 1])),
        ("method", str_col(vec!["GET"])),
    ]);
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    let err = build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, &mut 0)
        .expect_err("end before start");
    assert_eq!(
        err,
        format!(
            "span ends at {} ns, before it starts at {NOW_NS} ns",
            NOW_NS - 1
        )
    );
}

/// [`MAPPING_TOML`] plus the parent, status code and status message
/// columns, so one fixture shape can drive every optional field.
const FULL_MAPPING_TOML: &str = r#"
[spans]
trace_id_column       = "trace_id"
span_id_column        = "span_id"
parent_span_id_column = "parent"
name_column           = "name"
start_ts_column       = "start_ns"
start_ts_unit         = "nanos"
end_ts_column         = "end_ns"
end_ts_unit           = "nanos"
status_code_column    = "status"
status_message_column = "status_msg"

[[spans.attribute]]
key = "http.method"
column = "method"
type = "str"
"#;

fn opt_bin_col(vals: Vec<Option<Vec<u8>>>) -> ArrayRef {
    let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
    Arc::new(BinaryArray::from(refs))
}

fn opt_str_col(vals: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

fn opt_i64_col(vals: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

/// One row over [`FULL_MAPPING_TOML`], every column overridable.
struct Row {
    parent: ArrayRef,
    name: ArrayRef,
    start: ArrayRef,
    end: ArrayRef,
    status: ArrayRef,
    status_msg: ArrayRef,
    method: ArrayRef,
}

impl Default for Row {
    fn default() -> Self {
        Row {
            parent: opt_bin_col(vec![None]),
            name: str_col(vec!["op"]),
            start: i64_col(vec![NOW_NS]),
            end: i64_col(vec![NOW_NS]),
            status: opt_i64_col(vec![None]),
            status_msg: opt_str_col(vec![None]),
            method: str_col(vec!["GET"]),
        }
    }
}

/// Build the single row `row` describes through the real
/// [`build_span`], under [`FULL_MAPPING_TOML`], with the count of
/// attribute values it dropped for being over the cap.
fn build_one_counting(row: Row) -> (Result<NormalizedSpan, String>, u64) {
    let mut dropped = 0u64;
    let span = build_one_into(row, &mut dropped);
    (span, dropped)
}

fn build_one_into(row: Row, dropped: &mut u64) -> Result<NormalizedSpan, String> {
    let limits = SpanIngestLimits::default();
    let mapping = parse_spans_mapping(FULL_MAPPING_TOML).expect("valid mapping");
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]])),
        ("span_id", bin_col(vec![vec![2u8; 8]])),
        ("parent", row.parent),
        ("name", row.name),
        ("start_ns", row.start),
        ("end_ns", row.end),
        ("status", row.status),
        ("status_msg", row.status_msg),
        ("method", row.method),
    ]);
    let mapped_keys = mapping.mapped_keys();
    let cols = SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
    build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, dropped)
}

/// [`build_one_counting`] for the tests that do not care how many
/// attribute values the row lost to the cap.
fn build_one(row: Row) -> Result<NormalizedSpan, String> {
    build_one_counting(row).0
}

/// A zero start takes the load time and a zero end takes the resolved
/// start, the two fallbacks `normalize_span` applies to the zeros an
/// under-instrumented OTLP sender emits.
#[test]
fn a_zero_start_takes_load_time_and_a_zero_end_takes_the_start() {
    let span = build_one(Row {
        start: i64_col(vec![0]),
        end: i64_col(vec![0]),
        ..Row::default()
    })
    .expect("two zeros are admitted");
    assert_eq!(span.start_ts_ns, NOW_NS, "a zero start takes the load time");
    assert_eq!(span.end_ts_ns, NOW_NS, "a zero end takes the start");

    // A zero end beside a REAL start takes that start, not the load
    // time: the two fallbacks are distinguishable only here.
    let earlier = NOW_NS - 5 * 60 * 1_000_000_000;
    let span = build_one(Row {
        start: i64_col(vec![earlier]),
        end: i64_col(vec![0]),
        ..Row::default()
    })
    .expect("a zero end beside a real start is admitted");
    assert_eq!(span.start_ts_ns, earlier);
    assert_eq!(
        span.end_ts_ns, earlier,
        "a zero end takes the span's own start, not the load time"
    );
}

/// A status outside OTLP's `0..=2` enum is `Unset`, including a value
/// too wide for `i64`: `status_code_from_i32` maps everything outside
/// the enum to `Unset`, and a `UInt64` cell above `i64::MAX` is
/// outside it by more, not by a different kind.
#[test]
fn a_status_outside_the_otlp_enum_is_unset() {
    for (code, want) in [
        (0i64, StatusCode::Unset),
        (1, StatusCode::Ok),
        (2, StatusCode::Error),
        (3, StatusCode::Unset),
        (-1, StatusCode::Unset),
        (i64::MAX, StatusCode::Unset),
    ] {
        let span = build_one(Row {
            status: opt_i64_col(vec![Some(code)]),
            ..Row::default()
        })
        .unwrap_or_else(|e| panic!("status {code} is admitted: {e}"));
        assert_eq!(span.status_code, want, "status {code}");
    }

    // A UInt64 column carrying a value above i64::MAX.
    let wide: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
    let span = build_one(Row {
        status: wide,
        ..Row::default()
    })
    .expect("a status above i64::MAX is admitted, not refused");
    assert_eq!(span.status_code, StatusCode::Unset);
}

/// An empty parent value is a root span, in each spelling a Parquet
/// column can carry one; a present, non-empty value of the wrong width
/// is still refused.
#[test]
fn an_empty_parent_is_a_root_and_a_wrong_width_one_is_refused() {
    for (label, cell) in [
        ("a null cell", opt_bin_col(vec![None])),
        ("an empty binary value", opt_bin_col(vec![Some(Vec::new())])),
        ("an empty string", opt_str_col(vec![Some("")])),
    ] {
        let span = build_one(Row {
            parent: cell,
            ..Row::default()
        })
        .unwrap_or_else(|e| panic!("{label} is a root span: {e}"));
        assert_eq!(span.parent_span_id, None, "{label} is a root span");
    }

    // A `FixedSizeBinary(0)` parent column: the schema itself says
    // every row is a root, and `check_id_column` accepts it only
    // BECAUSE it is the parent column (`empty_is_root`). Any other id
    // column of that width is refused for having no width to give.
    let zero_width: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some::<&[u8]>(&[])].into_iter(),
            0,
        )
        .expect("a zero-width fixed-size column"),
    );
    assert_eq!(
        zero_width.data_type(),
        &DataType::FixedSizeBinary(0),
        "the fixture really is the zero-width arm"
    );
    let span = build_one(Row {
        parent: zero_width,
        ..Row::default()
    })
    .expect("a zero-width fixed-size parent column is a file of root spans");
    assert_eq!(span.parent_span_id, None, "every row is a root span");
    let err = check_id_column(&DataType::FixedSizeBinary(0), "span_id", 8, false)
        .expect_err("a zero-width span_id column can produce no id");
    assert_eq!(
        err,
        "id column \"span_id\" is FixedSizeBinary(0), but this id is 8 bytes. Ravel never \
                 pads or truncates an id, so no row of this column can produce one."
    );

    // A hex string of the right width is a parent, so the empty-string
    // case above is emptiness and not "strings are never parents".
    let span = build_one(Row {
        parent: opt_str_col(vec![Some("0202020202020202")]),
        ..Row::default()
    })
    .expect("a 16-character hex parent is read");
    assert_eq!(span.parent_span_id, Some([2u8; 8]));

    let err = build_one(Row {
        parent: opt_bin_col(vec![Some(vec![3u8; 4])]),
        ..Row::default()
    })
    .expect_err("a non-empty 4-byte parent is refused");
    assert!(
        err.contains("is not an 8-byte value"),
        "the refusal names the width: {err}"
    );
}

/// A null timestamp cell and a null name cell are both refused. OTLP
/// has no null for either, so neither has a reading to match; giving
/// one a load-time default would hide a mapping mistake.
#[test]
fn a_null_timestamp_or_name_cell_is_refused() {
    for (want, row) in [
        (
            "start_ts column \"start_ns\" is null",
            Row {
                start: opt_i64_col(vec![None]),
                ..Row::default()
            },
        ),
        (
            "end_ts column \"end_ns\" is null",
            Row {
                end: opt_i64_col(vec![None]),
                ..Row::default()
            },
        ),
        (
            "name column \"name\" is null",
            Row {
                name: opt_str_col(vec![None]),
                ..Row::default()
            },
        ),
    ] {
        let err = build_one(row).expect_err("a null cell is refused");
        assert_eq!(err, want, "the refusal names the mapped column");
    }
}

/// An empty status message is no message, as an OTLP status with an
/// empty `message` field is.
#[test]
fn an_empty_status_message_is_stored_as_no_message() {
    let span = build_one(Row {
        status_msg: opt_str_col(vec![Some("")]),
        ..Row::default()
    })
    .expect("an empty status message is admitted");
    assert_eq!(span.status_message, None);

    let span = build_one(Row {
        status_msg: opt_str_col(vec![Some("deadlock")]),
        ..Row::default()
    })
    .expect("a real status message is admitted");
    assert_eq!(span.status_message.as_deref(), Some("deadlock"));
}

/// An attribute value over the OTLP cap drops THAT attribute and keeps
/// the span, which is `convert_attrs_lossy`'s rule on the OTLP path,
/// and the drop is COUNTED so the load summary can say the stored
/// record is an approximation.
#[test]
fn an_over_cap_attribute_value_is_dropped_and_the_span_kept() {
    let limits = SpanIngestLimits::default();
    let big = "x".repeat(limits.max_attribute_value_len + 1);
    let (span, dropped) = build_one_counting(Row {
        method: str_col(vec![big.as_str()]),
        ..Row::default()
    });
    let span = span.expect("an over-cap attribute value does not reject the span");
    assert_eq!(span.attrs, Vec::new(), "the attribute itself is dropped");
    assert_eq!(dropped, 1, "the drop is counted, exactly once");

    // Exactly at the cap is kept, so the case above is the cap and not
    // the column going missing, and it counts nothing.
    let at_cap = "x".repeat(limits.max_attribute_value_len);
    let (span, dropped) = build_one_counting(Row {
        method: str_col(vec![at_cap.as_str()]),
        ..Row::default()
    });
    let span = span.expect("exactly at the cap is admitted");
    assert_eq!(span.attrs, vec![("http.method".to_string(), at_cap)]);
    assert_eq!(dropped, 0, "nothing was dropped");
}

/// On a FAILED load the `attrs_dropped` line says what the count
/// covers, because the count is taken where each span is BUILT: two
/// values were dropped from a batch whose write never landed, so no
/// stored span is missing them and the success path's wording would be
/// false. The success path keeps that wording.
#[tokio::test]
async fn a_failed_spans_load_says_attrs_dropped_covers_abandoned_batches() {
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
    use ravel_object_store::memory::MemoryStore;

    let limits = SpanIngestLimits::default();
    let big = "x".repeat(limits.max_attribute_value_len + 1);
    // Two rows in one batch, each carrying one over-cap attribute
    // value: two drops counted before any write is attempted.
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16], vec![1u8; 16]])),
        ("span_id", bin_col(vec![vec![2u8; 8], vec![3u8; 8]])),
        ("name", str_col(vec!["op", "op"])),
        ("start_ns", i64_col(vec![NOW_NS, NOW_NS])),
        ("end_ns", i64_col(vec![NOW_NS, NOW_NS])),
        ("method", str_col(vec![big.as_str(), big.as_str()])),
    ]);
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("spans.parquet");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    // Fail every span data-object PUT, so the one batch that was
    // decoded is the one the failure abandons and nothing lands.
    let fault = ScriptedFault::Transient("injected PUT failure".into());
    let mut seq = Sequence::new(Op::Put).with_key_contains("/s/l0/");
    for _ in 0..8 {
        seq = seq.then_fault(fault.clone());
    }
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_sequence(seq),
    ));

    let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
    let mut report = SpansLoadReport::default();
    let err = load_spans_into(
        &mut report,
        store,
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        NOW_NS,
        Arc::new(SystemClock),
    )
    .await
    .expect_err("the scripted PUT fault fails the load");

    assert!(
        matches!(err, LoadError::Flush { .. }),
        "expected a flush failure, got: {err}"
    );
    assert_eq!(
        report.attributes_dropped, 2,
        "both drops are counted, though neither span landed"
    );
    assert_eq!(
        report.rows_processed, 0,
        "no row acked durable, so the count covers spans in no object"
    );
    assert_eq!(
        spans_attrs_dropped_line(&report, AttrsDroppedScope::Failed),
        "  attrs_dropped    : 2 (attribute values over the OTLP value-length cap and \
                 attrs_map_column keys over the key-length cap; counted where each span was \
                 built, so this includes batches the failure abandoned, whose spans are in no \
                 object)"
    );
    assert_eq!(
        spans_attrs_dropped_line(&report, AttrsDroppedScope::Complete),
        "  attrs_dropped    : 2 (attribute values over the OTLP value-length cap and \
                 attrs_map_column keys over the key-length cap; each span was stored without \
                 them)",
        "a load that completed still says the spans were stored without them"
    );
}

/// A row rejection mid-batch still counts the drops of the rows built
/// before it, which the failure-path line claims to cover, and not the
/// rejected row's own.
#[tokio::test]
async fn a_row_rejected_spans_load_counts_the_drops_built_before_it() {
    use ravel_object_store::memory::MemoryStore;

    let limits = SpanIngestLimits::default();
    let big = "x".repeat(limits.max_attribute_value_len + 1);
    // Every row drops its over-cap `method` value. Row 2 is then
    // rejected by the attribute after it, an integer attribute whose
    // cell there is a string, so its own drop is counted before the
    // rejection and must not reach the report.
    let batch = batch(vec![
        ("trace_id", bin_col(vec![vec![1u8; 16]; 3])),
        (
            "span_id",
            bin_col(vec![vec![2u8; 8], vec![3u8; 8], vec![4u8; 8]]),
        ),
        ("name", str_col(vec!["op"; 3])),
        ("start_ns", i64_col(vec![NOW_NS; 3])),
        ("end_ns", i64_col(vec![NOW_NS; 3])),
        ("method", str_col(vec![big.as_str(); 3])),
        (
            "code",
            Arc::new(StringArray::from(vec![None, None, Some("x")])) as ArrayRef,
        ),
    ]);
    let mapping_toml = format!(
        "{MAPPING_TOML}\n[[spans.attribute]]\nkey = \"code\"\ncolumn = \"code\"\ntype = \
                 \"i64\"\n"
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("spans.parquet");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = parse_spans_mapping(&mapping_toml).expect("valid mapping");
    let mut report = SpansLoadReport::default();
    let err = load_spans_into(
        &mut report,
        store,
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        NOW_NS,
        Arc::new(SystemClock),
    )
    .await
    .expect_err("row 2 is rejected");

    assert!(
        matches!(err, LoadError::RowRejected { row: 2, .. }),
        "expected row 2 rejected, got: {err}"
    );
    assert_eq!(
        report.attributes_dropped, 2,
        "the two rows built before the rejection are counted, the rejected row is not"
    );
}

/// The cap applies to the STORED string, so a bytes attribute is
/// measured as its lowercase hex: a value of `cap / 2 + 1` raw bytes
/// is under the cap as bytes and over it as hex, and is dropped.
#[test]
fn a_bytes_attribute_is_measured_as_its_hex_form() {
    let limits = SpanIngestLimits::default();
    let cap = limits.max_attribute_value_len;
    let mapping_toml = format!(
        "{FULL_MAPPING_TOML}\n[[spans.attribute]]\nkey = \"request.digest\"\ncolumn = \
                 \"digest\"\ntype = \"bytes\"\n"
    );
    let mapping = parse_spans_mapping(&mapping_toml).expect("valid mapping");
    let build = |raw: Vec<u8>| {
        let batch = batch(vec![
            ("trace_id", bin_col(vec![vec![1u8; 16]])),
            ("span_id", bin_col(vec![vec![2u8; 8]])),
            ("parent", opt_bin_col(vec![None])),
            ("name", str_col(vec!["op"])),
            ("start_ns", i64_col(vec![NOW_NS])),
            ("end_ns", i64_col(vec![NOW_NS])),
            ("status", opt_i64_col(vec![None])),
            ("status_msg", opt_str_col(vec![None])),
            ("method", str_col(vec!["GET"])),
            ("digest", bin_col(vec![raw])),
        ]);
        let mapped_keys = mapping.mapped_keys();
        let cols =
            SpansColumnIndex::resolve(&batch, &mapping, &mapped_keys).expect("columns resolve");
        let mut dropped = 0u64;
        let span = build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, &mut dropped)
            .expect("an over-cap attribute value does not reject the span");
        (span, dropped)
    };

    // cap/2 + 1 raw bytes: under the cap as bytes, two characters over
    // it as hex. Measuring the raw bytes would keep this attribute and
    // this assertion would fail.
    let raw_len = cap / 2 + 1;
    assert!(raw_len <= cap, "the raw value is itself under the cap");
    assert_eq!(raw_len * 2, cap + 2, "its hex form is over the cap");
    let (span, dropped) = build(vec![0xABu8; raw_len]);
    assert_eq!(
        span.attrs,
        vec![("http.method".to_string(), "GET".to_string())],
        "the digest is dropped and its neighbour is kept"
    );
    assert_eq!(dropped, 1, "the drop is counted");

    // cap/2 raw bytes hexes to exactly the cap and is kept, so the case
    // above is the hex length and not the column going missing.
    let (span, dropped) = build(vec![0xABu8; cap / 2]);
    assert_eq!(
        span.attrs,
        vec![
            ("http.method".to_string(), "GET".to_string()),
            ("request.digest".to_string(), "ab".repeat(cap / 2)),
        ]
    );
    assert_eq!(dropped, 0, "nothing was dropped");
}

/// A negative start or end is refused, naming the unit each was read
/// in: OTLP's two `u64` timestamps have no negative to match against.
#[test]
fn a_negative_timestamp_is_refused() {
    let err = build_one(Row {
        start: i64_col(vec![-1]),
        ..Row::default()
    })
    .expect_err("a negative start is refused");
    assert_eq!(
        err,
        format!(
            "span timestamps are before the Unix epoch (start -1 ns, read as \
                     start_ts_unit = nanos; end {NOW_NS} ns, read as end_ts_unit = nanos); a \
                     timestamp column holds a negative value"
        )
    );

    let err = build_one(Row {
        start: i64_col(vec![-2]),
        end: i64_col(vec![-1]),
        ..Row::default()
    })
    .expect_err("a wholly negative interval is refused too");
    assert!(err.contains("before the Unix epoch"), "{err}");

    // Zero is the epoch, not a negative, and it takes the zero
    // fallbacks rather than this refusal.
    build_one(Row {
        start: i64_col(vec![0]),
        end: i64_col(vec![0]),
        ..Row::default()
    })
    .expect("zero is the fallback case, not a negative one");
}

/// A native `Timestamp(Second)` start scales by its own unit, not by
/// the declared `start_ts_unit`, so the refusal names seconds for the
/// start, while the integer end still names `end_ts_unit`.
#[test]
fn a_negative_native_start_names_its_own_unit() {
    let err = build_one(Row {
        start: Arc::new(TimestampSecondArray::from(vec![-5])) as ArrayRef,
        ..Row::default()
    })
    .expect_err("a negative native start is refused");
    assert_eq!(
        err,
        format!(
            "span timestamps are before the Unix epoch (start -5000000000 ns, read in \
                     the column's own Timestamp unit, seconds; end {NOW_NS} ns, read as \
                     end_ts_unit = nanos); a timestamp column holds a negative value"
        )
    );
}

/// A zero end cell takes the start, so a negative start makes the end
/// negative too. The end was never read from the end column, and the
/// refusal says it came from the start rather than naming
/// `end_ts_unit`, under which the end column holds a 0.
#[test]
fn a_substituted_end_names_the_start_as_its_source() {
    let err = build_one(Row {
        start: Arc::new(TimestampSecondArray::from(vec![-5])) as ArrayRef,
        end: i64_col(vec![0]),
        ..Row::default()
    })
    .expect_err("a negative start with a zero end is refused");
    assert_eq!(
        err,
        "span timestamps are before the Unix epoch (start -5000000000 ns, read in the \
                 column's own Timestamp unit, seconds; end -5000000000 ns, taken from start_ts \
                 because end_ts is 0); a timestamp column holds a negative value"
    );
}

/// A zero start cell takes load time, so a negative end is refused
/// beside a start the start column does not hold. The refusal says the
/// start came from load time rather than naming `start_ts_unit`.
#[test]
fn a_substituted_start_names_load_time_as_its_source() {
    let err = build_one(Row {
        start: i64_col(vec![0]),
        end: i64_col(vec![-5]),
        ..Row::default()
    })
    .expect_err("a zero start with a negative end is refused");
    assert_eq!(
        err,
        format!(
            "span timestamps are before the Unix epoch (start {NOW_NS} ns, taken from \
                     load time because start_ts is 0; end -5 ns, read as end_ts_unit = nanos); \
                     a timestamp column holds a negative value"
        )
    );
}

/// Both attribute-count caps are properties of the mapping, so both
/// are refused at mapping parse rather than per row.
#[test]
fn the_attribute_count_caps_are_checked_against_the_mapping() {
    let attr_list = |list: &str, key_prefix: &str, n: usize| {
        let mut text = MAPPING_TOML.to_string();
        for i in 0..n {
            text.push_str(&format!(
                "\n[[spans.{list}]]\nkey = \"{key_prefix}{i}\"\ncolumn = \
                         \"c{key_prefix}{i}\"\ntype = \"str\"\n"
            ));
        }
        text
    };

    // MAPPING_TOML already declares one [[spans.attribute]], so the
    // cap is reached at `cap - 1` more.
    let cap = LOADER_MAX_ATTRIBUTES_PER_RECORD;
    parse_spans_mapping(&attr_list("attribute", "a", cap - 1))
        .expect("exactly at the loader per-record cap is accepted");
    let err = parse_spans_mapping(&attr_list("attribute", "a", cap))
        .expect_err("one column past the loader per-record cap");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert_eq!(
        message,
        format!(
            "--mapping [spans] declares {} attribute columns, more than the loader \
                     per-record cap of {cap}",
            cap + 1
        )
    );

    let resource_cap = SpanIngestLimits::default().max_resource_attributes;
    parse_spans_mapping(&attr_list("resource_attribute", "r", resource_cap))
        .expect("exactly at the OTLP per-resource cap is accepted");
    let err = parse_spans_mapping(&attr_list("resource_attribute", "r", resource_cap + 1))
        .expect_err("one column past OTLP's max_resource_attributes");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert_eq!(
        message,
        format!(
            "--mapping [spans] declares {} resource_attribute columns, more than the OTLP \
                     per-resource cap of {resource_cap}",
            resource_cap + 1
        )
    );
}

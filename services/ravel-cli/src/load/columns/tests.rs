use arrow::array::DictionaryArray;
use arrow::datatypes::Int32Type;

use super::*;
use crate::load::test_support::*;

fn build_row(
    batch: &RecordBatch,
    mapping: &Mapping,
    row: usize,
) -> Result<NormalizedLogRecord, String> {
    let cols = ColumnIndex::resolve(batch, mapping).expect("resolve columns");
    build_record(
        batch,
        &cols,
        mapping,
        &LogIngestLimits::default(),
        NOW_NS,
        row,
    )
}

#[test]
fn future_skew_beyond_the_bound_is_rejected() {
    let limits = LogIngestLimits::default();
    let m = base_mapping();
    let b = batch(vec![(
        "ts",
        i64_col(vec![NOW_NS + limits.max_future_skew_ns + 1]),
    )]);
    let err = build_row(&b, &m, 0).expect_err("a far-future row must be rejected");
    assert!(err.contains("future skew"), "{err}");
}

#[test]
fn event_exactly_at_the_future_skew_bound_is_accepted() {
    let limits = LogIngestLimits::default();
    let m = base_mapping();
    let b = batch(vec![(
        "ts",
        i64_col(vec![NOW_NS + limits.max_future_skew_ns]),
    )]);
    let rec = build_row(&b, &m, 0).expect("the bound itself passes");
    assert_eq!(rec.ts_ns, NOW_NS + limits.max_future_skew_ns);
}

/// The deliberate ADR-0089 relaxation: a 2013-era timestamp lagging load
/// time by a decade is accepted, where OTLP would reject it as `TooOld`.
#[test]
fn past_event_time_lag_is_not_rejected() {
    let m = base_mapping();
    let ts_2013 = 1_356_998_400_000_000_000; // 2013-01-01
    let b = batch(vec![("ts", i64_col(vec![ts_2013]))]);
    let rec = build_row(&b, &m, 0).expect("a decade-old event is admitted, not rejected");
    assert_eq!(rec.ts_ns, ts_2013);
}

#[test]
fn oversized_body_is_rejected_at_the_otlp_bound() {
    let limits = LogIngestLimits::default();
    let mut m = base_mapping();
    m.body_column = Some("body".to_string());
    let big = "x".repeat(limits.max_body_len + 1);
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS])),
        ("body", str_col(vec![big.as_str()])),
    ]);
    let err = build_row(&b, &m, 0).expect_err("an oversized body is rejected");
    assert!(err.contains("body is"), "{err}");
}

#[test]
fn oversized_attribute_key_is_rejected_at_the_otlp_bound() {
    let limits = LogIngestLimits::default();
    let mut m = base_mapping();
    let long_key = "k".repeat(limits.max_attribute_key_len + 1);
    m.attributes = vec![attr(&long_key, "v", ColType::Str)];
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS])),
        ("v", str_col(vec!["value"])),
    ]);
    let err = build_row(&b, &m, 0).expect_err("an oversized attribute key is rejected");
    assert!(err.contains("attribute key"), "{err}");
}

#[test]
fn oversized_attribute_value_is_rejected_at_the_otlp_bound() {
    let limits = LogIngestLimits::default();
    let mut m = base_mapping();
    m.attributes = vec![attr("k", "v", ColType::Str)];
    let big = "x".repeat(limits.max_attribute_value_len + 1);
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS])),
        ("v", str_col(vec![big.as_str()])),
    ]);
    let err = build_row(&b, &m, 0).expect_err("an oversized attribute value is rejected");
    assert!(err.contains("value is"), "{err}");
}

#[test]
fn record_attributes_at_the_loader_cap_pass_and_over_it_are_rejected() {
    // Build `cap + 1` record-attribute columns; one row over the cap is
    // rejected (not silently truncated), and exactly-at-cap passes.
    let cap = LOADER_MAX_ATTRIBUTES_PER_RECORD;
    let over = cap + 1;
    let mut cols: Vec<(String, ArrayRef)> = vec![("ts".to_string(), i64_col(vec![NOW_NS]))];
    let mut attrs = Vec::new();
    for i in 0..over {
        let name = format!("a{i}");
        cols.push((name.clone(), i64_col(vec![i as i64])));
        attrs.push(attr(&name, &name, ColType::I64));
    }
    let b = RecordBatch::try_from_iter(cols).expect("wide batch");

    let mut m_over = base_mapping();
    m_over.attributes = attrs.clone();
    let err = build_row(&b, &m_over, 0).expect_err("over the loader cap must be rejected");
    assert!(err.contains("loader per-record cap"), "{err}");

    let mut m_at = base_mapping();
    m_at.attributes = attrs[..cap].to_vec();
    let rec = build_row(&b, &m_at, 0).expect("exactly at the cap passes");
    assert_eq!(rec.attrs.len(), cap);
}

/// Resource-attribute columns determine stream identity; record-attribute
/// columns never do.
#[test]
fn stream_identity_follows_resource_attributes_not_record_attributes() {
    let mut m = base_mapping();
    m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
    m.attributes = vec![attr("http.status_code", "status", ColType::I64)];
    // Row 0: svc=api status=1; Row 1: svc=web status=1; Row 2: svc=api status=2.
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS, NOW_NS, NOW_NS])),
        ("svc", str_col(vec!["api", "web", "api"])),
        ("status", i64_col(vec![1, 1, 2])),
    ]);
    let r0 = build_row(&b, &m, 0).expect("row 0");
    let r1 = build_row(&b, &m, 1).expect("row 1");
    let r2 = build_row(&b, &m, 2).expect("row 2");

    assert_ne!(
        r0.stream_id, r1.stream_id,
        "different resource attribute values must produce different streams"
    );
    assert_eq!(
        r0.stream_id, r2.stream_id,
        "a differing record attribute must not change stream identity"
    );
    // The record attribute is carried, typed, in attrs.
    assert_eq!(
        r0.attrs,
        vec![("http.status_code".to_string(), AttrValue::I64(1))]
    );
}

#[test]
fn typed_columns_become_typed_attr_values() {
    let mut m = base_mapping();
    m.attributes = vec![
        attr("s", "s", ColType::Str),
        attr("i", "i", ColType::I64),
        attr("f", "f", ColType::F64),
        attr("b", "b", ColType::Bool),
    ];
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS])),
        ("s", str_col(vec!["hi"])),
        ("i", i64_col(vec![7])),
        ("f", Arc::new(Float64Array::from(vec![1.5f64])) as ArrayRef),
        ("b", Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
    ]);
    let rec = build_row(&b, &m, 0).expect("typed row");
    assert_eq!(
        rec.attrs,
        vec![
            ("s".to_string(), AttrValue::Str("hi".to_string())),
            ("i".to_string(), AttrValue::I64(7)),
            ("f".to_string(), AttrValue::F64(1.5)),
            ("b".to_string(), AttrValue::Bool(true)),
        ]
    );
}

#[test]
fn ts_unit_scales_to_nanoseconds() {
    let mut m = base_mapping();
    m.ts_unit = TsUnit::Millis;
    let b = batch(vec![("ts", i64_col(vec![1_700_000_000_000]))]);
    let rec = build_row(&b, &m, 0).expect("millis ts");
    assert_eq!(rec.ts_ns, 1_700_000_000_000 * 1_000_000);
}

/// #708: a dictionary-encoded string column whose values array is empty (an
/// all-null dictionary chunk a Parquet writer may emit) must resolve to an
/// all-null column. Before the fix, `str_src` called
/// `DictionaryArray::normalized_keys`, which in arrow-array 59.1 asserts the
/// values array is non-empty and panicked here instead.
#[test]
fn empty_dictionary_str_column_is_all_null() {
    let keys = Int32Array::from(vec![None, None, None]);
    let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let dict = DictionaryArray::<Int32Type>::new(keys, values);
    let arr: ArrayRef = Arc::new(dict);

    let src = str_src(&arr);
    assert!(
        matches!(src, StrSrc::AllNull),
        "empty-dictionary string column takes the all-null path"
    );
    for row in 0..arr.len() {
        assert_eq!(
            src.get(row).expect("no error"),
            None,
            "every row of an empty-dictionary column is null"
        );
    }
}

/// #708, binary analogue: an empty-dictionary binary column resolves to an
/// all-null column rather than panicking in `normalized_keys`.
#[test]
fn empty_dictionary_bytes_column_is_all_null() {
    let keys = Int32Array::from(vec![None, None]);
    let values = Arc::new(BinaryArray::from(Vec::<&[u8]>::new())) as ArrayRef;
    let dict = DictionaryArray::<Int32Type>::new(keys, values);
    let arr: ArrayRef = Arc::new(dict);

    let src = bytes_src(&arr);
    assert!(
        matches!(src, BytesSrc::AllNull),
        "empty-dictionary binary column takes the all-null path"
    );
    for row in 0..arr.len() {
        assert_eq!(
            src.get_ref(row).expect("no error"),
            None,
            "every row of an empty-dictionary binary column is null"
        );
    }
}

/// The logs row path reads every mapped dictionary column in place: each
/// non-null cell is read out of the batch's own dictionary values, not out
/// of a copy of the values the keys reference, which overflows a `Utf8`
/// copy's i32 offsets once those bytes pass 2 GiB and fails the batch. A
/// null key still reads as a null cell.
#[test]
fn the_logs_row_path_reads_dictionary_columns_in_place() {
    let trace_hex = hex::encode([1u8; 16]);
    let span_hex = hex::encode([2u8; 8]);
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS; 3])),
        ("body", opt_dict_col(vec![Some("hello"), None, Some("bye")])),
        ("sev", opt_dict_col(vec![Some("WARN"), Some("INFO"), None])),
        (
            "trace_id",
            opt_dict_col(vec![
                Some(trace_hex.as_str()),
                None,
                Some(trace_hex.as_str()),
            ]),
        ),
        (
            "span_id",
            opt_dict_col(vec![Some(span_hex.as_str()), Some(span_hex.as_str()), None]),
        ),
        (
            "svc",
            opt_dict_col(vec![Some("api"), Some("web"), Some("api")]),
        ),
        ("cat", opt_dict_col(vec![None, Some("beta"), Some("alpha")])),
    ]);
    let mut m = base_mapping();
    m.body_column = Some("body".to_string());
    m.severity_text_column = Some("sev".to_string());
    m.trace_id_column = Some("trace_id".to_string());
    m.span_id_column = Some("span_id".to_string());
    m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
    m.attributes = vec![attr("cat", "cat", ColType::Str)];

    let cols = ColumnIndex::resolve(&b, &m).expect("columns resolve");
    for name in ["body", "sev", "trace_id", "span_id", "svc", "cat"] {
        let i = b.schema().index_of(name).expect("a mapped column");
        let own = b.column(i).as_any_dictionary().values();
        let row = (0..b.num_rows())
            .find(|row| b.column(i).is_valid(*row))
            .expect("a non-null cell");
        let (read_from, _) = cols.cell(&b, i, row).expect("the cell reads");
        assert!(
            Arc::ptr_eq(read_from, own),
            "the logs row path reads {name} out of the batch's own dictionary values"
        );
    }

    let limits = LogIngestLimits::default();
    let records: Vec<NormalizedLogRecord> = (0..b.num_rows())
        .map(|row| build_record(&b, &cols, &m, &limits, NOW_NS, row).expect("the row builds"))
        .collect();
    let str_attr = |key: &str, value: &str| (key.to_string(), AttrValue::Str(value.into()));
    let got: Vec<_> = records
        .iter()
        .map(|r| {
            (
                r.body.as_str(),
                r.severity_text.as_str(),
                r.trace_id,
                r.span_id,
                r.attrs.clone(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("hello", "WARN", Some([1u8; 16]), Some([2u8; 8]), vec![]),
            (
                "",
                "INFO",
                None,
                Some([2u8; 8]),
                vec![str_attr("cat", "beta")]
            ),
            (
                "bye",
                "",
                Some([1u8; 16]),
                None,
                vec![str_attr("cat", "alpha")]
            ),
        ]
    );
    assert_eq!(
        records[0].stream_id, records[2].stream_id,
        "the resource dictionary reads one value for both of its keys"
    );
    assert_ne!(records[0].stream_id, records[1].stream_id);
}

/// The columnar path's id columns are read in place: a dictionary id
/// column's cells are read out of the batch's own dictionary values, by the
/// hex rule for a string dictionary and the binary rule otherwise, and a
/// null key is a null cell.
#[test]
fn the_columnar_id_columns_are_read_in_place() {
    let trace_hex = hex::encode([1u8; 16]);
    let hex_ids = opt_dict_col(vec![Some(trace_hex.as_str()), None, Some("not hex")]);
    let bin_ids: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![Some(0), None, Some(0)]),
        Arc::new(BinaryArray::from(vec![[2u8; 8].as_slice()])),
    ));
    for (name, arr, want_hex, want) in [
        (
            "hex",
            &hex_ids,
            true,
            vec![Some([1u8; 16].to_vec()), None, None],
        ),
        (
            "binary",
            &bin_ids,
            false,
            vec![Some([2u8; 8].to_vec()), None, Some([2u8; 8].to_vec())],
        ),
    ] {
        let column = id_column(arr).expect("the id column resolves");
        let src = id_column_src(&column);
        let IdSrc::Dict { column: dict, hex } = &src else {
            panic!("the {name} id column is read as a dictionary");
        };
        assert!(
            Arc::ptr_eq(&dict.values, arr.as_any_dictionary().values()),
            "the {name} id column reads the batch's own dictionary values"
        );
        assert_eq!(*hex, want_hex, "the {name} id column's read rule");
        let got: Vec<Option<Vec<u8>>> = (0..arr.len())
            .map(|row| src.get(row).expect("the cell reads"))
            .collect();
        assert_eq!(got, want, "the {name} id column's cells");
    }
}

mod logs_ids_and_negative_timestamps {
    use std::path::PathBuf;

    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const TRACE_A: &str = "0102030405060708090a0b0c0d0e0f10";
    const TRACE_B: &str = "a1a2a3a4a5a6a7a8a9aaabacadaeafb0";
    const SPAN_A: &str = "1112131415161718";
    const SPAN_B: &str = "b1b2b3b4b5b6b7b8";

    /// Write `batch` to a Parquet file with dictionary encoding on or off
    /// for every column.
    fn write_with(batch: &RecordBatch, dictionary: bool) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("in.parquet");
        let props = WriterProperties::builder()
            .set_dictionary_enabled(dictionary)
            .build();
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("arrow writer");
        w.write(batch).expect("write batch");
        w.close().expect("close writer");
        (dir, pq)
    }

    /// A stored log record's `(ts, trace_id, span_id)`.
    type StoredIds = (i64, Option<[u8; 16]>, Option<[u8; 8]>);

    /// Every stored log record's [`StoredIds`], sorted by ts.
    async fn stored_ids(store: &dyn ObjectStoreBackend) -> Vec<StoredIds> {
        use ravel_logseg::{Predicate, RlogConfig, RlogReader};
        use ravel_object_store::GetRange;

        let cfg = RlogConfig::default();
        let mut out = Vec::new();
        for (key, _) in list_data_objects(store).await {
            let got = store.get(&key, GetRange::Full).await.expect("get object");
            let reader = RlogReader::new(got.data.as_ref(), &cfg).expect("open rlog");
            let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
            out.extend(rows.into_iter().map(|r| (r.ts_ns, r.trace_id, r.span_id)));
        }
        out.sort();
        out
    }

    fn id<const N: usize>(hex_id: &str) -> Option<[u8; N]> {
        hex::decode(hex_id).ok().and_then(|b| b.try_into().ok())
    }

    async fn load_columnar(
        pq: &Path,
        mapping: &Mapping,
    ) -> (Result<LoadReport, LoadError>, Arc<dyn ObjectStoreBackend>) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let result = load(
            Arc::clone(&store),
            pq,
            "acme",
            mapping,
            1,
            1_000,
            None,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await;
        (result, store)
    }

    /// A hex id column loads the same whether its Parquet pages are plain
    /// or dictionary-encoded: the columnar path resolves a dictionary id
    /// column instead of refusing it, and a null cell (a null key, once
    /// encoded) stores no id either way.
    #[tokio::test]
    async fn dictionary_encoded_hex_id_columns_load_like_plain_ones() {
        let ts: Vec<i64> = (0..6).map(|i| NOW_NS - 1_000 + i).collect();
        let trace = vec![
            Some(TRACE_A),
            Some(TRACE_A),
            None,
            Some(TRACE_B),
            Some(TRACE_A),
            Some(TRACE_B),
        ];
        let span = vec![
            Some(SPAN_A),
            Some(SPAN_B),
            Some(SPAN_A),
            None,
            Some(SPAN_B),
            Some(SPAN_A),
        ];
        let b = batch(vec![
            ("ts", i64_col(ts.clone())),
            ("svc", str_col(vec!["api"; 6])),
            (
                "trace_id",
                Arc::new(StringArray::from(trace.clone())) as ArrayRef,
            ),
            (
                "span_id",
                Arc::new(StringArray::from(span.clone())) as ArrayRef,
            ),
        ]);
        let mut m = base_mapping();
        m.trace_id_column = Some("trace_id".to_string());
        m.span_id_column = Some("span_id".to_string());
        m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];

        let (_plain_dir, plain) = write_with(&b, false);
        let (_dict_dir, dict) = write_with(&b, true);
        let id_types = |pq: &Path| -> Vec<DataType> {
            let schema = reader_schema_for(pq);
            ["trace_id", "span_id"]
                .iter()
                .map(|name| {
                    schema.as_ref().map_or(DataType::Utf8, |s| {
                        s.field_with_name(name)
                            .expect("id field")
                            .data_type()
                            .clone()
                    })
                })
                .collect()
        };
        let dict_ty = DataType::Dictionary(Box::new(DICT_KEY_TYPE), Box::new(DataType::Utf8));
        assert_eq!(id_types(&plain), vec![DataType::Utf8, DataType::Utf8]);
        assert_eq!(
            id_types(&dict),
            vec![dict_ty.clone(), dict_ty],
            "the loader reads both id columns of the encoded file as dictionaries"
        );

        let (plain_result, plain_store) = load_columnar(&plain, &m).await;
        let plain_report = plain_result.expect("the plain file loads");
        let (dict_result, dict_store) = load_columnar(&dict, &m).await;
        let dict_report = dict_result.expect("the dictionary-encoded file loads");
        assert_eq!(plain_report.rows_processed, 6);
        assert_eq!(dict_report.rows_processed, 6);
        assert!(
            dict_report.columnar_batches_built > 0,
            "the columnar path ran"
        );

        let want: Vec<StoredIds> = (0..6)
            .map(|i| (ts[i], trace[i].and_then(id), span[i].and_then(id)))
            .collect();
        assert_eq!(stored_ids(plain_store.as_ref()).await, want);
        assert_eq!(
            stored_ids(dict_store.as_ref()).await,
            want,
            "the encoded file stores the same ids, and none for a null key"
        );
        assert_eq!(
            decoded_records(dict_store.as_ref()).await,
            decoded_records(plain_store.as_ref()).await,
            "every stored field matches the plain load"
        );
    }

    fn logs_mapping_millis() -> Mapping {
        let mut m = base_mapping();
        m.ts_unit = TsUnit::Millis;
        m
    }

    fn negative_ts_file() -> (tempfile::TempDir, PathBuf) {
        let now_ms = NOW_NS / 1_000_000;
        write_with(
            &batch(vec![("ts", i64_col(vec![now_ms, -5, now_ms]))]),
            false,
        )
    }

    const NEGATIVE_MILLIS: &str = "timestamp is before the Unix epoch (-5000000 ns, read as \
                                       ts_unit = millis); the column holds a negative value";

    /// The logs row path refuses a negative resolved timestamp as a row
    /// rejection naming the declared unit, and stores nothing.
    #[tokio::test]
    async fn the_logs_row_path_refuses_a_negative_timestamp() {
        let (_dir, pq) = negative_ts_file();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = load_row(
            Arc::clone(&store),
            &pq,
            "acme",
            &logs_mapping_millis(),
            1,
            1_000,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("a negative timestamp is refused");
        let LoadError::RowRejected { row, reason, .. } = &err else {
            panic!("expected RowRejected, got {err:?}");
        };
        assert_eq!(*row, 1);
        assert_eq!(reason, NEGATIVE_MILLIS);
        assert!(list_data_objects(store.as_ref()).await.is_empty());
    }

    /// The same refusal on the columnar path.
    #[tokio::test]
    async fn the_logs_columnar_path_refuses_a_negative_timestamp() {
        let (_dir, pq) = negative_ts_file();
        let (result, store) = load_columnar(&pq, &logs_mapping_millis()).await;
        let err = result.expect_err("a negative timestamp is refused");
        let LoadError::RowRejected { row, reason, .. } = &err else {
            panic!("expected RowRejected, got {err:?}");
        };
        assert_eq!(*row, 1);
        assert_eq!(reason, NEGATIVE_MILLIS);
        assert!(list_data_objects(store.as_ref()).await.is_empty());
    }

    /// The same refusal on the metrics path.
    #[tokio::test]
    async fn the_metrics_path_refuses_a_negative_timestamp() {
        let mapping = parse_metrics_mapping(
            "[metrics]\nname = \"probe\"\nvalue_column = \"value\"\nts_column = \
                 \"ts\"\nts_unit = \"millis\"\nkind = \"gauge\"\n",
        )
        .expect("valid mapping");
        let now_ms = NOW_NS / 1_000_000;
        let (_dir, pq) = write_with(
            &batch(vec![
                ("ts", i64_col(vec![now_ms, -5, now_ms])),
                (
                    "value",
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
                ),
            ]),
            false,
        );
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = load_metrics(
            Arc::clone(&store),
            &pq,
            "acme",
            &mapping,
            1,
            1_000,
            0,
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            1,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("a negative timestamp is refused");
        let LoadError::RowRejected { row, reason, .. } = &err else {
            panic!("expected RowRejected, got {err:?}");
        };
        assert_eq!(*row, 1);
        assert_eq!(reason, NEGATIVE_MILLIS);
        assert!(list_data_objects(store.as_ref()).await.is_empty());
    }

    async fn load_logs_row(
        pq: &Path,
        mapping: &Mapping,
    ) -> (Result<LoadReport, LoadError>, Arc<dyn ObjectStoreBackend>) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let result = load_row(
            Arc::clone(&store),
            pq,
            "acme",
            mapping,
            1,
            1_000,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await;
        (result, store)
    }

    /// A gauge mapping over `ts` and `value`, declaring `ts_unit`.
    fn metrics_mapping(ts_unit: &str) -> MetricsMapping {
        parse_metrics_mapping(&format!(
            "[metrics]\nname = \"probe\"\nvalue_column = \"value\"\nts_column = \
                 \"ts\"\nts_unit = \"{ts_unit}\"\nkind = \"gauge\"\n"
        ))
        .expect("valid mapping")
    }

    async fn load_metrics_on(
        pq: &Path,
        mapping: &MetricsMapping,
    ) -> (
        Result<MetricsLoadReport, LoadError>,
        Arc<dyn ObjectStoreBackend>,
    ) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let result = load_metrics(
            Arc::clone(&store),
            pq,
            "acme",
            mapping,
            1,
            1_000,
            0,
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            1,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await;
        (result, store)
    }

    /// A file whose `ts` is `ts` beside a `value` column, one row per cell.
    fn ts_file(ts: ArrayRef) -> (tempfile::TempDir, PathBuf) {
        let values: Vec<f64> = (0..ts.len()).map(|i| i as f64).collect();
        write_with(
            &batch(vec![
                ("ts", ts),
                ("value", Arc::new(Float64Array::from(values)) as ArrayRef),
            ]),
            false,
        )
    }

    /// A native `Timestamp(Second)` column scales by its own unit, not by
    /// the declared `ts_unit`, so the refusal names the unit that was
    /// applied: `-5` seconds is `-5000000000` ns, and `nanos` is not what
    /// produced it.
    const NEGATIVE_NATIVE_SECONDS: &str = "timestamp is before the Unix epoch (-5000000000 \
                                               ns, read in the column's own Timestamp unit, \
                                               seconds); the column holds a negative value";

    fn native_seconds_negative_file() -> (tempfile::TempDir, PathBuf) {
        let now_s = NOW_NS / 1_000_000_000;
        ts_file(Arc::new(TimestampSecondArray::from(vec![now_s, -5, now_s])) as ArrayRef)
    }

    fn assert_native_seconds_refusal(err: &LoadError) {
        let LoadError::RowRejected { row, reason, .. } = err else {
            panic!("expected RowRejected, got {err:?}");
        };
        assert_eq!(*row, 1);
        assert_eq!(reason, NEGATIVE_NATIVE_SECONDS);
    }

    /// The logs and metrics refusal spells every native Arrow unit, and an
    /// integer column's declared unit, exactly as the mapping writes it.
    #[test]
    fn the_logs_and_metrics_refusal_spells_every_unit() {
        let native = |unit: TimeUnit| {
            negative_ts_rejection(-5, &DataType::Timestamp(unit, None), TsUnit::Nanos)
        };
        for (unit, name) in [
            (TimeUnit::Second, "seconds"),
            (TimeUnit::Millisecond, "millis"),
            (TimeUnit::Microsecond, "micros"),
            (TimeUnit::Nanosecond, "nanos"),
        ] {
            assert_eq!(
                native(unit),
                format!(
                    "timestamp is before the Unix epoch (-5 ns, read in the column's own \
                         Timestamp unit, {name}); the column holds a negative value"
                )
            );
        }
        assert_eq!(
            negative_ts_rejection(-5_000_000, &DataType::Int64, TsUnit::Millis),
            NEGATIVE_MILLIS
        );
    }

    #[tokio::test]
    async fn a_native_timestamp_refusal_names_the_column_unit_on_every_path() {
        let (_dir, pq) = native_seconds_negative_file();
        let schema = reader_schema_for(&pq);
        let ts_type = schema.as_ref().map_or_else(
            || {
                let file = std::fs::File::open(&pq).expect("open parquet");
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .expect("reader")
                    .schema()
                    .field_with_name("ts")
                    .expect("ts field")
                    .data_type()
                    .clone()
            },
            |s| {
                s.field_with_name("ts")
                    .expect("ts field")
                    .data_type()
                    .clone()
            },
        );
        assert_eq!(
            ts_type,
            DataType::Timestamp(TimeUnit::Second, None),
            "the loader reads a native seconds column"
        );
        let mut logs = base_mapping();
        logs.ts_unit = TsUnit::Nanos;

        let (result, store) = load_logs_row(&pq, &logs).await;
        assert_native_seconds_refusal(&result.expect_err("row path refuses"));
        assert!(list_data_objects(store.as_ref()).await.is_empty());

        let (result, store) = load_columnar(&pq, &logs).await;
        assert_native_seconds_refusal(&result.expect_err("columnar path refuses"));
        assert!(list_data_objects(store.as_ref()).await.is_empty());

        let (result, store) = load_metrics_on(&pq, &metrics_mapping("nanos")).await;
        assert_native_seconds_refusal(&result.expect_err("metrics path refuses"));
        assert!(list_data_objects(store.as_ref()).await.is_empty());
    }

    /// A timestamp of exactly 0 is the epoch, not before it: every path
    /// loads it.
    fn zero_ts_file() -> (tempfile::TempDir, PathBuf) {
        ts_file(i64_col(vec![NOW_NS, 0, NOW_NS]))
    }

    #[tokio::test]
    async fn the_logs_row_path_accepts_a_zero_timestamp() {
        let (_dir, pq) = zero_ts_file();
        let (result, store) = load_logs_row(&pq, &base_mapping()).await;
        let report = result.expect("a zero timestamp loads");
        assert_eq!(report.rows_processed, 3);
        let ts: Vec<i64> = stored_ids(store.as_ref())
            .await
            .into_iter()
            .map(|(ts, _, _)| ts)
            .collect();
        assert_eq!(ts, vec![0, NOW_NS, NOW_NS], "the zero row is stored at 0");
    }

    #[tokio::test]
    async fn the_logs_columnar_path_accepts_a_zero_timestamp() {
        let (_dir, pq) = zero_ts_file();
        let (result, store) = load_columnar(&pq, &base_mapping()).await;
        let report = result.expect("a zero timestamp loads");
        assert_eq!(report.rows_processed, 3);
        assert!(report.columnar_batches_built > 0, "the columnar path ran");
        let ts: Vec<i64> = stored_ids(store.as_ref())
            .await
            .into_iter()
            .map(|(ts, _, _)| ts)
            .collect();
        assert_eq!(ts, vec![0, NOW_NS, NOW_NS], "the zero row is stored at 0");
    }

    #[tokio::test]
    async fn the_metrics_path_accepts_a_zero_timestamp() {
        let (_dir, pq) = zero_ts_file();
        let (result, _store) = load_metrics_on(&pq, &metrics_mapping("nanos")).await;
        let report = result.expect("a zero timestamp loads");
        assert_eq!(report.rows_processed, 3, "every row, the zero one included");
    }
}

mod empty_dictionary_chunk_file {
    use std::path::PathBuf;

    use parquet::column::page::{CompressedPage, Page, PageWriteSpec, PageWriter};
    use parquet::column::writer::{get_column_writer, get_typed_column_writer};
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::{SerializedFileWriter, SerializedPageWriter, TrackedWrite};
    use parquet::schema::parser::parse_message_type;
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    /// Passes every page through except the dictionary page, which it
    /// replaces with one holding no values. The data pages still carry the
    /// keys the real dictionary answered, so a non-null key names a value
    /// the written dictionary does not have.
    struct EmptyDictionaryPage<P> {
        inner: P,
        empty: bool,
    }

    impl<P: PageWriter> PageWriter for EmptyDictionaryPage<P> {
        fn write_page(&mut self, page: CompressedPage) -> parquet::errors::Result<PageWriteSpec> {
            let Page::DictionaryPage {
                encoding,
                is_sorted,
                ..
            } = page.compressed_page()
            else {
                return self.inner.write_page(page);
            };
            if !self.empty {
                return self.inner.write_page(page);
            }
            let empty = Page::DictionaryPage {
                buf: bytes::Bytes::new(),
                num_values: 0,
                encoding: *encoding,
                is_sorted: *is_sorted,
            };
            self.inner.write_page(CompressedPage::new(empty, 0))
        }

        fn close(&mut self) -> parquet::errors::Result<()> {
            self.inner.close()
        }
    }

    /// A three-row file: `ts` plain, and `svc` dictionary-encoded with keys
    /// `[0, null, 0]`, under an empty dictionary page when `empty_dictionary`
    /// is set and under its real one-value dictionary otherwise. The `svc`
    /// chunk is written by a column writer over [`EmptyDictionaryPage`] and
    /// spliced into the row group whole.
    fn write_file(empty_dictionary: bool) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("in.parquet");
        let schema = Arc::new(
            parse_message_type("message log { required int64 ts; optional binary svc (UTF8); }")
                .expect("schema"),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(true)
                .build(),
        );
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer =
            SerializedFileWriter::new(file, schema, Arc::clone(&props)).expect("file writer");
        let svc_descr = writer.schema_descr().column(1);
        let mut rg = writer.next_row_group().expect("row group");

        let mut ts = rg.next_column().expect("ts column").expect("ts writer");
        ts.typed::<Int64Type>()
            .write_batch(&[NOW_NS, NOW_NS + 1, NOW_NS + 2], None, None)
            .expect("write ts");
        ts.close().expect("close ts");

        let mut chunk = TrackedWrite::new(Vec::new());
        let close = {
            let pages = EmptyDictionaryPage {
                inner: SerializedPageWriter::new(&mut chunk),
                empty: empty_dictionary,
            };
            let mut svc = get_typed_column_writer::<ByteArrayType>(get_column_writer(
                svc_descr,
                props,
                Box::new(pages),
            ));
            svc.write_batch(
                &[ByteArray::from("api"), ByteArray::from("api")],
                Some(&[1, 0, 1]),
                None,
            )
            .expect("write svc");
            svc.close().expect("close svc")
        };
        let chunk = bytes::Bytes::from(chunk.into_inner().expect("chunk bytes"));
        rg.append_column(&chunk, close).expect("splice svc");
        rg.close().expect("close row group");
        writer.close().expect("close file");
        (dir, pq)
    }

    fn svc_mapping() -> Mapping {
        let mut m = base_mapping();
        m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
        m
    }

    async fn load_on(
        path: LoadPath,
        pq: &Path,
    ) -> (Result<LoadReport, LoadError>, Arc<dyn ObjectStoreBackend>) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let result = load_instrumented(
            Arc::clone(&store),
            pq,
            "acme",
            &svc_mapping(),
            1,
            1_000,
            0,
            None,
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            path,
            None,
            None,
        )
        .await;
        (result, store)
    }

    /// The loader reads `svc` as a dictionary column, so the batch this
    /// file would decode to is the shape `str_src` and
    /// `resolve_dictionary_column` answer differently.
    fn assert_read_as_dictionary(pq: &Path) {
        let schema = reader_schema_for(pq).expect("a dictionary-preserving schema");
        assert_eq!(
            schema
                .field_with_name("svc")
                .expect("svc field")
                .data_type(),
            &DataType::Dictionary(Box::new(DICT_KEY_TYPE), Box::new(DataType::Utf8)),
        );
    }

    /// An empty dictionary page under a non-null key never reaches either
    /// load path as a batch: the Parquet reader itself fails the decode,
    /// with the same batch refusal on the row path and the columnar path,
    /// and nothing is stored by either.
    #[tokio::test]
    async fn both_paths_refuse_an_empty_dictionary_page_under_a_key() {
        let (_dir, pq) = write_file(true);
        assert_read_as_dictionary(&pq);

        let mut reasons = Vec::new();
        for path in [LoadPath::Row, LoadPath::Columnar] {
            let (result, store) = load_on(path, &pq).await;
            let err = result.expect_err("the corrupt file is refused");
            let LoadError::BatchFailed { reason, .. } = &err else {
                panic!("expected BatchFailed on {path:?}, got {err:?}");
            };
            assert!(
                reason.starts_with("failed to read Parquet batch: "),
                "the reader refuses the chunk on {path:?}: {reason}"
            );
            assert!(list_data_objects(store.as_ref()).await.is_empty());
            reasons.push(reason.clone());
        }
        assert_eq!(reasons[0], reasons[1], "both paths refuse identically");
    }

    /// The control: the same writer with the dictionary page left intact
    /// loads all three rows on both paths, so the refusal above comes from
    /// the emptied dictionary and not from the spliced chunk.
    #[tokio::test]
    async fn the_same_file_with_its_dictionary_loads_on_both_paths() {
        let (_dir, pq) = write_file(false);
        assert_read_as_dictionary(&pq);

        let mut stored = Vec::new();
        for path in [LoadPath::Row, LoadPath::Columnar] {
            let (result, store) = load_on(path, &pq).await;
            let report = result.expect("the intact file loads");
            assert_eq!(report.rows_processed, 3, "every row loads on {path:?}");
            stored.push(decoded_records(store.as_ref()).await);
        }
        assert_eq!(stored[0], stored[1], "both paths store the same records");
    }
}

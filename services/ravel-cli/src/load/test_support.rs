use arrow::array::DictionaryArray;
use arrow::datatypes::Int32Type;

use super::*;

/// A fixed, plausible (post-2020) load-time anchor for the admission
/// checks; the exact value only matters relative to the event timestamps.
pub(super) const NOW_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14

pub(super) fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(cols.into_iter().map(|(n, a)| (n.to_string(), a)))
        .expect("record batch")
}

pub(super) fn i64_col(vals: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

pub(super) fn str_col(vals: Vec<&str>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

/// A minimal mapping over a single `ts` column (nanoseconds).
pub(super) fn base_mapping() -> Mapping {
    Mapping {
        ts_column: "ts".to_string(),
        ts_unit: TsUnit::Nanos,
        body_column: None,
        severity_number_column: None,
        severity_text_column: None,
        trace_id_column: None,
        span_id_column: None,
        resource_attributes: Vec::new(),
        attributes: Vec::new(),
        attrs_map_column: None,
    }
}

pub(super) fn attr(key: &str, column: &str, ty: ColType) -> AttrMap {
    AttrMap {
        key: key.to_string(),
        column: column.to_string(),
        value_type: ty,
    }
}

/// A clock pinned to `NOW_NS`, so the router buckets and routes against the
/// same instant the provisioning `now_ns` uses (as the loader integration
/// tests do).
pub(super) struct FixedClock(pub(super) i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// Load a fixture of one record with `n_attrs` distinct i64 attribute
/// columns (plus one resource attribute) through the real `load`, and return
/// its report. `n_attrs` past the writer's 1000-column budget forces
/// overflow; below it exercises the near-cap path.
pub(super) async fn run_wide_load(n_attrs: usize) -> LoadReport {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("wide.parquet");
    let mut cols: Vec<(String, ArrayRef)> = vec![
        ("ts".to_string(), i64_col(vec![NOW_NS])),
        ("svc".to_string(), str_col(vec!["api"])),
    ];
    let mut attr_toml = String::new();
    for i in 0..n_attrs {
        let name = format!("a{i}");
        cols.push((name.clone(), i64_col(vec![i as i64])));
        attr_toml.push_str(&format!(
            "\n[[attribute]]\nkey = \"{name}\"\ncolumn = \"{name}\"\ntype = \"i64\"\n"
        ));
    }
    let batch = RecordBatch::try_from_iter(cols).expect("wide batch");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let m = parse_mapping(&format!(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n{attr_toml}"
    ))
    .expect("valid mapping");

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        10_000,
        None,
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect("load succeeds")
}

/// Collect every data object (`/l0/`) a store holds, deduped by key, as
/// `(key, size)`. Used to compare two loads structurally end to end.
pub(super) async fn list_data_objects(store: &dyn ObjectStoreBackend) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut page: Option<ravel_object_store::PageToken> = None;
    loop {
        let p = store.list("", page).await.expect("list");
        for o in p.objects {
            if o.key.contains("/l0/") && seen.insert(o.key.clone()) {
                out.push((o.key, o.size));
            }
        }
        match p.next {
            Some(t) => page = Some(t),
            None => break,
        }
    }
    out
}

/// The first host value (by an incrementing suffix) whose loader stream
/// identity routes to `target` under `shards`. Uses the loader's own
/// identity inputs -- resource attributes in mapping order, empty scope --
/// so it matches how `build_record` computes `stream_id`.
pub(super) fn host_for_shard(target: u32, shards: u32) -> String {
    use ravel_types::shard_for_log;
    for i in 0..1_000_000u32 {
        let host = format!("h{i}");
        let resource = vec![
            (
                "service.name".to_string(),
                AttrValue::Str("api".to_string()),
            ),
            ("host".to_string(), AttrValue::Str(host.clone())),
        ];
        let stream_id = log_stream_id(&resource, "", "", &[]);
        if shard_for_log(&stream_id, shards) == target {
            return host;
        }
    }
    panic!("no host routes to shard {target} of {shards}");
}

/// Stride reading (issue #560) turns a sorted, one-shard-per-run input
/// into per-batch shard spread: a 4-row-group file where each row group
/// holds only one shard's host value (`hits.parquet`'s CounterID-sorted
/// shape in miniature). With one stride cursor per row group
/// (`--read-cursors 4`), every `batch_rows`-sized batch draws one row
/// from each group, so every flush touches all 4 shards and
/// `objects_written()` is exactly `batches * shards`. With
/// `--read-cursors 1` (today's sequential read), each batch is one whole
/// row group -- one shard -- so it is exactly `batches * 1`.
///
/// Non-vacuity (prove-the-test): change the `Some(shards as usize)`
/// argument in the first `load` call below to `Some(1)` and the `16`
/// assertion fails (`left: 4, right: 16`), since a single sequential
/// cursor never interleaves the row groups.
/// A multi-row-group fixture whose groups each hold exactly one shard's
/// `host` value (`hits.parquet`'s CounterID-sorted shape in miniature):
/// `shards` row groups of `rows_per_group` rows. Read with
/// `--read-cursors <shards>` and `--batch-rows <rows_per_group>` it yields
/// exactly `rows_per_group` batches, each drawing one row from every group,
/// so every batch touches all `shards` shards.
pub(super) fn sorted_by_shard_fixture(
    shards: u32,
    rows_per_group: usize,
) -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    use parquet::arrow::ArrowWriter;

    let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("sorted_by_shard.parquet");
    let first = batch(vec![
        ("ts", i64_col(vec![NOW_NS; rows_per_group])),
        ("svc", str_col(vec!["api"; rows_per_group])),
        ("host", str_col(vec![hosts[0].as_str(); rows_per_group])),
    ]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
    writer.write(&first).expect("write row group");
    writer.flush().expect("flush row group");
    for host in &hosts[1..] {
        let rg = batch(vec![
            ("ts", i64_col(vec![NOW_NS; rows_per_group])),
            ("svc", str_col(vec!["api"; rows_per_group])),
            ("host", str_col(vec![host.as_str(); rows_per_group])),
        ]);
        writer.write(&rg).expect("write row group");
        writer.flush().expect("flush row group");
    }
    writer.close().expect("close writer");

    let m = parse_mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
    )
    .expect("valid mapping");

    (dir, pq, m)
}

/// [`sorted_by_shard_fixture`] plus one fat record attribute, and the
/// mapping written to disk so the real entry point can be driven over the
/// same file.
///
/// `payload_len` bytes of filler per row is what makes the shard buffer's
/// footprint estimate large enough for the `--target-bytes` regimes to be
/// distinguishable at unit-test row counts: the estimate charges every
/// attribute occurrence's key and uncompressed value bytes once per row,
/// dictionary-encoded or not (`est_columnar_bytes`,
/// crates/ravel-ingest/src/log_shard.rs), so one row's footprint is about
/// `payload_len` and one (batch, shard) slice's is that times its rows.
/// `payload` is a record attribute, not a resource attribute, so stream
/// identity and therefore the shard each row lands on are unchanged.
pub(super) fn fat_attr_sorted_by_shard_fixture(
    shards: u32,
    rows_per_group: usize,
    payload_len: usize,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    Mapping,
) {
    use parquet::arrow::ArrowWriter;

    let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();
    let payload = "p".repeat(payload_len);

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("fat_attr_sorted_by_shard.parquet");
    let mapping_path = dir.path().join("fat_attr_mapping.toml");

    let group = |host: &str| {
        batch(vec![
            ("ts", i64_col(vec![NOW_NS; rows_per_group])),
            ("svc", str_col(vec!["api"; rows_per_group])),
            ("host", str_col(vec![host; rows_per_group])),
            ("payload", str_col(vec![payload.as_str(); rows_per_group])),
        ])
    };
    let first = group(hosts[0].as_str());
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
    writer.write(&first).expect("write row group");
    writer.flush().expect("flush row group");
    for host in &hosts[1..] {
        writer
            .write(&group(host.as_str()))
            .expect("write row group");
        writer.flush().expect("flush row group");
    }
    writer.close().expect("close writer");

    let toml = "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n\n\
             [[attribute]]\nkey = \"payload\"\ncolumn = \"payload\"\ntype = \"str\"\n";
    std::fs::write(&mapping_path, toml).expect("write mapping");
    let m = parse_mapping(toml).expect("valid mapping");

    (dir, pq, mapping_path, m)
}

/// Every `/l0/` object in `store`, decoded to its records and sorted, so
/// two loads that laid the same rows out over a different number of objects
/// can be compared on content alone. `Predicate::And(vec![])` matches every
/// record, so this is the whole object, not a filtered view.
pub(super) async fn decoded_records(store: &dyn ObjectStoreBackend) -> Vec<String> {
    use ravel_logseg::{Predicate, RlogConfig, RlogReader};
    use ravel_object_store::GetRange;

    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for (key, _) in list_data_objects(store).await {
        let got = store.get(&key, GetRange::Full).await.expect("get object");
        let reader = RlogReader::new(got.data.as_ref(), &cfg).expect("open rlog");
        let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan rlog");
        out.extend(rows.into_iter().map(|r| format!("{r:?}")));
    }
    out.sort();
    out
}

// ---------------------------------------------------------------------
// ADR-0109 columnar fast path.
// ---------------------------------------------------------------------

pub(super) fn to_logrecord(r: &NormalizedLogRecord) -> ravel_logseg::LogRecord {
    ravel_logseg::LogRecord {
        stream_id: r.stream_id,
        stream_attrs: r.stream_attrs.clone(),
        ts_ns: r.ts_ns,
        observed_ts_ns: r.observed_ts_ns,
        severity_num: r.severity_num,
        severity_text: r.severity_text.clone(),
        body: r.body.clone(),
        trace_id: r.trace_id,
        span_id: r.span_id,
        flags: r.flags,
        attrs: r.attrs.clone(),
    }
}

/// The row differential-reference records for `batch` under `mapping`.
pub(super) fn row_records(batch: &RecordBatch, mapping: &Mapping) -> Vec<NormalizedLogRecord> {
    let cols = ColumnIndex::resolve(batch, mapping).expect("resolve columns");
    (0..batch.num_rows())
        .map(|r| {
            build_record(
                batch,
                &cols,
                mapping,
                &LogIngestLimits::default(),
                NOW_NS,
                r,
            )
            .expect("build_record")
        })
        .collect()
}

/// Write `batch` to a Parquet file with the default writer properties (which
/// dictionary-encode a `BYTE_ARRAY` column until its dictionary outgrows the
/// page-size limit). The returned `TempDir` must stay alive while the path
/// is read.
pub(super) fn write_parquet(batch: &RecordBatch) -> (tempfile::TempDir, std::path::PathBuf) {
    use parquet::arrow::ArrowWriter;
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("rt.parquet");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut w = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    w.write(batch).expect("write batch");
    w.close().expect("close writer");
    (dir, pq)
}

/// The dictionary-preserving reader schema the loader derives for `pq`, or
/// `None` when no column qualifies. Parses the footer once, exactly as the
/// loader does, then hands the shared metadata to [`load_reader_schema`].
pub(super) fn reader_schema_for(pq: &Path) -> Option<SchemaRef> {
    let metadata = read_input_metadata(&FileInput { path: pq }).expect("read metadata");
    load_reader_schema(&metadata)
}

/// Read `pq` back as one RecordBatch. `loader_schema` selects the reader the
/// loader actually opens: `true` applies [`load_reader_schema`], the
/// dictionary-preserving derivation `open_stride_cursors` uses (#660);
/// `false` lets the reader infer on its own, which is what the loader did
/// before #660 and what the byte-identity anchor compares against.
pub(super) fn read_parquet(pq: &Path, loader_schema: bool) -> RecordBatch {
    let schema = if loader_schema {
        reader_schema_for(pq)
    } else {
        None
    };
    let f = std::fs::File::open(pq).expect("open parquet");
    let builder = match &schema {
        Some(s) => ParquetRecordBatchReaderBuilder::try_new_with_options(
            f,
            ArrowReaderOptions::new().with_schema(Arc::clone(s)),
        ),
        None => ParquetRecordBatchReaderBuilder::try_new(f),
    }
    .expect("reader builder");
    let reader = builder
        // Every fixture here fits one row group, so one oversized read batch
        // yields the whole file and the assertion below holds.
        .with_batch_size(1 << 20)
        .build()
        .expect("reader");
    let mut batches: Vec<RecordBatch> = reader.map(|b| b.expect("read batch")).collect();
    assert_eq!(batches.len(), 1, "fixture fits one read batch");
    batches.pop().expect("one batch")
}

/// The `StrColumnDict` attached to dynamic column `pos`, if any.
/// `dyn_col_dicts` is left empty (not a vec of `None`) when no column in the
/// batch carries a dictionary, so indexing it directly is not safe.
pub(super) fn col_dict(b: &ColumnarLogBatch, pos: usize) -> Option<&StrColumnDict> {
    b.dyn_col_dicts.get(pos).and_then(Option::as_ref)
}

/// A pinned object identity so two objects are comparable byte for byte: the
/// footer stamps `writer_id`/`epoch`/`seq` verbatim, so only a real drift in
/// the encoded records could move a byte.
pub(super) fn fixed_identity() -> ravel_logseg::ObjectIdentity {
    ravel_logseg::ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [9u8; 16],
        writer_epoch: 1,
        writer_seq: 0,
    }
}

pub(super) fn row_object(records: &[NormalizedLogRecord]) -> Vec<u8> {
    let mut w =
        ravel_logseg::RlogWriter::new(ravel_logseg::RlogConfig::default(), fixed_identity());
    for r in records {
        w.push(to_logrecord(r)).expect("push row record");
    }
    w.finish().expect("finish row object")
}

pub(super) fn columnar_object(batch: ColumnarLogBatch) -> Vec<u8> {
    let mut w =
        ravel_logseg::RlogWriter::new(ravel_logseg::RlogConfig::default(), fixed_identity());
    w.push_columnar(batch).expect("push columnar batch");
    w.finish().expect("finish columnar object")
}

pub(super) fn build_columnar_or_panic(batch: &RecordBatch, mapping: &Mapping) -> ColumnarLogBatch {
    let spans = vec![(batch.clone(), 0u64)];
    match build_columnar_batch(&spans, mapping, &LogIngestLimits::default(), NOW_NS) {
        Ok(b) => b,
        Err(ColBuildError::Batch(reason)) => panic!("columnar batch failed: {reason}"),
        Err(ColBuildError::Row { row, reason }) => {
            panic!("columnar row {row} rejected: {reason}")
        }
    }
}

/// Build `batch` through both the row path and the columnar builder and
/// assert (a) the columnar batch equals `from_records` of the row records
/// (ignoring the additive dictionary shapes), and (b) the encoded RLOG
/// objects are byte-for-byte identical (ADR-0109 decision 7). Returns the
/// columnar batch for further inspection (e.g. dictionary attachment).
pub(super) fn assert_paths_match(batch: &RecordBatch, mapping: &Mapping) -> ColumnarLogBatch {
    let records = row_records(batch, mapping);
    let col = build_columnar_or_panic(batch, mapping);

    let logrecords: Vec<ravel_logseg::LogRecord> = records.iter().map(to_logrecord).collect();
    let expected = ColumnarLogBatch::from_records(&logrecords);
    let mut col_no_dict = col.clone();
    col_no_dict.dyn_col_dicts = Vec::new();
    assert_eq!(
        col_no_dict, expected,
        "columnar builder must produce the same batch as from_records of the row records"
    );

    let row_bytes = row_object(&records);
    let col_bytes = columnar_object(col.clone());
    assert_eq!(
        row_bytes, col_bytes,
        "row and columnar RLOG objects must be byte-for-byte identical"
    );
    col
}

/// A dictionary column of optional strings, nulls as null keys.
pub(super) fn opt_dict_col(vals: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(vals.into_iter().collect::<DictionaryArray<Int32Type>>())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_row(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &Mapping,
    shards: u32,
    batch_rows: usize,
    read_cursors: Option<usize>,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<LoadReport, LoadError> {
    load_instrumented(
        store,
        parquet_path,
        tenant,
        mapping,
        shards,
        batch_rows,
        0,
        read_cursors,
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        now_ns,
        clock,
        LoadPath::Row,
        None,
        None,
    )
    .await
}

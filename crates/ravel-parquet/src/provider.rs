//! The DataFusion table over one Parquet table manifest version.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::provider_as_source;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DfResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType, cast, ident,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{transform_binary_to_string, transform_schema_to_view};
use object_store::ObjectMeta;
use object_store::path::Path;
use parquet::arrow::parquet_to_arrow_schema;
use ravel_object_store::ObjectStoreBackend;
use ravel_pqtable::manifest::Manifest;
use ravel_query::PhaseAccounting;
use ravel_types::TenantHash;

use crate::error::ParquetTableError;
use crate::reader::{PinnedFile, PinnedReaderFactory, ReadServices};
use crate::store::{file_path, store_url};

const BINARY_AS_STRING: &str = "binary_as_string";
const CAST_PREFIX: &str = "ravel.cast.";

/// An integer column's reinterpretation, from a `ravel.cast.<column>` option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cast {
    /// Days since the epoch, read as `DATE`.
    DateFromDays,
    /// Seconds since the epoch, read as a timestamp without a time zone.
    TimestampFromSeconds,
    /// Milliseconds since the epoch, read as a timestamp without a time zone.
    TimestampFromMillis,
}

impl Cast {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "date-from-days" => Some(Cast::DateFromDays),
            "timestamp-from-seconds" => Some(Cast::TimestampFromSeconds),
            "timestamp-from-millis" => Some(Cast::TimestampFromMillis),
            _ => None,
        }
    }

    /// `CAST(CAST(col AS <integer>) AS <target>)`, the plan upstream
    /// ClickBench's view produces for `EventDate`.
    fn apply(self, column: Expr) -> Expr {
        match self {
            Cast::DateFromDays => cast(cast(column, DataType::Int32), DataType::Date32),
            Cast::TimestampFromSeconds => cast(
                cast(column, DataType::Int64),
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            Cast::TimestampFromMillis => cast(
                cast(column, DataType::Int64),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
        }
    }
}

/// The table options ADR-2040 D5 admits, parsed from a manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableOptions {
    pub binary_as_string: bool,
    pub casts: BTreeMap<String, Cast>,
}

impl TableOptions {
    /// Parse `options`, refusing any key D5 does not admit and any value it
    /// does not name.
    pub fn parse(
        table: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<Self, ParquetTableError> {
        let invalid = |key: &str, value: &str, reason: &str| ParquetTableError::Option {
            table: table.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            reason: reason.to_string(),
        };
        let mut parsed = TableOptions::default();
        for (key, value) in options {
            if key == BINARY_AS_STRING {
                parsed.binary_as_string = match value.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(invalid(key, value, "expected 'true' or 'false'")),
                };
            } else if let Some(column) = key.strip_prefix(CAST_PREFIX) {
                let cast = Cast::parse(value).ok_or_else(|| {
                    invalid(
                        key,
                        value,
                        "expected 'date-from-days', 'timestamp-from-seconds' or \
                         'timestamp-from-millis'",
                    )
                })?;
                if column.is_empty() {
                    return Err(invalid(key, value, "the option names no column"));
                }
                parsed.casts.insert(column.to_string(), cast);
            } else {
                return Err(invalid(
                    key,
                    value,
                    "only binary_as_string and ravel.cast.<column> are admitted",
                ));
            }
        }
        Ok(parsed)
    }
}

/// A Parquet table as DataFusion sees it: the files of one manifest version,
/// read through [`PinnedReaderFactory`].
///
/// The schema comes from the first file's footer, read through the pinned
/// reader. `parallel` selects the file grouping: up to `target_partitions`
/// contiguous groups when true, and exactly one group in manifest file order,
/// with file-scan repartitioning off, when false. Casts from
/// [`TableOptions`] are a logical projection over the scan.
#[derive(Debug)]
pub struct ParquetTableProvider {
    raw: Arc<RawParquetScan>,
    casts: BTreeMap<String, Cast>,
    /// The projection applying the table's casts; `None` without casts.
    cast_plan: Option<LogicalPlan>,
    schema: SchemaRef,
}

impl ParquetTableProvider {
    /// Build the table for `manifest`, reading the first file's footer.
    /// `stores` maps each file's `(profile, bucket)` to the store it is read
    /// through.
    pub async fn try_new(
        tenant: TenantHash,
        manifest: &Manifest,
        stores: &HashMap<(String, String), Arc<dyn ObjectStoreBackend>>,
        services: ReadServices,
        accounting: PhaseAccounting,
        parallel: bool,
    ) -> Result<Self, ParquetTableError> {
        let table = manifest.table.clone();
        if manifest.dropped {
            return Err(ParquetTableError::Dropped { table });
        }
        if manifest.files.is_empty() {
            return Err(ParquetTableError::NoFiles { table });
        }
        let options = TableOptions::parse(&table, &manifest.options)?;

        let mut files = Vec::with_capacity(manifest.files.len());
        for file in &manifest.files {
            let store = stores
                .get(&(file.profile.clone(), file.bucket.clone()))
                .ok_or_else(|| ParquetTableError::NoStore {
                    table: table.clone(),
                    profile: file.profile.clone(),
                    bucket: file.bucket.clone(),
                })?;
            files.push(Arc::new(PinnedFile {
                file: file.clone(),
                store: Arc::clone(store),
            }));
        }
        let factory = Arc::new(PinnedReaderFactory::new(
            tenant,
            table.clone(),
            manifest.version,
            files.into(),
            services,
            accounting,
        ));

        let first = factory
            .reader(0)
            .ok_or_else(|| ParquetTableError::NoFiles {
                table: table.clone(),
            })?;
        let metadata = first
            .metadata()
            .await
            .map_err(|source| ParquetTableError::Read {
                table: table.clone(),
                source,
            })?;
        let file_metadata = metadata.file_metadata();
        let file_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .map_err(|err| ParquetTableError::Schema {
            table: table.clone(),
            message: err.to_string(),
        })?;
        let mut parquet_options = TableParquetOptions::default();
        parquet_options.global.binary_as_string = options.binary_as_string;
        parquet_options.global.pushdown_filters = true;
        // DataFusion's own schema inference clears the schema's metadata and
        // each top-level field's, then applies these two rewrites in this
        // order.
        let fields: Fields = file_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_metadata(HashMap::new()))
            .collect();
        let mut schema = Schema::new(fields);
        if parquet_options.global.binary_as_string {
            schema = transform_binary_to_string(&schema);
        }
        if parquet_options.global.schema_force_view_types {
            schema = transform_schema_to_view(&schema);
        }
        let raw_schema = Arc::new(schema);

        for column in options.casts.keys() {
            let field =
                raw_schema
                    .field_with_name(column)
                    .map_err(|_| ParquetTableError::Option {
                        table: table.clone(),
                        key: format!("{CAST_PREFIX}{column}"),
                        value: String::new(),
                        reason: "the table has no such column".to_string(),
                    })?;
            if !field.data_type().is_integer() {
                return Err(ParquetTableError::Option {
                    table: table.clone(),
                    key: format!("{CAST_PREFIX}{column}"),
                    value: String::new(),
                    reason: format!("column is {}, not an integer", field.data_type()),
                });
            }
        }

        let paths: Vec<ScanFile> = manifest
            .files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                (
                    file_path(&table, manifest.version, index),
                    file.size,
                    Some(file.etag.clone()),
                    (!file.version.is_empty()).then(|| file.version.clone()),
                    file.footer_len,
                )
            })
            .collect();
        let url = ObjectStoreUrl::parse(store_url(&tenant)).map_err(|source| {
            ParquetTableError::Plan {
                table: table.clone(),
                source,
            }
        })?;
        let raw = Arc::new(RawParquetScan {
            table: table.clone(),
            schema: Arc::clone(&raw_schema),
            url,
            files: paths,
            factory,
            options: parquet_options,
            parallel,
        });

        Self::over(raw, options.casts)
    }

    /// The same table with its file grouping chosen by `parallel`, reading
    /// nothing: the footer read when the table was built is reused.
    pub fn with_parallel(&self, parallel: bool) -> Result<Self, ParquetTableError> {
        let raw = Arc::new(RawParquetScan {
            parallel,
            ..self.raw.as_ref().clone()
        });
        Self::over(raw, self.casts.clone())
    }

    /// Whether the scan is split into up to `target_partitions` file groups.
    pub fn parallel(&self) -> bool {
        self.raw.parallel
    }

    fn over(
        raw: Arc<RawParquetScan>,
        casts: BTreeMap<String, Cast>,
    ) -> Result<Self, ParquetTableError> {
        if casts.is_empty() {
            let schema = Arc::clone(&raw.schema);
            return Ok(ParquetTableProvider {
                raw,
                casts,
                cast_plan: None,
                schema,
            });
        }
        let plan =
            cast_plan(&raw.table, &raw, &casts).map_err(|source| ParquetTableError::Plan {
                table: raw.table.clone(),
                source,
            })?;
        let schema = Arc::new(plan.schema().as_arrow().clone());
        Ok(ParquetTableProvider {
            raw,
            casts,
            cast_plan: Some(plan),
            schema,
        })
    }
}

fn cast_plan(
    table: &str,
    raw: &Arc<RawParquetScan>,
    casts: &BTreeMap<String, Cast>,
) -> DfResult<LogicalPlan> {
    let exprs: Vec<Expr> = raw
        .schema
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            match casts.get(name) {
                Some(cast) => cast.apply(ident(name)).alias(name),
                None => ident(name),
            }
        })
        .collect();
    LogicalPlanBuilder::scan(
        table,
        provider_as_source(Arc::clone(raw) as Arc<dyn TableProvider>),
        None,
    )?
    .project(exprs)?
    .build()
}

#[async_trait]
impl TableProvider for ParquetTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// With casts, the projection over the scan, which DataFusion inlines in
    /// place of this table as it does a view's plan.
    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.cast_plan.as_ref().map(Cow::Borrowed)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        if self.cast_plan.is_some() {
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }
        self.raw.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Some(plan) = &self.cast_plan else {
            return self.raw.scan(state, projection, filters, limit).await;
        };
        let mut builder = LogicalPlanBuilder::from(plan.clone());
        if let Some(projection) = projection {
            let columns = projection
                .iter()
                .map(|&index| Expr::Column(Column::from(plan.schema().qualified_field(index))));
            builder = builder.project(columns)?;
        }
        if limit.is_some() {
            builder = builder.limit(0, limit)?;
        }
        state.create_physical_plan(&builder.build()?).await
    }
}

/// One manifest file as the scan names it: its scan path, size, ETag, version
/// and footer length.
type ScanFile = (String, u64, Option<String>, Option<String>, u32);

/// The Parquet scan itself, before any cast.
#[derive(Clone)]
struct RawParquetScan {
    table: String,
    schema: SchemaRef,
    url: ObjectStoreUrl,
    files: Vec<ScanFile>,
    factory: Arc<PinnedReaderFactory>,
    options: TableParquetOptions,
    parallel: bool,
}

impl fmt::Debug for RawParquetScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawParquetScan")
            .field("table", &self.table)
            .field("files", &self.files.len())
            .field("parallel", &self.parallel)
            .finish_non_exhaustive()
    }
}

impl RawParquetScan {
    fn partitioned_files(&self) -> Vec<PartitionedFile> {
        self.files
            .iter()
            .map(|(path, size, etag, version, footer_len)| {
                PartitionedFile::new_from_meta(ObjectMeta {
                    location: Path::from(path.as_str()),
                    last_modified: Default::default(),
                    size: *size,
                    e_tag: etag.clone(),
                    version: version.clone(),
                })
                .with_metadata_size_hint(*footer_len as usize + 8)
            })
            .collect()
    }
}

/// Split `files` into at most `target_partitions` contiguous groups, the
/// first `files.len() % groups` of them one file longer, or into one group
/// when `parallel` is false. Manifest order is kept either way.
fn file_groups(
    files: Vec<PartitionedFile>,
    parallel: bool,
    target_partitions: usize,
) -> Vec<FileGroup> {
    let groups = if parallel {
        target_partitions.clamp(1, files.len().max(1))
    } else {
        1
    };
    let base = files.len() / groups;
    let extra = files.len() % groups;
    let mut files = files.into_iter();
    (0..groups)
        .map(|group| {
            let len = base + usize::from(group < extra);
            FileGroup::new(files.by_ref().take(len).collect())
        })
        .collect()
}

#[async_trait]
impl TableProvider for RawParquetScan {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Inexact: the reader prunes row groups and filters rows with the
    /// predicate, and DataFusion re-applies every filter above the scan.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let predicate = match conjunction(filters.to_vec()) {
            Some(filter) => {
                let df_schema = Arc::clone(&self.schema).to_dfschema()?;
                Some(state.create_physical_expr(filter, &df_schema)?)
            }
            None => None,
        };
        // Every reader hands DataFusion a footer whose page index it has
        // already loaded and checked, so page pruning, whether from this
        // predicate or from a dynamic filter pushed in after planning, never
        // loads an unchecked one.
        let mut source = ParquetSource::new(Arc::clone(&self.schema))
            .with_table_parquet_options(self.options.clone())
            .with_parquet_file_reader_factory(Arc::clone(&self.factory) as _)
            .with_pushdown_filters(self.options.global.pushdown_filters);
        if let Some(predicate) = predicate {
            source = source.with_predicate(predicate);
        }
        let groups = file_groups(
            self.partitioned_files(),
            self.parallel,
            state.config().target_partitions(),
        );
        let config = FileScanConfigBuilder::new(self.url.clone(), Arc::new(source))
            .with_file_groups(groups)
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_partitioned_by_file_group(!self.parallel)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::test_support::{
        Fixture, binary_parquet_bytes, file_groups_of, int_parquet_bytes, parquet_bytes, read_all,
        read_columns,
    };
    use datafusion::arrow::array::{
        Array, Date32Array, Int64Array, TimestampMillisecondArray, TimestampSecondArray,
    };
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::SessionConfig;
    use datafusion_datasource_parquet::ParquetFileReaderFactory;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::QueryPhase;
    use ravel_types::accounting::AccountedOp;

    /// Each file's footer and page index are one Probe read each; the column
    /// chunks are Scan reads.
    #[tokio::test]
    async fn reads_exact_rows_with_footer_charged_to_probe_and_chunks_to_scan() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let a = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[1, 2], &["x", "y"]),
                true,
            )
            .await;
        let b = fixture
            .put_file(
                &store,
                "lake/t/b.parquet",
                parquet_bytes(&[3, 4, 5], &["z", "w", "v"]),
                true,
            )
            .await;
        let footers: u64 = [&a, &b].iter().map(|f| u64::from(f.footer_len) + 8).sum();
        let page_indexes = fixture.page_index_bytes(&a).await + fixture.page_index_bytes(&b).await;
        let chunks = fixture.column_chunk_bytes(&[&a, &b]).await;
        let (a_entry, b_entry) = (
            fixture.decoded_footer_bytes(&a).await,
            fixture.decoded_footer_bytes(&b).await,
        );

        let accounting = PhaseAccounting::new();
        let table = fixture
            .provider_with("t", 1, vec![a, b], false, accounting.clone())
            .await;
        let ctx = fixture.session(&[("t", table)]);
        let rows = read_all(&ctx, "t", &["a", "b"]).await;
        assert_eq!(rows.expect("rows"), "1|x,2|y,3|z,4|w,5|v");

        let snapshot = accounting.snapshot();
        let probe = snapshot.phase(QueryPhase::Probe);
        let scan = snapshot.phase(QueryPhase::Scan);
        assert_eq!(probe.s3_bytes(AccountedOp::Get), footers + page_indexes);
        assert_eq!(
            probe.s3_requests(AccountedOp::Get),
            4,
            "a footer and a page index per file"
        );
        assert_eq!(
            probe.cache_hits, 1,
            "the scan finds a's footer decoded when the table was built"
        );
        assert_eq!(
            probe.cache_bytes, a_entry,
            "a footer cache hit charges the entry's size"
        );
        assert_eq!(scan.s3_bytes(AccountedOp::Get), chunks);
        assert_eq!(
            scan.s3_requests(AccountedOp::Get),
            4,
            "two columns, two files"
        );

        let rows = read_all(&ctx, "t", &["a", "b"]).await;
        assert_eq!(rows.expect("rows"), "1|x,2|y,3|z,4|w,5|v");
        let second = accounting.snapshot();
        let probe = second.phase(QueryPhase::Probe);
        assert_eq!(
            probe.s3_requests(AccountedOp::Get),
            4,
            "a second read issues no Probe GET"
        );
        assert_eq!(probe.s3_bytes(AccountedOp::Get), footers + page_indexes);
        assert_eq!(probe.cache_hits, 3, "one metadata cache hit per file");
        assert_eq!(probe.cache_bytes, 2 * a_entry + b_entry);
        assert_eq!(
            second.phase(QueryPhase::Scan).s3_requests(AccountedOp::Get),
            4
        );
    }

    #[tokio::test]
    async fn one_group_in_manifest_order_or_up_to_target_partitions() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let mut files = Vec::new();
        for index in 0..5_i64 {
            files.push(
                fixture
                    .put_file(
                        &store,
                        &format!("lake/t/{index}.parquet"),
                        parquet_bytes(&[index], &["r"]),
                        true,
                    )
                    .await,
            );
        }
        let path = |index: usize| file_path("t", 1, index);

        let parallel = fixture.provider("t", 1, files.clone(), true).await;
        let ctx = fixture.session(&[]);
        assert_eq!(ctx.state().config().target_partitions(), 4);
        let groups = file_groups_of(parallel.as_ref(), &ctx).await;
        assert_eq!(
            groups,
            vec![
                vec![path(0), path(1)],
                vec![path(2)],
                vec![path(3)],
                vec![path(4)],
            ]
        );

        let serial = fixture.provider("t", 1, files, false).await;
        let groups = file_groups_of(serial.as_ref(), &ctx).await;
        assert_eq!(groups, vec![(0..5).map(path).collect::<Vec<_>>()]);

        let regrouped = serial.with_parallel(true).expect("regroup");
        assert!(regrouped.parallel());
        assert_eq!(
            file_groups_of(&regrouped, &ctx).await,
            file_groups_of(parallel.as_ref(), &ctx).await
        );

        let ctx = fixture.session(&[("t", serial)]);
        let plan = ctx
            .table("t")
            .await
            .expect("table")
            .create_physical_plan()
            .await
            .expect("plan");
        assert_eq!(plan.output_partitioning().partition_count(), 1);
        let rows = read_all(&ctx, "t", &["a"]).await;
        assert_eq!(rows.expect("rows"), "0,1,2,3,4");
    }

    /// With byte-range repartitioning admitting every file (a 1-byte minimum),
    /// the serial table keeps its one partition and the parallel table's
    /// single file is split, so the serial scan's single group survives the
    /// physical optimizer and is not only what the provider emitted.
    #[tokio::test]
    async fn file_scan_repartitioning_splits_a_parallel_scan_and_never_a_serial_one() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let values: Vec<i64> = (0..2_000).collect();
        let labels: Vec<String> = values.iter().map(|v| format!("row-{v}")).collect();
        let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
        let file = fixture
            .put_file(
                &store,
                "lake/t/big.parquet",
                parquet_bytes(&values, &labels),
                true,
            )
            .await;
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(1);

        let partitions = |parallel: bool| {
            let fixture = &fixture;
            let config = config.clone();
            let file = file.clone();
            async move {
                let table = fixture.provider("t", 1, vec![file], parallel).await;
                let ctx = fixture.session_with(config, &[("t", table)]);
                let frame = ctx.table("t").await.expect("table");
                let plan = frame.create_physical_plan().await.expect("plan");
                let rows = read_all(&ctx, "t", &["a"]).await.expect("rows");
                (plan.output_partitioning().partition_count(), rows)
            }
        };
        let expected: Vec<String> = values.iter().map(i64::to_string).collect();
        let expected = expected.join(",");

        let (serial, rows) = partitions(false).await;
        assert_eq!(serial, 1);
        assert_eq!(rows, expected);
        let (parallel, rows) = partitions(true).await;
        assert_eq!(parallel, 4);
        assert_eq!(rows, expected);
    }

    /// ADR-2040 D6: the scan evaluates pushed-down filters inside the reader.
    #[tokio::test]
    async fn the_scan_evaluates_filters_inside_the_reader() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[1], &["x"]),
                true,
            )
            .await;
        let table = fixture.provider("t", 1, vec![file], false).await;
        let ctx = fixture.session(&[]);
        let filter = datafusion::logical_expr::ident("a").gt(datafusion::logical_expr::lit(0_i64));
        let plan = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("scan");
        let exec = plan
            .downcast_ref::<DataSourceExec>()
            .expect("a Parquet scan");
        let (_, source) = exec
            .downcast_to_file_source::<ParquetSource>()
            .expect("a Parquet file source");
        assert!(source.table_parquet_options().global.pushdown_filters);
        assert!(datafusion::datasource::physical_plan::FileSource::filter(source).is_some());
    }

    #[test]
    fn groups_split_contiguously_and_never_exceed_the_file_count() {
        let files = |n: usize| {
            (0..n)
                .map(|i| PartitionedFile::new(format!("f/{i}"), 1))
                .collect::<Vec<_>>()
        };
        let sizes = |groups: Vec<FileGroup>| groups.iter().map(FileGroup::len).collect::<Vec<_>>();
        assert_eq!(sizes(file_groups(files(5), true, 4)), vec![2, 1, 1, 1]);
        assert_eq!(sizes(file_groups(files(9), true, 4)), vec![3, 2, 2, 2]);
        assert_eq!(sizes(file_groups(files(2), true, 4)), vec![1, 1]);
        assert_eq!(sizes(file_groups(files(5), false, 4)), vec![5]);
        assert_eq!(sizes(file_groups(files(3), true, 0)), vec![3]);
    }

    /// A file whose columns carry `PARQUET:field_id` yields a table schema
    /// with no field metadata, equal to what DataFusion's own inference
    /// returns for the same bytes.
    #[tokio::test]
    async fn the_schema_carries_no_field_metadata_and_equals_datafusions_inference() {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::datasource::file_format::FileFormat;
        use datafusion_datasource_parquet::ParquetFormat;
        use object_store::memory::InMemory;
        use object_store::{ObjectStore, ObjectStoreExt};

        let field_id = |id: &str| {
            HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false).with_metadata(field_id("1")),
            Field::new("b", DataType::Utf8, false).with_metadata(field_id("2")),
        ]));
        let bytes = crate::test_support::write(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        );

        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(&store, "lake/t/ids.parquet", bytes.clone(), true)
            .await;
        let table = fixture.provider("t", 1, vec![file], false).await;
        let ours = table.schema();
        for field in ours.fields() {
            assert!(field.metadata().is_empty(), "{field:?}");
        }
        assert!(ours.metadata().is_empty());

        let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = Path::from("ids.parquet");
        memory.put(&location, bytes.into()).await.expect("put");
        let meta = memory.head(&location).await.expect("head");
        let ctx = fixture.session(&[]);
        let inferred = ParquetFormat::default()
            .infer_schema(&ctx.state(), &memory, &[meta])
            .await
            .expect("infer");
        assert_eq!(ours, inferred);
    }

    #[tokio::test]
    async fn binary_as_string_reads_unannotated_binary_as_strings() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &store,
                "lake/t/bin.parquet",
                binary_parquet_bytes(&[b"hi", b"yo"]),
                true,
            )
            .await;

        let plain = fixture.provider("t", 1, vec![file.clone()], false).await;
        assert_eq!(plain.schema().field(0).data_type(), &DataType::BinaryView);

        let options = BTreeMap::from([(BINARY_AS_STRING.to_string(), "true".to_string())]);
        let strings = fixture
            .provider_with_options("s", 1, vec![file], options)
            .await
            .expect("provider");
        assert_eq!(strings.schema().field(0).data_type(), &DataType::Utf8View);
        let ctx = fixture.session(&[("s", Arc::new(strings))]);
        let rows = read_all(&ctx, "s", &["c"]).await;
        assert_eq!(rows.expect("rows"), "hi,yo");
    }

    #[tokio::test]
    async fn each_cast_gives_its_exact_type_and_values() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &store,
                "lake/t/i.parquet",
                int_parquet_bytes(&[0, 19_000, -1]),
                true,
            )
            .await;

        let cases: [(&str, DataType); 3] = [
            ("date-from-days", DataType::Date32),
            (
                "timestamp-from-seconds",
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                "timestamp-from-millis",
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
        ];
        for (cast, expected) in cases {
            let options = BTreeMap::from([("ravel.cast.n".to_string(), cast.to_string())]);
            let provider = fixture
                .provider_with_options("t", 1, vec![file.clone()], options)
                .await
                .expect("provider");
            assert_eq!(provider.schema().field(0).data_type(), &expected, "{cast}");
            assert_eq!(provider.schema().field(1).data_type(), &DataType::Int64);
            let ctx = fixture.session(&[("t", Arc::new(provider))]);
            let columns = read_columns(&ctx, "t", &["n", "k"]).await.expect(cast);
            let values = &columns[0];
            assert_eq!(values.data_type(), &expected, "{cast}");
            let got: Vec<i64> = match expected {
                DataType::Date32 => values
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("date")
                    .values()
                    .iter()
                    .map(|&v| i64::from(v))
                    .collect(),
                DataType::Timestamp(TimeUnit::Second, _) => values
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .expect("seconds")
                    .values()
                    .to_vec(),
                _ => values
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("millis")
                    .values()
                    .to_vec(),
            };
            assert_eq!(got, vec![0, 19_000, -1], "{cast}");
            let untouched = columns[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("k stays Int64");
            assert_eq!(untouched.values().to_vec(), vec![10, 11, 12]);
        }

        let rendered = {
            let options =
                BTreeMap::from([("ravel.cast.n".to_string(), "date-from-days".to_string())]);
            let provider = fixture
                .provider_with_options("t", 1, vec![file], options)
                .await
                .expect("provider");
            let ctx = fixture.session(&[("t", Arc::new(provider))]);
            read_all(&ctx, "t", &["k", "n"]).await.expect("rows")
        };
        assert_eq!(rendered, "10|1970-01-01,11|2022-01-08,12|1969-12-31");
    }

    #[tokio::test]
    async fn options_outside_d5_are_refused() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(&store, "lake/t/i.parquet", int_parquet_bytes(&[1]), true)
            .await;
        for (key, value, reason) in [
            ("format", "parquet", "only binary_as_string"),
            ("binary_as_string", "yes", "'true' or 'false'"),
            ("ravel.cast.n", "date-from-weeks", "date-from-days"),
            ("ravel.cast.missing", "date-from-days", "no such column"),
            ("ravel.cast.", "date-from-days", "names no column"),
        ] {
            let options = BTreeMap::from([(key.to_string(), value.to_string())]);
            let err = fixture
                .provider_with_options("t", 1, vec![file.clone()], options)
                .await
                .expect_err("option must be refused");
            assert!(
                matches!(&err, ParquetTableError::Option { .. })
                    && err.to_string().contains(reason),
                "{key}: {err}"
            );
        }
        let string_file = fixture
            .put_file(
                &store,
                "lake/t/s.parquet",
                parquet_bytes(&[1], &["x"]),
                true,
            )
            .await;
        let options = BTreeMap::from([("ravel.cast.b".to_string(), "date-from-days".to_string())]);
        let err = fixture
            .provider_with_options("t", 1, vec![string_file], options)
            .await
            .expect_err("a cast of a string column must be refused");
        assert!(err.to_string().contains("not an integer"), "{err}");
    }

    #[tokio::test]
    async fn the_factory_refuses_a_path_outside_its_manifest() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[1], &["x"]),
                true,
            )
            .await;
        let factory = fixture.factory("t", 1, vec![file]);
        let metrics = datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new();
        for path in [
            file_path("t", 1, 1),
            file_path("t", 2, 0),
            file_path("u", 1, 0),
            "t/1/f/00".to_string(),
        ] {
            let err = factory
                .create_reader(0, PartitionedFile::new(path.clone(), 1), None, &metrics)
                .err()
                .expect("a path outside the manifest must be refused");
            assert!(
                err.to_string().contains("is not a file of"),
                "{path}: {err}"
            );
        }
        let mut reader = factory
            .create_reader(
                0,
                PartitionedFile::new(file_path("t", 1, 0), 1),
                None,
                &metrics,
            )
            .expect("the manifest's own file");
        let metadata = reader.get_metadata(None).await.expect("footer");
        assert_eq!(metadata.file_metadata().num_rows(), 1);
    }
}

//! Fixtures shared by this crate's tests: small Parquet files in a
//! `MemoryStore`, manifests over them, and a session that reads through
//! [`SingleStoreRegistry`].
#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::catalog::TableProvider;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::ident;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource_parquet::source::ParquetSource;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use ravel_cache::{Cache, CacheLimits};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_pqtable::manifest::{Manifest, ParquetFile};
use ravel_query::{GetLimiter, PhaseAccounting, ReadCache};
use ravel_types::TenantHash;

use crate::error::{ParquetReadError, ParquetTableError};
use crate::metadata_cache::MetadataCache;
use crate::provider::ParquetTableProvider;
use crate::reader::{PinnedFile, PinnedParquetReader, PinnedReaderFactory, ReadServices};
use crate::store::{SingleStoreRegistry, TenantParquetStore};

pub(crate) const PROFILE: &str = "default";
pub(crate) const BUCKET: &str = "lake";
pub(crate) const TENANT: TenantHash = TenantHash([0x5a; 16]);

pub(crate) fn write(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Bytes {
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, Some(properties)).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    Bytes::from(out)
}

/// One row group with `a: Int64` and `b: Utf8`.
pub(crate) fn parquet_bytes(a: &[i64], b: &[&str]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    write(
        schema,
        vec![
            Arc::new(Int64Array::from(a.to_vec())),
            Arc::new(StringArray::from(b.to_vec())),
        ],
    )
}

/// One `c: Binary` column with no string annotation.
pub(crate) fn binary_parquet_bytes(c: &[&[u8]]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Binary, false)]));
    write(schema, vec![Arc::new(BinaryArray::from(c.to_vec()))])
}

/// `n: Int64` from `n`, and `k: Int64` counting up from 10.
pub(crate) fn int_parquet_bytes(n: &[i64]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
    ]));
    let k: Vec<i64> = (10..).take(n.len()).collect();
    write(
        schema,
        vec![
            Arc::new(Int64Array::from(n.to_vec())),
            Arc::new(Int64Array::from(k)),
        ],
    )
}

pub(crate) fn footer_len_of(bytes: &[u8]) -> u32 {
    let len = bytes.len();
    let mut word = [0u8; 4];
    word.copy_from_slice(&bytes[len - 8..len - 4]);
    u32::from_le_bytes(word)
}

pub(crate) fn manifest_for(table: &str, version: u64, files: &[(Vec<u8>, u64, u32)]) -> Manifest {
    manifest(
        table,
        version,
        files
            .iter()
            .map(|(key, size, footer_len)| ParquetFile {
                profile: PROFILE.to_string(),
                bucket: BUCKET.to_string(),
                key: key.clone(),
                size: *size,
                etag: "\"etag\"".to_string(),
                version: String::new(),
                row_count: 0,
                footer_len: *footer_len,
            })
            .collect(),
        BTreeMap::new(),
    )
}

fn manifest(
    table: &str,
    version: u64,
    files: Vec<ParquetFile>,
    options: BTreeMap<String, String>,
) -> Manifest {
    Manifest {
        table: table.to_string(),
        version,
        dropped: false,
        location: format!("s3://{BUCKET}/{table}/"),
        grant: "test-grant".to_string(),
        files,
        options,
        created_by: "test".to_string(),
        created_unix_ns: 0,
        statement: String::new(),
        apply_nonce: vec![0; 16],
    }
}

/// A store, the services a reader shares, and the tenant's registered store.
pub(crate) struct Fixture {
    store: Arc<dyn ObjectStoreBackend>,
    services: ReadServices,
    registered: Arc<TenantParquetStore>,
}

impl Fixture {
    /// Needs a tokio runtime: the byte cache starts its sweeper.
    pub(crate) fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        let cache = Cache::new(CacheLimits::new(64 << 20, 4096, 8 << 20));
        Fixture {
            store,
            services: ReadServices {
                limiter: Arc::new(GetLimiter::new(4).expect("limiter")),
                cache: ReadCache::Ram(Arc::new(cache)),
                metadata: Arc::new(MetadataCache::new(8 << 20)),
            },
            registered: Arc::new(TenantParquetStore::new(TENANT)),
        }
    }

    /// Write `bytes` at `key` and describe it as a manifest would. With
    /// `record_version` false the manifest pins the ETag alone.
    pub(crate) async fn put_file(
        &self,
        memory: &MemoryStore,
        key: &str,
        bytes: Bytes,
        record_version: bool,
    ) -> ParquetFile {
        let size = bytes.len() as u64;
        let footer_len = footer_len_of(&bytes);
        let footer = &bytes[bytes.len() - 8 - footer_len as usize..bytes.len() - 8];
        let row_count = ParquetMetaDataReader::decode_metadata(footer)
            .expect("footer")
            .file_metadata()
            .num_rows() as u64;
        let put = memory
            .put(key, bytes, PutOptions::default())
            .await
            .expect("put");
        ParquetFile {
            profile: PROFILE.to_string(),
            bucket: BUCKET.to_string(),
            key: key.as_bytes().to_vec(),
            size,
            etag: put.etag.0,
            version: if record_version {
                put.version.0
            } else {
                String::new()
            },
            row_count,
            footer_len,
        }
    }

    fn pinned(&self, file: ParquetFile) -> Arc<PinnedFile> {
        Arc::new(PinnedFile {
            file,
            store: Arc::clone(&self.store),
        })
    }

    pub(crate) fn reader(&self, file: ParquetFile) -> PinnedParquetReader {
        PinnedParquetReader::new(
            TENANT,
            self.pinned(file),
            self.services.clone(),
            PhaseAccounting::new(),
        )
    }

    pub(crate) fn factory(
        &self,
        table: &str,
        version: u64,
        files: Vec<ParquetFile>,
    ) -> PinnedReaderFactory {
        let files: Vec<Arc<PinnedFile>> = files.into_iter().map(|f| self.pinned(f)).collect();
        PinnedReaderFactory::new(
            TENANT,
            table.to_string(),
            version,
            files.into(),
            self.services.clone(),
            PhaseAccounting::new(),
        )
    }

    async fn build(
        &self,
        manifest: Manifest,
        parallel: bool,
        accounting: PhaseAccounting,
    ) -> Result<ParquetTableProvider, ParquetTableError> {
        self.registered.add_manifest(&manifest);
        let stores = HashMap::from([(
            (PROFILE.to_string(), BUCKET.to_string()),
            Arc::clone(&self.store),
        )]);
        ParquetTableProvider::try_new(
            TENANT,
            &manifest,
            &stores,
            self.services.clone(),
            accounting,
            parallel,
        )
        .await
    }

    pub(crate) async fn provider(
        &self,
        table: &str,
        version: u64,
        files: Vec<ParquetFile>,
        parallel: bool,
    ) -> Arc<ParquetTableProvider> {
        self.provider_with(table, version, files, parallel, PhaseAccounting::new())
            .await
    }

    pub(crate) async fn provider_with(
        &self,
        table: &str,
        version: u64,
        files: Vec<ParquetFile>,
        parallel: bool,
        accounting: PhaseAccounting,
    ) -> Arc<ParquetTableProvider> {
        let manifest = manifest(table, version, files, BTreeMap::new());
        Arc::new(
            self.build(manifest, parallel, accounting)
                .await
                .expect("provider"),
        )
    }

    pub(crate) async fn provider_with_options(
        &self,
        table: &str,
        version: u64,
        files: Vec<ParquetFile>,
        options: BTreeMap<String, String>,
    ) -> Result<ParquetTableProvider, ParquetTableError> {
        let manifest = manifest(table, version, files, options);
        self.build(manifest, false, PhaseAccounting::new()).await
    }

    /// Sum of every column chunk's byte length, read from the stored bytes
    /// without going through the reader or its caches.
    pub(crate) async fn column_chunk_bytes(&self, files: &[&ParquetFile]) -> u64 {
        let mut total = 0;
        for file in files {
            let key = String::from_utf8(file.key.clone()).expect("ascii key");
            let bytes = self
                .store
                .get(&key, GetRange::Full)
                .await
                .expect("get")
                .data;
            let end = bytes.len() - 8;
            let footer = &bytes[end - file.footer_len as usize..end];
            let metadata = ParquetMetaDataReader::decode_metadata(footer).expect("footer");
            for row_group in metadata.row_groups() {
                for column in row_group.columns() {
                    total += column.byte_range().1;
                }
            }
        }
        total
    }

    pub(crate) fn session(&self, tables: &[(&str, Arc<ParquetTableProvider>)]) -> SessionContext {
        self.session_with(SessionConfig::new().with_target_partitions(4), tables)
    }

    pub(crate) fn session_with(
        &self,
        config: SessionConfig,
        tables: &[(&str, Arc<ParquetTableProvider>)],
    ) -> SessionContext {
        let registry = Arc::new(SingleStoreRegistry::new(Arc::clone(&self.registered)));
        let runtime = RuntimeEnvBuilder::new()
            .with_object_store_registry(registry)
            .build_arc()
            .expect("runtime");
        let ctx = SessionContext::new_with_config_rt(config, runtime);
        for (name, table) in tables {
            ctx.register_table(*name, Arc::clone(table) as Arc<dyn TableProvider>)
                .expect("register");
        }
        ctx
    }
}

/// `columns` of `table`, each concatenated across batches, in scan order.
pub(crate) async fn read_columns(
    ctx: &SessionContext,
    table: &str,
    columns: &[&str],
) -> DfResult<Vec<ArrayRef>> {
    let batches = ctx
        .table(table)
        .await?
        .select(columns.iter().map(|c| ident(*c)).collect::<Vec<_>>())?
        .collect()
        .await?;
    (0..columns.len())
        .map(|index| {
            let parts: Vec<&dyn datafusion::arrow::array::Array> = batches
                .iter()
                .map(|batch| batch.column(index).as_ref())
                .collect();
            concat(&parts).map_err(DataFusionError::from)
        })
        .collect()
}

/// `columns` of `table` sorted by the first, rendered as `v|v,v|v`.
pub(crate) async fn read_all(
    ctx: &SessionContext,
    table: &str,
    columns: &[&str],
) -> DfResult<String> {
    let batches = ctx
        .table(table)
        .await?
        .select(columns.iter().map(|c| ident(*c)).collect::<Vec<_>>())?
        .sort(vec![ident(columns[0]).sort(true, false)])?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells = (0..batch.num_columns())
                .map(|c| array_value_to_string(batch.column(c), row))
                .collect::<Result<Vec<_>, _>>()?;
            rows.push(cells.join("|"));
        }
    }
    Ok(rows.join(","))
}

/// The file groups `provider`'s own scan emits, as paths, before any
/// physical optimization.
pub(crate) async fn file_groups_of(
    provider: &ParquetTableProvider,
    ctx: &SessionContext,
) -> Vec<Vec<String>> {
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan");
    let exec = plan
        .downcast_ref::<DataSourceExec>()
        .expect("a Parquet scan");
    let (config, _) = exec
        .downcast_to_file_source::<ParquetSource>()
        .expect("a Parquet file source");
    config
        .file_groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|file| file.object_meta.location.to_string())
                .collect()
        })
        .collect()
}

/// The [`ParquetReadError`] somewhere in `err`'s source chain.
pub(crate) fn read_error(err: &DataFusionError) -> Option<ParquetReadError> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = current {
        if let Some(found) = error.downcast_ref::<ParquetReadError>() {
            return Some(found.clone());
        }
        current = error.source();
    }
    None
}

pub(crate) fn assert_file_changed(err: &DataFusionError, key: &str) {
    let found = read_error(err).unwrap_or_else(|| panic!("no ParquetReadError in {err}"));
    assert!(
        matches!(&found, ParquetReadError::FileChanged { key: k } if k == key),
        "{found}"
    );
    let message = err.to_string();
    assert!(message.contains(key), "{message}");
    assert!(message.contains("CREATE OR REPLACE"), "{message}");
}

/// A backend that records the range of every read and advertises the
/// suffix-range capability it was built with.
pub(crate) struct RecordingStore {
    inner: Arc<MemoryStore>,
    suffix_range: bool,
    ranges: Mutex<Vec<GetRange>>,
}

impl RecordingStore {
    pub(crate) fn new(inner: Arc<MemoryStore>, suffix_range: bool) -> Self {
        RecordingStore {
            inner,
            suffix_range,
            ranges: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn ranges(&self) -> Vec<GetRange> {
        self.ranges
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn record(&self, range: &GetRange) {
        self.ranges
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(range.clone());
    }
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.record(&range);
        self.inner.get(key, range).await
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.record(&range);
        self.inner.get_pinned(key, range, pin).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            suffix_range: self.suffix_range,
            ..self.inner.capabilities()
        }
    }
}

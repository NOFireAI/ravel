//! The registered store DataFusion's file scan needs, and the registry that
//! answers only it.
//!
//! [`crate::PinnedReaderFactory`] does every read, so the store behind a
//! Parquet scan exists only because `FileScanConfig` looks one up by URL. It
//! serves `head` from the manifest's recorded size, the one call that needs no
//! I/O, and refuses the rest: a read through it would skip the pin, the
//! limiter and the cache.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use ravel_pqtable::manifest::Manifest;
use ravel_types::TenantHash;
use url::Url;

const SCHEME: &str = "ravel-pq";
const STORE_NAME: &str = "TenantParquetStore";

/// `ravel-pq://<tenant_hash>/`, the one URL a tenant's Parquet scans use.
pub fn store_url(tenant: &TenantHash) -> String {
    format!("{SCHEME}://{}/", tenant.to_hex())
}

/// The path of file `index` of `table` at manifest `version`, under
/// [`store_url`]: `<table>/<version>/f/<index>`. Two tables, or two versions
/// of one table, in the same query never share a path.
pub fn file_path(table: &str, version: u64, index: usize) -> String {
    format!("{table}/{version}/f/{index}")
}

/// The `object_store` 0.13 store for one tenant's Parquet scans.
#[derive(Debug)]
pub struct TenantParquetStore {
    tenant: TenantHash,
    sizes: RwLock<HashMap<String, u64>>,
}

impl TenantParquetStore {
    pub fn new(tenant: TenantHash) -> Self {
        TenantParquetStore {
            tenant,
            sizes: RwLock::new(HashMap::new()),
        }
    }

    pub fn url(&self) -> String {
        store_url(&self.tenant)
    }

    /// Make every file of `manifest` known to `head`, at the size the
    /// manifest recorded.
    pub fn add_manifest(&self, manifest: &Manifest) {
        let mut sizes = self.sizes.write().unwrap_or_else(PoisonError::into_inner);
        for (index, file) in manifest.files.iter().enumerate() {
            sizes.insert(
                file_path(&manifest.table, manifest.version, index),
                file.size,
            );
        }
    }

    fn size_of(&self, location: &Path) -> Option<u64> {
        self.sizes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(location.as_ref())
            .copied()
    }

    fn refuse(&self, operation: &str) -> object_store::Error {
        object_store::Error::NotSupported {
            source: format!(
                "{STORE_NAME} for {} refuses {operation}: Parquet files are read only through \
                 the pinned reader",
                self.url()
            )
            .into(),
        }
    }
}

impl fmt::Display for TenantParquetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{STORE_NAME}({})", self.url())
    }
}

#[async_trait]
impl ObjectStore for TenantParquetStore {
    async fn put_opts(
        &self,
        location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(self.refuse(&format!("put of {location}")))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(self.refuse(&format!("multipart upload of {location}")))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let Some(size) = self.size_of(location) else {
            return Err(object_store::Error::PermissionDenied {
                path: location.to_string(),
                source: format!(
                    "{STORE_NAME} for {} serves only the files of the manifests this query \
                     resolved",
                    self.url()
                )
                .into(),
            });
        };
        if !options.head {
            return Err(self.refuse(&format!("a read of {location}")));
        }
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::empty().boxed()),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: Default::default(),
                size,
                e_tag: None,
                version: None,
            },
            range: 0..size,
            attributes: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let refusal = self.refuse("delete");
        let message = refusal.to_string();
        locations
            .map(move |_| {
                Err(object_store::Error::NotSupported {
                    source: message.clone().into(),
                })
            })
            .boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        stream::once(futures::future::ready(Err(self.refuse("list")))).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(self.refuse("list"))
    }

    async fn copy_opts(
        &self,
        from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(self.refuse(&format!("copy of {from}")))
    }
}

/// An `ObjectStoreRegistry` that answers exactly one URL, the tenant's
/// [`store_url`], with its [`TenantParquetStore`].
///
/// `register_store` declines (returns `None` and installs nothing), and every
/// other URL is an error, so a scan cannot fall back to a local filesystem or
/// reach another tenant's or an `s3://` store.
#[derive(Debug)]
pub struct SingleStoreRegistry {
    url: String,
    store: Arc<TenantParquetStore>,
}

impl SingleStoreRegistry {
    pub fn new(store: Arc<TenantParquetStore>) -> Self {
        SingleStoreRegistry {
            url: store.url(),
            store,
        }
    }

    /// The URL a `FileScanConfig` over this tenant's files names.
    pub fn object_store_url(&self) -> DfResult<ObjectStoreUrl> {
        ObjectStoreUrl::parse(&self.url)
    }
}

impl ObjectStoreRegistry for SingleStoreRegistry {
    fn register_store(
        &self,
        _url: &Url,
        _store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        None
    }

    fn get_store(&self, url: &Url) -> DfResult<Arc<dyn ObjectStore>> {
        if url.as_str() == self.url {
            return Ok(Arc::clone(&self.store) as Arc<dyn ObjectStore>);
        }
        Err(DataFusionError::Execution(format!(
            "no object store for {url}: a Parquet table query reads only {}",
            self.url
        )))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::test_support::manifest_for;
    use object_store::ObjectStoreExt;

    fn tenant(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    fn store_with_one_table() -> Arc<TenantParquetStore> {
        let store = Arc::new(TenantParquetStore::new(tenant(7)));
        store.add_manifest(&manifest_for(
            "hits",
            3,
            &[(b"lake/a.parquet".to_vec(), 1234, 10)],
        ));
        store
    }

    #[tokio::test]
    async fn a_key_outside_the_manifest_is_refused() {
        let store = store_with_one_table();

        let meta = store
            .head(&Path::from(file_path("hits", 3, 0)))
            .await
            .expect("a manifest file is served");
        assert_eq!(meta.size, 1234);

        for outside in [
            file_path("hits", 3, 1),
            file_path("hits", 2, 0),
            file_path("other", 3, 0),
            "lake/a.parquet".to_string(),
        ] {
            let err = store
                .head(&Path::from(outside.clone()))
                .await
                .expect_err("a path outside the manifest must be refused");
            assert!(
                matches!(err, object_store::Error::PermissionDenied { .. }),
                "{outside}: {err}"
            );
        }

        let manifest_path = Path::from(file_path("hits", 3, 0));
        let err = store
            .get(&manifest_path)
            .await
            .expect_err("a read must be refused even for a manifest file");
        assert!(
            matches!(err, object_store::Error::NotSupported { .. }),
            "{err}"
        );
        let err = store
            .put(&manifest_path, PutPayload::from_static(b"x"))
            .await
            .expect_err("a write must be refused");
        assert!(
            matches!(err, object_store::Error::NotSupported { .. }),
            "{err}"
        );
        let err = store
            .delete(&manifest_path)
            .await
            .expect_err("a delete must be refused");
        assert!(
            matches!(err, object_store::Error::NotSupported { .. }),
            "{err}"
        );
        let listed: Vec<_> = store.list(None).collect().await;
        assert_eq!(listed.len(), 1);
        assert!(matches!(
            listed[0],
            Err(object_store::Error::NotSupported { .. })
        ));
        let err = store
            .list_with_delimiter(None)
            .await
            .expect_err("a delimited list must be refused");
        assert!(
            matches!(err, object_store::Error::NotSupported { .. }),
            "{err}"
        );
        let err = store
            .copy(&manifest_path, &Path::from("elsewhere"))
            .await
            .expect_err("a copy must be refused");
        assert!(
            matches!(err, object_store::Error::NotSupported { .. }),
            "{err}"
        );
    }

    #[test]
    fn the_registry_answers_only_its_tenant_url() {
        let store = store_with_one_table();
        let registry = SingleStoreRegistry::new(Arc::clone(&store));

        let own = Url::parse(&store_url(&tenant(7))).expect("url");
        assert_eq!(own.as_str(), "ravel-pq://07070707070707070707070707070707/");
        let found = registry
            .get_store(&own)
            .expect("the tenant's own URL resolves");
        assert_eq!(found.to_string(), store.to_string());

        for refused in [
            store_url(&tenant(8)),
            "ravel-pq://07070707070707070707070707070707/hits/3/f/0".to_string(),
            "s3://customer-lake/".to_string(),
            "file:///".to_string(),
            "file:///etc/".to_string(),
        ] {
            let url = Url::parse(&refused).expect("url");
            let err = registry
                .get_store(&url)
                .expect_err("only the tenant's own URL resolves");
            assert!(err.to_string().contains("reads only"), "{refused}: {err}");
        }
    }

    #[test]
    fn register_store_installs_nothing() {
        let store = store_with_one_table();
        let registry = SingleStoreRegistry::new(Arc::clone(&store));
        let intruder: Arc<dyn ObjectStore> = Arc::new(TenantParquetStore::new(tenant(9)));

        let s3 = Url::parse("s3://customer-lake/").expect("url");
        assert!(
            registry
                .register_store(&s3, Arc::clone(&intruder))
                .is_none()
        );
        assert!(registry.get_store(&s3).is_err());

        let own = Url::parse(&store_url(&tenant(7))).expect("url");
        assert!(registry.register_store(&own, intruder).is_none());
        let found = registry.get_store(&own).expect("still the tenant's store");
        assert_eq!(found.to_string(), store.to_string());
    }
}

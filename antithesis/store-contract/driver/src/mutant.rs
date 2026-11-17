//! The common area's negative configs as a driver-side broken store
//! (ADR-4326 decision 3). RustFS cannot be mutated, so each broken switch
//! lives in [`BrokenStore`], a wrapper over the real store, and
//! `RAVEL_ANTITHESIS_MUTANT=common/<stem>` turns at most one of them on.

use std::sync::Arc;

use bytes::Bytes;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutMode, PutOptions, PutOutcome, StoreError,
};

/// One broken switch, named `<area>/<negative cfg stem>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutant {
    LostResponseNotApplied,
    CasAcceptsStaleVersion,
    ListNeverProgresses,
}

impl Mutant {
    pub const ALL: [Mutant; 3] = [
        Mutant::LostResponseNotApplied,
        Mutant::CasAcceptsStaleVersion,
        Mutant::ListNeverProgresses,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Mutant::LostResponseNotApplied => "common/lost-response-not-applied",
            Mutant::CasAcceptsStaleVersion => "common/cas-accepts-stale-version",
            Mutant::ListNeverProgresses => "common/list-never-progresses",
        }
    }

    /// The mutant a literal names, evaluated at compile time by `inv_mutant!`,
    /// so a misspelled switch fails the build instead of never firing.
    pub const fn from_literal(name: &str) -> Mutant {
        let mut i = 0;
        while i < Mutant::ALL.len() {
            if str_eq(Mutant::ALL[i].name(), name) {
                return Mutant::ALL[i];
            }
            i += 1;
        }
        panic!("inv_mutant!: not a common-area mutant name");
    }

    /// The value of `RAVEL_ANTITHESIS_MUTANT`: unset or empty selects none,
    /// and a name that is not a mutant is refused.
    pub fn select(value: Option<&str>) -> Result<Option<Mutant>, String> {
        let Some(v) = value.map(str::trim).filter(|v| !v.is_empty()) else {
            return Ok(None);
        };
        match Mutant::ALL.iter().find(|m| m.name() == v) {
            Some(m) => Ok(Some(*m)),
            None => Err(format!(
                "RAVEL_ANTITHESIS_MUTANT={v} names no mutant; known: {}",
                Mutant::ALL.map(Mutant::name).join(", ")
            )),
        }
    }
}

const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// The mutant a `<area>/<stem>` literal names, in the shape
/// scripts/check-tla.sh looks for. The driver does not link ravel-invariant.
macro_rules! inv_mutant {
    ($name:literal) => {
        const { $crate::mutant::Mutant::from_literal($name) }
    };
}

/// The store every driver action goes through: the real store, with the
/// selected mutant's switch on.
pub struct BrokenStore {
    inner: Arc<dyn ObjectStoreBackend>,
    mutant: Option<Mutant>,
}

impl BrokenStore {
    pub fn new(inner: Arc<dyn ObjectStoreBackend>, mutant: Option<Mutant>) -> BrokenStore {
        BrokenStore { inner, mutant }
    }

    fn is(&self, m: Mutant) -> bool {
        self.mutant == Some(m)
    }

    /// An overwrite whose response the caller loses: the store acknowledged
    /// it, and the caller only learns that the request was sent. `Err` is a
    /// write that failed before any acknowledgement.
    pub async fn put_dropping_response(&self, key: &str, data: Bytes) -> Result<(), StoreError> {
        if self.is(inv_mutant!("common/lost-response-not-applied")) {
            return Ok(());
        }
        self.inner
            .put(key, data, PutOptions::default())
            .await
            .map(|_| ())
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for BrokenStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if matches!(opts.mode, PutMode::CasVersion(_))
            && self.is(inv_mutant!("common/cas-accepts-stale-version"))
        {
            let overwrite = PutOptions {
                mode: PutMode::Overwrite,
                checksum: opts.checksum,
            };
            return self.inner.put(key, data, overwrite).await;
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        if self.is(inv_mutant!("common/list-never-progresses")) {
            return Ok(ListPage {
                objects: Vec::new(),
                next: Some(page.unwrap_or_else(|| PageToken("stalled".to_string()))),
                unaddressable: Vec::new(),
            });
        }
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

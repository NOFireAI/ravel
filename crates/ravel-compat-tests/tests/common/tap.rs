//! A pass-through store that records every write it forwards, can yield to its
//! caller's executor once before each call, and can play a crashed process, so
//! a test can enumerate the writes a step makes, interleave several steps at
//! store-call granularity, and stop a step dead at a scripted fault.

use std::future::Future;
use std::pin::Pin as StdPin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use ravel_object_store::fault::Op;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};

/// One write as the wrapped store received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub op: Op,
    pub key: String,
}

type CrashWhen = Box<dyn Fn() -> bool + Send + Sync>;

pub struct TapStore<S> {
    inner: S,
    yield_each_call: bool,
    writes: Mutex<Vec<Write>>,
    crash_when: Option<CrashWhen>,
    crashed: AtomicBool,
}

impl<S: ObjectStoreBackend> TapStore<S> {
    /// Records writes and never yields.
    pub fn recording(inner: S) -> Self {
        TapStore {
            inner,
            yield_each_call: false,
            writes: Mutex::new(Vec::new()),
            crash_when: None,
            crashed: AtomicBool::new(false),
        }
    }

    /// Records writes and returns `Pending` once at the start of every call.
    pub fn yielding(inner: S) -> Self {
        TapStore {
            yield_each_call: true,
            ..TapStore::recording(inner)
        }
    }

    /// Forwards calls until `crash_when` holds after one returns, then fails
    /// every later call without forwarding it: the process died there, and
    /// nothing it would have done next reaches the store.
    pub fn crashing(inner: S, crash_when: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        TapStore {
            crash_when: Some(Box::new(crash_when)),
            ..TapStore::recording(inner)
        }
    }

    pub fn crashed(&self) -> bool {
        self.crashed.load(Ordering::SeqCst)
    }

    pub fn writes(&self) -> Vec<Write> {
        self.writes.lock().expect("writes lock").clone()
    }

    pub fn take_writes(&self) -> Vec<Write> {
        std::mem::take(&mut *self.writes.lock().expect("writes lock"))
    }

    async fn enter(&self) -> Result<(), StoreError> {
        if self.yield_each_call {
            YieldOnce(false).await;
        }
        if self.crashed() {
            return Err(StoreError::Permanent("tap: the process crashed".into()));
        }
        Ok(())
    }

    fn after<T>(&self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        if self.crash_when.as_ref().is_some_and(|crash| crash()) {
            self.crashed.store(true, Ordering::SeqCst);
        }
        result
    }

    fn note(&self, op: Op, key: &str) {
        self.writes.lock().expect("writes lock").push(Write {
            op,
            key: key.to_string(),
        });
    }
}

/// `Pending` on the first poll (waking itself), `Ready` on the second.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: StdPin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[async_trait::async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for TapStore<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.enter().await?;
        self.note(Op::Put, key);
        self.after(self.inner.put(key, data, opts).await)
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.enter().await?;
        self.after(self.inner.get(key, range).await)
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.enter().await?;
        self.after(self.inner.get_pinned(key, range, pin).await)
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.enter().await?;
        self.after(self.inner.get_with_pin(key, range).await)
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.enter().await?;
        self.after(self.inner.pin_of(key).await)
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.enter().await?;
        self.note(Op::Put, key);
        self.after(self.inner.put_multipart(key).await)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.enter().await?;
        self.after(self.inner.head(key).await)
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.enter().await?;
        self.after(self.inner.list(prefix, page).await)
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.enter().await?;
        self.after(self.inner.list_after(prefix, start_after, page).await)
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.enter().await?;
        self.after(self.inner.list_delimited(prefix).await)
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.enter().await?;
        self.note(Op::Delete, key);
        self.after(self.inner.delete(key).await)
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}
